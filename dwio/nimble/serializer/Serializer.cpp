/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "dwio/nimble/serializer/Serializer.h"

namespace facebook::nimble {

namespace {

// Context stored on FlatMap TypeBuilders to enable encoding layout lookup
// when new keys are dynamically discovered during writing.
class FlatmapEncodingLayoutContext : public TypeBuilderContext {
 public:
  explicit FlatmapEncodingLayoutContext(
      folly::F14FastMap<std::string_view, const EncodingLayoutTree*>
          keyEncodings)
      : keyEncodings_{std::move(keyEncodings)} {}

  const folly::F14FastMap<std::string_view, const EncodingLayoutTree*>
      keyEncodings_;
};

} // namespace

Serializer::Serializer(
    SerializerOptions options,
    const std::shared_ptr<const velox::Type>& type,
    velox::memory::MemoryPool* pool)
    : options_{std::move(options)},
      pool_{pool},
      context_{*pool_},
      buffer_{context_.bufferMemoryPool().get()} {
  const std::shared_ptr<const velox::dwio::common::TypeWithId> typeWithId =
      velox::dwio::common::TypeWithId::create(type);

  // Set up flat map node IDs if specified.
  if (!options_.flatMapColumns.empty()) {
    context_.clearAndReserveFlatMapNodeIds(options_.flatMapColumns.size());
    for (const auto& column : options_.flatMapColumns) {
      context_.addFlatMapNodeId(typeWithId->childByName(column)->id());
    }
  }

  context_.initStatsCollectors(typeWithId);
  writer_ = FieldWriter::create(context_, typeWithId);

  // Set up encoding layout support if encodingLayoutTree is specified.
  // Must be called after writer_ is created so we can traverse the schema.
  buildStreamEncodingLayouts();
}

std::string_view Serializer::serialize(
    const velox::VectorPtr& vector,
    const OrderedRanges& ranges) const {
  buffer_.resize(0);
  serialize(vector, ranges, buffer_);
  return {buffer_.data(), buffer_.size()};
}

void Serializer::buildStreamEncodingLayouts() {
  if (!options_.encodingLayoutTree.has_value()) {
    return;
  }

  // Register handler for dynamically discovered FlatMap keys.
  // When a new key is found during writing, look up its encoding layout
  // and add it to streamEncodingLayouts_.
  context_.setFlatmapFieldAddedEventHandler([this](
                                                const TypeBuilder& flatmap,
                                                std::string_view fieldKey,
                                                const TypeBuilder& fieldType) {
    // Context may be null if this FlatMap isn't covered by the encoding
    // layout tree (e.g., schema evolution from Map, or tree doesn't
    // include this column).
    auto* ctx = flatmap.context<FlatmapEncodingLayoutContext>();
    if (ctx != nullptr) {
      auto it = ctx->keyEncodings_.find(fieldKey);
      if (it != ctx->keyEncodings_.end()) {
        initEncodingLayouts(*it->second, fieldType);
      }
    }
  });

  // Traverse the encoding layout tree to build the stream encoding layouts map.
  const auto& rootType = context_.schemaBuilder().root();
  NIMBLE_CHECK_NOT_NULL(rootType, "SchemaBuilder root must be set");
  initEncodingLayouts(options_.encodingLayoutTree.value(), *rootType);
}

void Serializer::initEncodingLayouts(
    const EncodingLayoutTree& tree,
    const TypeBuilder& typeBuilder) {
  // Helper to add encoding layout to the map for a given stream identifier.
  const auto addLayout = [this, &tree](
                             uint32_t streamOffset,
                             EncodingLayoutTree::StreamIdentifier identifier) {
    if (const auto* layout = tree.encodingLayout(identifier)) {
      streamEncodingLayouts_[streamOffset] = layout;
    }
  };

  // Match the encoding layout tree schema kind with the type builder.
  // For each stream, add the encoding layout to the map if it exists.
  switch (typeBuilder.kind()) {
    case Kind::Scalar: {
      NIMBLE_CHECK_EQ(
          tree.schemaKind(),
          Kind::Scalar,
          "Incompatible encoding layout node. Expecting scalar node.");
      addLayout(
          typeBuilder.asScalar().scalarDescriptor().offset(),
          EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream);
      break;
    }
    case Kind::Row: {
      NIMBLE_CHECK_EQ(
          tree.schemaKind(),
          Kind::Row,
          "Incompatible encoding layout node. Expecting row node.");
      addLayout(
          typeBuilder.asRow().nullsDescriptor().offset(),
          EncodingLayoutTree::StreamIdentifiers::Row::NullsStream);

      // Initialize encoding layouts for children.
      const auto& rowBuilder = typeBuilder.asRow();
      for (uint32_t i = 0;
           i < rowBuilder.childrenCount() && i < tree.childrenCount();
           ++i) {
        initEncodingLayouts(tree.child(i), rowBuilder.childAt(i));
      }
      break;
    }
    case Kind::Array: {
      NIMBLE_CHECK_EQ(
          tree.schemaKind(),
          Kind::Array,
          "Incompatible encoding layout node. Expecting array node.");
      addLayout(
          typeBuilder.asArray().lengthsDescriptor().offset(),
          EncodingLayoutTree::StreamIdentifiers::Array::LengthsStream);

      // Initialize encoding layouts for element child.
      if (tree.childrenCount() > 0) {
        initEncodingLayouts(tree.child(0), typeBuilder.asArray().elements());
      }
      break;
    }
    case Kind::Map: {
      NIMBLE_CHECK_EQ(
          tree.schemaKind(),
          Kind::Map,
          "Incompatible encoding layout node. Expecting map node.");
      addLayout(
          typeBuilder.asMap().lengthsDescriptor().offset(),
          EncodingLayoutTree::StreamIdentifiers::Map::LengthsStream);

      // Initialize encoding layouts for key and value children.
      const auto& mapBuilder = typeBuilder.asMap();
      if (tree.childrenCount() > 0) {
        initEncodingLayouts(tree.child(0), mapBuilder.keys());
      }
      if (tree.childrenCount() > 1) {
        initEncodingLayouts(tree.child(1), mapBuilder.values());
      }
      break;
    }
    case Kind::FlatMap: {
      NIMBLE_CHECK_EQ(
          tree.schemaKind(),
          Kind::FlatMap,
          "Incompatible encoding layout node. Expecting flatmap node.");

      auto& flatMapBuilder = typeBuilder.asFlatMap();
      addLayout(
          flatMapBuilder.nullsDescriptor().offset(),
          EncodingLayoutTree::StreamIdentifiers::FlatMap::NullsStream);

      // For FlatMap, children are keyed by name, not position.
      // Build a map from key name to encoding layout tree child.
      folly::F14FastMap<std::string_view, const EncodingLayoutTree*>
          keyEncodings;
      keyEncodings.reserve(tree.childrenCount());
      for (uint32_t i = 0; i < tree.childrenCount(); ++i) {
        const auto& child = tree.child(i);
        keyEncodings.emplace(child.name(), &child);
      }

      // Store context for dynamic key discovery during writing.
      // FlatMap keys are discovered dynamically via
      // FlatmapFieldAddedEventHandler.
      flatMapBuilder.setContext(
          std::make_unique<FlatmapEncodingLayoutContext>(keyEncodings));
      break;
    }
    default:
      // Other types (ArrayWithOffsets, SlidingWindowMap, etc.) - skip for now.
      break;
  }
}

} // namespace facebook::nimble
