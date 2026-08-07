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

#include <glog/logging.h>

#include "dwio/nimble/encodings/common/EncodingLayout.h"
#include "dwio/nimble/writer/EncodingLayoutStreamContext.h"

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

// Legacy writer spellings with a version header are read-only / migration-only.
// Callers that still pass them are silently upgraded to kSerialization so
// round-trips use the current wire format while call sites migrate. A missing
// version is the production no-header legacy format and must remain stable.
SerializerOptions normalizeWriterVersion(SerializerOptions options) {
  if (!options.version.has_value()) {
    return options;
  }

  const auto version = options.version.value();
  if (version == SerializationVersion::kLegacy ||
      version == SerializationVersion::kLegacyCompact ||
      version == SerializationVersion::kLegacySerialization) {
    LOG_FIRST_N(WARNING, 10)
        << "Serializer constructed with " << toString(version)
        << " (legacy writer spelling); silently upgrading to kSerialization. "
           "Migrate the caller to pass kSerialization explicitly.";
    options.version = SerializationVersion::kSerialization;
  }
  return options;
}

} // namespace

Serializer::Serializer(
    SerializerOptions options,
    const std::shared_ptr<const velox::Type>& type,
    velox::memory::MemoryPool* pool)
    : options_{normalizeWriterVersion(std::move(options))},
      context_{*pool},
      nestedEncodingBufferPool_{
          options_.enableEncoding() &&
                  options_.maxCachedNestedEncodingBuffers > 0
              ? std::make_unique<EncodingBufferPool>(
                    context_.bufferMemoryPool().get(),
                    options_.maxCachedNestedEncodingBuffers)
              : nullptr},
      buffer_{context_.bufferMemoryPool().get()} {
  options_.encodingOptions.encodingBufferPool = nestedEncodingBufferPool_.get();

  const auto version = options_.serializationVersion();
  NIMBLE_CHECK(
      version == SerializationVersion::kLegacy ||
          version == SerializationVersion::kSerialization,
      "Serializer writes must use kLegacy or kSerialization. Got: {}",
      version);
  const std::shared_ptr<const velox::dwio::common::TypeWithId> typeWithId =
      velox::dwio::common::TypeWithId::create(type);

  // Set up flat map node IDs and predefined keys if specified.
  if (!options_.flatMapColumns.empty()) {
    context_.reserveFlatMapNodes(options_.flatMapColumns.size());
    for (const auto& [columnName, keys] : options_.flatMapColumns) {
      auto nodeId = typeWithId->childByName(columnName)->id();
      context_.addFlatMapNodeId(nodeId, keys);
    }
  }

  typeWithId_ = typeWithId;

  // Register handler before creating the writer tree so both predefined and
  // dynamically discovered FlatMap keys are tracked.
  if (!options_.flatMapColumns.empty()) {
    context_.setFlatmapFieldAddedEventHandler(
        [this](
            const TypeBuilder& flatmap,
            std::string_view fieldKey,
            const TypeBuilder& fieldType) {
          const auto& flatmapBuilder = flatmap.asFlatMap();
          inMapStreamOffsets_.insert(
              flatmapBuilder
                  .inMapDescriptorAt(flatmapBuilder.childrenCount() - 1)
                  .offset());

          if (options_.encodingLayoutTree.has_value()) {
            auto* ctx = flatmap.context<FlatmapEncodingLayoutContext>();
            if (ctx != nullptr) {
              auto it = ctx->keyEncodings_.find(fieldKey);
              if (it != ctx->keyEncodings_.end()) {
                initializeEncodingLayouts(fieldType, *it->second);
              }
            }
          }
        });
  }

  // NOTE: Stats collectors are intentionally NOT initialized here.
  // The Serializer never reads column statistics, so skipping
  // initStatsCollectors() avoids unnecessary per-row stats overhead
  // in all field writers (their null statisticsCollector_ guards handle this).
  writer_ = FieldWriter::create(context_, typeWithId);

  if (options_.encodingLayoutTree.has_value()) {
    const auto& rootType = context_.schemaBuilder().root();
    NIMBLE_CHECK_NOT_NULL(rootType, "SchemaBuilder root must be set");
    initializeEncodingLayouts(*rootType, options_.encodingLayoutTree.value());
  }
}

std::string_view Serializer::serialize(
    const velox::VectorPtr& vector,
    const OrderedRanges& ranges) const {
  buffer_.resize(0);
  serialize(vector, ranges, buffer_);
  return {buffer_.data(), buffer_.size()};
}

void Serializer::validateSupportedInput(
    const velox::VectorPtr& vector,
    const OrderedRanges& ranges) const {
  if (options_.flatMapColumns.empty() || !vector->mayHaveNulls()) {
    return;
  }

  bool hasNullRow{false};
  ranges.applyEach([&](auto offset) {
    if (vector->isNullAt(offset)) {
      hasNullRow = true;
    }
  });
  NIMBLE_CHECK(
      !hasNullRow,
      "Top-level row nulls are not supported when serializing FlatMap columns.");
}

void Serializer::initializeEncodingLayouts(
    const TypeBuilder& typeBuilder,
    const EncodingLayoutTree& tree) {
  const auto stampLayout =
      [&tree](
          const StreamDescriptorBuilder& descriptor,
          EncodingLayoutTree::StreamIdentifier identifier) {
        if (const auto* layout = tree.encodingLayout(identifier)) {
          encodingLayoutContext(descriptor).setEncoding(*layout);
        }
      };

  switch (typeBuilder.kind()) {
    case Kind::Scalar: {
      NIMBLE_CHECK_EQ(
          tree.schemaKind(),
          Kind::Scalar,
          "Incompatible encoding layout node. Expecting scalar node.");
      stampLayout(
          typeBuilder.asScalar().scalarDescriptor(),
          EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream);
      break;
    }
    case Kind::Row: {
      NIMBLE_CHECK_EQ(
          tree.schemaKind(),
          Kind::Row,
          "Incompatible encoding layout node. Expecting row node.");
      const auto& rowBuilder = typeBuilder.asRow();
      stampLayout(
          rowBuilder.nullsDescriptor(),
          EncodingLayoutTree::StreamIdentifiers::Row::NullsStream);
      for (uint32_t i = 0;
           i < rowBuilder.childrenCount() && i < tree.childrenCount();
           ++i) {
        initializeEncodingLayouts(rowBuilder.childAt(i), tree.child(i));
      }
      break;
    }
    case Kind::Array: {
      NIMBLE_CHECK_EQ(
          tree.schemaKind(),
          Kind::Array,
          "Incompatible encoding layout node. Expecting array node.");
      const auto& arrayBuilder = typeBuilder.asArray();
      stampLayout(
          arrayBuilder.lengthsDescriptor(),
          EncodingLayoutTree::StreamIdentifiers::Array::LengthsStream);
      if (tree.childrenCount() > 0) {
        initializeEncodingLayouts(arrayBuilder.elements(), tree.child(0));
      }
      break;
    }
    case Kind::Map: {
      NIMBLE_CHECK_EQ(
          tree.schemaKind(),
          Kind::Map,
          "Incompatible encoding layout node. Expecting map node.");
      const auto& mapBuilder = typeBuilder.asMap();
      stampLayout(
          mapBuilder.lengthsDescriptor(),
          EncodingLayoutTree::StreamIdentifiers::Map::LengthsStream);
      if (tree.childrenCount() > 0) {
        initializeEncodingLayouts(mapBuilder.keys(), tree.child(0));
      }
      if (tree.childrenCount() > 1) {
        initializeEncodingLayouts(mapBuilder.values(), tree.child(1));
      }
      break;
    }
    case Kind::FlatMap: {
      NIMBLE_CHECK_EQ(
          tree.schemaKind(),
          Kind::FlatMap,
          "Incompatible encoding layout node. Expecting flatmap node.");
      auto& flatMapBuilder = typeBuilder.asFlatMap();
      stampLayout(
          flatMapBuilder.nullsDescriptor(),
          EncodingLayoutTree::StreamIdentifiers::FlatMap::NullsStream);

      // For FlatMap, children are keyed by name, not position. Register a
      // context on the FlatMap builder so the dynamic-key handler can resolve
      // per-key layouts as new keys are discovered during writing.
      folly::F14FastMap<std::string_view, const EncodingLayoutTree*>
          keyEncodings;
      keyEncodings.reserve(tree.childrenCount());
      for (uint32_t i = 0; i < tree.childrenCount(); ++i) {
        const auto& child = tree.child(i);
        keyEncodings.emplace(child.name(), &child);
      }
      flatMapBuilder.setContext(
          std::make_unique<FlatmapEncodingLayoutContext>(keyEncodings));
      break;
    }
    default:
      break;
  }
}

namespace {

using LayoutMap =
    std::unordered_map<EncodingLayoutTree::StreamIdentifier, EncodingLayout>;

EncodingLayoutTree captureLayoutForType(
    const TypeBuilder& typeBuilder,
    const std::string& name = "") {
  using StreamIds = EncodingLayoutTree::StreamIdentifiers;
  LayoutMap layouts;

  const auto captureStreamLayout =
      [&](const StreamDescriptorBuilder& descriptor,
          EncodingLayoutTree::StreamIdentifier id) {
        if (const auto* ctx =
                descriptor.context<EncodingLayoutStreamContext>()) {
          if (const auto* layout = ctx->encoding()) {
            layouts.emplace(id, *layout);
          }
        }
      };

  std::vector<EncodingLayoutTree> children;
  switch (typeBuilder.kind()) {
    case Kind::Scalar:
      captureStreamLayout(
          typeBuilder.asScalar().scalarDescriptor(),
          StreamIds::Scalar::ScalarStream);
      break;
    case Kind::Row: {
      const auto& row = typeBuilder.asRow();
      captureStreamLayout(row.nullsDescriptor(), StreamIds::Row::NullsStream);
      children.reserve(row.childrenCount());
      for (uint32_t i = 0; i < row.childrenCount(); ++i) {
        children.push_back(captureLayoutForType(row.childAt(i)));
      }
      break;
    }
    case Kind::Array: {
      const auto& array = typeBuilder.asArray();
      captureStreamLayout(
          array.lengthsDescriptor(), StreamIds::Array::LengthsStream);
      children.push_back(captureLayoutForType(array.elements()));
      break;
    }
    case Kind::Map: {
      const auto& map = typeBuilder.asMap();
      captureStreamLayout(
          map.lengthsDescriptor(), StreamIds::Map::LengthsStream);
      children.reserve(2);
      children.push_back(captureLayoutForType(map.keys()));
      children.push_back(captureLayoutForType(map.values()));
      break;
    }
    case Kind::FlatMap: {
      const auto& flatMap = typeBuilder.asFlatMap();
      captureStreamLayout(
          flatMap.nullsDescriptor(), StreamIds::FlatMap::NullsStream);
      children.reserve(flatMap.childrenCount());
      for (uint32_t i = 0; i < flatMap.childrenCount(); ++i) {
        children.push_back(captureLayoutForType(
            flatMap.childAt(i), std::string{flatMap.nameAt(i)}));
      }
      break;
    }
    default:
      break;
  }

  return EncodingLayoutTree{
      typeBuilder.kind(), std::move(layouts), name, std::move(children)};
}

} // namespace

void Serializer::assembleEncodingLayoutTree() const {
  const auto rootType = context_.schemaBuilder().root();
  if (rootType == nullptr) {
    encodingLayoutTree_.reset();
    return;
  }
  encodingLayoutTree_.emplace(captureLayoutForType(*rootType));
}

} // namespace facebook::nimble
