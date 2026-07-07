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
#include "dwio/nimble/velox/SchemaSerialization.h"

#include <optional>
#include <utility>
#include <vector>

#include "dwio/nimble/velox/SchemaReader.h"

namespace facebook::nimble {

namespace {

constexpr uint32_t kInitialSchemaSectionSize = 1 << 20; // 1MB

serialization::Kind nodeToSerializationKind(const SchemaNode* node) {
  switch (node->kind()) {
    case Kind::Scalar: {
      switch (node->scalarKind()) {
        case ScalarKind::Int8:
          return serialization::Kind_Int8;
        case ScalarKind::UInt8:
          return serialization::Kind_UInt8;
        case ScalarKind::Int16:
          return serialization::Kind_Int16;
        case ScalarKind::UInt16:
          return serialization::Kind_UInt16;
        case ScalarKind::Int32:
          return serialization::Kind_Int32;
        case ScalarKind::UInt32:
          return serialization::Kind_UInt32;
        case ScalarKind::Int64:
          return serialization::Kind_Int64;
        case ScalarKind::UInt64:
          return serialization::Kind_UInt64;
        case ScalarKind::Float:
          return serialization::Kind_Float;
        case ScalarKind::Double:
          return serialization::Kind_Double;
        case ScalarKind::Bool:
          return serialization::Kind_Bool;
        case ScalarKind::String:
          return serialization::Kind_String;
        case ScalarKind::Binary:
          return serialization::Kind_Binary;
        default:
          NIMBLE_UNREACHABLE(
              "Unknown scalar kind {}.", toString(node->scalarKind()));
      }
    }
    case Kind::TimestampMicroNano:
      return serialization::Kind_TimestampMicroNano;
    case Kind::Array:
      return serialization::Kind_Array;
    case Kind::ArrayWithOffsets:
      return serialization::Kind_ArrayWithOffsets;
    case Kind::Row:
      return serialization::Kind_Row;
    case Kind::Map:
      return serialization::Kind_Map;
    case Kind::SlidingWindowMap:
      return serialization::Kind_SlidingWindowMap;
    case Kind::FlatMap: {
      switch (node->scalarKind()) {
        case ScalarKind::Int8:
          return serialization::Kind_FlatMapInt8;
        case ScalarKind::UInt8:
          return serialization::Kind_FlatMapUInt8;
        case ScalarKind::Int16:
          return serialization::Kind_FlatMapInt16;
        case ScalarKind::UInt16:
          return serialization::Kind_FlatMapUInt16;
        case ScalarKind::Int32:
          return serialization::Kind_FlatMapInt32;
        case ScalarKind::UInt32:
          return serialization::Kind_FlatMapUInt32;
        case ScalarKind::Int64:
          return serialization::Kind_FlatMapInt64;
        case ScalarKind::UInt64:
          return serialization::Kind_FlatMapUInt64;
        case ScalarKind::Float:
          return serialization::Kind_FlatMapFloat;
        case ScalarKind::Double:
          return serialization::Kind_FlatMapDouble;
        case ScalarKind::Bool:
          return serialization::Kind_FlatMapBool;
        case ScalarKind::String:
          return serialization::Kind_FlatMapString;
        case ScalarKind::Binary:
          return serialization::Kind_FlatMapBinary;
        default:
          NIMBLE_UNREACHABLE(
              "Unknown flat map key kind {}.", toString(node->scalarKind()));
      }
    }
    default:

      NIMBLE_UNREACHABLE("Unknown node kind {}.", toString(node->kind()));
  }
}

std::pair<Kind, ScalarKind> serializationNodeToKind(
    const serialization::SchemaNode* node) {
  switch (node->kind()) {
    case nimble::serialization::Kind_Int8:
      return {Kind::Scalar, ScalarKind::Int8};
    case nimble::serialization::Kind_UInt8:
      return {Kind::Scalar, ScalarKind::UInt8};
    case nimble::serialization::Kind_Int16:
      return {Kind::Scalar, ScalarKind::Int16};
    case nimble::serialization::Kind_UInt16:
      return {Kind::Scalar, ScalarKind::UInt16};
    case nimble::serialization::Kind_Int32:
      return {Kind::Scalar, ScalarKind::Int32};
    case nimble::serialization::Kind_UInt32:
      return {Kind::Scalar, ScalarKind::UInt32};
    case nimble::serialization::Kind_Int64:
      return {Kind::Scalar, ScalarKind::Int64};
    case nimble::serialization::Kind_UInt64:
      return {Kind::Scalar, ScalarKind::UInt64};
    case nimble::serialization::Kind_Float:
      return {Kind::Scalar, ScalarKind::Float};
    case nimble::serialization::Kind_Double:
      return {Kind::Scalar, ScalarKind::Double};
    case nimble::serialization::Kind_Bool:
      return {Kind::Scalar, ScalarKind::Bool};
    case nimble::serialization::Kind_String:
      return {Kind::Scalar, ScalarKind::String};
    case nimble::serialization::Kind_Binary:
      return {Kind::Scalar, ScalarKind::Binary};
    case nimble::serialization::Kind_TimestampMicroNano:
      return {Kind::TimestampMicroNano, ScalarKind::Undefined};
    case nimble::serialization::Kind_Row:
      return {Kind::Row, ScalarKind::Undefined};
    case nimble::serialization::Kind_Array:
      return {Kind::Array, ScalarKind::Undefined};
    case nimble::serialization::Kind_ArrayWithOffsets:
      return {Kind::ArrayWithOffsets, ScalarKind::Undefined};
    case nimble::serialization::Kind_Map:
      return {Kind::Map, ScalarKind::Undefined};
    case nimble::serialization::Kind_SlidingWindowMap:
      return {Kind::SlidingWindowMap, ScalarKind::Undefined};
    case nimble::serialization::Kind_FlatMapInt8:
      return {Kind::FlatMap, ScalarKind::Int8};
    case nimble::serialization::Kind_FlatMapUInt8:
      return {Kind::FlatMap, ScalarKind::UInt8};
    case nimble::serialization::Kind_FlatMapInt16:
      return {Kind::FlatMap, ScalarKind::Int16};
    case nimble::serialization::Kind_FlatMapUInt16:
      return {Kind::FlatMap, ScalarKind::UInt16};
    case nimble::serialization::Kind_FlatMapInt32:
      return {Kind::FlatMap, ScalarKind::Int32};
    case nimble::serialization::Kind_FlatMapUInt32:
      return {Kind::FlatMap, ScalarKind::UInt32};
    case nimble::serialization::Kind_FlatMapInt64:
      return {Kind::FlatMap, ScalarKind::Int64};
    case nimble::serialization::Kind_FlatMapUInt64:
      return {Kind::FlatMap, ScalarKind::UInt64};
    case nimble::serialization::Kind_FlatMapFloat:
      return {Kind::FlatMap, ScalarKind::Float};
    case nimble::serialization::Kind_FlatMapDouble:
      return {Kind::FlatMap, ScalarKind::Double};
    case nimble::serialization::Kind_FlatMapBool:
      return {Kind::FlatMap, ScalarKind::Bool};
    case nimble::serialization::Kind_FlatMapString:
      return {Kind::FlatMap, ScalarKind::String};
    case nimble::serialization::Kind_FlatMapBinary:
      return {Kind::FlatMap, ScalarKind::Binary};
    default:
      NIMBLE_UNSUPPORTED(
          "Unknown schema node kind {}.",
          nimble::serialization::EnumNameKind(node->kind()));
  }
}

void addNode(
    std::vector<SchemaNode>& nodes,
    const Type& type,
    std::optional<std::string> name = std::nullopt) {
  switch (type.kind()) {
    case Kind::Scalar: {
      const auto& scalar = type.asScalar();
      nodes.emplace_back(
          type.kind(),
          scalar.scalarDescriptor().offset(),
          scalar.scalarDescriptor().scalarKind(),
          std::move(name));
      break;
    }
    case Kind::TimestampMicroNano: {
      const auto& timestampMicroNano = type.asTimestampMicroNano();
      nodes.emplace_back(
          type.kind(),
          timestampMicroNano.microsDescriptor().offset(),
          ScalarKind::Int64,
          std::move(name));
      nodes.emplace_back(
          Kind::Scalar,
          timestampMicroNano.nanosDescriptor().offset(),
          ScalarKind::UInt16,
          std::nullopt);
      break;
    }
    case Kind::Array: {
      const auto& array = type.asArray();
      nodes.emplace_back(
          type.kind(),
          array.lengthsDescriptor().offset(),
          ScalarKind::UInt32,
          std::move(name));
      addNode(nodes, *array.elements());
      break;
    }
    case Kind::ArrayWithOffsets: {
      const auto& array = type.asArrayWithOffsets();
      nodes.emplace_back(
          type.kind(),
          array.lengthsDescriptor().offset(),
          ScalarKind::UInt32,
          std::move(name));
      nodes.emplace_back(
          Kind::Scalar,
          array.offsetsDescriptor().offset(),
          ScalarKind::UInt32,
          std::nullopt);
      addNode(nodes, *array.elements());
      break;
    }
    case Kind::Row: {
      const auto& row = type.asRow();
      nodes.emplace_back(
          type.kind(),
          row.nullsDescriptor().offset(),
          ScalarKind::Bool,
          std::move(name),
          row.childrenCount());
      for (size_t i = 0; i < row.childrenCount(); ++i) {
        addNode(nodes, *row.childAt(i), row.nameAt(i));
      }
      break;
    }
    case Kind::Map: {
      const auto& map = type.asMap();
      nodes.emplace_back(
          type.kind(),
          map.lengthsDescriptor().offset(),
          ScalarKind::UInt32,
          std::move(name));
      addNode(nodes, *map.keys());
      addNode(nodes, *map.values());
      break;
    }
    case Kind::SlidingWindowMap: {
      const auto& map = type.asSlidingWindowMap();
      nodes.emplace_back(
          type.kind(),
          map.offsetsDescriptor().offset(),
          ScalarKind::UInt32,
          std::move(name));
      nodes.emplace_back(
          Kind::Scalar,
          map.lengthsDescriptor().offset(),
          ScalarKind::UInt32,
          std::nullopt);
      addNode(nodes, *map.keys());
      addNode(nodes, *map.values());
      break;
    }
    case Kind::FlatMap: {
      const auto& map = type.asFlatMap();
      nodes.emplace_back(
          type.kind(),
          map.nullsDescriptor().offset(),
          map.keyScalarKind(),
          std::move(name),
          map.childrenCount());
      for (size_t i = 0; i < map.childrenCount(); ++i) {
        nodes.emplace_back(
            Kind::Scalar,
            map.inMapDescriptorAt(i).offset(),
            ScalarKind::Bool,
            map.nameAt(i));
        addNode(nodes, *map.childAt(i));
      }
      break;
    }
    default:
      NIMBLE_UNREACHABLE("Unknown type kind {}.", toString(type.kind()));
  }
}

std::vector<SchemaNode> schemaNodes(const Type& type) {
  std::vector<SchemaNode> nodes;
  addNode(nodes, type);
  return nodes;
}

std::string_view serializeNodes(
    flatbuffers::FlatBufferBuilder& builder,
    const std::vector<SchemaNode>& nodes) {
  builder.Clear();
  auto schema = builder.CreateVector<flatbuffers::Offset<
      serialization::SchemaNode>>(nodes.size(), [&builder, &nodes](size_t i) {
    const auto& node = nodes[i];
    // Build the attribute vector for this node. Empty attributes
    // skip the vtable slot so wire output stays byte-identical to
    // pre-attributes serialization for callers that never set
    // attributes.
    flatbuffers::Offset<
        flatbuffers::Vector<flatbuffers::Offset<serialization::StringPair>>>
        attributes = 0;
    if (!node.attributes().empty()) {
      std::vector<flatbuffers::Offset<serialization::StringPair>> attrOffsets;
      attrOffsets.reserve(node.attributes().size());
      for (const auto& [key, value] : node.attributes()) {
        attrOffsets.push_back(
            serialization::CreateStringPair(
                builder,
                builder.CreateString(key),
                builder.CreateString(value)));
      }
      attributes = builder.CreateVector(attrOffsets);
    }
    return serialization::CreateSchemaNode(
        builder,
        nodeToSerializationKind(&node),
        node.childrenCount(),
        node.name().has_value() ? builder.CreateString(node.name().value()) : 0,
        node.offset(),
        attributes);
  });

  builder.Finish(serialization::CreateSchema(builder, schema));
  return {
      reinterpret_cast<const char*>(builder.GetBufferPointer()),
      builder.GetSize()};
}

} // namespace

SchemaSerializer::SchemaSerializer() : builder_{kInitialSchemaSectionSize} {}

std::string_view SchemaSerializer::serialize(
    const SchemaBuilder& schemaBuilder) {
  return serializeNodes(builder_, schemaBuilder.schemaNodes());
}

std::string_view SchemaSerializer::serialize(const Type& type) {
  return serializeNodes(builder_, schemaNodes(type));
}

std::shared_ptr<const Type> SchemaDeserializer::deserialize(
    std::string_view input) {
  auto schema = flatbuffers::GetRoot<serialization::Schema>(input.data());
  auto nodeCount = schema->nodes()->size();
  std::vector<SchemaNode> nodes;
  nodes.reserve(nodeCount);

  for (auto i = 0; i < nodeCount; ++i) {
    auto* node = schema->nodes()->Get(i);
    auto kind = serializationNodeToKind(node);
    // Older NIMBLE files predate the `attributes` flatbuffer field and
    // surface as nullptr here. Treat that case the same as a present-but-
    // empty list so the deserialized SchemaNode always exposes a vector.
    std::vector<std::pair<std::string, std::string>> attributes;
    if (node->attributes() != nullptr) {
      attributes.reserve(node->attributes()->size());
      for (const auto* pair : *node->attributes()) {
        attributes.emplace_back(
            pair->key() ? pair->key()->str() : std::string{},
            pair->value() ? pair->value()->str() : std::string{});
      }
    }
    nodes.emplace_back(
        kind.first,
        node->offset(),
        kind.second,
        node->name() ? std::optional<std::string>(node->name()->str())
                     : std::nullopt,
        node->children(),
        std::move(attributes));
  }

  return SchemaReader::getSchema(nodes);
}

} // namespace facebook::nimble
