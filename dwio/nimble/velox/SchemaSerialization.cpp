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
              fmt::format(
                  "Unknown scalar kind {}.", toString(node->scalarKind())));
      }
    }
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
              fmt::format(
                  "Unknown flat map key kind {}.",
                  toString(node->scalarKind())));
      }
    }
    default:
      NIMBLE_UNREACHABLE(
          fmt::format("Unknown node kind {}.", toString(node->kind())));
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
      NIMBLE_NOT_SUPPORTED(
          fmt::format(
              "Unknown schema node kind {}.",
              nimble::serialization::EnumNameKind(node->kind())));
  }
}

} // namespace

SchemaSerializer::SchemaSerializer() : builder_{kInitialSchemaSectionSize} {}

std::string_view SchemaSerializer::serialize(
    const SchemaBuilder& schemaBuilder) {
  auto nodes = schemaBuilder.getSchemaNodes();
  builder_.Clear();
  auto schema =
      builder_.CreateVector<flatbuffers::Offset<serialization::SchemaNode>>(
          nodes.size(), [this, &nodes](size_t i) {
            const auto& node = nodes[i];
            return serialization::CreateSchemaNode(
                builder_,
                nodeToSerializationKind(&node),
                node.childrenCount(),
                node.name().has_value()
                    ? builder_.CreateString(node.name().value())
                    : 0,
                node.offset());
          });

  builder_.Finish(serialization::CreateSchema(builder_, schema));
  return {
      reinterpret_cast<const char*>(builder_.GetBufferPointer()),
      builder_.GetSize()};
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
    nodes.emplace_back(
        kind.first,
        node->offset(),
        kind.second,
        node->name() ? std::optional<std::string>(node->name()->str())
                     : std::nullopt,
        node->children());
  }

  return SchemaReader::getSchema(nodes);
}

} // namespace facebook::nimble
