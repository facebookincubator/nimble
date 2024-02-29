// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/SchemaSerialization.h"
#include "dwio/alpha/velox/SchemaReader.h"

namespace facebook::alpha {

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
          ALPHA_UNREACHABLE(fmt::format(
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
          ALPHA_UNREACHABLE(fmt::format(
              "Unknown flat map key kind {}.", toString(node->scalarKind())));
      }
    }
    default:
      ALPHA_UNREACHABLE(
          fmt::format("Unknown node kind {}.", toString(node->kind())));
  }
}

std::pair<Kind, ScalarKind> serializationNodeToKind(
    const serialization::SchemaNode* node) {
  switch (node->kind()) {
    case alpha::serialization::Kind_Int8:
      return {Kind::Scalar, ScalarKind::Int8};
    case alpha::serialization::Kind_UInt8:
      return {Kind::Scalar, ScalarKind::UInt8};
    case alpha::serialization::Kind_Int16:
      return {Kind::Scalar, ScalarKind::Int16};
    case alpha::serialization::Kind_UInt16:
      return {Kind::Scalar, ScalarKind::UInt16};
    case alpha::serialization::Kind_Int32:
      return {Kind::Scalar, ScalarKind::Int32};
    case alpha::serialization::Kind_UInt32:
      return {Kind::Scalar, ScalarKind::UInt32};
    case alpha::serialization::Kind_Int64:
      return {Kind::Scalar, ScalarKind::Int64};
    case alpha::serialization::Kind_UInt64:
      return {Kind::Scalar, ScalarKind::UInt64};
    case alpha::serialization::Kind_Float:
      return {Kind::Scalar, ScalarKind::Float};
    case alpha::serialization::Kind_Double:
      return {Kind::Scalar, ScalarKind::Double};
    case alpha::serialization::Kind_Bool:
      return {Kind::Scalar, ScalarKind::Bool};
    case alpha::serialization::Kind_String:
      return {Kind::Scalar, ScalarKind::String};
    case alpha::serialization::Kind_Binary:
      return {Kind::Scalar, ScalarKind::Binary};
    case alpha::serialization::Kind_Row:
      return {Kind::Row, ScalarKind::Undefined};
    case alpha::serialization::Kind_Array:
      return {Kind::Array, ScalarKind::Undefined};
    case alpha::serialization::Kind_ArrayWithOffsets:
      return {Kind::ArrayWithOffsets, ScalarKind::Undefined};
    case alpha::serialization::Kind_Map:
      return {Kind::Map, ScalarKind::Undefined};
    case alpha::serialization::Kind_FlatMapInt8:
      return {Kind::FlatMap, ScalarKind::Int8};
    case alpha::serialization::Kind_FlatMapUInt8:
      return {Kind::FlatMap, ScalarKind::UInt8};
    case alpha::serialization::Kind_FlatMapInt16:
      return {Kind::FlatMap, ScalarKind::Int16};
    case alpha::serialization::Kind_FlatMapUInt16:
      return {Kind::FlatMap, ScalarKind::UInt16};
    case alpha::serialization::Kind_FlatMapInt32:
      return {Kind::FlatMap, ScalarKind::Int32};
    case alpha::serialization::Kind_FlatMapUInt32:
      return {Kind::FlatMap, ScalarKind::UInt32};
    case alpha::serialization::Kind_FlatMapInt64:
      return {Kind::FlatMap, ScalarKind::Int64};
    case alpha::serialization::Kind_FlatMapUInt64:
      return {Kind::FlatMap, ScalarKind::UInt64};
    case alpha::serialization::Kind_FlatMapFloat:
      return {Kind::FlatMap, ScalarKind::Float};
    case alpha::serialization::Kind_FlatMapDouble:
      return {Kind::FlatMap, ScalarKind::Double};
    case alpha::serialization::Kind_FlatMapBool:
      return {Kind::FlatMap, ScalarKind::Bool};
    case alpha::serialization::Kind_FlatMapString:
      return {Kind::FlatMap, ScalarKind::String};
    case alpha::serialization::Kind_FlatMapBinary:
      return {Kind::FlatMap, ScalarKind::Binary};
    default:
      ALPHA_NOT_SUPPORTED(fmt::format(
          "Unknown schema node kind {}.",
          alpha::serialization::EnumNameKind(node->kind())));
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
            auto& node = nodes[i];
            return serialization::CreateSchemaNode(
                builder_,
                nodeToSerializationKind(node.get()),
                node->childrenCount(),
                node->name().has_value()
                    ? builder_.CreateString(node->name().value())
                    : 0,
                node->offset());
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
  std::vector<std::unique_ptr<const SchemaNode>> nodes(nodeCount);

  for (auto i = 0; i < nodeCount; ++i) {
    auto* node = schema->nodes()->Get(i);
    auto kind = serializationNodeToKind(node);
    nodes[i] = std::make_unique<const SchemaNode>(
        kind.first,
        node->offset(),
        node->name() ? std::optional<std::string>(node->name()->str())
                     : std::nullopt,
        kind.second,
        node->children());
  }

  return SchemaReader::getSchema(nodes);
}

} // namespace facebook::alpha
