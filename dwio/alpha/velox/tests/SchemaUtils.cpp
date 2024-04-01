// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#include <optional>

#include "dwio/alpha/velox/SchemaReader.h"
#include "dwio/alpha/velox/tests/SchemaUtils.h"

namespace facebook::alpha::test {
void verifySchemaNodes(
    const std::vector<std::unique_ptr<const alpha::SchemaNode>>& nodes,
    std::vector<alpha::SchemaNode> expected) {
  ASSERT_EQ(expected.size(), nodes.size());
  for (auto i = 0; i < expected.size(); ++i) {
    ASSERT_TRUE(nodes[i]) << "i = " << i;
    EXPECT_EQ(expected[i].kind(), nodes[i]->kind()) << "i = " << i;
    EXPECT_EQ(expected[i].offset(), nodes[i]->offset()) << "i = " << i;
    EXPECT_EQ(expected[i].name(), nodes[i]->name()) << "i = " << i;
    EXPECT_EQ(expected[i].childrenCount(), nodes[i]->childrenCount())
        << "i = " << i;
    EXPECT_EQ(expected[i].scalarKind(), nodes[i]->scalarKind()) << "i = " << i;
  }
}

void compareSchema(
    uint32_t& index,
    const std::vector<std::unique_ptr<const alpha::SchemaNode>>& nodes,
    const std::shared_ptr<const alpha::Type>& type,
    std::optional<std::string> name = std::nullopt) {
  auto& node = nodes[index++];
  EXPECT_EQ(node->name().has_value(), name.has_value());
  if (name.has_value()) {
    EXPECT_EQ(name.value(), node->name().value());
  }

  EXPECT_EQ(type->kind(), node->kind());

  switch (type->kind()) {
    case alpha::Kind::Scalar: {
      auto& scalar = type->asScalar();
      EXPECT_EQ(scalar.scalarDescriptor().offset(), node->offset());
      EXPECT_EQ(scalar.scalarDescriptor().scalarKind(), node->scalarKind());
      EXPECT_EQ(0, node->childrenCount());
      break;
    }
    case alpha::Kind::Row: {
      auto& row = type->asRow();
      EXPECT_EQ(row.nullsDescriptor().offset(), node->offset());
      EXPECT_EQ(alpha::ScalarKind::Bool, node->scalarKind());
      EXPECT_EQ(row.childrenCount(), node->childrenCount());

      for (auto i = 0; i < row.childrenCount(); ++i) {
        compareSchema(index, nodes, row.childAt(i), row.nameAt(i));
      }

      break;
    }
    case alpha::Kind::Array: {
      auto& array = type->asArray();
      EXPECT_EQ(array.lengthsDescriptor().offset(), node->offset());
      EXPECT_EQ(alpha::ScalarKind::UInt32, node->scalarKind());
      EXPECT_EQ(0, node->childrenCount());

      compareSchema(index, nodes, array.elements());

      break;
    }
    case alpha::Kind::ArrayWithOffsets: {
      auto& arrayWithOffsets = type->asArrayWithOffsets();
      EXPECT_EQ(arrayWithOffsets.lengthsDescriptor().offset(), node->offset());
      EXPECT_EQ(alpha::ScalarKind::UInt32, node->scalarKind());
      EXPECT_EQ(0, node->childrenCount());

      auto& offsetNode = nodes[index++];
      EXPECT_FALSE(offsetNode->name().has_value());
      EXPECT_EQ(Kind::Scalar, offsetNode->kind());
      EXPECT_EQ(ScalarKind::UInt32, offsetNode->scalarKind());
      EXPECT_EQ(
          arrayWithOffsets.offsetsDescriptor().offset(), offsetNode->offset());

      compareSchema(index, nodes, arrayWithOffsets.elements());

      break;
    }
    case alpha::Kind::Map: {
      auto& map = type->asMap();
      EXPECT_EQ(map.lengthsDescriptor().offset(), node->offset());
      EXPECT_EQ(alpha::ScalarKind::UInt32, node->scalarKind());
      EXPECT_EQ(0, node->childrenCount());

      compareSchema(index, nodes, map.keys());
      compareSchema(index, nodes, map.values());

      break;
    }
    case alpha::Kind::FlatMap: {
      auto& map = type->asFlatMap();
      EXPECT_EQ(map.nullsDescriptor().offset(), node->offset());
      EXPECT_EQ(map.keyScalarKind(), node->scalarKind());
      EXPECT_EQ(map.childrenCount(), node->childrenCount());

      for (auto i = 0; i < map.childrenCount(); ++i) {
        auto& inMapNode = nodes[index++];
        ASSERT_TRUE(inMapNode->name().has_value());
        EXPECT_EQ(map.nameAt(i), inMapNode->name().value());
        EXPECT_EQ(Kind::Scalar, inMapNode->kind());
        EXPECT_EQ(ScalarKind::Bool, inMapNode->scalarKind());
        EXPECT_EQ(map.inMapDescriptorAt(i).offset(), inMapNode->offset());
        compareSchema(index, nodes, map.childAt(i));
      }

      break;
    }
    default:
      FAIL() << "Unknown type kind: " << (int)type->kind();
  }
}

void compareSchema(
    const std::vector<std::unique_ptr<const alpha::SchemaNode>>& nodes,
    const std::shared_ptr<const alpha::Type>& root) {
  uint32_t index = 0;
  compareSchema(index, nodes, root);
  EXPECT_EQ(nodes.size(), index);
}

std::shared_ptr<alpha::RowTypeBuilder> row(
    alpha::SchemaBuilder& builder,
    std::vector<std::pair<std::string, std::shared_ptr<alpha::TypeBuilder>>>
        children) {
  auto row = builder.createRowTypeBuilder(children.size());
  for (const auto& pair : children) {
    row->addChild(pair.first, pair.second);
  }
  return row;
}

std::shared_ptr<alpha::ArrayTypeBuilder> array(
    alpha::SchemaBuilder& builder,
    std::shared_ptr<alpha::TypeBuilder> elements) {
  auto array = builder.createArrayTypeBuilder();
  array->setChildren(std::move(elements));

  return array;
}

std::shared_ptr<alpha::ArrayWithOffsetsTypeBuilder> arrayWithOffsets(
    alpha::SchemaBuilder& builder,
    std::shared_ptr<alpha::TypeBuilder> elements) {
  auto arrayWithOffsets = builder.createArrayWithOffsetsTypeBuilder();
  arrayWithOffsets->setChildren(std::move(elements));

  return arrayWithOffsets;
}

std::shared_ptr<alpha::MapTypeBuilder> map(
    alpha::SchemaBuilder& builder,
    std::shared_ptr<alpha::TypeBuilder> keys,
    std::shared_ptr<alpha::TypeBuilder> values) {
  auto map = builder.createMapTypeBuilder();
  map->setChildren(std::move(keys), std::move(values));

  return map;
}

std::shared_ptr<alpha::FlatMapTypeBuilder> flatMap(
    alpha::SchemaBuilder& builder,
    alpha::ScalarKind keyScalarKind,
    std::function<std::shared_ptr<alpha::TypeBuilder>(alpha::SchemaBuilder&)>
        valueFactory,
    FlatMapChildAdder& childAdder) {
  auto map = builder.createFlatMapTypeBuilder(keyScalarKind);
  childAdder.initialize(builder, *map, valueFactory);
  return map;
}

void schema(
    alpha::SchemaBuilder& builder,
    std::function<void(alpha::SchemaBuilder&)> factory) {
  factory(builder);
}

} // namespace facebook::alpha::test
