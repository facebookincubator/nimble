#include <gtest/gtest.h>
#include <optional>

#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/velox/SchemaBuilder.h"
#include "dwio/alpha/velox/SchemaReader.h"
#include "dwio/alpha/velox/SchemaTypes.h"
#include "dwio/alpha/velox/StreamLabels.h"
#include "dwio/alpha/velox/tests/SchemaUtils.h"

using namespace ::facebook;

namespace {
void verifyLabels(
    const std::vector<std::unique_ptr<const alpha::SchemaNode>>& schemaNodes,
    std::vector<std::string_view> expected) {
  alpha::StreamLabels streamLabels{alpha::SchemaReader::getSchema(schemaNodes)};
  std::vector<std::string_view> actual;
  actual.reserve(schemaNodes.size());
  for (size_t i = 0, end = schemaNodes.size(); i < end; ++i) {
    actual.push_back(streamLabels.streamLabel(schemaNodes[i]->offset()));
  }

  EXPECT_EQ(actual, expected);
}

} // namespace

TEST(SchemaTests, SchemaUtils) {
  alpha::SchemaBuilder builder;

  alpha::test::FlatMapChildAdder fm1;
  alpha::test::FlatMapChildAdder fm2;

  SCHEMA(
      builder,
      ROW({
          {"c1", TINYINT()},
          {"c2", ARRAY(TINYINT())},
          {"c3", FLATMAP(Int8, TINYINT(), fm1)},
          {"c4", MAP(TINYINT(), TINYINT())},
          {"c5", FLATMAP(Float, ARRAY(BIGINT()), fm2)},
          {"c6", SMALLINT()},
          {"c7", INTEGER()},
          {"c8", BIGINT()},
          {"c9", REAL()},
          {"c10", DOUBLE()},
          {"c11", BOOLEAN()},
          {"c12", STRING()},
          {"c13", BINARY()},
          {"c14", OFFSETARRAY(UNSIGNEDINTEGER(), INTEGER())},
      }));

  auto nodes = builder.getSchemaNodes();
  alpha::test::verifySchemaNodes(
      nodes,
      {
          {alpha::Kind::Row,
           19,
           std::nullopt,
           alpha::ScalarKind::Undefined,
           14},
          {alpha::Kind::Scalar, 0, "c1", alpha::ScalarKind::Int8},
          {alpha::Kind::Array, 2, "c2", alpha::ScalarKind::Undefined},
          {alpha::Kind::Scalar, 1, std::nullopt, alpha::ScalarKind::Int8},
          {alpha::Kind::FlatMap, 3, "c3", alpha::ScalarKind::Int8, 0},
          {alpha::Kind::Map, 6, "c4", alpha::ScalarKind::Undefined},
          {alpha::Kind::Scalar, 4, std::nullopt, alpha::ScalarKind::Int8},
          {alpha::Kind::Scalar, 5, std::nullopt, alpha::ScalarKind::Int8},
          {alpha::Kind::FlatMap, 7, "c5", alpha::ScalarKind::Float, 0},
          {alpha::Kind::Scalar, 8, "c6", alpha::ScalarKind::Int16},
          {alpha::Kind::Scalar, 9, "c7", alpha::ScalarKind::Int32},
          {alpha::Kind::Scalar, 10, "c8", alpha::ScalarKind::Int64},
          {alpha::Kind::Scalar, 11, "c9", alpha::ScalarKind::Float},
          {alpha::Kind::Scalar, 12, "c10", alpha::ScalarKind::Double},
          {alpha::Kind::Scalar, 13, "c11", alpha::ScalarKind::Bool},
          {alpha::Kind::Scalar, 14, "c12", alpha::ScalarKind::String},
          {alpha::Kind::Scalar, 15, "c13", alpha::ScalarKind::Binary},
          {alpha::Kind::ArrayWithOffsets,
           18,
           "c14",
           alpha::ScalarKind::Undefined},
          {alpha::Kind::Scalar, 16, std::nullopt, alpha::ScalarKind::UInt32},
          {alpha::Kind::Scalar, 17, std::nullopt, alpha::ScalarKind::Int32},
      });

  verifyLabels(nodes, {"/",   "/0",  "/1",  "/1",  "/2",  "/3", "/3",
                       "/3",  "/4",  "/5",  "/6",  "/7",  "/8", "/9",
                       "/10", "/11", "/12", "/13", "/13", "/13"});

  fm2.addChild("f1");

  nodes = builder.getSchemaNodes();
  alpha::test::verifySchemaNodes(
      nodes,
      {
          {alpha::Kind::Row,
           19,
           std::nullopt,
           alpha::ScalarKind::Undefined,
           14},
          {alpha::Kind::Scalar, 0, "c1", alpha::ScalarKind::Int8},
          {alpha::Kind::Array, 2, "c2", alpha::ScalarKind::Undefined},
          {alpha::Kind::Scalar, 1, std::nullopt, alpha::ScalarKind::Int8},
          {alpha::Kind::FlatMap, 3, "c3", alpha::ScalarKind::Int8, 0},
          {alpha::Kind::Map, 6, "c4", alpha::ScalarKind::Undefined},
          {alpha::Kind::Scalar, 4, std::nullopt, alpha::ScalarKind::Int8},
          {alpha::Kind::Scalar, 5, std::nullopt, alpha::ScalarKind::Int8},
          {alpha::Kind::FlatMap, 7, "c5", alpha::ScalarKind::Float, 1},
          {alpha::Kind::Scalar, 20, "f1", alpha::ScalarKind::Bool},
          {alpha::Kind::Array, 22, std::nullopt, alpha::ScalarKind::Undefined},
          {alpha::Kind::Scalar, 21, std::nullopt, alpha::ScalarKind::Int64},
          {alpha::Kind::Scalar, 8, "c6", alpha::ScalarKind::Int16},
          {alpha::Kind::Scalar, 9, "c7", alpha::ScalarKind::Int32},
          {alpha::Kind::Scalar, 10, "c8", alpha::ScalarKind::Int64},
          {alpha::Kind::Scalar, 11, "c9", alpha::ScalarKind::Float},
          {alpha::Kind::Scalar, 12, "c10", alpha::ScalarKind::Double},
          {alpha::Kind::Scalar, 13, "c11", alpha::ScalarKind::Bool},
          {alpha::Kind::Scalar, 14, "c12", alpha::ScalarKind::String},
          {alpha::Kind::Scalar, 15, "c13", alpha::ScalarKind::Binary},
          {alpha::Kind::ArrayWithOffsets,
           18,
           "c14",
           alpha::ScalarKind::Undefined},
          {alpha::Kind::Scalar, 16, std::nullopt, alpha::ScalarKind::UInt32},
          {alpha::Kind::Scalar, 17, std::nullopt, alpha::ScalarKind::Int32},
      });

  verifyLabels(
      nodes, {"/",  "/0",    "/1",    "/1",    "/2",  "/3",  "/3", "/3",
              "/4", "/4/f1", "/4/f1", "/4/f1", "/5",  "/6",  "/7", "/8",
              "/9", "/10",   "/11",   "/12",   "/13", "/13", "/13"});

  fm1.addChild("f1");
  fm1.addChild("f2");
  fm2.addChild("f2");
  fm2.addChild("f3");

  nodes = builder.getSchemaNodes();
  alpha::test::verifySchemaNodes(
      nodes,
      {
          {alpha::Kind::Row,
           19,
           std::nullopt,
           alpha::ScalarKind::Undefined,
           14},
          {alpha::Kind::Scalar, 0, "c1", alpha::ScalarKind::Int8},
          {alpha::Kind::Array, 2, "c2", alpha::ScalarKind::Undefined},
          {alpha::Kind::Scalar, 1, std::nullopt, alpha::ScalarKind::Int8},
          {alpha::Kind::FlatMap, 3, "c3", alpha::ScalarKind::Int8, 2},
          {alpha::Kind::Scalar, 23, "f1", alpha::ScalarKind::Bool},
          {alpha::Kind::Scalar, 24, std::nullopt, alpha::ScalarKind::Int8},
          {alpha::Kind::Scalar, 25, "f2", alpha::ScalarKind::Bool},
          {alpha::Kind::Scalar, 26, std::nullopt, alpha::ScalarKind::Int8},
          {alpha::Kind::Map, 6, "c4", alpha::ScalarKind::Undefined},
          {alpha::Kind::Scalar, 4, std::nullopt, alpha::ScalarKind::Int8},
          {alpha::Kind::Scalar, 5, std::nullopt, alpha::ScalarKind::Int8},
          {alpha::Kind::FlatMap, 7, "c5", alpha::ScalarKind::Float, 3},
          {alpha::Kind::Scalar, 20, "f1", alpha::ScalarKind::Bool},
          {alpha::Kind::Array, 22, std::nullopt, alpha::ScalarKind::Undefined},
          {alpha::Kind::Scalar, 21, std::nullopt, alpha::ScalarKind::Int64},
          {alpha::Kind::Scalar, 27, "f2", alpha::ScalarKind::Bool},
          {alpha::Kind::Array, 29, std::nullopt, alpha::ScalarKind::Undefined},
          {alpha::Kind::Scalar, 28, std::nullopt, alpha::ScalarKind::Int64},
          {alpha::Kind::Scalar, 30, "f3", alpha::ScalarKind::Bool},
          {alpha::Kind::Array, 32, std::nullopt, alpha::ScalarKind::Undefined},
          {alpha::Kind::Scalar, 31, std::nullopt, alpha::ScalarKind::Int64},
          {alpha::Kind::Scalar, 8, "c6", alpha::ScalarKind::Int16},
          {alpha::Kind::Scalar, 9, "c7", alpha::ScalarKind::Int32},
          {alpha::Kind::Scalar, 10, "c8", alpha::ScalarKind::Int64},
          {alpha::Kind::Scalar, 11, "c9", alpha::ScalarKind::Float},
          {alpha::Kind::Scalar, 12, "c10", alpha::ScalarKind::Double},
          {alpha::Kind::Scalar, 13, "c11", alpha::ScalarKind::Bool},
          {alpha::Kind::Scalar, 14, "c12", alpha::ScalarKind::String},
          {alpha::Kind::Scalar, 15, "c13", alpha::ScalarKind::Binary},
          {alpha::Kind::ArrayWithOffsets,
           18,
           "c14",
           alpha::ScalarKind::Undefined},
          {alpha::Kind::Scalar, 16, std::nullopt, alpha::ScalarKind::UInt32},
          {alpha::Kind::Scalar, 17, std::nullopt, alpha::ScalarKind::Int32},
      });

  verifyLabels(
      nodes, {"/",     "/0",    "/1",    "/1",    "/2",    "/2/f1", "/2/f1",
              "/2/f2", "/2/f2", "/3",    "/3",    "/3",    "/4",    "/4/f1",
              "/4/f1", "/4/f1", "/4/f2", "/4/f2", "/4/f2", "/4/f3", "/4/f3",
              "/4/f3", "/5",    "/6",    "/7",    "/8",    "/9",    "/10",
              "/11",   "/12",   "/13",   "/13",   "/13"});
}

TEST(SchemaTests, RoundTrip) {
  alpha::SchemaBuilder builder;
  // ROW(c1:INT, c2:FLATMAP<TINYINT, ARRAY<DOUBLE>>, c3:MAP<VARCHAR, REAL>,
  // c4:FLATMAP<BIGINT, INT>, c5:BOOL, c6:OFFSETARRAY<FLOAT>)

  auto row = builder.createRowTypeBuilder(6);
  {
    auto scalar = builder.createScalarTypeBuilder(alpha::ScalarKind::Int32);
    row->addChild("c1", scalar);
  }

  auto flatMapCol2 = builder.createFlatMapTypeBuilder(alpha::ScalarKind::Int8);
  row->addChild("c2", flatMapCol2);

  {
    auto map = builder.createMapTypeBuilder();
    auto keys = builder.createScalarTypeBuilder(alpha::ScalarKind::String);
    auto values = builder.createScalarTypeBuilder(alpha::ScalarKind::Float);
    map->setChildren(std::move(keys), std::move(values));
    row->addChild("c3", map);
  }

  auto flatMapCol4 = builder.createFlatMapTypeBuilder(alpha::ScalarKind::Int64);
  row->addChild("c4", flatMapCol4);

  {
    auto scalar = builder.createScalarTypeBuilder(alpha::ScalarKind::Bool);
    row->addChild("c5", scalar);
  }

  {
    auto arrayWithOffsets = builder.createArrayWithOffsetsTypeBuilder();
    auto offsets = builder.createScalarTypeBuilder(alpha::ScalarKind::UInt32);
    auto lengths = builder.createScalarTypeBuilder(alpha::ScalarKind::Int32);
    arrayWithOffsets->setChildren(std::move(offsets), std::move(lengths));
    row->addChild("c6", arrayWithOffsets);
  }

  auto nodes = builder.getSchemaNodes();
  alpha::test::verifySchemaNodes(
      nodes,
      {
          {alpha::Kind::Row, 0, std::nullopt, alpha::ScalarKind::Undefined, 6},
          {alpha::Kind::Scalar, 1, "c1", alpha::ScalarKind::Int32, 0},
          {alpha::Kind::FlatMap, 2, "c2", alpha::ScalarKind::Int8, 0},
          {alpha::Kind::Map, 3, "c3"},
          {alpha::Kind::Scalar, 4, std::nullopt, alpha::ScalarKind::String},
          {alpha::Kind::Scalar, 5, std::nullopt, alpha::ScalarKind::Float},
          {alpha::Kind::FlatMap, 6, "c4", alpha::ScalarKind::Int64, 0},
          {alpha::Kind::Scalar, 7, "c5", alpha::ScalarKind::Bool},
          {alpha::Kind::ArrayWithOffsets, 8, "c6"},
          {alpha::Kind::Scalar, 9, std::nullopt, alpha::ScalarKind::UInt32},
          {alpha::Kind::Scalar, 10, std::nullopt, alpha::ScalarKind::Int32},
      });

  verifyLabels(
      nodes, {"/", "/0", "/1", "/2", "/2", "/2", "/3", "/4", "/5", "/5", "/5"});

  {
    auto array = builder.createArrayTypeBuilder();
    auto inMap = builder.createScalarTypeBuilder(alpha::ScalarKind::Bool);
    auto elements = builder.createScalarTypeBuilder(alpha::ScalarKind::Double);
    array->setChildren(elements);
    flatMapCol2->addChild("f1", inMap, array);
  }

  {
    auto array = builder.createArrayTypeBuilder();
    auto inMap = builder.createScalarTypeBuilder(alpha::ScalarKind::Bool);
    auto elements = builder.createScalarTypeBuilder(alpha::ScalarKind::Double);
    array->setChildren(elements);
    flatMapCol2->addChild("f2", inMap, array);
  }

  {
    auto inMap = builder.createScalarTypeBuilder(alpha::ScalarKind::Bool);
    auto scalar = builder.createScalarTypeBuilder(alpha::ScalarKind::Int32);
    flatMapCol4->addChild("f1", inMap, scalar);
  }

  nodes = builder.getSchemaNodes();

  alpha::test::verifySchemaNodes(
      nodes,
      {
          {alpha::Kind::Row, 0, std::nullopt, alpha::ScalarKind::Undefined, 6},
          {alpha::Kind::Scalar, 1, "c1", alpha::ScalarKind::Int32, 0},
          {alpha::Kind::FlatMap, 2, "c2", alpha::ScalarKind::Int8, 2},
          {alpha::Kind::Scalar, 12, "f1", alpha::ScalarKind::Bool},
          {alpha::Kind::Array, 11, std::nullopt},
          {alpha::Kind::Scalar, 13, std::nullopt, alpha::ScalarKind::Double},
          {alpha::Kind::Scalar, 15, "f2", alpha::ScalarKind::Bool},
          {alpha::Kind::Array, 14, std::nullopt},
          {alpha::Kind::Scalar, 16, std::nullopt, alpha::ScalarKind::Double},
          {alpha::Kind::Map, 3, "c3"},
          {alpha::Kind::Scalar, 4, std::nullopt, alpha::ScalarKind::String},
          {alpha::Kind::Scalar, 5, std::nullopt, alpha::ScalarKind::Float},
          {alpha::Kind::FlatMap, 6, "c4", alpha::ScalarKind::Int64, 1},
          {alpha::Kind::Scalar, 17, "f1", alpha::ScalarKind::Bool},
          {alpha::Kind::Scalar, 18, std::nullopt, alpha::ScalarKind::Int32},
          {alpha::Kind::Scalar, 7, "c5", alpha::ScalarKind::Bool},
          {alpha::Kind::ArrayWithOffsets, 8, "c6"},
          {alpha::Kind::Scalar, 9, std::nullopt, alpha::ScalarKind::UInt32},
          {alpha::Kind::Scalar, 10, std::nullopt, alpha::ScalarKind::Int32},
      });

  verifyLabels(
      nodes,
      {"/",
       "/0",
       "/1",
       "/1/f1",
       "/1/f1",
       "/1/f1",
       "/1/f2",
       "/1/f2",
       "/1/f2",
       "/2",
       "/2",
       "/2",
       "/3",
       "/3/f1",
       "/3/f1",
       "/4",
       "/5",
       "/5",
       "/5"});

  {
    auto array = builder.createArrayTypeBuilder();
    auto inMap = builder.createScalarTypeBuilder(alpha::ScalarKind::Bool);
    auto elements = builder.createScalarTypeBuilder(alpha::ScalarKind::Double);
    array->setChildren(elements);
    flatMapCol2->addChild("f3", inMap, array);
  }

  {
    auto inMap = builder.createScalarTypeBuilder(alpha::ScalarKind::Bool);
    auto scalar = builder.createScalarTypeBuilder(alpha::ScalarKind::Int32);
    flatMapCol4->addChild("f2", inMap, scalar);
  }

  nodes = builder.getSchemaNodes();

  alpha::test::verifySchemaNodes(
      nodes,
      {
          {alpha::Kind::Row, 0, std::nullopt, alpha::ScalarKind::Undefined, 6},
          {alpha::Kind::Scalar, 1, "c1", alpha::ScalarKind::Int32, 0},
          {alpha::Kind::FlatMap, 2, "c2", alpha::ScalarKind::Int8, 3},
          {alpha::Kind::Scalar, 12, "f1", alpha::ScalarKind::Bool},
          {alpha::Kind::Array, 11, std::nullopt},
          {alpha::Kind::Scalar, 13, std::nullopt, alpha::ScalarKind::Double},
          {alpha::Kind::Scalar, 15, "f2", alpha::ScalarKind::Bool},
          {alpha::Kind::Array, 14, std::nullopt},
          {alpha::Kind::Scalar, 16, std::nullopt, alpha::ScalarKind::Double},
          {alpha::Kind::Scalar, 20, "f3", alpha::ScalarKind::Bool},
          {alpha::Kind::Array, 19, std::nullopt},
          {alpha::Kind::Scalar, 21, std::nullopt, alpha::ScalarKind::Double},
          {alpha::Kind::Map, 3, "c3"},
          {alpha::Kind::Scalar, 4, std::nullopt, alpha::ScalarKind::String},
          {alpha::Kind::Scalar, 5, std::nullopt, alpha::ScalarKind::Float},
          {alpha::Kind::FlatMap, 6, "c4", alpha::ScalarKind::Int64, 2},
          {alpha::Kind::Scalar, 17, "f1", alpha::ScalarKind::Bool},
          {alpha::Kind::Scalar, 18, std::nullopt, alpha::ScalarKind::Int32},
          {alpha::Kind::Scalar, 22, "f2", alpha::ScalarKind::Bool},
          {alpha::Kind::Scalar, 23, std::nullopt, alpha::ScalarKind::Int32},
          {alpha::Kind::Scalar, 7, "c5", alpha::ScalarKind::Bool},
          {alpha::Kind::ArrayWithOffsets, 8, "c6"},
          {alpha::Kind::Scalar, 9, std::nullopt, alpha::ScalarKind::UInt32},
          {alpha::Kind::Scalar, 10, std::nullopt, alpha::ScalarKind::Int32},
      });

  verifyLabels(nodes, {"/",     "/0",    "/1",    "/1/f1", "/1/f1", "/1/f1",
                       "/1/f2", "/1/f2", "/1/f2", "/1/f3", "/1/f3", "/1/f3",
                       "/2",    "/2",    "/2",    "/3",    "/3/f1", "/3/f1",
                       "/3/f2", "/3/f2", "/4",    "/5",    "/5",    "/5"});

  auto result = alpha::SchemaReader::getSchema(nodes);
  alpha::test::compareSchema(nodes, result);
}
