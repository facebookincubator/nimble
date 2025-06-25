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
#include <gtest/gtest.h>
#include <optional>

#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/SchemaReader.h"
#include "dwio/nimble/velox/SchemaTypes.h"
#include "dwio/nimble/velox/StreamLabels.h"
#include "dwio/nimble/velox/tests/SchemaUtils.h"

using namespace facebook;

namespace {
void verifyLabels(
    const std::vector<nimble::SchemaNode>& schemaNodes,
    std::vector<std::string_view> expected) {
  nimble::StreamLabels streamLabels{
      nimble::SchemaReader::getSchema(schemaNodes)};
  std::vector<std::string_view> actual;
  actual.reserve(schemaNodes.size());
  for (size_t i = 0, end = schemaNodes.size(); i < end; ++i) {
    actual.push_back(streamLabels.streamLabel(schemaNodes[i].offset()));
  }

  EXPECT_EQ(actual, expected);
}

} // namespace

TEST(SchemaTests, SchemaUtils) {
  nimble::SchemaBuilder builder;

  nimble::test::FlatMapChildAdder fm1;
  nimble::test::FlatMapChildAdder fm2;

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
          {"c14", OFFSETARRAY(INTEGER())},
          {"c15", SLIDINGWINDOWMAP(INTEGER(), INTEGER())},
          {"c16", ROW({{"d1", TINYINT()}, {"d2", ARRAY(TINYINT())}})},
      }));

  auto nodes = builder.getSchemaNodes();
  nimble::test::verifySchemaNodes(
      nodes,
      {
          {nimble::Kind::Row, 27, nimble::ScalarKind::Bool, std::nullopt, 16},
          {nimble::Kind::Scalar, 0, nimble::ScalarKind::Int8, "c1"},
          {nimble::Kind::Array, 2, nimble::ScalarKind::UInt32, "c2"},
          {nimble::Kind::Scalar, 1, nimble::ScalarKind::Int8},
          {nimble::Kind::FlatMap, 3, nimble::ScalarKind::Int8, "c3", 0},
          {nimble::Kind::Map, 6, nimble::ScalarKind::UInt32, "c4"},
          {nimble::Kind::Scalar, 4, nimble::ScalarKind::Int8},
          {nimble::Kind::Scalar, 5, nimble::ScalarKind::Int8},
          {nimble::Kind::FlatMap, 7, nimble::ScalarKind::Float, "c5", 0},
          {nimble::Kind::Scalar, 8, nimble::ScalarKind::Int16, "c6"},
          {nimble::Kind::Scalar, 9, nimble::ScalarKind::Int32, "c7"},
          {nimble::Kind::Scalar, 10, nimble::ScalarKind::Int64, "c8"},
          {nimble::Kind::Scalar, 11, nimble::ScalarKind::Float, "c9"},
          {nimble::Kind::Scalar, 12, nimble::ScalarKind::Double, "c10"},
          {nimble::Kind::Scalar, 13, nimble::ScalarKind::Bool, "c11"},
          {nimble::Kind::Scalar, 14, nimble::ScalarKind::String, "c12"},
          {nimble::Kind::Scalar, 15, nimble::ScalarKind::Binary, "c13"},
          {nimble::Kind::ArrayWithOffsets,
           18,
           nimble::ScalarKind::UInt32,
           "c14"},
          {nimble::Kind::Scalar, 17, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 16, nimble::ScalarKind::Int32},
          {nimble::Kind::SlidingWindowMap,
           21,
           nimble::ScalarKind::UInt32,
           "c15"},
          {nimble::Kind::Scalar, 22, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 19, nimble::ScalarKind::Int32},
          {nimble::Kind::Scalar, 20, nimble::ScalarKind::Int32},
          {nimble::Kind::Row, 26, nimble::ScalarKind::Bool, "c16", 2},
          {nimble::Kind::Scalar, 23, nimble::ScalarKind::Int8, "d1"},
          {nimble::Kind::Array, 25, nimble::ScalarKind::UInt32, "d2"},
          {nimble::Kind::Scalar, 24, nimble::ScalarKind::Int8},
      });

  verifyLabels(nodes, {"/",   "/0",  "/1",  "/1",   "/2",    "/3",    "/3",
                       "/3",  "/4",  "/5",  "/6",   "/7",    "/8",    "/9",
                       "/10", "/11", "/12", "/13",  "/13",   "/13",   "/14",
                       "/14", "/14", "/14", "/15/", "/15/0", "/15/1", "/15/1"});

  fm2.addChild("f1");

  nodes = builder.getSchemaNodes();
  nimble::test::verifySchemaNodes(
      nodes,
      {
          {nimble::Kind::Row, 27, nimble::ScalarKind::Bool, std::nullopt, 16},
          {nimble::Kind::Scalar, 0, nimble::ScalarKind::Int8, "c1"},
          {nimble::Kind::Array, 2, nimble::ScalarKind::UInt32, "c2"},
          {nimble::Kind::Scalar, 1, nimble::ScalarKind::Int8},
          {nimble::Kind::FlatMap, 3, nimble::ScalarKind::Int8, "c3", 0},
          {nimble::Kind::Map, 6, nimble::ScalarKind::UInt32, "c4"},
          {nimble::Kind::Scalar, 4, nimble::ScalarKind::Int8},
          {nimble::Kind::Scalar, 5, nimble::ScalarKind::Int8},
          {nimble::Kind::FlatMap, 7, nimble::ScalarKind::Float, "c5", 1},
          {nimble::Kind::Scalar, 30, nimble::ScalarKind::Bool, "f1"},
          {nimble::Kind::Array, 29, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 28, nimble::ScalarKind::Int64},
          {nimble::Kind::Scalar, 8, nimble::ScalarKind::Int16, "c6"},
          {nimble::Kind::Scalar, 9, nimble::ScalarKind::Int32, "c7"},
          {nimble::Kind::Scalar, 10, nimble::ScalarKind::Int64, "c8"},
          {nimble::Kind::Scalar, 11, nimble::ScalarKind::Float, "c9"},
          {nimble::Kind::Scalar, 12, nimble::ScalarKind::Double, "c10"},
          {nimble::Kind::Scalar, 13, nimble::ScalarKind::Bool, "c11"},
          {nimble::Kind::Scalar, 14, nimble::ScalarKind::String, "c12"},
          {nimble::Kind::Scalar, 15, nimble::ScalarKind::Binary, "c13"},
          {nimble::Kind::ArrayWithOffsets,
           18,
           nimble::ScalarKind::UInt32,
           "c14"},
          {nimble::Kind::Scalar, 17, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 16, nimble::ScalarKind::Int32},
          {nimble::Kind::SlidingWindowMap,
           21,
           nimble::ScalarKind::UInt32,
           "c15"},
          {nimble::Kind::Scalar, 22, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 19, nimble::ScalarKind::Int32},
          {nimble::Kind::Scalar, 20, nimble::ScalarKind::Int32},
          {nimble::Kind::Row, 26, nimble::ScalarKind::Bool, "c16", 2},
          {nimble::Kind::Scalar, 23, nimble::ScalarKind::Int8, "d1"},
          {nimble::Kind::Array, 25, nimble::ScalarKind::UInt32, "d2"},
          {nimble::Kind::Scalar, 24, nimble::ScalarKind::Int8},
      });

  verifyLabels(
      nodes, {"/",   "/0",    "/1",    "/1",    "/2",    "/3",    "/3",   "/3",
              "/4",  "/4/f1", "/4/f1", "/4/f1", "/5",    "/6",    "/7",   "/8",
              "/9",  "/10",   "/11",   "/12",   "/13",   "/13",   "/13",  "/14",
              "/14", "/14",   "/14",   "/15/",  "/15/0", "/15/1", "/15/1"});

  fm1.addChild("f1");
  fm1.addChild("f2");
  fm2.addChild("f2");
  fm2.addChild("f3");

  nodes = builder.getSchemaNodes();
  nimble::test::verifySchemaNodes(
      nodes,
      {
          {nimble::Kind::Row, 27, nimble::ScalarKind::Bool, std::nullopt, 16},
          {nimble::Kind::Scalar, 0, nimble::ScalarKind::Int8, "c1"},
          {nimble::Kind::Array, 2, nimble::ScalarKind::UInt32, "c2"},
          {nimble::Kind::Scalar, 1, nimble::ScalarKind::Int8},
          {nimble::Kind::FlatMap, 3, nimble::ScalarKind::Int8, "c3", 2},
          {nimble::Kind::Scalar, 32, nimble::ScalarKind::Bool, "f1"},
          {nimble::Kind::Scalar, 31, nimble::ScalarKind::Int8},
          {nimble::Kind::Scalar, 34, nimble::ScalarKind::Bool, "f2"},
          {nimble::Kind::Scalar, 33, nimble::ScalarKind::Int8},
          {nimble::Kind::Map, 6, nimble::ScalarKind::UInt32, "c4"},
          {nimble::Kind::Scalar, 4, nimble::ScalarKind::Int8},
          {nimble::Kind::Scalar, 5, nimble::ScalarKind::Int8},
          {nimble::Kind::FlatMap, 7, nimble::ScalarKind::Float, "c5", 3},
          {nimble::Kind::Scalar, 30, nimble::ScalarKind::Bool, "f1"},
          {nimble::Kind::Array, 29, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 28, nimble::ScalarKind::Int64},
          {nimble::Kind::Scalar, 37, nimble::ScalarKind::Bool, "f2"},
          {nimble::Kind::Array, 36, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 35, nimble::ScalarKind::Int64},
          {nimble::Kind::Scalar, 40, nimble::ScalarKind::Bool, "f3"},
          {nimble::Kind::Array, 39, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 38, nimble::ScalarKind::Int64},
          {nimble::Kind::Scalar, 8, nimble::ScalarKind::Int16, "c6"},
          {nimble::Kind::Scalar, 9, nimble::ScalarKind::Int32, "c7"},
          {nimble::Kind::Scalar, 10, nimble::ScalarKind::Int64, "c8"},
          {nimble::Kind::Scalar, 11, nimble::ScalarKind::Float, "c9"},
          {nimble::Kind::Scalar, 12, nimble::ScalarKind::Double, "c10"},
          {nimble::Kind::Scalar, 13, nimble::ScalarKind::Bool, "c11"},
          {nimble::Kind::Scalar, 14, nimble::ScalarKind::String, "c12"},
          {nimble::Kind::Scalar, 15, nimble::ScalarKind::Binary, "c13"},
          {nimble::Kind::ArrayWithOffsets,
           18,
           nimble::ScalarKind::UInt32,
           "c14"},
          {nimble::Kind::Scalar, 17, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 16, nimble::ScalarKind::Int32},
          {nimble::Kind::SlidingWindowMap,
           21,
           nimble::ScalarKind::UInt32,
           "c15"},
          {nimble::Kind::Scalar, 22, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 19, nimble::ScalarKind::Int32},
          {nimble::Kind::Scalar, 20, nimble::ScalarKind::Int32},
          {nimble::Kind::Row, 26, nimble::ScalarKind::Bool, "c16", 2},
          {nimble::Kind::Scalar, 23, nimble::ScalarKind::Int8, "d1"},
          {nimble::Kind::Array, 25, nimble::ScalarKind::UInt32, "d2"},
          {nimble::Kind::Scalar, 24, nimble::ScalarKind::Int8},
      });

  verifyLabels(
      nodes, {"/",     "/0",    "/1",    "/1",    "/2",    "/2/f1", "/2/f1",
              "/2/f2", "/2/f2", "/3",    "/3",    "/3",    "/4",    "/4/f1",
              "/4/f1", "/4/f1", "/4/f2", "/4/f2", "/4/f2", "/4/f3", "/4/f3",
              "/4/f3", "/5",    "/6",    "/7",    "/8",    "/9",    "/10",
              "/11",   "/12",   "/13",   "/13",   "/13",   "/14",   "/14",
              "/14",   "/14",   "/15/",  "/15/0", "/15/1", "/15/1"});
}

TEST(SchemaTests, RoundTrip) {
  nimble::SchemaBuilder builder;
  // ROW(
  //   c1:INT,
  //   c2:FLATMAP<TINYINT, ARRAY<DOUBLE>>,
  //   c3:MAP<VARCHAR, REAL>,
  //   c4:FLATMAP<BIGINT, INT>,
  //   c5:BOOL,
  //   c6:OFFSETARRAY<FLOAT>,
  //   c7:SLIDINGWINDOWMAP<INT, INT>)
  //   c8:ROW(
  //     d1:TINYINT,
  //     d2:ARRAY<UINT>)
  //   )

  auto row = builder.createRowTypeBuilder(8);
  {
    auto scalar = builder.createScalarTypeBuilder(nimble::ScalarKind::Int32);
    row->addChild("c1", scalar);
  }

  auto flatMapCol2 = builder.createFlatMapTypeBuilder(nimble::ScalarKind::Int8);
  row->addChild("c2", flatMapCol2);

  {
    auto map = builder.createMapTypeBuilder();
    auto keys = builder.createScalarTypeBuilder(nimble::ScalarKind::String);
    auto values = builder.createScalarTypeBuilder(nimble::ScalarKind::Float);
    map->setChildren(std::move(keys), std::move(values));
    row->addChild("c3", map);
  }

  auto flatMapCol4 =
      builder.createFlatMapTypeBuilder(nimble::ScalarKind::Int64);
  row->addChild("c4", flatMapCol4);

  {
    auto scalar = builder.createScalarTypeBuilder(nimble::ScalarKind::Bool);
    row->addChild("c5", scalar);
  }

  {
    auto arrayWithOffsets = builder.createArrayWithOffsetsTypeBuilder();
    auto elements = builder.createScalarTypeBuilder(nimble::ScalarKind::Float);
    arrayWithOffsets->setChildren(std::move(elements));
    row->addChild("c6", arrayWithOffsets);
  }

  {
    auto slidingWindowMap = builder.createSlidingWindowMapTypeBuilder();
    auto keys = builder.createScalarTypeBuilder(nimble::ScalarKind::Int32);
    auto values = builder.createScalarTypeBuilder(nimble::ScalarKind::Int32);
    slidingWindowMap->setChildren(std::move(keys), std::move(values));
    row->addChild("c7", slidingWindowMap);
  }

  {
    auto row2 = builder.createRowTypeBuilder(2);
    auto scalar1 = builder.createScalarTypeBuilder(nimble::ScalarKind::Int8);
    row2->addChild("d1", scalar1);

    auto array = builder.createArrayTypeBuilder();
    auto elements = builder.createScalarTypeBuilder(nimble::ScalarKind::UInt32);
    array->setChildren(elements);
    row2->addChild("d2", array);

    row->addChild("c8", row2);
  }

  auto nodes = builder.getSchemaNodes();
  nimble::test::verifySchemaNodes(
      nodes,
      {
          {nimble::Kind::Row, 0, nimble::ScalarKind::Bool, std::nullopt, 8},
          {nimble::Kind::Scalar, 1, nimble::ScalarKind::Int32, "c1", 0},
          {nimble::Kind::FlatMap, 2, nimble::ScalarKind::Int8, "c2", 0},
          {nimble::Kind::Map, 3, nimble::ScalarKind::UInt32, "c3"},
          {nimble::Kind::Scalar, 4, nimble::ScalarKind::String, std::nullopt},
          {nimble::Kind::Scalar, 5, nimble::ScalarKind::Float, std::nullopt},
          {nimble::Kind::FlatMap, 6, nimble::ScalarKind::Int64, "c4", 0},
          {nimble::Kind::Scalar, 7, nimble::ScalarKind::Bool, "c5"},
          {nimble::Kind::ArrayWithOffsets, 9, nimble::ScalarKind::UInt32, "c6"},
          {
              nimble::Kind::Scalar,
              8,
              nimble::ScalarKind::UInt32,
              std::nullopt,
          },
          {nimble::Kind::Scalar, 10, nimble::ScalarKind::Float, std::nullopt},
          {nimble::Kind::SlidingWindowMap,
           11,
           nimble::ScalarKind::UInt32,
           "c7"},
          {nimble::Kind::Scalar, 12, nimble::ScalarKind::UInt32, std::nullopt},
          {nimble::Kind::Scalar, 13, nimble::ScalarKind::Int32, std::nullopt},
          {nimble::Kind::Scalar, 14, nimble::ScalarKind::Int32, std::nullopt},
          {nimble::Kind::Row, 15, nimble::ScalarKind::Bool, "c8", 2},
          {nimble::Kind::Scalar, 16, nimble::ScalarKind::Int8, "d1"},
          {nimble::Kind::Array, 17, nimble::ScalarKind::UInt32, "d2"},
          {nimble::Kind::Scalar, 18, nimble::ScalarKind::UInt32},
      });

  verifyLabels(
      nodes,
      {"/",
       "/0",
       "/1",
       "/2",
       "/2",
       "/2",
       "/3",
       "/4",
       "/5",
       "/5",
       "/5",
       "/6",
       "/6",
       "/6",
       "/6",
       "/7/",
       "/7/0",
       "/7/1",
       "/7/1"});

  {
    auto array = builder.createArrayTypeBuilder();
    auto elements = builder.createScalarTypeBuilder(nimble::ScalarKind::Double);
    array->setChildren(elements);
    flatMapCol2->addChild("f1", array);
  }

  {
    auto array = builder.createArrayTypeBuilder();
    auto elements = builder.createScalarTypeBuilder(nimble::ScalarKind::Double);
    array->setChildren(elements);
    flatMapCol2->addChild("f2", array);
  }

  {
    auto scalar = builder.createScalarTypeBuilder(nimble::ScalarKind::Int32);
    flatMapCol4->addChild("f1", scalar);
  }

  nodes = builder.getSchemaNodes();

  nimble::test::verifySchemaNodes(
      nodes,
      {
          {nimble::Kind::Row, 0, nimble::ScalarKind::Bool, std::nullopt, 8},
          {nimble::Kind::Scalar, 1, nimble::ScalarKind::Int32, "c1", 0},
          {nimble::Kind::FlatMap, 2, nimble::ScalarKind::Int8, "c2", 2},
          {nimble::Kind::Scalar, 21, nimble::ScalarKind::Bool, "f1"},
          {nimble::Kind::Array, 19, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 20, nimble::ScalarKind::Double},
          {nimble::Kind::Scalar, 24, nimble::ScalarKind::Bool, "f2"},
          {nimble::Kind::Array, 22, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 23, nimble::ScalarKind::Double},
          {nimble::Kind::Map, 3, nimble::ScalarKind::UInt32, "c3"},
          {nimble::Kind::Scalar, 4, nimble::ScalarKind::String},
          {nimble::Kind::Scalar, 5, nimble::ScalarKind::Float},
          {nimble::Kind::FlatMap, 6, nimble::ScalarKind::Int64, "c4", 1},
          {nimble::Kind::Scalar, 26, nimble::ScalarKind::Bool, "f1"},
          {nimble::Kind::Scalar, 25, nimble::ScalarKind::Int32},
          {nimble::Kind::Scalar, 7, nimble::ScalarKind::Bool, "c5"},
          {nimble::Kind::ArrayWithOffsets, 9, nimble::ScalarKind::UInt32, "c6"},
          {nimble::Kind::Scalar, 8, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 10, nimble::ScalarKind::Float},
          {nimble::Kind::SlidingWindowMap,
           11,
           nimble::ScalarKind::UInt32,
           "c7"},
          {nimble::Kind::Scalar, 12, nimble::ScalarKind::UInt32, std::nullopt},
          {nimble::Kind::Scalar, 13, nimble::ScalarKind::Int32, std::nullopt},
          {nimble::Kind::Scalar, 14, nimble::ScalarKind::Int32, std::nullopt},
          {nimble::Kind::Row, 15, nimble::ScalarKind::Bool, "c8", 2},
          {nimble::Kind::Scalar, 16, nimble::ScalarKind::Int8, "d1"},
          {nimble::Kind::Array, 17, nimble::ScalarKind::UInt32, "d2"},
          {nimble::Kind::Scalar, 18, nimble::ScalarKind::UInt32},
      });

  verifyLabels(
      nodes,
      {"/",  "/0", "/1", "/1/f1", "/1/f1", "/1/f1", "/1/f2", "/1/f2", "/1/f2",
       "/2", "/2", "/2", "/3",    "/3/f1", "/3/f1", "/4",    "/5",    "/5",
       "/5", "/6", "/6", "/6",    "/6",    "/7/",   "/7/0",  "/7/1",  "/7/1"});

  {
    auto array = builder.createArrayTypeBuilder();
    auto elements = builder.createScalarTypeBuilder(nimble::ScalarKind::Double);
    array->setChildren(elements);
    flatMapCol2->addChild("f3", array);
  }

  {
    auto scalar = builder.createScalarTypeBuilder(nimble::ScalarKind::Int32);
    flatMapCol4->addChild("f2", scalar);
  }

  nodes = builder.getSchemaNodes();

  nimble::test::verifySchemaNodes(
      nodes,
      {
          {nimble::Kind::Row, 0, nimble::ScalarKind::Bool, std::nullopt, 8},
          {nimble::Kind::Scalar, 1, nimble::ScalarKind::Int32, "c1", 0},
          {nimble::Kind::FlatMap, 2, nimble::ScalarKind::Int8, "c2", 3},
          {nimble::Kind::Scalar, 21, nimble::ScalarKind::Bool, "f1"},
          {nimble::Kind::Array, 19, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 20, nimble::ScalarKind::Double},
          {nimble::Kind::Scalar, 24, nimble::ScalarKind::Bool, "f2"},
          {nimble::Kind::Array, 22, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 23, nimble::ScalarKind::Double},
          {nimble::Kind::Scalar, 29, nimble::ScalarKind::Bool, "f3"},
          {nimble::Kind::Array, 27, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 28, nimble::ScalarKind::Double},
          {nimble::Kind::Map, 3, nimble::ScalarKind::UInt32, "c3"},
          {nimble::Kind::Scalar, 4, nimble::ScalarKind::String},
          {nimble::Kind::Scalar, 5, nimble::ScalarKind::Float},
          {nimble::Kind::FlatMap, 6, nimble::ScalarKind::Int64, "c4", 2},
          {nimble::Kind::Scalar, 26, nimble::ScalarKind::Bool, "f1"},
          {nimble::Kind::Scalar, 25, nimble::ScalarKind::Int32},
          {nimble::Kind::Scalar, 31, nimble::ScalarKind::Bool, "f2"},
          {nimble::Kind::Scalar, 30, nimble::ScalarKind::Int32},
          {nimble::Kind::Scalar, 7, nimble::ScalarKind::Bool, "c5"},
          {nimble::Kind::ArrayWithOffsets, 9, nimble::ScalarKind::UInt32, "c6"},
          {nimble::Kind::Scalar, 8, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 10, nimble::ScalarKind::Float},
          {nimble::Kind::SlidingWindowMap,
           11,
           nimble::ScalarKind::UInt32,
           "c7"},
          {nimble::Kind::Scalar, 12, nimble::ScalarKind::UInt32, std::nullopt},
          {nimble::Kind::Scalar, 13, nimble::ScalarKind::Int32, std::nullopt},
          {nimble::Kind::Scalar, 14, nimble::ScalarKind::Int32, std::nullopt},
          {nimble::Kind::Row, 15, nimble::ScalarKind::Bool, "c8", 2},
          {nimble::Kind::Scalar, 16, nimble::ScalarKind::Int8, "d1"},
          {nimble::Kind::Array, 17, nimble::ScalarKind::UInt32, "d2"},
          {nimble::Kind::Scalar, 18, nimble::ScalarKind::UInt32},
      });

  verifyLabels(
      nodes,
      {"/",     "/0",    "/1",    "/1/f1", "/1/f1", "/1/f1", "/1/f2", "/1/f2",
       "/1/f2", "/1/f3", "/1/f3", "/1/f3", "/2",    "/2",    "/2",    "/3",
       "/3/f1", "/3/f1", "/3/f2", "/3/f2", "/4",    "/5",    "/5",    "/5",
       "/6",    "/6",    "/6",    "/6",    "/7/",   "/7/0",  "/7/1",  "/7/1"});

  auto result = nimble::SchemaReader::getSchema(nodes);
  nimble::test::compareSchema(nodes, result);
}
