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

#include "dwio/nimble/common/tests/GTestUtils.h"
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

TEST(SchemaTests, schemaUtils) {
  nimble::SchemaBuilder builder;

  nimble::test::FlatMapChildAdder fm1;
  nimble::test::FlatMapChildAdder fm2;

  NIMBLE_SCHEMA(
      builder,
      NIMBLE_ROW({
          {"c1", NIMBLE_TINYINT()},
          {"c2", NIMBLE_ARRAY(NIMBLE_TINYINT())},
          {"c3", NIMBLE_FLATMAP(Int8, NIMBLE_TINYINT(), fm1)},
          {"c4", NIMBLE_MAP(NIMBLE_TINYINT(), NIMBLE_TINYINT())},
          {"c5", NIMBLE_FLATMAP(Float, NIMBLE_ARRAY(NIMBLE_BIGINT()), fm2)},
          {"c6", NIMBLE_SMALLINT()},
          {"c7", NIMBLE_INTEGER()},
          {"c8", NIMBLE_BIGINT()},
          {"c9", NIMBLE_REAL()},
          {"c10", NIMBLE_DOUBLE()},
          {"c11", NIMBLE_BOOLEAN()},
          {"c12", NIMBLE_STRING()},
          {"c13", NIMBLE_BINARY()},
          {"c14", NIMBLE_OFFSETARRAY(NIMBLE_INTEGER())},
          {"c15", NIMBLE_SLIDINGWINDOWMAP(NIMBLE_INTEGER(), NIMBLE_INTEGER())},
          {"c16",
           NIMBLE_ROW(
               {{"d1", NIMBLE_TINYINT()},
                {"d2", NIMBLE_ARRAY(NIMBLE_TINYINT())}})},
          {"c17", NIMBLE_TIMESTAMPMICRONANO()},
      }));

  auto nodes = builder.schemaNodes();
  nimble::test::verifySchemaNodes(
      nodes,
      {
          {nimble::Kind::Row, 29, nimble::ScalarKind::Bool, std::nullopt, 17},
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
          {nimble::Kind::TimestampMicroNano,
           27,
           nimble::ScalarKind::Int64,
           "c17"},
          {nimble::Kind::Scalar, 28, nimble::ScalarKind::UInt16},
      });

  verifyLabels(
      nodes, {"/",    "/0",    "/1",    "/1",    "/2",  "/3",  "/3",  "/3",
              "/4",   "/5",    "/6",    "/7",    "/8",  "/9",  "/10", "/11",
              "/12",  "/13",   "/13",   "/13",   "/14", "/14", "/14", "/14",
              "/15/", "/15/0", "/15/1", "/15/1", "/16", "/16"});

  fm2.addChild("f1");

  nodes = builder.schemaNodes();
  nimble::test::verifySchemaNodes(
      nodes,
      {
          {nimble::Kind::Row, 29, nimble::ScalarKind::Bool, std::nullopt, 17},
          {nimble::Kind::Scalar, 0, nimble::ScalarKind::Int8, "c1"},
          {nimble::Kind::Array, 2, nimble::ScalarKind::UInt32, "c2"},
          {nimble::Kind::Scalar, 1, nimble::ScalarKind::Int8},
          {nimble::Kind::FlatMap, 3, nimble::ScalarKind::Int8, "c3", 0},
          {nimble::Kind::Map, 6, nimble::ScalarKind::UInt32, "c4"},
          {nimble::Kind::Scalar, 4, nimble::ScalarKind::Int8},
          {nimble::Kind::Scalar, 5, nimble::ScalarKind::Int8},
          {nimble::Kind::FlatMap, 7, nimble::ScalarKind::Float, "c5", 1},
          {nimble::Kind::Scalar, 32, nimble::ScalarKind::Bool, "f1"},
          {nimble::Kind::Array, 31, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 30, nimble::ScalarKind::Int64},
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
          {nimble::Kind::TimestampMicroNano,
           27,
           nimble::ScalarKind::Int64,
           "c17"},
          {nimble::Kind::Scalar, 28, nimble::ScalarKind::UInt16},
      });

  verifyLabels(
      nodes,
      {"/",     "/0",    "/1",    "/1",    "/2",  "/3",  "/3",  "/3",  "/4",
       "/4/f1", "/4/f1", "/4/f1", "/5",    "/6",  "/7",  "/8",  "/9",  "/10",
       "/11",   "/12",   "/13",   "/13",   "/13", "/14", "/14", "/14", "/14",
       "/15/",  "/15/0", "/15/1", "/15/1", "/16", "/16"});

  fm1.addChild("f1");
  fm1.addChild("f2");
  fm2.addChild("f2");
  fm2.addChild("f3");

  nodes = builder.schemaNodes();
  nimble::test::verifySchemaNodes(
      nodes,
      {
          {nimble::Kind::Row, 29, nimble::ScalarKind::Bool, std::nullopt, 17},
          {nimble::Kind::Scalar, 0, nimble::ScalarKind::Int8, "c1"},
          {nimble::Kind::Array, 2, nimble::ScalarKind::UInt32, "c2"},
          {nimble::Kind::Scalar, 1, nimble::ScalarKind::Int8},
          {nimble::Kind::FlatMap, 3, nimble::ScalarKind::Int8, "c3", 2},
          {nimble::Kind::Scalar, 34, nimble::ScalarKind::Bool, "f1"},
          {nimble::Kind::Scalar, 33, nimble::ScalarKind::Int8},
          {nimble::Kind::Scalar, 36, nimble::ScalarKind::Bool, "f2"},
          {nimble::Kind::Scalar, 35, nimble::ScalarKind::Int8},
          {nimble::Kind::Map, 6, nimble::ScalarKind::UInt32, "c4"},
          {nimble::Kind::Scalar, 4, nimble::ScalarKind::Int8},
          {nimble::Kind::Scalar, 5, nimble::ScalarKind::Int8},
          {nimble::Kind::FlatMap, 7, nimble::ScalarKind::Float, "c5", 3},
          {nimble::Kind::Scalar, 32, nimble::ScalarKind::Bool, "f1"},
          {nimble::Kind::Array, 31, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 30, nimble::ScalarKind::Int64},
          {nimble::Kind::Scalar, 39, nimble::ScalarKind::Bool, "f2"},
          {nimble::Kind::Array, 38, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 37, nimble::ScalarKind::Int64},
          {nimble::Kind::Scalar, 42, nimble::ScalarKind::Bool, "f3"},
          {nimble::Kind::Array, 41, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 40, nimble::ScalarKind::Int64},
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
          {nimble::Kind::TimestampMicroNano,
           27,
           nimble::ScalarKind::Int64,
           "c17"},
          {nimble::Kind::Scalar, 28, nimble::ScalarKind::UInt16},
      });

  verifyLabels(
      nodes,
      {"/",     "/0",    "/1",    "/1",    "/2",    "/2/f1", "/2/f1", "/2/f2",
       "/2/f2", "/3",    "/3",    "/3",    "/4",    "/4/f1", "/4/f1", "/4/f1",
       "/4/f2", "/4/f2", "/4/f2", "/4/f3", "/4/f3", "/4/f3", "/5",    "/6",
       "/7",    "/8",    "/9",    "/10",   "/11",   "/12",   "/13",   "/13",
       "/13",   "/14",   "/14",   "/14",   "/14",   "/15/",  "/15/0", "/15/1",
       "/15/1", "/16",   "/16"});
}

TEST(SchemaTests, roundTrip) {
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
  //   c9:TIMESTAMPMICROS

  auto row = builder.createRowTypeBuilder(9);
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

  {
    auto timestampMicroNano = builder.createTimestampMicroNanoTypeBuilder();
    row->addChild("c9", timestampMicroNano);
  }

  auto nodes = builder.schemaNodes();
  nimble::test::verifySchemaNodes(
      nodes,
      {
          {nimble::Kind::Row, 0, nimble::ScalarKind::Bool, std::nullopt, 9},
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
          {nimble::Kind::TimestampMicroNano,
           19,
           nimble::ScalarKind::Int64,
           "c9"},
          {nimble::Kind::Scalar, 20, nimble::ScalarKind::UInt16, std::nullopt},
      });

  verifyLabels(nodes, {"/",  "/0",  "/1",   "/2",   "/2",   "/2", "/3",
                       "/4", "/5",  "/5",   "/5",   "/6",   "/6", "/6",
                       "/6", "/7/", "/7/0", "/7/1", "/7/1", "/8", "/8"});

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

  nodes = builder.schemaNodes();

  nimble::test::verifySchemaNodes(
      nodes,
      {
          {nimble::Kind::Row, 0, nimble::ScalarKind::Bool, std::nullopt, 9},
          {nimble::Kind::Scalar, 1, nimble::ScalarKind::Int32, "c1", 0},
          {nimble::Kind::FlatMap, 2, nimble::ScalarKind::Int8, "c2", 2},
          {nimble::Kind::Scalar, 23, nimble::ScalarKind::Bool, "f1"},
          {nimble::Kind::Array, 21, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 22, nimble::ScalarKind::Double},
          {nimble::Kind::Scalar, 26, nimble::ScalarKind::Bool, "f2"},
          {nimble::Kind::Array, 24, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 25, nimble::ScalarKind::Double},
          {nimble::Kind::Map, 3, nimble::ScalarKind::UInt32, "c3"},
          {nimble::Kind::Scalar, 4, nimble::ScalarKind::String},
          {nimble::Kind::Scalar, 5, nimble::ScalarKind::Float},
          {nimble::Kind::FlatMap, 6, nimble::ScalarKind::Int64, "c4", 1},
          {nimble::Kind::Scalar, 28, nimble::ScalarKind::Bool, "f1"},
          {nimble::Kind::Scalar, 27, nimble::ScalarKind::Int32},
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
          {nimble::Kind::TimestampMicroNano,
           19,
           nimble::ScalarKind::Int64,
           "c9"},
          {nimble::Kind::Scalar, 20, nimble::ScalarKind::UInt16, std::nullopt},
      });

  verifyLabels(nodes, {"/",     "/0",    "/1",    "/1/f1", "/1/f1", "/1/f1",
                       "/1/f2", "/1/f2", "/1/f2", "/2",    "/2",    "/2",
                       "/3",    "/3/f1", "/3/f1", "/4",    "/5",    "/5",
                       "/5",    "/6",    "/6",    "/6",    "/6",    "/7/",
                       "/7/0",  "/7/1",  "/7/1",  "/8",    "/8"});

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

  nodes = builder.schemaNodes();

  nimble::test::verifySchemaNodes(
      nodes,
      {
          {nimble::Kind::Row, 0, nimble::ScalarKind::Bool, std::nullopt, 9},
          {nimble::Kind::Scalar, 1, nimble::ScalarKind::Int32, "c1", 0},
          {nimble::Kind::FlatMap, 2, nimble::ScalarKind::Int8, "c2", 3},
          {nimble::Kind::Scalar, 23, nimble::ScalarKind::Bool, "f1"},
          {nimble::Kind::Array, 21, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 22, nimble::ScalarKind::Double},
          {nimble::Kind::Scalar, 26, nimble::ScalarKind::Bool, "f2"},
          {nimble::Kind::Array, 24, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 25, nimble::ScalarKind::Double},
          {nimble::Kind::Scalar, 31, nimble::ScalarKind::Bool, "f3"},
          {nimble::Kind::Array, 29, nimble::ScalarKind::UInt32},
          {nimble::Kind::Scalar, 30, nimble::ScalarKind::Double},
          {nimble::Kind::Map, 3, nimble::ScalarKind::UInt32, "c3"},
          {nimble::Kind::Scalar, 4, nimble::ScalarKind::String},
          {nimble::Kind::Scalar, 5, nimble::ScalarKind::Float},
          {nimble::Kind::FlatMap, 6, nimble::ScalarKind::Int64, "c4", 2},
          {nimble::Kind::Scalar, 28, nimble::ScalarKind::Bool, "f1"},
          {nimble::Kind::Scalar, 27, nimble::ScalarKind::Int32},
          {nimble::Kind::Scalar, 33, nimble::ScalarKind::Bool, "f2"},
          {nimble::Kind::Scalar, 32, nimble::ScalarKind::Int32},
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
          {nimble::Kind::TimestampMicroNano,
           19,
           nimble::ScalarKind::Int64,
           "c9"},
          {nimble::Kind::Scalar, 20, nimble::ScalarKind::UInt16, std::nullopt},
      });

  verifyLabels(
      nodes, {"/",     "/0",    "/1",    "/1/f1", "/1/f1", "/1/f1", "/1/f2",
              "/1/f2", "/1/f2", "/1/f3", "/1/f3", "/1/f3", "/2",    "/2",
              "/2",    "/3",    "/3/f1", "/3/f1", "/3/f2", "/3/f2", "/4",
              "/5",    "/5",    "/5",    "/6",    "/6",    "/6",    "/6",
              "/7/",   "/7/0",  "/7/1",  "/7/1",  "/8",    "/8"});

  auto result = nimble::SchemaReader::getSchema(nodes);
  nimble::test::compareSchema(nodes, result);
}

// Custom context classes for testing checkedContext
class TestStreamContext : public nimble::StreamContext {
 public:
  explicit TestStreamContext(int value) : value_(value) {}

  int value() const {
    return value_;
  }

 private:
  int value_;
};

class OtherStreamContext : public nimble::StreamContext {
 public:
  explicit OtherStreamContext(std::string name) : name_(std::move(name)) {}
  const std::string& name() const {
    return name_;
  }

 private:
  std::string name_;
};

class TestTypeBuilderContext : public nimble::TypeBuilderContext {
 public:
  explicit TestTypeBuilderContext(int value) : value_(value) {}

  int value() const {
    return value_;
  }

 private:
  int value_;
};

class OtherTypeBuilderContext : public nimble::TypeBuilderContext {
 public:
  explicit OtherTypeBuilderContext(std::string name) : name_(std::move(name)) {}
  const std::string& name() const {
    return name_;
  }

 private:
  std::string name_;
};

TEST(SchemaTests, streamDescriptorBuilderCheckedContext) {
  nimble::SchemaBuilder builder;
  auto scalar = builder.createScalarTypeBuilder(nimble::ScalarKind::Int32);
  const auto& descriptor = scalar->scalarDescriptor();

  // Before setting context, checkedContext should throw
  NIMBLE_ASSERT_THROW(
      descriptor.checkedContext<TestStreamContext>(),
      "context is not set or not of expected type");

  // Set context and verify checkedContext returns it
  descriptor.setContext(std::make_unique<TestStreamContext>(42));
  auto* ctx = descriptor.checkedContext<TestStreamContext>();
  EXPECT_NE(ctx, nullptr);
  EXPECT_EQ(ctx->value(), 42);

  // context() should also work
  auto* ctx2 = descriptor.context<TestStreamContext>();
  EXPECT_NE(ctx2, nullptr);
  EXPECT_EQ(ctx2->value(), 42);

  // checkedContext with wrong type should throw
  NIMBLE_ASSERT_THROW(
      descriptor.checkedContext<OtherStreamContext>(),
      "context is not set or not of expected type");

  // context() with wrong type should return nullptr
  auto* wrongCtx = descriptor.context<OtherStreamContext>();
  EXPECT_EQ(wrongCtx, nullptr);
}

TEST(SchemaTests, typeBuilderCheckedContext) {
  nimble::SchemaBuilder builder;
  auto scalar = builder.createScalarTypeBuilder(nimble::ScalarKind::Int32);

  // Before setting context, checkedContext should throw
  NIMBLE_ASSERT_THROW(
      scalar->checkedContext<TestTypeBuilderContext>(),
      "context is not set or not of expected type");

  // Set context and verify checkedContext returns it
  scalar->setContext(std::make_unique<TestTypeBuilderContext>(100));
  const auto* ctx = scalar->checkedContext<TestTypeBuilderContext>();
  EXPECT_NE(ctx, nullptr);
  EXPECT_EQ(ctx->value(), 100);

  // context() should also work
  const auto* ctx2 = scalar->context<TestTypeBuilderContext>();
  EXPECT_NE(ctx2, nullptr);
  EXPECT_EQ(ctx2->value(), 100);

  // checkedContext with wrong type should throw
  NIMBLE_ASSERT_THROW(
      scalar->checkedContext<OtherTypeBuilderContext>(),
      "context is not set or not of expected type");

  // context() with wrong type should return nullptr
  const auto* wrongCtx = scalar->context<OtherTypeBuilderContext>();
  EXPECT_EQ(wrongCtx, nullptr);
}

TEST(SchemaTests, rowTypeFindChild) {
  nimble::SchemaBuilder builder;

  NIMBLE_SCHEMA(
      builder,
      NIMBLE_ROW({
          {"alpha", NIMBLE_INTEGER()},
          {"beta", NIMBLE_STRING()},
          {"gamma", NIMBLE_DOUBLE()},
      }));

  auto schema = nimble::SchemaReader::getSchema(builder.schemaNodes());
  ASSERT_TRUE(schema->isRow());
  const auto& row = schema->asRow();

  // Find existing children.
  auto alphaIdx = row.findChild("alpha");
  ASSERT_TRUE(alphaIdx.has_value());
  EXPECT_EQ(*alphaIdx, 0);
  EXPECT_EQ(row.nameAt(*alphaIdx), "alpha");

  auto betaIdx = row.findChild("beta");
  ASSERT_TRUE(betaIdx.has_value());
  EXPECT_EQ(*betaIdx, 1);
  EXPECT_EQ(row.nameAt(*betaIdx), "beta");

  auto gammaIdx = row.findChild("gamma");
  ASSERT_TRUE(gammaIdx.has_value());
  EXPECT_EQ(*gammaIdx, 2);
  EXPECT_EQ(row.nameAt(*gammaIdx), "gamma");

  // Non-existent child returns nullopt.
  auto notFound = row.findChild("nonexistent");
  EXPECT_FALSE(notFound.has_value());

  auto empty = row.findChild("");
  EXPECT_FALSE(empty.has_value());
}

TEST(SchemaTests, flatMapTypeFindChild) {
  nimble::SchemaBuilder builder;
  nimble::test::FlatMapChildAdder fm;

  NIMBLE_SCHEMA(
      builder,
      NIMBLE_ROW({
          {"flatmap", NIMBLE_FLATMAP(String, NIMBLE_INTEGER(), fm)},
      }));

  // Add keys to the flat map.
  fm.addChild("key_one");
  fm.addChild("key_two");
  fm.addChild("key_three");

  auto schema = nimble::SchemaReader::getSchema(builder.schemaNodes());
  ASSERT_TRUE(schema->isRow());
  const auto& row = schema->asRow();
  ASSERT_TRUE(row.childAt(0)->isFlatMap());
  const auto& flatMap = row.childAt(0)->asFlatMap();

  // Find existing keys.
  auto keyOneIdx = flatMap.findChild("key_one");
  ASSERT_TRUE(keyOneIdx.has_value());
  EXPECT_EQ(*keyOneIdx, 0);
  EXPECT_EQ(flatMap.nameAt(*keyOneIdx), "key_one");

  auto keyTwoIdx = flatMap.findChild("key_two");
  ASSERT_TRUE(keyTwoIdx.has_value());
  EXPECT_EQ(*keyTwoIdx, 1);
  EXPECT_EQ(flatMap.nameAt(*keyTwoIdx), "key_two");

  auto keyThreeIdx = flatMap.findChild("key_three");
  ASSERT_TRUE(keyThreeIdx.has_value());
  EXPECT_EQ(*keyThreeIdx, 2);
  EXPECT_EQ(flatMap.nameAt(*keyThreeIdx), "key_three");

  // Non-existent key returns nullopt.
  auto notFound = flatMap.findChild("nonexistent_key");
  EXPECT_FALSE(notFound.has_value());

  auto empty = flatMap.findChild("");
  EXPECT_FALSE(empty.has_value());
}
