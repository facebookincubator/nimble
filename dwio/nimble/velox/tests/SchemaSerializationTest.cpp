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
#include <gtest/gtest.h>
#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/SchemaReader.h"
#include "dwio/nimble/velox/tests/SchemaUtils.h"

using namespace facebook;
using namespace facebook::nimble;

namespace {

std::shared_ptr<const Type> roundTrip(SchemaBuilder& schemaBuilder) {
  SchemaSerializer serializer;
  auto serialized = serializer.serialize(schemaBuilder);
  return SchemaDeserializer::deserialize(serialized);
}

} // namespace

TEST(SchemaSerializationTest, ScalarInt8) {
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(schemaBuilder, NIMBLE_ROW({{"field", NIMBLE_TINYINT()}}));
  auto root = roundTrip(schemaBuilder);
  ASSERT_EQ(Kind::Row, root->kind());
  auto& row = root->asRow();
  ASSERT_EQ(1, row.childrenCount());
  EXPECT_EQ("field", row.nameAt(0));
  EXPECT_EQ(Kind::Scalar, row.childAt(0)->kind());
  EXPECT_EQ(
      ScalarKind::Int8,
      row.childAt(0)->asScalar().scalarDescriptor().scalarKind());
}

TEST(SchemaSerializationTest, ScalarTypes) {
  struct TestCase {
    std::string name;
    ScalarKind expectedKind;
  };

  std::vector<TestCase> cases = {
      {"int8", ScalarKind::Int8},
      {"uint8", ScalarKind::UInt8},
      {"int16", ScalarKind::Int16},
      {"uint16", ScalarKind::UInt16},
      {"int32", ScalarKind::Int32},
      {"uint32", ScalarKind::UInt32},
      {"int64", ScalarKind::Int64},
      {"uint64", ScalarKind::UInt64},
      {"float", ScalarKind::Float},
      {"double", ScalarKind::Double},
      {"bool", ScalarKind::Bool},
      {"string", ScalarKind::String},
      {"binary", ScalarKind::Binary},
  };

  for (const auto& tc : cases) {
    SchemaBuilder schemaBuilder;
    NIMBLE_SCHEMA(
        schemaBuilder,
        NIMBLE_ROW({{"f", builder.createScalarTypeBuilder(tc.expectedKind)}}));
    auto root = roundTrip(schemaBuilder);
    auto& row = root->asRow();
    ASSERT_EQ(1, row.childrenCount()) << tc.name;
    EXPECT_EQ(Kind::Scalar, row.childAt(0)->kind()) << tc.name;
    EXPECT_EQ(
        tc.expectedKind,
        row.childAt(0)->asScalar().scalarDescriptor().scalarKind())
        << tc.name;
  }
}

TEST(SchemaSerializationTest, RowType) {
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW(
          {{"a", NIMBLE_TINYINT()},
           {"b", NIMBLE_INTEGER()},
           {"c", NIMBLE_STRING()}}));
  auto root = roundTrip(schemaBuilder);
  ASSERT_EQ(Kind::Row, root->kind());
  auto& row = root->asRow();
  ASSERT_EQ(3, row.childrenCount());
  EXPECT_EQ("a", row.nameAt(0));
  EXPECT_EQ("b", row.nameAt(1));
  EXPECT_EQ("c", row.nameAt(2));
  EXPECT_EQ(
      ScalarKind::Int8,
      row.childAt(0)->asScalar().scalarDescriptor().scalarKind());
  EXPECT_EQ(
      ScalarKind::Int32,
      row.childAt(1)->asScalar().scalarDescriptor().scalarKind());
  EXPECT_EQ(
      ScalarKind::String,
      row.childAt(2)->asScalar().scalarDescriptor().scalarKind());
}

TEST(SchemaSerializationTest, ArrayType) {
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder, NIMBLE_ROW({{"arr", NIMBLE_ARRAY(NIMBLE_BIGINT())}}));
  auto root = roundTrip(schemaBuilder);
  auto& row = root->asRow();
  ASSERT_EQ(1, row.childrenCount());
  auto& arr = row.childAt(0)->asArray();
  EXPECT_EQ(Kind::Array, row.childAt(0)->kind());
  EXPECT_EQ(Kind::Scalar, arr.elements()->kind());
  EXPECT_EQ(
      ScalarKind::Int64,
      arr.elements()->asScalar().scalarDescriptor().scalarKind());
}

TEST(SchemaSerializationTest, MapType) {
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW({{"m", NIMBLE_MAP(NIMBLE_STRING(), NIMBLE_INTEGER())}}));
  auto root = roundTrip(schemaBuilder);
  auto& row = root->asRow();
  ASSERT_EQ(1, row.childrenCount());
  auto& map = row.childAt(0)->asMap();
  EXPECT_EQ(Kind::Map, row.childAt(0)->kind());
  EXPECT_EQ(
      ScalarKind::String,
      map.keys()->asScalar().scalarDescriptor().scalarKind());
  EXPECT_EQ(
      ScalarKind::Int32,
      map.values()->asScalar().scalarDescriptor().scalarKind());
}

TEST(SchemaSerializationTest, ArrayWithOffsetsType) {
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder, NIMBLE_ROW({{"oa", NIMBLE_OFFSETARRAY(NIMBLE_BIGINT())}}));
  auto root = roundTrip(schemaBuilder);
  auto& row = root->asRow();
  ASSERT_EQ(1, row.childrenCount());
  EXPECT_EQ(Kind::ArrayWithOffsets, row.childAt(0)->kind());
  auto& arr = row.childAt(0)->asArrayWithOffsets();
  EXPECT_EQ(Kind::Scalar, arr.elements()->kind());
  EXPECT_EQ(
      ScalarKind::Int64,
      arr.elements()->asScalar().scalarDescriptor().scalarKind());
}

TEST(SchemaSerializationTest, SlidingWindowMapType) {
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW(
          {{"swm",
            NIMBLE_SLIDINGWINDOWMAP(NIMBLE_STRING(), NIMBLE_INTEGER())}}));
  auto root = roundTrip(schemaBuilder);
  auto& row = root->asRow();
  ASSERT_EQ(1, row.childrenCount());
  EXPECT_EQ(Kind::SlidingWindowMap, row.childAt(0)->kind());
  auto& swm = row.childAt(0)->asSlidingWindowMap();
  EXPECT_EQ(
      ScalarKind::String,
      swm.keys()->asScalar().scalarDescriptor().scalarKind());
  EXPECT_EQ(
      ScalarKind::Int32,
      swm.values()->asScalar().scalarDescriptor().scalarKind());
}

TEST(SchemaSerializationTest, FlatMapType) {
  SchemaBuilder schemaBuilder;
  test::FlatMapChildAdder adder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW({{"fm", NIMBLE_FLATMAP(String, NIMBLE_INTEGER(), adder)}}));
  adder.addChild("key1");
  auto root = roundTrip(schemaBuilder);
  auto& row = root->asRow();
  ASSERT_EQ(1, row.childrenCount());
  EXPECT_EQ(Kind::FlatMap, row.childAt(0)->kind());
  auto& fm = row.childAt(0)->asFlatMap();
  EXPECT_EQ(ScalarKind::String, fm.keyScalarKind());
  ASSERT_EQ(1, fm.childrenCount());
  EXPECT_EQ(
      ScalarKind::Int32,
      fm.childAt(0)->asScalar().scalarDescriptor().scalarKind());
}

TEST(SchemaSerializationTest, TimestampMicroNanoType) {
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder, NIMBLE_ROW({{"ts", NIMBLE_TIMESTAMPMICRONANO()}}));
  auto root = roundTrip(schemaBuilder);
  auto& row = root->asRow();
  ASSERT_EQ(1, row.childrenCount());
  EXPECT_EQ(Kind::TimestampMicroNano, row.childAt(0)->kind());
}

TEST(SchemaSerializationTest, NestedComplex) {
  // ROW containing ARRAY of MAP
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW(
          {{"nested",
            NIMBLE_ARRAY(NIMBLE_MAP(NIMBLE_STRING(), NIMBLE_BIGINT()))}}));
  auto root = roundTrip(schemaBuilder);
  auto& row = root->asRow();
  ASSERT_EQ(1, row.childrenCount());
  EXPECT_EQ("nested", row.nameAt(0));

  auto& arr = row.childAt(0)->asArray();
  EXPECT_EQ(Kind::Array, row.childAt(0)->kind());

  auto& map = arr.elements()->asMap();
  EXPECT_EQ(Kind::Map, arr.elements()->kind());
  EXPECT_EQ(
      ScalarKind::String,
      map.keys()->asScalar().scalarDescriptor().scalarKind());
  EXPECT_EQ(
      ScalarKind::Int64,
      map.values()->asScalar().scalarDescriptor().scalarKind());
}
