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
#include "dwio/nimble/velox/SchemaUtils.h"
#include <gtest/gtest.h>
#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/SchemaReader.h"
#include "dwio/nimble/velox/tests/SchemaUtils.h"
#include "velox/type/Type.h"

using namespace facebook;
using namespace facebook::nimble;

// --- convertToVeloxType tests ---

TEST(SchemaUtilsTest, ConvertScalarToVelox) {
  struct TestCase {
    ScalarKind scalarKind;
    velox::TypeKind expectedVeloxKind;
  };

  std::vector<TestCase> cases = {
      {ScalarKind::Int8, velox::TypeKind::TINYINT},
      {ScalarKind::Int16, velox::TypeKind::SMALLINT},
      {ScalarKind::Int32, velox::TypeKind::INTEGER},
      {ScalarKind::Int64, velox::TypeKind::BIGINT},
      {ScalarKind::Float, velox::TypeKind::REAL},
      {ScalarKind::Double, velox::TypeKind::DOUBLE},
      {ScalarKind::Bool, velox::TypeKind::BOOLEAN},
      {ScalarKind::String, velox::TypeKind::VARCHAR},
      {ScalarKind::Binary, velox::TypeKind::VARBINARY},
  };

  for (const auto& tc : cases) {
    SchemaBuilder schemaBuilder;
    NIMBLE_SCHEMA(
        schemaBuilder,
        NIMBLE_ROW({{"f", builder.createScalarTypeBuilder(tc.scalarKind)}}));
    auto nimbleType = SchemaReader::getSchema(schemaBuilder.schemaNodes());
    auto& row = nimbleType->asRow();
    auto veloxType = convertToVeloxType(*row.childAt(0));
    EXPECT_EQ(tc.expectedVeloxKind, veloxType->kind())
        << toString(tc.scalarKind);
  }
}

TEST(SchemaUtilsTest, ConvertUnsupportedScalarToVeloxThrows) {
  std::vector<ScalarKind> unsupported = {
      ScalarKind::UInt8,
      ScalarKind::UInt16,
      ScalarKind::UInt32,
      ScalarKind::UInt64,
      ScalarKind::Undefined,
  };

  for (auto kind : unsupported) {
    SchemaBuilder schemaBuilder;
    NIMBLE_SCHEMA(
        schemaBuilder,
        NIMBLE_ROW({{"f", builder.createScalarTypeBuilder(kind)}}));
    auto nimbleType = SchemaReader::getSchema(schemaBuilder.schemaNodes());
    auto& row = nimbleType->asRow();
    EXPECT_THROW(convertToVeloxType(*row.childAt(0)), NimbleUserError)
        << toString(kind);
  }
}

TEST(SchemaUtilsTest, ConvertTimestampMicroNanoToVelox) {
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder, NIMBLE_ROW({{"ts", NIMBLE_TIMESTAMPMICRONANO()}}));
  auto nimbleType = SchemaReader::getSchema(schemaBuilder.schemaNodes());
  auto& row = nimbleType->asRow();
  auto veloxType = convertToVeloxType(*row.childAt(0));
  EXPECT_EQ(velox::TypeKind::TIMESTAMP, veloxType->kind());
}

TEST(SchemaUtilsTest, ConvertRowToVelox) {
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW({{"a", NIMBLE_TINYINT()}, {"b", NIMBLE_BIGINT()}}));
  auto nimbleType = SchemaReader::getSchema(schemaBuilder.schemaNodes());
  auto veloxType = convertToVeloxType(*nimbleType);
  EXPECT_EQ(velox::TypeKind::ROW, veloxType->kind());
  auto& veloxRow = veloxType->asRow();
  ASSERT_EQ(2, veloxRow.size());
  EXPECT_EQ("a", veloxRow.nameOf(0));
  EXPECT_EQ("b", veloxRow.nameOf(1));
  EXPECT_EQ(velox::TypeKind::TINYINT, veloxRow.childAt(0)->kind());
  EXPECT_EQ(velox::TypeKind::BIGINT, veloxRow.childAt(1)->kind());
}

TEST(SchemaUtilsTest, ConvertArrayToVelox) {
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder, NIMBLE_ROW({{"arr", NIMBLE_ARRAY(NIMBLE_BIGINT())}}));
  auto nimbleType = SchemaReader::getSchema(schemaBuilder.schemaNodes());
  auto& row = nimbleType->asRow();
  auto veloxType = convertToVeloxType(*row.childAt(0));
  EXPECT_EQ(velox::TypeKind::ARRAY, veloxType->kind());
  EXPECT_EQ(
      velox::TypeKind::BIGINT, veloxType->asArray().elementType()->kind());
}

TEST(SchemaUtilsTest, ConvertArrayWithOffsetsToVelox) {
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder, NIMBLE_ROW({{"oa", NIMBLE_OFFSETARRAY(NIMBLE_BIGINT())}}));
  auto nimbleType = SchemaReader::getSchema(schemaBuilder.schemaNodes());
  auto& row = nimbleType->asRow();
  auto veloxType = convertToVeloxType(*row.childAt(0));
  EXPECT_EQ(velox::TypeKind::ARRAY, veloxType->kind());
  EXPECT_EQ(
      velox::TypeKind::BIGINT, veloxType->asArray().elementType()->kind());
}

TEST(SchemaUtilsTest, ConvertMapToVelox) {
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW({{"m", NIMBLE_MAP(NIMBLE_STRING(), NIMBLE_INTEGER())}}));
  auto nimbleType = SchemaReader::getSchema(schemaBuilder.schemaNodes());
  auto& row = nimbleType->asRow();
  auto veloxType = convertToVeloxType(*row.childAt(0));
  EXPECT_EQ(velox::TypeKind::MAP, veloxType->kind());
  EXPECT_EQ(velox::TypeKind::VARCHAR, veloxType->asMap().keyType()->kind());
  EXPECT_EQ(velox::TypeKind::INTEGER, veloxType->asMap().valueType()->kind());
}

TEST(SchemaUtilsTest, ConvertSlidingWindowMapToVelox) {
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW(
          {{"swm",
            NIMBLE_SLIDINGWINDOWMAP(NIMBLE_STRING(), NIMBLE_INTEGER())}}));
  auto nimbleType = SchemaReader::getSchema(schemaBuilder.schemaNodes());
  auto& row = nimbleType->asRow();
  auto veloxType = convertToVeloxType(*row.childAt(0));
  EXPECT_EQ(velox::TypeKind::MAP, veloxType->kind());
  EXPECT_EQ(velox::TypeKind::VARCHAR, veloxType->asMap().keyType()->kind());
  EXPECT_EQ(velox::TypeKind::INTEGER, veloxType->asMap().valueType()->kind());
}

TEST(SchemaUtilsTest, ConvertFlatMapToVelox) {
  SchemaBuilder schemaBuilder;
  test::FlatMapChildAdder adder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW({{"fm", NIMBLE_FLATMAP(String, NIMBLE_INTEGER(), adder)}}));
  adder.addChild("key1");
  auto nimbleType = SchemaReader::getSchema(schemaBuilder.schemaNodes());
  auto& row = nimbleType->asRow();
  auto veloxType = convertToVeloxType(*row.childAt(0));
  EXPECT_EQ(velox::TypeKind::MAP, veloxType->kind());
  EXPECT_EQ(velox::TypeKind::VARCHAR, veloxType->asMap().keyType()->kind());
  EXPECT_EQ(velox::TypeKind::INTEGER, veloxType->asMap().valueType()->kind());
}

// --- convertToNimbleType tests ---

TEST(SchemaUtilsTest, ConvertVeloxScalarToNimble) {
  struct TestCase {
    velox::TypePtr veloxType;
    ScalarKind expectedScalarKind;
  };

  std::vector<TestCase> cases = {
      {velox::BOOLEAN(), ScalarKind::Bool},
      {velox::TINYINT(), ScalarKind::Int8},
      {velox::SMALLINT(), ScalarKind::Int16},
      {velox::INTEGER(), ScalarKind::Int32},
      {velox::BIGINT(), ScalarKind::Int64},
      {velox::REAL(), ScalarKind::Float},
      {velox::DOUBLE(), ScalarKind::Double},
      {velox::VARCHAR(), ScalarKind::String},
      {velox::VARBINARY(), ScalarKind::Binary},
  };

  for (const auto& tc : cases) {
    auto nimbleType = convertToNimbleType(*tc.veloxType);
    ASSERT_EQ(Kind::Scalar, nimbleType->kind()) << tc.veloxType->toString();
    EXPECT_EQ(
        tc.expectedScalarKind,
        nimbleType->asScalar().scalarDescriptor().scalarKind())
        << tc.veloxType->toString();
  }
}

TEST(SchemaUtilsTest, ConvertVeloxTimestampToNimble) {
  auto nimbleType = convertToNimbleType(*velox::TIMESTAMP());
  ASSERT_EQ(Kind::TimestampMicroNano, nimbleType->kind());
}

TEST(SchemaUtilsTest, ConvertVeloxArrayToNimble) {
  auto vType = velox::ARRAY(velox::BIGINT());
  auto nimbleType = convertToNimbleType(*vType);
  ASSERT_EQ(Kind::Array, nimbleType->kind());
  auto& arr = nimbleType->asArray();
  EXPECT_EQ(Kind::Scalar, arr.elements()->kind());
  EXPECT_EQ(
      ScalarKind::Int64,
      arr.elements()->asScalar().scalarDescriptor().scalarKind());
}

TEST(SchemaUtilsTest, ConvertVeloxMapToNimble) {
  auto vType = velox::MAP(velox::VARCHAR(), velox::INTEGER());
  auto nimbleType = convertToNimbleType(*vType);
  ASSERT_EQ(Kind::Map, nimbleType->kind());
  auto& map = nimbleType->asMap();
  EXPECT_EQ(
      ScalarKind::String,
      map.keys()->asScalar().scalarDescriptor().scalarKind());
  EXPECT_EQ(
      ScalarKind::Int32,
      map.values()->asScalar().scalarDescriptor().scalarKind());
}

TEST(SchemaUtilsTest, ConvertVeloxRowToNimble) {
  auto vType = velox::ROW({"x", "y"}, {velox::TINYINT(), velox::DOUBLE()});
  auto nimbleType = convertToNimbleType(*vType);
  ASSERT_EQ(Kind::Row, nimbleType->kind());
  auto& row = nimbleType->asRow();
  ASSERT_EQ(2, row.childrenCount());
  EXPECT_EQ("x", row.nameAt(0));
  EXPECT_EQ("y", row.nameAt(1));
  EXPECT_EQ(
      ScalarKind::Int8,
      row.childAt(0)->asScalar().scalarDescriptor().scalarKind());
  EXPECT_EQ(
      ScalarKind::Double,
      row.childAt(1)->asScalar().scalarDescriptor().scalarKind());
}

// --- Round-trip tests ---

TEST(SchemaUtilsTest, RoundTripScalarTypes) {
  std::vector<velox::TypePtr> scalarTypes = {
      velox::BOOLEAN(),
      velox::TINYINT(),
      velox::SMALLINT(),
      velox::INTEGER(),
      velox::BIGINT(),
      velox::REAL(),
      velox::DOUBLE(),
      velox::VARCHAR(),
      velox::VARBINARY(),
  };

  for (const auto& vType : scalarTypes) {
    auto nimbleType = convertToNimbleType(*vType);
    auto roundTripped = convertToVeloxType(*nimbleType);
    EXPECT_TRUE(vType->equivalent(*roundTripped))
        << "Failed round-trip for " << vType->toString() << " got "
        << roundTripped->toString();
  }
}

TEST(SchemaUtilsTest, RoundTripTimestamp) {
  auto vType = velox::TIMESTAMP();
  auto nimbleType = convertToNimbleType(*vType);
  auto roundTripped = convertToVeloxType(*nimbleType);
  EXPECT_TRUE(vType->equivalent(*roundTripped));
}

TEST(SchemaUtilsTest, RoundTripArray) {
  auto vType = velox::ARRAY(velox::INTEGER());
  auto nimbleType = convertToNimbleType(*vType);
  auto roundTripped = convertToVeloxType(*nimbleType);
  EXPECT_TRUE(vType->equivalent(*roundTripped)) << roundTripped->toString();
}

TEST(SchemaUtilsTest, RoundTripMap) {
  auto vType = velox::MAP(velox::VARCHAR(), velox::BIGINT());
  auto nimbleType = convertToNimbleType(*vType);
  auto roundTripped = convertToVeloxType(*nimbleType);
  EXPECT_TRUE(vType->equivalent(*roundTripped)) << roundTripped->toString();
}

TEST(SchemaUtilsTest, RoundTripRow) {
  auto vType = velox::ROW({"a", "b"}, {velox::TINYINT(), velox::DOUBLE()});
  auto nimbleType = convertToNimbleType(*vType);
  auto roundTripped = convertToVeloxType(*nimbleType);
  EXPECT_TRUE(vType->equivalent(*roundTripped)) << roundTripped->toString();
}
