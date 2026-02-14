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
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/SchemaReader.h"
#include "dwio/nimble/velox/tests/SchemaUtils.h"
#include "velox/type/Type.h"

using namespace facebook;
using namespace facebook::nimble;

// Helper functions to create velox types without conflicting with
// test SchemaUtils.h macros (ROW, BOOLEAN, INTEGER, etc.).
namespace {

velox::TypePtr veloxBool() {
  return velox::ScalarType<velox::TypeKind::BOOLEAN>::create();
}
velox::TypePtr veloxTinyint() {
  return velox::ScalarType<velox::TypeKind::TINYINT>::create();
}
velox::TypePtr veloxSmallint() {
  return velox::ScalarType<velox::TypeKind::SMALLINT>::create();
}
velox::TypePtr veloxInt() {
  return velox::ScalarType<velox::TypeKind::INTEGER>::create();
}
velox::TypePtr veloxBigint() {
  return velox::ScalarType<velox::TypeKind::BIGINT>::create();
}
velox::TypePtr veloxReal() {
  return velox::ScalarType<velox::TypeKind::REAL>::create();
}
velox::TypePtr veloxDouble() {
  return velox::ScalarType<velox::TypeKind::DOUBLE>::create();
}
velox::TypePtr veloxVarchar() {
  return velox::ScalarType<velox::TypeKind::VARCHAR>::create();
}
velox::TypePtr veloxVarbinary() {
  return velox::ScalarType<velox::TypeKind::VARBINARY>::create();
}
velox::TypePtr veloxTimestamp() {
  return velox::ScalarType<velox::TypeKind::TIMESTAMP>::create();
}
velox::TypePtr veloxArray(velox::TypePtr elementType) {
  return std::make_shared<const velox::ArrayType>(std::move(elementType));
}
velox::TypePtr veloxMap(velox::TypePtr keyType, velox::TypePtr valueType) {
  return std::make_shared<const velox::MapType>(
      std::move(keyType), std::move(valueType));
}
velox::TypePtr veloxRow(
    std::vector<std::string> names,
    std::vector<velox::TypePtr> types) {
  return std::make_shared<const velox::RowType>(
      std::move(names), std::move(types));
}

} // namespace

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
    SCHEMA(
        schemaBuilder,
        ROW({{"f", builder.createScalarTypeBuilder(tc.scalarKind)}}));
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
    SCHEMA(schemaBuilder, ROW({{"f", builder.createScalarTypeBuilder(kind)}}));
    auto nimbleType = SchemaReader::getSchema(schemaBuilder.schemaNodes());
    auto& row = nimbleType->asRow();
    EXPECT_THROW(convertToVeloxType(*row.childAt(0)), NimbleUserError)
        << toString(kind);
  }
}

TEST(SchemaUtilsTest, ConvertTimestampMicroNanoToVelox) {
  SchemaBuilder schemaBuilder;
  SCHEMA(schemaBuilder, ROW({{"ts", TIMESTAMPMICRONANO()}}));
  auto nimbleType = SchemaReader::getSchema(schemaBuilder.schemaNodes());
  auto& row = nimbleType->asRow();
  auto veloxType = convertToVeloxType(*row.childAt(0));
  EXPECT_EQ(velox::TypeKind::TIMESTAMP, veloxType->kind());
}

TEST(SchemaUtilsTest, ConvertRowToVelox) {
  SchemaBuilder schemaBuilder;
  SCHEMA(schemaBuilder, ROW({{"a", TINYINT()}, {"b", BIGINT()}}));
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
  SCHEMA(schemaBuilder, ROW({{"arr", ARRAY(BIGINT())}}));
  auto nimbleType = SchemaReader::getSchema(schemaBuilder.schemaNodes());
  auto& row = nimbleType->asRow();
  auto veloxType = convertToVeloxType(*row.childAt(0));
  EXPECT_EQ(velox::TypeKind::ARRAY, veloxType->kind());
  EXPECT_EQ(
      velox::TypeKind::BIGINT, veloxType->asArray().elementType()->kind());
}

TEST(SchemaUtilsTest, ConvertArrayWithOffsetsToVelox) {
  SchemaBuilder schemaBuilder;
  SCHEMA(schemaBuilder, ROW({{"oa", OFFSETARRAY(BIGINT())}}));
  auto nimbleType = SchemaReader::getSchema(schemaBuilder.schemaNodes());
  auto& row = nimbleType->asRow();
  auto veloxType = convertToVeloxType(*row.childAt(0));
  EXPECT_EQ(velox::TypeKind::ARRAY, veloxType->kind());
  EXPECT_EQ(
      velox::TypeKind::BIGINT, veloxType->asArray().elementType()->kind());
}

TEST(SchemaUtilsTest, ConvertMapToVelox) {
  SchemaBuilder schemaBuilder;
  SCHEMA(schemaBuilder, ROW({{"m", MAP(STRING(), INTEGER())}}));
  auto nimbleType = SchemaReader::getSchema(schemaBuilder.schemaNodes());
  auto& row = nimbleType->asRow();
  auto veloxType = convertToVeloxType(*row.childAt(0));
  EXPECT_EQ(velox::TypeKind::MAP, veloxType->kind());
  EXPECT_EQ(velox::TypeKind::VARCHAR, veloxType->asMap().keyType()->kind());
  EXPECT_EQ(velox::TypeKind::INTEGER, veloxType->asMap().valueType()->kind());
}

TEST(SchemaUtilsTest, ConvertSlidingWindowMapToVelox) {
  SchemaBuilder schemaBuilder;
  SCHEMA(schemaBuilder, ROW({{"swm", SLIDINGWINDOWMAP(STRING(), INTEGER())}}));
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
  SCHEMA(schemaBuilder, ROW({{"fm", FLATMAP(String, INTEGER(), adder)}}));
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
      {veloxBool(), ScalarKind::Bool},
      {veloxTinyint(), ScalarKind::Int8},
      {veloxSmallint(), ScalarKind::Int16},
      {veloxInt(), ScalarKind::Int32},
      {veloxBigint(), ScalarKind::Int64},
      {veloxReal(), ScalarKind::Float},
      {veloxDouble(), ScalarKind::Double},
      {veloxVarchar(), ScalarKind::String},
      {veloxVarbinary(), ScalarKind::Binary},
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
  auto nimbleType = convertToNimbleType(*veloxTimestamp());
  ASSERT_EQ(Kind::TimestampMicroNano, nimbleType->kind());
}

TEST(SchemaUtilsTest, ConvertVeloxArrayToNimble) {
  auto vType = veloxArray(veloxBigint());
  auto nimbleType = convertToNimbleType(*vType);
  ASSERT_EQ(Kind::Array, nimbleType->kind());
  auto& arr = nimbleType->asArray();
  EXPECT_EQ(Kind::Scalar, arr.elements()->kind());
  EXPECT_EQ(
      ScalarKind::Int64,
      arr.elements()->asScalar().scalarDescriptor().scalarKind());
}

TEST(SchemaUtilsTest, ConvertVeloxMapToNimble) {
  auto vType = veloxMap(veloxVarchar(), veloxInt());
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
  auto vType = veloxRow({"x", "y"}, {veloxTinyint(), veloxDouble()});
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
      veloxBool(),
      veloxTinyint(),
      veloxSmallint(),
      veloxInt(),
      veloxBigint(),
      veloxReal(),
      veloxDouble(),
      veloxVarchar(),
      veloxVarbinary(),
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
  auto vType = veloxTimestamp();
  auto nimbleType = convertToNimbleType(*vType);
  auto roundTripped = convertToVeloxType(*nimbleType);
  EXPECT_TRUE(vType->equivalent(*roundTripped));
}

TEST(SchemaUtilsTest, RoundTripArray) {
  auto vType = veloxArray(veloxInt());
  auto nimbleType = convertToNimbleType(*vType);
  auto roundTripped = convertToVeloxType(*nimbleType);
  EXPECT_TRUE(vType->equivalent(*roundTripped)) << roundTripped->toString();
}

TEST(SchemaUtilsTest, RoundTripMap) {
  auto vType = veloxMap(veloxVarchar(), veloxBigint());
  auto nimbleType = convertToNimbleType(*vType);
  auto roundTripped = convertToVeloxType(*nimbleType);
  EXPECT_TRUE(vType->equivalent(*roundTripped)) << roundTripped->toString();
}

TEST(SchemaUtilsTest, RoundTripRow) {
  auto vType = veloxRow({"a", "b"}, {veloxTinyint(), veloxDouble()});
  auto nimbleType = convertToNimbleType(*vType);
  auto roundTripped = convertToVeloxType(*nimbleType);
  EXPECT_TRUE(vType->equivalent(*roundTripped)) << roundTripped->toString();
}
