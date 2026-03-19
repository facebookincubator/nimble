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

#include <folly/Random.h>

#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/SchemaReader.h"
#include "dwio/nimble/velox/tests/SchemaUtils.h"
#include "velox/type/Subfield.h"
#include "velox/type/Type.h"

using namespace facebook;
using namespace facebook::nimble;
using Subfield = velox::common::Subfield;

namespace {

// Recursively compares two nimble Type trees for structural equivalence.
// Checks kind, names, children count, scalar kinds, and FlatMap key scalar
// kinds. Ignores stream offsets (they differ between the two overloads).
void expectSameType(const Type& a, const Type& b, const std::string& path) {
  SCOPED_TRACE(path);
  ASSERT_EQ(a.kind(), b.kind());

  switch (a.kind()) {
    case Kind::Scalar:
      EXPECT_EQ(
          a.asScalar().scalarDescriptor().scalarKind(),
          b.asScalar().scalarDescriptor().scalarKind());
      break;
    case Kind::TimestampMicroNano:
      break;
    case Kind::Row: {
      const auto& ra = a.asRow();
      const auto& rb = b.asRow();
      ASSERT_EQ(ra.childrenCount(), rb.childrenCount());
      for (size_t i = 0; i < ra.childrenCount(); ++i) {
        EXPECT_EQ(ra.nameAt(i), rb.nameAt(i));
        expectSameType(
            *ra.childAt(i), *rb.childAt(i), path + "." + ra.nameAt(i));
      }
      break;
    }
    case Kind::Array:
      expectSameType(
          *a.asArray().elements(), *b.asArray().elements(), path + "[]");
      break;
    case Kind::ArrayWithOffsets:
      expectSameType(
          *a.asArrayWithOffsets().elements(),
          *b.asArrayWithOffsets().elements(),
          path + "[]");
      break;
    case Kind::Map:
      expectSameType(*a.asMap().keys(), *b.asMap().keys(), path + ".keys");
      expectSameType(
          *a.asMap().values(), *b.asMap().values(), path + ".values");
      break;
    case Kind::SlidingWindowMap:
      expectSameType(
          *a.asSlidingWindowMap().keys(),
          *b.asSlidingWindowMap().keys(),
          path + ".keys");
      expectSameType(
          *a.asSlidingWindowMap().values(),
          *b.asSlidingWindowMap().values(),
          path + ".values");
      break;
    case Kind::FlatMap: {
      const auto& fa = a.asFlatMap();
      const auto& fb = b.asFlatMap();
      EXPECT_EQ(fa.keyScalarKind(), fb.keyScalarKind());
      ASSERT_EQ(fa.childrenCount(), fb.childrenCount());
      for (size_t i = 0; i < fa.childrenCount(); ++i) {
        EXPECT_EQ(fa.nameAt(i), fb.nameAt(i));
        expectSameType(
            *fa.childAt(i), *fb.childAt(i), path + "[" + fa.nameAt(i) + "]");
      }
      break;
    }
    default:
      FAIL() << "Unexpected kind at " << path;
  }
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

// --- convertToNimbleType with projected subfields tests ---

TEST(SchemaUtilsTest, projectionScalarOnly) {
  auto veloxType = velox::ROW(
      {"col_a", "col_b", "col_c"},
      {velox::INTEGER(), velox::BIGINT(), velox::VARCHAR()});

  std::vector<Subfield> subfields;
  subfields.emplace_back("col_a");
  subfields.emplace_back("col_b");
  subfields.emplace_back("col_c");

  auto projectedNimbleTypeFromVelox =
      buildProjectedNimbleType(*veloxType, subfields);
  ASSERT_EQ(Kind::Row, projectedNimbleTypeFromVelox->kind());
  const auto& row = projectedNimbleTypeFromVelox->asRow();
  ASSERT_EQ(3, row.childrenCount());
  EXPECT_EQ("col_a", row.nameAt(0));
  EXPECT_EQ("col_b", row.nameAt(1));
  EXPECT_EQ("col_c", row.nameAt(2));

  EXPECT_EQ(Kind::Scalar, row.childAt(0)->kind());
  EXPECT_EQ(
      ScalarKind::Int32,
      row.childAt(0)->asScalar().scalarDescriptor().scalarKind());
  EXPECT_EQ(Kind::Scalar, row.childAt(1)->kind());
  EXPECT_EQ(
      ScalarKind::Int64,
      row.childAt(1)->asScalar().scalarDescriptor().scalarKind());
  EXPECT_EQ(Kind::Scalar, row.childAt(2)->kind());
  EXPECT_EQ(
      ScalarKind::String,
      row.childAt(2)->asScalar().scalarDescriptor().scalarKind());

  // Cross-validate with nimble-type overload.
  auto convertedNimbleType = convertToNimbleType(*veloxType);
  std::vector<uint32_t> projectedStreamOffsets;
  auto projectedNimbleType = buildProjectedNimbleType(
      convertedNimbleType.get(), subfields, projectedStreamOffsets);
  expectSameType(*projectedNimbleTypeFromVelox, *projectedNimbleType, "root");
}

TEST(SchemaUtilsTest, projectionFlatMap) {
  // Velox type has MAP columns (FlatMap in file).
  auto veloxType = velox::ROW(
      {"user_id", "int_traits_map", "long_traits_map"},
      {velox::BIGINT(),
       velox::MAP(velox::INTEGER(), velox::ARRAY(velox::INTEGER())),
       velox::MAP(velox::INTEGER(), velox::ARRAY(velox::BIGINT()))});

  std::vector<Subfield> subfields;
  subfields.emplace_back("user_id");
  subfields.emplace_back("int_traits_map[2]");
  subfields.emplace_back("int_traits_map[387]");
  subfields.emplace_back("long_traits_map[10]");

  ColumnEncodings encodings;
  encodings.flatMapColumns.insert("int_traits_map");
  encodings.flatMapColumns.insert("long_traits_map");

  auto projectedNimbleTypeFromVelox =
      buildProjectedNimbleType(*veloxType, subfields, encodings);
  ASSERT_EQ(Kind::Row, projectedNimbleTypeFromVelox->kind());
  const auto& row = projectedNimbleTypeFromVelox->asRow();
  ASSERT_EQ(3, row.childrenCount());

  // user_id: scalar.
  EXPECT_EQ("user_id", row.nameAt(0));
  EXPECT_EQ(Kind::Scalar, row.childAt(0)->kind());
  EXPECT_EQ(
      ScalarKind::Int64,
      row.childAt(0)->asScalar().scalarDescriptor().scalarKind());

  // int_traits_map: FlatMap with 2 keys.
  EXPECT_EQ("int_traits_map", row.nameAt(1));
  ASSERT_EQ(Kind::FlatMap, row.childAt(1)->kind());
  const auto& intFlatMap = row.childAt(1)->asFlatMap();
  EXPECT_EQ(ScalarKind::Int32, intFlatMap.keyScalarKind());
  ASSERT_EQ(2, intFlatMap.childrenCount());
  EXPECT_EQ("2", intFlatMap.nameAt(0));
  EXPECT_EQ("387", intFlatMap.nameAt(1));
  // Value type: Array<Int32>.
  ASSERT_EQ(Kind::Array, intFlatMap.childAt(0)->kind());
  EXPECT_EQ(
      ScalarKind::Int32,
      intFlatMap.childAt(0)
          ->asArray()
          .elements()
          ->asScalar()
          .scalarDescriptor()
          .scalarKind());

  // long_traits_map: FlatMap with 1 key.
  EXPECT_EQ("long_traits_map", row.nameAt(2));
  ASSERT_EQ(Kind::FlatMap, row.childAt(2)->kind());
  const auto& longFlatMap = row.childAt(2)->asFlatMap();
  EXPECT_EQ(ScalarKind::Int32, longFlatMap.keyScalarKind());
  ASSERT_EQ(1, longFlatMap.childrenCount());
  EXPECT_EQ("10", longFlatMap.nameAt(0));
  // Value type: Array<Int64>.
  ASSERT_EQ(Kind::Array, longFlatMap.childAt(0)->kind());
  EXPECT_EQ(
      ScalarKind::Int64,
      longFlatMap.childAt(0)
          ->asArray()
          .elements()
          ->asScalar()
          .scalarDescriptor()
          .scalarKind());

  // Cross-validate with nimble-type overload.
  // Build equivalent nimble input schema with FlatMap encoding.
  SchemaBuilder schemaBuilder;
  test::FlatMapChildAdder intAdder;
  test::FlatMapChildAdder longAdder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW({
          {"user_id", NIMBLE_BIGINT()},
          {"int_traits_map",
           NIMBLE_FLATMAP(Int32, NIMBLE_ARRAY(NIMBLE_INTEGER()), intAdder)},
          {"long_traits_map",
           NIMBLE_FLATMAP(Int32, NIMBLE_ARRAY(NIMBLE_BIGINT()), longAdder)},
      }));
  intAdder.addChild("2");
  intAdder.addChild("387");
  longAdder.addChild("10");
  auto convertedNimbleType =
      SchemaReader::getSchema(schemaBuilder.schemaNodes());

  std::vector<uint32_t> projectedStreamOffsets;
  auto projectedNimbleType = buildProjectedNimbleType(
      convertedNimbleType.get(), subfields, projectedStreamOffsets);
  expectSameType(*projectedNimbleTypeFromVelox, *projectedNimbleType, "root");
}

TEST(SchemaUtilsTest, projectionDictionaryArray) {
  // dictionaryArrayColumns: ARRAY columns become ArrayWithOffsets.
  auto veloxType = velox::ROW(
      {"id", "tags", "scores"},
      {velox::BIGINT(),
       velox::ARRAY(velox::VARCHAR()),
       velox::ARRAY(velox::INTEGER())});

  std::vector<Subfield> subfields;
  subfields.emplace_back("id");
  subfields.emplace_back("tags");
  subfields.emplace_back("scores");

  ColumnEncodings encodings;
  encodings.dictionaryArrayColumns.insert("tags");

  auto projectedNimbleTypeFromVelox =
      buildProjectedNimbleType(*veloxType, subfields, encodings);
  ASSERT_EQ(Kind::Row, projectedNimbleTypeFromVelox->kind());
  const auto& row = projectedNimbleTypeFromVelox->asRow();
  ASSERT_EQ(3, row.childrenCount());

  // id: scalar.
  EXPECT_EQ("id", row.nameAt(0));
  EXPECT_EQ(Kind::Scalar, row.childAt(0)->kind());

  // tags: ArrayWithOffsets (dictionaryArray encoding).
  EXPECT_EQ("tags", row.nameAt(1));
  ASSERT_EQ(Kind::ArrayWithOffsets, row.childAt(1)->kind());
  const auto& tagsArray = row.childAt(1)->asArrayWithOffsets();
  EXPECT_EQ(Kind::Scalar, tagsArray.elements()->kind());
  EXPECT_EQ(
      ScalarKind::String,
      tagsArray.elements()->asScalar().scalarDescriptor().scalarKind());

  // scores: plain Array (not in dictionaryArrayColumns).
  EXPECT_EQ("scores", row.nameAt(2));
  ASSERT_EQ(Kind::Array, row.childAt(2)->kind());
  const auto& scoresArray = row.childAt(2)->asArray();
  EXPECT_EQ(Kind::Scalar, scoresArray.elements()->kind());
  EXPECT_EQ(
      ScalarKind::Int32,
      scoresArray.elements()->asScalar().scalarDescriptor().scalarKind());

  // Cross-validate with nimble-type overload.
  // Build equivalent nimble schema with ArrayWithOffsets.
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW({
          {"id", NIMBLE_BIGINT()},
          {"tags", NIMBLE_OFFSETARRAY(NIMBLE_STRING())},
          {"scores", NIMBLE_ARRAY(NIMBLE_INTEGER())},
      }));
  auto convertedNimbleType =
      SchemaReader::getSchema(schemaBuilder.schemaNodes());

  std::vector<uint32_t> projectedStreamOffsets;
  auto projectedNimbleType = buildProjectedNimbleType(
      convertedNimbleType.get(), subfields, projectedStreamOffsets);
  expectSameType(*projectedNimbleTypeFromVelox, *projectedNimbleType, "root");
}

TEST(SchemaUtilsTest, projectionDeduplicatedMap) {
  // deduplicatedMapColumns: MAP columns become SlidingWindowMap.
  auto veloxType = velox::ROW(
      {"id", "dedup_map", "regular_map"},
      {velox::BIGINT(),
       velox::MAP(velox::VARCHAR(), velox::INTEGER()),
       velox::MAP(velox::VARCHAR(), velox::BIGINT())});

  std::vector<Subfield> subfields;
  subfields.emplace_back("id");
  subfields.emplace_back("dedup_map");
  subfields.emplace_back("regular_map");

  ColumnEncodings encodings;
  encodings.deduplicatedMapColumns.insert("dedup_map");

  auto projectedNimbleTypeFromVelox =
      buildProjectedNimbleType(*veloxType, subfields, encodings);
  ASSERT_EQ(Kind::Row, projectedNimbleTypeFromVelox->kind());
  const auto& row = projectedNimbleTypeFromVelox->asRow();
  ASSERT_EQ(3, row.childrenCount());

  // id: scalar.
  EXPECT_EQ("id", row.nameAt(0));
  EXPECT_EQ(Kind::Scalar, row.childAt(0)->kind());

  // dedup_map: SlidingWindowMap (deduplicatedMap encoding).
  EXPECT_EQ("dedup_map", row.nameAt(1));
  ASSERT_EQ(Kind::SlidingWindowMap, row.childAt(1)->kind());
  const auto& dedupMap = row.childAt(1)->asSlidingWindowMap();
  EXPECT_EQ(Kind::Scalar, dedupMap.keys()->kind());
  EXPECT_EQ(
      ScalarKind::String,
      dedupMap.keys()->asScalar().scalarDescriptor().scalarKind());
  EXPECT_EQ(Kind::Scalar, dedupMap.values()->kind());
  EXPECT_EQ(
      ScalarKind::Int32,
      dedupMap.values()->asScalar().scalarDescriptor().scalarKind());

  // regular_map: plain Map (not in deduplicatedMapColumns).
  EXPECT_EQ("regular_map", row.nameAt(2));
  ASSERT_EQ(Kind::Map, row.childAt(2)->kind());
  const auto& regularMap = row.childAt(2)->asMap();
  EXPECT_EQ(
      ScalarKind::String,
      regularMap.keys()->asScalar().scalarDescriptor().scalarKind());
  EXPECT_EQ(
      ScalarKind::Int64,
      regularMap.values()->asScalar().scalarDescriptor().scalarKind());

  // Cross-validate with nimble-type overload.
  // Build equivalent nimble schema with SlidingWindowMap.
  SchemaBuilder schemaBuilder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW({
          {"id", NIMBLE_BIGINT()},
          {"dedup_map",
           NIMBLE_SLIDINGWINDOWMAP(NIMBLE_STRING(), NIMBLE_INTEGER())},
          {"regular_map", NIMBLE_MAP(NIMBLE_STRING(), NIMBLE_BIGINT())},
      }));
  auto convertedNimbleType =
      SchemaReader::getSchema(schemaBuilder.schemaNodes());

  std::vector<uint32_t> projectedStreamOffsets;
  auto projectedNimbleType = buildProjectedNimbleType(
      convertedNimbleType.get(), subfields, projectedStreamOffsets);
  expectSameType(*projectedNimbleTypeFromVelox, *projectedNimbleType, "root");
}

TEST(SchemaUtilsTest, projectionMixedEncodings) {
  // All three encoding types together.
  auto veloxType = velox::ROW(
      {"id", "dict_arr", "dedup_map", "flat_map"},
      {velox::BIGINT(),
       velox::ARRAY(velox::VARCHAR()),
       velox::MAP(velox::VARCHAR(), velox::INTEGER()),
       velox::MAP(velox::INTEGER(), velox::DOUBLE())});

  std::vector<Subfield> subfields;
  subfields.emplace_back("id");
  subfields.emplace_back("dict_arr");
  subfields.emplace_back("dedup_map");
  subfields.emplace_back("flat_map[1]");
  subfields.emplace_back("flat_map[5]");

  ColumnEncodings encodings;
  encodings.dictionaryArrayColumns.insert("dict_arr");
  encodings.deduplicatedMapColumns.insert("dedup_map");
  encodings.flatMapColumns.insert("flat_map");

  auto projectedNimbleTypeFromVelox =
      buildProjectedNimbleType(*veloxType, subfields, encodings);
  ASSERT_EQ(Kind::Row, projectedNimbleTypeFromVelox->kind());
  const auto& row = projectedNimbleTypeFromVelox->asRow();
  ASSERT_EQ(4, row.childrenCount());

  // id: scalar.
  EXPECT_EQ("id", row.nameAt(0));
  EXPECT_EQ(Kind::Scalar, row.childAt(0)->kind());

  // dict_arr: ArrayWithOffsets.
  EXPECT_EQ("dict_arr", row.nameAt(1));
  ASSERT_EQ(Kind::ArrayWithOffsets, row.childAt(1)->kind());

  // dedup_map: SlidingWindowMap.
  EXPECT_EQ("dedup_map", row.nameAt(2));
  ASSERT_EQ(Kind::SlidingWindowMap, row.childAt(2)->kind());

  // flat_map: FlatMap with 2 keys.
  EXPECT_EQ("flat_map", row.nameAt(3));
  ASSERT_EQ(Kind::FlatMap, row.childAt(3)->kind());
  const auto& flatMap = row.childAt(3)->asFlatMap();
  EXPECT_EQ(ScalarKind::Int32, flatMap.keyScalarKind());
  ASSERT_EQ(2, flatMap.childrenCount());
  EXPECT_EQ("1", flatMap.nameAt(0));
  EXPECT_EQ("5", flatMap.nameAt(1));

  // Cross-validate with nimble-type overload.
  SchemaBuilder schemaBuilder;
  test::FlatMapChildAdder flatAdder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW({
          {"id", NIMBLE_BIGINT()},
          {"dict_arr", NIMBLE_OFFSETARRAY(NIMBLE_STRING())},
          {"dedup_map",
           NIMBLE_SLIDINGWINDOWMAP(NIMBLE_STRING(), NIMBLE_INTEGER())},
          {"flat_map", NIMBLE_FLATMAP(Int32, NIMBLE_DOUBLE(), flatAdder)},
      }));
  flatAdder.addChild("1");
  flatAdder.addChild("5");
  auto convertedNimbleType =
      SchemaReader::getSchema(schemaBuilder.schemaNodes());

  std::vector<uint32_t> projectedStreamOffsets;
  auto projectedNimbleType = buildProjectedNimbleType(
      convertedNimbleType.get(), subfields, projectedStreamOffsets);
  expectSameType(*projectedNimbleTypeFromVelox, *projectedNimbleType, "root");
}

TEST(SchemaUtilsTest, fullFlatMapProjectionFails) {
  auto veloxType = velox::ROW(
      {"user_id", "traits_map"},
      {velox::BIGINT(),
       velox::MAP(velox::INTEGER(), velox::ARRAY(velox::INTEGER()))});

  // Projecting entire FlatMap column without key subscripts should fail.
  std::vector<Subfield> subfields;
  subfields.emplace_back("user_id");
  subfields.emplace_back("traits_map");

  ColumnEncodings encodings;
  encodings.flatMapColumns.insert("traits_map");

  NIMBLE_ASSERT_THROW(
      buildProjectedNimbleType(*veloxType, subfields, encodings),
      "Cannot project entire FlatMap column without key subscripts");

  // Cross-validate with nimble-type overload.
  SchemaBuilder schemaBuilder;
  test::FlatMapChildAdder childAdder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW({
          {"user_id", NIMBLE_BIGINT()},
          {"traits_map",
           NIMBLE_FLATMAP(Int32, NIMBLE_ARRAY(NIMBLE_INTEGER()), childAdder)},
      }));
  childAdder.addChild("1");
  childAdder.addChild("2");
  auto convertedNimbleType =
      SchemaReader::getSchema(schemaBuilder.schemaNodes());

  std::vector<uint32_t> offsets;
  NIMBLE_ASSERT_THROW(
      buildProjectedNimbleType(convertedNimbleType.get(), subfields, offsets),
      "Cannot project entire FlatMap column without key subscripts");
}

TEST(SchemaUtilsTest, flatMapKeyProjectionRequiresFlatMapEncoding) {
  auto veloxType = velox::ROW(
      {"user_id", "traits_map"},
      {velox::BIGINT(),
       velox::MAP(velox::INTEGER(), velox::ARRAY(velox::INTEGER()))});

  // Key-level projection on a MAP column not in flatMapColumns should fail.
  std::vector<Subfield> subfields;
  subfields.emplace_back("user_id");
  subfields.emplace_back("traits_map[1]");

  // No flatMapColumns specified — traits_map is a regular MAP.
  ColumnEncodings encodings;
  NIMBLE_ASSERT_THROW(
      buildProjectedNimbleType(*veloxType, subfields, encodings),
      "FlatMap key-level projection requires the column to be in flatMapColumns");
}

TEST(SchemaUtilsTest, projectionStreamOffsets) {
  // Verify stream offsets are sequential.
  auto veloxType = velox::ROW(
      {"key", "traits_map"},
      {velox::BIGINT(),
       velox::MAP(velox::INTEGER(), velox::ARRAY(velox::INTEGER()))});

  std::vector<Subfield> subfields;
  subfields.emplace_back("key");
  subfields.emplace_back("traits_map[1]");
  subfields.emplace_back("traits_map[2]");

  ColumnEncodings encodings;
  encodings.flatMapColumns.insert("traits_map");

  auto projectedNimbleTypeFromVelox =
      buildProjectedNimbleType(*veloxType, subfields, encodings);
  // Collect all stream offsets and verify they are sequential.
  std::set<uint32_t> offsets;
  std::function<void(const Type&)> collectOffsets = [&](const Type& type) {
    switch (type.kind()) {
      case Kind::Scalar:
        offsets.insert(type.asScalar().scalarDescriptor().offset());
        break;
      case Kind::Row: {
        const auto& r = type.asRow();
        offsets.insert(r.nullsDescriptor().offset());
        for (size_t i = 0; i < r.childrenCount(); ++i) {
          collectOffsets(*r.childAt(i));
        }
        break;
      }
      case Kind::Array: {
        const auto& a = type.asArray();
        offsets.insert(a.lengthsDescriptor().offset());
        collectOffsets(*a.elements());
        break;
      }
      case Kind::FlatMap: {
        const auto& fm = type.asFlatMap();
        offsets.insert(fm.nullsDescriptor().offset());
        for (size_t i = 0; i < fm.childrenCount(); ++i) {
          offsets.insert(fm.inMapDescriptorAt(i).offset());
          collectOffsets(*fm.childAt(i));
        }
        break;
      }
      default:
        break;
    }
  };
  collectOffsets(*projectedNimbleTypeFromVelox);

  // Offsets should be 0, 1, 2, ... (no gaps, no duplicates).
  uint32_t expected = 0;
  for (auto offset : offsets) {
    EXPECT_EQ(expected, offset);
    ++expected;
  }
}

TEST(SchemaUtilsTest, projectionRowChild) {
  auto veloxType = velox::ROW(
      {"id", "struct_col"},
      {velox::BIGINT(),
       velox::ROW(
           {"field_a", "field_b", "field_c"},
           {velox::INTEGER(), velox::VARCHAR(), velox::DOUBLE()})});

  std::vector<Subfield> subfields;
  subfields.emplace_back("id");
  subfields.emplace_back("struct_col.field_a");
  subfields.emplace_back("struct_col.field_c");

  auto projectedNimbleTypeFromVelox =
      buildProjectedNimbleType(*veloxType, subfields);
  ASSERT_EQ(Kind::Row, projectedNimbleTypeFromVelox->kind());
  const auto& row = projectedNimbleTypeFromVelox->asRow();
  // Only projected columns in the root Row.
  ASSERT_EQ(2, row.childrenCount());
  EXPECT_EQ("id", row.nameAt(0));
  EXPECT_EQ("struct_col", row.nameAt(1));

  // id: scalar.
  EXPECT_EQ(Kind::Scalar, row.childAt(0)->kind());

  // struct_col: Row with only projected children.
  ASSERT_EQ(Kind::Row, row.childAt(1)->kind());
  const auto& structRow = row.childAt(1)->asRow();
  ASSERT_EQ(2, structRow.childrenCount());
  EXPECT_EQ("field_a", structRow.nameAt(0));
  EXPECT_EQ("field_c", structRow.nameAt(1));
  EXPECT_EQ(
      ScalarKind::Int32,
      structRow.childAt(0)->asScalar().scalarDescriptor().scalarKind());
  EXPECT_EQ(
      ScalarKind::Double,
      structRow.childAt(1)->asScalar().scalarDescriptor().scalarKind());

  // Cross-validate with nimble-type overload.
  auto convertedNimbleType = convertToNimbleType(*veloxType);
  std::vector<uint32_t> projectedStreamOffsets;
  auto projectedNimbleType = buildProjectedNimbleType(
      convertedNimbleType.get(), subfields, projectedStreamOffsets);
  expectSameType(*projectedNimbleTypeFromVelox, *projectedNimbleType, "root");
}

TEST(SchemaUtilsTest, projectionOnlyProjectedColumns) {
  // Verify that unprojected columns are excluded from the output.
  auto veloxType = velox::ROW(
      {"col_a", "col_b", "col_c"},
      {velox::INTEGER(), velox::BIGINT(), velox::VARCHAR()});

  std::vector<Subfield> subfields;
  subfields.emplace_back("col_a");
  subfields.emplace_back("col_c");

  auto projectedNimbleTypeFromVelox =
      buildProjectedNimbleType(*veloxType, subfields);
  ASSERT_EQ(Kind::Row, projectedNimbleTypeFromVelox->kind());
  const auto& row = projectedNimbleTypeFromVelox->asRow();
  ASSERT_EQ(2, row.childrenCount());
  EXPECT_EQ("col_a", row.nameAt(0));
  EXPECT_EQ("col_c", row.nameAt(1));

  // Cross-validate with nimble-type overload.
  auto convertedNimbleType = convertToNimbleType(*veloxType);
  std::vector<uint32_t> projectedStreamOffsets;
  auto projectedNimbleType = buildProjectedNimbleType(
      convertedNimbleType.get(), subfields, projectedStreamOffsets);
  expectSameType(*projectedNimbleTypeFromVelox, *projectedNimbleType, "root");
}

TEST(SchemaUtilsTest, projectionDeep) {
  auto veloxType = velox::ROW(
      {"map_col"},
      {velox::MAP(
          velox::VARCHAR(), velox::ROW({"inner"}, {velox::INTEGER()}))});

  // "map_col["key"].inner" — depth 3, projects FlatMap key "key" with only
  // the "inner" field of the value struct.
  std::vector<std::unique_ptr<velox::common::Subfield::PathElement>> path;
  path.push_back(std::make_unique<Subfield::NestedField>("map_col"));
  path.push_back(std::make_unique<Subfield::StringSubscript>("key"));
  path.push_back(std::make_unique<Subfield::NestedField>("inner"));

  std::vector<Subfield> subfields;
  subfields.emplace_back(std::move(path));

  ColumnEncodings encodings;
  encodings.flatMapColumns.insert("map_col");

  auto projectedNimbleTypeFromVelox =
      buildProjectedNimbleType(*veloxType, subfields, encodings);
  ASSERT_EQ(projectedNimbleTypeFromVelox->kind(), Kind::Row);
  const auto& root = projectedNimbleTypeFromVelox->asRow();
  ASSERT_EQ(root.childrenCount(), 1);
  EXPECT_EQ(root.nameAt(0), "map_col");

  // map_col should be a FlatMap with one key "key".
  ASSERT_EQ(root.childAt(0)->kind(), Kind::FlatMap);
  const auto& flatMap = root.childAt(0)->asFlatMap();
  ASSERT_EQ(flatMap.childrenCount(), 1);
  EXPECT_EQ(flatMap.nameAt(0), "key");

  // The value type should be a Row with only the "inner" field.
  ASSERT_EQ(flatMap.childAt(0)->kind(), Kind::Row);
  const auto& valueRow = flatMap.childAt(0)->asRow();
  ASSERT_EQ(valueRow.childrenCount(), 1);
  EXPECT_EQ(valueRow.nameAt(0), "inner");
  EXPECT_EQ(valueRow.childAt(0)->kind(), Kind::Scalar);

  // Cross-validate with nimble-type overload.
  SchemaBuilder schemaBuilder;
  test::FlatMapChildAdder childAdder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW({
          {"map_col",
           NIMBLE_FLATMAP(
               String, NIMBLE_ROW({{"inner", NIMBLE_INTEGER()}}), childAdder)},
      }));
  childAdder.addChild("key");
  auto convertedNimbleType =
      SchemaReader::getSchema(schemaBuilder.schemaNodes());

  // Rebuild subfields for nimble-type overload (path was moved above).
  std::vector<std::unique_ptr<velox::common::Subfield::PathElement>> path2;
  path2.push_back(std::make_unique<Subfield::NestedField>("map_col"));
  path2.push_back(std::make_unique<Subfield::StringSubscript>("key"));
  path2.push_back(std::make_unique<Subfield::NestedField>("inner"));
  std::vector<Subfield> subfields2;
  subfields2.emplace_back(std::move(path2));

  std::vector<uint32_t> projectedStreamOffsets;
  auto projectedNimbleType = buildProjectedNimbleType(
      convertedNimbleType.get(), subfields2, projectedStreamOffsets);
  expectSameType(*projectedNimbleTypeFromVelox, *projectedNimbleType, "root");
}

TEST(SchemaUtilsTest, projectionDeepWithStreamOffsets) {
  // Build nimble schema: root Row with a FlatMap column whose values are
  // Row(inner_a: INT, inner_b: STRING).
  //
  // Schema tree (stream offsets assigned by SchemaBuilder in DFS order):
  //   Row nulls [0]
  //     FlatMap nulls [1]
  //       key1: Row nulls [2]
  //         inner_a: INT [3]
  //         inner_b: STRING [4]
  //       key1 inMap: BOOL [5]
  //       key2: Row nulls [6]
  //         inner_a: INT [7]
  //         inner_b: STRING [8]
  //       key2 inMap: BOOL [9]
  SchemaBuilder schemaBuilder;
  test::FlatMapChildAdder childAdder;
  NIMBLE_SCHEMA(
      schemaBuilder,
      NIMBLE_ROW({
          {"map_col",
           NIMBLE_FLATMAP(
               String,
               NIMBLE_ROW({
                   {"inner_a", NIMBLE_INTEGER()},
                   {"inner_b", NIMBLE_STRING()},
               }),
               childAdder)},
      }));
  childAdder.addChild("key1");
  childAdder.addChild("key2");
  auto convertedNimbleType =
      SchemaReader::getSchema(schemaBuilder.schemaNodes());

  // Project "map_col["key1"].inner_b" — depth 3: FlatMap key + struct field.
  std::vector<std::unique_ptr<velox::common::Subfield::PathElement>> path;
  path.push_back(std::make_unique<Subfield::NestedField>("map_col"));
  path.push_back(std::make_unique<Subfield::StringSubscript>("key1"));
  path.push_back(std::make_unique<Subfield::NestedField>("inner_b"));

  std::vector<Subfield> subfields;
  subfields.emplace_back(std::move(path));

  std::vector<uint32_t> projectedStreamOffsets;
  auto projectedNimbleType = buildProjectedNimbleType(
      convertedNimbleType.get(), subfields, projectedStreamOffsets);

  // Verify projected schema structure.
  ASSERT_EQ(projectedNimbleType->kind(), Kind::Row);
  const auto& root = projectedNimbleType->asRow();
  ASSERT_EQ(root.childrenCount(), 1);
  EXPECT_EQ(root.nameAt(0), "map_col");

  ASSERT_EQ(root.childAt(0)->kind(), Kind::FlatMap);
  const auto& flatMap = root.childAt(0)->asFlatMap();
  ASSERT_EQ(flatMap.childrenCount(), 1);
  EXPECT_EQ(flatMap.nameAt(0), "key1");

  // Value should be a Row with only "inner_b" (not "inner_a").
  ASSERT_EQ(flatMap.childAt(0)->kind(), Kind::Row);
  const auto& valueRow = flatMap.childAt(0)->asRow();
  ASSERT_EQ(valueRow.childrenCount(), 1);
  EXPECT_EQ(valueRow.nameAt(0), "inner_b");
  EXPECT_EQ(valueRow.childAt(0)->kind(), Kind::Scalar);

  // Verify projected stream offsets contain the expected input offsets.
  // The exact offset assignment depends on SchemaBuilder's DFS traversal order.
  EXPECT_EQ(projectedStreamOffsets, std::vector<uint32_t>({1, 0, 4, 3, 5}));
}

TEST(SchemaUtilsTest, projectionEmptySubfieldsFails) {
  auto type = velox::ROW({{"a", velox::BIGINT()}, {"b", velox::VARCHAR()}});
  std::vector<Subfield> subfields;
  NIMBLE_ASSERT_THROW(
      buildProjectedNimbleType(type->asRow(), subfields, {}),
      "projectedSubfields must not be empty");

  // Cross-validate with nimble-type overload.
  auto convertedNimbleType = convertToNimbleType(*type);
  std::vector<uint32_t> offsets;
  NIMBLE_ASSERT_THROW(
      buildProjectedNimbleType(convertedNimbleType.get(), subfields, offsets),
      "projectedSubfields must not be empty");
}

namespace {

// Encoding choice for a top-level column.
enum class ColumnEncoding {
  None,
  DictionaryArray, // ARRAY → ArrayWithOffsets
  DeduplicatedMap, // MAP → SlidingWindowMap
  FlatMap, // MAP → FlatMap
};

// Describes a randomly generated top-level column.
struct ColumnDesc {
  std::string name;
  velox::TypePtr veloxType;
  ColumnEncoding encoding{ColumnEncoding::None};

  std::string debugString() const {
    return fmt::format(
        "name={}, veloxType={}, encoding={}",
        name,
        veloxType->toString(),
        static_cast<int>(encoding));
  }
};

velox::TypePtr randomScalarType(folly::Random::DefaultGenerator& rng) {
  static const std::vector<velox::TypePtr> scalars = {
      velox::BOOLEAN(),
      velox::TINYINT(),
      velox::SMALLINT(),
      velox::INTEGER(),
      velox::BIGINT(),
      velox::REAL(),
      velox::DOUBLE(),
      velox::VARCHAR(),
      velox::VARBINARY(),
      velox::TIMESTAMP(),
  };
  return scalars[folly::Random::rand32(scalars.size(), rng)];
}

velox::TypePtr randomLeafType(folly::Random::DefaultGenerator& rng) {
  return randomScalarType(rng);
}

velox::TypePtr randomValueType(
    folly::Random::DefaultGenerator& rng,
    int depth) {
  if (depth <= 0) {
    return randomLeafType(rng);
  }
  auto choice = folly::Random::rand32(4, rng);
  switch (choice) {
    case 0:
      return randomLeafType(rng);
    case 1:
      return velox::ARRAY(randomValueType(rng, depth - 1));
    case 2:
      return velox::MAP(randomScalarType(rng), randomValueType(rng, depth - 1));
    case 3: {
      auto numFields = 1 + folly::Random::rand32(3, rng);
      std::vector<std::string> names;
      std::vector<velox::TypePtr> types;
      for (uint32_t i = 0; i < numFields; ++i) {
        names.push_back(fmt::format("f{}", i));
        types.push_back(randomValueType(rng, depth - 1));
      }
      return velox::ROW(std::move(names), std::move(types));
    }
    default:
      return randomLeafType(rng);
  }
}

// Key type for FlatMap must be a scalar kind convertible to ScalarKind.
velox::TypePtr randomMapKeyType(folly::Random::DefaultGenerator& rng) {
  static const std::vector<velox::TypePtr> keyTypes = {
      velox::TINYINT(),
      velox::SMALLINT(),
      velox::INTEGER(),
      velox::BIGINT(),
      velox::VARCHAR(),
  };
  return keyTypes[folly::Random::rand32(keyTypes.size(), rng)];
}

ColumnDesc randomColumn(
    const std::string& name,
    folly::Random::DefaultGenerator& rng) {
  ColumnDesc col;
  col.name = name;

  auto kind = folly::Random::rand32(5, rng);
  switch (kind) {
    case 0:
      // Scalar column — no special encoding.
      col.veloxType = randomScalarType(rng);
      col.encoding = ColumnEncoding::None;
      break;
    case 1: {
      // ARRAY column — may use DictionaryArray encoding.
      col.veloxType = velox::ARRAY(randomValueType(rng, 1));
      col.encoding = folly::Random::oneIn(2, rng)
          ? ColumnEncoding::DictionaryArray
          : ColumnEncoding::None;
      break;
    }
    case 2: {
      // MAP column — may use DeduplicatedMap encoding.
      col.veloxType =
          velox::MAP(randomMapKeyType(rng), randomValueType(rng, 1));
      col.encoding = folly::Random::oneIn(2, rng)
          ? ColumnEncoding::DeduplicatedMap
          : ColumnEncoding::None;
      break;
    }
    case 3: {
      // MAP column with FlatMap encoding.
      col.veloxType =
          velox::MAP(randomMapKeyType(rng), randomValueType(rng, 1));
      col.encoding = ColumnEncoding::FlatMap;
      break;
    }
    case 4: {
      // ROW column — no special encoding at non-top-level.
      auto numFields = 1 + folly::Random::rand32(3, rng);
      std::vector<std::string> names;
      std::vector<velox::TypePtr> types;
      for (uint32_t i = 0; i < numFields; ++i) {
        names.push_back(fmt::format("sub{}", i));
        types.push_back(randomValueType(rng, 1));
      }
      col.veloxType = velox::ROW(std::move(names), std::move(types));
      col.encoding = ColumnEncoding::None;
      break;
    }
    default:
      col.veloxType = randomScalarType(rng);
      col.encoding = ColumnEncoding::None;
      break;
  }
  return col;
}

// Generates random subfield paths for the given columns.
// For FlatMap columns, generates key subscript paths.
// For ROW columns, may generate nested field paths.
// For other columns, generates top-level paths.
std::vector<Subfield> randomSubfields(
    const std::vector<ColumnDesc>& columns,
    folly::Random::DefaultGenerator& rng) {
  std::vector<Subfield> subfields;
  for (const auto& col : columns) {
    // Each column is projected with 80% probability.
    if (folly::Random::rand32(5, rng) == 0) {
      continue;
    }

    if (col.encoding == ColumnEncoding::FlatMap) {
      // Must project with key subscripts.
      auto numKeys = 1 + folly::Random::rand32(3, rng);
      for (uint32_t k = 0; k < numKeys; ++k) {
        auto keyStr = fmt::format("{}", k + 1);
        std::vector<std::unique_ptr<velox::common::Subfield::PathElement>> path;
        path.push_back(std::make_unique<Subfield::NestedField>(col.name));

        // Use appropriate subscript type based on key type.
        auto keyKind = col.veloxType->asMap().keyType()->kind();
        if (keyKind == velox::TypeKind::VARCHAR) {
          path.push_back(std::make_unique<Subfield::StringSubscript>(keyStr));
        } else {
          path.push_back(std::make_unique<Subfield::LongSubscript>(k + 1));
        }
        subfields.emplace_back(std::move(path));
      }
    } else if (
        col.encoding == ColumnEncoding::None &&
        col.veloxType->kind() == velox::TypeKind::ROW) {
      // For ROW columns, project some nested fields.
      const auto& rowType = col.veloxType->asRow();
      bool projectedAny = false;
      for (size_t i = 0; i < rowType.size(); ++i) {
        if (folly::Random::oneIn(2, rng)) {
          std::vector<std::unique_ptr<velox::common::Subfield::PathElement>>
              path;
          path.push_back(std::make_unique<Subfield::NestedField>(col.name));
          path.push_back(
              std::make_unique<Subfield::NestedField>(rowType.nameOf(i)));
          subfields.emplace_back(std::move(path));
          projectedAny = true;
        }
      }
      if (!projectedAny) {
        // Project all fields.
        subfields.emplace_back(col.name);
      }
    } else {
      subfields.emplace_back(col.name);
    }
  }
  return subfields;
}

ScalarKind toNimbleScalarKind(velox::TypeKind kind) {
  switch (kind) {
    case velox::TypeKind::TINYINT:
      return ScalarKind::Int8;
    case velox::TypeKind::SMALLINT:
      return ScalarKind::Int16;
    case velox::TypeKind::INTEGER:
      return ScalarKind::Int32;
    case velox::TypeKind::BIGINT:
      return ScalarKind::Int64;
    case velox::TypeKind::VARCHAR:
      return ScalarKind::String;
    default:
      NIMBLE_UNREACHABLE("Unsupported key type: {}", static_cast<int>(kind));
  }
}

// Builds the nimble schema corresponding to velox type + encodings.
// For top-level children, applies encoding-specific types.
std::shared_ptr<nimble::TypeBuilder> buildNimbleTypeFromVelox(
    nimble::SchemaBuilder& builder,
    const velox::Type& type) {
  switch (type.kind()) {
    case velox::TypeKind::BOOLEAN:
      return builder.createScalarTypeBuilder(ScalarKind::Bool);
    case velox::TypeKind::TINYINT:
      return builder.createScalarTypeBuilder(ScalarKind::Int8);
    case velox::TypeKind::SMALLINT:
      return builder.createScalarTypeBuilder(ScalarKind::Int16);
    case velox::TypeKind::INTEGER:
      return builder.createScalarTypeBuilder(ScalarKind::Int32);
    case velox::TypeKind::BIGINT:
      return builder.createScalarTypeBuilder(ScalarKind::Int64);
    case velox::TypeKind::REAL:
      return builder.createScalarTypeBuilder(ScalarKind::Float);
    case velox::TypeKind::DOUBLE:
      return builder.createScalarTypeBuilder(ScalarKind::Double);
    case velox::TypeKind::VARCHAR:
      return builder.createScalarTypeBuilder(ScalarKind::String);
    case velox::TypeKind::VARBINARY:
      return builder.createScalarTypeBuilder(ScalarKind::Binary);
    case velox::TypeKind::TIMESTAMP:
      return builder.createTimestampMicroNanoTypeBuilder();
    case velox::TypeKind::ARRAY: {
      auto arr = builder.createArrayTypeBuilder();
      arr->setChildren(
          buildNimbleTypeFromVelox(builder, *type.asArray().elementType()));
      return arr;
    }
    case velox::TypeKind::MAP: {
      auto m = builder.createMapTypeBuilder();
      auto k = buildNimbleTypeFromVelox(builder, *type.asMap().keyType());
      auto v = buildNimbleTypeFromVelox(builder, *type.asMap().valueType());
      m->setChildren(k, v);
      return m;
    }
    case velox::TypeKind::ROW: {
      auto r = builder.createRowTypeBuilder(type.asRow().size());
      for (size_t i = 0; i < type.asRow().size(); ++i) {
        r->addChild(
            type.asRow().nameOf(i),
            buildNimbleTypeFromVelox(builder, *type.asRow().childAt(i)));
      }
      return r;
    }
    default:
      NIMBLE_UNREACHABLE(
          "Unsupported type kind: {}", static_cast<int>(type.kind()));
  }
}

// Builds nimble schema for a top-level child with encoding applied.
std::shared_ptr<nimble::TypeBuilder> buildNimbleChildType(
    nimble::SchemaBuilder& builder,
    const velox::Type& type,
    ColumnEncoding encoding,
    const std::vector<std::string>& flatMapKeys) {
  switch (encoding) {
    case ColumnEncoding::DictionaryArray: {
      auto arr = builder.createArrayWithOffsetsTypeBuilder();
      arr->setChildren(
          buildNimbleTypeFromVelox(builder, *type.asArray().elementType()));
      return arr;
    }
    case ColumnEncoding::DeduplicatedMap: {
      auto m = builder.createSlidingWindowMapTypeBuilder();
      auto k = buildNimbleTypeFromVelox(builder, *type.asMap().keyType());
      auto v = buildNimbleTypeFromVelox(builder, *type.asMap().valueType());
      m->setChildren(k, v);
      return m;
    }
    case ColumnEncoding::FlatMap: {
      auto keyScalarKind = toNimbleScalarKind(type.asMap().keyType()->kind());
      auto fm = builder.createFlatMapTypeBuilder(keyScalarKind);
      for (const auto& key : flatMapKeys) {
        fm->addChild(
            key, buildNimbleTypeFromVelox(builder, *type.asMap().valueType()));
      }
      return fm;
    }
    case ColumnEncoding::None:
      return buildNimbleTypeFromVelox(builder, type);
  }
  NIMBLE_UNREACHABLE("Invalid encoding");
}

} // namespace

TEST(SchemaUtilsTest, projectionFuzzer) {
  constexpr int kIterations = 100;
  auto seed = folly::Random::rand32();
  LOG(INFO) << "projectionFuzzer seed: " << seed;

  for (int iter = 0; iter < kIterations; ++iter) {
    folly::Random::DefaultGenerator rng(seed + iter);

    // Generate 2-6 random top-level columns.
    auto numColumns = 2 + folly::Random::rand32(5, rng);
    std::vector<ColumnDesc> columns;
    for (uint32_t i = 0; i < numColumns; ++i) {
      columns.push_back(randomColumn(fmt::format("col{}", i), rng));
    }

    // Build velox RowType.
    std::vector<std::string> names;
    std::vector<velox::TypePtr> types;
    for (const auto& col : columns) {
      names.push_back(col.name);
      types.push_back(col.veloxType);
    }
    auto veloxType = velox::ROW(std::move(names), std::move(types));

    // Generate random subfields.
    auto subfields = randomSubfields(columns, rng);
    if (subfields.empty()) {
      // Need at least one subfield.
      subfields.emplace_back(columns[0].name);
      if (columns[0].encoding == ColumnEncoding::FlatMap) {
        // Fix: need key subscript for FlatMap.
        subfields.clear();
        auto keyKind = columns[0].veloxType->asMap().keyType()->kind();
        std::vector<std::unique_ptr<velox::common::Subfield::PathElement>> path;
        path.push_back(
            std::make_unique<Subfield::NestedField>(columns[0].name));
        if (keyKind == velox::TypeKind::VARCHAR) {
          path.push_back(std::make_unique<Subfield::StringSubscript>("1"));
        } else {
          path.push_back(std::make_unique<Subfield::LongSubscript>(1));
        }
        subfields.emplace_back(std::move(path));
      }
    }

    // Build ColumnEncodings.
    ColumnEncodings encodings;
    for (const auto& col : columns) {
      switch (col.encoding) {
        case ColumnEncoding::DictionaryArray:
          encodings.dictionaryArrayColumns.insert(col.name);
          break;
        case ColumnEncoding::DeduplicatedMap:
          encodings.deduplicatedMapColumns.insert(col.name);
          break;
        case ColumnEncoding::FlatMap:
          encodings.flatMapColumns.insert(col.name);
          break;
        case ColumnEncoding::None:
          break;
      }
    }

    // Call velox-type overload.
    auto projectedNimbleTypeFromVelox =
        buildProjectedNimbleType(*veloxType, subfields, encodings);
    ASSERT_NE(projectedNimbleTypeFromVelox, nullptr) << "iter=" << iter;

    // Determine which FlatMap keys were projected per column.
    std::map<std::string, std::set<std::string>> flatMapKeysPerColumn;
    for (const auto& sf : subfields) {
      const auto& path = sf.path();
      if (path.size() >= 2) {
        auto* nested =
            dynamic_cast<const Subfield::NestedField*>(path[0].get());
        if (nested != nullptr) {
          auto* strSub =
              dynamic_cast<const Subfield::StringSubscript*>(path[1].get());
          auto* longSub =
              dynamic_cast<const Subfield::LongSubscript*>(path[1].get());
          if (strSub != nullptr) {
            flatMapKeysPerColumn[nested->name()].insert(strSub->index());
          } else if (longSub != nullptr) {
            flatMapKeysPerColumn[nested->name()].insert(
                std::to_string(longSub->index()));
          }
        }
      }
    }

    // Build equivalent nimble input schema with encoding-specific types.
    SchemaBuilder schemaBuilder;
    auto nimbleRoot = schemaBuilder.createRowTypeBuilder(columns.size());
    for (const auto& col : columns) {
      std::vector<std::string> keys;
      if (col.encoding == ColumnEncoding::FlatMap) {
        auto it = flatMapKeysPerColumn.find(col.name);
        if (it != flatMapKeysPerColumn.end()) {
          keys.assign(it->second.begin(), it->second.end());
        } else {
          keys.push_back("1");
        }
      }
      nimbleRoot->addChild(
          col.name,
          buildNimbleChildType(
              schemaBuilder, *col.veloxType, col.encoding, keys));
    }
    auto convertedNimbleType =
        SchemaReader::getSchema(schemaBuilder.schemaNodes());

    // Call nimble-type overload.
    std::vector<uint32_t> projectedStreamOffsets;
    auto projectedNimbleType = buildProjectedNimbleType(
        convertedNimbleType.get(), subfields, projectedStreamOffsets);
    ASSERT_NE(projectedNimbleType, nullptr) << "iter=" << iter;

    // Cross-validate: both should produce structurally equivalent types.
    expectSameType(
        *projectedNimbleTypeFromVelox,
        *projectedNimbleType,
        fmt::format("iter={}", iter));
  }
}
