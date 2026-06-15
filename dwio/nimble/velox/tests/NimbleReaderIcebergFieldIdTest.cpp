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

#include "dwio/nimble/velox/SchemaReader.h"
#include "dwio/nimble/velox/SchemaTypes.h"
#include "dwio/nimble/velox/reader/fb/IcebergFieldIdResolver.h"
#include "velox/dwio/common/ParquetFieldId.h"
#include "velox/type/Type.h"

// ---------------------------------------------------------------------------
// Iceberg V3 field-id resolution for NIMBLE-backed Iceberg files.
//
// `IcebergFieldIdResolver::rewriteSchemaByIcebergFieldId` walks the file's
// `facebook::nimble::Type` tree in lockstep with the requested Velox schema
// and the per-column Iceberg field-id tree, producing a rewritten schema
// that uses the file's on-disk names at every nested-Row level so the
// downstream name-based selector resolves projected columns even after
// Iceberg column renames. These tests exercise the recursive walk across
// top-level columns, nested struct fields, array element fields, and map
// key/value fields, plus the structural-mismatch fallback that returns
// std::nullopt and lets the caller fall back to the legacy name-based
// path.
// ---------------------------------------------------------------------------
namespace facebook::velox::nimble {
namespace {

using facebook::nimble::ArrayType;
using facebook::nimble::Kind;
using facebook::nimble::MapType;
using facebook::nimble::RowType;
using facebook::nimble::ScalarKind;
using facebook::nimble::ScalarType;
using facebook::nimble::StreamDescriptor;
using facebook::nimble::Type;

using Attrs = std::vector<std::pair<std::string, std::string>>;

// Builds a unique stream descriptor each call. The offset/scalarKind are
// not exercised by the resolver -- only `attributes()` and the structural
// shape are.
StreamDescriptor descriptor(ScalarKind kind = ScalarKind::Int64) {
  static facebook::nimble::offset_size next{0};
  return StreamDescriptor{next++, kind};
}

// Helper that stamps the `iceberg.id` attribute on a NIMBLE Type. Mirrors
// what `IcebergDataSink` stamps onto the writer side.
Attrs icebergId(int32_t id) {
  return {{"iceberg.id", std::to_string(id)}};
}

// Anonymous-namespace synonyms for nimble Type subclasses, so the test
// fixtures read close to the prod schema construction at
// `facebook::nimble::SchemaReader::getType`.
std::shared_ptr<const Type> scalar(int32_t icebergFieldId) {
  return std::make_shared<ScalarType>(descriptor(), icebergId(icebergFieldId));
}

std::shared_ptr<const Type> scalarWithoutId() {
  return std::make_shared<ScalarType>(descriptor(), Attrs{});
}

std::shared_ptr<const Type> row(
    int32_t icebergFieldId,
    std::vector<std::string> names,
    std::vector<std::shared_ptr<const Type>> children) {
  return std::make_shared<RowType>(
      descriptor(),
      std::move(names),
      std::move(children),
      icebergId(icebergFieldId));
}

std::shared_ptr<const Type> rowWithoutId(
    std::vector<std::string> names,
    std::vector<std::shared_ptr<const Type>> children) {
  return std::make_shared<RowType>(
      descriptor(), std::move(names), std::move(children), Attrs{});
}

std::shared_ptr<const Type> array(
    int32_t icebergFieldId,
    std::shared_ptr<const Type> elements) {
  return std::make_shared<ArrayType>(
      descriptor(), std::move(elements), icebergId(icebergFieldId));
}

std::shared_ptr<const Type> map(
    int32_t icebergFieldId,
    std::shared_ptr<const Type> keys,
    std::shared_ptr<const Type> values) {
  return std::make_shared<MapType>(
      descriptor(),
      std::move(keys),
      std::move(values),
      icebergId(icebergFieldId));
}

// Top-level file Row -- the schema-root analog of what
// `VeloxReader::schema()` returns. Carries no `iceberg.id` attribute at
// the root (matches the writer side, which only stamps attributes on
// columns, not the synthetic schema-root Row).
std::shared_ptr<const Type> fileSchema(
    std::vector<std::string> names,
    std::vector<std::shared_ptr<const Type>> children) {
  return rowWithoutId(std::move(names), std::move(children));
}

dwio::common::ParquetFieldId pf(
    int32_t fieldId,
    std::vector<dwio::common::ParquetFieldId> children = {}) {
  return dwio::common::ParquetFieldId{
      .fieldId = fieldId, .children = std::move(children)};
}

class NimbleReaderIcebergFieldIdTest : public ::testing::Test {};

// Top-level rename. File has columns ("renamed_a", "b") with ids 100,
// 200. Caller requests ("a", "b") in original order with ids 100, 200.
// Rewritten schema is ("renamed_a", "b") so the downstream name-based
// selector can find each column in the file by its on-disk name.
TEST_F(NimbleReaderIcebergFieldIdTest, topLevelRenameIsRegressionStable) {
  auto file = fileSchema(
      {"renamed_a", "b"}, {scalar(/*icebergFieldId=*/100), scalar(200)});
  auto requested = ROW({"a", "b"}, {BIGINT(), BIGINT()});
  auto rewritten = rewriteSchemaByIcebergFieldId(
      requested, {pf(/*fieldId=*/100), pf(200)}, *file);
  ASSERT_TRUE(rewritten.has_value());
  ASSERT_EQ(rewritten.value()->size(), 2);
  EXPECT_EQ(rewritten.value()->nameOf(0), "renamed_a");
  EXPECT_EQ(rewritten.value()->nameOf(1), "b");
}

// Nested struct rename. File has user{renamed_name VARCHAR, age INT}
// where `name` was renamed to `renamed_name`. Caller requests
// user{name VARCHAR, age INT}. The rewrite must descend into the nested
// Row and pick up the file's child name `renamed_name`.
TEST_F(NimbleReaderIcebergFieldIdTest, nestedStructFieldRename) {
  auto file = fileSchema(
      {"user"},
      {row(
          /*icebergFieldId=*/100,
          {"renamed_name", "age"},
          {scalar(101), scalar(102)})});
  auto requested =
      ROW({"user"}, {ROW({"name", "age"}, {VARCHAR(), INTEGER()})});
  auto rewritten = rewriteSchemaByIcebergFieldId(
      requested, {pf(100, {pf(101), pf(102)})}, *file);
  ASSERT_TRUE(rewritten.has_value());
  ASSERT_EQ(rewritten.value()->size(), 1);
  EXPECT_EQ(rewritten.value()->nameOf(0), "user");
  const auto& userType = rewritten.value()->childAt(0)->asRow();
  ASSERT_EQ(userType.size(), 2);
  EXPECT_EQ(userType.nameOf(0), "renamed_name");
  EXPECT_EQ(userType.nameOf(1), "age");
}

// ARRAY of ROW where the element struct's field was renamed. File has
// items: array<{renamed_label VARCHAR, count INT}>. Caller requests
// items: array<{label VARCHAR, count INT}>. The walk recurses through
// the array's element into the nested Row and rewrites the child name.
TEST_F(NimbleReaderIcebergFieldIdTest, arrayOfRowWithRenamedElementField) {
  auto elementInFile = row(/*icebergFieldId=*/101,
                           {"renamed_label", "count"},
                           {scalar(102), scalar(103)});
  auto file = fileSchema(
      {"items"}, {array(/*icebergFieldId=*/100, std::move(elementInFile))});
  auto requested =
      ROW({"items"}, {ARRAY(ROW({"label", "count"}, {VARCHAR(), INTEGER()}))});
  auto rewritten = rewriteSchemaByIcebergFieldId(
      requested, {pf(100, {pf(101, {pf(102), pf(103)})})}, *file);
  ASSERT_TRUE(rewritten.has_value());
  const auto& items = rewritten.value()->childAt(0);
  ASSERT_EQ(items->kind(), TypeKind::ARRAY);
  const auto& element = items->asArray().elementType()->asRow();
  ASSERT_EQ(element.size(), 2);
  EXPECT_EQ(element.nameOf(0), "renamed_label");
  EXPECT_EQ(element.nameOf(1), "count");
}

// MAP<INT, ROW> where the value struct's field was renamed. File has
// scores: map<int, {renamed_score DOUBLE, units VARCHAR}>. Caller
// requests map<int, {score DOUBLE, units VARCHAR}>. The walk recurses
// through both the map's key (scalar) and value (Row).
TEST_F(NimbleReaderIcebergFieldIdTest, mapIntToRowWithRenamedValueField) {
  auto valueInFile = row(/*icebergFieldId=*/102,
                         {"renamed_score", "units"},
                         {scalar(103), scalar(104)});
  auto file = fileSchema(
      {"scores"},
      {map(/*icebergFieldId=*/100,
           scalar(/*key icebergFieldId=*/101),
           std::move(valueInFile))});
  auto requested =
      ROW({"scores"},
          {MAP(INTEGER(), ROW({"score", "units"}, {DOUBLE(), VARCHAR()}))});
  auto rewritten = rewriteSchemaByIcebergFieldId(
      requested, {pf(100, {pf(101), pf(102, {pf(103), pf(104)})})}, *file);
  ASSERT_TRUE(rewritten.has_value());
  const auto& scores = rewritten.value()->childAt(0);
  ASSERT_EQ(scores->kind(), TypeKind::MAP);
  const auto& valueRow = scores->asMap().valueType()->asRow();
  ASSERT_EQ(valueRow.size(), 2);
  EXPECT_EQ(valueRow.nameOf(0), "renamed_score");
  EXPECT_EQ(valueRow.nameOf(1), "units");
}

// Two-level nesting: array<struct<inner_array<scalar>>>. Verifies the
// recursion descends past two intervening complex types.
TEST_F(NimbleReaderIcebergFieldIdTest, twoLevelArrayOfRowOfArray) {
  auto innermost = scalar(/*icebergFieldId=*/104);
  auto innerArray = array(/*icebergFieldId=*/103, std::move(innermost));
  auto innerRow =
      row(/*icebergFieldId=*/102, {"renamed_inner"}, {std::move(innerArray)});
  auto outerArray = array(/*icebergFieldId=*/101, std::move(innerRow));
  auto file = fileSchema({"outer"}, {std::move(outerArray)});
  auto requested = ROW({"outer"}, {ARRAY(ROW({"inner"}, {ARRAY(BIGINT())}))});
  auto rewritten = rewriteSchemaByIcebergFieldId(
      requested, {pf(101, {pf(102, {pf(103, {pf(104)})})})}, *file);
  ASSERT_TRUE(rewritten.has_value());
  const auto& outer = rewritten.value()->childAt(0)->asArray();
  const auto& innerRowType = outer.elementType()->asRow();
  ASSERT_EQ(innerRowType.size(), 1);
  EXPECT_EQ(innerRowType.nameOf(0), "renamed_inner");
  EXPECT_EQ(innerRowType.childAt(0)->kind(), TypeKind::ARRAY);
}

// Kind mismatch: file column is a scalar but caller requests it as a
// ROW. The walk must bail with std::nullopt so the caller falls back to
// the legacy name-based path.
TEST_F(NimbleReaderIcebergFieldIdTest, kindMismatchReturnsNullopt) {
  auto file = fileSchema({"renamed_a"}, {scalar(/*icebergFieldId=*/100)});
  auto requested = ROW({"a"}, {ROW({"x"}, {BIGINT()})});
  auto rewritten =
      rewriteSchemaByIcebergFieldId(requested, {pf(100, {pf(101)})}, *file);
  EXPECT_FALSE(rewritten.has_value());
}

// Missing `iceberg.id` at top level: file column has no attribute, so
// the file index can't find it for the requested id, and the walk bails
// to nullopt.
TEST_F(NimbleReaderIcebergFieldIdTest, missingIcebergIdReturnsNullopt) {
  auto file = fileSchema({"a"}, {scalarWithoutId()});
  auto requested = ROW({"a"}, {BIGINT()});
  auto rewritten =
      rewriteSchemaByIcebergFieldId(requested, {pf(/*fieldId=*/100)}, *file);
  EXPECT_FALSE(rewritten.has_value());
}

// Missing `iceberg.id` on a nested struct child: the nested walk
// recurses into the file Row, fails to find a child with the requested
// id, and propagates std::nullopt up.
TEST_F(NimbleReaderIcebergFieldIdTest, missingNestedIcebergIdReturnsNullopt) {
  auto file = fileSchema(
      {"user"},
      {row(/*icebergFieldId=*/100,
           {"name", "age"},
           {scalarWithoutId(), scalar(102)})});
  auto requested =
      ROW({"user"}, {ROW({"name", "age"}, {VARCHAR(), INTEGER()})});
  auto rewritten = rewriteSchemaByIcebergFieldId(
      requested, {pf(100, {pf(101), pf(102)})}, *file);
  EXPECT_FALSE(rewritten.has_value());
}

// ParquetFieldId children count mismatch at MAP level: a MAP must have
// exactly two children in [key, value] order. Anything else bails.
TEST_F(NimbleReaderIcebergFieldIdTest, mapFieldIdChildrenCountMismatch) {
  auto file = fileSchema(
      {"scores"}, {map(/*icebergFieldId=*/100, scalar(101), scalar(102))});
  auto requested = ROW({"scores"}, {MAP(INTEGER(), BIGINT())});
  // Only 1 child instead of [key, value]. Walk bails.
  auto rewritten =
      rewriteSchemaByIcebergFieldId(requested, {pf(100, {pf(101)})}, *file);
  EXPECT_FALSE(rewritten.has_value());
}

// Requested schema / fieldId count mismatch at the top level. Top-level
// is wrapped in a synthesized ParquetFieldId, so the size check happens
// inside the Row dispatch arm against `requestedRow.size()`.
TEST_F(NimbleReaderIcebergFieldIdTest, topLevelChildrenCountMismatch) {
  auto file = fileSchema({"a", "b"}, {scalar(100), scalar(200)});
  auto requested = ROW({"a", "b"}, {BIGINT(), BIGINT()});
  // Only one fieldId supplied for two requested columns.
  auto rewritten =
      rewriteSchemaByIcebergFieldId(requested, {pf(/*fieldId=*/100)}, *file);
  EXPECT_FALSE(rewritten.has_value());
}

} // namespace
} // namespace facebook::velox::nimble
