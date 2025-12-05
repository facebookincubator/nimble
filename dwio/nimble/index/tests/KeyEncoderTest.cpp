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
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <algorithm>

#include "velox/core/PlanNode.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include "dwio/nimble/index/KeyEncoder.h"

namespace facebook::nimble::test {
namespace {

int lexicographicalCompare(const std::string& key1, const std::string& key2) {
  const auto begin1 = reinterpret_cast<const unsigned char*>(key1.data());
  const auto end1 = begin1 + key1.size();
  const auto begin2 = reinterpret_cast<const unsigned char*>(key2.data());
  const auto end2 = begin2 + key2.size();
  bool lessThan = std::lexicographical_compare(begin1, end1, begin2, end2);

  bool equal = std::equal(begin1, end1, begin2, end2);

  return lessThan ? -1 : (equal ? 0 : 1);
}

class KeyEncoderTest : public ::testing::Test,
                       public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance(
        velox::memory::MemoryManager::Options{});
  }

  int compareRowVector(
      const velox::RowVectorPtr& rowVector,
      const std::vector<
          std::shared_ptr<const velox::core::FieldAccessTypedExpr>>& fields,
      const std::vector<velox::core::SortOrder>& ordering) {
    NIMBLE_CHECK_EQ(rowVector->size(), 2);

    // Extract field names from the expressions
    std::vector<std::string> fieldNames;
    for (const auto& field : fields) {
      fieldNames.push_back(field->name());
    }

    // Create KeyEncoder
    auto keyEncoder = KeyEncoder::create(
        fieldNames, asRowType(rowVector->type()), ordering, pool_.get());

    // Encode the keys
    Buffer buffer(*pool_);
    Vector<std::string_view> encodedKeys(pool_.get());
    keyEncoder->encode(rowVector, encodedKeys, buffer);

    // Compare the two encoded keys
    return lexicographicalCompare(
        std::string(encodedKeys[0]), std::string(encodedKeys[1]));
  }

  template <typename T>
  int singlePrimitiveFieldCompare(
      const std::vector<std::optional<T>>& values,
      const velox::core::SortOrder& ordering) {
    NIMBLE_CHECK_EQ(values.size(), 2);
    auto c0 = vectorMaker_.flatVectorNullable<T>(values);
    auto rowVector = vectorMaker_.rowVector({c0});

    std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>
        fields;
    fields.push_back(
        std::make_shared<const velox::core::FieldAccessTypedExpr>(
            velox::CppToType<T>::create(), /*name=*/"c0"));
    return compareRowVector(rowVector, fields, {ordering});
  }

  template <typename T>
  int singleArrayFieldCompare(
      const std::vector<std::optional<std::vector<std::optional<T>>>>& values,
      const velox::core::SortOrder& ordering) {
    NIMBLE_CHECK_EQ(values.size(), 2);
    auto c0 = makeNullableArrayVector<T>(values);
    auto rowVector = vectorMaker_.rowVector({c0});

    std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>
        fields;
    fields.push_back(
        std::make_shared<velox::core::FieldAccessTypedExpr>(
            velox::CppToType<T>::create(), /*name=*/"c0"));
    return compareRowVector(rowVector, fields, {ordering});
  }

  template <typename T0, typename T1>
  int singleRowFieldCompare(
      const std::vector<std::optional<T0>>& col0,
      const std::vector<std::optional<T1>>& col1,
      const velox::core::SortOrder& ordering,
      const std::function<bool(velox::vector_size_t /* row */)>& isNullAt =
          nullptr) {
    NIMBLE_CHECK_EQ(col0.size(), 2);
    NIMBLE_CHECK_EQ(col1.size(), 2);
    auto c0 = vectorMaker_.flatVectorNullable<T0>(col0);
    auto c1 = vectorMaker_.flatVectorNullable<T1>(col1);
    auto rowField = vectorMaker_.rowVector({c0, c1});
    auto rowVector = vectorMaker_.rowVector({rowField});

    if (isNullAt) {
      rowVector->childAt(0)->setNull(0, isNullAt(0));
      rowVector->childAt(0)->setNull(1, isNullAt(1));
    }
    std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>
        fields;
    fields.push_back(
        std::make_shared<const velox::core::FieldAccessTypedExpr>(
            rowField->type(), /*name=*/"c0"));
    return compareRowVector(rowVector, fields, {ordering});
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_ =
      velox::memory::deprecatedAddDefaultLeafMemoryPool();
  velox::test::VectorMaker vectorMaker_{pool_.get()};
};
} // namespace

TEST_F(KeyEncoderTest, indexBounds) {
  const auto boundRow = makeRowVector({makeNullableFlatVector<int32_t>({2})});
  // Test valid bounds with lower bound
  IndexBounds boundsWithLower;
  boundsWithLower.indexColumns = {"c0"};
  boundsWithLower.lowerBound = IndexBound{boundRow, true};
  EXPECT_TRUE(boundsWithLower.validate());
  EXPECT_EQ(
      boundsWithLower.toString(),
      "IndexBounds{indexColumns=[c0], lowerBound=[...]}");

  // Test valid bounds with upper bound
  IndexBounds boundsWithUpper;
  boundsWithUpper.indexColumns = {"c0"};
  boundsWithUpper.upperBound = IndexBound{boundRow, false};
  EXPECT_TRUE(boundsWithUpper.validate());
  EXPECT_EQ(
      boundsWithUpper.toString(),
      "IndexBounds{indexColumns=[c0], upperBound=(...)}");

  // Test valid bounds with both
  IndexBounds boundsWithBoth;
  boundsWithBoth.indexColumns = {"c0"};
  boundsWithBoth.lowerBound = IndexBound{boundRow, true};
  boundsWithBoth.upperBound = IndexBound{boundRow, false};
  EXPECT_TRUE(boundsWithBoth.validate());
  EXPECT_EQ(
      boundsWithBoth.toString(),
      "IndexBounds{indexColumns=[c0], lowerBound=[...], upperBound=(...)}");

  // Test invalid bounds with no bounds
  IndexBounds noBounds;
  noBounds.indexColumns = {"c0"};
  EXPECT_FALSE(noBounds.validate());

  auto badRowBound =
      makeRowVector({makeNullableFlatVector<int32_t>({2, std::nullopt})});
  IndexBounds badSizeBounds;
  badSizeBounds.indexColumns = {"c0"};
  badSizeBounds.lowerBound = IndexBound{badRowBound, true};
  EXPECT_FALSE(badSizeBounds.validate());

  // Test invalid bounds with extra index columns.
  IndexBounds extraColBounds;
  extraColBounds.indexColumns = {"c0", "c1", "c2"};
  extraColBounds.lowerBound = IndexBound{boundRow, true};
  EXPECT_FALSE(extraColBounds.validate());

  extraColBounds.lowerBound = std::nullopt;
  extraColBounds.upperBound = IndexBound{boundRow, true};
  EXPECT_FALSE(extraColBounds.validate());

  const auto multiColumnBoundRow = makeRowVector(
      {makeNullableFlatVector<int32_t>({2}),
       makeNullableFlatVector<int32_t>({1})});
  IndexBounds multiColumnBounds;
  multiColumnBounds.indexColumns = {"c0", "c1"};
  multiColumnBounds.lowerBound = IndexBound{multiColumnBoundRow, true};
  multiColumnBounds.upperBound = IndexBound{multiColumnBoundRow, false};
  EXPECT_TRUE(multiColumnBounds.validate());
  EXPECT_EQ(
      multiColumnBounds.toString(),
      "IndexBounds{indexColumns=[c0, c1], lowerBound=[...], upperBound=(...)}");

  IndexBounds missingColBounds;
  missingColBounds.indexColumns = {"c0"};
  missingColBounds.upperBound = IndexBound{multiColumnBoundRow, false};
  EXPECT_FALSE(missingColBounds.validate());
}

#if 0
TEST_F(KeyEncoderTest, longTypeAllFields) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};
  auto c0 = vectorMaker_.flatVectorNullable<int64_t>({0, 2});
  auto c1 = vectorMaker_.flatVectorNullable<int64_t>({1, 1});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({2, 0});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.reserve(3);
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(
        std::make_shared<const velox::core::FieldAccessTypedExpr>(
            velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  EXPECT_TRUE(result < 0);
}

TEST_F(KeyEncoderTest, longTypeAllFieldsDescending) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kDescNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};
  auto c0 = vectorMaker_.flatVectorNullable<int64_t>({0, 2});
  auto c1 = vectorMaker_.flatVectorNullable<int64_t>({1, 1});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({2, 0});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.reserve(3);
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(
        std::make_shared<const velox::core::FieldAccessTypedExpr>(
            velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  EXPECT_TRUE(result > 0);
}

TEST_F(KeyEncoderTest, longTypeAllFieldsWithNulls) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};
  auto c0 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c1 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({2, 0});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.reserve(3);
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(
        std::make_shared<const velox::core::FieldAccessTypedExpr>(
            velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  EXPECT_TRUE(result > 0);
}

TEST_F(KeyEncoderTest, defaultNullsOrdering) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};
  auto c0 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c1 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({0, std::nullopt});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.reserve(3);
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(
        std::make_shared<const velox::core::FieldAccessTypedExpr>(
            velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  EXPECT_TRUE(result > 0);
}

TEST_F(KeyEncoderTest, explicitNullsOrderingAscendingNullsLast) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsLast)};
  auto c0 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c1 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({0, std::nullopt});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.reserve(3);
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(
        std::make_shared<const velox::core::FieldAccessTypedExpr>(
            velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  EXPECT_TRUE(result < 0);
}

TEST_F(KeyEncoderTest, explicitNullsOrderingDescendingNullsLast) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kDescNullsLast),
      velox::core::SortOrder(velox::core::kDescNullsLast),
      velox::core::SortOrder(velox::core::kDescNullsLast)};
  auto c0 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c1 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({0, std::nullopt});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.reserve(3);
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(
        std::make_shared<const velox::core::FieldAccessTypedExpr>(
            velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  EXPECT_TRUE(result < 0);
}

TEST_F(KeyEncoderTest, longTypeSubsetOfFields) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  auto c0 = vectorMaker_.flatVectorNullable<int64_t>({0, 2});
  auto c1 = vectorMaker_.flatVectorNullable<int64_t>({1, 1});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({2, 0});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.push_back(
      std::make_shared<const velox::core::FieldAccessTypedExpr>(
          velox::BIGINT(), /*name=*/"c2"));
  fields.push_back(
      std::make_shared<const velox::core::FieldAccessTypedExpr>(
          velox::BIGINT(), /*name=*/"c0"));

  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  EXPECT_TRUE(result > 0);
}

TEST_F(KeyEncoderTest, complexTypeDictionaryEncoded) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields =
      {
          std::make_shared<const velox::core::FieldAccessTypedExpr>(
              velox::ROW(
                  {"f0", "f1", "f2"},
                  {velox::BIGINT(), velox::BIGINT(), velox::BIGINT()}),
              "c0"),
          std::make_shared<const velox::core::FieldAccessTypedExpr>(
              velox::ARRAY(velox::INTEGER()), "c1"),
      };

  auto c0 = vectorMaker_.flatVectorNullable<int64_t>({0, 2});
  auto c1 = vectorMaker_.flatVectorNullable<int64_t>({1, 1});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({1, 2});

  auto baseRow = vectorMaker_.rowVector({"f0", "f1", "f2"}, {c0, c1, c2});
  auto baseArray = makeArrayVector<int32_t>({{1, 2, 3}, {4, 5, 6}});
  auto rowVector = vectorMaker_.rowVector(
      {"c0", "c1"},
      {
          velox::VectorFuzzer::wrapInLazyVector(
              velox::BaseVector::wrapInDictionary(
                  nullptr, makeIndicesInReverse(2), 2, baseRow)),
          velox::VectorFuzzer::wrapInLazyVector(
              velox::BaseVector::wrapInDictionary(
                  nullptr, makeIndicesInReverse(2), 2, baseArray)),
      });

  auto result = compareRowVector(rowVector, fields, ordering);

  EXPECT_TRUE(result > 0);
}

TEST_F(KeyEncoderTest, constantVector) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast)};
  auto c0 = vectorMaker_.flatVectorNullable<int64_t>({0, 2});
  auto c1 = vectorMaker_.flatVectorNullable<int64_t>({1, 1});
  auto c2 = vectorMaker_.constantVector<int64_t>({2});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.reserve(3);
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(
        std::make_shared<const velox::core::FieldAccessTypedExpr>(
            velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  EXPECT_TRUE(result < 0);
}

TEST_F(KeyEncoderTest, booleanTypeSingleFieldTests) {
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<bool>(
          {true, true}, velox::core::kAscNullsFirst) == 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<bool>(
          {false, true}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<bool>(
          {true, false}, velox::core::kDescNullsLast) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<bool>(
          {std::nullopt, false}, velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyEncoderTest, doubleTypeSingleFieldTests) {
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {0.1, 0.1}, velox::core::kAscNullsFirst) == 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {0.01, 0.1}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {0.1, 0.01}, velox::core::kDescNullsLast) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {std::numeric_limits<double>::quiet_NaN(),
           std::numeric_limits<double>::quiet_NaN()},
          velox::core::kAscNullsFirst) == 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {std::nullopt, std::numeric_limits<double>::quiet_NaN()},
          velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {std::nullopt, std::numeric_limits<double>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyEncoderTest, floatTypeSingleFieldTests) {
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {0.1f, 0.1f}, velox::core::kAscNullsFirst) == 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {0.01f, 0.1f}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {0.1f, 0.01f}, velox::core::kDescNullsLast) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {std::numeric_limits<float>::quiet_NaN(),
           std::numeric_limits<float>::quiet_NaN()},
          velox::core::kAscNullsFirst) == 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {std::nullopt, std::numeric_limits<float>::quiet_NaN()},
          velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {std::nullopt, std::numeric_limits<float>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyEncoderTest, byteTypeSingleFieldTests) {
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>({1, 2}, velox::core::kAscNullsFirst) <
      0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>({2, 1}, velox::core::kDescNullsLast) <
      0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {std::numeric_limits<int8_t>::min(),
           std::numeric_limits<int8_t>::max()},
          velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {std::numeric_limits<int8_t>::min(),
           std::numeric_limits<int8_t>::max()},
          velox::core::kDescNullsLast) > 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {std::nullopt, std::numeric_limits<int8_t>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyEncoderTest, shortTypeSingleFieldTests) {
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {1, 2}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {2, 1}, velox::core::kDescNullsLast) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {std::numeric_limits<int16_t>::min(),
           std::numeric_limits<int16_t>::max()},
          velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {std::numeric_limits<int16_t>::min(),
           std::numeric_limits<int16_t>::max()},
          velox::core::kDescNullsLast) > 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {std::nullopt, std::numeric_limits<int16_t>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyEncoderTest, integerTypeSingleFieldTests) {
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {1, 2}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {2, 1}, velox::core::kDescNullsLast) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::numeric_limits<int32_t>::min(),
           std::numeric_limits<int32_t>::max()},
          velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::numeric_limits<int32_t>::min(),
           std::numeric_limits<int32_t>::max()},
          velox::core::kDescNullsLast) > 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::nullopt, std::numeric_limits<int32_t>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyEncoderTest, dateTypeSingleFieldTests) {
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {1, 2}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {2, 1}, velox::core::kDescNullsLast) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyEncoderTest, longTypeSingleFieldTests) {
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {1, 2}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {2, 1}, velox::core::kDescNullsLast) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {std::numeric_limits<int64_t>::min(),
           std::numeric_limits<int64_t>::max()},
          velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {std::numeric_limits<int64_t>::min(),
           std::numeric_limits<int64_t>::max()},
          velox::core::kDescNullsLast) > 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {std::nullopt, std::numeric_limits<int64_t>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyEncoderTest, timestampTypeSingleFieldTests) {
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<velox::Timestamp>(
          {velox::Timestamp::fromMicros(1), velox::Timestamp::fromMicros(1)},
          velox::core::kAscNullsFirst) == 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<velox::Timestamp>(
          {velox::Timestamp::fromMicros(1), velox::Timestamp::fromMicros(2)},
          velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<velox::Timestamp>(
          {velox::Timestamp::fromMicros(2), velox::Timestamp::fromMicros(1)},
          velox::core::kDescNullsLast) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<velox::Timestamp>(
          {std::nullopt, velox::Timestamp::fromMicros(1)},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyEncoderTest, stringTypeSingleFieldTests) {
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {"abc", "abc"}, velox::core::kAscNullsFirst) == 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {"aaa", "abc"}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {"aaa", "aaaa"}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {"abc", "aaa"}, velox::core::kDescNullsLast) < 0);

  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {std::nullopt, ""}, velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyEncoderTest, arrayTypeSingleFieldTests) {
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, 1L}}, {{1L, 1L}}}, velox::core::kAscNullsFirst) == 0);

  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, 1L}}, {{1L, 1L, 1L}}}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, 1L, 1L}}, {{1L, 1L}}}, velox::core::kDescNullsLast) < 0);

  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, 1L, 1L}}, {{1L, 2L}}}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, std::nullopt, 1L}}, {{1L, 0L, 1L}}},
          velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {std::nullopt, {{1L}}}, velox::core::kAscNullsFirst) < 0);

  std::vector<std::optional<int64_t>> arrayWithNull{std::nullopt};
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {std::nullopt, arrayWithNull}, velox::core::kAscNullsFirst) < 0);

  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {std::nullopt, {std::vector<std::optional<int64_t>>{}}},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyEncoderTest, rowTypeSingleFieldTests) {
  int cmp = singleRowFieldCompare<int64_t, std::string>(
      {1L, 1L}, {"aaa", "aaa"}, velox::core::kAscNullsFirst);
  EXPECT_TRUE(cmp == 0);

  cmp = singleRowFieldCompare<int64_t, std::string>(
      {1L, 1L}, {"aaa", "abc"}, velox::core::kAscNullsFirst);
  EXPECT_TRUE(cmp < 0);

  cmp = singleRowFieldCompare<int64_t, std::string>(
      {1L, 1L}, {"aaa", "abc"}, velox::core::kDescNullsLast);
  cmp = singleRowFieldCompare<int64_t, std::string>(
      {1L, 1L}, {std::nullopt, "abc"}, velox::core::kAscNullsFirst);
  EXPECT_TRUE(cmp < 0);

  cmp = singleRowFieldCompare<int64_t, std::string>(
      {std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt},
      velox::core::kAscNullsFirst,
      [](velox::vector_size_t row) { return row == 0 ? true : false; });
  EXPECT_TRUE(cmp < 0);
}
#endif
} // namespace facebook::nimble::test
