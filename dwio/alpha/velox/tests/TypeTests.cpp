// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>

#include "dwio/alpha/common/tests/AlphaFileWriter.h"
#include "dwio/alpha/velox/VeloxReader.h"
#include "dwio/alpha/velox/VeloxWriterOptions.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace ::facebook;

namespace {
auto rootPool =
    velox::memory::deprecatedDefaultMemoryManager().addRootPool("type_tests");
auto leafPool = rootPool -> addLeafChild("leaf");
} // namespace

TEST(TypeTests, MatchingSchema) {
  const uint32_t batchSize = 10;
  auto type = velox::ROW({
      {"simple", velox::TINYINT()},
      {"array", velox::ARRAY(velox::BIGINT())},
      {"map", velox::MAP(velox::INTEGER(), velox::BIGINT())},
      {"struct",
       velox::ROW({
           {"a", velox::REAL()},
           {"b", velox::DOUBLE()},
       })},
      {"nested",
       velox::MAP(
           velox::INTEGER(),
           velox::ROW({
               {"a", velox::REAL()},
               {"b",
                velox::ARRAY(velox::MAP(velox::INTEGER(), velox::BIGINT()))},
           }))},
      {"arraywithoffsets", velox::ARRAY(velox::BIGINT())},
  });

  auto vector =
      velox::test::BatchMaker::createBatch(type, batchSize, *leafPool);
  auto file = alpha::test::createAlphaFile(*rootPool, vector);

  velox::InMemoryReadFile readFile(file);
  auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(
      std::dynamic_pointer_cast<const velox::RowType>(vector->type()));
  alpha::VeloxReader reader(*leafPool, &readFile, std::move(selector));

  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(batchSize, result));
  ASSERT_EQ(result->type()->kind(), velox::TypeKind::ROW);
  ASSERT_TRUE(result->type()->kindEquals(vector->type()));
  ASSERT_EQ(batchSize, result->size());

  for (auto i = 0; i < result->size(); ++i) {
    ASSERT_TRUE(vector->equalValueAt(result.get(), i, i))
        << "Content mismatch row " << i << "\nExpected: " << vector->toString(i)
        << "\nActual: " << result->toString(i);
  }
}

TEST(TypeTests, ExtraColumnWithRename) {
  const uint32_t batchSize = 10;
  auto fileType = velox::ROW({
      {"simple", velox::TINYINT()},
      {"array", velox::ARRAY(velox::BIGINT())},
      {"map", velox::MAP(velox::INTEGER(), velox::BIGINT())},
      {"struct",
       velox::ROW({
           {"a", velox::REAL()},
           {"b", velox::DOUBLE()},
       })},
      {"nested",
       velox::MAP(
           velox::INTEGER(),
           velox::ROW({
               {"a", velox::REAL()},
               {"b",
                velox::ARRAY(velox::MAP(velox::INTEGER(), velox::BIGINT()))},
           }))},
      {"arraywithoffsets", velox::ARRAY(velox::INTEGER())},
  });

  auto newType = velox::ROW({
      {"simple", velox::TINYINT()},
      {"array", velox::ARRAY(velox::BIGINT())},
      {"map", velox::MAP(velox::INTEGER(), velox::BIGINT())},
      {"struct_rename",
       velox::ROW({
           {"a", velox::REAL()},
           {"b", velox::DOUBLE()},
       })},
      {"nested",
       velox::MAP(
           velox::INTEGER(),
           velox::ROW({
               {"a", velox::REAL()},
               {"b",
                velox::ARRAY(velox::MAP(velox::INTEGER(), velox::BIGINT()))},
           }))},
      {"arraywithoffsets", velox::ARRAY(velox::INTEGER())},
      {"new", velox::TINYINT()},
  });

  auto vector =
      velox::test::BatchMaker::createBatch(fileType, batchSize, *leafPool);
  auto file = alpha::test::createAlphaFile(*rootPool, vector);

  velox::InMemoryReadFile readFile(file);
  auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(
      std::dynamic_pointer_cast<const velox::RowType>(newType));
  alpha::VeloxReader reader(*leafPool, &readFile, std::move(selector));

  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(batchSize, result));
  ASSERT_EQ(result->type()->kind(), velox::TypeKind::ROW);
  ASSERT_TRUE(result->type()->kindEquals(newType));

  for (auto i = 0; i < fileType->size(); ++i) {
    auto& expectedChild = vector->as<velox::RowVector>()->childAt(i);
    auto& actualChild = result->as<velox::RowVector>()->childAt(i);
    ASSERT_EQ(batchSize, expectedChild->size());
    ASSERT_EQ(batchSize, actualChild->size());

    for (auto j = 0; j < batchSize; ++j) {
      ASSERT_TRUE(expectedChild->equalValueAt(actualChild.get(), j, j))
          << "Content mismatch at column " << i << ", row " << j
          << "\nExpected: " << expectedChild->toString(j)
          << "\nActual: " << actualChild->toString(j);
    }
  }

  auto& extraChild =
      result->as<velox::RowVector>()->childAt(newType->size() - 1);
  ASSERT_EQ(batchSize, extraChild->size());
  for (auto i = 0; i < batchSize; ++i) {
    ASSERT_TRUE(extraChild->isNullAt(i));
  }
}

TEST(TypeTests, SameTypeWithProjection) {
  const uint32_t batchSize = 10;
  auto type = velox::ROW({
      {"simple", velox::TINYINT()},
      {"array", velox::ARRAY(velox::BIGINT())},
      {"map", velox::MAP(velox::INTEGER(), velox::BIGINT())},
      {"struct",
       velox::ROW({
           {"a", velox::REAL()},
           {"b", velox::DOUBLE()},
       })},
      {"nested",
       velox::MAP(
           velox::INTEGER(),
           velox::ROW({
               {"a", velox::REAL()},
               {"b",
                velox::ARRAY(velox::MAP(velox::INTEGER(), velox::BIGINT()))},
           }))},
      {"arraywithoffsets", velox::ARRAY(velox::BIGINT())},
  });

  auto vector =
      velox::test::BatchMaker::createBatch(type, batchSize, *leafPool);
  auto file = alpha::test::createAlphaFile(*rootPool, vector);

  velox::InMemoryReadFile readFile(file);
  auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(
      std::dynamic_pointer_cast<const velox::RowType>(type),
      std::vector<std::string>{"array", "nested", "arraywithoffsets"});
  alpha::VeloxReader reader(*leafPool, &readFile, std::move(selector));

  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(batchSize, result));
  ASSERT_EQ(result->type()->kind(), velox::TypeKind::ROW);
  ASSERT_TRUE(result->type()->kindEquals(type));

  for (auto i : {1, 4, 5}) {
    auto& expectedChild = vector->as<velox::RowVector>()->childAt(i);
    auto& actualChild = result->as<velox::RowVector>()->childAt(i);
    ASSERT_EQ(batchSize, expectedChild->size());
    ASSERT_EQ(batchSize, actualChild->size());

    for (auto j = 0; j < batchSize; ++j) {
      ASSERT_TRUE(expectedChild->equalValueAt(actualChild.get(), j, j))
          << "Content mismatch at column " << i << ", row " << j
          << "\nExpected: " << expectedChild->toString(j)
          << "\nActual: " << actualChild->toString(j);
    }
  }

  // Not projected
  for (auto i : {0, 2, 3}) {
    ASSERT_EQ(result->as<velox::RowVector>()->childAt(i), nullptr);
  }
}

TEST(TypeTests, ProjectingNewColumn) {
  const uint32_t batchSize = 10;
  auto fileType = velox::ROW({
      {"simple", velox::TINYINT()},
      {"array", velox::ARRAY(velox::BIGINT())},
      {"map", velox::MAP(velox::INTEGER(), velox::BIGINT())},
      {"struct",
       velox::ROW({
           {"a", velox::REAL()},
           {"b", velox::DOUBLE()},
       })},
      {"nested",
       velox::MAP(
           velox::INTEGER(),
           velox::ROW({
               {"a", velox::REAL()},
               {"b",
                velox::ARRAY(velox::MAP(velox::INTEGER(), velox::BIGINT()))},
           }))},
  });

  auto newType = velox::ROW({
      {"simple", velox::TINYINT()},
      {"array", velox::ARRAY(velox::BIGINT())},
      {"map", velox::MAP(velox::INTEGER(), velox::BIGINT())},
      {"struct_rename",
       velox::ROW({
           {"a", velox::REAL()},
           {"b", velox::DOUBLE()},
       })},
      {"nested",
       velox::MAP(
           velox::INTEGER(),
           velox::ROW({
               {"a", velox::REAL()},
               {"b",
                velox::ARRAY(velox::MAP(velox::INTEGER(), velox::BIGINT()))},
           }))},
      {"new", velox::TINYINT()},
  });

  auto vector =
      velox::test::BatchMaker::createBatch(fileType, batchSize, *leafPool);
  auto file = alpha::test::createAlphaFile(*rootPool, vector);

  velox::InMemoryReadFile readFile(file);
  auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(
      std::dynamic_pointer_cast<const velox::RowType>(newType),
      std::vector<std::string>{"struct_rename", "new"});
  alpha::VeloxReader reader(*leafPool, &readFile, std::move(selector));

  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(batchSize, result));
  ASSERT_EQ(result->type()->kind(), velox::TypeKind::ROW);
  ASSERT_TRUE(result->type()->kindEquals(newType));

  for (auto i : {3}) {
    auto& expectedChild = vector->as<velox::RowVector>()->childAt(i);
    auto& actualChild = result->as<velox::RowVector>()->childAt(i);
    ASSERT_EQ(batchSize, expectedChild->size());
    ASSERT_EQ(batchSize, actualChild->size());

    for (auto j = 0; j < batchSize; ++j) {
      ASSERT_TRUE(expectedChild->equalValueAt(actualChild.get(), j, j))
          << "Content mismatch at column " << i << ", row " << j
          << "\nExpected: " << expectedChild->toString(j)
          << "\nActual: " << actualChild->toString(j);
    }
  }

  // Projected, but missing
  for (auto i : {5}) {
    auto& nullChild = result->as<velox::RowVector>()->childAt(i);
    ASSERT_EQ(batchSize, nullChild->size());
    for (auto j = 0; j < batchSize; ++j) {
      ASSERT_TRUE(nullChild->isNullAt(j))
          << "Expecting null value at column " << i << ", row " << j;
    }
  }

  // Not projected
  for (auto i : {0, 1, 2, 4}) {
    ASSERT_EQ(result->as<velox::RowVector>()->childAt(i), nullptr);
  }
}

TEST(TypeTests, FlatMapFeatureSelection) {
  const uint32_t batchSize = 200;
  auto type = velox::ROW({
      {"map", velox::MAP(velox::INTEGER(), velox::BIGINT())},
  });

  velox::VectorPtr vector = nullptr;
  std::unordered_set<int32_t> uniqueKeys;
  while (uniqueKeys.empty()) {
    vector = velox::test::BatchMaker::createBatch(type, batchSize, *leafPool);
    auto keys = vector->as<velox::RowVector>()
                    ->childAt(0)
                    ->as<velox::MapVector>()
                    ->mapKeys()
                    ->asFlatVector<int32_t>();
    auto values = vector->as<velox::RowVector>()
                      ->childAt(0)
                      ->as<velox::MapVector>()
                      ->mapValues()
                      ->asFlatVector<int64_t>();

    for (auto i = 0; i < keys->size(); ++i) {
      if (!values->isNullAt(i)) {
        uniqueKeys.emplace(keys->valueAt(i));
      }
    }
  }

  alpha::VeloxWriterOptions options{
      .flatMapColumns = {"map"},
  };
  auto file =
      alpha::test::createAlphaFile(*rootPool, vector, std::move(options));

  velox::InMemoryReadFile readFile(file);
  auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(
      std::dynamic_pointer_cast<const velox::RowType>(type));

  auto findKeyNotInMap = [&uniqueKeys](int32_t start) {
    while (uniqueKeys.find(start) != uniqueKeys.end()) {
      ++start;
    }
    return start;
  };

  int32_t existingKey = *uniqueKeys.begin();
  int32_t nonExistingKey1 = findKeyNotInMap(0);
  int32_t nonExistingKey2 = findKeyNotInMap(nonExistingKey1 + 1);

  auto expectedInnerType =
      velox::ROW({velox::BIGINT(), velox::BIGINT(), velox::BIGINT()});

  alpha::VeloxReadParams params;
  params.readFlatMapFieldAsStruct = {"map"},
  params.flatMapFeatureSelector = {
      {"map",
       {{folly::to<std::string>(nonExistingKey1),
         folly::to<std::string>(existingKey),
         folly::to<std::string>(nonExistingKey2)}}}};

  alpha::VeloxReader reader(
      *leafPool, &readFile, std::move(selector), std::move(params));

  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(batchSize, result));
  ASSERT_EQ(result->type()->kind(), velox::TypeKind::ROW);
  ASSERT_EQ(1, result->type()->as<velox::TypeKind::ROW>().size());
  ASSERT_EQ(
      result->as<velox::RowVector>()->childAt(0)->type()->kind(),
      velox::TypeKind::ROW);
  ASSERT_TRUE(result->as<velox::RowVector>()->childAt(0)->type()->kindEquals(
      expectedInnerType));

  auto resultStruct =
      result->as<velox::RowVector>()->childAt(0)->as<velox::RowVector>();
  ASSERT_EQ(3, resultStruct->childrenSize());
  ASSERT_EQ(
      folly::to<std::string>(nonExistingKey1),
      resultStruct->type()->as<velox::TypeKind::ROW>().nameOf(0));
  ASSERT_EQ(
      folly::to<std::string>(existingKey),
      resultStruct->type()->as<velox::TypeKind::ROW>().nameOf(1));
  ASSERT_EQ(
      folly::to<std::string>(nonExistingKey2),
      resultStruct->type()->as<velox::TypeKind::ROW>().nameOf(2));
  ASSERT_EQ(
      velox::TypeKind::BIGINT,
      resultStruct->as<velox::RowVector>()->childAt(0)->typeKind());
  ASSERT_EQ(
      velox::TypeKind::BIGINT,
      resultStruct->as<velox::RowVector>()->childAt(1)->typeKind());
  ASSERT_EQ(
      velox::TypeKind::BIGINT,
      resultStruct->as<velox::RowVector>()->childAt(2)->typeKind());

  for (auto i : {1}) {
    ASSERT_EQ(batchSize, resultStruct->size());

    bool foundNotNullValue = false;
    for (auto j = 0; j < batchSize; ++j) {
      if (!resultStruct->childAt(i)->isNullAt(j)) {
        foundNotNullValue = true;
        break;
      }
    }

    ASSERT_TRUE(foundNotNullValue);
  }

  for (auto i : {0, 2}) {
    auto& nullChild = resultStruct->childAt(i);
    ASSERT_EQ(batchSize, nullChild->size());
    for (auto j = 0; j < batchSize; ++j) {
      ASSERT_TRUE(nullChild->isNullAt(j))
          << "Expecting null value at column " << i << ", row " << j;
    }
  }
}
