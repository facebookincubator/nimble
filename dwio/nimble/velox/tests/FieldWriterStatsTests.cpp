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

#include "dwio/nimble/stats/ColumnStatsUtils.h"
#include "dwio/nimble/velox/RawSizeUtils.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace facebook;
using nimble::ColumnStats;

class FieldWriterStatsTests : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::initializeMemoryManager(
        velox::memory::MemoryManagerOptions{});
  }

  void SetUp() override {
    rootPool_ = velox::memory::memoryManager()->addRootPool("default_root");
    leafPool_ = rootPool_->addLeafChild("default_leaf");
    vectorMaker_ = std::make_unique<velox::test::VectorMaker>(leafPool_.get());
  }

  ColumnStats combineStats(const std::vector<ColumnStats>& stats) {
    ColumnStats combinedStats;
    bool dedupStatFound = false;
    uint64_t dedupedLogicalSize = 0;
    for (auto& stat : stats) {
      combinedStats.logicalSize += stat.logicalSize;
      combinedStats.physicalSize += stat.physicalSize;
      if (stat.dedupedLogicalSize.has_value()) {
        dedupStatFound = true;
      }
      dedupedLogicalSize += stat.dedupedLogicalSize.value_or(stat.logicalSize);
    }
    if (dedupStatFound) {
      combinedStats.dedupedLogicalSize = dedupedLogicalSize;
    }
    return combinedStats;
  }

  void verifyReturnedColumnStats(
      const velox::VectorPtr& input,
      const std::vector<ColumnStats>& expectedStats,
      const nimble::VeloxWriterOptions& options = {},
      std::shared_ptr<const velox::Type> rowType = nullptr) {
    // Write Nimble file.
    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    if (!rowType) {
      rowType = input->type();
    }
    nimble::VeloxWriter writer(
        *rootPool_, rowType, std::move(writeFile), options);
    writer.write(input);
    writer.close();

    // Verify returned column stats.
    const auto actualStats = writer.getRunStats().columnStats;
    ASSERT_EQ(actualStats.size(), expectedStats.size());
    for (auto i = 0; i < actualStats.size(); ++i) {
      const auto& actualStat = actualStats.at(i);
      const auto& expectedStat = expectedStats.at(i);
      EXPECT_EQ(expectedStat.logicalSize, actualStat.logicalSize)
          << "i = " << i;
      EXPECT_EQ(expectedStat.physicalSize, actualStat.physicalSize)
          << "i = " << i;
      EXPECT_EQ(expectedStat.nullCount, actualStat.nullCount) << "i = " << i;
      EXPECT_EQ(expectedStat.valueCount, actualStat.valueCount) << "i = " << i;
      EXPECT_EQ(
          expectedStat.dedupedLogicalSize.has_value(),
          actualStat.dedupedLogicalSize.has_value())
          << "i = " << i;
      if (expectedStat.dedupedLogicalSize.has_value() &&
          actualStat.dedupedLogicalSize.has_value()) {
        EXPECT_EQ(
            expectedStat.dedupedLogicalSize.value(),
            actualStat.dedupedLogicalSize.value())
            << "i = " << i;
      }
    }

    // Check top level stats using raw size utils.
    if (!expectedStats.empty()) {
      ASSERT_FALSE(actualStats.empty());
      const auto& actualTopLevelStat = actualStats.at(0);
      nimble::RawSizeContext context;
      velox::common::Ranges ranges;
      ranges.add(0, input->size());
      auto expectedLogicalSize =
          nimble::getRawSizeFromVector(input, ranges, context);
      EXPECT_EQ(expectedLogicalSize, actualTopLevelStat.logicalSize);
      EXPECT_EQ(context.nullCount, actualTopLevelStat.nullCount);
    }
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> leafPool_;
  std::unique_ptr<velox::test::VectorMaker> vectorMaker_;
};

TEST_F(FieldWriterStatsTests, SimpleFieldWriterStats) {
  {
    // String and int flat columns with nulls.
    auto c1 = vectorMaker_->flatVector<int32_t>({1, 2, 3});
    c1->setNull(1, true);
    uint64_t columnSize = c1->size();
    auto stat1 = ColumnStats{
        .logicalSize = sizeof(int32_t) * (columnSize - 1) + nimble::NULL_SIZE,
        .physicalSize = 45,
        .nullCount = 1,
        .valueCount = columnSize};

    auto c2 =
        vectorMaker_->flatVector<velox::StringView>({"a", "bbbb", "ccccccccc"});
    c2->setNull(0, true);
    c2->setNull(2, true);
    auto stat2 = ColumnStats{
        .logicalSize = std::string("bbbb").size() + 2 * nimble::NULL_SIZE,
        .physicalSize = 44,
        .nullCount = 2,
        .valueCount = columnSize};

    auto vector = vectorMaker_->rowVector({c1, c2});
    auto stat0 = combineStats({stat1, stat2});
    stat0.valueCount = columnSize;
    verifyReturnedColumnStats(vector, {stat0, stat1, stat2});
  }

  {
    // Int and boolean flat columns no nulls.
    auto c1 = vectorMaker_->flatVector<int32_t>({1, 2, 3});
    uint64_t columnSize = c1->size();
    auto stat1 = ColumnStats{
        .logicalSize = sizeof(int32_t) * columnSize,
        .physicalSize = 18,
        .valueCount = columnSize};

    auto c2 = vectorMaker_->flatVector<bool>({true, true, false});
    auto stat2 = ColumnStats{
        .logicalSize = sizeof(bool) * columnSize,
        .physicalSize = 20,
        .valueCount = columnSize};

    auto vector = vectorMaker_->rowVector({c1, c2});
    auto stat0 = combineStats({stat1, stat2});
    stat0.valueCount = columnSize;
    verifyReturnedColumnStats(vector, {stat0, stat1, stat2});
  }

  {
    // Two columns, all nulls.
    auto c1 = vectorMaker_->flatVector<int32_t>({1, 2, 3});
    c1->setNull(0, true);
    c1->setNull(2, true);
    uint64_t columnSize = c1->size();
    auto stat1 = ColumnStats{};

    auto c2 = vectorMaker_->flatVector<bool>({true, true, false});
    auto stat2 = ColumnStats{};

    auto vector = vectorMaker_->rowVector({c1, c2});
    velox::BufferPtr nulls = velox::AlignedBuffer::allocate<bool>(
        columnSize, leafPool_.get(), velox::bits::kNull);
    vector->setNulls(nulls);
    auto stat0 = ColumnStats{
        .logicalSize = nimble::NULL_SIZE * columnSize,
        .physicalSize = 12, // Null Stream.
        .nullCount = columnSize,
        .valueCount = columnSize};
    verifyReturnedColumnStats(vector, {stat0, stat1, stat2});
  }

  {
    // Dictionary and constant columns with nulls.
    auto c1 =
        vectorMaker_->flatVectorNullable<int64_t>({0, 0, 1, std::nullopt});
    uint64_t columnSize = c1->size();
    auto stat1 = ColumnStats{
        .logicalSize = sizeof(int64_t) * (columnSize - 2) + nimble::NULL_SIZE,
        .physicalSize = 46,
        .nullCount = 1,
        .valueCount = columnSize - 1,
    };

    auto valueVector =
        vectorMaker_->flatVectorNullable<int64_t>({0, std::nullopt});
    velox::BufferPtr indices =
        velox::AlignedBuffer::allocate<velox::vector_size_t>(
            columnSize, leafPool_.get());
    auto* rawIndices = indices->asMutable<velox::vector_size_t>();
    rawIndices[0] = 0;
    rawIndices[1] = 0;
    rawIndices[2] = 1;
    rawIndices[3] = 1;
    auto c2 = velox::BaseVector::wrapInDictionary(
        velox::BufferPtr(nullptr), indices, columnSize, valueVector);
    auto stat2 = ColumnStats{
        .logicalSize = sizeof(int64_t) + nimble::NULL_SIZE * 2,
        .physicalSize = 45,
        .nullCount = 2,
        .valueCount = columnSize - 1,
    };

    auto c3 = velox::BaseVector::wrapInConstant(columnSize, 3, c1);
    auto stat3 = ColumnStats{
        .logicalSize = nimble::NULL_SIZE * 3,
        .physicalSize = 0,
        .nullCount = 3,
        .valueCount = columnSize - 1,
    };

    auto vector = vectorMaker_->rowVector({c1, c2, c3});
    vector->setNull(1, true);
    auto stat0 = combineStats({stat1, stat2, stat3});
    stat0.valueCount = columnSize;
    stat0.nullCount = 1;
    stat0.logicalSize += stat0.nullCount * nimble::NULL_SIZE;
    stat0.physicalSize += 20; // Null Stream.
    verifyReturnedColumnStats(vector, {stat0, stat1, stat2, stat3});
  }
}

TEST_F(FieldWriterStatsTests, ArrayFieldWriterStats) {
  auto simpleArrayVector =
      vectorMaker_->arrayVector<int8_t>({{0, 1, 2}, {0, 1, 2}, {0, 1, 2}});
  simpleArrayVector->setNull(1, true);
  auto elementsStat1 = ColumnStats{
      .logicalSize = sizeof(int8_t) * 3, .physicalSize = 15, .valueCount = 3};
  auto topLevelStat1 = ColumnStats{
      .logicalSize = elementsStat1.logicalSize + nimble::NULL_SIZE,
      .physicalSize = elementsStat1.physicalSize + 41,
      .nullCount = 1,
      .valueCount = 2,
  };
  uint64_t columnSize = simpleArrayVector->size();

  auto simpleArrayVector2 =
      vectorMaker_->arrayVector<int32_t>({{0, 1, 2}, {3, 4}, {5}});
  auto elementsStat2 = ColumnStats{
      .logicalSize = sizeof(int32_t) * 3,
      .physicalSize = 18,
      .valueCount = 3,
  };
  auto topLevelStat2 = ColumnStats{
      .logicalSize = elementsStat2.logicalSize,
      .physicalSize = elementsStat2.physicalSize + 20,
      .valueCount = 2,
  };

  auto constantArrayVector =
      velox::BaseVector::wrapInConstant(columnSize, 0, simpleArrayVector);
  auto elementsStat3 = ColumnStats{
      .logicalSize = sizeof(int8_t) * 6, .physicalSize = 18, .valueCount = 6};
  auto topLevelStat3 = ColumnStats{
      .logicalSize = elementsStat3.logicalSize,
      .physicalSize = elementsStat3.physicalSize + 15,
      .valueCount = 2,
  };

  velox::BufferPtr indices =
      velox::AlignedBuffer::allocate<velox::vector_size_t>(
          columnSize, leafPool_.get());
  auto* rawIndices = indices->asMutable<velox::vector_size_t>();
  rawIndices[0] = 0;
  rawIndices[1] = 0;
  rawIndices[2] = 1;
  auto dictionaryArrayVector = velox::BaseVector::wrapInDictionary(
      velox::BufferPtr(nullptr), indices, columnSize, simpleArrayVector);
  auto elementsStat4 = ColumnStats{
      .logicalSize = sizeof(int8_t) * 3, .physicalSize = 15, .valueCount = 3};
  auto topLevelStat4 = ColumnStats{
      .logicalSize = elementsStat4.logicalSize + nimble::NULL_SIZE,
      .physicalSize = elementsStat4.physicalSize + 41, // Length Stream.
      .nullCount = 1,
      .valueCount = 2};

  auto vector = vectorMaker_->rowVector(
      {simpleArrayVector,
       simpleArrayVector2,
       constantArrayVector,
       dictionaryArrayVector});
  vector->setNull(0, true);
  // Stats from children nodes get passed up to their parent node.
  auto stat0 = combineStats({
      topLevelStat1,
      topLevelStat2,
      topLevelStat3,
      topLevelStat4,
  });
  stat0.nullCount = 1;
  stat0.valueCount = columnSize;
  stat0.logicalSize += stat0.nullCount * nimble::NULL_SIZE;
  stat0.physicalSize += 20;
  verifyReturnedColumnStats(
      vector,
      {stat0,
       topLevelStat1,
       elementsStat1,
       topLevelStat2,
       elementsStat2,
       topLevelStat3,
       elementsStat3,
       topLevelStat4,
       elementsStat4});
}

TEST_F(FieldWriterStatsTests, ArrayWithOffsetFieldWriterStats) {
  auto simpleArrayVector =
      vectorMaker_->arrayVector<int8_t>({{0, 1, 2}, {0, 1, 2}, {0, 1, 2}});
  auto elementsStat1 = ColumnStats{
      .logicalSize = sizeof(int8_t) * 3, .physicalSize = 15, .valueCount = 3};
  auto offsetStat1 = ColumnStats{.physicalSize = 15};
  auto topLevelStat1 = ColumnStats{
      .logicalSize = sizeof(int8_t) * 6,
      .dedupedLogicalSize = sizeof(int8_t) * 3,
      .physicalSize =
          elementsStat1.physicalSize + offsetStat1.physicalSize + 16,
      .valueCount = 2,
  };
  velox::vector_size_t columnSize = simpleArrayVector->size();

  auto simpleArrayVector2 =
      vectorMaker_->arrayVector<int32_t>({{0, 1, 2}, {3, 4}, {5}});
  auto offsetStat2 = ColumnStats{.physicalSize = 20};
  auto elementsStat2 = ColumnStats{
      .logicalSize = sizeof(int32_t) * 3,
      .physicalSize = 18,
      .valueCount = 3,
  };
  auto topLevelStat2 = ColumnStats{
      .logicalSize = elementsStat2.logicalSize,
      .dedupedLogicalSize = sizeof(int32_t) * 3,
      .physicalSize =
          elementsStat2.physicalSize + offsetStat2.physicalSize + 20,
      .valueCount = 2,
  };

  auto constantArrayVector =
      velox::BaseVector::wrapInConstant(columnSize, 0, simpleArrayVector);
  auto offsetStat3 = ColumnStats{.physicalSize = 15};
  auto elementsStat3 = ColumnStats{
      .logicalSize = sizeof(int8_t) * 3, .physicalSize = 15, .valueCount = 3};
  auto topLevelStat3 = ColumnStats{
      .logicalSize = sizeof(int8_t) * 6,
      .dedupedLogicalSize = sizeof(int8_t) * 3,
      .physicalSize =
          elementsStat3.physicalSize + offsetStat3.physicalSize + 16,
      .valueCount = 2,
  };

  velox::BufferPtr indices =
      velox::AlignedBuffer::allocate<velox::vector_size_t>(
          columnSize, leafPool_.get());
  auto* rawIndices = indices->asMutable<velox::vector_size_t>();
  rawIndices[0] = 0;
  rawIndices[1] = 0;
  rawIndices[2] = 1;
  auto dictionaryArrayVector = velox::BaseVector::wrapInDictionary(
      velox::BufferPtr(nullptr), indices, columnSize, simpleArrayVector);
  auto offsetStat4 = ColumnStats{.physicalSize = 20};
  auto elementsStat4 = ColumnStats{
      .logicalSize = sizeof(int8_t) * 6, .physicalSize = 18, .valueCount = 6};
  auto topLevelStat4 = ColumnStats{
      .logicalSize = elementsStat4.logicalSize,
      .dedupedLogicalSize = elementsStat4.logicalSize,
      .physicalSize = elementsStat4.physicalSize + offsetStat4.physicalSize +
          15, // Length Stream.
      .valueCount = 2,
  };

  auto vector = vectorMaker_->rowVector({
      simpleArrayVector,
      simpleArrayVector2,
      constantArrayVector,
      dictionaryArrayVector,
  });
  vector->setNull(0, true);
  // Stats from children nodes get passed up to their parent node.
  auto stat0 = combineStats({
      topLevelStat1,
      topLevelStat2,
      topLevelStat3,
      topLevelStat4,
  });
  stat0.dedupedLogicalSize = std::nullopt;
  stat0.nullCount = 1;
  stat0.valueCount = columnSize;
  stat0.logicalSize += stat0.nullCount * nimble::NULL_SIZE;
  stat0.physicalSize += 20;
  verifyReturnedColumnStats(
      vector,
      {
          stat0,
          offsetStat1,
          topLevelStat1,
          elementsStat1,
          offsetStat2,
          topLevelStat2,
          elementsStat2,
          offsetStat3,
          topLevelStat3,
          elementsStat3,
          offsetStat4,
          topLevelStat4,
          elementsStat4,
      },
      {
          .dictionaryArrayColumns = {"c0", "c1", "c2", "c3"},
      });
}

TEST_F(FieldWriterStatsTests, MapFieldWriterStats) {
  auto mapVector = vectorMaker_->mapVector<int8_t, int32_t>(
      {{{0, 1}, {2, 3}},
       {{0, 1}, {2, 3}},
       {{8, 9}, {10, 11}},
       {{0, 1}, {2, 3}},
       {{0, 1}, {2, 3}},
       {{4, 5}, {6, 7}}});
  mapVector->setNull(3, true);
  mapVector->setNull(4, true);
  uint64_t columnSize = mapVector->size();
  {
    // No Map Deduplication.
    auto valueStat = ColumnStats{
        .logicalSize = sizeof(int32_t) * 6,
        .physicalSize = 21,
        .valueCount = 6,
    };
    auto keyStat = ColumnStats{
        .logicalSize = sizeof(int8_t) * 6,
        .physicalSize = 18,
        .valueCount = 6,
    };
    auto mapStat = ColumnStats{
        .logicalSize = (sizeof(int32_t) * 6) + (sizeof(int8_t) * 6) +
            (2 * nimble::NULL_SIZE),
        .physicalSize = keyStat.physicalSize + valueStat.physicalSize + 40,
        .nullCount = 2,
        .valueCount = 5};
    auto vector = vectorMaker_->rowVector({mapVector});
    vector->setNull(5, true);
    auto stat0 = ColumnStats{
        .logicalSize = mapStat.logicalSize + nimble::NULL_SIZE,
        .physicalSize = mapStat.physicalSize + 20,
        .nullCount = 1,
        .valueCount = columnSize};
    verifyReturnedColumnStats(vector, {stat0, mapStat, keyStat, valueStat});
  }
}

TEST_F(FieldWriterStatsTests, FlatMapFieldWriterStats) {
  auto mapVector = vectorMaker_->mapVector<int8_t, int32_t>(
      {{{0, 1}, {2, 3}},
       {{0, 1}},
       {{10, 11}},
       {{0, 1}, {2, 3}},
       {{0, 1}, {2, 3}},
       {{4, 5}, {6, 7}}});
  uint64_t columnSize = mapVector->size();
  mapVector->setNull(3, true);
  mapVector->setNull(4, true);

  // Key 0
  auto childValueStat1 = ColumnStats{
      .logicalSize = sizeof(int32_t) * 2,
      .physicalSize = 15,
      .valueCount = 2,
  };
  auto childInMap1 = ColumnStats{
      .physicalSize = 20,
      .valueCount = 2,
  };

  // Key 2
  auto childValueStat2 = ColumnStats{
      .logicalSize = sizeof(int32_t),
      .physicalSize = 16,
      .valueCount = 1,
  };
  auto childInMap2 = ColumnStats{
      .physicalSize = 20,
      .valueCount = 1,
  };

  // Key 10
  auto childValueStat3 = ColumnStats{
      .logicalSize = sizeof(int32_t),
      .physicalSize = 16,
      .valueCount = 1,
  };
  auto childInMap3 = ColumnStats{
      .physicalSize = 20,
      .valueCount = 1,
  };

  auto flatmapStat = combineStats({
      childValueStat1,
      childInMap1,
      childValueStat2,
      childInMap2,
      childValueStat3,
      childInMap3,
  });
  flatmapStat.valueCount = columnSize - 1;
  flatmapStat.physicalSize += 20;
  flatmapStat.logicalSize += 2 * nimble::NULL_SIZE;
  flatmapStat.nullCount = 2;

  // logical size of all keys.
  flatmapStat.logicalSize += sizeof(int8_t) * 4;

  auto vector = vectorMaker_->rowVector({mapVector});
  vector->setNull(5, true);
  auto stat0 = ColumnStats{
      .logicalSize = flatmapStat.logicalSize + nimble::NULL_SIZE,
      .physicalSize = flatmapStat.physicalSize + 20, // Null Stream.
      .nullCount = 1,
      .valueCount = columnSize,
  };
  verifyReturnedColumnStats(
      vector,
      {stat0,
       flatmapStat,
       childValueStat1,
       childInMap1,
       childValueStat2,
       childInMap2,
       childValueStat3,
       childInMap3},
      {.flatMapColumns = {"c0"}});
}

TEST_F(FieldWriterStatsTests, FlatMapPassThroughValueFieldWriterStats) {
  auto feature0 = vectorMaker_->flatVectorNullable<int32_t>(
      {{1}, {1}, std::nullopt, {1}, {1}, std::nullopt});
  auto feature2 = vectorMaker_->flatVectorNullable<int32_t>(
      {{3}, std::nullopt, std::nullopt, {3}, {3}, std::nullopt});
  auto feature10 = vectorMaker_->flatVectorNullable<int32_t>(
      {std::nullopt,
       std::nullopt,
       {11},
       std::nullopt,
       std::nullopt,
       std::nullopt});
  auto flatmapVector = vectorMaker_->rowVector(
      {"0", "2", "10"}, {feature0, feature2, feature10});
  uint64_t columnSize = flatmapVector->size();
  flatmapVector->setNull(3, true);
  flatmapVector->setNull(4, true);

  // Key 0
  auto childValueStat1 = ColumnStats{
      .logicalSize = sizeof(int32_t) * 2 + nimble::NULL_SIZE,
      .physicalSize = 40,
      .nullCount = 1,
      .valueCount = 3,
  };
  auto childInMap1 = ColumnStats{
      .physicalSize = 12,
      .valueCount = 3,
  };

  // Key 2
  auto childValueStat2 = ColumnStats{
      .logicalSize = sizeof(int32_t) + 2 * nimble::NULL_SIZE,
      .physicalSize = 41,
      .nullCount = 2,
      .valueCount = 3,
  };
  auto childInMap2 = ColumnStats{
      .physicalSize = 12,
      .valueCount = 3,
  };

  // Key 10
  auto childValueStat3 = ColumnStats{
      .logicalSize = sizeof(int32_t) + 2 * nimble::NULL_SIZE,
      .physicalSize = 41,
      .nullCount = 2,
      .valueCount = 3,
  };
  auto childInMap3 = ColumnStats{
      .physicalSize = 12,
      .valueCount = 3,
  };

  auto flatmapStat = combineStats({
      childValueStat1,
      childInMap1,
      childValueStat2,
      childInMap2,
      childValueStat3,
      childInMap3,
  });
  flatmapStat.valueCount = columnSize - 1;
  flatmapStat.physicalSize += 20;
  flatmapStat.logicalSize += 2 * nimble::NULL_SIZE;
  flatmapStat.nullCount = 2;

  // TODO(T226402409): Track the expected logical the size of keys for struct
  // injected maps.
  auto vector = vectorMaker_->rowVector({flatmapVector});

  vector->setNull(5, true);
  auto stat0 = ColumnStats{
      .logicalSize = flatmapStat.logicalSize + nimble::NULL_SIZE,
      .physicalSize = flatmapStat.physicalSize + 20, // Null Stream.
      .nullCount = 1,
      .valueCount = columnSize,
  };

  verifyReturnedColumnStats(
      vector,
      {
          stat0,
          flatmapStat,
          childValueStat1,
          childInMap1,
          childValueStat2,
          childInMap2,
          childValueStat3,
          childInMap3,
      },
      {.flatMapColumns = {"c0"}},
      /*rowType=*/
      velox::ROW({{"c0", velox::MAP(velox::TINYINT(), velox::INTEGER())}}));
}

TEST_F(FieldWriterStatsTests, SlidingWindowMapFieldWriterStats) {
  auto mapVector = vectorMaker_->mapVector<int8_t, int32_t>(
      {{{0, 1}, {2, 3}},
       {{0, 1}, {2, 3}},
       {{8, 9}, {10, 11}},
       {{0, 1}, {2, 3}},
       {{0, 1}, {2, 3}},
       {{4, 5}, {6, 7}}});
  mapVector->setNull(3, true);
  mapVector->setNull(4, true);
  uint64_t columnSize = mapVector->size();

  // With Map Deduplication.
  auto valueStat = ColumnStats{
      .logicalSize = sizeof(int32_t) * 4,
      .physicalSize = 19,
      .valueCount = 4,
  };
  auto keyStat = ColumnStats{
      .logicalSize = sizeof(int8_t) * 4,
      .physicalSize = 16,
      .valueCount = 4,
  };

  auto offsetStats = ColumnStats{.physicalSize = 43};
  // Non deduped stats should be the same, with and without deduplication.
  auto mapStat = ColumnStats{
      .logicalSize = (sizeof(int32_t) * 6) + (sizeof(int8_t) * 6) +
          (2 * nimble::NULL_SIZE),
      .dedupedLogicalSize =
          valueStat.logicalSize + keyStat.logicalSize + (2 * nimble::NULL_SIZE),
      .physicalSize = keyStat.physicalSize + valueStat.physicalSize +
          offsetStats.physicalSize + 15, // Lengths Stream.
      .nullCount = 2,
      .valueCount = 5};

  auto vector = vectorMaker_->rowVector({mapVector});
  vector->setNull(5, true);
  auto stat0 = ColumnStats{
      .logicalSize = mapStat.logicalSize + nimble::NULL_SIZE,
      .physicalSize = mapStat.physicalSize + 20, // Null Stream.
      .nullCount = 1,
      .valueCount = columnSize,
  };
  verifyReturnedColumnStats(
      vector,
      {stat0, offsetStats, mapStat, keyStat, valueStat},
      {
          .deduplicatedMapColumns = {"c0"},
      });
}

TEST_F(FieldWriterStatsTests, RowFieldWriterStats) {
  auto c1 = vectorMaker_->rowVector({
      vectorMaker_->flatVectorNullable<int8_t>({0, 0, 0, 1, 1, std::nullopt}),
      vectorMaker_->flatVector<velox::StringView>(
          {"a", "bb", "ccc", "dddd", "eeeee", "ffffff"}),
  });
  uint64_t columnSize = c1->size();
  auto c1SubFieldStat1 = ColumnStats{
      .logicalSize = sizeof(int8_t) * (columnSize - 1) + nimble::NULL_SIZE,
      .physicalSize = 42,
      .nullCount = 1,
      .valueCount = columnSize};
  auto c1SubFieldStat2 = ColumnStats{
      .logicalSize = 21, .physicalSize = 53, .valueCount = columnSize};
  auto stat1 = combineStats({c1SubFieldStat1, c1SubFieldStat2});
  stat1.valueCount = columnSize;

  auto c2 = velox::BaseVector::wrapInConstant(columnSize, 5, c1);
  auto c2SubFieldStat1 = ColumnStats{
      .logicalSize = columnSize * nimble::NULL_SIZE,
      .nullCount = columnSize,
      .valueCount = columnSize};
  auto c2SubFieldStat2 = ColumnStats{
      .logicalSize = 6 * uint64_t(columnSize),
      .physicalSize = 21,
      .valueCount = columnSize};
  auto stat2 = combineStats({c2SubFieldStat1, c2SubFieldStat2});
  stat2.valueCount = columnSize;

  velox::BufferPtr nulls = velox::AlignedBuffer::allocate<bool>(
      columnSize, leafPool_.get(), velox::bits::kNotNull);
  auto* rawNulls = nulls->asMutable<uint64_t>();
  velox::bits::setNull(rawNulls, 1);
  velox::bits::setNull(rawNulls, 2);
  velox::BufferPtr indices =
      velox::AlignedBuffer::allocate<velox::vector_size_t>(
          columnSize, leafPool_.get());
  auto* rawIndices = indices->asMutable<velox::vector_size_t>();
  rawIndices[0] = 0;
  rawIndices[1] = 0; // Null
  rawIndices[2] = 1; // Null
  rawIndices[3] = 2;
  rawIndices[4] = 3;
  rawIndices[5] = 4;
  auto c3 = velox::BaseVector::wrapInDictionary(nulls, indices, columnSize, c1);
  auto c3SubFieldStat1 = ColumnStats{
      .logicalSize = sizeof(int8_t) * 4,
      .physicalSize = 33,
      .valueCount = columnSize - 2};
  auto c3SubFieldStat2 = ColumnStats{
      .logicalSize = 13, .physicalSize = 43, .valueCount = columnSize - 2};
  auto stat3 = combineStats({c3SubFieldStat1, c3SubFieldStat2});
  stat3.physicalSize += 20; // Null Stream.
  stat3.nullCount = 2;
  stat3.logicalSize += stat3.nullCount * nimble::NULL_SIZE;
  stat3.valueCount = columnSize;

  auto vector = vectorMaker_->rowVector({c1, c2, c3});
  auto stat0 = combineStats({stat1, stat2, stat3});
  stat0.valueCount = columnSize;
  verifyReturnedColumnStats(
      vector,
      {stat0,
       stat1,
       c1SubFieldStat1,
       c1SubFieldStat2,
       stat2,
       c2SubFieldStat1,
       c2SubFieldStat2,
       stat3,
       c3SubFieldStat1,
       c3SubFieldStat2});
}
