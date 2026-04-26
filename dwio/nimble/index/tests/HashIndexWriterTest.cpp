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

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <limits>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/index/HashIndex.h"
#include "dwio/nimble/index/HashIndexUtils.h"
#include "dwio/nimble/index/HashIndexWriter.h"
#include "dwio/nimble/index/IndexConfig.h"
#include "dwio/nimble/index/tests/HashIndexTestUtils.h"
#include "dwio/nimble/tablet/HashIndexGenerated.h"
#include "velox/common/base/tests/GTestUtils.h"

#include "velox/common/memory/Memory.h"
#include "velox/serializers/KeyEncoder.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::nimble::index::test {
namespace {

class HashIndexWriterTestBase : public velox::test::VectorTestBase {
 protected:
  static constexpr std::string_view kCol0{"col0"};
  static constexpr std::string_view kCol1{"col1"};
  static constexpr std::string_view kCol2{"col2"};

  void setupType() {
    type_ = velox::ROW(
        {{std::string(kCol0), velox::VARCHAR()},
         {std::string(kCol1), velox::INTEGER()},
         {std::string(kCol2), velox::VARCHAR()}});
  }

  static HashIndexConfig config(std::vector<std::string> columns) {
    return HashIndexConfig{.columns = std::move(columns)};
  }

  // Creates a row vector with the given key column values.
  // col0 (VARCHAR) is derived from keyValues for uniqueness.
  velox::RowVectorPtr makeInput(const std::vector<int32_t>& keyValues) {
    col0Strings_.clear();
    col0Strings_.reserve(keyValues.size());
    std::vector<velox::StringView> col0Vals;
    std::vector<velox::StringView> col2Vals;
    col0Vals.reserve(keyValues.size());
    col2Vals.reserve(keyValues.size());
    for (const auto val : keyValues) {
      col0Strings_.emplace_back(fmt::format("s_{}", val));
      col0Vals.emplace_back(col0Strings_.back());
      col2Vals.emplace_back("b");
    }
    return makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>(col0Vals),
         makeFlatVector<int32_t>(keyValues),
         makeFlatVector<velox::StringView>(col2Vals)});
  }

  // Builds a mock createMetadataSection function that stores sections
  // in memory and returns MetadataSection with sequential offsets.
  struct MockSectionStore {
    std::vector<std::string> sections;
    uint64_t nextOffset{0};

    CreateMetadataSectionFn createFn() {
      return [this](std::string_view content) -> MetadataSection {
        const auto offset = nextOffset;
        const auto size = content.size();
        sections.emplace_back(content);
        nextOffset += size;
        return MetadataSection{
            offset, static_cast<uint32_t>(size), CompressionType::Uncompressed};
      };
    }
  };

  // Captures the directory written by close().
  static WriteOptionalSectionFn writeFn(std::string& output) {
    return [&output](const std::string& /*name*/, std::string_view content) {
      output = std::string(content);
    };
  }

  // No-op write function for tests that don't inspect the output.
  static WriteOptionalSectionFn noopWriteFn() {
    return [](const std::string&, std::string_view) {};
  }

  // Validates the HashIndexDirectory has the expected indices with the
  // expected column names per index.
  static void validateDirectory(
      const std::string& directory,
      const std::vector<std::vector<std::string>>& expectedColumns) {
    const auto* root = flatbuffers::GetRoot<serialization::HashIndexDirectory>(
        directory.data());
    ASSERT_NE(root, nullptr);
    const auto* indices = root->indices();
    ASSERT_NE(indices, nullptr);
    ASSERT_EQ(indices->size(), expectedColumns.size());

    for (size_t idx = 0; idx < expectedColumns.size(); ++idx) {
      SCOPED_TRACE(fmt::format("index {}", idx));
      const auto* section = indices->Get(idx);
      ASSERT_NE(section, nullptr);
      const auto* columns = section->index_columns();
      ASSERT_NE(columns, nullptr);
      ASSERT_EQ(columns->size(), expectedColumns[idx].size());
      for (size_t i = 0; i < expectedColumns[idx].size(); ++i) {
        EXPECT_EQ(columns->Get(i)->string_view(), expectedColumns[idx][i]);
      }
    }
  }

  // Validates the HashIndex FlatBuffer has the expected row/key counts
  // and valid partition structure. If expectedNumPartitions is 0, only
  // validates at least one partition exists.
  static void validateIndex(
      const std::string& indexSection,
      uint64_t expectedNumKeys,
      uint32_t expectedNumPartitions = 0) {
    const auto* hashIndex =
        flatbuffers::GetRoot<serialization::HashIndex>(indexSection.data());
    ASSERT_NE(hashIndex, nullptr);
    EXPECT_EQ(hashIndex->row_count(), expectedNumKeys);
    EXPECT_EQ(hashIndex->num_keys(), expectedNumKeys);
    EXPECT_GT(hashIndex->num_buckets(), 0u);

    const auto* partitionSections = hashIndex->partition_sections();
    ASSERT_NE(partitionSections, nullptr);
    if (expectedNumPartitions > 0) {
      EXPECT_EQ(partitionSections->size(), expectedNumPartitions);
    } else {
      EXPECT_GE(partitionSections->size(), 1u);
    }

    const auto* partitionStartBuckets = hashIndex->partition_start_buckets();
    ASSERT_NE(partitionStartBuckets, nullptr);
    EXPECT_EQ(partitionStartBuckets->size(), partitionSections->size());
  }

  velox::RowTypePtr type_;
  std::vector<std::string> col0Strings_;
};

// Non-parameterized fixture for tests with specific column configs.
class HashIndexWriterTest : public HashIndexWriterTestBase,
                            public testing::Test {
 public:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

 protected:
  void SetUp() override {
    setupType();
  }
};

// Parameterized fixture: runs with single-column and composite-key configs.
struct HashIndexWriterTestParam {
  std::vector<std::string> columns;

  std::string debugString() const {
    return fmt::format("columns: [{}]", fmt::join(columns, ", "));
  }
};

class HashIndexWriterParamTest
    : public HashIndexWriterTestBase,
      public testing::TestWithParam<HashIndexWriterTestParam> {
 public:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

 protected:
  void SetUp() override {
    setupType();
  }

  HashIndexConfig config() const {
    return HashIndexConfig{.columns = GetParam().columns};
  }
};

INSTANTIATE_TEST_SUITE_P(
    SingleAndCompositeKey,
    HashIndexWriterParamTest,
    testing::Values(
        HashIndexWriterTestParam{{"col1"}},
        HashIndexWriterTestParam{{"col0", "col1"}}),
    [](const testing::TestParamInfo<HashIndexWriterTestParam>& info) {
      return fmt::format("columns_{}", fmt::join(info.param.columns, "_"));
    });

TEST_F(HashIndexWriterTest, createWithEmptyConfigs) {
  auto writer = HashIndexWriter::create({}, type_, pool_.get());
  EXPECT_EQ(writer, nullptr);
}

TEST_F(HashIndexWriterTest, createWithValidConfig) {
  auto writer = HashIndexWriter::create({config({"col1"})}, type_, pool_.get());
  EXPECT_NE(writer, nullptr);
}

TEST_F(HashIndexWriterTest, createWithNullPool) {
  NIMBLE_ASSERT_THROW(
      HashIndexWriter::create({config({"col1"})}, type_, nullptr),
      "memory pool must not be null");
}

TEST_F(HashIndexWriterTest, createWithInvalidConfig) {
  {
    SCOPED_TRACE("empty columns");
    HashIndexConfig config{.columns = {}};
    NIMBLE_ASSERT_THROW(
        HashIndexWriter::create({config}, type_, pool_.get()),
        "Hash index must have at least one column");
  }

  {
    SCOPED_TRACE("zero load factor");
    HashIndexConfig config{.columns = {"col1"}, .loadFactor = 0.0f};
    NIMBLE_ASSERT_THROW(
        HashIndexWriter::create({config}, type_, pool_.get()),
        "Load factor must be positive");
  }

  {
    SCOPED_TRACE("negative load factor");
    HashIndexConfig config{.columns = {"col1"}, .loadFactor = -0.5f};
    NIMBLE_ASSERT_THROW(
        HashIndexWriter::create({config}, type_, pool_.get()),
        "Load factor must be positive");
  }

  {
    SCOPED_TRACE("load factor > 1.0");
    HashIndexConfig config{.columns = {"col1"}, .loadFactor = 1.5f};
    NIMBLE_ASSERT_THROW(
        HashIndexWriter::create({config}, type_, pool_.get()),
        "Load factor must be at most 1.0");
  }

  {
    SCOPED_TRACE("non-existent column");
    HashIndexConfig config{.columns = {"non_existent"}};
    VELOX_ASSERT_USER_THROW(
        HashIndexWriter::create({config}, type_, pool_.get()),
        "Field not found");
  }
}

TEST_F(HashIndexWriterTest, createWithDuplicateColumns) {
  // Exact duplicate columns.
  NIMBLE_ASSERT_THROW(
      HashIndexWriter::create(
          {config({"col1"}), config({"col1"})}, type_, pool_.get()),
      "Duplicate hash index columns");

  // Duplicate multi-column index.
  NIMBLE_ASSERT_THROW(
      HashIndexWriter::create(
          {config({"col0", "col1"}), config({"col0", "col1"})},
          type_,
          pool_.get()),
      "Duplicate hash index columns");

  // Same columns in different order are allowed (different index).
  auto writer = HashIndexWriter::create(
      {config({"col0", "col1"}), config({"col1", "col0"})}, type_, pool_.get());
  EXPECT_NE(writer, nullptr);
}

TEST_F(HashIndexWriterTest, createMultipleIndices) {
  auto writer = HashIndexWriter::create(
      {config({"col1"}), config({"col0", "col2"})}, type_, pool_.get());
  ASSERT_NE(writer, nullptr);

  HashIndexWriterTestHelper helper(writer.get());
  EXPECT_EQ(helper.numAccumulators(), 2);
}

TEST_P(HashIndexWriterParamTest, writeEmptyVector) {
  auto writer = HashIndexWriter::create({config()}, type_, pool_.get());
  ASSERT_NE(writer, nullptr);

  auto emptyBatch = makeInput({});
  writer->write(emptyBatch);

  HashIndexWriterTestHelper helper(writer.get());
  EXPECT_EQ(helper.numRows(), 0);
  EXPECT_EQ(helper.numEntries(0), 0);

  MockSectionStore store;
  bool writeCalled = false;
  writer->close(store.createFn(), [&](const std::string&, std::string_view) {
    writeCalled = true;
  });
  EXPECT_FALSE(writeCalled);
  EXPECT_TRUE(store.sections.empty());
}

TEST_P(HashIndexWriterParamTest, writeSingleBatch) {
  auto writer = HashIndexWriter::create({config()}, type_, pool_.get());
  ASSERT_NE(writer, nullptr);

  writer->write(makeInput({1, 2, 3, 4, 5}));

  HashIndexWriterTestHelper helper(writer.get());
  EXPECT_EQ(helper.numRows(), 5);
  EXPECT_EQ(helper.numEntries(0), 5);

  MockSectionStore store;
  std::string directory;
  writer->close(store.createFn(), writeFn(directory));

  const auto& columns = GetParam().columns;
  validateDirectory(directory, {columns});
  validateIndex(store.sections.back(), 5, /*expectedNumPartitions=*/1);
}

TEST_P(HashIndexWriterParamTest, writeMultipleBatches) {
  auto writer = HashIndexWriter::create({config()}, type_, pool_.get());
  ASSERT_NE(writer, nullptr);

  writer->write(makeInput({1, 2, 3}));
  writer->write(makeInput({4, 5}));
  writer->write(makeInput({6, 7, 8, 9}));

  HashIndexWriterTestHelper helper(writer.get());
  EXPECT_EQ(helper.numRows(), 9);
  EXPECT_EQ(helper.numEntries(0), 9);

  MockSectionStore store;
  std::string directory;
  writer->close(store.createFn(), writeFn(directory));

  const auto& columns = GetParam().columns;
  validateDirectory(directory, {columns});
  validateIndex(store.sections.back(), 9, /*expectedNumPartitions=*/1);
}

TEST_P(HashIndexWriterParamTest, writeWithEmptyBatchesMixed) {
  auto writer = HashIndexWriter::create({config()}, type_, pool_.get());
  ASSERT_NE(writer, nullptr);

  writer->write(makeInput({}));
  writer->write(makeInput({1, 2}));
  writer->write(makeInput({}));
  writer->write(makeInput({3}));
  writer->write(makeInput({}));

  HashIndexWriterTestHelper helper(writer.get());
  EXPECT_EQ(helper.numRows(), 3);
  EXPECT_EQ(helper.numEntries(0), 3);

  MockSectionStore store;
  std::string directory;
  writer->close(store.createFn(), writeFn(directory));

  const auto& columns = GetParam().columns;
  validateDirectory(directory, {columns});
  validateIndex(store.sections.back(), 3, /*expectedNumPartitions=*/1);
}

TEST_F(HashIndexWriterTest, rejectNullKeys) {
  struct TestCase {
    std::string name;
    std::vector<std::string> columns;
    std::vector<std::optional<velox::StringView>> col0;
    std::vector<std::optional<int32_t>> col1;
    bool expectThrow;

    std::string debugString() const {
      return name;
    }
  };

  const std::vector<TestCase> testCases = {
      {
          "single null in integer key",
          {"col1"},
          {{"a"}, {"b"}, {"c"}},
          {1, std::nullopt, 3},
          true,
      },
      {
          "all nulls in integer key",
          {"col1"},
          {{"a"}, {"b"}, {"c"}},
          {std::nullopt, std::nullopt, std::nullopt},
          true,
      },
      {
          "null in varchar key of composite index",
          {"col0", "col1"},
          {std::nullopt, {"b"}, {"c"}},
          {1, 2, 3},
          true,
      },
      {
          "no nulls passes",
          {"col1"},
          {{"a"}, {"b"}, {"c"}},
          {1, 2, 3},
          false,
      },
      {
          "no nulls composite passes",
          {"col0", "col1"},
          {{"a"}, {"b"}, {"c"}},
          {1, 2, 3},
          false,
      },
  };

  for (const auto& tc : testCases) {
    SCOPED_TRACE(tc.debugString());
    auto writer =
        HashIndexWriter::create({config(tc.columns)}, type_, pool_.get());
    auto batch = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeNullableFlatVector<velox::StringView>(tc.col0),
         makeNullableFlatVector<int32_t>(tc.col1),
         makeFlatVector<velox::StringView>({"d", "e", "f"})});

    if (tc.expectThrow) {
      NIMBLE_ASSERT_USER_THROW(
          writer->write(batch),
          "Null value not allowed in hash index key column");
    } else {
      EXPECT_NO_THROW(writer->write(batch));
    }
  }
}

TEST_P(HashIndexWriterParamTest, writeAfterCloseThrows) {
  auto writer = HashIndexWriter::create({config()}, type_, pool_.get());
  writer->write(makeInput({1}));

  MockSectionStore store;
  writer->close(store.createFn(), noopWriteFn());

  NIMBLE_ASSERT_THROW(writer->write(makeInput({2})), "Cannot write after");
}

TEST_P(HashIndexWriterParamTest, doubleCloseThrows) {
  auto writer = HashIndexWriter::create({config()}, type_, pool_.get());
  writer->write(makeInput({1}));

  MockSectionStore store;
  writer->close(store.createFn(), noopWriteFn());

  NIMBLE_ASSERT_THROW(
      writer->close(store.createFn(), noopWriteFn()), "close() already called");
}

TEST_P(HashIndexWriterParamTest, rowCountOverflow) {
  auto writer = HashIndexWriter::create({config()}, type_, pool_.get());
  ASSERT_NE(writer, nullptr);

  HashIndexWriterTestHelper helper(writer.get());
  helper.setNumRows(std::numeric_limits<uint32_t>::max() - 5);

  NIMBLE_ASSERT_USER_THROW(
      writer->write(makeInput({1, 2, 3, 4, 5, 6, 7, 8, 9, 10})),
      "Hash index row count exceeds uint32 limit");
}

TEST_F(HashIndexWriterTest, multipleIndices) {
  auto writer = HashIndexWriter::create(
      {config({"col1"}), config({"col0"})}, type_, pool_.get());
  writer->write(makeInput({1, 2, 3}));

  MockSectionStore store;
  std::string directory;
  writer->close(store.createFn(), writeFn(directory));

  validateDirectory(directory, {{"col1"}, {"col0"}});
  // store has: partition for index 0, index 0, partition for index 1, index 1.
  ASSERT_GE(store.sections.size(), 4);
  // Index sections are at positions 1 and 3 (after their partitions).
  validateIndex(store.sections[1], 3, /*expectedNumPartitions=*/1);
  validateIndex(store.sections[3], 3, /*expectedNumPartitions=*/1);
}

TEST_F(HashIndexWriterTest, withBloomFilter) {
  struct TestCase {
    std::string name;
    std::optional<BloomFilterConfig> bloomFilter;
    bool expectBloomFilter;

    std::string debugString() const {
      return name;
    }
  };

  const std::vector<TestCase> testCases = {
      {"enabled", BloomFilterConfig{}, true},
      {"disabled", std::nullopt, false},
  };

  for (const auto& tc : testCases) {
    SCOPED_TRACE(tc.debugString());
    HashIndexConfig indexConfig{
        .columns = {"col1"},
        .bloomFilter = tc.bloomFilter,
    };
    auto writer = HashIndexWriter::create({indexConfig}, type_, pool_.get());
    writer->write(makeInput({1, 2, 3, 4, 5}));

    MockSectionStore store;
    std::string directory;
    writer->close(store.createFn(), writeFn(directory));

    const auto* hashIndex = flatbuffers::GetRoot<serialization::HashIndex>(
        store.sections.back().data());
    ASSERT_NE(hashIndex, nullptr);
    if (tc.expectBloomFilter) {
      EXPECT_NE(hashIndex->bloom_filter(), nullptr);
    } else {
      EXPECT_EQ(hashIndex->bloom_filter(), nullptr);
    }
  }
}

TEST_F(HashIndexWriterTest, withPartitioning) {
  HashIndexConfig config{
      .columns = {"col1"},
      // Very small partition size to force multiple partitions.
      .maxPartitionSizeBytes = 32,
  };
  auto writer = HashIndexWriter::create({config}, type_, pool_.get());

  // Write enough rows to exceed partition size.
  std::vector<int32_t> values;
  for (int i = 0; i < 100; ++i) {
    values.push_back(i);
  }
  writer->write(makeInput(values));

  MockSectionStore store;
  std::string directory;
  writer->close(store.createFn(), writeFn(directory));

  validateDirectory(directory, {{"col1"}});
  // The last section is the HashIndex; preceding sections are partitions.
  // With 100 keys and 32-byte partition limit, expect multiple partitions.
  const auto* hashIndex = flatbuffers::GetRoot<serialization::HashIndex>(
      store.sections.back().data());
  ASSERT_NE(hashIndex, nullptr);
  ASSERT_NE(hashIndex->partition_sections(), nullptr);
  EXPECT_GT(hashIndex->partition_sections()->size(), 1u);
  validateIndex(store.sections.back(), 100);
}

TEST_F(HashIndexWriterTest, loadFactorVariations) {
  struct TestParam {
    float loadFactor;

    std::string debugString() const {
      return fmt::format("loadFactor: {}", loadFactor);
    }
  };

  std::vector<TestParam> testSettings = {{0.1f}, {0.5f}, {0.7f}, {1.0f}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    HashIndexConfig config{
        .columns = {"col1"}, .loadFactor = testData.loadFactor};
    auto writer = HashIndexWriter::create({config}, type_, pool_.get());

    std::vector<int32_t> values;
    for (int i = 0; i < 50; ++i) {
      values.push_back(i);
    }
    writer->write(makeInput(values));

    MockSectionStore store;
    std::string directory;
    writer->close(store.createFn(), writeFn(directory));

    validateDirectory(directory, {{"col1"}});
    validateIndex(store.sections.back(), 50, /*expectedNumPartitions=*/1);
  }
}

TEST_P(HashIndexWriterParamTest, duplicateKeysAllowed) {
  auto writer = HashIndexWriter::create({config()}, type_, pool_.get());

  writer->write(makeInput({1, 1, 2, 2, 3}));

  HashIndexWriterTestHelper helper(writer.get());
  EXPECT_EQ(helper.numRows(), 5);
  EXPECT_EQ(helper.numEntries(0), 5);

  MockSectionStore store;
  std::string directory;
  writer->close(store.createFn(), writeFn(directory));

  validateDirectory(directory, {GetParam().columns});
  validateIndex(store.sections.back(), 5, /*expectedNumPartitions=*/1);
}

} // namespace
} // namespace facebook::nimble::index::test
