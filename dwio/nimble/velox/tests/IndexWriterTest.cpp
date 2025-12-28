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
#include <fmt/ranges.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/index/IndexConfig.h"
#include "dwio/nimble/index/KeyEncoder.h"
#include "dwio/nimble/velox/IndexWriter.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::nimble {
namespace {

class IndexWriterTest : public testing::Test,
                        public velox::test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

 protected:
  static constexpr std::string_view kCol0{"col0"};
  static constexpr std::string_view kCol1{"col1"};
  static constexpr std::string_view kCol2{"col2"};

  void SetUp() override {
    buffer_ = std::make_unique<Buffer>(*pool_);
    type_ = velox::ROW(
        {{std::string(kCol0), velox::VARCHAR()},
         {std::string(kCol1), velox::INTEGER()},
         {std::string(kCol2), velox::VARCHAR()}});
  }

  void TearDown() override {
    buffer_ = nullptr;
  }

  IndexConfig indexConfig(
      CompressionType compressionType = CompressionType::Uncompressed) const {
    return IndexConfig{
        .columns = {std::string(kCol1)},
        .encodingLayout = EncodingLayout{
            EncodingType::Trivial,
            compressionType,
            {
                nimble::EncodingLayout{
                    nimble::EncodingType::Trivial,
                    nimble::CompressionType::Uncompressed},
            }}};
  }

  std::unique_ptr<KeyEncoder> createKeyEncoder() const {
    const auto keyType = velox::ROW({{std::string(kCol1), velox::INTEGER()}});
    return KeyEncoder::create(
        {std::string(kCol1)},
        keyType,
        {velox::core::SortOrder{true, true}},
        pool_.get());
  }

  std::unique_ptr<Buffer> buffer_;
  velox::RowTypePtr type_;
};

TEST_F(IndexWriterTest, createWithNoConfig) {
  auto type = velox::ROW({{"col0", velox::INTEGER()}});
  auto writer = IndexWriter::create(std::nullopt, type, pool_.get());
  EXPECT_EQ(writer, nullptr);
}

TEST_F(IndexWriterTest, createWithValidConfig) {
  auto type = velox::ROW({{"col0", velox::INTEGER()}});
  for (auto compressionType :
       {CompressionType::Uncompressed,
        CompressionType::Zstd,
        CompressionType::MetaInternal}) {
    SCOPED_TRACE(fmt::format("compressionType: {}", compressionType));
    IndexConfig config{
        .columns = {"col0"},
        .encodingLayout =
            EncodingLayout{EncodingType::Trivial, compressionType}};
    auto writer = IndexWriter::create(config, type_, pool_.get());
    EXPECT_NE(writer, nullptr);
  }
}

TEST_F(IndexWriterTest, createWithInvalidConfig) {
  auto type = velox::ROW({{"col0", velox::INTEGER()}});

  {
    SCOPED_TRACE("non-existent column");
    IndexConfig config{
        .columns = {"non_existent_col"},
        .encodingLayout = EncodingLayout{
            EncodingType::Trivial, CompressionType::Uncompressed}};
    VELOX_ASSERT_USER_THROW(
        IndexWriter::create(config, type, pool_.get()), "Field not found");
  }

  {
    SCOPED_TRACE("non-trivial encoding");
    IndexConfig config{
        .columns = {"col0"},
        .encodingLayout = EncodingLayout{
            EncodingType::FixedBitWidth, CompressionType::Uncompressed}};
    NIMBLE_ASSERT_THROW(
        IndexWriter::create(config, type, pool_.get()),
        "Key stream encoding only supports Trivial encoding");
  }
}

TEST_F(IndexWriterTest, emptyWriteFinishStripe) {
  auto config = indexConfig();
  auto writer = IndexWriter::create(config, type_, pool_.get());
  ASSERT_NE(writer, nullptr);

  EXPECT_FALSE(writer->hasKeys());
  auto stream = writer->finishStripe(*buffer_);
  EXPECT_FALSE(stream.has_value());
}

TEST_F(IndexWriterTest, enforceKeyOrderInvalidWithinBatch) {
  for (bool enforceKeyOrder : {true, false}) {
    SCOPED_TRACE(fmt::format("enforceKeyOrder: {}", enforceKeyOrder));
    auto config = indexConfig();
    config.enforceKeyOrder = enforceKeyOrder;
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    auto batch = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"a", "b", "c", "d", "e"}),
         makeFlatVector<int32_t>({5, 4, 3, 2, 1}),
         makeFlatVector<velox::StringView>({"f", "g", "h", "i", "j"})});

    if (enforceKeyOrder) {
      NIMBLE_ASSERT_THROW(
          writer->write(batch, *buffer_),
          "Encoded keys must be in non-descending order");
    } else {
      EXPECT_NO_THROW(writer->write(batch, *buffer_));
      writer->encodeStream(*buffer_);
      auto stream = writer->finishStripe(*buffer_);
      ASSERT_TRUE(stream.has_value());
      ASSERT_EQ(stream->chunks.size(), 1);
      EXPECT_EQ(stream->chunks[0].rowCount, 5);
      ASSERT_EQ(stream->chunkKeys.size(), 1);
    }
  }
}

TEST_F(IndexWriterTest, enforceKeyOrderInvalidAcrossBatches) {
  for (bool enforceKeyOrder : {true, false}) {
    SCOPED_TRACE(fmt::format("enforceKeyOrder: {}", enforceKeyOrder));
    auto config = indexConfig();
    config.enforceKeyOrder = enforceKeyOrder;
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    auto batch1 = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"a", "b", "c"}),
         makeFlatVector<int32_t>({1, 2, 3}),
         makeFlatVector<velox::StringView>({"d", "e", "f"})});
    auto batch2 = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"g", "h", "i"}),
         makeFlatVector<int32_t>({2, 3, 4}),
         makeFlatVector<velox::StringView>({"j", "k", "l"})});

    writer->write(batch1, *buffer_);
    if (enforceKeyOrder) {
      NIMBLE_ASSERT_THROW(
          writer->write(batch2, *buffer_),
          "Encoded keys must be in non-descending order");
    } else {
      EXPECT_NO_THROW(writer->write(batch2, *buffer_));
      writer->encodeStream(*buffer_);
      auto stream = writer->finishStripe(*buffer_);
      ASSERT_TRUE(stream.has_value());
      EXPECT_EQ(stream->chunks.size(), 1);
      EXPECT_EQ(stream->chunks[0].rowCount, 6);
    }
  }
}

class IndexWriterDataTest
    : public IndexWriterTest,
      public ::testing::WithParamInterface<CompressionType> {
 protected:
  void SetUp() override {
    IndexWriterTest::SetUp();
    velox::VectorFuzzer::Options opts;
    opts.nullRatio = 0;
    fuzzer_ = std::make_unique<velox::VectorFuzzer>(opts, pool_.get());
  }

  void TearDown() override {
    fuzzer_ = nullptr;
    IndexWriterTest::TearDown();
  }

  CompressionType compressionType() const {
    return GetParam();
  }

  // Creates a row vector with the given index column values and
  // fuzzer-generated non-index columns.
  velox::RowVectorPtr makeInput(const std::vector<int32_t>& indexValues) {
    auto opts = fuzzer_->getOptions();
    opts.vectorSize = indexValues.size();
    fuzzer_->setOptions(opts);
    auto fuzzed = fuzzer_->fuzzInputFlatRow(type_);
    return makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {fuzzed->childAt(0),
         makeFlatVector<int32_t>(indexValues),
         fuzzed->childAt(2)});
  }

  // Creates a row vector containing only the key column for KeyEncoder.
  velox::RowVectorPtr makeKeyInput(int32_t keyValue) {
    return makeRowVector(
        {std::string(kCol1)}, {makeFlatVector<int32_t>({keyValue})});
  }

  std::unique_ptr<velox::VectorFuzzer> fuzzer_;
};

TEST_P(IndexWriterDataTest, writeAndFinishStripeNonChunked) {
  auto config = indexConfig(compressionType());
  auto keyEncoder = createKeyEncoder();

  struct {
    std::vector<std::vector<int32_t>> batches;

    std::string debugString() const {
      std::vector<std::string> batchStrs;
      batchStrs.reserve(batches.size());
      for (const auto& batch : batches) {
        batchStrs.push_back(fmt::format("[{}]", fmt::join(batch, ", ")));
      }
      return fmt::format("batches: [{}]", fmt::join(batchStrs, ", "));
    }

    size_t totalRows() const {
      size_t total = 0;
      for (const auto& batch : batches) {
        total += batch.size();
      }
      return total;
    }
  } testCases[] = {
      // Single batch, single row
      {{{1}}},
      // Single batch, multiple rows
      {{{1, 2, 3, 4, 5}}},
      // Two batches
      {{{1, 2, 3}, {4, 5, 6}}},
      // Three batches
      {{{1, 2}, {3, 4}, {5, 6}}},
      // Multiple batches with varying sizes
      {{{1}, {2, 3}, {4, 5, 6}, {7, 8, 9, 10}}},
      // Large single batch
      {{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}},
      // Many small batches
      {{{1}, {2}, {3}, {4}, {5}}},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    for (const auto& batchData : testCase.batches) {
      writer->write(makeInput(batchData), *buffer_);
    }
    EXPECT_TRUE(writer->hasKeys());

    writer->encodeStream(*buffer_);
    auto stream = writer->finishStripe(*buffer_);

    ASSERT_TRUE(stream.has_value());
    ASSERT_EQ(stream->chunks.size(), 1);
    EXPECT_EQ(stream->chunks[0].rowCount, testCase.totalRows());
    ASSERT_EQ(stream->chunkKeys.size(), 1);

    Vector<std::string_view> encodedKeys(pool_.get());
    Buffer keyBuffer(*pool_);
    // Verify firstKey matches the first value of the first batch.
    keyEncoder->encode(
        makeKeyInput(testCase.batches.front().front()),
        encodedKeys,
        [&keyBuffer](size_t size) { return keyBuffer.reserve(size); });
    EXPECT_EQ(stream->chunkKeys[0].firstKey, encodedKeys[0]);

    // Verify lastKey matches the last value of the last batch.
    encodedKeys.clear();
    keyEncoder->encode(
        makeKeyInput(testCase.batches.back().back()),
        encodedKeys,
        [&keyBuffer](size_t size) { return keyBuffer.reserve(size); });
    EXPECT_EQ(stream->chunkKeys[0].lastKey, encodedKeys[0]);
  }
}

TEST_P(IndexWriterDataTest, writeAndFinishStripeChunked) {
  auto config = indexConfig(compressionType());
  // Use small chunk sizes to force multiple chunks.
  config.minChunkRawSize = 1;
  config.maxChunkRawSize = 50;
  auto keyEncoder = createKeyEncoder();

  struct {
    std::vector<std::vector<int32_t>> batches;

    std::string debugString() const {
      std::vector<std::string> batchStrs;
      batchStrs.reserve(batches.size());
      for (const auto& batch : batches) {
        batchStrs.push_back(fmt::format("[{}]", fmt::join(batch, ", ")));
      }
      return fmt::format("batches: [{}]", fmt::join(batchStrs, ", "));
    }

    // Returns all index values flattened in order.
    std::vector<int32_t> allValues() const {
      std::vector<int32_t> values;
      for (const auto& batch : batches) {
        values.insert(values.end(), batch.begin(), batch.end());
      }
      return values;
    }
  } testCases[] = {
      // Single batch with multiple rows to force chunking.
      {{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}},
      // Multiple batches.
      {{{1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}}},
      // Many small batches.
      {{{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}}},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    for (const auto& batchData : testCase.batches) {
      writer->write(makeInput(batchData), *buffer_);
    }
    EXPECT_TRUE(writer->hasKeys());

    while (writer->encodeChunk(false, false, *buffer_)) {
    }

    while (writer->encodeChunk(false, true, *buffer_)) {
    }

    auto stream = writer->finishStripe(*buffer_);
    ASSERT_TRUE(stream.has_value());
    ASSERT_GE(stream->chunks.size(), 1);

    // Encode all keys upfront for verification.
    const auto allValues = testCase.allValues();
    Vector<std::string_view> allEncodedKeys(pool_.get());
    Buffer keyBuffer(*pool_);
    keyEncoder->encode(
        makeRowVector(
            {std::string(kCol1)}, {makeFlatVector<int32_t>(allValues)}),
        allEncodedKeys,
        [&keyBuffer](size_t size) { return keyBuffer.reserve(size); });

    // Verify chunk keys based on row offsets.
    ASSERT_EQ(stream->chunkKeys.size(), stream->chunks.size());
    uint64_t rowOffset = 0;
    for (size_t i = 0; i < stream->chunks.size(); ++i) {
      SCOPED_TRACE(fmt::format("chunk {}", i));
      const auto& chunk = stream->chunks[i];
      const auto& chunkKey = stream->chunkKeys[i];

      // Verify firstKey matches the value at chunk start offset.
      EXPECT_EQ(chunkKey.firstKey, allEncodedKeys[rowOffset]);

      // Verify lastKey matches the value at chunk end offset.
      EXPECT_EQ(
          chunkKey.lastKey, allEncodedKeys[rowOffset + chunk.rowCount - 1]);

      rowOffset += chunk.rowCount;
    }
    // Verify all rows are accounted for.
    EXPECT_EQ(rowOffset, allValues.size());
  }
}

TEST_P(IndexWriterDataTest, indexChunkSizeControl) {
  struct TestCase {
    // Number of input rows.
    size_t numInputRows;
    uint64_t minChunkRawSize;
    uint64_t maxChunkRawSize;
    // Whether to use ensureFullChunks in encodeChunk calls.
    bool ensureFullChunks;
    // Expected number of chunks before last chunk call.
    size_t expectedChunksBeforeLast;
    // Expected total chunk count after all encoding.
    size_t expectedChunks;

    std::string debugString() const {
      return fmt::format(
          "numInputRows: {}, minChunkRawSize: {}, maxChunkRawSize: {}, ensureFullChunks: {}",
          numInputRows,
          minChunkRawSize,
          maxChunkRawSize,
          ensureFullChunks);
    }
  } testCases[] = {
      // ===== Input smaller than thresholds =====
      // Single row input - always produces single chunk.
      {
          .numInputRows = 1,
          .minChunkRawSize = 1 << 20,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 0,
          .expectedChunks = 1,
      },
      // Small input with large thresholds - single chunk.
      {
          .numInputRows = 10,
          .minChunkRawSize = 1 << 20,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 0,
          .expectedChunks = 1,
      },
      // Medium input with large thresholds - single chunk.
      {
          .numInputRows = 100,
          .minChunkRawSize = 1 << 20,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 0,
          .expectedChunks = 1,
      },
      // Large input with very large thresholds - single chunk.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 1 << 20,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 0,
          .expectedChunks = 1,
      },
      // Large input with ensureFullChunks=true and large thresholds.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 1 << 20,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = true,
          .expectedChunksBeforeLast = 0,
          .expectedChunks = 1,
      },
      // Single row with minimum possible min threshold.
      {
          .numInputRows = 1,
          .minChunkRawSize = 1,
          .maxChunkRawSize = 1 << 20,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 1,
          .expectedChunks = 1,
      },
      // ensureFullChunks with single row.
      {
          .numInputRows = 1,
          .minChunkRawSize = 1 << 20,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = true,
          .expectedChunksBeforeLast = 0,
          .expectedChunks = 1,
      },
      // Medium input with ensureFullChunks - single chunk.
      {
          .numInputRows = 100,
          .minChunkRawSize = 1 << 20,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = true,
          .expectedChunksBeforeLast = 0,
          .expectedChunks = 1,
      },
      // ===== Input larger than max threshold - forces multiple chunks =====

      // Large input with tiny thresholds - many chunks.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 1,
          .maxChunkRawSize = 100,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 250,
          .expectedChunks = 250,
      },
      // Large input with tiny thresholds and ensureFullChunks.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 1,
          .maxChunkRawSize = 100,
          .ensureFullChunks = true,
          .expectedChunksBeforeLast = 249,
          .expectedChunks = 250,
      },
      // Large input with small max threshold.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 1,
          .maxChunkRawSize = 20,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 1'000,
          .expectedChunks = 1'000,
      },
      // Large input with small max threshold and ensureFullChunks.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 1,
          .maxChunkRawSize = 20,
          .ensureFullChunks = true,
          .expectedChunksBeforeLast = 1'000,
          .expectedChunks = 1'000,
      },
      // Large input with medium thresholds.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 500,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 44,
          .expectedChunks = 44,
      },
      // Large input with medium thresholds and ensureFullChunks.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 500,
          .ensureFullChunks = true,
          .expectedChunksBeforeLast = 43,
          .expectedChunks = 44,
      },
      // Very large input with small thresholds.
      {
          .numInputRows = 10'000,
          .minChunkRawSize = 50,
          .maxChunkRawSize = 200,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 1'111,
          .expectedChunks = 1'112,
      },
      // Very large input with small thresholds and ensureFullChunks.
      {
          .numInputRows = 10'000,
          .minChunkRawSize = 50,
          .maxChunkRawSize = 200,
          .ensureFullChunks = true,
          .expectedChunksBeforeLast = 1'111,
          .expectedChunks = 1'112,
      },

      // ===== Input larger than min but smaller than max threshold =====

      // Input between min and max - may or may not produce chunks.
      {
          .numInputRows = 100,
          .minChunkRawSize = 50,
          .maxChunkRawSize = 10'000,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 1,
          .expectedChunks = 1,
      },
      // Input between min and max with ensureFullChunks.
      {
          .numInputRows = 100,
          .minChunkRawSize = 50,
          .maxChunkRawSize = 10'000,
          .ensureFullChunks = true,
          .expectedChunksBeforeLast = 0,
          .expectedChunks = 1,
      },
      // Medium input with tight min/max range.
      {
          .numInputRows = 500,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 200,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 56,
          .expectedChunks = 56,
      },
      // Medium input with tight min/max range and ensureFullChunks.
      {
          .numInputRows = 500,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 200,
          .ensureFullChunks = true,
          .expectedChunksBeforeLast = 55,
          .expectedChunks = 56,
      },
      // ===== Edge cases with equal min and max =====

      // Min equals max - fixed chunk size.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 100,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 0,
          .expectedChunks = 250,
      },
      // Min equals max with ensureFullChunks.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 100,
          .ensureFullChunks = true,
          .expectedChunksBeforeLast = 0,
          .expectedChunks = 250,
      },
      // Tiny equal thresholds.
      {
          .numInputRows = 100,
          .minChunkRawSize = 1,
          .maxChunkRawSize = 20,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 100,
          .expectedChunks = 100,
      },

      // ===== Different input sizes with same thresholds =====

      // Varying input sizes with fixed medium thresholds.
      {
          .numInputRows = 50,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 1'000,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 1,
          .expectedChunks = 2,
      },
      {
          .numInputRows = 200,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 1'000,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 5,
          .expectedChunks = 5,
      },
      {
          .numInputRows = 500,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 1'000,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 11,
          .expectedChunks = 11,
      },
      {
          .numInputRows = 2'000,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 1'000,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 43,
          .expectedChunks = 43,
      },
      {
          .numInputRows = 5'000,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 1'000,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 107,
          .expectedChunks = 107,
      },

      // ===== Zero min threshold =====

      // Zero min with small max.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 0,
          .maxChunkRawSize = 100,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 250,
          .expectedChunks = 250,
      },
      // Zero min with small max and ensureFullChunks.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 0,
          .maxChunkRawSize = 100,
          .ensureFullChunks = true,
          .expectedChunksBeforeLast = 249,
          .expectedChunks = 250,
      },
      // Zero min with large max.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 0,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = false,
          .expectedChunksBeforeLast = 1,
          .expectedChunks = 1,
      },
  };

  auto keyEncoder = createKeyEncoder();

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());

    // Generate input.
    std::vector<int32_t> input;
    input.reserve(testCase.numInputRows);
    for (size_t i = 0; i < testCase.numInputRows; ++i) {
      input.push_back(static_cast<int32_t>(i));
    }

    auto config = indexConfig(compressionType());
    config.minChunkRawSize = testCase.minChunkRawSize;
    config.maxChunkRawSize = testCase.maxChunkRawSize;

    auto writer = IndexWriter::create(config, type_, pool_.get());

    writer->write(makeInput(input), *buffer_);
    ASSERT_TRUE(writer->hasKeys());

    // Encode chunks with ensureFullChunks setting (not last chunk).
    size_t chunksBeforeLast = 0;
    while (writer->encodeChunk(
        testCase.ensureFullChunks, /*lastChunk=*/false, *buffer_)) {
      ++chunksBeforeLast;
    }

    ASSERT_EQ(chunksBeforeLast, testCase.expectedChunksBeforeLast);

    // Encode remaining chunks with lastChunk=true.
    while (writer->encodeChunk(
        /*ensureFullChunks=*/false, /*lastChunk=*/true, *buffer_)) {
    }

    auto stream = writer->finishStripe(*buffer_);
    ASSERT_TRUE(stream.has_value());
    ASSERT_EQ(stream->chunks.size(), testCase.expectedChunks);

    // Encode all keys upfront for verification.
    Vector<std::string_view> allEncodedKeys(pool_.get());
    Buffer keyBuffer(*pool_);
    keyEncoder->encode(
        makeRowVector({std::string(kCol1)}, {makeFlatVector<int32_t>(input)}),
        allEncodedKeys,
        [&keyBuffer](size_t size) { return keyBuffer.reserve(size); });

    // Verify chunk keys based on row offsets.
    ASSERT_EQ(stream->chunkKeys.size(), stream->chunks.size());
    uint64_t rowOffset = 0;
    for (size_t i = 0; i < stream->chunks.size(); ++i) {
      SCOPED_TRACE(fmt::format("chunk {}", i));
      const auto& chunk = stream->chunks[i];
      const auto& chunkKey = stream->chunkKeys[i];
      ASSERT_FALSE(chunk.content.empty());

      // Verify firstKey matches the value at chunk start offset.
      ASSERT_EQ(chunkKey.firstKey, allEncodedKeys[rowOffset]);

      // Verify lastKey matches the value at chunk end offset.
      ASSERT_EQ(
          chunkKey.lastKey, allEncodedKeys[rowOffset + chunk.rowCount - 1]);

      rowOffset += chunk.rowCount;
    }
    // Verify all rows are accounted for.
    ASSERT_EQ(rowOffset, input.size());
  }
}

TEST_P(IndexWriterDataTest, multiColumnIndex) {
  auto type = velox::ROW({
      {"col0", velox::INTEGER()},
      {"col1", velox::VARCHAR()},
  });
  IndexConfig config{
      .columns = {"col0", "col1"},
      .encodingLayout = EncodingLayout{
          EncodingType::Trivial,
          compressionType(),
          {
              nimble::EncodingLayout{
                  nimble::EncodingType::Trivial,
                  nimble::CompressionType::Uncompressed},
          }}};
  auto writer = IndexWriter::create(config, type, pool_.get());

  auto batch = makeRowVector(
      {"col0", "col1"},
      {makeFlatVector<int32_t>({1, 2, 3}),
       makeFlatVector<velox::StringView>({"a", "b", "c"})});

  writer->write(batch, *buffer_);
  writer->encodeStream(*buffer_);
  auto stream = writer->finishStripe(*buffer_);

  ASSERT_TRUE(stream.has_value());
  EXPECT_EQ(stream->chunks.size(), 1);
  EXPECT_EQ(stream->chunks[0].rowCount, 3);
}

TEST_P(IndexWriterDataTest, reset) {
  auto config = indexConfig(compressionType());
  auto writer = IndexWriter::create(config, type_, pool_.get());
  ASSERT_NE(writer, nullptr);

  writer->write(makeInput({1, 2, 3}), *buffer_);
  EXPECT_TRUE(writer->hasKeys());

  writer->reset();
  EXPECT_FALSE(writer->hasKeys());
  auto stream = writer->finishStripe(*buffer_);
  EXPECT_FALSE(stream.has_value());
}

TEST_P(IndexWriterDataTest, close) {
  auto config = indexConfig(compressionType());
  auto writer = IndexWriter::create(config, type_, pool_.get());
  ASSERT_NE(writer, nullptr);

  writer->write(makeInput({1, 2, 3}), *buffer_);
  EXPECT_TRUE(writer->hasKeys());

  writer->close();
  EXPECT_FALSE(writer->hasKeys());
}

INSTANTIATE_TEST_SUITE_P(
    IndexWriterDataTestSuite,
    IndexWriterDataTest,
    ::testing::Values(CompressionType::Uncompressed, CompressionType::Zstd),
    [](const ::testing::TestParamInfo<CompressionType>& info) {
      return toString(info.param);
    });

} // namespace
} // namespace facebook::nimble
