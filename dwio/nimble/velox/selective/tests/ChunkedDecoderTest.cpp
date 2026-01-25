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

#include "dwio/nimble/velox/selective/ChunkedDecoder.h"
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingLayout.h"
#include "dwio/nimble/encodings/EncodingSelection.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/encodings/NullableEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/index/tests/TabletIndexTestBase.h"
#include "dwio/nimble/velox/ChunkedStreamWriter.h"
#include "velox/common/base/Nulls.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/dwio/common/DirectBufferedInput.h"

#include <folly/executors/IOThreadPoolExecutor.h>
#include <gtest/gtest.h>

namespace facebook::nimble {

using namespace facebook::velox;

class ChunkedDecoderTestHelper {
 public:
  explicit ChunkedDecoderTestHelper(ChunkedDecoder* decoder)
      : decoder_(decoder) {
    NIMBLE_CHECK_NOT_NULL(decoder_);
  }

  bool ensureInput(int size) {
    return decoder_->ensureInput(size);
  }

  std::string_view inputData() const {
    return std::string_view(decoder_->inputData_, decoder_->inputSize_);
  }

  void advanceInputData(int size) {
    decoder_->inputData_ += size;
    decoder_->inputSize_ -= size;
  }

 private:
  ChunkedDecoder* const decoder_;
};

namespace {

class ChunkedDecoderTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    leafPool_ = memory::MemoryManager::getInstance()->addLeafPool();
  }

  MemoryPool& pool() {
    return *leafPool_;
  }

 private:
  std::shared_ptr<MemoryPool> leafPool_;
};

TEST_F(ChunkedDecoderTest, bufferedInput) {
  auto fileIoStats = std::make_shared<velox::io::IoStatistics>();
  constexpr size_t kFileSize = 1 << 12;
  constexpr size_t kLoadQuantum = 1 << 4;

  // Set up a file that has different content every load quantum for
  // the direct input stream.
  std::string fileContent{};
  size_t filePos = 0;
  size_t segmentLength;
  for (size_t segmentIdx = 0; filePos < kFileSize; ++segmentIdx) {
    segmentLength = std::min(kLoadQuantum, kFileSize - filePos);
    fileContent.append(segmentLength, 'a' + segmentIdx);
    filePos += segmentLength;
  }

  auto file = std::make_shared<InMemoryReadFile>(fileContent);
  dwio::common::ReaderOptions readerOpts{&pool()};
  readerOpts.setLoadQuantum(kLoadQuantum);
  auto executor = std::make_unique<folly::IOThreadPoolExecutor>(10, 10);
  auto input = std::make_unique<dwio::common::DirectBufferedInput>(
      file,
      dwio::common::MetricsLog::voidLog(),
      StringIdLease{},
      nullptr,
      StringIdLease{},
      fileIoStats,
      std::make_shared<filesystems::File::IoStats>(),
      executor.get(),
      readerOpts);

  auto chunkedDecoder = std::make_unique<nimble::ChunkedDecoder>(
      input->read(0, kFileSize, velox::dwio::common::LogType::TEST),
      false,
      nullptr,
      &pool());
  ChunkedDecoderTestHelper helper(chunkedDecoder.get());
  helper.ensureInput(kFileSize);
  ASSERT_EQ(helper.inputData(), fileContent);
}

TEST_F(ChunkedDecoderTest, ensureInput) {
  // Test that ensureInput correctly buffers data when reading in small chunks.
  // SeekableArrayInputStream with block_size=1 simulates reading one byte at a
  // time.
  std::string data = "abcdefgh";
  ChunkedDecoder decoder(
      std::make_unique<dwio::common::SeekableArrayInputStream>(
          data.data(), data.size(), /*block_size=*/1),
      false,
      nullptr,
      &pool());
  ChunkedDecoderTestHelper helper(&decoder);
  auto checkNext = [&](const std::string& expected) {
    helper.ensureInput(expected.size());
    ASSERT_GE(helper.inputData().size(), expected.size());
    ASSERT_EQ(helper.inputData().substr(0, expected.size()), expected);
    helper.advanceInputData(expected.size());
  };
  checkNext("ab");
  checkNext("c");
  checkNext("de");
  checkNext("f");
  checkNext("gh");
  ASSERT_TRUE(helper.inputData().empty());
  ASSERT_FALSE(helper.ensureInput(1));
}

// Test fixture for ChunkedDecoder data operations with parameterized
// stream index support.
class ChunkedDecoderDataTest : public index::test::TabletIndexTestBase,
                               public testing::WithParamInterface<bool> {
 protected:
  using Stream = index::test::TabletIndexTestBase::Stream;
  using KeyStream = index::test::TabletIndexTestBase::KeyStream;
  using Stripe = index::test::TabletIndexTestBase::Stripe;
  using IndexBuffers = index::test::TabletIndexTestBase::IndexBuffers;

  void SetUp() override {}

  bool useStreamIndex() const {
    return GetParam();
  }

  // Encodes integer values into a chunked stream format.
  // Returns the encoded stream data and chunk metadata (row counts and
  // offsets).
  struct ChunkInfo {
    uint32_t rowCount;
    uint32_t streamOffset;
  };

  template <typename T>
  std::pair<std::string, std::vector<ChunkInfo>> encodeChunkedStream(
      const std::vector<std::vector<T>>& chunks) {
    Buffer buffer{*pool_};
    std::string streamData;
    std::vector<ChunkInfo> chunkInfos;

    uint32_t currentOffset = 0;
    for (const auto& chunk : chunks) {
      // Encode each chunk using TrivialEncoding
      auto encodedChunk = encodeValues<T>(chunk, buffer);

      // Write chunk using ChunkedStreamWriter
      ChunkedStreamWriter writer{
          buffer, {.type = CompressionType::Uncompressed}};
      auto segments = writer.encode(encodedChunk);

      for (const auto& segment : segments) {
        streamData += segment;
      }

      chunkInfos.push_back({
          .rowCount = static_cast<uint32_t>(chunk.size()),
          .streamOffset = currentOffset,
      });
      currentOffset = streamData.size();
    }

    return {streamData, chunkInfos};
  }

  template <typename T>
  std::unique_ptr<EncodingSelectionPolicy<T>> createEncodingSelectionPolicy(
      EncodingType encodingType) {
    std::vector<std::optional<const EncodingLayout>> children;
    if (encodingType == EncodingType::Nullable) {
      // Nullable encoding needs child encodings for:
      // 0: nulls bitmap (bool)
      // 1: non-null values encoding
      children.emplace_back(EncodingLayout(
          EncodingType::Trivial, {}, CompressionType::Uncompressed));
      children.emplace_back(EncodingLayout(
          EncodingType::Trivial, {}, CompressionType::Uncompressed));
    }
    EncodingLayout layout(
        encodingType, {}, CompressionType::Uncompressed, std::move(children));
    return std::make_unique<ReplayedEncodingSelectionPolicy<T>>(
        std::move(layout),
        CompressionOptions{},
        [](DataType) -> std::unique_ptr<EncodingSelectionPolicyBase> {
          return nullptr;
        });
  }

  template <typename T>
  std::string_view encodeValues(const std::vector<T>& values, Buffer& buffer) {
    using physicalType = typename TypeTraits<T>::physicalType;
    EncodingSelectionResult selectionResult{
        .encodingType = EncodingType::Trivial};
    std::span<const physicalType> valuesSpan(
        reinterpret_cast<const physicalType*>(values.data()), values.size());
    Statistics<physicalType> stats =
        Statistics<physicalType>::create(valuesSpan);
    auto policy = createEncodingSelectionPolicy<T>(EncodingType::Trivial);
    EncodingSelection<T> selection{
        std::move(selectionResult), std::move(stats), std::move(policy)};
    return TrivialEncoding<T>::encode(selection, valuesSpan, buffer);
  }

  // Enum for null configurations in test cases.
  enum class NullConfig {
    NoNulls, // All values are non-null
    SomeNulls, // Some values are null (alternating pattern)
    AllNulls // All values are null
  };

  static std::string nullConfigName(NullConfig config) {
    switch (config) {
      case NullConfig::NoNulls:
        return "no_nulls";
      case NullConfig::SomeNulls:
        return "some_nulls";
      case NullConfig::AllNulls:
        return "all_nulls";
    }
    NIMBLE_UNREACHABLE();
  }

  // Encodes nullable integer values into a chunked stream format.
  // Uses std::optional<T> where std::nullopt represents null values.
  // Returns the encoded stream data and chunk metadata.
  template <typename T>
  std::pair<std::string, std::vector<ChunkInfo>> encodeNullableChunkedStream(
      const std::vector<std::vector<std::optional<T>>>& chunks) {
    Buffer buffer{*pool_};
    std::string streamData;
    std::vector<ChunkInfo> chunkInfos;

    uint32_t currentOffset = 0;
    for (const auto& chunk : chunks) {
      // Encode each chunk using NullableEncoding
      auto encodedChunk = encodeNullableValues<T>(chunk, buffer);

      // Write chunk using ChunkedStreamWriter
      ChunkedStreamWriter writer{
          buffer, {.type = CompressionType::Uncompressed}};
      auto segments = writer.encode(encodedChunk);

      for (const auto& segment : segments) {
        streamData += segment;
      }

      chunkInfos.push_back({
          .rowCount = static_cast<uint32_t>(chunk.size()),
          .streamOffset = currentOffset,
      });
      currentOffset = streamData.size();
    }

    return {streamData, chunkInfos};
  }

  // Encodes nullable values using std::optional<T>.
  // std::nullopt indicates a null value, otherwise the value is present.
  template <typename T>
  std::string_view encodeNullableValues(
      const std::vector<std::optional<T>>& values,
      Buffer& buffer) {
    using physicalType = typename TypeTraits<T>::physicalType;

    // Extract non-null values and build nulls bitmap
    // In Nimble: true = non-null, false = null
    // Use Vector<bool> instead of std::vector<bool> to work with std::span
    std::vector<physicalType> nonNullValues;
    Vector<bool> nulls{pool_.get()};
    nulls.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      nulls[i] = values[i].has_value();
      if (values[i].has_value()) {
        nonNullValues.push_back(
            reinterpret_cast<const physicalType&>(values[i].value()));
      }
    }

    EncodingSelectionResult selectionResult{
        .encodingType = EncodingType::Nullable};
    std::span<const physicalType> nonNullValuesSpan(
        nonNullValues.data(), nonNullValues.size());
    Statistics<physicalType> stats =
        Statistics<physicalType>::create(nonNullValuesSpan);
    auto policy = createEncodingSelectionPolicy<T>(EncodingType::Nullable);
    EncodingSelection<T> selection{
        std::move(selectionResult), std::move(stats), std::move(policy)};
    std::span<const bool> nullsSpan(nulls.data(), nulls.size());
    return NullableEncoding<T>::encodeNullable(
        selection, nonNullValuesSpan, nullsSpan, buffer);
  }

  // Generates test data based on NullConfig.
  // Returns a vector of std::optional<T> where std::nullopt represents null.
  // startIndex is the global index offset for value generation.
  template <typename T>
  static std::vector<std::optional<T>> generateNullableData(
      NullConfig config,
      uint32_t size,
      uint32_t startIndex = 0) {
    std::vector<std::optional<T>> data(size);
    switch (config) {
      case NullConfig::NoNulls:
        for (uint32_t i = 0; i < size; ++i) {
          data[i] = static_cast<T>(startIndex + i);
        }
        break;
      case NullConfig::SomeNulls:
        // Alternating pattern: odd global indices have values, even are null
        for (uint32_t i = 0; i < size; ++i) {
          const uint32_t globalIndex = startIndex + i;
          if (globalIndex % 2 == 0) {
            data[i] = std::nullopt;
          } else {
            data[i] = static_cast<T>(globalIndex);
          }
        }
        break;
      case NullConfig::AllNulls:
        for (uint32_t i = 0; i < size; ++i) {
          data[i] = std::nullopt;
        }
        break;
    }
    return data;
  }

  // Generates expected nulls based on NullConfig for a given range.
  // Returns a vector of booleans where true = non-null, false = null.
  static std::vector<bool> generateExpectedNulls(
      NullConfig config,
      uint32_t startIndex,
      uint32_t count) {
    std::vector<bool> nulls(count);
    switch (config) {
      case NullConfig::NoNulls:
        std::fill(nulls.begin(), nulls.end(), velox::bits::kNotNull);
        break;
      case NullConfig::SomeNulls:
        // Matches generateNullableData: odd indices are non-null
        for (uint32_t i = 0; i < count; ++i) {
          nulls[i] = (startIndex + i) % 2 != 0 ? velox::bits::kNotNull
                                               : velox::bits::kNull;
        }
        break;
      case NullConfig::AllNulls:
        std::fill(nulls.begin(), nulls.end(), velox::bits::kNull);
        break;
    }
    return nulls;
  }

  // Generates expected values from allValues starting at startIndex.
  // For non-null positions, returns the actual value; for null positions,
  // returns 0.
  template <typename T>
  static std::vector<T> generateExpectedValuesWithNulls(
      const std::vector<std::optional<T>>& allValues,
      uint32_t startIndex,
      uint32_t count) {
    std::vector<T> values(count);
    for (uint32_t i = 0; i < count; ++i) {
      const auto& val = allValues[startIndex + i];
      values[i] = val.has_value() ? val.value() : 0;
    }
    return values;
  }

  // Creates a stream index from chunk metadata for parameterized tests.
  // Returns nullptr when useStreamIndex() is false.
  std::shared_ptr<index::StreamIndex> createTestStreamIndex(
      const std::vector<ChunkInfo>& chunkInfos) {
    if (!useStreamIndex()) {
      return nullptr;
    }
    return createTestStreamIndexInternal(chunkInfos);
  }

  // Creates a stream index from chunk metadata (internal implementation).
  // Always returns a valid stream index regardless of test parameter.
  std::shared_ptr<index::StreamIndex> createTestStreamIndexInternal(
      const std::vector<ChunkInfo>& chunkInfos) {
    // Build stripe index data from chunk infos
    std::vector<int32_t> chunkRows;
    std::vector<uint32_t> chunkOffsets;
    uint32_t accumulatedRows = 0;
    for (const auto& info : chunkInfos) {
      accumulatedRows += info.rowCount;
      chunkRows.push_back(info.rowCount);
      chunkOffsets.push_back(info.streamOffset);
    }

    std::vector<std::string> indexColumns = {"col1"};
    std::string minKey = "aaa";
    std::vector<Stripe> stripes = {
        {.streams =
             {{.numChunks = static_cast<uint32_t>(chunkInfos.size()),
               .chunkRows = chunkRows,
               .chunkOffsets = chunkOffsets}},
         .keyStream = {
             .streamOffset = 0,
             .streamSize = 100,
             .stream =
                 {.numChunks = 1,
                  .chunkRows = {static_cast<int32_t>(accumulatedRows)},
                  .chunkOffsets = {0}},
             .chunkKeys = {"zzz"}}}};
    std::vector<int> stripeGroups = {1};

    auto testIndexBuffers =
        createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
    testStripeIndexGroup_ = createStripeIndexGroup(testIndexBuffers, 0);
    return testStripeIndexGroup_->createStreamIndex(0, 0);
  }

 private:
  std::shared_ptr<index::StripeIndexGroup> testStripeIndexGroup_;
};

TEST_P(ChunkedDecoderDataTest, skipSingleChunk) {
  struct TestCase {
    std::string name;
    uint32_t skipCount;
    // All values to encode (full chunk). Expected values/nulls after skip
    // are simply allValues[skipCount:].
    std::vector<std::optional<uint32_t>> allValues;
    // Expected error message when skip fails. Empty string means no error
    // expected. For skip with index vs without index, we may have different
    // error messages.
    std::string expectedErrorMessageWithIndex;
    std::string expectedErrorMessageWithoutIndex;
  };

  // Test with 100 values
  constexpr uint32_t kNumValues = 100;

  std::vector<TestCase> testCases;

  // Generate test cases for all combinations of skip positions and null
  // configurations
  for (auto nullConfig :
       //{NullConfig::NoNulls, NullConfig::SomeNulls, NullConfig::AllNulls}) {
       {NullConfig::SomeNulls}) {
    const auto configName = nullConfigName(nullConfig);
    auto allValues = generateNullableData<uint32_t>(nullConfig, kNumValues);

    testCases.push_back(
        {.name = "skip_0_" + configName,
         .skipCount = 0,
         .allValues = allValues,
         .expectedErrorMessageWithIndex = "",
         .expectedErrorMessageWithoutIndex = ""});
    testCases.push_back(
        {.name = "skip_50_" + configName,
         .skipCount = 50,
         .allValues = allValues,
         .expectedErrorMessageWithIndex = "",
         .expectedErrorMessageWithoutIndex = ""});
    testCases.push_back(
        {.name = "skip_second_last_" + configName,
         .skipCount = kNumValues - 2,
         .allValues = allValues,
         .expectedErrorMessageWithIndex = "",
         .expectedErrorMessageWithoutIndex = ""});
    testCases.push_back(
        {.name = "skip_last_" + configName,
         .skipCount = kNumValues - 1,
         .allValues = allValues,
         .expectedErrorMessageWithIndex = "",
         .expectedErrorMessageWithoutIndex = ""});
    testCases.push_back(
        {.name = "skip_all_" + configName,
         .skipCount = kNumValues,
         .allValues = allValues,
         .expectedErrorMessageWithIndex = "",
         .expectedErrorMessageWithoutIndex = ""});
    // Skip beyond available values - expect error
    testCases.push_back(
        {.name = "skip_beyond_" + configName,
         .skipCount = kNumValues + 1,
         .allValues = allValues,
         .expectedErrorMessageWithIndex =
             "Cannot skip beyond end of stream in stream 0",
         .expectedErrorMessageWithoutIndex = "Failed to read chunk header"});
    testCases.push_back(
        {.name = "skip_beyond_" + configName,
         .skipCount = kNumValues + 10,
         .allValues = allValues,
         .expectedErrorMessageWithIndex =
             "Cannot skip beyond end of stream in stream 0",
         .expectedErrorMessageWithoutIndex = "Failed to read chunk header"});
  }

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);

    // Encode stream from allValues in test case
    auto [streamData, chunkInfos] = encodeNullableChunkedStream<uint32_t>(
        std::vector<std::vector<std::optional<uint32_t>>>{testCase.allValues});

    auto streamIndex = createTestStreamIndex(chunkInfos);
    ChunkedDecoder decoder(
        std::make_unique<dwio::common::SeekableArrayInputStream>(
            streamData.data(), streamData.size()),
        false,
        streamIndex,
        pool_.get());

    // Check if we expect an error
    const auto& expectedErrorMessage = useStreamIndex()
        ? testCase.expectedErrorMessageWithIndex
        : testCase.expectedErrorMessageWithoutIndex;
    if (!expectedErrorMessage.empty()) {
      NIMBLE_ASSERT_THROW(
          decoder.skip(testCase.skipCount), expectedErrorMessage);
      continue;
    }

    // Skip values (including skip 0 to test that path)
    decoder.skip(testCase.skipCount);

    // Read remaining values (including 0 remaining to test that path)
    const uint32_t remainingCount =
        testCase.allValues.size() - testCase.skipCount;

    // Use decodeNullable for nullable data
    std::vector<uint64_t> nullBits(
        (remainingCount + 63) / 64, velox::bits::kNotNull64);
    std::vector<uint32_t> result(remainingCount);
    decoder.decodeNullable(
        nullBits.data(), result.data(), remainingCount, nullptr);

    // Verify against allValues[skipCount:]
    for (uint32_t i = 0; i < remainingCount; ++i) {
      const auto& expected = testCase.allValues[testCase.skipCount + i];
      const bool actualNonNull = velox::bits::isBitSet(nullBits.data(), i);
      const bool expectedNonNull = expected.has_value();

      EXPECT_EQ(actualNonNull, expectedNonNull)
          << "Null mismatch at position " << i;

      if (expectedNonNull) {
        EXPECT_EQ(result[i], expected.value())
            << "Value mismatch at position " << i;
      }
    }
  }
}

TEST_P(ChunkedDecoderDataTest, skipMultipleChunks) {
  struct TestCase {
    std::string name;
    uint32_t skipCount;
    uint32_t readCount;
    // Expected error message when skip fails (empty means no error)
    std::string expectedErrorMessageWithIndex;
    std::string expectedErrorMessageWithoutIndex;
  };

  // Chunk sizes: 50, 60, 40 (total 150)
  constexpr std::array<uint32_t, 3> kChunkSizes = {50, 60, 40};
  constexpr uint32_t kTotalValues = 150;

  std::vector<TestCase> testCases = {
      // Skip 0 values
      {.name = "skip_0",
       .skipCount = 0,
       .readCount = 10,
       .expectedErrorMessageWithIndex = "",
       .expectedErrorMessageWithoutIndex = ""},
      // Skip within first chunk
      {.name = "skip_within_first_chunk",
       .skipCount = 30,
       .readCount = 10,
       .expectedErrorMessageWithIndex = "",
       .expectedErrorMessageWithoutIndex = ""},
      // Skip to exact chunk boundary (end of chunk 0)
      {.name = "skip_to_chunk_boundary",
       .skipCount = 50,
       .readCount = 10,
       .expectedErrorMessageWithIndex = "",
       .expectedErrorMessageWithoutIndex = ""},
      // Skip across first chunk into second
      {.name = "skip_across_first_chunk",
       .skipCount = 80,
       .readCount = 40,
       .expectedErrorMessageWithIndex = "",
       .expectedErrorMessageWithoutIndex = ""},
      // Skip across two chunks (lands in third chunk)
      {.name = "skip_across_two_chunks",
       .skipCount = 120,
       .readCount = 20,
       .expectedErrorMessageWithIndex = "",
       .expectedErrorMessageWithoutIndex = ""},
      // Skip to last value
      {.name = "skip_to_last_value",
       .skipCount = kTotalValues - 1,
       .readCount = 1,
       .expectedErrorMessageWithIndex = "",
       .expectedErrorMessageWithoutIndex = ""},
      // Skip all values
      {.name = "skip_all",
       .skipCount = kTotalValues,
       .readCount = 0,
       .expectedErrorMessageWithIndex = "",
       .expectedErrorMessageWithoutIndex = ""},
      // Skip beyond available values
      {.name = "skip_beyond",
       .skipCount = kTotalValues + 1,
       .readCount = 0,
       .expectedErrorMessageWithIndex =
           "Cannot skip beyond end of stream in stream 0",
       .expectedErrorMessageWithoutIndex = "Failed to read chunk header"},
      // Skip way beyond available values
      {.name = "skip_way_beyond",
       .skipCount = kTotalValues + 100,
       .readCount = 0,
       .expectedErrorMessageWithIndex =
           "Cannot skip beyond end of stream in stream 0",
       .expectedErrorMessageWithoutIndex = "Failed to read chunk header"},
  };

  for (auto nullConfig : {NullConfig::NoNulls, NullConfig::SomeNulls}) {
    const auto configName = nullConfigName(nullConfig);

    // Generate chunks with the specified null config
    // Chunk 0: rows 0-49, Chunk 1: rows 50-109, Chunk 2: rows 110-149
    std::vector<std::vector<std::optional<uint32_t>>> chunks;
    std::vector<std::optional<uint32_t>> allValues;
    uint32_t globalIndex = 0;
    for (auto chunkSize : kChunkSizes) {
      auto chunkData =
          generateNullableData<uint32_t>(nullConfig, chunkSize, globalIndex);
      allValues.insert(allValues.end(), chunkData.begin(), chunkData.end());
      chunks.push_back(std::move(chunkData));
      globalIndex += chunkSize;
    }

    for (const auto& testCase : testCases) {
      SCOPED_TRACE(configName + "_" + testCase.name);

      auto [streamData, chunkInfos] =
          encodeNullableChunkedStream<uint32_t>(chunks);

      auto streamIndex = createTestStreamIndex(chunkInfos);
      ChunkedDecoder decoder(
          std::make_unique<dwio::common::SeekableArrayInputStream>(
              streamData.data(), streamData.size()),
          false,
          streamIndex,
          pool_.get());

      // Check if we expect an error
      const auto& expectedErrorMessage = useStreamIndex()
          ? testCase.expectedErrorMessageWithIndex
          : testCase.expectedErrorMessageWithoutIndex;
      if (!expectedErrorMessage.empty()) {
        NIMBLE_ASSERT_THROW(
            decoder.skip(testCase.skipCount), expectedErrorMessage);
        continue;
      }

      decoder.skip(testCase.skipCount);

      if (testCase.readCount == 0) {
        continue;
      }

      // Read and verify values
      std::vector<uint64_t> nullBits(
          (testCase.readCount + 63) / 64, velox::bits::kNotNull64);
      std::vector<uint32_t> result(testCase.readCount);
      decoder.decodeNullable(
          nullBits.data(), result.data(), testCase.readCount, nullptr);

      // Verify against allValues[skipCount:]
      for (uint32_t i = 0; i < testCase.readCount; ++i) {
        const auto& expected = allValues[testCase.skipCount + i];
        const bool actualNonNull = velox::bits::isBitSet(nullBits.data(), i);
        const bool expectedNonNull = expected.has_value();

        EXPECT_EQ(actualNonNull, expectedNonNull)
            << "Null mismatch at position " << i;

        if (expectedNonNull) {
          EXPECT_EQ(result[i], expected.value())
              << "Value mismatch at position " << i;
        }
      }
    }
  }
}

// Test skip within current chunk optimization in skipWithIndex
TEST_P(ChunkedDecoderDataTest, skipWithinCurrentChunk) {
  // Create test data: single chunk with 100 values
  std::vector<uint32_t> values(100);
  std::iota(values.begin(), values.end(), 0);

  auto [streamData, chunkInfos] =
      encodeChunkedStream<uint32_t>(std::vector<std::vector<uint32_t>>{values});

  auto streamIndex = createTestStreamIndex(chunkInfos);
  ChunkedDecoder decoder(
      std::make_unique<dwio::common::SeekableArrayInputStream>(
          streamData.data(), streamData.size()),
      false,
      streamIndex,
      pool_.get());

  // Read 10 values first

  // Read 10 values first to load the chunk and set remainingValues_
  std::vector<int32_t> result1(10);
  decoder.nextIndices(result1.data(), 10, nullptr);
  std::vector<int32_t> expected1(10);
  std::iota(expected1.begin(), expected1.end(), 0);
  EXPECT_EQ(result1, expected1);

  // Skip 20 values within the same chunk (tests skipWithIndex optimization)
  decoder.skip(20);

  // Read remaining 70 values
  std::vector<int32_t> result2(70);
  decoder.nextIndices(result2.data(), 70, nullptr);
  std::vector<int32_t> expected2(70);
  std::iota(expected2.begin(), expected2.end(), 30);
  EXPECT_EQ(result2, expected2);
}

TEST_P(ChunkedDecoderDataTest, skipEntireStream) {
  // Create test data: 2 chunks with 30, 20 uint32 values
  std::vector<std::vector<uint32_t>> chunks;
  uint32_t value = 0;
  for (int size : {30, 20}) {
    std::vector<uint32_t> chunk(size);
    std::iota(chunk.begin(), chunk.end(), value);
    value += size;
    chunks.push_back(std::move(chunk));
  }

  auto [streamData, chunkInfos] = encodeChunkedStream<uint32_t>(chunks);

  auto streamIndex = createTestStreamIndex(chunkInfos);
  ChunkedDecoder decoder(
      std::make_unique<dwio::common::SeekableArrayInputStream>(
          streamData.data(), streamData.size()),
      false,
      streamIndex,
      pool_.get());

  // Skip all 50 values
  decoder.skip(50);

  // Try to read after skipping entire stream - expect throw
  std::vector<int32_t> result(1);
  NIMBLE_ASSERT_THROW(
      decoder.nextIndices(result.data(), 1, nullptr),
      "Failed to read chunk header");
}

TEST_P(ChunkedDecoderDataTest, skipAndReadMixed) {
  // Create test data: 4 chunks with varying sizes
  std::vector<std::vector<uint32_t>> chunks;
  uint32_t value = 0;
  for (int size : {25, 35, 40, 50}) {
    std::vector<uint32_t> chunk(size);
    std::iota(chunk.begin(), chunk.end(), value);
    value += size;
    chunks.push_back(std::move(chunk));
  }

  auto [streamData, chunkInfos] = encodeChunkedStream<uint32_t>(chunks);

  auto streamIndex = createTestStreamIndex(chunkInfos);
  ChunkedDecoder decoder(
      std::make_unique<dwio::common::SeekableArrayInputStream>(
          streamData.data(), streamData.size()),
      false,
      streamIndex,
      pool_.get());

  // Read 10 values
  std::vector<int32_t> result1(10);
  decoder.nextIndices(result1.data(), 10, nullptr);
  std::vector<int32_t> expected1(10);
  std::iota(expected1.begin(), expected1.end(), 0);
  EXPECT_EQ(result1, expected1);

  // Skip 50 values (from position 10 to 60)
  decoder.skip(50);

  // Read 20 values (from position 60)
  std::vector<int32_t> result2(20);
  decoder.nextIndices(result2.data(), 20, nullptr);
  std::vector<int32_t> expected2(20);
  std::iota(expected2.begin(), expected2.end(), 60);
  EXPECT_EQ(result2, expected2);

  // Skip 30 values (from position 80 to 110)
  decoder.skip(30);

  // Read remaining 40 values (from position 110 to 150)
  std::vector<int32_t> result3(40);
  decoder.nextIndices(result3.data(), 40, nullptr);
  std::vector<int32_t> expected3(40);
  std::iota(expected3.begin(), expected3.end(), 110);
  EXPECT_EQ(result3, expected3);
}

// Test skip with index using simple values to verify index-based skip is
// triggered. This test creates multiple chunks and verifies that skipping
// across chunks or within chunks correctly calls skipWithIndex.
DEBUG_ONLY_TEST_P(ChunkedDecoderDataTest, skipChunkWithIndexCheck) {
  // Create 4 chunks with known values:
  // Chunk 0: [0, 1, 2, 3, 4] (5 values, rows 0-4)
  // Chunk 1: [5, 6, 7, 8, 9] (5 values, rows 5-9)
  // Chunk 2: [10, 11, 12, 13, 14] (5 values, rows 10-14)
  // Chunk 3: [15, 16, 17, 18, 19] (5 values, rows 15-19)
  // Total: 20 values
  std::vector<std::vector<uint32_t>> chunks = {
      {0, 1, 2, 3, 4},
      {5, 6, 7, 8, 9},
      {10, 11, 12, 13, 14},
      {15, 16, 17, 18, 19},
  };

  auto [streamData, chunkInfos] = encodeChunkedStream<uint32_t>(chunks);

  auto streamIndex = createTestStreamIndex(chunkInfos);
  ChunkedDecoder decoder(
      std::make_unique<dwio::common::SeekableArrayInputStream>(
          streamData.data(), streamData.size()),
      false,
      streamIndex,
      pool_.get());

  // Use TestValue to track skipWithIndex calls
  uint32_t skipWithIndexCount = 0;
  SCOPED_TESTVALUE_SET(
      "facebook::nimble::ChunkedDecoder::skipWithIndex",
      std::function<void(ChunkedDecoder*)>(
          [&](ChunkedDecoder*) { ++skipWithIndexCount; }));

  // Test scenario 1: Skip across first chunk (0->6, crosses chunk 0 to chunk 1)
  // This should trigger skipWithIndex when index is set
  decoder.skip(6);
  if (useStreamIndex()) {
    EXPECT_EQ(skipWithIndexCount, 1);
  } else {
    EXPECT_EQ(skipWithIndexCount, 0);
  }

  // Verify we're at value 6
  {
    std::vector<int32_t> result(1);
    decoder.nextIndices(result.data(), 1, nullptr);
    EXPECT_EQ(result[0], 6);
  }
  // Now at row 7

  // Test scenario 2: Skip within current chunk (7->9, stays in chunk 1)
  // This should still call skipWithIndex (but it optimizes internally)
  decoder.skip(2);
  if (useStreamIndex()) {
    EXPECT_EQ(skipWithIndexCount, 2);
  } else {
    EXPECT_EQ(skipWithIndexCount, 0);
  }

  // Verify we're at value 9
  {
    std::vector<int32_t> result(1);
    decoder.nextIndices(result.data(), 1, nullptr);
    EXPECT_EQ(result[0], 9);
  }
  // Now at row 10 (start of chunk 2)

  // Test scenario 3: Skip across multiple chunks (10->17, crosses chunk 2 to
  // chunk 3)
  decoder.skip(7);
  if (useStreamIndex()) {
    EXPECT_EQ(skipWithIndexCount, 3);
  } else {
    EXPECT_EQ(skipWithIndexCount, 0);
  }

  // Verify we're at value 17
  {
    std::vector<int32_t> result(1);
    decoder.nextIndices(result.data(), 1, nullptr);
    EXPECT_EQ(result[0], 17);
  }
  // Now at row 18

  // Test scenario 4: Skip to end of stream (18->20, lands at chunk boundary)
  decoder.skip(2);
  if (useStreamIndex()) {
    EXPECT_EQ(skipWithIndexCount, 4);
  } else {
    EXPECT_EQ(skipWithIndexCount, 0);
  }

  // Test scenario 5: Skip beyond available values - expect throw
  // Error messages differ between with/without index
  if (useStreamIndex()) {
    NIMBLE_ASSERT_THROW(decoder.skip(1), "Cannot skip beyond end of stream");
  } else {
    NIMBLE_ASSERT_THROW(decoder.skip(1), "Failed to read chunk header");
  }
}

INSTANTIATE_TEST_SUITE_P(
    ChunkedDecoderDataTests,
    ChunkedDecoderDataTest,
    testing::Bool(),
    [](const testing::TestParamInfo<bool>& info) {
      return info.param ? "WithStreamIndex" : "WithoutStreamIndex";
    });

// Fuzzer test for ChunkedDecoder with randomized operations.
// Uses two decoders (one with index, one without) and verifies they produce
// the same results for identical operations.
// Tests various combinations of:
// - Null configurations: all nulls, some nulls, no nulls
// - Chunk configurations: single chunk, multiple chunks
// - Operations: interleaved skip and read (multiple read APIs)
TEST_F(ChunkedDecoderDataTest, fuzzer) {
  constexpr uint32_t kNumIterations = 100;
  constexpr uint32_t kMinTotalRows = 10;
  constexpr uint32_t kMaxTotalRows = 1'000;
  constexpr uint32_t kMinChunks = 1;
  constexpr uint32_t kMaxChunks = 5;
  constexpr uint32_t kMinOpsPerIteration = 3;
  constexpr uint32_t kMaxOpsPerIteration = 10;

  std::mt19937 rng(42); // Fixed seed for reproducibility

  // Distribution for null configurations
  std::uniform_int_distribution<int> nullConfigDist(0, 2);
  // Distribution for number of chunks
  std::uniform_int_distribution<uint32_t> numChunksDist(kMinChunks, kMaxChunks);
  // Distribution for total rows
  std::uniform_int_distribution<uint32_t> totalRowsDist(
      kMinTotalRows, kMaxTotalRows);
  // Distribution for number of operations
  std::uniform_int_distribution<uint32_t> numOpsDist(
      kMinOpsPerIteration, kMaxOpsPerIteration);
  // Distribution for operation type: 0=skip, 1=decodeNullable, 2=nextIndices
  std::uniform_int_distribution<int> opTypeDist(0, 2);

  for (uint32_t iteration = 0; iteration < kNumIterations; ++iteration) {
    // Generate test configuration
    const auto nullConfig = static_cast<NullConfig>(nullConfigDist(rng));
    const uint32_t numChunks = numChunksDist(rng);
    const uint32_t totalRows = totalRowsDist(rng);

    SCOPED_TRACE(
        fmt::format(
            "iteration={} nullConfig={} numChunks={} totalRows={}",
            iteration,
            nullConfigName(nullConfig),
            numChunks,
            totalRows));

    // Generate chunk sizes that sum to totalRows
    std::vector<uint32_t> chunkSizes(numChunks);
    {
      uint32_t remainingRows = totalRows;
      for (uint32_t i = 0; i < numChunks - 1; ++i) {
        // Leave at least 1 row for remaining chunks
        const uint32_t maxChunkSize =
            remainingRows - (numChunks - i - 1); // Leave room for other chunks
        std::uniform_int_distribution<uint32_t> chunkSizeDist(1, maxChunkSize);
        chunkSizes[i] = chunkSizeDist(rng);
        remainingRows -= chunkSizes[i];
      }
      chunkSizes[numChunks - 1] = remainingRows;
    }

    // Generate test data
    std::vector<std::vector<std::optional<uint32_t>>> chunks;
    std::vector<std::optional<uint32_t>> allValues;
    uint32_t rowOffset = 0;
    for (uint32_t i = 0; i < numChunks; ++i) {
      auto chunkData =
          generateNullableData<uint32_t>(nullConfig, chunkSizes[i], rowOffset);
      allValues.insert(allValues.end(), chunkData.begin(), chunkData.end());
      chunks.push_back(std::move(chunkData));
      rowOffset += chunkSizes[i];
    }

    // Encode the data
    auto [streamData, chunkInfos] =
        encodeNullableChunkedStream<uint32_t>(chunks);

    // Create stream index for the decoder with index
    auto streamIndex = createTestStreamIndexInternal(chunkInfos);

    // Create two decoders: one with index, one without
    ChunkedDecoder decoderWithIndex(
        std::make_unique<dwio::common::SeekableArrayInputStream>(
            streamData.data(), streamData.size()),
        false,
        streamIndex,
        pool_.get());

    ChunkedDecoder decoderWithoutIndex(
        std::make_unique<dwio::common::SeekableArrayInputStream>(
            streamData.data(), streamData.size()),
        false,
        nullptr,
        pool_.get());

    // Generate and execute random operations on both decoders
    const uint32_t numOps = numOpsDist(rng);
    uint32_t currentPosition = 0;

    for (uint32_t opIdx = 0; opIdx < numOps; ++opIdx) {
      const uint32_t remainingRows = totalRows - currentPosition;
      if (remainingRows == 0) {
        break;
      }

      const int opType = opTypeDist(rng);
      std::uniform_int_distribution<uint32_t> countDist(1, remainingRows);
      const uint32_t count = countDist(rng);

      SCOPED_TRACE(
          fmt::format(
              "op={} opType={} count={} currentPosition={} remainingRows={}",
              opIdx,
              opType == 0 ? "skip"
                          : (opType == 1 ? "decodeNullable" : "nextIndices"),
              count,
              currentPosition,
              remainingRows));

      if (opType == 0) {
        // Skip operation - apply to both decoders
        decoderWithIndex.skip(count);
        decoderWithoutIndex.skip(count);
        currentPosition += count;
      } else if (opType == 1 || nullConfig != NullConfig::NoNulls) {
        // decodeNullable operation - apply to both decoders and compare
        std::vector<uint64_t> nullBitsWithIndex(
            (count + 63) / 64, velox::bits::kNotNull64);
        std::vector<uint32_t> resultWithIndex(count);
        decoderWithIndex.decodeNullable(
            nullBitsWithIndex.data(), resultWithIndex.data(), count, nullptr);

        std::vector<uint64_t> nullBitsWithoutIndex(
            (count + 63) / 64, velox::bits::kNotNull64);
        std::vector<uint32_t> resultWithoutIndex(count);
        decoderWithoutIndex.decodeNullable(
            nullBitsWithoutIndex.data(),
            resultWithoutIndex.data(),
            count,
            nullptr);

        // Compare results from both decoders
        for (uint32_t i = 0; i < count; ++i) {
          const bool nullWithIndex =
              velox::bits::isBitSet(nullBitsWithIndex.data(), i);
          const bool nullWithoutIndex =
              velox::bits::isBitSet(nullBitsWithoutIndex.data(), i);

          EXPECT_EQ(nullWithIndex, nullWithoutIndex)
              << "Null mismatch between decoders at position " << i
              << " (global " << (currentPosition + i) << ")";

          if (nullWithIndex) {
            EXPECT_EQ(resultWithIndex[i], resultWithoutIndex[i])
                << "Value mismatch between decoders at position " << i
                << " (global " << (currentPosition + i) << ")";
          }

          // Also verify against expected values
          const auto& expected = allValues[currentPosition + i];
          const bool expectedNonNull = expected.has_value();
          EXPECT_EQ(nullWithIndex, expectedNonNull)
              << "Null mismatch with expected at position " << i << " (global "
              << (currentPosition + i) << ")";

          if (expectedNonNull) {
            EXPECT_EQ(resultWithIndex[i], expected.value())
                << "Value mismatch with expected at position " << i
                << " (global " << (currentPosition + i) << ")";
          }
        }
        currentPosition += count;
      } else {
        // nextIndices operation for non-null data - apply to both decoders
        std::vector<int32_t> resultWithIndex(count);
        decoderWithIndex.nextIndices(resultWithIndex.data(), count, nullptr);

        std::vector<int32_t> resultWithoutIndex(count);
        decoderWithoutIndex.nextIndices(
            resultWithoutIndex.data(), count, nullptr);

        // Compare results from both decoders
        EXPECT_EQ(resultWithIndex, resultWithoutIndex)
            << "nextIndices results differ between decoders";

        // Also verify against expected values
        for (uint32_t i = 0; i < count; ++i) {
          const auto& expected = allValues[currentPosition + i];
          EXPECT_TRUE(expected.has_value())
              << "Expected non-null at position " << i;
          EXPECT_EQ(static_cast<uint32_t>(resultWithIndex[i]), expected.value())
              << "Value mismatch with expected at position " << i << " (global "
              << (currentPosition + i) << ")";
        }
        currentPosition += count;
      }
    }
  }
}

} // namespace

} // namespace facebook::nimble
