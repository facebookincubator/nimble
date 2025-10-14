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

#include <utility>

#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/StreamChunker.h"
#include "dwio/nimble/velox/StreamData.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/SharedArbitrator.h"

namespace facebook::nimble {
template <typename T>
void populateData(Vector<T>& vec, const std::vector<T>& data) {
  vec.reserve(data.size());
  for (const auto& item : data) {
    vec.push_back(item);
  }
}

template <typename T>
std::vector<T> toVector(std::string_view data) {
  const T* chunkData = reinterpret_cast<const T*>(data.data());
  return std::vector<T>(chunkData, chunkData + data.size() / sizeof(T));
}

// Expected chunk result structure
template <typename T>
struct ExpectedChunk {
  explicit ExpectedChunk(
      std::vector<T> chunkData,
      std::vector<bool> chunkNonNulls = {},
      bool hasNulls = false,
      size_t extraMemory = 0)
      : chunkData{std::move(chunkData)},
        chunkNonNulls{std::move(chunkNonNulls)},
        hasNulls{hasNulls},
        extraMemory{extraMemory} {}
  std::vector<T> chunkData;
  std::vector<bool> chunkNonNulls;
  bool hasNulls;
  size_t extraMemory;
};

class StreamChunkerTestsBase : public ::testing::Test {
 protected:
  // Helper method to validate chunk results against expected data
  template <typename T>
  void validateChunk(
      const StreamData& stream,
      std::unique_ptr<StreamChunker> chunker,
      const std::vector<ExpectedChunk<T>>& expectedChunks,
      const std::vector<T>& expectedRetainedData,
      const std::vector<bool>& expectedRetainedNonNulls = {}) {
    int chunkIndex = 0;
    while (const auto chunkView = chunker->next()) {
      ASSERT_LT(chunkIndex, expectedChunks.size());
      ASSERT_TRUE(chunkView.has_value());
      auto chunkData = toVector<T>(chunkView->data());
      const auto& expectedChunk = expectedChunks[chunkIndex];
      auto expectedChunkData = expectedChunk.chunkData;
      auto expectedChunkNonNulls = expectedChunk.chunkNonNulls;
      EXPECT_THAT(chunkData, ::testing::ElementsAreArray(expectedChunkData));
      EXPECT_THAT(
          chunkView->nonNulls(),
          ::testing::ElementsAreArray(expectedChunkNonNulls));
      EXPECT_EQ(chunkView->hasNulls(), expectedChunk.hasNulls);
      EXPECT_EQ(chunkView->extraMemory(), expectedChunk.extraMemory);
      chunkIndex++;
    }
    ASSERT_EQ(chunkIndex, expectedChunks.size());

    // Simulates chunker going out of scope
    chunker.reset();

    // Validate buffer is properly compacted to only retain expected data
    EXPECT_THAT(
        toVector<T>(stream.data()),
        ::testing::ElementsAreArray(expectedRetainedData));
    EXPECT_THAT(
        stream.nonNulls(),
        ::testing::ElementsAreArray(expectedRetainedNonNulls));
  }

  static void SetUpTestCase() {
    velox::memory::SharedArbitrator::registerFactory();
    velox::memory::MemoryManager::Options options;
    options.arbitratorKind = "SHARED";
    velox::memory::MemoryManager::testingSetInstance(options);
  }

  void SetUp() override {
    rootPool_ = velox::memory::memoryManager()->addRootPool("default_root");
    leafPool_ = rootPool_->addLeafChild("default_leaf");
    schemaBuilder_ = std::make_unique<SchemaBuilder>();
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> leafPool_;
  std::unique_ptr<SchemaBuilder> schemaBuilder_;
};

TEST_F(StreamChunkerTestsBase, getNewBufferCapacityTest) {
  uint64_t maxChunkSize = 1024;
  uint64_t currentCapacity = 512;
  uint64_t newBufferSize = 10;

  // currentCapacity  < maxChunkSize
  EXPECT_EQ(
      getNewBufferCapacity<int32_t>(
          /*maxChunkSize=*/maxChunkSize,
          /*currentCapacity=*/currentCapacity,
          /*newBufferSize=*/newBufferSize),
      currentCapacity);

  maxChunkSize = 1024;
  currentCapacity = 2048;
  newBufferSize = 10;
  // maxChunkSize + (currentCapacity - maxChunkSize) * 0.5 > (newBufferSize * 4)
  EXPECT_EQ(
      getNewBufferCapacity<int32_t>(
          /*maxChunkSize=*/maxChunkSize,
          /*currentCapacity=*/currentCapacity,
          /*newBufferSize=*/newBufferSize),
      1536);

  maxChunkSize = 1024;
  currentCapacity = 2048;
  newBufferSize = 500;
  // maxChunkSize + (currentCapacity - maxChunkSize) * 0.5 < (newBufferSize * 4)
  EXPECT_EQ(
      getNewBufferCapacity<int32_t>(
          /*maxChunkSize=*/maxChunkSize,
          /*currentCapacity=*/currentCapacity,
          /*newBufferSize=*/newBufferSize),
      2000);
}

TEST_F(StreamChunkerTestsBase, omitStreamTest) {
  auto scalarTypeBuilder =
      schemaBuilder_->createScalarTypeBuilder(ScalarKind::Bool);
  auto descriptor = &scalarTypeBuilder->scalarDescriptor();
  NullsStreamData nullsStream(*leafPool_, *descriptor);

  // Setup test data with some nulls
  std::vector<bool> nonNullsTestData = {
      true, false, true, true, false, true, false, true, false, false};
  nullsStream.ensureNullsCapacity(
      /*mayHaveNulls=*/true, static_cast<uint32_t>(nonNullsTestData.size()));
  auto& nonNulls = nullsStream.mutableNonNulls();
  populateData(nonNulls, nonNullsTestData);

  // Null Stream Basic Test
  {
    // Non-empty null stream with size above minChunkSize
    EXPECT_FALSE(omitStream(
        nullsStream,
        /*minChunkSize=*/4,
        /*isNullStream=*/true,
        /*hasEmptyStreamContent=*/false,
        /*isLastChunk=*/false));

    // Non-empty null stream with size below minChunkSize
    EXPECT_TRUE(omitStream(
        nullsStream,
        /*minChunkSize=*/11,
        /*isNullStream=*/true,
        /*hasEmptyStreamContent=*/true,
        /*isLastChunk=*/false));

    // Non-empty null stream with size below minChunkSize and last chunk
    EXPECT_FALSE(omitStream(
        nullsStream,
        /*minChunkSize=*/11,
        /*isNullStream=*/true,
        /*hasEmptyStreamContent=*/true,
        /*isLastChunk=*/true));

    // Non-empty null stream with size below minChunkSize
    // and non-empty written stream content
    EXPECT_FALSE(omitStream(
        nullsStream,
        /*minChunkSize=*/11,
        /*isNullStream=*/true,
        /*hasEmptyStreamContent=*/false,
        /*isLastChunk=*/false));
  }

  scalarTypeBuilder =
      schemaBuilder_->createScalarTypeBuilder(ScalarKind::Int32);
  descriptor = &scalarTypeBuilder->scalarDescriptor();
  NullableContentStreamData<int32_t> nullableContentStream(
      *leafPool_, *descriptor);
  std::vector<int32_t> testData = {1, 2, 3, 4, 5};
  // Populate data using ensureNullsCapacity and manual data insertion
  nullableContentStream.ensureNullsCapacity(
      /*mayHaveNulls=*/true, static_cast<uint32_t>(nonNullsTestData.size()));
  populateData(nullableContentStream.mutableData(), testData);
  populateData(nullableContentStream.mutableNonNulls(), nonNullsTestData);

  // Nullable Content Stream Basic Test
  {
    // Non-empty stream with size above minChunkSize
    EXPECT_FALSE(omitStream(
        nullableContentStream,
        /*minChunkSize=*/4,
        /*isNullStream=*/true,
        /*hasEmptyStreamContent=*/false,
        /*isLastChunk=*/false));

    // Non-empty stream with size below minChunkSize
    EXPECT_TRUE(omitStream(
        nullableContentStream,
        /*minChunkSize=*/100,
        /*isNullStream=*/true,
        /*hasEmptyStreamContent=*/true,
        /*isLastChunk=*/false));

    // Non-empty stream with size below minChunkSize and last chunk
    EXPECT_FALSE(omitStream(
        nullableContentStream,
        /*minChunkSize=*/100,
        /*isNullStream=*/true,
        /*hasEmptyStreamContent=*/true,
        /*isLastChunk=*/true));

    // Non-empty stream with size below minChunkSize
    // and non-empty written stream content
    EXPECT_FALSE(omitStream(
        nullableContentStream,
        /*minChunkSize=*/100,
        /*isNullStream=*/true,
        /*hasEmptyStreamContent=*/false,
        /*isLastChunk=*/false));
  }

  //   NullsStreamChunker respects omitStream
  {
    // Test case where omitStream returns true - chunker should return nullopt
    auto chunker = getStreamChunker(
        nullsStream,
        /*maxChunkSize=*/4,
        /*minChunkSize=*/11, // Set high minChunkSize to trigger omitStream
        /*ensureFullChunks=*/false,
        /*emptyStreamContent=*/true,
        /*isNullStream=*/true,
        /*isLastChunk=*/false);

    // Validate that the correct chunker type was created
    ASSERT_NE(dynamic_cast<NullsStreamChunker*>(chunker.get()), nullptr);

    // Should return nullopt because omitStream should return true
    auto result = chunker->next();
    EXPECT_FALSE(result.has_value());

    // Test case where omitStream returns false
    auto chunker2 = getStreamChunker(
        nullsStream,
        /*maxChunkSize=*/4,
        /*minChunkSize=*/4, // Set reasonable minChunkSize
        /*ensureFullChunks=*/false,
        /*emptyStreamContent=*/false,
        /*isNullStream=*/true,
        /*isLastChunk=*/false);

    // Should return valid chunks because omitStream should return false
    auto result2 = chunker2->next();
    EXPECT_TRUE(result2.has_value());
  }

  //   NullableContentStreamChunker respects omitStream
  {
    // Test case where omitStream returns true - chunker should return nullopt
    auto chunker = getStreamChunker(
        nullableContentStream,
        /*maxChunkSize=*/20,
        /*minChunkSize=*/100, // Set high minChunkSize to trigger omitStream
        /*ensureFullChunks=*/false,
        /*emptyStreamContent=*/true,
        /*isNullStream=*/false,
        /*isLastChunk=*/false);

    // Validate that the correct chunker type was created
    ASSERT_NE(
        dynamic_cast<NullableContentStreamChunker<int32_t>*>(chunker.get()),
        nullptr);

    // Should return nullopt because omitStream should return true
    auto result = chunker->next();
    EXPECT_FALSE(result.has_value());

    // Test case where omitStream returns false
    auto chunker2 = getStreamChunker(
        nullableContentStream,
        /*maxChunkSize=*/20,
        /*minChunkSize=*/4, // Set reasonable minChunkSize
        /*ensureFullChunks=*/false,
        /*emptyStreamContent=*/false,
        /*isNullStream=*/true,
        /*isLastChunk=*/false);

    // Should return valid chunks because omitStream should return false
    auto result2 = chunker2->next();
    EXPECT_TRUE(result2.has_value());
  }
}

TEST_F(StreamChunkerTestsBase, ContentStreamIntChunking) {
  auto scalarTypeBuilder =
      schemaBuilder_->createScalarTypeBuilder(ScalarKind::Int32);
  auto descriptor = &scalarTypeBuilder->scalarDescriptor();
  ContentStreamData<int32_t> stream(*leafPool_, *descriptor);

  // Test 1: Do not return last chunk.
  {
    std::vector<int32_t> testData = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    auto& data = stream.mutableData();
    populateData(data, testData);
    uint64_t maxChunkSize = 18; // 4.5 elements
    uint64_t minChunkSize = 12; // 3 elements
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/false,
        /*emptyStreamContent=*/false,
        /*isNullStream=*/false,
        /*isLastChunk=*/false);
    // Validate that the correct chunker type was created
    ASSERT_NE(
        dynamic_cast<ContentStreamChunker<int32_t>*>(chunker.get()), nullptr);
    std::vector<ExpectedChunk<int32_t>> expectedChunks = {
        ExpectedChunk<int32_t>(
            /*chunkData=*/{1, 2, 3, 4}), // First chunk: 4 elements
        ExpectedChunk<int32_t>(
            /*chunkData=*/{5, 6, 7, 8}) // Second chunk: 4 elements
        // Remaining 2 elements won't be chunked due to minChunkSize
    };
    std::vector<int32_t> expectedRetainedData = {9, 10};
    validateChunk<int32_t>(
        stream, std::move(chunker), expectedChunks, expectedRetainedData);
  }

  // Test 2: Nothing returned when ensureFullChunks = true and remaining data in
  // StreamData is less than maxChunkSize.
  {
    uint64_t maxChunkSize = 18; // 4.5 elements
    uint64_t minChunkSize = 12; // 3 elements
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/true,
        /*emptyStreamContent=*/false,
        /*isNullStream=*/false,
        /*isLastChunk=*/false);
    // Validate that the correct chunker type was created
    ASSERT_NE(
        dynamic_cast<ContentStreamChunker<int32_t>*>(chunker.get()), nullptr);
    std::vector<ExpectedChunk<int32_t>> expectedChunks = {};
    std::vector<int32_t> expectedRetainedData = {9, 10};
    validateChunk<int32_t>(
        stream, std::move(chunker), expectedChunks, expectedRetainedData);
  }

  // Test 3: Return last chunk.
  {
    auto& data = stream.mutableData();
    data.clear(); // Clear existing data first
    std::vector<int32_t> testData = {7, 8, 9, 10, 11};
    populateData(data, testData);

    uint64_t maxChunkSize = 12; // 3 elements
    uint64_t minChunkSize = 0; // 1 elements
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/true,
        /*emptyStreamContent=*/false,
        /*isNullStream=*/false,
        /*isLastChunk=*/true);
    // Validate that the correct chunker type was created
    ASSERT_NE(
        dynamic_cast<ContentStreamChunker<int32_t>*>(chunker.get()), nullptr);

    std::vector<ExpectedChunk<int32_t>> expectedChunks = {
        ExpectedChunk<int32_t>(
            /*chunkData=*/{7, 8, 9}), // First chunk: 3 elements
        ExpectedChunk<int32_t>(
            /*chunkData=*/{10, 11}), // Second chunk: 2 elements (lastChunk=true
                                     // allows smaller chunks)
    };
    std::vector<int32_t> expectedRetainedData = {};
    validateChunk<int32_t>(
        stream, std::move(chunker), expectedChunks, expectedRetainedData);
  }
}

TEST_F(StreamChunkerTestsBase, ContentStreamStringChunking) {
  auto scalarTypeBuilder =
      schemaBuilder_->createScalarTypeBuilder(ScalarKind::String);
  auto descriptor = &scalarTypeBuilder->scalarDescriptor();
  ContentStreamData<std::string_view> stream(*leafPool_, *descriptor);
  uint64_t maxChunkSize = 55;
  uint64_t minChunkSize = 40;
  // Test 1: Basic string chunking. lastChunk=false
  {
    std::vector<std::string_view> testData = {
        "short", "a_longer_string", "x", "medium_size", "tiny"};
    auto& data = stream.mutableData();
    populateData(data, testData);

    // Calculate extra memory for string content
    for (const auto& str : testData) {
      stream.extraMemory() += str.size();
    }

    // Total string content sizes:
    // "short"(5) + "a_longer_string"(15) + "x"(1) + "medium_size"(11) +
    // "tiny"(4) = 36 bytes
    ASSERT_EQ(stream.extraMemory(), 36);
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/false,
        /*emptyStreamContent=*/false,
        /*isNullStream=*/false,
        /*isLastChunk=*/false);

    std::vector<ExpectedChunk<std::string_view>> expectedChunks = {
        ExpectedChunk<std::string_view>(
            {"short", "a_longer_string"},
            {},
            false,
            20), // "short"(5) + "a_longer_string"(15) = 20 bytes extra
        ExpectedChunk<std::string_view>(
            {"x", "medium_size"},
            {},
            false,
            12) // "x"(1) + "medium_size"(11) = 12 bytes extra
        // "tiny"(4) remains due to minChunkSize constraint
    };

    std::vector<std::string_view> expectedRetainedData = {"tiny"};
    validateChunk<std::string_view>(
        stream, std::move(chunker), expectedChunks, expectedRetainedData);
  }

  // Test 2: Remaining chunk with lastChunk=true
  {
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/true,
        /*emptyStreamContent=*/false,
        /*isNullStream=*/false,
        /*isLastChunk=*/true);
    // Validate that the correct chunker type was created
    ASSERT_NE(
        dynamic_cast<ContentStreamChunker<std::string_view>*>(chunker.get()),
        nullptr);

    std::vector<ExpectedChunk<std::string_view>> expectedChunks = {
        ExpectedChunk<std::string_view>(
            {"tiny"},
            {},
            false,
            4), // "tiny"(4) = 4 bytes extra
    };
    std::vector<std::string_view> expectedRetainedData = {};
    validateChunk<std::string_view>(
        stream, std::move(chunker), expectedChunks, expectedRetainedData);
  }

  // Test 3: String chunking with lastChunk=true on fresh data
  {
    auto& data = stream.mutableData();
    data.clear(); // Clear existing data first
    stream.extraMemory() = 0; // Reset extra memory

    std::vector<std::string_view> testData = {
        "hello", "world", "test", "final"};
    populateData(data, testData);

    // Calculate extra memory for string content
    for (const auto& str : testData) {
      stream.extraMemory() += str.size();
    }

    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/true,
        /*emptyStreamContent=*/false,
        /*isNullStream=*/false,
        /*isLastChunk=*/true);
    // Validate that the correct chunker type was created
    ASSERT_NE(
        dynamic_cast<ContentStreamChunker<std::string_view>*>(chunker.get()),
        nullptr);

    std::vector<ExpectedChunk<std::string_view>> expectedChunks = {
        ExpectedChunk<std::string_view>(
            {"hello", "world"},
            {},
            false,
            10), // "hello"(5) + "world"(5) = 10 bytes extra
        ExpectedChunk<std::string_view>(
            {"test", "final"},
            {},
            false,
            9) // "test"(4) + "final"(5) = 9 bytes extra
    };

    // All data should be processed with lastChunk=true
    std::vector<std::string_view> expectedRetainedData = {};
    validateChunk<std::string_view>(
        stream, std::move(chunker), expectedChunks, expectedRetainedData);
  }

  // Test 4: String chunking with ensureFullChunks_
  {
    minChunkSize = 2;
    std::vector<std::string_view> testData = {"last_string"};
    auto& data = stream.mutableData();
    populateData(data, testData);

    // Calculate extra memory for string content
    for (const auto& str : testData) {
      stream.extraMemory() += str.size();
    }

    // Total string content sizes: "last_string"(11) = 11 bytes
    ASSERT_EQ(stream.extraMemory(), 11);
    auto memoryUsed = stream.memoryUsed();

    // maxChunkSize is greater than available memory
    auto chunker = getStreamChunker(
        stream,
        memoryUsed + 1,
        minChunkSize,
        /*ensureFullChunks=*/true,
        /*emptyStreamContent=*/false,
        /*isNullStream=*/false,
        /*isLastChunk=*/false);

    std::vector<ExpectedChunk<std::string_view>> expectedChunks = {};
    std::vector<std::string_view> expectedRetainedData = {"last_string"};
    validateChunk<std::string_view>(
        stream, std::move(chunker), expectedChunks, expectedRetainedData);

    // When maxChunkSize is equal to available memory, chunk is returned.
    auto chunker2 = getStreamChunker(
        stream,
        memoryUsed,
        minChunkSize,
        /*ensureFullChunks=*/true,
        /*emptyStreamContent=*/false,
        /*isNullStream=*/false,
        /*isLastChunk=*/false);
    expectedChunks = {
        ExpectedChunk<std::string_view>({"last_string"}, {}, false, 11)};
    expectedRetainedData = {};
    validateChunk<std::string_view>(
        stream, std::move(chunker2), expectedChunks, expectedRetainedData);
  }
}

TEST_F(StreamChunkerTestsBase, NullsStreamChunking) {
  auto scalarTypeBuilder =
      schemaBuilder_->createScalarTypeBuilder(ScalarKind::Bool);
  auto descriptor = &scalarTypeBuilder->scalarDescriptor();
  NullsStreamData stream(*leafPool_, *descriptor);

  // Setup test data with some nulls
  std::vector<bool> testData = {
      true, false, true, true, false, true, false, true, false, false};
  stream.ensureNullsCapacity(
      /*mayHaveNulls=*/true, static_cast<uint32_t>(testData.size()));
  auto& nonNulls = stream.mutableNonNulls();
  populateData(nonNulls, testData);

  // size of data is 10 * sizeof(bool) = 10 bytes
  ASSERT_EQ(stream.memoryUsed(), 10);
  EXPECT_EQ(stream.data().size(), 0);

  // 4 bytes (arbitrary chunk size)
  uint32_t maxChunkSize = 4;
  uint32_t minChunkSize = 3;

  // Test 1: Not last chunk.
  {
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/false,
        /*emptyStreamContent=*/true,
        /*isNullStream=*/true,
        /*isLastChunk=*/false);
    // Validate that the correct chunker type was created
    ASSERT_NE(dynamic_cast<NullsStreamChunker*>(chunker.get()), nullptr);

    std::vector<ExpectedChunk<bool>> expectedChunks = {
        ExpectedChunk<bool>({}, {true, false, true, true}, true),
        ExpectedChunk<bool>({}, {false, true, false, true}, true)};
    std::vector<bool> expectedRetainedNonNulls = {false, false};
    validateChunk<bool>(
        stream,
        std::move(chunker),
        expectedChunks,
        {},
        expectedRetainedNonNulls);
  }

  // Test 2: Last chunk.
  {
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/false,
        /*emptyStreamContent=*/true,
        /*isNullStream=*/true,
        /*isLastChunk=*/true);
    // Validate that the correct chunker type was created
    ASSERT_NE(dynamic_cast<NullsStreamChunker*>(chunker.get()), nullptr);
    std::vector<ExpectedChunk<bool>> expectedChunks = {
        ExpectedChunk<bool>({}, {false, false}, true)};
    validateChunk<bool>(stream, std::move(chunker), expectedChunks, {}, {});
  }

  // Test 3: Generate chunks when ensureFullChunks_ = true
  {
    testData = {
        true, false, true, true, false, true, false, true, false, false};
    stream.ensureNullsCapacity(
        /*mayHaveNulls=*/true, static_cast<uint32_t>(testData.size()));
    nonNulls = stream.mutableNonNulls();
    populateData(nonNulls, testData);

    auto memoryUsed = stream.memoryUsed();
    // maxChunkSize is greater than available memory
    auto chunker = getStreamChunker(
        stream,
        memoryUsed + 1,
        minChunkSize,
        /*ensureFullChunks=*/true,
        /*emptyStreamContent=*/false,
        /*isNullStream=*/false,
        /*isLastChunk=*/false);

    std::vector<ExpectedChunk<bool>> expectedChunks = {};
    validateChunk<bool>(
        stream,
        std::move(chunker),
        expectedChunks,
        /*expectedRetainedData=*/{},
        /*expectedRetainedNonNulls=*/testData);

    // When maxChunkSize is equal to available memory, chunk is returned.
    auto chunker2 = getStreamChunker(
        stream,
        memoryUsed,
        minChunkSize,
        /*ensureFullChunks=*/true,
        /*emptyStreamContent=*/false,
        /*isNullStream=*/false,
        /*isLastChunk=*/false);
    expectedChunks = {ExpectedChunk<bool>({}, testData, true)};
    validateChunk<bool>(stream, std::move(chunker2), expectedChunks, {}, {});
  }
}

TEST_F(StreamChunkerTestsBase, NullableContentStreamIntChunking) {
  auto scalarTypeBuilder =
      schemaBuilder_->createScalarTypeBuilder(ScalarKind::Int32);
  auto descriptor = &scalarTypeBuilder->scalarDescriptor();
  NullableContentStreamData<int32_t> stream(*leafPool_, *descriptor);

  // Setup test data with some nulls
  std::vector<int32_t> testData = {1, 2, 3, 4, 5, 6, 7};
  std::vector<bool> nonNullsData = {
      true, false, true, true, false, true, false, true, true, true};

  // Populate data using ensureNullsCapacity and manual data insertion
  stream.ensureNullsCapacity(
      /*mayHaveNulls=*/true, static_cast<uint32_t>(nonNullsData.size()));
  populateData(stream.mutableData(), testData);
  populateData(stream.mutableNonNulls(), nonNullsData);

  // 7 * sizeof(int32_t) + 10 * sizeof(bool)) = 38
  ASSERT_EQ(stream.memoryUsed(), 38);

  // Test 1: Not last chunk
  uint64_t maxChunkSize = 22;
  uint64_t minChunkSize = 20;
  {
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/false,
        /*emptyStreamContent=*/true,
        /*isNullStream=*/false,
        /*isLastChunk=*/false);

    std::vector<ExpectedChunk<int32_t>> expectedChunks = {
        ExpectedChunk<int32_t>(
            {1, 2, 3, 4}, {true, false, true, true, false, true}, true)};
    std::vector<int32_t> expectedRetainedData = {5, 6, 7};
    std::vector<bool> expectedRetainedNonNulls = {false, true, true, true};
    validateChunk<int32_t>(
        stream,
        std::move(chunker),
        expectedChunks,
        expectedRetainedData,
        expectedRetainedNonNulls);
  }

  // Test 2: Last chunk - process remaining data
  {
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/false,
        /*emptyStreamContent=*/true,
        /*isNullStream=*/false,
        /*isLastChunk=*/true);
    // Validate that the correct chunker type was created
    ASSERT_NE(
        dynamic_cast<NullableContentStreamChunker<int32_t>*>(chunker.get()),
        nullptr);
    std::vector<ExpectedChunk<int32_t>> expectedChunks = {
        ExpectedChunk<int32_t>({5, 6, 7}, {false, true, true, true}, true)};
    std::vector<int32_t> expectedRetainedData = {};
    std::vector<bool> expectedRetainedNonNulls = {};
    validateChunk<int32_t>(
        stream,
        std::move(chunker),
        expectedChunks,
        expectedRetainedData,
        expectedRetainedNonNulls);
  }

  // Test 3: Small chunks with mixed null/non-null data
  {
    std::vector<int32_t> newTestData = {10, 11};
    std::vector<bool> test3NonNullsData = {
        true, false, false, true, false, false};
    stream.ensureNullsCapacity(
        /*mayHaveNulls=*/true, static_cast<uint32_t>(test3NonNullsData.size()));
    auto& data = stream.mutableData();
    auto& nonNulls = stream.mutableNonNulls();
    populateData(data, newTestData);
    populateData(nonNulls, test3NonNullsData);

    maxChunkSize = 5; // Very small chunks
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/false,
        /*emptyStreamContent=*/true,
        /*isNullStream=*/false,
        /*isLastChunk=*/true);
    // Validate that the correct chunker type was created
    ASSERT_NE(
        dynamic_cast<NullableContentStreamChunker<int32_t>*>(chunker.get()),
        nullptr);

    std::vector<ExpectedChunk<int32_t>> expectedChunks = {
        ExpectedChunk<int32_t>({10}, {true}, true),
        ExpectedChunk<int32_t>({}, {false, false}, true),
        ExpectedChunk<int32_t>({11}, {true}, true),
        ExpectedChunk<int32_t>({}, {false, false}, true),
    };
    std::vector<int32_t> expectedRetainedData = {};
    std::vector<bool> expectedRetainedNonNulls = {};
    validateChunk<int32_t>(
        stream,
        std::move(chunker),
        expectedChunks,
        expectedRetainedData,
        expectedRetainedNonNulls);
  }

  // Test 4: ensureFullChunks=true with data that can't fill maxChunkSize
  {
    stream.reset();
    std::vector<int32_t> smallTestData = {20, 21};
    std::vector<bool> smallNonNullsData = {true, false, true};
    stream.ensureNullsCapacity(
        /*mayHaveNulls=*/true, static_cast<uint32_t>(smallNonNullsData.size()));
    auto& data = stream.mutableData();
    auto& nonNulls = stream.mutableNonNulls();
    populateData(data, smallTestData);
    populateData(nonNulls, smallNonNullsData);

    // maxChunkSize greater than available memory
    maxChunkSize = stream.memoryUsed() + 1;
    minChunkSize = 2;
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/true,
        /*emptyStreamContent=*/true,
        /*isNullStream=*/false,
        /*isLastChunk=*/false);
    ASSERT_NE(
        dynamic_cast<NullableContentStreamChunker<int32_t>*>(chunker.get()),
        nullptr);

    // Should return nothing due to ensureFullChunks=true and insufficient data
    std::vector<ExpectedChunk<int32_t>> expectedChunks = {};
    std::vector<int32_t> expectedRetainedData = {20, 21};
    std::vector<bool> expectedRetainedNonNulls = {true, false, true};
    validateChunk<int32_t>(
        stream,
        std::move(chunker),
        expectedChunks,
        expectedRetainedData,
        expectedRetainedNonNulls);

    // maxChunkSize equal to available memory
    maxChunkSize = stream.memoryUsed();
    auto chunker2 = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/true,
        /*emptyStreamContent=*/true,
        /*isNullStream=*/false,
        /*isLastChunk=*/false);
    ASSERT_NE(
        dynamic_cast<NullableContentStreamChunker<int32_t>*>(chunker2.get()),
        nullptr);

    // Should return nothing due to ensureFullChunks=true and insufficient data
    std::vector<ExpectedChunk<int32_t>> expectedChunks2 = {
        ExpectedChunk<int32_t>({20, 21}, {true, false, true}, true),
    };
    validateChunk<int32_t>(
        stream, std::move(chunker2), expectedChunks2, {}, {});
  }
}

TEST_F(StreamChunkerTestsBase, NullableContentStreamStringChunking) {
  auto scalarTypeBuilder =
      schemaBuilder_->createScalarTypeBuilder(ScalarKind::String);
  auto descriptor = &scalarTypeBuilder->scalarDescriptor();
  NullableContentStreamData<std::string_view> stream(*leafPool_, *descriptor);

  // Setup test data with some nulls
  std::vector<std::string_view> testData = {
      "hello", "world", "test", "string", "chunk", "data", "example"};
  std::vector<bool> nonNullsData = {
      true, false, true, true, false, true, false, true, true, true, false};

  // Populate data using ensureNullsCapacity and manual data insertion
  stream.ensureNullsCapacity(
      /*mayHaveNulls=*/true, static_cast<uint32_t>(nonNullsData.size()));
  auto& data = stream.mutableData();
  auto& nonNulls = stream.mutableNonNulls();
  populateData(data, testData);
  populateData(nonNulls, nonNullsData);

  // Set extra memory for string overhead
  // number of strings: 7
  // size of string view data pointers = 7 * 8 = 56 bytes
  // size of string view data size = 7 * 8 = 56 bytes
  // size of nulls = 11 * 1 = 11 bytes
  // total size of stored string data = 36 bytes
  // total size of stream data = 56 + 56 + 11 + 36 = 159 bytes
  for (const auto& entry : testData) {
    stream.extraMemory() += entry.size();
  }
  ASSERT_EQ(stream.memoryUsed(), 159);

  // Test 1: Not last chunk
  {
    uint64_t maxChunkSize = 72;
    uint64_t minChunkSize = 50;
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/false,
        /*emptyStreamContent=*/true,
        /*isNullStream=*/false,
        /*isLastChunk=*/false);

    std::vector<ExpectedChunk<std::string_view>> expectedChunks = {
        ExpectedChunk<std::string_view>(
            {"hello", "world", "test"},
            {true, false, true, true, false},
            true,
            14), // "hello"(5) + "world"(5) + "test"(4) = 14 bytes extra
        ExpectedChunk<std::string_view>(
            {"string", "chunk", "data"},
            {true, false, true, true},
            true,
            15) // "string"(6) + "chunk"(5) + "data"(4) = 14 bytes extra
    };
    std::vector<std::string_view> expectedRetainedData = {"example"};
    std::vector<bool> expectedRetainedNonNulls = {true, false};
    validateChunk<std::string_view>(
        stream,
        std::move(chunker),
        expectedChunks,
        expectedRetainedData,
        expectedRetainedNonNulls);
  }

  // Test 2: Last chunk - process remaining data
  {
    uint64_t maxChunkSize = 25;
    uint64_t minChunkSize = 22;
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/false,
        /*emptyStreamContent=*/true,
        /*isNullStream=*/false,
        /*isLastChunk=*/true);
    // Validate that the correct chunker type was created
    ASSERT_NE(
        dynamic_cast<NullableContentStreamChunker<std::string_view>*>(
            chunker.get()),
        nullptr);

    std::vector<ExpectedChunk<std::string_view>> expectedChunks = {
        ExpectedChunk<std::string_view>(
            {"example"},
            {true, false},
            true,
            7) // "example"(7) = 7 bytes extra
    };
    std::vector<std::string_view> expectedRetainedData = {};
    std::vector<bool> expectedRetainedNonNulls = {};
    validateChunk<std::string_view>(
        stream,
        std::move(chunker),
        expectedChunks,
        expectedRetainedData,
        expectedRetainedNonNulls);
  }

  //   Test 3: ensureFullChunks=true with data that can't fill maxChunkSize -
  //   should return nothing
  {
    std::vector<std::string_view> smallTestData = {"test", "data"};
    std::vector<bool> smallNonNullsData = {true, false, true};
    stream.ensureNullsCapacity(
        /*mayHaveNulls=*/true, static_cast<uint32_t>(smallNonNullsData.size()));
    auto& smallData = stream.mutableData();
    auto& smallNonNulls = stream.mutableNonNulls();
    populateData(smallData, smallTestData);
    populateData(smallNonNulls, smallNonNullsData);

    // Reset extra memory for new test data
    stream.extraMemory() = 0;
    for (const auto& entry : smallTestData) {
      stream.extraMemory() += entry.size();
    }

    uint64_t maxChunkSize = 50; // Larger than available data
    uint64_t minChunkSize = 10;
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/true,
        /*emptyStreamContent=*/true,
        /*isNullStream=*/false,
        /*isLastChunk=*/false);
    ASSERT_NE(
        dynamic_cast<NullableContentStreamChunker<std::string_view>*>(
            chunker.get()),
        nullptr);

    // Should return nothing due to ensureFullChunks=true and insufficient data
    std::vector<ExpectedChunk<std::string_view>> expectedChunks = {};
    std::vector<std::string_view> expectedRetainedData = {"test", "data"};
    std::vector<bool> expectedRetainedNonNulls = {true, false, true};
    validateChunk<std::string_view>(
        stream,
        std::move(chunker),
        expectedChunks,
        expectedRetainedData,
        expectedRetainedNonNulls);
  }

  // Test 4: ensureFullChunks=true with exact boundary case
  {
    stream.reset();
    std::vector<std::string_view> exactData = {"a", "b"};
    std::vector<bool> exactNonNulls = {true, true};
    stream.ensureNullsCapacity(
        /*mayHaveNulls=*/true, static_cast<uint32_t>(exactNonNulls.size()));
    auto& exactTestData = stream.mutableData();
    auto& exactTestNonNulls = stream.mutableNonNulls();
    populateData(exactTestData, exactData);
    populateData(exactTestNonNulls, exactNonNulls);

    // Reset extra memory for new test data
    stream.extraMemory() = 0;
    for (const auto& entry : exactData) {
      stream.extraMemory() += entry.size();
    }

    // Set maxChunkSize to exactly match the memory used
    uint64_t maxChunkSize = stream.memoryUsed();
    uint64_t minChunkSize = 5;
    auto chunker = getStreamChunker(
        stream,
        maxChunkSize,
        minChunkSize,
        /*ensureFullChunks=*/true,
        /*emptyStreamContent=*/true,
        /*isNullStream=*/false,
        /*isLastChunk=*/false);
    ASSERT_NE(
        dynamic_cast<NullableContentStreamChunker<std::string_view>*>(
            chunker.get()),
        nullptr);

    // Should return chunk since it exactly matches maxChunkSize
    std::vector<ExpectedChunk<std::string_view>> expectedChunks = {
        ExpectedChunk<std::string_view>(
            {"a", "b"},
            {true, true},
            true,
            2) // "a"(1) + "b"(1) = 2 bytes extra
    };
    std::vector<std::string_view> expectedRetainedData = {};
    std::vector<bool> expectedRetainedNonNulls = {};
    validateChunk<std::string_view>(
        stream,
        std::move(chunker),
        expectedChunks,
        expectedRetainedData,
        expectedRetainedNonNulls);
  }
}
} // namespace facebook::nimble
