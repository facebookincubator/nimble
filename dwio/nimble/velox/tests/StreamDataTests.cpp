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

#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/StreamData.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/SharedArbitrator.h"

namespace facebook::nimble {

class StreamDataTestsBase : public ::testing::Test {
 protected:
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

class ContentStreamDataTests : public StreamDataTestsBase {};
class NullsStreamDataTests : public StreamDataTestsBase {};
class NullableContentStreamDataTests : public StreamDataTestsBase {};

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

template <typename T>
bool isEqual(const Vector<T>& actual, const std::vector<T>& expected) {
  if (expected.size() != actual.size()) {
    return false;
  }
  for (size_t i = 0; i < actual.size(); ++i) {
    if (expected[i] != actual[i]) {
      LOG(INFO) << fmt::format(
          "[{}] Expected: {}, Actual: {}", i, expected[i], actual[i]);
      return false;
    }
  }
  return true;
}

template <typename T>
bool isEqual(const T* actual, const std::vector<T>& expected) {
  for (size_t i = 0; i < expected.size(); ++i) {
    if (expected[i] != actual[i]) {
      LOG(INFO) << fmt::format(
          "[{}] Expected: {}, Actual: {}", i, expected[i], actual[i]);
      return false;
    }
  }
  return true;
}

TEST_F(ContentStreamDataTests, NextIntChunk) {
  auto scalarTypeBuilder =
      schemaBuilder_->createScalarTypeBuilder(ScalarKind::Int32);
  auto descriptor = &scalarTypeBuilder->scalarDescriptor();
  ContentStreamData<int32_t> stream(*leafPool_, *descriptor);

  // size of data is 10 * sizeof(int32_t) = 40 bytes
  std::vector<int32_t> testData = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  auto& data = stream.mutableData();
  populateData(data, testData);
  uint64_t initialMemoryUsed = stream.extraMemory();

  // 18 bytes (arbitrary)
  uint32_t maxChunkSize = 18;

  // Chunk 1: 4 elements popped, 6 remaining
  {
    auto chunk = stream.nextChunk(maxChunkSize);
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->memoryUsed(), 4 * sizeof(int32_t));
    EXPECT_THAT(
        toVector<int32_t>(chunk->data()), ::testing::ElementsAre(1, 2, 3, 4));

    // Remaining Chunk
    EXPECT_EQ(stream.data().size(), 6 * sizeof(int32_t));
    EXPECT_THAT(
        toVector<int32_t>(stream.data()),
        ::testing::ElementsAre(5, 6, 7, 8, 9, 10));
  }

  // Chunk 2: 4 elements popped, 2 remaining
  {
    auto chunk = stream.nextChunk(maxChunkSize);
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->memoryUsed(), 4 * sizeof(int32_t));
    EXPECT_THAT(
        toVector<int32_t>(chunk->data()), ::testing::ElementsAre(5, 6, 7, 8));

    // Remaining Chunk
    EXPECT_EQ(stream.memoryUsed(), 2 * sizeof(int32_t));
    EXPECT_THAT(
        toVector<int32_t>(stream.data()), ::testing::ElementsAre(9, 10));
  }

  // Chunk 3: Attempt to chunk more than is available
  {
    auto chunk = stream.nextChunk(maxChunkSize);
    // Remaining Chunk
    ASSERT_EQ(chunk, nullptr);
    EXPECT_EQ(stream.memoryUsed(), 2 * sizeof(int32_t));
    EXPECT_THAT(
        toVector<int32_t>(stream.data()), ::testing::ElementsAre(9, 10));
  }

  // Chunk 4: Chunk exactly what is available
  {
    auto chunk = stream.nextChunk(2 * sizeof(int32_t));
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->memoryUsed(), 2 * sizeof(int32_t));
    EXPECT_THAT(
        toVector<int32_t>(chunk->data()), ::testing::ElementsAre(9, 10));

    // Remaining Chunk
    EXPECT_EQ(stream.memoryUsed(), 0);
    EXPECT_THAT(toVector<int32_t>(stream.data()), ::testing::ElementsAre());
  }

  // Should stay unchanged for non-string types.
  EXPECT_EQ(initialMemoryUsed, stream.extraMemory());
}

TEST_F(ContentStreamDataTests, NextStringChunk) {
  auto scalarTypeBuilder =
      schemaBuilder_->createScalarTypeBuilder(ScalarKind::String);
  auto descriptor = &scalarTypeBuilder->scalarDescriptor();
  ContentStreamData<std::string_view> stream(*leafPool_, *descriptor);

  // Populate string data
  auto& data = stream.mutableData();
  std::vector<std::string_view> streamEntry = {
      "hello", "world", "test", "string", "chunk", "data", "example", "values"};
  populateData(data, streamEntry);

  // Set extra memory for string overhead
  // number of strings: 8
  // size of string view data pointers = 8 * 8 = 64 bytes
  // size of string view data size = 8 * 8 = 64 bytes
  // total size of stored string data = 42 bytes
  // total size of stream data = 64 + 64 + 42 = 170 bytes
  for (const auto& entry : streamEntry) {
    stream.extraMemory() += entry.size();
  }
  ASSERT_EQ(stream.memoryUsed(), 170);

  uint32_t maxChunkSize = 64;
  // Chunk 1: 3 elements processed, 5 remaining
  {
    auto chunk = stream.nextChunk(maxChunkSize);
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->data().size(), 3 * sizeof(std::string_view));
    EXPECT_FALSE(chunk->hasNulls());

    // Verify chunk contains string_view data
    auto chunkStringViews = toVector<std::string_view>(chunk->data());
    EXPECT_THAT(
        chunkStringViews, ::testing::ElementsAre("hello", "world", "test"));

    // Verify extra memory for chunk
    // Size of "hello" + "world" + "test" = 5 + 5 + 4 = 14
    EXPECT_EQ(chunk->extraMemory(), 14);
    // Memory used = 14 + 16 * 3 = 62
    EXPECT_EQ(chunk->memoryUsed(), 62);

    // Remaining Chunk
    ASSERT_EQ(stream.data().size(), 5 * sizeof(std::string_view));
    auto streamData = toVector<std::string_view>(stream.data());
    EXPECT_EQ(streamData[0], "string");
    // Verify remaining extra memory
    EXPECT_EQ(stream.extraMemory(), 28); // 42 - 14
    EXPECT_EQ(stream.memoryUsed(), 108); // 170 - 62
  }

  // Chunk 2: 3 more elements processed, 2 remaining
  {
    auto chunk = stream.nextChunk(maxChunkSize);
    // Returned Chunk
    // Verify chunk contains string_view data
    auto chunkStringViews = toVector<std::string_view>(chunk->data());
    EXPECT_THAT(
        chunkStringViews, ::testing::ElementsAre("string", "chunk", "data"));
    // Verify extra memory for chunk
    // Size of "string" + "chunk" + "data" = 6 + 5 + 4 = 15
    EXPECT_EQ(chunk->extraMemory(), 15);
    // Memory used = 15 + 16 * 3 = 63
    EXPECT_EQ(chunk->memoryUsed(), 63);

    // Remaining data should contain 2 strings
    EXPECT_EQ(stream.data().size(), 2 * sizeof(std::string_view));
    auto streamData = toVector<std::string_view>(stream.data());
    EXPECT_EQ(streamData[0], "example");
    EXPECT_EQ(
        stream.extraMemory(), 13); // 28 - 15 (size of "string"+"chunk"+"data")
    EXPECT_EQ(stream.memoryUsed(), 45); // 108 - 63
  }

  // Chunk 3: Attempt to chunk more than is available
  {
    auto chunk = stream.nextChunk(maxChunkSize);
    // Returned Chunk should be null since chunk size exceeds remaining data
    ASSERT_EQ(chunk, nullptr);

    // Remaining data should stay unchanged
    EXPECT_EQ(stream.data().size(), 2 * sizeof(std::string_view));
    auto streamData = toVector<std::string_view>(stream.data());
    EXPECT_EQ(streamData[0], "example");
    EXPECT_EQ(streamData[1], "values");
    // Verify remaining extra memory unchanged
    EXPECT_EQ(stream.extraMemory(), 13); // Should remain 13
    EXPECT_EQ(stream.memoryUsed(), 45); // Should remain 45
  }

  // Chunk 4: Chunk exactly what is available
  {
    // Use exact size for remaining 2 strings
    // Size of "example" + "values" = 7 + 6 = 13 + sizeof(string_view) * 2 = 45
    uint32_t exactChunkSize = 45;
    auto chunk = stream.nextChunk(exactChunkSize);

    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    auto chunkStringViews = toVector<std::string_view>(chunk->data());
    ASSERT_EQ(chunkStringViews.size(), 2);
    EXPECT_EQ(chunkStringViews[0], "example");
    EXPECT_EQ(chunkStringViews[1], "values");

    // Remaining data should be empty
    EXPECT_EQ(stream.data().size(), 0);
    EXPECT_EQ(stream.extraMemory(), 0);
    EXPECT_EQ(stream.memoryUsed(), 0);
  }
}

TEST_F(NullsStreamDataTests, NextChunk) {
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

  // Chunk 1: 4 elements popped, 6 remaining
  {
    auto chunk = stream.nextChunk(maxChunkSize);
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->memoryUsed(), 4);
    EXPECT_EQ(chunk->data().size(), 0);
    EXPECT_THAT(
        chunk->nonNulls(), ::testing::ElementsAre(true, false, true, true));
    EXPECT_TRUE(chunk->hasNulls());

    // Remaining Chunk
    EXPECT_EQ(stream.memoryUsed(), 6);
    EXPECT_THAT(
        stream.nonNulls(),
        ::testing::ElementsAre(false, true, false, true, false, false));
    EXPECT_TRUE(stream.hasNulls());
  }

  // Chunk 2: 4 elements popped, 2 remaining
  {
    auto chunk = stream.nextChunk(maxChunkSize);
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->memoryUsed(), 4);
    EXPECT_EQ(chunk->data().size(), 0);
    EXPECT_THAT(
        chunk->nonNulls(), ::testing::ElementsAre(false, true, false, true));
    EXPECT_TRUE(chunk->hasNulls());

    // Remaining Chunk
    EXPECT_EQ(stream.memoryUsed(), 2);
    EXPECT_THAT(stream.nonNulls(), ::testing::ElementsAre(false, false));
  }

  // Chunk 3: Attempt to chunk more than is available
  {
    auto chunk = stream.nextChunk(maxChunkSize);
    // Returned Chunk should be null since chunk size exceeds remaining data
    ASSERT_EQ(chunk, nullptr);

    // Remaining Chunk should stay unchanged
    EXPECT_EQ(stream.memoryUsed(), 2);
    EXPECT_THAT(stream.nonNulls(), ::testing::ElementsAre(false, false));
  }

  // Chunk 4: Chunk exactly what is available
  {
    auto chunk = stream.nextChunk(2);
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->memoryUsed(), 2);
    EXPECT_THAT(chunk->nonNulls(), ::testing::ElementsAre(false, false));

    // NOTE: Should be false, but we avoid paying the cost of accertaining this.
    EXPECT_TRUE(chunk->hasNulls());

    // Remaining Chunk should be empty
    EXPECT_EQ(stream.memoryUsed(), 0);
    EXPECT_EQ(stream.nonNulls().size(), 0);
    // Accertain no change to mutable data
    EXPECT_TRUE(isEqual(stream.mutableNonNulls(), testData));
  }
}

TEST_F(NullableContentStreamDataTests, NextIntChunk) {
  auto scalarTypeBuilder =
      schemaBuilder_->createScalarTypeBuilder(ScalarKind::Int32);
  auto descriptor = &scalarTypeBuilder->scalarDescriptor();
  NullableContentStreamData<int32_t> stream(*leafPool_, *descriptor);

  // Setup test data with some nulls
  std::vector<int32_t> testData = {1, 2, 3, 4, 5, 6, 7};
  std::vector<bool> nullData = {
      true, false, true, true, false, true, false, true, true, true};

  // Populate data using ensureNullsCapacity and manual data insertion
  stream.ensureNullsCapacity(
      /*mayHaveNulls=*/true, static_cast<uint32_t>(nullData.size()));
  auto& data = stream.mutableData();
  auto& nonNulls = stream.mutableNonNulls();
  populateData(data, testData);
  populateData(nonNulls, nullData);

  // 7 * sizeof(int32_t) + 10 * sizeof(bool)) = 38
  ASSERT_EQ(stream.memoryUsed(), 38);

  // 18 bytes (arbitrary chunk size)
  uint32_t maxChunkSize = 22;

  // Chunk 1: 4 elements processed, 6 remaining
  {
    auto chunk = stream.nextChunk(maxChunkSize);

    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->data().size(), 4 * sizeof(int32_t));
    EXPECT_EQ(chunk->nonNulls().size(), 6);
    EXPECT_TRUE(chunk->hasNulls());
    EXPECT_EQ(chunk->memoryUsed(), 4 * sizeof(int32_t) + 6); // 16 + 6 = 22

    // Verify chunk contains int32_t data
    auto chunkInts = toVector<int32_t>(chunk->data());
    EXPECT_THAT(chunkInts, ::testing::ElementsAre(1, 2, 3, 4));

    // Verify chunk contains nulls data
    auto chunkNonNulls =
        std::vector<bool>(chunk->nonNulls().begin(), chunk->nonNulls().end());
    EXPECT_THAT(
        chunkNonNulls,
        ::testing::ElementsAre(true, false, true, true, false, true));

    // Remaining Chunk
    EXPECT_EQ(stream.data().size(), 3 * sizeof(int32_t));
    EXPECT_THAT(
        toVector<int32_t>(stream.data()), ::testing::ElementsAre(5, 6, 7));
    EXPECT_THAT(
        stream.nonNulls(), ::testing::ElementsAre(false, true, true, true));
    EXPECT_TRUE(stream.hasNulls());
    EXPECT_EQ(stream.memoryUsed(), 3 * sizeof(int32_t) + 4); // 12 + 4 = 16
  }

  // Chunk 2: 4 more elements processed, 2 remaining
  {
    auto chunk = stream.nextChunk(1);

    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->data().size(), 0 * sizeof(int32_t));
    EXPECT_EQ(chunk->nonNulls().size(), 1);
    EXPECT_TRUE(chunk->hasNulls());
    EXPECT_EQ(chunk->memoryUsed(), 0 * sizeof(int32_t) + 1); // 0 + 1 = 1

    // Verify chunk contains int32_t data
    auto chunkInts = toVector<int32_t>(chunk->data());
    EXPECT_THAT(chunkInts, ::testing::ElementsAre());

    // Verify chunk contains nulls data
    auto chunkNonNulls =
        std::vector<bool>(chunk->nonNulls().begin(), chunk->nonNulls().end());
    EXPECT_THAT(chunkNonNulls, ::testing::ElementsAre(false));

    // Remaining data
    EXPECT_EQ(stream.data().size(), 3 * sizeof(int32_t));
    EXPECT_THAT(
        toVector<int32_t>(stream.data()), ::testing::ElementsAre(5, 6, 7));
    EXPECT_THAT(stream.nonNulls(), ::testing::ElementsAre(true, true, true));
    EXPECT_TRUE(stream.hasNulls());
    EXPECT_EQ(stream.memoryUsed(), 3 * sizeof(int32_t) + 3); // 12 + 3 = 15
  }

  // Chunk 3: Attempt to chunk more than is available
  {
    auto chunk = stream.nextChunk(maxChunkSize);
    // Returned Chunk should be null since chunk size exceeds remaining data
    ASSERT_EQ(chunk, nullptr);

    // Remaining data should stay unchanged
    EXPECT_EQ(stream.data().size(), 3 * sizeof(int32_t));
    EXPECT_THAT(
        toVector<int32_t>(stream.data()), ::testing::ElementsAre(5, 6, 7));
    EXPECT_THAT(stream.nonNulls(), ::testing::ElementsAre(true, true, true));
    EXPECT_TRUE(stream.hasNulls());
    EXPECT_EQ(stream.memoryUsed(), 3 * sizeof(int32_t) + 3); // 12 + 3 = 15
  }

  // Chunk 4: Chunk exactly what is available
  {
    auto chunk = stream.nextChunk(3 * (sizeof(int32_t) + sizeof(bool)));
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->data().size(), 3 * sizeof(int32_t));
    EXPECT_EQ(chunk->nonNulls().size(), 3);
    // NOTE: Should be false, but we avoid paying the cost of accertaining this.
    EXPECT_TRUE(chunk->hasNulls());
    EXPECT_EQ(chunk->memoryUsed(), 3 * sizeof(int32_t) + 3); // 12 + 3 = 15

    // Verify chunk contains int32_t data
    auto chunkInts = toVector<int32_t>(chunk->data());
    EXPECT_THAT(chunkInts, ::testing::ElementsAre(5, 6, 7));

    // Verify chunk contains nulls data
    auto chunkNonNulls =
        std::vector<bool>(chunk->nonNulls().begin(), chunk->nonNulls().end());
    EXPECT_THAT(chunkNonNulls, ::testing::ElementsAre(true, true, true));

    // Remaining data should be empty
    EXPECT_EQ(stream.data().size(), 0);
    EXPECT_EQ(stream.extraMemory(), 0);
    EXPECT_EQ(stream.memoryUsed(), 0);

    // Accertain no change to mutable data
    EXPECT_TRUE(isEqual(stream.mutableData(), testData));
    EXPECT_TRUE(isEqual(stream.mutableNonNulls(), nullData));
  }
}

TEST_F(NullableContentStreamDataTests, NextStringChunk) {
  auto scalarTypeBuilder =
      schemaBuilder_->createScalarTypeBuilder(ScalarKind::String);
  auto descriptor = &scalarTypeBuilder->scalarDescriptor();
  NullableContentStreamData<std::string_view> stream(*leafPool_, *descriptor);

  // Setup test data with some nulls
  std::vector<std::string_view> testData = {
      "hello", "world", "test", "string", "chunk", "data", "example"};
  std::vector<bool> nullData = {
      true, false, true, true, false, true, false, true, true, true, false};

  // Populate data using ensureNullsCapacity and manual data insertion
  stream.ensureNullsCapacity(
      /*mayHaveNulls=*/true, static_cast<uint32_t>(nullData.size()));
  auto& data = stream.mutableData();
  auto& nonNulls = stream.mutableNonNulls();
  populateData(data, testData);
  populateData(nonNulls, nullData);

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

  uint32_t maxChunkSize = 72;
  // Chunk 1: String specialization determines chunk size based on individual
  // string costs
  {
    auto chunk = stream.nextChunk(maxChunkSize);
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->data().size(), 3 * sizeof(std::string_view));
    EXPECT_EQ(chunk->nonNulls().size(), 5);
    EXPECT_TRUE(chunk->hasNulls()); // Should detect false in nulls
    // Verify extra memory for chunk
    // Size of "hello" + "world" + "test" = 5 + 5 + 4 = 14
    EXPECT_EQ(chunk->extraMemory(), 14);
    // Memory used = 14 + 16 * 3 + 5 = 67
    EXPECT_EQ(chunk->memoryUsed(), 67);

    // Verify chunk contains string_view data
    auto chunkStringViews = toVector<std::string_view>(chunk->data());
    EXPECT_THAT(
        chunkStringViews, ::testing::ElementsAre("hello", "world", "test"));

    // Verify chunk contains nulls data
    auto chunkNonNulls =
        std::vector<bool>(chunk->nonNulls().begin(), chunk->nonNulls().end());
    EXPECT_THAT(
        chunkNonNulls, ::testing::ElementsAre(true, false, true, true, false));

    // Remaining data should still contain all original data but memory and
    // extra memory are reduced
    auto remainingStringViews = toVector<std::string_view>(stream.data());
    EXPECT_THAT(
        remainingStringViews,
        ::testing::ElementsAre("string", "chunk", "data", "example"));
    LOG(INFO) << "checking here";
    auto remainingNonNulls =
        std::vector<bool>(stream.nonNulls().begin(), stream.nonNulls().end());
    EXPECT_THAT(
        remainingNonNulls,
        ::testing::ElementsAre(true, false, true, true, true, false));
    EXPECT_TRUE(stream.hasNulls()); // Should detect false in remaining nulls
    // Verify remaining extra memory
    EXPECT_EQ(stream.extraMemory(), 22); // 36 - 14
    EXPECT_EQ(stream.memoryUsed(), 92); // 159 - 67
  }

  // Chunk 2: 3 elements processed, 2 remaining
  {
    auto chunk = stream.nextChunk(maxChunkSize);
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->data().size(), 3 * sizeof(std::string_view));
    EXPECT_EQ(chunk->nonNulls().size(), 4);
    EXPECT_TRUE(chunk->hasNulls());
    // Verify extra memory for chunk
    // Size of "string" + "chunk" + "data" = 6 + 5 + 4 = 15
    EXPECT_EQ(chunk->extraMemory(), 15);
    // Memory used = 15 + 16 * 3 + 4 = 67
    EXPECT_EQ(chunk->memoryUsed(), 67);

    // Verify chunk contains string_view data
    auto chunkStringViews = toVector<std::string_view>(chunk->data());
    EXPECT_THAT(
        chunkStringViews, ::testing::ElementsAre("string", "chunk", "data"));

    // Verify chunk contains nulls data
    auto chunkNonNulls =
        std::vector<bool>(chunk->nonNulls().begin(), chunk->nonNulls().end());
    EXPECT_THAT(chunkNonNulls, ::testing::ElementsAre(true, false, true, true));

    // Remaining data
    auto remainingStringViews = toVector<std::string_view>(stream.data());
    EXPECT_THAT(remainingStringViews, ::testing::ElementsAre("example"));
    auto remainingNonNulls =
        std::vector<bool>(stream.nonNulls().begin(), stream.nonNulls().end());
    EXPECT_THAT(remainingNonNulls, ::testing::ElementsAre(true, false));
    EXPECT_TRUE(stream.hasNulls());
    // Verify remaining extra memory
    EXPECT_EQ(stream.extraMemory(), 7); // 22 - 15
    EXPECT_EQ(stream.memoryUsed(), 25); // 92 - 67
  }

  // Chunk 3: Attempt to chunk more than is available
  {
    auto chunk = stream.nextChunk(maxChunkSize);
    // Returned Chunk should be null since chunk size exceeds remaining data
    ASSERT_EQ(chunk, nullptr);

    // Remaining data should stay unchanged
    auto remainingStringViews = toVector<std::string_view>(stream.data());
    EXPECT_THAT(remainingStringViews, ::testing::ElementsAre("example"));
    auto remainingNonNulls =
        std::vector<bool>(stream.nonNulls().begin(), stream.nonNulls().end());
    EXPECT_THAT(remainingNonNulls, ::testing::ElementsAre(true, false));
    EXPECT_TRUE(stream.hasNulls());
    // Verify remaining extra memory
    EXPECT_EQ(stream.extraMemory(), 7); // 22 - 15
    EXPECT_EQ(stream.memoryUsed(), 25); // 92 - 67
  }

  // Chunk 4: Chunk exactly what is available
  {
    // Use exact size for remaining 2 strings
    // Size of "example" = 7 + sizeof(string_view) = 23 + sizeof(bool) * 2 = 25
    uint32_t exactChunkSize = 25;
    auto chunk = stream.nextChunk(exactChunkSize);
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->data().size(), sizeof(std::string_view));
    EXPECT_EQ(chunk->nonNulls().size(), 2);
    EXPECT_TRUE(chunk->hasNulls());
    // Verify extra memory for chunk
    // Size of "example" = 7
    EXPECT_EQ(chunk->extraMemory(), 7);
    // Memory used = 7 + 16 + 2 = 25
    EXPECT_EQ(chunk->memoryUsed(), 25);

    // Verify chunk contains string_view data
    auto chunkStringViews = toVector<std::string_view>(chunk->data());
    EXPECT_THAT(chunkStringViews, ::testing::ElementsAre("example"));

    // Verify chunk contains nulls data
    auto chunkNonNulls =
        std::vector<bool>(chunk->nonNulls().begin(), chunk->nonNulls().end());
    EXPECT_THAT(chunkNonNulls, ::testing::ElementsAre(true, false));

    // Remaining data should be empty
    EXPECT_EQ(stream.memoryUsed(), 0);
    EXPECT_EQ(stream.data().size(), 0);
    EXPECT_EQ(stream.nonNulls().size(), 0);
    EXPECT_EQ(stream.extraMemory(), 0);

    // Accertain no change to mutable data
    EXPECT_TRUE(isEqual(stream.mutableData(), testData));
    EXPECT_TRUE(isEqual(stream.mutableNonNulls(), nullData));
  }
}
} // namespace facebook::nimble
