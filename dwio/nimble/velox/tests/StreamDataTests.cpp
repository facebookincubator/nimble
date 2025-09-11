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
bool isEqual(const Vector<T>& actual, const std::vector<T>& expected) {
  if (expected.size() != actual.size()) {
    return false;
  }
  for (size_t i = 0; i < actual.size(); ++i) {
    if (expected[i] != actual[i]) {
      return false;
    }
  }
  return true;
}

TEST_F(ContentStreamDataTests, PopIntChunk) {
  auto scalarTypeBuilder =
      schemaBuilder_->createScalarTypeBuilder(ScalarKind::Int32);
  auto descriptor = &scalarTypeBuilder->scalarDescriptor();
  ContentStreamData<int32_t> stream(*leafPool_, *descriptor);

  // size of data is 10 * sizeof(int32_t) = 40 bytes
  std::vector<int32_t> testData = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  auto& data = stream.mutableData();
  populateData(data, testData);

  // 18 bytes (arbitrary)
  uint32_t maxChunkSize = 18;

  // Chunk 1: 4 elements popped, 6 remaining
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk =
        dynamic_cast<ContentStreamData<int32_t>*>(streamDataChunk.get());
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->data().size(), 4 * sizeof(int32_t));
    EXPECT_TRUE(isEqual(chunk->mutableData(), {1, 2, 3, 4}));

    // Remaining Chunk
    EXPECT_EQ(stream.data().size(), 6 * sizeof(int32_t));
    EXPECT_TRUE(isEqual(stream.mutableData(), {5, 6, 7, 8, 9, 10}));

    // Should stay unchanged for non-string types.
    EXPECT_EQ(chunk->extraMemory(), stream.extraMemory());
  }

  // Chunk 2: 4 elements popped, 2 remaining
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk =
        dynamic_cast<ContentStreamData<int32_t>*>(streamDataChunk.get());
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->memoryUsed(), 4 * sizeof(int32_t));
    EXPECT_TRUE(isEqual(chunk->mutableData(), {5, 6, 7, 8}));

    // Remaining Chunk
    EXPECT_EQ(stream.memoryUsed(), 2 * sizeof(int32_t));
    EXPECT_TRUE(isEqual(stream.mutableData(), {9, 10}));
  }

  // Chunk 3: Attempt to chunk more than is available
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk =
        dynamic_cast<ContentStreamData<int32_t>*>(streamDataChunk.get());
    // Returned Chunk
    ASSERT_EQ(chunk, nullptr);

    // Remaining Chunk
    EXPECT_EQ(stream.memoryUsed(), 2 * sizeof(int32_t));
    EXPECT_TRUE(isEqual(stream.mutableData(), {9, 10}));
  }

  // Chunk 4: Chunk exactly what is available
  {
    auto streamDataChunk = stream.popChunk(2 * sizeof(int32_t));
    auto chunk =
        dynamic_cast<ContentStreamData<int32_t>*>(streamDataChunk.get());
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->memoryUsed(), 2 * sizeof(int32_t));
    EXPECT_TRUE(isEqual(chunk->mutableData(), {9, 10}));

    // Remaining Chunk
    EXPECT_EQ(stream.memoryUsed(), 0);
    EXPECT_TRUE(isEqual(stream.mutableData(), {}));
  }
}

TEST_F(ContentStreamDataTests, PopStringChunk) {
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
  // Chunk 1: 3 elements popped, 5 remaining
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk = dynamic_cast<ContentStreamData<std::string_view>*>(
        streamDataChunk.get());
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    ASSERT_EQ(chunk->mutableData().size(), 3);
    EXPECT_EQ(chunk->mutableData()[0], "hello");
    EXPECT_EQ(chunk->mutableData()[1], "world");
    EXPECT_EQ(chunk->mutableData()[2], "test");
    // Verify extra memory for chunk
    // Size of "hello" + "world" + "test" = 5 + 5 + 4 = 14
    EXPECT_EQ(chunk->extraMemory(), 14);
    // Memory used = 14 + 16 * 3 = 62
    EXPECT_EQ(chunk->memoryUsed(), 62);

    // Remaining Chunk
    ASSERT_EQ(stream.mutableData().size(), 5);
    EXPECT_EQ(stream.mutableData()[0], "string");
    // Verify remaining extra memory
    EXPECT_EQ(stream.extraMemory(), 28); // 42 - 14
    EXPECT_EQ(stream.memoryUsed(), 108); // 170 - 62
  }

  // Chunk 2: 3 elements popped, 2 remaining
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk = dynamic_cast<ContentStreamData<std::string_view>*>(
        streamDataChunk.get());
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    ASSERT_EQ(chunk->mutableData().size(), 3);
    EXPECT_EQ(chunk->mutableData()[0], "string");
    EXPECT_EQ(chunk->mutableData()[1], "chunk");
    EXPECT_EQ(chunk->mutableData()[2], "data");
    // Verify extra memory for chunk
    // Size of "string" + "chunk" + "data" = 6 + 5 + 4 = 15
    EXPECT_EQ(chunk->extraMemory(), 15);
    // Memory used = 15 + 16 * 3 = 63
    EXPECT_EQ(chunk->memoryUsed(), 63);

    // Remaining Chunk
    ASSERT_EQ(stream.mutableData().size(), 2);
    EXPECT_EQ(stream.mutableData()[0], "example");
    // Verify remaining extra memory
    EXPECT_EQ(stream.extraMemory(), 13); // 28 - 15
    EXPECT_EQ(stream.memoryUsed(), 45); // 108 - 63
  }

  // Chunk 3: Attempt to chunk more than is available
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk = dynamic_cast<ContentStreamData<std::string_view>*>(
        streamDataChunk.get());
    // Returned Chunk should be null since chunk size exceeds remaining data
    ASSERT_EQ(chunk, nullptr);

    // Remaining Chunk should stay unchanged
    EXPECT_EQ(stream.mutableData().size(), 2);
    EXPECT_EQ(stream.mutableData()[0], "example");
    EXPECT_EQ(stream.mutableData()[1], "values");
    // Verify remaining extra memory unchanged
    EXPECT_EQ(stream.extraMemory(), 13); // Should remain 13
    EXPECT_EQ(stream.memoryUsed(), 45); // Should remain 45
  }

  // Chunk 4: Chunk exactly what is available
  {
    // Use exact size for remaining 2 strings
    // Size of "example" + "values" = 7 + 6 = 13 + sizeof(string_view) * 2 = 45
    uint32_t exactChunkSize = 45;
    auto streamDataChunk = stream.popChunk(exactChunkSize);
    auto chunk = dynamic_cast<ContentStreamData<std::string_view>*>(
        streamDataChunk.get());
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->mutableData().size(), 2);
    EXPECT_EQ(chunk->mutableData()[0], "example");
    EXPECT_EQ(chunk->mutableData()[1], "values");
    // Verify extra memory for chunk
    // Size of "example" + "values" = 7 + 6 = 13
    EXPECT_EQ(chunk->extraMemory(), 13);
    // Memory used = 13 + 16 * 2 = 45
    EXPECT_EQ(chunk->memoryUsed(), 45);

    // Remaining Chunk should be empty
    ASSERT_EQ(stream.mutableData().size(), 0);
    EXPECT_EQ(stream.extraMemory(), 0);
    EXPECT_EQ(stream.memoryUsed(), 0);
  }
}

TEST_F(NullsStreamDataTests, PopChunk) {
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

  // 4 bytes (arbitrary chunk size)
  uint32_t maxChunkSize = 4;

  // Chunk 1: 4 elements popped, 6 remaining
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk = dynamic_cast<NullsStreamData*>(streamDataChunk.get());

    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->memoryUsed(), 4);
    EXPECT_TRUE(isEqual(chunk->mutableNonNulls(), {true, false, true, true}));
    EXPECT_TRUE(chunk->hasNulls());

    // Remaining Chunk
    EXPECT_EQ(stream.memoryUsed(), 6);
    EXPECT_TRUE(isEqual(
        stream.mutableNonNulls(), {false, true, false, true, false, false}));
    EXPECT_TRUE(stream.hasNulls());
  }

  // Chunk 2: 4 elements popped, 2 remaining
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk = dynamic_cast<NullsStreamData*>(streamDataChunk.get());

    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->memoryUsed(), 4);
    EXPECT_TRUE(isEqual(chunk->mutableNonNulls(), {false, true, false, true}));
    EXPECT_TRUE(chunk->hasNulls());

    // Remaining Chunk
    EXPECT_EQ(stream.memoryUsed(), 2);
    EXPECT_TRUE(isEqual(stream.mutableNonNulls(), {false, false}));
  }

  // Chunk 3: Attempt to chunk more than is available
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk = dynamic_cast<NullsStreamData*>(streamDataChunk.get());
    // Returned Chunk should be null since chunk size exceeds remaining data
    ASSERT_EQ(chunk, nullptr);

    // Remaining Chunk should stay unchanged
    EXPECT_EQ(stream.memoryUsed(), 2);
    EXPECT_TRUE(isEqual(stream.mutableNonNulls(), {false, false}));
  }

  // Chunk 4: Chunk exactly what is available
  {
    auto streamDataChunk = stream.popChunk(2);
    auto chunk = dynamic_cast<NullsStreamData*>(streamDataChunk.get());
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->memoryUsed(), 2);
    EXPECT_TRUE(isEqual(chunk->mutableNonNulls(), {false, false}));

    // NOTE: Should be false, but we avoid paying the cost of accertaining this.
    EXPECT_TRUE(chunk->hasNulls());

    // Remaining Chunk should be empty
    EXPECT_EQ(stream.memoryUsed(), 0);
    EXPECT_TRUE(isEqual(stream.mutableNonNulls(), {}));
  }
}

TEST_F(NullableContentStreamDataTests, PopIntChunk) {
  auto scalarTypeBuilder =
      schemaBuilder_->createScalarTypeBuilder(ScalarKind::Int32);
  auto descriptor = &scalarTypeBuilder->scalarDescriptor();
  NullableContentStreamData<int32_t> stream(*leafPool_, *descriptor);

  // Setup test data with some nulls
  std::vector<int32_t> testData = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<bool> nullData = {
      true, false, true, true, false, true, false, true, true, true};

  // Populate data using ensureNullsCapacity and manual data insertion
  stream.ensureNullsCapacity(
      /*mayHaveNulls=*/true, static_cast<uint32_t>(testData.size()));
  auto& data = stream.mutableData();
  auto& nonNulls = stream.mutableNonNulls();
  populateData(data, testData);
  populateData(nonNulls, nullData);

  // 10 * (sizeof(int32_t) + sizeof(bool)) = 10 * (4 + 1) = 50 bytes
  ASSERT_EQ(stream.memoryUsed(), 50);

  // 18 bytes (arbitrary chunk size)
  uint32_t maxChunkSize = 21;

  // Chunk 1: 4 elements popped, 6 remaining
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk = dynamic_cast<NullableContentStreamData<int32_t>*>(
        streamDataChunk.get());

    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->data().size(), 4 * sizeof(int32_t));
    EXPECT_TRUE(isEqual(chunk->mutableData(), {1, 2, 3, 4}));
    EXPECT_TRUE(isEqual(chunk->mutableNonNulls(), {true, false, true, true}));
    EXPECT_TRUE(chunk->hasNulls()); // Should detect false in nulls
    EXPECT_EQ(chunk->memoryUsed(), 4 * sizeof(int32_t) + 4); // 16 + 4 = 20

    // Remaining Chunk
    EXPECT_EQ(stream.data().size(), 6 * sizeof(int32_t));
    EXPECT_TRUE(isEqual(stream.mutableData(), {5, 6, 7, 8, 9, 10}));
    EXPECT_TRUE(isEqual(
        stream.mutableNonNulls(), {false, true, false, true, true, true}));
    EXPECT_TRUE(stream.hasNulls()); // Should detect false in remaining nulls
    EXPECT_EQ(stream.memoryUsed(), 6 * sizeof(int32_t) + 6); // 24 + 6 = 30
  }

  // Chunk 2: 4 elements popped, 2 remaining
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk = dynamic_cast<NullableContentStreamData<int32_t>*>(
        streamDataChunk.get());

    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->memoryUsed(), 4 * sizeof(int32_t) + 4); // 16 + 4 = 20
    EXPECT_TRUE(isEqual(chunk->mutableData(), {5, 6, 7, 8}));
    EXPECT_TRUE(isEqual(chunk->mutableNonNulls(), {false, true, false, true}));
    EXPECT_TRUE(chunk->hasNulls());

    // Remaining Chunk
    EXPECT_EQ(stream.memoryUsed(), 2 * sizeof(int32_t) + 2); // 8 + 2 = 10
    EXPECT_TRUE(isEqual(stream.mutableData(), {9, 10}));
    EXPECT_TRUE(isEqual(stream.mutableNonNulls(), {true, true}));
    // NOTE: Should be false, but we avoid paying the cost of accertaining this.
    EXPECT_TRUE(stream.hasNulls());
  }

  // Chunk 3: Attempt to chunk more than is available
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk = dynamic_cast<NullableContentStreamData<int32_t>*>(
        streamDataChunk.get());
    // Returned Chunk should be null since chunk size exceeds remaining data
    ASSERT_EQ(chunk, nullptr);

    // Remaining Chunk should stay unchanged
    EXPECT_EQ(stream.memoryUsed(), 2 * sizeof(int32_t) + 2); // 8 + 2 = 10
    EXPECT_TRUE(isEqual(stream.mutableData(), {9, 10}));
    EXPECT_TRUE(isEqual(stream.mutableNonNulls(), {true, true}));
  }

  // Chunk 4: Chunk exactly what is available
  {
    auto streamDataChunk =
        stream.popChunk(2 * (sizeof(int32_t) + sizeof(bool)));
    auto chunk = dynamic_cast<NullableContentStreamData<int32_t>*>(
        streamDataChunk.get());
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->memoryUsed(), 2 * sizeof(int32_t) + 2); // 8 + 2 = 10
    EXPECT_TRUE(isEqual(chunk->mutableData(), {9, 10}));
    EXPECT_TRUE(isEqual(chunk->mutableNonNulls(), {true, true}));
    // NOTE: Should be false, but we avoid paying the cost of accertaining this.
    EXPECT_TRUE(chunk->hasNulls());

    // Remaining Chunk should be empty
    EXPECT_EQ(stream.memoryUsed(), 0);
    EXPECT_TRUE(isEqual(stream.mutableData(), {}));
    EXPECT_TRUE(isEqual(stream.mutableNonNulls(), {}));
  }
}

TEST_F(NullableContentStreamDataTests, PopStringChunk) {
  auto scalarTypeBuilder =
      schemaBuilder_->createScalarTypeBuilder(ScalarKind::String);
  auto descriptor = &scalarTypeBuilder->scalarDescriptor();
  NullableContentStreamData<std::string_view> stream(*leafPool_, *descriptor);

  // Setup test data with some nulls
  std::vector<std::string_view> testData = {
      "hello", "world", "test", "string", "chunk", "data", "example", "values"};
  std::vector<bool> nullData = {
      true, false, true, true, false, true, false, true};

  // Populate data using ensureNullsCapacity and manual data insertion
  stream.ensureNullsCapacity(
      /*mayHaveNulls=*/true, static_cast<uint32_t>(testData.size()));
  auto& data = stream.mutableData();
  auto& nonNulls = stream.mutableNonNulls();
  populateData(data, testData);
  populateData(nonNulls, nullData);

  // Set extra memory for string overhead
  // number of strings: 8
  // size of string view data pointers = 8 * 8 = 64 bytes
  // size of string view data size = 8 * 8 = 64 bytes
  // size of nulls = 8 * 1 = 8 bytes
  // total size of stored string data = 42 bytes
  // total size of stream data = 64 + 64 + 8 + 42 = 178 bytes
  for (const auto& entry : testData) {
    stream.extraMemory() += entry.size();
  }
  ASSERT_EQ(stream.memoryUsed(), 178);

  uint32_t maxChunkSize = 72;
  // Chunk 1: 3 elements popped, 5 remaining
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk = dynamic_cast<NullableContentStreamData<std::string_view>*>(
        streamDataChunk.get());
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    ASSERT_EQ(chunk->mutableData().size(), 3);
    EXPECT_EQ(chunk->mutableData()[0], "hello");
    EXPECT_EQ(chunk->mutableData()[1], "world");
    EXPECT_EQ(chunk->mutableData()[2], "test");
    EXPECT_TRUE(isEqual(chunk->mutableNonNulls(), {true, false, true}));
    EXPECT_TRUE(chunk->hasNulls()); // Should detect false in nulls
    // Verify extra memory for chunk
    // Size of "hello" + "world" + "test" = 5 + 5 + 4 = 14
    EXPECT_EQ(chunk->extraMemory(), 14);
    // Memory used = 14 + 16 * 3 + 3 = 14 + 48 + 3 = 65
    EXPECT_EQ(chunk->memoryUsed(), 65);

    // Remaining Chunk
    EXPECT_EQ(stream.mutableData().size(), 5);
    EXPECT_EQ(stream.mutableData()[0], "string");
    EXPECT_TRUE(
        isEqual(stream.mutableNonNulls(), {true, false, true, false, true}));
    EXPECT_TRUE(stream.hasNulls()); // Should detect false in remaining nulls
    // Verify remaining extra memory
    EXPECT_EQ(stream.extraMemory(), 28); // 42 - 14
    EXPECT_EQ(stream.memoryUsed(), 113); // 178 - 65
  }

  // Chunk 2: 3 elements popped, 2 remaining
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk = dynamic_cast<NullableContentStreamData<std::string_view>*>(
        streamDataChunk.get());
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    ASSERT_EQ(chunk->mutableData().size(), 3);
    EXPECT_EQ(chunk->mutableData()[0], "string");
    EXPECT_EQ(chunk->mutableData()[1], "chunk");
    EXPECT_EQ(chunk->mutableData()[2], "data");
    EXPECT_TRUE(isEqual(chunk->mutableNonNulls(), {true, false, true}));
    EXPECT_TRUE(chunk->hasNulls());
    // Verify extra memory for chunk
    // Size of "string" + "chunk" + "data" = 6 + 5 + 4 = 15
    EXPECT_EQ(chunk->extraMemory(), 15);
    // Memory used = 15 + 16 * 3 + 3 = 15 + 48 + 3 = 66
    EXPECT_EQ(chunk->memoryUsed(), 66);

    // Remaining Chunk
    EXPECT_EQ(stream.mutableData().size(), 2);
    EXPECT_EQ(stream.mutableData()[0], "example");
    EXPECT_TRUE(isEqual(stream.mutableNonNulls(), {false, true}));
    EXPECT_TRUE(stream.hasNulls());
    // Verify remaining extra memory
    EXPECT_EQ(stream.extraMemory(), 13); // 28 - 15
    EXPECT_EQ(stream.memoryUsed(), 47); // 113 - 66
  }

  // Chunk 3: Attempt to chunk more than is available
  {
    auto streamDataChunk = stream.popChunk(maxChunkSize);
    auto chunk = dynamic_cast<NullableContentStreamData<std::string_view>*>(
        streamDataChunk.get());
    // Returned Chunk should be null since chunk size exceeds remaining data
    ASSERT_EQ(chunk, nullptr);

    // Remaining Chunk should stay unchanged
    EXPECT_EQ(stream.mutableData().size(), 2);
    EXPECT_EQ(stream.mutableData()[0], "example");
    EXPECT_EQ(stream.mutableData()[1], "values");
    EXPECT_TRUE(isEqual(stream.mutableNonNulls(), {false, true}));
    // Verify remaining extra memory unchanged
    EXPECT_EQ(stream.extraMemory(), 13); // Should remain 13
    EXPECT_EQ(stream.memoryUsed(), 47); // Should remain 47
  }

  // Chunk 4: Chunk exactly what is available
  {
    // Use exact size for remaining 2 strings
    // Size of "example" + "values" = 7 + 6 = 13 + sizeof(string_view) * 2 +
    // sizeof(bool) * 2 = 47
    uint32_t exactChunkSize = 47;
    auto streamDataChunk = stream.popChunk(exactChunkSize);
    auto chunk = dynamic_cast<NullableContentStreamData<std::string_view>*>(
        streamDataChunk.get());
    // Returned Chunk
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->mutableData().size(), 2);
    EXPECT_EQ(chunk->mutableData()[0], "example");
    EXPECT_EQ(chunk->mutableData()[1], "values");
    EXPECT_TRUE(isEqual(chunk->mutableNonNulls(), {false, true}));
    EXPECT_TRUE(chunk->hasNulls());
    // Verify extra memory for chunk
    // Size of "example" + "values" = 7 + 6 = 13
    EXPECT_EQ(chunk->extraMemory(), 13);
    // Memory used = 13 + 16 * 2 + 2 = 47
    EXPECT_EQ(chunk->memoryUsed(), 47);

    // Remaining Chunk should be empty
    EXPECT_EQ(stream.mutableData().size(), 0);
    ASSERT_EQ(stream.mutableNonNulls().size(), 0);
    EXPECT_EQ(stream.extraMemory(), 0);
    EXPECT_EQ(stream.memoryUsed(), 0);
  }
}
} // namespace facebook::nimble
