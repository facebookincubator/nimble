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

#include <cstring>

#include "dwio/nimble/common/Buffer.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble::test {

class BufferTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

TEST_F(BufferTest, ReserveSmall) {
  Buffer buffer(*pool_);
  char* ptr = buffer.reserve(10);
  ASSERT_NE(ptr, nullptr);
  // Write and read back
  std::memset(ptr, 'A', 10);
  EXPECT_EQ(ptr[0], 'A');
  EXPECT_EQ(ptr[9], 'A');
}

TEST_F(BufferTest, ReserveMultipleWithinChunk) {
  Buffer buffer(*pool_);
  char* ptr1 = buffer.reserve(100);
  char* ptr2 = buffer.reserve(200);
  ASSERT_NE(ptr1, nullptr);
  ASSERT_NE(ptr2, nullptr);

  // Both pointers should be valid and non-overlapping
  std::memset(ptr1, 'A', 100);
  std::memset(ptr2, 'B', 200);
  EXPECT_EQ(ptr1[0], 'A');
  EXPECT_EQ(ptr2[0], 'B');
}

TEST_F(BufferTest, ReserveOversized) {
  // Request more than kMinChunkSize (1 MB) to trigger a new chunk allocation
  Buffer buffer(*pool_);
  constexpr uint64_t bigSize = 2 * 1024 * 1024; // 2 MB
  char* ptr = buffer.reserve(bigSize);
  ASSERT_NE(ptr, nullptr);
  // Write at boundaries
  ptr[0] = 'X';
  ptr[bigSize - 1] = 'Y';
  EXPECT_EQ(ptr[0], 'X');
  EXPECT_EQ(ptr[bigSize - 1], 'Y');
}
TEST_F(BufferTest, WriteStringBasic) {
  // Single write
  {
    Buffer buffer(*pool_);
    std::string_view input = "hello world";
    auto result = buffer.writeString(input);
    EXPECT_EQ(result, "hello world");
    EXPECT_EQ(result.size(), input.size());
  }

  // Multiple writes and view validity
  {
    Buffer buffer(*pool_);
    auto r1 = buffer.writeString("aaa");
    auto r2 = buffer.writeString("bbb");
    auto r3 = buffer.writeString("ccc");

    EXPECT_EQ(r1, "aaa");
    EXPECT_EQ(r2, "bbb");
    EXPECT_EQ(r3, "ccc");

    // All views should remain valid
    EXPECT_EQ(r1, "aaa");
  }

  // Empty write
  {
    Buffer buffer(*pool_);
    auto result = buffer.writeString("");
    EXPECT_EQ(result, "");
    EXPECT_EQ(result.size(), 0);
  }
}
TEST_F(BufferTest, TakeOwnership) {
  Buffer buffer(*pool_);

  auto simple_view = [&]() {
    auto bufferPtr = velox::AlignedBuffer::allocate<char>(128, pool_.get());
    char* raw = bufferPtr->asMutable<char>();
    std::memcpy(raw, "simple extended test data", 25);
    return buffer.takeOwnership(std::move(bufferPtr));
  }();
  EXPECT_GE(simple_view.size(), 25);
  EXPECT_EQ(
      std::string_view(simple_view.data(), 25), "simple extended test data");
}

TEST_F(BufferTest, GetMemoryPool) {
  Buffer buffer(*pool_);
  auto& poolRef = buffer.getMemoryPool();
  // Just verify we get a valid reference back
  EXPECT_GT(poolRef.capacity(), 0);
}

} // namespace facebook::nimble::test
