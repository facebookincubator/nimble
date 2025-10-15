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

#include "dwio/nimble/velox/BufferGrowthPolicy.h"
#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/StreamData.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble {
using ::testing::InSequence;
using ::testing::Return;

class MockGrowthPolicy : public InputBufferGrowthPolicy {
 public:
  MOCK_METHOD(
      uint64_t,
      getExtendedCapacity,
      (uint64_t newSize, uint64_t capacity),
      (const, override));
};

template <typename T>
uint64_t getAlignedBufferCapacity(
    uint64_t size,
    std::shared_ptr<velox::memory::MemoryPool> pool) {
  // Nimble Vector deoptimizes boolean from 1 bit per value to 1 byte per value
  using InnerType =
      typename std::conditional<std::is_same_v<T, bool>, uint8_t, T>::type;
  auto buf = velox::AlignedBuffer::allocateExact<InnerType>(size, pool.get());
  return buf->capacity() / sizeof(InnerType);
}

template <typename T>
class NullStreamDataTest : public ::testing::Test {};
using NullTestTypes =
    ::testing::Types<NullsStreamData, NullableContentStreamData<int32_t>>;
TYPED_TEST_SUITE(NullStreamDataTest, NullTestTypes);

TYPED_TEST(NullStreamDataTest, EnsureAdditionalNullsCapacity) {
  using T = TypeParam;
  velox::memory::MemoryManager::testingSetInstance(
      velox::memory::MemoryManager::Options{});
  auto memoryPool = velox::memory::memoryManager()->addLeafPool("test");
  auto helperPool = velox::memory::memoryManager()->addLeafPool("helper");

  SchemaBuilder schemaBuilder;
  const auto scalarBuilder =
      schemaBuilder.createScalarTypeBuilder(ScalarKind::Int32);
  const auto& descriptor = scalarBuilder->scalarDescriptor();

  // Grow from 0 to 20, but the actual capacity will be larger. Then grow again.
  MockGrowthPolicy growthPolicy;
  {
    InSequence seq;
    EXPECT_CALL(growthPolicy, getExtendedCapacity(20, 0)).WillOnce(Return(20));
    EXPECT_CALL(
        growthPolicy,
        getExtendedCapacity(
            1030, getAlignedBufferCapacity<bool>(20, helperPool)))
        .WillOnce(Return(1030));
  }
  T streamData(*memoryPool, descriptor, growthPolicy);

  // First ensure some capacity with nulls
  streamData.ensureAdditionalNullsCapacity(true, 20);
  EXPECT_EQ(memoryPool->stats().numAllocs, 1);

  // Check initial state
  EXPECT_TRUE(streamData.hasNulls());
  auto& nonNulls = streamData.mutableNonNulls();
  const auto actualCapacity1 = nonNulls.capacity();
  EXPECT_GE(actualCapacity1, 20);
  EXPECT_EQ(actualCapacity1, getAlignedBufferCapacity<bool>(20, helperPool));
  EXPECT_EQ(nonNulls.size(), 0);

  // Write 20 values: true for even indices, false for odd
  for (size_t i = 0; i < 20; ++i) {
    nonNulls.push_back(i % 2 == 0);
  }
  EXPECT_EQ(nonNulls.size(), 20);
  EXPECT_EQ(actualCapacity1, nonNulls.capacity());

  // Ask for additoinal capacity for 10 elements, it should be within the range
  // of already allocated capacity.
  EXPECT_GE(actualCapacity1, 20 + 10);
  streamData.ensureAdditionalNullsCapacity(true, 10);
  EXPECT_EQ(nonNulls.capacity(), actualCapacity1);
  EXPECT_EQ(nonNulls.size(), 20);
  EXPECT_EQ(memoryPool->stats().numAllocs, 1);

  // Grow the capacity
  streamData.ensureAdditionalNullsCapacity(true, 1000);
  const auto actualCapacity2 = nonNulls.capacity();
  EXPECT_EQ(memoryPool->stats().numAllocs, 2);

  // Check the capacity increased and size stayed the same
  EXPECT_GT(actualCapacity2, actualCapacity1);
  EXPECT_EQ(actualCapacity2, getAlignedBufferCapacity<bool>(1030, helperPool));
  EXPECT_EQ(nonNulls.size(), 20);

  // Check that previously written values are still present
  for (size_t i = 0; i < 20; ++i) {
    EXPECT_EQ(nonNulls[i], (i % 2 == 0))
        << "Null value at index " << i << " was corrupted";
  }

  // Ensure new capacity is usable
  for (size_t i = 20; i < actualCapacity2; ++i) {
    nonNulls.push_back(i % 2 == 0);
  }

  // Check the capacity and size not changed
  EXPECT_EQ(nonNulls.size(), actualCapacity2);
  EXPECT_EQ(nonNulls.capacity(), actualCapacity2);

  // Verify all data (old and new) is still correct
  for (size_t i = 0; i < actualCapacity2; ++i) {
    EXPECT_EQ(nonNulls[i], (i % 2 == 0))
        << "Null value at index " << i << " was corrupted";
  }
  EXPECT_EQ(memoryPool->stats().numAllocs, 2);
}

template <typename T>
class ContentStreamDataTest : public ::testing::Test {};

using ContentTestTypes = ::testing::
    Types<ContentStreamData<int32_t>, NullableContentStreamData<int32_t>>;
TYPED_TEST_SUITE(ContentStreamDataTest, ContentTestTypes);

TYPED_TEST(ContentStreamDataTest, EnsureMutableDataCapacity) {
  using T = TypeParam;
  velox::memory::MemoryManager::testingSetInstance(
      velox::memory::MemoryManager::Options{});
  auto memoryPool = velox::memory::memoryManager()->addLeafPool("test");
  auto helperPool = velox::memory::memoryManager()->addLeafPool("helper");
  SchemaBuilder schemaBuilder;
  const auto scalarBuilder =
      schemaBuilder.createScalarTypeBuilder(ScalarKind::Int32);
  const auto& descriptor = scalarBuilder->scalarDescriptor();

  // Grow from 0 to 20, then to 1000.
  MockGrowthPolicy growthPolicy;
  {
    InSequence seq;
    EXPECT_CALL(growthPolicy, getExtendedCapacity(20, 0)).WillOnce(Return(20));
    EXPECT_CALL(
        growthPolicy,
        getExtendedCapacity(
            1000, getAlignedBufferCapacity<int32_t>(20, helperPool)))
        .WillOnce(Return(1000));
  }
  T streamData(*memoryPool, descriptor, growthPolicy);
  EXPECT_EQ(memoryPool->stats().numAllocs, 0);

  // Ensure the size of 20, make sure capacity has changed
  streamData.ensureMutableDataCapacity(20);
  EXPECT_EQ(memoryPool->stats().numAllocs, 1);

  auto& data = streamData.mutableData();
  const auto actualCapacity1 = data.capacity();
  EXPECT_GE(actualCapacity1, 20);
  EXPECT_EQ(actualCapacity1, getAlignedBufferCapacity<int32_t>(20, helperPool));
  EXPECT_EQ(data.size(), 0);

  // Write 20 values into the data
  for (int32_t i = 0; i < 20; ++i) {
    data.push_back(i);
  }
  EXPECT_EQ(data.size(), 20);
  EXPECT_EQ(data.capacity(), actualCapacity1);

  // Call ensure with the same size 20, make sure capacity did not change
  streamData.ensureMutableDataCapacity(20);
  EXPECT_EQ(data.size(), 20);
  EXPECT_EQ(data.capacity(), actualCapacity1);
  EXPECT_EQ(memoryPool->stats().numAllocs, 1);

  // Grow the capacityfrom 20 to 1000
  streamData.ensureMutableDataCapacity(1000);
  const auto actualCapacity2 = data.capacity();
  EXPECT_GT(actualCapacity2, actualCapacity1);
  EXPECT_EQ(
      actualCapacity2, getAlignedBufferCapacity<int32_t>(1000, helperPool));
  EXPECT_EQ(data.size(), 20);
  EXPECT_EQ(memoryPool->stats().numAllocs, 2);

  // Check that previously written values are still present
  for (int32_t i = 0; i < 20; ++i) {
    EXPECT_EQ(data[i], i) << "Value at index " << i << " was corrupted";
  }

  // Ensure all new capacity is usable
  for (int32_t i = 20; i < actualCapacity2; ++i) {
    data.push_back(i);
  }
  // Check there are no changes in size and capacity
  EXPECT_EQ(data.size(), actualCapacity2);
  EXPECT_EQ(data.capacity(), actualCapacity2);

  // Verify all data (old and new) is still correct
  for (int32_t i = 0; i < actualCapacity2; ++i) {
    EXPECT_EQ(data[i], i) << "Value at index " << i << " was corrupted";
  }
  EXPECT_EQ(memoryPool->stats().numAllocs, 2);
}
} // namespace facebook::nimble
