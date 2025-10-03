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

class StreamDataTest : public ::testing::Test {
 protected:
  void SetUp() override {
    memoryPool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
    schemaBuilder_ = std::make_unique<SchemaBuilder>();
    auto scalarBuilder =
        schemaBuilder_->createScalarTypeBuilder(ScalarKind::Int32);
    streamDescriptor_ = &scalarBuilder->scalarDescriptor();
  }

  std::shared_ptr<velox::memory::MemoryPool> memoryPool_;
  std::unique_ptr<SchemaBuilder> schemaBuilder_;
  const StreamDescriptorBuilder* streamDescriptor_;
};

TEST_F(StreamDataTest, ContentStreamDataMutableDataCapacity) {
  ExactGrowthPolicy exactPolicy;
  ContentStreamData<int32_t> streamData(
      *memoryPool_, *streamDescriptor_, exactPolicy);

  // Ensure the size of 20
  streamData.ensureMutableDataCapacity(20);

  // Check the capacity and size
  auto& data = streamData.mutableData();
  // Actual capacity is controlled by the growth policy + AlignedBuffer capacity
  // logic, it can be larger than requested.
  const auto actualCapacity1 = data.capacity();
  EXPECT_GE(actualCapacity1, 20);
  EXPECT_EQ(data.size(), 0);

  // Write 20 values into the data
  for (int32_t i = 0; i < 20; ++i) {
    data.push_back(i);
  }
  EXPECT_EQ(data.size(), 20);

  // Call ensure with the same size 20, make sure capacity did not change
  streamData.ensureMutableDataCapacity(20);
  EXPECT_EQ(data.capacity(), actualCapacity1);
  EXPECT_EQ(data.size(), 20);

  // Grow the capacity
  streamData.ensureMutableDataCapacity(actualCapacity1 + 10);
  const auto actualCapacity2 = data.capacity();

  // Check the capacity and size
  EXPECT_GE(actualCapacity2, actualCapacity1 + 10);
  EXPECT_EQ(data.size(), 20);

  // Check that previously written values are still present
  for (int32_t i = 0; i < 20; ++i) {
    EXPECT_EQ(data[i], i) << "Value at index " << i << " was corrupted";
  }

  // Ensure new capacity is usable
  for (int32_t i = 20; i < actualCapacity2; ++i) {
    data.push_back(i);
  }
  EXPECT_EQ(data.size(), actualCapacity2);

  // Verify all data (old and new) is still correct
  for (int32_t i = 0; i < actualCapacity2; ++i) {
    EXPECT_EQ(data[i], i) << "Value at index " << i << " was corrupted";
  }
}

TEST_F(StreamDataTest, NullableContentStreamDataMutableDataCapacity) {
  ExactGrowthPolicy exactPolicy;
  NullableContentStreamData<int32_t> streamData(
      *memoryPool_, *streamDescriptor_, exactPolicy);

  // Ensure the size of 20
  streamData.ensureMutableDataCapacity(20);

  // Check the capacity and size
  auto& data = streamData.mutableData();
  // Actual capacity is controlled by the growth policy + AlignedBuffer capacity
  // logic, it can be larger than requested.
  const auto actualCapacity1 = data.capacity();
  EXPECT_GE(actualCapacity1, 20);
  EXPECT_EQ(data.size(), 0);

  // Write 20 values into the data
  for (int32_t i = 0; i < 20; ++i) {
    data.push_back(i);
  }
  EXPECT_EQ(data.size(), 20);

  // Call ensure with the same size 20, make sure capacity did not change
  streamData.ensureMutableDataCapacity(20);
  EXPECT_EQ(data.capacity(), actualCapacity1);
  EXPECT_EQ(data.size(), 20);

  // Grow the capacity
  streamData.ensureMutableDataCapacity(actualCapacity1 + 10);
  const auto actualCapacity2 = data.capacity();

  // Check the capacity and size
  EXPECT_GE(actualCapacity2, actualCapacity1 + 10);
  EXPECT_EQ(data.size(), 20);

  // Check that previously written values are still present
  for (int32_t i = 0; i < 20; ++i) {
    EXPECT_EQ(data[i], i) << "Value at index " << i << " was corrupted";
  }

  // Ensure new capacity is usable
  for (int32_t i = 20; i < actualCapacity2; ++i) {
    data.push_back(i);
  }
  EXPECT_EQ(data.size(), actualCapacity2);

  // Verify all data (old and new) is still correct
  for (int32_t i = 0; i < actualCapacity2; ++i) {
    EXPECT_EQ(data[i], i) << "Value at index " << i << " was corrupted";
  }
}

TEST_F(StreamDataTest, NullsStreamDataensureAdditionalNullsCapacity) {
  ExactGrowthPolicy exactPolicy;
  NullsStreamData streamData(*memoryPool_, *streamDescriptor_, exactPolicy);

  // First ensure some capacity with nulls
  streamData.ensureAdditionalNullsCapacity(true, 20);

  // Check initial state
  EXPECT_TRUE(streamData.hasNulls());
  auto& nonNulls = streamData.mutableNonNulls();
  const auto actualCapacity1 = nonNulls.capacity();
  EXPECT_GE(actualCapacity1, 20);
  EXPECT_EQ(nonNulls.size(), 0);

  // Write 20 values: true for even indices, false for odd
  for (size_t i = 0; i < 20; ++i) {
    nonNulls.push_back(i % 2 == 0);
  }
  EXPECT_EQ(nonNulls.size(), 20);

  // Call ensure with the same size 20, make sure capacity did not change
  streamData.ensureAdditionalNullsCapacity(true, 20);
  EXPECT_EQ(nonNulls.capacity(), actualCapacity1);
  EXPECT_EQ(nonNulls.size(), 20);

  // Grow the capacity
  streamData.ensureAdditionalNullsCapacity(true, actualCapacity1 + 10);
  const auto actualCapacity2 = nonNulls.capacity();

  // Check the capacity and size
  EXPECT_GE(actualCapacity2, actualCapacity1 + 10);
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
  EXPECT_EQ(nonNulls.size(), actualCapacity2);

  // Verify all data (old and new) is still correct
  for (size_t i = 0; i < actualCapacity2; ++i) {
    EXPECT_EQ(nonNulls[i], (i % 2 == 0))
        << "Null value at index " << i << " was corrupted";
  }
}

TEST_F(StreamDataTest, NullableContentStreamDataensureAdditionalNullsCapacity) {
  ExactGrowthPolicy exactPolicy;
  NullableContentStreamData<int32_t> streamData(
      *memoryPool_, *streamDescriptor_, exactPolicy);

  // First ensure some capacity with nulls
  streamData.ensureAdditionalNullsCapacity(true, 20);

  // Check initial state
  EXPECT_TRUE(streamData.hasNulls());
  auto& nonNulls = streamData.mutableNonNulls();
  const auto actualCapacity1 = nonNulls.capacity();
  EXPECT_GE(actualCapacity1, 20);
  EXPECT_EQ(nonNulls.size(), 0);

  // Write 20 values: true for even indices, false for odd
  for (size_t i = 0; i < 20; ++i) {
    nonNulls.push_back(i % 2 == 0);
  }
  EXPECT_EQ(nonNulls.size(), 20);

  // Call ensure with the same size 20, make sure capacity did not change
  streamData.ensureAdditionalNullsCapacity(true, 20);
  EXPECT_EQ(nonNulls.capacity(), actualCapacity1);
  EXPECT_EQ(nonNulls.size(), 20);

  // Grow the capacity
  streamData.ensureAdditionalNullsCapacity(true, actualCapacity1 + 10);
  const auto actualCapacity2 = nonNulls.capacity();

  // Check the capacity and size
  EXPECT_GE(actualCapacity2, actualCapacity1 + 10);
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
  EXPECT_EQ(nonNulls.size(), actualCapacity2);

  // Verify all data (old and new) is still correct
  for (size_t i = 0; i < actualCapacity2; ++i) {
    EXPECT_EQ(nonNulls[i], (i % 2 == 0))
        << "Null value at index " << i << " was corrupted";
  }
}
} // namespace facebook::nimble
