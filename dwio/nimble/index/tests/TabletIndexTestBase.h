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
#pragma once

#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/index/StripeIndexGroup.h"
#include "dwio/nimble/index/TabletIndex.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble::test {

/// Base test class providing common test utilities for tablet index tests.
class TabletIndexTestBase : public ::testing::Test {
 protected:
  static void SetUpTestCase();

  void SetUp() override {}

  struct Stream {
    uint32_t numChunks;
    std::vector<int32_t> chunkRows;
    std::vector<uint32_t> chunkOffsets;
  };

  struct KeyStream {
    uint32_t streamOffset;
    uint32_t streamSize;
    Stream stream;
    std::vector<std::string> chunkKeys;
  };

  struct Stripe {
    std::vector<Stream> streams;
    KeyStream keyStream;
  };

  /// Structure to hold the serialized index buffers.
  struct IndexBuffers {
    /// Serialized StripeIndexGroup flatbuffers appended sequentially.
    std::string indexGroups;
    /// Serialized root Index flatbuffer.
    std::string rootIndex;
  };

  /// Helper function to create a serialized Index flatbuffer for testing.
  /// @param indexColumns Vector of index column names
  /// @param stripes Vector of stripe data
  /// @param stripeGroups Vector indicating how many stripes are in each group
  /// @return IndexBuffers containing serialized StripeIndexGroups and root
  /// Index
  IndexBuffers createTestTabletIndex(
      const std::vector<std::string>& indexColumns,
      const std::string& minKey,
      const std::vector<Stripe>& stripes,
      const std::vector<int>& stripeGroups);

  /// Creates a TabletIndex from the serialized IndexBuffers.
  /// @param indexBuffers The serialized index buffers from
  /// createTestTabletIndex
  /// @return A unique pointer to a TabletIndex
  std::unique_ptr<TabletIndex> createTabletIndex(
      const IndexBuffers& indexBuffers);

  /// Creates a StripeIndexGroup from the serialized IndexBuffers.
  /// @param indexBuffers The serialized index buffers from
  /// createTestTabletIndex
  /// @param stripeGroupIndex The index of the stripe group to create
  /// @return A shared pointer to a StripeIndexGroup
  std::shared_ptr<StripeIndexGroup> createStripeIndexGroup(
      const IndexBuffers& indexBuffers,
      uint32_t stripeGroupIndex);

  std::shared_ptr<velox::memory::MemoryPool> rootPool_{
      velox::memory::memoryManager()->addRootPool("TabletIndexTestBase")};
  std::shared_ptr<velox::memory::MemoryPool> pool_{
      rootPool_->addLeafChild("TabletIndexTestBase")};
};

} // namespace facebook::nimble::test
