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

#include "dwio/nimble/tablet/TabletIndexWriter.h"

#include <gtest/gtest.h>

#include "dwio/nimble/common/Buffer.h"

#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/index/StripeIndexGroup.h"
#include "dwio/nimble/index/TabletIndex.h"
#include "dwio/nimble/index/tests/TabletIndexTestUtils.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/IndexGenerated.h"
#include "dwio/nimble/tablet/TabletWriter.h"
#include "velox/common/memory/Memory.h"
#include "velox/core/PlanNode.h"

namespace facebook::nimble::test {

using index::test::ChunkSpec;
using index::test::createChunks;
using index::test::createKeyStream;
using index::test::KeyChunkSpec;
using index::test::StripeIndexGroupTestHelper;

// Holds all the index data written during a test.
// Use with the callback methods to capture index data for verification.
struct TestFileIndex {
  // Key stream data written via writeStreamCallback().
  std::string keyStreamData;
  // Metadata sections for each stripe group, written via
  // createMetadataSectionCallback().
  std::vector<std::string> groupMetadataSections;
  // Root index data written via writeRootIndexCallback().
  std::string rootIndexData;
};

class TabletIndexWriterTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = velox::memory::memoryManager()->addLeafPool();
  }

  // Returns a callback that appends key stream data to TestFileIndex.
  static auto writeStreamCallback(TestFileIndex& fileIndex) {
    return [&fileIndex](std::string_view data) {
      fileIndex.keyStreamData.append(data);
    };
  }

  // Returns a callback that stores metadata sections in TestFileIndex.
  static auto createMetadataSectionCallback(TestFileIndex& fileIndex) {
    return [&fileIndex](std::string_view metadata) -> MetadataSection {
      fileIndex.groupMetadataSections.emplace_back(metadata);
      return MetadataSection(0, metadata.size(), CompressionType::Uncompressed);
    };
  }

  // Returns a callback that stores root index data in TestFileIndex.
  static auto writeRootIndexCallback(TestFileIndex& fileIndex) {
    return [&fileIndex](const std::string& name, std::string_view content) {
      EXPECT_EQ(name, nimble::kIndexSection);
      fileIndex.rootIndexData = std::string(content);
    };
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

TEST_F(TabletIndexWriterTest, basic) {
  // Test create with valid config
  {
    TabletIndexConfig config{
        .columns = {"col1", "col2"},
        .sortOrders =
            {velox::core::kAscNullsFirst, velox::core::kDescNullsLast},
        .enforceKeyOrder = true,
    };
    auto writer = TabletIndexWriter::create(config, *pool_);
    EXPECT_NE(writer, nullptr);
  }

  // Test create with nullopt returns nullptr
  {
    auto writer = TabletIndexWriter::create(std::nullopt, *pool_);
    EXPECT_EQ(writer, nullptr);
  }

  // Test create with empty columns throws
  {
    TabletIndexConfig config{
        .columns = {},
        .sortOrders = {},
        .enforceKeyOrder = false,
    };
    NIMBLE_ASSERT_USER_THROW(
        TabletIndexWriter::create(config, *pool_),
        "Index columns must not be empty in TabletIndexConfig");
  }

  // Test create with empty sortOrders throws
  {
    TabletIndexConfig config{
        .columns = {"col1"},
        .sortOrders = {},
        .enforceKeyOrder = false,
    };
    NIMBLE_ASSERT_USER_THROW(
        TabletIndexWriter::create(config, *pool_),
        "(1 vs. 0) Index columns and sort orders must have the same size in TabletIndexConfig");
  }

  // Test create with mismatched columns and sortOrders size throws
  {
    TabletIndexConfig config{
        .columns = {"col1", "col2"},
        .sortOrders = {velox::core::kAscNullsFirst},
        .enforceKeyOrder = false,
    };
    NIMBLE_ASSERT_USER_THROW(
        TabletIndexWriter::create(config, *pool_),
        "Index columns and sort orders must have the same size in TabletIndexConfig");
  }
}

TEST_F(TabletIndexWriterTest, stripeKeyOutOfOrderCheck) {
  for (bool enforceKeyOrder : {false, true}) {
    SCOPED_TRACE(fmt::format("enforceKeyOrder={}", enforceKeyOrder));

    TabletIndexConfig config{
        .columns = {"col1"},
        .sortOrders = {velox::core::kAscNullsFirst},
        .enforceKeyOrder = enforceKeyOrder};
    auto writer = TabletIndexWriter::create(config, *pool_);

    Buffer buffer{*pool_};

    // First stripe
    writer->ensureStripeWrite(2);
    auto keyStream1 = createKeyStream(buffer, {{100, "bbb", "ccc"}});
    writer->addStripeKey(keyStream1);

    // Second stripe with key less than previous
    writer->ensureStripeWrite(2);
    auto keyStream2 = createKeyStream(buffer, {{100, "aaa", "aab"}});

    if (enforceKeyOrder) {
      NIMBLE_ASSERT_USER_THROW(
          writer->addStripeKey(keyStream2),
          "(aab vs. ccc) Stripe keys must be in strictly ascending order (duplicates are not allowed)");
    } else {
      EXPECT_NO_THROW(writer->addStripeKey(keyStream2));
    }
  }
}

TEST_F(TabletIndexWriterTest, checkNotFinalizedAfterWriteRootIndex) {
  TabletIndexConfig config{
      .columns = {"col1"},
      .sortOrders = {velox::core::kAscNullsFirst},
      .enforceKeyOrder = false};
  auto writer = TabletIndexWriter::create(config, *pool_);

  Buffer buffer{*pool_};
  TestFileIndex fileIndex;
  writer->ensureStripeWrite(2);

  auto keyStream = createKeyStream(buffer, {{100, "aaa", "bbb"}});
  writer->addStripeKey(keyStream);

  writer->writeKeyStream(0, keyStream.chunks, writeStreamCallback(fileIndex));

  auto chunks = createChunks(buffer, {{100, 20}});
  writer->addStreamIndex(0, chunks);

  // Write root index - this finalizes the writer
  writer->writeRootIndex({0}, writeRootIndexCallback(fileIndex));

  // After finalization, mutation operations should throw
  NIMBLE_ASSERT_THROW(
      writer->ensureStripeWrite(2), "TabletIndexWriter has been finalized");
  NIMBLE_ASSERT_THROW(
      writer->addStripeKey(keyStream), "TabletIndexWriter has been finalized");
  NIMBLE_ASSERT_THROW(
      writer->addStreamIndex(0, chunks),
      "TabletIndexWriter has been finalized");
  NIMBLE_ASSERT_THROW(
      writer->writeKeyStream(
          0, keyStream.chunks, writeStreamCallback(fileIndex)),
      "TabletIndexWriter has been finalized");
  NIMBLE_ASSERT_THROW(
      writer->writeRootIndex({0}, writeRootIndexCallback(fileIndex)),
      "TabletIndexWriter has been finalized");
}

TEST_F(TabletIndexWriterTest, writeRootIndexWithEmptyStripeKeys) {
  TabletIndexConfig config{
      .columns = {"col1"},
      .sortOrders = {velox::core::kAscNullsFirst},
      .enforceKeyOrder = false};
  auto writer = TabletIndexWriter::create(config, *pool_);

  // Initialize but don't add any stripe keys
  Buffer buffer{*pool_};
  writer->ensureStripeWrite(2);

  TestFileIndex fileIndex;

  // writeRootIndex should succeed but not write anything when stripe keys are
  // empty
  EXPECT_NO_THROW(
      writer->writeRootIndex({}, writeRootIndexCallback(fileIndex)));
  EXPECT_TRUE(fileIndex.rootIndexData.empty());
}

TEST_F(TabletIndexWriterTest, emptyStreamInStripe) {
  TabletIndexConfig config{
      .columns = {"col1"},
      .sortOrders = {velox::core::kAscNullsFirst},
      .enforceKeyOrder = false};
  auto writer = TabletIndexWriter::create(config, *pool_);

  Buffer buffer{*pool_};
  TestFileIndex fileIndex;
  writer->ensureStripeWrite(3);

  auto keyStream = createKeyStream(buffer, {{100, "aaa", "bbb"}});
  writer->addStripeKey(keyStream);

  writer->writeKeyStream(0, keyStream.chunks, writeStreamCallback(fileIndex));

  // Stream 0 has data
  auto chunks0 = createChunks(buffer, {{50, 10}, {50, 12}});
  writer->addStreamIndex(0, chunks0);

  // Stream 1 is empty (no addStreamIndex call)

  // Stream 2 has data
  auto chunks2 = createChunks(buffer, {{100, 25}});
  writer->addStreamIndex(2, chunks2);

  EXPECT_NO_THROW(
      writer->writeIndexGroup(3, 1, createMetadataSectionCallback(fileIndex)));
  EXPECT_EQ(fileIndex.groupMetadataSections.size(), 1);
}

// Test writing and reading back index data with a single stripe group.
// This test verifies the complete write/read cycle by:
// 1. Writing index data using TabletIndexWriter with fake callbacks
// 2. Reading back the serialized data using TabletIndex and StripeIndexGroup
// 3. Verifying all index metadata matches expected values
// All stripes are in a single group, testing multi-stripe index within one
// group.
TEST_F(TabletIndexWriterTest, writeAndReadWithSingleGroup) {
  TabletIndexConfig config{
      .columns = {"col1", "col2"},
      .sortOrders = {velox::core::kAscNullsFirst, velox::core::kDescNullsLast},
      .enforceKeyOrder = true,
  };
  auto writer = TabletIndexWriter::create(config, *pool_);

  Buffer buffer{*pool_};
  TestFileIndex fileIndex;

  // ========== Stripe 0 ==========
  // Total rows: 100
  // Stream 0: 3 chunks (rows: 30, 45, 25)
  // Stream 1: 2 chunks (rows: 60, 40)
  // Key stream: 2 chunks (rows: 50, 50), keys: "aaa"->"bbb", "bbb"->"ccc"
  {
    writer->ensureStripeWrite(2);
    auto keyStream = createKeyStream(
        buffer,
        {
            {50, "aaa", "bbb"},
            {50, "bbb", "ccc"},
        });
    writer->addStripeKey(keyStream);
    writer->writeKeyStream(
        fileIndex.keyStreamData.size(),
        keyStream.chunks,
        writeStreamCallback(fileIndex));

    auto chunks0 = createChunks(buffer, {{30, 10}, {45, 15}, {25, 8}});
    auto chunks1 = createChunks(buffer, {{60, 20}, {40, 12}});
    writer->addStreamIndex(0, chunks0);
    writer->addStreamIndex(1, chunks1);
  }

  // ========== Stripe 1 ==========
  // Total rows: 200
  // Stream 0: 4 chunks (rows: 40, 55, 65, 40)
  // Stream 1: 1 chunk (rows: 200)
  // Key stream: 4 chunks (rows: 50, 50, 50, 50), keys: "ccc"->"ddd",
  // "ddd"->"eee", "eee"->"fff", "fff"->"ggg"
  {
    writer->ensureStripeWrite(2);
    auto keyStream = createKeyStream(
        buffer,
        {
            {50, "ccc", "ddd"},
            {50, "ddd", "eee"},
            {50, "eee", "fff"},
            {50, "fff", "ggg"},
        });
    writer->addStripeKey(keyStream);
    writer->writeKeyStream(
        fileIndex.keyStreamData.size(),
        keyStream.chunks,
        writeStreamCallback(fileIndex));

    auto chunks0 = createChunks(buffer, {{40, 5}, {55, 18}, {65, 22}, {40, 7}});
    auto chunks1 = createChunks(buffer, {{200, 50}});
    writer->addStreamIndex(0, chunks0);
    writer->addStreamIndex(1, chunks1);
  }

  // ========== Stripe 2 ==========
  // Total rows: 150
  // Stream 0: 2 chunks (rows: 100, 50)
  // Stream 1: 5 chunks (rows: 20, 35, 40, 25, 30)
  // Key stream: 1 chunk (rows: 150), keys: "ggg"->"hhh"
  {
    writer->ensureStripeWrite(2);
    auto keyStream = createKeyStream(
        buffer,
        {
            {150, "ggg", "hhh"},
        });
    writer->addStripeKey(keyStream);
    writer->writeKeyStream(
        fileIndex.keyStreamData.size(),
        keyStream.chunks,
        writeStreamCallback(fileIndex));

    auto chunks0 = createChunks(buffer, {{100, 30}, {50, 14}});
    auto chunks1 =
        createChunks(buffer, {{20, 6}, {35, 9}, {40, 11}, {25, 4}, {30, 16}});
    writer->addStreamIndex(0, chunks0);
    writer->addStreamIndex(1, chunks1);
  }

  // Write single index group containing all 3 stripes
  writer->writeIndexGroup(2, 3, createMetadataSectionCallback(fileIndex));

  // Write root index with all stripes in group 0
  writer->writeRootIndex({0, 0, 0}, writeRootIndexCallback(fileIndex));

  // ========== Verify written data ==========
  ASSERT_EQ(fileIndex.groupMetadataSections.size(), 1);
  ASSERT_FALSE(fileIndex.rootIndexData.empty());

  // Read and verify root index
  Section rootIndexSection{MetadataBuffer(
      *pool_, fileIndex.rootIndexData, CompressionType::Uncompressed)};
  auto tabletIndex = index::TabletIndex::create(std::move(rootIndexSection));

  // Verify basic properties
  EXPECT_EQ(tabletIndex->numStripes(), 3);
  EXPECT_EQ(tabletIndex->numIndexGroups(), 1);
  EXPECT_EQ(tabletIndex->indexColumns().size(), 2);
  EXPECT_EQ(tabletIndex->indexColumns()[0], "col1");
  EXPECT_EQ(tabletIndex->indexColumns()[1], "col2");

  // Verify sort orders
  ASSERT_EQ(tabletIndex->sortOrders().size(), 2);
  EXPECT_EQ(tabletIndex->sortOrders()[0], velox::core::kAscNullsFirst);
  EXPECT_EQ(tabletIndex->sortOrders()[1], velox::core::kDescNullsLast);

  // Verify stripe keys
  EXPECT_EQ(tabletIndex->minKey(), "aaa");
  EXPECT_EQ(tabletIndex->stripeKey(0), "ccc");
  EXPECT_EQ(tabletIndex->stripeKey(1), "ggg");
  EXPECT_EQ(tabletIndex->stripeKey(2), "hhh");
  EXPECT_EQ(tabletIndex->maxKey(), "hhh");

  // Verify key lookups
  // Keys before minKey
  EXPECT_FALSE(tabletIndex->lookup("9").has_value());
  EXPECT_FALSE(tabletIndex->lookup("aa").has_value());

  // Stripe 0: ["aaa", "ccc"]
  EXPECT_EQ(tabletIndex->lookup("aaa")->stripeIndex, 0);
  EXPECT_EQ(tabletIndex->lookup("bbb")->stripeIndex, 0);
  EXPECT_EQ(tabletIndex->lookup("ccc")->stripeIndex, 0);

  // Stripe 1: ("ccc", "ggg"]
  EXPECT_EQ(tabletIndex->lookup("ccd")->stripeIndex, 1);
  EXPECT_EQ(tabletIndex->lookup("ddd")->stripeIndex, 1);
  EXPECT_EQ(tabletIndex->lookup("eee")->stripeIndex, 1);
  EXPECT_EQ(tabletIndex->lookup("fff")->stripeIndex, 1);
  EXPECT_EQ(tabletIndex->lookup("ggg")->stripeIndex, 1);

  // Stripe 2: ("ggg", "hhh"]
  EXPECT_EQ(tabletIndex->lookup("ggh")->stripeIndex, 2);
  EXPECT_EQ(tabletIndex->lookup("hhh")->stripeIndex, 2);

  // Keys after maxKey
  EXPECT_FALSE(tabletIndex->lookup("hhi").has_value());
  EXPECT_FALSE(tabletIndex->lookup("iii").has_value());

  // ========== Verify Single Group (all 3 stripes) ==========
  {
    auto groupBuffer = std::make_unique<MetadataBuffer>(
        *pool_,
        fileIndex.groupMetadataSections[0],
        CompressionType::Uncompressed);
    auto stripeIndexGroup = index::StripeIndexGroup::create(
        0, tabletIndex->rootIndex(), std::move(groupBuffer));

    index::test::StripeIndexGroupTestHelper helper(stripeIndexGroup.get());
    EXPECT_EQ(helper.groupIndex(), 0);
    EXPECT_EQ(helper.firstStripe(), 0);
    EXPECT_EQ(helper.stripeCount(), 3);
    EXPECT_EQ(helper.streamCount(), 2);

    // Verify key stream stats
    // Stripe 0: 2 chunks (rows: 50, 50), keys: "bbb", "ccc"
    // Stripe 1: 4 chunks (rows: 50, 50, 50, 50), keys: "ddd", "eee", "fff",
    // "ggg" Stripe 2: 1 chunk (rows: 150), keys: "hhh"
    const auto keyStats = helper.keyStreamStats();

    // Accumulated chunk counts per stripe: 2, 2+4=6, 6+1=7
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{2, 6, 7}));

    // Accumulated row counts per chunk (within each stripe)
    // Stripe 0: 50, 100
    // Stripe 1: 50, 100, 150, 200
    // Stripe 2: 150
    EXPECT_EQ(
        keyStats.chunkRows,
        (std::vector<uint32_t>{50, 100, 50, 100, 150, 200, 150}));

    // Last key for each chunk
    EXPECT_EQ(
        keyStats.chunkKeys,
        (std::vector<std::string>{
            "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh"}));

    // Verify stream 0 position index stats
    // Stripe 0: 3 chunks (rows: 30, 45, 25)
    // Stripe 1: 4 chunks (rows: 40, 55, 65, 40)
    // Stripe 2: 2 chunks (rows: 100, 50)
    {
      auto stream0Stats = helper.streamStats(0);

      // Per-stream accumulated chunk counts:
      // Stripe 0: 3 chunks -> accumulated = 3
      // Stripe 1: 4 chunks -> accumulated = 3 + 4 = 7
      // Stripe 2: 2 chunks -> accumulated = 7 + 2 = 9
      EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{3, 7, 9}));

      // Accumulated row counts per chunk
      // Stripe 0: 30, 75, 100
      // Stripe 1: 40, 95, 160, 200
      // Stripe 2: 100, 150
      EXPECT_EQ(
          stream0Stats.chunkRows,
          (std::vector<uint32_t>{30, 75, 100, 40, 95, 160, 200, 100, 150}));
    }

    // Verify stream 1 position index stats
    // Stripe 0: 2 chunks (rows: 60, 40)
    // Stripe 1: 1 chunk (rows: 200)
    // Stripe 2: 5 chunks (rows: 20, 35, 40, 25, 30)
    {
      auto stream1Stats = helper.streamStats(1);

      // Per-stream accumulated chunk counts for stream 1:
      // Stripe 0: 2 chunks -> accumulated = 2
      // Stripe 1: 1 chunk -> accumulated = 2 + 1 = 3
      // Stripe 2: 5 chunks -> accumulated = 3 + 5 = 8
      EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{2, 3, 8}));

      // Accumulated row counts per chunk
      // Stripe 0: 60, 100
      // Stripe 1: 200
      // Stripe 2: 20, 55, 95, 120, 150
      EXPECT_EQ(
          stream1Stats.chunkRows,
          (std::vector<uint32_t>{60, 100, 200, 20, 55, 95, 120, 150}));
    }
  }
}

// Test writing and reading back index data with multiple stripe groups.
// This test verifies the complete write/read cycle by:
// 1. Writing index data using TabletIndexWriter with fake callbacks
// 2. Reading back the serialized data using TabletIndex and StripeIndexGroup
// 3. Verifying all index metadata matches expected values
TEST_F(TabletIndexWriterTest, writeAndReadWithMultipleGroups) {
  TabletIndexConfig config{
      .columns = {"col1", "col2"},
      .sortOrders = {velox::core::kAscNullsFirst, velox::core::kDescNullsLast},
      .enforceKeyOrder = true,
  };
  auto writer = TabletIndexWriter::create(config, *pool_);

  Buffer buffer{*pool_};
  TestFileIndex fileIndex;

  // ========== Stripe Group 0: Stripe 0 ==========
  // Total rows: 100
  // Stream 0: 2 chunks (rows: 50, 50)
  // Stream 1: 3 chunks (rows: 30, 40, 30)
  // Key stream: 2 chunks (rows: 50, 50), keys: "aaa"->"bbb", "bbb"->"ccc"
  {
    writer->ensureStripeWrite(2);
    auto keyStream = createKeyStream(
        buffer,
        {
            {50, "aaa", "bbb"},
            {50, "bbb", "ccc"},
        });
    writer->addStripeKey(keyStream);
    writer->writeKeyStream(
        fileIndex.keyStreamData.size(),
        keyStream.chunks,
        writeStreamCallback(fileIndex));

    auto chunks0 = createChunks(buffer, {{50, 10}, {50, 12}});
    auto chunks1 = createChunks(buffer, {{30, 8}, {40, 10}, {30, 6}});
    writer->addStreamIndex(0, chunks0);
    writer->addStreamIndex(1, chunks1);
    writer->writeIndexGroup(2, 1, createMetadataSectionCallback(fileIndex));
  }

  // ========== Stripe Group 1: Stripe 1 ==========
  // Total rows: 150
  // Stream 0: 3 chunks (rows: 40, 60, 50)
  // Stream 1: 2 chunks (rows: 80, 70)
  // Key stream: 3 chunks (rows: 50, 50, 50), keys: "ccc"->"ddd", "ddd"->"eee",
  // "eee"->"fff"
  {
    writer->ensureStripeWrite(2);
    auto keyStream = createKeyStream(
        buffer,
        {
            {50, "ccc", "ddd"},
            {50, "ddd", "eee"},
            {50, "eee", "fff"},
        });
    writer->addStripeKey(keyStream);
    writer->writeKeyStream(
        fileIndex.keyStreamData.size(),
        keyStream.chunks,
        writeStreamCallback(fileIndex));

    auto chunks0 = createChunks(buffer, {{40, 8}, {60, 14}, {50, 11}});
    auto chunks1 = createChunks(buffer, {{80, 18}, {70, 15}});
    writer->addStreamIndex(0, chunks0);
    writer->addStreamIndex(1, chunks1);
    writer->writeIndexGroup(2, 1, createMetadataSectionCallback(fileIndex));
  }

  // ========== Stripe Group 2: Stripe 2 ==========
  // Total rows: 120
  // Stream 0: 2 chunks (rows: 70, 50)
  // Stream 1: 4 chunks (rows: 25, 35, 30, 30)
  // Key stream: 2 chunks (rows: 60, 60), keys: "fff"->"ggg", "ggg"->"hhh"
  {
    writer->ensureStripeWrite(2);
    auto keyStream = createKeyStream(
        buffer,
        {
            {60, "fff", "ggg"},
            {60, "ggg", "hhh"},
        });
    writer->addStripeKey(keyStream);
    writer->writeKeyStream(
        fileIndex.keyStreamData.size(),
        keyStream.chunks,
        writeStreamCallback(fileIndex));

    auto chunks0 = createChunks(buffer, {{70, 16}, {50, 13}});
    auto chunks1 = createChunks(buffer, {{25, 5}, {35, 7}, {30, 6}, {30, 8}});
    writer->addStreamIndex(0, chunks0);
    writer->addStreamIndex(1, chunks1);
    writer->writeIndexGroup(2, 1, createMetadataSectionCallback(fileIndex));
  }

  // Write root index with stripe group indices {0, 1, 2}
  writer->writeRootIndex({0, 1, 2}, writeRootIndexCallback(fileIndex));

  // ========== Verify written data ==========
  ASSERT_EQ(fileIndex.groupMetadataSections.size(), 3);
  ASSERT_FALSE(fileIndex.rootIndexData.empty());

  // Read and verify root index
  Section rootIndexSection{MetadataBuffer(
      *pool_, fileIndex.rootIndexData, CompressionType::Uncompressed)};
  auto tabletIndex = index::TabletIndex::create(std::move(rootIndexSection));

  // Verify basic properties
  EXPECT_EQ(tabletIndex->numStripes(), 3);
  EXPECT_EQ(tabletIndex->numIndexGroups(), 3);
  EXPECT_EQ(tabletIndex->indexColumns().size(), 2);
  EXPECT_EQ(tabletIndex->indexColumns()[0], "col1");
  EXPECT_EQ(tabletIndex->indexColumns()[1], "col2");

  // Verify stripe keys
  EXPECT_EQ(tabletIndex->minKey(), "aaa");
  EXPECT_EQ(tabletIndex->stripeKey(0), "ccc");
  EXPECT_EQ(tabletIndex->stripeKey(1), "fff");
  EXPECT_EQ(tabletIndex->stripeKey(2), "hhh");
  EXPECT_EQ(tabletIndex->maxKey(), "hhh");

  // Verify key lookups
  // Keys before minKey
  EXPECT_FALSE(tabletIndex->lookup("9").has_value());
  EXPECT_FALSE(tabletIndex->lookup("aa").has_value());

  // Stripe 0: ["aaa", "ccc"]
  EXPECT_EQ(tabletIndex->lookup("aaa")->stripeIndex, 0);
  EXPECT_EQ(tabletIndex->lookup("bbb")->stripeIndex, 0);
  EXPECT_EQ(tabletIndex->lookup("ccc")->stripeIndex, 0);

  // Stripe 1: ("ccc", "fff"]
  EXPECT_EQ(tabletIndex->lookup("ccd")->stripeIndex, 1);
  EXPECT_EQ(tabletIndex->lookup("ddd")->stripeIndex, 1);
  EXPECT_EQ(tabletIndex->lookup("eee")->stripeIndex, 1);
  EXPECT_EQ(tabletIndex->lookup("fff")->stripeIndex, 1);

  // Stripe 2: ("fff", "hhh"]
  EXPECT_EQ(tabletIndex->lookup("ffg")->stripeIndex, 2);
  EXPECT_EQ(tabletIndex->lookup("ggg")->stripeIndex, 2);
  EXPECT_EQ(tabletIndex->lookup("hhh")->stripeIndex, 2);

  // Keys after maxKey
  EXPECT_FALSE(tabletIndex->lookup("hhi").has_value());
  EXPECT_FALSE(tabletIndex->lookup("iii").has_value());

  // ========== Verify Group 0 (Stripe 0) ==========
  {
    auto groupBuffer = std::make_unique<MetadataBuffer>(
        *pool_,
        fileIndex.groupMetadataSections[0],
        CompressionType::Uncompressed);
    auto stripeIndexGroup = index::StripeIndexGroup::create(
        0, tabletIndex->rootIndex(), std::move(groupBuffer));

    index::test::StripeIndexGroupTestHelper helper(stripeIndexGroup.get());
    EXPECT_EQ(helper.groupIndex(), 0);
    EXPECT_EQ(helper.firstStripe(), 0);
    EXPECT_EQ(helper.stripeCount(), 1);
    EXPECT_EQ(helper.streamCount(), 2);

    // Verify key stream stats
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{50, 100}));
    EXPECT_EQ(keyStats.chunkKeys, (std::vector<std::string>{"bbb", "ccc"}));

    // Verify stream 0 stats: 2 chunks (rows: 50, 50)
    auto stream0Stats = helper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream0Stats.chunkRows, (std::vector<uint32_t>{50, 100}));

    // Verify stream 1 stats: 3 chunks (rows: 30, 40, 30)
    auto stream1Stats = helper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(stream1Stats.chunkRows, (std::vector<uint32_t>{30, 70, 100}));
  }

  // ========== Verify Group 1 (Stripe 1) ==========
  {
    auto groupBuffer = std::make_unique<MetadataBuffer>(
        *pool_,
        fileIndex.groupMetadataSections[1],
        CompressionType::Uncompressed);
    auto stripeIndexGroup = index::StripeIndexGroup::create(
        1, tabletIndex->rootIndex(), std::move(groupBuffer));

    index::test::StripeIndexGroupTestHelper helper(stripeIndexGroup.get());
    EXPECT_EQ(helper.groupIndex(), 1);
    EXPECT_EQ(helper.firstStripe(), 1);
    EXPECT_EQ(helper.stripeCount(), 1);
    EXPECT_EQ(helper.streamCount(), 2);

    // Verify key stream stats: 3 chunks
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{50, 100, 150}));
    EXPECT_EQ(
        keyStats.chunkKeys, (std::vector<std::string>{"ddd", "eee", "fff"}));

    // Verify stream 0 stats: 3 chunks (rows: 40, 60, 50)
    auto stream0Stats = helper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(stream0Stats.chunkRows, (std::vector<uint32_t>{40, 100, 150}));

    // Verify stream 1 stats: 2 chunks (rows: 80, 70)
    auto stream1Stats = helper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream1Stats.chunkRows, (std::vector<uint32_t>{80, 150}));
  }

  // ========== Verify Group 2 (Stripe 2) ==========
  {
    auto groupBuffer = std::make_unique<MetadataBuffer>(
        *pool_,
        fileIndex.groupMetadataSections[2],
        CompressionType::Uncompressed);
    auto stripeIndexGroup = index::StripeIndexGroup::create(
        2, tabletIndex->rootIndex(), std::move(groupBuffer));

    index::test::StripeIndexGroupTestHelper helper(stripeIndexGroup.get());
    EXPECT_EQ(helper.groupIndex(), 2);
    EXPECT_EQ(helper.firstStripe(), 2);
    EXPECT_EQ(helper.stripeCount(), 1);
    EXPECT_EQ(helper.streamCount(), 2);

    // Verify key stream stats: 2 chunks
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{60, 120}));
    EXPECT_EQ(keyStats.chunkKeys, (std::vector<std::string>{"ggg", "hhh"}));

    // Verify stream 0 stats: 2 chunks (rows: 70, 50)
    auto stream0Stats = helper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream0Stats.chunkRows, (std::vector<uint32_t>{70, 120}));

    // Verify stream 1 stats: 4 chunks (rows: 25, 35, 30, 30)
    auto stream1Stats = helper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{4}));
    EXPECT_EQ(stream1Stats.chunkRows, (std::vector<uint32_t>{25, 60, 90, 120}));
  }
}

// Test writing and reading back index data with multiple stripe groups and
// empty streams. This test verifies the position index handles missing streams
// correctly across group boundaries.
//
// Configuration (4 stripes, 4 streams, 4 groups - one group per stripe):
// - Stream 0: empty in first stripe (stripe 0 / group 0)
// - Stream 1: empty in middle stripe (stripe 1 / group 1)
// - Stream 2: empty in stripe 2 (group 2)
// - Stream 3: has data in all stripes
// - Stripe 3 / Group 3: ALL streams have data (no empty streams)
TEST_F(TabletIndexWriterTest, writeAndReadWithMultipleGroupsAndEmptyStream) {
  TabletIndexConfig config{
      .columns = {"col1"},
      .sortOrders = {velox::core::kAscNullsFirst},
      .enforceKeyOrder = true,
  };
  auto writer = TabletIndexWriter::create(config, *pool_);

  Buffer buffer{*pool_};
  TestFileIndex fileIndex;

  // ========== Stripe 0 (Group 0) ==========
  // Total rows: 100
  // Stream 0: EMPTY (no data in first stripe)
  // Stream 1: 2 chunks (rows: 50, 50)
  // Stream 2: 3 chunks (rows: 30, 40, 30)
  // Stream 3: 2 chunks (rows: 60, 40)
  // Key stream: 2 chunks (rows: 50, 50), keys: "aaa"->"bbb", "bbb"->"ccc"
  {
    writer->ensureStripeWrite(4);
    auto keyStream = createKeyStream(
        buffer,
        {
            {50, "aaa", "bbb"},
            {50, "bbb", "ccc"},
        });
    writer->addStripeKey(keyStream);
    writer->writeKeyStream(
        fileIndex.keyStreamData.size(),
        keyStream.chunks,
        writeStreamCallback(fileIndex));

    // Stream 0 is empty - no addStreamIndex call
    auto chunks1 = createChunks(buffer, {{50, 10}, {50, 12}});
    auto chunks2 = createChunks(buffer, {{30, 8}, {40, 10}, {30, 6}});
    auto chunks3 = createChunks(buffer, {{60, 15}, {40, 10}});
    writer->addStreamIndex(1, chunks1);
    writer->addStreamIndex(2, chunks2);
    writer->addStreamIndex(3, chunks3);
    writer->writeIndexGroup(4, 1, createMetadataSectionCallback(fileIndex));
  }

  // ========== Stripe 1 (Group 1) ==========
  // Total rows: 150
  // Stream 0: 3 chunks (rows: 40, 60, 50)
  // Stream 1: EMPTY (no data in middle stripe)
  // Stream 2: 2 chunks (rows: 80, 70)
  // Stream 3: 1 chunk (rows: 150)
  // Key stream: 3 chunks (rows: 50, 50, 50), keys: "ccc"->"ddd", "ddd"->"eee",
  // "eee"->"fff"
  {
    writer->ensureStripeWrite(4);
    auto keyStream = createKeyStream(
        buffer,
        {
            {50, "ccc", "ddd"},
            {50, "ddd", "eee"},
            {50, "eee", "fff"},
        });
    writer->addStripeKey(keyStream);
    writer->writeKeyStream(
        fileIndex.keyStreamData.size(),
        keyStream.chunks,
        writeStreamCallback(fileIndex));

    auto chunks0 = createChunks(buffer, {{40, 8}, {60, 14}, {50, 11}});
    // Stream 1 is empty - no addStreamIndex call
    auto chunks2 = createChunks(buffer, {{80, 18}, {70, 15}});
    auto chunks3 = createChunks(buffer, {{150, 30}});
    writer->addStreamIndex(0, chunks0);
    writer->addStreamIndex(2, chunks2);
    writer->addStreamIndex(3, chunks3);
    writer->writeIndexGroup(4, 1, createMetadataSectionCallback(fileIndex));
  }

  // ========== Stripe 2 (Group 2) ==========
  // Total rows: 120
  // Stream 0: 2 chunks (rows: 70, 50)
  // Stream 1: 4 chunks (rows: 25, 35, 30, 30)
  // Stream 2: EMPTY (no data in this stripe)
  // Stream 3: 3 chunks (rows: 40, 40, 40)
  // Key stream: 2 chunks (rows: 60, 60), keys: "fff"->"ggg", "ggg"->"hhh"
  {
    writer->ensureStripeWrite(4);
    auto keyStream = createKeyStream(
        buffer,
        {
            {60, "fff", "ggg"},
            {60, "ggg", "hhh"},
        });
    writer->addStripeKey(keyStream);
    writer->writeKeyStream(
        fileIndex.keyStreamData.size(),
        keyStream.chunks,
        writeStreamCallback(fileIndex));

    auto chunks0 = createChunks(buffer, {{70, 16}, {50, 13}});
    auto chunks1 = createChunks(buffer, {{25, 5}, {35, 7}, {30, 6}, {30, 8}});
    // Stream 2 is empty - no addStreamIndex call
    auto chunks3 = createChunks(buffer, {{40, 10}, {40, 10}, {40, 10}});
    writer->addStreamIndex(0, chunks0);
    writer->addStreamIndex(1, chunks1);
    writer->addStreamIndex(3, chunks3);
    writer->writeIndexGroup(4, 1, createMetadataSectionCallback(fileIndex));
  }

  // ========== Stripe 3 (Group 3) ==========
  // Total rows: 100 - ALL STREAMS HAVE DATA
  // Stream 0: 2 chunks (rows: 50, 50)
  // Stream 1: 2 chunks (rows: 40, 60)
  // Stream 2: 1 chunk (rows: 100)
  // Stream 3: 2 chunks (rows: 30, 70)
  // Key stream: 2 chunks (rows: 50, 50), keys: "hhh"->"iii", "iii"->"jjj"
  {
    writer->ensureStripeWrite(4);
    auto keyStream = createKeyStream(
        buffer,
        {
            {50, "hhh", "iii"},
            {50, "iii", "jjj"},
        });
    writer->addStripeKey(keyStream);
    writer->writeKeyStream(
        fileIndex.keyStreamData.size(),
        keyStream.chunks,
        writeStreamCallback(fileIndex));

    auto chunks0 = createChunks(buffer, {{50, 12}, {50, 12}});
    auto chunks1 = createChunks(buffer, {{40, 10}, {60, 14}});
    auto chunks2 = createChunks(buffer, {{100, 25}});
    auto chunks3 = createChunks(buffer, {{30, 8}, {70, 18}});
    writer->addStreamIndex(0, chunks0);
    writer->addStreamIndex(1, chunks1);
    writer->addStreamIndex(2, chunks2);
    writer->addStreamIndex(3, chunks3);
    writer->writeIndexGroup(4, 1, createMetadataSectionCallback(fileIndex));
  }

  // Write root index with stripe group indices {0, 1, 2, 3}
  writer->writeRootIndex({0, 1, 2, 3}, writeRootIndexCallback(fileIndex));

  // ========== Verify written data ==========
  ASSERT_EQ(fileIndex.groupMetadataSections.size(), 4);
  ASSERT_FALSE(fileIndex.rootIndexData.empty());

  // Read and verify root index
  Section rootIndexSection{MetadataBuffer(
      *pool_, fileIndex.rootIndexData, CompressionType::Uncompressed)};
  auto tabletIndex = index::TabletIndex::create(std::move(rootIndexSection));

  // Verify basic properties
  EXPECT_EQ(tabletIndex->numStripes(), 4);
  EXPECT_EQ(tabletIndex->numIndexGroups(), 4);

  // Verify key lookups
  EXPECT_EQ(tabletIndex->lookup("aaa")->stripeIndex, 0);
  EXPECT_EQ(tabletIndex->lookup("ccc")->stripeIndex, 0);
  EXPECT_EQ(tabletIndex->lookup("ddd")->stripeIndex, 1);
  EXPECT_EQ(tabletIndex->lookup("fff")->stripeIndex, 1);
  EXPECT_EQ(tabletIndex->lookup("ggg")->stripeIndex, 2);
  EXPECT_EQ(tabletIndex->lookup("hhh")->stripeIndex, 2);
  EXPECT_EQ(tabletIndex->lookup("iii")->stripeIndex, 3);
  EXPECT_EQ(tabletIndex->lookup("jjj")->stripeIndex, 3);
  EXPECT_FALSE(tabletIndex->lookup("kkk").has_value());

  // ========== Verify Group 0 (Stripe 0) ==========
  // Stream 0: EMPTY
  // Stream 1: 2 chunks (rows: 50, 50)
  // Stream 2: 3 chunks (rows: 30, 40, 30)
  // Stream 3: 2 chunks (rows: 60, 40)
  {
    auto groupBuffer = std::make_unique<MetadataBuffer>(
        *pool_,
        fileIndex.groupMetadataSections[0],
        CompressionType::Uncompressed);
    auto stripeIndexGroup = index::StripeIndexGroup::create(
        0, tabletIndex->rootIndex(), std::move(groupBuffer));

    index::test::StripeIndexGroupTestHelper helper(stripeIndexGroup.get());
    EXPECT_EQ(helper.groupIndex(), 0);
    EXPECT_EQ(helper.firstStripe(), 0);
    EXPECT_EQ(helper.stripeCount(), 1);
    EXPECT_EQ(helper.streamCount(), 4);

    // Verify key stream stats
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{50, 100}));
    EXPECT_EQ(keyStats.chunkKeys, (std::vector<std::string>{"bbb", "ccc"}));

    // Stream 0: EMPTY in this group
    auto stream0Stats = helper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{0}));
    EXPECT_TRUE(stream0Stats.chunkRows.empty());

    // Stream 1: 2 chunks (rows: 50, 50)
    auto stream1Stats = helper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream1Stats.chunkRows, (std::vector<uint32_t>{50, 100}));

    // Stream 2: 3 chunks (rows: 30, 40, 30)
    auto stream2Stats = helper.streamStats(2);
    EXPECT_EQ(stream2Stats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(stream2Stats.chunkRows, (std::vector<uint32_t>{30, 70, 100}));

    // Stream 3: 2 chunks (rows: 60, 40)
    auto stream3Stats = helper.streamStats(3);
    EXPECT_EQ(stream3Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream3Stats.chunkRows, (std::vector<uint32_t>{60, 100}));
  }

  // ========== Verify Group 1 (Stripe 1) ==========
  // Stream 0: 3 chunks (rows: 40, 60, 50)
  // Stream 1: EMPTY
  // Stream 2: 2 chunks (rows: 80, 70)
  // Stream 3: 1 chunk (rows: 150)
  {
    auto groupBuffer = std::make_unique<MetadataBuffer>(
        *pool_,
        fileIndex.groupMetadataSections[1],
        CompressionType::Uncompressed);
    auto stripeIndexGroup = index::StripeIndexGroup::create(
        1, tabletIndex->rootIndex(), std::move(groupBuffer));

    index::test::StripeIndexGroupTestHelper helper(stripeIndexGroup.get());
    EXPECT_EQ(helper.groupIndex(), 1);
    EXPECT_EQ(helper.firstStripe(), 1);
    EXPECT_EQ(helper.stripeCount(), 1);
    EXPECT_EQ(helper.streamCount(), 4);

    // Verify key stream stats
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{50, 100, 150}));
    EXPECT_EQ(
        keyStats.chunkKeys, (std::vector<std::string>{"ddd", "eee", "fff"}));

    // Stream 0: 3 chunks (rows: 40, 60, 50)
    auto stream0Stats = helper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(stream0Stats.chunkRows, (std::vector<uint32_t>{40, 100, 150}));

    // Stream 1: EMPTY in this group
    auto stream1Stats = helper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{0}));
    EXPECT_TRUE(stream1Stats.chunkRows.empty());

    // Stream 2: 2 chunks (rows: 80, 70)
    auto stream2Stats = helper.streamStats(2);
    EXPECT_EQ(stream2Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream2Stats.chunkRows, (std::vector<uint32_t>{80, 150}));

    // Stream 3: 1 chunk (rows: 150)
    auto stream3Stats = helper.streamStats(3);
    EXPECT_EQ(stream3Stats.chunkCounts, (std::vector<uint32_t>{1}));
    EXPECT_EQ(stream3Stats.chunkRows, (std::vector<uint32_t>{150}));
  }

  // ========== Verify Group 2 (Stripe 2) ==========
  // Stream 0: 2 chunks (rows: 70, 50)
  // Stream 1: 4 chunks (rows: 25, 35, 30, 30)
  // Stream 2: EMPTY
  // Stream 3: 3 chunks (rows: 40, 40, 40)
  {
    auto groupBuffer = std::make_unique<MetadataBuffer>(
        *pool_,
        fileIndex.groupMetadataSections[2],
        CompressionType::Uncompressed);
    auto stripeIndexGroup = index::StripeIndexGroup::create(
        2, tabletIndex->rootIndex(), std::move(groupBuffer));

    index::test::StripeIndexGroupTestHelper helper(stripeIndexGroup.get());
    EXPECT_EQ(helper.groupIndex(), 2);
    EXPECT_EQ(helper.firstStripe(), 2);
    EXPECT_EQ(helper.stripeCount(), 1);
    EXPECT_EQ(helper.streamCount(), 4);

    // Verify key stream stats
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{60, 120}));
    EXPECT_EQ(keyStats.chunkKeys, (std::vector<std::string>{"ggg", "hhh"}));

    // Stream 0: 2 chunks (rows: 70, 50)
    auto stream0Stats = helper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream0Stats.chunkRows, (std::vector<uint32_t>{70, 120}));

    // Stream 1: 4 chunks (rows: 25, 35, 30, 30)
    auto stream1Stats = helper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{4}));
    EXPECT_EQ(stream1Stats.chunkRows, (std::vector<uint32_t>{25, 60, 90, 120}));

    // Stream 2: EMPTY in this group
    auto stream2Stats = helper.streamStats(2);
    EXPECT_EQ(stream2Stats.chunkCounts, (std::vector<uint32_t>{0}));
    EXPECT_TRUE(stream2Stats.chunkRows.empty());

    // Stream 3: 3 chunks (rows: 40, 40, 40)
    auto stream3Stats = helper.streamStats(3);
    EXPECT_EQ(stream3Stats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(stream3Stats.chunkRows, (std::vector<uint32_t>{40, 80, 120}));
  }

  // ========== Verify Group 3 (Stripe 3) - ALL STREAMS HAVE DATA ==========
  // Stream 0: 2 chunks (rows: 50, 50)
  // Stream 1: 2 chunks (rows: 40, 60)
  // Stream 2: 1 chunk (rows: 100)
  // Stream 3: 2 chunks (rows: 30, 70)
  {
    auto groupBuffer = std::make_unique<MetadataBuffer>(
        *pool_,
        fileIndex.groupMetadataSections[3],
        CompressionType::Uncompressed);
    auto stripeIndexGroup = index::StripeIndexGroup::create(
        3, tabletIndex->rootIndex(), std::move(groupBuffer));

    index::test::StripeIndexGroupTestHelper helper(stripeIndexGroup.get());
    EXPECT_EQ(helper.groupIndex(), 3);
    EXPECT_EQ(helper.firstStripe(), 3);
    EXPECT_EQ(helper.stripeCount(), 1);
    EXPECT_EQ(helper.streamCount(), 4);

    // Verify key stream stats
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{50, 100}));
    EXPECT_EQ(keyStats.chunkKeys, (std::vector<std::string>{"iii", "jjj"}));

    // Stream 0: 2 chunks (rows: 50, 50)
    auto stream0Stats = helper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream0Stats.chunkRows, (std::vector<uint32_t>{50, 100}));

    // Stream 1: 2 chunks (rows: 40, 60)
    auto stream1Stats = helper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream1Stats.chunkRows, (std::vector<uint32_t>{40, 100}));

    // Stream 2: 1 chunk (rows: 100)
    auto stream2Stats = helper.streamStats(2);
    EXPECT_EQ(stream2Stats.chunkCounts, (std::vector<uint32_t>{1}));
    EXPECT_EQ(stream2Stats.chunkRows, (std::vector<uint32_t>{100}));

    // Stream 3: 2 chunks (rows: 30, 70)
    auto stream3Stats = helper.streamStats(3);
    EXPECT_EQ(stream3Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream3Stats.chunkRows, (std::vector<uint32_t>{30, 100}));
  }
}

} // namespace facebook::nimble::test
