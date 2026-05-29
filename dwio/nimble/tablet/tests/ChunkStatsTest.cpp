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

#include "dwio/nimble/tablet/ChunkIndexWriter.h"

#include <cmath>
#include <limits>
#include <vector>

#include <gtest/gtest.h>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/index/tests/ClusterIndexTestUtils.h"
#include "dwio/nimble/tablet/ChunkIndexGenerated.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/TabletWriter.h"
#include "dwio/nimble/velox/StreamChunker.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble::test {

using index::test::ChunkSpec;
using index::test::createChunks;

namespace {

// Helper to create a ChunkStats with PLAIN-encoded int64 min/max.
ChunkStats makeIntStats(
    int64_t min,
    int64_t max,
    bool hasNulls = false,
    uint64_t nullCount = 0) {
  ChunkStats stats;
  stats.hasNulls = hasNulls;
  stats.nullCount = nullCount;
  stats.minValue.assign(reinterpret_cast<const char*>(&min), sizeof(int64_t));
  stats.maxValue.assign(reinterpret_cast<const char*>(&max), sizeof(int64_t));
  return stats;
}

// Helper to create a ChunkStats with PLAIN-encoded double min/max.
ChunkStats makeDoubleStats(double min, double max, bool hasNulls = false) {
  ChunkStats stats;
  stats.hasNulls = hasNulls;
  stats.minValue.assign(reinterpret_cast<const char*>(&min), sizeof(double));
  stats.maxValue.assign(reinterpret_cast<const char*>(&max), sizeof(double));
  return stats;
}

// Helper to read an int64 from a raw byte vector at chunk index i.
int64_t readInt64(const flatbuffers::Vector<uint8_t>* vec, size_t chunkIndex) {
  int64_t val;
  std::memcpy(&val, vec->data() + chunkIndex * sizeof(int64_t), sizeof(val));
  return val;
}

// Helper to read a double from a raw byte vector at chunk index i.
double readDouble(const flatbuffers::Vector<uint8_t>* vec, size_t chunkIndex) {
  double val;
  std::memcpy(&val, vec->data() + chunkIndex * sizeof(double), sizeof(val));
  return val;
}

} // namespace

// Captures serialized chunk index data for verification in tests.
struct TestChunkStatsFileIndex {
  std::vector<std::string> groupMetadataSections;
  std::string rootIndexData;
};

class ChunkStatsTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = velox::memory::memoryManager()->addLeafPool();
  }

  static auto createMetadataSectionCallback(
      TestChunkStatsFileIndex& fileIndex) {
    return [&fileIndex](std::string_view metadata) -> MetadataSection {
      fileIndex.groupMetadataSections.emplace_back(metadata);
      return MetadataSection(
          0,
          static_cast<uint32_t>(metadata.size()),
          CompressionType::Uncompressed);
    };
  }

  static auto writeRootCallback(TestChunkStatsFileIndex& fileIndex) {
    return [&fileIndex](const std::string& name, std::string_view content) {
      EXPECT_EQ(name, nimble::kChunkIndexSection);
      fileIndex.rootIndexData = std::string(content);
    };
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

TEST_F(ChunkStatsTest, integerStatsRoundTrip) {
  ChunkIndexWriter writer(*pool_, 0, /*enableChunkStats=*/true);
  Buffer buffer{*pool_};
  TestChunkStatsFileIndex fileIndex;

  // 1 stripe, 2 streams. Stream 0 has int stats, stream 1 has no stats.
  writer.newStripe(2);

  auto chunks0 = createChunks(buffer, {{100, 40}, {100, 40}});
  chunks0[0].stats = makeIntStats(10, 50);
  chunks0[1].stats = makeIntStats(-5, 100, /*hasNulls=*/true, /*nullCount=*/7);
  writer.addStream(0, chunks0);

  auto chunks1 = createChunks(buffer, {{100, 30}});
  writer.addStream(1, chunks1);

  writer.writeGroup(2, 1, createMetadataSectionCallback(fileIndex));
  writer.writeRoot(writeRootCallback(fileIndex));

  ASSERT_EQ(fileIndex.groupMetadataSections.size(), 1);
  ASSERT_FALSE(fileIndex.rootIndexData.empty());

  const auto* stripeIndex =
      flatbuffers::GetRoot<serialization::StripeChunkIndex>(
          fileIndex.groupMetadataSections[0].data());
  ASSERT_NE(stripeIndex, nullptr);

  const auto* streamStatsVec = stripeIndex->stream_stats();
  ASSERT_NE(streamStatsVec, nullptr);
  ASSERT_EQ(streamStatsVec->size(), 2);

  // Stream 0: int stats for 2 chunks.
  const auto* ss0 = streamStatsVec->Get(0);
  ASSERT_NE(ss0, nullptr);
  ASSERT_NE(ss0->min_values(), nullptr);
  ASSERT_NE(ss0->max_values(), nullptr);
  ASSERT_NE(ss0->has_nulls(), nullptr);
  EXPECT_EQ(ss0->value_size(), sizeof(int64_t));
  ASSERT_EQ(ss0->min_values()->size(), 2 * sizeof(int64_t));

  ASSERT_NE(ss0->null_counts(), nullptr);
  EXPECT_EQ(readInt64(ss0->min_values(), 0), 10);
  EXPECT_EQ(readInt64(ss0->max_values(), 0), 50);
  EXPECT_EQ(ss0->has_nulls()->Get(0), 0);
  EXPECT_EQ(ss0->null_counts()->Get(0), 0);

  EXPECT_EQ(readInt64(ss0->min_values(), 1), -5);
  EXPECT_EQ(readInt64(ss0->max_values(), 1), 100);
  EXPECT_EQ(ss0->has_nulls()->Get(1), 1);
  EXPECT_EQ(ss0->null_counts()->Get(1), 7);

  // Stream 1: no typed stats.
  const auto* ss1 = streamStatsVec->Get(1);
  ASSERT_NE(ss1, nullptr);
  EXPECT_EQ(ss1->min_values(), nullptr);
}

TEST_F(ChunkStatsTest, doubleStatsRoundTrip) {
  ChunkIndexWriter writer(*pool_, 0, /*enableChunkStats=*/true);
  Buffer buffer{*pool_};
  TestChunkStatsFileIndex fileIndex;

  writer.newStripe(1);

  auto chunks = createChunks(buffer, {{200, 80}});
  chunks[0].stats = makeDoubleStats(-1.5, 99.9);
  writer.addStream(0, chunks);

  writer.writeGroup(1, 1, createMetadataSectionCallback(fileIndex));
  writer.writeRoot(writeRootCallback(fileIndex));

  const auto* stripeIndex =
      flatbuffers::GetRoot<serialization::StripeChunkIndex>(
          fileIndex.groupMetadataSections[0].data());
  const auto* ss0 = stripeIndex->stream_stats()->Get(0);

  ASSERT_NE(ss0->min_values(), nullptr);
  ASSERT_NE(ss0->max_values(), nullptr);
  EXPECT_EQ(ss0->value_size(), sizeof(double));
  EXPECT_DOUBLE_EQ(readDouble(ss0->min_values(), 0), -1.5);
  EXPECT_DOUBLE_EQ(readDouble(ss0->max_values(), 0), 99.9);
}

TEST_F(ChunkStatsTest, noStatsNoStreamStatsField) {
  ChunkIndexWriter writer(*pool_, 0, /*enableChunkStats=*/false);
  Buffer buffer{*pool_};
  TestChunkStatsFileIndex fileIndex;

  writer.newStripe(1);
  writer.addStream(0, createChunks(buffer, {{100, 40}}));
  writer.writeGroup(1, 1, createMetadataSectionCallback(fileIndex));
  writer.writeRoot(writeRootCallback(fileIndex));

  const auto* stripeIndex =
      flatbuffers::GetRoot<serialization::StripeChunkIndex>(
          fileIndex.groupMetadataSections[0].data());
  // No stats attached — stream_stats should be absent (null offset).
  EXPECT_EQ(stripeIndex->stream_stats(), nullptr);
}

TEST_F(ChunkStatsTest, multipleStripesInGroup) {
  ChunkIndexWriter writer(*pool_, 0, /*enableChunkStats=*/true);
  Buffer buffer{*pool_};
  TestChunkStatsFileIndex fileIndex;

  writer.newStripe(1);
  auto chunks0 = createChunks(buffer, {{100, 40}});
  chunks0[0].stats = makeIntStats(1, 10);
  writer.addStream(0, chunks0);

  writer.newStripe(1);
  auto chunks1 = createChunks(buffer, {{100, 40}});
  chunks1[0].stats = makeIntStats(20, 30, /*hasNulls=*/true);
  writer.addStream(0, chunks1);

  writer.writeGroup(1, 2, createMetadataSectionCallback(fileIndex));
  writer.writeRoot(writeRootCallback(fileIndex));

  const auto* stripeIndex =
      flatbuffers::GetRoot<serialization::StripeChunkIndex>(
          fileIndex.groupMetadataSections[0].data());
  const auto* streamStatsVec = stripeIndex->stream_stats();
  ASSERT_NE(streamStatsVec, nullptr);
  ASSERT_EQ(streamStatsVec->size(), 2);

  const auto* s0 = streamStatsVec->Get(0);
  EXPECT_EQ(readInt64(s0->min_values(), 0), 1);
  EXPECT_EQ(readInt64(s0->max_values(), 0), 10);
  EXPECT_EQ(s0->has_nulls()->Get(0), 0);

  const auto* s1 = streamStatsVec->Get(1);
  EXPECT_EQ(readInt64(s1->min_values(), 0), 20);
  EXPECT_EQ(readInt64(s1->max_values(), 0), 30);
  EXPECT_EQ(s1->has_nulls()->Get(0), 1);
}

TEST_F(ChunkStatsTest, partialStatsOmitsMinMax) {
  // When some chunks have stats and others don't (e.g., floats with NaN),
  // the writer must omit min/max for the entire stream to avoid false
  // bounds that would cause incorrect filter pushdown.
  ChunkIndexWriter writer(*pool_, 0, /*enableChunkStats=*/true);
  Buffer buffer{*pool_};
  TestChunkStatsFileIndex fileIndex;

  writer.newStripe(1);

  auto chunks = createChunks(buffer, {{100, 40}, {100, 40}});
  chunks[0].stats = makeIntStats(10, 50);
  // Chunk 1 has stats but with empty minValue/maxValue (simulates NaN).
  chunks[1].stats = ChunkStats{};
  chunks[1].stats->hasNulls = false;
  writer.addStream(0, chunks);

  writer.writeGroup(1, 1, createMetadataSectionCallback(fileIndex));
  writer.writeRoot(writeRootCallback(fileIndex));

  const auto* stripeIndex =
      flatbuffers::GetRoot<serialization::StripeChunkIndex>(
          fileIndex.groupMetadataSections[0].data());
  const auto* streamStatsVec = stripeIndex->stream_stats();
  ASSERT_NE(streamStatsVec, nullptr);
  ASSERT_EQ(streamStatsVec->size(), 1);

  const auto* ss0 = streamStatsVec->Get(0);
  ASSERT_NE(ss0, nullptr);
  // has_nulls is still available since it's independent of min/max.
  ASSERT_NE(ss0->has_nulls(), nullptr);
  // min/max must be omitted (not zero-filled) to avoid false bounds.
  EXPECT_EQ(ss0->min_values(), nullptr);
  EXPECT_EQ(ss0->max_values(), nullptr);
  EXPECT_EQ(ss0->value_size(), 0);
}

// Direct unit tests for detail::computeChunkStats.

TEST(ComputeChunkStatsTest, integerMinMax) {
  std::vector<int64_t> data = {5, -3, 42, 0, 17};
  auto stats = detail::computeChunkStats(data.begin(), data.end());

  EXPECT_FALSE(stats.hasNulls);
  ASSERT_EQ(stats.minValue.size(), sizeof(int64_t));
  ASSERT_EQ(stats.maxValue.size(), sizeof(int64_t));

  int64_t minVal, maxVal;
  std::memcpy(&minVal, stats.minValue.data(), sizeof(int64_t));
  std::memcpy(&maxVal, stats.maxValue.data(), sizeof(int64_t));
  EXPECT_EQ(minVal, -3);
  EXPECT_EQ(maxVal, 42);
}

TEST(ComputeChunkStatsTest, integerWithNulls) {
  std::vector<int32_t> data = {10, 20};
  auto stats = detail::computeChunkStats(
      data.begin(), data.end(), /*hasNulls=*/true, /*nullCount=*/3);

  EXPECT_TRUE(stats.hasNulls);
  EXPECT_EQ(stats.nullCount, 3);
  ASSERT_EQ(stats.minValue.size(), sizeof(int32_t));
  ASSERT_EQ(stats.maxValue.size(), sizeof(int32_t));

  int32_t minVal, maxVal;
  std::memcpy(&minVal, stats.minValue.data(), sizeof(int32_t));
  std::memcpy(&maxVal, stats.maxValue.data(), sizeof(int32_t));
  EXPECT_EQ(minVal, 10);
  EXPECT_EQ(maxVal, 20);
}

TEST(ComputeChunkStatsTest, floatMinMax) {
  std::vector<double> data = {1.5, -0.5, 100.0};
  auto stats = detail::computeChunkStats(data.begin(), data.end());

  EXPECT_FALSE(stats.hasNulls);
  ASSERT_EQ(stats.minValue.size(), sizeof(double));
  ASSERT_EQ(stats.maxValue.size(), sizeof(double));

  double minVal, maxVal;
  std::memcpy(&minVal, stats.minValue.data(), sizeof(double));
  std::memcpy(&maxVal, stats.maxValue.data(), sizeof(double));
  EXPECT_DOUBLE_EQ(minVal, -0.5);
  EXPECT_DOUBLE_EQ(maxVal, 100.0);
}

TEST(ComputeChunkStatsTest, floatNaNBailout) {
  std::vector<double> data = {
      1.0, std::numeric_limits<double>::quiet_NaN(), 3.0};
  auto stats = detail::computeChunkStats(data.begin(), data.end());

  EXPECT_FALSE(stats.hasNulls);
  EXPECT_TRUE(stats.minValue.empty());
  EXPECT_TRUE(stats.maxValue.empty());
}

TEST(ComputeChunkStatsTest, floatAllNaN) {
  std::vector<float> data = {
      std::numeric_limits<float>::quiet_NaN(),
      std::numeric_limits<float>::quiet_NaN()};
  auto stats = detail::computeChunkStats(data.begin(), data.end());

  EXPECT_TRUE(stats.minValue.empty());
  EXPECT_TRUE(stats.maxValue.empty());
}

TEST(ComputeChunkStatsTest, emptyRange) {
  std::vector<int64_t> data;
  auto stats = detail::computeChunkStats(data.begin(), data.end());

  EXPECT_FALSE(stats.hasNulls);
  EXPECT_TRUE(stats.minValue.empty());
  EXPECT_TRUE(stats.maxValue.empty());
}

TEST(ComputeChunkStatsTest, boolProducesNoMinMax) {
  std::vector<bool> data = {true, false, true};
  auto stats = detail::computeChunkStats(data.begin(), data.end());

  EXPECT_FALSE(stats.hasNulls);
  EXPECT_TRUE(stats.minValue.empty());
  EXPECT_TRUE(stats.maxValue.empty());
}

TEST(ComputeChunkStatsTest, singleElement) {
  std::vector<int64_t> data = {42};
  auto stats = detail::computeChunkStats(data.begin(), data.end());

  ASSERT_EQ(stats.minValue.size(), sizeof(int64_t));
  ASSERT_EQ(stats.maxValue.size(), sizeof(int64_t));

  int64_t minVal, maxVal;
  std::memcpy(&minVal, stats.minValue.data(), sizeof(int64_t));
  std::memcpy(&maxVal, stats.maxValue.data(), sizeof(int64_t));
  EXPECT_EQ(minVal, 42);
  EXPECT_EQ(maxVal, 42);
}

} // namespace facebook::nimble::test
