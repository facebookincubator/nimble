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

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/index/ClusterIndex.h"
#include "dwio/nimble/index/tests/ClusterIndexTestBase.h"
#include "dwio/nimble/index/tests/ClusterIndexTestUtils.h"

#include "dwio/nimble/velox/ChunkedStreamWriter.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/serializers/KeyEncoder.h"

namespace facebook::nimble::index::test {
namespace {

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;

// Creates a range scan LookupRequest with only a lower bound (no upper bound).
// The result endRow will be numRows_ (total file rows).
IndexLookup::LookupRequest makeRangeScanRequest(std::string_view key) {
  velox::serializer::EncodedKeyBounds bounds{
      .lowerKey = std::string(key), .upperKey = std::nullopt};
  return IndexLookup::LookupRequest::rangeScan({bounds});
}

class ClusterIndexTest : public ClusterIndexTestBase {
 protected:
  struct EncodedKeyStream {
    std::string data;
    std::vector<uint32_t> chunkOffsets;
  };

  // Encodes multiple chunks of keys into a single stream.
  EncodedKeyStream encodeKeyStream(
      const std::vector<std::vector<std::string>>& keyChunks) {
    EncodedKeyStream result;
    Buffer buffer{*pool_};

    for (const auto& keys : keyChunks) {
      result.chunkOffsets.push_back(result.data.size());

      std::vector<std::string_view> keyViews;
      keyViews.reserve(keys.size());
      for (const auto& key : keys) {
        keyViews.push_back(key);
      }

      Buffer encodingBuffer{*pool_};
      auto policy =
          std::make_unique<ManualEncodingSelectionPolicy<std::string_view>>(
              std::vector<std::pair<EncodingType, float>>{
                  {EncodingType::Trivial, 1.0}},
              CompressionOptions{},
              std::nullopt);
      auto encoded = EncodingFactory::encode<std::string_view>(
          std::move(policy), keyViews, encodingBuffer);

      ChunkedStreamWriter writer{buffer};
      auto segments = writer.encode(encoded);
      for (const auto& segment : segments) {
        result.data.append(segment.data(), segment.size());
      }
    }
    return result;
  }

  // Creates a LoadDataFn from key stream data.
  LoadDataFn createLoadDataFn(const std::string& streamData) {
    auto data = std::make_shared<std::string>(streamData);
    return [data](const velox::common::Region& region)
               -> std::unique_ptr<SeekableInputStream> {
      return std::make_unique<SeekableArrayInputStream>(
          reinterpret_cast<const unsigned char*>(data->data() + region.offset),
          region.length > 0 ? region.length : data->size() - region.offset);
    };
  }

  // Creates a Stripe with only the keyStream specified.
  Stripe createStripeWithKeys(const KeyStream& keyStream) {
    Stream defaultStream{
        .numChunks = keyStream.stream.numChunks,
        .chunkRows = keyStream.stream.chunkRows,
        .chunkOffsets = std::vector<uint32_t>(keyStream.stream.numChunks, 0)};
    return Stripe{.streams = {defaultStream}, .keyStream = keyStream};
  }
};

TEST_F(ClusterIndexTest, basic) {
  std::vector<std::string> indexColumns = {"col1", "col2", "col3"};
  std::string minKey = "aaa";
  std::vector<Stripe> stripes = {
      {.streams = {{.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 0,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}},
            .chunkKeys = {"bbb"}}},
      {.streams = {{.numChunks = 1, .chunkRows = {200}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 100,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {200}, .chunkOffsets = {0}},
            .chunkKeys = {"ccc"}}},
      {.streams = {{.numChunks = 1, .chunkRows = {150}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 200,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {150}, .chunkOffsets = {0}},
            .chunkKeys = {"ddd"}}},
      {.streams = {{.numChunks = 1, .chunkRows = {120}, .chunkOffsets = {0}}},
       .keyStream = {
           .streamOffset = 300,
           .streamSize = 100,
           .stream = {.numChunks = 1, .chunkRows = {120}, .chunkOffsets = {0}},
           .chunkKeys = {"eee"}}}};
  std::vector<int> stripeGroups = {2, 1, 1};

  auto indexBuffers =
      createTestClusterIndex(indexColumns, minKey, stripes, stripeGroups);
  auto clusterIndex = createClusterIndex(indexBuffers);

  EXPECT_EQ(clusterIndex->indexType(), IndexType::Cluster);
  EXPECT_EQ(clusterIndex->layout().numPartitions, 3);

  const auto& cols = clusterIndex->indexColumns();
  EXPECT_EQ(cols.size(), 3);
  EXPECT_EQ(cols[0], "col1");
  EXPECT_EQ(cols[1], "col2");
  EXPECT_EQ(cols[2], "col3");

  ClusterIndexTestHelper helper(clusterIndex.get());
  auto metadata0 = helper.partitionSection(0);
  EXPECT_EQ(metadata0.offset(), 0);
  EXPECT_GT(metadata0.size(), 0);
  EXPECT_EQ(metadata0.compressionType(), CompressionType::Uncompressed);

  auto metadata1 = helper.partitionSection(1);
  EXPECT_EQ(metadata1.offset(), metadata0.size());
  EXPECT_GT(metadata1.size(), 0);
  EXPECT_EQ(metadata1.compressionType(), CompressionType::Uncompressed);

  auto metadata2 = helper.partitionSection(2);
  EXPECT_EQ(metadata2.offset(), metadata0.size() + metadata1.size());
  EXPECT_GT(metadata2.size(), 0);
  EXPECT_EQ(metadata2.compressionType(), CompressionType::Uncompressed);
  EXPECT_EQ(
      metadata2.offset() + metadata2.size(),
      indexBuffers.indexPartitions.size());
}

TEST_F(ClusterIndexTest, lookup) {
  std::vector<std::string> indexColumns = {"key_col"};
  std::string minKey = "aaa";

  // Encode key stream data for each stripe's keys.
  // Stripe 0: 1 chunk with 1 key "ccc" (1 row)
  // Stripe 1: 1 chunk with 1 key "fff" (1 row)
  // Stripe 2: 1 chunk with 1 key "iii" (1 row)
  auto encoded0 = encodeKeyStream({{"ccc"}});
  auto encoded1 = encodeKeyStream({{"fff"}});
  auto encoded2 = encodeKeyStream({{"iii"}});
  std::string combinedStream = encoded0.data + encoded1.data + encoded2.data;

  const auto size0 = static_cast<uint32_t>(encoded0.data.size());
  const auto size1 = static_cast<uint32_t>(encoded1.data.size());
  const auto size2 = static_cast<uint32_t>(encoded2.data.size());

  std::vector<Stripe> stripes = {
      createStripeWithKeys(
          {.streamOffset = 0,
           .streamSize = size0,
           .stream = {.numChunks = 1, .chunkRows = {1}, .chunkOffsets = {0}},
           .chunkKeys = {"ccc"}}),
      createStripeWithKeys(
          {.streamOffset = size0,
           .streamSize = size1,
           .stream = {.numChunks = 1, .chunkRows = {1}, .chunkOffsets = {0}},
           .chunkKeys = {"fff"}}),
      createStripeWithKeys(
          {.streamOffset = size0 + size1,
           .streamSize = size2,
           .stream = {.numChunks = 1, .chunkRows = {1}, .chunkOffsets = {0}},
           .chunkKeys = {"iii"}})};
  // 2 partitions: partition 0 has stripes 0+1 (2 rows), partition 1 has
  // stripe 2 (1 row). Total 3 rows.
  std::vector<int> stripeGroups = {2, 1};

  auto indexBuffers =
      createTestClusterIndex(indexColumns, minKey, stripes, stripeGroups);
  auto clusterIndex = createClusterIndex(
      indexBuffers, createLoadDataFn(combinedStream), pool_.get());

  // Partition 0: rows 0-1 (keys "ccc", "fff")
  // Partition 1: rows 2 (key "iii")
  // Total: 3 rows.

  // Key before minKey returns full file range.
  {
    auto result = clusterIndex->lookup(makeRangeScanRequest("000"));
    ASSERT_EQ(result.size(), 1);
    auto ranges = result[0];
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].startRow, 0);
    EXPECT_EQ(ranges[0].endRow, 3);
  }

  // Key in partition 0, chunk 0 → row-level start.
  {
    auto result = clusterIndex->lookup(makeRangeScanRequest("bbb"));
    ASSERT_EQ(result.size(), 1);
    auto ranges = result[0];
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].startRow, 0);
    EXPECT_EQ(ranges[0].endRow, 3);
  }

  // Key "ddd" is in partition 0, chunk 1 → row 1.
  {
    auto result = clusterIndex->lookup(makeRangeScanRequest("ddd"));
    ASSERT_EQ(result.size(), 1);
    auto ranges = result[0];
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].startRow, 1);
    EXPECT_EQ(ranges[0].endRow, 3);
  }

  // Key "ggg" is in partition 1 → row 2.
  {
    auto result = clusterIndex->lookup(makeRangeScanRequest("ggg"));
    ASSERT_EQ(result.size(), 1);
    auto ranges = result[0];
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].startRow, 2);
    EXPECT_EQ(ranges[0].endRow, 3);
  }

  // Key after last stripe max key returns empty.
  {
    auto result = clusterIndex->lookup(makeRangeScanRequest("zzz"));
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].size(), 0);
  }
}

TEST_F(ClusterIndexTest, lookupAllStripes) {
  std::vector<std::string> indexColumns = {"key_col"};
  std::string minKey = "aaa";

  // Each stripe has 1 chunk with 1 key.
  // Stripe 0: key "ccc", Stripe 1: key "fff", Stripe 2: key "iii".
  auto encoded0 = encodeKeyStream({{"ccc"}});
  auto encoded1 = encodeKeyStream({{"fff"}});
  auto encoded2 = encodeKeyStream({{"iii"}});
  std::string combinedStream = encoded0.data + encoded1.data + encoded2.data;

  const auto size0 = static_cast<uint32_t>(encoded0.data.size());
  const auto size1 = static_cast<uint32_t>(encoded1.data.size());
  const auto size2 = static_cast<uint32_t>(encoded2.data.size());

  std::vector<Stripe> stripes = {
      createStripeWithKeys(
          {.streamOffset = 0,
           .streamSize = size0,
           .stream = {.numChunks = 1, .chunkRows = {1}, .chunkOffsets = {0}},
           .chunkKeys = {"ccc"}}),
      createStripeWithKeys(
          {.streamOffset = size0,
           .streamSize = size1,
           .stream = {.numChunks = 1, .chunkRows = {1}, .chunkOffsets = {0}},
           .chunkKeys = {"fff"}}),
      createStripeWithKeys(
          {.streamOffset = size0 + size1,
           .streamSize = size2,
           .stream = {.numChunks = 1, .chunkRows = {1}, .chunkOffsets = {0}},
           .chunkKeys = {"iii"}})};
  // 2 partitions: partition 0 = stripes 0+1 (2 rows), partition 1 = stripe 2
  // (1 row). Total 3 rows.
  std::vector<int> stripeGroups = {2, 1};

  auto indexBuffers =
      createTestClusterIndex(indexColumns, minKey, stripes, stripeGroups);
  auto clusterIndex = createClusterIndex(
      indexBuffers, createLoadDataFn(combinedStream), pool_.get());

  // Partition 0: rows 0-1 (keys "ccc", "fff")
  // Partition 1: row 2 (key "iii")
  struct TestCase {
    std::string lookupKey;
    std::optional<RowRange> expectedRange;

    std::string debugString() const {
      if (expectedRange.has_value()) {
        return fmt::format(
            "key '{}', expected [{}, {})",
            lookupKey,
            expectedRange->startRow,
            expectedRange->endRow);
      }
      return fmt::format("key '{}', expected empty", lookupKey);
    }
  };

  // makeRangeScanRequest sets only lowerKey, so endRow = numRows_ = 3.
  std::vector<TestCase> testCases = {
      // Keys before minKey return full file range.
      {"000", RowRange(0, 3)},
      {"aa", RowRange(0, 3)},
      // Keys in partition 0, chunk 0 (lastKey "ccc") → row 0.
      {"aaa", RowRange(0, 3)},
      {"bbb", RowRange(0, 3)},
      {"ccc", RowRange(0, 3)},
      // Keys in partition 0, chunk 1 (lastKey "fff") → row 1.
      {"ddd", RowRange(1, 3)},
      {"eee", RowRange(1, 3)},
      {"fff", RowRange(1, 3)},
      // Keys in partition 1, chunk 0 (lastKey "iii") → row 2.
      {"ggg", RowRange(2, 3)},
      {"hhh", RowRange(2, 3)},
      {"iii", RowRange(2, 3)},
      // Keys after last stripe max key.
      {"jjj", std::nullopt},
      {"zzz", std::nullopt}};

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    auto result =
        clusterIndex->lookup(makeRangeScanRequest(testCase.lookupKey));
    ASSERT_EQ(result.size(), 1);
    auto ranges = result[0];
    if (testCase.expectedRange.has_value()) {
      ASSERT_EQ(ranges.size(), 1);
      EXPECT_EQ(ranges[0].startRow, testCase.expectedRange->startRow);
      EXPECT_EQ(ranges[0].endRow, testCase.expectedRange->endRow);
    } else {
      EXPECT_EQ(ranges.size(), 0);
    }
  }
}

TEST_F(ClusterIndexTest, lookupWithSameKeys) {
  std::vector<std::string> indexColumns = {"key_col"};
  std::string minKey = "aaa";

  // All stripes have the same key "aaa". Each stripe has 1 chunk with 1 key.
  auto encoded0 = encodeKeyStream({{"aaa"}});
  auto encoded1 = encodeKeyStream({{"aaa"}});
  auto encoded2 = encodeKeyStream({{"aaa"}});
  std::string combinedStream = encoded0.data + encoded1.data + encoded2.data;

  const auto size0 = static_cast<uint32_t>(encoded0.data.size());
  const auto size1 = static_cast<uint32_t>(encoded1.data.size());
  const auto size2 = static_cast<uint32_t>(encoded2.data.size());

  std::vector<Stripe> stripes = {
      createStripeWithKeys(
          {.streamOffset = 0,
           .streamSize = size0,
           .stream = {.numChunks = 1, .chunkRows = {1}, .chunkOffsets = {0}},
           .chunkKeys = {"aaa"}}),
      createStripeWithKeys(
          {.streamOffset = size0,
           .streamSize = size1,
           .stream = {.numChunks = 1, .chunkRows = {1}, .chunkOffsets = {0}},
           .chunkKeys = {"aaa"}}),
      createStripeWithKeys(
          {.streamOffset = size0 + size1,
           .streamSize = size2,
           .stream = {.numChunks = 1, .chunkRows = {1}, .chunkOffsets = {0}},
           .chunkKeys = {"aaa"}})};
  // 2 partitions: partition 0 = stripes 0+1 (2 rows), partition 1 = stripe 2
  // (1 row). Total 3 rows.
  std::vector<int> stripeGroups = {2, 1};

  auto indexBuffers =
      createTestClusterIndex(indexColumns, minKey, stripes, stripeGroups);
  auto clusterIndex = createClusterIndex(
      indexBuffers, createLoadDataFn(combinedStream), pool_.get());

  // Partition 0: rows 0-1 (both "aaa"), Partition 1: row 2 ("aaa").
  struct TestCase {
    std::string lookupKey;
    std::optional<RowRange> expectedRange;

    std::string debugString() const {
      if (expectedRange.has_value()) {
        return fmt::format(
            "key '{}', expected [{}, {})",
            lookupKey,
            expectedRange->startRow,
            expectedRange->endRow);
      }
      return fmt::format("key '{}', expected empty", lookupKey);
    }
  };

  // makeRangeScanRequest sets only lowerKey, so endRow = numRows_ = 3.
  std::vector<TestCase> testCases = {
      // Keys before minKey return full file range.
      {"000", RowRange(0, 3)},
      {"aa", RowRange(0, 3)},
      // Key at minKey (same as all stripe keys) → first partition row 0.
      {"aaa", RowRange(0, 3)},
      // Keys after the common key.
      {"aab", std::nullopt},
      {"bbb", std::nullopt},
      {"zzz", std::nullopt}};

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    auto result =
        clusterIndex->lookup(makeRangeScanRequest(testCase.lookupKey));
    ASSERT_EQ(result.size(), 1);
    auto ranges = result[0];
    if (testCase.expectedRange.has_value()) {
      ASSERT_EQ(ranges.size(), 1);
      EXPECT_EQ(ranges[0].startRow, testCase.expectedRange->startRow);
      EXPECT_EQ(ranges[0].endRow, testCase.expectedRange->endRow);
    } else {
      EXPECT_EQ(ranges.size(), 0);
    }
  }
}
TEST_F(ClusterIndexTest, lookupWithKeyStream) {
  // Test row-level precision when key stream is available.
  // Single partition with multiple chunks of sorted keys.
  std::vector<std::vector<std::string>> keyChunks = {
      {"key_a", "key_b", "key_c"},
      {"key_d", "key_e", "key_f"},
      {"key_g", "key_h", "key_i"},
  };
  auto encodedStream = encodeKeyStream(keyChunks);

  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "key_0";
  std::vector<Stripe> stripes = {createStripeWithKeys(
      {.streamOffset = 0,
       .streamSize = static_cast<uint32_t>(encodedStream.data.size()),
       .stream =
           {.numChunks = 3,
            .chunkRows = {3, 3, 3},
            .chunkOffsets = encodedStream.chunkOffsets},
       .chunkKeys = {"key_c", "key_f", "key_i"}})};
  std::vector<int> stripeGroups = {1};

  auto indexBuffers =
      createTestClusterIndex(indexColumns, minKey, stripes, stripeGroups);
  auto clusterIndex = createClusterIndex(
      indexBuffers, createLoadDataFn(encodedStream.data), pool_.get());

  struct TestCase {
    std::string lookupKey;
    std::optional<RowRange> expectedRange;

    std::string debugString() const {
      if (expectedRange.has_value()) {
        return fmt::format(
            "key '{}', expected [{}, {})",
            lookupKey,
            expectedRange->startRow,
            expectedRange->endRow);
      }
      return fmt::format("key '{}', expected empty", lookupKey);
    }
  };

  // With key stream, lookup resolves to exact row positions.
  // 9 rows total: key_a(0), key_b(1), key_c(2), key_d(3), key_e(4),
  //               key_f(5), key_g(6), key_h(7), key_i(8)
  std::vector<TestCase> testCases = {
      // Exact key matches.
      {"key_a", RowRange(0, 9)},
      {"key_d", RowRange(3, 9)},
      {"key_i", RowRange(8, 9)},
      // Key between existing keys — resolves to next key's row.
      {"key_ab", RowRange(1, 9)},
      {"key_de", RowRange(4, 9)},
      // Key before all data.
      {"key_0", RowRange(0, 9)},
      // Key after all data.
      {"key_z", std::nullopt},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    auto result =
        clusterIndex->lookup(makeRangeScanRequest(testCase.lookupKey));
    ASSERT_EQ(result.size(), 1);
    auto ranges = result[0];
    if (testCase.expectedRange.has_value()) {
      ASSERT_EQ(ranges.size(), 1);
      EXPECT_EQ(ranges[0].startRow, testCase.expectedRange->startRow);
      EXPECT_EQ(ranges[0].endRow, testCase.expectedRange->endRow);
    } else {
      EXPECT_EQ(ranges.size(), 0);
    }
  }
}

TEST_F(ClusterIndexTest, lookupWithKeyStreamMultiplePartitions) {
  // Test row-level precision across multiple partitions.
  std::vector<std::vector<std::string>> partition0Keys = {
      {"key_a", "key_b", "key_c"}};
  std::vector<std::vector<std::string>> partition1Keys = {
      {"key_d", "key_e", "key_f"}};

  auto encoded0 = encodeKeyStream(partition0Keys);
  auto encoded1 = encodeKeyStream(partition1Keys);
  std::string combinedStream = encoded0.data + encoded1.data;

  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "key_0";
  std::vector<Stripe> stripes = {
      createStripeWithKeys(
          {.streamOffset = 0,
           .streamSize = static_cast<uint32_t>(encoded0.data.size()),
           .stream = {.numChunks = 1, .chunkRows = {3}, .chunkOffsets = {0}},
           .chunkKeys = {"key_c"}}),
      createStripeWithKeys(
          {.streamOffset = static_cast<uint32_t>(encoded0.data.size()),
           .streamSize = static_cast<uint32_t>(encoded1.data.size()),
           .stream = {.numChunks = 1, .chunkRows = {3}, .chunkOffsets = {0}},
           .chunkKeys = {"key_f"}})};
  std::vector<int> stripeGroups = {1, 1};

  auto indexBuffers =
      createTestClusterIndex(indexColumns, minKey, stripes, stripeGroups);
  auto clusterIndex = createClusterIndex(
      indexBuffers, createLoadDataFn(combinedStream), pool_.get());

  struct TestCase {
    std::string lookupKey;
    std::optional<RowRange> expectedRange;

    std::string debugString() const {
      if (expectedRange.has_value()) {
        return fmt::format(
            "key '{}', expected [{}, {})",
            lookupKey,
            expectedRange->startRow,
            expectedRange->endRow);
      }
      return fmt::format("key '{}', expected empty", lookupKey);
    }
  };

  // Partition 0: rows 0-2 (key_a, key_b, key_c)
  // Partition 1: rows 3-5 (key_d, key_e, key_f)
  // makeRangeScanRequest sets only lowerKey, so endRow = numRows_ = 6.
  std::vector<TestCase> testCases = {
      {"key_a", RowRange(0, 6)},
      {"key_b", RowRange(1, 6)},
      {"key_c", RowRange(2, 6)},
      {"key_d", RowRange(3, 6)},
      {"key_e", RowRange(4, 6)},
      {"key_f", RowRange(5, 6)},
      {"key_0", RowRange(0, 6)},
      {"key_z", std::nullopt},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    auto result =
        clusterIndex->lookup(makeRangeScanRequest(testCase.lookupKey));
    ASSERT_EQ(result.size(), 1);
    auto ranges = result[0];
    if (testCase.expectedRange.has_value()) {
      ASSERT_EQ(ranges.size(), 1);
      EXPECT_EQ(ranges[0].startRow, testCase.expectedRange->startRow);
      EXPECT_EQ(ranges[0].endRow, testCase.expectedRange->endRow);
    } else {
      EXPECT_EQ(ranges.size(), 0);
    }
  }
}

TEST_F(ClusterIndexTest, chunkLocation) {
  ChunkLocation loc1{100, 50, 200};
  EXPECT_EQ(loc1.chunkOffset, 100);
  EXPECT_EQ(loc1.chunkSize, 50);
  EXPECT_EQ(loc1.rowOffset, 200);

  ChunkLocation loc2{0, 0, 0};
  EXPECT_EQ(loc2.chunkOffset, 0);
  EXPECT_EQ(loc2.chunkSize, 0);
  EXPECT_EQ(loc2.rowOffset, 0);
}

TEST_F(ClusterIndexTest, keyStreamRegion) {
  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "aaa";
  std::vector<Stripe> stripes = {
      {.streams = {{.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 0,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}},
            .chunkKeys = {"bbb"}}},
      {.streams = {{.numChunks = 1, .chunkRows = {200}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 100,
            .streamSize = 150,
            .stream = {.numChunks = 1, .chunkRows = {200}, .chunkOffsets = {0}},
            .chunkKeys = {"ccc"}}},
      {.streams = {{.numChunks = 1, .chunkRows = {150}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 250,
            .streamSize = 200,
            .stream = {.numChunks = 1, .chunkRows = {150}, .chunkOffsets = {0}},
            .chunkKeys = {"ddd"}}},
      {.streams = {{.numChunks = 1, .chunkRows = {120}, .chunkOffsets = {0}}},
       .keyStream = {
           .streamOffset = 450,
           .streamSize = 50,
           .stream = {.numChunks = 1, .chunkRows = {120}, .chunkOffsets = {0}},
           .chunkKeys = {"eee"}}}};
  std::vector<int> stripeGroups = {2, 2};

  auto indexBuffers =
      createTestClusterIndex(indexColumns, minKey, stripes, stripeGroups);
  auto clusterIndex = createClusterIndex(indexBuffers);
  ASSERT_NE(clusterIndex, nullptr);

  struct TestCase {
    uint32_t partitionId;
    uint32_t expectedOffset;
    uint32_t expectedLength;

    std::string debugString() const {
      return fmt::format(
          "partitionId {}, expected [{}, {})",
          partitionId,
          expectedOffset,
          expectedOffset + expectedLength);
    }
  };

  std::vector<TestCase> testCases = {
      // Partition 0: stripes 0, 1. keyStreamOffset = 0, keyStreamSize = 250.
      {0, 0, 250},
      // Partition 1: stripes 2, 3. keyStreamOffset = 250, keyStreamSize = 250.
      {1, 250, 250},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    ClusterIndexTestHelper helper(clusterIndex.get());
    auto region = helper.keyStreamRegion(testCase.partitionId);
    EXPECT_EQ(region.offset, testCase.expectedOffset);
    EXPECT_EQ(region.length, testCase.expectedLength);
  }
}

TEST_F(ClusterIndexTest, lookupChunk) {
  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "aaa";
  // Two stripes in one partition:
  // Stripe 0: 3 chunks, keys "ccc"/"fff"/"iii"
  //   chunkRows = {100, 100, 100}, offsets = {0, 50, 120}
  //   streamOffset = 0, streamSize = 200
  // Stripe 1: 2 chunks, keys "mmm"/"ppp"
  //   chunkRows = {150, 150}, offsets = {0, 80}
  //   streamOffset = 200, streamSize = 150
  //
  // The test base computes FlatBuffer chunkOffsets as:
  //   streamOffset + chunkOffset[i]
  // So the partition-level offsets are:
  //   Stripe 0: 0, 50, 120
  //   Stripe 1: 200, 280
  // These must be monotonically increasing.
  // Total key_stream_size = 200 + 150 = 350.
  std::vector<Stripe> stripes = {
      {.streams = {{.numChunks = 1, .chunkRows = {300}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 0,
            .streamSize = 200,
            .stream =
                {.numChunks = 3,
                 .chunkRows = {100, 100, 100},
                 .chunkOffsets = {0, 50, 120}},
            .chunkKeys = {"ccc", "fff", "iii"}}},
      {.streams = {{.numChunks = 1, .chunkRows = {300}, .chunkOffsets = {0}}},
       .keyStream = {
           .streamOffset = 200,
           .streamSize = 150,
           .stream =
               {.numChunks = 2,
                .chunkRows = {150, 150},
                .chunkOffsets = {0, 80}},
           .chunkKeys = {"mmm", "ppp"}}}};
  std::vector<int> stripeGroups = {2};

  auto indexBuffers =
      createTestClusterIndex(indexColumns, minKey, stripes, stripeGroups);
  auto clusterIndex = createClusterIndex(indexBuffers);
  ASSERT_NE(clusterIndex, nullptr);
  ClusterIndexTestHelper helper(clusterIndex.get());

  // lookupChunk() searches all chunks in the partition and returns
  // partition-wide chunk locations.
  //
  // Chunk 0: key="ccc", chunkOffset=0, chunkSize=50, rowOffset=0
  // Chunk 1: key="fff", chunkOffset=50, chunkSize=70, rowOffset=100
  // Chunk 2: key="iii", chunkOffset=120, chunkSize=80, rowOffset=200
  // Chunk 3: key="mmm", chunkOffset=200, chunkSize=80, rowOffset=300
  // Chunk 4: key="ppp", chunkOffset=280, chunkSize=70, rowOffset=450
  //
  // Total key stream size = 350 bytes.

  struct TestCase {
    std::string encodedKey;
    uint32_t expectedChunkOffset;
    uint32_t expectedChunkSize;
    uint32_t expectedRowOffset;

    std::string debugString() const {
      return fmt::format("encodedKey '{}'", encodedKey);
    }
  };

  std::vector<TestCase> testCases = {
      // Chunk 0: offset=0, size=50, rowOffset=0
      {"aa", 0, 50, 0},
      {"aaa", 0, 50, 0},
      {"bbb", 0, 50, 0},
      {"ccc", 0, 50, 0},
      // Chunk 1: offset=50, size=70, rowOffset=100
      {"ddd", 50, 70, 100},
      {"eee", 50, 70, 100},
      {"fff", 50, 70, 100},
      // Chunk 2: offset=120, size=80, rowOffset=200
      {"ggg", 120, 80, 200},
      {"hhh", 120, 80, 200},
      {"iii", 120, 80, 200},
      // Chunk 3: offset=200, size=80, rowOffset=300
      {"jjj", 200, 80, 300},
      {"lll", 200, 80, 300},
      {"mmm", 200, 80, 300},
      // Chunk 4: offset=280, size=70, rowOffset=450
      {"nnn", 280, 70, 450},
      {"ooo", 280, 70, 450},
      {"ppp", 280, 70, 450},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    auto result = helper.lookupChunk(0, testCase.encodedKey);
    EXPECT_EQ(result.chunkOffset, testCase.expectedChunkOffset);
    EXPECT_EQ(result.chunkSize, testCase.expectedChunkSize);
    EXPECT_EQ(result.rowOffset, testCase.expectedRowOffset);
  }

  // Keys beyond all chunks throw.
  NIMBLE_ASSERT_THROW(helper.lookupChunk(0, "qqq"), "");
  NIMBLE_ASSERT_THROW(helper.lookupChunk(0, "zzz"), "");
}

// Parameterized test verifying lookup correctness across different chunk
// granularities. The same 9 keys (key_a..key_i) are split into chunks of
// different sizes, and all lookups should return the same row positions.
class ClusterIndexLookupTest : public ClusterIndexTest,
                               public ::testing::WithParamInterface<uint32_t> {
 protected:
  void SetUp() override {
    const uint32_t rowsPerChunk = GetParam();
    const std::vector<std::string> allKeys = {
        "key_a",
        "key_b",
        "key_c",
        "key_d",
        "key_e",
        "key_f",
        "key_g",
        "key_h",
        "key_i"};
    const uint32_t numKeys = allKeys.size();

    std::vector<std::vector<std::string>> keyChunks;
    for (uint32_t offset = 0; offset < numKeys; offset += rowsPerChunk) {
      const auto end = std::min(offset + rowsPerChunk, numKeys);
      keyChunks.emplace_back(allKeys.begin() + offset, allKeys.begin() + end);
    }

    encodedStream_ = encodeKeyStream(keyChunks);

    std::vector<int32_t> chunkRows;
    std::vector<std::string> chunkKeys;
    for (const auto& chunk : keyChunks) {
      chunkRows.push_back(chunk.size());
      chunkKeys.push_back(chunk.back());
    }

    std::vector<std::string> indexColumns = {"col1"};
    std::string minKey = "key_0";
    std::vector<Stripe> stripes = {createStripeWithKeys(
        {.streamOffset = 0,
         .streamSize = static_cast<uint32_t>(encodedStream_.data.size()),
         .stream =
             {.numChunks = static_cast<uint32_t>(keyChunks.size()),
              .chunkRows = chunkRows,
              .chunkOffsets = encodedStream_.chunkOffsets},
         .chunkKeys = chunkKeys})};
    std::vector<int> stripeGroups = {1};

    auto indexBuffers =
        createTestClusterIndex(indexColumns, minKey, stripes, stripeGroups);
    clusterIndex_ = createClusterIndex(
        indexBuffers, createLoadDataFn(encodedStream_.data), pool_.get());
  }

  EncodedKeyStream encodedStream_;
  std::unique_ptr<ClusterIndex> clusterIndex_;
};

TEST_P(ClusterIndexLookupTest, rangeScanEmptyRange) {
  struct TestCase {
    std::string lower;
    std::string upper;
    std::string debugString() const {
      return fmt::format("lower='{}', upper='{}'", lower, upper);
    }
  };

  std::vector<TestCase> testCases = {
      // upper == lower → empty (exclusive upper means [x, x) = empty).
      {"key_d", "key_d"},
      // upper < lower → empty.
      {"key_f", "key_a"},
      // Both beyond all keys.
      {"key_z", "key_a"},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    velox::serializer::EncodedKeyBounds bounds{
        .lowerKey = testCase.lower, .upperKey = testCase.upper};
    auto result =
        clusterIndex_->lookup(IndexLookup::LookupRequest::rangeScan({bounds}));
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].size(), 0);
  }
}

TEST_P(ClusterIndexLookupTest, rangeScanValidRange) {
  // 9 rows: key_a(0)..key_i(8). Upper bound is exclusive.
  struct TestCase {
    std::string lower;
    std::string upper;
    RowRange expected;
    std::string debugString() const {
      return fmt::format(
          "lower='{}', upper='{}', expected=[{}, {})",
          lower,
          upper,
          expected.startRow,
          expected.endRow);
    }
  };

  std::vector<TestCase> testCases = {
      // Full range.
      {"key_0", "key_z", RowRange(0, 9)},
      // Subset range.
      {"key_d", "key_g", RowRange(3, 6)},
      // Single row range: [key_a, key_b) → row 0.
      {"key_a", "key_b", RowRange(0, 1)},
      // Last row: [key_i, key_z) → row 8.
      {"key_i", "key_z", RowRange(8, 9)},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    velox::serializer::EncodedKeyBounds bounds{
        .lowerKey = testCase.lower, .upperKey = testCase.upper};
    auto result =
        clusterIndex_->lookup(IndexLookup::LookupRequest::rangeScan({bounds}));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result[0].size(), 1);
    EXPECT_EQ(result[0][0].startRow, testCase.expected.startRow);
    EXPECT_EQ(result[0][0].endRow, testCase.expected.endRow);
  }
}

TEST_P(ClusterIndexLookupTest, pointLookupExactMatch) {
  // 9 rows: key_a(0)..key_i(8).
  // Point lookup returns [row, row+1) for exact matches.
  struct TestCase {
    std::string key;
    RowRange expected;
    std::string debugString() const {
      return fmt::format(
          "key='{}', expected=[{}, {})",
          key,
          expected.startRow,
          expected.endRow);
    }
  };

  std::vector<TestCase> testCases = {
      {"key_a", RowRange(0, 1)},
      {"key_b", RowRange(1, 2)},
      {"key_c", RowRange(2, 3)},
      {"key_d", RowRange(3, 4)},
      {"key_e", RowRange(4, 5)},
      {"key_f", RowRange(5, 6)},
      {"key_g", RowRange(6, 7)},
      {"key_h", RowRange(7, 8)},
      {"key_i", RowRange(8, 9)},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    auto result = clusterIndex_->lookup(
        IndexLookup::LookupRequest::pointLookup({testCase.key}));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result[0].size(), 1);
    EXPECT_EQ(result[0][0].startRow, testCase.expected.startRow);
    EXPECT_EQ(result[0][0].endRow, testCase.expected.endRow);
  }
}

TEST_P(ClusterIndexLookupTest, pointLookupNonExistent) {
  struct TestCase {
    std::string key;
    bool expectEmpty;
    std::string debugString() const {
      return fmt::format("key='{}', expectEmpty={}", key, expectEmpty);
    }
  };

  std::vector<TestCase> testCases = {
      {"key_z", true},
      {"key_0", true},
      {"key_ab", true},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    auto result = clusterIndex_->lookup(
        IndexLookup::LookupRequest::pointLookup({testCase.key}));
    ASSERT_EQ(result.size(), 1);
    if (testCase.expectEmpty) {
      EXPECT_EQ(result[0].size(), 0);
    } else {
      EXPECT_GE(result[0].size(), 1);
    }
  }
}

TEST_P(ClusterIndexLookupTest, pointLookupBatch) {
  auto result = clusterIndex_->lookup(
      IndexLookup::LookupRequest::pointLookup({"key_a", "key_e", "key_i"}));
  ASSERT_EQ(result.size(), 3);

  ASSERT_EQ(result[0].size(), 1);
  EXPECT_EQ(result[0][0].startRow, 0);
  EXPECT_EQ(result[0][0].endRow, 1);

  ASSERT_EQ(result[1].size(), 1);
  EXPECT_EQ(result[1][0].startRow, 4);
  EXPECT_EQ(result[1][0].endRow, 5);

  ASSERT_EQ(result[2].size(), 1);
  EXPECT_EQ(result[2][0].startRow, 8);
  EXPECT_EQ(result[2][0].endRow, 9);
}

TEST_P(ClusterIndexLookupTest, pointLookupBatchMixed) {
  auto result = clusterIndex_->lookup(
      IndexLookup::LookupRequest::pointLookup({"key_a", "key_z", "key_e"}));
  ASSERT_EQ(result.size(), 3);

  ASSERT_EQ(result[0].size(), 1);
  EXPECT_EQ(result[0][0].startRow, 0);
  EXPECT_EQ(result[0][0].endRow, 1);

  EXPECT_EQ(result[1].size(), 0);

  ASSERT_EQ(result[2].size(), 1);
  EXPECT_EQ(result[2][0].startRow, 4);
  EXPECT_EQ(result[2][0].endRow, 5);
}

INSTANTIATE_TEST_SUITE_P(
    ChunkGranularity,
    ClusterIndexLookupTest,
    ::testing::Values(1, 3, 9),
    [](const ::testing::TestParamInfo<uint32_t>& info) {
      return fmt::format("rowsPerChunk_{}", info.param);
    });

} // namespace
} // namespace facebook::nimble::index::test
