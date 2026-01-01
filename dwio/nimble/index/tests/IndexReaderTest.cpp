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
#include "dwio/nimble/common/Exceptions.h"

#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/index/IndexReader.h"
#include "dwio/nimble/index/tests/TabletIndexTestBase.h"
#include "dwio/nimble/velox/ChunkedStreamWriter.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::nimble::index::test {

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;

class IndexReaderTest : public TabletIndexTestBase {
 protected:
  // Encodes a vector of string keys into a chunked stream format.
  // Returns the encoded stream data and the chunk offsets for each key group.
  struct EncodedKeyStream {
    std::string data;
    std::vector<uint32_t> chunkOffsets;
  };

  // Encodes multiple chunks of keys into a single stream.
  // Each chunk is a vector of keys that will be encoded together.
  EncodedKeyStream encodeKeyStream(
      const std::vector<std::vector<std::string>>& keyChunks) {
    EncodedKeyStream result;
    Buffer buffer{*pool_};

    for (const auto& keys : keyChunks) {
      result.chunkOffsets.push_back(result.data.size());

      // Convert keys to string_view for encoding
      std::vector<std::string_view> keyViews;
      keyViews.reserve(keys.size());
      for (const auto& key : keys) {
        keyViews.push_back(key);
      }

      // Encode keys using TrivialEncoding
      Buffer encodingBuffer{*pool_};
      auto policy =
          std::make_unique<ManualEncodingSelectionPolicy<std::string_view>>(
              std::vector<std::pair<EncodingType, float>>{
                  {EncodingType::Trivial, 1.0}},
              CompressionOptions{},
              std::nullopt);
      auto encoded = EncodingFactory::encode<std::string_view>(
          std::move(policy), keyViews, encodingBuffer);

      // Write chunk using ChunkedStreamWriter
      ChunkedStreamWriter writer{buffer};
      auto segments = writer.encode(encoded);
      for (const auto& segment : segments) {
        result.data.append(segment.data(), segment.size());
      }
    }
    return result;
  }

  // Creates a SeekableInputStream from string data.
  std::unique_ptr<SeekableInputStream> createInputStream(
      const std::string& data) {
    return std::make_unique<SeekableArrayInputStream>(
        reinterpret_cast<const unsigned char*>(data.data()), data.size());
  }

  // Creates a Stripe with only the keyStream specified.
  // Uses default stream specs since they are not needed for key reader tests.
  Stripe createStripe(const KeyStream& keyStream) {
    // Create a default stream that matches the keyStream's chunk structure.
    Stream defaultStream{
        .numChunks = keyStream.stream.numChunks,
        .chunkRows = keyStream.stream.chunkRows,
        .chunkOffsets = std::vector<uint32_t>(keyStream.stream.numChunks, 0)};
    return Stripe{.streams = {defaultStream}, .keyStream = keyStream};
  }
};

TEST_F(IndexReaderTest, singleChunkSingleKey) {
  // Create a single chunk with one key
  auto encodedStream = encodeKeyStream({{"key_a"}});

  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "key_0";
  std::vector<Stripe> stripes = {createStripe(
      {.streamOffset = 0,
       .streamSize = static_cast<uint32_t>(encodedStream.data.size()),
       .stream = {.numChunks = 1, .chunkRows = {1}, .chunkOffsets = {0}},
       .chunkKeys = {"key_a"}})};
  std::vector<int> stripeGroups = {1};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto stripeIndexGroup =
      createStripeIndexGroup(indexBuffers, /*stripeGroupIndex=*/0);

  auto reader = IndexReader::create(
      createInputStream(encodedStream.data),
      /*stripeIndex=*/0,
      stripeIndexGroup,
      pool_.get());

  // Test exact key match
  auto result = reader->seekAtOrAfter("key_a");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 0);

  // Test key before the only key - should return first key
  result = reader->seekAtOrAfter("key_");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 0);

  // Test key after the only key - should return false.
  result = reader->seekAtOrAfter("key_b");
  ASSERT_FALSE(result.has_value());
}

TEST_F(IndexReaderTest, singleChunkMultipleKeys) {
  // Create a single chunk with multiple sorted keys
  std::vector<std::string> keys = {
      "key_aaa", "key_bbb", "key_ccc", "key_ddd", "key_eee"};
  auto encodedStream = encodeKeyStream({keys});

  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "key_000";
  std::vector<Stripe> stripes = {createStripe(
      {.streamOffset = 0,
       .streamSize = static_cast<uint32_t>(encodedStream.data.size()),
       .stream = {.numChunks = 1, .chunkRows = {5}, .chunkOffsets = {0}},
       .chunkKeys = {"key_eee"}})};
  std::vector<int> stripeGroups = {1};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto stripeIndexGroup =
      createStripeIndexGroup(indexBuffers, /*stripeGroupIndex=*/0);

  auto reader = IndexReader::create(
      createInputStream(encodedStream.data), 0, stripeIndexGroup, pool_.get());

  // Test exact matches, keys between existing keys, and keys after last key.
  // Keys between existing keys should return the next key's row.
  // Keys after the last key should return std::nullopt.
  // Mix found and not-found cases with different seek sequences.
  std::vector<std::pair<std::string, std::optional<uint32_t>>> testCases = {
      {"key_ccc", 2}, // exact match in middle
      {"key_zzz", std::nullopt}, // after last key -> not found
      {"key_aaa", 0}, // seek backward to first key
      {"key_fff", std::nullopt}, // after last key -> not found
      {"key_eee", 4}, // seek forward to last key
      {"key_bbb", 1}, // seek backward
      {"key_bbc", 2}, // between bbb and ccc -> returns ccc
      {"key_000", 0}, // before first key -> returns first
      {"key_ddd", 3}, // seek forward
      {"key_ggg", std::nullopt}, // after last key -> not found
      {"key_aab", 1}, // between aaa and bbb -> returns bbb
      {"key_dde", 4}, // between ddd and eee -> returns eee
      {"key_ccd", 3}, // between ccc and ddd -> returns ddd
  };

  for (const auto& [key, expectedRow] : testCases) {
    SCOPED_TRACE(fmt::format("key: {}", key));
    auto result = reader->seekAtOrAfter(key);
    EXPECT_EQ(result, expectedRow);
  }
}

TEST_F(IndexReaderTest, multipleChunks) {
  // Create multiple chunks with different keys
  std::vector<std::vector<std::string>> keyChunks = {
      {"key_a", "key_b", "key_c"}, // Chunk 0: 3 keys
      {"key_d", "key_e", "key_f"}, // Chunk 1: 3 keys
      {"key_g", "key_h", "key_i"}, // Chunk 2: 3 keys
  };
  auto encodedStream = encodeKeyStream(keyChunks);

  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "key_0";
  std::vector<Stripe> stripes = {createStripe(
      {.streamOffset = 0,
       .streamSize = static_cast<uint32_t>(encodedStream.data.size()),
       .stream =
           {.numChunks = 3,
            .chunkRows = {3, 3, 3},
            .chunkOffsets = encodedStream.chunkOffsets},
       .chunkKeys = {"key_c", "key_f", "key_i"}})};
  std::vector<int> stripeGroups = {1};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto stripeIndexGroup =
      createStripeIndexGroup(indexBuffers, /*stripeGroupIndex=*/0);

  auto reader = IndexReader::create(
      createInputStream(encodedStream.data), 0, stripeIndexGroup, pool_.get());

  // Test keys across all chunks with mixed found/not-found cases.
  // Chunk 0: row offset 0, Chunk 1: row offset 3, Chunk 2: row offset 6
  // Keys after last key should return std::nullopt.
  // Mix different seek sequence combinations (forward, backward, between
  // chunks).
  std::vector<std::pair<std::string, std::optional<uint32_t>>> testCases = {
      {"key_e", 4}, // chunk 1: middle key (start in middle)
      {"key_z", std::nullopt}, // after last key -> not found
      {"key_a", 0}, // seek backward to chunk 0: first key
      {"key_j", std::nullopt}, // after last key -> not found
      {"key_h", 7}, // seek forward to chunk 2: second key
      {"key_b", 1}, // seek backward to chunk 0: second key
      {"key_x", std::nullopt}, // after last key -> not found
      {"key_f1", 6}, // between chunks -> returns first of chunk 2
      {"key_0", 0}, // before first key -> returns first
      {"key_i", 8}, // chunk 2: last key
      {"key_m", std::nullopt}, // after last key -> not found
      {"key_c", 2}, // seek backward to chunk 0: last key
      {"key_d", 3}, // seek forward to chunk 1: first key
      {"key_c1", 3}, // between chunks -> returns first of chunk 1
      {"key_y", std::nullopt}, // after last key -> not found
      {"key_g", 6}, // chunk 2: first key
      {"key_f", 5}, // seek backward to chunk 1: last key
  };

  for (const auto& [key, expectedRow] : testCases) {
    SCOPED_TRACE(fmt::format("key: {}", key));
    auto result = reader->seekAtOrAfter(key);
    EXPECT_EQ(result, expectedRow);
  }
}

TEST_F(IndexReaderTest, seekBetweenChunks) {
  // Create multiple chunks and test seeking keys that fall between chunks
  std::vector<std::vector<std::string>> keyChunks = {
      {"key_aaa", "key_bbb", "key_ccc"}, // Chunk 0
      {"key_ddd", "key_eee", "key_fff"}, // Chunk 1
  };
  auto encodedStream = encodeKeyStream(keyChunks);

  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "key_000";
  std::vector<Stripe> stripes = {createStripe(
      {.streamOffset = 0,
       .streamSize = static_cast<uint32_t>(encodedStream.data.size()),
       .stream =
           {.numChunks = 2,
            .chunkRows = {3, 3},
            .chunkOffsets = encodedStream.chunkOffsets},
       .chunkKeys = {"key_ccc", "key_fff"}})};
  std::vector<int> stripeGroups = {1};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto stripeIndexGroup =
      createStripeIndexGroup(indexBuffers, /*stripeGroupIndex=*/0);

  auto reader = IndexReader::create(
      createInputStream(encodedStream.data), 0, stripeIndexGroup, pool_.get());

  // Key falls between chunk 0's max (ccc) and chunk 1's content (ddd)
  // The index lookup should direct us to chunk 1, and we find ddd
  auto result = reader->seekAtOrAfter("key_ccd");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 3); // First key in chunk 1

  // Key exactly at chunk boundary
  result = reader->seekAtOrAfter("key_ddd");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 3);

  result = reader->seekAtOrAfter("key_aaa");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 0);

  result = reader->seekAtOrAfter("key_aa");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 0);

  result = reader->seekAtOrAfter("key_aaa");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 0);

  result = reader->seekAtOrAfter("key_zzz");
  ASSERT_FALSE(result.has_value());

  result = reader->seekAtOrAfter("key_aaa");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 0);

  result = reader->seekAtOrAfter("key_eee");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 4);
}

TEST_F(IndexReaderTest, multipleStripes) {
  // Create key streams for two stripes
  std::vector<std::vector<std::string>> stripe0Keys = {
      {"key_a", "key_b", "key_c"}};
  std::vector<std::vector<std::string>> stripe1Keys = {
      {"key_d", "key_e", "key_f"}};

  auto encodedStream0 = encodeKeyStream(stripe0Keys);
  auto encodedStream1 = encodeKeyStream(stripe1Keys);

  // Concatenate streams
  std::string combinedStream = encodedStream0.data + encodedStream1.data;

  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "key_0";
  std::vector<Stripe> stripes = {
      createStripe(
          {.streamOffset = 0,
           .streamSize = static_cast<uint32_t>(encodedStream0.data.size()),
           .stream = {.numChunks = 1, .chunkRows = {3}, .chunkOffsets = {0}},
           .chunkKeys = {"key_c"}}),
      createStripe(
          {.streamOffset = static_cast<uint32_t>(encodedStream0.data.size()),
           .streamSize = static_cast<uint32_t>(encodedStream1.data.size()),
           .stream = {.numChunks = 1, .chunkRows = {3}, .chunkOffsets = {0}},
           .chunkKeys = {"key_f"}})};
  std::vector<int> stripeGroups = {2};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto stripeIndexGroup =
      createStripeIndexGroup(indexBuffers, /*stripeGroupIndex=*/0);

  // Test reader for stripe 0
  auto reader0 = IndexReader::create(
      createInputStream(encodedStream0.data), 0, stripeIndexGroup, pool_.get());

  auto result = reader0->seekAtOrAfter("key_a");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 0);

  result = reader0->seekAtOrAfter("key_b");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 1);

  // Test reader for stripe 1
  auto reader1 = IndexReader::create(
      createInputStream(encodedStream1.data), 1, stripeIndexGroup, pool_.get());

  result = reader1->seekAtOrAfter("key_d");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 0);

  result = reader1->seekAtOrAfter("key_e");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 1);
}

TEST_F(IndexReaderTest, emptyStringKey) {
  // Test with empty string as a key
  std::vector<std::string> keys = {"", "a", "b"};
  auto encodedStream = encodeKeyStream({keys});

  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey;
  std::vector<Stripe> stripes = {createStripe(
      {.streamOffset = 0,
       .streamSize = static_cast<uint32_t>(encodedStream.data.size()),
       .stream = {.numChunks = 1, .chunkRows = {3}, .chunkOffsets = {0}},
       .chunkKeys = {"b"}})};
  std::vector<int> stripeGroups = {1};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto stripeIndexGroup =
      createStripeIndexGroup(indexBuffers, /*stripeGroupIndex=*/0);

  auto reader = IndexReader::create(
      createInputStream(encodedStream.data), 0, stripeIndexGroup, pool_.get());

  // Test empty string key
  auto result = reader->seekAtOrAfter("");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 0);

  result = reader->seekAtOrAfter("a");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 1);

  result = reader->seekAtOrAfter("e");
  ASSERT_FALSE(result.has_value());
}

TEST_F(IndexReaderTest, largeNumberOfKeys) {
  // Test with a large number of keys in a single chunk
  std::vector<std::string> keys;
  constexpr int numKeys = 1000;
  keys.reserve(numKeys);
  for (int i = 0; i < numKeys; ++i) {
    keys.push_back(fmt::format("key_{:06d}", i));
  }
  auto encodedStream = encodeKeyStream({keys});

  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "key_000000";
  std::vector<Stripe> stripes = {createStripe(
      {.streamOffset = 0,
       .streamSize = static_cast<uint32_t>(encodedStream.data.size()),
       .stream = {.numChunks = 1, .chunkRows = {numKeys}, .chunkOffsets = {0}},
       .chunkKeys = {keys.back()}})};
  std::vector<int> stripeGroups = {1};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto stripeIndexGroup =
      createStripeIndexGroup(indexBuffers, /*stripeGroupIndex=*/0);

  auto reader = IndexReader::create(
      createInputStream(encodedStream.data), 0, stripeIndexGroup, pool_.get());

  // Test some specific keys
  std::vector<int> testIndices = {0, 1, 10, 100, 500, 999};
  for (int idx : testIndices) {
    SCOPED_TRACE(fmt::format("index: {}", idx));
    auto result = reader->seekAtOrAfter(keys[idx]);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), idx);
  }

  // Test key between existing keys
  auto result = reader->seekAtOrAfter("key_000500");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 500);

  // Test key slightly after an existing key
  result = reader->seekAtOrAfter("key_000500x");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 501);
}

TEST_F(IndexReaderTest, repeatedSeeksToSameChunk) {
  // Test that repeated seeks to the same chunk work correctly
  // (tests chunk caching behavior)
  std::vector<std::vector<std::string>> keyChunks = {
      {"key_a", "key_b", "key_c"},
      {"key_d", "key_e", "key_f"},
  };
  auto encodedStream = encodeKeyStream(keyChunks);

  std::vector<int32_t> chunkRows;
  std::vector<std::string> chunkKeys;
  for (const auto& chunk : keyChunks) {
    chunkRows.push_back(static_cast<int32_t>(chunk.size()));
    chunkKeys.push_back(chunk.back());
  }

  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "key_0";
  std::vector<Stripe> stripes = {createStripe(
      {.streamOffset = 0,
       .streamSize = static_cast<uint32_t>(encodedStream.data.size()),
       .stream =
           {.numChunks = static_cast<uint32_t>(keyChunks.size()),
            .chunkRows = chunkRows,
            .chunkOffsets = encodedStream.chunkOffsets},
       .chunkKeys = chunkKeys})};
  std::vector<int> stripeGroups = {1};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto stripeIndexGroup =
      createStripeIndexGroup(indexBuffers, /*stripeGroupIndex=*/0);

  auto reader = IndexReader::create(
      createInputStream(encodedStream.data), 0, stripeIndexGroup, pool_.get());

  // Test repeated seeks within each chunk and across chunks.
  // Mix forward, backward, and repeated seeks to test caching behavior.
  std::vector<std::pair<std::string, std::optional<uint32_t>>> testCases = {
      {"key_a", 0}, // chunk 0: first key
      {"key_b", 1}, // chunk 0: forward seek
      {"key_a", 0}, // chunk 0: backward seek (repeat)
      {"key_d", 3}, // chunk 1: switch chunk
      {"key_c", 2}, // chunk 0: switch back
      {"key_e", 4}, // chunk 1: switch again
      {"key_f", 5}, // chunk 1: forward seek
      {"key_d", 3}, // chunk 1: backward seek (repeat)
      {"key_z", std::nullopt}, // after last key -> not found
  };

  for (const auto& [key, expectedRow] : testCases) {
    SCOPED_TRACE(fmt::format("key: {}", key));
    auto result = reader->seekAtOrAfter(key);
    EXPECT_EQ(result, expectedRow);
  }
}

TEST_F(IndexReaderTest, unevenChunkSizes) {
  // Test with chunks of varying sizes
  std::vector<std::vector<std::string>> keyChunks = {
      {"key_a"}, // Small chunk: 1 key
      {"key_b", "key_c", "key_d", "key_e", "key_f"}, // Large chunk: 5 keys
      {"key_g", "key_h"}, // Medium chunk: 2 keys
  };
  auto encodedStream = encodeKeyStream(keyChunks);

  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "key_0";
  std::vector<Stripe> stripes = {createStripe(
      {.streamOffset = 0,
       .streamSize = static_cast<uint32_t>(encodedStream.data.size()),
       .stream =
           {.numChunks = 3,
            .chunkRows = {1, 5, 2},
            .chunkOffsets = encodedStream.chunkOffsets},
       .chunkKeys = {"key_a", "key_f", "key_h"}})};
  std::vector<int> stripeGroups = {1};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto stripeIndexGroup =
      createStripeIndexGroup(indexBuffers, /*stripeGroupIndex=*/0);

  auto reader = IndexReader::create(
      createInputStream(encodedStream.data), 0, stripeIndexGroup, pool_.get());

  // Chunk 0: row offset 0, 1 key
  // Chunk 1: row offset 1, 5 keys
  // Chunk 2: row offset 6, 2 keys
  // Mix found and not-found cases with different seek sequences.
  std::vector<std::pair<std::string, std::optional<uint32_t>>> testCases = {
      {"key_d", 3}, // chunk 1: middle key (start in middle)
      {"key_z", std::nullopt}, // after last key -> not found
      {"key_a", 0}, // seek backward to chunk 0
      {"key_i", std::nullopt}, // after last key -> not found
      {"key_h", 7}, // seek forward to chunk 2: last key
      {"key_b", 1}, // seek backward to chunk 1: first key
      {"key_x", std::nullopt}, // after last key -> not found
      {"key_0", 0}, // before first key -> returns first
      {"key_g", 6}, // chunk 2: first key
      {"key_m", std::nullopt}, // after last key -> not found
      {"key_c", 2}, // seek backward to chunk 1
      {"key_f", 5}, // chunk 1: last key
      {"key_e", 4}, // chunk 1: second to last
      {"key_y", std::nullopt}, // after last key -> not found
  };

  for (const auto& [key, expectedRow] : testCases) {
    SCOPED_TRACE(fmt::format("key: {}", key));
    auto result = reader->seekAtOrAfter(key);
    EXPECT_EQ(result, expectedRow);
  }
}

TEST_F(IndexReaderTest, binaryKeys) {
  // Test with binary keys (keys containing null bytes and non-printable chars)
  std::vector<std::string> keys = {
      std::string("\x00\x01\x02", 3),
      std::string("\x00\x01\x03", 3),
      std::string("\x01\x00\x00", 3),
      std::string("\x01\x00\x01", 3),
      std::string("\xff\xfe\xfd", 3),
  };
  auto encodedStream = encodeKeyStream({keys});

  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = std::string("\x00\x00\x00", 3);
  std::vector<Stripe> stripes = {createStripe(
      {.streamOffset = 0,
       .streamSize = static_cast<uint32_t>(encodedStream.data.size()),
       .stream = {.numChunks = 1, .chunkRows = {5}, .chunkOffsets = {0}},
       .chunkKeys = {keys.back()}})};
  std::vector<int> stripeGroups = {1};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto stripeIndexGroup =
      createStripeIndexGroup(indexBuffers, /*stripeGroupIndex=*/0);

  auto reader = IndexReader::create(
      createInputStream(encodedStream.data), 0, stripeIndexGroup, pool_.get());

  // Test exact matches and not-found cases for binary keys.
  // Keys after the last key should return std::nullopt.
  // Mix found and not-found cases with different seek sequences.
  std::vector<std::pair<std::string, std::optional<uint32_t>>> testCases = {
      {std::string("\x01\x00\x00", 3), 2}, // exact match in middle
      {std::string("\xff\xff\xff", 3), std::nullopt}, // after last -> not found
      {std::string("\x00\x01\x02", 3), 0}, // seek backward to first key
      {std::string("\xff\xff\x00", 3), std::nullopt}, // after last -> not found
      {std::string("\xff\xfe\xfd", 3), 4}, // seek forward to last key
      {std::string("\x00\x01\x03", 3), 1}, // seek backward to second key
      {std::string("\x00\x00\x00", 3), 0}, // before first -> returns first
      {std::string("\x01\x00\x01", 3), 3}, // seek forward
      {std::string("\xff\xff\xfe", 3), std::nullopt}, // after last -> not found
      {std::string("\x00\x01\x02\x01", 4), 1}, // between keys -> returns next
      {std::string("\x01\x00\x00\x01", 4), 3}, // between keys -> returns next
  };

  for (const auto& [key, expectedRow] : testCases) {
    SCOPED_TRACE(fmt::format("key size: {}", key.size()));
    auto result = reader->seekAtOrAfter(key);
    EXPECT_EQ(result, expectedRow);
  }
}

} // namespace facebook::nimble::index::test
