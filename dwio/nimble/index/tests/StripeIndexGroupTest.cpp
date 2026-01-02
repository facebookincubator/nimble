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
#include <limits>

#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/index/StripeIndexGroup.h"
#include "dwio/nimble/index/tests/TabletIndexTestBase.h"
#include "dwio/nimble/tablet/TabletReader.h"

namespace facebook::nimble::index::test {

class StripeIndexGroupTest : public TabletIndexTestBase {};

TEST_F(StripeIndexGroupTest, chunkLocation) {
  ChunkLocation loc1{100, 200};
  EXPECT_EQ(loc1.streamOffset, 100);
  EXPECT_EQ(loc1.rowOffset, 200);

  ChunkLocation loc2{0, 0};
  EXPECT_EQ(loc2.streamOffset, 0);
  EXPECT_EQ(loc2.rowOffset, 0);

  ChunkLocation loc3{UINT32_MAX, UINT32_MAX};
  EXPECT_EQ(loc3.streamOffset, UINT32_MAX);
  EXPECT_EQ(loc3.rowOffset, UINT32_MAX);
}

TEST_F(StripeIndexGroupTest, keyStreamRegion) {
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
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);

  // Test all key stream regions using parameterized test cases
  struct {
    uint32_t groupIndex;
    uint32_t stripeIndex;
    uint32_t expectedOffset;
    uint32_t expectedLength;
  } testCases[] = {
      // Group 0: stripes 0, 1
      {0, 0, 0, 100},
      {0, 1, 100, 150},
      // Group 1: stripes 2, 3
      {1, 2, 250, 200},
      {1, 3, 450, 50},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(
        fmt::format(
            "groupIndex {} stripeIndex {}",
            testCase.groupIndex,
            testCase.stripeIndex));
    auto stripeIndexGroup =
        createStripeIndexGroup(indexBuffers, testCase.groupIndex);
    ASSERT_NE(stripeIndexGroup, nullptr);

    auto region = stripeIndexGroup->keyStreamRegion(testCase.stripeIndex);
    ASSERT_TRUE(region.has_value());
    EXPECT_EQ(region->offset, testCase.expectedOffset);
    EXPECT_EQ(region->length, testCase.expectedLength);
  }
}

TEST_F(StripeIndexGroupTest, lookupChunkWithEncodedKey) {
  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "aaa";
  std::vector<Stripe> stripes = {
      {.streams = {{.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 0,
            .streamSize = 100,
            .stream =
                {.numChunks = 3,
                 .chunkRows = {100, 200, 300},
                 .chunkOffsets = {0, 50, 120}},
            .chunkKeys = {"ccc", "fff", "iii"}}},
      {.streams = {{.numChunks = 1, .chunkRows = {200}, .chunkOffsets = {0}}},
       .keyStream = {
           .streamOffset = 100,
           .streamSize = 150,
           .stream =
               {.numChunks = 2,
                .chunkRows = {150, 300},
                .chunkOffsets = {0, 80}},
           .chunkKeys = {"mmm", "ppp"}}}};
  std::vector<int> stripeGroups = {2};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto stripeIndexGroup = createStripeIndexGroup(indexBuffers, 0);
  ASSERT_NE(stripeIndexGroup, nullptr);

  struct {
    uint32_t stripeIndex;
    std::string encodedKey;
    std::optional<uint32_t> expectedStreamOffset;
    std::optional<uint32_t> expectedRowOffset;
  } testCases[] = {
      // Stripe 0: chunks with keys "ccc", "fff", "iii"
      // chunkRows = {100, 200, 300}
      // Key before first chunk locate at the first chunk as we
      // expect the caller will locate the right stripe with tablet index.
      {0, "aa", 0, 0},
      {0, "aaa", 0, 0},
      {0, "bbb", 0, 0},
      // Key at first chunk boundary
      {0, "ccc", 0, 0},
      // Key between first and second chunk
      {0, "ddd", 50, 100},
      {0, "eee", 50, 100},
      // Key at second chunk boundary
      {0, "fff", 50, 100},
      // Key between second and third chunk
      {0, "ggg", 120, 300},
      {0, "hhh", 120, 300},
      // Key at third chunk boundary
      {0, "iii", 120, 300},
      // Key after last chunk - not found
      {0, "jjj", std::nullopt, std::nullopt},
      {0, "zzz", std::nullopt, std::nullopt},

      // Stripe 1: chunks with keys "mmm", "ppp"
      // chunkRows = {150, 300}
      // Key before first chunk locate at the first chunk as we
      // expect the caller will locate the right stripe with tablet index.
      {1, "aaa", 0, 0},
      {1, "lll", 0, 0},
      // Key at first chunk boundary
      {1, "mmm", 0, 0},
      // Key between first and second chunk
      {1, "nnn", 80, 150},
      {1, "ooo", 80, 150},
      // Key at second chunk boundary
      {1, "ppp", 80, 150},
      // Key after last chunk - not found
      {1, "qqq", std::nullopt, std::nullopt},
      {1, "zzz", std::nullopt, std::nullopt}};

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(
        fmt::format(
            "stripeIndex {} encodedKey {}",
            testCase.stripeIndex,
            testCase.encodedKey));
    auto result = stripeIndexGroup->lookupChunk(
        testCase.stripeIndex, testCase.encodedKey);
    if (testCase.expectedStreamOffset.has_value()) {
      ASSERT_TRUE(result.has_value())
          << "Expected chunk at streamOffset "
          << testCase.expectedStreamOffset.value() << ", rowOffset "
          << testCase.expectedRowOffset.value() << " for key '"
          << testCase.encodedKey << "', but lookup returned nullopt";
      EXPECT_EQ(result->streamOffset, testCase.expectedStreamOffset.value());
      EXPECT_EQ(result->rowOffset, testCase.expectedRowOffset.value());
    } else {
      EXPECT_FALSE(result.has_value())
          << "Expected nullopt for key '" << testCase.encodedKey
          << "', but got streamOffset " << result->streamOffset
          << ", rowOffset " << result->rowOffset;
    }
  }
}

TEST_F(StripeIndexGroupTest, lookupChunkWithRowId) {
  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "aaa";
  // Setup: 2 stripes, each with 2 streams with different chunk configurations
  std::vector<Stripe> stripes = {
      {.streams =
           {// Stream 0: 3 chunks at rows {100, 250, 400}, offsets {0, 50, 120}
            {.numChunks = 3,
             .chunkRows = {100, 250, 400},
             .chunkOffsets = {0, 50, 120}},
            // Stream 1: 2 chunks at rows {150, 300}, offsets {0, 80}
            {.numChunks = 2, .chunkRows = {150, 300}, .chunkOffsets = {0, 80}}},
       .keyStream =
           {.streamOffset = 0,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {400}, .chunkOffsets = {0}},
            .chunkKeys = {"bbb"}}},
      {.streams =
           {// Stream 0: 2 chunks at rows {200, 350}, offsets {0, 60}
            {.numChunks = 2, .chunkRows = {200, 350}, .chunkOffsets = {0, 60}},
            // Stream 1: 3 chunks at rows {100, 200, 350}, offsets {0, 40, 90}
            {.numChunks = 3,
             .chunkRows = {100, 200, 350},
             .chunkOffsets = {0, 40, 90}}},
       .keyStream = {
           .streamOffset = 100,
           .streamSize = 100,
           .stream = {.numChunks = 1, .chunkRows = {350}, .chunkOffsets = {0}},
           .chunkKeys = {"ccc"}}}};
  std::vector<int> stripeGroups = {2};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto stripeIndexGroup = createStripeIndexGroup(indexBuffers, 0);
  ASSERT_NE(stripeIndexGroup, nullptr);

  struct {
    uint32_t stripeIndex;
    uint32_t streamId;
    uint32_t rowId;
    std::optional<uint32_t> expectedStreamOffset;
    std::optional<uint32_t> expectedRowOffset;
  } testCases[] = {
      // Stripe 0, Stream 0: chunks at rows {100, 250, 400}, offsets {0, 50,
      // 120}
      // Accumulated rows: {100, 350, 750}
      // Row 0 - first row of first chunk
      {0, 0, 0, 0, 0},
      // Row in first chunk [0, 100)
      {0, 0, 50, 0, 0},
      {0, 0, 99, 0, 0},
      // Row at first chunk boundary (100) - second chunk
      {0, 0, 100, 50, 100},
      // Row in second chunk [100, 350)
      {0, 0, 150, 50, 100},
      {0, 0, 249, 50, 100},
      {0, 0, 349, 50, 100},
      // Row at second chunk boundary (350) - third chunk
      {0, 0, 350, 120, 350},
      // Row in third chunk [350, 750)
      {0, 0, 351, 120, 350},
      {0, 0, 399, 120, 350},
      {0, 0, 749, 120, 350},
      // Row at/after last chunk boundary - not found
      {0, 0, 750, std::nullopt, std::nullopt},
      {0, 0, 850, std::nullopt, std::nullopt},

      // Stripe 0, Stream 1: chunks at rows {150, 300}, offsets {0, 80}
      // Accumulated rows: {150, 450}
      // Row 0 - first row of first chunk
      {0, 1, 0, 0, 0},
      // Row in first chunk [0, 150)
      {0, 1, 75, 0, 0},
      {0, 1, 149, 0, 0},
      // Row at first chunk boundary (150) - second chunk
      {0, 1, 150, 80, 150},
      // Row in second chunk [150, 450)
      {0, 1, 200, 80, 150},
      {0, 1, 299, 80, 150},
      {0, 1, 449, 80, 150},
      // Row at/after last chunk boundary - not found
      {0, 1, 450, std::nullopt, std::nullopt},
      {0, 1, 500, std::nullopt, std::nullopt},

      // Stripe 1, Stream 0: chunks at rows {200, 350}, offsets {0, 60}
      // Accumulated rows: {200, 550}
      // Row 0 - first row of first chunk
      {1, 0, 0, 0, 0},
      // Row in first chunk [0, 200)
      {1, 0, 100, 0, 0},
      {1, 0, 199, 0, 0},
      // Row at first chunk boundary (200) - second chunk
      {1, 0, 200, 60, 200},
      // Row in second chunk [200, 550)
      {1, 0, 275, 60, 200},
      {1, 0, 400, 60, 200},
      {1, 0, 549, 60, 200},
      // Row at/after last chunk boundary - not found
      {1, 0, 550, std::nullopt, std::nullopt},
      {1, 0, 600, std::nullopt, std::nullopt},

      // Stripe 1, Stream 1: chunks at rows {100, 200, 350}, offsets {0, 40, 90}
      // Accumulated rows: {100, 300, 650}
      // Row 0 - first row of first chunk
      {1, 1, 0, 0, 0},
      // Row in first chunk [0, 100)
      {1, 1, 50, 0, 0},
      {1, 1, 99, 0, 0},
      // Row at first chunk boundary (100) - second chunk
      {1, 1, 100, 40, 100},
      // Row in second chunk [100, 300)
      {1, 1, 150, 40, 100},
      {1, 1, 200, 40, 100},
      {1, 1, 299, 40, 100},
      // Row at second chunk boundary (300) - third chunk
      {1, 1, 300, 90, 300},
      // Row in third chunk [300, 650)
      {1, 1, 400, 90, 300},
      {1, 1, 500, 90, 300},
      {1, 1, 649, 90, 300},
      // Row at/after last chunk boundary - not found
      {1, 1, 650, std::nullopt, std::nullopt},
      {1, 1, 700, std::nullopt, std::nullopt}};

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(
        fmt::format(
            "stripeIndex {} streamId {} rowId {} expectedStreamOffset {} expectedRowOffset {}",
            testCase.stripeIndex,
            testCase.streamId,
            testCase.rowId,
            testCase.expectedStreamOffset.has_value()
                ? std::to_string(testCase.expectedStreamOffset.value())
                : "nullopt",
            testCase.expectedRowOffset.has_value()
                ? std::to_string(testCase.expectedRowOffset.value())
                : "nullopt"));
    auto result = stripeIndexGroup->lookupChunk(
        testCase.stripeIndex, testCase.streamId, testCase.rowId);
    if (testCase.expectedStreamOffset.has_value()) {
      ASSERT_TRUE(result.has_value())
          << "Expected chunk at streamOffset "
          << testCase.expectedStreamOffset.value() << ", rowOffset "
          << testCase.expectedRowOffset.value() << " for stripe "
          << testCase.stripeIndex << ", stream " << testCase.streamId
          << ", rowId " << testCase.rowId << ", but lookup returned nullopt";
      EXPECT_EQ(result->streamOffset, testCase.expectedStreamOffset.value());
      EXPECT_EQ(result->rowOffset, testCase.expectedRowOffset.value());
    } else {
      EXPECT_FALSE(result.has_value())
          << "Expected nullopt for stripe " << testCase.stripeIndex
          << ", stream " << testCase.streamId << ", rowId " << testCase.rowId
          << ", but got streamOffset " << result->streamOffset << ", rowOffset "
          << result->rowOffset;
    }
  }
}

TEST_F(StripeIndexGroupTest, createStreamIndex) {
  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "aaa";
  std::vector<Stripe> stripes = {
      {.streams =
           {{.numChunks = 3,
             .chunkRows = {100, 200, 300},
             .chunkOffsets = {0, 50, 120}},
            {.numChunks = 2, .chunkRows = {150, 300}, .chunkOffsets = {0, 80}}},
       .keyStream = {
           .streamOffset = 0,
           .streamSize = 100,
           .stream =
               {.numChunks = 3,
                .chunkRows = {100, 200, 300},
                .chunkOffsets = {0, 30, 70}},
           .chunkKeys = {"bbb", "ccc", "ddd"}}}};
  std::vector<int> stripeGroups = {1};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto stripeIndexGroup = createStripeIndexGroup(indexBuffers, 0);
  ASSERT_NE(stripeIndexGroup, nullptr);

  // Create StreamIndex for stream 0 in stripe 0
  auto streamIndex = stripeIndexGroup->createStreamIndex(0, 0);
  ASSERT_NE(streamIndex, nullptr);
}

TEST_F(StripeIndexGroupTest, streamIndexStreamId) {
  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "aaa";
  std::vector<Stripe> stripes = {
      {.streams =
           {{.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}},
            {.numChunks = 1, .chunkRows = {150}, .chunkOffsets = {0}},
            {.numChunks = 1, .chunkRows = {200}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 0,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {200}, .chunkOffsets = {0}},
            .chunkKeys = {"bbb"}}},
      {.streams =
           {{.numChunks = 1, .chunkRows = {120}, .chunkOffsets = {0}},
            {.numChunks = 1, .chunkRows = {180}, .chunkOffsets = {0}},
            {.numChunks = 1, .chunkRows = {240}, .chunkOffsets = {0}}},
       .keyStream = {
           .streamOffset = 100,
           .streamSize = 100,
           .stream = {.numChunks = 1, .chunkRows = {240}, .chunkOffsets = {0}},
           .chunkKeys = {"ccc"}}}};
  std::vector<int> stripeGroups = {2};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto stripeIndexGroup = createStripeIndexGroup(indexBuffers, 0);
  ASSERT_NE(stripeIndexGroup, nullptr);

  struct {
    uint32_t stripeIndex;
    uint32_t streamId;
  } testCases[] = {
      {0, 0},
      {0, 1},
      {0, 2},
      {1, 0},
      {1, 1},
      {1, 2},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(
        fmt::format(
            "stripeIndex {} streamId {}",
            testCase.stripeIndex,
            testCase.streamId));
    auto streamIndex = stripeIndexGroup->createStreamIndex(
        testCase.stripeIndex, testCase.streamId);
    ASSERT_NE(streamIndex, nullptr);
    EXPECT_EQ(streamIndex->streamId(), testCase.streamId);
  }
}

TEST_F(StripeIndexGroupTest, streamIndexLookupChunk) {
  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "aaa";
  // Setup: 4 stripes across 2 groups, each stripe with 2 streams
  std::vector<Stripe> stripes = {
      // Stripe 0 (Group 0)
      {.streams =
           {// Stream 0: 3 chunks at rows {100, 250, 400}, offsets {0, 50, 120}
            {.numChunks = 3,
             .chunkRows = {100, 250, 400},
             .chunkOffsets = {0, 50, 120}},
            // Stream 1: 2 chunks at rows {150, 300}, offsets {0, 80}
            {.numChunks = 2, .chunkRows = {150, 300}, .chunkOffsets = {0, 80}}},
       .keyStream =
           {.streamOffset = 0,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {400}, .chunkOffsets = {0}},
            .chunkKeys = {"bbb"}}},
      // Stripe 1 (Group 0)
      {.streams =
           {// Stream 0: 2 chunks at rows {200, 350}, offsets {0, 60}
            {.numChunks = 2, .chunkRows = {200, 350}, .chunkOffsets = {0, 60}},
            // Stream 1: 3 chunks at rows {100, 200, 350}, offsets {0, 40, 90}
            {.numChunks = 3,
             .chunkRows = {100, 200, 350},
             .chunkOffsets = {0, 40, 90}}},
       .keyStream =
           {.streamOffset = 100,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {350}, .chunkOffsets = {0}},
            .chunkKeys = {"ccc"}}},
      // Stripe 2 (Group 1)
      {.streams =
           {// Stream 0: 2 chunks at rows {150, 300}, offsets {0, 70}
            {.numChunks = 2, .chunkRows = {150, 300}, .chunkOffsets = {0, 70}},
            // Stream 1: 2 chunks at rows {180, 360}, offsets {0, 55}
            {.numChunks = 2, .chunkRows = {180, 360}, .chunkOffsets = {0, 55}}},
       .keyStream =
           {.streamOffset = 200,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {300}, .chunkOffsets = {0}},
            .chunkKeys = {"ddd"}}},
      // Stripe 3 (Group 1)
      {.streams =
           {// Stream 0: 3 chunks at rows {80, 160, 240}, offsets {0, 30, 65}
            {.numChunks = 3,
             .chunkRows = {80, 160, 240},
             .chunkOffsets = {0, 30, 65}},
            // Stream 1: 2 chunks at rows {120, 240}, offsets {0, 45}
            {.numChunks = 2, .chunkRows = {120, 240}, .chunkOffsets = {0, 45}}},
       .keyStream = {
           .streamOffset = 300,
           .streamSize = 100,
           .stream = {.numChunks = 1, .chunkRows = {240}, .chunkOffsets = {0}},
           .chunkKeys = {"eee"}}}};
  std::vector<int> stripeGroups = {2, 2};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);

  struct {
    uint32_t groupIndex;
    uint32_t stripeIndex;
    uint32_t streamId;
    uint32_t rowId;
    std::optional<uint32_t> expectedStreamOffset;
    std::optional<uint32_t> expectedRowOffset;
  } testCases[] = {
      // Group 0, Stripe 0, Stream 0: chunks at rows {100, 250, 400}, offsets
      // {0, 50, 120}
      // Accumulated rows: {100, 350, 750}
      {0, 0, 0, 0, 0, 0},
      {0, 0, 0, 50, 0, 0},
      {0, 0, 0, 99, 0, 0},
      {0, 0, 0, 100, 50, 100},
      {0, 0, 0, 349, 50, 100},
      {0, 0, 0, 350, 120, 350},
      {0, 0, 0, 749, 120, 350},
      {0, 0, 0, 750, std::nullopt, std::nullopt},

      // Group 0, Stripe 0, Stream 1: chunks at rows {150, 300}, offsets {0, 80}
      // Accumulated rows: {150, 450}
      {0, 0, 1, 0, 0, 0},
      {0, 0, 1, 149, 0, 0},
      {0, 0, 1, 150, 80, 150},
      {0, 0, 1, 449, 80, 150},
      {0, 0, 1, 450, std::nullopt, std::nullopt},

      // Group 0, Stripe 1, Stream 0: chunks at rows {200, 350}, offsets {0, 60}
      // Accumulated rows: {200, 550}
      {0, 1, 0, 0, 0, 0},
      {0, 1, 0, 199, 0, 0},
      {0, 1, 0, 200, 60, 200},
      {0, 1, 0, 549, 60, 200},
      {0, 1, 0, 550, std::nullopt, std::nullopt},

      // Group 0, Stripe 1, Stream 1: chunks at rows {100, 200, 350}, offsets
      // {0, 40, 90}
      // Accumulated rows: {100, 300, 650}
      {0, 1, 1, 0, 0, 0},
      {0, 1, 1, 99, 0, 0},
      {0, 1, 1, 100, 40, 100},
      {0, 1, 1, 299, 40, 100},
      {0, 1, 1, 300, 90, 300},
      {0, 1, 1, 649, 90, 300},
      {0, 1, 1, 650, std::nullopt, std::nullopt},

      // Group 1, Stripe 2, Stream 0: chunks at rows {150, 300}, offsets {0, 70}
      // Accumulated rows: {150, 450}
      {1, 2, 0, 0, 0, 0},
      {1, 2, 0, 149, 0, 0},
      {1, 2, 0, 150, 70, 150},
      {1, 2, 0, 449, 70, 150},
      {1, 2, 0, 450, std::nullopt, std::nullopt},

      // Group 1, Stripe 2, Stream 1: chunks at rows {180, 360}, offsets {0, 55}
      // Accumulated rows: {180, 540}
      {1, 2, 1, 0, 0, 0},
      {1, 2, 1, 179, 0, 0},
      {1, 2, 1, 180, 55, 180},
      {1, 2, 1, 539, 55, 180},
      {1, 2, 1, 540, std::nullopt, std::nullopt},

      // Group 1, Stripe 3, Stream 0: chunks at rows {80, 160, 240}, offsets {0,
      // 30, 65}
      // Accumulated rows: {80, 240, 480}
      {1, 3, 0, 0, 0, 0},
      {1, 3, 0, 79, 0, 0},
      {1, 3, 0, 80, 30, 80},
      {1, 3, 0, 239, 30, 80},
      {1, 3, 0, 240, 65, 240},
      {1, 3, 0, 479, 65, 240},
      {1, 3, 0, 480, std::nullopt, std::nullopt},

      // Group 1, Stripe 3, Stream 1: chunks at rows {120, 240}, offsets {0, 45}
      // Accumulated rows: {120, 360}
      {1, 3, 1, 0, 0, 0},
      {1, 3, 1, 119, 0, 0},
      {1, 3, 1, 120, 45, 120},
      {1, 3, 1, 359, 45, 120},
      {1, 3, 1, 360, std::nullopt, std::nullopt},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(
        fmt::format(
            "groupIndex {} stripeIndex {} streamId {} rowId {}",
            testCase.groupIndex,
            testCase.stripeIndex,
            testCase.streamId,
            testCase.rowId));

    // Create StripeIndexGroup based on groupIndex parameter
    auto stripeIndexGroup =
        createStripeIndexGroup(indexBuffers, testCase.groupIndex);
    ASSERT_NE(stripeIndexGroup, nullptr);

    // Create StreamIndex and test lookupChunk through it
    auto streamIndex = stripeIndexGroup->createStreamIndex(
        testCase.stripeIndex, testCase.streamId);
    ASSERT_NE(streamIndex, nullptr);

    auto result = streamIndex->lookupChunk(testCase.rowId);
    if (testCase.expectedStreamOffset.has_value()) {
      ASSERT_TRUE(result.has_value())
          << "Expected chunk at streamOffset "
          << testCase.expectedStreamOffset.value() << ", rowOffset "
          << testCase.expectedRowOffset.value() << " for group "
          << testCase.groupIndex << ", stripe " << testCase.stripeIndex
          << ", stream " << testCase.streamId << ", rowId " << testCase.rowId
          << ", but lookup returned nullopt";
      EXPECT_EQ(result->streamOffset, testCase.expectedStreamOffset.value());
      EXPECT_EQ(result->rowOffset, testCase.expectedRowOffset.value());
    } else {
      EXPECT_FALSE(result.has_value())
          << "Expected nullopt for group " << testCase.groupIndex << ", stripe "
          << testCase.stripeIndex << ", stream " << testCase.streamId
          << ", rowId " << testCase.rowId << ", but got streamOffset "
          << result->streamOffset << ", rowOffset " << result->rowOffset;
    }
  }
}

TEST_F(StripeIndexGroupTest, stripeIndexAndStreamIdOutOfBound) {
  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "aaa";
  // Setup: 4 stripes across 2 groups, each stripe with 2 streams
  std::vector<Stripe> stripes = {
      // Stripe 0 (Group 0)
      {.streams =
           {{.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}},
            {.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 0,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}},
            .chunkKeys = {"bbb"}}},
      // Stripe 1 (Group 0)
      {.streams =
           {{.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}},
            {.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 100,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}},
            .chunkKeys = {"ccc"}}},
      // Stripe 2 (Group 1)
      {.streams =
           {{.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}},
            {.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 200,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}},
            .chunkKeys = {"ddd"}}},
      // Stripe 3 (Group 1)
      {.streams =
           {{.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}},
            {.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}}},
       .keyStream = {
           .streamOffset = 300,
           .streamSize = 100,
           .stream = {.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}},
           .chunkKeys = {"eee"}}}};
  std::vector<int> stripeGroups = {2, 2};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);

  // Test Group 0 (stripes 0, 1)
  auto stripeIndexGroup0 = createStripeIndexGroup(indexBuffers, 0);
  ASSERT_NE(stripeIndexGroup0, nullptr);

  // Stripe index before group's range (group 0 starts at stripe 0)
  // This won't trigger "before range" since group 0 starts at stripe 0.
  // Test stripe index after group's range
  NIMBLE_ASSERT_THROW(
      stripeIndexGroup0->lookupChunk(2, 0, 0),
      "Stripe offset is out of range for this stripe index group");
  NIMBLE_ASSERT_THROW(
      stripeIndexGroup0->lookupChunk(3, 0, 0),
      "Stripe offset is out of range for this stripe index group");

  // Test streamId out of range (group has 2 streams: 0, 1)
  NIMBLE_ASSERT_THROW(
      stripeIndexGroup0->lookupChunk(0, 2, 0), "streamId out of range");
  NIMBLE_ASSERT_THROW(
      stripeIndexGroup0->lookupChunk(1, 10, 0), "streamId out of range");

  // Test Group 1 (stripes 2, 3)
  auto stripeIndexGroup1 = createStripeIndexGroup(indexBuffers, 1);
  ASSERT_NE(stripeIndexGroup1, nullptr);

  // Stripe index before group's range (group 1 starts at stripe 2)
  NIMBLE_ASSERT_THROW(
      stripeIndexGroup1->lookupChunk(0, 0, 0),
      "Stripe index is before this group's range");
  NIMBLE_ASSERT_THROW(
      stripeIndexGroup1->lookupChunk(1, 0, 0),
      "Stripe index is before this group's range");

  // Stripe index after group's range (group 1 has stripes 2, 3)
  NIMBLE_ASSERT_THROW(
      stripeIndexGroup1->lookupChunk(4, 0, 0),
      "Stripe offset is out of range for this stripe index group");
  NIMBLE_ASSERT_THROW(
      stripeIndexGroup1->lookupChunk(5, 0, 0),
      "Stripe offset is out of range for this stripe index group");

  // Test streamId out of range for group 1
  NIMBLE_ASSERT_THROW(
      stripeIndexGroup1->lookupChunk(2, 2, 0), "streamId out of range");
  NIMBLE_ASSERT_THROW(
      stripeIndexGroup1->lookupChunk(3, 5, 0), "streamId out of range");
}

TEST_F(StripeIndexGroupTest, streamIndexRowCount) {
  std::vector<std::string> indexColumns = {"col1"};
  std::string minKey = "aaa";
  // Setup: 4 stripes across 2 groups, each stripe with 2 streams
  std::vector<Stripe> stripes = {
      // Stripe 0 (Group 0)
      {.streams =
           {// Stream 0: 3 chunks at rows {100, 250, 400}, offsets {0, 50, 120}
            // Accumulated rows: 100, 350, 750 -> total rows = 750
            {.numChunks = 3,
             .chunkRows = {100, 250, 400},
             .chunkOffsets = {0, 50, 120}},
            // Stream 1: 2 chunks at rows {150, 300}, offsets {0, 80}
            // Accumulated rows: 150, 450 -> total rows = 450
            {.numChunks = 2, .chunkRows = {150, 300}, .chunkOffsets = {0, 80}}},
       .keyStream =
           {.streamOffset = 0,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {750}, .chunkOffsets = {0}},
            .chunkKeys = {"bbb"}}},
      // Stripe 1 (Group 0)
      {.streams =
           {// Stream 0: 2 chunks at rows {200, 350}, offsets {0, 60}
            // Accumulated rows: 200, 550 -> total rows = 550
            {.numChunks = 2, .chunkRows = {200, 350}, .chunkOffsets = {0, 60}},
            // Stream 1: 3 chunks at rows {100, 200, 350}, offsets {0, 40, 90}
            // Accumulated rows: 100, 300, 650 -> total rows = 650
            {.numChunks = 3,
             .chunkRows = {100, 200, 350},
             .chunkOffsets = {0, 40, 90}}},
       .keyStream =
           {.streamOffset = 100,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {550}, .chunkOffsets = {0}},
            .chunkKeys = {"ccc"}}},
      // Stripe 2 (Group 1)
      {.streams =
           {// Stream 0: 2 chunks at rows {150, 300}, offsets {0, 70}
            // Accumulated rows: 150, 450 -> total rows = 450
            {.numChunks = 2, .chunkRows = {150, 300}, .chunkOffsets = {0, 70}},
            // Stream 1: 2 chunks at rows {180, 360}, offsets {0, 55}
            // Accumulated rows: 180, 540 -> total rows = 540
            {.numChunks = 2, .chunkRows = {180, 360}, .chunkOffsets = {0, 55}}},
       .keyStream =
           {.streamOffset = 200,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {450}, .chunkOffsets = {0}},
            .chunkKeys = {"ddd"}}},
      // Stripe 3 (Group 1)
      {.streams =
           {// Stream 0: 3 chunks at rows {80, 160, 240}, offsets {0, 30, 65}
            // Accumulated rows: 80, 240, 480 -> total rows = 480
            {.numChunks = 3,
             .chunkRows = {80, 160, 240},
             .chunkOffsets = {0, 30, 65}},
            // Stream 1: 2 chunks at rows {120, 240}, offsets {0, 45}
            // Accumulated rows: 120, 360 -> total rows = 360
            {.numChunks = 2, .chunkRows = {120, 240}, .chunkOffsets = {0, 45}}},
       .keyStream = {
           .streamOffset = 300,
           .streamSize = 100,
           .stream = {.numChunks = 1, .chunkRows = {480}, .chunkOffsets = {0}},
           .chunkKeys = {"eee"}}}};
  std::vector<int> stripeGroups = {2, 2};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);

  struct {
    uint32_t groupIndex;
    uint32_t stripeIndex;
    uint32_t streamId;
    uint32_t expectedRowCount;
  } testCases[] = {
      // Group 0, Stripe 0
      {0, 0, 0, 750}, // Stream 0: accumulated rows = 100 + 250 + 400 = 750
      {0, 0, 1, 450}, // Stream 1: accumulated rows = 150 + 300 = 450

      // Group 0, Stripe 1
      {0, 1, 0, 550}, // Stream 0: accumulated rows = 200 + 350 = 550
      {0, 1, 1, 650}, // Stream 1: accumulated rows = 100 + 200 + 350 = 650

      // Group 1, Stripe 2
      {1, 2, 0, 450}, // Stream 0: accumulated rows = 150 + 300 = 450
      {1, 2, 1, 540}, // Stream 1: accumulated rows = 180 + 360 = 540

      // Group 1, Stripe 3
      {1, 3, 0, 480}, // Stream 0: accumulated rows = 80 + 160 + 240 = 480
      {1, 3, 1, 360}, // Stream 1: accumulated rows = 120 + 240 = 360
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(
        fmt::format(
            "groupIndex {} stripeIndex {} streamId {}",
            testCase.groupIndex,
            testCase.stripeIndex,
            testCase.streamId));

    // Create StripeIndexGroup based on groupIndex parameter
    auto stripeIndexGroup =
        createStripeIndexGroup(indexBuffers, testCase.groupIndex);
    ASSERT_NE(stripeIndexGroup, nullptr);

    // Test rowCount through StripeIndexGroup
    EXPECT_EQ(
        stripeIndexGroup->rowCount(testCase.stripeIndex, testCase.streamId),
        testCase.expectedRowCount);

    // Test rowCount through StreamIndex
    auto streamIndex = stripeIndexGroup->createStreamIndex(
        testCase.stripeIndex, testCase.streamId);
    ASSERT_NE(streamIndex, nullptr);
    EXPECT_EQ(streamIndex->rowCount(), testCase.expectedRowCount);
  }
}

} // namespace facebook::nimble::index::test
