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

#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/index/TabletIndex.h"
#include "dwio/nimble/index/tests/TabletIndexTestBase.h"
#include "dwio/nimble/tablet/TabletReader.h"

namespace facebook::nimble::index::test {
namespace {

class TabletIndexTest : public TabletIndexTestBase {};

TEST_F(TabletIndexTest, stripeLocation) {
  StripeLocation loc1{0};
  EXPECT_EQ(loc1.stripeIndex, 0);

  StripeLocation loc2{42};
  EXPECT_EQ(loc2.stripeIndex, 42);

  StripeLocation loc3{UINT32_MAX};
  EXPECT_EQ(loc3.stripeIndex, UINT32_MAX);

  EXPECT_NE(loc1, loc2);
  EXPECT_EQ(loc1, loc1);
}

TEST_F(TabletIndexTest, basic) {
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
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto tabletIndex = createTabletIndex(indexBuffers);

  EXPECT_EQ(tabletIndex->numStripes(), 4);
  EXPECT_FALSE(tabletIndex->empty());
  EXPECT_EQ(tabletIndex->minKey(), "aaa");
  EXPECT_EQ(tabletIndex->maxKey(), "eee");

  EXPECT_EQ(tabletIndex->stripeKey(0), "bbb");
  EXPECT_EQ(tabletIndex->stripeKey(1), "ccc");
  EXPECT_EQ(tabletIndex->stripeKey(2), "ddd");
  EXPECT_EQ(tabletIndex->stripeKey(3), "eee");

  const auto& cols = tabletIndex->indexColumns();
  EXPECT_EQ(cols.size(), 3);
  EXPECT_EQ(cols[0], "col1");
  EXPECT_EQ(cols[1], "col2");
  EXPECT_EQ(cols[2], "col3");

  auto metadata0 = tabletIndex->groupIndexMetadata(0);
  EXPECT_EQ(metadata0.offset(), 0);
  EXPECT_GT(metadata0.size(), 0);
  EXPECT_EQ(metadata0.compressionType(), CompressionType::Uncompressed);

  auto metadata1 = tabletIndex->groupIndexMetadata(1);
  EXPECT_EQ(metadata1.offset(), metadata0.size());
  EXPECT_GT(metadata1.size(), 0);
  EXPECT_EQ(metadata1.compressionType(), CompressionType::Uncompressed);

  auto metadata2 = tabletIndex->groupIndexMetadata(2);
  EXPECT_EQ(metadata2.offset(), metadata0.size() + metadata1.size());
  EXPECT_GT(metadata2.size(), 0);
  EXPECT_EQ(metadata2.compressionType(), CompressionType::Uncompressed);
  EXPECT_EQ(
      metadata2.offset() + metadata2.size(), indexBuffers.indexGroups.size());
}

TEST_F(TabletIndexTest, lookup) {
  std::vector<std::string> indexColumns = {"key_col"};
  std::string minKey = "aaa";
  std::vector<Stripe> stripes = {
      {.streams = {{.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 0,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}},
            .chunkKeys = {"ccc"}}},
      {.streams = {{.numChunks = 1, .chunkRows = {200}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 100,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {200}, .chunkOffsets = {0}},
            .chunkKeys = {"fff"}}},
      {.streams = {{.numChunks = 1, .chunkRows = {150}, .chunkOffsets = {0}}},
       .keyStream = {
           .streamOffset = 200,
           .streamSize = 100,
           .stream = {.numChunks = 1, .chunkRows = {150}, .chunkOffsets = {0}},
           .chunkKeys = {"iii"}}}};
  std::vector<int> stripeGroups = {2, 1};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto tabletIndex = createTabletIndex(indexBuffers);

  // Stripe keys: minKey="aaa", stripe0="ccc", stripe1="fff", stripe2="iii"
  struct {
    std::string lookupKey;
    std::optional<uint32_t> expectedStripeIndex;
  } testCases[] = {
      // Keys before minKey
      {"000", std::nullopt},
      {"aa", std::nullopt},
      // Keys in stripe 0 range [aaa, ccc]
      {"aaa", 0},
      {"bbb", 0},
      {"ccc", 0},
      // Keys in stripe 1 range (ccc, fff]
      {"ddd", 1},
      {"eee", 1},
      {"fff", 1},
      // Keys in stripe 2 range (fff, iii]
      {"ggg", 2},
      {"hhh", 2},
      {"iii", 2},
      // Keys after last stripe max key
      {"jjj", std::nullopt},
      {"zzz", std::nullopt}};

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.lookupKey);
    auto result = tabletIndex->lookup(testCase.lookupKey);
    if (testCase.expectedStripeIndex.has_value()) {
      ASSERT_TRUE(result.has_value());
      EXPECT_EQ(result->stripeIndex, testCase.expectedStripeIndex.value());
    } else {
      EXPECT_FALSE(result.has_value());
    }
  }
}

TEST_F(TabletIndexTest, lookupWithSameKeys) {
  std::vector<std::string> indexColumns = {"key_col"};
  std::string minKey = "aaa";
  std::vector<Stripe> stripes = {
      {.streams = {{.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 0,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {100}, .chunkOffsets = {0}},
            .chunkKeys = {"aaa"}}},
      {.streams = {{.numChunks = 1, .chunkRows = {200}, .chunkOffsets = {0}}},
       .keyStream =
           {.streamOffset = 100,
            .streamSize = 100,
            .stream = {.numChunks = 1, .chunkRows = {200}, .chunkOffsets = {0}},
            .chunkKeys = {"aaa"}}},
      {.streams = {{.numChunks = 1, .chunkRows = {150}, .chunkOffsets = {0}}},
       .keyStream = {
           .streamOffset = 200,
           .streamSize = 100,
           .stream = {.numChunks = 1, .chunkRows = {150}, .chunkOffsets = {0}},
           .chunkKeys = {"aaa"}}}};
  std::vector<int> stripeGroups = {2, 1};

  auto indexBuffers =
      createTestTabletIndex(indexColumns, minKey, stripes, stripeGroups);
  auto tabletIndex = createTabletIndex(indexBuffers);

  // All stripe keys are the same: minKey="aaa", stripe0="aaa", stripe1="aaa",
  // stripe2="aaa"
  struct {
    std::string lookupKey;
    std::optional<uint32_t> expectedStripeIndex;
  } testCases[] = {
      // Keys before minKey
      {"000", std::nullopt},
      {"aa", std::nullopt},
      // Key at minKey (same as all stripe keys) returns last stripe
      {"aaa", 0},
      // Keys after the common key
      {"aab", std::nullopt},
      {"bbb", std::nullopt},
      {"zzz", std::nullopt}};

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.lookupKey);
    auto result = tabletIndex->lookup(testCase.lookupKey);
    if (testCase.expectedStripeIndex.has_value()) {
      ASSERT_TRUE(result.has_value());
      EXPECT_EQ(result->stripeIndex, testCase.expectedStripeIndex.value());
    } else {
      EXPECT_FALSE(result.has_value());
    }
  }
}
} // namespace
} // namespace facebook::nimble::index::test
