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

#include "dwio/nimble/index/IndexConfig.h"

namespace facebook::nimble::index::test {

TEST(IndexConfigTest, defaultValues) {
  IndexConfig config;
  EXPECT_TRUE(config.columns.empty());
  EXPECT_TRUE(config.sortOrders.empty());
  EXPECT_FALSE(config.enforceKeyOrder);
  EXPECT_FALSE(config.noDuplicateKey);
  EXPECT_EQ(config.encodingLayout.encodingType(), EncodingType::Prefix);
  EXPECT_EQ(
      config.encodingLayout.compressionType(), CompressionType::Uncompressed);
  EXPECT_EQ(config.minChunkRawSize, 512 << 10);
  EXPECT_EQ(config.maxChunkRawSize, 20 << 20);
}

TEST(IndexConfigTest, customValues) {
  IndexConfig config{
      .columns = {"col1", "col2"},
      .sortOrders = {{.ascending = true}, {.ascending = false}},
      .enforceKeyOrder = true,
      .noDuplicateKey = true,
  };
  EXPECT_EQ(config.columns.size(), 2);
  EXPECT_EQ(config.columns[0], "col1");
  EXPECT_EQ(config.columns[1], "col2");
  EXPECT_EQ(config.sortOrders.size(), 2);
  EXPECT_TRUE(config.sortOrders[0].ascending);
  EXPECT_FALSE(config.sortOrders[1].ascending);
  EXPECT_TRUE(config.enforceKeyOrder);
  EXPECT_TRUE(config.noDuplicateKey);
}

TEST(IndexConfigTest, chunkSizeThresholds) {
  IndexConfig config;
  config.minChunkRawSize = 1024;
  config.maxChunkRawSize = 4096;
  EXPECT_EQ(config.minChunkRawSize, 1024);
  EXPECT_EQ(config.maxChunkRawSize, 4096);
}

TEST(IndexConfigTest, customEncodingLayout) {
  IndexConfig config;
  config.encodingLayout =
      EncodingLayout{EncodingType::Trivial, {}, CompressionType::Zstd};
  EXPECT_EQ(config.encodingLayout.encodingType(), EncodingType::Trivial);
  EXPECT_EQ(config.encodingLayout.compressionType(), CompressionType::Zstd);
}

} // namespace facebook::nimble::index::test
