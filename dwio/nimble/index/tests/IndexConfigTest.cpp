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
#include "dwio/nimble/index/IndexSerialization.h"
#include "dwio/nimble/tablet/IndexGenerated.h"

namespace facebook::nimble::index::test {

TEST(IndexConfigTest, defaultValues) {
  ClusterIndexConfig config;
  EXPECT_TRUE(config.columns.empty());
  EXPECT_TRUE(config.sortOrders.empty());
  EXPECT_FALSE(config.enforceKeyOrder);
  EXPECT_FALSE(config.noDuplicateKey);
  EXPECT_EQ(config.encodingLayout.encodingType(), EncodingType::Prefix);
  EXPECT_EQ(
      config.encodingLayout.compressionType(), CompressionType::Uncompressed);
}

TEST(IndexConfigTest, customValues) {
  ClusterIndexConfig config{
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

TEST(IndexConfigTest, customEncodingLayout) {
  ClusterIndexConfig config;
  config.encodingLayout =
      EncodingLayout{EncodingType::Trivial, {}, CompressionType::Zstd};
  EXPECT_EQ(config.encodingLayout.encodingType(), EncodingType::Trivial);
  EXPECT_EQ(config.encodingLayout.compressionType(), CompressionType::Zstd);
}

TEST(IndexConfigTest, indexFamilySerializationRoundTrip) {
  for (const auto [family, serializedFamily] :
       {std::pair{IndexFamily::Cluster, serialization::IndexFamily_Cluster},
        std::pair{IndexFamily::Dense, serialization::IndexFamily_Dense}}) {
    SCOPED_TRACE(static_cast<int>(family));
    EXPECT_EQ(toIndexFamily(family), serializedFamily);
    EXPECT_EQ(toIndexFamily(serializedFamily), family);
  }
}

} // namespace facebook::nimble::index::test
