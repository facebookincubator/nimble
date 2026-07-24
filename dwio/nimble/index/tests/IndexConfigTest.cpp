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

#include "dwio/nimble/common/NimbleException.h"
#include "dwio/nimble/encodings/PrefixEncoding.h"
#include "dwio/nimble/index/IndexConfig.h"
#include "dwio/nimble/index/IndexSerialization.h"
#include "dwio/nimble/tablet/IndexGenerated.h"
#include "velox/common/config/Config.h"

namespace facebook::nimble::index::test {
namespace {

void expectEncodingLayoutEqual(
    const EncodingLayout& expected,
    const EncodingLayout& actual) {
  EXPECT_EQ(actual.encodingType(), expected.encodingType());
  EXPECT_EQ(actual.compressionType(), expected.compressionType());
  EXPECT_EQ(actual.config().values(), expected.config().values());
  ASSERT_EQ(actual.childrenCount(), expected.childrenCount());
  for (NestedEncodingIdentifier i = 0; i < expected.childrenCount(); ++i) {
    const auto& expectedChild = expected.child(i);
    const auto& actualChild = actual.child(i);
    ASSERT_EQ(actualChild.has_value(), expectedChild.has_value());
    if (expectedChild.has_value()) {
      expectEncodingLayoutEqual(expectedChild.value(), actualChild.value());
    }
  }
}

} // namespace

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

TEST(IndexConfigTest, clusterConfigRoundTrip) {
  ClusterIndexConfig expected{
      .indexName = "test.cluster.v1",
      .columns = {"key", "timestamp"},
      .sortOrders = {{.ascending = true}, {.ascending = false}},
      .enforceKeyOrder = true,
      .noDuplicateKey = true,
      .encodingLayout =
          EncodingLayout{
              EncodingType::Prefix,
              EncodingLayout::Config{{
                  {std::string(PrefixEncoding::kRestartIntervalConfigKey),
                   "32"},
              }},
              CompressionType::Zstd},
      .maxRowsPerKeyChunk = 123,
      .keyChunkCompressionType = CompressionType::Lz4,
  };

  const auto actual = toClusterIndexConfig(toIndexConfig(expected));
  EXPECT_EQ(actual.indexName, expected.indexName);
  EXPECT_EQ(actual.columns, expected.columns);
  ASSERT_EQ(actual.sortOrders.size(), expected.sortOrders.size());
  EXPECT_TRUE(actual.sortOrders[0].ascending);
  EXPECT_FALSE(actual.sortOrders[1].ascending);
  EXPECT_EQ(actual.enforceKeyOrder, expected.enforceKeyOrder);
  EXPECT_EQ(actual.noDuplicateKey, expected.noDuplicateKey);
  expectEncodingLayoutEqual(expected.encodingLayout, actual.encodingLayout);
  EXPECT_EQ(actual.maxRowsPerKeyChunk, expected.maxRowsPerKeyChunk);
  EXPECT_EQ(actual.keyChunkCompressionType, expected.keyChunkCompressionType);
}

TEST(IndexConfigTest, hashConfigRoundTrip) {
  HashIndexConfig expected{
      .columns = {"key"},
      .loadFactor = 0.5,
      .bloomFilter = BloomFilterConfig{.bitsPerKey = 7},
      .maxPartitionSizeBytes = 123,
  };

  const auto actual = toHashIndexConfig(toIndexConfig(expected));
  EXPECT_EQ(actual.indexName, expected.indexName);
  EXPECT_EQ(actual.columns, expected.columns);
  EXPECT_EQ(actual.loadFactor, expected.loadFactor);
  ASSERT_TRUE(actual.bloomFilter.has_value());
  EXPECT_EQ(actual.bloomFilter->bitsPerKey, expected.bloomFilter->bitsPerKey);
  EXPECT_EQ(actual.maxPartitionSizeBytes, expected.maxPartitionSizeBytes);
}

TEST(IndexConfigTest, sortedConfigRoundTrip) {
  SortedIndexConfig expected{
      .columns = {"key"},
      .maxRowsPerKeyChunk = 123,
  };

  const auto actual = toSortedIndexConfig(toIndexConfig(expected));
  EXPECT_EQ(actual.indexName, expected.indexName);
  EXPECT_EQ(actual.columns, expected.columns);
  expectEncodingLayoutEqual(expected.encodingLayout, actual.encodingLayout);
  EXPECT_EQ(actual.maxRowsPerKeyChunk, expected.maxRowsPerKeyChunk);
}

TEST(IndexConfigTest, encodingLayoutRoundTrip) {
  struct TestCase {
    std::string_view name;
    EncodingLayout layout;
  };
  const std::vector<TestCase> testCases{
      {
          "prefix_config",
          EncodingLayout{
              EncodingType::Prefix,
              EncodingLayout::Config{
                  {{std::string(PrefixEncoding::kRestartIntervalConfigKey),
                    "32"}}},
              CompressionType::Zstd},
      },
      {
          "nested_trivial",
          EncodingLayout{
              EncodingType::Trivial,
              {},
              CompressionType::Zstd,
              {EncodingLayout{
                  EncodingType::Trivial,
                  EncodingLayout::Config{{{"child.key", "value"}}},
                  CompressionType::Lz4,
                  {std::nullopt}}}},
      },
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);
    const auto actual = toSortedIndexConfig(toIndexConfig(
        SortedIndexConfig{
            .columns = {"key"}, .encodingLayout = testCase.layout}));
    expectEncodingLayoutEqual(testCase.layout, actual.encodingLayout);
  }
}

TEST(IndexConfigTest, builtInDefaults) {
  const auto cluster = toClusterIndexConfig(
      IndexConfig{
          .family = IndexFamily::Cluster,
          .name = std::string{kClusterIndexName},
          .options = std::make_shared<const velox::config::ConfigBase>(
              std::unordered_map<std::string, std::string>{}),
      });
  EXPECT_EQ(cluster.maxRowsPerKeyChunk, 10'000);
  EXPECT_EQ(cluster.encodingLayout.encodingType(), EncodingType::Prefix);

  const auto sorted = toSortedIndexConfig(
      IndexConfig{
          .family = IndexFamily::Dense,
          .name = std::string{kDenseSortedIndexName},
          .options = std::make_shared<const velox::config::ConfigBase>(
              std::unordered_map<std::string, std::string>{}),
      });
  EXPECT_EQ(sorted.maxRowsPerKeyChunk, 0);
}

TEST(IndexConfigTest, malformedJsonIsUserError) {
  auto makeConfig = [](std::string key) {
    return IndexConfig{
        .family = IndexFamily::Cluster,
        .name = std::string{kClusterIndexName},
        .options = std::make_shared<const velox::config::ConfigBase>(
            std::unordered_map<std::string, std::string>{
                {std::move(key), "{"}}),
    };
  };

  for (const auto* key : {"columns", "encodingLayout"}) {
    SCOPED_TRACE(key);
    EXPECT_THROW(toClusterIndexConfig(makeConfig(key)), NimbleUserError);
  }
}

TEST(IndexConfigTest, keyChunkCompressionRoundTrip) {
  ClusterIndexConfig config{
      .keyChunkCompressionType = CompressionType::MetaInternal};
  EXPECT_EQ(
      toClusterIndexConfig(toIndexConfig(std::move(config)))
          .keyChunkCompressionType,
      CompressionType::MetaInternal);
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
