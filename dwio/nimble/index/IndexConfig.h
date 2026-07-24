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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/common/EncodingLayout.h"
#include "dwio/nimble/index/SortOrder.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::nimble::index {

enum class IndexFamily : uint8_t {
  Cluster,
  Dense,
};

struct IndexDescriptor {
  IndexFamily family;
  std::string name;
  MetadataSection root;
};

inline constexpr std::string_view kClusterIndexName{"nimble.cluster.v1"};
inline constexpr std::string_view kDenseHashIndexName{"nimble.dense.hash.v1"};
inline constexpr std::string_view kDenseSortedIndexName{
    "nimble.dense.sorted.v1"};

/// Configuration for index generation.
/// The index allows efficient filtering and pruning of data based on
/// the specified index columns.
// EXPERIMENTAL: Cluster index is not production-ready. Do not enable for
// production tables without consulting the Nimble team (oncall: dwios).
struct ClusterIndexConfig {
  /// Named cluster index implementation.
  std::string indexName{std::string(kClusterIndexName)};
  /// Columns to be indexed for data pruning.
  /// These columns will be encoded using KeyWriter to generate index keys
  /// that enable efficient data skipping during reads.
  std::vector<std::string> columns;
  /// Specifies the sort order for each index column.
  /// If empty, defaults to ascending order for all columns.
  /// If not empty, must have the same size as 'columns'.
  std::vector<SortOrder> sortOrders;
  /// If true, enforces that encoded keys must be in ascending order.
  /// This ensures that stripe boundaries maintain sorted order for efficient
  /// range-based filtering. An exception is thrown if keys are found to be
  /// out of order. Duplicate keys are allowed unless noDuplicateKey is also
  /// set.
  bool enforceKeyOrder{false};
  /// If true, enforces that encoded keys must be in strictly ascending order
  /// with no duplicate keys allowed. If enforceKeyOrder is true, setting this
  /// option enforces the no duplicates check. An exception is thrown if keys
  /// are out of order or if duplicate keys are detected.
  bool noDuplicateKey{false};
  /// The encoding layout for the key stream.
  /// Only Prefix and Trivial encodings are supported.
  /// Users should pass a fully constructed EncodingLayout.
  /// For PrefixEncoding, use EncodingConfig with key "prefixRestartInterval"
  /// to control the restart interval (default: 16).
  /// For Trivial encoding, include a child EncodingLayout for the lengths
  /// stream.
  EncodingLayout encodingLayout{
      EncodingType::Prefix,
      {},
      CompressionType::Uncompressed};
  /// Maximum rows per chunk within a partition. Controls in-memory search
  /// granularity — chunks have start/end keys for binary search. Smaller values
  /// give finer-grained lookups at the cost of more metadata.
  uint64_t maxRowsPerKeyChunk{10'000};
  /// Chunk-level compression for the encoded key stream. Prefix encoding does
  /// not compress its own output, so this is the only way to block-compress a
  /// Prefix key index. Only Uncompressed, Zstd, and Lz4 are supported.
  CompressionType keyChunkCompressionType{CompressionType::Uncompressed};
};

/// Configuration for bloom filter.
/// Shared by hash index and sorted index.
struct BloomFilterConfig {
  /// Bits per key for the bloom filter.
  /// Higher values reduce false positive rate at the cost of more memory.
  /// 10 bits/key ≈ 1% FPR, 7 bits/key ≈ 3% FPR.
  float bitsPerKey{10.0f};
};

/// Configuration for hash index generation.
/// A hash index provides point lookups from composite key columns to row
/// numbers without requiring data to be sorted. Multiple hash indices can
/// coexist per file, each on a different set of columns.
// EXPERIMENTAL: Hash index is not production-ready. Do not enable for
// production tables without consulting the Nimble team (oncall: dwios).
struct HashIndexConfig {
  /// Named dense index implementation.
  std::string indexName{std::string(kDenseHashIndexName)};
  /// Columns forming the composite key for point lookups.
  std::vector<std::string> columns;

  /// Target load factor for the hash table. numBuckets is computed as
  /// nextPowerOfTwo(numKeys / loadFactor). Lower values reduce collisions
  /// at the cost of more buckets.
  float loadFactor{0.7f};

  /// Optional bloom filter for fast negative lookups.
  std::optional<BloomFilterConfig> bloomFilter;

  /// Maximum partition size in bytes for on-demand loading.
  /// When the serialized index data exceeds this threshold, buckets are
  /// split into independently loadable partitions. Each partition is stored
  /// as a separate section and loaded lazily during lookup.
  /// 0 means no partitioning (all data in a single section).
  uint64_t maxPartitionSizeBytes{0};
};

/// Configuration for sorted index generation.
/// A sorted index stores a sorted key stream with embedded row IDs for
/// point lookups and range scans on unsorted data. Unlike ClusterIndex
/// (which requires sorted data), this is a secondary index.
// EXPERIMENTAL: Sorted index is not production-ready. Do not enable for
// production tables without consulting the Nimble team (oncall: dwios).
struct SortedIndexConfig {
  /// Named dense index implementation.
  std::string indexName{std::string(kDenseSortedIndexName)};
  /// Columns forming the composite key.
  std::vector<std::string> columns;
  /// The encoding layout for the key stream.
  /// Only Prefix and Trivial encodings are supported.
  EncodingLayout encodingLayout{
      EncodingType::Prefix,
      {},
      CompressionType::Uncompressed};
  /// Maximum rows per chunk within a partition. Controls in-memory search
  /// granularity — chunks have boundary keys for binary search.
  /// 0 means no splitting (one chunk per partition).
  uint64_t maxRowsPerKeyChunk{0};
};

/// Configures one named index implementation.
struct IndexConfig {
  /// Selects the index factory registry.
  IndexFamily family;
  /// Names the registered index factory.
  std::string name;
  /// Provides implementation-specific configuration options.
  std::shared_ptr<const velox::config::ConfigBase> options;
};

/// Converts built-in cluster index options to the generic factory
/// configuration.
IndexConfig toIndexConfig(ClusterIndexConfig config);

/// Converts built-in hash index options to the generic factory configuration.
IndexConfig toIndexConfig(HashIndexConfig config);

/// Converts built-in sorted index options to the generic factory
/// configuration.
IndexConfig toIndexConfig(SortedIndexConfig config);

/// Parses generic options for the built-in cluster index implementation.
ClusterIndexConfig toClusterIndexConfig(const IndexConfig& config);

/// Parses generic options for the built-in hash index implementation.
HashIndexConfig toHashIndexConfig(const IndexConfig& config);

/// Parses generic options for the built-in sorted index implementation.
SortedIndexConfig toSortedIndexConfig(const IndexConfig& config);

} // namespace facebook::nimble::index
