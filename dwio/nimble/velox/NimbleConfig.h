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

#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"
#include "velox/common/config/Config.h"

namespace facebook::nimble {

// Default cap on the number of distinct flat-map keys per file; 0 means
// unlimited. Shared default for Config::MAP_FLAT_MAX_KEYS,
// VeloxWriterOptions::maxFlatMapKeys, and FieldWriterContext::maxFlatMapKeys_.
inline constexpr uint32_t kDefaultMaxFlatMapKeys = 200000;

class Config : public velox::config::ConfigBase {
 public:
  template <typename T>
  using Entry = velox::config::ConfigBase::Entry<T>;

  static Entry<bool> FLATTEN_MAP;
  static Entry<const std::vector<uint32_t>> MAP_FLAT_COLS;
  // @lint-ignore CLANGTIDY facebook-hte-NonPodStaticDeclaration
  static Entry<uint32_t> MAP_FLAT_MAX_KEYS;
  static Entry<const std::vector<uint32_t>> BATCH_REUSE_COLS;
  static Entry<const std::vector<uint32_t>> DEDUPLICATED_COLS;
  static Entry<uint64_t> RAW_STRIPE_SIZE;
  static Entry<bool> ENABLE_DEFAULT_ENCODING_EXECUTOR;
  static Entry<bool> ENABLE_STREAM_DEDUPLICATION;
  static Entry<bool> DISABLE_SHARED_STRING_BUFFERS;
  static Entry<const std::vector<std::pair<EncodingType, float>>>
      MANUAL_ENCODING_SELECTION_READ_FACTORS;
  static Entry<float> ENCODING_SELECTION_COMPRESSION_ACCEPT_RATIO;
  static Entry<const std::vector<std::pair<EncodingType, float>>>
      COMPRESSION_ACCEPT_RATIO_OVERRIDES;
  static Entry<uint64_t> ZSTD_COMPRESSION_MIN_SIZE;
  static Entry<uint64_t> ZSTRONG_COMPRESSION_MIN_SIZE;
  static Entry<uint32_t> ZSTRONG_COMPRESSION_LEVEL;
  static Entry<uint32_t> ZSTRONG_DECOMPRESSION_LEVEL;
  static Entry<bool> ENABLE_ZSTRONG_VARIABLE_BITWIDTH_COMPRESSOR;
  static Entry<bool> USE_MANAGED_COMPRESSION;
  static Entry<MetaInternalCompressionKey> MANAGED_COMPRESSION_KEY;
  static Entry<const std::map<uint64_t, float>>
      INPUT_BUFFER_DEFAULT_GROWTH_CONFIGS;
  static Entry<const std::map<uint64_t, float>> STRING_BUFFER_GROWTH_CONFIGS;
  static Entry<bool> ENABLE_CHUNKING;
  // EXPERIMENTAL: Not production-ready. Do not enable for production tables
  // without consulting the Nimble team (oncall: dwios).
  static Entry<bool> ENABLE_CHUNK_INDEX;
  static Entry<uint64_t> CHUNKING_WRITER_MEMORY_HIGH_THRESHOLD;
  static Entry<uint64_t> CHUNKING_WRITER_MEMORY_LOW_THRESHOLD;
  static Entry<uint64_t> CHUNKING_WRITER_TARGET_STRIPE_STORAGE_SIZE;
  static Entry<double> CHUNKING_WRITER_ESTIMATED_COMPRESSION_FACTOR;
  static Entry<uint64_t> CHUNKING_WRITER_MIN_CHUNK_SIZE;
  static Entry<uint64_t> CHUNKING_WRITER_MAX_CHUNK_SIZE;
  static Entry<uint64_t> CHUNKING_WRITER_WIDE_SCHEMA_MAX_CHUNK_SIZE;

  /// Selects and tunes the writer flush policy via a comma-separated
  /// "key:value" spec whose "type" key chooses the policy. An absent key keeps
  /// the standard size-based behavior. Test-only types: "type:test_per_batch"
  /// cuts a chunk every batch; "type:test_random" randomizes chunk/stripe
  /// boundaries. The remaining keys tune the test types: seed,
  /// flush_chunk_probability, flush_stripe_probability,
  /// max_stripe_physical_size (bytes; accepts a KB/MB/GB suffix),
  /// estimated_compression_factor; unset keys keep the TestFlushPolicy::Options
  /// defaults. This only selects the flush policy; enable and size chunking
  /// separately via the CHUNKING_WRITER_* configs. Not for production use. E.g.
  /// "type:test_random,flush_chunk_probability:0.1,seed:42".
  // @lint-ignore CLANGTIDY facebook-hte-NonPodStaticDeclaration
  static Entry<std::string> FLUSH_POLICY_CONFIG;

  // EXPERIMENTAL: Cluster index is not production-ready. Do not enable for
  // production tables without consulting the Nimble team (oncall: dwios).

  /// Index columns to build cluster index during writing.
  /// Column names are separated by commas.
  static Entry<const std::vector<std::string>> INDEX_COLUMNS;

  /// Sort orders for each index column (one per column).
  /// Format: "ASC_NULLS_FIRST,DESC_NULLS_LAST,..."
  /// Valid values: ASC_NULLS_FIRST, ASC_NULLS_LAST, DESC_NULLS_FIRST,
  /// DESC_NULLS_LAST. If empty, defaults to ASC_NULLS_FIRST for all columns.
  static Entry<const std::vector<std::string>> INDEX_SORT_ORDERS;

  /// Whether to enforce that encoded keys are in strictly ascending order.
  static Entry<bool> INDEX_ENFORCE_KEY_ORDER;

  /// Whether to enforce strictly ascending order with no duplicate keys.
  /// When true, both enforceKeyOrder and noDuplicateKey are set in IndexConfig.
  static Entry<bool> INDEX_NO_DUPLICATE_KEY;

  /// Encoding type for the index key stream.
  /// Valid values: "prefix" (default), "trivial".
  static Entry<std::string> INDEX_ENCODING_TYPE;

  /// Maximum rows per key chunk within a cluster index partition. Controls
  /// search granularity and encoding batch size. 0 means no splitting (one
  /// chunk per partition), which can hang on large tables. Default: 10000.
  static Entry<uint64_t> INDEX_MAX_ROWS_PER_KEY_CHUNK;

  // EXPERIMENTAL: BlockBitPacking encoding is not production-ready. Do not
  // enable for production tables without consulting the Nimble team
  // (oncall: dwios).
  static Entry<uint16_t> BLOCK_BIT_PACKING_BLOCK_SIZE;

  /// Enable column statistics collection during writing.
  static Entry<bool> ENABLE_STATS_COLLECTION;

  /// Enable vectorized column statistics for row size estimation.
  static Entry<bool> ENABLE_VECTORIZED_STATS;

  static constexpr const char* kNimbleWriteTargetRawStripeSize =
      "nimble_write_target_raw_stripe_size";

  static std::shared_ptr<Config> fromMap(
      const std::map<std::string, std::string>& map) {
    auto config = std::make_shared<Config>();
    for (const auto& pair : map) {
      config->set(pair.first, pair.second);
    }
    return config;
  }

  Config() : velox::config::ConfigBase({}, true) {}

  std::map<std::string, std::string> toSerdeParams() {
    return std::map{configs_.cbegin(), configs_.cend()};
  }
};

} // namespace facebook::nimble
