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
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>
#include <folly/Range.h>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/velox/RowRange.h"
#include "velox/serializers/KeyEncoder.h"

namespace facebook::nimble::index {

/// Type of index.
enum class IndexType {
  /// Range-based index on sorted data. Supports range scans and point lookups
  /// using key boundaries per stripe/chunk.
  Cluster,
  /// Hash-based index for point lookups on unsorted data. Maps composite key
  /// columns to exact row numbers.
  Dense,
};

std::string toString(IndexType indexType);
std::ostream& operator<<(std::ostream& out, IndexType indexType);

/// Unified index interface for both cluster and dense indices.
///
/// This provides a common abstraction for index metadata and lookup
/// capabilities. Both ClusterIndex and DenseIndex implement this interface,
/// enabling the reader to select the best index for a given query without
/// knowing the concrete type.
///
/// Two lookup modes:
/// - Point lookup: find rows matching exact keys. Works on both index types.
/// - Range scan: find rows in [lower, upper) ranges with exclusive upper
///   bound. Only supported on cluster index.
class IndexLookup {
 public:
  /// Batch lookup request. Contains encoded keys or key bounds and options.
  class LookupRequest {
   public:
    /// Lookup mode determines how key bounds are interpreted.
    enum class Mode {
      /// Point lookup: find rows where key == encodedKey.
      /// Each key bound has lowerKey == upperKey == the lookup key.
      PointLookup,
      /// Range scan: find rows where key in [lower, upper).
      /// Upper bound is exclusive.
      RangeScan,
    };

    /// Options for controlling lookup behavior.
    struct Options {
      /// Limit results to this file-level row range. Rows outside this range
      /// are excluded from the result. When set, the index only searches
      /// within the specified range. When not set, the entire file is searched.
      std::optional<RowRange> rowRange;
    };

    /// Creates a point lookup request for exact key matching.
    static LookupRequest pointLookup(
        std::vector<std::string> encodedKeys,
        Options options = {}) {
      return LookupRequest(std::move(encodedKeys), options);
    }

    /// Creates a range scan request with exclusive upper bounds.
    static LookupRequest rangeScan(
        std::vector<velox::serializer::EncodedKeyBounds> keyBounds,
        Options options = {}) {
      return LookupRequest(std::move(keyBounds), options);
    }

    Mode mode() const {
      return mode_;
    }

    size_t size() const {
      return mode_ == Mode::PointLookup ? pointKeys_.size()
                                        : rangeBounds_.size();
    }

    /// Returns the encoded key at the given index (point lookup only).
    std::string_view pointKey(uint32_t idx) const {
      NIMBLE_CHECK_EQ(mode_, Mode::PointLookup);
      NIMBLE_CHECK_LT(idx, pointKeys_.size());
      return pointKeys_[idx];
    }

    /// Returns the key bounds at the given index (range scan only).
    const velox::serializer::EncodedKeyBounds& rangeBound(uint32_t idx) const {
      NIMBLE_CHECK_EQ(mode_, Mode::RangeScan);
      NIMBLE_CHECK_LT(idx, rangeBounds_.size());
      return rangeBounds_[idx];
    }

    const Options& options() const {
      return options_;
    }

   private:
    // Point lookup constructor.
    LookupRequest(std::vector<std::string> keys, Options options)
        : mode_{Mode::PointLookup},
          pointKeys_{std::move(keys)},
          options_{options} {
      NIMBLE_CHECK(!pointKeys_.empty());
    }

    // Range scan constructor.
    LookupRequest(
        std::vector<velox::serializer::EncodedKeyBounds> keyBounds,
        Options options)
        : mode_{Mode::RangeScan},
          rangeBounds_{std::move(keyBounds)},
          options_{options} {
      NIMBLE_CHECK(!rangeBounds_.empty());
    }

    const Mode mode_;
    // Point lookup: single encoded keys.
    const std::vector<std::string> pointKeys_;
    // Range scan: key bounds with lower/upper.
    const std::vector<velox::serializer::EncodedKeyBounds> rangeBounds_;
    const Options options_;
  };

  using LookupOptions = LookupRequest::Options;

  /// Batch lookup result. Stores a flat array of RowRanges with
  /// per-key offsets for O(1) access via operator[].
  class LookupResult {
   public:
    LookupResult(
        std::vector<RowRange> rowRanges,
        std::vector<uint32_t> resultOffsets)
        : rowRanges_{std::move(rowRanges)},
          resultOffsets_{std::move(resultOffsets)} {
      NIMBLE_CHECK_GT(resultOffsets_.size(), 1);
      NIMBLE_CHECK_GE(
          rowRanges_.size(),
          resultOffsets_.back(),
          "Result offsets reference beyond rowRanges");
    }

    size_t size() const {
      return resultOffsets_.size() - 1;
    }

    folly::Range<const RowRange*> operator[](uint32_t idx) const {
      NIMBLE_DCHECK_LT(idx, size());
      NIMBLE_DCHECK_LE(resultOffsets_[idx], resultOffsets_[idx + 1]);
      NIMBLE_DCHECK_LE(resultOffsets_[idx + 1], rowRanges_.size());
      return {
          rowRanges_.data() + resultOffsets_[idx],
          rowRanges_.data() + resultOffsets_[idx + 1]};
    }

   private:
    const std::vector<RowRange> rowRanges_;
    const std::vector<uint32_t> resultOffsets_;
  };

  virtual ~IndexLookup() = default;

  /// Returns the type of this index.
  virtual IndexType indexType() const = 0;

  /// Returns the column names this index covers, in index definition order.
  virtual const std::vector<std::string>& indexColumns() const = 0;

  /// Batch lookup of row locations matching the encoded key bounds.
  ///
  /// For cluster index: supports both point lookups and range lookups.
  ///   Returns file-level RowRanges spanning matching rows.
  /// For dense index: only supports point lookups. Returns file-level
  ///   RowRanges for exact single rows (may return multiple per key).
  ///
  /// Results are indexed by input key position: result[i] returns the
  /// RowRanges for request.keyBounds()[i].
  virtual LookupResult lookup(const LookupRequest& request) const = 0;
};

inline std::string toString(IndexLookup::LookupRequest::Mode mode) {
  switch (mode) {
    case IndexLookup::LookupRequest::Mode::PointLookup:
      return "PointLookup";
    case IndexLookup::LookupRequest::Mode::RangeScan:
      return "RangeScan";
    default:
      NIMBLE_UNREACHABLE("Unknown Mode: {}", static_cast<int>(mode));
  }
}

inline std::ostream& operator<<(
    std::ostream& out,
    IndexLookup::LookupRequest::Mode mode) {
  return out << toString(mode);
}

} // namespace facebook::nimble::index

template <>
struct fmt::formatter<facebook::nimble::index::IndexType>
    : formatter<std::string> {
  auto format(facebook::nimble::index::IndexType s, format_context& ctx) const {
    return formatter<std::string>::format(
        facebook::nimble::index::toString(s), ctx);
  }
};

template <>
struct fmt::formatter<facebook::nimble::index::IndexLookup::LookupRequest::Mode>
    : formatter<std::string> {
  auto format(
      facebook::nimble::index::IndexLookup::LookupRequest::Mode mode,
      format_context& ctx) const {
    return formatter<std::string>::format(
        facebook::nimble::index::toString(mode), ctx);
  }
};
