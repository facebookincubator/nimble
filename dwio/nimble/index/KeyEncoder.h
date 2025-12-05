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

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Vector.h"
#include "velox/common/memory/Scratch.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::nimble {

// Use IndexBound and IndexBounds from ScanSpec
using IndexBound = velox::common::ScanSpec::IndexBound;
using IndexBounds = velox::common::ScanSpec::IndexBounds;

/// Encoded representation of index bounds as byte-comparable strings.
/// These keys can be compared lexicographically to perform range filtering.
struct EncodedKeyBounds {
  /// Encoded lower bound key. If present, represents the minimum key value
  /// (inclusive or exclusive based on IndexBound.inclusive).
  std::optional<std::string> lowerKey;
  /// Encoded upper bound key. If present, represents the maximum key value
  /// (inclusive or exclusive based on IndexBound.inclusive).
  std::optional<std::string> upperKey;
};

/// KeyEncoder encodes multi-column keys into byte-comparable strings that
/// preserve the sort order defined by the column types and sort orders.
/// This enables efficient range-based filtering and comparison of composite
/// keys.
///
/// The encoding is designed such that:
/// - Lexicographic comparison of encoded keys matches the logical comparison
///   of the original values
/// - Null values are sorted according to the specified sort order (nulls
///   first/last)
/// - Ascending/descending sort orders are respected
/// - Only scalar types are supported (e.g., integers, floats, strings,
///   timestamps, booleans). Complex types (arrays, maps, rows) are not
///   supported.
///
/// Example usage:
///   auto keyEncoder = KeyEncoder::create(
///       {"col1", "col2"}, rowType, sortOrders, pool);
///   Vector<std::string_view> encodedKeys(pool);
///   Buffer buffer(pool);
///   keyEncoder->encode(inputVector, encodedKeys, buffer);
class KeyEncoder {
 public:
  /// Factory method to create a KeyEncoder instance.
  ///
  /// @param keyColumns Names of columns to include in the encoded key, in order
  /// @param inputType Row type of the input data containing these columns
  /// @param sortOrders Sort order for each key column (ascending/descending,
  ///                   nulls first/last)
  /// @param pool Memory pool for allocations
  /// @return Unique pointer to a new KeyEncoder instance
  static std::unique_ptr<KeyEncoder> create(
      const std::vector<std::string>& keyColumns,
      const velox::RowTypePtr& inputType,
      const std::vector<velox::core::SortOrder>& sortOrders,
      velox::memory::MemoryPool* pool);

  /// Encodes the key columns from the input vector into byte-comparable keys.
  ///
  /// Each row in the input produces one encoded key string. The keys can be
  /// compared lexicographically, and the comparison result will match the
  /// logical comparison based on the specified sort orders.
  ///
  /// @param input Input vector containing rows to encode
  /// @param encodedKeys Output vector to store the encoded key strings (views
  ///                    into buffer)
  /// @param buffer Buffer to hold the actual encoded key data
  void encode(
      const velox::VectorPtr& input,
      Vector<std::string_view>& encodedKeys,
      Buffer& buffer);

  /// Encodes index bounds into byte-comparable boundary keys.
  ///
  /// For non-inclusive bounds, the key is adjusted (incremented for lower
  /// bound, decremented for upper bound) to convert to an inclusive range.
  ///
  /// @param indexBounds Index bounds containing lower/upper bounds with
  ///                    inclusive flags
  /// @return EncodedKeyBounds with encoded lower and/or upper keys
  EncodedKeyBounds encodeIndexBounds(const IndexBounds& indexBounds);

 private:
  KeyEncoder(
      const std::vector<std::string>& keyColumns,
      const velox::RowTypePtr& inputType,
      const std::vector<velox::core::SortOrder>& sortOrders,
      velox::memory::MemoryPool* pool);

  uint64_t estimateEncodedSize();

  // Encodes a single column for all rows in columnar fashion.
  void encodeColumn(
      const velox::DecodedVector& decodedVector,
      velox::vector_size_t numRows,
      bool nullLast,
      bool descending,
      std::vector<char*>& rowOffsets) const;

  // Encodes a RowVector and returns encoded keys as strings.
  // Each row in the input vector produces one encoded key string.
  std::vector<std::string> encode(const velox::RowVectorPtr& input);

  // Creates a new row vector with the key columns incremented by 1.
  // Similar to Apache Kudu's IncrementKey, this increments from the
  // rightmost (least significant) column. Returns std::nullopt if all columns
  // overflow (key is at maximum value).
  std::optional<velox::RowVectorPtr> createIncrementedBound(
      const velox::RowVectorPtr& bound) const;

  const velox::RowTypePtr inputType_;
  const std::vector<velox::core::SortOrder> sortOrders_;
  const std::vector<velox::vector_size_t> keyChannels_;
  const velox::RowTypePtr keyType_;
  velox::memory::MemoryPool* const pool_;

  // Reusable buffers.
  velox::DecodedVector decodedVector_;
  std::vector<velox::DecodedVector> childDecodedVectors_;
  std::vector<velox::vector_size_t> encodedSizes_;
  velox::Scratch scratch_;
};
} // namespace facebook::nimble
