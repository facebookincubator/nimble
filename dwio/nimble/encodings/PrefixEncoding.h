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

#include <span>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingIdentifier.h"
#include "dwio/nimble/encodings/EncodingSelection.h"
#include "velox/common/memory/Memory.h"

/// PrefixEncoding stores sorted string data with prefix compression. Common
/// prefixes are shared across consecutive entries to reduce storage space.
///
/// Based on prefix encoding algorithms from RocksDB, Apache Kudu, and Doris:
/// - Consecutive sorted entries typically share common prefixes
/// - Store prefix length + suffix for each entry instead of full strings
/// - Periodically store full restart points for seek operations
///
/// Binary layout:
/// - Encoding::kPrefixSize bytes: standard Encoding prefix
/// - 4 bytes: restart interval (number of entries between restart points)
/// - ZZ bytes: restart offsets array (uint32_t array of byte offsets,
///   size = ceil(rowCount / restartInterval))
/// - XX bytes: encoded entries (prefix_len | suffix_len | suffix_data)*
///
/// The restart offsets are placed at the head of the encoding block to
/// accelerate memory access patterns for seek operations, as prefix encoding
/// is primarily used for seeking in sorted data.
///
/// The number of restarts can be computed from rowCount and restartInterval:
///   numRestarts = (rowCount + restartInterval - 1) / restartInterval
///
/// Each entry stores:
/// - shared_prefix_len (uint32): bytes shared with previous entry
/// - suffix_len (uint32): length of unique suffix
/// - suffix_data: the suffix bytes
///
/// Restart points are full entries (shared_prefix_len = 0) stored at regular
/// intervals to enable efficient seek operations.

namespace facebook::nimble {

/// Encoding for sorted string data with prefix compression and seek support.
/// Only supports std::string_view data type.
class PrefixEncoding final
    : public TypedEncoding<std::string_view, std::string_view> {
 public:
  using cppDataType = std::string_view;
  using physicalType = std::string_view;

  static constexpr int kRestartIntervalOffset = Encoding::kPrefixSize;
  static constexpr int kRestartOffsetsOffset = kRestartIntervalOffset + 4;
  static constexpr uint32_t kDefaultRestartInterval = 16;

  PrefixEncoding(velox::memory::MemoryPool& pool, std::string_view data);

  void reset() final;
  void skip(uint32_t rowCount) final;

  /// Decodes and materializes string values into the output buffer.
  ///
  /// The output string_views remain valid until the next call to materialize(),
  /// reset(), or until the encoding is destroyed. The encoding maintains an
  /// internal buffer that holds the decoded string data.
  ///
  /// @param rowCount Number of rows to materialize.
  /// @param buffer Output buffer for std::string_view values.
  void materialize(uint32_t rowCount, void* buffer) final;

  /// Seeks to the position at or after the given value.
  ///
  /// @param value Pointer to target value to seek.
  /// @return Row index if found, std::nullopt if value is greater than all
  /// entries.
  std::optional<uint32_t> seekAtOrAfter(const void* value) final;

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  /// Encodes sorted string values with prefix compression.
  ///
  /// @param selection Encoding selection policy.
  /// @param values Values to encode. Must be sorted in ascending order.
  /// @param buffer Output buffer for encoded data.
  /// @return View of the encoded data.
  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer);

  std::string debugString(int offset) const final;

 private:
  // Static helper methods for initializing const members
  static uint32_t readRestartInterval(std::string_view data);
  static const char* restartOffsets(std::string_view data);
  static const char* dataStart(std::string_view data, uint32_t numRestarts);

  // Computes numRestarts from rowCount and restartInterval
  static uint32_t computeNumRestarts(
      uint32_t rowCount,
      uint32_t restartInterval);

  // Decodes entry at current position and returns the full string.
  // Stores result in decodedValue_ buffer.
  //
  // NOTE: The returned string_view is invalidated on the next decodeEntry call
  // as decodedValue_ buffer is overwritten.
  std::string_view decodeEntry();

  // Seeks to the restart point at the given index.
  void seekToRestartPoint(uint32_t restartIndex);

  // Gets the offset for the restart point at the given index.
  uint32_t restartOffset(uint32_t restartIndex) const;

  // Restart interval - number of entries between restart points
  const uint32_t restartInterval_;
  // Number of restart points (computed from rowCount and restartInterval)
  const uint32_t numRestarts_;
  // Offset to start of restart offsets array
  const char* const restartOffsets_;
  // Offset to start of data section
  const char* const dataStart_;

  // Current read position in the data
  const char* currentPos_{nullptr};
  // Current row index being read
  uint32_t currentRow_{0};
  // Buffer to store decoded value. The shared prefix from the previous entry
  // is already at the beginning of this buffer, so we only need to append the
  // suffix for each new entry.
  Vector<char> decodedValue_;
  // Buffer to hold all materialized string data for the current batch.
  // String views returned by materialize() point into this buffer and
  // remain valid until the next materialize() call.
  Vector<char> materializedValues_;
};

/// Template implementation (remain in header)
///
/// TODO: tune this later if needs for regular data column storage.
template <typename V>
void PrefixEncoding::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  detail::readWithVisitorSlow(
      visitor,
      params,
      [&](auto toSkip) { this->skip(toSkip); },
      [&] { return decodeEntry(); });
}

} // namespace facebook::nimble
