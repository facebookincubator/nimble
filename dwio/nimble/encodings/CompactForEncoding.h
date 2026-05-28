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

#include <algorithm>
#include <cstring>
#include <span>
#include <type_traits>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"
#include "velox/common/base/BitUtil.h"
#include "velox/dwio/common/DecoderUtil.h"

// CompactForEncoding (slot 17) stores 64-bit integer streams using the same
// core algorithm as FixedBitWidthEncoding (slot 3): subtract a min baseline,
// bitpack residuals via FixedBitArray. Two wire-format differences:
//
//   1. Baseline is varint-encoded (1-10 bytes) instead of fixed sizeof(T)
//      (always 8 bytes for int64). Modest: saves up to 7B when baseline is
//      small.
//   2. BitWidth is exact (not rounded to byte boundary). FixedBitWidth
//      rounds up to the next multiple of 8 via `(bw + 7) & ~7`
//      (e.g., 4→8, 9→16, 17→24). CompactFor keeps the exact value
//      (e.g., 4 stays 4), which can halve the bitpacked region.
//
//
// Int64-only because the varint baseline savings diminish for smaller types:
// int32 saves at most 3 bytes (and Pfor/SimdForBitpack dominate there),
// int8/int16 save nothing.
//
// Ported from AUS LONG_LIST_FOR_BITPACKED.

namespace facebook::nimble {

/// Data layout (after the standard Encoding prefix):
///   varint bytes : minimum baseline (varint-encoded uint64; 1..10 bytes)
///   1 byte       : bitWidth -- bits per bitpacked residual; 0..64
///   variable     : FixedBitArray::bufferSize(N, bitWidth) bytes of bitpacked
///                  residuals.  Omitted entirely when bitWidth == 0.
///                  bufferSize already includes the 7-byte tail slop so
///                  loadU64 can safely read 8 bytes from the final element
///                  offset.
template <typename T>
class CompactForEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
  static_assert(
      std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>,
      "CompactFor only supports 64-bit integer types.");

 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  /// Construct from wire data produced by encode().
  CompactForEncoding(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory,
      const Encoding::Options& options = {});

  /// Reset the read cursor to the beginning of the stream.
  void reset() final;

  /// Advance the read cursor by `rowCount` rows without materializing.
  void skip(uint32_t rowCount) final;

  /// Decode `rowCount` consecutive rows into `buffer`.
  void materialize(uint32_t rowCount, void* buffer) final;

  /// Visitor-based row-at-a-time decode with an optional fast bulk path.
  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  /// Bulk scan method for the readWithVisitorFast path.
  /// Reads multiple values at once and processes them through the visitor.
  template <bool kScatter, typename Visitor>
  void bulkScan(
      Visitor& visitor,
      vector_size_t currentRow,
      const vector_size_t* selectedRows,
      vector_size_t numSelected,
      const vector_size_t* scatterRows);

  /// Encode `values` into a self-contained wire-format blob.
  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {});

  /// Return a human-readable summary for diagnostics.
  std::string debugString(int offset) const final;

 private:
  // Minimum value subtracted from every element before bitpacking.
  uint64_t baseline_{0};

  // Number of bits per bitpacked residual (0..64).
  uint8_t bitWidth_{0};

  // Bitpacked residual region.  Default-constructed (null buffer) when
  // bitWidth_ == 0 because no residuals are stored.
  FixedBitArray fixedBitArray_{};

  // Read cursor -- absolute row position within the stream.
  uint32_t row_{0};
};

//
// End of public API. Implementations follow.
//

template <typename T>
CompactForEncoding<T>::CompactForEncoding(
    velox::memory::MemoryPool& pool,
    std::string_view data,
    std::function<void*(uint32_t)> /* stringBufferFactory */,
    const Encoding::Options& options)
    : TypedEncoding<T, physicalType>{pool, data, options} {
  const auto* pos = data.data() + this->dataOffset();
  baseline_ = varint::readVarint64(&pos);
  bitWidth_ = static_cast<uint8_t>(encoding::readChar(pos));
  NIMBLE_CHECK_LE(bitWidth_, 64, "CompactFor bit width must be in [0, 64].");

  if (bitWidth_ > 0) {
    const size_t bitpackedSize =
        FixedBitArray::bufferSize(this->rowCount_, bitWidth_);
    NIMBLE_CHECK_GE(
        static_cast<size_t>(data.end() - pos),
        bitpackedSize,
        "CompactFor bitpacked region underruns wire data.");
    fixedBitArray_ =
        FixedBitArray{{pos, static_cast<size_t>(data.end() - pos)}, bitWidth_};
  }
  reset();
}

template <typename T>
void CompactForEncoding<T>::reset() {
  row_ = 0;
}

template <typename T>
void CompactForEncoding<T>::skip(uint32_t rowCount) {
  row_ += rowCount;
}

template <typename T>
void CompactForEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  if (rowCount == 0) {
    return;
  }
  auto* output = static_cast<physicalType*>(buffer);
  if (bitWidth_ == 0) {
    std::fill(output, output + rowCount, static_cast<physicalType>(baseline_));
  } else {
    fixedBitArray_.bulkGet64WithBaseline(
        /*start=*/row_,
        /*length=*/rowCount,
        reinterpret_cast<uint64_t*>(output),
        /*baseline=*/baseline_);
  }
  row_ += rowCount;
}

template <typename T>
template <typename V>
void CompactForEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  using OutputType = detail::ValueType<typename V::DataType>;
  // CompactFor only supports int64/uint64.
  static_assert(
      sizeof(OutputType) == sizeof(physicalType),
      "CompactFor readWithVisitor: OutputType must be same size as physicalType (int64/uint64).");
  static_assert(
      std::is_integral_v<OutputType>,
      "CompactFor readWithVisitor: OutputType must be integral.");

  // Fast path: use bulkScan when the visitor supports ExtractToReader
  // and useFastPath() passes.
  if constexpr (std::is_same_v<
                    typename V::Extract,
                    velox::dwio::common::ExtractToReader>) {
    auto* nulls = visitor.reader().rawNullsInReadRange();
    if (velox::dwio::common::useFastPath(visitor, nulls)) {
      detail::readWithVisitorFast(*this, visitor, params, nulls);
      return;
    }
  }
  // Slow path: process one value at a time. Only reached for
  // non-ExtractToReader visitors or non-deterministic filters.
  detail::readWithVisitorSlow(
      visitor,
      params,
      [&](auto toSkip) { skip(toSkip); },
      [&]() {
        physicalType value;
        if (bitWidth_ == 0) {
          value = static_cast<physicalType>(baseline_);
        } else {
          value =
              static_cast<physicalType>(fixedBitArray_.get(row_) + baseline_);
        }
        ++row_;
        return value;
      });
}

template <typename T>
template <bool kScatter, typename V>
void CompactForEncoding<T>::bulkScan(
    V& visitor,
    vector_size_t currentRow,
    const vector_size_t* selectedRows,
    vector_size_t numSelected,
    const vector_size_t* scatterRows) {
  using OutputType = detail::ValueType<typename V::DataType>;
  static_assert(
      sizeof(OutputType) == sizeof(physicalType),
      "CompactFor bulkScan requires same-size output type.");

  if (numSelected == 0) {
    return;
  }

  const auto numRows = visitor.numRows() - visitor.rowIndex();

  // Offset between internal position (row_) and the external row number
  // (currentRow).  Handles cases where the encoding cursor differs from the
  // logical row number.
  const auto offset =
      static_cast<int32_t>(row_) - static_cast<int32_t>(currentRow);

  auto* values = detail::mutableValues<OutputType>(visitor, numRows);

  if constexpr (V::dense) {
    if (bitWidth_ == 0) {
      std::fill(
          values, values + numSelected, static_cast<OutputType>(baseline_));
    } else {
      fixedBitArray_.bulkGet64WithBaseline(
          selectedRows[0] + offset,
          numSelected,
          reinterpret_cast<uint64_t*>(values),
          baseline_);
    }
  } else {
    // Sparse case: read individual values at specified positions.
    if (bitWidth_ == 0) {
      for (vector_size_t i = 0; i < numSelected; ++i) {
        values[i] = static_cast<OutputType>(baseline_);
      }
    } else {
      for (vector_size_t i = 0; i < numSelected; ++i) {
        values[i] = static_cast<OutputType>(
            fixedBitArray_.get(selectedRows[i] + offset) + baseline_);
      }
    }
  }

  row_ += selectedRows[numSelected - 1] - currentRow + 1;

  detail::finalizeBulkScan<OutputType, kScatter>(
      visitor, values, numRows, selectedRows, numSelected, scatterRows);
}

template <typename T>
std::string_view CompactForEncoding<T>::encode(
    EncodingSelection<typename TypeTraits<T>::physicalType>& selection,
    std::span<const typename TypeTraits<T>::physicalType> values,
    Buffer& buffer,
    const Encoding::Options& options) {
  static_assert(
      std::is_same_v<
          typename std::make_unsigned<physicalType>::type,
          physicalType>,
      "CompactFor physical type must be unsigned.");
  const bool useVarint = options.useVarintRowCount;
  if (values.empty()) {
    NIMBLE_INCOMPATIBLE_ENCODING(
        "CompactFor encoding cannot be used with 0 rows.");
  }
  const uint32_t rowCount = static_cast<uint32_t>(values.size());
  const uint64_t baseline = static_cast<uint64_t>(selection.statistics().min());
  const uint64_t maxValue = static_cast<uint64_t>(selection.statistics().max());
  // Unsigned subtraction is safe: Statistics computes min/max on the same
  // physical (unsigned 64-bit) representation, so the difference is the true
  // non-negative range.
  const uint64_t fullRange = maxValue - baseline;
  const uint8_t bitWidth = fullRange == 0
      ? uint8_t{0}
      : static_cast<uint8_t>(velox::bits::bitsRequired(fullRange));
  const uint64_t bitpackedSize =
      bitWidth == 0 ? 0 : FixedBitArray::bufferSize(rowCount, bitWidth);

  const uint32_t headerSize = varint::varintSize(baseline) + 1;
  const uint32_t encodingSize =
      Encoding::serializePrefixSize(rowCount, useVarint) + headerSize +
      static_cast<uint32_t>(bitpackedSize);

  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::CompactFor,
      TypeTraits<T>::dataType,
      rowCount,
      useVarint,
      pos);
  varint::writeVarint(baseline, &pos);
  encoding::writeChar(static_cast<char>(bitWidth), pos);

  if (bitWidth > 0) {
    char* bitpackedStart = pos;
    std::memset(bitpackedStart, 0, bitpackedSize);
    FixedBitArray fba(bitpackedStart, bitWidth);
    if (bitWidth <= 32) {
      // When the full range fits in 32 bits, narrow residuals to uint32
      // and use the template-unrolled bulkSet32 path.
      Vector<uint32_t> residuals32{&buffer.getMemoryPool()};
      residuals32.resize(rowCount);
      for (auto i = 0; i < rowCount; ++i) {
        residuals32[i] =
            static_cast<uint32_t>(static_cast<uint64_t>(values[i]) - baseline);
      }
      fba.bulkSet32(/*start=*/0, /*length=*/rowCount, residuals32.data());
    } else {
      for (auto i = 0; i < rowCount; ++i) {
        fba.set(i, static_cast<uint64_t>(values[i]) - baseline);
      }
    }
    pos += bitpackedSize;
  }

  NIMBLE_DCHECK_EQ(
      pos - reserved, encodingSize, "CompactFor encoding size mismatch.");
  return {reserved, encodingSize};
}

template <typename T>
std::string CompactForEncoding<T>::debugString(int offset) const {
  return fmt::format(
      "{}{}<{}> rowCount={} bitWidth={}",
      std::string(offset, ' '),
      toString(Encoding::encodingType()),
      toString(Encoding::dataType()),
      Encoding::rowCount(),
      static_cast<int>(bitWidth_));
}

} // namespace facebook::nimble
