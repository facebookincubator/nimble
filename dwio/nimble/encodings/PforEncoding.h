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

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"
#include "velox/common/base/BitUtil.h"

// PforEncoding stores integer data using Patched Frame-of-Reference (slot 15).
// Each value is decomposed as `value = baseline + residual`, where the
// residuals are bitpacked at a base bit width chosen so that ~90% of the
// residuals fit. Values whose residual overflows the base width are recorded
// as (position, value) "exceptions" stored separately as varints.
//
// Decode uses a branchless two-pass strategy (ported from AusIntListPfor):
//   Pass 1: Unpack all base residuals in a tight branchless loop.
//   Pass 2: Patch the ~10% exception positions with their full values.
// This eliminates per-element branches from the hot loop.
//
// Pfor wins over plain FixedBitWidth on dense numeric streams that contain a
// small fraction of large outliers -- FixedBitWidth would have to widen every
// slot to the worst case, while Pfor pays the wide cost only for the outliers.

namespace facebook::nimble {

/// Data layout (after the standard Encoding prefix):
///
///   sizeof(physicalType) bytes : baseline (a.k.a. min value)
///   1 byte                    : baseBitWidth (bits per bitpacked residual;
///                               range [0, 64])
///   4 bytes                   : numExceptions (uint32_t, fixed width)
///   numExceptions varints     : exception positions, strictly ascending
///   numExceptions varints     : exception values (full residual, i.e.
///                               value - baseline)
///   FixedBitArray::bufferSize(rowCount, baseBitWidth) bytes:
///                               bitpacked base residuals (omitted when
///                               baseBitWidth == 0)
template <typename T>
class PforEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  /// Fixed-size header that follows the standard Encoding prefix:
  /// baseline (sizeof(physicalType)) + baseBitWidth (1) + numExceptions (4).
  static constexpr int kPrefixSize =
      sizeof(physicalType) + 1 + sizeof(uint32_t);

  /// Constructs a PforEncoding decoder from serialized wire data.
  PforEncoding(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory,
      const Encoding::Options& options = {});

  /// Resets the read cursor to the beginning of the stream.
  void reset() final;

  /// Advances the read cursor by `rowCount` rows without materializing.
  void skip(uint32_t rowCount) final;

  /// Materializes `rowCount` rows into `buffer` starting at the current
  /// cursor position. Uses a branchless two-pass strategy: first unpack
  /// all base residuals via FixedBitArray bulk decode, then patch
  /// exception positions with their full values.
  void materialize(uint32_t rowCount, void* buffer) final;

  /// Reads values through a DecoderVisitor for selective/filtered reads.
  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  /// Encodes `values` using Patched Frame-of-Reference.
  /// Selects baseBitWidth from the 90th-percentile of the bucket histogram,
  /// records overflows as varint exceptions, and bitpacks the base residuals.
  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {});

  /// Returns a human-readable description of this encoding instance.
  std::string debugString(int offset) const final;

 private:
  // Patches any exceptions whose absolute position falls inside
  // [row_, row_ + count) onto `output`, where output[k] corresponds to
  // absolute position row_ + k. Advances exceptionCursor_ past consumed
  // exceptions.
  void patchExceptions(uint32_t count, physicalType* output);

  // Advances exceptionCursor_ past any exception positions that fall before
  // `targetRow`. Uses binary search for efficient seeking during skip()
  // and reset().
  void seekExceptionsTo(uint32_t targetRow);

  // Wire-format header values, populated in the constructor.
  physicalType baseline_{};
  uint8_t baseBitWidth_{0};
  uint32_t numExceptions_{0};

  // Decoded exception side-channel. Eagerly decoded and stored at
  // construction time since numExceptions_ is bounded by ~10% of rowCount
  // in the typical case. Positions are in strictly ascending order.
  Vector<uint32_t> exceptionPositions_;
  Vector<physicalType> exceptionValues_;

  // Bitpacked base-residual region. Default-constructed (empty) when
  // baseBitWidth_ == 0.
  FixedBitArray fixedBitArray_{};

  // Current absolute row position in the stream.
  uint32_t row_{0};

  // Index of the next unconsumed exception in exceptionPositions_ /
  // exceptionValues_. Monotonically increases as rows are consumed.
  uint32_t exceptionCursor_{0};
};

//
// End of public API. Implementations follow.
//

template <typename T>
PforEncoding<T>::PforEncoding(
    velox::memory::MemoryPool& pool,
    std::string_view data,
    std::function<void*(uint32_t)> /* stringBufferFactory */,
    const Encoding::Options& options)
    : TypedEncoding<T, physicalType>{pool, data, options},
      exceptionPositions_{this->pool_},
      exceptionValues_{this->pool_} {
  if constexpr (!isIntegralType<physicalType>()) {
    NIMBLE_INCOMPATIBLE_ENCODING(
        "Pfor encoding only supports integral data types.");
  } else {
    const char* pos = data.data() + this->dataOffset();

    // Parse the fixed-size header: baseline, baseBitWidth, numExceptions.
    baseline_ = encoding::read<physicalType>(pos);
    baseBitWidth_ = static_cast<uint8_t>(encoding::readChar(pos));
    NIMBLE_CHECK_LE(
        baseBitWidth_, 64, "Pfor base bit width must be in [0, 64].");
    numExceptions_ = encoding::readUint32(pos);
    NIMBLE_CHECK_LE(
        numExceptions_,
        this->rowCount_,
        "Pfor exception count exceeds row count.");

    // Eagerly decode exception positions (varint-encoded, ascending order).
    exceptionPositions_.resize(numExceptions_);
    for (uint32_t i = 0; i < numExceptions_; ++i) {
      exceptionPositions_[i] = varint::readVarint32(&pos);
      NIMBLE_CHECK_LT(
          exceptionPositions_[i],
          this->rowCount_,
          "Pfor exception position out of range.");
      // Verify strictly ascending order so that seekExceptionsTo() and
      // patchExceptions() can rely on binary search / linear scan.
      if (i > 0) {
        NIMBLE_CHECK_GT(
            exceptionPositions_[i],
            exceptionPositions_[i - 1],
            "Pfor exception positions must be strictly ascending.");
      }
    }

    // Eagerly decode exception values (varint-encoded full residuals).
    exceptionValues_.resize(numExceptions_);
    for (uint32_t i = 0; i < numExceptions_; ++i) {
      if constexpr (sizeof(physicalType) <= 4) {
        exceptionValues_[i] =
            static_cast<physicalType>(varint::readVarint32(&pos));
      } else {
        exceptionValues_[i] =
            static_cast<physicalType>(varint::readVarint64(&pos));
      }
    }

    // Map the bitpacked residual region. Verify the wire data has
    // enough bytes remaining to prevent out-of-bounds reads during decode.
    if (baseBitWidth_ > 0) {
      const size_t bitpackedSize =
          FixedBitArray::bufferSize(this->rowCount_, baseBitWidth_);
      NIMBLE_CHECK_GE(
          static_cast<size_t>(data.end() - pos),
          bitpackedSize,
          "Pfor bitpacked region underruns wire data.");
      fixedBitArray_ = FixedBitArray{
          {pos, static_cast<size_t>(data.end() - pos)}, baseBitWidth_};
    }
  }
  reset();
}

template <typename T>
void PforEncoding<T>::reset() {
  row_ = 0;
  exceptionCursor_ = 0;
}

template <typename T>
void PforEncoding<T>::seekExceptionsTo(uint32_t targetRow) {
  // Search from the current cursor forward. The positions array is sorted
  // ascending. For small remaining counts a linear scan is faster than
  // binary search due to cache locality and branch-predictor friendliness.
  const uint32_t remaining = numExceptions_ - exceptionCursor_;
  constexpr uint32_t kLinearScanThreshold = 64;
  if (remaining < kLinearScanThreshold) {
    while (exceptionCursor_ < numExceptions_ &&
           exceptionPositions_[exceptionCursor_] < targetRow) {
      ++exceptionCursor_;
    }
  } else {
    const auto begin = exceptionPositions_.begin();
    const auto it = std::lower_bound(
        begin + exceptionCursor_, exceptionPositions_.end(), targetRow);
    exceptionCursor_ = static_cast<uint32_t>(it - begin);
  }
}

template <typename T>
void PforEncoding<T>::skip(uint32_t rowCount) {
  row_ += rowCount;
  seekExceptionsTo(row_);
}

template <typename T>
void PforEncoding<T>::patchExceptions(uint32_t count, physicalType* output) {
  // Pass 2 of the two-pass decode: overwrite the ~10% exception slots
  // with their full (baseline + exception residual) values. Exceptions
  // are in ascending position order, so this is a forward linear scan.
  const uint32_t endRow = row_ + count;
  while (exceptionCursor_ < numExceptions_ &&
         exceptionPositions_[exceptionCursor_] < endRow) {
    const uint32_t absolutePosition = exceptionPositions_[exceptionCursor_];
    output[absolutePosition - row_] = static_cast<physicalType>(
        baseline_ + exceptionValues_[exceptionCursor_]);
    ++exceptionCursor_;
  }
}

template <typename T>
void PforEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  if (rowCount == 0) {
    return;
  }

  // Pass 1: Unpack all base residuals (branchless bulk decode).
  physicalType* output = static_cast<physicalType*>(buffer);
  if (baseBitWidth_ == 0) {
    // All residuals are zero; every value equals baseline.
    std::fill(output, output + rowCount, baseline_);
  } else if constexpr (isFourByteIntegralType<physicalType>()) {
    fixedBitArray_.bulkGetWithBaseline32(
        /*start=*/row_,
        /*length=*/rowCount,
        reinterpret_cast<uint32_t*>(output),
        static_cast<uint32_t>(baseline_));
  } else if constexpr (isEightByteIntegralType<physicalType>()) {
    fixedBitArray_.bulkGet64WithBaseline(
        /*start=*/row_,
        /*length=*/rowCount,
        reinterpret_cast<uint64_t*>(output),
        static_cast<uint64_t>(baseline_));
  } else {
    // Narrow integral path (1- or 2-byte types). The bulk paths require
    // 4- or 8-byte outputs; per-element get is acceptable for rare narrow
    // streams.
    for (uint32_t i = 0; i < rowCount; ++i) {
      output[i] =
          static_cast<physicalType>(fixedBitArray_.get(row_ + i) + baseline_);
    }
  }

  // Pass 2: Patch exception positions with their full values.
  patchExceptions(rowCount, output);
  row_ += rowCount;
}

template <typename T>
template <typename V>
void PforEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  // TODO: Add a bulkScan / readWithVisitorFast path for 4-byte and 8-byte
  // integral types once Pfor is used on hot read paths. The challenge is
  // interleaving exception patching with the bulk scan framework. For now
  // the slow path is sufficient since Pfor is a niche encoding.
  detail::readWithVisitorSlow(
      visitor,
      params,
      [&](auto toSkip) { skip(toSkip); },
      [&]() {
        physicalType value;
        if (baseBitWidth_ == 0) {
          value = baseline_;
        } else {
          value =
              static_cast<physicalType>(fixedBitArray_.get(row_) + baseline_);
        }
        // Override with the exception value if one lands exactly on this row.
        if (exceptionCursor_ < numExceptions_ &&
            exceptionPositions_[exceptionCursor_] == row_) {
          value = static_cast<physicalType>(
              baseline_ + exceptionValues_[exceptionCursor_]);
          ++exceptionCursor_;
        }
        ++row_;
        return value;
      });
}

template <typename T>
std::string_view PforEncoding<T>::encode(
    EncodingSelection<typename TypeTraits<T>::physicalType>& selection,
    std::span<const typename TypeTraits<T>::physicalType> values,
    Buffer& buffer,
    const Encoding::Options& options) {
  if constexpr (!isIntegralType<physicalType>()) {
    NIMBLE_INCOMPATIBLE_ENCODING(
        "Pfor encoding only supports integral data types.");
  } else {
    static_assert(
        std::is_same_v<
            typename std::make_unsigned<physicalType>::type,
            physicalType>,
        "Pfor physical type must be unsigned.");
    const bool useVarint = options.useVarintRowCount;
    if (values.empty()) {
      NIMBLE_INCOMPATIBLE_ENCODING("Pfor encoding cannot be used with 0 rows.");
    }

    const uint32_t rowCount = static_cast<uint32_t>(values.size());
    const physicalType baseline = selection.statistics().min();
    const physicalType maxValue = selection.statistics().max();
    const physicalType fullRange =
        static_cast<physicalType>(maxValue - baseline);
    const uint8_t maxBitWidth =
        static_cast<uint8_t>(velox::bits::bitsRequired(fullRange));

    // Choose base bit width from the 90th-percentile bucket.
    // Bucket k covers residuals representable in (k+1)*7 bits (capped at 64);
    // the smallest k whose cumulative count reaches 90% gives the base width.
    // This targets ~10% exception rate, balancing bitpacked density against
    // varint exception overhead.
    uint8_t baseBitWidth = maxBitWidth;
    if (maxBitWidth > 0) {
      constexpr double kCoverageThreshold = 0.9;
      const auto& bucketCounts = selection.statistics().bucketCounts();
      const uint64_t threshold = static_cast<uint64_t>(
          static_cast<double>(rowCount) * kCoverageThreshold);
      uint64_t cumulative = 0;
      for (size_t k = 0; k < bucketCounts.size(); ++k) {
        cumulative += bucketCounts[k];
        if (cumulative >= threshold) {
          const uint8_t bucketEndBitWidth =
              static_cast<uint8_t>(std::min<size_t>((k + 1) * 7, 64));
          baseBitWidth = std::min(bucketEndBitWidth, maxBitWidth);
          break;
        }
      }
    }

    // Single pass: compute residuals, identify exceptions that overflow
    // baseBitWidth, and zero-mask exception slots in the residual array
    // so they don't corrupt the bitpacked region.
    // Compute a mask of the low `baseBitWidth` bits. We cannot use
    // velox::bits::lowMask() unconditionally because it performs
    // (1ULL << bits) - 1, which is undefined behavior when bits == 64.
    // Handle the boundary cases (0 and >= type width) explicitly and
    // delegate the common case to lowMask.
    constexpr uint32_t kBitsPerPhysicalType = sizeof(physicalType) * 8;
    const physicalType baseMask = baseBitWidth == 0
        ? physicalType{0}
        : (baseBitWidth >= kBitsPerPhysicalType
               ? static_cast<physicalType>(~physicalType{0})
               : static_cast<physicalType>(velox::bits::lowMask(baseBitWidth)));

    Vector<uint32_t> exceptionPositions{&buffer.getMemoryPool()};
    Vector<physicalType> exceptionValues{&buffer.getMemoryPool()};
    // Pre-reserve for ~10% exceptions to avoid repeated reallocs.
    exceptionPositions.reserve(rowCount / 10);
    exceptionValues.reserve(rowCount / 10);

    Vector<physicalType> maskedResiduals{&buffer.getMemoryPool()};
    maskedResiduals.resize(rowCount);
    for (uint32_t i = 0; i < rowCount; ++i) {
      const physicalType residual =
          static_cast<physicalType>(values[i] - baseline);
      if (baseBitWidth < kBitsPerPhysicalType && residual > baseMask) {
        exceptionPositions.push_back(i);
        exceptionValues.push_back(residual);
        maskedResiduals[i] = physicalType{0};
      } else {
        maskedResiduals[i] = residual;
      }
    }
    const uint32_t numExceptions =
        static_cast<uint32_t>(exceptionPositions.size());

    // Compute varint sizes for the exception side-channel.
    uint64_t exceptionPositionBytes = 0;
    uint64_t exceptionValueBytes = 0;
    for (uint32_t i = 0; i < numExceptions; ++i) {
      exceptionPositionBytes += varint::varintSize(exceptionPositions[i]);
      exceptionValueBytes += varint::varintSize(exceptionValues[i]);
    }
    const uint64_t bitpackedSize = baseBitWidth == 0
        ? 0
        : FixedBitArray::bufferSize(rowCount, baseBitWidth);
    const uint32_t encodingSize =
        Encoding::serializePrefixSize(rowCount, useVarint) +
        PforEncoding<T>::kPrefixSize +
        static_cast<uint32_t>(
            exceptionPositionBytes + exceptionValueBytes + bitpackedSize);

    // Serialize into the output buffer.
    char* reserved = buffer.reserve(encodingSize);
    char* pos = reserved;
    Encoding::serializePrefix(
        EncodingType::Pfor, TypeTraits<T>::dataType, rowCount, useVarint, pos);
    encoding::write(baseline, pos);
    encoding::writeChar(static_cast<char>(baseBitWidth), pos);
    encoding::writeUint32(numExceptions, pos);

    // Write exception positions (ascending varint sequence).
    for (uint32_t i = 0; i < numExceptions; ++i) {
      varint::writeVarint(exceptionPositions[i], &pos);
    }
    // Write exception values (full residual varints).
    for (uint32_t i = 0; i < numExceptions; ++i) {
      varint::writeVarint(exceptionValues[i], &pos);
    }

    // Bitpack the masked residuals. Exception slots are zeroed so they
    // don't affect the packed region.
    if (baseBitWidth > 0) {
      char* bitpackedStart = pos;
      std::memset(bitpackedStart, 0, bitpackedSize);
      FixedBitArray fba(bitpackedStart, baseBitWidth);
      if constexpr (sizeof(physicalType) == 4) {
        fba.bulkSet32WithBaseline(
            /*start=*/0,
            /*length=*/rowCount,
            reinterpret_cast<const uint32_t*>(maskedResiduals.data()),
            /*baseline=*/0);
      } else {
        // TODO: Add a bulkSet64 path to FixedBitArray and use it here
        // for 8-byte types to match the 4-byte bulk path above.
        for (uint32_t i = 0; i < rowCount; ++i) {
          fba.set(i, static_cast<uint64_t>(maskedResiduals[i]));
        }
      }
      pos += bitpackedSize;
    }

    NIMBLE_DCHECK_EQ(
        pos - reserved, encodingSize, "Pfor encoding size mismatch.");
    return {reserved, encodingSize};
  }
}

template <typename T>
std::string PforEncoding<T>::debugString(int offset) const {
  return fmt::format(
      "{}{}<{}> rowCount={} baseBitWidth={} numExceptions={}",
      std::string(offset, ' '),
      toString(Encoding::encodingType()),
      toString(Encoding::dataType()),
      Encoding::rowCount(),
      static_cast<int>(baseBitWidth_),
      numExceptions_);
}

} // namespace facebook::nimble
