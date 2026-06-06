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
#include <array>
#include <cstring>
#include <span>
#include <type_traits>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"
#include "folly/Likely.h"
#include "velox/common/base/BitUtil.h"
#include "velox/dwio/common/Lemire/BitPacking/bitpackinghelpers.h"

// SIMD Frame-of-Reference bitpacking: value = baseline + residual.
// Packs residuals in groups of 32 via Lemire FastPFor (fastpack/fastunpack).
// Ported from AusList SIMD_FOR_BITPACKED (D97646777).

namespace facebook::nimble {

/// SIMD Frame-of-Reference bitpacking for integer streams.
///
/// Wire format (after Encoding prefix):
///   [baseline: sizeof(T)] [bitWidth: 1B] [packed groups]
///   Each group = bitWidth * 4 bytes (32 values). Omitted when bitWidth == 0.
///   Narrow types (uint8/16) widen to uint32 for packing.
template <typename T>
class SimdForBitpackEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
  static_assert(
      isIntegralType<typename TypeTraits<T>::physicalType>(),
      "SimdForBitpack only supports integral data types.");

 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  SimdForBitpackEncoding(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      const std::function<void*(uint32_t)>& stringBufferFactory,
      const Encoding::Options& options = {});

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {});

  std::string debugString(int offset) const final;

  /// Return the fixed-prefix encoded size estimate used by encoding selection.
  static uint64_t
  estimateSize(uint64_t rowCount, physicalType min, physicalType max) {
    return EncodingPrefix::kFixedPrefixSize + kPrefixSize +
        packedDataSize(rowCount, bitWidth(min, max));
  }

 private:
  static constexpr int kPrefixSize = sizeof(physicalType) + sizeof(uint8_t);

  static constexpr uint32_t kGroupSize =
      facebook::velox::fastpforlib::BITPACKING_ALGORITHM_GROUP_SIZE;

  // Return the number of FastPFor groups needed to cover `rowCount` values.
  static constexpr uint64_t numGroups(uint64_t rowCount) {
    return velox::bits::divRoundUp(rowCount, kGroupSize);
  }

  // Return the total packed data size in bytes.
  // FastPFor emits `bitWidth` uint32_t words per 32-value group.
  static constexpr uint64_t packedDataSize(
      uint64_t rowCount,
      uint8_t bitWidth) {
    return numGroups(rowCount) * bitWidth * sizeof(uint32_t);
  }

  static uint8_t bitWidth(physicalType min, physicalType max) {
    const physicalType fullRange = static_cast<physicalType>(max - min);
    return fullRange == 0
        ? uint8_t{0}
        : static_cast<uint8_t>(velox::bits::bitsRequired(fullRange));
  }

  // Unpack a single group at the given group index into `output`.
  // `output` must have room for kGroupSize elements.
  void unpackGroup(uint32_t groupIndex, physicalType* output) const;

  // Materialize a slice from one packed group into `output`.
  inline void materializePartialGroup(
      uint32_t groupIndex,
      uint32_t offsetInGroup,
      uint32_t count,
      physicalType* output) const;

  physicalType baseline_{};
  uint8_t bitWidth_{0};
  const char* packedData_{nullptr};
  uint32_t row_{0};

  // Avoid re-unpacking when consecutive rows hit the same group.
  mutable uint32_t cachedGroupIndex_{~0u};
  mutable std::array<physicalType, kGroupSize> cachedGroup_{};
};

//
// End of class declaration. Implementations follow.
//

template <typename T>
SimdForBitpackEncoding<T>::SimdForBitpackEncoding(
    velox::memory::MemoryPool& pool,
    std::string_view data,
    const std::function<void*(uint32_t)>& /* stringBufferFactory */,
    const Encoding::Options& options)
    : TypedEncoding<T, physicalType>{pool, data, options} {
  if constexpr (!isIntegralType<physicalType>()) {
    NIMBLE_INCOMPATIBLE_ENCODING(
        "SimdForBitpack encoding only supports integral data types.");
  } else {
    const char* pos = data.data() + this->dataOffset();
    baseline_ = encoding::read<physicalType>(pos);
    bitWidth_ = static_cast<uint8_t>(encoding::readChar(pos));
    constexpr uint8_t kMaxBits = static_cast<uint8_t>(sizeof(physicalType) * 8);
    NIMBLE_CHECK_LE(
        bitWidth_,
        kMaxBits,
        "SimdForBitpack bit width exceeds physical type size.");

    if (bitWidth_ > 0) {
      const uint64_t expectedSize = packedDataSize(this->rowCount_, bitWidth_);
      NIMBLE_CHECK_GE(
          static_cast<size_t>(data.end() - pos),
          expectedSize,
          "SimdForBitpack packed region underruns wire data.");
      packedData_ = pos;
    }
  }
  reset();
}

template <typename T>
void SimdForBitpackEncoding<T>::reset() {
  row_ = 0;
  cachedGroupIndex_ = ~0u;
}

template <typename T>
void SimdForBitpackEncoding<T>::skip(uint32_t rowCount) {
  row_ += rowCount;
}

template <typename T>
void SimdForBitpackEncoding<T>::unpackGroup(
    uint32_t groupIndex,
    physicalType* output) const {
  const uint32_t* groupStart = reinterpret_cast<const uint32_t*>(packedData_) +
      static_cast<uint64_t>(groupIndex) * bitWidth_;
  if constexpr (isFourByteIntegralType<physicalType>()) {
    facebook::velox::fastpforlib::fastunpack(
        groupStart, reinterpret_cast<uint32_t*>(output), bitWidth_);
  } else if constexpr (isEightByteIntegralType<physicalType>()) {
    facebook::velox::fastpforlib::fastunpack(
        groupStart, reinterpret_cast<uint64_t*>(output), bitWidth_);
  } else {
    // Narrow types: unpack to uint32 temp then downcast.
    std::array<uint32_t, kGroupSize> temp{};
    facebook::velox::fastpforlib::fastunpack(
        groupStart, temp.data(), bitWidth_);
    for (uint32_t i = 0; i < kGroupSize; ++i) {
      output[i] = static_cast<physicalType>(temp[i]);
    }
  }
}

template <typename T>
inline void SimdForBitpackEncoding<T>::materializePartialGroup(
    uint32_t groupIndex,
    uint32_t offsetInGroup,
    uint32_t count,
    physicalType* output) const {
  std::array<physicalType, kGroupSize> temp{};
  unpackGroup(groupIndex, temp.data());
  for (uint32_t i = 0; i < count; ++i) {
    output[i] = static_cast<physicalType>(temp[offsetInGroup + i] + baseline_);
  }
}

template <typename T>
void SimdForBitpackEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  if (FOLLY_UNLIKELY(rowCount == 0)) {
    return;
  }
  auto* output = static_cast<physicalType*>(buffer);

  if (bitWidth_ == 0) {
    // All values match the baseline, so there are no residual bits to unpack.
    std::fill(output, output + rowCount, baseline_);
    row_ += rowCount;
    return;
  }

  uint32_t remaining = rowCount;
  uint32_t outputIndex = 0;

  // Partial first group.
  const uint32_t offsetInGroup = row_ % kGroupSize;
  if (offsetInGroup != 0) {
    const uint32_t groupIndex = row_ / kGroupSize;
    const uint32_t available = kGroupSize - offsetInGroup;
    const uint32_t toCopy = std::min(available, remaining);
    materializePartialGroup(
        groupIndex, offsetInGroup, toCopy, output + outputIndex);
    outputIndex += toCopy;
    remaining -= toCopy;
  }

  // Full groups.
  while (remaining >= kGroupSize) {
    const uint32_t groupIndex = (row_ + outputIndex) / kGroupSize;
    unpackGroup(groupIndex, output + outputIndex);
    for (uint32_t i = 0; i < kGroupSize; ++i) {
      output[outputIndex + i] += baseline_;
    }
    outputIndex += kGroupSize;
    remaining -= kGroupSize;
  }

  // Partial last group.
  if (remaining > 0) {
    const uint32_t groupIndex = (row_ + outputIndex) / kGroupSize;
    materializePartialGroup(groupIndex, 0, remaining, output + outputIndex);
    outputIndex += remaining;
  }

  row_ += rowCount;
}

template <typename T>
template <typename V>
void SimdForBitpackEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  detail::readWithVisitorSlow(
      visitor,
      params,
      [&](auto toSkip) { skip(toSkip); },
      [&]() {
        physicalType value;
        if (bitWidth_ == 0) {
          value = baseline_;
        } else {
          const uint32_t groupIndex = row_ / kGroupSize;
          const uint32_t offsetInGroup = row_ % kGroupSize;
          if (groupIndex != cachedGroupIndex_) {
            unpackGroup(groupIndex, cachedGroup_.data());
            cachedGroupIndex_ = groupIndex;
          }
          value = static_cast<physicalType>(
              cachedGroup_[offsetInGroup] + baseline_);
        }
        ++row_;
        return value;
      });
}

template <typename T>
std::string_view SimdForBitpackEncoding<T>::encode(
    EncodingSelection<typename TypeTraits<T>::physicalType>& selection,
    std::span<const typename TypeTraits<T>::physicalType> values,
    Buffer& buffer,
    const Encoding::Options& options) {
  if constexpr (!isIntegralType<physicalType>()) {
    NIMBLE_INCOMPATIBLE_ENCODING(
        "SimdForBitpack encoding only supports integral data types.");
  } else {
    static_assert(
        std::is_same_v<
            typename std::make_unsigned<physicalType>::type,
            physicalType>,
        "SimdForBitpack physical type must be unsigned.");
    const bool useVarint = options.useVarintRowCount;
    if (values.empty()) {
      NIMBLE_INCOMPATIBLE_ENCODING(
          "SimdForBitpack encoding cannot be used with 0 rows.");
    }
    const uint32_t rowCount = static_cast<uint32_t>(values.size());
    const physicalType baseline = selection.statistics().min();
    const physicalType maxValue = selection.statistics().max();
    const physicalType fullRange =
        static_cast<physicalType>(maxValue - baseline);
    const uint8_t bitWidth = fullRange == 0
        ? uint8_t{0}
        : static_cast<uint8_t>(velox::bits::bitsRequired(fullRange));

    const uint32_t numGroupsTotal = static_cast<uint32_t>(numGroups(rowCount));
    const uint64_t packedSize = packedDataSize(rowCount, bitWidth);

    const uint32_t encodingSize =
        Encoding::serializePrefixSize(rowCount, useVarint) + kPrefixSize +
        static_cast<uint32_t>(packedSize);

    char* reserved = buffer.reserve(encodingSize);
    char* pos = reserved;
    Encoding::serializePrefix(
        EncodingType::SimdForBitpack,
        TypeTraits<T>::dataType,
        rowCount,
        useVarint,
        pos);
    encoding::write(baseline, pos);
    encoding::writeChar(static_cast<char>(bitWidth), pos);

    if (bitWidth > 0) {
      auto* packedOut = reinterpret_cast<uint32_t*>(pos);
      std::array<physicalType, kGroupSize> residuals{};

      // FastPFor packs one group of 32 residuals at a time:
      //
      //   32 values * bitWidth bits/value = bitWidth uint32_t words
      //
      // The last group is zero-padded before packing when rowCount is not a
      // multiple of 32.
      for (uint32_t group = 0; group < numGroupsTotal; ++group) {
        const uint32_t groupStart = group * kGroupSize;
        const uint32_t groupEnd = std::min(groupStart + kGroupSize, rowCount);
        const uint32_t groupLen = groupEnd - groupStart;

        // Only zero-fill the last partial group; full groups overwrite all 32.
        if (groupLen < kGroupSize) {
          std::fill(residuals.begin(), residuals.end(), physicalType{0});
        }
        for (uint32_t i = 0; i < groupLen; ++i) {
          residuals[i] =
              static_cast<physicalType>(values[groupStart + i] - baseline);
        }

        if constexpr (isFourByteIntegralType<physicalType>()) {
          facebook::velox::fastpforlib::fastpack(
              reinterpret_cast<const uint32_t*>(residuals.data()),
              packedOut,
              bitWidth);
        } else if constexpr (isEightByteIntegralType<physicalType>()) {
          facebook::velox::fastpforlib::fastpack(
              reinterpret_cast<const uint64_t*>(residuals.data()),
              packedOut,
              bitWidth);
        } else {
          // Narrow types: widen to uint32 for packing.
          std::array<uint32_t, kGroupSize> widenedResiduals{};
          for (uint32_t i = 0; i < kGroupSize; ++i) {
            widenedResiduals[i] = static_cast<uint32_t>(residuals[i]);
          }
          facebook::velox::fastpforlib::fastpack(
              widenedResiduals.data(), packedOut, bitWidth);
        }
        // `packedOut` is a uint32_t pointer. One packed group occupies
        // `bitWidth` uint32_t words.
        packedOut += bitWidth;
      }
      pos += packedSize;
    }

    NIMBLE_CHECK_EQ(
        pos - reserved, encodingSize, "SimdForBitpack encoding size mismatch.");
    return {reserved, encodingSize};
  }
}

template <typename T>
std::string SimdForBitpackEncoding<T>::debugString(int offset) const {
  return fmt::format(
      "{}{}<{}> rowCount={} bitWidth={}",
      std::string(offset, ' '),
      Encoding::encodingType(),
      Encoding::dataType(),
      Encoding::rowCount(),
      static_cast<int>(bitWidth_));
}

} // namespace facebook::nimble
