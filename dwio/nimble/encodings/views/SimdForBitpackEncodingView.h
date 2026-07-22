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

#include "dwio/nimble/encodings/SimdForBitpackEncoding.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/views/EncodingView.h"

namespace facebook::nimble {

template <typename T>
class SimdForBitpackEncodingView final : public TypedEncodingView<T> {
 public:
  using physicalType = typename TypedEncodingView<T>::physicalType;

  SimdForBitpackEncodingView(
      std::string_view data,
      velox::memory::MemoryPool* pool,
      const Encoding::Options& options)
      : TypedEncodingView<T>{data, pool, options} {
    NIMBLE_CHECK_EQ(this->encodingType_, EncodingType::SimdForBitpack);
    const char* pos = data.data() + this->dataOffset_;
    baseline_ = encoding::read<physicalType>(pos);
    bitWidth_ = static_cast<uint8_t>(encoding::readChar(pos));
    NIMBLE_CHECK_LE(bitWidth_, sizeof(physicalType) * 8);
    packedData_ = pos;
  }

 private:
  T readTypedAt(uint32_t index) const final {
    NIMBLE_CHECK_LT(index, this->rowCount_);
    if (bitWidth_ == 0) {
      return detail::castFromPhysicalType<T>(baseline_);
    }

    const auto groupIndex = index / kGroupSize;
    std::array<physicalType, kGroupSize> group{};
    unpackGroup(groupIndex, group.data());
    return detail::castFromPhysicalType<T>(
        static_cast<physicalType>(group[index % kGroupSize] + baseline_));
  }

  void readPhysical(uint32_t offset, uint32_t length, physicalType* output)
      const final {
    this->checkReadRange(offset, length);
    if (bitWidth_ == 0) {
      std::fill(output, output + length, baseline_);
      return;
    }

    uint32_t outputOffset{0};
    while (outputOffset < length) {
      const auto groupIndex = offset / kGroupSize;
      const auto groupOffset = offset % kGroupSize;
      const auto count =
          std::min<uint32_t>(length - outputOffset, kGroupSize - groupOffset);
      std::array<physicalType, kGroupSize> group{};
      unpackGroup(groupIndex, group.data());
      for (uint32_t i = 0; i < count; ++i) {
        output[outputOffset + i] =
            static_cast<physicalType>(group[groupOffset + i] + baseline_);
      }
      outputOffset += count;
      offset += count;
    }
  }

  static constexpr uint32_t kGroupSize = SimdForBitpackEncoding<T>::kGroupSize;

  void unpackGroup(uint32_t groupIndex, physicalType* output) const {
    static_assert(
        isOneByteIntegralType<physicalType>() ||
            isTwoByteIntegralType<physicalType>() ||
            isFourByteIntegralType<physicalType>() ||
            isEightByteIntegralType<physicalType>(),
        "Unexpected SimdForBitpack physical type width.");
    const auto* groupStart = reinterpret_cast<const uint32_t*>(packedData_) +
        static_cast<uint64_t>(groupIndex) * bitWidth_;
    if constexpr (isEightByteIntegralType<physicalType>()) {
      facebook::velox::fastpforlib::fastunpack(
          groupStart, reinterpret_cast<uint64_t*>(output), bitWidth_);
    } else if constexpr (isFourByteIntegralType<physicalType>()) {
      facebook::velox::fastpforlib::fastunpack(
          groupStart, reinterpret_cast<uint32_t*>(output), bitWidth_);
    } else {
      std::array<uint32_t, kGroupSize> temp{};
      facebook::velox::fastpforlib::fastunpack(
          groupStart, temp.data(), bitWidth_);
      for (uint32_t i = 0; i < kGroupSize; ++i) {
        output[i] = static_cast<physicalType>(temp[i]);
      }
    }
  }

  physicalType baseline_{};
  uint8_t bitWidth_{0};
  const char* packedData_{nullptr};
};

} // namespace facebook::nimble
