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
#include <type_traits>

#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/views/EncodingViewFactory.h"
#include "velox/common/base/BitUtil.h"

namespace facebook::nimble {

template <typename T>
class FOREncodingView final : public TypedEncodingView<T> {
 public:
  using physicalType = typename TypedEncodingView<T>::physicalType;

  FOREncodingView(
      std::string_view data,
      velox::memory::MemoryPool* pool,
      const Encoding::Options& options)
      : TypedEncodingView<T>{data, pool, options},
        bitWidths_{this->template getVectorBuffer<uint8_t>()},
        references_{this->template getVectorBuffer<physicalType>()},
        bitOffsets_{this->template getVectorBuffer<uint64_t>()} {
    static_assert(
        std::is_integral_v<physicalType>,
        "FOREncodingView only supports integral types");

    NIMBLE_CHECK_EQ(this->encodingType_, EncodingType::FOR);
    const char* pos = data.data() + this->dataOffset_;
    const auto compressionType =
        static_cast<CompressionType>(encoding::readChar(pos));
    NIMBLE_CHECK_EQ(
        compressionType,
        CompressionType::Uncompressed,
        "EncodingView does not support compressed FOR streams.");
    frameSize_ = encoding::readUint32(pos);
    numFrames_ = encoding::readUint32(pos);
    enableBitOffsets_ = static_cast<bool>(encoding::readChar(pos));
    NIMBLE_CHECK_GT(frameSize_, 0);
    NIMBLE_CHECK_EQ(
        numFrames_, (this->rowCount_ + frameSize_ - 1) / frameSize_);

    const auto bitWidthsSize = encoding::readUint32(pos);
    auto bitWidths = detail::createTypedEncodingView<uint8_t>(
        {pos, bitWidthsSize}, pool, options);
    NIMBLE_CHECK_NOT_NULL(bitWidths);
    NIMBLE_CHECK_EQ(bitWidths->rowCount(), numFrames_);
    bitWidths_.resize(numFrames_);
    for (uint32_t frame = 0; frame < numFrames_; ++frame) {
      bitWidths_[frame] = bitWidths->readAt(frame);
    }
    pos += bitWidthsSize;

    const auto referencesSize = encoding::readUint32(pos);
    auto references = detail::createTypedEncodingView<physicalType>(
        {pos, referencesSize}, pool, options);
    NIMBLE_CHECK_NOT_NULL(references);
    NIMBLE_CHECK_EQ(references->rowCount(), numFrames_);
    references_.resize(numFrames_);
    for (uint32_t frame = 0; frame < numFrames_; ++frame) {
      references_[frame] = references->readAt(frame);
    }
    pos += referencesSize;

    bitOffsets_.resize(numFrames_);
    if (enableBitOffsets_) {
      const auto bitOffsetsSize = encoding::readUint32(pos);
      auto bitOffsets = detail::createTypedEncodingView<uint64_t>(
          {pos, bitOffsetsSize}, pool, options);
      NIMBLE_CHECK_NOT_NULL(bitOffsets);
      NIMBLE_CHECK_EQ(bitOffsets->rowCount(), numFrames_);
      for (uint32_t frame = 0; frame < numFrames_; ++frame) {
        bitOffsets_[frame] = bitOffsets->readAt(frame);
      }
      pos += bitOffsetsSize;
    } else {
      uint64_t cumulativeBitOffset{0};
      for (uint32_t frame = 0; frame < numFrames_; ++frame) {
        bitOffsets_[frame] = cumulativeBitOffset;
        cumulativeBitOffset += frameRowCount(frame) * bitWidths_[frame];
      }
    }

    const auto packedDataSize = encoding::readUint32(pos);
    packedData_ = pos;
    pos += packedDataSize;
    NIMBLE_CHECK_EQ(pos, data.end(), "Unexpected FOR view end.");
  }

  ~FOREncodingView() override {
    this->releaseVectorBuffer(bitOffsets_);
    this->releaseVectorBuffer(references_);
    this->releaseVectorBuffer(bitWidths_);
  }

 private:
  uint32_t frameRowCount(uint32_t frame) const {
    const auto startRow = frame * frameSize_;
    return std::min(frameSize_, this->rowCount_ - startRow);
  }

  uint64_t bitOffset(uint32_t frame) const {
    return bitOffsets_[frame];
  }

  uint64_t byteAlignedResidualAt(uint64_t byteOffset, uint8_t bitWidth) const {
    switch (bitWidth) {
      case 8:
        return static_cast<uint8_t>(packedData_[byteOffset]);
      case 16: {
        uint16_t value;
        std::memcpy(&value, packedData_ + byteOffset, sizeof(value));
        return value;
      }
      case 32: {
        uint32_t value;
        std::memcpy(&value, packedData_ + byteOffset, sizeof(value));
        return value;
      }
      case 64: {
        uint64_t value;
        std::memcpy(&value, packedData_ + byteOffset, sizeof(value));
        return value;
      }
      default:
        return 0;
    }
  }

  uint64_t residualAt(uint64_t rowBitOffset, uint8_t bitWidth) const {
    if (bitWidth == 0) {
      return 0;
    }

    const auto byteOffset = rowBitOffset / 8;
    const auto bitOffsetInByte = rowBitOffset & 7;
    if (bitOffsetInByte == 0 &&
        (bitWidth == 8 || bitWidth == 16 || bitWidth == 32 || bitWidth == 64)) {
      return byteAlignedResidualAt(byteOffset, bitWidth);
    }

    const auto bytesToRead = velox::bits::nbytes(bitOffsetInByte + bitWidth);
    uint64_t word{0};
    std::memcpy(
        &word, packedData_ + byteOffset, std::min<uint64_t>(8, bytesToRead));
    if (bytesToRead > 8) {
      word = (word >> bitOffsetInByte) |
          (static_cast<uint64_t>(
               static_cast<uint8_t>(packedData_[byteOffset + 8]))
           << (64 - bitOffsetInByte));
      return word;
    }
    word >>= bitOffsetInByte;
    const auto mask = bitWidth == 64 ? ~0ULL : ((uint64_t{1} << bitWidth) - 1);
    return word & mask;
  }

  physicalType addResidual(physicalType reference, uint64_t residual) const {
    if constexpr (std::is_signed_v<physicalType>) {
      return static_cast<physicalType>(
          static_cast<int64_t>(reference) + static_cast<int64_t>(residual));
    } else {
      return static_cast<physicalType>(reference + residual);
    }
  }

  T readTypedAt(uint32_t index) const final {
    NIMBLE_CHECK_LT(index, this->rowCount_);
    const auto frame = index / frameSize_;
    const auto positionInFrame = index % frameSize_;
    const auto bitWidth = bitWidths_[frame];
    NIMBLE_CHECK_LE(bitWidth, sizeof(physicalType) * 8);
    const auto residual =
        residualAt(bitOffset(frame) + positionInFrame * bitWidth, bitWidth);
    const auto reference = references_[frame];
    return detail::castFromPhysicalType<T>(addResidual(reference, residual));
  }

  uint32_t frameSize_{0};
  uint32_t numFrames_{0};
  bool enableBitOffsets_{false};
  Vector<uint8_t> bitWidths_;
  Vector<physicalType> references_;
  Vector<uint64_t> bitOffsets_;
  const char* packedData_{nullptr};
};

} // namespace facebook::nimble
