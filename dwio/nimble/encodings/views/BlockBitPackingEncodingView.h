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
#include <vector>

#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/BlockBitPackingEncoding.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/views/EncodingView.h"
#include "velox/common/base/BitUtil.h"

namespace facebook::nimble {

template <typename T>
class BlockBitPackingEncodingView final : public TypedEncodingView<T> {
 public:
  using physicalType = typename TypedEncodingView<T>::physicalType;

  BlockBitPackingEncodingView(
      std::string_view data,
      velox::memory::MemoryPool* pool,
      const Encoding::Options& options)
      : TypedEncodingView<T>{data, pool, options},
        blocks_{this->template getVectorBuffer<BlockMeta>()} {
    NIMBLE_CHECK_EQ(this->encodingType_, EncodingType::BlockBitPacking);
    const char* pos = data.data() + this->dataOffset_;
    const auto compressionType =
        static_cast<CompressionType>(encoding::readChar(pos));
    NIMBLE_CHECK_EQ(
        compressionType,
        CompressionType::Uncompressed,
        "EncodingView does not support compressed BlockBitPacking streams.");
    blockSize_ = encoding::read<const uint16_t>(pos);
    numBlocks_ = encoding::read<const uint16_t>(pos);
    NIMBLE_CHECK_GT(blockSize_, 0);
    NIMBLE_CHECK_EQ(
        numBlocks_, velox::bits::divRoundUp(this->rowCount_, blockSize_));

    const auto baselinesSize = encoding::readUint32(pos);
    auto noStringBufferFactory = [](uint32_t) -> void* { return nullptr; };
    auto baselinesEncoding = EncodingFactory(options).create(
        *this->pool_, {pos, baselinesSize}, noStringBufferFactory);
    NIMBLE_CHECK_NOT_NULL(baselinesEncoding);
    auto baselines = this->template getVectorBuffer<physicalType>();
    baselines.resize(numBlocks_);
    baselinesEncoding->materialize(numBlocks_, baselines.data());
    pos += baselinesSize;

    const auto bitWidthsSize = encoding::readUint32(pos);
    auto bitWidthsEncoding = EncodingFactory(options).create(
        *this->pool_, {pos, bitWidthsSize}, noStringBufferFactory);
    NIMBLE_CHECK_NOT_NULL(bitWidthsEncoding);
    auto bitWidths = this->template getVectorBuffer<uint8_t>();
    bitWidths.resize(numBlocks_);
    bitWidthsEncoding->materialize(numBlocks_, bitWidths.data());
    pos += bitWidthsSize;

    const auto offsetsSize = encoding::readUint32(pos);
    auto offsetsEncoding = EncodingFactory(options).create(
        *this->pool_, {pos, offsetsSize}, noStringBufferFactory);
    NIMBLE_CHECK_NOT_NULL(offsetsEncoding);
    auto offsets = this->template getVectorBuffer<uint32_t>();
    offsets.resize(numBlocks_);
    offsetsEncoding->materialize(numBlocks_, offsets.data());
    pos += offsetsSize;

    blocks_.reserve(numBlocks_);
    for (uint16_t i = 0; i < numBlocks_; ++i) {
      const auto bitWidth = bitWidths[i];
      blocks_.push_back(
          BlockMeta{
              .baseline = baselines[i],
              .bitWidth = bitWidth,
              .offset = offsets[i],
          });
    }
    this->releaseVectorBuffer(offsets);
    this->releaseVectorBuffer(bitWidths);
    this->releaseVectorBuffer(baselines);
    packedData_ = pos;
  }

  ~BlockBitPackingEncodingView() override {
    this->releaseVectorBuffer(blocks_);
  }

 private:
  T readTypedAt(uint32_t index) const final {
    NIMBLE_CHECK_LT(index, this->rowCount_);
    const auto blockIndex = index / blockSize_;
    const auto blockOffset = index % blockSize_;
    const auto& block = blocks_[blockIndex];
    if (block.bitWidth == BlockBitPackingEncoding<T>::kRawBlockBitWidth) {
      const auto* values =
          reinterpret_cast<const physicalType*>(packedData_ + block.offset);
      return detail::castFromPhysicalType<T>(values[blockOffset]);
    }
    if (block.bitWidth == 0) {
      return detail::castFromPhysicalType<T>(block.baseline);
    }
    const auto numRows = blockRowCount(blockIndex);
    FixedBitArray fba{
        {packedData_ + block.offset,
         FixedBitArray::bufferSize(numRows, block.bitWidth)},
        block.bitWidth};
    return detail::castFromPhysicalType<T>(
        static_cast<physicalType>(fba.get(blockOffset) + block.baseline));
  }

  uint32_t blockRowCount(uint32_t blockIndex) const {
    const auto start = blockIndex * blockSize_;
    return std::min<uint32_t>(blockSize_, this->rowCount_ - start);
  }

  struct BlockMeta {
    physicalType baseline;
    uint8_t bitWidth;
    uint32_t offset;
  };

  uint16_t blockSize_{0};
  uint16_t numBlocks_{0};
  Vector<BlockMeta> blocks_;
  const char* packedData_{nullptr};
};

} // namespace facebook::nimble
