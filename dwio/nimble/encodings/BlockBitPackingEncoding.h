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
#include <span>
#include <type_traits>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Constants.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/compression/Compression.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/common/EncodingType.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"
#include "velox/common/base/BitUtil.h"
#include "velox/dwio/common/DecoderUtil.h"
#include "velox/dwio/common/Lemire/BitPacking/bitpackinghelpers.h"

// Stores integer data in fixed-size blocks (default 1024 rows), each with its
// own baseline and bit width. Adapted from Impulse's per-block bitpacking
// approach for better compression when value ranges vary across the stream.

namespace facebook::nimble {

template <typename T>
class BlockBitPackingEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  static constexpr uint16_t kMaxBlockSize = kBlockBitPackingBlockSize;

  BlockBitPackingEncoding(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      const std::function<void*(uint32_t)>& stringBufferFactory,
      const Encoding::Options& options = {});

  ~BlockBitPackingEncoding() override {
    this->releaseBuffer(uncompressedData_);
    this->releaseVectorBuffer(buffer_);
  }

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  template <bool kScatter, typename Visitor>
  void bulkScan(
      Visitor& visitor,
      vector_size_t currentRow,
      const vector_size_t* selectedRows,
      vector_size_t numSelected,
      const vector_size_t* scatterRows);

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {});

  /// Returns the metadata overhead in bytes for the encoding header.
  /// Shared by encode() and EncodingSizeEstimation to keep the two in sync.
  static uint32_t encodingOverhead(uint32_t numBlocks) {
    return 1 /* compressionType */ + 2 /* blockSize */ + 2 /* numBlocks */ +
        numBlocks *
        (sizeof(physicalType) + 1) /* per-block baseline + bitWidth */
        + numBlocks * sizeof(uint32_t) /* per-block data offsets */;
  }

  std::string debugString(int offset) const final;

 private:
  /// Per-block metadata stored in the header: baseline (minimum) value,
  /// bit width for the packed representation, and whether this block
  /// falls back to storing raw uncompressed values.
  struct BlockMeta {
    /// Minimum value in the block; packed values are stored as
    /// (value - baseline).
    physicalType baseline;
    /// Number of bits per packed value (0 when all values equal the baseline).
    uint8_t bitWidth;
    /// When true, this block stores raw values (packing was not beneficial).
    bool skipEncoding;
  };

  /// Reads a single decoded value at the given absolute row index.
  physicalType readSingleValue(uint32_t row) const;

  /// Decodes a contiguous range within one block into 'output'.
  void materializeBlockRange(
      uint32_t blockIndex,
      uint32_t blockValueOffset,
      uint32_t blockValueCount,
      physicalType* output) const;

  /// Returns the number of rows in the given block (may be less than
  /// blockSize_ for the last block).
  uint32_t blockRowCount(uint32_t blockIndex) const;

  /// Unpacks 'numRows' bit-packed values in fixed-size groups, falling
  /// back to FixedBitArray for any trailing remainder.
  static void fullUnpack(
      const uint8_t* input,
      physicalType* output,
      uint32_t numRows,
      uint8_t bitWidth,
      physicalType baseline);

  uint16_t blockSize_;
  uint16_t numBlocks_;
  Vector<BlockMeta> blocksMetadata_;
  Vector<uint32_t> blockDataOffsets_;
  const char* packedData_;
  uint32_t row_ = 0;
  velox::BufferPtr uncompressedData_;
  Vector<physicalType> buffer_;
};

//
// End of public API. Implementations follow.
//

template <typename T>
BlockBitPackingEncoding<T>::BlockBitPackingEncoding(
    velox::memory::MemoryPool& pool,
    std::string_view data,
    const std::function<void*(uint32_t)>& /* stringBufferFactory */,
    const Encoding::Options& options)
    : TypedEncoding<T, physicalType>{pool, data, options},
      blocksMetadata_{&pool},
      blockDataOffsets_{&pool},
      packedData_{nullptr},
      buffer_(this->template getVectorBuffer<physicalType>()) {
  auto pos = data.data() + this->dataOffset();
  auto compressionType = static_cast<CompressionType>(encoding::readChar(pos));
  blockSize_ = encoding::read<const uint16_t>(pos);
  numBlocks_ = encoding::read<const uint16_t>(pos);

  NIMBLE_CHECK(blockSize_ > 0 && blockSize_ <= kMaxBlockSize);
  NIMBLE_CHECK_EQ(numBlocks_, (this->rowCount() + blockSize_ - 1) / blockSize_);

  blocksMetadata_.resize(numBlocks_);
  for (uint16_t i = 0; i < numBlocks_; ++i) {
    blocksMetadata_[i].baseline = encoding::read<const physicalType>(pos);
    const auto bitWidth = static_cast<uint8_t>(encoding::readChar(pos));
    blocksMetadata_[i].skipEncoding = (bitWidth == 255);
    blocksMetadata_[i].bitWidth =
        blocksMetadata_[i].skipEncoding ? 0 : bitWidth;
  }

  blockDataOffsets_.resize(numBlocks_);
  for (uint16_t i = 0; i < numBlocks_; ++i) {
    blockDataOffsets_[i] = encoding::read<const uint32_t>(pos);
  }

  if (compressionType != CompressionType::Uncompressed) {
    uncompressedData_ = Compression::uncompress(
        pool,
        compressionType,
        DataType::Undefined,
        {pos, static_cast<size_t>(data.end() - pos)},
        options.bufferPool);
    packedData_ = uncompressedData_->as<char>();
  } else {
    packedData_ = pos;
  }
}

template <typename T>
void BlockBitPackingEncoding<T>::reset() {
  row_ = 0;
}

template <typename T>
void BlockBitPackingEncoding<T>::skip(uint32_t rowCount) {
  row_ += rowCount;
}

template <typename T>
uint32_t BlockBitPackingEncoding<T>::blockRowCount(uint32_t blockIndex) const {
  const auto totalRows = this->rowCount();
  const auto start = static_cast<uint32_t>(blockIndex) * blockSize_;
  return std::min(static_cast<uint32_t>(blockSize_), totalRows - start);
}

template <typename T>
typename BlockBitPackingEncoding<T>::physicalType
BlockBitPackingEncoding<T>::readSingleValue(uint32_t row) const {
  const auto blockIndex = row / blockSize_;
  const auto blockOffset = row % blockSize_;
  const auto& blockMetadata = blocksMetadata_[blockIndex];
  if (blockMetadata.skipEncoding) {
    const auto* rawValues = reinterpret_cast<const physicalType*>(
        packedData_ + blockDataOffsets_[blockIndex]);
    return rawValues[blockOffset];
  }
  if (blockMetadata.bitWidth == 0) {
    return blockMetadata.baseline;
  }
  const auto numRows = blockRowCount(blockIndex);
  FixedBitArray fba{
      {packedData_ + blockDataOffsets_[blockIndex],
       FixedBitArray::bufferSize(numRows, blockMetadata.bitWidth)},
      blockMetadata.bitWidth};
  return static_cast<physicalType>(fba.get(blockOffset)) +
      blockMetadata.baseline;
}

template <typename T>
void BlockBitPackingEncoding<T>::materializeBlockRange(
    uint32_t blockIndex,
    uint32_t blockValueOffset,
    uint32_t blockValueCount,
    physicalType* output) const {
  const auto& blockMetadata = blocksMetadata_[blockIndex];
  const auto* blockData = packedData_ + blockDataOffsets_[blockIndex];

  if (blockMetadata.skipEncoding) {
    const auto* rawValues = reinterpret_cast<const physicalType*>(blockData);
    std::memcpy(
        output,
        rawValues + blockValueOffset,
        blockValueCount * sizeof(physicalType));
    return;
  }

  if (blockMetadata.bitWidth == 0) {
    std::fill(output, output + blockValueCount, blockMetadata.baseline);
    return;
  }

  const auto* inputBytes = reinterpret_cast<const uint8_t*>(blockData);

  if (blockValueOffset == 0) {
    fullUnpack(
        inputBytes,
        output,
        blockValueCount,
        blockMetadata.bitWidth,
        blockMetadata.baseline);
  } else {
    physicalType tmp[kMaxBlockSize];
    fullUnpack(
        inputBytes,
        tmp,
        blockValueOffset + blockValueCount,
        blockMetadata.bitWidth,
        blockMetadata.baseline);
    std::memcpy(
        output, tmp + blockValueOffset, blockValueCount * sizeof(physicalType));
  }
}

template <typename T>
void BlockBitPackingEncoding<T>::fullUnpack(
    const uint8_t* input,
    physicalType* output,
    uint32_t numRows,
    uint8_t bitWidth,
    physicalType baseline) {
  constexpr uint32_t kGroupSize = [] {
    if constexpr (sizeof(physicalType) == 1) {
      return 8u;
    } else if constexpr (sizeof(physicalType) == 2) {
      return 16u;
    } else {
      return 32u;
    }
  }();

  uint32_t currentRow = 0;

  if constexpr (std::is_same_v<physicalType, uint8_t>) {
    for (; currentRow + kGroupSize <= numRows; currentRow += kGroupSize) {
      velox::fastpforlib::internal::fastunpack_quarter(
          input, output + currentRow, bitWidth);
      input += bitWidth;
    }
  } else if constexpr (std::is_same_v<physicalType, uint16_t>) {
    for (; currentRow + kGroupSize <= numRows; currentRow += kGroupSize) {
      velox::fastpforlib::internal::fastunpack_half(
          reinterpret_cast<const uint16_t*>(input),
          output + currentRow,
          bitWidth);
      input += bitWidth * 2;
    }
  } else if constexpr (isFourByteIntegralType<physicalType>()) {
    for (; currentRow + kGroupSize <= numRows; currentRow += kGroupSize) {
      velox::fastpforlib::fastunpack(
          reinterpret_cast<const uint32_t*>(input),
          reinterpret_cast<uint32_t*>(output + currentRow),
          bitWidth);
      input += bitWidth * 4;
    }
  } else if constexpr (isEightByteIntegralType<physicalType>()) {
    for (; currentRow + kGroupSize <= numRows; currentRow += kGroupSize) {
      velox::fastpforlib::fastunpack(
          reinterpret_cast<const uint32_t*>(input),
          reinterpret_cast<uint64_t*>(output + currentRow),
          bitWidth);
      input += bitWidth * 4;
    }
  }

  if (currentRow < numRows) {
    const auto tailCount = numRows - currentRow;
    FixedBitArray fba(
        {reinterpret_cast<const char*>(input),
         FixedBitArray::bufferSize(tailCount, bitWidth)},
        bitWidth);
    for (uint32_t i = 0; i < tailCount; ++i) {
      output[currentRow + i] = static_cast<physicalType>(fba.get(i));
    }
  }

  if (baseline != 0) {
    for (uint32_t i = 0; i < numRows; ++i) {
      output[i] += baseline;
    }
  }
}

template <typename T>
void BlockBitPackingEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  auto* output = static_cast<physicalType*>(buffer);
  uint32_t remaining = rowCount;
  uint32_t currentRow = row_;

  while (remaining > 0) {
    const auto blockIndex = currentRow / blockSize_;
    const auto blockOffset = currentRow % blockSize_;
    const auto rowsInBlock = std::min(remaining, blockSize_ - blockOffset);

    materializeBlockRange(blockIndex, blockOffset, rowsInBlock, output);

    output += rowsInBlock;
    currentRow += rowsInBlock;
    remaining -= rowsInBlock;
  }
  row_ += rowCount;
}

template <typename T>
template <typename V>
void BlockBitPackingEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  using OutputType = detail::ValueType<typename V::DataType>;
  constexpr bool kIsSuitableWidth =
      (isFourByteIntegralType<physicalType>() ||
       isEightByteIntegralType<physicalType>());
  constexpr bool kIsFluidCast = sizeof(OutputType) >= sizeof(physicalType) &&
      std::is_integral_v<OutputType> && std::is_integral_v<physicalType>;
  if constexpr (
      kIsSuitableWidth &&
      std::is_same_v<
          typename V::Extract,
          velox::dwio::common::ExtractToReader> &&
      kIsFluidCast) {
    auto* nulls = visitor.reader().rawNullsInReadRange();
    if (velox::dwio::common::useFastPath(visitor, nulls)) {
      detail::readWithVisitorFast(*this, visitor, params, nulls);
      return;
    }
  }
  detail::readWithVisitorSlow(
      visitor,
      params,
      [&](auto toSkip) { skip(toSkip); },
      [&] {
        physicalType value = readSingleValue(row_++);
        return value;
      });
}

template <typename T>
template <bool kScatter, typename V>
void BlockBitPackingEncoding<T>::bulkScan(
    V& visitor,
    vector_size_t currentRow,
    const vector_size_t* selectedRows,
    vector_size_t numSelected,
    const vector_size_t* scatterRows) {
  using OutputType = detail::ValueType<typename V::DataType>;
  static_assert(
      isFourByteIntegralType<physicalType>() ||
          isEightByteIntegralType<physicalType>(),
      "bulkScan only supports 4-byte or 8-byte integral types");

  if (numSelected == 0) {
    return;
  }

  const auto numRows = visitor.numRows() - visitor.rowIndex();
  const auto offset =
      static_cast<int64_t>(row_) - static_cast<int64_t>(currentRow);

  auto* values = detail::mutableValues<OutputType>(visitor, numRows);

  constexpr bool kSameSize = sizeof(physicalType) == sizeof(OutputType);
  constexpr bool kIsUpcast = sizeof(OutputType) > sizeof(physicalType) &&
      std::is_integral_v<OutputType> && std::is_integral_v<physicalType>;

  if constexpr (V::dense) {
    const auto startRow = selectedRows[0] + offset;

    if constexpr (kSameSize) {
      // Materialize directly into output — no temp buffer needed.
      auto* dst = reinterpret_cast<physicalType*>(values);
      uint32_t decodedRows = 0;
      uint32_t currentAbsRow = startRow;
      while (decodedRows < static_cast<uint32_t>(numSelected)) {
        const auto blockIndex = currentAbsRow / blockSize_;
        const auto blockOffset = currentAbsRow % blockSize_;
        const auto toDecode = std::min(
            static_cast<uint32_t>(numSelected) - decodedRows,
            blockSize_ - blockOffset);
        materializeBlockRange(
            blockIndex, blockOffset, toDecode, dst + decodedRows);
        decodedRows += toDecode;
        currentAbsRow += toDecode;
      }
    } else if constexpr (kIsUpcast) {
      buffer_.resize(numSelected);
      auto* rawBuf = buffer_.data();
      uint32_t decodedRows = 0;
      uint32_t currentAbsRow = startRow;
      while (decodedRows < static_cast<uint32_t>(numSelected)) {
        const auto blockIndex = currentAbsRow / blockSize_;
        const auto blockOffset = currentAbsRow % blockSize_;
        const auto toDecode = std::min(
            static_cast<uint32_t>(numSelected) - decodedRows,
            blockSize_ - blockOffset);
        materializeBlockRange(
            blockIndex, blockOffset, toDecode, rawBuf + decodedRows);
        decodedRows += toDecode;
        currentAbsRow += toDecode;
      }
      for (vector_size_t i = 0; i < numSelected; ++i) {
        values[i] = static_cast<OutputType>(rawBuf[i]);
      }
    }
  } else {
    // Sparse: decode full block into tmp, then pick selected values.
    // Simpler than per-element decode — no branching on block type.
    vector_size_t i = 0;
    while (i < numSelected) {
      const auto absRow = static_cast<uint32_t>(selectedRows[i]) +
          static_cast<uint32_t>(offset);
      const auto blockIndex = absRow / blockSize_;
      const auto blockStart = static_cast<uint32_t>(blockIndex) * blockSize_;

      vector_size_t runEnd = i + 1;
      while (runEnd < numSelected) {
        const auto nextAbsRow = static_cast<uint32_t>(selectedRows[runEnd]) +
            static_cast<uint32_t>(offset);
        if (nextAbsRow / blockSize_ != blockIndex) {
          break;
        }
        ++runEnd;
      }

      physicalType tmp[kMaxBlockSize];
      const auto numRows = blockRowCount(blockIndex);
      materializeBlockRange(blockIndex, 0, numRows, tmp);
      for (vector_size_t j = i; j < runEnd; ++j) {
        const auto blockOffset = static_cast<uint32_t>(selectedRows[j]) +
            static_cast<uint32_t>(offset) - blockStart;
        values[j] = static_cast<OutputType>(tmp[blockOffset]);
      }
      i = runEnd;
    }
  }

  row_ += selectedRows[numSelected - 1] - currentRow + 1;

  if constexpr (!kScatter && !V::kHasFilter && !V::kHasHook) {
    visitor.addNumValues(numRows);
    visitor.setRowIndex(visitor.numRows());
    return;
  }

  if constexpr (!V::kHasHook) {
    values = reinterpret_cast<OutputType*>(visitor.reader().rawValues());
  }

  auto numValues = visitor.reader().numValues();
  int32_t* filterHits = nullptr;
  if constexpr (V::kHasFilter) {
    filterHits = visitor.outputRows(numSelected) - numValues;
  }

  velox::dwio::common::
      processFixedWidthRun<OutputType, V::kFilterOnly, kScatter, V::dense>(
          velox::RowSet(selectedRows, numSelected),
          0,
          numSelected,
          scatterRows,
          values,
          filterHits,
          numValues,
          visitor.filter(),
          visitor.hook());

  if constexpr (!V::kHasHook) {
    visitor.addNumValues(
        V::kHasFilter ? numValues - visitor.reader().numValues() : numRows);
  }
  visitor.setRowIndex(visitor.numRows());
}

template <typename T>
std::string_view BlockBitPackingEncoding<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer,
    const Encoding::Options& options) {
  const bool useVarint = options.useVarintRowCount;
  static_assert(
      std::is_unsigned_v<physicalType>, "Physical type must be unsigned.");

  const uint16_t blockSize = selection.blockBitPackingBlockSize();
  const auto rowCount = static_cast<uint32_t>(values.size());
  const uint32_t numBlocks = (rowCount + blockSize - 1) / blockSize;
  NIMBLE_CHECK_LE(
      numBlocks,
      std::numeric_limits<uint16_t>::max(),
      "Row count too large for BlockBitPacking encoding.");

  struct BlockInfo {
    physicalType baseline;
    uint8_t bitWidth;
    uint32_t packedSize;
    uint32_t start;
    uint32_t count;
    bool skipEncoding;
  };

  Vector<BlockInfo> blocks{&buffer.getMemoryPool()};
  blocks.resize(numBlocks);

  uint32_t totalPackedSize = 0;
  for (uint16_t blockIndex = 0; blockIndex < numBlocks; ++blockIndex) {
    const auto start = static_cast<uint32_t>(blockIndex) * blockSize;
    const auto end = std::min(start + blockSize, rowCount);
    const auto count = end - start;

    auto blockValues = values.subspan(start, count);
    auto [minIt, maxIt] =
        std::minmax_element(blockValues.begin(), blockValues.end());
    const auto minVal = *minIt;
    const auto maxVal = *maxIt;

    const auto range = maxVal - minVal;
    const auto rawSize = count * sizeof(physicalType);

    const auto bitsRequired =
        (range == 0) ? 0 : velox::bits::bitsRequired(range);
    NIMBLE_DCHECK_LE(
        bitsRequired,
        sizeof(physicalType) * 8,
        "bitsRequired cannot exceed type width.");
    if (bitsRequired == sizeof(physicalType) * 8) {
      blocks[blockIndex] = {
          0, 0, static_cast<uint32_t>(rawSize), start, count, true};
      totalPackedSize += blocks[blockIndex].packedSize;
      continue;
    }

    // Skip encoding when packing doesn't reduce size.
    const auto packedSize =
        static_cast<uint32_t>(FixedBitArray::bufferSize(count, bitsRequired));
    if (packedSize >= rawSize) {
      blocks[blockIndex] = {
          0, 0, static_cast<uint32_t>(rawSize), start, count, true};
    } else {
      blocks[blockIndex] = {
          minVal,
          static_cast<uint8_t>(bitsRequired),
          packedSize,
          start,
          count,
          false};
    }
    totalPackedSize += blocks[blockIndex].packedSize;
  }

  const uint32_t metaOverhead = encodingOverhead(numBlocks);

  // Note: if totalPackedSize + metaOverhead >= rawSize, this encoding is not
  // beneficial. The encoding selection policy should avoid selecting it in that
  // case. We still encode correctly so the round-trip contract holds.

  Vector<char> packedVector{&buffer.getMemoryPool()};
  auto dataCompressionPolicy = selection.compressionPolicy();
  CompressionEncoder<T> compressionEncoder{
      buffer.getMemoryPool(),
      *dataCompressionPolicy,
      DataType::Undefined,
      /*bitWidth=*/8,
      /*uncompressedSize=*/totalPackedSize,
      [&]() {
        packedVector.resize(totalPackedSize);
        return std::span<char>{packedVector};
      },
      [&](char*& pos) {
        std::memset(pos, 0, totalPackedSize);
        uint32_t dataOffset = 0;
        for (uint16_t blockIndex = 0; blockIndex < numBlocks; ++blockIndex) {
          const auto& block = blocks[blockIndex];

          if (block.skipEncoding) {
            std::memcpy(
                pos + dataOffset,
                values.data() + block.start,
                block.count * sizeof(physicalType));
          } else if (block.bitWidth > 0) {
            if constexpr (sizeof(physicalType) == 4) {
              constexpr uint32_t kGroupSize = 32;
              const auto bitWidth = block.bitWidth;
              const auto baseline32 = static_cast<uint32_t>(block.baseline);
              auto* dst = reinterpret_cast<uint32_t*>(pos + dataOffset);
              const auto* src = reinterpret_cast<const uint32_t*>(
                  values.data() + block.start);

              const auto fullGroups = block.count / kGroupSize;
              const auto remainder = block.count % kGroupSize;

              uint32_t tmp[kGroupSize];
              for (uint32_t group = 0; group < fullGroups; ++group) {
                for (uint32_t i = 0; i < kGroupSize; ++i) {
                  tmp[i] = src[group * kGroupSize + i] - baseline32;
                }
                velox::fastpforlib::fastpack(tmp, dst, bitWidth);
                dst += bitWidth;
              }
              if (remainder > 0) {
                const auto remainderOffset =
                    fullGroups * kGroupSize * bitWidth / 8;
                FixedBitArray fba(
                    pos + dataOffset + remainderOffset, block.bitWidth);
                for (uint32_t i = 0; i < remainder; ++i) {
                  fba.set(
                      i,
                      values[block.start + fullGroups * kGroupSize + i] -
                          block.baseline);
                }
              }
            } else if constexpr (sizeof(physicalType) < 4) {
              FixedBitArray fba(pos + dataOffset, block.bitWidth);
              uint32_t tmp[kMaxBlockSize];
              for (uint32_t i = 0; i < block.count; ++i) {
                tmp[i] = static_cast<uint32_t>(values[block.start + i]);
              }
              fba.bulkSet32WithBaseline(
                  0, block.count, tmp, static_cast<uint32_t>(block.baseline));
            } else {
              FixedBitArray fba(pos + dataOffset, block.bitWidth);
              for (uint32_t i = 0; i < block.count; ++i) {
                fba.set(i, values[block.start + i] - block.baseline);
              }
            }
          }
          dataOffset += block.packedSize;
        }
        pos += totalPackedSize;
        return pos;
      }};

  const uint32_t encodingSize =
      Encoding::serializePrefixSize(rowCount, useVarint) + metaOverhead +
      compressionEncoder.getSize();

  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::BlockBitPacking,
      TypeTraits<T>::dataType,
      rowCount,
      useVarint,
      pos);
  encoding::writeChar(
      static_cast<char>(compressionEncoder.compressionType()), pos);
  encoding::write(blockSize, pos);
  encoding::write(static_cast<uint16_t>(numBlocks), pos);

  uint32_t runningOffset = 0;
  for (uint16_t blockIndex = 0; blockIndex < numBlocks; ++blockIndex) {
    encoding::write(blocks[blockIndex].baseline, pos);
    const uint8_t bwOnDisk =
        blocks[blockIndex].skipEncoding ? 255 : blocks[blockIndex].bitWidth;
    encoding::writeChar(static_cast<char>(bwOnDisk), pos);
  }
  for (uint16_t blockIndex = 0; blockIndex < numBlocks; ++blockIndex) {
    encoding::write(runningOffset, pos);
    runningOffset += blocks[blockIndex].packedSize;
  }

  compressionEncoder.write(pos);

  NIMBLE_DCHECK_EQ(encodingSize, pos - reserved, "Encoding size mismatch.");
  return {reserved, encodingSize};
}

template <typename T>
std::string BlockBitPackingEncoding<T>::debugString(int offset) const {
  return fmt::format(
      "{}{}<{}> rowCount={} blockSize={} numBlocks={}",
      std::string(offset, ' '),
      toString(Encoding::encodingType()),
      toString(Encoding::dataType()),
      Encoding::rowCount(),
      blockSize_,
      numBlocks_);
}

} // namespace facebook::nimble
