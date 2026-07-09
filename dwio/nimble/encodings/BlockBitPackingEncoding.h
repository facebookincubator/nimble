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
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/common/EncodingType.h"
#include "dwio/nimble/encodings/selection/EncodingIdentifier.h"
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

  /// Maximum rows in one block; bounded so stack decode buffers stay fixed.
  static constexpr uint16_t kMaxBlockSize = kBlockBitPackingBlockSize;
  /// Serialized bit-width marker for blocks stored as raw physical values.
  static constexpr uint8_t kRawBlockBitWidth{255};

  BlockBitPackingEncoding(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      const std::function<void*(uint32_t)>& stringBufferFactory,
      const Encoding::Options& options = {});

  ~BlockBitPackingEncoding() override {
    this->releaseBuffer(uncompressedData_);
    this->releaseVectorBuffer(buffer_);
    this->releaseVectorBuffer(bitWidths_);
    this->releaseVectorBuffer(baselines_);
    this->releaseVectorBuffer(blockOffsets_);
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

  /// Estimates the uncompressed encoded size using the same per-block packing
  /// decisions as encode(). The per-block metadata is routed through nested
  /// encoding selection (data-dependent size), so it is estimated as Trivial
  /// sub-encodings; the estimate is approximate, not exact.
  static uint64_t estimateSize(
      std::span<const physicalType> values,
      uint16_t blockSize = kBlockBitPackingBlockSize);

  static uint64_t estimateSize(
      const Statistics<physicalType>& statistics,
      uint16_t blockSize = kBlockBitPackingBlockSize) {
    const auto& blocks = statistics.minMaxBlocks(blockSize);
    const auto numBlocks = static_cast<uint32_t>(blocks.size());

    uint64_t packedSize = 0;
    for (const auto& block : blocks) {
      const auto rawSize = block.count * sizeof(physicalType);
      const auto range = block.max - block.min;
      const auto bw = range == 0 ? 0 : velox::bits::bitsRequired(range);
      if (bw >= sizeof(physicalType) * 8) {
        packedSize += rawSize;
        continue;
      }
      const auto blockPackedSize =
          static_cast<uint64_t>(FixedBitArray::bufferSize(block.count, bw));
      if (blockPackedSize < rawSize) {
        packedSize += blockPackedSize;
      } else {
        packedSize += rawSize;
      }
    }

    const uint64_t metadataSize = kMetadataHeaderSize +
        TrivialEncoding<physicalType>::estimateSize(numBlocks) +
        TrivialEncoding<uint8_t>::estimateSize(numBlocks) +
        TrivialEncoding<uint32_t>::estimateSize(numBlocks);
    return EncodingPrefix::kFixedPrefixSize + metadataSize + packedSize;
  }

  std::string debugString(int offset) const final;

 private:
  // Fixed metadata header written before the three nested sub-streams:
  // compressionType + blockSize + numBlocks + three 4-byte sub-stream size
  // prefixes. Shared by encode() and estimateSize() so the two stay in sync.
  static constexpr uint32_t kMetadataHeaderSize =
      sizeof(uint8_t) /*compressionType=*/ + sizeof(uint16_t) /*blockSize=*/ +
      sizeof(uint16_t) /*numBlocks=*/ +
      3 * sizeof(uint32_t) /*subStreamSizePrefixes=*/;

  // Per-block encoding plan used while serializing and estimating size.
  struct BlockInfo {
    physicalType baseline;
    uint8_t bitWidth;
    uint32_t packedSize;
    uint32_t start;
    uint32_t count;
    bool skipEncoding;
  };

  // Reads a single decoded value at the given absolute row index.
  physicalType readSingleValue(uint32_t row) const;

  // Decodes a contiguous range within one block into 'output'.
  void materializeBlockRange(
      uint32_t blockIndex,
      uint32_t blockValueOffset,
      uint32_t blockValueCount,
      physicalType* output) const;

  // Returns the number of rows in the given block (may be less than
  // blockSize_ for the last block).
  uint32_t blockRowCount(uint32_t blockIndex) const;

  // Unpacks 'numRows' bit-packed values in fixed-size groups, falling
  // back to FixedBitArray for any trailing remainder.
  static void fullUnpack(
      const uint8_t* input,
      physicalType* output,
      uint32_t numRows,
      uint8_t bitWidth,
      physicalType baseline);

  // Builds the encoding plan for a single block.
  static BlockInfo makeBlockInfo(
      std::span<const physicalType> values,
      uint32_t start,
      uint32_t count);

  uint16_t blockSize_;
  uint16_t numBlocks_;
  Vector<physicalType> baselines_;
  Vector<uint8_t> bitWidths_;
  Vector<uint32_t> blockOffsets_;
  const char* packedData_;
  uint32_t row_ = 0;
  velox::BufferPtr uncompressedData_;
  Vector<physicalType> buffer_;
};

//
// End of class declaration. Implementations follow.
//

template <typename T>
uint64_t BlockBitPackingEncoding<T>::estimateSize(
    std::span<const physicalType> values,
    uint16_t blockSize) {
  const auto rowCount = static_cast<uint32_t>(values.size());
  const uint32_t numBlocks = velox::bits::divRoundUp(rowCount, blockSize);
  uint64_t packedSize = 0;
  for (uint32_t blockIndex = 0; blockIndex < numBlocks; ++blockIndex) {
    const auto start = blockIndex * blockSize;
    const auto end = std::min<uint32_t>(start + blockSize, rowCount);
    packedSize += makeBlockInfo(values, start, end - start).packedSize;
  }
  // Per-block metadata (baselines, bit widths, data offsets) is stored as
  // nested encodings; dumping real files shows selection picks Trivial for all
  // three, so estimate them as Trivial sub-encodings -- mirroring how
  // Dictionary estimates its alphabet child. This is an approximation (the
  // actual nested encoding is data-dependent); the estimate-vs-actual test
  // allows a 2x band.
  const uint64_t metadataSize = kMetadataHeaderSize +
      TrivialEncoding<physicalType>::estimateSize(numBlocks) +
      TrivialEncoding<uint8_t>::estimateSize(numBlocks) +
      TrivialEncoding<uint32_t>::estimateSize(numBlocks);
  return EncodingPrefix::kFixedPrefixSize + metadataSize + packedSize;
}

template <typename T>
typename BlockBitPackingEncoding<T>::BlockInfo
BlockBitPackingEncoding<T>::makeBlockInfo(
    std::span<const physicalType> values,
    uint32_t start,
    uint32_t count) {
  auto blockValues = values.subspan(start, count);
  auto [minValue, maxValue] =
      std::minmax_element(blockValues.begin(), blockValues.end());
  const auto range = *maxValue - *minValue;
  const auto rawSize = count * sizeof(physicalType);

  const auto bitsRequired = range == 0 ? 0 : velox::bits::bitsRequired(range);
  NIMBLE_DCHECK_LE(
      bitsRequired,
      sizeof(physicalType) * 8,
      "bitsRequired cannot exceed type width.");
  if (bitsRequired == sizeof(physicalType) * 8) {
    return {0, 0, static_cast<uint32_t>(rawSize), start, count, true};
  }

  // Skip encoding when packing doesn't reduce size.
  const auto packedSize =
      static_cast<uint32_t>(FixedBitArray::bufferSize(count, bitsRequired));
  if (packedSize >= rawSize) {
    return {0, 0, static_cast<uint32_t>(rawSize), start, count, true};
  }

  return {
      *minValue,
      static_cast<uint8_t>(bitsRequired),
      packedSize,
      start,
      count,
      false};
}

template <typename T>
BlockBitPackingEncoding<T>::BlockBitPackingEncoding(
    velox::memory::MemoryPool& pool,
    std::string_view data,
    const std::function<void*(uint32_t)>& stringBufferFactory,
    const Encoding::Options& options)
    : TypedEncoding<T, physicalType>{pool, data, options},
      baselines_{this->template getVectorBuffer<physicalType>()},
      bitWidths_{this->template getVectorBuffer<uint8_t>()},
      blockOffsets_{this->template getVectorBuffer<uint32_t>()},
      packedData_{nullptr},
      buffer_{&pool} {
  auto pos = data.data() + this->dataOffset();
  auto compressionType = static_cast<CompressionType>(encoding::readChar(pos));
  blockSize_ = encoding::read<const uint16_t>(pos);
  numBlocks_ = encoding::read<const uint16_t>(pos);

  NIMBLE_CHECK(blockSize_ > 0 && blockSize_ <= kMaxBlockSize);
  NIMBLE_CHECK_EQ(numBlocks_, (this->rowCount() + blockSize_ - 1) / blockSize_);

  baselines_.resize(numBlocks_);
  bitWidths_.resize(numBlocks_);
  blockOffsets_.resize(numBlocks_);

  // BlockBitPacking stores the per-block metadata (baselines, bit widths, data
  // offsets) as self-describing nested encodings, letting Nimble's recursive
  // encoding selection pick the best encoding for each metadata stream. The
  // packed block data stays raw.
  // Each metadata stream: read its 4-byte size, decode the nested sub-encoding
  // into the target, then advance past it.
  auto readMetadataStream = [&](auto& subStream) {
    const uint32_t size = encoding::readUint32(pos);
    EncodingFactory(options)
        .create(pool, {pos, size}, stringBufferFactory)
        ->materialize(numBlocks_, subStream.data());
    pos += size;
  };
  readMetadataStream(baselines_); // per-block baselines
  readMetadataStream(bitWidths_); // per-block bit widths
  readMetadataStream(blockOffsets_); // per-block data offsets

  if (compressionType != CompressionType::Uncompressed) {
    uncompressedData_ = Compression::uncompress(
        pool,
        compressionType,
        DataType::Undefined,
        {pos, static_cast<size_t>(data.end() - pos)},
        options.decompressCounter(),
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
  const auto bitWidth = bitWidths_[blockIndex];
  const auto baseline = baselines_[blockIndex];
  if (bitWidth == kRawBlockBitWidth) {
    const auto* rawValues = reinterpret_cast<const physicalType*>(
        packedData_ + blockOffsets_[blockIndex]);
    return rawValues[blockOffset];
  }
  if (bitWidth == 0) {
    return baseline;
  }
  const auto numRows = blockRowCount(blockIndex);
  FixedBitArray fba{
      {packedData_ + blockOffsets_[blockIndex],
       FixedBitArray::bufferSize(numRows, bitWidth)},
      bitWidth};
  return static_cast<physicalType>(fba.get(blockOffset)) + baseline;
}

template <typename T>
void BlockBitPackingEncoding<T>::materializeBlockRange(
    uint32_t blockIndex,
    uint32_t blockValueOffset,
    uint32_t blockValueCount,
    physicalType* output) const {
  const auto bitWidth = bitWidths_[blockIndex];
  const auto baseline = baselines_[blockIndex];
  const auto* blockData = packedData_ + blockOffsets_[blockIndex];

  if (bitWidth == kRawBlockBitWidth) {
    const auto* rawValues = reinterpret_cast<const physicalType*>(blockData);
    std::memcpy(
        output,
        rawValues + blockValueOffset,
        blockValueCount * sizeof(physicalType));
    return;
  }

  if (bitWidth == 0) {
    std::fill(output, output + blockValueCount, baseline);
    return;
  }

  const auto* inputBytes = reinterpret_cast<const uint8_t*>(blockData);

  if (blockValueOffset == 0) {
    fullUnpack(inputBytes, output, blockValueCount, bitWidth, baseline);
  } else {
    physicalType tmp[kMaxBlockSize];
    fullUnpack(
        inputBytes,
        tmp,
        blockValueOffset + blockValueCount,
        bitWidth,
        baseline);
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
  constexpr bool kIsWideType =
      (isFourByteIntegralType<physicalType>() ||
       isEightByteIntegralType<physicalType>());
  constexpr bool kIsNarrowType =
      std::is_integral_v<physicalType> && !kIsWideType;
  constexpr bool kIsFluidCast = sizeof(OutputType) >= sizeof(physicalType) &&
      std::is_integral_v<OutputType> && std::is_integral_v<physicalType>;
  // Wide types (4/8 byte): full fast path with filter/scatter/hook support.
  // Narrow types (1/2 byte): fast path only without filter/scatter/hook AND
  // without nulls, because processFixedWidthRun requires >= 4-byte types
  // and the scatter path (used for nullable columns) calls it.
  constexpr bool kCanUseFastPath =
      (kIsWideType || (kIsNarrowType && !V::kHasFilter && !V::kHasHook));
  if constexpr (
      kCanUseFastPath &&
      std::is_same_v<
          typename V::Extract,
          velox::dwio::common::ExtractToReader> &&
      kIsFluidCast) {
    auto* nulls = visitor.reader().rawNullsInReadRange();
    if (velox::dwio::common::useFastPath(visitor, nulls) &&
        (kIsWideType || nulls == nullptr)) {
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
      if (buffer_.capacity() == 0) {
        buffer_ = this->template getVectorBuffer<physicalType>();
      }
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
    // Sparse: per-block strategy based on block type and selectivity:
    //  - skipEncoding: direct array access, no decode
    //  - bitWidth == 0: constant block, return baseline
    //  - low selectivity (<5%): FBA per-element for selected rows only
    //  - high selectivity: full block decode, pick rows
    constexpr uint32_t kMaxFbaSelectivityPct = 5;

    vector_size_t i = 0;
    while (i < numSelected) {
      const auto absRow = static_cast<uint32_t>(selectedRows[i]) +
          static_cast<uint32_t>(offset);
      const auto blockIndex = absRow / blockSize_;
      const auto blockStart = static_cast<uint32_t>(blockIndex) * blockSize_;
      const auto bitWidth = bitWidths_[blockIndex];
      const auto baseline = baselines_[blockIndex];

      // Find all selected rows belonging to this block.
      vector_size_t runEnd = i + 1;
      while (runEnd < numSelected) {
        const auto nextAbsRow = static_cast<uint32_t>(selectedRows[runEnd]) +
            static_cast<uint32_t>(offset);
        if (nextAbsRow / blockSize_ != blockIndex) {
          break;
        }
        ++runEnd;
      }

      const auto runLength = static_cast<uint32_t>(runEnd - i);
      const auto numBlockRows = blockRowCount(blockIndex);

      // Raw: direct index, no decode.
      if (bitWidth == kRawBlockBitWidth) {
        const auto* rawValues = reinterpret_cast<const physicalType*>(
            packedData_ + blockOffsets_[blockIndex]);
        for (vector_size_t j = i; j < runEnd; ++j) {
          const auto blockOffset = static_cast<uint32_t>(selectedRows[j]) +
              static_cast<uint32_t>(offset) - blockStart;
          values[j] = static_cast<OutputType>(rawValues[blockOffset]);
        }
        // Constant: every value equals baseline.
      } else if (bitWidth == 0) {
        for (vector_size_t j = i; j < runEnd; ++j) {
          values[j] = static_cast<OutputType>(baseline);
        }
      } else if (runLength * 100 < numBlockRows * kMaxFbaSelectivityPct) {
        // Low selectivity: FBA per-element decode for selected rows only.
        FixedBitArray fba{
            {packedData_ + blockOffsets_[blockIndex],
             FixedBitArray::bufferSize(numBlockRows, bitWidth)},
            bitWidth};
        for (vector_size_t j = i; j < runEnd; ++j) {
          const auto blockOffset = static_cast<uint32_t>(selectedRows[j]) +
              static_cast<uint32_t>(offset) - blockStart;
          values[j] = static_cast<OutputType>(
              static_cast<physicalType>(fba.get(blockOffset)) + baseline);
        }
      } else {
        // High selectivity: full block decode, pick rows.
        physicalType tmp[kMaxBlockSize];
        materializeBlockRange(blockIndex, 0, numBlockRows, tmp);
        for (vector_size_t j = i; j < runEnd; ++j) {
          const auto blockOffset = static_cast<uint32_t>(selectedRows[j]) +
              static_cast<uint32_t>(offset) - blockStart;
          values[j] = static_cast<OutputType>(tmp[blockOffset]);
        }
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

  // processFixedWidthRun requires >= 4-byte OutputType. Narrow types
  // with filters/hooks are routed to the slow path by readWithVisitor,
  // so this branch is unreachable for them.
  if constexpr (sizeof(OutputType) >= 4) {
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
  } else {
    NIMBLE_UNREACHABLE(
        "Narrow-type bulkScan with filter/hook should use the slow path.");
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

  const uint16_t blockSize = options.blockBitPackingBlockSize;
  const auto rowCount = static_cast<uint32_t>(values.size());
  const uint32_t numBlocks = velox::bits::divRoundUp(rowCount, blockSize);
  NIMBLE_CHECK_LE(
      numBlocks,
      std::numeric_limits<uint16_t>::max(),
      "Row count too large for BlockBitPacking encoding.");

  Vector<BlockInfo> blocks{&buffer.getMemoryPool()};
  blocks.resize(numBlocks);

  uint32_t totalPackedSize = 0;
  for (uint16_t blockIndex = 0; blockIndex < numBlocks; ++blockIndex) {
    const auto start = static_cast<uint32_t>(blockIndex) * blockSize;
    const auto end = std::min(start + blockSize, rowCount);
    blocks[blockIndex] = makeBlockInfo(values, start, end - start);
    totalPackedSize += blocks[blockIndex].packedSize;
  }

  // Note: if the encoded size (packed data + nested metadata sub-streams +
  // header) >= rawSize, this encoding is not beneficial. The encoding selection
  // policy should avoid selecting it in that case. We still encode correctly so
  // the round-trip contract holds.
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
            } else {
              // 1-, 2-, and 8-byte types: the unified bulk path picks the
              // optimal packing for the width (4-byte uses SIMD fastpack
              // above).
              FixedBitArray fba(pos + dataOffset, block.bitWidth);
              fba.bulkSetWithBaseline(
                  0, block.count, values.data() + block.start, block.baseline);
            }
          }
          dataOffset += block.packedSize;
        }
        pos += totalPackedSize;
        return pos;
      }};

  // BlockBitPacking routes the per-block metadata (baselines, bit widths, data
  // offsets) through recursive encoding selection so Nimble can pick the best
  // encoding for each (e.g. Constant/Delta for correlated baselines, Delta for
  // the ascending offsets). The packed block data stays raw (still optionally
  // Zstd-compressed via the compression encoder).
  Vector<physicalType> baselines{&buffer.getMemoryPool()};
  baselines.resize(numBlocks);
  Vector<uint8_t> bitWidths{&buffer.getMemoryPool()};
  bitWidths.resize(numBlocks);
  Vector<uint32_t> blockOffsets{&buffer.getMemoryPool()};
  blockOffsets.resize(numBlocks);
  uint32_t runningOffset{0};
  for (uint16_t blockIndex = 0; blockIndex < numBlocks; ++blockIndex) {
    baselines[blockIndex] = blocks[blockIndex].baseline;
    bitWidths[blockIndex] = blocks[blockIndex].skipEncoding
        ? kRawBlockBitWidth
        : blocks[blockIndex].bitWidth;
    blockOffsets[blockIndex] = runningOffset;
    runningOffset += blocks[blockIndex].packedSize;
  }

  Buffer tempBuffer{buffer.getMemoryPool()};
  const std::string_view baselinesEncoded =
      selection.template encodeNested<physicalType>(
          EncodingIdentifiers::BlockBitPacking::Baselines,
          baselines,
          tempBuffer,
          options);
  const std::string_view bitWidthsEncoded =
      selection.template encodeNested<uint8_t>(
          EncodingIdentifiers::BlockBitPacking::BitWidths,
          bitWidths,
          tempBuffer,
          options);
  const std::string_view offsetsEncoded =
      selection.template encodeNested<uint32_t>(
          EncodingIdentifiers::BlockBitPacking::Offsets,
          blockOffsets,
          tempBuffer,
          options);

  const uint32_t metaHeaderSize = kMetadataHeaderSize +
      static_cast<uint32_t>(baselinesEncoded.size() + bitWidthsEncoded.size() +
                            offsetsEncoded.size());
  const uint32_t encodingSize =
      Encoding::serializePrefixSize(rowCount, useVarint) + metaHeaderSize +
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
  encoding::writeString(baselinesEncoded, pos);
  encoding::writeString(bitWidthsEncoded, pos);
  encoding::writeString(offsetsEncoded, pos);
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
