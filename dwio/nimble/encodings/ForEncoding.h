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
#include <type_traits>
#include <vector>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/compression/Compression.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/common/EncodingType.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"

// Frame of Reference encoding. Divides data into fixed-size frames, storing
// a reference value per frame and bit-packing the exceptions (value - ref).
// Supports O(1) random access when BitOffsets are enabled.
//
// Data layout is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// 1 byte: compression type
// 4 bytes: frame size
// 4 bytes: number of frames
// 1 byte: enableBitOffsets flag
// XX bytes: BitWidths array encoding (nested)
// YY bytes: References array encoding (nested)
// ZZ bytes: BitOffsets array encoding (nested, optional)
// WW bytes: bit-packed exceptions data

namespace facebook::nimble {

template <typename T>
class ForEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  static const int kCompressionOffset = Encoding::kPrefixSize;
  static const int kFrameSizeOffset = kCompressionOffset + 1;
  static const int kNumFramesOffset = kFrameSizeOffset + 4;
  static const int kEnableBitOffsetsOffset = kNumFramesOffset + 4;
  static const int kPrefixSize = kEnableBitOffsetsOffset + 1;

  ForEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory = nullptr,
      const Encoding::Options& options = {});

  ~ForEncoding() override {
    this->releaseBuffer(uncompressedData_);
  }

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

#ifdef NIMBLE_ENABLE_EXPERIMENTAL_ENCODINGS
  /// Statistics-only size estimate for general encoding selection (e.g. as a
  /// SubIntSplit segment candidate), where only `Statistics<physicalType>` --
  /// not the raw values -- is available. The local (per-frame) bit width is
  /// estimated from the average step size scaled to the frame size -- a
  /// random-walk heuristic where the local range over a frame of
  /// `kFrameSize` steps grows roughly with avgAbsDelta -- capped by the
  /// overall range. Per-frame metadata streams are estimated as Trivial,
  /// matching encode()'s frame size of 128.
  static uint64_t estimateSize(
      uint64_t rowCount,
      const Statistics<physicalType>& statistics) {
    if (rowCount == 0) {
      return EncodingPrefix::kFixedPrefixSize;
    }
    constexpr uint32_t kFrameSize = 128;
    const auto numFrames =
        static_cast<uint32_t>(velox::bits::divRoundUp(rowCount, kFrameSize));

    const auto fullRange =
        static_cast<uint64_t>(statistics.max() - statistics.min());
    const double avgAbsDelta = rowCount > 1
        ? static_cast<double>(fullRange) / static_cast<double>(rowCount - 1)
        : 0.0;
    const double localRange = std::min(
        static_cast<double>(fullRange),
        avgAbsDelta * static_cast<double>(kFrameSize) / 2.0);
    const uint8_t localBits = localRange < 1.0
        ? uint8_t{0}
        : static_cast<uint8_t>(
              velox::bits::bitsRequired(static_cast<uint64_t>(localRange)));

    const uint64_t packedSize = FixedBitArray::bufferSize(rowCount, localBits);
    const uint64_t bitWidthsSize =
        TrivialEncoding<uint8_t>::estimateSize(numFrames);
    const uint64_t referencesSize =
        TrivialEncoding<physicalType>::estimateSize(numFrames);
    const uint64_t bitOffsetsSize =
        TrivialEncoding<uint64_t>::estimateSize(numFrames);

    // EncodingPrefix::kFixedPrefixSize(6) + FOR-specific fixed fields
    // (compressionType + frameSize + numFrames + enableBitOffsets = 10),
    // plus each of the four sub-streams' 4-byte size prefix.
    return EncodingPrefix::kFixedPrefixSize +
        (kPrefixSize - Encoding::kPrefixSize) + 4 + bitWidthsSize + 4 +
        referencesSize + 4 + bitOffsetsSize + 4 + packedSize;
  }
#endif

  std::string debugString(int offset) const final;

 private:
  struct FrameInfo {
    physicalType reference;
    uint8_t bitWidth;
    uint64_t bitOffset;
    uint32_t size;
  };

  uint32_t frameSize_;
  uint32_t numFrames_;
  bool enableBitOffsets_;
  Vector<FrameInfo> frames_;
  const char* packedData_;
  uint32_t currentRow_;
  velox::BufferPtr uncompressedData_;
  Vector<physicalType> buffer_;

  uint32_t getFrameIndex(uint32_t row) const {
    return row / frameSize_;
  }

  uint32_t getPositionInFrame(uint32_t row) const {
    return row % frameSize_;
  }

  void decodeRange(uint32_t startRow, uint32_t rowCount, physicalType* output)
      const;

  physicalType decodeValue(uint32_t row) const;
};
//
// Implementation
//

template <typename T>
ForEncoding<T>::ForEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    std::function<void*(uint32_t)> stringBufferFactory,
    const Encoding::Options& options)
    : TypedEncoding<T, physicalType>{memoryPool, data, options},
      frames_{&memoryPool},
      currentRow_(0),
      buffer_{&memoryPool} {
  static_assert(
      std::is_integral_v<physicalType>,
      "ForEncoding only supports integral types");

  const char* pos = data.data() + kCompressionOffset;
  const EncodingFactory encodingFactory(options);
  const auto nullStringBufferFactory = [](uint32_t /*size*/) -> void* {
    return nullptr;
  };
  const auto& decodeStringBufferFactory =
      stringBufferFactory ? stringBufferFactory : nullStringBufferFactory;

  CompressionType compressionType =
      static_cast<CompressionType>(encoding::readChar(pos));

  frameSize_ = encoding::readUint32(pos);
  numFrames_ = encoding::readUint32(pos);
  enableBitOffsets_ = static_cast<bool>(encoding::readChar(pos));

  // Read BitWidths array
  const uint32_t bitWidthsSize = encoding::readUint32(pos);
  auto bitWidthsEncoding = encodingFactory.create(
      *this->pool_,
      std::string_view(pos, bitWidthsSize),
      decodeStringBufferFactory);
  pos += bitWidthsSize;

  Vector<uint8_t> bitWidths(&memoryPool, numFrames_);
  bitWidthsEncoding->materialize(numFrames_, bitWidths.data());

  // Read References array
  const uint32_t referencesSize = encoding::readUint32(pos);
  auto referencesEncoding = encodingFactory.create(
      *this->pool_,
      std::string_view(pos, referencesSize),
      decodeStringBufferFactory);
  pos += referencesSize;

  Vector<physicalType> references(&memoryPool, numFrames_);
  referencesEncoding->materialize(numFrames_, references.data());

  // Read BitOffsets array if enabled
  Vector<uint64_t> bitOffsets(&memoryPool);
  if (enableBitOffsets_) {
    const uint32_t bitOffsetsSize = encoding::readUint32(pos);
    auto bitOffsetsEncoding = encodingFactory.create(
        *this->pool_,
        std::string_view(pos, bitOffsetsSize),
        decodeStringBufferFactory);
    pos += bitOffsetsSize;

    bitOffsets.resize(numFrames_);
    bitOffsetsEncoding->materialize(numFrames_, bitOffsets.data());
  }

  const uint32_t packedDataSize = encoding::readUint32(pos);
  std::string_view packedDataView{pos, packedDataSize};

  if (compressionType != CompressionType::Uncompressed) {
    uncompressedData_ = Compression::uncompress(
        *this->pool_, compressionType, DataType::Undefined, packedDataView);
    packedData_ = uncompressedData_->as<char>();
  } else {
    packedData_ = packedDataView.data();
  }

  // Build frame metadata
  frames_.reserve(numFrames_);
  uint64_t cumulativeBitOffset = 0;

  for (uint32_t i = 0; i < numFrames_; ++i) {
    FrameInfo frame;
    frame.reference = references[i];
    frame.bitWidth = bitWidths[i];
    frame.bitOffset = enableBitOffsets_ ? bitOffsets[i] : cumulativeBitOffset;

    uint32_t startRow = i * frameSize_;
    uint32_t endRow = std::min(startRow + frameSize_, this->rowCount_);
    frame.size = endRow - startRow;

    frames_.push_back(frame);

    // Update cumulative offset for next frame
    if (!enableBitOffsets_) {
      cumulativeBitOffset += frame.size * frame.bitWidth;
    }
  }
}

template <typename T>
void ForEncoding<T>::reset() {
  currentRow_ = 0;
}

template <typename T>
void ForEncoding<T>::skip(uint32_t rowCount) {
  currentRow_ += rowCount;
  NIMBLE_DCHECK(
      currentRow_ <= this->rowCount_, "Skipping past end of encoding");
}

template <typename T>
void ForEncoding<T>::decodeRange(
    uint32_t startRow,
    uint32_t rowCount,
    physicalType* output) const {
  NIMBLE_DCHECK(
      startRow + rowCount <= this->rowCount_, "Decoding past end of encoding");

  // Streaming bit-cursor decoder: reads rowsToDecode values of frame.bitWidth
  // bits from byteCursor at bitOffsetInByte, writing results via
  // decodeException.
  auto decodeBitStream = [](const uint8_t* byteCursor,
                            uint8_t bitOffsetInByte,
                            uint8_t bitWidth,
                            uint32_t rowsToDecode,
                            auto&& decodeException) {
    uint64_t bitBuffer = 0;
    uint32_t bitsInBuffer = 0;

    // Consume the partial first byte if not byte-aligned
    if (bitOffsetInByte != 0) {
      bitBuffer = static_cast<uint8_t>(*byteCursor) >> bitOffsetInByte;
      bitsInBuffer = 8u - bitOffsetInByte;
      ++byteCursor;
    }

    const uint64_t mask = (bitWidth == 64) ? ~0ULL : ((1ULL << bitWidth) - 1);

    for (uint32_t i = 0; i < rowsToDecode; ++i) {
      while (bitsInBuffer < bitWidth) {
        bitBuffer |= static_cast<uint64_t>(static_cast<uint8_t>(*byteCursor))
            << bitsInBuffer;
        ++byteCursor;
        bitsInBuffer += 8;
      }
      decodeException(i, bitBuffer & mask);
      bitBuffer >>= bitWidth;
      bitsInBuffer -= bitWidth;
    }
  };

  uint32_t currentRow = startRow;
  uint32_t outputOffset = 0;
  uint32_t remainingRowCount = rowCount;

  while (remainingRowCount > 0) {
    const uint32_t frameIndex = getFrameIndex(currentRow);
    const auto& frame = frames_[frameIndex];
    const uint32_t positionInFrame = getPositionInFrame(currentRow);
    const uint32_t rowsAvailableInFrame = frame.size - positionInFrame;
    const uint32_t rowsToDecode =
        std::min(remainingRowCount, rowsAvailableInFrame);

    // Per-frame reference application — captures frame by ref
    auto decodeException = [&](uint32_t i, uint64_t residual) {
      if constexpr (std::is_signed_v<physicalType>) {
        output[outputOffset + i] = static_cast<physicalType>(
            static_cast<int64_t>(residual) +
            static_cast<int64_t>(frame.reference));
      } else {
        output[outputOffset + i] =
            static_cast<physicalType>(residual + frame.reference);
      }
    };

    if (frame.bitWidth == 0) {
      // All values in frame are identical to reference
      std::fill(
          output + outputOffset,
          output + outputOffset + rowsToDecode,
          frame.reference);

    } else {
      const uint64_t bitPosition = frame.bitOffset +
          static_cast<uint64_t>(positionInFrame) * frame.bitWidth;
      const uint8_t bitOffsetInByte = static_cast<uint8_t>(bitPosition & 7U);
      const uint8_t* byteCursor =
          reinterpret_cast<const uint8_t*>(packedData_) + (bitPosition / 8);

      if (bitOffsetInByte == 0) {
        // Byte-aligned: use typed loads for power-of-two widths, bit-stream
        // otherwise
        switch (frame.bitWidth) {
          case 8:
            for (uint32_t i = 0; i < rowsToDecode; ++i) {
              decodeException(i, byteCursor[i]);
            }
            break;
          case 16:
            for (uint32_t i = 0; i < rowsToDecode; ++i) {
              uint16_t v;
              std::memcpy(
                  &v, byteCursor + i * sizeof(uint16_t), sizeof(uint16_t));
              decodeException(i, v);
            }
            break;
          case 32:
            for (uint32_t i = 0; i < rowsToDecode; ++i) {
              uint32_t v;
              std::memcpy(
                  &v, byteCursor + i * sizeof(uint32_t), sizeof(uint32_t));
              decodeException(i, v);
            }
            break;
          case 64:
            for (uint32_t i = 0; i < rowsToDecode; ++i) {
              uint64_t v;
              std::memcpy(
                  &v, byteCursor + i * sizeof(uint64_t), sizeof(uint64_t));
              decodeException(i, v);
            }
            break;
          default:
            // Non-power-of-two width, byte-aligned start: bitOffsetInByte == 0
            decodeBitStream(
                byteCursor, 0, frame.bitWidth, rowsToDecode, decodeException);
            break;
        }
      } else {
        // Unaligned: always use the bit-streaming path
        decodeBitStream(
            byteCursor,
            bitOffsetInByte,
            frame.bitWidth,
            rowsToDecode,
            decodeException);
      }
    }

    currentRow += rowsToDecode;
    outputOffset += rowsToDecode;
    remainingRowCount -= rowsToDecode;
  }
}

template <typename T>
typename ForEncoding<T>::physicalType ForEncoding<T>::decodeValue(
    uint32_t row) const {
  physicalType value{};
  decodeRange(row, 1, &value);
  return value;
}

template <typename T>
void ForEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  physicalType* output = static_cast<physicalType*>(buffer);

  decodeRange(currentRow_, rowCount, output);

  currentRow_ += rowCount;
}

template <typename T>
template <typename V>
void ForEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  // Provide a skip function so readWithVisitorSlow can advance currentRow_ past
  // non-selected non-null rows when the visitor's row set is sparse (e.g. due
  // to chunk-index seeks or filter-driven row group skipping). The skip lambda
  // receives the count of non-null rows to bypass, matching how ForEncoding
  // stores only non-null values sequentially.  Using currentRow_ keeps state
  // consistent across multiple readWithVisitor calls and with interleaved
  // skip()/materialize() calls on the same instance.
  detail::readWithVisitorSlow(
      visitor,
      params,
      [&](auto numNonNullsToSkip) { skip(numNonNullsToSkip); },
      [&] { return decodeValue(currentRow_++); });
}

template <typename T>
std::string ForEncoding<T>::debugString(int offset) const {
  std::string log = Encoding::debugString(offset);
  log += fmt::format(
      "\n{}frameSize={}, numFrames={}, enableBitOffsets={}",
      std::string(offset, ' '),
      frameSize_,
      numFrames_,
      enableBitOffsets_);

  std::map<uint8_t, uint32_t> bitWidthCounts;
  for (const auto& frame : frames_) {
    bitWidthCounts[frame.bitWidth]++;
  }

  log +=
      fmt::format("\n{}bitWidth distribution: ", std::string(offset + 2, ' '));
  for (const auto& [width, count] : bitWidthCounts) {
    log += fmt::format("{}b:{} ", width, count);
  }

  return log;
}

// Static encode method
template <typename T>
std::string_view ForEncoding<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer,
    const Encoding::Options& options) {
  static_assert(
      std::is_integral_v<physicalType>,
      "ForEncoding only supports integral types");

  const bool useVarint = options.useVarintRowCount;
  const uint32_t rowCount = values.size();
  if (rowCount == 0) {
    NIMBLE_UNSUPPORTED("Cannot encode empty data with ForEncoding");
  }

  // Configuration (TODO: make configurable via policy)
  const bool enableBitOffsets = true; // Default: enable for O(1) random access
  const uint32_t frameSize =
      128; // Default frame size (TODO: adaptive selection)

  const uint32_t numFrames = (rowCount + frameSize - 1) / frameSize;

  Vector<uint8_t> bitWidths(&buffer.getMemoryPool(), numFrames);
  Vector<physicalType> references(&buffer.getMemoryPool(), numFrames);
  Vector<uint64_t> bitOffsets(&buffer.getMemoryPool());
  if (enableBitOffsets) {
    bitOffsets.resize(numFrames);
  }

  Vector<char> packedData(&buffer.getMemoryPool());
  uint64_t bitBuffer = 0;
  size_t bitBufferLen = 0;

  auto writeBits = [&](uint64_t value, uint8_t numBits) {
    size_t bitsToWrite = numBits;

    while (bitsToWrite > 0) {
      size_t spaceInBuffer = 64 - bitBufferLen;
      size_t bitsThisRound = std::min(bitsToWrite, spaceInBuffer);

      uint64_t mask =
          (bitsThisRound == 64) ? ~0ULL : ((1ULL << bitsThisRound) - 1);
      uint64_t chunk = value & mask;

      bitBuffer |= (chunk << bitBufferLen);
      bitBufferLen += bitsThisRound;

      // Shifting by the full width (64) is undefined behavior, so guard it the
      // same way as the mask computation above.
      value = (bitsThisRound == 64) ? 0 : (value >> bitsThisRound);
      bitsToWrite -= bitsThisRound;

      while (bitBufferLen >= 8) {
        packedData.push_back(static_cast<char>(bitBuffer & 0xFF));
        bitBuffer >>= 8;
        bitBufferLen -= 8;
      }
    }
  };

  constexpr std::array<uint8_t, 7> BIT_WIDTHS = {1, 2, 4, 8, 16, 32, 64};

  auto findMinBitWidth = [&](uint64_t maxValue) -> uint8_t {
    if (maxValue == 0) {
      return 1;
    }

    size_t bitsNeeded = 64 - __builtin_clzll(maxValue);

    for (uint8_t width : BIT_WIDTHS) {
      if (width >= bitsNeeded) {
        return width;
      }
    }

    return 64;
  };

  uint64_t totalBits = 0;

  for (uint32_t frameIdx = 0; frameIdx < numFrames; ++frameIdx) {
    uint32_t frameStart = frameIdx * frameSize;
    uint32_t frameEnd = std::min(frameStart + frameSize, rowCount);
    uint32_t frameLength = frameEnd - frameStart;

    physicalType minValue = values[frameStart];
    physicalType maxValue = values[frameStart];

    for (uint32_t i = frameStart; i < frameEnd; ++i) {
      minValue = std::min(minValue, values[i]);
      maxValue = std::max(maxValue, values[i]);
    }

    uint64_t maxException = 0;
    if constexpr (std::is_signed_v<physicalType>) {
      maxException = static_cast<uint64_t>(
          static_cast<int64_t>(maxValue) - static_cast<int64_t>(minValue));
    } else {
      maxException = static_cast<uint64_t>(maxValue - minValue);
    }

    uint8_t bitWidth = findMinBitWidth(maxException);

    references[frameIdx] = minValue;
    bitWidths[frameIdx] = bitWidth;

    if (enableBitOffsets) {
      bitOffsets[frameIdx] = totalBits;
    }
    for (uint32_t i = frameStart; i < frameEnd; ++i) {
      uint64_t exception;
      if constexpr (std::is_signed_v<physicalType>) {
        exception = static_cast<uint64_t>(
            static_cast<int64_t>(values[i]) - static_cast<int64_t>(minValue));
      } else {
        exception = static_cast<uint64_t>(values[i] - minValue);
      }
      writeBits(exception, bitWidth);
    }

    totalBits += frameLength * bitWidth;
  }

  if (bitBufferLen > 0) {
    packedData.push_back(static_cast<char>(bitBuffer & 0xFF));
  }

  Buffer tempBuffer{buffer.getMemoryPool()};

  std::string_view serializedBitWidths =
      selection.template encodeNested<uint8_t>(
          0, // identifier - TODO: define proper identifiers
          {bitWidths},
          tempBuffer);

  std::string_view serializedReferences =
      selection.template encodeNested<physicalType>(
          1, {references}, tempBuffer);

  std::string_view serializedBitOffsets;
  if (enableBitOffsets) {
    serializedBitOffsets =
        selection.template encodeNested<uint64_t>(2, {bitOffsets}, tempBuffer);
  }

  auto dataCompressionPolicy = selection.compressionPolicy();
  CompressionEncoder<char> compressionEncoder{
      buffer.getMemoryPool(),
      *dataCompressionPolicy,
      DataType::Undefined,
      0, // bitWidth not applicable
      static_cast<uint32_t>(packedData.size()),
      [&]() { return std::span<char>{packedData}; },
      [&](char*& pos) {
        std::memcpy(pos, packedData.data(), packedData.size());
        pos += packedData.size();
        return pos;
      }};

  uint32_t encodingSize = Encoding::serializePrefixSize(rowCount, useVarint) +
      // FOR-specific fixed fields only; the standard prefix is accounted for
      // separately above via serializePrefixSize (kPrefixSize already includes
      // Encoding::kPrefixSize, so subtract it to avoid double-counting).
      (ForEncoding<T>::kPrefixSize - Encoding::kPrefixSize) + 4 +
      serializedBitWidths.size() + // BitWidths
      4 + serializedReferences.size() + // References
      (enableBitOffsets ? 4 + serializedBitOffsets.size() : 0) + 4 +
      compressionEncoder.getSize();

  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;

  Encoding::serializePrefix(
      EncodingType::FOR, TypeTraits<T>::dataType, rowCount, useVarint, pos);

  encoding::writeChar(
      static_cast<char>(compressionEncoder.compressionType()), pos);
  encoding::writeUint32(frameSize, pos);
  encoding::writeUint32(numFrames, pos);
  encoding::writeChar(enableBitOffsets ? 1 : 0, pos);

  encoding::writeUint32(serializedBitWidths.size(), pos);
  encoding::writeBytes(serializedBitWidths, pos);

  encoding::writeUint32(serializedReferences.size(), pos);
  encoding::writeBytes(serializedReferences, pos);

  if (enableBitOffsets) {
    encoding::writeUint32(serializedBitOffsets.size(), pos);
    encoding::writeBytes(serializedBitOffsets, pos);
  }

  encoding::writeUint32(compressionEncoder.getSize(), pos);
  compressionEncoder.write(pos);

  NIMBLE_DCHECK_EQ(encodingSize, pos - reserved, "Encoding size mismatch.");
  return {reserved, encodingSize};
}

} // namespace facebook::nimble
