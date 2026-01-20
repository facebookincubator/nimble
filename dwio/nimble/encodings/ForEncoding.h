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

#include "dwio/nimble/common/Bits.h"
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/Compression.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingSelection.h"

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

  ForEncoding(velox::memory::MemoryPool& memoryPool, std::string_view data);

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer);

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
  Vector<char> uncompressedData_;
  Vector<physicalType> buffer_;

  uint64_t readBits(uint64_t bitOffset, uint8_t numBits) const;

  uint32_t getFrameIndex(uint32_t row) const {
    return row / frameSize_;
  }

  uint32_t getPositionInFrame(uint32_t row) const {
    return row % frameSize_;
  }

  physicalType decodeValue(uint32_t row) const;
};
//
// Implementation
//

template <typename T>
ForEncoding<T>::ForEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data)
    : TypedEncoding<T, physicalType>{memoryPool, data},
      frames_{&memoryPool},
      currentRow_(0),
      uncompressedData_{&memoryPool},
      buffer_{&memoryPool} {
  static_assert(
      std::is_integral_v<physicalType>,
      "ForEncoding only supports integral types");

  const char* pos = data.data() + kCompressionOffset;
  CompressionType compressionType =
      static_cast<CompressionType>(encoding::readChar(pos));

  frameSize_ = encoding::readUint32(pos);
  numFrames_ = encoding::readUint32(pos);
  enableBitOffsets_ = static_cast<bool>(encoding::readChar(pos));

  // Read BitWidths array
  const uint32_t bitWidthsSize = encoding::readUint32(pos);
  auto bitWidthsEncoding =
      EncodingFactory::decode(*this->pool_, {pos, bitWidthsSize});
  pos += bitWidthsSize;

  Vector<uint8_t> bitWidths(&memoryPool, numFrames_);
  bitWidthsEncoding->materialize(numFrames_, bitWidths.data());

  // Read References array
  const uint32_t referencesSize = encoding::readUint32(pos);
  auto referencesEncoding =
      EncodingFactory::decode(*this->pool_, {pos, referencesSize});
  pos += referencesSize;

  Vector<physicalType> references(&memoryPool, numFrames_);
  referencesEncoding->materialize(numFrames_, references.data());

  // Read BitOffsets array if enabled
  Vector<uint64_t> bitOffsets(&memoryPool);
  if (enableBitOffsets_) {
    const uint32_t bitOffsetsSize = encoding::readUint32(pos);
    auto bitOffsetsEncoding =
        EncodingFactory::decode(*this->pool_, {pos, bitOffsetsSize});
    pos += bitOffsetsSize;

    bitOffsets.resize(numFrames_);
    bitOffsetsEncoding->materialize(numFrames_, bitOffsets.data());
  }

  const uint32_t packedDataSize = encoding::readUint32(pos);
  std::string_view packedDataView{pos, packedDataSize};

  if (compressionType != CompressionType::Uncompressed) {
    uncompressedData_ = Compression::uncompress(
        *this->pool_, compressionType, packedDataView);
    packedData_ = uncompressedData_.data();
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
    frame.bitOffset =
        enableBitOffsets_ ? bitOffsets[i] : cumulativeBitOffset;

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
uint64_t ForEncoding<T>::readBits(uint64_t bitOffset, uint8_t numBits) const {
  if (numBits == 0) {
    return 0;
  }

  uint64_t byteOffset = bitOffset / 8;
  uint64_t bitInByte = bitOffset % 8;

  uint64_t result = 0;
  uint64_t bitsRead = 0;

  while (bitsRead < numBits) {
    uint64_t bitsAvailable = 8 - bitInByte;
    uint64_t bitsToRead = std::min(bitsAvailable, numBits - bitsRead);

    uint8_t byte = static_cast<uint8_t>(packedData_[byteOffset]);
    uint64_t mask = ((1ULL << bitsToRead) - 1) << bitInByte;
    uint64_t bits = (byte & mask) >> bitInByte;
    result |= bits << bitsRead;

    bitsRead += bitsToRead;
    byteOffset++;
    bitInByte = 0;
  }

  return result;
}

template <typename T>
typename ForEncoding<T>::physicalType ForEncoding<T>::decodeValue(
    uint32_t row) const {
  uint32_t frameIdx = getFrameIndex(row);
  uint32_t posInFrame = getPositionInFrame(row);

  const auto& frame = frames_[frameIdx];

  uint64_t bitPos = frame.bitOffset + posInFrame * frame.bitWidth;
  uint64_t exception = readBits(bitPos, frame.bitWidth);

  // Reconstruct value
  if constexpr (std::is_signed_v<physicalType>) {
    return static_cast<physicalType>(
        static_cast<int64_t>(exception) + static_cast<int64_t>(frame.reference));
  } else {
    return static_cast<physicalType>(exception + frame.reference);
  }
}

template <typename T>
void ForEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  physicalType* output = static_cast<physicalType*>(buffer);

  for (uint32_t i = 0; i < rowCount; ++i) {
    output[i] = decodeValue(currentRow_ + i);
  }

  currentRow_ += rowCount;
}

template <typename T>
template <typename V>
void ForEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  detail::readWithVisitorSlow(visitor, params, nullptr, [&] {
    return decodeValue(visitor.rowIndex());
  });
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

  log += fmt::format(
      "\n{}bitWidth distribution: ", std::string(offset + 2, ' '));
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
    Buffer& buffer) {
  static_assert(
      std::is_integral_v<physicalType>,
      "ForEncoding only supports integral types");

  const uint32_t rowCount = values.size();
  if (rowCount == 0) {
    NIMBLE_UNSUPPORTED("Cannot encode empty data with ForEncoding");
  }

  // Configuration (TODO: make configurable via policy)
  const bool enableBitOffsets = true;  // Default: enable for O(1) random access
  const uint32_t frameSize = 128;      // Default frame size (TODO: adaptive selection)

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
      
      uint64_t mask = (bitsThisRound == 64) ? ~0ULL : ((1ULL << bitsThisRound) - 1);
      uint64_t chunk = value & mask;
      
      bitBuffer |= (chunk << bitBufferLen);
      bitBufferLen += bitsThisRound;
      
      value >>= bitsThisRound;
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
    if (maxValue == 0) return 1;

    size_t bitsNeeded = 64 - __builtin_clzll(maxValue);

    for (uint8_t width : BIT_WIDTHS) {
      if (width >= bitsNeeded) return width;
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

  std::string_view serializedBitWidths = selection.template encodeNested<uint8_t>(
      0,  // identifier - TODO: define proper identifiers
      {bitWidths},
      tempBuffer);

  std::string_view serializedReferences = selection.template encodeNested<physicalType>(
      1,
      {references},
      tempBuffer);

  std::string_view serializedBitOffsets;
  if (enableBitOffsets) {
    serializedBitOffsets = selection.template encodeNested<uint64_t>(
        2,
        {bitOffsets},
        tempBuffer);
  }

  auto dataCompressionPolicy = selection.compressionPolicy();
  CompressionEncoder<char> compressionEncoder{
      buffer.getMemoryPool(),
      *dataCompressionPolicy,
      DataType::Undefined,
      0,  // bitWidth not applicable
      static_cast<uint32_t>(packedData.size()),
      [&]() { return std::span<char>{packedData}; },
      [&](char*& pos) {
        std::memcpy(pos, packedData.data(), packedData.size());
        pos += packedData.size();
        return pos;
      }};

  uint32_t encodingSize = Encoding::kPrefixSize + ForEncoding<T>::kPrefixSize +
      4 + serializedBitWidths.size() +       // BitWidths
      4 + serializedReferences.size() +      // References
      (enableBitOffsets ? 4 + serializedBitOffsets.size() : 0) +
      4 + compressionEncoder.getSize();

  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;

  Encoding::serializePrefix(
      EncodingType::FOR, TypeTraits<T>::dataType, rowCount, pos);

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
