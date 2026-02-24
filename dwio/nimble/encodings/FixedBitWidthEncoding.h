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

#include "dwio/nimble/common/Bits.h"
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/Compression.h"
#include "dwio/nimble/encodings/Encoding.h"

// The FixedBitWidthEncoding stores integer data in a fixed number of
// bits equal to the number of bits required to represent the largest value in
// the encoding. For now we only support encoding non-negative values, but
// we may later add an optional zigzag encoding that will let us handle
// negatives.

namespace facebook::nimble {

// Data layout is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// 1 byte: compression type
// sizeof(T) byte: baseline value
// 1 byte: bit width
// FixedBitArray::BufferSize(rowCount, bit_width) bytes: packed values.
template <typename T>
class FixedBitWidthEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  static const int kCompressionOffset = Encoding::kPrefixSize;
  static const int kPrefixSize = 2 + sizeof(T);

  FixedBitWidthEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory);

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
  /// Computes the minimum number of bits needed to represent the value range
  /// [min, max], rounded up to the nearest byte boundary (multiple of 8).
  ///
  /// Byte-aligned bit widths avoid sub-byte packing that significantly
  /// degrades downstream compression efficiency.
  static inline int computeByteAlignedBitWidth(
      physicalType min,
      physicalType max);

  /// Computes the minimum number of bits needed to represent the value range
  /// [min, max].
  static inline int computeBitWidth(physicalType min, physicalType max);

  int bitWidth_;
  physicalType baseline_;
  FixedBitArray fixedBitArray_;
  uint32_t row_ = 0;
  Vector<char> uncompressedData_;
  Vector<physicalType> buffer_;
};

//
// End of public API. Implementations follow.
//
template <typename T>
int FixedBitWidthEncoding<T>::computeBitWidth(
    physicalType min,
    physicalType max) {
  return bits::bitsRequired(max - min);
}

template <typename T>
int FixedBitWidthEncoding<T>::computeByteAlignedBitWidth(
    physicalType min,
    physicalType max) {
  return (bits::bitsRequired(max - min) + 7) & ~7;
}

template <typename T>
FixedBitWidthEncoding<T>::FixedBitWidthEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    std::function<void*(uint32_t)> /* stringBufferFactory */)
    : TypedEncoding<T, physicalType>{memoryPool, data},
      uncompressedData_{&memoryPool},
      buffer_{&memoryPool} {
  auto pos = data.data() + kCompressionOffset;
  auto compressionType = static_cast<CompressionType>(encoding::readChar(pos));
  baseline_ = encoding::read<const physicalType>(pos);
  bitWidth_ = static_cast<uint32_t>(encoding::readChar(pos));
  if (compressionType != CompressionType::Uncompressed) {
    uncompressedData_ = Compression::uncompress(
        memoryPool,
        compressionType,
        DataType::Undefined,
        {pos, static_cast<size_t>(data.end() - pos)});
    fixedBitArray_ = FixedBitArray{
        {uncompressedData_.data(), uncompressedData_.size()}, bitWidth_};
  } else {
    fixedBitArray_ =
        FixedBitArray{{pos, static_cast<size_t>(data.end() - pos)}, bitWidth_};
  }
}

template <typename T>
void FixedBitWidthEncoding<T>::reset() {
  row_ = 0;
}

template <typename T>
void FixedBitWidthEncoding<T>::skip(uint32_t rowCount) {
  row_ += rowCount;
}

template <typename T>
void FixedBitWidthEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  if constexpr (isFourByteIntegralType<physicalType>()) {
    fixedBitArray_.bulkGetWithBaseline32(
        row_, rowCount, static_cast<uint32_t*>(buffer), baseline_);
  } else {
    if (sizeof(physicalType) == 8 && bitWidth_ <= 32) {
      fixedBitArray_.bulkGetWithBaseline32Into64(
          row_, rowCount, static_cast<uint64_t*>(buffer), baseline_);
    } else {
      const uint32_t start = row_;
      const uint32_t end = start + rowCount;
      physicalType* output = static_cast<physicalType*>(buffer);
      for (uint32_t i = start; i < end; ++i) {
        *output++ = fixedBitArray_.get(i) + baseline_;
      }
    }
  }
  row_ += rowCount;
}

template <typename T>
template <typename V>
void FixedBitWidthEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  detail::readWithVisitorSlow(
      visitor,
      params,
      [&](auto toSkip) { skip(toSkip); },
      [&] {
        physicalType value = fixedBitArray_.get(row_++) + baseline_;
        return value;
      });
}

template <typename T>
std::string_view FixedBitWidthEncoding<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer) {
  static_assert(
      std::is_same_v<
          typename std::make_unsigned<physicalType>::type,
          physicalType>,
      "Physical type must be unsigned.");
  // NOTE: If we end up with bitsRequired not being a multiple of 8, it degrades
  // compression efficiency a lot. To (temporarily) mitigate this, we revert to
  // using "FixedByteWidth", meaning that we round the required bitness to the
  // closest byte. This is a temporary solution. Going forward we should
  // evaluate other options. For example:
  // 1. Asymetric bit width, where we don't encode the data during write
  // (instead we use Trivial + Compression), but we encode it when we read (for
  // better in-memory representation).
  // 2. Apply compression only if bit width is a multiple of 8
  // 3. Try both bit width and byte width and pick one.
  // 4. etc...
  const auto min = selection.statistics().min();
  const auto max = selection.statistics().max();
  const int rawBits = computeBitWidth(min, max);
  const int byteAlignedBits = computeByteAlignedBitWidth(min, max);

  const uint32_t rawFixedBitArraySize =
      FixedBitArray::bufferSize(values.size(), rawBits);
  const uint32_t byteAlignedFixedBitArraySize =
      FixedBitArray::bufferSize(values.size(), byteAlignedBits);

  const uint32_t rowCount = values.size();

  // Start with byte-aligned bit width for compression attempt.
  int bitsRequired = byteAlignedBits;
  uint32_t fixedBitArraySize = byteAlignedFixedBitArraySize;

  Vector<char> vector{&buffer.getMemoryPool()};

  auto dataCompressionPolicy = selection.compressionPolicy();

  auto allocateBuffer = [&]() {
    vector.resize(fixedBitArraySize);
    return std::span<char>{vector};
  };

  auto writeData = [&, baseline = min](char*& pos) {
    memset(pos, 0, fixedBitArraySize);
    FixedBitArray fba(pos, bitsRequired);
    if constexpr (sizeof(physicalType) == 4) {
      fba.bulkSet32WithBaseline(
          0,
          rowCount,
          reinterpret_cast<const uint32_t*>(values.data()),
          baseline);
    } else {
      // TODO: We may want to support 32-bit mode with (u)int64 here as well.
      for (uint64_t i = 0; i < values.size(); ++i) {
        fba.set(i, values[i] - baseline);
      }
    }
    pos += fixedBitArraySize;
    return pos;
  };

  // Try compression with byte-aligned bit width first. If compression was
  // not applied (encoder chose Uncompressed) and the uncompressed byte-aligned
  // size exceeds the raw (non-byte-aligned) size, use the raw bit width for
  // tighter packing.
  auto makeEncoder = [&]() -> CompressionEncoder<T> {
    CompressionEncoder<T> compressed{
        buffer.getMemoryPool(),
        *dataCompressionPolicy,
        DataType::Undefined,
        bitsRequired,
        fixedBitArraySize,
        allocateBuffer,
        writeData};

    if (compressed.compressionType() != CompressionType::Uncompressed ||
        compressed.getSize() <= rawFixedBitArraySize) {
      return compressed;
    }

    // Compression was not applied and byte-aligned packing is larger than
    // raw packing. Fall back to raw bit width without compression.
    bitsRequired = rawBits;
    fixedBitArraySize = rawFixedBitArraySize;
    vector = Vector<char>{&buffer.getMemoryPool()};

    NoCompressionPolicy noCompressionPolicy;
    return CompressionEncoder<T>{
        buffer.getMemoryPool(),
        noCompressionPolicy,
        DataType::Undefined,
        bitsRequired,
        fixedBitArraySize,
        allocateBuffer,
        writeData};
  };

  auto compressionEncoder = makeEncoder();

  const uint32_t encodingSize = Encoding::kPrefixSize +
      FixedBitWidthEncoding<T>::kPrefixSize + compressionEncoder.getSize();

  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;

  // Write the standard encoding prefix:
  // - Encoding type: FixedBitWidth
  // - Logical data type for T
  // - Row count
  // - Advances 'pos' by the serialized prefix length
  Encoding::serializePrefix(
      EncodingType::FixedBitWidth, TypeTraits<T>::dataType, rowCount, pos);

  // Write the compression type used for the payload (1 byte).
  // This informs the decoder whether the following fixed-bit array is
  // compressed.
  encoding::writeChar(
      static_cast<char>(compressionEncoder.compressionType()), pos);

  // Write the baseline (minimum value) for delta encoding.
  encoding::write(selection.statistics().min(), pos);

  // Write the bit width (rounded up to a byte boundary) used for packing.
  // Decoder uses this to reconstruct values from the fixed-bit array.
  encoding::writeChar(bitsRequired, pos);

  // Serialize the fixed-bit packed values, potentially compressed.
  // This writes the payload into the buffer starting at 'pos' and advances it.
  compressionEncoder.write(pos);

  NIMBLE_DCHECK_EQ(encodingSize, pos - reserved, "Encoding size mismatch.");
  return {reserved, encodingSize};
}

template <typename T>
std::string FixedBitWidthEncoding<T>::debugString(int offset) const {
  return fmt::format(
      "{}{}<{}> rowCount={} bit_width={}",
      std::string(offset, ' '),
      toString(Encoding::encodingType()),
      toString(Encoding::dataType()),
      Encoding::rowCount(),
      bitWidth_);
}

} // namespace facebook::nimble
