// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <numeric>
#include <span>

#include "dwio/alpha/common/AlphaCompare.h"
#include "dwio/alpha/common/Bits.h"
#include "dwio/alpha/common/Buffer.h"
#include "dwio/alpha/common/EncodingPrimitives.h"
#include "dwio/alpha/common/EncodingType.h"
#include "dwio/alpha/common/FixedBitArray.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/encodings/Compression.h"
#include "dwio/alpha/encodings/Encoding.h"
#include "dwio/alpha/encodings/EncodingIdentifier.h"
#include "dwio/alpha/encodings/EncodingSelection.h"
#include "velox/common/memory/Memory.h"

// Holds data in the the 'trivial' way for each data type.
// Namely as physicalType* for numerics, a packed set of offsets and a blob of
// characters for strings, and bit-packed for bools.

namespace facebook::alpha {

// Handles the numeric cases. Bools and strings are specialized below.
// Data layout is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// 1 byte: lengths compression
// rowCount * sizeof(physicalType) bytes: the data
template <typename T>
class TrivialEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  static constexpr int kPrefixSize = 1;
  static constexpr int kCompressionTypeOffset = Encoding::kPrefixSize;
  static constexpr int kDataOffset =
      Encoding::kPrefixSize + TrivialEncoding<T>::kPrefixSize;

  TrivialEncoding(velox::memory::MemoryPool& memoryPool, std::string_view data);

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer);

 private:
  uint32_t row_;
  const T* values_;
  Vector<char> uncompressed_;
};

// For the string case the layout is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// 1 byte: data compression
// 4 bytes: offset to start of blob
// XX bytes: bit packed lengths
// YY bytes: actual characters
template <>
class TrivialEncoding<std::string_view> final
    : public TypedEncoding<std::string_view, std::string_view> {
 public:
  using cppDataType = std::string_view;

  static constexpr int kPrefixSize = 5;
  static constexpr int kDataCompressionOffset = Encoding::kPrefixSize;
  static constexpr int kBlobOffsetOffset = Encoding::kPrefixSize + 1;
  static constexpr int kLengthOffset =
      Encoding::kPrefixSize + TrivialEncoding<std::string_view>::kPrefixSize;

  TrivialEncoding(velox::memory::MemoryPool& memoryPool, std::string_view data);

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  static std::string_view encode(
      EncodingSelection<std::string_view>& selection,
      std::span<const std::string_view> values,
      Buffer& buffer);

 private:
  uint32_t row_;
  const char* blob_;
  const char* pos_;
  std::unique_ptr<Encoding> lengths_;
  Vector<uint32_t> buffer_;
  Vector<char> dataUncompressed_;
};

// For the bool case the layout is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// 1 byte: compression type
// FBA::BufferSize(rowCount, 1) bytes: the bitmap
template <>
class TrivialEncoding<bool> final : public TypedEncoding<bool, bool> {
 public:
  using cppDataType = bool;

  static constexpr int kPrefixSize = 1;
  static constexpr int kCompressionTypeOffset = Encoding::kPrefixSize;
  static constexpr int kDataOffset =
      Encoding::kPrefixSize + TrivialEncoding<bool>::kPrefixSize;

  TrivialEncoding(velox::memory::MemoryPool& memoryPool, std::string_view data);

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  static std::string_view encode(
      EncodingSelection<bool>& selection,
      std::span<const bool> values,
      Buffer& buffer);

 private:
  uint32_t row_;
  const char* bitmap_;
  Vector<char> uncompressed_;
};

//
// End of public API. Implementations follow.
//

template <typename T>
TrivialEncoding<T>::TrivialEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data)
    : TypedEncoding<T, physicalType>{memoryPool, data},
      row_{0},
      values_{reinterpret_cast<const T*>(data.data() + kDataOffset)},
      uncompressed_{&memoryPool} {
  auto compressionType =
      static_cast<CompressionType>(data[kCompressionTypeOffset]);
  if (compressionType != CompressionType::Uncompressed) {
    uncompressed_ = Compression::uncompress(
        memoryPool,
        compressionType,
        {data.data() + kDataOffset, data.size() - kDataOffset});
    values_ = reinterpret_cast<const T*>(uncompressed_.data());
    ALPHA_CHECK(
        reinterpret_cast<const char*>(values_ + this->rowCount()) ==
            uncompressed_.end(),
        "Unexpected trivial encoding end");
  } else {
    ALPHA_CHECK(
        reinterpret_cast<const char*>(values_ + this->rowCount()) == data.end(),
        "Unexpected trivial encoding end");
  }
}

template <typename T>
void TrivialEncoding<T>::reset() {
  row_ = 0;
}

template <typename T>
void TrivialEncoding<T>::skip(uint32_t rowCount) {
  row_ += rowCount;
}

template <typename T>
void TrivialEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  memcpy(buffer, values_ + row_, sizeof(physicalType) * rowCount);
  row_ += rowCount;
}

template <typename T>
std::string_view TrivialEncoding<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer) {
  const uint32_t rowCount = values.size();
  const uint32_t uncompressedSize = sizeof(T) * rowCount;

  auto compressionPolicy = selection.compressionPolicy();
  CompressionEncoder<T> compressionEncoder{
      buffer.getMemoryPool(),
      *compressionPolicy,
      TypeTraits<physicalType>::dataType,
      {reinterpret_cast<const char*>(values.data()), uncompressedSize}};

  const uint32_t encodingSize = kDataOffset + compressionEncoder.getSize();
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::Trivial, TypeTraits<T>::dataType, rowCount, pos);
  encoding::writeChar(
      static_cast<char>(compressionEncoder.compressionType()), pos);
  compressionEncoder.write(pos);
  return {reserved, encodingSize};
}

} // namespace facebook::alpha