/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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

#include <numeric>
#include <span>

#include "dwio/nimble/common/Bits.h"
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/Compression.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingIdentifier.h"
#include "dwio/nimble/encodings/EncodingSelection.h"
#include "velox/common/memory/Memory.h"

// Holds data in the the 'trivial' way for each data type.
// Namely as physicalType* for numerics, a packed set of offsets and a blob of
// characters for strings, and bit-packed for bools.

namespace facebook::nimble {

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

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

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

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  // Returns the total size of the characters payload in bytes
  uint64_t uncompressedDataBytes() const;

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

  // The size of the uncompressed data in bytes.
  //
  // NOTE: This is not necessarily the size of 'dataUncompressed_' because the
  // data could come as uncompressed.
  uint64_t uncompressedDataBytes_;
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

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

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
    NIMBLE_CHECK(
        reinterpret_cast<const char*>(values_ + this->rowCount()) ==
            uncompressed_.end(),
        "Unexpected trivial encoding end");
  } else {
    NIMBLE_CHECK(
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
  const auto start = values_ + row_;
  std::copy(start, start + rowCount, static_cast<T*>(buffer));
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

template <typename T>
template <typename V>
void TrivialEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  this->template readWithVisitorSlow<true>(
      visitor, params, [&] { return values_[row_++]; });
}

template <typename V>
void TrivialEncoding<std::string_view>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  const auto endRow = visitor.rowAt(visitor.numRows() - 1);
  auto numNonNulls = endRow + 1 - params.numScanned;
  if (auto& nulls = visitor.reader().nullsInReadRange()) {
    numNonNulls -= velox::bits::countNulls(
        nulls->template as<uint64_t>(), params.numScanned, endRow + 1);
  }
  buffer_.resize(numNonNulls);
  lengths_->materialize(numNonNulls, buffer_.data());
  auto* lengths = buffer_.data();
  detail::readWithVisitorSlow<true>(
      visitor,
      params,
      [&](auto toSkip) {
        row_ += toSkip;
        pos_ += std::accumulate(lengths, lengths + toSkip, 0ull);
        lengths += toSkip;
      },
      [&] {
        auto len = *lengths++;
        folly::StringPiece value(pos_, len);
        ++row_;
        pos_ += len;
        return value;
      });
}

template <typename V>
void TrivialEncoding<bool>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  readWithVisitorSlow<true>(
      visitor, params, [&] { return bits::getBit(row_++, bitmap_); });
}

} // namespace facebook::nimble
