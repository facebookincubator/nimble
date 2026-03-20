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

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/Compression.h"
#include "dwio/nimble/encodings/Encoding.h"

// V2 variant of FixedBitWidthEncoding for benchmarking and experimentation.

namespace facebook::nimble {

template <typename T>
class FixedBitWidthEncodingV2 final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  static const int kPrefixSize = 2 + sizeof(T);

  FixedBitWidthEncodingV2(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory,
      const Encoding::Options& options = {});

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  template <bool kHasNulls, typename Visitor>
  void bulkScan(
      Visitor& visitor,
      uint32_t nonNullsSoFar,
      const int32_t* rows,
      int32_t numRows,
      const int32_t* scatterRows);

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {});

  std::string debugString(int offset) const final;

 private:
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
FixedBitWidthEncodingV2<T>::FixedBitWidthEncodingV2(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    std::function<void*(uint32_t)> /* stringBufferFactory */,
    const Encoding::Options& options)
    : TypedEncoding<T, physicalType>{memoryPool, data, options},
      uncompressedData_{&memoryPool},
      buffer_{&memoryPool} {
  auto pos = data.data() + this->dataOffset();
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
void FixedBitWidthEncodingV2<T>::reset() {
  row_ = 0;
}

template <typename T>
void FixedBitWidthEncodingV2<T>::skip(uint32_t rowCount) {
  row_ += rowCount;
}

template <typename T>
void FixedBitWidthEncodingV2<T>::materialize(uint32_t rowCount, void* buffer) {
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
void FixedBitWidthEncodingV2<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  using OutputType = detail::ValueType<typename V::DataType>;
  constexpr bool kExtractToReader =
      std::is_same_v<typename V::Extract, velox::dwio::common::ExtractToReader>;
  constexpr bool kSameType = std::is_same_v<physicalType, OutputType>;
  constexpr bool kIsWidening = sizeof(OutputType) > sizeof(physicalType) &&
      std::is_integral_v<OutputType> && std::is_integral_v<physicalType>;
  constexpr bool kCanUseFastPath = isFourByteIntegralType<physicalType>() &&
      !V::kHasFilter && !V::kHasHook && kExtractToReader &&
      (kSameType || kIsWidening);
  if constexpr (kCanUseFastPath) {
    auto* nulls = visitor.reader().rawNullsInReadRange();
    detail::readWithVisitorFast(*this, visitor, params, nulls);
    return;
  }
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
template <bool kHasNulls, typename V>
void FixedBitWidthEncodingV2<T>::bulkScan(
    V& visitor,
    uint32_t currentRow,
    const int32_t* nonNullRows,
    int32_t numNonNulls,
    const int32_t* scatterRows) {
  using DataType = typename V::DataType;
  using OutputType = detail::ValueType<DataType>;
  static_assert(
      isFourByteIntegralType<physicalType>(),
      "bulkScan only supports 4-byte integral types");

  if (numNonNulls == 0) {
    return;
  }

  const auto numRows = visitor.numRows() - visitor.rowIndex();

  const auto offset =
      static_cast<int32_t>(row_) - static_cast<int32_t>(currentRow);

  auto* values = detail::mutableValues<OutputType>(visitor, numRows);

  constexpr bool kSameSize = sizeof(physicalType) == sizeof(OutputType);
  constexpr bool kIsUpcast = sizeof(OutputType) > sizeof(physicalType) &&
      std::is_integral_v<OutputType> && std::is_integral_v<physicalType>;

  if constexpr (V::dense) {
    buffer_.resize(numNonNulls);
    fixedBitArray_.bulkGetWithBaseline32(
        nonNullRows[0] + offset,
        numNonNulls,
        reinterpret_cast<uint32_t*>(buffer_.data()),
        baseline_);

    if constexpr (kSameSize) {
      std::memcpy(values, buffer_.data(), numNonNulls * sizeof(physicalType));
    } else if constexpr (kIsUpcast) {
      for (int32_t i = 0; i < numNonNulls; ++i) {
        values[i] = static_cast<OutputType>(buffer_[i]);
      }
    }
  } else {
    for (int32_t i = 0; i < numNonNulls; ++i) {
      values[i] = fixedBitArray_.get(nonNullRows[i] + offset) + baseline_;
    }
  }

  row_ += nonNullRows[numNonNulls - 1] - currentRow + 1;

  visitor.addNumValues(V::dense ? numRows : numNonNulls);
  visitor.setRowIndex(visitor.numRows());
}

template <typename T>
std::string_view FixedBitWidthEncodingV2<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer,
    const Encoding::Options& options) {
  const bool useVarint = options.useVarintRowCount;
  static_assert(
      std::is_same_v<
          typename std::make_unsigned<physicalType>::type,
          physicalType>,
      "Physical type must be unsigned.");
  const uint32_t rowCount = values.size();
  const int bitsRequired =
      (velox::bits::bitsRequired(
           selection.statistics().max() - selection.statistics().min()) +
       7) &
      ~7;

  const uint32_t fixedBitArraySize =
      FixedBitArray::bufferSize(values.size(), bitsRequired);

  Vector<char> vector{&buffer.getMemoryPool()};

  auto dataCompressionPolicy = selection.compressionPolicy();
  CompressionEncoder<T> compressionEncoder{
      buffer.getMemoryPool(),
      *dataCompressionPolicy,
      DataType::Undefined,
      bitsRequired,
      fixedBitArraySize,
      [&]() {
        vector.resize(fixedBitArraySize);
        return std::span<char>{vector};
      },
      [&, baseline = selection.statistics().min()](char*& pos) {
        memset(pos, 0, fixedBitArraySize);
        FixedBitArray fba(pos, bitsRequired);
        if constexpr (sizeof(physicalType) == 4) {
          fba.bulkSet32WithBaseline(
              0,
              rowCount,
              reinterpret_cast<const uint32_t*>(values.data()),
              baseline);
        } else {
          for (uint32_t i = 0; i < values.size(); ++i) {
            fba.set(i, values[i] - baseline);
          }
        }
        pos += fixedBitArraySize;
        return pos;
      }};

  const uint32_t encodingSize =
      Encoding::serializePrefixSize(rowCount, useVarint) +
      FixedBitWidthEncodingV2<T>::kPrefixSize + compressionEncoder.getSize();
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::FixedBitWidthV2,
      TypeTraits<T>::dataType,
      rowCount,
      useVarint,
      pos);
  encoding::writeChar(
      static_cast<char>(compressionEncoder.compressionType()), pos);
  encoding::write(selection.statistics().min(), pos);
  encoding::writeChar(bitsRequired, pos);
  compressionEncoder.write(pos);

  NIMBLE_DCHECK_EQ(encodingSize, pos - reserved, "Encoding size mismatch.");
  return {reserved, encodingSize};
}

template <typename T>
std::string FixedBitWidthEncodingV2<T>::debugString(int offset) const {
  return fmt::format(
      "{}{}<{}> rowCount={} bit_width={}",
      std::string(offset, ' '),
      toString(Encoding::encodingType()),
      toString(Encoding::dataType()),
      Encoding::rowCount(),
      bitWidth_);
}

} // namespace facebook::nimble
