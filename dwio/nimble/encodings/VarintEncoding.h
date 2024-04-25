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
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingSelection.h"
#include "dwio/nimble/encodings/Statistics.h"

// Stores integer data in a varint encoding. For now we only support encoding
// non-negative values, but we may later add an optional zigzag encoding that
// will let us handle negatives.

namespace facebook::nimble {

// Data layout is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// sizeof(T) bytes: baseline value
// X bytes: the varint encoded bytes
template <typename T>
class VarintEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  static const int kBaselineOffset = Encoding::kPrefixSize;
  static const int kDataOffset = kBaselineOffset + sizeof(T);
  static const int kPrefixSize = kDataOffset - kBaselineOffset;

  explicit VarintEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data);

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer);

 private:
  const physicalType baseline_;
  uint32_t row_ = 0;
  const char* pos_;
  Vector<physicalType> buf_;
};

//
// End of public API. Implementations follow.
//

template <typename T>
VarintEncoding<T>::VarintEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data)
    : TypedEncoding<T, physicalType>(memoryPool, data),
      baseline_{*reinterpret_cast<const physicalType*>(
          data.data() + kBaselineOffset)},
      buf_{&memoryPool} {
  reset();
}

template <typename T>
void VarintEncoding<T>::reset() {
  row_ = 0;
  pos_ = Encoding::data_.data() + Encoding::kPrefixSize +
      VarintEncoding<T>::kPrefixSize;
}

template <typename T>
void VarintEncoding<T>::skip(uint32_t rowCount) {
  row_ += rowCount;
  pos_ = varint::bulkVarintSkip(rowCount, pos_);
}

template <typename T>
void VarintEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  static_assert(
      sizeof(T) == 4 || sizeof(T) == 8,
      "Varint encoding require 4 or 8 bytes data types.");
  if constexpr (isFourByteIntegralType<physicalType>()) {
    pos_ = varint::bulkVarintDecode32(
        rowCount, pos_, static_cast<uint32_t*>(buffer));
  } else {
    pos_ = varint::bulkVarintDecode64(
        rowCount, pos_, static_cast<uint64_t*>(buffer));
  }

  if (baseline_ != 0) {
    // Add baseline to values
    auto output = reinterpret_cast<physicalType*>(buffer);
    for (auto i = 0; i < rowCount; ++i) {
      output[i] += baseline_;
    }
  }
  row_ += rowCount;
}

template <typename T>
std::string_view VarintEncoding<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer) {
  static_assert(
      std::is_same_v<
          typename std::make_unsigned<physicalType>::type,
          physicalType>,
      "Physical type must be unsigned.");
  static_assert(
      sizeof(T) == 4 || sizeof(T) == 8,
      "Varint encoding require 4 or 8 bytes data types.");
  const uint32_t valueCount = values.size();
  uint8_t index = 0;
  uint32_t dataSize = std::accumulate(
      selection.statistics().bucketCounts().cbegin(),
      selection.statistics().bucketCounts().cend(),
      0,
      [&index](uint32_t sum, const uint64_t bucketSize) {
        // First (7 bit) bucket is going to consume 1 byte, 2nd bucket is going
        // to consume 2 bytes, etc.
        return sum + (bucketSize * (++index));
      });
  uint32_t encodingSize =
      dataSize + Encoding::kPrefixSize + VarintEncoding<T>::kPrefixSize;

  // Adding 7 bytes, to allow bulk materialization to access the last 64 bit
  // word, even if it is not full.
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::Varint, TypeTraits<T>::dataType, valueCount, pos);
  encoding::write(selection.statistics().min(), pos);
  for (auto value : values) {
    varint::writeVarint(value - selection.statistics().min(), &pos);
  }
  NIMBLE_DASSERT(pos - reserved == encodingSize, "Encoding size mismatch.");
  // Adding 7 bytes, to allow bulk materialization to access the last 64 bit
  // word, even if it is not full.
  return {reserved, encodingSize};
}
} // namespace facebook::nimble
