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

#include <numeric>
#include <span>
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

// V2 variant of VarintEncoding for benchmarking and experimentation.

namespace facebook::nimble {

template <typename T>
class VarintEncodingV2 final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  static constexpr int kPrefixSize = sizeof(T);

  VarintEncodingV2(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory,
      const Encoding::Options& options = {});

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

 private:
  const physicalType baseValue_;
  uint32_t row_{0};
  const char* pos_;
  Vector<physicalType> buf_;
};

//
// End of public API. Implementations follow.
//

template <typename T>
VarintEncodingV2<T>::VarintEncodingV2(
    velox::memory::MemoryPool& pool,
    std::string_view data,
    std::function<void*(uint32_t)> /* stringBufferFactory */,
    const Encoding::Options& options)
    : TypedEncoding<T, physicalType>(pool, data, options),
      baseValue_{*reinterpret_cast<const physicalType*>(
          data.data() + this->dataOffset())},
      buf_{this->pool_} {
  reset();
}

template <typename T>
void VarintEncodingV2<T>::reset() {
  row_ = 0;
  pos_ = Encoding::data_.data() + this->dataOffset() +
      VarintEncodingV2<T>::kPrefixSize;
}

template <typename T>
void VarintEncodingV2<T>::skip(uint32_t rowCount) {
  row_ += rowCount;
  pos_ = varint::bulkVarintSkip(rowCount, pos_);
}

template <typename T>
void VarintEncodingV2<T>::materialize(uint32_t rowCount, void* buffer) {
  static_assert(
      sizeof(T) == 4 || sizeof(T) == 8,
      "Varint encoding require 4 or 8 bytes data types.");
  if constexpr (isFourByteIntegralType<physicalType>()) {
    pos_ = varint_v2::bulkVarintDecode32(
        rowCount, pos_, static_cast<uint32_t*>(buffer));
  } else {
    pos_ = varint_v2::bulkVarintDecode64(
        rowCount, pos_, static_cast<uint64_t*>(buffer));
  }

  if (baseValue_ != 0) {
    auto* output = reinterpret_cast<physicalType*>(buffer);
    for (auto i = 0; i < rowCount; ++i) {
      output[i] += baseValue_;
    }
  }
  row_ += rowCount;
}

template <typename T>
template <typename V>
void VarintEncodingV2<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  detail::readWithVisitorSlow(
      visitor,
      params,
      [&](auto toSkip) { skip(toSkip); },
      [&] {
        physicalType value;
        if constexpr (isFourByteIntegralType<physicalType>()) {
          value = varint::readVarint32(&pos_);
        } else {
          static_assert(sizeof(T) == 8);
          value = varint::readVarint64(&pos_);
        }
        return baseValue_ + value;
      });
}

template <typename T>
std::string_view VarintEncodingV2<T>::encode(
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
  static_assert(
      sizeof(T) == 4 || sizeof(T) == 8,
      "Varint encoding require 4 or 8 bytes data types.");
  const uint32_t valueCount = values.size();
  uint8_t index{0};
  const uint32_t dataSize = std::accumulate(
      selection.statistics().bucketCounts().cbegin(),
      selection.statistics().bucketCounts().cend(),
      0,
      [&index](uint32_t sum, uint64_t bucketSize) {
        return sum + (bucketSize * (++index));
      });
  const uint32_t encodingSize = dataSize +
      Encoding::serializePrefixSize(valueCount, useVarint) +
      VarintEncodingV2<T>::kPrefixSize;

  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::VarintV2,
      TypeTraits<T>::dataType,
      valueCount,
      useVarint,
      pos);
  encoding::write(selection.statistics().min(), pos);
  for (auto value : values) {
    varint::writeVarint(value - selection.statistics().min(), &pos);
  }
  NIMBLE_DCHECK_EQ(pos - reserved, encodingSize, "Encoding size mismatch.");
  return {reserved, encodingSize};
}
} // namespace facebook::nimble
