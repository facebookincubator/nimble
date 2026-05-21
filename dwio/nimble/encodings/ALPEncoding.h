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

#include <cstring>
#include <span>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/common/EncodingType.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"

// TODO: Replace this no-op passthrough with the actual ALP compression
// algorithm. The current implementation stores raw bytes identically to
// TrivialEncoding and provides no compression benefit.

namespace facebook::nimble {

// Data layout is:
// Prefix bytes: standard Encoding prefix
// rowCount * sizeof(physicalType) bytes: raw values (passthrough)

template <typename T>
class ALPEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
  static_assert(
      isFloatingPointType<T>(),
      "ALPEncoding only supports float and double types.");

 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  ALPEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      const std::function<void*(uint32_t)>& /* stringBufferFactory */,
      const Encoding::Options& options = {})
      : TypedEncoding<T, physicalType>(memoryPool, data, options),
        data_(data.data() + this->dataOffset()),
        pos_(0) {}

  void reset() final {
    pos_ = 0;
  }

  void skip(uint32_t rowCount) final {
    pos_ += rowCount;
  }

  // TODO: Implement actual ALP decoding instead of raw memcpy.
  void materialize(uint32_t rowCount, void* buffer) final {
    auto* output = static_cast<physicalType*>(buffer);
    std::memcpy(
        output,
        data_ + pos_ * sizeof(physicalType),
        rowCount * sizeof(physicalType));
    pos_ += rowCount;
  }

  void materializeBoolsAsBits(
      uint32_t /*rowCount*/,
      uint64_t* /*buffer*/,
      int /*begin*/) final {
    NIMBLE_UNREACHABLE("ALP encoding does not support bool type.");
  }

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params) {
    auto skipFn = [&](auto toSkip) { pos_ += toSkip; };
    auto decodeFn = [&] {
      physicalType value;
      std::memcpy(&value, data_ + pos_ * sizeof(physicalType), sizeof(value));
      ++pos_;
      return value;
    };
    detail::readWithVisitorSlow(visitor, params, skipFn, decodeFn);
  }

  std::string debugString(int offset) const final {
    return fmt::format(
        "{}{}<{}> rowCount={}",
        std::string(offset, ' '),
        toString(this->encodingType()),
        toString(this->dataType()),
        this->rowCount());
  }

  // TODO: Implement actual ALP encoding instead of raw memcpy passthrough.
  static std::string_view encode(
      EncodingSelection<physicalType>& /* selection */,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {}) {
    const bool useVarint = options.useVarintRowCount;
    if (values.empty()) {
      NIMBLE_INCOMPATIBLE_ENCODING("ALP encoding cannot encode empty data.");
    }

    const uint32_t rowCount = values.size();
    const uint32_t dataBytes = rowCount * sizeof(physicalType);
    const uint32_t encodingSize =
        Encoding::serializePrefixSize(rowCount, useVarint) + dataBytes;
    char* reserved = buffer.reserve(encodingSize);
    char* pos = reserved;
    Encoding::serializePrefix(
        EncodingType::ALP, TypeTraits<T>::dataType, rowCount, useVarint, pos);
    std::memcpy(pos, values.data(), dataBytes);
    pos += dataBytes;
    NIMBLE_DCHECK_EQ(pos - reserved, encodingSize, "Encoding size mismatch.");
    return {reserved, encodingSize};
  }

 private:
  const char* data_;
  uint32_t pos_;
};

} // namespace facebook::nimble
