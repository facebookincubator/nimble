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
#include "dwio/nimble/common/Bits.h"
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingSelection.h"

// Encodes data that is constant, i.e. there is a single unique value.

namespace facebook::nimble {

// Data layout is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// X bytes: the constant value via encoding primitive.

template <typename T>
class ConstantEncodingBase
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  ConstantEncodingBase(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data)
      : TypedEncoding<T, physicalType>(memoryPool, data) {}

  void reset() final {}

  void skip(uint32_t /* rowCount */) final {}

  void materialize(uint32_t rowCount, void* buffer) final {
    physicalType* castBuffer = static_cast<physicalType*>(buffer);
    for (uint32_t i = 0; i < rowCount; ++i) {
      castBuffer[i] = value_;
    }
  }

  void materializeBoolsAsBits(
      uint32_t /*rowCount*/,
      uint64_t* /*buffer*/,
      int /*begin*/) override {
    NIMBLE_UNREACHABLE("");
  }

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params) {
    detail::readWithVisitorSlow(
        visitor, params, nullptr, [&] { return value_; });
  }

  std::string debugString(int offset) const final {
    return fmt::format(
        "{}{}<{}> rowCount={} value={}",
        std::string(offset, ' '),
        toString(this->encodingType()),
        toString(this->dataType()),
        this->rowCount(),
        AS_CONST(T, value_));
  }

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer) {
    if (values.empty()) {
      NIMBLE_INCOMPATIBLE_ENCODING("ConstantEncoding cannot be empty.");
    }

    if (selection.statistics().uniqueCounts().value().size() != 1) {
      NIMBLE_INCOMPATIBLE_ENCODING("ConstantEncoding requires constant data.");
    }

    const uint32_t rowCount = values.size();
    uint32_t encodingSize = Encoding::kPrefixSize;
    if constexpr (isStringType<physicalType>()) {
      encodingSize += 4 + values[0].size();
    } else {
      encodingSize += sizeof(physicalType);
    }
    char* reserved = buffer.reserve(encodingSize);
    char* pos = reserved;
    Encoding::serializePrefix(
        EncodingType::Constant, TypeTraits<T>::dataType, rowCount, pos);
    encoding::write<physicalType>(values[0], pos);
    NIMBLE_DCHECK_EQ(pos - reserved, encodingSize, "Encoding size mismatch.");
    return {reserved, encodingSize};
  }

 protected:
  physicalType value_;
};

template <typename T>
class ConstantEncoding : public ConstantEncodingBase<T> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  ConstantEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory);
};

//
// End of public API. Implementation follows.
//

template <typename T>
ConstantEncoding<T>::ConstantEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    std::function<void*(uint32_t)> stringBufferFactory)
    : ConstantEncodingBase<T>(memoryPool, data) {
  const char* pos = data.data() + Encoding::kPrefixSize;
  this->value_ = encoding::read<physicalType>(pos);
  NIMBLE_CHECK(pos == data.end(), "Unexpected constant encoding end");
}

// Specialization for bool to override materializeBoolsAsBits
template <>
class ConstantEncoding<bool> final : public ConstantEncodingBase<bool> {
 public:
  using cppDataType = bool;
  using physicalType = bool;

  ConstantEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory)
      : ConstantEncodingBase<bool>(memoryPool, data) {
    const char* pos = data.data() + Encoding::kPrefixSize;
    this->value_ = encoding::read<physicalType>(pos);
    NIMBLE_CHECK(pos == data.end(), "Unexpected constant encoding end");
  }

  void materializeBoolsAsBits(uint32_t rowCount, uint64_t* buffer, int begin)
      final {
    velox::bits::fillBits(buffer, begin, begin + rowCount, this->value_);
  }
};

template <>
class ConstantEncoding<std::string_view> final
    : public ConstantEncodingBase<std::string_view> {
 public:
  using cppDataType = std::string_view;
  using physicalType = std::string_view;

  ConstantEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory);
};
} // namespace facebook::nimble
