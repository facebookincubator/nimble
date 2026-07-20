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

#include <algorithm>

#include "dwio/nimble/encodings/ALPEncoding.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/views/EncodingViewFactory.h"
#include "velox/common/encode/Coding.h"

namespace facebook::nimble {

template <typename T>
class ALPEncodingView final : public TypedEncodingView<T> {
  static_assert(
      isFloatingPointType<T>(),
      "ALPEncodingView only supports float and double types.");

 public:
  using physicalType = typename TypedEncodingView<T>::physicalType;

  ALPEncodingView(
      std::string_view data,
      velox::memory::MemoryPool* pool,
      const Encoding::Options& options)
      : TypedEncodingView<T>{data, pool, options} {
    NIMBLE_CHECK_EQ(this->encodingType_, EncodingType::ALP);
    const char* pos = data.data() + this->dataOffset_;
    const auto header = detail::alp::readHeader(pos);
    exponent_ = header.exponent;
    factor_ = header.factor;
    exceptionCount_ = header.hasExceptions ? varint::readVarint32(&pos) : 0;
    const auto encodedValuesSize = varint::readVarint32(&pos);

    encodedValues_ = detail::createTypedEncodingView<uint64_t>(
        {pos, encodedValuesSize}, this->pool_, options);
    NIMBLE_CHECK_NOT_NULL(encodedValues_);
    pos += encodedValuesSize;

    exceptionPositions_ = reinterpret_cast<const uint32_t*>(pos);
    pos += exceptionCount_ * sizeof(uint32_t);
    exceptionValues_ = reinterpret_cast<const physicalType*>(pos);
  }

 private:
  T readTypedAt(uint32_t index) const final {
    NIMBLE_CHECK_LT(index, this->rowCount_);
    if (const auto* exception = findException(index)) {
      return detail::castFromPhysicalType<T>(*exception);
    }

    const auto encoded = velox::ZigZag::decode(encodedValues_->readAt(index));
    return decodeValue(encoded);
  }

  const physicalType* findException(uint32_t index) const {
    const auto* begin = exceptionPositions_;
    const auto* end = begin + exceptionCount_;
    const auto* it = std::lower_bound(begin, end, index);
    if (it == end || *it != index) {
      return nullptr;
    }
    return exceptionValues_ + (it - begin);
  }

  T decodeValue(int64_t encoded) const {
    return static_cast<T>(
        static_cast<double>(encoded) * ALPEncoding<T>::kPow10Double[factor_] /
        ALPEncoding<T>::kPow10Double[exponent_]);
  }

  uint8_t exponent_{0};
  uint8_t factor_{0};
  uint32_t exceptionCount_{0};
  std::unique_ptr<TypedEncodingView<uint64_t>> encodedValues_;
  const uint32_t* exceptionPositions_{nullptr};
  const physicalType* exceptionValues_{nullptr};
};

} // namespace facebook::nimble
