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

#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/views/EncodingViewFactory.h"

namespace facebook::nimble {

template <typename T>
class MainlyConstantEncodingView final : public TypedEncodingView<T> {
 public:
  using physicalType = typename TypedEncodingView<T>::physicalType;

  MainlyConstantEncodingView(
      std::string_view data,
      velox::memory::MemoryPool* pool,
      const Encoding::Options& options)
      : TypedEncodingView<T>{data, pool, options},
        isCommon_{this->template getVectorBuffer<bool>()},
        otherValueIndices_{this->template getVectorBuffer<uint32_t>()} {
    NIMBLE_CHECK_EQ(this->encodingType_, EncodingType::MainlyConstant);
    const char* pos = data.data() + this->dataOffset_;
    const auto isCommonSize = encoding::readUint32(pos);
    auto noStringBufferFactory = [](uint32_t) -> void* { return nullptr; };
    auto isCommon = EncodingFactory{}.create(
        *this->pool_, {pos, isCommonSize}, noStringBufferFactory, options);
    NIMBLE_CHECK_NOT_NULL(isCommon);
    isCommon_.resize(this->rowCount_);
    isCommon->materialize(this->rowCount_, isCommon_.data());
    pos += isCommonSize;

    const auto otherValuesSize = encoding::readUint32(pos);
    otherValues_ = detail::createTypedEncodingView<T>(
        {pos, otherValuesSize}, this->pool_, options);
    NIMBLE_CHECK_NOT_NULL(otherValues_);
    pos += otherValuesSize;

    commonValue_ = encoding::read<physicalType>(pos);
    NIMBLE_CHECK_EQ(pos, data.end(), "Unexpected MainlyConstant view end.");

    uint32_t otherValueCount = 0;
    otherValueIndices_.resize(this->rowCount_);
    for (uint32_t i = 0; i < this->rowCount_; ++i) {
      otherValueIndices_[i] = otherValueCount;
      if (!isCommon_[i]) {
        ++otherValueCount;
      }
    }
    NIMBLE_CHECK_EQ(otherValueCount, otherValues_->rowCount());
  }

  ~MainlyConstantEncodingView() override {
    this->releaseVectorBuffer(otherValueIndices_);
    this->releaseVectorBuffer(isCommon_);
  }

 private:
  uint32_t otherValueIndex(uint32_t index) const {
    return otherValueIndices_[index];
  }

  T readTypedAt(uint32_t index) const final {
    NIMBLE_CHECK_LT(index, this->rowCount_);
    if (isCommon_[index]) {
      return detail::castFromPhysicalType<T>(commonValue_);
    }
    return otherValues_->readAt(otherValueIndex(index));
  }

  Vector<bool> isCommon_;
  Vector<uint32_t> otherValueIndices_;
  std::unique_ptr<TypedEncodingView<T>> otherValues_;
  physicalType commonValue_;
};

} // namespace facebook::nimble
