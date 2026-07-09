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

#include <cstdint>
#include <memory>
#include <string_view>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/common/EncodingPrefix.h"
#include "dwio/nimble/encodings/common/EncodingType.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble {

class EncodingView {
 public:
  virtual ~EncodingView() = default;

  /// Reads the decoded value at the given row index into a typed output buffer.
  virtual void readAt(uint32_t index, void* output) const = 0;

  /// Returns the number of rows in the encoded stream.
  uint32_t rowCount() const {
    return rowCount_;
  }

  /// Returns the encoding type backing this view.
  EncodingType encodingType() const {
    return encodingType_;
  }

  /// Returns the logical data type stored by this view.
  DataType dataType() const {
    return dataType_;
  }

 protected:
  EncodingView(
      std::string_view data,
      velox::memory::MemoryPool* pool,
      const Encoding::Options& options)
      : data_{data},
        pool_{pool},
        options_{options},
        encodingType_{static_cast<EncodingType>(
            data[EncodingPrefix::kEncodingTypeOffset])},
        dataType_{EncodingPrefix::readDataType(data)},
        rowCount_{
            EncodingPrefix::readRowCount(data, options.useVarintRowCount)},
        dataOffset_{
            EncodingPrefix::readPrefixSize(data, options.useVarintRowCount)} {
    NIMBLE_CHECK_NOT_NULL(pool_);
  }

  template <typename V>
  Vector<V> getVectorBuffer() {
    if (auto* bufferPool = options_.bufferPool) {
      if (auto buffer = bufferPool->get()) {
        return Vector<V>{std::move(buffer)};
      }
    }
    return Vector<V>{pool_};
  }

  void releaseVectorBuffer(auto& vector) {
    if (auto* bufferPool = options_.bufferPool) {
      bufferPool->release(vector.releaseBuffer());
    }
  }

  std::string_view data_;
  velox::memory::MemoryPool* pool_;
  Encoding::Options options_;
  EncodingType encodingType_;
  DataType dataType_;
  uint32_t rowCount_;
  uint32_t dataOffset_;
};

template <typename T>
class TypedEncodingView : public EncodingView {
 public:
  using physicalType = typename TypeTraits<T>::physicalType;

  T readAt(uint32_t index) const {
    T value;
    readAt(index, &value);
    return value;
  }

  void readAt(uint32_t index, void* output) const final {
    *static_cast<T*>(output) = readTypedAt(index);
  }

 protected:
  TypedEncodingView(
      std::string_view data,
      velox::memory::MemoryPool* pool,
      const Encoding::Options& options)
      : EncodingView{data, pool, options} {}

  virtual T readTypedAt(uint32_t index) const = 0;
};

} // namespace facebook::nimble
