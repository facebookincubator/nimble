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
#include "dwio/nimble/encodings/EncodingSliceFactory.h"

#include <algorithm>
#include <span>
#include <vector>

#include "dwio/nimble/common/NimbleException.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/ConstantEncoding.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/MainlyConstantEncoding.h"
#include "dwio/nimble/encodings/NullableEncoding.h"
#include "dwio/nimble/encodings/RLEEncoding.h"
#include "dwio/nimble/encodings/SparseBoolEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingLayout.h"
#include "dwio/nimble/encodings/common/EncodingPrefix.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/common/EncodingTypeDispatch.h"
#include "dwio/nimble/encodings/selection/EncodingSelectionPolicy.h"

namespace facebook::nimble {
namespace {

template <typename T>
std::string_view encodeValuesWithLayout(
    EncodingLayout encodingLayout,
    std::span<const T> values,
    Buffer& buffer,
    const Encoding::Options& options) {
  auto policy = std::make_unique<ReplayedEncodingSelectionPolicy<T>>(
      std::move(encodingLayout),
      CompressionOptions{},
      [](DataType dataType) -> std::unique_ptr<EncodingSelectionPolicyBase> {
        NIMBLE_FAIL(
            "Captured encoding layout is missing nested child for {}.",
            dataType);
      });
  return EncodingFactory::encode<T>(std::move(policy), values, buffer, options);
}

template <typename T>
std::string_view sliceByMaterializing(
    std::string_view encoded,
    uint32_t offset,
    uint32_t length,
    Buffer& buffer,
    const Encoding::Options& options) {
  using physicalType = typename TypeTraits<T>::physicalType;
  auto* pool = &buffer.getMemoryPool();
  ScopedEncodingBuffer scopedBuffer{pool, options.encodingBufferPool};
  Vector<physicalType> physicalValues{pool, length};

  auto encoding = EncodingFactory{options}.create(
      *pool, encoded, [&scopedBuffer](uint32_t size) -> void* {
        return scopedBuffer.get().reserve(size);
      });
  encoding->skip(offset);
  encoding->materialize(length, physicalValues.data());

  return encodeValuesWithLayout<T>(
      EncodingLayoutCapture::capture(encoded),
      {reinterpret_cast<const T*>(physicalValues.data()),
       physicalValues.size()},
      buffer,
      options);
}

std::string_view sliceByMaterializing(
    std::string_view encoded,
    EncodingType encodingType,
    DataType dataType,
    uint32_t offset,
    uint32_t length,
    Buffer& buffer,
    const Encoding::Options& options) {
  NIMBLE_CHECK_NE(
      encodingType,
      EncodingType::Nullable,
      "Slicing nullable {} encoding by materializing is not supported.",
      encodingType);
  NIMBLE_RETURN_BY_DATA_TYPE(
      dataType,
      T,
      sliceByMaterializing<T>(encoded, offset, length, buffer, options));
}

std::string_view sliceNullable(
    std::string_view encoded,
    DataType dataType,
    uint32_t offset,
    uint32_t length,
    Buffer& buffer,
    const Encoding::Options& options) {
  NIMBLE_RETURN_BY_DATA_TYPE(
      dataType,
      T,
      NullableEncoding<T>::slice(encoded, offset, length, buffer, options));
}

std::string_view sliceMainlyConstant(
    std::string_view encoded,
    DataType dataType,
    uint32_t offset,
    uint32_t length,
    Buffer& buffer,
    const Encoding::Options& options) {
  NIMBLE_RETURN_BY_NON_BOOL_DATA_TYPE(
      dataType,
      T,
      MainlyConstantEncoding<T>::slice(
          encoded, offset, length, buffer, options));
}

std::string_view sliceTrivial(
    std::string_view encoded,
    DataType dataType,
    uint32_t offset,
    uint32_t length,
    Buffer& buffer,
    const Encoding::Options& options) {
  NIMBLE_RETURN_BY_DATA_TYPE(
      dataType,
      T,
      TrivialEncoding<T>::slice(encoded, offset, length, buffer, options));
}

std::string_view sliceRLE(
    std::string_view encoded,
    DataType dataType,
    uint32_t offset,
    uint32_t length,
    Buffer& buffer,
    const Encoding::Options& options) {
  NIMBLE_RETURN_BY_DATA_TYPE(
      dataType,
      T,
      RLEEncoding<T>::slice(encoded, offset, length, buffer, options));
}

std::string_view sliceConstant(
    std::string_view encoded,
    DataType dataType,
    uint32_t offset,
    uint32_t length,
    Buffer& buffer,
    const Encoding::Options& options) {
  NIMBLE_RETURN_BY_DATA_TYPE(
      dataType,
      T,
      ConstantEncoding<T>::slice(encoded, offset, length, buffer, options));
}

std::string_view sliceFixedBitWidth(
    std::string_view encoded,
    DataType dataType,
    uint32_t offset,
    uint32_t length,
    Buffer& buffer,
    const Encoding::Options& options) {
  NIMBLE_RETURN_BY_NUMERIC_DATA_TYPE(
      dataType,
      T,
      FixedBitWidthEncoding<T>::slice(
          encoded, offset, length, buffer, options));
}

} // namespace

std::string_view EncodingSliceFactory::slice(
    std::string_view encoded,
    uint32_t offset,
    uint32_t length,
    Buffer& buffer,
    const Encoding::Options& options) {
  const auto rowCount =
      EncodingPrefix::readRowCount(encoded, options.useVarintRowCount);
  NIMBLE_CHECK_LE(offset, rowCount);
  NIMBLE_CHECK_LE(length, rowCount - offset);
  if (offset == 0 && length == rowCount) {
    return encoded;
  }

  const auto encodingType = EncodingPrefix::encodingType(encoded);
  const auto dataType = EncodingPrefix::dataType(encoded);
  switch (encodingType) {
    case EncodingType::Constant:
      return sliceConstant(encoded, dataType, offset, length, buffer, options);
    case EncodingType::Trivial:
      return sliceTrivial(encoded, dataType, offset, length, buffer, options);
    case EncodingType::RLE:
      return sliceRLE(encoded, dataType, offset, length, buffer, options);
    case EncodingType::FixedBitWidth:
      return sliceFixedBitWidth(
          encoded, dataType, offset, length, buffer, options);
    case EncodingType::Nullable:
      return sliceNullable(encoded, dataType, offset, length, buffer, options);
    case EncodingType::SparseBool:
      return SparseBoolEncoding::slice(
          encoded, offset, length, buffer, options);
    case EncodingType::MainlyConstant:
      return sliceMainlyConstant(
          encoded, dataType, offset, length, buffer, options);
    default:
      return sliceByMaterializing(
          encoded, encodingType, dataType, offset, length, buffer, options);
  }
}

} // namespace facebook::nimble
