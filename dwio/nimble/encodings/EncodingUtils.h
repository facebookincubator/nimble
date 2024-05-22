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

#include "dwio/nimble/encodings/ConstantEncoding.h"
#include "dwio/nimble/encodings/DictionaryEncoding.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/MainlyConstantEncoding.h"
#include "dwio/nimble/encodings/NullableEncoding.h"
#include "dwio/nimble/encodings/RleEncoding.h"
#include "dwio/nimble/encodings/SparseBoolEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/VarintEncoding.h"

namespace facebook::nimble {

template <typename F>
auto encodingTypeDispatch(Encoding& encoding, F&& f);

template <typename DecoderVisitor>
void callReadWithVisitor(
    Encoding& encoding,
    DecoderVisitor& visitor,
    ReadWithVisitorParams& params);

namespace detail {

template <template <typename D> typename E, typename F>
auto encodingTypeDispatchNumeric(Encoding& encoding, F f) {
  switch (encoding.dataType()) {
    case DataType::Int8:
      return f(static_cast<E<int8_t>&>(encoding));
    case DataType::Uint8:
      return f(static_cast<E<uint8_t>&>(encoding));
    case DataType::Int16:
      return f(static_cast<E<int16_t>&>(encoding));
    case DataType::Uint16:
      return f(static_cast<E<uint16_t>&>(encoding));
    case DataType::Int32:
      return f(static_cast<E<int32_t>&>(encoding));
    case DataType::Uint32:
      return f(static_cast<E<uint32_t>&>(encoding));
    case DataType::Int64:
      return f(static_cast<E<int64_t>&>(encoding));
    case DataType::Uint64:
      return f(static_cast<E<uint64_t>&>(encoding));
    case DataType::Float:
      return f(static_cast<E<float>&>(encoding));
    case DataType::Double:
      return f(static_cast<E<double>&>(encoding));
    case DataType::Bool:
      return f(static_cast<E<bool>&>(encoding));
    default:
      NIMBLE_NOT_SUPPORTED(toString(encoding.dataType()));
  }
}

template <template <typename D> typename E, typename F>
auto encodingTypeDispatchVarint(Encoding& encoding, F f) {
  switch (encoding.dataType()) {
    case DataType::Int32:
      return f(static_cast<E<int32_t>&>(encoding));
    case DataType::Uint32:
      return f(static_cast<E<uint32_t>&>(encoding));
    case DataType::Int64:
      return f(static_cast<E<int64_t>&>(encoding));
    case DataType::Uint64:
      return f(static_cast<E<uint64_t>&>(encoding));
    case DataType::Float:
      return f(static_cast<E<float>&>(encoding));
    case DataType::Double:
      return f(static_cast<E<double>&>(encoding));
    default:
      NIMBLE_NOT_SUPPORTED(toString(encoding.dataType()));
  }
}

template <typename F>
auto encodingTypeDispatchString(Encoding& encoding, F f) {
  switch (encoding.encodingType()) {
    case EncodingType::Trivial:
      return f(static_cast<TrivialEncoding<std::string_view>&>(encoding));
    case EncodingType::RLE:
      return f(static_cast<RLEEncoding<std::string_view>&>(encoding));
    case EncodingType::Dictionary:
      return f(static_cast<DictionaryEncoding<std::string_view>&>(encoding));
    case EncodingType::Nullable:
      return f(static_cast<NullableEncoding<std::string_view>&>(encoding));
    case EncodingType::Constant:
      return f(static_cast<ConstantEncoding<std::string_view>&>(encoding));
    case EncodingType::MainlyConstant:
      return f(
          static_cast<MainlyConstantEncoding<std::string_view>&>(encoding));
    default:
      NIMBLE_NOT_SUPPORTED(toString(encoding.encodingType()));
  }
}

template <typename F>
auto encodingTypeDispatchNonString(Encoding& encoding, F&& f) {
  switch (encoding.encodingType()) {
    case EncodingType::Trivial:
      return detail::encodingTypeDispatchNumeric<TrivialEncoding>(
          encoding, std::forward<F>(f));
    case EncodingType::RLE:
      return detail::encodingTypeDispatchNumeric<RLEEncoding>(
          encoding, std::forward<F>(f));
    case EncodingType::Dictionary:
      return detail::encodingTypeDispatchNumeric<DictionaryEncoding>(
          encoding, std::forward<F>(f));
    case EncodingType::FixedBitWidth:
      return detail::encodingTypeDispatchNumeric<FixedBitWidthEncoding>(
          encoding, std::forward<F>(f));
    case EncodingType::Nullable:
      return detail::encodingTypeDispatchNumeric<NullableEncoding>(
          encoding, std::forward<F>(f));
    case EncodingType::SparseBool:
      return f(static_cast<SparseBoolEncoding&>(encoding));
    case EncodingType::Varint:
      return detail::encodingTypeDispatchVarint<VarintEncoding>(
          encoding, std::forward<F>(f));
    case EncodingType::Constant:
      return detail::encodingTypeDispatchNumeric<ConstantEncoding>(
          encoding, std::forward<F>(f));
    case EncodingType::MainlyConstant:
      return detail::encodingTypeDispatchNumeric<MainlyConstantEncoding>(
          encoding, std::forward<F>(f));
    default:
      NIMBLE_NOT_SUPPORTED(toString(encoding.encodingType()));
  }
}

} // namespace detail

template <typename F>
auto encodingTypeDispatch(Encoding& encoding, F&& f) {
  if (encoding.dataType() == DataType::String) {
    return detail::encodingTypeDispatchString(encoding, std::forward<F>(f));
  } else {
    return detail::encodingTypeDispatchNonString(encoding, std::forward<F>(f));
  }
}

template <typename V>
void callReadWithVisitor(
    Encoding& encoding,
    V& visitor,
    ReadWithVisitorParams& params) {
  if constexpr (std::is_same_v<typename V::DataType, folly::StringPiece>) {
    detail::encodingTypeDispatchString(encoding, [&](auto& typedEncoding) {
      typedEncoding.readWithVisitor(visitor, params);
    });
  } else if constexpr (std::is_same_v<typename V::DataType, velox::int128_t>) {
    NIMBLE_NOT_SUPPORTED("Int128 is not supported in Nimble");
  } else {
    detail::encodingTypeDispatchNonString(encoding, [&](auto& typedEncoding) {
      typedEncoding.readWithVisitor(visitor, params);
    });
  }
}

} // namespace facebook::nimble
