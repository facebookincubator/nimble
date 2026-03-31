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

#include "dwio/nimble/encodings/EncodingUtils.h"
#include "dwio/nimble/encodings/legacy/ConstantEncoding.h"
#include "dwio/nimble/encodings/legacy/DeltaEncoding.h"
#include "dwio/nimble/encodings/legacy/DictionaryEncoding.h"
#include "dwio/nimble/encodings/legacy/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/legacy/MainlyConstantEncoding.h"
#include "dwio/nimble/encodings/legacy/NullableEncoding.h"
#include "dwio/nimble/encodings/legacy/RleEncoding.h"
#include "dwio/nimble/encodings/legacy/SparseBoolEncoding.h"
#include "dwio/nimble/encodings/legacy/TrivialEncoding.h"
#include "dwio/nimble/encodings/legacy/VarintEncoding.h"

namespace facebook::nimble::legacy {

template <typename DecoderVisitor>
void callReadWithVisitor(
    Encoding& encoding,
    DecoderVisitor& visitor,
    ReadWithVisitorParams& params);

namespace detail {

template <typename F>
auto encodingTypeDispatchString(Encoding& encoding, F f) {
  NIMBLE_CHECK_EQ(
      encoding.dataType(),
      DataType::String,
      "{}",
      toString(encoding.dataType()));
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
      NIMBLE_UNSUPPORTED(toString(encoding.encodingType()));
  }
}

template <typename T, typename F>
auto encodingTypeDispatchNonString(Encoding& encoding, F&& f) {
  NIMBLE_CHECK_EQ(
      ::facebook::nimble::detail::dataTypeSize(encoding.dataType()),
      sizeof(T),
      "{}",
      toString(encoding.dataType()));
  switch (encoding.encodingType()) {
    case EncodingType::Trivial:
      return f(static_cast<TrivialEncoding<T>&>(encoding));
    case EncodingType::RLE:
      return f(static_cast<RLEEncoding<T>&>(encoding));
    case EncodingType::Dictionary:
      return f(static_cast<DictionaryEncoding<T>&>(encoding));
    case EncodingType::FixedBitWidth:
      return f(static_cast<FixedBitWidthEncoding<T>&>(encoding));
    case EncodingType::Nullable:
      return f(static_cast<NullableEncoding<T>&>(encoding));
    case EncodingType::SparseBool:
      if constexpr (std::is_same_v<T, bool>) {
        return f(static_cast<SparseBoolEncoding&>(encoding));
      } else {
        NIMBLE_UNREACHABLE(toString(encoding.dataType()));
      }
    case EncodingType::Varint:
      if constexpr (folly::IsOneOf<
                        T,
                        int32_t,
                        uint32_t,
                        int64_t,
                        uint64_t,
                        float,
                        double>::value) {
        return f(static_cast<VarintEncoding<T>&>(encoding));
      } else {
        NIMBLE_UNREACHABLE(toString(encoding.dataType()));
      }
    case EncodingType::Constant:
      return f(static_cast<ConstantEncoding<T>&>(encoding));
    case EncodingType::MainlyConstant:
      return f(static_cast<MainlyConstantEncoding<T>&>(encoding));
    case EncodingType::Delta:
      return f(static_cast<DeltaEncoding<T>&>(encoding));
    default:
      NIMBLE_UNSUPPORTED(toString(encoding.encodingType()));
  }
}

} // namespace detail

template <typename V>
void callReadWithVisitor(
    Encoding& encoding,
    V& visitor,
    ReadWithVisitorParams& params) {
  using T = typename V::DataType;
  if constexpr (std::is_same_v<T, std::string_view>) {
    detail::encodingTypeDispatchString(encoding, [&](auto& typedEncoding) {
      typedEncoding.readWithVisitor(visitor, params);
    });
  } else if constexpr (std::is_same_v<T, velox::int128_t>) {
    NIMBLE_UNSUPPORTED("Int128 is not supported in Nimble");
  } else if constexpr (std::is_same_v<T, int8_t>) {
    if (encoding.dataType() == DataType::Bool) {
      detail::encodingTypeDispatchNonString<bool>(
          encoding, [&](auto& typedEncoding) {
            typedEncoding.readWithVisitor(visitor, params);
          });
    } else {
      detail::encodingTypeDispatchNonString<int8_t>(
          encoding, [&](auto& typedEncoding) {
            typedEncoding.readWithVisitor(visitor, params);
          });
    }
  } else {
    detail::encodingTypeDispatchNonString<T>(
        encoding, [&](auto& typedEncoding) {
          typedEncoding.readWithVisitor(visitor, params);
        });
  }
}

} // namespace facebook::nimble::legacy
