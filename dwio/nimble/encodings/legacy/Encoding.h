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

// Import the original Encoding base classes
#include "dwio/nimble/encodings/Encoding.h"

// Release namespace derives from original base classes
namespace facebook::nimble::legacy {

// Import base classes from original namespace via type aliases
using Encoding = ::facebook::nimble::Encoding;
template <typename T, typename physicalType>
using TypedEncoding = ::facebook::nimble::TypedEncoding<T, physicalType>;

// Import supporting types
using ReadWithVisitorParams = ::facebook::nimble::ReadWithVisitorParams;
template <typename T, typename Filter, typename ExtractValues, bool kIsDense>
using DecoderVisitor =
    ::facebook::nimble::DecoderVisitor<T, Filter, ExtractValues, kIsDense>;
using vector_size_t = ::facebook::nimble::vector_size_t;

// Import callReadWithVisitor helper
template <typename V>
void callReadWithVisitor(
    Encoding& encoding,
    V& visitor,
    ReadWithVisitorParams& params) {
  ::facebook::nimble::callReadWithVisitor(encoding, visitor, params);
}

// Import detail namespace utilities
namespace detail {
template <typename T, uint16_t BufferSize>
using BufferedEncoding =
    ::facebook::nimble::detail::BufferedEncoding<T, BufferSize>;

template <typename T, typename PhysicalType>
T castFromPhysicalType(const PhysicalType& value) {
  return ::facebook::nimble::detail::castFromPhysicalType<T, PhysicalType>(
      value);
}

template <typename DecoderVisitor, typename Skip, typename F>
void readWithVisitorSlow(
    DecoderVisitor& visitor,
    const ReadWithVisitorParams& params,
    Skip skip,
    F decodeOne) {
  ::facebook::nimble::detail::readWithVisitorSlow(
      visitor, params, skip, decodeOne);
}

template <typename TEncoding, typename V>
void readWithVisitorFast(
    TEncoding& encoding,
    V& visitor,
    ReadWithVisitorParams& params,
    const uint64_t* nulls) {
  ::facebook::nimble::detail::readWithVisitorFast(
      encoding, visitor, params, nulls);
}

template <typename DataType>
using ValueType = ::facebook::nimble::detail::ValueType<DataType>;

template <typename V, typename DataType>
ValueType<DataType> dataToValue(const V& visitor, DataType data) {
  return ::facebook::nimble::detail::dataToValue(visitor, data);
}

template <typename T, typename V>
T* mutableValues(const V& visitor, vector_size_t size) {
  return ::facebook::nimble::detail::mutableValues<T, V>(visitor, size);
}

} // namespace detail

} // namespace facebook::nimble::legacy
