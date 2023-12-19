// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <span>
#include <type_traits>

#include "dwio/alpha/common/Types.h"

namespace facebook::alpha {

// This class is used to manage semantic type and its corresponding encoding
// type. For most data types, they are the same. They will be different for
// floating point types. The main reason it is different for floating point
// numbers are because of +/-0.0 ( +0.0 == -0.0 but their sign bits are
// different), and NaNs can have different payload bits but even if NaNs
// with the same bits, == will return false, that causes trouble for encoding.
// So they will be treated as integers during encoding (their type will
// still be encoded as double/float)
template <typename T>
struct EncodingPhysicalType {
  using type = typename TypeTraits<T>::physicalType;

  static std::span<const type> asEncodingPhysicalTypeSpan(
      std::span<const T> values) {
    return std::span<const type>(
        reinterpret_cast<const type*>(values.data()), values.size());
  }

  static type asEncodingPhysicalType(T v) {
    return *reinterpret_cast<type*>(&v);
  }

  static T asEncodingLogicalType(type v) {
    return *reinterpret_cast<T*>(&v);
  }
};

#define AS(D, v) *(reinterpret_cast<D*>(&(v)))

#define AS_CONST(D, v) *(reinterpret_cast<const D*>(&(v)))
} // namespace facebook::alpha
