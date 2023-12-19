// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <memory>
#include <span>

#include "dwio/alpha/common/Buffer.h"
#include "dwio/alpha/encodings/Encoding.h"

namespace facebook::alpha {

template <typename T>
class EncodingSelection;

template <typename T>
class EncodingSelectionPolicy;

class EncodingFactory {
 public:
  static std::unique_ptr<Encoding> decode(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data);

  template <typename T>
  static std::string_view encode(
      std::unique_ptr<EncodingSelectionPolicy<T>>&& selectorPolicy,
      std::span<const T> values,
      Buffer& buffer);

  template <typename T>
  static std::string_view encodeNullable(
      std::unique_ptr<EncodingSelectionPolicy<T>>&& selectorPolicy,
      std::span<const T> values,
      std::span<const bool> nulls,
      Buffer& buffer);

 private:
  template <typename T>
  static std::string_view encode(
      EncodingSelection<typename TypeTraits<T>::physicalType>&& selection,
      std::span<const typename TypeTraits<T>::physicalType> values,
      Buffer& buffer);

  template <typename T>
  static std::string_view encodeNullable(
      EncodingSelection<typename TypeTraits<T>::physicalType>&& selection,
      std::span<const typename TypeTraits<T>::physicalType> values,
      std::span<const bool> nulls,
      Buffer& buffer);

  friend class EncodingSelection<int8_t>;
  friend class EncodingSelection<uint8_t>;
  friend class EncodingSelection<int16_t>;
  friend class EncodingSelection<uint16_t>;
  friend class EncodingSelection<int32_t>;
  friend class EncodingSelection<uint32_t>;
  friend class EncodingSelection<int64_t>;
  friend class EncodingSelection<uint64_t>;
  friend class EncodingSelection<float>;
  friend class EncodingSelection<double>;
  friend class EncodingSelection<bool>;
  friend class EncodingSelection<std::string_view>;
};

} // namespace facebook::alpha
