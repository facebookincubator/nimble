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
#include "dwio/nimble/encodings/EncodingFactoryNew.h"
#include "dwio/nimble/encodings/ConstantEncoding.h"
#include "dwio/nimble/encodings/DictionaryEncoding.h"
#include "dwio/nimble/encodings/EncodingSelection.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/MainlyConstantEncoding.h"
#include "dwio/nimble/encodings/NullableEncoding.h"
#include "dwio/nimble/encodings/RleEncoding.h"
#include "dwio/nimble/encodings/SparseBoolEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/VarintEncoding.h"

namespace facebook::nimble {

namespace {

template <typename T>
static std::span<const typename TypeTraits<T>::physicalType> toPhysicalSpan(
    std::span<const T> values) {
  return std::span<const typename TypeTraits<T>::physicalType>(
      reinterpret_cast<const typename TypeTraits<T>::physicalType*>(
          values.data()),
      values.size());
}
} // namespace

std::unique_ptr<Encoding> EncodingFactory::decode(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data) {
  // Maybe we should have a magic number of encodings too? Hrm.
  const EncodingType encodingType = static_cast<EncodingType>(data[0]);
  const DataType dataType = static_cast<DataType>(data[1]);
#define RETURN_ENCODING_BY_LEAF_TYPE(Encoding, dataType)                     \
  switch (dataType) {                                                        \
    case DataType::Int8:                                                     \
      return std::make_unique<Encoding<int8_t>>(memoryPool, data);           \
    case DataType::Uint8:                                                    \
      return std::make_unique<Encoding<uint8_t>>(memoryPool, data);          \
    case DataType::Int16:                                                    \
      return std::make_unique<Encoding<int16_t>>(memoryPool, data);          \
    case DataType::Uint16:                                                   \
      return std::make_unique<Encoding<uint16_t>>(memoryPool, data);         \
    case DataType::Int32:                                                    \
      return std::make_unique<Encoding<int32_t>>(memoryPool, data);          \
    case DataType::Uint32:                                                   \
      return std::make_unique<Encoding<uint32_t>>(memoryPool, data);         \
    case DataType::Int64:                                                    \
      return std::make_unique<Encoding<int64_t>>(memoryPool, data);          \
    case DataType::Uint64:                                                   \
      return std::make_unique<Encoding<uint64_t>>(memoryPool, data);         \
    case DataType::Float:                                                    \
      return std::make_unique<Encoding<float>>(memoryPool, data);            \
    case DataType::Double:                                                   \
      return std::make_unique<Encoding<double>>(memoryPool, data);           \
    case DataType::Bool:                                                     \
      return std::make_unique<Encoding<bool>>(memoryPool, data);             \
    case DataType::String:                                                   \
      return std::make_unique<Encoding<std::string_view>>(memoryPool, data); \
    default:                                                                 \
      NIMBLE_UNREACHABLE(fmt::format("Unknown encoding type {}.", dataType)) \
  }

#define RETURN_ENCODING_BY_INTEGER_TYPE(Encoding, dataType)          \
  switch (dataType) {                                                \
    case DataType::Int8:                                             \
      return std::make_unique<Encoding<int8_t>>(memoryPool, data);   \
    case DataType::Uint8:                                            \
      return std::make_unique<Encoding<uint8_t>>(memoryPool, data);  \
    case DataType::Int16:                                            \
      return std::make_unique<Encoding<int16_t>>(memoryPool, data);  \
    case DataType::Uint16:                                           \
      return std::make_unique<Encoding<uint16_t>>(memoryPool, data); \
    case DataType::Int32:                                            \
      return std::make_unique<Encoding<int32_t>>(memoryPool, data);  \
    case DataType::Uint32:                                           \
      return std::make_unique<Encoding<uint32_t>>(memoryPool, data); \
    case DataType::Int64:                                            \
      return std::make_unique<Encoding<int64_t>>(memoryPool, data);  \
    case DataType::Uint64:                                           \
      return std::make_unique<Encoding<uint64_t>>(memoryPool, data); \
    default:                                                         \
      NIMBLE_UNREACHABLE(fmt::format(                                \
          "Trying to deserialize an integer-only stream for "        \
          "a nonintegral data type {}.",                             \
          dataType));                                                \
  }

#define RETURN_ENCODING_BY_VARINT_TYPE(Encoding, dataType)           \
  switch (dataType) {                                                \
    case DataType::Int32:                                            \
      return std::make_unique<Encoding<int32_t>>(memoryPool, data);  \
    case DataType::Uint32:                                           \
      return std::make_unique<Encoding<uint32_t>>(memoryPool, data); \
    case DataType::Int64:                                            \
      return std::make_unique<Encoding<int64_t>>(memoryPool, data);  \
    case DataType::Uint64:                                           \
      return std::make_unique<Encoding<uint64_t>>(memoryPool, data); \
    case DataType::Float:                                            \
      return std::make_unique<Encoding<float>>(memoryPool, data);    \
    case DataType::Double:                                           \
      return std::make_unique<Encoding<double>>(memoryPool, data);   \
    default:                                                         \
      NIMBLE_UNREACHABLE(fmt::format(                                \
          "Trying to deserialize a varint stream for "               \
          "an incompatible data type {}.",                           \
          dataType));                                                \
  }

#define RETURN_ENCODING_BY_NON_BOOL_TYPE(Encoding, dataType)                 \
  switch (dataType) {                                                        \
    case DataType::Int8:                                                     \
      return std::make_unique<Encoding<int8_t>>(memoryPool, data);           \
    case DataType::Uint8:                                                    \
      return std::make_unique<Encoding<uint8_t>>(memoryPool, data);          \
    case DataType::Int16:                                                    \
      return std::make_unique<Encoding<int16_t>>(memoryPool, data);          \
    case DataType::Uint16:                                                   \
      return std::make_unique<Encoding<uint16_t>>(memoryPool, data);         \
    case DataType::Int32:                                                    \
      return std::make_unique<Encoding<int32_t>>(memoryPool, data);          \
    case DataType::Uint32:                                                   \
      return std::make_unique<Encoding<uint32_t>>(memoryPool, data);         \
    case DataType::Int64:                                                    \
      return std::make_unique<Encoding<int64_t>>(memoryPool, data);          \
    case DataType::Uint64:                                                   \
      return std::make_unique<Encoding<uint64_t>>(memoryPool, data);         \
    case DataType::Float:                                                    \
      return std::make_unique<Encoding<float>>(memoryPool, data);            \
    case DataType::Double:                                                   \
      return std::make_unique<Encoding<double>>(memoryPool, data);           \
    case DataType::String:                                                   \
      return std::make_unique<Encoding<std::string_view>>(memoryPool, data); \
    default:                                                                 \
      NIMBLE_UNREACHABLE(fmt::format(                                        \
          "Trying to deserialize a non-bool stream for "                     \
          "the bool data type {}.",                                          \
          dataType));                                                        \
  }

#define RETURN_ENCODING_BY_NUMERIC_TYPE(Encoding, dataType)          \
  switch (dataType) {                                                \
    case DataType::Int8:                                             \
      return std::make_unique<Encoding<int8_t>>(memoryPool, data);   \
    case DataType::Uint8:                                            \
      return std::make_unique<Encoding<uint8_t>>(memoryPool, data);  \
    case DataType::Int16:                                            \
      return std::make_unique<Encoding<int16_t>>(memoryPool, data);  \
    case DataType::Uint16:                                           \
      return std::make_unique<Encoding<uint16_t>>(memoryPool, data); \
    case DataType::Int32:                                            \
      return std::make_unique<Encoding<int32_t>>(memoryPool, data);  \
    case DataType::Uint32:                                           \
      return std::make_unique<Encoding<uint32_t>>(memoryPool, data); \
    case DataType::Int64:                                            \
      return std::make_unique<Encoding<int64_t>>(memoryPool, data);  \
    case DataType::Uint64:                                           \
      return std::make_unique<Encoding<uint64_t>>(memoryPool, data); \
    case DataType::Float:                                            \
      return std::make_unique<Encoding<float>>(memoryPool, data);    \
    case DataType::Double:                                           \
      return std::make_unique<Encoding<double>>(memoryPool, data);   \
    default:                                                         \
      NIMBLE_UNREACHABLE(fmt::format(                                \
          "Trying to deserialize a non-numeric stream for "          \
          "a numeric data type {}.",                                 \
          dataType));                                                \
  }

  switch (encodingType) {
    case EncodingType::Trivial: {
      RETURN_ENCODING_BY_LEAF_TYPE(TrivialEncoding, dataType);
    }
    case EncodingType::RLE: {
      RETURN_ENCODING_BY_LEAF_TYPE(RLEEncoding, dataType);
    }
    case EncodingType::Dictionary: {
      RETURN_ENCODING_BY_NON_BOOL_TYPE(DictionaryEncoding, dataType);
    }
    case EncodingType::FixedBitWidth: {
      RETURN_ENCODING_BY_NUMERIC_TYPE(FixedBitWidthEncoding, dataType);
    }
    case EncodingType::Nullable: {
      RETURN_ENCODING_BY_LEAF_TYPE(NullableEncoding, dataType);
    }
    case EncodingType::SparseBool: {
      NIMBLE_ASSERT(
          dataType == DataType::Bool,
          "Trying to deserialize a SparseBoolEncoding with a non-bool data type.");
      return std::make_unique<SparseBoolEncoding>(memoryPool, data);
    }
    case EncodingType::Varint: {
      RETURN_ENCODING_BY_VARINT_TYPE(VarintEncoding, dataType);
    }
    case EncodingType::Constant: {
      RETURN_ENCODING_BY_LEAF_TYPE(ConstantEncoding, dataType);
    }
    case EncodingType::MainlyConstant: {
      RETURN_ENCODING_BY_NON_BOOL_TYPE(MainlyConstantEncoding, dataType);
    }
    default: {
      NIMBLE_UNREACHABLE(
          "Trying to deserialize invalid EncodingType -- garbage input?");
    }
  }
}

template <typename T>
std::string_view EncodingFactory::encode(
    std::unique_ptr<EncodingSelectionPolicy<T>>&& selectorPolicy,
    std::span<const T> values,
    Buffer& buffer) {
  using physicalType = typename TypeTraits<T>::physicalType;
  auto physicalValues = toPhysicalSpan(values);
  auto statistics = Statistics<physicalType>::create(physicalValues);
  auto selectionResult = selectorPolicy->select(physicalValues, statistics);
  EncodingSelection<physicalType> selection{
      std::move(selectionResult),
      std::move(statistics),
      std::move(selectorPolicy)};
  return EncodingFactory::encode<T>(
      std::move(selection), physicalValues, buffer);
}

template <typename T>
std::string_view EncodingFactory::encodeNullable(
    std::unique_ptr<EncodingSelectionPolicy<T>>&& selectorPolicy,
    std::span<const T> values,
    std::span<const bool> nulls,
    Buffer& buffer) {
  using physicalType = typename TypeTraits<T>::physicalType;
  auto physicalValues = toPhysicalSpan(values);
  auto statistics = Statistics<physicalType>::create(physicalValues);
  auto selectionResult =
      selectorPolicy->selectNullable(physicalValues, nulls, statistics);
  EncodingSelection<physicalType> selection{
      std::move(selectionResult),
      std::move(statistics),
      std::move(selectorPolicy)};
  return EncodingFactory::encodeNullable<T>(
      std::move(selection), physicalValues, nulls, buffer);
}

template <typename T>
std::string_view EncodingFactory::encode(
    EncodingSelection<typename TypeTraits<T>::physicalType>&& selection,
    std::span<const typename TypeTraits<T>::physicalType> values,
    Buffer& buffer) {
  using physicalType = typename TypeTraits<T>::physicalType;
  auto castedValues = toPhysicalSpan(values);
  switch (selection.encodingType()) {
    case EncodingType::Constant: {
      return ConstantEncoding<T>::encode(selection, castedValues, buffer);
    }
    case EncodingType::Trivial: {
      return TrivialEncoding<T>::encode(selection, castedValues, buffer);
    }
    case EncodingType::RLE: {
      return RLEEncoding<T>::encode(selection, castedValues, buffer);
    }
    case EncodingType::Dictionary: {
      NIMBLE_DASSERT(
          (!std::is_same<T, bool>::value && !castedValues.empty()),
          "Invalid DictionaryEncoding selection.");
      return DictionaryEncoding<T>::encode(selection, castedValues, buffer);
    }
    case EncodingType::FixedBitWidth: {
      if constexpr (isNumericType<physicalType>()) {
        return FixedBitWidthEncoding<T>::encode(
            selection, castedValues, buffer);
      } else {
        NIMBLE_INCOMPATIBLE_ENCODING(
            "FixedBitWidth encoding should not be selected for non-numeric data types.");
      }
    }
    case EncodingType::Varint: {
      // TODO: we can support floating point types, but currently Statistics
      // doesn't calculate buckets for floating point types. We should convert
      // floating point types to their physical type, and then Statistics and
      // Varint encoding will just work.
      if constexpr (
          isNumericType<physicalType>() &&
          (sizeof(physicalType) == 4 || sizeof(T) == 8)) {
        return VarintEncoding<T>::encode(selection, castedValues, buffer);
      } else {
        NIMBLE_INCOMPATIBLE_ENCODING(
            "Varint encoding can only be selected for large numeric data types.");
      }
    }
    case EncodingType::MainlyConstant: {
      if constexpr (std::is_same<T, bool>::value) {
        NIMBLE_INCOMPATIBLE_ENCODING(
            "MainlyConstant encoding should not be selected for bool data types.");
      } else {
        return MainlyConstantEncoding<T>::encode(
            selection, castedValues, buffer);
      }
    }
    case EncodingType::SparseBool: {
      if constexpr (!std::is_same<T, bool>::value) {
        NIMBLE_INCOMPATIBLE_ENCODING(
            "SparseBool encoding should not be selected for non-bool data types.");
      } else {
        return SparseBoolEncoding::encode(selection, castedValues, buffer);
      }
    }
    default: {
      NIMBLE_NOT_SUPPORTED(fmt::format(
          "Encoding {} is not supported.", toString(selection.encodingType())));
    }
  }
}

template <typename T>
std::string_view EncodingFactory::encodeNullable(
    EncodingSelection<typename TypeTraits<T>::physicalType>&& selection,
    std::span<const typename TypeTraits<T>::physicalType> values,
    std::span<const bool> nulls,
    Buffer& buffer) {
  auto physicalValues = toPhysicalSpan(values);
  switch (selection.encodingType()) {
    case EncodingType::Nullable: {
      return NullableEncoding<T>::encodeNullable(
          selection, physicalValues, nulls, buffer);
    }
    default: {
      NIMBLE_NOT_SUPPORTED(fmt::format(
          "Encoding {} is not supported for nullable data.",
          toString(selection.encodingType())));
    }
  }
}

#define DEFINE_TEMPLATES(type)                                                 \
  template std::string_view EncodingFactory::encode<type>(                     \
      std::unique_ptr<EncodingSelectionPolicy<type>> && selectorPolicy,        \
      std::span<const type> values,                                            \
      Buffer & buffer);                                                        \
  template std::string_view EncodingFactory::encodeNullable<type>(             \
      std::unique_ptr<EncodingSelectionPolicy<type>> && selectorPolicy,        \
      std::span<const type> values,                                            \
      std::span<const bool> nulls,                                             \
      Buffer & buffer);                                                        \
  template std::string_view EncodingFactory::encode<type>(                     \
      EncodingSelection<typename TypeTraits<type>::physicalType> && selection, \
      std::span<const typename TypeTraits<type>::physicalType> values,         \
      Buffer & buffer);                                                        \
  template std::string_view EncodingFactory::encodeNullable<type>(             \
      EncodingSelection<typename TypeTraits<type>::physicalType> && selection, \
      std::span<const typename TypeTraits<type>::physicalType> values,         \
      std::span<const bool> nulls,                                             \
      Buffer & buffer);

DEFINE_TEMPLATES(int8_t);
DEFINE_TEMPLATES(uint8_t);
DEFINE_TEMPLATES(int16_t);
DEFINE_TEMPLATES(uint16_t);
DEFINE_TEMPLATES(int32_t);
DEFINE_TEMPLATES(uint32_t);
DEFINE_TEMPLATES(int64_t);
DEFINE_TEMPLATES(uint64_t);
DEFINE_TEMPLATES(float);
DEFINE_TEMPLATES(double);
DEFINE_TEMPLATES(bool);
DEFINE_TEMPLATES(std::string_view);

} // namespace facebook::nimble
