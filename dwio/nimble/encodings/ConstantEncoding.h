// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <span>
#include "dwio/nimble/common/Bits.h"
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingSelection.h"

// Encodes data that is constant, i.e. there is a single unique value.

namespace facebook::nimble {

// Data layout is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// X bytes: the constant value via encoding primitive.
template <typename T>
class ConstantEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  ConstantEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data);

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer);

  std::string debugString(int offset) const final;

 private:
  physicalType value_;
};

//
// End of public API. Implementation follows.
//

template <typename T>
ConstantEncoding<T>::ConstantEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data)
    : TypedEncoding<T, physicalType>(memoryPool, data) {
  const char* pos = data.data() + Encoding::kPrefixSize;
  value_ = encoding::read<physicalType>(pos);
  NIMBLE_CHECK(pos == data.end(), "Unexpected constant encoding end");
}

template <typename T>
void ConstantEncoding<T>::reset() {}

template <typename T>
void ConstantEncoding<T>::skip(uint32_t /* rowCount */) {}

template <typename T>
void ConstantEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  physicalType* castBuffer = static_cast<physicalType*>(buffer);
  for (uint32_t i = 0; i < rowCount; ++i) {
    castBuffer[i] = value_;
  }
}

template <typename T>
std::string_view ConstantEncoding<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer) {
  if (values.empty()) {
    NIMBLE_INCOMPATIBLE_ENCODING("ConstantEncoding cannot be empty.");
  }

  if (selection.statistics().uniqueCounts().size() != 1) {
    NIMBLE_INCOMPATIBLE_ENCODING("ConstantEncoding requires constant data.");
  }

  const uint32_t rowCount = values.size();
  uint32_t encodingSize = Encoding::kPrefixSize;
  if constexpr (isStringType<physicalType>()) {
    encodingSize += 4 + values[0].size();
  } else {
    encodingSize += sizeof(physicalType);
  }
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::Constant, TypeTraits<T>::dataType, rowCount, pos);
  encoding::write<physicalType>(values[0], pos);
  NIMBLE_DASSERT(pos - reserved == encodingSize, "Encoding size mismatch.");
  return {reserved, encodingSize};
}

template <typename T>
std::string ConstantEncoding<T>::debugString(int offset) const {
  return fmt::format(
      "{}{}<{}> rowCount={} value={}",
      std::string(offset, ' '),
      toString(Encoding::encodingType()),
      toString(Encoding::dataType()),
      Encoding::rowCount(),
      AS_CONST(T, value_));
}

} // namespace facebook::nimble
