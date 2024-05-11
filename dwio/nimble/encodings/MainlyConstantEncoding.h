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

#include <span>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingIdentifier.h"
#include "dwio/nimble/encodings/EncodingSelection.h"
#include "velox/common/memory/Memory.h"

// Encodes data that is 'mainly' a single value by using a bool child vectors
// to mark the rows that are that value, and another child encoding to encode
// the other values. We don't actually require that the single value be any
// given fraction of the data, but generally the encoding is effective when that
// constant fraction is large (say 50%+). All data types except bool are
// supported.
//
// E.g. if the data is
// 1 1 2 1 1 1 3 1 1 9
//
// The single value would be 1
//
// The bool vector would be
// T T F T T T F T T F
//
// The other-values child encoding would be
// 2 3 9
//
// This construction is quite similar to that of NullableEncoding, but instead
// of marking nulls specially we mark the constant value.

namespace facebook::nimble {

// Data layout is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// 4 bytes: num isCommon encoding bytes (X)
// X bytes: isCommon encoding bytes
// 4 bytes: num otherValues encoding bytes (Y)
// Y bytes: otherValues encoding bytes
// Z bytes: the constant value via encoding primitive.
template <typename T>
class MainlyConstantEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  MainlyConstantEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data);

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer);

  std::string debugString(int offset) const final;

 private:
  std::unique_ptr<Encoding> isCommon_;
  std::unique_ptr<Encoding> otherValues_;
  physicalType commonValue_;
  // Temporary bufs.
  Vector<bool> isCommonBuffer_;
  Vector<physicalType> otherValuesBuffer_;
};

//
// End of public API. Implementation follows.
//

template <typename T>
MainlyConstantEncoding<T>::MainlyConstantEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data)
    : TypedEncoding<T, physicalType>(memoryPool, data),
      isCommonBuffer_(&memoryPool),
      otherValuesBuffer_(&memoryPool) {
  const char* pos = data.data() + Encoding::kPrefixSize;
  const uint32_t isCommonBytes = encoding::readUint32(pos);
  isCommon_ = EncodingFactory::decode(this->memoryPool_, {pos, isCommonBytes});
  pos += isCommonBytes;
  const uint32_t otherValuesBytes = encoding::readUint32(pos);
  otherValues_ =
      EncodingFactory::decode(this->memoryPool_, {pos, otherValuesBytes});
  pos += otherValuesBytes;
  commonValue_ = encoding::read<physicalType>(pos);
  NIMBLE_CHECK(pos == data.end(), "Unexpected mainly constant encoding end");
}

template <typename T>
void MainlyConstantEncoding<T>::reset() {
  isCommon_->reset();
  otherValues_->reset();
}

template <typename T>
void MainlyConstantEncoding<T>::skip(uint32_t rowCount) {
  // Hrm this isn't ideal. We should return to this later -- a new
  // encoding func? Encoding::Accumulate to add up next N rows?
  isCommonBuffer_.resize(rowCount);
  isCommon_->materialize(rowCount, isCommonBuffer_.data());
  const uint32_t commonCount =
      std::accumulate(isCommonBuffer_.begin(), isCommonBuffer_.end(), 0U);
  const uint32_t nonCommonCount = rowCount - commonCount;
  if (nonCommonCount == 0) {
    return;
  }

  otherValues_->skip(nonCommonCount);
}

template <typename T>
void MainlyConstantEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  // This too isn't ideal. We will want an Encoding::Indices method or
  // something our SparseBool can use, giving back just the set indices
  // rather than a materialization.
  isCommonBuffer_.resize(rowCount);
  isCommon_->materialize(rowCount, isCommonBuffer_.data());
  const uint32_t commonCount =
      std::accumulate(isCommonBuffer_.begin(), isCommonBuffer_.end(), 0U);
  const uint32_t nonCommonCount = rowCount - commonCount;

  if (nonCommonCount == 0) {
    physicalType* output = static_cast<physicalType*>(buffer);
    std::fill(output, output + rowCount, commonValue_);
    return;
  }

  otherValuesBuffer_.reserve(nonCommonCount);
  otherValues_->materialize(nonCommonCount, otherValuesBuffer_.data());
  physicalType* output = static_cast<physicalType*>(buffer);
  const physicalType* nextOtherValue = otherValuesBuffer_.begin();
  // This is a generic scatter -- should we have a common scatter func?
  for (uint32_t i = 0; i < rowCount; ++i) {
    if (isCommonBuffer_[i]) {
      *output++ = commonValue_;
    } else {
      *output++ = *nextOtherValue++;
    }
  }
  NIMBLE_DASSERT(
      nextOtherValue - otherValuesBuffer_.begin() == nonCommonCount,
      "Encoding size mismatch.");
}

template <typename T>
template <typename V>
void MainlyConstantEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  this->template readWithVisitorSlow<true>(visitor, params, [&] {
    bool isCommon;
    isCommon_->materialize(1, &isCommon);
    if (isCommon) {
      return commonValue_;
    }
    physicalType otherValue;
    otherValues_->materialize(1, &otherValue);
    return otherValue;
  });
}

namespace internal {} // namespace internal

template <typename T>
std::string_view MainlyConstantEncoding<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer) {
  if (values.empty()) {
    NIMBLE_INCOMPATIBLE_ENCODING("MainlyConstantEncoding cannot be empty.");
  }

  const auto commonElement = std::max_element(
      selection.statistics().uniqueCounts().cbegin(),
      selection.statistics().uniqueCounts().cend(),
      [](const auto& a, const auto& b) { return a.second < b.second; });

  const uint32_t entryCount = values.size();
  const uint32_t uncommonCount = entryCount - commonElement->second;

  Vector<bool> isCommon{&buffer.getMemoryPool(), values.size(), true};
  Vector<physicalType> otherValues(&buffer.getMemoryPool());
  otherValues.reserve(uncommonCount);

  physicalType commonValue = commonElement->first;
  for (auto i = 0; i < values.size(); ++i) {
    physicalType currentValue = values[i];
    if (currentValue != commonValue) {
      isCommon[i] = false;
      otherValues.push_back(std::move(currentValue));
    }
  }

  Buffer tempBuffer{buffer.getMemoryPool()};
  std::string_view serializedIsCommon = selection.template encodeNested<bool>(
      EncodingIdentifiers::MainlyConstant::IsCommon, isCommon, tempBuffer);
  std::string_view serializedOtherValues =
      selection.template encodeNested<physicalType>(
          EncodingIdentifiers::MainlyConstant::OtherValues,
          otherValues,
          tempBuffer);

  uint32_t encodingSize = Encoding::kPrefixSize + 8 +
      serializedIsCommon.size() + serializedOtherValues.size();
  if constexpr (isNumericType<physicalType>()) {
    encodingSize += sizeof(physicalType);
  } else {
    encodingSize += 4 + commonValue.size();
  }
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::MainlyConstant, TypeTraits<T>::dataType, entryCount, pos);
  // TODO: Reorder these so that metadata is at the beginning.
  encoding::writeString(serializedIsCommon, pos);
  encoding::writeString(serializedOtherValues, pos);
  encoding::write<physicalType>(commonValue, pos);
  NIMBLE_DASSERT(pos - reserved == encodingSize, "Encoding size mismatch.");
  return {reserved, encodingSize};
}

template <typename T>
std::string MainlyConstantEncoding<T>::debugString(int offset) const {
  std::string log = fmt::format(
      "{}{}<{}> rowCount={} commonValue={}",
      std::string(offset, ' '),
      toString(Encoding::encodingType()),
      toString(Encoding::dataType()),
      Encoding::rowCount(),
      commonValue_);
  log += fmt::format(
      "\n{}isCommon child:\n{}",
      std::string(offset + 2, ' '),
      isCommon_->debugString(offset + 4));
  log += fmt::format(
      "\n{}otherValues child:\n{}",
      std::string(offset + 2, ' '),
      otherValues_->debugString(offset + 4));
  return log;
}

} // namespace facebook::nimble
