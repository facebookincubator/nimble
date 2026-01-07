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
class MainlyConstantEncodingBase
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  MainlyConstantEncodingBase(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data)
      : TypedEncoding<T, physicalType>(memoryPool, data),
        isCommonBuffer_(&memoryPool),
        otherValuesBuffer_(&memoryPool) {}

  void reset() final {
    isCommon_->reset();
    otherValues_->reset();
  }

  void skip(uint32_t rowCount) final {
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

  void materialize(uint32_t rowCount, void* buffer) final {
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
    for (uint32_t i = 0; i < rowCount; ++i) {
      if (isCommonBuffer_[i]) {
        *output++ = commonValue_;
      } else {
        *output++ = *nextOtherValue++;
      }
    }
    NIMBLE_DCHECK_EQ(
        nextOtherValue - otherValuesBuffer_.begin(),
        nonCommonCount,
        "Encoding size mismatch.");
  }

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params) {
    auto* nulls = visitor.reader().rawNullsInReadRange();
    if (velox::dwio::common::useFastPath(visitor, nulls)) {
      detail::readWithVisitorFast(*this, visitor, params, nulls);
      return;
    }
    detail::readWithVisitorSlow(
        visitor,
        params,
        [&](auto toSkip) { skip(toSkip); },
        [&] {
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

  template <bool kScatter, typename V>
  void bulkScan(
      V& visitor,
      vector_size_t currentNonNullRow,
      const vector_size_t* nonNullRows,
      vector_size_t numNonNulls,
      const vector_size_t* scatterRows) {
    using DataType = typename V::DataType;
    using ValueType = detail::ValueType<DataType>;
    constexpr bool kScatterValues = kScatter && !V::kHasFilter && !V::kHasHook;
    ValueType* values;
    const auto commonData =
        detail::castFromPhysicalType<DataType>(commonValue_);
    const bool commonPassed =
        velox::common::applyFilter(visitor.filter(), commonData);
    if constexpr (!V::kFilterOnly) {
      auto numRows = visitor.numRows() - visitor.rowIndex();
      values = detail::mutableValues<ValueType>(visitor, numRows);
      if (commonPassed) {
        auto commonValue = detail::dataToValue(visitor, commonData);
        std::fill(values, values + numRows, commonValue);
      }
    }
    const auto numIsCommon =
        nonNullRows[numNonNulls - 1] + 1 - currentNonNullRow;
    isCommonBuffer_.resize(velox::bits::nwords(numIsCommon) * sizeof(uint64_t));
    auto* isCommon = reinterpret_cast<uint64_t*>(isCommonBuffer_.data());
    isCommon_->materializeBoolsAsBits(numIsCommon, isCommon, 0);
    auto numOtherValues =
        numIsCommon - velox::bits::countBits(isCommon, 0, numIsCommon);
    otherValuesBuffer_.resize(numOtherValues);
    otherValues_->materialize(numOtherValues, otherValuesBuffer_.data());
    numOtherValues = 0;
    auto* filterHits =
        V::kHasFilter ? visitor.outputRows(numNonNulls) : nullptr;
    auto* rows = kScatter ? scatterRows : nonNullRows;
    vector_size_t numValues = 0;
    vector_size_t numHits = 0;
    vector_size_t nonNullRowIndex = 0;
    velox::bits::forEachUnsetBit(
        isCommon, 0, numIsCommon, [&](vector_size_t i) {
          i += currentNonNullRow;
          auto commonBegin = nonNullRowIndex;
          if constexpr (V::dense) {
            nonNullRowIndex += i - nonNullRows[nonNullRowIndex];
          } else {
            while (nonNullRows[nonNullRowIndex] < i) {
              ++nonNullRowIndex;
            }
          }
          const auto numCommon = nonNullRowIndex - commonBegin;
          if (V::kHasFilter && commonPassed && numCommon > 0) {
            auto* begin = rows + commonBegin;
            std::copy(begin, begin + numCommon, filterHits + numHits);
            numHits += numCommon;
          }
          if (nonNullRows[nonNullRowIndex] > i) {
            if constexpr (!V::kFilterOnly) {
              vector_size_t numRows;
              if constexpr (kScatterValues) {
                numRows = scatterRows[nonNullRowIndex] - visitor.rowIndex();
                visitor.addRowIndex(numRows);
              } else {
                numRows = commonPassed * numCommon;
              }
              numValues += numRows;
            }
            ++numOtherValues;
            return;
          }
          auto otherData = detail::castFromPhysicalType<DataType>(
              otherValuesBuffer_[numOtherValues++]);
          bool otherPassed;
          if constexpr (V::kHasFilter) {
            otherPassed =
                velox::common::applyFilter(visitor.filter(), otherData);
            if (otherPassed) {
              filterHits[numHits++] = rows[nonNullRowIndex];
            }
          } else {
            otherPassed = true;
          }
          if constexpr (!V::kFilterOnly) {
            auto* begin = values + numValues;
            vector_size_t numRows;
            if constexpr (kScatterValues) {
              begin[scatterRows[nonNullRowIndex] - visitor.rowIndex()] =
                  detail::dataToValue(visitor, otherData);
              auto end = nonNullRowIndex + 1;
              if (FOLLY_UNLIKELY(end == numNonNulls)) {
                numRows = visitor.numRows() - visitor.rowIndex();
              } else {
                numRows = scatterRows[end] - visitor.rowIndex();
              }
              visitor.addRowIndex(numRows);
            } else {
              numRows = commonPassed * numCommon;
              if (otherPassed) {
                begin[numRows++] = detail::dataToValue(visitor, otherData);
              }
            }
            numValues += numRows;
          }
          ++nonNullRowIndex;
        });
    auto numCommon = numNonNulls - nonNullRowIndex;
    if (commonPassed && numCommon > 0) {
      if constexpr (V::kHasFilter) {
        auto* begin = rows + nonNullRowIndex;
        std::copy(begin, begin + numCommon, filterHits + numHits);
        numHits += numCommon;
      }
      if constexpr (!V::kFilterOnly) {
        if constexpr (kScatterValues) {
          numValues += visitor.numRows() - visitor.rowIndex();
        } else {
          numValues += numCommon;
        }
      }
    }
    visitor.setRowIndex(visitor.numRows());
    if constexpr (V::kHasHook) {
      NIMBLE_DCHECK_EQ(numValues, numNonNulls);
      visitor.hook().addValues(scatterRows, values, numNonNulls);
    } else {
      visitor.addNumValues(V::kFilterOnly ? numHits : numValues);
    }
  }

  std::string debugString(int offset) const final {
    std::string log = fmt::format(
        "{}{}<{}> rowCount={} commonValue={}",
        std::string(offset, ' '),
        toString(this->encodingType()),
        toString(this->dataType()),
        this->rowCount(),
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

 protected:
  std::unique_ptr<Encoding> isCommon_;
  std::unique_ptr<Encoding> otherValues_;
  physicalType commonValue_;
  Vector<bool> isCommonBuffer_;
  Vector<physicalType> otherValuesBuffer_;
};

template <typename T>
class MainlyConstantEncoding final : public MainlyConstantEncodingBase<T> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  MainlyConstantEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory);

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer);
};

//
// End of public API. Implementation follows.
//

template <typename T>
MainlyConstantEncoding<T>::MainlyConstantEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    std::function<void*(uint32_t)> stringBufferFactory)
    : MainlyConstantEncodingBase<T>(memoryPool, data) {
  const char* pos = data.data() + Encoding::kPrefixSize;
  const uint32_t isCommonBytes = encoding::readUint32(pos);
  this->isCommon_ = EncodingFactory::decode(
      *this->pool_, {pos, isCommonBytes}, stringBufferFactory);
  pos += isCommonBytes;
  const uint32_t otherValuesBytes = encoding::readUint32(pos);
  this->otherValues_ = EncodingFactory::decode(
      *this->pool_, {pos, otherValuesBytes}, stringBufferFactory);
  pos += otherValuesBytes;
  this->commonValue_ = encoding::read<physicalType>(pos);
  NIMBLE_CHECK(pos == data.end(), "Unexpected mainly constant encoding end");
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
      selection.statistics().uniqueCounts().value().cbegin(),
      selection.statistics().uniqueCounts().value().cend(),
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
  NIMBLE_DCHECK_EQ(pos - reserved, encodingSize, "Encoding size mismatch.");
  return {reserved, encodingSize};
}

template <>
class MainlyConstantEncoding<std::string_view> final
    : public MainlyConstantEncodingBase<std::string_view> {
 public:
  using cppDataType = std::string_view;
  using physicalType = std::string_view;

  MainlyConstantEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory);

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer);
};
} // namespace facebook::nimble
