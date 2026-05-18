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

#include <folly/CPortability.h>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/DictionaryEncoding.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/common/EncodingType.h"
#include "dwio/nimble/encodings/selection/EncodingIdentifier.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"
#include "velox/buffer/BufferPool.h"
#include "velox/common/base/SimdUtil.h"
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

namespace detail {

template <typename V>
Vector<V> getPooledBuffer(
    velox::memory::MemoryPool& pool,
    velox::BufferPool* bufferPool) {
  if (bufferPool != nullptr) {
    if (auto buf = bufferPool->get()) {
      return Vector<V>(std::move(buf));
    }
  }
  return Vector<V>(&pool);
}

} // namespace detail

// Data layout is:
// EncodingPrefix::kFixedPrefixSize bytes: standard Encoding prefix
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
      velox::memory::MemoryPool& pool,
      std::string_view data,
      const Encoding::Options& options = {})
      : TypedEncoding<T, physicalType>(pool, data, options),
        isCommonBuffer_(
            detail::getPooledBuffer<bool>(pool, this->options_.bufferPool)),
        otherValuesBuffer_(
            detail::getPooledBuffer<physicalType>(
                pool,
                this->options_.bufferPool)),
        dictionaryIndicesBuffer_(&pool),
        dictionaryAlphabet_(&pool) {}

  ~MainlyConstantEncodingBase() override {
    auto* bufferPool = this->options_.bufferPool;
    if (bufferPool != nullptr) {
      bufferPool->release(isCommonBuffer_.releaseBuffer());
      bufferPool->release(otherValuesBuffer_.releaseBuffer());
    }
  }

  void reset() final {
    isCommon_->reset();
    otherValues_->reset();
  }

  void skip(uint32_t rowCount) override {
    // Use bit-packed booleans for efficient SIMD counting.
    const auto numWords = velox::bits::nwords(rowCount);
    isCommonBuffer_.resize(numWords * sizeof(uint64_t));
    auto* isCommon = reinterpret_cast<uint64_t*>(isCommonBuffer_.data());
    // isCommon_ encodes a bool stream so materializeBoolsAsBits is always
    // implemented.
    isCommon_->materializeBoolsAsBits(rowCount, isCommon, 0);

    // Mask tail bits to 1 (common) so countNonCommon is branchless.
    const auto tailBits = rowCount & 63;
    if (tailBits != 0) {
      isCommon[numWords - 1] |= ~((1ULL << tailBits) - 1);
    }

    const uint32_t nonCommonCount = countNonCommon(isCommon, numWords);
    if (nonCommonCount != 0) {
      otherValues_->skip(nonCommonCount);
    }
  }

  void materialize(uint32_t rowCount, void* buffer) override {
    // Use bit-packed booleans for efficient SIMD counting.
    // isCommon_ encodes a bool stream so materializeBoolsAsBits is always
    // implemented.
    const auto numWords = velox::bits::nwords(rowCount);
    isCommonBuffer_.resize(numWords * sizeof(uint64_t));
    auto* isCommon = reinterpret_cast<uint64_t*>(isCommonBuffer_.data());
    isCommon_->materializeBoolsAsBits(rowCount, isCommon, 0);

    // Mask tail bits to 1 (common) so countNonCommon and the scatter
    // loop (~isCommon) are branchless.
    const auto tailBits = rowCount & 63;
    if (tailBits != 0) {
      isCommon[numWords - 1] |= ~((1ULL << tailBits) - 1);
    }

    const uint32_t nonCommonCount = countNonCommon(isCommon, numWords);

    physicalType* output = static_cast<physicalType*>(buffer);
    velox::simd::simdFill(output, commonValue_, rowCount);

    if (nonCommonCount == 0) {
      return;
    }

    otherValuesBuffer_.reserve(nonCommonCount);
    otherValues_->materialize(nonCommonCount, otherValuesBuffer_.data());

    // Scatter non-common values. Tail bits already masked in the bitmap,
    // so ~isCommon[w] naturally has zeros in tail positions.
    uint32_t otherIdx = 0;
    for (uint32_t w = 0; w < numWords; ++w) {
      uint64_t unset = ~isCommon[w];
      const uint32_t base = w * 64;
      while (unset) {
        output[base + __builtin_ctzll(unset)] = otherValuesBuffer_[otherIdx++];
        unset &= unset - 1;
      }
    }
    NIMBLE_CHECK_EQ(otherIdx, nonCommonCount, "Encoding size mismatch.");
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

  /// Reads dictionary indices through a MainlyConstant wrapper.
  /// Materializes inner dict indices into dictionaryIndicesBuffer_, then
  /// scatters them and commonValueIndex directly into rawValues_.
  ///
  /// The combined alphabet is [innerAlphabet[0], ..., commonValue].
  /// Common rows → commonValueIndex, non-common rows → inner dict index.
  template <typename IndicesVisitor>
  void readIndicesWithVisitor(
      IndicesVisitor& visitor,
      ReadWithVisitorParams& params) {
    NIMBLE_CHECK(
        this->dictionaryEnabled(),
        "readIndicesWithVisitor requires dictionary-enabled inner encoding");
    NIMBLE_CHECK(
        !IndicesVisitor::kHasFilter && !IndicesVisitor::kHasHook,
        "readIndicesWithVisitor should only be invoked in dictionary fast path");
    const auto numReadRows =
        visitor.rowAt(visitor.numRows() - 1) - params.numScanned + 1;

    auto* rawNulls = visitor.reader().rawNullsInReadRange();
    const auto numNonNulls = rawNulls != nullptr
        ? velox::bits::countNonNulls(
              rawNulls, params.numScanned, params.numScanned + numReadRows)
        : numReadRows;

    // Dense fast path: materialize directly into rawValues_, scatter for nulls.
    if (IndicesVisitor::dense) {
      NIMBLE_CHECK_EQ(
          visitor.rowAt(visitor.numRows() - 1),
          visitor.rowAt(0) + visitor.numRows() - 1,
          "Dense visitor must have contiguous rows");
      detail::readDenseMaterializedIndices(
          *this,
          visitor,
          rawNulls,
          params.numScanned,
          numReadRows,
          numNonNulls);
      return;
    }

    // Sparse path: materialize all rows into buffer, gather only the
    // requested positions into rawValues_.
    // TODO: Use templated materializeIndices to avoid materializing
    // unrequested rows — see commented-out API mock-ups below and in
    // EncodingUtils.h.
    dictionaryIndicesBuffer_.resize(numNonNulls);
    detail::readSparseMaterializedIndices(
        *this,
        visitor,
        params.numScanned,
        params.prepareResultNulls,
        rawNulls,
        numReadRows,
        numNonNulls,
        reinterpret_cast<uint32_t*>(dictionaryIndicesBuffer_.data()));
  }

  template <bool kScatter, typename V>
  void bulkScan(
      V& visitor,
      vector_size_t currentRow,
      const vector_size_t* selectedRows,
      vector_size_t numSelected,
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
        velox::simd::simdFill(values, commonValue, numRows);
      }
    }
    const auto numIsCommon = selectedRows[numSelected - 1] + 1 - currentRow;
    isCommonBuffer_.resize(velox::bits::nwords(numIsCommon) * sizeof(uint64_t));
    auto* isCommon = reinterpret_cast<uint64_t*>(isCommonBuffer_.data());
    isCommon_->materializeBoolsAsBits(numIsCommon, isCommon, 0);
    auto numOtherValues =
        numIsCommon - velox::bits::countBits(isCommon, 0, numIsCommon);
    otherValuesBuffer_.resize(numOtherValues);
    otherValues_->materialize(numOtherValues, otherValuesBuffer_.data());
    numOtherValues = 0;
    auto* filterHits =
        V::kHasFilter ? visitor.outputRows(numSelected) : nullptr;
    auto* rows = kScatter ? scatterRows : selectedRows;
    vector_size_t numValues = 0;
    vector_size_t numHits = 0;
    vector_size_t selectedRowIndex = 0;
    velox::bits::forEachUnsetBit(
        isCommon, 0, numIsCommon, [&](vector_size_t i) {
          i += currentRow;
          auto commonBegin = selectedRowIndex;
          if constexpr (V::dense) {
            selectedRowIndex += i - selectedRows[selectedRowIndex];
          } else {
            while (selectedRows[selectedRowIndex] < i) {
              ++selectedRowIndex;
            }
          }
          const auto numCommon = selectedRowIndex - commonBegin;
          if (V::kHasFilter && commonPassed && numCommon > 0) {
            auto* begin = rows + commonBegin;
            std::copy(begin, begin + numCommon, filterHits + numHits);
            numHits += numCommon;
          }
          if (selectedRows[selectedRowIndex] > i) {
            if constexpr (!V::kFilterOnly) {
              vector_size_t numRows;
              if constexpr (kScatterValues) {
                numRows = scatterRows[selectedRowIndex] - visitor.rowIndex();
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
              filterHits[numHits++] = rows[selectedRowIndex];
            }
          } else {
            otherPassed = true;
          }
          if constexpr (!V::kFilterOnly) {
            auto* begin = values + numValues;
            vector_size_t numRows;
            if constexpr (kScatterValues) {
              begin[scatterRows[selectedRowIndex] - visitor.rowIndex()] =
                  detail::dataToValue(visitor, otherData);
              auto end = selectedRowIndex + 1;
              if (FOLLY_UNLIKELY(end == numSelected)) {
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
          ++selectedRowIndex;
        });
    auto numCommon = numSelected - selectedRowIndex;
    if (commonPassed && numCommon > 0) {
      if constexpr (V::kHasFilter) {
        auto* begin = rows + selectedRowIndex;
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
      NIMBLE_DCHECK_EQ(numValues, numSelected);
      visitor.hook().addValues(scatterRows, values, numSelected);
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

  // MC wraps an inner Dictionary encoding. The combined alphabet is
  // [innerAlphabet..., commonValue], so dictionarySize is inner + 1.
  bool dictionaryEnabled() const override {
    return otherValues_->dictionaryEnabled();
  }

  // MainlyConstantEncoding pads the inner dictionary by the common value.
  // Hence adds 1 to the inner dictionary size.
  uint32_t dictionarySize() const override {
    return otherValues_->dictionarySize() + 1;
  }

  const void* dictionaryEntry(uint32_t index) const override {
    if (index < otherValues_->dictionarySize()) {
      return otherValues_->dictionaryEntry(index);
    }
    return &commonValue_;
  }

  const void* dictionaryEntries() const override {
    if (dictionaryAlphabet_.empty()) {
      const auto numOtherValues = otherValues_->dictionarySize();
      dictionaryAlphabet_.resize(numOtherValues + 1);
      const auto* otherValues =
          static_cast<const physicalType*>(otherValues_->dictionaryEntries());
      std::copy(
          otherValues,
          otherValues + numOtherValues,
          dictionaryAlphabet_.data());
      dictionaryAlphabet_[numOtherValues] = commonValue_;
    }
    return dictionaryAlphabet_.data();
  }

  /// Materializes composed dictionary indices for rowCount dense non-null rows.
  /// Reads isCommon, delegates to inner encoding's materializeIndices for
  /// non-common rows, fills common positions with commonValueIndex.
  void materializeIndices(uint32_t rowCount, uint32_t* buffer) override {
    const auto numWords = velox::bits::nwords(rowCount);
    isCommonBuffer_.resize(numWords * sizeof(uint64_t));
    auto* isCommon = reinterpret_cast<uint64_t*>(isCommonBuffer_.data());
    // Set the last word to all-ones so tail bits beyond rowCount are treated
    // as common, making countNonCommon branchless without post-masking.
    // materializeBoolsAsBits only overwrites bits [0, rowCount).
    isCommon[numWords - 1] = ~0ULL;
    isCommon_->materializeBoolsAsBits(rowCount, isCommon, /*bitOffset=*/0);

    const uint32_t numNonCommon = countNonCommon(isCommon, numWords);
    const auto commonValueIndex =
        static_cast<uint32_t>(otherValues_->dictionarySize());

    if (numNonCommon == 0) {
      std::fill(buffer, buffer + rowCount, commonValueIndex);
      return;
    }

    // Materialize inner indices densely at the front of buffer, then
    // reverse-scatter interleaving with commonValueIndex.
    otherValues_->materializeIndices(numNonCommon, buffer);

    uint32_t index = numNonCommon;
    for (int32_t i = static_cast<int32_t>(rowCount) - 1; i >= 0; --i) {
      if (velox::bits::isBitSet(isCommon, i)) {
        buffer[i] = commonValueIndex;
      } else {
        buffer[i] = buffer[--index];
      }
    }
    NIMBLE_CHECK_EQ(index, 0);
  }

 protected:
  // Counts unset bits across all words. Caller must mask tail bits
  // before calling (set garbage tail bits to 1 in the last word).
  FOLLY_ALWAYS_INLINE static uint32_t countNonCommon(
      const uint64_t* isCommon,
      uint32_t numWords) {
    uint32_t count = 0;
    for (uint32_t w = 0; w < numWords; ++w) {
      count += 64 - __builtin_popcountll(isCommon[w]);
    }
    return count;
  }

  std::unique_ptr<Encoding> isCommon_;
  std::unique_ptr<Encoding> otherValues_;
  physicalType commonValue_;
  Vector<bool> isCommonBuffer_;
  Vector<physicalType> otherValuesBuffer_;
  Vector<int32_t> dictionaryIndicesBuffer_;
  mutable Vector<physicalType> dictionaryAlphabet_;
};

template <typename T>
class MainlyConstantEncoding final : public MainlyConstantEncodingBase<T> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  MainlyConstantEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory,
      const Encoding::Options& options = {});

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {});
};

//
// End of public API. Implementation follows.
//

template <typename T>
MainlyConstantEncoding<T>::MainlyConstantEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    std::function<void*(uint32_t)> stringBufferFactory,
    const Encoding::Options& options)
    : MainlyConstantEncodingBase<T>(memoryPool, data, options) {
  const EncodingFactory factory{options};
  const char* pos = data.data() + this->dataOffset();
  const uint32_t isCommonBytes = encoding::readUint32(pos);
  this->isCommon_ =
      factory.create(*this->pool_, {pos, isCommonBytes}, stringBufferFactory);
  pos += isCommonBytes;
  const uint32_t otherValuesBytes = encoding::readUint32(pos);
  this->otherValues_ = factory.create(
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
    Buffer& buffer,
    const Encoding::Options& options) {
  const bool useVarint = options.useVarintRowCount;
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
      EncodingIdentifiers::MainlyConstant::IsCommon,
      isCommon,
      tempBuffer,
      options);
  std::string_view serializedOtherValues =
      selection.template encodeNested<physicalType>(
          EncodingIdentifiers::MainlyConstant::OtherValues,
          otherValues,
          tempBuffer,
          options);

  uint32_t encodingSize = Encoding::serializePrefixSize(entryCount, useVarint) +
      8 + serializedIsCommon.size() + serializedOtherValues.size();
  if constexpr (isNumericType<physicalType>()) {
    encodingSize += sizeof(physicalType);
  } else {
    encodingSize += 4 + commonValue.size();
  }
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::MainlyConstant,
      TypeTraits<T>::dataType,
      entryCount,
      useVarint,
      pos);
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
      std::function<void*(uint32_t)> stringBufferFactory,
      const Encoding::Options& options = {});

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {});
};
} // namespace facebook::nimble
