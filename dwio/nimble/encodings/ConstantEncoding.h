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

#include <optional>
#include <span>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/common/EncodingType.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"

// Encodes data that is constant, i.e. there is a single unique value.

namespace facebook::nimble {

// Data layout is:
// EncodingPrefix::kFixedPrefixSize bytes: standard Encoding prefix
// X bytes: the constant value via encoding primitive.

template <typename T>
class ConstantEncodingBase
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  ConstantEncodingBase(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      const Encoding::Options& options = {})
      : TypedEncoding<T, physicalType>(pool, data, options) {}

  static std::optional<uint64_t> estimateSize(
      const Statistics<physicalType>& statistics) {
    if (statistics.uniqueCounts().value().size() != 1) {
      return std::nullopt;
    }
    const uint64_t outerEncodingSize = EncodingPrefix::kFixedPrefixSize;
    if constexpr (isStringType<physicalType>()) {
      const uint64_t valueSize = statistics.max().size() + sizeof(uint32_t);
      return outerEncodingSize + valueSize;
    } else {
      const uint64_t valueSize = sizeof(physicalType);
      return outerEncodingSize + valueSize;
    }
  }

  void reset() final {}

  void skip(uint32_t /* rowCount */) final {}

  /// O(1) point read backing get<T>(): returns the single constant value,
  /// ignoring `row`. Stateless and safe for concurrent calls.
  void getImpl(uint32_t /* row */, void* buffer) final {
    *static_cast<physicalType*>(buffer) = value_;
  }

  void materialize(uint32_t rowCount, void* buffer) final {
    physicalType* castBuffer = static_cast<physicalType*>(buffer);
    for (uint32_t i = 0; i < rowCount; ++i) {
      castBuffer[i] = value_;
    }
  }

  void materializeBoolsAsBits(
      uint32_t /*rowCount*/,
      uint64_t* /*buffer*/,
      int /*begin*/) override {
    NIMBLE_UNREACHABLE("");
  }

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params) {
    detail::readWithVisitorSlow(
        visitor, params, nullptr, [&] { return value_; });
  }

  bool dictionaryEnabled() const override {
    return true;
  }

  uint32_t dictionarySize() const override {
    return 1;
  }

  const void* dictionaryEntry(uint32_t index) const override {
    NIMBLE_DCHECK_EQ(index, 0);
    return &value_;
  }

  const void* dictionaryEntries() const override {
    return &value_;
  }

  void materializeIndices(uint32_t rowCount, uint32_t* buffer) override {
    std::fill(buffer, buffer + rowCount, 0);
  }

  /// Reads dictionary indices for a constant encoding. Every row maps to
  /// index 0 (the single dictionary entry).
  template <typename V>
  void readIndicesWithVisitor(V& visitor, ReadWithVisitorParams& params) {
    NIMBLE_CHECK(
        !V::kHasHook, "readIndicesWithVisitor does not support value hooks");
    const auto numReadRows =
        visitor.rowAt(visitor.numRows() - 1) - params.numScanned + 1;
    auto* rawNulls = visitor.reader().rawNullsInReadRange();
    const auto numNonNulls = rawNulls != nullptr
        ? velox::bits::countNonNulls(
              rawNulls, params.numScanned, params.numScanned + numReadRows)
        : numReadRows;

    if (V::dense) {
      NIMBLE_CHECK_EQ(
          visitor.rowAt(visitor.numRows() - 1),
          visitor.rowAt(0) + visitor.numRows() - 1,
          "Dense visitor must have contiguous rows");
      detail::readDenseMaterializedIndices(
          *this, visitor, params, rawNulls, numReadRows, numNonNulls);
      return;
    }

    // Sparse path is unlikely for constant encoding but supported for
    // correctness.
    auto indicesBuffer =
        velox::AlignedBuffer::allocate<uint32_t>(numNonNulls, this->pool_);
    auto* rawIndices = indicesBuffer->template asMutable<uint32_t>();
    detail::readSparseMaterializedIndices(
        *this,
        visitor,
        params.numScanned,
        params.prepareResultNulls,
        rawNulls,
        numReadRows,
        numNonNulls,
        rawIndices);
  }

  std::string debugString(int offset) const final {
    return fmt::format(
        "{}{}<{}> rowCount={} value={}",
        std::string(offset, ' '),
        toString(this->encodingType()),
        toString(this->dataType()),
        this->rowCount(),
        NIMBLE_AS_CONST(T, value_));
  }

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {}) {
    const bool useVarint = options.useVarintRowCount;
    if (values.empty()) {
      NIMBLE_INCOMPATIBLE_ENCODING("ConstantEncoding cannot be empty.");
    }

    if (selection.statistics().uniqueCounts().value().size() != 1) {
      NIMBLE_INCOMPATIBLE_ENCODING("ConstantEncoding requires constant data.");
    }

    const uint32_t rowCount = values.size();
    uint32_t encodingSize = Encoding::serializePrefixSize(rowCount, useVarint);
    if constexpr (isStringType<physicalType>()) {
      encodingSize += 4 + values[0].size();
    } else {
      encodingSize += sizeof(physicalType);
    }
    char* reserved = buffer.reserve(encodingSize);
    char* pos = reserved;
    Encoding::serializePrefix(
        EncodingType::Constant,
        TypeTraits<T>::dataType,
        rowCount,
        useVarint,
        pos);
    encoding::write<physicalType>(values[0], pos);
    NIMBLE_DCHECK_EQ(pos - reserved, encodingSize, "Encoding size mismatch.");
    return {reserved, encodingSize};
  }

 protected:
  physicalType value_;
};

template <typename T>
class ConstantEncoding : public ConstantEncodingBase<T> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  ConstantEncoding(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory,
      const Encoding::Options& options = {});
};

//
// End of public API. Implementation follows.
//

template <typename T>
ConstantEncoding<T>::ConstantEncoding(
    velox::memory::MemoryPool& pool,
    std::string_view data,
    std::function<void*(uint32_t)> /* stringBufferFactory */,
    const Encoding::Options& options)
    : ConstantEncodingBase<T>(pool, data, options) {
  const char* pos = data.data() + this->dataOffset();
  this->value_ = encoding::read<physicalType>(pos);
  NIMBLE_CHECK_EQ(pos, data.end(), "Unexpected constant encoding end");
}

// Specialization for bool to override materializeBoolsAsBits
template <>
class ConstantEncoding<bool> final : public ConstantEncodingBase<bool> {
 public:
  using cppDataType = bool;
  using physicalType = bool;

  ConstantEncoding(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      std::function<void*(uint32_t)> /* stringBufferFactory */,
      const Encoding::Options& options = {})
      : ConstantEncodingBase<bool>(pool, data, options) {
    const char* pos = data.data() + this->dataOffset();
    this->value_ = encoding::read<physicalType>(pos);
    NIMBLE_CHECK_EQ(pos, data.end(), "Unexpected constant encoding end");
  }

  void materializeBoolsAsBits(uint32_t rowCount, uint64_t* buffer, int begin)
      final {
    velox::bits::fillBits(buffer, begin, begin + rowCount, this->value_);
  }
};

template <>
class ConstantEncoding<std::string_view> final
    : public ConstantEncodingBase<std::string_view> {
 public:
  using cppDataType = std::string_view;
  using physicalType = std::string_view;

  ConstantEncoding(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory,
      const Encoding::Options& options = {});
};
} // namespace facebook::nimble
