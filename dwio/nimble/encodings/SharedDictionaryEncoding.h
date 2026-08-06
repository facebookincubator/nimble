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

#include <algorithm>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <fmt/core.h>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/encodings/DictionaryEncoding.h"
#include "dwio/nimble/encodings/SharedDictionaryTypes.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingPrefix.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"
#include "dwio/nimble/encodings/selection/EncodingSelectionPolicy.h"
#include "velox/buffer/Buffer.h"

namespace facebook::nimble {

namespace detail {

template <typename T>
class SharedDictionarySliceSelectionPolicy final
    : public EncodingSelectionPolicy<T> {
 public:
  using physicalType = typename TypeTraits<T>::physicalType;

  SharedDictionarySliceSelectionPolicy(
      EncodingType encodingType,
      std::optional<EncodingType> alphabetEncodingType,
      std::optional<EncodingType> indicesEncodingType)
      : encodingType_{encodingType},
        alphabetEncodingType_{alphabetEncodingType},
        indicesEncodingType_{indicesEncodingType} {}

  EncodingSelectionResult select(
      std::span<const physicalType> /* values */,
      const Statistics<physicalType>& /* statistics */,
      const Encoding::Options& /* options */) final {
    return {.encodingType = encodingType_};
  }

  EncodingSelectionResult selectNullable(
      std::span<const physicalType> /* values */,
      std::span<const bool> /* nulls */,
      const Statistics<physicalType>& /* statistics */,
      const Encoding::Options& /* options */) final {
    NIMBLE_UNREACHABLE(
        "Shared dictionary slice selection does not support nullable values.");
  }

 private:
  std::unique_ptr<EncodingSelectionPolicyBase> createImpl(
      EncodingType parentEncodingType,
      NestedEncodingIdentifier nestedEncodingIdentifier,
      DataType nestedDataType) final {
    if (parentEncodingType == EncodingType::Dictionary &&
        nestedEncodingIdentifier == EncodingIdentifiers::Dictionary::Alphabet &&
        alphabetEncodingType_.has_value()) {
      UNIQUE_PTR_FACTORY(
          nestedDataType,
          SharedDictionarySliceSelectionPolicy,
          *alphabetEncodingType_,
          std::nullopt,
          std::nullopt);
    }
    if (parentEncodingType == EncodingType::Dictionary &&
        nestedEncodingIdentifier == EncodingIdentifiers::Dictionary::Indices &&
        indicesEncodingType_.has_value()) {
      UNIQUE_PTR_FACTORY(
          nestedDataType,
          SharedDictionarySliceSelectionPolicy,
          *indicesEncodingType_,
          std::nullopt,
          std::nullopt);
    }

    auto nestedEncodingReadFactors =
        ManualEncodingSelectionPolicyFactory::defaultEncodingReadFactors();
    nestedEncodingReadFactors.erase(
        std::remove_if(
            nestedEncodingReadFactors.begin(),
            nestedEncodingReadFactors.end(),
            [parentEncodingType](const auto& entry) {
              return entry.first == parentEncodingType;
            }),
        nestedEncodingReadFactors.end());
    UNIQUE_PTR_FACTORY(
        nestedDataType,
        ManualEncodingSelectionPolicy,
        std::move(nestedEncodingReadFactors),
        std::nullopt,
        nestedEncodingIdentifier);
  }

  const EncodingType encodingType_;
  const std::optional<EncodingType> alphabetEncodingType_;
  const std::optional<EncodingType> indicesEncodingType_;
};

inline void materializeLocalDictionaryIndices(
    std::span<const uint32_t> sortedUniqueSharedIndices,
    std::span<const uint32_t> slicedSharedIndices,
    std::span<uint32_t> localIndices) {
  NIMBLE_DCHECK_GT(sortedUniqueSharedIndices.size(), 1);
  NIMBLE_DCHECK_EQ(localIndices.size(), slicedSharedIndices.size());

  for (size_t i{0}; i < slicedSharedIndices.size(); ++i) {
    const auto it = std::lower_bound(
        sortedUniqueSharedIndices.begin(),
        sortedUniqueSharedIndices.end(),
        slicedSharedIndices[i]);
    NIMBLE_DCHECK(
        it != sortedUniqueSharedIndices.end() && *it == slicedSharedIndices[i],
        "Shared dictionary slice index missing from local dictionary.");
    localIndices[i] = static_cast<uint32_t>(
        std::distance(sortedUniqueSharedIndices.begin(), it));
  }
}

} // namespace detail

/// The layout for a shared dictionary encoding is:
/// Encoding prefix, one-byte scope, varint dictionary ID, encoded indices.
template <typename T>
class SharedDictionaryEncoding
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using physicalType = typename TypeTraits<T>::physicalType;

  SharedDictionaryEncoding(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory,
      const Encoding::Options& options = {});

  ~SharedDictionaryEncoding() override {
    this->releaseBuffer(indicesBuffer_);
  }

  SharedDictionaryEncoding(const SharedDictionaryEncoding&) = delete;
  SharedDictionaryEncoding& operator=(const SharedDictionaryEncoding&) = delete;
  SharedDictionaryEncoding(SharedDictionaryEncoding&&) = delete;
  SharedDictionaryEncoding& operator=(SharedDictionaryEncoding&&) = delete;

  void reset() final {
    indicesEncoding_->reset();
  }

  void skip(uint32_t rowCount) final {
    indicesEncoding_->skip(rowCount);
  }

  void materialize(uint32_t rowCount, void* buffer) final;

  template <typename V>
  void readWithVisitor(V& visitor, ReadWithVisitorParams& params);

  void materializeIndices(uint32_t rowCount, uint32_t* buffer) override {
    indicesEncoding_->materialize(rowCount, buffer);
  }

  bool dictionaryEnabled() const override {
    // TODO: Enable dictionary preservation after SharedDictionary implements
    // readIndicesWithVisitor().
    return false;
  }

  uint32_t dictionarySize() const override {
    return alphabet_->entryCount();
  }

  SharedDictionaryScope scope() const {
    return scope_;
  }

  uint32_t dictionaryId() const {
    return dictionaryId_;
  }

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {});

  static std::string_view slice(
      std::string_view encoded,
      uint32_t offset,
      uint32_t length,
      Buffer& buffer,
      const Encoding::Options& options = {});

  std::string debugString(int offset) const final;

 private:
  static constexpr uint32_t kScopeSize = sizeof(uint8_t);

  static std::string_view encodeIndices(
      EncodingSelection<physicalType>& selection,
      std::span<const uint32_t> indices,
      Buffer& buffer,
      const Encoding::Options& options = {});

  static std::string_view encodeMaterializedDictionarySlice(
      const SharedDictionaryAlphabet& alphabet,
      std::string_view encodedIndices,
      uint32_t offset,
      uint32_t length,
      Buffer& buffer,
      const Encoding::Options& options = {});

  uint32_t* ensureIndexBuffer(uint32_t numElements) {
    const auto bytes = numElements * sizeof(uint32_t);
    if (indicesBuffer_ == nullptr || indicesBuffer_->capacity() < bytes) {
      if (indicesBuffer_ != nullptr) {
        this->releaseBuffer(indicesBuffer_);
      }
      indicesBuffer_ = this->getBuffer(bytes);
    }
    return indicesBuffer_->asMutable<uint32_t>();
  }

  SharedDictionaryScope scope_;
  uint32_t dictionaryId_;
  std::shared_ptr<const SharedDictionaryAlphabet> alphabet_;
  std::unique_ptr<Encoding> indicesEncoding_;
  velox::BufferPtr indicesBuffer_;
};

template <typename T>
SharedDictionaryEncoding<T>::SharedDictionaryEncoding(
    velox::memory::MemoryPool& pool,
    std::string_view data,
    std::function<void*(uint32_t)> stringBufferFactory,
    const Encoding::Options& options)
    : TypedEncoding<T, physicalType>{pool, data, options} {
  static_assert(isIntegralType<T>() && !std::is_same_v<T, bool>);
  NIMBLE_CHECK_NOT_NULL(options.sharedDictionaryResolver);

  const char* pos = data.data() + this->dataOffset();
  scope_ = readSharedDictionaryScope(data, pos);
  dictionaryId_ = readSharedDictionaryId(data, pos);
  const auto indicesOffset = static_cast<size_t>(pos - data.data());
  NIMBLE_CHECK_LT(
      indicesOffset,
      data.size(),
      "Shared dictionary encoding is missing its indices.");
  alphabet_ = options.sharedDictionaryResolver->resolve(
      scope_, dictionaryId_, TypeTraits<T>::dataType);
  NIMBLE_CHECK_NOT_NULL(alphabet_);
  NIMBLE_CHECK_EQ(
      alphabet_->dataType(),
      TypeTraits<T>::dataType,
      "Shared dictionary {} has unexpected type.",
      dictionaryId_);

  indicesEncoding_ = EncodingFactory().create(
      *this->pool_,
      {pos, data.size() - indicesOffset},
      std::move(stringBufferFactory),
      options);
  NIMBLE_CHECK_EQ(
      indicesEncoding_->dataType(),
      DataType::Uint32,
      "Shared dictionary indices have unexpected type.");
  NIMBLE_CHECK_EQ(
      indicesEncoding_->rowCount(),
      this->rowCount(),
      "Shared dictionary index count differs from row count.");
}

template <typename T>
void SharedDictionaryEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  auto* indices = ensureIndexBuffer(rowCount);
  indicesEncoding_->materialize(rowCount, indices);
  alphabet_->template materialize<T>(
      std::span<const uint32_t>{indices, rowCount},
      static_cast<physicalType*>(buffer));
}

template <typename T>
template <typename V>
void SharedDictionaryEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  if constexpr (sizeof(T) < sizeof(uint32_t)) {
    // Nested index decoders may use the reader values buffer before the hook
    // copies indices out, so it must fit uint32_t indices rather than T values.
    visitor.reader().template ensureValuesCapacity<uint32_t>(
        visitor.numRows(), /*preserveValues=*/true);
  }
  const auto startRowIndex = visitor.rowIndex();
  const auto numIndices = visitor.numRows() - startRowIndex;
  auto* indices = ensureIndexBuffer(numIndices);
  velox::common::AlwaysTrue indicesFilter;
  detail::DictionaryIndicesHook indicesHook(indices, startRowIndex);
  auto indicesVisitor = DecoderVisitor<
      int32_t,
      velox::common::AlwaysTrue,
      velox::dwio::common::ExtractToHook<detail::DictionaryIndicesHook>,
      V::dense>(
      indicesFilter,
      &visitor.reader(),
      velox::RowSet(visitor.rows(), visitor.numRows()),
      velox::dwio::common::ExtractToHook<detail::DictionaryIndicesHook>(
          &indicesHook));
  indicesVisitor.setRowIndex(startRowIndex);
  callReadWithVisitor(*indicesEncoding_, indicesVisitor, params);
  detail::readWithVisitorSlow(visitor, params, nullptr, [&] {
    return alphabet_->template physicalValueAt<T>(
        indices[visitor.rowIndex() - startRowIndex]);
  });
}

template <typename T>
std::string_view SharedDictionaryEncoding<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer,
    const Encoding::Options& options) {
  static_assert(isIntegralType<T>() && !std::is_same_v<T, bool>);
  const auto sharedDictionaryInput = selection.sharedDictionaryInput();
  NIMBLE_CHECK(
      sharedDictionaryInput.has_value(),
      "Shared dictionary encoding requires input from selection.");
  NIMBLE_CHECK_EQ(
      sharedDictionaryInput->indices.size(),
      values.size(),
      "Shared dictionary index count differs from value count.");
  NIMBLE_CHECK_NE(
      sharedDictionaryInput->dictionaryId,
      kInvalidSharedDictionaryId,
      "Shared dictionary encoding requires a valid dictionary id.");

  ScopedEncodingBuffer scopedBuffer{
      &buffer.getMemoryPool(), options.encodingBufferPool};
  const auto encodedIndices = encodeIndices(
      selection, sharedDictionaryInput->indices, scopedBuffer.get(), options);

  const auto rowCount =
      static_cast<uint32_t>(sharedDictionaryInput->indices.size());
  const auto dictionaryId = sharedDictionaryInput->dictionaryId;
  const uint64_t encodingSize =
      static_cast<uint64_t>(
          Encoding::serializePrefixSize(rowCount, options.useVarintRowCount)) +
      kScopeSize + varint::varintSize(dictionaryId) + encodedIndices.size();
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::SharedDictionary,
      TypeTraits<T>::dataType,
      rowCount,
      options.useVarintRowCount,
      pos);
  encoding::write<uint8_t>(
      static_cast<uint8_t>(sharedDictionaryInput->scope), pos);
  varint::writeVarint(dictionaryId, &pos);
  encoding::writeBytes(encodedIndices, pos);
  NIMBLE_DCHECK_EQ(
      static_cast<uint64_t>(pos - reserved),
      encodingSize,
      "Encoding size mismatch.");

  return {reserved, encodingSize};
}

template <typename T>
std::string_view SharedDictionaryEncoding<T>::encodeIndices(
    EncodingSelection<physicalType>& selection,
    std::span<const uint32_t> indices,
    Buffer& buffer,
    const Encoding::Options& options) {
  static_assert(isIntegralType<T>() && !std::is_same_v<T, bool>);
  NIMBLE_CHECK_LE(
      indices.size(),
      kMaxSharedDictionarySize,
      "Shared dictionary index count exceeds maximum.");
  return selection.template encodeNested<uint32_t>(
      EncodingIdentifiers::SharedDictionary::Indices, indices, buffer, options);
}

template <typename T>
std::string_view SharedDictionaryEncoding<T>::slice(
    std::string_view encoded,
    uint32_t offset,
    uint32_t length,
    Buffer& buffer,
    const Encoding::Options& options) {
  static_assert(isIntegralType<T>() && !std::is_same_v<T, bool>);
  const auto sourceRowCount =
      EncodingPrefix::readRowCount(encoded, options.useVarintRowCount);
  NIMBLE_CHECK_LE(offset, sourceRowCount);
  NIMBLE_CHECK_LE(length, sourceRowCount - offset);
  NIMBLE_CHECK_GT(length, 0, "Cannot slice zero rows.");

  NIMBLE_CHECK_NOT_NULL(options.sharedDictionaryResolver);
  const char* pos = encoded.data() +
      EncodingPrefix::prefixSize(encoded, options.useVarintRowCount);
  const auto scope = readSharedDictionaryScope(encoded, pos);
  const auto dictionaryId = readSharedDictionaryId(encoded, pos);
  const auto indicesOffset = static_cast<size_t>(pos - encoded.data());
  NIMBLE_CHECK_LT(
      indicesOffset,
      encoded.size(),
      "Shared dictionary encoding is missing its indices.");
  const std::string_view encodedIndices{pos, encoded.size() - indicesOffset};

  const auto alphabet = options.sharedDictionaryResolver->resolve(
      scope, dictionaryId, TypeTraits<T>::dataType);
  NIMBLE_CHECK_NOT_NULL(alphabet);
  NIMBLE_CHECK_EQ(
      alphabet->dataType(),
      TypeTraits<T>::dataType,
      "Shared dictionary {} has unexpected type.",
      dictionaryId);

  return encodeMaterializedDictionarySlice(
      *alphabet, encodedIndices, offset, length, buffer, options);
}

template <typename T>
std::string_view SharedDictionaryEncoding<T>::encodeMaterializedDictionarySlice(
    const SharedDictionaryAlphabet& alphabet,
    std::string_view encodedIndices,
    uint32_t offset,
    uint32_t length,
    Buffer& buffer,
    const Encoding::Options& options) {
  auto* pool = &buffer.getMemoryPool();
  ScopedEncodingBuffer scopedBuffer{pool, options.encodingBufferPool};
  ScopedVector<uint32_t> slicedIndices{length, pool, options.bufferPool};
  auto indicesEncoding = EncodingFactory{options}.create(
      *pool, encodedIndices, [&scopedBuffer](uint32_t size) -> void* {
        return scopedBuffer.get().reserve(size);
      });
  indicesEncoding->skip(offset);
  indicesEncoding->materialize(length, slicedIndices.data());

  ScopedVector<uint32_t> uniqueIndices{length, pool, options.bufferPool};
  std::copy(slicedIndices.begin(), slicedIndices.end(), uniqueIndices.begin());
  std::sort(uniqueIndices.begin(), uniqueIndices.end());
  uniqueIndices.resize(
      static_cast<uint64_t>(
          std::unique(uniqueIndices.begin(), uniqueIndices.end()) -
          uniqueIndices.begin()));

  ScopedVector<physicalType> values{
      uniqueIndices.size(), pool, options.bufferPool};
  alphabet.template materialize<T>(
      std::span<const uint32_t>{uniqueIndices.data(), uniqueIndices.size()},
      values.data());

  if (uniqueIndices.size() == 1) {
    const uint64_t encodingSize =
        EncodingPrefix::serializedSize(length, options.useVarintRowCount) +
        sizeof(physicalType);
    char* reserved = buffer.reserve(encodingSize);
    char* writePos = reserved;
    EncodingPrefix::serialize(
        EncodingType::Constant,
        TypeTraits<T>::dataType,
        length,
        options.useVarintRowCount,
        writePos);
    encoding::write<physicalType>(values[0], writePos);
    NIMBLE_DCHECK_EQ(
        static_cast<uint64_t>(writePos - reserved),
        encodingSize,
        "Encoding size mismatch.");
    return {reserved, encodingSize};
  }

  ScopedVector<uint32_t> localIndices{length, pool, options.bufferPool};
  detail::materializeLocalDictionaryIndices(
      uniqueIndices, slicedIndices, localIndices);

  // TODO: When the slice references most of the shared alphabet, consider
  // preserving the shared dictionary form by slicing the shared alphabet or
  // carrying the full shared alphabet instead of rebuilding a local dictionary.
  auto policy =
      std::make_unique<detail::SharedDictionarySliceSelectionPolicy<T>>(
          EncodingType::Dictionary,
          alphabet.encodingType(),
          EncodingPrefix::encodingType(encodedIndices));
  EncodingSelection<physicalType> selection{
      {.encodingType = EncodingType::Dictionary},
      Statistics<physicalType>::create(
          std::span<const physicalType>{values.data(), values.size()}),
      std::move(policy)};
  const auto serializedAlphabet = selection.template encodeNested<physicalType>(
      EncodingIdentifiers::Dictionary::Alphabet,
      std::span<const physicalType>{values.data(), values.size()},
      scopedBuffer.get(),
      options);
  const auto serializedIndices = selection.template encodeNested<uint32_t>(
      EncodingIdentifiers::Dictionary::Indices,
      std::span<const uint32_t>{localIndices.data(), localIndices.size()},
      scopedBuffer.get(),
      options);

  NIMBLE_CHECK_LE(
      serializedAlphabet.size(),
      std::numeric_limits<uint32_t>::max(),
      "Shared dictionary slice alphabet encoding exceeds maximum size.");
  const uint64_t encodingSize =
      EncodingPrefix::serializedSize(length, options.useVarintRowCount) +
      sizeof(uint32_t) + serializedAlphabet.size() + serializedIndices.size();
  char* reserved = buffer.reserve(encodingSize);
  char* writePos = reserved;
  EncodingPrefix::serialize(
      EncodingType::Dictionary,
      TypeTraits<T>::dataType,
      length,
      options.useVarintRowCount,
      writePos);
  encoding::writeUint32(
      static_cast<uint32_t>(serializedAlphabet.size()), writePos);
  encoding::writeBytes(serializedAlphabet, writePos);
  encoding::writeBytes(serializedIndices, writePos);
  NIMBLE_DCHECK_EQ(
      static_cast<uint64_t>(writePos - reserved),
      encodingSize,
      "Encoding size mismatch.");
  return {reserved, encodingSize};
}

template <typename T>
std::string SharedDictionaryEncoding<T>::debugString(int offset) const {
  return fmt::format(
      "{}\n{}scope={} dictionaryId={} entries={}\n{}indices child:\n{}",
      Encoding::debugString(offset),
      std::string(offset + 2, ' '),
      scope_,
      dictionaryId_,
      dictionarySize(),
      std::string(offset, ' '),
      indicesEncoding_->debugString(offset + 2));
}

} // namespace facebook::nimble
