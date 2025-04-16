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
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingIdentifier.h"
#include "dwio/nimble/encodings/EncodingSelection.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "folly/container/F14Map.h"
#include "velox/common/memory/Memory.h"

// A dictionary encoded stream is comprised of two pieces: a mapping from the
// n unique values in a stream to the integers [0, n) and the vector of indices
// that scatter those uniques back to the original ordering.

namespace facebook::nimble {

// The layout for a dictionary encoding is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// 4 bytes: alphabet size
// XX bytes: alphabet encoding bytes
// YY bytes: indices encoding bytes
template <typename T>
class DictionaryEncoding
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  static const int kAlphabetSizeOffset = Encoding::kPrefixSize;

  DictionaryEncoding(
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
  // Stores pre-loaded alphabet
  Vector<T> alphabet_;

  // Indices are uint32_t.
  std::unique_ptr<Encoding> indicesEncoding_;
  std::unique_ptr<Encoding> alphabetEncoding_;
  Vector<uint32_t> buffer_; // Temporary buffer.
};

//
// End of public API. Implementation follows.
//

template <typename T>
DictionaryEncoding<T>::DictionaryEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data)
    : TypedEncoding<T, physicalType>(memoryPool, data),
      alphabet_{&memoryPool},
      buffer_(&memoryPool) {
  auto pos = data.data() + kAlphabetSizeOffset;
  const uint32_t alphabetSize = encoding::readUint32(pos);

  alphabetEncoding_ =
      EncodingFactory::decode(this->memoryPool_, {pos, alphabetSize});
  const uint32_t alphabetCount = alphabetEncoding_->rowCount();
  alphabet_.resize(alphabetCount);
  alphabetEncoding_->materialize(alphabetCount, alphabet_.data());

  pos += alphabetSize;
  indicesEncoding_ = EncodingFactory::decode(
      this->memoryPool_, {pos, static_cast<size_t>(data.end() - pos)});
}

template <typename T>
void DictionaryEncoding<T>::reset() {
  indicesEncoding_->reset();
}

template <typename T>
void DictionaryEncoding<T>::skip(uint32_t rowCount) {
  indicesEncoding_->skip(rowCount);
}

template <typename T>
void DictionaryEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  buffer_.resize(rowCount);
  indicesEncoding_->materialize(rowCount, buffer_.data());

  T* output = static_cast<T*>(buffer);
  for (uint32_t index : buffer_) {
    *output = alphabet_[index];
    ++output;
  }
}

namespace detail {

class DictionaryIndicesHook : public velox::ValueHook {
 public:
  static constexpr bool kSkipNulls = true;

  DictionaryIndicesHook(uint32_t* indices, vector_size_t offset)
      : indices_(indices), offset_(offset) {}

  bool acceptsNulls() const final {
    return false;
  }

  void addValue(vector_size_t i, int64_t value) final {
    indices_[i - offset_] = value;
  }

  void addValues(
      const vector_size_t* rows,
      const int32_t* values,
      vector_size_t size) final {
    for (vector_size_t i = 0; i < size; ++i) {
      indices_[rows[i] - offset_] = values[i];
    }
  }

  void addNull(vector_size_t /*i*/) final {
    NIMBLE_UNREACHABLE(__PRETTY_FUNCTION__);
  }

 private:
  uint32_t* const indices_;
  const vector_size_t offset_;
};

} // namespace detail

template <typename T>
template <typename V>
void DictionaryEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  const auto startRowIndex = visitor.rowIndex();
  buffer_.resize(visitor.numRows() - startRowIndex);
  velox::common::AlwaysTrue indicesFilter;
  detail::DictionaryIndicesHook indicesHook(buffer_.data(), startRowIndex);
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
    auto index = buffer_[visitor.rowIndex() - startRowIndex];
    return alphabet_[index];
  });
}

template <typename T>
std::string_view DictionaryEncoding<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer) {
  const uint32_t valueCount = values.size();
  const uint32_t alphabetCount = selection.statistics().uniqueCounts().size();

  folly::F14FastMap<physicalType, uint32_t> alphabetMapping;
  alphabetMapping.reserve(alphabetCount);
  Vector<physicalType> alphabet{&buffer.getMemoryPool()};
  alphabet.reserve(alphabetCount);
  uint32_t index = 0;
  for (const auto& pair : selection.statistics().uniqueCounts()) {
    alphabet.push_back(pair.first);
    alphabetMapping.emplace(pair.first, index++);
  }

  Vector<uint32_t> indices{&buffer.getMemoryPool()};
  indices.reserve(valueCount);
  for (const auto& value : values) {
    auto it = alphabetMapping.find(value);
    NIMBLE_DASSERT(
        it != alphabetMapping.end(),
        "Statistics corruption. Missing alphabet entry.");
    indices.push_back(it->second);
  }

  Buffer tempBuffer{buffer.getMemoryPool()};
  std::string_view serializedAlphabet =
      selection.template encodeNested<physicalType>(
          EncodingIdentifiers::Dictionary::Alphabet, {alphabet}, tempBuffer);
  std::string_view serializedIndices =
      selection.template encodeNested<uint32_t>(
          EncodingIdentifiers::Dictionary::Indices, {indices}, tempBuffer);

  const uint32_t encodingSize = Encoding::kPrefixSize + 4 +
      serializedAlphabet.size() + serializedIndices.size();
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::Dictionary, TypeTraits<T>::dataType, valueCount, pos);
  encoding::writeUint32(serializedAlphabet.size(), pos);
  encoding::writeBytes(serializedAlphabet, pos);
  encoding::writeBytes(serializedIndices, pos);
  NIMBLE_DASSERT(pos - reserved == encodingSize, "Encoding size mismatch.");
  return {reserved, encodingSize};
}

template <typename T>
std::string DictionaryEncoding<T>::debugString(int offset) const {
  std::string log = Encoding::debugString(offset);
  log += fmt::format(
      "\n{}indices child:\n{}",
      std::string(offset, ' '),
      indicesEncoding_->debugString(offset + 2));
  log += fmt::format(
      "\n{}alphabet:\n{}entries={}",
      std::string(offset, ' '),
      std::string(offset + 2, ' '),
      alphabet_.size());
  return log;
}

} // namespace facebook::nimble
