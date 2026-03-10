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

#include <numeric>
#include <span>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingIdentifier.h"
#include "dwio/nimble/encodings/EncodingSelection.h"

// Stores integer data in a delta encoding. We use three child encodings:
// one for whether each row is a delta from the last or a restatement,
// one for the deltas, and one for the restatements. For now we
// only support positive deltas.
//
// As an example, consider the data
//
// 1 2 4 1 2 3 4 1 2 4 8 8
//
// The is-restatement  bool vector is
// T F F T F F F T F F F F
//
// The delta vector is
// 1 2 1 1 1 1 2 4 0
//
// The restatement vector is
// 1 1 1

namespace facebook::nimble {

// Data layout is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// 4 bytes: restatement relative offset (X)
// 4 bytes: is-restatement relative offset (Y)
// X bytes: delta encoding bytes
// Y bytes: restatement encoding bytes
// Z bytes: is-restatement encoding bytes
template <typename T>
class DeltaEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  DeltaEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory,
      const Encoding::Options& options = {});

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {});

 private:
  physicalType currentValue_;
  std::unique_ptr<Encoding> deltas_;
  std::unique_ptr<Encoding> restatements_;
  std::unique_ptr<Encoding> isRestatements_;
  // Temporary bufs.
  Vector<physicalType> deltasBuffer_;
  Vector<physicalType> restatementsBuffer_;
  Vector<bool> isRestatementsBuffer_;
};

//
// End of public API. Implementation follows.
//

template <typename T>
DeltaEncoding<T>::DeltaEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    std::function<void*(uint32_t)> stringBufferFactory,
    const Encoding::Options& options)
    : TypedEncoding<T, physicalType>(memoryPool, data, options),
      deltasBuffer_(&memoryPool),
      restatementsBuffer_(&memoryPool),
      isRestatementsBuffer_(&memoryPool) {
  auto pos = data.data() + this->dataOffset();
  const uint32_t restatementsOffset = encoding::readUint32(pos);
  const uint32_t isRestatementsOffset = encoding::readUint32(pos);
  deltas_ = EncodingFactory::decode(
      memoryPool, {pos, restatementsOffset}, stringBufferFactory, options);
  pos += restatementsOffset;
  restatements_ = EncodingFactory::decode(
      memoryPool, {pos, isRestatementsOffset}, stringBufferFactory, options);
  pos += isRestatementsOffset;
  isRestatements_ = EncodingFactory::decode(
      memoryPool,
      {pos, static_cast<size_t>(data.end() - pos)},
      std::move(stringBufferFactory),
      options);
}

template <typename T>
void DeltaEncoding<T>::reset() {
  deltas_->reset();
  restatements_->reset();
  isRestatements_->reset();
}

template <typename T>
void DeltaEncoding<T>::skip(uint32_t rowCount) {
  if (rowCount == 0) {
    return;
  }
  isRestatementsBuffer_.resize(rowCount);
  isRestatements_->materialize(rowCount, isRestatementsBuffer_.data());
  const uint32_t numRestatements = std::accumulate(
      isRestatementsBuffer_.begin(), isRestatementsBuffer_.end(), 0UL);
  // Find the last restatement, then accumulate deltas forward from it.
  int64_t lastRestatement = rowCount - 1;
  while (lastRestatement >= 0) {
    if (isRestatementsBuffer_[lastRestatement]) {
      break;
    }
    --lastRestatement;
  }
  if (lastRestatement >= 0) {
    restatements_->skip(numRestatements - 1);
    restatements_->materialize(1, &currentValue_);
    const uint32_t deltasToSkip = lastRestatement -
        std::accumulate(isRestatementsBuffer_.begin(),
                        isRestatementsBuffer_.begin() + lastRestatement,
                        0UL);
    deltas_->skip(deltasToSkip);
  }
  const uint32_t deltasToAccumulate = rowCount - 1 - lastRestatement;
  deltasBuffer_.resize(deltasToAccumulate);
  deltas_->materialize(deltasToAccumulate, deltasBuffer_.data());
  currentValue_ += std::accumulate(
      deltasBuffer_.begin(), deltasBuffer_.end(), physicalType());
}

template <typename T>
void DeltaEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  isRestatementsBuffer_.resize(rowCount);
  isRestatements_->materialize(rowCount, isRestatementsBuffer_.data());
  const uint32_t numRestatements = std::accumulate(
      isRestatementsBuffer_.begin(), isRestatementsBuffer_.end(), 0UL);
  restatementsBuffer_.reserve(numRestatements);
  restatements_->materialize(numRestatements, restatementsBuffer_.data());
  deltasBuffer_.reserve(rowCount - numRestatements);
  deltas_->materialize(rowCount - numRestatements, deltasBuffer_.data());
  physicalType* castValue = static_cast<physicalType*>(buffer);
  physicalType* nextRestatement = restatementsBuffer_.begin();
  physicalType* nextDelta = deltasBuffer_.begin();
  for (uint32_t i = 0; i < rowCount; ++i) {
    if (isRestatementsBuffer_[i]) {
      currentValue_ = *nextRestatement++;
    } else {
      currentValue_ += *nextDelta++;
    }
    *castValue++ = currentValue_;
  }
}

template <typename T>
template <typename V>
void DeltaEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  detail::readWithVisitorSlow(
      visitor,
      params,
      [&](auto toSkip) { skip(toSkip); },
      [&] {
        physicalType value;
        materialize(1, &value);
        return value;
      });
}

namespace internal {

template <typename physicalType>
void computeDeltas(
    std::span<const physicalType> values,
    Vector<physicalType>* deltas,
    Vector<physicalType>* restatements,
    Vector<bool>* isRestatements) {
  const uint32_t n = static_cast<uint32_t>(values.size());

  // Pass 1: Count how many elements use deltas vs restatements.
  // Must mirror the logic in Pass 2: a transition that crosses zero
  // (negative → positive) is treated as a restatement even when increasing,
  // to avoid signed overflow in the delta.
  uint32_t numDeltas = 0;
  for (uint32_t i = 1; i < n; ++i) {
    const bool isDelta = values[i] >= values[i - 1];
    const bool crossesZero = isSignedIntegralType<physicalType>() &&
        values[i] > 0 && values[i - 1] < 0;
    numDeltas += static_cast<uint32_t>(isDelta && !crossesZero);
  }
  const uint32_t numRestatements = (n - 1) - numDeltas;

  // Pre-allocate all outputs with exact sizes — eliminates per-element
  // capacity checks from push_back and the intermediate useDelta vector.
  isRestatements->resize(n);
  deltas->resize(numDeltas);
  restatements->resize(numRestatements + 1);

  isRestatements->data()[0] = true;
  restatements->data()[0] = values[0];

  // Pass 2: Scatter into pre-allocated outputs via raw pointers.
  bool* isRestPtr = isRestatements->data() + 1;
  physicalType* deltaPtr = deltas->data();
  physicalType* restPtr = restatements->data() + 1;

  for (uint32_t i = 1; i < n; ++i) {
    const bool isDelta = values[i] >= values[i - 1];
    const bool crossesZero = isSignedIntegralType<physicalType>() &&
        values[i] > 0 && values[i - 1] < 0;

    *isRestPtr++ = !isDelta || crossesZero;
    if (isDelta && !crossesZero) {
      *deltaPtr++ = values[i] - values[i - 1];
    } else {
      *restPtr++ = values[i];
    }
  }
}

} // namespace internal

template <typename T>
std::string_view DeltaEncoding<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer,
    const Encoding::Options& options) {
  const bool useVarint = options.useVarintRowCount;
  if (values.empty()) {
    NIMBLE_INCOMPATIBLE_ENCODING("DeltaEncoding can't be used with 0 rows.");
  }

  const uint32_t rowCount = values.size();
  Vector<physicalType> deltas(&buffer.getMemoryPool());
  Vector<physicalType> restatements(&buffer.getMemoryPool());
  Vector<bool> isRestatements(&buffer.getMemoryPool());
  internal::computeDeltas(values, &deltas, &restatements, &isRestatements);

  Buffer tempBuffer{buffer.getMemoryPool()};
  std::string_view serializedDeltas =
      selection.template encodeNested<physicalType>(
          EncodingIdentifiers::Delta::Deltas, deltas, tempBuffer, options);
  std::string_view serializedRestatements =
      selection.template encodeNested<physicalType>(
          EncodingIdentifiers::Delta::Restatements,
          restatements,
          tempBuffer,
          options);
  std::string_view serializedIsRestatements =
      selection.template encodeNested<bool>(
          EncodingIdentifiers::Delta::IsRestatements,
          isRestatements,
          tempBuffer,
          options);

  const uint32_t encodingSize =
      Encoding::serializePrefixSize(rowCount, useVarint) + 8 +
      serializedDeltas.size() + serializedRestatements.size() +
      serializedIsRestatements.size();
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::Delta, TypeTraits<T>::dataType, rowCount, useVarint, pos);
  encoding::writeUint32(serializedDeltas.size(), pos);
  encoding::writeUint32(serializedRestatements.size(), pos);
  encoding::writeBytes(serializedDeltas, pos);
  encoding::writeBytes(serializedRestatements, pos);
  encoding::writeBytes(serializedIsRestatements, pos);
  NIMBLE_DCHECK_EQ(pos - reserved, encodingSize, "Encoding size mismatch.");
  return {reserved, encodingSize};
}

} // namespace facebook::nimble
