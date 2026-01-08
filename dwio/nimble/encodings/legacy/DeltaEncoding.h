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
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/Encoding.h" // Original base classes
#include "dwio/nimble/encodings/legacy/Encoding.h" // Release namespace type aliases
#include "dwio/nimble/encodings/legacy/EncodingFactory.h"

// Stores integer data in a delta encoding. We use three child encodings:
// one for whether each row is a delta from the last or a restatement,
// one for the deltas, and one one for the restatements. For now we
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

namespace facebook::nimble::legacy {

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

  DeltaEncoding(velox::memory::MemoryPool& memoryPool, std::string_view data);

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  std::string debugString(int offset) const final;

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
    std::string_view data)
    : TypedEncoding<T, physicalType>(memoryPool, data),
      deltasBuffer_(&memoryPool),
      restatementsBuffer_(&memoryPool),
      isRestatementsBuffer_(&memoryPool) {
  auto pos = data.data() + Encoding::kPrefixSize;
  const uint32_t restatementsOffset = encoding::readUint32(pos);
  const uint32_t isRestatementsOffset = encoding::readUint32(pos);
  deltas_ = EncodingFactory::decode(memoryPool, {pos, restatementsOffset});
  pos += restatementsOffset;
  restatements_ =
      EncodingFactory::decode(memoryPool, {pos, isRestatementsOffset});
  pos += isRestatementsOffset;
  isRestatements_ = EncodingFactory::decode(
      memoryPool, {pos, static_cast<size_t>(data.end() - pos)});
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

// namespace internal {

// template <typename physicalType>
// void computeDeltas(
//     std::span<const physicalType> values,
//     Vector<physicalType>* deltas,
//     Vector<physicalType>* restatements,
//     Vector<bool>* isRestatements) {
//   isRestatements->push_back(true);
//   restatements->push_back(values[0]);
//   // For signed integer types we avoid the potential overflow in the
//   // delta by restating whenever the last value was negative and the
//   // next is positive. We could be more elegant by storing the
//   // deltas as the appropriate unsigned type.
//   if constexpr (isSignedIntegralType<physicalType>()) {
//     for (uint32_t i = 1; i < values.size(); ++i) {
//       const bool crossesZero = values[i] > 0 && values[i - 1] < 0;
//       if (values[i] >= values[i - 1] && !crossesZero) {
//         isRestatements->push_back(false);
//         deltas->push_back(values[i] - values[i - 1]);
//       } else {
//         isRestatements->push_back(true);
//         restatements->push_back(values[i]);
//       }
//     }
//   } else {
//     for (uint32_t i = 1; i < values.size(); ++i) {
//       if (values[i] >= values[i - 1]) {
//         isRestatements->push_back(false);
//         deltas->push_back(values[i] - values[i - 1]);
//       } else {
//         isRestatements->push_back(true);
//         restatements->push_back(values[i]);
//       }
//     }
//   }
// }

// } // namespace internal

// template <typename T>
// bool DeltaEncoding<T>::estimateSize(
//     velox::memory::MemoryPool& memoryPool,
//     std::span<const T> dataValues,
//     OptimalSearchParams searchParams,
//     encodings::EncodingParameters& encodingParameters,
//     uint32_t* size) {
//   auto values =
//   EncodingPhysicalType<T>::asEncodingPhysicalTypeSpan(dataValues); if
//   (values.empty()) {
//     return false;
//   }
//   Vector<physicalType> deltas(&memoryPool);
//   Vector<physicalType> restatements(&memoryPool);
//   Vector<bool> isRestatements(&memoryPool);
//   internal::computeDeltas(values, &deltas, &restatements, &isRestatements);
//   uint32_t deltasSize;
//   auto& deltaEncodingParameters = encodingParameters.set_delta();
//   estimateOptimalEncodingSize<physicalType>(
//       memoryPool,
//       deltas,
//       searchParams,
//       &deltasSize,
//       deltaEncodingParameters.deltasParameters().ensure());
//   uint32_t restatementsSize;
//   estimateOptimalEncodingSize<physicalType>(
//       memoryPool,
//       restatements,
//       searchParams,
//       &restatementsSize,
//       deltaEncodingParameters.restatementsParameters().ensure());
//   uint32_t isRestatementsSize;
//   estimateOptimalEncodingSize<bool>(
//       memoryPool,
//       isRestatements,
//       searchParams,
//       &isRestatementsSize,
//       deltaEncodingParameters.isRestatementsParameters().ensure());
//   *size = Encoding::kPrefixSize + 8 + deltasSize + restatementsSize +
//       isRestatementsSize;
//   return true;
// }

// template <typename T>
// std::string_view DeltaEncoding<T>::serialize(
//     std::span<const T> values,
//     Buffer* buffer) {
//   uint32_t unusedSize;
//   encodings::EncodingParameters encodingParameters;
//   // Hrm should we pass these in? This call won't normally be used outside of
//   // testing.
//   OptimalSearchParams searchParams;
//   estimateSize(
//       buffer->getMemoryPool(),
//       values,
//       searchParams,
//       encodingParameters,
//       &unusedSize);
//   return serialize(values, encodingParameters, buffer);
// }

// template <typename T>
// std::string_view DeltaEncoding<T>::serialize(
//     std::span<const T> dataValues,
//     const encodings::EncodingParameters& encodingParameters,
//     Buffer* buffer) {
//   auto values =
//   EncodingPhysicalType<T>::asEncodingPhysicalTypeSpan(dataValues); if
//   (values.empty()) {
//     NIMBLE_INCOMPATIBLE_ENCODING("DeltaEncoding can't be used with 0 rows.");
//   }
//   NIMBLE_CHECK(
//       encodingParameters.getType() ==
//               encodings::EncodingParameters::Type::delta &&
//           encodingParameters.delta_ref().has_value() &&
//           encodingParameters.delta_ref()->deltasParameters().has_value() &&
//           encodingParameters.delta_ref()
//               ->restatementsParameters()
//               .has_value() &&
//           encodingParameters.delta_ref()
//               ->isRestatementsParameters()
//               .has_value(),
//       "Incomplete or incompatible Delta encoding parameters.");

//   auto& memoryPool = buffer->getMemoryPool();
//   const uint32_t rowCount = values.size();
//   Vector<physicalType> deltas(&memoryPool);
//   Vector<physicalType> restatements(&memoryPool);
//   Vector<bool> isRestatements(&memoryPool);
//   auto& deltaEncodingParameters = encodingParameters.delta_ref().value();
//   internal::computeDeltas(values, &deltas, &restatements, &isRestatements);
//   std::string_view serializedDeltas = serializeEncoding<physicalType>(
//       deltas, deltaEncodingParameters.deltasParameters().value(), buffer);
//   std::string_view serializedRestatements = serializeEncoding<physicalType>(
//       restatements,
//       deltaEncodingParameters.restatementsParameters().value(),
//       buffer);
//   std::string_view serializedIsRestatements = serializeEncoding<bool>(
//       isRestatements,
//       deltaEncodingParameters.isRestatementsParameters().value(),
//       buffer);
//   const uint32_t encodingSize = Encoding::kPrefixSize + 8 +
//       serializedDeltas.size() + serializedRestatements.size() +
//       serializedIsRestatements.size();
//   char* reserved = buffer->reserve(encodingSize);
//   char* pos = reserved;
//   Encoding::serializePrefix(
//       EncodingType::Delta, TypeTraits<T>::dataType, rowCount, pos);
//   encoding::writeUint32(serializedDeltas.size(), pos);
//   encoding::writeUint32(serializedRestatements.size(), pos);
//   encoding::writeBytes(serializedDeltas, pos);
//   encoding::writeBytes(serializedRestatements, pos);
//   encoding::writeBytes(serializedIsRestatements, pos);
//   NIMBLE_DCHECK_EQ(pos - reserved, encodingSize, "Encoding size mismatch.");
//   return {reserved, encodingSize};
// }

template <typename T>
std::string DeltaEncoding<T>::debugString(int offset) const {
  std::string log = Encoding::debugString(offset);
  log += fmt::format(
      "\n{}deltas child:\n{}",
      std::string(offset + 2, ' '),
      deltas_->debugString(offset + 4));
  log += fmt::format(
      "\n{}restatements child:\n{}",
      std::string(offset + 2, ' '),
      restatements_->debugString(offset + 4));
  log += fmt::format(
      "\n{}isRestatements child:\n{}",
      std::string(offset + 2, ' '),
      isRestatements_->debugString(offset + 4));
  return log;
}

} // namespace facebook::nimble::legacy
