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
#include "dwio/nimble/common/Bits.h"
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingIdentifier.h"
#include "dwio/nimble/encodings/EncodingSelection.h"

// A nullable encoding holds a subencoding of non-null values and another
// subencoding of booleans representing whether each row was null.

namespace facebook::nimble {

// Data layout is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// 4 bytes: non-null child encoding size (X)
// X bytes: non-null child encoding bytes
// Y byes: null child encoding bytes
template <typename T>
class NullableEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  NullableEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data);

  uint32_t nullCount() const final;
  bool isNullable() const final;
  const Encoding* nonNulls() const;

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;
  uint32_t materializeNullable(
      uint32_t rowCount,
      void* buffer,
      std::function<void*()> nulls,
      const bits::Bitmap* scatterBitmap = nullptr,
      uint32_t offset = 0) final;

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  static std::string_view encodeNullable(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      std::span<const bool> nulls,
      Buffer& buffer);

  std::string debugString(int offset) const final;

 private:
  // One bit for each row. A true bit represents a row with a non-null value.
  const char* bitmap_;
  std::unique_ptr<Encoding> nonNullValues_;
  std::unique_ptr<Encoding> nulls_;
  uint32_t row_ = 0;

  // Temporary buffers.
  Vector<uint32_t> indicesBuffer_;
  Vector<char> charBuffer_;
  Vector<bool> boolBuffer_;
};

//
// End of public API. Implementations follow.
//

template <typename T>
NullableEncoding<T>::NullableEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data)
    : TypedEncoding<T, physicalType>(memoryPool, data),
      indicesBuffer_(&memoryPool),
      charBuffer_(&memoryPool),
      boolBuffer_(&memoryPool) {
  const char* pos = data.data() + Encoding::kPrefixSize;
  const uint32_t nonNullsBytes = encoding::readUint32(pos);
  nonNullValues_ =
      EncodingFactory::decode(this->memoryPool_, {pos, nonNullsBytes});
  pos += nonNullsBytes;
  nulls_ = EncodingFactory::decode(
      this->memoryPool_, {pos, static_cast<size_t>(data.end() - pos)});
  NIMBLE_DASSERT(
      Encoding::rowCount() == nulls_->rowCount(), "Nulls count mismatch.");
}

template <typename T>
uint32_t NullableEncoding<T>::nullCount() const {
  return nulls_->rowCount() - nonNullValues_->rowCount();
}

template <typename T>
const Encoding* NullableEncoding<T>::nonNulls() const {
  return nonNullValues_.get();
}

template <typename T>
bool NullableEncoding<T>::isNullable() const {
  return true;
}

template <typename T>
void NullableEncoding<T>::reset() {
  row_ = 0;
  nonNullValues_->reset();
  nulls_->reset();
}

template <typename T>
void NullableEncoding<T>::skip(uint32_t rowCount) {
  // Hrm this isn't ideal. We should return to this later -- a new
  // encoding func? Encoding::Accumulate to add up next N rows?
  boolBuffer_.resize(rowCount);
  nulls_->materialize(rowCount, boolBuffer_.data());
  const uint32_t nonNullCount =
      std::accumulate(boolBuffer_.begin(), boolBuffer_.end(), 0U);
  nonNullValues_->skip(nonNullCount);
}

template <typename T>
void NullableEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  // This too isn't ideal. We will want an Encoding::Indices method or
  // something our SparseBool can use, giving back just the set indices
  // rather than a materialization.
  boolBuffer_.resize(rowCount);
  nulls_->materialize(rowCount, boolBuffer_.data());
  const uint32_t nonNullCount =
      std::accumulate(boolBuffer_.begin(), boolBuffer_.end(), 0U);
  nonNullValues_->materialize(nonNullCount, buffer);

  if (nonNullCount != rowCount) {
    physicalType* output = static_cast<physicalType*>(buffer) + rowCount - 1;
    const physicalType* lastNonNull =
        static_cast<physicalType*>(buffer) + nonNullCount - 1;
    // This is a generic scatter -- should we have a common scatter func?
    uint32_t pos = rowCount - 1;
    while (output != lastNonNull) {
      if (boolBuffer_[pos]) {
        *output = *lastNonNull;
        --lastNonNull;
      } else {
        *output = physicalType();
      }
      --output;
      --pos;
    }
  }

  row_ += rowCount;
}

template <typename T>
uint32_t NullableEncoding<T>::materializeNullable(
    uint32_t rowCount,
    void* buffer,
    std::function<void*()> nulls,
    const bits::Bitmap* scatterBitmap,
    uint32_t offset) {
  boolBuffer_.resize(rowCount);
  nulls_->materialize(rowCount, boolBuffer_.data());
  const uint32_t nonNullCount =
      std::accumulate(boolBuffer_.begin(), boolBuffer_.end(), 0U);

  if (offset > 0) {
    buffer = static_cast<physicalType*>(buffer) + offset;
  }
  nonNullValues_->materialize(nonNullCount, buffer);

  auto scatterSize = scatterBitmap ? scatterBitmap->size() - offset : rowCount;
  if (nonNullCount != scatterSize) {
    void* nullBitmap = nulls();
    bits::BitmapBuilder nullBits{nullBitmap, offset + scatterSize};
    nullBits.clear(offset, offset + scatterSize);

    uint32_t pos = offset + scatterSize - 1;
    physicalType* output = static_cast<physicalType*>(buffer) + scatterSize - 1;
    const physicalType* lastNonNull =
        static_cast<physicalType*>(buffer) + nonNullCount - 1;
    auto nonNullIt = boolBuffer_.begin() + rowCount - 1;

    if (scatterSize != rowCount) {
      // In scattered reads, spread the items into the right positions in
      // |buffer| and |nullBitmap| based on the bits set to 1 in
      // |scatterBitmap|.
      while (output != lastNonNull) {
        if (scatterBitmap->test(pos)) {
          if (*nonNullIt--) {
            nullBits.set(pos);
            *output = *lastNonNull;
            --lastNonNull;
          }
        }
        --output;
        --pos;
      }
    } else {
      while (output != lastNonNull) {
        if (*nonNullIt--) {
          nullBits.set(pos);
          *output = *lastNonNull;
          --lastNonNull;
        }
        --output;
        --pos;
      }
    }

    if (output >= buffer) {
      nullBits.set(offset, pos + 1);
    }
  }

  row_ += rowCount;
  return nonNullCount;
}

template <typename T>
template <typename V>
void NullableEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  const auto endRow = visitor.rowAt(visitor.numRows() - 1);
  const auto rowCount = endRow - params.numScanned + 1;
  if (const uint64_t* incomingNulls = visitor.reader().rawNullsInReadRange()) {
    auto nwords = velox::bits::nwords(rowCount);
    if (params.numScanned % 64 == 0) {
      boolBuffer_.resize(nwords * sizeof(uint64_t));
    } else {
      boolBuffer_.resize(2 * nwords * sizeof(uint64_t));
    }
    auto* chunkNulls = reinterpret_cast<uint64_t*>(boolBuffer_.data());
    auto* chunkNullBytes = reinterpret_cast<char*>(boolBuffer_.data());
    if (params.numScanned % 64 == 0) {
      incomingNulls += params.numScanned / 64;
    } else {
      auto* incomingNullsCopy = chunkNulls + nwords;
      velox::bits::copyBits(
          incomingNulls, params.numScanned, incomingNullsCopy, 0, rowCount);
      incomingNulls = incomingNullsCopy;
    }
    auto numInner = velox::bits::countNonNulls(incomingNulls, 0, rowCount);
    nulls_->materializeBoolsAsBits(numInner, chunkNulls, 0);
    velox::bits::scatterBits(
        numInner, rowCount, chunkNullBytes, incomingNulls, chunkNullBytes);
    auto* nulls = params.makeReaderNulls();
    velox::bits::copyBits(chunkNulls, 0, nulls, params.numScanned, rowCount);
  } else {
    auto* nulls = params.makeReaderNulls();
    nulls_->materializeBoolsAsBits(rowCount, nulls, params.numScanned);
  }
  params.initReturnReaderNulls();
  callReadWithVisitor(*nonNullValues_, visitor, params);
}

template <typename T>
std::string_view NullableEncoding<T>::encodeNullable(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    std::span<const bool> nulls,
    Buffer& buffer) {
  const uint32_t rowCount = nulls.size();

  Buffer tempBuffer{buffer.getMemoryPool()};
  std::string_view serializedValues =
      selection.template encodeNested<physicalType>(
          EncodingIdentifiers::Nullable::Data, values, tempBuffer);
  std::string_view serializedNulls = selection.template encodeNested<bool>(
      EncodingIdentifiers::Nullable::Nulls, nulls, tempBuffer);

  const uint32_t encodingSize = Encoding::kPrefixSize + 4 +
      serializedValues.size() + serializedNulls.size();
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::Nullable, TypeTraits<T>::dataType, rowCount, pos);
  encoding::writeString(serializedValues, pos);
  encoding::writeBytes(serializedNulls, pos);
  NIMBLE_DASSERT(pos - reserved == encodingSize, "Encoding size mismatch.");
  return {reserved, encodingSize};
}

template <typename T>
std::string NullableEncoding<T>::debugString(int offset) const {
  std::string log = Encoding::debugString(offset);
  log += fmt::format(
      "\n{}non-null child:\n{}",
      std::string(offset + 2, ' '),
      nonNullValues_->debugString(offset + 4));
  log += fmt::format(
      "\n{}null child:\n{}",
      std::string(offset + 2, ' '),
      nulls_->debugString(offset + 4));
  return log;
}

} // namespace facebook::nimble
