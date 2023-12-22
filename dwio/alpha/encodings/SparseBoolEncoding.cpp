// Copyright 2004-present Facebook. All Rights Reserved.

#include <numeric>

#include "dwio/alpha/encodings/SparseBoolEncoding.h"

#include "dwio/alpha/common/Bits.h"
#include "dwio/alpha/common/EncodingPrimitives.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/encodings/Compression.h"
#include "dwio/alpha/encodings/EncodingFactoryNew.h"

namespace facebook::alpha {

SparseBoolEncoding::SparseBoolEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data)
    : TypedEncoding<bool, bool>{memoryPool, data},
      sparseValue_{static_cast<bool>(data[kSparseValueOffset])},
      indicesUncompressed_{&memoryPool},
      indices_{EncodingFactory::decode(
          memoryPool,
          {data.data() + kIndicesOffset, data.size() - kIndicesOffset})} {
  reset();
}

void SparseBoolEncoding::reset() {
  row_ = 0;
  indices_.reset();
  nextIndex_ = indices_.nextValue();
}

void SparseBoolEncoding::skip(uint32_t rowCount) {
  const uint32_t end = row_ + rowCount;
  while (nextIndex_ < end) {
    nextIndex_ = indices_.nextValue();
  }
  row_ = end;
}

void SparseBoolEncoding::materialize(uint32_t rowCount, void* buffer) {
  const uint32_t end = row_ + rowCount;
  if (sparseValue_) {
    memset(buffer, 0, rowCount);
    while (nextIndex_ < end) {
      static_cast<bool*>(buffer)[nextIndex_ - row_] = true;
      nextIndex_ = indices_.nextValue();
    }
  } else {
    memset(buffer, 1, rowCount);
    while (nextIndex_ < end) {
      static_cast<bool*>(buffer)[nextIndex_ - row_] = false;
      nextIndex_ = indices_.nextValue();
    }
  }
  row_ = end;
}

std::string_view SparseBoolEncoding::encode(
    EncodingSelection<bool>& selection,
    std::span<const bool> values,
    Buffer& buffer) {
  // Decide the polarity of the encoding.
  const uint32_t valueCount = values.size();
  const uint32_t setCount = selection.statistics().uniqueCounts().at(true);
  bool sparseValue;
  uint32_t indexCount;
  if (setCount > (valueCount >> 1)) {
    sparseValue = false;
    indexCount = valueCount - setCount;
  } else {
    sparseValue = true;
    indexCount = setCount;
  }

  Vector<uint32_t> indices{&buffer.getMemoryPool()};
  indices.reserve(indexCount + 1);
  if (sparseValue) {
    for (auto i = 0; i < values.size(); ++i) {
      if (values[i]) {
        indices.push_back(i);
      }
    }
  } else {
    for (auto i = 0; i < values.size(); ++i) {
      if (!values[i]) {
        indices.push_back(i);
      }
    }
  }

  // Pushing rowCount as the last item. Materialize relies on finding this value
  // in order to stop looping as this value is greater than any possible index.
  indices.push_back(valueCount);

  Buffer tempBuffer{buffer.getMemoryPool()};
  std::string_view serializedIndices =
      selection.template encodeNested<uint32_t>(
          EncodingIdentifiers::SparseBool::Indices, indices, tempBuffer);

  const uint32_t encodingSize = Encoding::kPrefixSize +
      SparseBoolEncoding::kPrefixSize + serializedIndices.size();
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::SparseBool, DataType::Bool, valueCount, pos);
  encoding::writeChar(sparseValue, pos);
  encoding::writeBytes(serializedIndices, pos);

  ALPHA_DASSERT(pos - reserved == encodingSize, "Encoding size mismatch.");
  return {reserved, encodingSize};
}

} // namespace facebook::alpha
