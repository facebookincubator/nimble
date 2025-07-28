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

#include "dwio/nimble/velox/selective/NimbleData.h"

#include "dwio/nimble/velox/selective/ChunkedDecoder.h"
#include "velox/dwio/common/BufferUtil.h"

namespace facebook::nimble {

using namespace facebook::velox;

NimbleData::NimbleData(
    const std::shared_ptr<const Type>& nimbleType,
    StripeStreams& streams,
    memory::MemoryPool& memoryPool,
    ChunkedDecoder* inMapDecoder)
    : nimbleType_(nimbleType),
      streams_(streams),
      memoryPool_(memoryPool),
      inMapDecoder_(inMapDecoder) {
  switch (nimbleType->kind()) {
    case Kind::Scalar:
      // Nulls in scalar types will be decoded along with values.
      break;
    case Kind::Row: {
      auto& rowType = nimbleType->asRow();
      nullsDecoder_ = makeDecoder(
          rowType.nullsDescriptor(), /*decodeValuesWithNulls=*/false);
      break;
    }
    case Kind::Array: {
      auto& arrayType = nimbleType->asArray();
      nullsDecoder_ = makeDecoder(
          arrayType.lengthsDescriptor(), /*decodeValuesWithNulls=*/true);
      break;
    }
    case Kind::Map: {
      auto& mapType = nimbleType->asMap();
      nullsDecoder_ = makeDecoder(
          mapType.lengthsDescriptor(), /*decodeValuesWithNulls=*/true);
      break;
    }
    case Kind::ArrayWithOffsets: {
      auto& arrayWithOffsetsType = nimbleType->asArrayWithOffsets();
      nullsDecoder_ = makeDecoder(
          arrayWithOffsetsType.offsetsDescriptor(),
          /*decodeValuesWithNulls=*/true);
      break;
    }
    case Kind::SlidingWindowMap: {
      auto& slidingWindowMapType = nimbleType->asSlidingWindowMap();
      nullsDecoder_ = makeDecoder(
          slidingWindowMapType.offsetsDescriptor(),
          /*decodeValuesWithNulls=*/true);
      break;
    }
    case Kind::FlatMap: {
      auto& flatMapType = nimbleType->asFlatMap();
      nullsDecoder_ = makeDecoder(
          flatMapType.nullsDescriptor(), /*decodeValuesWithNulls=*/false);
      break;
    }
    default:
      VELOX_UNSUPPORTED("{}", toString(nimbleType->kind()));
  }
}

void NimbleData::readNulls(
    vector_size_t numValues,
    const uint64_t* incomingNulls,
    BufferPtr& nulls,
    bool /*nullsOnly*/) {
  if (!nullsDecoder_ && !inMapDecoder_ && !incomingNulls) {
    nulls.reset();
    return;
  }
  auto numBytes = velox::bits::nbytes(numValues);
  if (!nulls || nulls->capacity() < numBytes) {
    nulls = AlignedBuffer::allocate<char>(numBytes, &memoryPool_);
  }
  nulls->setSize(numBytes);
  auto* nullsPtr = nulls->asMutable<uint64_t>();
  if (inMapDecoder_) {
    if (nullsDecoder_) {
      dwio::common::ensureCapacity<char>(inMap_, numBytes, &memoryPool_);
      inMapDecoder_->nextBools(
          inMap_->asMutable<uint64_t>(), numValues, incomingNulls);
      nullsDecoder_->nextBools(nullsPtr, numValues, inMap_->as<uint64_t>());
    } else {
      inMapDecoder_->nextBools(nullsPtr, numValues, incomingNulls);
      inMap_ = nulls;
    }
  } else if (nullsDecoder_) {
    nullsDecoder_->nextBools(nullsPtr, numValues, incomingNulls);
  } else {
    memcpy(nullsPtr, incomingNulls, numBytes);
  }
}

const velox::BufferPtr& NimbleData::getPreloadedValues() {
  VELOX_CHECK(nullsDecoder_, "A valid nullable decoder is required");
  return nullsDecoder_->getPreloadedValues();
}

uint64_t NimbleData::skipNulls(uint64_t numValues, bool /*nullsOnly*/) {
  if (!nullsDecoder_ && !inMapDecoder_) {
    return numValues;
  }
  constexpr uint64_t kBufferWords = 256;
  constexpr auto kBitCount = 64 * kBufferWords;
  auto countNulls = [](ChunkedDecoder& decoder, size_t size) {
    uint64_t buffer[kBufferWords];
    decoder.nextBools(buffer, size, nullptr);
    return velox::bits::countNulls(buffer, 0, size);
  };
  auto remaining = numValues;
  while (remaining > 0) {
    auto chunkSize = std::min(remaining, kBitCount);
    uint64_t nullCount;
    if (inMapDecoder_) {
      nullCount = countNulls(*inMapDecoder_, chunkSize);
      if (nullsDecoder_) {
        if (auto inMapSize = chunkSize - nullCount; inMapSize > 0) {
          nullCount += countNulls(*nullsDecoder_, inMapSize);
        }
      }
    } else {
      nullCount = countNulls(*nullsDecoder_, chunkSize);
    }
    remaining -= chunkSize;
    numValues -= nullCount;
  }
  return numValues;
}

ChunkedDecoder NimbleData::makeScalarDecoder() {
  return ChunkedDecoder(
      streams_.enqueue(nimbleType_->asScalar().scalarDescriptor().offset()),
      memoryPool_,
      /*decodeValuesWithNulls=*/false);
}

std::unique_ptr<ChunkedDecoder> NimbleData::makeLengthDecoder() {
  VELOX_CHECK(
      nimbleType().isArrayWithOffsets() || nimbleType().isSlidingWindowMap());
  if (nimbleType().isArrayWithOffsets()) {
    return makeDecoder(
        nimbleType().asArrayWithOffsets().lengthsDescriptor(),
        false /* decodeValuesWithNulls */);
  } else {
    return makeDecoder(
        nimbleType().asSlidingWindowMap().lengthsDescriptor(),
        false /* decodeValuesWithNulls */);
  }
}

std::unique_ptr<ChunkedDecoder> NimbleData::makeDecoder(
    const StreamDescriptor& descriptor,
    bool decodeValuesWithNulls) {
  auto input = streams_.enqueue(descriptor.offset());
  if (!input) {
    return nullptr;
  }
  return std::make_unique<ChunkedDecoder>(
      std::move(input), memoryPool_, decodeValuesWithNulls);
}

std::unique_ptr<velox::dwio::common::FormatData> NimbleParams::toFormatData(
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& /*type*/,
    const velox::common::ScanSpec& /*scanSpec*/) {
  return std::make_unique<NimbleData>(
      nimbleType_, streams_, pool(), inMapDecoder_);
}

} // namespace facebook::nimble
