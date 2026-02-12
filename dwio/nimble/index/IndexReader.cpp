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

#include "dwio/nimble/index/IndexReader.h"

#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/EncodingFactory.h"

namespace facebook::nimble::index {

using namespace facebook::velox;

// Returns std::nullopt from the current function if the optional is empty.
#define RETURN_IF_NULLOPT(opt) \
  if (!(opt).has_value()) {    \
    return std::nullopt;       \
  }

IndexReader::IndexReader(
    std::unique_ptr<velox::dwio::common::SeekableInputStream> input,
    uint32_t stripeIndex,
    std::shared_ptr<StripeIndexGroup> indexGroup,
    velox::memory::MemoryPool* pool)
    : stripeIndex_{stripeIndex},
      indexGroup_{std::move(indexGroup)},
      input_{std::move(input)},
      pool_{pool} {
  NIMBLE_CHECK_NOT_NULL(input_);
  NIMBLE_CHECK_NOT_NULL(pool_);
}

std::unique_ptr<IndexReader> IndexReader::create(
    std::unique_ptr<velox::dwio::common::SeekableInputStream> input,
    uint32_t stripeIndex,
    std::shared_ptr<StripeIndexGroup> indexGroup,
    velox::memory::MemoryPool* pool) {
  return std::unique_ptr<IndexReader>(new IndexReader(
      std::move(input), stripeIndex, std::move(indexGroup), pool));
}

std::optional<uint32_t> IndexReader::seekAtOrAfter(
    std::string_view encodedKey) {
  const auto chunkLocation = indexGroup_->lookupChunk(stripeIndex_, encodedKey);
  RETURN_IF_NULLOPT(chunkLocation);

  const auto rowOffset =
      seekAtOrAfterInChunk(chunkLocation->streamOffset, encodedKey);
  NIMBLE_CHECK(rowOffset.has_value());
  return chunkLocation->rowOffset + rowOffset.value();
}

std::optional<uint32_t> IndexReader::seekAtOrAfterInChunk(
    uint32_t chunkOffset,
    std::string_view encodedKey) {
  seekToChunk(chunkOffset);
  NIMBLE_CHECK_NOT_NULL(encoding_);
  return encoding_->seekAtOrAfter(&encodedKey);
}

bool IndexReader::ensureInput(int size) {
  while (inputSize_ < size) {
    if (inputSize_ > 0) {
      prepareInputBuffer(inputSize_);
    }
    const char* buf;
    int len{0};
    if (!input_->Next(reinterpret_cast<const void**>(&buf), &len)) {
      NIMBLE_CHECK_EQ(inputSize_, 0);
      return false;
    }
    if (inputSize_ == 0) {
      inputData_ = buf;
    } else {
      prepareInputBuffer(inputSize_ + len);
      std::memcpy(inputBuffer_->asMutable<char>() + inputSize_, buf, len);
    }
    inputSize_ += len;
  }
  return true;
}

void IndexReader::seekToChunk(uint32_t chunkOffset) {
  if ((chunkOffset_ == chunkOffset) && (encoding_ != nullptr)) {
    return;
  }

  chunkOffset_ = chunkOffset;
  const std::vector<uint64_t> offsets{chunkOffset_};
  velox::dwio::common::PositionProvider positionProvider(offsets);
  input_->seekToPosition(positionProvider);

  inputData_ = nullptr;
  inputSize_ = 0;
  loadChunk();
}

void IndexReader::loadChunk() {
  auto ret = ensureInput(5);
  NIMBLE_CHECK(ret);
  const auto length = encoding::readUint32(inputData_);
  const auto compressionType =
      static_cast<CompressionType>(encoding::readChar(inputData_));
  inputSize_ -= 5;
  ret = ensureInput(length);
  NIMBLE_CHECK(ret);
  const char* chunkData;
  int64_t chunkSize;
  switch (compressionType) {
    case CompressionType::Uncompressed:
      chunkData = inputData_;
      chunkSize = length;
      break;
    default:
      NIMBLE_UNSUPPORTED("Unsupported compression type: {}", compressionType);
  }
  inputData_ += length;
  inputSize_ -= length;
  stringBuffers_.clear();
  encoding_ = nimble::EncodingFactory::decode(
      *pool_,
      std::string_view(chunkData, chunkSize),
      [&](uint32_t totalLength) {
        auto& buffer = stringBuffers_.emplace_back(
            velox::AlignedBuffer::allocate<char>(totalLength, pool_));
        return buffer->asMutable<void>();
      });
  NIMBLE_CHECK_EQ(encoding_->dataType(), DataType::String);
  NIMBLE_CHECK(
      encoding_->encodingType() == EncodingType::Trivial ||
          encoding_->encodingType() == EncodingType::Prefix,
      "Unsupported encoding type: {}",
      encoding_->encodingType());
}

void IndexReader::prepareInputBuffer(int32_t size) {
  NIMBLE_DCHECK_LE(inputSize_, size);
  if (inputBuffer_ && size <= inputBuffer_->capacity()) {
    if (inputData_ == inputBuffer_->as<char>()) {
      return;
    }
    char* newInputData = inputBuffer_->asMutable<char>();
    if (inputSize_ > 0) {
      memmove(newInputData, inputData_, inputSize_);
    }
    inputData_ = newInputData;
  } else {
    auto newInputBuffer = AlignedBuffer::allocate<char>(size, pool_);
    char* newInputData = newInputBuffer->asMutable<char>();
    if (inputSize_ > 0) {
      std::memcpy(newInputData, inputData_, inputSize_);
    }
    inputBuffer_ = std::move(newInputBuffer);
    inputData_ = newInputData;
  }
}

} // namespace facebook::nimble::index
