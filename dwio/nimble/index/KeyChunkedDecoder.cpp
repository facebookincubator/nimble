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

#include "dwio/nimble/index/KeyChunkedDecoder.h"

#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Types.h"

namespace facebook::nimble {

using namespace facebook::velox;

std::optional<uint32_t> KeyChunkedDecoder::seekAtOrAfter(
    uint32_t chunkOffset,
    std::string_view encodedKey,
    bool& exactMatch) {
  seekToChunk(chunkOffset);
  NIMBLE_CHECK_NOT_NULL(encoding_);
  return encoding_->seekAtOrAfter(&encodedKey, exactMatch);
}

bool KeyChunkedDecoder::ensureInput(int size) {
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
      memcpy(inputBuffer_->asMutable<char>() + inputSize_, buf, len);
    }
    inputSize_ += len;
  }
  return true;
}

void KeyChunkedDecoder::seekToChunk(uint32_t chunkOffset) {
  if (chunkOffset_ == chunkOffset) {
    return;
  }

  chunkOffset_ = chunkOffset;
  // Use position provider to seek to the chunk offset
  velox::dwio::common::PositionProvider positionProvider({chunkOffset_});
  input_->seekToPosition(positionProvider);

  // Reset buffer state after seeking
  inputData_ = nullptr;
  inputSize_ = 0;

  // Load the chunk at this position
  loadChunk();
}

void KeyChunkedDecoder::loadChunk() {
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
  encoding_ =
      EncodingFactory::decode(*pool_, std::string_view(chunkData, chunkSize));
  NIMBLE_CHECK_EQ(encoding_->dataType(), DataType::String);
  // TODO: support prefix encoding for encoded keys.
  NIMBLE_CHECK_EQ(encoding_->encodingType(), EncodingType::Trivial);
}

void KeyChunkedDecoder::prepareInputBuffer(int32_t size) {
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
      memcpy(newInputData, inputData_, inputSize_);
    }
    inputBuffer_ = std::move(newInputBuffer);
    inputData_ = newInputData;
  }
}

} // namespace facebook::nimble
