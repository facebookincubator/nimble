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

#include "dwio/nimble/velox/selective/ChunkedDecoder.h"

#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/EncodingFactory.h"

#include <cstddef>

namespace facebook::nimble {

using namespace facebook::velox;

bool ChunkedDecoder::loadNextChunk() {
  if (!ensureInput(5)) {
    return false;
  }
  auto length = encoding::readUint32(inputData_);
  const auto compressionType =
      static_cast<CompressionType>(encoding::readChar(inputData_));
  inputSize_ -= 5;
  VELOX_CHECK(ensureInput(length));
  const char* chunkData;
  int64_t chunkSize;
  switch (compressionType) {
    case CompressionType::Uncompressed:
      chunkData = inputData_;
      chunkSize = length;
      break;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported compression type: {}", toString(compressionType));
  }
  inputData_ += length;
  inputSize_ -= length;
  encoding_ = encodingFactory_(*pool_, std::string_view(chunkData, chunkSize));
  remainingValues_ = encoding_->rowCount();
  VELOX_CHECK_GT(remainingValues_, 0);
  VLOG(1) << encoding_->debugString();
  return true;
}

bool ChunkedDecoder::ensureInput(int size) {
  while (inputSize_ < size) {
    if (inputSize_ > 0) {
      // We need to copy the values from `inputData_' to our own buffer before
      // calling `input_->Next' because `buf' could be overwritten by the call.
      prepareInputBuffer(inputSize_);
    }
    const char* buf;
    int len{0};
    if (!input_->Next(reinterpret_cast<const void**>(&buf), &len)) {
      VELOX_CHECK_EQ(inputSize_, 0);
      return false;
    }
    if (inputSize_ == 0) {
      inputData_ = buf;
    } else {
      prepareInputBuffer(inputSize_ + len);
      // Append after the previous content.
      memcpy(inputBuffer_->asMutable<char>() + inputSize_, buf, len);
    }
    inputSize_ += len;
  }
  return true;
}

bool ChunkedDecoder::ensureInputIncremental_hack(int size, const char*& pos) {
  const auto currentOffset = pos - inputData_;
  const bool ensured = ensureInput(size);
  if (LIKELY(ensured)) {
    pos = inputData_ + currentOffset;
  }
  return ensured;
}

// After this function is called, we ensure these:
// 1. `inputBuffer_' is allocated and at least `size' bytes large.
// 2. The first `inputSize_' bytes in `inputData_' before the call are copied to
//    the beginning of `inputBuffer_'.
// 3. `inputData_' is pointing to `inputBuffer_'.
void ChunkedDecoder::prepareInputBuffer(int size) {
  VELOX_DCHECK_LE(inputSize_, size);
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

void ChunkedDecoder::nextBools(
    uint64_t* data,
    int64_t count,
    const uint64_t* incomingNulls) {
  if (!decodeValuesWithNulls_) {
    const int64_t totalNumValues = !incomingNulls
        ? count
        : velox::bits::countNonNulls(incomingNulls, 0, count);
    for (int64_t i = 0; i < totalNumValues;) {
      if (FOLLY_UNLIKELY(remainingValues_ == 0)) {
        VELOX_CHECK(loadNextChunk());
        VELOX_CHECK_EQ(encoding_->dataType(), DataType::Bool);
      }
      const auto numValues = std::min(totalNumValues - i, remainingValues_);
      encoding_->materializeBoolsAsBits(numValues, data, i);
      remainingValues_ -= numValues;
      i += numValues;
    }
    if (incomingNulls != nullptr) {
      velox::bits::scatterBits(
          totalNumValues,
          count,
          reinterpret_cast<char*>(data),
          incomingNulls,
          reinterpret_cast<char*>(data));
    }
  } else {
    // This is a temporary solution for array and map types, which only needs to
    // support fully reading uint32_t offsets and sizes.
    if (!nullableValues_ ||
        nullableValues_->capacity() < count * sizeof(uint32_t)) {
      nullableValues_ = AlignedBuffer::allocate<uint32_t>(count, pool_, 0);
    }
    decodeNullable(
        data, nullableValues_->asMutable<uint32_t>(), count, incomingNulls);
  }
}

void ChunkedDecoder::nextIndices(
    int32_t* data,
    int64_t count,
    const uint64_t* incomingNulls) {
  velox::BufferPtr nullsBuffer;
  if (incomingNulls != nullptr) {
    nullsBuffer = velox::allocateNulls(count, pool_, velox::bits::kNull);
  }
  decodeNullable(
      incomingNulls ? nullsBuffer->asMutable<uint64_t>() : nullptr,
      reinterpret_cast<uint32_t*>(data),
      count,
      incomingNulls);
}

void ChunkedDecoder::skip(int64_t numValues) {
  while (numValues > 0) {
    if (FOLLY_UNLIKELY(remainingValues_ == 0)) {
      VELOX_CHECK(loadNextChunk());
    }
    if (numValues < remainingValues_) {
      encoding_->skip(numValues);
      remainingValues_ -= numValues;
      break;
    }
    numValues -= remainingValues_;
    remainingValues_ = 0;
  }
}

std::optional<size_t> ChunkedDecoder::estimateRowCount() const {
  if (encoding_ != nullptr) {
    return rowCountEstimate_;
  }
  constexpr int kChunkCompressionTypeOffset = 4;
  constexpr int kEncodingOffset =
      kChunkCompressionTypeOffset + /*chunkCompressionType=*/1;
  constexpr int kChunkRowCountOffset =
      kEncodingOffset + Encoding::kRowCountOffset;
  VELOX_CHECK(
      const_cast<ChunkedDecoder*>(this)->ensureInput(
          kChunkRowCountOffset + sizeof(uint32_t)));
  if (static_cast<CompressionType>(inputData_[kChunkCompressionTypeOffset]) !=
      CompressionType::Uncompressed) {
    rowCountEstimate_ = std::nullopt;
    return rowCountEstimate_;
  }
  rowCountEstimate_ =
      folly::loadUnaligned<uint32_t>(inputData_ + kChunkRowCountOffset);
  return rowCountEstimate_;
}

std::optional<size_t> ChunkedDecoder::estimateStringDataSize() const {
  if (encoding_) {
    return stringDataSizeEstimate_;
  }
  constexpr int kChunkCompressionTypeOffset{4};
  constexpr int kEncodingOffset{
      kChunkCompressionTypeOffset + /*chunkCompressionType=*/1};
  VELOX_CHECK(
      const_cast<ChunkedDecoder*>(this)->ensureInput(kEncodingOffset + 6));
  auto* pos = inputData_;
  const auto chunkSize = encoding::readUint32(pos);
  const auto compressionType =
      static_cast<CompressionType>(encoding::readChar(pos));
  // We don't want to decompress the chunk just for the estimate. We will fall
  // back on a top level heuristics instead.
  if (compressionType != CompressionType::Uncompressed) {
    stringDataSizeEstimate_ = std::nullopt;
    return stringDataSizeEstimate_;
  }
  auto encodingStart = kEncodingOffset;
  size_t totalSize = pos + chunkSize - inputData_;
  auto encodingType = static_cast<EncodingType>(encoding::readChar(pos));
  VELOX_CHECK_EQ(
      static_cast<DataType>(encoding::readChar(pos)), DataType::String);
  const auto rowCount = encoding::readUint32(pos);
  // Peel off nullable encoding.
  if (encodingType == EncodingType::Nullable) {
    encodingStart += Encoding::kPrefixSize + /*nonNullEncodingSize=*/4;
    VELOX_CHECK(
        const_cast<ChunkedDecoder*>(this)->ensureInputIncremental_hack(
            encodingStart + 6, pos));
    const auto nonNullsBytes = encoding::readUint32(pos);
    // TODO: it might not require an update here.
    totalSize = pos + nonNullsBytes - inputData_;
    encodingType = static_cast<EncodingType>(encoding::readChar(pos));
    VELOX_CHECK_EQ(
        static_cast<DataType>(encoding::readChar(pos)), DataType::String);
    VELOX_CHECK_LE(encoding::readUint32(pos), rowCount);
  }
  // TODO: we will soon add simple support for other encodings before we have
  // column stats implementation. In the vast majority of cases, String types
  // are encoded with TrivialEncoding.
  if (encodingType != EncodingType::Trivial) {
    stringDataSizeEstimate_ = std::nullopt;
    return stringDataSizeEstimate_;
  }
  {
    const auto ensured =
        const_cast<ChunkedDecoder*>(this)->ensureInputIncremental_hack(
            encodingStart + Encoding::kPrefixSize +
                TrivialEncoding<std::string_view>::kPrefixSize,
            pos);
    VELOX_CHECK(ensured);
  }
  const auto dataCompressionType =
      static_cast<CompressionType>(encoding::readChar(pos));
  const auto lengthBlobSize = encoding::readUint32(pos);
  const auto blobOffset = pos + lengthBlobSize - inputData_;
  VELOX_CHECK_GE(totalSize, blobOffset);
  const size_t blobSize = totalSize - blobOffset;
  if (dataCompressionType == CompressionType::Uncompressed) {
    stringDataSizeEstimate_ = blobSize;
    return stringDataSizeEstimate_;
  }
  {
    const auto ensured =
        const_cast<ChunkedDecoder*>(this)->ensureInput(totalSize);
    VELOX_CHECK(ensured);
  }
  stringDataSizeEstimate_ = Compression::uncompressedSize(
      dataCompressionType, {inputData_ + blobOffset, blobSize});
  return stringDataSizeEstimate_;
}

} // namespace facebook::nimble
