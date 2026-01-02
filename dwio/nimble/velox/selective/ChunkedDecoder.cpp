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

#include "dwio/nimble/common/Constants.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "velox/common/testutil/TestValue.h"

#include <cstddef>

using facebook::velox::common::testutil::TestValue;

namespace facebook::nimble {

using namespace facebook::velox;

void ChunkedDecoder::loadNextChunk() {
  auto ret = ensureInput(kChunkHeaderSize);
  NIMBLE_CHECK(ret, "Failed to read chunk header");
  auto length = encoding::readUint32(inputData_);
  const auto compressionType =
      static_cast<CompressionType>(encoding::readChar(inputData_));
  inputSize_ -= kChunkHeaderSize;
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
  encoding_ = encodingFactory_(*pool_, std::string_view(chunkData, chunkSize));
  remainingValues_ = encoding_->rowCount();
  NIMBLE_CHECK_GT(remainingValues_, 0);
  VLOG(1) << encoding_->debugString();
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
      NIMBLE_CHECK_EQ(inputSize_, 0);
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
void ChunkedDecoder::prepareInputBuffer(int32_t size) {
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
        loadNextChunk();
        NIMBLE_CHECK_EQ(encoding_->dataType(), DataType::Bool);
      }
      const auto numValues = std::min(totalNumValues - i, remainingValues_);
      encoding_->materializeBoolsAsBits(numValues, data, i);
      advancePosition(numValues);
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
  NIMBLE_DCHECK_GE(numValues, 0);
  if (numValues == 0) {
    return;
  }

  if (streamIndex_ != nullptr) {
    skipWithIndex(numValues);
  } else {
    skipWithoutIndex(numValues);
  }
}

void ChunkedDecoder::skipWithIndex(int64_t numValues) {
  TestValue::adjust("facebook::nimble::ChunkedDecoder::skipWithIndex", this);
  NIMBLE_CHECK_GT(numValues, 0);
  // If we can skip within the current chunk, do so without seeking.
  if (numValues <= remainingValues_) {
    encoding_->skip(numValues);
    advancePosition(numValues);
    return;
  }

  const uint32_t targetRow = rowPosition_ + numValues;

  // Use streamRowCount_ to validate we're not skipping beyond the stream.
  NIMBLE_DCHECK(streamRowCount_.has_value());
  NIMBLE_CHECK_LE(
      targetRow,
      streamRowCount_.value(),
      "Cannot skip beyond end of stream in stream {}",
      streamIndex_->streamId());

  // If targetRow is exactly at the end, just reset state.
  if (targetRow == streamRowCount_.value()) {
    encoding_ = nullptr;
    remainingValues_ = 0;
    rowPosition_ = targetRow;
    // Advance input buffer to the end.
    inputData_ += inputSize_;
    inputSize_ = 0;
    // Exhaust the underlying input stream.
    input_->SkipInt64(std::numeric_limits<int64_t>::max());
    return;
  }

  // Lookup the chunk containing targetRow.
  const auto location = streamIndex_->lookupChunk(targetRow);
  NIMBLE_DCHECK(location.has_value());

  // Seek to the chunk and skip within it.
  seekToChunk(location->streamOffset);
  rowPosition_ = location->rowOffset;

  const uint32_t rowsToSkipInChunk = targetRow - rowPosition_;
  NIMBLE_DCHECK_LT(rowsToSkipInChunk, remainingValues_);
  encoding_->skip(rowsToSkipInChunk);
  advancePosition(rowsToSkipInChunk);
}

void ChunkedDecoder::skipWithoutIndex(int64_t numValues) {
  while (numValues > 0) {
    if (FOLLY_UNLIKELY(remainingValues_ == 0)) {
      loadNextChunk();
    }
    if (numValues < remainingValues_) {
      encoding_->skip(numValues);
      advancePosition(numValues);
      break;
    }
    numValues -= remainingValues_;
    rowPosition_ += remainingValues_;
    remainingValues_ = 0;
  }
}

void ChunkedDecoder::seekToChunk(uint32_t offset) {
  // Use position provider to seek to the chunk offset
  const std::vector<uint64_t> offsets{offset};
  velox::dwio::common::PositionProvider positionProvider(offsets);
  input_->seekToPosition(positionProvider);

  // Reset buffer state after seeking
  inputData_ = nullptr;
  inputSize_ = 0;
  remainingValues_ = 0;

  // Load the chunk at this position
  loadNextChunk();
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
  NIMBLE_CHECK(
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
  NIMBLE_CHECK(
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
  NIMBLE_CHECK_EQ(
      static_cast<DataType>(encoding::readChar(pos)), DataType::String);
  const auto rowCount = encoding::readUint32(pos);
  // Peel off nullable encoding.
  if (encodingType == EncodingType::Nullable) {
    encodingStart += Encoding::kPrefixSize + /*nonNullEncodingSize=*/4;
    NIMBLE_CHECK(
        const_cast<ChunkedDecoder*>(this)->ensureInputIncremental_hack(
            encodingStart + 6, pos));
    const auto nonNullsBytes = encoding::readUint32(pos);
    // TODO: it might not require an update here.
    totalSize = pos + nonNullsBytes - inputData_;
    encodingType = static_cast<EncodingType>(encoding::readChar(pos));
    NIMBLE_CHECK_EQ(
        static_cast<DataType>(encoding::readChar(pos)), DataType::String);
    NIMBLE_CHECK_LE(encoding::readUint32(pos), rowCount);
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
    NIMBLE_CHECK(ensured);
  }
  const auto dataCompressionType =
      static_cast<CompressionType>(encoding::readChar(pos));
  const auto lengthBlobSize = encoding::readUint32(pos);
  const auto blobOffset = pos + lengthBlobSize - inputData_;
  NIMBLE_CHECK_GE(totalSize, blobOffset);
  const size_t blobSize = totalSize - blobOffset;
  if (dataCompressionType == CompressionType::Uncompressed) {
    stringDataSizeEstimate_ = blobSize;
    return stringDataSizeEstimate_;
  }
  {
    const auto ensured =
        const_cast<ChunkedDecoder*>(this)->ensureInput(totalSize);
    NIMBLE_CHECK(ensured);
  }
  stringDataSizeEstimate_ = Compression::uncompressedSize(
      dataCompressionType, {inputData_ + blobOffset, blobSize});
  return stringDataSizeEstimate_;
}

} // namespace facebook::nimble
