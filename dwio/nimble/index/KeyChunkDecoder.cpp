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
#include "dwio/nimble/index/KeyChunkDecoder.h"

#include "dwio/nimble/common/ChunkHeader.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"

namespace facebook::nimble::index {

std::shared_ptr<DecodedKeyChunk> decodeKeyChunk(
    std::unique_ptr<velox::dwio::common::SeekableInputStream> inputStream,
    velox::memory::MemoryPool& pool,
    velox::BufferPtr& dataBuffer) {
  const void* buf;
  int bufLen{0};
  NIMBLE_CHECK(inputStream->Next(&buf, &bufLen));
  NIMBLE_CHECK_GE(bufLen, kChunkHeaderSize);
  const auto* header = static_cast<const char*>(buf);
  const auto chunkHeader = readChunkHeader(header);
  NIMBLE_CHECK_EQ(
      chunkHeader.compressionType,
      CompressionType::Uncompressed,
      "Compressed key chunks are not supported");

  const auto dataLen = chunkHeader.length;
  auto result = std::make_shared<DecodedKeyChunk>();

  // Determine the contiguous data pointer. If the first buffer contains all
  // the data, use it directly (zero-copy). Otherwise, copy across buffers.
  // readChunkHeader advanced 'header' past the chunk header, so the remaining
  // available bytes are bufLen - kChunkHeaderSize.
  const char* chunkData;
  bufLen -= kChunkHeaderSize;
  if (bufLen >= static_cast<int>(dataLen)) {
    result->dataStream = std::move(inputStream);
    chunkData = header;
  } else {
    // Reuse the caller's buffer when it has sufficient capacity; otherwise
    // allocate fresh and write it back into the slot. The destination buffer
    // is appended to stringBuffers so the returned DecodedKeyChunk holds its
    // own reference (co-owned with the caller's slot).
    if (dataBuffer == nullptr || dataBuffer->capacity() < dataLen) {
      dataBuffer = velox::AlignedBuffer::allocate<char>(dataLen, &pool);
    }
    dataBuffer->setSize(dataLen);
    auto* dest = dataBuffer->asMutable<char>();
    std::memcpy(dest, header, bufLen);
    int copied = bufLen;
    while (copied < static_cast<int>(dataLen)) {
      NIMBLE_CHECK(inputStream->Next(&buf, &bufLen));
      const int toCopy = std::min(bufLen, static_cast<int>(dataLen) - copied);
      std::memcpy(dest + copied, buf, toCopy);
      copied += toCopy;
    }
    chunkData = dest;
    result->stringBuffers.push_back(dataBuffer);
  }

  auto* raw = result.get();
  result->encoding = nimble::EncodingFactory().create(
      pool,
      std::string_view(chunkData, dataLen),
      [raw, &pool](uint32_t totalLength) {
        auto& buffer = raw->stringBuffers.emplace_back(
            velox::AlignedBuffer::allocate<char>(totalLength, &pool));
        return buffer->asMutable<void>();
      });
  NIMBLE_CHECK(
      result->encoding->encodingType() == EncodingType::Trivial ||
          result->encoding->encodingType() == EncodingType::Prefix,
      "Unsupported encoding type: {}",
      result->encoding->encodingType());
  return result;
}

} // namespace facebook::nimble::index
