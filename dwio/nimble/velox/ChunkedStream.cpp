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
#include "dwio/nimble/velox/ChunkedStream.h"
#include "dwio/nimble/common/ChunkHeader.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/compression/Compression.h"
#include "folly/io/Cursor.h"

namespace facebook::nimble {

void InMemoryChunkedStream::ensureLoaded() {
  if (!pos_) {
    stream_ = streamLoader_->getStream();
    pos_ = stream_.data();
  }
}

bool InMemoryChunkedStream::hasNext() {
  ensureLoaded();
  return pos_ - stream_.data() < stream_.size();
}

std::string_view InMemoryChunkedStream::nextChunk() {
  ensureLoaded();
  uncompressed_.reset();
  NIMBLE_CHECK_LE(
      kChunkHeaderSize,
      stream_.size() - (pos_ - stream_.data()),
      "Read beyond end of stream");
  const auto [length, compressionType] = readChunkHeader(pos_);
  NIMBLE_CHECK_LE(
      length,
      stream_.size() - (pos_ - stream_.data()),
      "Read beyond end of stream");
  std::string_view chunk;
  if (compressionType == CompressionType::Uncompressed) {
    chunk = {pos_, length};
  } else {
    // Any registered codec decodes through the registry; getCompressor()
    // rejects types that are not registered.
    uncompressed_ = Compression::uncompress(
        memoryPool_,
        compressionType,
        DataType::String,
        {pos_, length},
        /*decompressCounter=*/nullptr);
    chunk = {uncompressed_->as<char>(), uncompressed_->size()};
  }
  pos_ += length;
  return chunk;
}

CompressionType InMemoryChunkedStream::peekCompressionType() {
  ensureLoaded();
  NIMBLE_CHECK_LE(
      kChunkHeaderSize,
      stream_.size() - (pos_ - stream_.data()),
      "Read beyond end of stream");
  auto* pos = pos_ + sizeof(uint32_t);
  return static_cast<CompressionType>(encoding::readChar(pos));
}

void InMemoryChunkedStream::reset() {
  uncompressed_.reset();
  pos_ = stream_.data();
}

} // namespace facebook::nimble
