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
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/tablet/Compression.h"
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
  uncompressed_.clear();
  NIMBLE_ASSERT(
      sizeof(uint32_t) + sizeof(char) <=
          stream_.size() - (pos_ - stream_.data()),
      "Read beyond end of stream");
  auto length = encoding::readUint32(pos_);
  auto compressionType = static_cast<CompressionType>(encoding::readChar(pos_));
  NIMBLE_ASSERT(
      length <= stream_.size() - (pos_ - stream_.data()),
      "Read beyond end of stream");
  std::string_view chunk;
  switch (compressionType) {
    case CompressionType::Uncompressed: {
      chunk = {pos_, length};
      break;
    }
    case CompressionType::Zstd: {
      uncompressed_ = ZstdCompression::uncompress(
          *uncompressed_.memoryPool(), {pos_, length});
      chunk = {uncompressed_.data(), uncompressed_.size()};
      break;
    }
    default: {
      NIMBLE_UNREACHABLE(
          fmt::format(
              "Unexpected stream compression type: ",
              toString(compressionType)));
    }
  }
  pos_ += length;
  return chunk;
}

CompressionType InMemoryChunkedStream::peekCompressionType() {
  ensureLoaded();
  NIMBLE_ASSERT(
      sizeof(uint32_t) + sizeof(char) <=
          stream_.size() - (pos_ - stream_.data()),
      "Read beyond end of stream");
  auto pos = pos_ + sizeof(uint32_t);
  return static_cast<CompressionType>(encoding::readChar(pos));
}

void InMemoryChunkedStream::reset() {
  uncompressed_.clear();
  pos_ = stream_.data();
}

} // namespace facebook::nimble
