// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/ChunkedStream.h"
#include "dwio/alpha/common/EncodingPrimitives.h"
#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/tablet/Compression.h"
#include "folly/io/Cursor.h"

namespace facebook::alpha {

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
  ALPHA_ASSERT(
      sizeof(uint32_t) + sizeof(char) <=
          stream_.size() - (pos_ - stream_.data()),
      "Read beyond end of stream");
  auto length = encoding::readUint32(pos_);
  auto compressionType = static_cast<CompressionType>(encoding::readChar(pos_));
  ALPHA_ASSERT(
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
      ALPHA_UNREACHABLE(fmt::format(
          "Unexpected stream compression type: ", toString(compressionType)));
    }
  }
  pos_ += length;
  return chunk;
}

CompressionType InMemoryChunkedStream::peekCompressionType() {
  ensureLoaded();
  ALPHA_ASSERT(
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

} // namespace facebook::alpha
