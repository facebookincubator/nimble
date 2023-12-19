// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/tablet/StreamInput.h"
#include "dwio/alpha/common/EncodingPrimitives.h"
#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/tablet/Compression.h"
#include "folly/io/Cursor.h"

namespace facebook::alpha {

bool InMemoryStreamInput::hasNext() const {
  return pos_ - data_.data() < data_.size();
}

std::string_view InMemoryStreamInput::nextChunk() {
  uncompressed_.clear();
  ALPHA_ASSERT(
      sizeof(uint32_t) + sizeof(char) <= data_.size() - (pos_ - data_.data()),
      "Read beyond end of stream");
  auto length = encoding::readUint32(pos_);
  auto compressionType = static_cast<CompressionType>(encoding::readChar(pos_));
  ALPHA_ASSERT(
      length <= data_.size() - (pos_ - data_.data()),
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

CompressionType InMemoryStreamInput::peekCompressionType() const {
  ALPHA_ASSERT(
      sizeof(uint32_t) + sizeof(char) <= data_.size() - (pos_ - data_.data()),
      "Read beyond end of stream");
  auto pos = pos_ + sizeof(uint32_t);
  return static_cast<CompressionType>(encoding::readChar(pos));
}

void InMemoryStreamInput::reset() {
  uncompressed_.clear();
  pos_ = data_.data();
}

void InMemoryStreamInput::extract(
    std::function<void(std::string_view)> consumer) const {
  consumer({data_.data(), data_.size()});
}

Vector<char> InMemoryStreamInput::toVector(
    velox::memory::MemoryPool& memoryPool,
    folly::IOBuf input) {
  Vector<char> result{&memoryPool, input.computeChainDataLength()};
  folly::io::Cursor cursor(&input);
  cursor.pull(result.data(), result.size());
  return result;
}

} // namespace facebook::alpha
