// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/nimble/velox/ChunkedStreamWriter.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/tablet/Compression.h"

namespace facebook::nimble {

ChunkedStreamWriter::ChunkedStreamWriter(
    Buffer& buffer,
    CompressionParams compressionParams)
    : buffer_{buffer}, compressionParams_{compressionParams} {
  NIMBLE_CHECK(
      compressionParams_.type == CompressionType::Uncompressed ||
          compressionParams_.type == CompressionType::Zstd,
      fmt::format(
          "Unsupported chunked stream compression type: {}",
          toString(compressionParams_.type)));
}

std::vector<std::string_view> ChunkedStreamWriter::encode(
    std::string_view chunk) {
  constexpr uint8_t headerSize = sizeof(uint32_t) + sizeof(CompressionType);
  auto* header = buffer_.reserve(headerSize);
  auto* pos = header;

  if (compressionParams_.type == CompressionType::Zstd) {
    auto compressed = ZstdCompression::compress(
        buffer_.getMemoryPool(), chunk, compressionParams_.zstdLevel);
    if (compressed.has_value()) {
      encoding::writeUint32(compressed->size(), pos);
      encoding::write(CompressionType::Zstd, pos);
      return {
          {header, headerSize},
          buffer_.takeOwnership(compressed->releaseOwnership())};
    }
  }

  encoding::writeUint32(chunk.size(), pos);
  encoding::write(CompressionType::Uncompressed, pos);
  return {{header, headerSize}, chunk};
}

} // namespace facebook::nimble
