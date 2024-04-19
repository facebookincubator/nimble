// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <zstd.h>
#include <zstd_errors.h>
#include <optional>

#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/tablet/Compression.h"

namespace facebook::nimble {

std::optional<Vector<char>> ZstdCompression::compress(
    velox::memory::MemoryPool& memoryPool,
    std::string_view source,
    int32_t level) {
  Vector<char> buffer{&memoryPool, source.size() + sizeof(uint32_t)};
  auto pos = buffer.data();
  encoding::writeUint32(source.size(), pos);
  auto ret = ZSTD_compress(
      pos,
      source.size() - sizeof(uint32_t),
      source.data(),
      source.size(),
      level);
  if (ZSTD_isError(ret)) {
    NIMBLE_ASSERT(
        ZSTD_getErrorCode(ret) == ZSTD_ErrorCode::ZSTD_error_dstSize_tooSmall,
        fmt::format(
            "Error while compressing data: {}", ZSTD_getErrorName(ret)));
    return std::nullopt;
  }

  buffer.resize(ret + sizeof(uint32_t));
  return {buffer};
}

Vector<char> ZstdCompression::uncompress(
    velox::memory::MemoryPool& memoryPool,
    std::string_view source) {
  auto pos = source.data();
  const uint32_t uncompressedSize = encoding::readUint32(pos);
  Vector<char> buffer{&memoryPool, uncompressedSize};
  auto ret = ZSTD_decompress(
      buffer.data(), buffer.size(), pos, source.size() - sizeof(uint32_t));
  NIMBLE_CHECK(
      !ZSTD_isError(ret),
      fmt::format("Error uncompressing data: {}", ZSTD_getErrorName(ret)));
  return buffer;
}

} // namespace facebook::nimble
