// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <zstd.h>
#include <zstd_errors.h>
#include <optional>
#include "dwio/alpha/encodings/CompressionInternal.h"

namespace facebook::alpha {

CompressionResult compressZstd(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    DataType dataType,
    int /* bitWidth */,
    const CompressionPolicy& /* compressionPolicy */,
    const ZstdCompressionParameters& zstdParams) {
  Vector<char> buffer{&memoryPool, data.size() + sizeof(uint32_t)};
  auto pos = buffer.data();
  encoding::writeUint32(data.size(), pos);
  auto ret = ZSTD_compress(
      pos, data.size(), data.data(), data.size(), zstdParams.compressionLevel);
  if (ZSTD_isError(ret)) {
    ALPHA_ASSERT(
        ZSTD_getErrorCode(ret) == ZSTD_ErrorCode::ZSTD_error_dstSize_tooSmall,
        fmt::format(
            "Error while compressing data: {}", ZSTD_getErrorName(ret)));
    return {
        .compressionType = CompressionType::Uncompressed,
        .buffer = std::nullopt,
    };
  }

  buffer.resize(ret + sizeof(uint32_t));
  return {
      .compressionType = CompressionType::Zstd,
      .buffer = std::move(buffer),
  };
}

Vector<char> uncompressZstd(
    velox::memory::MemoryPool& memoryPool,
    CompressionType compressionType,
    std::string_view data) {
  auto pos = data.data();
  const uint32_t uncompressedSize = encoding::readUint32(pos);
  Vector<char> buffer{&memoryPool, uncompressedSize};
  auto ret = ZSTD_decompress(
      buffer.data(), buffer.size(), pos, data.size() - sizeof(uint32_t));
  ALPHA_CHECK(
      !ZSTD_isError(ret),
      fmt::format("Error uncompressing data: {}", ZSTD_getErrorName(ret)));
  return buffer;
}

} // namespace facebook::alpha
