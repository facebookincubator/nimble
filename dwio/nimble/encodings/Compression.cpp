// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/nimble/encodings/Compression.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/encodings/CompressionInternal.h"

namespace facebook::nimble {

/* static */ CompressionResult Compression::compress(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    DataType dataType,
    int bitWidth,
    const CompressionPolicy& compressionPolicy) {
  auto compression = compressionPolicy.compression();
  switch (compression.compressionType) {
#ifdef NIMBLE_HAS_ZSTD
    case CompressionType::Zstd:
      return compressZstd(
          memoryPool,
          data,
          dataType,
          bitWidth,
          compressionPolicy,
          compression.parameters.zstd);
#endif

#ifdef NIMBLE_HAS_ZSTRONG
    case CompressionType::Zstrong:
      return compressZstrong(
          memoryPool,
          data,
          dataType,
          bitWidth,
          compressionPolicy,
          compression.parameters.zstrong);
#endif

    default:
      break;
  }
  NIMBLE_NOT_SUPPORTED(fmt::format(
      "Unsupported compression type: {}.",
      toString(compression.compressionType)));
}

/* static */ Vector<char> Compression::uncompress(
    velox::memory::MemoryPool& memoryPool,
    CompressionType compressionType,
    std::string_view data) {
  switch (compressionType) {
    case CompressionType::Uncompressed: {
      NIMBLE_UNREACHABLE(
          "uncompress() shouldn't be called on uncompressed buffer.");
    }

#ifdef NIMBLE_HAS_ZSTD
    case CompressionType::Zstd:
      return uncompressZstd(memoryPool, compressionType, data);
#endif

#ifdef NIMBLE_HAS_ZSTRONG
    case CompressionType::Zstrong:
      return uncompressZstrong(memoryPool, compressionType, data);
#endif

    default:
      break;
  }
  NIMBLE_NOT_SUPPORTED(fmt::format(
      "Unsupported decompression type: {}.", toString(compressionType)));
}

} // namespace facebook::nimble
