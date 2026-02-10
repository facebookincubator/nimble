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

#include "dwio/nimble/encodings/ZstdCompressor.h"

#include <zstd.h>
#include <zstd_errors.h>
#include <optional>

namespace facebook::nimble {

CompressionResult ZstdCompressor::compress(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    DataType dataType,
    int /* bitWidth */,
    const CompressionPolicy& compressionPolicy) {
  auto parameters = compressionPolicy.compression().parameters.zstd;
  Vector<char> buffer{&memoryPool, data.size() + sizeof(uint32_t)};
  auto pos = buffer.data();
  encoding::writeUint32(data.size(), pos);
  auto ret = ZSTD_compress(
      pos, data.size(), data.data(), data.size(), parameters.compressionLevel);
  if (ZSTD_isError(ret)) {
    NIMBLE_CHECK(
        ZSTD_getErrorCode(ret) == ZSTD_ErrorCode::ZSTD_error_dstSize_tooSmall,
        "Error while compressing data: {}",
        ZSTD_getErrorName(ret));
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

Vector<char> ZstdCompressor::uncompress(
    velox::memory::MemoryPool& memoryPool,
    const CompressionType compressionType,
    const DataType /* dataType */,
    std::string_view data) {
  auto pos = data.data();
  const uint32_t uncompressedSize = encoding::readUint32(pos);
  Vector<char> buffer{&memoryPool, uncompressedSize};
  auto ret = ZSTD_decompress(
      buffer.data(), buffer.size(), pos, data.size() - sizeof(uint32_t));
  NIMBLE_CHECK(
      !ZSTD_isError(ret),
      "Error uncompressing data: {}",
      ZSTD_getErrorName(ret));
  return buffer;
}

std::optional<size_t> ZstdCompressor::uncompressedSize(
    std::string_view data) const {
  auto* pos = data.data();
  return encoding::readUint32(pos);
}

CompressionType ZstdCompressor::compressionType() {
  return CompressionType::Zstd;
}

} // namespace facebook::nimble
