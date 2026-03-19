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
#include "dwio/nimble/velox/ChunkedStreamWriter.h"
#include "dwio/nimble/common/ChunkHeader.h"
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
  auto* header = buffer_.reserve(kChunkHeaderSize);
  auto* pos = header;

  if (compressionParams_.type == CompressionType::Zstd) {
    auto compressed = ZstdCompression::compress(
        buffer_.getMemoryPool(), chunk, compressionParams_.zstdLevel);
    if (compressed.has_value()) {
      writeChunkHeader(compressed->size(), CompressionType::Zstd, pos);
      return {
          {header, kChunkHeaderSize},
          buffer_.takeOwnership(compressed->releaseOwnership())};
    }
  }

  writeChunkHeader(chunk.size(), CompressionType::Uncompressed, pos);
  return {{header, kChunkHeaderSize}, chunk};
}

} // namespace facebook::nimble
