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
#include "dwio/nimble/encodings/Compression.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/encodings/ZstdCompressor.h"

#ifndef DISABLE_META_INTERNAL_COMPRESSOR
#include "dwio/nimble/encodings/fb/MetaInternalCompressor.h"
#endif

namespace facebook::nimble {

namespace {

struct CompressorRegistry {
  CompressorRegistry() {
    compressors.reserve(2);
    compressors.emplace(
        CompressionType::Zstd, std::make_unique<ZstdCompressor>());
#ifndef DISABLE_META_INTERNAL_COMPRESSOR
    compressors.emplace(
        CompressionType::MetaInternal,
        std::make_unique<MetaInternalCompressor>());
#endif
  }

  std::unordered_map<CompressionType, std::unique_ptr<ICompressor>> compressors;
};

ICompressor& getCompressor(CompressionType compressionType) {
  static CompressorRegistry registry;
  auto it = registry.compressors.find(compressionType);
  NIMBLE_CHECK(
      it != registry.compressors.end(),
      fmt::format(
          "Compressor for type {} is not registered.",
          toString(compressionType)));
  return *it->second;
}
} // namespace

/* static */ CompressionResult Compression::compress(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    DataType dataType,
    int bitWidth,
    const CompressionPolicy& compressionPolicy) {
  auto compression = compressionPolicy.compression();

  return getCompressor(compression.compressionType)
      .compress(memoryPool, data, dataType, bitWidth, compressionPolicy);
}

/* static */ Vector<char> Compression::uncompress(
    velox::memory::MemoryPool& memoryPool,
    CompressionType compressionType,
    std::string_view data) {
  return getCompressor(compressionType)
      .uncompress(memoryPool, compressionType, data);
}

/* static */ std::optional<size_t> Compression::uncompressedSize(
    CompressionType compressionType,
    std::string_view data) {
  return getCompressor(compressionType).uncompressedSize(data);
}

} // namespace facebook::nimble
