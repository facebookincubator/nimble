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
  
#ifdef DISABLE_META_INTERNAL_COMPRESSOR
  // When MetaInternal is not available, redirect to Zstd
  if (compressionType == CompressionType::MetaInternal) {
    compressionType = CompressionType::Zstd;
  }
#endif
  
  auto it = registry.compressors.find(compressionType);
  NIMBLE_CHECK(
      it != registry.compressors.end(),
      "Compressor for type {} is not registered.",
      toString(compressionType));
  return *it->second;
}

#ifdef DISABLE_META_INTERNAL_COMPRESSOR
// Wrapper to redirect MetaInternal compression to Zstd
class MetaInternalToZstdPolicy : public CompressionPolicy {
 public:
  explicit MetaInternalToZstdPolicy(const CompressionPolicy& base)
      : base_(base) {}

  CompressionInformation compression() const override {
    auto info = base_.compression();
    if (info.compressionType == CompressionType::MetaInternal) {
      // Convert MetaInternal parameters to Zstd
      CompressionInformation zstdInfo{
          .compressionType = CompressionType::Zstd,
          .minCompressionSize = info.minCompressionSize};
      // Map MetaInternal compression level to Zstd compression level
      zstdInfo.parameters.zstd.compressionLevel = 
          info.parameters.metaInternal.compressionLevel;
      return zstdInfo;
    }
    return info;
  }

  bool shouldAccept(
      CompressionType compressionType,
      uint64_t uncompressedSize,
      uint64_t compressedSize) const override {
    // Redirect MetaInternal to Zstd for acceptance check
    if (compressionType == CompressionType::MetaInternal) {
      compressionType = CompressionType::Zstd;
    }
    return base_.shouldAccept(compressionType, uncompressedSize, compressedSize);
  }

 private:
  const CompressionPolicy& base_;
};
#endif

} // namespace

/* static */ CompressionResult Compression::compress(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    DataType dataType,
    int bitWidth,
    const CompressionPolicy& compressionPolicy) {
  auto compression = compressionPolicy.compression();

#ifdef DISABLE_META_INTERNAL_COMPRESSOR
  // Wrap the policy to redirect MetaInternal to Zstd
  if (compression.compressionType == CompressionType::MetaInternal) {
    MetaInternalToZstdPolicy wrapper(compressionPolicy);
    compression = wrapper.compression();
    return getCompressor(compression.compressionType)
        .compress(memoryPool, data, dataType, bitWidth, wrapper);
  }
#endif

  return getCompressor(compression.compressionType)
      .compress(memoryPool, data, dataType, bitWidth, compressionPolicy);
}

/* static */ Vector<char> Compression::uncompress(
    velox::memory::MemoryPool& memoryPool,
    CompressionType compressionType,
    DataType dataType,
    std::string_view data) {
  return getCompressor(compressionType)
      .uncompress(memoryPool, compressionType, dataType, data);
}

/* static */ std::optional<size_t> Compression::uncompressedSize(
    CompressionType compressionType,
    std::string_view data) {
  return getCompressor(compressionType).uncompressedSize(data);
}

} // namespace facebook::nimble
