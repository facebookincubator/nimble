// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/encodings/Compression.h"
#include "dwio/alpha/encodings/EncodingSelection.h"

namespace facebook::alpha {

CompressionResult compressZstd(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    DataType dataType,
    int bitWidth,
    const CompressionPolicy& compressionPolicy,
    const ZstdCompressionParameters& zstdParams);

CompressionResult compressZstrong(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    DataType dataType,
    int bitWidth,
    const CompressionPolicy& compressionPolicy,
    const ZstrongCompressionParameters& zstrongParams);

Vector<char> uncompressZstd(
    velox::memory::MemoryPool& memoryPool,
    CompressionType compressionType,
    std::string_view data);

Vector<char> uncompressZstrong(
    velox::memory::MemoryPool& memoryPool,
    CompressionType compressionType,
    std::string_view data);

}; // namespace facebook::alpha
