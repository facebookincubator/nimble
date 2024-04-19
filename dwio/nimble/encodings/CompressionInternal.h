// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/nimble/encodings/Compression.h"
#include "dwio/nimble/encodings/EncodingSelection.h"

namespace facebook::nimble {

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

} // namespace facebook::nimble
