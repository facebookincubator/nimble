/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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
