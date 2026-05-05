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
#pragma once

#include <memory>
#include <vector>

#include "dwio/nimble/encodings/Encoding.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::nimble::index {

/// Decoded key chunk with ownership of backing buffers.
struct DecodedKeyChunk {
  // Zero-copy path: stream kept alive so its buffer backs the encoding.
  std::unique_ptr<velox::dwio::common::SeekableInputStream> dataStream;
  std::unique_ptr<nimble::Encoding> encoding;
  std::vector<velox::BufferPtr> stringBuffers;
};

/// Decodes a key chunk from an input stream. Reads the chunk header, validates
/// compression and encoding type, and creates the encoding. Handles both
/// contiguous (zero-copy) and multi-buffer reads.
///
/// Returns a shared_ptr so the encoding's string allocator lambda (which
/// captures a pointer to the DecodedKeyChunk) remains valid when the caller
/// moves or reassigns the shared_ptr.
///
/// @param dataBuffer Reusable buffer for multi-buffer reads. When the data
///        spans multiple stream buffers, this buffer is resized and reused
///        instead of allocating a new one. The caller retains ownership and the
///        buffer must outlive the returned DecodedKeyChunk.
std::shared_ptr<DecodedKeyChunk> decodeKeyChunk(
    std::unique_ptr<velox::dwio::common::SeekableInputStream> inputStream,
    velox::memory::MemoryPool& pool,
    velox::BufferPtr& dataBuffer);

} // namespace facebook::nimble::index
