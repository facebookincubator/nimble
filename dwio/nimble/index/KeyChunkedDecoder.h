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

#include "dwio/nimble/encodings/EncodingUtils.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::nimble {

/// KeyChunkedDecoder is a decoder specialized for decoding keys from chunks.
/// It provides chunk-level seeking and key-based positioning capabilities
/// similar to ChunkedDecoder but optimized for key seek operations.
class KeyChunkedDecoder {
 public:
  static std::unique_ptr<KeyChunkedDecoder> create(
      std::unique_ptr<velox::dwio::common::SeekableInputStream> input,
      velox::memory::MemoryPool* pool) {
    return std::unique_ptr<KeyChunkedDecoder>(
        new KeyChunkedDecoder(std::move(input), pool));
  }

  /// Seek to a key at or after the given encoded key.
  /// Returns the row position of the key found.
  /// @param encodedKey The encoded key to seek to
  /// @return The row position at or after the encoded key
  std::optional<uint32_t> seekAtOrAfter(
      uint32_t chunkOffset,
      std::string_view encodedKey,
      bool& exactMatch);

 private:
  KeyChunkedDecoder(
      std::unique_ptr<velox::dwio::common::SeekableInputStream> input,
      velox::memory::MemoryPool* pool)
      : input_{std::move(input)}, pool_{pool} {
    NIMBLE_CHECK_NOT_NULL(input_);
    NIMBLE_CHECK_NOT_NULL(pool_);
  }

  void seekToChunk(uint32_t chunkOffset);

  // Ensures that the input buffer has at least 'size' bytes available for
  // reading. Returns true if successful, false if end of stream is reached.
  bool ensureInput(int size);

  // Allocates and prepares the input buffer.
  void prepareInputBuffer(int32_t size);

  // Loads the next chunk from the input stream.
  void loadChunk();

  const std::unique_ptr<velox::dwio::common::SeekableInputStream> input_;
  velox::memory::MemoryPool* const pool_;

  const char* inputData_{nullptr};
  int64_t inputSize_{0};
  velox::BufferPtr inputBuffer_;

  uint32_t chunkOffset_{0};
  std::unique_ptr<Encoding> encoding_;
};

} // namespace facebook::nimble
