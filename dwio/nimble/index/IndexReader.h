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
#include <string_view>

#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/index/StripeIndexGroup.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::nimble::index {

/// Reader for cluster index keys within a stripe.
///
/// The IndexReader provides access to sorted keys stored in a special key
/// stream at kKeyStreamId. It works in conjunction with StripeIndexGroup to
/// locate and decode key data for efficient row filtering based on cluster
/// index lookups.
///
/// The key stream contains keys organized in chunks, where each chunk is
/// independently decodable. The IndexReader performs binary search within
/// chunks to find keys efficiently.
///
/// TODO: Integrate with velox data cache for improved read performance.
class IndexReader {
 public:
  /// Static factory method to create a IndexReader instance
  static std::unique_ptr<IndexReader> create(
      std::unique_ptr<velox::dwio::common::SeekableInputStream> input,
      uint32_t stripeIndex,
      std::shared_ptr<StripeIndexGroup> indexGroup,
      bool noDuplicateKey,
      velox::memory::MemoryPool* pool);

  /// Seek to the position at or after the given encoded key.
  /// Returns the row position if found, or std::nullopt if the key is beyond
  /// the range (i.e., greater than all stripe keys).
  std::optional<uint32_t> seekAtOrAfter(std::string_view encodedKey);

 private:
  IndexReader(
      std::unique_ptr<velox::dwio::common::SeekableInputStream> input,
      uint32_t stripeIndex,
      std::shared_ptr<StripeIndexGroup> indexGroup,
      bool noDuplicateKey,
      velox::memory::MemoryPool* pool);

  /// Seek to a key at or after the given encoded key within a chunk.
  /// Returns the row position of the key found relative to the chunk start.
  std::optional<uint32_t> seekAtOrAfterInChunk(
      uint32_t chunkOffset,
      std::string_view encodedKey);

  void seekToChunk(uint32_t chunkOffset);

  /// Ensures that the input buffer has at least 'size' bytes available for
  /// reading. Returns true if successful, false if end of stream is reached.
  bool ensureInput(int size);

  /// Allocates and prepares the input buffer.
  void prepareInputBuffer(int32_t size);

  /// Loads the next chunk from the input stream.
  void loadChunk();

  const uint32_t stripeIndex_;
  const std::shared_ptr<StripeIndexGroup> indexGroup_;
  const bool noDuplicateKey_;

  const std::unique_ptr<velox::dwio::common::SeekableInputStream> input_;
  velox::memory::MemoryPool* const pool_;

  const char* inputData_{nullptr};
  int64_t inputSize_{0};
  velox::BufferPtr inputBuffer_;

  // The current chunk offset in the key stream. Used to avoid re-seeking if
  // the same chunk is accessed multiple times.
  uint32_t chunkOffset_{0};
  // The encoding for the current chunk. Reset when seeking to a new chunk.
  std::unique_ptr<nimble::Encoding> encoding_;
  // Buffers holding string data for the current encoding
  std::vector<velox::BufferPtr> stringBuffers_;
};

} // namespace facebook::nimble::index
