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
#include <string>
#include <vector>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/index/StripeIndexGroup.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/tablet/TabletWriter.h"
#include "dwio/nimble/velox/ChunkedStream.h"

namespace facebook::nimble::index::test {

/// Simple StreamLoader implementation for testing that holds data in memory.
class TestStreamLoader : public StreamLoader {
 public:
  explicit TestStreamLoader(std::string_view data) : data_(data) {}

  const std::string_view getStream() const override {
    return data_;
  }

 private:
  std::string_view data_;
};

/// Wrapper around InMemoryChunkedStream that ensures exactly one chunk is
/// decoded. Use this when the data is expected to contain exactly one chunk.
class SingleChunkDecoder {
 public:
  explicit SingleChunkDecoder(
      velox::memory::MemoryPool& pool,
      std::string_view data)
      : stream_(pool, std::make_unique<TestStreamLoader>(data)) {}

  /// Decodes and returns the single chunk.
  /// Asserts that exactly one chunk exists in the data.
  std::string_view decode() {
    NIMBLE_CHECK(stream_.hasNext(), "Expected at least one chunk");
    auto chunk = stream_.nextChunk();
    NIMBLE_CHECK(!stream_.hasNext(), "Expected exactly one chunk");
    return chunk;
  }

 private:
  InMemoryChunkedStream stream_;
};

/// Holds key stream statistics extracted from a StripeIndexGroup for testing.
struct KeyStreamStats {
  /// Byte offset of key stream for each stripe.
  std::vector<uint32_t> offsets;
  /// Byte size of key stream for each stripe.
  std::vector<uint32_t> sizes;
  /// Accumulated chunk counts per stripe.
  std::vector<uint32_t> chunkCounts;
  /// Accumulated row counts per chunk.
  std::vector<uint32_t> chunkRows;
  /// Byte offset of each chunk within its key stream.
  std::vector<uint32_t> chunkOffsets;
  /// Last key value for each chunk.
  std::vector<std::string> chunkKeys;
};

/// Holds stream position index statistics for a specific stream.
struct StreamStats {
  /// Accumulated chunk count for each stripe for this stream.
  std::vector<uint32_t> chunkCounts;
  /// Accumulated row counts per chunk for this stream.
  std::vector<uint32_t> chunkRows;
  /// Byte offset of each chunk within its stream.
  std::vector<uint32_t> chunkOffsets;
};

/// Specification for a single chunk in a stream (for test data creation).
struct ChunkSpec {
  uint32_t rowCount;
  uint32_t size;
};

/// Specification for a single stream (for test data creation).
struct StreamSpec {
  uint32_t offset;
  std::vector<ChunkSpec> chunks;
};

/// Specification for a key chunk (for test data creation).
struct KeyChunkSpec {
  uint32_t rowCount;
  std::string firstKey;
  std::string lastKey;
};

/// Creates a KeyStream with test data from the given chunk specifications.
/// The buffer is used to store the chunk content and key data.
KeyStream createKeyStream(
    Buffer& buffer,
    const std::vector<KeyChunkSpec>& chunkSpecs);

/// Creates chunks for a stream from the given chunk specifications.
/// The buffer is used to store the chunk content.
std::vector<Chunk> createChunks(
    Buffer& buffer,
    const std::vector<ChunkSpec>& chunkSpecs);

/// Creates a Stream with the specified test parameters.
/// The buffer is used to store the chunk content.
Stream createStream(Buffer& buffer, const StreamSpec& spec);

/// Test helper class for StripeIndexGroup
/// to private members for testing purposes. This is a friend class of
/// StripeIndexGroup.
class StripeIndexGroupTestHelper {
 public:
  explicit StripeIndexGroupTestHelper(const StripeIndexGroup* indexGroup)
      : indexGroup_(indexGroup) {}

  /// Returns the group index.
  uint32_t groupIndex() const {
    return indexGroup_->groupIndex_;
  }

  /// Returns the first stripe index in this group.
  uint32_t firstStripe() const {
    return indexGroup_->firstStripe_;
  }

  /// Returns the number of stripes in this group.
  uint32_t stripeCount() const {
    return indexGroup_->stripeCount_;
  }

  /// Returns the number of streams per stripe.
  uint32_t streamCount() const {
    return indexGroup_->streamCount_;
  }

  /// Returns the key stream statistics for testing verification.
  KeyStreamStats keyStreamStats() const;

  /// Returns the stream position index statistics for the specified stream.
  StreamStats streamStats(uint32_t streamId) const;

  /// Returns the length of a key chunk given its stream offset within a stripe.
  /// Uses the next chunk offset (if exists) or stream length for the last
  /// chunk.
  uint32_t keyChunkLength(
      uint32_t stripeIndex,
      uint32_t chunkStreamOffset,
      uint32_t keyStreamLength) const;

 private:
  const StripeIndexGroup* const indexGroup_;
};

} // namespace facebook::nimble::index::test
