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

#include <string>
#include <vector>

#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/index/StripeIndexGroup.h"
#include "dwio/nimble/tablet/Compression.h"

namespace facebook::nimble::index::test {

/// Test utility class to decode chunks from raw chunk data.
/// Follows the same pattern as InMemoryChunkedStream::nextChunk().
/// Chunk format: uint32_t length + char compressionType + data.
class TestChunkDecoder {
 public:
  explicit TestChunkDecoder(
      velox::memory::MemoryPool& pool,
      std::string_view data)
      : pool_(pool), data_(data), pos_(data_.data()) {}

  /// Returns true if there is more data to decode.
  bool hasNext() const {
    return pos_ - data_.data() < data_.size();
  }

  /// Decodes and returns the next chunk.
  /// The returned string_view is valid until the next call to nextChunk().
  std::string_view nextChunk() {
    uncompressed_.clear();
    NIMBLE_CHECK_LE(
        sizeof(uint32_t) + sizeof(char),
        data_.size() - (pos_ - data_.data()),
        "Read beyond end of data");
    auto length = encoding::readUint32(pos_);
    auto compressionType =
        static_cast<CompressionType>(encoding::readChar(pos_));
    NIMBLE_CHECK_LE(
        length,
        data_.size() - (pos_ - data_.data()),
        "Read beyond end of data");
    std::string_view chunk;
    switch (compressionType) {
      case CompressionType::Uncompressed: {
        chunk = {pos_, length};
        break;
      }
      case CompressionType::Zstd: {
        uncompressed_ = ZstdCompression::uncompress(pool_, {pos_, length});
        chunk = {uncompressed_.data(), uncompressed_.size()};
        break;
      }
      default: {
        NIMBLE_UNREACHABLE(
            "Unexpected stream compression type: ", toString(compressionType));
      }
    }
    pos_ += length;
    return chunk;
  }

  /// Resets the decoder to the beginning of the data.
  void reset() {
    uncompressed_.clear();
    pos_ = data_.data();
  }

 private:
  velox::memory::MemoryPool& pool_;
  std::string_view data_;
  const char* pos_;
  Vector<char> uncompressed_{&pool_};
};

/// Wrapper around TestChunkDecoder that ensures exactly one chunk is decoded.
/// Use this when the data is expected to contain exactly one chunk.
class SingleChunkDecoder {
 public:
  explicit SingleChunkDecoder(
      velox::memory::MemoryPool& pool,
      std::string_view data)
      : decoder_(pool, data) {}

  /// Decodes and returns the single chunk.
  /// Asserts that exactly one chunk exists in the data.
  std::string_view decode() {
    NIMBLE_CHECK(decoder_.hasNext(), "Expected at least one chunk");
    auto chunk = decoder_.nextChunk();
    NIMBLE_CHECK(!decoder_.hasNext(), "Expected exactly one chunk");
    return chunk;
  }

 private:
  TestChunkDecoder decoder_;
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

/// Test helper class for StripeIndexGroup that provides convenient access
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

 private:
  const StripeIndexGroup* const indexGroup_;
};

} // namespace facebook::nimble::index::test
