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

#include <cstdint>
#include <memory>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/index/IndexTypes.h"

namespace facebook::nimble {
class MetadataBuffer;
} // namespace facebook::nimble

namespace facebook::nimble::index {

namespace test {
class ChunkIndexTestHelper;
} // namespace test

class StreamIndex;

/// ChunkIndexGroup provides O(1) chunk-level seeking by row ID within stripes.
/// It reads from the standalone ChunkIndex flatbuffer stored in the
/// "chunk_index" optional section.
///
/// Each ChunkIndexGroup instance corresponds to one stripe group and contains
/// chunk-level position data for all streams across all stripes in that group.
class ChunkIndexGroup : public std::enable_shared_from_this<ChunkIndexGroup> {
 public:
  /// Creates a ChunkIndexGroup from a decompressed ChunkIndex flatbuffer.
  ///
  /// @param firstStripe First stripe index in this stripe group.
  /// @param stripeCount Number of stripes in this stripe group.
  /// @param metadata Decompressed ChunkIndex flatbuffer data.
  static std::shared_ptr<ChunkIndexGroup> create(
      uint32_t firstStripe,
      uint32_t stripeCount,
      std::unique_ptr<MetadataBuffer> metadata);

  /// Creates a StreamIndex for the specified stripe and stream ID.
  /// Returns nullptr if the stream is not indexed (has ≤1 chunk).
  std::shared_ptr<StreamIndex> createStreamIndex(
      uint32_t stripe,
      uint32_t streamId) const;

 private:
  ChunkIndexGroup(
      uint32_t firstStripe,
      uint32_t stripeCount,
      std::unique_ptr<MetadataBuffer> metadata);

  inline uint32_t stripeOffset(uint32_t stripe) const {
    NIMBLE_CHECK_GE(
        stripe, firstStripe_, "Stripe index is before this group's range");
    const uint32_t offset = stripe - firstStripe_;
    NIMBLE_CHECK_LT(
        offset,
        stripeCount_,
        "Stripe offset is out of range for this chunk index group");
    return offset;
  }

  const std::unique_ptr<MetadataBuffer> metadata_;
  const uint32_t firstStripe_;
  const uint32_t stripeCount_;
  // Total number of streams indexed in this stripe group.
  const uint32_t streamCount_;

  friend class StreamIndex;
  friend class test::ChunkIndexTestHelper;
};

/// StreamIndex provides O(1) chunk-level seeking for a specific stream within
/// a stripe. Precomputes the chunk range at construction to avoid repeated
/// stream/stripe resolution on each lookup.
class StreamIndex {
 public:
  static std::shared_ptr<StreamIndex> create(
      std::shared_ptr<const ChunkIndexGroup> chunkIndex,
      uint32_t streamId,
      uint32_t startChunkOffset,
      uint32_t endChunkOffset);

  /// Lookup chunk by row ID within the stream's row range.
  ChunkLocation lookupChunk(uint32_t rowId) const;

  /// Returns the total number of rows in this stream.
  uint32_t rowCount() const;

  /// Returns the stream ID this index is for.
  uint32_t streamId() const {
    return streamId_;
  }

 private:
  StreamIndex(
      std::shared_ptr<const ChunkIndexGroup> chunkIndex,
      uint32_t streamId,
      uint32_t startChunkOffset,
      uint32_t endChunkOffset);

  // Kept alive to ensure metadata_ stays valid.
  const std::shared_ptr<const ChunkIndexGroup> chunkIndex_;
  const uint32_t streamId_;
  // Precomputed chunk range [startChunkOffset_, endChunkOffset_) into the
  // flattened chunk_rows/chunk_offsets arrays.
  const uint32_t startChunkOffset_;
  const uint32_t endChunkOffset_;
};

} // namespace facebook::nimble::index
