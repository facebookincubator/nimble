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

#include <functional>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "dwio/nimble/index/IndexLookup.h"
#include "dwio/nimble/index/IndexTypes.h"
#include "dwio/nimble/index/KeyChunkDecoder.h"
#include "dwio/nimble/index/SortOrder.h"
#include "dwio/nimble/tablet/Callbacks.h"
#include "dwio/nimble/tablet/ClusterIndexGenerated.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "dwio/nimble/tablet/MetadataCache.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/file/Region.h"

namespace facebook::nimble {
class Encoding;
} // namespace facebook::nimble

namespace facebook::velox::dwio::common {
class SeekableInputStream;
} // namespace facebook::velox::dwio::common

namespace facebook::nimble::index {

namespace test {
class ClusterIndexTestHelper;
} // namespace test

/// ClusterIndex is a value index that maps encoded keys to file-level row
/// locations. It provides key-to-row-ID mapping with chunk-level granularity
/// using only FlatBuffer metadata (no I/O for the common path).
///
/// The index uses partition keys at the root level for binary search across
/// all partitions, then chunk keys within each partition for finer-grained
/// lookup. When a key stream loader is provided, lookup returns exact row
/// positions by decoding key stream data within chunks.
///
/// NOTE: Not thread-safe. Callers must provide external synchronization.
///
/// Example usage:
///   auto index = ClusterIndex::create(std::move(rootSection), ...);
///   auto locations = index->lookup(encodedKey);
///   // locations[0].range is a file-level [startRow, endRow) range
///
///   // With row range constraint (e.g., within a stripe):
///   auto locations = index->lookup(encodedKey,
///       {.rowRange = RowRange(stripeStart, stripeEnd)});
///
class ClusterIndex : public IndexLookup {
 public:
  ~ClusterIndex() override;

  /// @param rootSection Root index section from the file footer.
  /// @param loadMetadata Callback to load partition metadata on demand.
  /// @param loadData Optional callback for row-level precision.
  ///        When null, lookup() returns chunk-level precision.
  /// @param pool Memory pool. Required when loadData is provided.
  static std::unique_ptr<ClusterIndex> create(
      Section rootSection,
      LoadMetadataFn loadMetadata,
      LoadDataFn loadData,
      velox::memory::MemoryPool* pool);

  const std::vector<std::string>& indexColumns() const override {
    return indexColumns_;
  }

  LookupResult lookup(const LookupRequest& request) const override;

  // -- ClusterIndex-specific methods --

  /// Returns the minimum key across all partitions.
  std::string_view minKey() const override {
    return firstKey_;
  }

  /// Returns the maximum key across all partitions.
  std::string_view maxKey() const override {
    return lastKey_;
  }

  /// Returns the number of partitions in the index.
  uint32_t numPartitions() const {
    return numPartitions_;
  }

  /// Returns the sort orders for each index column.
  const std::vector<SortOrder>& sortOrders() const {
    return sortOrders_;
  }

  /// File-level index layout for diagnostic output (nimble_dump, FileLayout).
  struct Layout {
    /// Per-partition detail. Only populated when layout(detail=true).
    struct Partition {
      /// Location and compression of partition metadata in the file.
      MetadataSection metadataSection;
      /// File region containing the encoded key data blob.
      /// Only populated when detail=true.
      velox::common::Region keyStreamRegion;
      /// Number of key chunks in this partition.
      uint32_t numChunks{0};
      /// Total rows in this partition.
      uint32_t numRows{0};
      /// Size of the partition metadata FlatBuffer in bytes.
      uint32_t metadataSizeBytes{0};
    };

    std::vector<std::string> indexColumns;
    std::vector<SortOrder> sortOrders;
    uint32_t numPartitions{0};

    /// Per-partition details. Empty when detail=false.
    std::vector<Partition> partitions;
  };

  /// Returns the metadata section for a specific partition (from root
  /// FlatBuffer, no I/O).
  MetadataSection partitionSection(uint32_t partitionId) const;

  /// Returns the index layout for diagnostic consumers.
  /// @param detail When true, loads per-partition metadata (requires I/O).
  ///        When false, returns only root-level metadata (no I/O).
  Layout layout(bool detail = true) const;

 private:
  ClusterIndex(
      Section rootSection,
      LoadMetadataFn loadMetadata,
      LoadDataFn loadData,
      velox::memory::MemoryPool* pool);

  // Cached decoded chunk, one per IndexPartition.
  struct DecodedChunk {
    void reset(uint32_t newChunkOffset) {
      chunkOffset = newChunkOffset;
      data = {};
    }

    uint32_t chunkOffset{0};
    DecodedKeyChunk data;
  };

  // Parsed partition metadata, cached on demand.
  struct IndexPartition {
    const uint32_t id;
    const std::unique_ptr<MetadataBuffer> metadata;
    const serialization::ClusterIndexPartition* const index;

    // One cached decoded chunk. Replaced when a different chunk is requested.
    mutable DecodedChunk decodedChunk;
    // Reusable buffer for chunk data that spans multiple stream buffers.
    mutable velox::BufferPtr dataBuffer;

    IndexPartition(uint32_t id, std::unique_ptr<MetadataBuffer> metadata);

    uint32_t numChunks() const {
      return index->chunk_keys()->size();
    }

    // Returns the chunk index for the given encoded key via binary search.
    // The key must be within the partition's chunk range.
    uint32_t chunkIndex(std::string_view encodedKey) const;

    uint32_t chunkOffset(uint32_t chunkIdx) const {
      return index->chunk_offsets()->Get(chunkIdx);
    }

    uint32_t chunkSize(uint32_t chunkIdx) const {
      const uint32_t offset = chunkOffset(chunkIdx);
      const uint32_t nextOffset = chunkIdx + 1 < numChunks()
          ? chunkOffset(chunkIdx + 1)
          : index->key_stream_size();
      return nextOffset - offset;
    }

    uint32_t rowOffset(uint32_t chunkIdx) const {
      return chunkIdx == 0 ? 0 : index->chunk_rows()->Get(chunkIdx - 1);
    }

    velox::common::Region chunkStreamRegion(
        const ChunkLocation& chunkLocation) const;

    // Ensures dataBuffer has at least the requested capacity.
    // Only grows, never shrinks, to avoid repeated allocations.
    char* ensureDataBuffer(uint32_t bytes, velox::memory::MemoryPool* pool)
        const;
  };

  // Resolves an encoded key to a file-level row number within a partition.
  // The partition's last key must be >= encodedKey (guaranteed by the caller's
  // binary search on partition keys).
  // @param inclusive true for first row >= key, false for first row > key.
  uint32_t resolvePartitionRow(
      const IndexPartition* partition,
      std::string_view encodedKey,
      bool inclusive) const;

  // Finds the partition index for a key via binary search on partition keys.
  // Returns nullopt if the key is beyond all partitions.
  std::optional<uint32_t> lookupPartition(std::string_view key) const;

  // A bound to resolve within a partition.
  // Stores a pointer to the target row field (startRow or endRow) in the
  // caller's RowRange. resolvePartitionBounds writes the resolved file-level
  // row directly through this pointer.
  struct PartitionLookup {
    uint32_t partitionId;
    std::string_view encodedKey;
    // true for lower bound (first row >= key), false for upper bound
    // (first row > key).
    bool inclusive;
    uint32_t* targetRow;

    PartitionLookup(
        uint32_t _partitionId,
        std::string_view _encodedKey,
        bool _inclusive,
        uint32_t* _targetRow)
        : partitionId{_partitionId},
          encodedKey{_encodedKey},
          inclusive{_inclusive},
          targetRow{_targetRow} {
      NIMBLE_CHECK_NOT_NULL(targetRow);
    }
  };

  // Builds partition lookups from request into partitionLookups_
  // and initializes scratchRanges_.
  void lookupPartitions(const LookupRequest& request) const;
  void partitionPointLookups(const LookupRequest& request) const;
  void partitionRangeLookups(const LookupRequest& request) const;

  // Resolves all partition lookups grouped by partition.
  void resolvePartitionBounds() const;

  // Builds the flat LookupResult from scratchRanges_.
  LookupResult buildLookupResult(const LookupOptions& options) const;

  // Binary searches chunk keys in the partition.
  ChunkLocation lookupChunk(
      const IndexPartition* partition,
      std::string_view encodedKey) const;

  // Loads or returns cached decoded chunk for the given location.
  // Reuses partition-level buffers to avoid repeated allocations.
  const DecodedChunk& getDecodedChunk(
      const IndexPartition* partition,
      const ChunkLocation& chunkLocation) const;

  // Seeks within a chunk's decoded encoding.
  // @param inclusive true: first key >= encodedKey. false: first key >
  //        encodedKey.
  // Returns the partition-level row offset.
  uint32_t seekInChunk(
      const IndexPartition* partition,
      const ChunkLocation& chunkLocation,
      std::string_view encodedKey,
      bool inclusive) const;

  // Returns the partition, loading on demand if needed. Never evicted.
  const IndexPartition* loadPartition(uint32_t partitionId) const;

  const Section rootSection_;
  const serialization::ClusterIndex* const indexRoot_;
  const uint32_t numPartitions_;
  const std::vector<std::string> indexColumns_;
  const std::vector<SortOrder> sortOrders_;

  // File-level key range for early pruning.
  const std::string_view firstKey_;
  const std::string_view lastKey_;

  // Last key of each partition for binary search.
  // partitionKeys_[i] is the last key of partition i.
  const std::vector<std::string_view> partitionKeys_;

  // Cumulative row offsets per partition (prefix sum).
  // partitionRows_[i] is the start row of partition i.
  // partitionRows_[numPartitions_] == numRows_.
  // Size = numPartitions_ + 1.
  const std::vector<uint32_t> partitionRows_;

  // Total number of rows in the file.
  const uint32_t numRows_;

  const LoadDataFn loadData_;
  const LoadMetadataFn loadMetadata_;

  /// Memory pool for allocations.
  velox::memory::MemoryPool* const pool_;

  // --- Mutable state ---

  // Partitions loaded on demand and never evicted. Accessed by raw pointer.
  mutable std::vector<std::unique_ptr<IndexPartition>> partitions_;

  // Reusable scratch buffers for lookup(). Cleared and reused each call.
  mutable std::vector<PartitionLookup> partitionLookups_;
  mutable std::vector<RowRange> scratchRanges_;

  friend class test::ClusterIndexTestHelper;
};

} // namespace facebook::nimble::index
