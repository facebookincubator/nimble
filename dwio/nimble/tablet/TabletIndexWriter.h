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
#include <optional>
#include <vector>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/index/SortOrder.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"

namespace facebook::nimble {

struct Chunk;
struct KeyChunk;
struct KeyStream;

/// Configuration for cluster index in TabletWriter.
/// Only includes fields needed at the tablet layer (columns and ordering).
struct TabletIndexConfig {
  /// Columns to be indexed for data pruning.
  std::vector<std::string> columns;
  /// Specifies the sort order for each index column.
  /// Must not be empty and must have the same size as 'columns'.
  std::vector<SortOrder> sortOrders;
  /// If true, enforces that encoded keys must be in ascending order
  /// across stripes. Duplicate keys are allowed unless noDuplicateKey is set.
  bool enforceKeyOrder{false};
  /// If true, indicates that the index was built with no duplicate keys.
  /// This enables optimized seek operations that can return immediately
  /// upon finding an exact match, rather than scanning for duplicates.
  /// When set, also enforces that keys are strictly ascending (no duplicates).
  bool noDuplicateKey{false};
};

/// TabletIndexWriter manages all index-related state and operations for
/// TabletWriter.
///
/// NOTE: This class is not thread-safe. All methods must be called from a
/// single thread.
///
/// It encapsulates:
/// - Root index: stripe key boundaries for file-level key range lookups
/// - Stripe index groups: chunk-level index data for streams and keys within
///   each stripe group
/// - All internal state maintenance across stripe and stripe group boundaries
///
/// This class separates index management concerns from the core tablet writing
/// logic, following the same pattern as IndexWriter in the velox layer.
///
/// Lifecycle:
/// 1. Create with config and memory pool
/// 2. For each stripe:
///    a. Call ensureStripeWrite() to initialize stripe index structures
///    b. Call addStripeKey() with key stream data
///    c. Call addStreamIndex() for each stream's chunk data
///    d. Call writeKeyStream() to persist key stream and update index
/// 3. At stripe group boundaries:
///    a. Call writeIndexGroup() to serialize and persist group index
/// 4. At file close:
///    a. Call writeRootIndex() to serialize root index as optional section
class TabletIndexWriter {
 public:
  /// Factory method to create a TabletIndexWriter instance.
  ///
  /// @param config Optional index configuration. If not present, returns
  ///               nullptr (no indexing).
  /// @param pool Memory pool for allocations.
  /// @return Unique pointer to TabletIndexWriter, or nullptr if config is not
  ///         present.
  static std::unique_ptr<TabletIndexWriter> create(
      const std::optional<TabletIndexConfig>& config,
      velox::memory::MemoryPool& pool);

  TabletIndexWriter(const TabletIndexWriter&) = delete;
  TabletIndexWriter& operator=(const TabletIndexWriter&) = delete;
  TabletIndexWriter(TabletIndexWriter&&) = delete;
  TabletIndexWriter& operator=(TabletIndexWriter&&) = delete;

  /// Initializes index structures for writing a new stripe.
  /// Must be called at the start of each stripe write.
  ///
  /// @param streamCount Number of streams in this stripe.
  void ensureStripeWrite(size_t streamCount);

  /// Adds stripe key boundaries to the root index.
  /// For the first stripe, stores both first and last keys.
  /// For subsequent stripes, stores only the last key.
  /// Validates key ordering if enforceKeyOrder is enabled.
  ///
  /// @param keyStream Key stream containing chunk key data.
  void addStripeKey(const KeyStream& keyStream);

  /// Adds chunk-level index data for a stream.
  /// Records row counts and byte offsets for each chunk.
  ///
  /// @param streamIndex Index of the stream.
  /// @param chunks Chunks from the stream.
  void addStreamIndex(uint32_t streamIndex, const std::vector<Chunk>& chunks);

  /// Callback type for writing data with checksum to the file.
  using WriteWithChecksumFn = std::function<void(std::string_view)>;

  /// Writes the key stream and updates the current stripe's key stream index.
  /// Records chunk-level metadata including row counts, byte offsets, and
  /// last key values for each chunk.
  ///
  /// @param keyStreamOffset File offset where the key stream starts.
  /// @param keyChunks Key chunks to write.
  /// @param writeWithChecksum Callback to write data with checksum.
  /// @return Number of bytes written for the key stream.
  uint64_t writeKeyStream(
      uint64_t keyStreamOffset,
      const std::vector<KeyChunk>& keyChunks,
      const WriteWithChecksumFn& writeWithChecksum);

  /// Callback type for creating metadata sections in the file.
  /// This is provided by TabletWriter to write metadata sections and return
  /// their file positions.
  using CreateMetadataSectionFn =
      std::function<MetadataSection(std::string_view)>;

  /// Writes the index group for a completed stripe group.
  /// Serializes key stream and stream chunk data into FlatBuffers format.
  ///
  /// @param streamCount Number of streams in the stripe group.
  /// @param stripeCount Number of stripes in the stripe group.
  /// @param createMetadataSection Callback to create metadata section.
  void writeIndexGroup(
      size_t streamCount,
      size_t stripeCount,
      const CreateMetadataSectionFn& createMetadataSection);

  /// Callback type for writing optional sections.
  using WriteOptionalSectionFn =
      std::function<void(std::string, std::string_view)>;

  /// Writes the root index as an optional section.
  /// Call this at file close to persist the file-level index.
  ///
  /// @param stripeGroupIndices Mapping of stripe index to stripe group index.
  /// @param writeOptionalSection Callback to write optional section.
  void writeRootIndex(
      const std::vector<uint32_t>& stripeGroupIndices,
      const WriteOptionalSectionFn& writeOptionalSection);

 private:
  void checkNotFinalized() const {
    NIMBLE_CHECK(!finalized_, "TabletIndexWriter has been finalized");
  }

  TabletIndexWriter(
      const TabletIndexConfig& config,
      velox::memory::MemoryPool& pool);

  // Holds chunk-level index data for a single stream within a stripe group.
  struct StreamIndex {
    // Number of rows in each chunk (accumulated).
    std::vector<uint32_t> chunkRows;
    // Byte offsets of each chunk within the stream.
    std::vector<uint32_t> chunkOffsets;
    // Number of chunks in this stripe for this stream.
    uint32_t chunkCount{0};
  };

  // Holds index data for the key stream within a stripe.
  // Extends StreamIndex with stream-level offset/size and chunk keys.
  struct KeyStreamIndex : public StreamIndex {
    // Byte offset of key stream for this stripe.
    uint32_t streamOffset{0};
    // Byte size of key stream for this stripe.
    uint32_t streamSize{0};
    // Last key values for each chunk.
    std::vector<std::string_view> chunkKeys;
  };

  // Holds index data for all streams in a single stripe.
  struct StripeIndex {
    // Per-stream chunk index data.
    std::vector<StreamIndex> streams;
    // Key stream index data.
    KeyStreamIndex keyStream;
  };

  // Holds index data for stream chunks across all stripes in a stripe group.
  struct GroupIndex {
    // Per-stripe index data.
    std::vector<StripeIndex> stripes;
    // Buffer for encoding group index data (keys).
    std::unique_ptr<Buffer> encodingBuffer;

    bool empty() const {
      return stripes.empty();
    }
  };

  // Holds the root index data for the entire file.
  struct RootIndex {
    // Key values for each stripe.
    // Contains the first key for the first stripe, and the last key for all
    // stripes.
    std::vector<std::string_view> stripeKeys;
    // Metadata sections for stripe index groups.
    std::vector<MetadataSection> stripeIndexGroups;
    // Buffer for encoding root index data.
    std::unique_ptr<Buffer> encodingBuffer;
  };

  // Updates a StreamIndex with chunk data.
  // Works with both Chunk and KeyChunk types.
  template <typename ChunkT>
  static void updateStreamIndex(
      const std::vector<ChunkT>& chunks,
      StreamIndex& index);

  velox::memory::MemoryPool* const pool_;
  const TabletIndexConfig config_;

  std::unique_ptr<GroupIndex> groupIndex_;
  std::unique_ptr<RootIndex> rootIndex_;

  // Set to true after writeRootIndex() is called. No mutation calls are
  // expected after this.
  bool finalized_{false};
};

} // namespace facebook::nimble
