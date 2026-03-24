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

struct KeyChunk;
struct KeyStream;

/// Configuration for cluster index in TabletWriter.
/// Only includes fields needed at the tablet layer (columns and ordering).
struct ClusterIndexConfig {
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

/// ClusterIndexWriter manages cluster index (value index + key stream)
/// state and operations for TabletWriter.
///
/// NOTE: This class is not thread-safe. All methods must be called from a
/// single thread.
///
/// It encapsulates:
/// - Root index: stripe key boundaries for file-level key range lookups
/// - StripeClusterIndex: key stream chunk data within each stripe group
///
/// Lifecycle:
/// - Create with config and memory pool
/// - For each stripe:
///    a. Call newStripe() to initialize key stream structures
///    b. Call addStripeKey() with key stream data
///    c. Call writeKeyStream() to persist key stream and update index
/// - At stripe group boundaries:
///    a. Call writeGroup() to serialize and persist index group
/// - At file close:
///    a. Call writeRoot() to serialize root index as optional section
class ClusterIndexWriter {
 public:
  /// Factory method to create a ClusterIndexWriter instance.
  ///
  /// @param config Optional index configuration. If not present, returns
  ///               nullptr (no indexing).
  /// @param pool Memory pool for allocations.
  /// @return Unique pointer to ClusterIndexWriter, or nullptr if config is not
  ///         present.
  static std::unique_ptr<ClusterIndexWriter> create(
      const std::optional<ClusterIndexConfig>& config,
      velox::memory::MemoryPool& pool);

  ClusterIndexWriter(const ClusterIndexWriter&) = delete;
  ClusterIndexWriter& operator=(const ClusterIndexWriter&) = delete;
  ClusterIndexWriter(ClusterIndexWriter&&) = delete;
  ClusterIndexWriter& operator=(ClusterIndexWriter&&) = delete;

  /// Initializes key stream index structures for writing a new stripe.
  /// Must be called at the start of each stripe write.
  void newStripe();

  /// Adds stripe key boundaries to the root index.
  /// For the first stripe, stores both first and last keys.
  /// For subsequent stripes, stores only the last key.
  /// Validates key ordering if enforceKeyOrder is enabled.
  ///
  /// @param keyStream Key stream containing chunk key data.
  void addStripeKey(const KeyStream& keyStream);

  /// Callback type for writing data with checksum to the file.
  using WriteWithChecksumFn = std::function<void(std::string_view)>;

  /// Writes the key stream and updates the current stripe's key stream index.
  /// Records chunk-level metadata including row counts, byte offsets, and
  /// last key values for each chunk.
  ///
  /// @param keyStreamOffset Key stream offset relative to the stripe's data
  ///        start position.
  /// @param keyChunks Key chunks to write.
  /// @param writeWithChecksum Callback to write data with checksum.
  /// @return Number of bytes written for the key stream.
  uint64_t writeKeyStream(
      uint64_t keyStreamOffset,
      const std::vector<KeyChunk>& keyChunks,
      const WriteWithChecksumFn& writeWithChecksum);

  /// Writes the index group for a completed stripe group.
  /// Builds StripeClusterIndex from key stream data.
  ///
  /// @param stripeCount Number of stripes in the stripe group.
  /// @param createMetadataSection Callback to create metadata section.
  void writeGroup(
      size_t stripeCount,
      const CreateMetadataSectionFn& createMetadataSection);

  /// Writes the root index as an optional section.
  /// Call this at file close to persist the file-level index.
  /// For empty files (no stripes), writes config only (columns, sort orders)
  /// but no stripe keys or stripe index groups.
  ///
  /// @param stripeGroupIndices Mapping of stripe index to stripe group index.
  /// @param writeOptionalSection Callback to write optional section.
  void writeRoot(
      const std::vector<uint32_t>& stripeGroupIndices,
      const WriteOptionalSectionFn& writeOptionalSection);

 private:
  void checkNotFinalized() const {
    NIMBLE_CHECK(!finalized_, "ClusterIndexWriter has been finalized");
  }

  // Writes an empty root index with config only (columns, sort orders)
  // but no stripe keys or stripe index groups.
  void writeEmptyRootIndex(const WriteOptionalSectionFn& writeOptionalSection);

  ClusterIndexWriter(
      const ClusterIndexConfig& config,
      velox::memory::MemoryPool& pool);

  // Holds chunk-level index data for the key stream within a stripe.
  struct KeyStreamIndex {
    // Accumulated row counts per chunk.
    std::vector<uint32_t> chunkRows;
    // Byte offsets of each chunk within the stream.
    std::vector<uint32_t> chunkOffsets;
    // Number of chunks in this stripe for this stream.
    uint32_t chunkCount{0};
    // Byte offset of key stream for this stripe.
    uint32_t streamOffset{0};
    // Byte size of key stream for this stripe.
    uint32_t streamSize{0};
    // Last key values for each chunk.
    std::vector<std::string_view> chunkKeys;
  };

  // Holds key stream index data for all stripes in a stripe group.
  struct StripeIndex {
    KeyStreamIndex keyStream;
  };

  // Holds key stream index data across all stripes in a stripe group.
  struct GroupIndex {
    std::vector<StripeIndex> stripes;
    // Buffer for encoding group index data (keys).
    std::unique_ptr<Buffer> encodingBuffer;

    bool empty() const {
      return stripes.empty();
    }
  };

  // Holds the root index data for the entire file.
  struct RootIndex {
    // Key values for each stripe. Contains the first key for the first stripe,
    // and the last key for all stripes.
    std::vector<std::string_view> stripeKeys;
    // Metadata sections for each stripe group's cluster index.
    std::vector<MetadataSection> stripeIndexes;
    // Buffer for encoding root index data.
    std::unique_ptr<Buffer> encodingBuffer;
  };

  // Updates a KeyStreamIndex with chunk data.
  static void updateKeyStreamIndex(
      const std::vector<KeyChunk>& chunks,
      KeyStreamIndex& index);

  velox::memory::MemoryPool* const pool_;
  const ClusterIndexConfig config_;

  std::unique_ptr<GroupIndex> groupIndex_;
  std::unique_ptr<RootIndex> rootIndex_;

  // Set to true after writeRoot() is called. No mutation calls are
  // expected after this.
  bool finalized_{false};
};

} // namespace facebook::nimble
