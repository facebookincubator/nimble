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

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Checksum.h"
#include "dwio/nimble/common/MetadataBuffer.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/index/IndexConfig.h"
#include "dwio/nimble/tablet/FooterGenerated.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::nimble {
/// Represents a single chunk of encoded data within a stream.
/// A stream may be divided into multiple chunks to control memory usage during
/// writing. Each chunk contains the encoded data, row count, and optional key
/// information for indexing.
struct Chunk {
  /// Number of rows contained in this chunk.
  uint32_t numRows{0};

  /// The encoded and compressed data content of this chunk, stored as a vector
  /// of string views. Each string_view points to a buffer containing a portion
  /// of the chunk's data. Multiple buffers may be used for large chunks.
  std::vector<std::string_view> content;

  /// Optional first key value in this chunk (for indexed streams only).
  /// Used by the cluster index to enable efficient range lookups. Only
  /// populated for the first chunk in a stream.
  std::optional<std::string_view> firstKey;

  /// Optional last key value in this chunk (for indexed streams only).
  /// Used by the cluster index to enable efficient range lookups. Populated
  /// for all chunks in indexed streams.
  std::optional<std::string_view> lastKey;
};

struct Stream {
  uint32_t offset{0};
  std::vector<Chunk> chunks;
};

class LayoutPlanner {
 public:
  virtual std::vector<Stream> getLayout(std::vector<Stream>&& streams) = 0;

  virtual ~LayoutPlanner() = default;
};

constexpr uint32_t kMetadataFlushThreshold = 8 * 1024 * 1024; // 8MB
constexpr uint32_t kMetadataCompressionThreshold = 64 * 1024; // 64kB

/// Writes a new nimble file.
class TabletWriter {
 public:
  struct Options {
    std::unique_ptr<LayoutPlanner> layoutPlanner{nullptr};
    uint32_t metadataFlushThreshold{kMetadataFlushThreshold};
    uint32_t metadataCompressionThreshold{kMetadataCompressionThreshold};
    ChecksumType checksumType{ChecksumType::XXH3_64};
    std::optional<IndexConfig> indexConfig;
  };

  static std::unique_ptr<TabletWriter> create(
      velox::WriteFile* file,
      velox::memory::MemoryPool& pool,
      TabletWriter::Options options);

  // Writes out the footer. Remember that the underlying file is not readable
  // until the write file itself is closed.
  void close();

  // TODO: do we want a stripe header? E.g. per stream min/max (at least for
  // key cols), that sort of thing? We can add later. We'll also want to
  // be able to control the stream order on disk, presumably via a options
  // param to the constructor.
  //
  // The first argument in the map gives the stream name. The second's first
  // gives part gives the uncompressed string returned from a Serialize
  // function, and the second part the compression type to apply when writing to
  // disk. Later we may want to generalize that compression type to include a
  // level or other params.
  //
  // A stream's type must be the same across all stripes.
  void writeStripe(
      uint32_t rowCount,
      std::vector<Stream> streams,
      std::optional<Stream> keyStream);

  void writeOptionalSection(std::string name, std::string_view content);

  // The number of bytes written so far.
  uint64_t size() const {
    return file_->size();
  }

  uint32_t testingStripeGroupCount() const {
    return stripeGroups_.size();
  }

 private:
  TabletWriter(
      velox::WriteFile* file,
      velox::memory::MemoryPool& pool,
      Options options);

  bool hasIndex() const {
    return options_.indexConfig.has_value();
  }

  // Write metadata entry to file
  CompressionType writeMetadata(std::string_view metadata);

  // Write stripe group metadata entry and also add that to footer sections if
  // exceeds metadata flush size.
  void tryWriteStripeGroup(bool force = false);
  bool shouldWriteStripeGroup(bool force) const;
  void writeStripeGroup(size_t streamCount, size_t stripeCount);
  void writeStripeGroupIndex(size_t streamCount);
  void finishStripeGroup();

  // Write stripes metadata section
  MetadataSection writeStripes(size_t stripeCount);

  // Write cluster index and add to optional sections
  void writeIndex();

  // Create metadata section in the file
  MetadataSection createMetadataSection(std::string_view metadata);

  static flatbuffers::Offset<serialization::OptionalMetadataSections>
  createOptionalMetadataSection(
      flatbuffers::FlatBufferBuilder& builder,
      const std::vector<std::pair<std::string, MetadataSection>>&
          optionalSections);

  void ensureIndexWrite(size_t streamCount);

  void addStripeIndexKey(const std::optional<Stream>& keyStream);

  void writeKeyStream(const std::optional<Stream>& keyStream);

  void addStreamChunkIndex(
      uint32_t streamIndex,
      const Chunk& chunk,
      uint32_t chunkOffset);

  void writeWithChecksum(std::string_view data);
  void writeWithChecksum(const folly::IOBuf& buf);
  void writeChunkWithChecksum(const Chunk& chunk);

  velox::WriteFile* const file_;
  velox::memory::MemoryPool* const pool_;
  const Options options_;
  const std::unique_ptr<Checksum> checksum_;

  // Number of rows in each stripe.
  std::vector<uint32_t> stripeRowCounts_;
  // Offsets from start of file to first byte in each stripe.
  std::vector<uint64_t> stripeOffsets_;
  // Sizes of the stripes
  std::vector<uint32_t> stripeSizes_;

  // Stripe group indices
  std::vector<uint32_t> stripeGroupIndices_;
  // Accumulated offsets within each stripe relative to start of the stripe,
  // with one value for each seen stream in the current OR PREVIOUS stripes.
  std::vector<std::vector<uint32_t>> streamOffsets_;
  // Accumulated stream sizes within each stripe. Same behavior as
  // streamOffsets_.
  std::vector<std::vector<uint32_t>> streamSizes_;

  /// Holds index data for stream chunks across all stripes in a stripe group.
  /// All stripes from the same group are consolidated together.
  struct StreamChunkIndex {
    /// Number of rows in each chunk for each stream.
    std::vector<std::vector<uint32_t>> streamChunkRows;
    /// Byte offsets of each chunk for each stream.
    std::vector<std::vector<uint32_t>> streamChunkOffsets;
    /// Number of chunks per stream per stripe.
    std::vector<std::vector<uint32_t>> streamChunkCounts;

    /// Byte offset of key stream for each stripe.
    std::vector<uint32_t> keyStreamOffsets;
    /// Byte sizes of key stream for each stripe.
    std::vector<uint32_t> keyStreamSizes;
    /// Number of rows in each chunk for the key stream.
    std::vector<uint32_t> keyStreamChunkRows;
    /// Byte offsets of each chunk for the key stream.
    std::vector<uint32_t> keyStreamChunkOffsets;
    /// Last key values for each chunk of the key stream.
    std::vector<std::string_view> keyStreamChunkKeys;
    /// Number of chunks for the key stream per stripe.
    std::vector<uint32_t> keyStreamChunkCounts;

    bool empty() const {
      return streamChunkRows.empty();
    }
  };
  std::unique_ptr<StreamChunkIndex> streamIndex_;
  std::unique_ptr<Buffer> streamIndexEncodingBuffer_;

  /// Holds the root index data for the entire file.
  /// Holds cluster index data.
  struct Index {
    /// Key values for each stripe.
    /// Contains the first key for the first stripe, and the last key for all
    /// stripes.
    std::vector<std::string_view> stripeKeys;
    /// Metadata sections for stripe group indexes.
    std::vector<MetadataSection> stripeGroupIndexes;
  };
  Index index_;
  std::unique_ptr<Buffer> indexEncodingBuffer_;

  // Current stripe group index
  uint32_t stripeGroupIndex_{0};
  // Stripe groups
  std::vector<MetadataSection> stripeGroups_;
  // Optional sections
  std::unordered_map<std::string, MetadataSection> optionalSections_;
};

} // namespace facebook::nimble
