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
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/tablet/FooterGenerated.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "dwio/nimble/tablet/TabletIndexWriter.h"
#include "velox/common/file/File.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::nimble {
/// Represents a single chunk of encoded data within a stream.
/// A stream may be divided into multiple chunks to control memory usage during
/// writing. Each chunk contains the encoded data and row count.
struct Chunk {
  /// Number of rows contained in this chunk.
  uint32_t rowCount{0};

  /// The encoded and compressed data content of this chunk, stored as a vector
  /// of string views. Each string_view points to a buffer containing a portion
  /// of the chunk's data. Multiple buffers may be used for large chunks.
  std::vector<std::string_view> content;

  /// Returns the total byte size of all content in this chunk.
  uint32_t contentSize() const;
};

struct Stream {
  uint32_t offset{0};
  std::vector<Chunk> chunks;
};

/// Represents a chunk in a key stream with key boundaries.
/// Used to enable efficient range-based lookups during reads.
struct KeyChunk : public Chunk {
  /// First key value in this chunk.
  std::string_view firstKey;
  /// Last key value in this chunk.
  std::string_view lastKey;
};

/// KeyStream holds key chunks for efficient range lookups in indexed data.
/// Unlike Stream, KeyStream is a standalone struct since it uses KeyChunk
/// instead of Chunk and doesn't need offset tracking.
struct KeyStream {
  std::vector<KeyChunk> chunks;
};

struct TabletIndexConfig;

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
    bool streamDeduplicationEnabled{true};
    /// Configuration for cluster index.
    std::optional<TabletIndexConfig> indexConfig;
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
      std::optional<KeyStream>&& keyStream = std::nullopt);

  // TODO: with low memory budget, we might want to have a version of this for
  // a vector of content to be concatenated. Right now the Buffer class prevents
  // us from draining its content incrementally.
  void writeOptionalSection(std::string name, std::string_view content);

  // The number of bytes written so far.
  uint64_t size() const {
    return file_->size();
  }

 private:
  TabletWriter(
      velox::WriteFile* file,
      velox::memory::MemoryPool& pool,
      Options options);

  inline bool hasIndex() const {
    return indexWriter_ != nullptr;
  }

  // Write metadata entry to file
  CompressionType writeMetadata(std::string_view metadata);

  // Write stripe group metadata entry and also add that to footer sections if
  // exceeds metadata flush size.
  void tryWriteStripeGroup(bool force = false);
  bool shouldWriteStripeGroup(bool force) const;
  void writeStripeGroup(size_t streamCount, size_t stripeCount);
  void finishStripeGroup();

  // Write stripes metadata section
  MetadataSection writeStripes(size_t stripeCount);

  // Create metadata section in the file
  MetadataSection createMetadataSection(std::string_view metadata);

  static flatbuffers::Offset<serialization::OptionalMetadataSections>
  createOptionalMetadataSection(
      flatbuffers::FlatBufferBuilder& builder,
      const std::vector<std::pair<std::string, MetadataSection>>&
          optionalSections);

  void writeWithChecksum(std::string_view data);
  void writeWithChecksum(const folly::IOBuf& buf);

  template <typename T>
  void writeStreamWithChecksum(const T& stream);

  // Indexing related methods
  // Starts index writing for a new stripe.
  void startStripeIndexWrite(
      size_t streamCount,
      const std::optional<KeyStream>& keyStream);

  // Writes key stream to file via index writer if indexing is enabled.
  void writeKeyStream(const std::optional<KeyStream>& keyStream);

  // Adds chunk-level index data for a stream.
  void addStreamIndex(uint32_t streamIndex, const std::vector<Chunk>& chunks);

  // Writes the index group for a completed stripe group.
  void writeIndexGroup(size_t streamCount, size_t stripeCount);

  // Writes the root index. This is the last call to indexWriter_.
  void writeRootIndex();

  velox::WriteFile* const file_;
  velox::memory::MemoryPool* const pool_;
  const Options options_;
  const std::unique_ptr<Checksum> checksum_;
  // Index writer for managing cluster index (nullable if indexing disabled).
  const std::unique_ptr<TabletIndexWriter> indexWriter_;

  // Number of rows in each stripe.
  std::vector<uint32_t> stripeRowCounts_;
  // Offsets from start of file to first byte in each stripe.
  std::vector<uint64_t> stripeOffsets_;
  // Sizes of the stripes
  std::vector<uint32_t> stripeSizes_;

  // For each stripe, which stripe group it belongs to.
  std::vector<uint32_t> stripeGroupIndices_;
  // Accumulated offsets within each stripe relative to start of the stripe,
  // with one value for each seen stream in the current OR PREVIOUS stripes.
  std::vector<std::vector<uint32_t>> streamOffsets_;
  // Accumulated stream sizes within each stripe. Same behavior as
  // streamOffsets_.
  std::vector<std::vector<uint32_t>> streamSizes_;

  // Current stripe group index.
  uint32_t stripeGroupIndex_{0};
  // Stripe groups
  std::vector<MetadataSection> stripeGroups_;
  // Optional sections
  std::unordered_map<std::string, MetadataSection> optionalSections_;
};

} // namespace facebook::nimble
