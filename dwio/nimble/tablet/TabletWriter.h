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
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::nimble {
struct Chunk {
  std::vector<std::string_view> content;
};

struct Stream {
  uint32_t offset;
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
    bool streamDeduplicationEnabled{true};
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
  void writeStripe(uint32_t rowCount, std::vector<Stream> streams);

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
  void writeStreamWithChecksum(const Stream& stream);

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

  // Current stripe group index
  uint32_t stripeGroupIndex_{0};
  // Stripe groups
  std::vector<MetadataSection> stripeGroups_;
  // Optional sections
  std::unordered_map<std::string, MetadataSection> optionalSections_;
};

} // namespace facebook::nimble
