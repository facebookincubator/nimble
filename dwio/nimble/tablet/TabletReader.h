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
#include <optional>
#include <span>
#include <vector>

#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/index/ChunkIndex.h"
#include "dwio/nimble/index/ChunkIndexGroup.h"
#include "dwio/nimble/index/ClusterIndex.h"
#include "dwio/nimble/index/DenseIndexRegistry.h"
#include "dwio/nimble/index/IndexLookup.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/FileLayout.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "dwio/nimble/tablet/MetadataCache.h"
#include "dwio/nimble/tablet/MetadataInput.h"
#include "folly/Synchronized.h"
#include "velox/common/caching/FileHandle.h"
#include "velox/common/file/File.h"
#include "velox/common/io/Options.h"
#include "velox/dwio/common/MetricsLog.h"
#include "velox/dwio/common/Options.h"

/// The TabletReader class is the on-disk layout for nimble.
///
/// As data is streamed into a tablet, we buffer it until the total amount
/// of memory used for buffering hits a chosen limit. Then we convert the
/// buffered memory to streams and write them out to disk in a stripe, recording
/// their byte ranges. This continues until all data for the file has been
/// streamed in, at which point we write out any remaining buffered data and
/// write out the byte ranges + some other metadata in the footer.
///
/// The general recommendation for the buffering limit is to make it as large
/// as the amount of memory you've allocated to a single processing task. The
/// rationale being that the highest memory read case (select *) loads all the
/// encoded stream, and in the worst case (totally random data) the encoded data
/// will be the same size as the raw data.

namespace facebook::nimble {

namespace test {
class TabletReaderTestHelper;
} // namespace test

using MemoryPool = facebook::velox::memory::MemoryPool;

/// Stream loader abstraction.
/// This is the returned object when loading streams from a tablet.
class StreamLoader {
 public:
  virtual ~StreamLoader() = default;
  virtual const std::string_view getStream() const = 0;
};

class StripeGroup {
 public:
  StripeGroup(
      uint32_t stripeGroupIndex,
      const MetadataBuffer& stripes,
      std::unique_ptr<MetadataBuffer> metadata);

  uint32_t index() const {
    return index_;
  }

  uint32_t streamCount() const {
    return streamCount_;
  }

  uint32_t firstStripe() const {
    return firstStripe_;
  }

  uint32_t stripeCount() const {
    return stripeCount_;
  }

  std::span<const uint32_t> streamOffsets(uint32_t stripe) const;
  std::span<const uint32_t> streamSizes(uint32_t stripe) const;

 private:
  const std::unique_ptr<MetadataBuffer> metadata_;
  const uint32_t index_;
  uint32_t streamCount_;
  uint32_t stripeCount_;
  uint32_t firstStripe_;
  const uint32_t* streamOffsets_;
  const uint32_t* streamSizes_;
};

using index::ChunkIndex;
using index::ChunkIndexGroup;

class StripeIdentifier {
 public:
  StripeIdentifier(
      uint32_t stripeId,
      std::shared_ptr<StripeGroup> stripeGroup,
      std::shared_ptr<ChunkIndexGroup> chunkIndex = nullptr)
      : stripeId_{stripeId},
        stripeGroup_{std::move(stripeGroup)},
        chunkIndex_{std::move(chunkIndex)} {}

  uint32_t stripeId() const {
    return stripeId_;
  }

  const std::shared_ptr<StripeGroup>& stripeGroup() const {
    return stripeGroup_;
  }

  const std::shared_ptr<ChunkIndexGroup>& chunkIndex() const {
    return chunkIndex_;
  }

 private:
  uint32_t stripeId_;
  std::shared_ptr<StripeGroup> stripeGroup_;
  std::shared_ptr<ChunkIndexGroup> chunkIndex_;
};

using index::ClusterIndex;
using index::DenseIndexRegistry;

/// Provides read access to a tablet written by a TabletWriter.
/// Example usage to read all streams from stripe 0 in a file:
///   auto readFile = std::make_unique<LocalReadFile>("/tmp/myfile");
///   TabletReader tablet(std::move(readFile));
///   auto serializedStreams = tablet.load(0, std::vector{1, 2});
///  |serializedStreams[i]| now contains the stream corresponding to
///  the stream identifier provided in the input vector.
class TabletReader {
 public:
  /// Options for configuring TabletReader behavior.
  struct Options {
    /// Speculative tail read size (0 = adaptive mode that reads postscript
    /// first, then exact footer size). Default is 8MB (same as
    /// kInitialFooterSize).
    uint64_t maxFooterIoBytes{8 * 1024 * 1024};

    /// Optional sections to eagerly load during initialization.
    std::vector<std::string> preloadOptionalSections;

    /// Whether to load the cluster index during initialization. Default true.
    bool loadClusterIndex{true};

    /// Whether to load the chunk index during initialization. Default true.
    bool loadChunkIndex{true};

    /// Whether to load the dense indexes during initialization. Default true.
    bool loadDenseIndexes{true};

    /// If true, pins parsed metadata objects (StripeGroup, ChunkIndexGroup)
    /// in the cache with strong references so they are never evicted. This
    /// avoids re-reading and re-parsing metadata on every stripe access when
    /// the weak-pointer cache entries would otherwise expire.
    bool pinFileMetadata{false};

    /// IO options providing pool, IO stats, coalescing params, and executor.
    /// Must be set with a non-null metadataIoStats; the constructor enforces
    /// this with a NIMBLE_CHECK.
    std::optional<velox::io::ReaderOptions> ioOptions;

    /// File handle and cache for CachedMetadataInput. Both must be set
    /// together. When set, creates CachedMetadataInput using
    /// fileHandle->uuid as cache key. When neither is set, creates
    /// DirectMetadataInput.
    ///
    /// FileHandle contains:
    ///   - file: shared_ptr<ReadFile> for IO
    ///   - uuid: StringIdLease used as identifier in downstream data
    ///     caching structures (saves memory vs using filename)
    ///   - groupId: StringIdLease for the group of files this belongs to
    ///     (e.g. directory), used for coarse granularity access tracking
    ///     such as deciding SSD placement
    const velox::FileHandle* fileHandle{nullptr};
    velox::cache::AsyncDataCache* cache{nullptr};
  };

  /// Compute checksum from the beginning of the file all the way to footer
  /// size and footer compression type field in postscript.
  /// chunkSize means each time reads up to chunkSize, until all data are
  /// done.
  static uint64_t calculateChecksum(
      MemoryPool& pool,
      velox::ReadFile* readFile,
      uint64_t chunkSize = 256 * 1024 * 1024);

  static std::shared_ptr<TabletReader> create(
      std::shared_ptr<velox::ReadFile> readFile,
      MemoryPool* pool,
      const Options& options);

  /// Configures TabletReader::Options from Velox ReaderOptions.
  static Options configureOptions(
      const velox::dwio::common::ReaderOptions& options);

  /// Returns a collection of stream loaders for the given stripe. The stream
  /// loaders are returned in the same order as the input stream identifiers
  /// span. If a stream was not present in the given stripe a nullptr is
  /// returned in its slot.
  std::vector<std::unique_ptr<StreamLoader>> load(
      const StripeIdentifier& stripe,
      std::span<const uint32_t> streamIdentifiers,
      std::function<std::string_view(uint32_t)> streamLabel = [](uint32_t) {
        return std::string_view{};
      }) const;

  uint64_t totalStreamSize(
      const StripeIdentifier& stripe,
      std::span<const uint32_t> streamIdentifiers) const;

  std::optional<MetadataSection> stripesMetadata() const;

  std::vector<MetadataSection> stripeGroupsMetadata() const;

  const std::unordered_map<std::string, MetadataSection>& optionalSections()
      const {
    return optionalSections_;
  }

  bool hasOptionalSection(const std::string& name) const;

  std::optional<Section> loadOptionalSection(
      const std::string& name,
      bool keepCache = false) const;

  // Returns true if the file has a chunk index optional section.
  bool hasChunkIndexSection() const;

  // Returns true if the file has a cluster index optional section.
  bool hasClusterIndexSection() const;

  // Returns true if the cluster index is loaded.
  inline bool hasClusterIndex() const {
    return clusterIndex_ != nullptr;
  }

  // Returns true if the chunk index is loaded and has data for the given
  // stripe group.
  inline bool hasChunkIndex(uint32_t stripeGroupIndex) const {
    return chunkIndex_ != nullptr &&
        chunkIndex_->groupMetadata(stripeGroupIndex).size() > 0;
  }

  // Returns the cluster index if available, nullptr otherwise.
  const ClusterIndex* clusterIndex() const {
    return clusterIndex_.get();
  }

  /// Finds the dense index matching the given columns, or nullptr if none.
  const index::IndexLookup* denseIndex(
      const std::vector<std::string>& columns) const;

  uint64_t fileSize() const {
    return fileSize_;
  }

  const Postscript& postscript() const {
    return ps_;
  }

  uint32_t footerSize() const {
    return ps_.footerSize();
  }

  CompressionType footerCompressionType() const {
    return ps_.footerCompressionType();
  }

  uint64_t checksum() const {
    return ps_.checksum();
  }

  ChecksumType checksumType() const {
    return ps_.checksumType();
  }

  uint32_t majorVersion() const {
    return ps_.majorVersion();
  }

  uint32_t minorVersion() const {
    return ps_.minorVersion();
  }

  /// Number of rows in the whole tablet.
  uint64_t tabletRowCount() const {
    return tabletRowCount_;
  }

  /// The number of rows in the given stripe. These sum to tabletRowCount().
  uint32_t stripeRowCount(uint32_t stripe) const {
    NIMBLE_CHECK_LT(stripe, stripeCount_);
    return static_cast<uint32_t>(stripeRows_[stripe + 1] - stripeRows_[stripe]);
  }

  /// The number of stripes in the tablet.
  uint32_t stripeCount() const {
    return stripeCount_;
  }

  /// Returns the stripe index containing the given file-level row.
  /// Uses binary search on cumulative stripe row counts.
  uint32_t rowToStripe(uint64_t row) const;

  /// Returns the file-level start row of the given stripe.
  uint64_t stripeStartRow(uint32_t stripe) const {
    NIMBLE_CHECK_LE(stripe, stripeCount_);
    return stripeRows_[stripe];
  }

  uint64_t stripeOffset(uint32_t stripe) const {
    return stripeOffsets_[stripe];
  }

  /// Returns stream offsets for the specified stripe. Number of streams is
  /// determined by schema node count at the time when stripe is written, so
  /// it may have fewer number of items than the final schema node count
  std::span<const uint32_t> streamOffsets(const StripeIdentifier& stripe) const;

  /// Returns stream sizes for the specified stripe. Has same constraint as
  /// `streamOffsets()`.
  std::span<const uint32_t> streamSizes(const StripeIdentifier& stripe) const;

  /// Returns the byte size of a single stream in the specified stripe.
  /// Returns 0 if the stream does not exist in this stripe.
  uint32_t streamSize(const StripeIdentifier& stripe, uint32_t streamId) const;

  /// Returns stream count for the specified stripe. Has same constraint as
  /// `streamOffsets()`.
  uint32_t streamCount(const StripeIdentifier& stripe) const;

  StripeIdentifier stripeIdentifier(uint32_t stripeIndex) const;

 private:
  TabletReader(
      std::shared_ptr<velox::ReadFile> readFile,
      MemoryPool& pool,
      const Options& options);

  void init(const Options& options);

  // Tries to initialize entirely from cache. Returns true if successful
  // (all metadata loaded from cache, no file IO needed).
  bool initFromCache(const Options& options);

  // Tries to load footer+PS from cache at synthetic key fileSize_.
  bool loadFooterFromCache();

  // Caches metadata sections from the speculative read buffer into
  // CachedMetadataInput for subsequent opens.
  void cacheMetadata(std::string_view footerBuf, uint64_t footerOffset);

  // Reads postscript and footer via MetadataInput. For speculative mode,
  // also does a speculative read into footerBuf for downstream preload.
  void loadFooter(
      uint64_t maxFooterIoBytes,
      velox::BufferPtr& footerBuf,
      uint64_t& footerIoSize,
      uint64_t& footerOffset);

  // Stripes.
  //
  // Loads stripes_ from the speculative read buffer, falling back to
  // MetadataInput when the buffer doesn't cover the stripes section.
  void loadStripes(
      velox::BufferPtr& footerBuf,
      uint64_t& footerIoSize,
      uint64_t& footerOffset,
      const Options& options);

  // Tracks a pending metadata section for batched coalesced loading.
  struct LoadSection {
    enum class Type { kStripes, kOptionalSection };

    Type type;
    std::string name;
    MetadataSection section;
  };

  // Collects stripes section for coalesced loading. Tries to extract from
  // footerBuf first; adds to pending on miss.
  void collectStripesSection(
      std::string_view footerBuf,
      uint64_t footerOffset,
      std::vector<LoadSection>& loadSections);

  void collectStripesSection(std::vector<LoadSection>& loadSections) {
    collectStripesSection({}, 0, loadSections);
  }

  // Collects optional sections for coalesced loading. Tries to extract from
  // footerBuf first; adds to pending on miss.
  void collectOptionalSections(
      const Options& options,
      std::string_view footerBuf,
      uint64_t footerOffset,
      std::vector<LoadSection>& loadSections);

  void collectOptionalSections(
      const Options& options,
      std::vector<LoadSection>& loadSections) {
    collectOptionalSections(options, {}, 0, loadSections);
  }

  // Loads all pending sections via MetadataInput and stores results:
  // stripes go to stripes_, optional sections go to optionalSectionsCache_.
  void loadSections(std::vector<LoadSection>& loadSections);

  // Parses stripes_ to populate stripe metadata and preloads the first
  // stripe group from footerBuf when available.
  void initStripes(std::string_view footerBuf = {}, uint64_t footerOffset = 0);

  // Stripe groups.
  //
  uint32_t stripeGroupIndex(uint32_t stripeIndex) const;

  std::shared_ptr<StripeGroup> stripeGroup(uint32_t stripeGroupIndex) const;

  std::shared_ptr<StripeGroup> loadStripeGroup(uint32_t stripeGroupIndex) const;

  // Index.
  //
  void initClusterIndex();

  // Holds the result of a coalesced metadata load for a stripe group.
  struct StripeGroupMetadata {
    std::shared_ptr<StripeGroup> stripeGroup;
    std::shared_ptr<ChunkIndexGroup> chunkIndex;
  };

  // Loads stripe group and chunk index together using coalesced IO via
  // MetadataInput.
  StripeGroupMetadata loadStripeGroupMetadata(uint32_t stripeGroupIndex) const;

  // Optional sections.
  //
  // Parses optional sections metadata from footer into optionalSections_ map.
  void initOptionalSections();

  // Returns the list of optional section names to preload: the user-specified
  // sections plus the index section if present.
  std::vector<std::string> preloadSectionNames(const Options& options) const;

  // Reads and decompresses a metadata section via MetadataInput.
  std::unique_ptr<MetadataBuffer> readMetadata(
      const MetadataSection& section) const;

  void initDenseIndexes();

  // Loads chunk index and preloads the first group from footerBuf
  // when available.
  void initChunkIndex(
      std::string_view footerBuf = {},
      uint64_t footerOffset = 0);

  // Returns the cached ChunkIndexGroup for the given stripe group index.
  std::shared_ptr<ChunkIndexGroup> chunkIndex(uint32_t stripeGroupIndex) const;

  // Loads the ChunkIndexGroup for the given stripe group index from file.
  std::shared_ptr<ChunkIndexGroup> loadChunkIndexGroup(
      uint32_t stripeGroupIndex) const;

  // Computes first stripe index for the given stripe group.
  uint32_t firstStripe(uint32_t stripeGroupIndex) const;

  // Computes the number of stripes for the given stripe group.
  uint32_t stripeCount(uint32_t stripeGroupIndex) const;

  MemoryPool* const pool_;
  const std::shared_ptr<velox::ReadFile> file_;
  const bool loadClusterIndex_;
  const bool loadChunkIndex_;
  const bool loadDenseIndexes_;
  // IO options copied from Options.ioOptions (required, with non-null
  // metadataIoStats).
  const velox::io::ReaderOptions ioOptions_;
  // IO config for index creation — indexes create their own MetadataInput
  // with index-specific IO stats.
  const index::IndexLookup::Options indexOptions_;

  // Owned MetadataInput for coalesced metadata IO. Always created —
  // uses DirectMetadataInput (no cache) or CachedMetadataInput depending
  // on options. Thread-safe: load() is stateless.
  const std::unique_ptr<MetadataInput> metadataInput_;

  uint64_t fileSize_{0};
  Postscript ps_;
  std::unique_ptr<MetadataBuffer> footer_;
  std::unique_ptr<MetadataBuffer> stripes_;

  mutable MetadataCache<uint32_t, StripeGroup> stripeGroupCache_;

  uint64_t tabletRowCount_{0};
  uint32_t stripeCount_{0};
  const uint64_t* stripeOffsets_{nullptr};
  // Prefix sum of stripe row counts for O(log n) rowToStripe lookup.
  // stripeRows_[i] = total rows in stripes [0, i).
  // Size is stripeCount_ + 1, with stripeRows_[0] = 0.
  std::vector<uint64_t> stripeRows_;

  // Index related fields.
  std::unique_ptr<ClusterIndex> clusterIndex_;

  // Unified dense index registry for hash and sorted indices.
  std::unique_ptr<DenseIndexRegistry> denseIndexRegistry_;

  // Chunk index root, loaded from "chunk_index" optional section.
  std::unique_ptr<ChunkIndex> chunkIndex_;
  mutable MetadataCache<uint32_t, ChunkIndexGroup> chunkIndexCache_;

  std::unordered_map<std::string, MetadataSection> optionalSections_;
  mutable folly::Synchronized<
      std::unordered_map<std::string, std::unique_ptr<MetadataBuffer>>>
      optionalSectionsCache_;

  friend class TabletHelper;
  friend class test::TabletReaderTestHelper;
};
} // namespace facebook::nimble
