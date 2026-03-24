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
#include "dwio/nimble/index/ClusterIndexGroup.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/FileLayout.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "folly/Synchronized.h"
#include "velox/common/file/File.h"
#include "velox/dwio/common/MetricsLog.h"
#include "velox/dwio/common/Options.h"

namespace facebook::velox::dwio::common {
class BufferedInput;
class SeekableInputStream;
} // namespace facebook::velox::dwio::common

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

template <typename Key, typename Value>
class ReferenceCountedCache {
 public:
  using BuilderCallback = std::function<std::shared_ptr<Value>(Key)>;

  explicit ReferenceCountedCache(BuilderCallback builder)
      : builder_{std::move(builder)} {}

  std::shared_ptr<Value> get(Key key) {
    return getPopulatedCacheEntry(key, builder_);
  }

  std::shared_ptr<Value> get(Key key, const BuilderCallback& builder) {
    return getPopulatedCacheEntry(key, builder);
  }

  /// Returns the number of non-expired cached entries for testing purposes.
  size_t testingCachedCount() const {
    size_t count = 0;
    auto rlockedCache = cache_.rlock();
    for (const auto& [key, entry] : *rlockedCache) {
      if (!entry.rlock()->expired()) {
        ++count;
      }
    }
    return count;
  }

  /// Returns whether the given key has a non-expired cached entry.
  bool hasCachedEntry(Key key) const {
    auto rlockedCache = cache_.rlock();
    auto it = rlockedCache->find(key);
    if (it == rlockedCache->end()) {
      return false;
    }
    return !it->second.rlock()->expired();
  }

 private:
  folly::Synchronized<std::weak_ptr<Value>>& getCacheEntry(Key key) {
    return cache_.wlock()->emplace(key, std::weak_ptr<Value>()).first->second;
  }

  std::shared_ptr<Value> getPopulatedCacheEntry(
      Key key,
      const BuilderCallback& builder) {
    auto& entry = getCacheEntry(key);
    auto wlockedEntry = entry.wlock();
    auto sharedPtr = wlockedEntry->lock();
    if (sharedPtr != nullptr) {
      return sharedPtr;
    }
    auto element = builder(key);
    std::weak_ptr<Value>(element).swap(*wlockedEntry);
    NIMBLE_DCHECK(!wlockedEntry->expired(), "Shouldn't be expired");
    return element;
  }

  const BuilderCallback builder_;
  folly::Synchronized<
      std::unordered_map<Key, folly::Synchronized<std::weak_ptr<Value>>>>
      cache_;
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
using index::ClusterIndexGroup;

class StripeIdentifier {
 public:
  StripeIdentifier(
      uint32_t stripeId,
      std::shared_ptr<StripeGroup> stripeGroup,
      std::shared_ptr<ClusterIndexGroup> clusterIndex,
      std::shared_ptr<ChunkIndexGroup> chunkIndex = nullptr)
      : stripeId_{stripeId},
        stripeGroup_{std::move(stripeGroup)},
        clusterIndex_{std::move(clusterIndex)},
        chunkIndex_{std::move(chunkIndex)} {}

  uint32_t stripeId() const {
    return stripeId_;
  }

  const std::shared_ptr<StripeGroup>& stripeGroup() const {
    return stripeGroup_;
  }

  const std::shared_ptr<ClusterIndexGroup>& clusterIndex() const {
    return clusterIndex_;
  }

  const std::shared_ptr<ChunkIndexGroup>& chunkIndex() const {
    return chunkIndex_;
  }

 private:
  uint32_t stripeId_;
  std::shared_ptr<StripeGroup> stripeGroup_;
  std::shared_ptr<ClusterIndexGroup> clusterIndex_;
  std::shared_ptr<ChunkIndexGroup> chunkIndex_;
};

using index::ClusterIndex;

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

    /// Non-owning pointer for cached reads. When provided, metadata reads
    /// go through BufferedInput for cache integration. Owned by caller
    /// (typically nimble::ReaderBase).
    velox::dwio::common::BufferedInput* bufferedInput{nullptr};
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

  static std::shared_ptr<TabletReader>
  create(velox::ReadFile* readFile, MemoryPool* pool, const Options& options);

  /// Configures TabletReader::Options from Velox ReaderOptions.
  /// @param options The Velox reader options.
  /// @param bufferedInput Optional BufferedInput for metadata caching
  ///        (only used when options.fileMetadataCacheEnabled() is true).
  static Options configureOptions(
      const velox::dwio::common::ReaderOptions& options,
      velox::dwio::common::BufferedInput* bufferedInput = nullptr);

  /// For testing use
  static std::shared_ptr<TabletReader> testingCreate(
      std::shared_ptr<velox::ReadFile> readFile,
      MemoryPool* pool,
      Postscript postscript,
      std::string_view footer,
      std::string_view stripes,
      std::string_view stripeGroup,
      std::unordered_map<std::string, std::string_view> optionalSections = {});

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
    return stripeRowCounts_[stripe];
  }

  /// The number of stripes in the tablet.
  uint32_t stripeCount() const {
    return stripeCount_;
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

  /// Returns stream count for the specified stripe. Has same constraint as
  /// `streamOffsets()`.
  uint32_t streamCount(const StripeIdentifier& stripe) const;

  StripeIdentifier stripeIdentifier(uint32_t stripeIndex) const;

 private:
  TabletReader(
      velox::ReadFile* readFile,
      std::shared_ptr<velox::ReadFile> ownedReadFile,
      MemoryPool& pool,
      const Options& options);

  // For testing use.
  TabletReader(
      std::shared_ptr<velox::ReadFile> readFile,
      MemoryPool& pool,
      Postscript postscript,
      std::string_view footer,
      std::string_view stripes,
      std::string_view stripeGroup,
      std::unordered_map<std::string, std::string_view> optionalSections = {});

  void init(const Options& options);

  // Cache init path.
  //
  // Returns true if this reader is backed by a BufferedInput with Velox
  // async data cache.
  bool hasCache() const;

  // Tries to initialize entirely from Velox async data cache (zero file IO).
  // Probes the cache for footer+PS at synthetic offset fileSize, parses PS and
  // footer, then loads all remaining metadata via cache hits. Returns true on
  // success, false on cache miss (caller falls through to cold path).
  bool tryInitFromCache(const Options& options);

  // Tries to parse PS and footer from cached footer+PS entry at synthetic
  // offset fileSize. Returns true on cache hit, false on miss.
  bool tryLoadAndInitFooterFromCache();

  // Populates AsyncDataCache with exact-size metadata entries from the
  // speculative tail read IOBuf. Enables zero-IO init for subsequent readers.
  // No-op if there is no cache.
  void cacheMetadata(const folly::IOBuf& footerIoBuf, uint64_t footerIoOffset);

  // Footer/postscript parsing.
  //
  // Parses the postscript from the last kPostscriptSize bytes of footerIoBuf.
  void initPostScript(const folly::IOBuf& footerIoBuf, uint64_t footerIoSize);

  // Parses the footer from footerIoBuf using the already-parsed postscript.
  void initFooter(const folly::IOBuf& footerIoBuf, uint64_t footerIoSize);

  // Reads and parses both postscript and footer from file. Supports two modes:
  // - Adaptive (maxFooterIoBytes=0): reads postscript first, then exact footer.
  // - Speculative: reads maxFooterIoBytes, re-reads if footer is larger.
  void loadAndInitFooter(
      uint64_t maxFooterIoBytes,
      folly::IOBuf& footerIoBuf,
      uint64_t& footerIoSize,
      uint64_t& footerIoOffset);

  // Stripes.
  //
  // Loads stripes_ from the footerIoBuf. Handles both single-read and re-read
  // cases when the speculative read didn't capture the full stripes section.
  void loadStripes(
      folly::IOBuf& footerIoBuf,
      uint64_t& footerIoSize,
      uint64_t& footerIoOffset,
      const Options& options);

  // Parses stripes_ to populate stripe metadata (counts, offsets, etc.).
  // Used by test init path.
  void initStripes();

  // Stripe groups.
  //
  uint32_t stripeGroupIndex(uint32_t stripeIndex) const;

  std::shared_ptr<StripeGroup> stripeGroup(uint32_t stripeGroupIndex) const;

  std::shared_ptr<StripeGroup> loadStripeGroup(uint32_t stripeGroupIndex) const;

  // Eagerly caches the first stripe group if it's already in the footer IOBuf.
  // Skipped when cache is present — on-demand reads will hit the cache.
  void preloadStripeGroup(const folly::IOBuf& footerIoBuf);

  // Index.
  //
  void initClusterIndex();

  // Returns the cached ClusterIndexGroup for the given stripe group index.
  // The ClusterIndexGroup contains index metadata for efficient data filtering
  // and skipping during reads.
  std::shared_ptr<ClusterIndexGroup> clusterIndexGroup(
      uint32_t stripeGroupIndex) const;

  // Loads the ClusterIndexGroup for the given stripe group index from file.
  // This is called by the cache when the index group is not already cached.
  std::shared_ptr<ClusterIndexGroup> loadClusterIndexGroup(
      uint32_t stripeGroupIndex) const;

  // Eagerly caches the first cluster index group if it's already in the footer
  // IOBuf. Skipped when cache is present — on-demand reads will hit the cache.
  void preloadClusterIndex(const folly::IOBuf& footerIoBuf);

  // Holds the result of a coalesced metadata load for a stripe group.
  struct StripeGroupMetadata {
    std::shared_ptr<StripeGroup> stripeGroup;
    std::shared_ptr<ClusterIndexGroup> clusterIndex;
    std::shared_ptr<ChunkIndexGroup> chunkIndex;
  };

  // Loads stripe group, cluster index group, and chunk index together using
  // coalesced IO when BufferedInput is available. Falls back to separate loads
  // otherwise.
  StripeGroupMetadata loadStripeGroupMetadata(uint32_t stripeGroupIndex) const;

  // Optional sections.
  //
  // Parses optional sections metadata from footer into optionalSections_ map.
  void initOptionalSections();

  // Returns the list of optional section names to preload: the user-specified
  // sections plus the index section if present.
  std::vector<std::string> preloadSectionNames(const Options& options) const;

  // Preloads optional sections into optionalSectionsCache_ using the provided
  // loader callback to read each section.
  using SectionLoader = std::function<std::unique_ptr<MetadataBuffer>(
      const MetadataSection& section)>;
  void preloadOptionalSections(
      const Options& options,
      const SectionLoader& loader);

  // Creates a SectionLoader that tries to extract from footerIoBuf first
  // (for sections at offset >= footerIoOffset), falling back to readMetadata.
  SectionLoader makeSectionLoader(
      const folly::IOBuf& footerIoBuf,
      uint64_t footerIoOffset) const;

  // BufferedInput coalesced IO.
  //
  // Holds a section enqueued for coalesced IO via BufferedInput.
  // After bufferedInput_->load(), the stream can be read to get section data.
  struct EnqueuedSection {
    std::string name;
    MetadataSection section;
    std::unique_ptr<velox::dwio::common::SeekableInputStream> stream;
  };

  // Loads stripes and optional sections via BufferedInput's enqueue/load
  // pattern for coalesced IO. Populates stripes_ and optionalSectionsCache_.
  // No-op for empty files (no stripes). Caller must call initStripes() after.
  void loadStripesAndSections(const Options& options);

  // Enqueues a read for the stripes section via BufferedInput. Returns nullopt
  // if no stripes (empty file). Caller must call bufferedInput_->load() after.
  std::optional<EnqueuedSection> enqueueStripesSection();

  // Enqueues reads for the given optional section names via BufferedInput.
  // Skips names not found in optionalSections_. Caller must call
  // bufferedInput_->load() after this to execute the coalesced IO.
  std::vector<EnqueuedSection> enqueueOptionalSections(
      const std::vector<std::string>& sectionNames);

  // Reads enqueued sections and inserts them into optionalSectionsCache_.
  // Must be called after bufferedInput_->load().
  void loadEnqueuedOptionalSections(std::vector<EnqueuedSection>&& sections);

  // Low-level IO.
  //
  // Reads metadata from a MetadataSection (offset + size + compressionType).
  // Uses BufferedInput when available, otherwise reads directly from file.
  std::unique_ptr<MetadataBuffer> readMetadata(
      const MetadataSection& section,
      velox::dwio::common::LogType logType) const;

  // Reads metadata from a pre-loaded stream (e.g. from enqueue/load pattern).
  std::unique_ptr<MetadataBuffer> readMetadata(
      std::unique_ptr<velox::dwio::common::SeekableInputStream> stream,
      const MetadataSection& section) const;

  void initChunkIndex();

  // Returns the cached ChunkIndexGroup for the given stripe group index.
  std::shared_ptr<ChunkIndexGroup> chunkIndex(uint32_t stripeGroupIndex) const;

  // Loads the ChunkIndexGroup for the given stripe group index from file.
  std::shared_ptr<ChunkIndexGroup> loadChunkIndexGroup(
      uint32_t stripeGroupIndex) const;

  // Eagerly caches the first chunk index group if it's already in the footer
  // IOBuf. Skipped when cache is present — on-demand reads will hit the cache.
  void preloadChunkIndex(const folly::IOBuf& footerIoBuf);

  // Computes first stripe index for the given stripe group.
  uint32_t firstStripe(uint32_t stripeGroupIndex) const;

  // Computes the number of stripes for the given stripe group.
  uint32_t stripeCount(uint32_t stripeGroupIndex) const;

  MemoryPool* const pool_;
  // Non-owning pointer to the file for reading. Always valid during the
  // lifetime of TabletReader.
  velox::ReadFile* const file_;
  // Optional owned file pointer. When provided, ensures the file remains
  // valid throughout TabletReader's lifetime.
  const std::shared_ptr<velox::ReadFile> ownedFile_;
  // Optional BufferedInput for cached reads (non-owning, owned by caller).
  // When set, metadata reads (stripe groups, index groups) go through
  // BufferedInput for cache integration.
  velox::dwio::common::BufferedInput* const bufferedInput_;

  uint64_t fileSize_{0};
  Postscript ps_;
  std::unique_ptr<MetadataBuffer> footer_;
  std::unique_ptr<MetadataBuffer> stripes_;

  mutable ReferenceCountedCache<uint32_t, StripeGroup> stripeGroupCache_;
  // Holds a strong reference to the first stripe group when preloaded from the
  // footer IO. This prevents the first stripe group from being garbage
  // collected when it's the only stripe group (common case). Reset when loading
  // a different stripe group.
  mutable folly::Synchronized<std::shared_ptr<StripeGroup>> firstStripeGroup_;

  uint64_t tabletRowCount_{0};
  uint32_t stripeCount_{0};
  const uint32_t* stripeRowCounts_{nullptr};
  const uint64_t* stripeOffsets_{nullptr};

  // Index related fields.
  std::unique_ptr<ClusterIndex> clusterIndex_;
  mutable ReferenceCountedCache<uint32_t, ClusterIndexGroup> clusterIndexCache_;
  // Holds a strong reference to the first cluster index group when preloaded
  // from the footer IO. This prevents it from being garbage collected when it's
  // the only index group (common case). Reset when loading a different group.
  mutable folly::Synchronized<std::shared_ptr<ClusterIndexGroup>>
      firstClusterIndexGroup_;

  // Chunk index root, loaded from "chunk_index" optional section.
  std::unique_ptr<ChunkIndex> chunkIndex_;
  mutable ReferenceCountedCache<uint32_t, ChunkIndexGroup> chunkIndexCache_;
  // Holds a strong reference to the first chunk index group when preloaded
  // from the footer IO. This prevents it from being garbage collected when it's
  // the only chunk index group (common case). Reset when loading a different
  // group.
  mutable folly::Synchronized<std::shared_ptr<ChunkIndexGroup>>
      firstChunkIndexGroup_;

  std::unordered_map<std::string, MetadataSection> optionalSections_;
  mutable folly::Synchronized<
      std::unordered_map<std::string, std::unique_ptr<MetadataBuffer>>>
      optionalSectionsCache_;

  friend class TabletHelper;
  friend class test::TabletReaderTestHelper;
};
} // namespace facebook::nimble
