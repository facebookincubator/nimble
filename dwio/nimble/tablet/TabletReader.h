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
#include <span>
#include <vector>

#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/index/StripeIndexGroup.h"
#include "dwio/nimble/index/TabletIndex.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "folly/Synchronized.h"
#include "folly/io/IOBuf.h"
#include "velox/common/file/File.h"

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

class Postscript {
 public:
  uint32_t footerSize() const {
    return footerSize_;
  }

  CompressionType footerCompressionType() const {
    return footerCompressionType_;
  }

  uint64_t checksum() const {
    return checksum_;
  }

  ChecksumType checksumType() const {
    return checksumType_;
  }

  uint32_t majorVersion() const {
    return majorVersion_;
  }

  uint32_t minorVersion() const {
    return minorVersion_;
  }

  static Postscript parse(std::string_view data);

 private:
  uint32_t footerSize_;
  CompressionType footerCompressionType_;
  uint64_t checksum_;
  ChecksumType checksumType_;
  uint32_t majorVersion_;
  uint32_t minorVersion_;
};

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

  /// Returns whether the given key has a non-expired cached entry for testing.
  bool testingHasCachedEntry(Key key) const {
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

  std::span<const uint32_t> streamOffsets(uint32_t stripe) const;
  std::span<const uint32_t> streamSizes(uint32_t stripe) const;

 private:
  const std::unique_ptr<MetadataBuffer> metadata_;
  const uint32_t index_;
  uint32_t streamCount_;
  uint32_t firstStripe_;
  const uint32_t* streamOffsets_;
  const uint32_t* streamSizes_;
};

using index::StripeIndexGroup;

class StripeIdentifier {
 public:
  StripeIdentifier(
      uint32_t stripeId,
      std::shared_ptr<StripeGroup> stripeGroup,
      std::shared_ptr<StripeIndexGroup> indexGroup)
      : stripeId_{stripeId},
        stripeGroup_{std::move(stripeGroup)},
        indexGroup_{std::move(indexGroup)} {}

  uint32_t stripeId() const {
    return stripeId_;
  }

  const std::shared_ptr<StripeGroup>& stripeGroup() const {
    return stripeGroup_;
  }

  const std::shared_ptr<StripeIndexGroup>& indexGroup() const {
    return indexGroup_;
  }

 private:
  uint32_t stripeId_;
  std::shared_ptr<StripeGroup> stripeGroup_;
  std::shared_ptr<StripeIndexGroup> indexGroup_;
};

using index::TabletIndex;

/// Provides read access to a tablet written by a TabletWriter.
/// Example usage to read all streams from stripe 0 in a file:
///   auto readFile = std::make_unique<LocalReadFile>("/tmp/myfile");
///   TabletReader tablet(std::move(readFile));
///   auto serializedStreams = tablet.load(0, std::vector{1, 2});
///  |serializedStreams[i]| now contains the stream corresponding to
///  the stream identifier provided in the input vector.
class TabletReader {
 public:
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
      MemoryPool& pool,
      const std::vector<std::string>& preloadOptionalSections = {});

  static std::shared_ptr<TabletReader> create(
      velox::ReadFile* readFile,
      MemoryPool& pool,
      const std::vector<std::string>& preloadOptionalSections = {});

  /// For testing use
  static std::shared_ptr<TabletReader> testingCreate(
      std::shared_ptr<velox::ReadFile> readFile,
      MemoryPool& pool,
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

  // Returns true if the file contains index data.
  bool hasIndex() const;

  // Returns the tablet index if available, nullptr otherwise.
  const TabletIndex* index() const {
    return tabletIndex_.get();
  }

  uint64_t fileSize() const {
    return file_->size();
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

  StripeIdentifier stripeIdentifier(
      uint32_t stripeIndex,
      bool loadIndex = false) const;

 private:
  TabletReader(
      velox::ReadFile* readFile,
      std::shared_ptr<velox::ReadFile> ownedReadFile,
      MemoryPool& pool,
      const std::vector<std::string>& preloadOptionalSections = {});

  /// For testing use
  TabletReader(
      std::shared_ptr<velox::ReadFile> readFile,
      MemoryPool& pool,
      Postscript postscript,
      std::string_view footer,
      std::string_view stripes,
      std::string_view stripeGroup,
      std::unordered_map<std::string, std::string_view> optionalSections = {});

  void init(const std::vector<std::string>& preloadOptionalSections);

  void initPostScript(const folly::IOBuf& footerIoBuf, uint64_t footerIoSize);

  void initFooter(const folly::IOBuf& footerIoBuf, uint64_t footerIoSize);

  uint32_t stripeGroupIndex(uint32_t stripeIndex) const;

  std::shared_ptr<StripeGroup> loadStripeGroup(uint32_t stripeGroupIndex) const;

  std::shared_ptr<StripeGroup> stripeGroup(uint32_t stripeGroupIndex) const;

  // Returns the cached StripeIndexGroup for the given stripe group index.
  // The StripeIndexGroup contains index metadata for efficient data filtering
  // and skipping during reads.
  std::shared_ptr<StripeIndexGroup> indexGroup(uint32_t stripeGroupIndex) const;

  // Loads the StripeIndexGroup for the given stripe group index from file.
  // This is called by the cache when the index group is not already cached.
  std::shared_ptr<StripeIndexGroup> loadIndexGroup(
      uint32_t stripeGroupIndex) const;

  void initStripes(
      const folly::IOBuf& footerIoBuf,
      uint64_t footerIoSize,
      uint64_t fileSize);

  // Used by test init path.
  void initStripes();

  void initOptionalSections(
      const folly::IOBuf& footerIoBuf,
      uint64_t footerIoOffset,
      const std::vector<std::string>& preloadOptionalSections);

  void initIndex(
      const folly::IOBuf& footerIoBuf,
      uint64_t footerIoOffset,
      uint64_t fileSize);

  MemoryPool* const pool_;
  // Non-owning pointer to the file for reading. Always valid during the
  // lifetime of TabletReader.
  velox::ReadFile* const file_;
  // Optional owned file pointer. When provided, ensures the file remains
  // valid throughout TabletReader's lifetime.
  const std::shared_ptr<velox::ReadFile> ownedFile_;

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
  std::unique_ptr<TabletIndex> tabletIndex_;
  mutable ReferenceCountedCache<uint32_t, StripeIndexGroup> indexGroupCache_;
  // Holds a strong reference to the first index group when preloaded from the
  // footer IO. This prevents the first index group from being garbage collected
  // when it's the only index group (common case). Reset when loading a
  // different index group.
  mutable folly::Synchronized<std::shared_ptr<StripeIndexGroup>>
      firstIndexGroup_;

  std::unordered_map<std::string, MetadataSection> optionalSections_;
  mutable folly::Synchronized<
      std::unordered_map<std::string, std::unique_ptr<MetadataBuffer>>>
      optionalSectionsCache_;

  friend class TabletHelper;
  friend class test::TabletReaderTestHelper;
};
} // namespace facebook::nimble
