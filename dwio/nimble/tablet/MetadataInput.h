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
#include <span>
#include <vector>

#include <optional>
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "folly/Range.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/file/File.h"
#include "velox/common/io/Options.h"

namespace facebook::velox {
struct FileHandle;
} // namespace facebook::velox

namespace facebook::nimble {

namespace test {
class MetadataInputTestHelper;
class CachedMetadataInputTestHelper;
} // namespace test

class MetadataInput;

/// Handle to an enqueued metadata section. Returned by
/// MetadataInput::enqueue(). Call read() to get the decompressed
/// MetadataBuffer — either from a prior coalesced load() or by
/// triggering on-demand IO for just this section.
class MetadataHandle {
 public:
  MetadataHandle(MetadataInput* input, uint32_t index)
      : input_{input}, index_{index} {}

  /// Returns the decompressed MetadataBuffer. If load() was called
  /// on the parent MetadataInput, returns the already-loaded result.
  /// Otherwise triggers on-demand IO for this section only.
  std::shared_ptr<MetadataBuffer> read();

 private:
  MetadataInput* const input_;
  const uint32_t index_;
};

/// Reads metadata sections from a Nimble file with IO coalescing.
///
/// Supports two-phase IO: enqueue() registers sections and returns a
/// MetadataHandle, load() executes coalesced preadv for all enqueued
/// sections. The handle's read() returns the decompressed MetadataBuffer.
///
/// IO coalescing is controlled by maxCoalesceDistance and maxCoalesceBytes
/// from io::ReaderOptions, shared with data IO settings.
///
/// Two implementations:
///   - DirectMetadataInput: IO coalescing, no caching
///   - CachedMetadataInput: IO coalescing + AsyncDataCache for decompressed
///     metadata with memory/SSD tier support
///
/// NOTE: Not thread-safe. All calls must be serialized by the caller.
class MetadataInput {
 public:
  /// Creates a DirectMetadataInput for non-cached reads.
  static std::unique_ptr<MetadataInput> create(
      velox::ReadFile* file,
      const velox::io::ReaderOptions& readerOptions);

  /// Creates a CachedMetadataInput using fileHandle->uuid as cache key.
  static std::unique_ptr<MetadataInput> create(
      const velox::FileHandle* fileHandle,
      velox::cache::AsyncDataCache* cache,
      const velox::io::ReaderOptions& readerOptions);

  virtual ~MetadataInput() = default;

  /// Returns true if this MetadataInput is backed by a cache.
  virtual bool cached() const {
    return false;
  }

  /// Enqueues a metadata section for coalesced IO. Returns a handle
  /// to retrieve the result later via handle.read().
  MetadataHandle enqueue(const MetadataSection& section);

  /// Executes coalesced preadv for all enqueued sections, then
  /// decompresses and stores each section via the subclass.
  virtual void load() = 0;

  /// Resets all internal state for the next round of enqueue/load/read.
  virtual void reset();

  /// RAII guard that calls load() on construction and reset() on destruction.
  class LoadGuard {
   public:
    explicit LoadGuard(MetadataInput* input) : input_{input} {
      NIMBLE_CHECK_NOT_NULL(input_);
      input_->load();
    }

    ~LoadGuard() {
      input_->reset();
    }

    LoadGuard(const LoadGuard&) = delete;
    LoadGuard& operator=(const LoadGuard&) = delete;

   private:
    MetadataInput* const input_;
  };

  /// Tries to find cached metadata at the given offset. Returns a
  /// MetadataBuffer holding a cache pin if found, nullptr otherwise.
  /// DirectMetadataInput always returns nullptr.
  virtual std::unique_ptr<MetadataBuffer> findCachedMetadata(uint64_t offset);

  /// Caches data at the given offset from multiple contiguous ranges.
  /// Ranges are written sequentially into a single cache entry.
  virtual void cacheMetadata(
      uint64_t cacheOffset,
      std::span<const std::string_view> ranges);

 protected:
  MetadataInput(velox::ReadFile* file, const velox::io::ReaderOptions& options);

  struct LoadedSection {
    explicit LoadedSection(MetadataSection _section) : section{_section} {}

    MetadataSection section;
    std::shared_ptr<MetadataBuffer> buffer;
  };

  /// Reads sections at the given indices from file with IO coalescing,
  /// decompresses, and calls store() for each. Sorts indices in-place.
  void loadFromFile(std::vector<uint32_t>& sectionIndices);

  /// Subclass sets up per-section read buffers and ranges.
  virtual void prepareReadBuffers(
      const std::vector<uint32_t>& sectionIndices) = 0;

  /// Subclass processes loaded buffers into MetadataBuffers.
  virtual void processLoadedBuffers(
      const std::vector<uint32_t>& sectionIndices) = 0;

  /// Decompresses the buffer and calls store(). For uncompressed data,
  /// passes the buffer directly (zero copy).
  std::shared_ptr<MetadataBuffer> decompressAndStore(
      const MetadataSection& section,
      velox::BufferPtr&& buffer);

  /// Subclass creates the MetadataBuffer from a decompressed buffer.
  virtual std::shared_ptr<MetadataBuffer> store(
      const MetadataSection& section,
      velox::BufferPtr&& decompressed) = 0;

  struct IoGroup {
    uint64_t offset;
    std::vector<folly::Range<char*>> ranges;
  };

  // Computes IO group boundaries using coalesceIo, then builds preadv
  // ranges for each group with null-data ranges for gaps.
  // 'readRanges' provides the writable destination for each section.
  std::vector<IoGroup> computeIoGroups(
      const std::vector<uint32_t>& sectionIndices,
      const std::vector<folly::Range<char*>>& readRanges);

  // Dispatches IO groups in parallel via AsyncSource when executor is
  // available, records IO stats, and propagates the first error.
  void executeIoGroups(std::vector<IoGroup>& ioGroups);

  // Returns the uncompressed size, or nullopt if unknown (old files
  // without uncompressed_size — will be deprecated).
  static std::optional<uint32_t> resolveUncompressedSize(
      const MetadataSection& section);

  // Records coalesced IO stats (payload, total read, overread).
  void recordCoalescedIoStats(const velox::CoalesceIoStats& ioStats);

  // RAII guard that measures elapsed time and records it as query thread
  // IO latency when the scope exits.
  class QueryIoLatencyGuard {
   public:
    explicit QueryIoLatencyGuard(velox::io::IoStatistics* ioStats)
        : ioStats_{ioStats}, timer_{&usecs_} {}

    ~QueryIoLatencyGuard() {
      ioStats_->queryThreadIoLatencyUs().increment(usecs_);
    }

   private:
    velox::io::IoStatistics* const ioStats_;
    uint64_t usecs_{0};
    velox::MicrosecondTimer timer_;
  };

  velox::ReadFile* const file_;
  velox::memory::MemoryPool* const pool_;
  // Max gap in bytes between consecutive sections to merge into one IO.
  const int32_t maxCoalesceDistance_;
  // Max total bytes per coalesced IO batch.
  const int64_t maxCoalesceBytes_;
  folly::Executor* const executor_;
  velox::io::IoStatistics* const ioStats_;

  std::vector<LoadedSection> sections_;
  bool loaded_{false};

  // Per-section read state, populated by prepareReadBuffers().
  std::vector<velox::BufferPtr> buffers_;
  std::vector<folly::Range<char*>> readRanges_;

 private:
  virtual std::shared_ptr<MetadataBuffer> read(uint32_t index);

  friend class MetadataHandle;
  friend class test::MetadataInputTestHelper;
};

/// Direct metadata loading with IO coalescing, no caching.
class DirectMetadataInput : public MetadataInput {
 public:
  void load() override;

 private:
  friend class MetadataInput;

  DirectMetadataInput(
      velox::ReadFile* file,
      const velox::io::ReaderOptions& options);

 protected:
  void prepareReadBuffers(const std::vector<uint32_t>& sectionIndices) override;
  void processLoadedBuffers(
      const std::vector<uint32_t>& sectionIndices) override;
  std::shared_ptr<MetadataBuffer> store(
      const MetadataSection& section,
      velox::BufferPtr&& decompressed) override;
};

/// Cached metadata loading with IO coalescing and AsyncDataCache.
/// Checks memory cache and SSD cache before file IO.
/// Caches decompressed metadata in contiguous cache entries.
/// Cache key is the compressed file offset.
class CachedMetadataInput : public MetadataInput {
 public:
  bool cached() const override {
    return true;
  }

  void load() override;

  void reset() override;

  std::unique_ptr<MetadataBuffer> findCachedMetadata(uint64_t offset) override;

  void cacheMetadata(
      uint64_t cacheOffset,
      std::span<const std::string_view> ranges) override;

 protected:
  void prepareReadBuffers(const std::vector<uint32_t>& sectionIndices) override;
  void processLoadedBuffers(
      const std::vector<uint32_t>& sectionIndices) override;
  std::shared_ptr<MetadataBuffer> store(
      const MetadataSection& section,
      velox::BufferPtr&& decompressed) override;

 private:
  // Checks memory cache for all sections. Returns indices of sections
  // not found in cache. Pre-claims exclusive pins for known-size misses.
  std::vector<uint32_t> loadFromCache();

  // Checks SSD cache for memory misses, batch-loads SSD hits into
  // memory cache. Removes loaded indices from missIndices in-place.
  void loadFromSsd(std::vector<uint32_t>& missIndices);

  // Waits for a cache pin to become available via findOrCreate.
  // Blocks until the pin is no longer exclusively held by another thread.
  velox::cache::CachePin acquireCachePin(
      const velox::cache::RawFileCacheKey& key,
      uint32_t size);

  // If the pin is shared (cache hit), validates the entry, records ramHit
  // stats, and returns the MetadataBuffer. Returns nullptr on cache miss.
  std::shared_ptr<MetadataBuffer> tryCacheHit(
      uint32_t expectedSize,
      velox::cache::CachePin& pin);

  // Validates an exclusive cache pin entry, promotes it to shared, and
  // returns a MetadataBuffer. Records the given IO counter stats.
  std::shared_ptr<MetadataBuffer> promoteCachePin(
      const MetadataSection& section,
      velox::cache::CachePin pin,
      velox::io::IoCounter& ioCounter);

  friend class MetadataInput;

  CachedMetadataInput(
      velox::ReadFile* file,
      velox::StringIdLease fileId,
      velox::cache::AsyncDataCache* cache,
      const velox::io::ReaderOptions& options);

  velox::cache::AsyncDataCache* const cache_;
  const velox::StringIdLease fileId_;

  // Pre-claimed exclusive cache pins from loadFromCache(), indexed by
  // section index. Present for sections with known uncompressed size.
  std::vector<std::optional<velox::cache::CachePin>> cachePins_;

  friend class test::CachedMetadataInputTestHelper;
};

} // namespace facebook::nimble
