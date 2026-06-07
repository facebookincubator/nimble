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
#include <string_view>
#include <vector>

#include <fmt/core.h>

#include "velox/common/file/File.h"
#include "velox/common/file/Region.h"
#include "velox/common/io/Options.h"
#include "velox/common/memory/Memory.h"

namespace folly {
class Executor;
} // namespace folly

namespace facebook::nimble {

/// Base class for data I/O with an enqueue + batch load pattern.
///
/// Reads are organized into groups (one per stripe). Within each group,
/// reads are sorted by file offset for coalescing. Across groups,
/// relative order is preserved (stripes are already in file order).
/// After load(), bufferRef() provides zero-copy access to loaded data.
///
/// Two planned implementations:
///   - DirectDataInput: coalesced I/O, no caching
///   - CachedDataInput: coalesced I/O + AsyncDataCache (future)
class DataInput {
 public:
  using Region = velox::common::Region;

  /// Reference to a loaded buffer region. Points into a contiguous
  /// aligned allocation.
  struct BufferRef {
    const char* data{nullptr};
    uint64_t length{0};
  };

  /// Opaque handle keeping all loaded allocations alive.
  /// Callers must hold this until done with all BufferRefs.
  using Handle = std::shared_ptr<void>;

  virtual ~DataInput() = default;

  /// Pre-allocate internal storage for 'numRegions' enqueued reads.
  /// Avoids reallocation during enqueue().
  virtual void reserve(uint32_t numRegions) = 0;

  /// Start a new group (one per stripe). Reads within a group are
  /// sorted by offset; across groups, relative order is preserved.
  virtual void startGroup() = 0;

  /// Enqueue a read in the current group. Returns an index for
  /// bufferRef() after load().
  virtual uint32_t enqueue(Region region) = 0;

  /// Load all enqueued requests via coalesced I/O. Returns a handle
  /// that keeps the loaded data alive. Caller must hold this until
  /// done with all BufferRefs (e.g., capture in IOBuf destructors).
  virtual Handle load() = 0;

  /// Get the loaded buffer for a previously enqueued request.
  virtual const BufferRef& bufferRef(uint32_t index) const = 0;

  /// Release all state (loaded buffers, enqueued requests).
  virtual void clear() = 0;
};

/// Direct data loading with IO coalescing, alignment for direct I/O,
/// and optional parallel reads via executor.
///
/// After coalescing, each I/O group is backed by a non-contiguous
/// Allocation (page-aligned runs). Reads are parallelized across
/// I/O groups via the executor.
///
/// NOTE: DirectDataInput is not thread-safe. Each thread must use its
/// own instance.
class DirectDataInput : public DataInput {
 public:
  static constexpr int32_t kDefaultCoalesceDistance{4 << 10};
  static constexpr int32_t kMaxCoalesceDistance{256 << 10};

  struct Options {
    velox::memory::MemoryPool* pool{nullptr};
    folly::Executor* executor{nullptr};
    std::shared_ptr<velox::io::IoStatistics> ioStats;
    uint64_t alignment{1};
    int32_t maxCoalesceDistance{kDefaultCoalesceDistance};
    int64_t maxCoalesceBytes{velox::io::ReaderOptions::kDefaultCoalesceBytes};
    /// Minimum number of IO groups per executor task. Batching multiple
    /// pread calls into one task reduces executor overhead (task timing,
    /// lambda allocation, scheduling) at the cost of less parallelism.
    int32_t minIoGroupsPerTask{4};
  };

  DirectDataInput(velox::ReadFile* file, const Options& options);

  void reserve(uint32_t numRegions) override;

  void startGroup() override;

  uint32_t enqueue(Region region) override;

  Handle load() override;

  const BufferRef& bufferRef(uint32_t index) const override;

  void clear() override;

  enum class State { kInit, kEnqueuing, kLoaded };

  static std::string_view stateName(State state);

 private:
  struct EnqueuedRegion {
    uint32_t bufferIndex;
    Region region;
  };

  // A coalesced I/O operation covering one or more enqueued regions.
  struct IoGroup {
    // Aligned file offset and size for the coalesced read.
    uint64_t readOffset{0};
    uint64_t readSize{0};
    // Unique physical bytes requested by the logical regions, excluding
    // exact duplicate ranges and alignment/coalescing overread.
    uint64_t payloadSize{0};
    // Byte offset within the shared read buffer.
    uint64_t bufferOffset{0};
    std::span<const EnqueuedRegion> regions;
  };

  // Coalesces sorted regions into physical IO groups. Exact duplicate ranges
  // can share one physical read when the caller maps them to the same bytes.
  std::vector<IoGroup> computeIoGroups(
      const std::vector<EnqueuedRegion>& sortedRegions);

  // Computes the unique payload size for a sorted span of regions. Exact
  // duplicate ranges do not add payload bytes. Returns {payloadSize, lastEnd}
  // where lastEnd is the end offset of the last accepted range.
  static std::pair<uint64_t, uint64_t> computePayloadSize(
      std::span<const EnqueuedRegion> sortedRegions);

  // Computes total read and payload bytes, and sets each group's buffer offset
  // in the shared read buffer. Returns {readBytes, payloadBytes}.
  static std::pair<uint64_t, uint64_t> computeIoSizes(
      std::vector<IoGroup>& ioGroups);

  // Points each logical buffer ref at its slice inside the shared read buffer.
  void populateBufferRefs(
      const std::vector<IoGroup>& ioGroups,
      const char* buffer);

  // Allocates the shared aligned read buffer and returns a handle that frees
  // it.
  std::pair<char*, Handle> allocateBuffer(uint64_t bytes);

  // Executes all physical IO groups, batching executor tasks to reduce
  // scheduling overhead.
  void executeIoGroups(std::vector<IoGroup>& ioGroups, char* buffer);

  uint64_t alignDown(uint64_t value) const {
    return value & ~(alignment_ - 1);
  }

  uint64_t alignUp(uint64_t value) const {
    return (value + alignment_ - 1) & ~(alignment_ - 1);
  }

  // File to read from.
  velox::ReadFile* const file_;
  // Memory pool for allocating the read buffer.
  velox::memory::MemoryPool* const pool_;
  // Executor for parallel I/O across coalesced groups.
  folly::Executor* const executor_;
  // IO statistics for tracking read bytes, latency, and gaps.
  const std::shared_ptr<velox::io::IoStatistics> ioStats_;
  // I/O alignment for file offsets and read sizes.
  const uint64_t alignment_;
  // Allocation alignment passed to allocateAligned, clamped to
  // MemoryAllocator::kMinAlignment.
  const uint64_t allocAlignment_;
  // Max gap in bytes between regions to bridge by coalescing.
  const int32_t maxCoalesceDistance_;
  // Max total bytes per coalesced I/O group.
  const int64_t maxCoalesceBytes_;
  // Min IO groups per executor task.
  const int32_t minIoGroupsPerTask_;

  State state_{State::kInit};

  // --- Enqueue state (flat CSR layout) ---
  // All enqueued regions in a single flat vector. groupOffsets_[i] marks
  // where group i begins; regions within each group are sorted by offset
  // during load().
  std::vector<EnqueuedRegion> regions_;
  // Start index in regions_ for each group.
  std::vector<uint32_t> groupOffsets_;
  // Populated by load() with pointers into the aligned buffer.
  std::vector<BufferRef> bufferRefs_;
};

} // namespace facebook::nimble

template <>
struct fmt::formatter<facebook::nimble::DirectDataInput::State>
    : fmt::formatter<std::string_view> {
  auto format(
      facebook::nimble::DirectDataInput::State state,
      fmt::format_context& ctx) const {
    return fmt::formatter<std::string_view>::format(
        facebook::nimble::DirectDataInput::stateName(state), ctx);
  }
};
