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
#include "dwio/nimble/tablet/DataInput.h"

#include <algorithm>
#include <numeric>

#include "dwio/nimble/common/Exceptions.h"
#include "folly/synchronization/Latch.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/CoalesceIo.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/common/time/Timer.h"

namespace facebook::nimble {

// --- DirectDataInput ---

/*static*/ std::string_view DirectDataInput::stateName(State state) {
  switch (state) {
    case State::kInit:
      return "kInit";
    case State::kEnqueuing:
      return "kEnqueuing";
    case State::kLoaded:
      return "kLoaded";
    default:
      NIMBLE_UNREACHABLE(
          fmt::format("Unknown State: {}", static_cast<int>(state)));
  }
}

DirectDataInput::DirectDataInput(velox::ReadFile* file, const Options& options)
    : file_{file},
      pool_{options.pool},
      executor_{options.executor},
      ioStats_{options.ioStats},
      alignment_{options.alignment},
      allocAlignment_{std::max(
          alignment_,
          static_cast<uint64_t>(
              velox::memory::MemoryAllocator::kMinAlignment))},
      maxCoalesceDistance_{
          std::min(options.maxCoalesceDistance, kMaxCoalesceDistance)},
      maxCoalesceBytes_{options.maxCoalesceBytes},
      minIoGroupsPerTask_{std::max(1, options.minIoGroupsPerTask)} {
  NIMBLE_CHECK_NOT_NULL(file_);
  NIMBLE_CHECK_NOT_NULL(pool_);
  NIMBLE_CHECK_NOT_NULL(executor_);
  NIMBLE_CHECK_NOT_NULL(ioStats_);
  NIMBLE_CHECK_GT(alignment_, 0, "alignment must be positive");
  NIMBLE_CHECK(
      velox::bits::isPowerOfTwo(alignment_), "alignment must be a power of 2");
}

void DirectDataInput::reserve(uint32_t numRegions) {
  NIMBLE_CHECK_EQ(state_, State::kInit);
  regions_.reserve(numRegions);
  bufferRefs_.reserve(numRegions);
}

void DirectDataInput::startGroup() {
  NIMBLE_CHECK_NE(state_, State::kLoaded);
  NIMBLE_CHECK(
      groupOffsets_.empty() ||
          groupOffsets_.back() < static_cast<uint32_t>(regions_.size()),
      "Previous group is empty");
  state_ = State::kEnqueuing;
  groupOffsets_.push_back(static_cast<uint32_t>(regions_.size()));
}

uint32_t DirectDataInput::enqueue(Region region) {
  NIMBLE_CHECK_EQ(state_, State::kEnqueuing);
  NIMBLE_CHECK_GT(region.length, 0, "Read region length must be positive");
  const auto index = static_cast<uint32_t>(bufferRefs_.size());
  bufferRefs_.emplace_back();
  regions_.emplace_back(EnqueuedRegion{index, region});
  return index;
}

/*static*/ std::pair<uint64_t, uint64_t> DirectDataInput::computeIoSizes(
    std::vector<IoGroup>& ioGroups) {
  uint64_t readBytes = 0;
  uint64_t payloadBytes = 0;
  for (auto& group : ioGroups) {
    group.bufferOffset = readBytes;
    readBytes += group.readSize;
    payloadBytes += group.payloadSize;
  }
  return {readBytes, payloadBytes};
}

/*static*/ std::pair<uint64_t, uint64_t> DirectDataInput::computePayloadSize(
    std::span<const EnqueuedRegion> sortedRegions) {
  NIMBLE_CHECK(!sortedRegions.empty(), "IO group must contain a region");

  const auto& firstRegion = sortedRegions.front().region;
  uint64_t prevRegionEnd{firstRegion.offset + firstRegion.length};
  uint64_t payloadSize{firstRegion.length};
  for (size_t i = 1; i < sortedRegions.size(); ++i) {
    const auto& region = sortedRegions[i].region;
    const auto regionEnd = region.offset + region.length;
    if (regionEnd == prevRegionEnd) {
      const auto& prevRegion = sortedRegions[i - 1].region;
      // coalesceIo only keeps exact duplicate ranges in the same IO group so
      // they can share the physical read. Contained and partially overlapping
      // ranges are emitted as separate groups.
      NIMBLE_CHECK_EQ(
          region.offset,
          prevRegion.offset,
          "Only exact duplicate ranges can end at the previous range end");
      NIMBLE_CHECK_EQ(
          region.length,
          prevRegion.length,
          "Only exact duplicate ranges can end at the previous range end");
      continue;
    }

    NIMBLE_CHECK_GE(
        region.offset,
        prevRegionEnd,
        "IO group cannot contain overlapping ranges");
    payloadSize += region.length;
    prevRegionEnd = regionEnd;
  }
  return {payloadSize, prevRegionEnd};
}

std::vector<DirectDataInput::IoGroup> DirectDataInput::computeIoGroups(
    const std::vector<EnqueuedRegion>& sortedRegions) {
  std::vector<int32_t> items(sortedRegions.size());
  std::iota(items.begin(), items.end(), 0);

  int64_t coalescedBytes = 0;
  std::vector<IoGroup> ioGroups;
  ioGroups.reserve(sortedRegions.size());

  const auto coalesceStats =
      velox::coalesceIo<int32_t, char, /*coalesceDuplicateRanges=*/true>(
          items,
          /*maxGap=*/maxCoalesceDistance_,
          /*rangesPerIo=*/std::numeric_limits<int32_t>::max(),
          /*offsetFunc=*/
          [&](int32_t i) -> uint64_t { return sortedRegions[i].region.offset; },
          /*sizeFunc=*/
          [&](int32_t i) -> int32_t {
            return static_cast<int32_t>(sortedRegions[i].region.length);
          },
          /*numRanges=*/
          [&](int32_t i) -> int32_t {
            const auto size =
                static_cast<int64_t>(sortedRegions[i].region.length);
            if (coalescedBytes + size > maxCoalesceBytes_) {
              coalescedBytes = 0;
              return velox::kNoCoalesce;
            }
            coalescedBytes += size;
            return 1;
          },
          /*addRanges=*/
          [](const int32_t& /*i*/, std::vector<char>& ranges) {
            ranges.push_back(0);
          },
          /*skipRange=*/
          [](int32_t /*gap*/, std::vector<char>& /*ranges*/) {},
          /*ioFunc=*/
          [&](const std::vector<int32_t>& /*items*/,
              int32_t begin,
              int32_t end,
              uint64_t /*offset*/,
              const std::vector<char>& /*ranges*/) {
            coalescedBytes = 0;
            IoGroup group;
            const auto groupRegions = std::span<const EnqueuedRegion>(
                &sortedRegions[begin], static_cast<size_t>(end - begin));
            const auto [payloadSize, lastEnd] =
                computePayloadSize(groupRegions);
            group.readOffset = alignDown(sortedRegions[begin].region.offset);
            group.readSize = alignUp(lastEnd) - group.readOffset;
            group.payloadSize = payloadSize;
            group.regions = groupRegions;
            ioGroups.emplace_back(group);
          });

  ioStats_->readGap().merge(coalesceStats.gaps);
  return ioGroups;
}

void DirectDataInput::populateBufferRefs(
    const std::vector<IoGroup>& ioGroups,
    const char* buffer) {
  for (const auto& group : ioGroups) {
    for (const auto& region : group.regions) {
      auto& ref = bufferRefs_[region.bufferIndex];
      ref.data = buffer + group.bufferOffset +
          (region.region.offset - group.readOffset);
      ref.length = region.region.length;
    }
  }
}

std::pair<char*, DataInput::Handle> DirectDataInput::allocateBuffer(
    uint64_t bytes) {
  velox::common::testutil::TestValue::adjust(
      "facebook::nimble::DirectDataInput::allocateBuffer", &bytes);
  auto* buffer = static_cast<char*>(
      pool_->allocateAligned(static_cast<int64_t>(bytes), allocAlignment_));
  NIMBLE_DCHECK(
      reinterpret_cast<uintptr_t>(buffer) % alignment_ == 0,
      "allocateAligned returned unaligned buffer");
  auto handle = Handle(
      buffer, [pool = pool_, bytes, allocAlignment = allocAlignment_](void* p) {
        pool->freeAligned(p, static_cast<int64_t>(bytes), allocAlignment);
      });
  return {buffer, std::move(handle)};
}

void DirectDataInput::executeIoGroups(
    std::vector<IoGroup>& ioGroups,
    char* buffer) {
  NIMBLE_CHECK(!ioGroups.empty());
  const auto numGroups = ioGroups.size();

  // Batch IO groups into tasks. Each task reads multiple groups
  // sequentially to reduce executor overhead.
  const auto batchSize = static_cast<size_t>(minIoGroupsPerTask_);
  const auto numTasks = velox::bits::divRoundUp(numGroups, batchSize);

  std::exception_ptr readError;
  std::mutex errorMutex;
  folly::Latch latch(numTasks);

  for (size_t t = 0; t < numTasks; ++t) {
    const auto start = t * batchSize;
    const auto end = std::min(start + batchSize, numGroups);
    executor_->add([this,
                    &ioGroups,
                    buffer,
                    start,
                    end,
                    &readError,
                    &errorMutex,
                    &latch]() {
      try {
        for (size_t i = start; i < end; ++i) {
          velox::common::testutil::TestValue::adjust(
              "facebook::nimble::DirectDataInput::executeIoGroups", this);
          file_->pread(
              ioGroups[i].readOffset,
              ioGroups[i].readSize,
              buffer + ioGroups[i].bufferOffset);
        }
      } catch (...) {
        std::lock_guard<std::mutex> lock(errorMutex);
        if (readError == nullptr) {
          readError = std::current_exception();
        }
      }
      latch.count_down();
    });
  }

  latch.wait();

  if (readError != nullptr) {
    std::rethrow_exception(readError);
  }
}

DataInput::Handle DirectDataInput::load() {
  NIMBLE_CHECK_EQ(state_, State::kEnqueuing);
  NIMBLE_CHECK(!bufferRefs_.empty(), "No regions enqueued");
  state_ = State::kLoaded;

  // Sort within each group segment in-place. Groups are expected to be
  // enqueued in non-overlapping file order, so boundary checks ensure the
  // flat vector remains globally sorted without sorting across groups.
  const auto numGroups = groupOffsets_.size();
  uint64_t prevGroupEnd{0};
  for (size_t i = 0; i < numGroups; ++i) {
    const auto start = groupOffsets_[i];
    const auto end = (i + 1 < numGroups)
        ? groupOffsets_[i + 1]
        : static_cast<uint32_t>(regions_.size());
    NIMBLE_CHECK_LT(start, end, "Read group must contain a region");
    std::sort(
        regions_.begin() + start,
        regions_.begin() + end,
        [](const EnqueuedRegion& a, const EnqueuedRegion& b) {
          return a.region.offset < b.region.offset;
        });

    if (i > 0) {
      NIMBLE_CHECK_GE(
          regions_[start].region.offset,
          prevGroupEnd,
          "Read groups must be sorted and non-overlapping by file offset");
    }

    const auto& lastRegion = regions_[end - 1].region;
    prevGroupEnd = lastRegion.offset + lastRegion.length;
  }

  auto ioGroups = computeIoGroups(regions_);

  const auto [readBytes, payloadBytes] = computeIoSizes(ioGroups);
  NIMBLE_CHECK_GE(readBytes, payloadBytes);
  NIMBLE_CHECK_EQ(readBytes % alignment_, 0);

  auto [buffer, handle] = allocateBuffer(readBytes);

  uint64_t ioUs{0};
  {
    velox::MicrosecondTimer ioTimer(&ioUs);
    executeIoGroups(ioGroups, buffer);
  }

  for (const auto& group : ioGroups) {
    ioStats_->read().increment(group.readSize);
    ioStats_->incRawBytesRead(group.payloadSize);
    ioStats_->incRawOverreadBytes(group.readSize - group.payloadSize);
  }
  ioStats_->storageReadLatencyUs().increment(ioUs);
  ioStats_->incTotalScanTimeNs(ioUs * 1'000);

  populateBufferRefs(ioGroups, buffer);

  regions_.clear();
  return handle;
}

const DataInput::BufferRef& DirectDataInput::bufferRef(uint32_t index) const {
  NIMBLE_CHECK_EQ(state_, State::kLoaded);
  NIMBLE_DCHECK_LT(index, bufferRefs_.size());
  return bufferRefs_[index];
}

void DirectDataInput::clear() {
  state_ = State::kInit;
  regions_.clear();
  groupOffsets_.clear();
  bufferRefs_.clear();
}

} // namespace facebook::nimble
