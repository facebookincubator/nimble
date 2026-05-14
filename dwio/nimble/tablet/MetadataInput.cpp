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
#include "dwio/nimble/tablet/MetadataInput.h"

#include <algorithm>

#include <numeric>
#include "velox/common/caching/FileHandle.h"

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/tablet/Compression.h"
#include "folly/Range.h"
#include "folly/ScopeGuard.h"
#include "folly/executors/QueuedImmediateExecutor.h"
#include "folly/io/Cursor.h"
#include "velox/common/base/AsyncSource.h"
#include "velox/common/base/CoalesceIo.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/common/time/Timer.h"

namespace facebook::nimble {

namespace {

inline int64_t sectionGap(
    const MetadataSection& prev,
    const MetadataSection& next) {
  const int64_t gap = static_cast<int64_t>(next.offset()) -
      static_cast<int64_t>(prev.offset() + prev.size());
  NIMBLE_CHECK_GE(gap, 0, "Metadata sections overlap");
  return gap;
}

} // namespace

// --- MetadataHandle ---

std::shared_ptr<MetadataBuffer> MetadataHandle::read() {
  return input_->read(index_);
}

// --- MetadataInput ---

std::unique_ptr<MetadataInput> MetadataInput::create(
    velox::ReadFile* file,
    const velox::io::ReaderOptions& readerOptions) {
  NIMBLE_CHECK_NOT_NULL(file);
  return std::unique_ptr<MetadataInput>(
      new DirectMetadataInput(file, readerOptions));
}

std::unique_ptr<MetadataInput> MetadataInput::create(
    const velox::FileHandle* fileHandle,
    velox::cache::AsyncDataCache* cache,
    const velox::io::ReaderOptions& readerOptions) {
  NIMBLE_CHECK_NOT_NULL(fileHandle);
  NIMBLE_CHECK_NOT_NULL(cache);
  auto* file = fileHandle->file.get();
  NIMBLE_CHECK_NOT_NULL(file);
  return std::unique_ptr<MetadataInput>(
      new CachedMetadataInput(file, fileHandle->uuid, cache, readerOptions));
}

MetadataInput::MetadataInput(
    velox::ReadFile* file,
    const velox::io::ReaderOptions& options)
    : file_{file},
      pool_{&options.memoryPool()},
      maxCoalesceDistance_{options.maxCoalesceDistance()},
      maxCoalesceBytes_{options.maxCoalesceBytes()},
      executor_{options.ioExecutor().get()},
      ioStats_{options.metadataIoStats()} {
  NIMBLE_CHECK_NOT_NULL(ioStats_);
}

MetadataHandle MetadataInput::enqueue(const MetadataSection& section) {
  NIMBLE_CHECK(!loaded_, "enqueue() not allowed after load(); call reset()");
  const auto index = static_cast<uint32_t>(sections_.size());
  sections_.emplace_back(section);
  return MetadataHandle{this, index};
}

void MetadataInput::reset() {
  sections_.clear();
  loaded_ = false;
}

std::unique_ptr<MetadataBuffer> MetadataInput::findCachedMetadata(
    uint64_t /*offset*/) {
  NIMBLE_UNREACHABLE("findCachedMetadata requires CachedMetadataInput");
}

void MetadataInput::cacheMetadata(
    uint64_t /*cacheOffset*/,
    std::span<const std::string_view> /*ranges*/) {
  NIMBLE_UNREACHABLE("cacheMetadata requires CachedMetadataInput");
}

std::shared_ptr<MetadataBuffer> MetadataInput::read(uint32_t index) {
  NIMBLE_CHECK(loaded_, "load() must be called before read()");
  NIMBLE_CHECK_LT(index, sections_.size(), "Invalid metadata handle index");
  NIMBLE_CHECK_NOT_NULL(
      sections_[index].buffer,
      "Metadata buffer already consumed or not loaded");
  return std::move(sections_[index].buffer);
}

std::shared_ptr<MetadataBuffer> MetadataInput::decompressAndStore(
    const MetadataSection& section,
    velox::BufferPtr&& buffer) {
  NIMBLE_CHECK_EQ(
      section.size(), buffer->size(), "Buffer size mismatch with section size");
  if (section.compressionType() == CompressionType::Uncompressed) {
    return store(section, std::move(buffer));
  }
  NIMBLE_CHECK_EQ(
      static_cast<int>(section.compressionType()),
      static_cast<int>(CompressionType::Zstd),
      "Unsupported metadata compression type");
  std::string_view data{buffer->as<char>(), buffer->size()};
  return store(section, ZstdCompression::uncompress(data, pool_));
}

std::optional<uint32_t> MetadataInput::resolveUncompressedSize(
    const MetadataSection& section) {
  if (section.uncompressedSize().has_value()) {
    return section.uncompressedSize();
  }
  if (section.compressionType() == CompressionType::Uncompressed) {
    return section.size();
  }
  return std::nullopt;
}

std::vector<MetadataInput::IoGroup> MetadataInput::computeIoGroups(
    const std::vector<uint32_t>& sectionIndices,
    const std::vector<folly::Range<char*>>& readRanges) {
  std::vector<int32_t> items(sectionIndices.size());
  std::iota(items.begin(), items.end(), 0);

  int64_t coalescedBytes = 0;
  std::vector<int32_t> groupEnds;

  const auto ioStats = velox::coalesceIo<int32_t, char>(
      items,
      /*maxGap=*/maxCoalesceDistance_,
      /*rangesPerIo=*/std::numeric_limits<int32_t>::max(),
      /*offsetFunc=*/
      [&](int32_t i) -> uint64_t {
        return sections_[sectionIndices[i]].section.offset();
      },
      /*sizeFunc=*/
      [&](int32_t i) -> int32_t {
        return sections_[sectionIndices[i]].section.size();
      },
      /*numRanges=*/
      [&](int32_t i) -> int32_t {
        const auto size = sections_[sectionIndices[i]].section.size();
        if (coalescedBytes + size > maxCoalesceBytes_) {
          coalescedBytes = 0;
          return velox::kNoCoalesce;
        }
        coalescedBytes += size;
        return 1;
      },
      /*addRanges=*/
      [&](const int32_t& /*i*/, std::vector<char>& ranges) {
        // Dummy range so coalesceIo sees non-empty ranges for kNoCoalesce
        // flush check.
        ranges.push_back(0);
      },
      /*skipRange=*/
      [&](int32_t /*gap*/, std::vector<char>& /*ranges*/) {},
      /*ioFunc=*/
      [&](const std::vector<int32_t>& /*items*/,
          int32_t /*begin*/,
          int32_t end,
          uint64_t /*offset*/,
          const std::vector<char>& /*ranges*/) {
        coalescedBytes = 0;
        groupEnds.push_back(end);
      });

  recordCoalescedIoStats(ioStats);
  velox::common::testutil::TestValue::adjust(
      "facebook::nimble::MetadataInput::computeIoGroups",
      const_cast<velox::CoalesceIoStats*>(&ioStats));

  std::vector<IoGroup> ioGroups;
  int32_t groupStart = 0;
  for (const auto groupEnd : groupEnds) {
    IoGroup group;
    group.offset = sections_[sectionIndices[groupStart]].section.offset();
    auto lastEnd = group.offset;
    for (int32_t i = groupStart; i < groupEnd; ++i) {
      const auto& section = sections_[sectionIndices[i]].section;
      if (section.offset() > lastEnd) {
        const auto gap = section.offset() - lastEnd;
        group.ranges.emplace_back(
            nullptr, reinterpret_cast<char*>(static_cast<uint64_t>(gap)));
      }
      group.ranges.emplace_back(readRanges[i]);
      lastEnd = section.offset() + section.size();
    }
    ioGroups.emplace_back(std::move(group));
    groupStart = groupEnd;
  }
  return ioGroups;
}

void MetadataInput::executeIoGroups(std::vector<IoGroup>& ioGroups) {
  std::vector<std::unique_ptr<velox::AsyncSource<folly::Unit>>> asyncIos;
  asyncIos.reserve(ioGroups.size());
  for (const auto& group : ioGroups) {
    asyncIos.emplace_back(
        std::make_unique<velox::AsyncSource<folly::Unit>>([this, &group]() {
          uint64_t storageReadUs{0};
          {
            velox::MicrosecondTimer ioTimer(&storageReadUs);
            file_->preadv(group.offset, group.ranges);
          }
          ioStats_->storageReadLatencyUs().increment(storageReadUs);
          ioStats_->incTotalScanTimeNs(storageReadUs * 1'000);
          return std::make_unique<folly::Unit>();
        }));
  }

  QueryIoLatencyGuard queryIoGuard{ioStats_};
  if (executor_ != nullptr && asyncIos.size() > 1) {
    for (size_t i = 0; i + 1 < asyncIos.size(); ++i) {
      executor_->add([&asyncIo = asyncIos[i]]() { asyncIo->prepare(); });
    }
  }
  std::exception_ptr error;
  for (auto& asyncIo : asyncIos) {
    try {
      asyncIo->move();
    } catch (...) {
      if (error == nullptr) {
        error = std::current_exception();
      }
    }
  }
  if (error != nullptr) {
    std::rethrow_exception(error);
  }
}

void MetadataInput::loadFromFile(std::vector<uint32_t>& sectionIndices) {
  if (sectionIndices.empty()) {
    return;
  }

  std::sort(sectionIndices.begin(), sectionIndices.end(), [&](auto a, auto b) {
    return sections_[a].section.offset() < sections_[b].section.offset();
  });

  prepareReadBuffers(sectionIndices);
  SCOPE_EXIT {
    buffers_.clear();
    readRanges_.clear();
  };
  auto ioGroups = computeIoGroups(sectionIndices, readRanges_);
  executeIoGroups(ioGroups);
  processLoadedBuffers(sectionIndices);
}

void MetadataInput::recordCoalescedIoStats(
    const velox::CoalesceIoStats& ioStats) {
  ioStats_->read().increment(ioStats.payloadBytes + ioStats.extraBytes);
  ioStats_->incRawBytesRead(ioStats.payloadBytes);
  ioStats_->incRawOverreadBytes(ioStats.extraBytes);
}

// --- DirectMetadataInput ---

DirectMetadataInput::DirectMetadataInput(
    velox::ReadFile* file,
    const velox::io::ReaderOptions& options)
    : MetadataInput(file, options) {}

void DirectMetadataInput::load() {
  NIMBLE_CHECK(!loaded_, "load() already called; call reset() first");
  NIMBLE_CHECK(!sections_.empty(), "No sections enqueued");
  std::vector<uint32_t> sectionIndices(sections_.size());
  std::iota(sectionIndices.begin(), sectionIndices.end(), 0);
  loadFromFile(sectionIndices);
  loaded_ = true;
}

void DirectMetadataInput::prepareReadBuffers(
    const std::vector<uint32_t>& sectionIndices) {
  buffers_.resize(sectionIndices.size());
  readRanges_.resize(sectionIndices.size());
  for (size_t i = 0; i < sectionIndices.size(); ++i) {
    const auto size = sections_[sectionIndices[i]].section.size();
    buffers_[i] = velox::AlignedBuffer::allocateExact<char>(size, pool_);
    readRanges_[i] = {buffers_[i]->asMutable<char>(), size};
  }
}

void DirectMetadataInput::processLoadedBuffers(
    const std::vector<uint32_t>& sectionIndices) {
  NIMBLE_CHECK_EQ(buffers_.size(), sectionIndices.size());
  for (size_t i = 0; i < sectionIndices.size(); ++i) {
    auto& loaded = sections_[sectionIndices[i]];
    loaded.buffer = decompressAndStore(loaded.section, std::move(buffers_[i]));
  }
}

std::shared_ptr<MetadataBuffer> DirectMetadataInput::store(
    const MetadataSection& /*section*/,
    velox::BufferPtr&& decompressed) {
  return std::make_shared<MetadataBuffer>(std::move(decompressed));
}

// --- CachedMetadataInput ---

CachedMetadataInput::CachedMetadataInput(
    velox::ReadFile* file,
    velox::StringIdLease fileId,
    velox::cache::AsyncDataCache* cache,
    const velox::io::ReaderOptions& options)
    : MetadataInput(file, options), cache_{cache}, fileId_{std::move(fileId)} {
  NIMBLE_CHECK_NOT_NULL(cache_);
}

void CachedMetadataInput::reset() {
  MetadataInput::reset();
  cachePins_.clear();
}

velox::cache::CachePin CachedMetadataInput::acquireCachePin(
    const velox::cache::RawFileCacheKey& key,
    uint32_t size) {
  for (;;) {
    folly::SemiFuture<bool> waitFuture{false};
    auto pin =
        cache_->findOrCreate(key, size, /*contiguous=*/true, &waitFuture);
    if (!pin.empty()) {
      return pin;
    }
    uint64_t waitUs{0};
    {
      velox::MicrosecondTimer timer(&waitUs);
      auto& exec = folly::QueuedImmediateExecutor::instance();
      std::move(waitFuture).via(&exec).wait();
    }
    ioStats_->queryThreadIoLatencyUs().increment(waitUs);
    ioStats_->cacheWaitLatencyUs().increment(waitUs);
  }
}

std::shared_ptr<MetadataBuffer> CachedMetadataInput::tryCacheHit(
    uint32_t expectedSize,
    velox::cache::CachePin& pin) {
  auto* entry = pin.checkedEntry();
  if (!entry->isShared()) {
    return nullptr;
  }
  NIMBLE_CHECK(
      entry->hasContiguousData(), "Cached entry must have contiguous data");
  NIMBLE_CHECK_EQ(
      entry->size(),
      expectedSize,
      "Cached entry size mismatch with expected size");
  ioStats_->ramHit().increment(entry->size());
  return std::make_shared<MetadataBuffer>(std::move(pin));
}

std::shared_ptr<MetadataBuffer> CachedMetadataInput::promoteCachePin(
    const MetadataSection& section,
    velox::cache::CachePin pin,
    velox::io::IoCounter& ioCounter) {
  auto* entry = pin.checkedEntry();
  NIMBLE_CHECK(entry->isExclusive(), "Expected exclusive cache pin");
  NIMBLE_CHECK(
      entry->hasContiguousData(), "Cache entry must have contiguous data");
  NIMBLE_CHECK_GE(
      entry->size(), section.size(), "Cache entry smaller than section size");
  const auto uncompressedSize = resolveUncompressedSize(section);
  if (uncompressedSize.has_value()) {
    NIMBLE_CHECK_EQ(
        entry->size(),
        uncompressedSize.value(),
        "Cache entry size mismatch with uncompressed size");
  }
  ioCounter.increment(entry->size());
  entry->setExclusiveToShared();
  return std::make_shared<MetadataBuffer>(std::move(pin));
}

void CachedMetadataInput::loadFromSsd(std::vector<uint32_t>& missIndices) {
  if (missIndices.empty()) {
    return;
  }
  auto* ssdCache = cache_->ssdCache();
  if (ssdCache == nullptr) {
    return;
  }

  struct SsdHit {
    uint32_t sectionIndex;
    velox::cache::SsdPin ssdPin;
    velox::cache::CachePin cachePin;
  };
  std::vector<SsdHit> ssdHits;
  std::vector<uint32_t> remainingMissIndices;
  remainingMissIndices.reserve(missIndices.size());

  auto& ssdFile = ssdCache->file(fileId_.id());
  for (const auto index : missIndices) {
    auto& loaded = sections_[index];
    const velox::cache::RawFileCacheKey key{
        fileId_.id(), loaded.section.offset()};

    auto ssdPin = ssdFile.find(key);
    if (ssdPin.empty()) {
      remainingMissIndices.emplace_back(index);
      continue;
    }

    const auto ssdSize = ssdPin.run().size();
    const auto uncompressedSize = resolveUncompressedSize(loaded.section);
    if (uncompressedSize.has_value()) {
      NIMBLE_CHECK_EQ(
          ssdSize,
          uncompressedSize.value(),
          "SSD entry size mismatch with expected uncompressed size");
    }

    // Allocate memory cache entry for loading from SSD.
    auto pin = acquireCachePin(key, ssdSize);
    auto buffer = tryCacheHit(ssdSize, pin);
    if (buffer != nullptr) {
      loaded.buffer = std::move(buffer);
      continue;
    }
    ssdHits.emplace_back(SsdHit{index, std::move(ssdPin), std::move(pin)});
  }

  missIndices = std::move(remainingMissIndices);

  if (ssdHits.empty()) {
    return;
  }

  // Batch SsdFile::load() for all SSD hits.
  std::vector<velox::cache::SsdPin> ssdPins;
  std::vector<velox::cache::CachePin> cachePins;
  ssdPins.reserve(ssdHits.size());
  cachePins.reserve(ssdHits.size());
  for (auto& hit : ssdHits) {
    ssdPins.emplace_back(std::move(hit.ssdPin));
    cachePins.emplace_back(std::move(hit.cachePin));
  }
  uint64_t ssdLoadUs{0};
  {
    velox::MicrosecondTimer timer(&ssdLoadUs);
    ssdFile.load(ssdPins, cachePins);
  }
  ioStats_->queryThreadIoLatencyUs().increment(ssdLoadUs);
  ioStats_->ssdCacheReadLatencyUs().increment(ssdLoadUs);
  for (uint32_t i = 0; i < ssdHits.size(); ++i) {
    const auto& section = sections_[ssdHits[i].sectionIndex].section;
    sections_[ssdHits[i].sectionIndex].buffer =
        promoteCachePin(section, std::move(cachePins[i]), ioStats_->ssdRead());
  }
}

std::vector<uint32_t> CachedMetadataInput::loadFromCache() {
  NIMBLE_CHECK(cachePins_.empty(), "Cache pins not cleared from previous load");
  cachePins_.resize(sections_.size());
  std::vector<uint32_t> missIndices;
  missIndices.reserve(sections_.size());
  for (uint32_t index = 0; index < sections_.size(); ++index) {
    const auto& section = sections_[index].section;
    const velox::cache::RawFileCacheKey key{fileId_.id(), section.offset()};
    const auto uncompressedSize = resolveUncompressedSize(section);

    if (uncompressedSize.has_value()) {
      auto pin = acquireCachePin(key, uncompressedSize.value());
      auto buffer = tryCacheHit(uncompressedSize.value(), pin);
      if (buffer != nullptr) {
        sections_[index].buffer = std::move(buffer);
        continue;
      }
      cachePins_[index] = std::move(pin);
      missIndices.push_back(index);
    } else {
      missIndices.push_back(index);
    }
  }
  return missIndices;
}

void CachedMetadataInput::load() {
  NIMBLE_CHECK(!loaded_, "load() already called; call reset() first");
  NIMBLE_CHECK(!sections_.empty(), "No sections enqueued");

  auto missIndices = loadFromCache();
  loadFromSsd(missIndices);
  loadFromFile(missIndices);
  loaded_ = true;
}

void CachedMetadataInput::prepareReadBuffers(
    const std::vector<uint32_t>& sectionIndices) {
  NIMBLE_CHECK_EQ(
      cachePins_.size(),
      sections_.size(),
      "Cache pins size mismatch with sections");

  // For uncompressed sections with cache pins, read directly into the
  // pin's contiguous data. For compressed sections or sections without
  // pins, allocate a temporary buffer.
  buffers_.resize(sectionIndices.size());
  readRanges_.resize(sectionIndices.size());
  for (size_t i = 0; i < sectionIndices.size(); ++i) {
    const auto sectionIndex = sectionIndices[i];
    const auto& section = sections_[sectionIndex].section;
    const auto& pin = cachePins_[sectionIndex];
    if (pin.has_value() &&
        section.compressionType() == CompressionType::Uncompressed) {
      NIMBLE_CHECK(
          pin->entry()->hasContiguousData(),
          "Cache entry must have contiguous data");
      readRanges_[i] = {pin->entry()->contiguousData(), section.size()};
    } else {
      buffers_[i] = velox::AlignedBuffer::allocate<char>(section.size(), pool_);
      readRanges_[i] = {buffers_[i]->asMutable<char>(), section.size()};
    }
  }
}

void CachedMetadataInput::processLoadedBuffers(
    const std::vector<uint32_t>& sectionIndices) {
  NIMBLE_CHECK_EQ(buffers_.size(), sectionIndices.size());
  NIMBLE_CHECK_EQ(cachePins_.size(), sections_.size());
  SCOPE_EXIT {
    cachePins_.clear();
  };

  for (size_t i = 0; i < sectionIndices.size(); ++i) {
    const auto sectionIndex = sectionIndices[i];
    auto& loaded = sections_[sectionIndex];
    auto& pin = cachePins_[sectionIndex];
    if (pin.has_value()) {
      if (loaded.section.compressionType() != CompressionType::Uncompressed) {
        NIMBLE_CHECK_NOT_NULL(buffers_[i]);
        NIMBLE_CHECK(
            pin->entry()->hasContiguousData(),
            "Cache entry must have contiguous data");
        std::string_view compressed{
            buffers_[i]->as<char>(), loaded.section.size()};
        ZstdCompression::uncompress(
            compressed, pin->entry()->contiguousData(), pin->entry()->size());
      }
      loaded.buffer =
          promoteCachePin(loaded.section, std::move(*pin), ioStats_->read());
    } else {
      NIMBLE_CHECK(
          !loaded.section.uncompressedSize().has_value(),
          "Section without cache pin should not have uncompressed size set");
      loaded.buffer =
          decompressAndStore(loaded.section, std::move(buffers_[i]));
    }
  }
}

std::shared_ptr<MetadataBuffer> CachedMetadataInput::store(
    const MetadataSection& section,
    velox::BufferPtr&& decompressed) {
  // NOTE: This path handles sections without pre-claimed cache pins (old files
  // without uncompressed_size). Sections with pins are handled directly in
  // loadFromFile. This will be deprecated once all nimble metadata sections
  // include uncompressed size.
  const velox::cache::RawFileCacheKey key{fileId_.id(), section.offset()};
  auto pin = acquireCachePin(key, decompressed->size());
  auto buffer = tryCacheHit(decompressed->size(), pin);
  if (buffer != nullptr) {
    return buffer;
  }

  auto* entry = pin.checkedEntry();
  NIMBLE_CHECK(
      entry->hasContiguousData(), "Cache entry must have contiguous data");
  NIMBLE_CHECK_EQ(
      entry->size(),
      decompressed->size(),
      "Cache entry size mismatch with decompressed size");
  auto* destination = entry->contiguousData();
  std::memcpy(destination, decompressed->as<char>(), decompressed->size());
  entry->setExclusiveToShared();
  return std::make_shared<MetadataBuffer>(std::move(pin));
}

std::unique_ptr<MetadataBuffer> CachedMetadataInput::findCachedMetadata(
    uint64_t offset) {
  for (;;) {
    const velox::cache::RawFileCacheKey key{fileId_.id(), offset};
    folly::SemiFuture<bool> waitFuture{false};
    auto pin = cache_->findOrCreate(key, 0, /*contiguous=*/true, &waitFuture);
    if (pin.empty()) {
      if (!waitFuture.valid()) {
        return nullptr;
      }
      uint64_t waitUs{0};
      {
        velox::MicrosecondTimer timer(&waitUs);
        auto& exec = folly::QueuedImmediateExecutor::instance();
        std::move(waitFuture).via(&exec).wait();
      }
      ioStats_->queryThreadIoLatencyUs().increment(waitUs);
      ioStats_->cacheWaitLatencyUs().increment(waitUs);
      continue;
    }
    auto* entry = pin.checkedEntry();
    if (!entry->isShared()) {
      return nullptr;
    }
    NIMBLE_CHECK(
        entry->hasContiguousData(), "Cached entry must have contiguous data");
    ioStats_->ramHit().increment(entry->size());
    return std::make_unique<MetadataBuffer>(std::move(pin));
  }
}

void CachedMetadataInput::cacheMetadata(
    uint64_t cacheOffset,
    std::span<const std::string_view> ranges) {
  uint64_t totalSize = 0;
  for (const auto& range : ranges) {
    totalSize += range.size();
  }
  const velox::cache::RawFileCacheKey key{fileId_.id(), cacheOffset};
  auto pin = acquireCachePin(key, totalSize);
  velox::common::testutil::TestValue::adjust(
      "facebook::nimble::CachedMetadataInput::cacheMetadata", &pin);
  auto* entry = pin.checkedEntry();
  if (entry->isShared()) {
    return;
  }
  NIMBLE_CHECK(
      entry->hasContiguousData(), "Cache entry must have contiguous data");
  NIMBLE_CHECK_EQ(entry->size(), totalSize);
  auto* dest = entry->contiguousData();
  for (const auto& range : ranges) {
    std::memcpy(dest, range.data(), range.size());
    dest += range.size();
  }
  entry->setExclusiveToShared();
}

} // namespace facebook::nimble
