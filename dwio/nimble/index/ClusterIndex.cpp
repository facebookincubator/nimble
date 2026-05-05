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

#include "dwio/nimble/index/ClusterIndex.h"

#include <algorithm>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/index/KeyChunkDecoder.h"
#include "dwio/nimble/tablet/ClusterIndexGenerated.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "folly/ScopeGuard.h"
#include "folly/json/json.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::nimble::index {

namespace {

const serialization::ClusterIndex* getIndexRoot(const Section& rootSection) {
  const auto* indexRoot = flatbuffers::GetRoot<serialization::ClusterIndex>(
      rootSection.content().data());
  NIMBLE_CHECK_NOT_NULL(indexRoot);
  return indexRoot;
}

uint32_t getIndexPartitionCount(const serialization::ClusterIndex* indexRoot) {
  const auto* indexPartitions = indexRoot->index_partitions();
  NIMBLE_CHECK_NOT_NULL(indexPartitions);
  NIMBLE_CHECK_GT(indexPartitions->size(), 0, "ClusterIndex cannot be empty");
  return indexPartitions->size();
}

std::string_view getFirstKey(const serialization::ClusterIndex* indexRoot) {
  const auto* keys = indexRoot->partition_keys();
  NIMBLE_CHECK_NOT_NULL(keys);
  NIMBLE_CHECK_GT(
      keys->size(),
      1,
      "partition_keys must have at least 2 entries (firstKey + one lastKey per partition)");
  return keys->Get(0)->string_view();
}

std::string_view getLastKey(const serialization::ClusterIndex* indexRoot) {
  const auto* keys = indexRoot->partition_keys();
  NIMBLE_CHECK_NOT_NULL(keys);
  NIMBLE_CHECK_GT(keys->size(), 1);
  return keys->Get(keys->size() - 1)->string_view();
}

std::vector<std::string_view> getPartitionLastKeys(
    const serialization::ClusterIndex* indexRoot) {
  const auto* keys = indexRoot->partition_keys();
  NIMBLE_CHECK_NOT_NULL(keys);
  NIMBLE_CHECK_GT(keys->size(), 1);
  std::vector<std::string_view> result;
  result.reserve(keys->size() - 1);
  for (uint32_t i = 1; i < keys->size(); ++i) {
    result.emplace_back(keys->Get(i)->string_view());
  }
  return result;
}

std::vector<std::string> getIndexColumns(
    const serialization::ClusterIndex* indexRoot) {
  const auto* indexColumns = indexRoot->index_columns();
  NIMBLE_CHECK_NOT_NULL(indexColumns);
  std::vector<std::string> result;
  result.reserve(indexColumns->size());
  for (const auto* column : *indexColumns) {
    result.emplace_back(column->string_view());
  }
  return result;
}

std::vector<SortOrder> getSortOrders(
    const serialization::ClusterIndex* indexRoot) {
  const auto* sortOrders = indexRoot->sort_orders();
  NIMBLE_CHECK_NOT_NULL(sortOrders);
  std::vector<SortOrder> result;
  result.reserve(sortOrders->size());
  for (const auto* sortOrder : *sortOrders) {
    const auto sv = sortOrder->string_view();
    try {
      result.emplace_back(SortOrder::deserialize(folly::parseJson(sv)));
    } catch (const folly::json::parse_error&) {
      // Backward compatibility: old files stored sort orders as raw strings
      // (e.g., "ASC NULLS FIRST") instead of JSON. Default to ascending.
      result.emplace_back(SortOrder{.ascending = true});
    }
  }
  return result;
}

// Builds cumulative row offsets from partition_row_counts in the FlatBuffer.
// Returns a prefix-sum vector of size numPartitions + 1.
std::vector<uint32_t> getPartitionRows(
    const serialization::ClusterIndex* indexRoot) {
  const auto* rowCounts = indexRoot->partition_row_counts();
  NIMBLE_CHECK_NOT_NULL(rowCounts);
  NIMBLE_CHECK_GT(rowCounts->size(), 0, "partition_row_counts cannot be empty");

  std::vector<uint32_t> offsets;
  offsets.reserve(rowCounts->size() + 1);
  offsets.emplace_back(0);
  for (uint32_t i = 0; i < rowCounts->size(); ++i) {
    const auto count = static_cast<uint32_t>(rowCounts->Get(i));
    NIMBLE_CHECK_GT(count, 0, "Partition {} has zero rows", i);
    offsets.emplace_back(offsets.back() + count);
  }
  return offsets;
}

} // namespace

// ---------------------------------------------------------------------------
// ClusterIndex
// ---------------------------------------------------------------------------

ClusterIndex::~ClusterIndex() = default;

std::unique_ptr<ClusterIndex> ClusterIndex::create(
    Section rootSection,
    LoadMetadataFn loadMetadata,
    LoadDataFn loadData,
    velox::memory::MemoryPool* pool) {
  return std::unique_ptr<ClusterIndex>(new ClusterIndex(
      std::move(rootSection),
      std::move(loadMetadata),
      std::move(loadData),
      pool));
}

ClusterIndex::ClusterIndex(
    Section rootSection,
    LoadMetadataFn loadMetadata,
    LoadDataFn loadData,
    velox::memory::MemoryPool* pool)
    : IndexLookup{IndexType::Cluster},
      rootSection_{std::move(rootSection)},
      indexRoot_{getIndexRoot(rootSection_)},
      numPartitions_{getIndexPartitionCount(indexRoot_)},
      indexColumns_{getIndexColumns(indexRoot_)},
      sortOrders_{getSortOrders(indexRoot_)},
      firstKey_{getFirstKey(indexRoot_)},
      lastKey_{getLastKey(indexRoot_)},
      partitionKeys_{getPartitionLastKeys(indexRoot_)},
      partitionRows_{getPartitionRows(indexRoot_)},
      numRows_{partitionRows_.back()},
      loadData_{std::move(loadData)},
      loadMetadata_{std::move(loadMetadata)},
      pool_{pool},
      partitions_(numPartitions_) {
  NIMBLE_CHECK(!indexColumns_.empty());
  NIMBLE_CHECK_EQ(indexColumns_.size(), sortOrders_.size());
  NIMBLE_CHECK_EQ(partitionRows_.size(), numPartitions_ + 1);
  NIMBLE_CHECK_EQ(partitionRows_.front(), 0);
  NIMBLE_CHECK_EQ(partitionKeys_.size(), numPartitions_);
  NIMBLE_CHECK_NOT_NULL(loadMetadata_);
  NIMBLE_CHECK_NOT_NULL(loadData_);
  NIMBLE_CHECK_NOT_NULL(pool_);
}

std::optional<uint32_t> ClusterIndex::lookupPartition(
    std::string_view key) const {
  if (key < firstKey_) {
    return 0;
  }
  const auto it =
      std::lower_bound(partitionKeys_.begin(), partitionKeys_.end(), key);
  if (it == partitionKeys_.end()) {
    return std::nullopt;
  }
  return static_cast<uint32_t>(it - partitionKeys_.begin());
}

void ClusterIndex::resolvePartitionBounds() const {
  std::sort(
      partitionLookups_.begin(),
      partitionLookups_.end(),
      [](const auto& a, const auto& b) {
        return a.partitionId < b.partitionId;
      });

  for (size_t i = 0; i < partitionLookups_.size();) {
    const auto* partition = loadPartition(partitionLookups_[i].partitionId);

    while (i < partitionLookups_.size() &&
           partitionLookups_[i].partitionId == partition->id) {
      const auto& entry = partitionLookups_[i];
      *entry.targetRow =
          resolvePartitionRow(partition, entry.encodedKey, entry.inclusive);
      ++i;
    }
  }
}

IndexLookup::LookupResult ClusterIndex::buildLookupResult(
    const LookupOptions& options) const {
  std::vector<RowRange> locations;
  std::vector<uint32_t> offsets;
  offsets.reserve(scratchRanges_.size() + 1);
  offsets.emplace_back(0);

  for (const auto& lookupRange : scratchRanges_) {
    auto range = lookupRange;
    if (options.rowRange.has_value()) {
      range = range.intersect(options.rowRange.value());
    }
    if (!range.empty()) {
      locations.emplace_back(range);
    }
    offsets.emplace_back(locations.size());
  }

  return LookupResult{std::move(locations), std::move(offsets)};
}

void ClusterIndex::lookupPartitions(const LookupRequest& request) const {
  NIMBLE_CHECK(partitionLookups_.empty());
  NIMBLE_CHECK(scratchRanges_.empty());
  partitionLookups_.reserve(2 * request.size());
  scratchRanges_.assign(request.size(), RowRange{});

  if (request.mode() == LookupRequest::Mode::PointLookup) {
    partitionPointLookups(request);
  } else {
    partitionRangeLookups(request);
  }
}

void ClusterIndex::partitionPointLookups(const LookupRequest& request) const {
  for (uint32_t i = 0; i < request.size(); ++i) {
    const auto encodedKey = request.pointKey(i);
    const auto partitionId = lookupPartition(encodedKey);
    if (!partitionId.has_value()) {
      continue;
    }
    // Lower bound: first row >= key (inclusive).
    partitionLookups_.emplace_back(
        partitionId.value(),
        encodedKey,
        /*inclusive=*/true,
        &scratchRanges_[i].startRow);
    // Upper bound: first row > key (exclusive).
    partitionLookups_.emplace_back(
        partitionId.value(),
        encodedKey,
        /*inclusive=*/false,
        &scratchRanges_[i].endRow);
  }
}

void ClusterIndex::partitionRangeLookups(const LookupRequest& request) const {
  for (uint32_t i = 0; i < request.size(); ++i) {
    const auto& keyBound = request.rangeBound(i);
    NIMBLE_CHECK(
        keyBound.lowerKey.has_value() || keyBound.upperKey.has_value(),
        "At least one of lowerKey or upperKey must be set");

    // Skip empty ranges where upper bound <= lower bound.
    if (keyBound.lowerKey.has_value() && keyBound.upperKey.has_value() &&
        keyBound.upperKey.value() <= keyBound.lowerKey.value()) {
      continue;
    }

    if (keyBound.lowerKey.has_value()) {
      const auto partitionId = lookupPartition(keyBound.lowerKey.value());
      if (!partitionId.has_value()) {
        continue;
      }
      partitionLookups_.emplace_back(
          partitionId.value(),
          keyBound.lowerKey.value(),
          /*inclusive=*/true,
          &scratchRanges_[i].startRow);
    }

    if (keyBound.upperKey.has_value()) {
      const auto partitionId = lookupPartition(keyBound.upperKey.value());
      if (partitionId.has_value()) {
        // Upper bound is exclusive: seekAtOrAfter gives the correct cutoff.
        partitionLookups_.emplace_back(
            partitionId.value(),
            keyBound.upperKey.value(),
            /*inclusive=*/true,
            &scratchRanges_[i].endRow);
      } else {
        scratchRanges_[i].endRow = numRows_;
      }
    } else {
      scratchRanges_[i].endRow = numRows_;
    }
  }
}

IndexLookup::LookupResult ClusterIndex::lookup(
    const LookupRequest& request) const {
  SCOPE_EXIT {
    partitionLookups_.clear();
    scratchRanges_.clear();
  };
  lookupPartitions(request);
  resolvePartitionBounds();

  return buildLookupResult(request.options());
}

uint32_t ClusterIndex::resolvePartitionRow(
    const IndexPartition* partition,
    std::string_view encodedKey,
    bool inclusive) const {
  const auto chunkLocation = lookupChunk(partition, encodedKey);
  const auto partitionRow =
      seekInChunk(partition, chunkLocation, encodedKey, inclusive);
  return partitionRows_[partition->id] + partitionRow;
}

MetadataSection ClusterIndex::partitionSection(uint32_t partitionId) const {
  NIMBLE_CHECK_LT(partitionId, numPartitions_);
  const auto* indexPartitions = indexRoot_->index_partitions();
  NIMBLE_CHECK_NOT_NULL(indexPartitions);
  const auto* metadata = indexPartitions->Get(partitionId);
  return MetadataSection{
      metadata->offset(),
      metadata->size(),
      static_cast<CompressionType>(metadata->compression_type())};
}

ClusterIndex::IndexPartition::IndexPartition(
    uint32_t _id,
    std::unique_ptr<MetadataBuffer> _metadata)
    : id{_id},
      metadata{std::move(_metadata)},
      index{flatbuffers::GetRoot<serialization::ClusterIndexPartition>(
          metadata->content().data())} {
  const auto* chunkKeys = index->chunk_keys();
  NIMBLE_CHECK_NOT_NULL(chunkKeys);
  const auto* chunkRows = index->chunk_rows();
  NIMBLE_CHECK_NOT_NULL(chunkRows);
  NIMBLE_CHECK_EQ(chunkRows->size(), chunkKeys->size());
  const auto* chunkOffsets = index->chunk_offsets();
  NIMBLE_CHECK_NOT_NULL(chunkOffsets);
  NIMBLE_CHECK_EQ(chunkOffsets->size(), chunkKeys->size());
}

uint32_t ClusterIndex::IndexPartition::chunkIndex(
    std::string_view encodedKey) const {
  const auto* chunkKeys = index->chunk_keys();
  auto it = std::lower_bound(
      chunkKeys->begin(),
      chunkKeys->end(),
      encodedKey,
      [](const flatbuffers::String* a, std::string_view b) {
        return a->string_view() < b;
      });
  NIMBLE_CHECK(
      it != chunkKeys->end(), "Key must be within partition's chunk range");
  return it - chunkKeys->begin();
}

velox::common::Region ClusterIndex::IndexPartition::chunkStreamRegion(
    const ChunkLocation& chunkLocation) const {
  const uint64_t offset =
      index->key_stream_offset() + chunkLocation.chunkOffset;
  return velox::common::Region{offset, chunkLocation.chunkSize};
}

char* ClusterIndex::IndexPartition::ensureDataBuffer(
    uint32_t bytes,
    velox::memory::MemoryPool* pool) const {
  if (dataBuffer == nullptr) {
    dataBuffer = velox::AlignedBuffer::allocate<char>(bytes, pool);
  } else if (dataBuffer->capacity() < bytes) {
    NIMBLE_CHECK(dataBuffer->unique());
    velox::AlignedBuffer::reallocate<char>(&dataBuffer, bytes);
  }
  return dataBuffer->asMutable<char>();
}

const ClusterIndex::IndexPartition* ClusterIndex::loadPartition(
    uint32_t partitionId) const {
  NIMBLE_CHECK_LT(partitionId, numPartitions_);
  auto& partition = partitions_[partitionId];
  if (partition == nullptr) {
    NIMBLE_CHECK_NOT_NULL(
        loadMetadata_,
        "No metadata loader — cannot load partition {} on demand",
        partitionId);
    const auto section = partitionSection(partitionId);
    partition =
        std::make_unique<IndexPartition>(partitionId, loadMetadata_(section));
  }
  return partition.get();
}

ChunkLocation ClusterIndex::lookupChunk(
    const IndexPartition* partition,
    std::string_view encodedKey) const {
  NIMBLE_DCHECK_NOT_NULL(partition);
  const uint32_t idx = partition->chunkIndex(encodedKey);
  return ChunkLocation{
      partition->chunkOffset(idx),
      partition->chunkSize(idx),
      partition->rowOffset(idx)};
}

ClusterIndex::Layout ClusterIndex::layout(bool detail) const {
  Layout result;
  result.indexColumns = indexColumns_;
  result.sortOrders = sortOrders_;
  result.numPartitions = numPartitions_;

  // Always populate partition metadata sections (cheap, from root FlatBuffer).
  result.partitions.resize(numPartitions_);
  for (uint32_t i = 0; i < numPartitions_; ++i) {
    result.partitions[i].metadataSection = partitionSection(i);
  }

  if (!detail) {
    return result;
  }

  // Populate per-partition detail (requires loading partition metadata via
  // I/O).
  for (uint32_t i = 0; i < numPartitions_; ++i) {
    const auto* partition = loadPartition(i);
    auto& partitionLayout = result.partitions[i];
    partitionLayout.keyStreamRegion = velox::common::Region{
        partition->index->key_stream_offset(),
        partition->index->key_stream_size()};
    partitionLayout.numChunks = partition->numChunks();
    partitionLayout.numRows = partitionRows_[i + 1] - partitionRows_[i];
    partitionLayout.metadataSizeBytes =
        static_cast<uint32_t>(partition->metadata->content().size());
  }
  return result;
}

// ---------------------------------------------------------------------------
// ClusterIndex::DecodedChunk
// ---------------------------------------------------------------------------

const ClusterIndex::DecodedChunk& ClusterIndex::getDecodedChunk(
    const IndexPartition* partition,
    const ChunkLocation& chunkLocation) const {
  NIMBLE_DCHECK_NOT_NULL(partition);
  auto& decodedChunk = partition->decodedChunk;
  if (decodedChunk.data != nullptr &&
      decodedChunk.chunkOffset == chunkLocation.chunkOffset) {
    return decodedChunk;
  }

  decodedChunk.reset(chunkLocation.chunkOffset);

  const auto region = partition->chunkStreamRegion(chunkLocation);
  decodedChunk.data =
      decodeKeyChunk(loadData_(region), *pool_, partition->dataBuffer);
  NIMBLE_CHECK_EQ(
      decodedChunk.data->encoding->dataType(),
      DataType::String,
      "Expected String data type");

  return decodedChunk;
}

uint32_t ClusterIndex::seekInChunk(
    const IndexPartition* partition,
    const ChunkLocation& chunkLocation,
    std::string_view encodedKey,
    bool inclusive) const {
  const auto& chunk = getDecodedChunk(partition, chunkLocation);
  const auto rowInChunk = chunk.data->encoding->seek(&encodedKey, inclusive);
  if (!rowInChunk.has_value()) {
    // For exclusive seek (inclusive=false), no row > key means the end is
    // past all rows in this partition.
    if (!inclusive) {
      return chunkLocation.rowOffset + chunk.data->encoding->rowCount();
    }
    NIMBLE_CHECK(false, "Key must be found within a matched chunk");
  }
  return chunkLocation.rowOffset + rowInChunk.value();
}

} // namespace facebook::nimble::index
