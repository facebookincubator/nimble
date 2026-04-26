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
#include "dwio/nimble/index/HashIndex.h"

#include <algorithm>

#include <fmt/ranges.h>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/index/HashIndexUtils.h"
#include "dwio/nimble/tablet/HashIndexGenerated.h"
#include "velox/common/base/BitUtil.h"

namespace facebook::nimble::index {

namespace {

// Checks if the given columns exactly match.
bool columnsMatch(
    const std::vector<std::string>& indexColumns,
    const std::vector<std::string>& queryColumns) {
  if (indexColumns.size() != queryColumns.size()) {
    return false;
  }
  for (size_t i = 0; i < indexColumns.size(); ++i) {
    if (indexColumns[i] != queryColumns[i]) {
      return false;
    }
  }
  return true;
}

const serialization::HashIndex* getHashIndexRoot(
    const MetadataBuffer& metadata) {
  const auto* entry =
      flatbuffers::GetRoot<serialization::HashIndex>(metadata.content().data());
  NIMBLE_CHECK_NOT_NULL(entry);
  return entry;
}

uint32_t getNumBuckets(const MetadataBuffer& metadata) {
  const auto numBuckets = getHashIndexRoot(metadata)->num_buckets();
  NIMBLE_CHECK_GT(numBuckets, 0u, "Hash index must have buckets");
  NIMBLE_CHECK(
      velox::bits::isPowerOfTwo(numBuckets), "numBuckets must be a power of 2");
  return numBuckets;
}

std::unique_ptr<BloomFilter> buildBloomFilter(
    const MetadataBuffer& metadata,
    velox::memory::MemoryPool* pool) {
  const auto* bloomFilter = getHashIndexRoot(metadata)->bloom_filter();
  if (bloomFilter == nullptr) {
    return nullptr;
  }
  NIMBLE_CHECK_NOT_NULL(bloomFilter->data());
  NIMBLE_CHECK_GT(bloomFilter->data()->size(), 0u);
  const auto numBlocks = bloomFilter->num_blocks();
  const auto* rawData = bloomFilter->data();
  return std::make_unique<BloomFilter>(
      numBlocks, rawData->data(), rawData->size(), pool);
}

// Sorts row numbers and merges consecutive rows into contiguous RowRanges.
// If 'filterRange' is set, each range is intersected with the filter and
// empty results are dropped.
std::vector<RowRange> resolveRowRanges(
    std::vector<uint32_t> rows,
    const std::optional<RowRange>& filterRange) {
  if (rows.empty()) {
    return {};
  }

  std::sort(rows.begin(), rows.end());

  std::vector<RowRange> result;
  const auto addRange = [&](uint32_t start, uint32_t end) {
    RowRange range{
        static_cast<velox::vector_size_t>(start),
        static_cast<velox::vector_size_t>(end)};
    if (filterRange.has_value()) {
      range = range.intersect(filterRange.value());
    }
    if (!range.empty()) {
      result.emplace_back(range);
    }
  };

  uint32_t rangeStart = rows[0];
  uint32_t rangeEnd = rows[0] + 1;

  for (size_t i = 1; i < rows.size(); ++i) {
    if (rows[i] == rangeEnd) {
      ++rangeEnd;
    } else {
      addRange(rangeStart, rangeEnd);
      rangeStart = rows[i];
      rangeEnd = rows[i] + 1;
    }
  }
  addRange(rangeStart, rangeEnd);
  return result;
}

} // namespace

// ---------------------------------------------------------------------------
// HashIndex
// ---------------------------------------------------------------------------

HashIndex::HashIndex(
    std::vector<std::string> columns,
    std::unique_ptr<MetadataBuffer> indexMetadata,
    LoadMetadataFn loadMetadata,
    velox::memory::MemoryPool* pool)
    : IndexLookup{IndexType::Hash},
      columns_{std::move(columns)},
      indexMetadata_{std::move(indexMetadata)},
      pool_{pool},
      numBuckets_{getNumBuckets(*indexMetadata_)},
      bucketMask_{numBuckets_ - 1},
      bloomFilter_{buildBloomFilter(*indexMetadata_, pool_)},
      partitions_{buildPartitionDescriptors(*indexMetadata_, numBuckets_)},
      partitionCache_{
          [this, loadMetadata = std::move(loadMetadata)](
              uint32_t partitionIndex) -> std::shared_ptr<Partition> {
            const auto& descriptor = partitions_[partitionIndex];
            auto metadata = loadMetadata(descriptor.section);
            return Partition::create(std::move(metadata));
          }} {}

// static
std::vector<HashIndex::PartitionDescriptor>
HashIndex::buildPartitionDescriptors(
    const MetadataBuffer& metadata,
    uint32_t numBuckets) {
  const auto* entry = getHashIndexRoot(metadata);
  const auto* partitionStartBuckets = entry->partition_start_buckets();
  const auto* partitionSections = entry->partition_sections();
  NIMBLE_CHECK_NOT_NULL(partitionStartBuckets);
  NIMBLE_CHECK_NOT_NULL(partitionSections);
  const auto numPartitions = partitionStartBuckets->size();
  NIMBLE_CHECK_GT(numPartitions, 0u, "Hash index must have partitions");
  NIMBLE_CHECK_EQ(partitionSections->size(), numPartitions);

  std::vector<PartitionDescriptor> partitions;
  partitions.reserve(numPartitions);
  for (uint32_t i = 0; i < numPartitions; ++i) {
    const uint32_t startBucket = partitionStartBuckets->Get(i);
    const uint32_t endBucket = (i + 1 < numPartitions)
        ? partitionStartBuckets->Get(i + 1)
        : numBuckets;
    const auto* sectionRef = partitionSections->Get(i);
    NIMBLE_CHECK_NOT_NULL(sectionRef);
    partitions.push_back(
        PartitionDescriptor{
            startBucket,
            endBucket - startBucket,
            MetadataSection{
                sectionRef->offset(),
                sectionRef->size(),
                static_cast<CompressionType>(sectionRef->compression_type())}});
  }
  return partitions;
}

std::unique_ptr<HashIndex> HashIndex::create(
    std::vector<std::string> columns,
    std::unique_ptr<MetadataBuffer> indexMetadata,
    LoadMetadataFn loadMetadata,
    velox::memory::MemoryPool* pool) {
  return std::unique_ptr<HashIndex>(new HashIndex(
      std::move(columns),
      std::move(indexMetadata),
      std::move(loadMetadata),
      pool));
}

IndexLookup::LookupResult HashIndex::lookup(
    const LookupRequest& request) const {
  NIMBLE_CHECK_EQ(
      request.mode(),
      LookupRequest::Mode::PointLookup,
      "Hash index only supports point lookups");
  const auto& options = request.options();

  std::vector<RowRange> rowRanges;
  std::vector<uint32_t> resultOffsets;
  resultOffsets.reserve(request.size() + 1);
  resultOffsets.push_back(0);

  for (uint32_t i = 0; i < request.size(); ++i) {
    const auto key = request.pointKey(i);
    ++numLookups_;

    // Check bloom filter for fast negative.
    if (bloomFilter_ != nullptr && !bloomFilter_->testKey(key)) {
      ++numBloomFilterSkips_;
      resultOffsets.push_back(rowRanges.size());
      continue;
    }

    // Find the bucket.
    const uint32_t bucket = bucketIndex(key, bucketMask_);

    // Load the partition containing this bucket.
    const uint32_t partitionIdx = findPartitionIndex(bucket);
    const auto& partition = partitionCache_.getOrCreate(partitionIdx);
    const uint32_t bucketOffset =
        bucket - partitions_[partitionIdx].startBucket;

    auto resolved = resolveRowRanges(
        partition->findRows(bucketOffset, key), options.rowRange);
    numMatchedRows_ += resolved.size();
    for (auto& range : resolved) {
      rowRanges.emplace_back(range);
    }
    resultOffsets.push_back(rowRanges.size());
  }

  return LookupResult{std::move(rowRanges), std::move(resultOffsets)};
}

folly::F14FastMap<std::string, velox::RuntimeMetric> HashIndex::stats() const {
  folly::F14FastMap<std::string, velox::RuntimeMetric> result;
  const auto numLookups = numLookups_.load();
  if (numLookups > 0) {
    result.emplace(kNumLookups, velox::RuntimeMetric(numLookups));
  }
  const auto numBloomFilterSkips = numBloomFilterSkips_.load();
  if (numBloomFilterSkips > 0) {
    result.emplace(
        kNumBloomFilterSkips, velox::RuntimeMetric(numBloomFilterSkips));
  }
  const auto numMatchedRows = numMatchedRows_.load();
  if (numMatchedRows > 0) {
    result.emplace(kNumMatchedRows, velox::RuntimeMetric(numMatchedRows));
  }
  return result;
}

uint32_t HashIndex::findPartitionIndex(uint32_t bucket) const {
  const auto it = std::upper_bound(
      partitions_.begin(),
      partitions_.end(),
      bucket,
      [](uint32_t b, const auto& p) { return b < p.startBucket; });
  NIMBLE_CHECK(
      it != partitions_.begin(),
      "Bucket {} is before the first partition",
      bucket);
  const uint32_t index =
      static_cast<uint32_t>(std::distance(partitions_.begin(), it) - 1);
  const auto& partition = partitions_[index];
  NIMBLE_CHECK_LT(
      bucket - partition.startBucket,
      partition.bucketCount,
      "Bucket {} is out of range for partition {}: {}",
      bucket,
      index,
      partition.toString());
  return index;
}

HashIndex::Partition::Partition(
    std::unique_ptr<MetadataBuffer> metadata,
    const uint32_t* bucketOffsets,
    const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>* keys,
    const uint32_t* rows)
    : metadata_{std::move(metadata)},
      bucketOffsets_{bucketOffsets},
      keys_{keys},
      rows_{rows} {}

std::vector<uint32_t> HashIndex::Partition::findRows(
    uint32_t bucketIndex,
    std::string_view key) const {
  const uint32_t bucketStart = bucketOffsets_[bucketIndex];
  const uint32_t bucketEnd = bucketOffsets_[bucketIndex + 1];

  std::vector<uint32_t> rows;
  for (uint32_t keyIdx = bucketStart; keyIdx < bucketEnd; ++keyIdx) {
    const auto* storedKey = keys_->Get(keyIdx);
    if (storedKey->string_view() == key) {
      rows.emplace_back(rows_[keyIdx]);
    }
  }
  return rows;
}

std::shared_ptr<HashIndex::Partition> HashIndex::Partition::create(
    std::unique_ptr<MetadataBuffer> metadata) {
  const auto* partition =
      flatbuffers::GetRoot<serialization::HashIndexPartition>(
          metadata->content().data());
  NIMBLE_CHECK_NOT_NULL(partition);

  const auto* bucketOffsets = partition->bucket_offsets();
  NIMBLE_CHECK_NOT_NULL(bucketOffsets);

  const auto* keys = partition->encoded_keys();
  NIMBLE_CHECK_NOT_NULL(keys);

  const auto* rows = partition->row_numbers();
  NIMBLE_CHECK_NOT_NULL(rows);

  return std::shared_ptr<Partition>(new Partition(
      std::move(metadata), bucketOffsets->data(), keys, rows->data()));
}

// ---------------------------------------------------------------------------
// DenseIndexRegistry
// ---------------------------------------------------------------------------

std::unique_ptr<DenseIndexRegistry> DenseIndexRegistry::create(
    Section directorySection,
    LoadMetadataFn loadMetadata,
    velox::memory::MemoryPool* pool) {
  const auto* root = flatbuffers::GetRoot<serialization::HashIndexDirectory>(
      directorySection.content().data());
  NIMBLE_CHECK_NOT_NULL(root);
  const auto* indices = root->indices();
  if (indices == nullptr) {
    return nullptr;
  }
  NIMBLE_CHECK_GT(
      indices->size(), 0u, "Hash index directory must not be empty");
  return std::unique_ptr<DenseIndexRegistry>(new DenseIndexRegistry(
      std::move(directorySection), std::move(loadMetadata), pool));
}

DenseIndexRegistry::DenseIndexRegistry(
    Section directorySection,
    LoadMetadataFn loadMetadata,
    velox::memory::MemoryPool* pool)
    : directorySection_{std::move(directorySection)},
      indexCache_{
          [this, loadMetadata = std::move(loadMetadata), pool](
              uint32_t index) -> std::shared_ptr<HashIndex> {
            const auto& descriptor = indexDescriptors_[index];
            auto indexMetadata = loadMetadata(descriptor.section);
            NIMBLE_CHECK_NOT_NULL(indexMetadata);
            return HashIndex::create(
                descriptor.columns,
                std::move(indexMetadata),
                loadMetadata,
                pool);
          },
          /*pinEntries=*/true} {
  parseIndexDescriptors();
}

void DenseIndexRegistry::parseIndexDescriptors() {
  NIMBLE_CHECK(
      indexDescriptors_.empty(), "Index descriptors already initialized");

  const auto* root = flatbuffers::GetRoot<serialization::HashIndexDirectory>(
      directorySection_.content().data());
  NIMBLE_CHECK_NOT_NULL(root);
  const auto* indices = root->indices();
  NIMBLE_CHECK_NOT_NULL(indices);

  indexDescriptors_.reserve(indices->size());
  for (const auto* indexSection : *indices) {
    NIMBLE_CHECK_NOT_NULL(indexSection);
    const auto* columns = indexSection->index_columns();
    NIMBLE_CHECK_NOT_NULL(columns);
    NIMBLE_CHECK_GT(columns->size(), 0u, "Hash index must have columns");

    std::vector<std::string> columnNames;
    columnNames.reserve(columns->size());
    for (const auto* col : *columns) {
      columnNames.emplace_back(col->string_view());
    }

    // Check for duplicate index columns.
    for (const auto& indexDescriptor : indexDescriptors_) {
      NIMBLE_CHECK(
          !columnsMatch(indexDescriptor.columns, columnNames),
          "Duplicate hash index columns: [{}]",
          fmt::join(columnNames, ", "));
    }

    const auto* sectionRef = indexSection->section();
    NIMBLE_CHECK_NOT_NULL(sectionRef);
    indexDescriptors_.emplace_back(
        IndexDescriptor{
            std::move(columnNames),
            MetadataSection{
                sectionRef->offset(),
                sectionRef->size(),
                static_cast<CompressionType>(sectionRef->compression_type())}});
  }
}

const HashIndex* DenseIndexRegistry::findIndex(
    const std::vector<std::string>& queryColumns) const {
  NIMBLE_CHECK(!queryColumns.empty(), "queryColumns must not be empty");
  for (uint32_t i = 0; i < indexDescriptors_.size(); ++i) {
    if (columnsMatch(indexDescriptors_[i].columns, queryColumns)) {
      return indexCache_.getOrCreate(i).get();
    }
  }
  return nullptr;
}

} // namespace facebook::nimble::index
