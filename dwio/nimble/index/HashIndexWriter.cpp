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
#include "dwio/nimble/index/HashIndexWriter.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <set>

#include <fmt/ranges.h>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/index/BloomFilter.h"
#include "dwio/nimble/index/HashIndexUtils.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/HashIndexGenerated.h"
#include "flatbuffers/flatbuffers.h"

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Nulls.h"

namespace facebook::nimble::index {

namespace {

std::vector<std::vector<std::string>> extractColumnSets(
    const std::vector<HashIndexConfig>& configs) {
  std::vector<std::vector<std::string>> columnSets;
  columnSets.reserve(configs.size());
  for (const auto& config : configs) {
    columnSets.emplace_back(config.columns);
  }
  return columnSets;
}

} // namespace

// static
// Estimates the serialized size of one bucket's keys.
// Accounts for encoded key strings + row numbers + bucket directory overhead.
size_t HashIndexWriter::estimateBucketSize(
    const std::vector<uint32_t>& keyIndices,
    const IndexAccumulator& accumulator) {
  // Per-bucket overhead: bucket_offsets (uint32).
  size_t size = sizeof(uint32_t);
  for (const auto idx : keyIndices) {
    // Per-key: row_numbers (uint32) + encoded key bytes + FlatBuffer string
    // overhead (length prefix + padding). Conservative estimate.
    size += sizeof(uint32_t) + accumulator.entries[idx].key.size() + 8;
  }
  return size;
}

// static
// Builds a HashIndexPartition FlatBuffer for a range of buckets.
void HashIndexWriter::buildIndexPartitionFlatBuffer(
    flatbuffers::FlatBufferBuilder& builder,
    const std::vector<std::vector<uint32_t>>& keyIndices,
    const IndexAccumulator& accumulator,
    uint32_t startBucket,
    uint32_t bucketCount) {
  // bucketCount + 1 offsets: offsets[i] is start, offsets[i+1] is end
  // (exclusive) for bucket i. The last element is the total entry count.
  std::vector<uint32_t> bucketOffsets(bucketCount + 1);
  std::vector<flatbuffers::Offset<flatbuffers::String>> keys;
  std::vector<uint32_t> rows;

  uint32_t rowOffset = 0;
  for (uint32_t i = 0; i < bucketCount; ++i) {
    bucketOffsets[i] = rowOffset;
    const auto& indices = keyIndices[startBucket + i];
    for (const auto idx : indices) {
      const auto& entry = accumulator.entries[idx];
      keys.emplace_back(
          builder.CreateString(entry.key.data(), entry.key.size()));
      rows.emplace_back(entry.row);
    }
    rowOffset += indices.size();
  }
  bucketOffsets[bucketCount] = rowOffset;

  builder.Finish(
      serialization::CreateHashIndexPartition(
          builder,
          builder.CreateVector(bucketOffsets),
          builder.CreateVector(keys),
          builder.CreateVector(rows)));
}

flatbuffers::Offset<serialization::BloomFilter>
HashIndexWriter::buildBloomFilter(
    flatbuffers::FlatBufferBuilder& builder,
    const IndexAccumulator& accumulator) const {
  if (!accumulator.config.bloomFilter.has_value()) {
    return 0;
  }
  const auto bitsPerKey = accumulator.config.bloomFilter->bitsPerKey;
  BloomFilter bloomFilter(accumulator.entries.size(), bitsPerKey, pool_);
  for (const auto& entry : accumulator.entries) {
    bloomFilter.insert(entry.key);
  }
  auto dataVec =
      builder.CreateVector(bloomFilter.data(), bloomFilter.dataSize());
  return serialization::CreateBloomFilter(
      builder, bloomFilter.numBlocks(), bitsPerKey, dataVec);
}

void HashIndexWriter::buildIndexFlatBuffer(
    flatbuffers::FlatBufferBuilder& builder,
    const IndexAccumulator& accumulator,
    const CreateMetadataSectionFn& createMetadataFn) const {
  NIMBLE_CHECK(!accumulator.entries.empty());
  const auto minIt = std::min_element(
      accumulator.entries.begin(),
      accumulator.entries.end(),
      [](const auto& a, const auto& b) { return a.key < b.key; });
  const auto maxIt = std::max_element(
      accumulator.entries.begin(),
      accumulator.entries.end(),
      [](const auto& a, const auto& b) { return a.key < b.key; });

  const uint64_t numKeys = accumulator.entries.size();
  const uint32_t numBuckets = static_cast<uint32_t>(velox::bits::nextPowerOfTwo(
      std::max(
          1u,
          static_cast<uint32_t>(std::ceil(
              static_cast<float>(numKeys) / accumulator.config.loadFactor)))));
  const uint32_t bucketMask = numBuckets - 1;

  // Assign keys to buckets.
  std::vector<std::vector<uint32_t>> bucketKeyIndices(numBuckets);
  for (uint64_t i = 0; i < numKeys; ++i) {
    const uint32_t bucket = bucketIndex(accumulator.entries[i].key, bucketMask);
    bucketKeyIndices[bucket].emplace_back(i);
  }

  const auto bloomFilterOffset = buildBloomFilter(builder, accumulator);

  const float actualLoadFactor =
      static_cast<float>(numKeys) / static_cast<float>(numBuckets);

  // Determine partition boundaries. Always create at least one partition.
  // When maxPartitionSizeBytes is set and data exceeds it, split into
  // multiple partitions for on-demand loading.
  uint32_t bucketsPerPartition = numBuckets;
  if (accumulator.config.maxPartitionSizeBytes > 0 && numBuckets > 1) {
    size_t estimatedTotalBucketSize = 0;
    for (uint32_t i = 0; i < numBuckets; ++i) {
      estimatedTotalBucketSize +=
          estimateBucketSize(bucketKeyIndices[i], accumulator);
    }
    if (estimatedTotalBucketSize > accumulator.config.maxPartitionSizeBytes) {
      const uint32_t numPartitions =
          static_cast<uint32_t>(velox::bits::divRoundUp(
              estimatedTotalBucketSize,
              accumulator.config.maxPartitionSizeBytes));
      bucketsPerPartition = velox::bits::divRoundUp(numBuckets, numPartitions);
    }
  }

  std::vector<uint32_t> partitionStartBuckets;
  std::vector<flatbuffers::Offset<serialization::MetadataSection>>
      partitionSections;

  for (uint32_t startBucket = 0; startBucket < numBuckets;
       startBucket += bucketsPerPartition) {
    const uint32_t endBucket =
        std::min(startBucket + bucketsPerPartition, numBuckets);
    const uint32_t bucketCount = endBucket - startBucket;

    flatbuffers::FlatBufferBuilder partitionBuilder;
    buildIndexPartitionFlatBuffer(
        partitionBuilder,
        bucketKeyIndices,
        accumulator,
        startBucket,
        bucketCount);
    const auto partitionSection =
        createMetadataFn(asStringView(partitionBuilder));

    partitionStartBuckets.emplace_back(startBucket);
    partitionSections.emplace_back(
        serialization::CreateMetadataSection(
            builder,
            partitionSection.offset(),
            partitionSection.size(),
            static_cast<serialization::CompressionType>(
                partitionSection.compressionType())));
  }

  auto partitionStartBucketsVec = builder.CreateVector(partitionStartBuckets);
  auto partitionSectionsVec = builder.CreateVector(partitionSections);
  auto minKeyOffset =
      builder.CreateString(minIt->key.data(), minIt->key.size());
  auto maxKeyOffset =
      builder.CreateString(maxIt->key.data(), maxIt->key.size());

  auto hashIndex = serialization::CreateHashIndex(
      builder,
      numKeys,
      numBuckets,
      numKeys,
      actualLoadFactor,
      bloomFilterOffset,
      minKeyOffset,
      maxKeyOffset,
      partitionStartBucketsVec,
      partitionSectionsVec);
  builder.Finish(hashIndex);
}

void HashIndexWriter::buildIndexDirectoryFlatBuffer(
    flatbuffers::FlatBufferBuilder& builder,
    const std::vector<MetadataSection>& indexSections) const {
  NIMBLE_CHECK(!accumulators_.empty());
  NIMBLE_CHECK_EQ(indexSections.size(), accumulators_.size());

  std::vector<flatbuffers::Offset<serialization::HashIndexSection>>
      sectionOffsets;
  sectionOffsets.reserve(accumulators_.size());

  for (size_t i = 0; i < accumulators_.size(); ++i) {
    // Build column names for the directory entry.
    std::vector<flatbuffers::Offset<flatbuffers::String>> columnOffsets;
    columnOffsets.reserve(accumulators_[i].config.columns.size());
    for (const auto& col : accumulators_[i].config.columns) {
      columnOffsets.emplace_back(builder.CreateString(col));
    }
    auto columnsVec = builder.CreateVector(columnOffsets);

    // Build MetadataSection reference.
    const auto& section = indexSections[i];
    auto metadataSection = serialization::CreateMetadataSection(
        builder,
        section.offset(),
        section.size(),
        static_cast<serialization::CompressionType>(section.compressionType()));

    sectionOffsets.emplace_back(
        serialization::CreateHashIndexSection(
            builder, columnsVec, metadataSection));
  }

  auto indicesVec = builder.CreateVector(sectionOffsets);
  auto directory = serialization::CreateHashIndexDirectory(builder, indicesVec);
  builder.Finish(directory);
}

std::unique_ptr<HashIndexWriter> HashIndexWriter::create(
    const std::vector<HashIndexConfig>& configs,
    const velox::TypePtr& inputType,
    velox::memory::MemoryPool* pool) {
  if (configs.empty()) {
    return nullptr;
  }
  NIMBLE_CHECK_NOT_NULL(pool, "memory pool must not be null");
  return std::unique_ptr<HashIndexWriter>(
      new HashIndexWriter(configs, velox::asRowType(inputType), pool));
}

HashIndexWriter::HashIndexWriter(
    const std::vector<HashIndexConfig>& configs,
    const velox::RowTypePtr& inputType,
    velox::memory::MemoryPool* pool)
    : pool_{pool},
      keyColumnIndices_{
          getKeyColumnIndices(extractColumnSets(configs), inputType)} {
  NIMBLE_CHECK(!configs.empty(), "Hash index configs must not be empty");
  accumulators_.reserve(configs.size());
  for (const auto& config : configs) {
    NIMBLE_CHECK(
        !config.columns.empty(), "Hash index must have at least one column");
    // Check for duplicate index columns.
    for (size_t i = 0; i < accumulators_.size(); ++i) {
      NIMBLE_CHECK(
          accumulators_[i].config.columns != config.columns,
          "Duplicate hash index columns: [{}]",
          fmt::join(config.columns, ", "));
    }
    NIMBLE_CHECK_GT(config.loadFactor, 0.0f, "Load factor must be positive");
    NIMBLE_CHECK_LE(config.loadFactor, 1.0f, "Load factor must be at most 1.0");
    accumulators_.emplace_back(
        IndexAccumulator{
            .config = config,
            .encoder = createKeyEncoder(config.columns, inputType, pool),
        });
  }
}

void HashIndexWriter::write(const velox::VectorPtr& input) {
  checkNotClosed();
  if (input->size() == 0) {
    return;
  }

  NIMBLE_USER_CHECK_LE(
      numRows_ + static_cast<uint64_t>(input->size()),
      static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()),
      "Hash index row count exceeds uint32 limit");

  validateNoNullKeys(input, keyColumnIndices_);
  ensureEncodingBuffer();

  for (auto& accumulator : accumulators_) {
    // Encode keys from this batch.
    std::vector<std::string_view> keys;
    accumulator.encoder->encode(input, keys, [this](size_t size) {
      return encodingBuffer_->reserve(size);
    });

    // Record index entries. Encoded keys are string_views into
    // encodingBuffer_ which guarantees pointer stability.
    const auto newSize = accumulator.entries.size() + input->size();
    if (accumulator.entries.capacity() < newSize) {
      accumulator.entries.reserve(
          std::max(accumulator.entries.size() * 2, newSize));
    }
    for (velox::vector_size_t i = 0; i < input->size(); ++i) {
      accumulator.entries.emplace_back(IndexEntry{keys[i], numRows_ + i});
    }
  }

  numRows_ += input->size();
}

void HashIndexWriter::close(
    const WriteDataFn& /*writeDataFn*/,
    const CreateMetadataSectionFn& createMetadataFn,
    const WriteOptionalSectionFn& writeMetadataFn) {
  setClosed();

  if (numRows_ == 0) {
    return;
  }

  // Write each hash index as a separate section.
  std::vector<MetadataSection> indexSections;
  indexSections.reserve(accumulators_.size());
  for (auto& accumulator : accumulators_) {
    flatbuffers::FlatBufferBuilder indexBuilder;
    buildIndexFlatBuffer(indexBuilder, accumulator, createMetadataFn);
    indexSections.emplace_back(createMetadataFn(asStringView(indexBuilder)));
  }

  // Free encoded keys now that they've been serialized into FlatBuffers.
  encodingBuffer_.reset();

  flatbuffers::FlatBufferBuilder directoryBuilder;
  buildIndexDirectoryFlatBuffer(directoryBuilder, indexSections);
  writeMetadataFn(
      std::string(kHashIndexSection), asStringView(directoryBuilder));

  for (auto& accumulator : accumulators_) {
    accumulator.clear();
  }
}

} // namespace facebook::nimble::index
