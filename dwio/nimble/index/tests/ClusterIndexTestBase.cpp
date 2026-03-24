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
#include "dwio/nimble/index/tests/ClusterIndexTestBase.h"

#include "dwio/nimble/tablet/ChunkIndexGenerated.h"
#include "dwio/nimble/tablet/ClusterIndexGenerated.h"
#include "folly/json/json.h"
#include "velox/common/testutil/TestValue.h"

namespace facebook::nimble::index::test {

void ClusterIndexTestBase::SetUpTestCase() {
  velox::memory::MemoryManager::initialize({});
  velox::common::testutil::TestValue::enable();
}

ClusterIndexTestBase::IndexBuffers ClusterIndexTestBase::createTestClusterIndex(
    const std::vector<std::string>& indexColumns,
    const std::vector<std::string>& sortOrders,
    const std::string& minKey,
    const std::vector<Stripe>& stripes,
    const std::vector<int>& stripeGroups) {
  NIMBLE_CHECK_EQ(indexColumns.size(), sortOrders.size());
  std::vector<uint32_t> stripeCounts;
  stripeCounts.reserve(stripeGroups.size());
  uint32_t accumulatedStripes = 0;
  for (uint32_t groupIdx = 0; groupIdx < stripeGroups.size(); ++groupIdx) {
    accumulatedStripes += stripeGroups[groupIdx];
    stripeCounts.push_back(accumulatedStripes);
  }

  flatbuffers::FlatBufferBuilder builder;
  IndexBuffers result;

  std::vector<flatbuffers::Offset<serialization::MetadataSection>>
      clusterIndexGroupMetadataOffsets;
  clusterIndexGroupMetadataOffsets.reserve(stripeGroups.size());

  uint32_t stripeIdx = 0;
  for (const auto& groupStripeCount : stripeGroups) {
    // Build StripeClusterIndex with key stream data.
    flatbuffers::FlatBufferBuilder groupBuilder;

    std::vector<uint32_t> keyStreamOffsets;
    std::vector<uint32_t> keyStreamSizes;
    std::vector<uint32_t> keyStreamChunkCounts;
    std::vector<uint32_t> keyStreamChunkRows;
    std::vector<flatbuffers::Offset<flatbuffers::String>> keyStreamChunkKeys;
    std::vector<uint32_t> keyStreamChunkOffsets;

    // Build standalone ChunkIndexGroup for this group
    flatbuffers::FlatBufferBuilder chunkBuilder;
    std::vector<uint32_t> streamChunkCounts;
    std::vector<uint32_t> streamChunkRows;
    std::vector<uint32_t> streamChunkOffsets;

    uint32_t accumulatedKeyStreamChunkCount = 0;
    uint32_t accumulatedStreamChunkCount = 0;
    for (int i = 0; i < groupStripeCount; ++i, ++stripeIdx) {
      NIMBLE_CHECK_LT(stripeIdx, stripes.size());
      const auto& stripe = stripes[stripeIdx];

      const auto& keyStream = stripe.keyStream;
      keyStreamOffsets.push_back(keyStream.streamOffset);
      keyStreamSizes.push_back(keyStream.streamSize);
      accumulatedKeyStreamChunkCount += keyStream.stream.numChunks;
      keyStreamChunkCounts.push_back(accumulatedKeyStreamChunkCount);
      NIMBLE_CHECK_EQ(
          keyStream.stream.numChunks, keyStream.stream.chunkRows.size());
      uint32_t accumulatedKeyStreamRows = 0;
      for (const auto& row : keyStream.stream.chunkRows) {
        accumulatedKeyStreamRows += row;
        keyStreamChunkRows.push_back(accumulatedKeyStreamRows);
      }
      NIMBLE_CHECK_EQ(
          keyStream.stream.numChunks, keyStream.stream.chunkOffsets.size());
      for (const auto& offset : keyStream.stream.chunkOffsets) {
        keyStreamChunkOffsets.push_back(offset);
      }
      NIMBLE_CHECK_EQ(keyStream.stream.numChunks, keyStream.chunkKeys.size());
      for (const auto& key : keyStream.chunkKeys) {
        keyStreamChunkKeys.push_back(groupBuilder.CreateString(key));
      }

      for (const auto& stream : stripe.streams) {
        accumulatedStreamChunkCount += stream.numChunks;
        streamChunkCounts.push_back(accumulatedStreamChunkCount);
        NIMBLE_CHECK_EQ(stream.numChunks, stream.chunkRows.size());
        uint32_t accumulatedRows = 0;
        for (const auto& row : stream.chunkRows) {
          accumulatedRows += row;
          streamChunkRows.push_back(accumulatedRows);
        }
        NIMBLE_CHECK_EQ(stream.numChunks, stream.chunkOffsets.size());
        for (const auto& offset : stream.chunkOffsets) {
          streamChunkOffsets.push_back(offset);
        }
      }
    }

    // Build all vectors first before creating sub-tables.
    auto keyStreamOffsetsVec = groupBuilder.CreateVector(keyStreamOffsets);
    auto keyStreamSizesVec = groupBuilder.CreateVector(keyStreamSizes);
    auto chunkCountsVec = groupBuilder.CreateVector(keyStreamChunkCounts);
    auto chunkRowsVec = groupBuilder.CreateVector(keyStreamChunkRows);
    auto chunkOffsetsVec = groupBuilder.CreateVector(keyStreamChunkOffsets);
    auto chunkKeysVec = groupBuilder.CreateVector(keyStreamChunkKeys);

    auto keyStreamLayout = serialization::CreateKeyStreamLayout(
        groupBuilder, keyStreamOffsetsVec, keyStreamSizesVec);
    auto chunkIndexOffset = serialization::CreateKeyStreamChunkIndex(
        groupBuilder,
        chunkCountsVec,
        chunkRowsVec,
        chunkOffsetsVec,
        chunkKeysVec);

    // Build StripeClusterIndex with sub-tables.
    groupBuilder.Finish(
        serialization::CreateStripeClusterIndex(
            groupBuilder, keyStreamLayout, chunkIndexOffset));

    uint64_t offset = result.indexGroups.size();
    uint32_t size = groupBuilder.GetSize();
    result.indexGroups.append(
        reinterpret_cast<const char*>(groupBuilder.GetBufferPointer()), size);

    auto metadataSection = serialization::CreateMetadataSection(
        builder, offset, size, serialization::CompressionType_Uncompressed);
    clusterIndexGroupMetadataOffsets.push_back(metadataSection);

    const uint32_t numStreams = streamChunkCounts.size() / groupStripeCount;

    // Build standalone StripeChunkIndex flatbuffer
    auto stripeChunkIndex = serialization::CreateStripeChunkIndex(
        chunkBuilder,
        numStreams,
        chunkBuilder.CreateVector(streamChunkCounts),
        chunkBuilder.CreateVector(streamChunkRows),
        chunkBuilder.CreateVector(streamChunkOffsets));
    chunkBuilder.Finish(stripeChunkIndex);

    result.chunkIndexGroupSizes.push_back(chunkBuilder.GetSize());
    result.chunkIndexGroups.append(
        reinterpret_cast<const char*>(chunkBuilder.GetBufferPointer()),
        chunkBuilder.GetSize());
  }

  std::vector<flatbuffers::Offset<flatbuffers::String>> stripeKeyOffsets;
  stripeKeyOffsets.reserve(stripes.size() + 1);
  stripeKeyOffsets.push_back(builder.CreateString(minKey));
  for (const auto& stripe : stripes) {
    const auto& chunkKeys = stripe.keyStream.chunkKeys;
    NIMBLE_CHECK(!chunkKeys.empty());
    stripeKeyOffsets.push_back(builder.CreateString(chunkKeys.back()));
  }

  std::vector<flatbuffers::Offset<flatbuffers::String>> indexColumnOffsets;
  indexColumnOffsets.reserve(indexColumns.size());
  for (const auto& column : indexColumns) {
    indexColumnOffsets.push_back(builder.CreateString(column));
  }

  std::vector<flatbuffers::Offset<flatbuffers::String>> sortOrderOffsets;
  sortOrderOffsets.reserve(sortOrders.size());
  for (const auto& sortOrder : sortOrders) {
    sortOrderOffsets.push_back(builder.CreateString(sortOrder));
  }

  auto stripeKeysVector = builder.CreateVector(stripeKeyOffsets);
  auto indexColumnsVector = builder.CreateVector(indexColumnOffsets);
  auto sortOrdersVector = builder.CreateVector(sortOrderOffsets);
  auto stripeCountsVector = builder.CreateVector(stripeCounts);
  auto stripeIndexesVector =
      builder.CreateVector(clusterIndexGroupMetadataOffsets);

  auto index = serialization::CreateClusterIndex(
      builder,
      stripeKeysVector,
      indexColumnsVector,
      sortOrdersVector,
      stripeCountsVector,
      stripeIndexesVector);

  builder.Finish(index);

  result.rootIndex = std::string(
      reinterpret_cast<const char*>(builder.GetBufferPointer()),
      builder.GetSize());

  return result;
}

ClusterIndexTestBase::IndexBuffers ClusterIndexTestBase::createTestClusterIndex(
    const std::vector<std::string>& indexColumns,
    const std::string& minKey,
    const std::vector<Stripe>& stripes,
    const std::vector<int>& stripeGroups) {
  std::vector<std::string> defaultSortOrders;
  defaultSortOrders.reserve(indexColumns.size());
  for (size_t i = 0; i < indexColumns.size(); ++i) {
    defaultSortOrders.push_back(
        folly::toJson(velox::core::kAscNullsFirst.serialize()));
  }
  return createTestClusterIndex(
      indexColumns, defaultSortOrders, minKey, stripes, stripeGroups);
}

ClusterIndexTestBase::IndexBuffers
ClusterIndexTestBase::createChunkOnlyTestClusterIndex(
    const std::vector<ChunkOnlyStripe>& stripes,
    const std::vector<int>& stripeGroups) {
  std::vector<uint32_t> stripeCounts;
  stripeCounts.reserve(stripeGroups.size());
  uint32_t accumulatedStripes = 0;
  for (const auto& groupStripeCount : stripeGroups) {
    accumulatedStripes += groupStripeCount;
    stripeCounts.push_back(accumulatedStripes);
  }

  flatbuffers::FlatBufferBuilder builder;
  IndexBuffers result;

  std::vector<flatbuffers::Offset<serialization::MetadataSection>>
      clusterIndexGroupMetadataOffsets;
  clusterIndexGroupMetadataOffsets.reserve(stripeGroups.size());

  uint32_t stripeIdx = 0;
  for (const auto& groupStripeCount : stripeGroups) {
    // Build standalone ChunkIndexGroup only (no StripeClusterIndex needed)
    flatbuffers::FlatBufferBuilder chunkBuilder;

    std::vector<uint32_t> streamChunkCounts;
    std::vector<uint32_t> streamChunkRows;
    std::vector<uint32_t> streamChunkOffsets;

    uint32_t accumulatedStreamChunkCount = 0;
    for (int i = 0; i < groupStripeCount; ++i, ++stripeIdx) {
      NIMBLE_CHECK_LT(stripeIdx, stripes.size());
      const auto& stripe = stripes[stripeIdx];

      for (const auto& stream : stripe.streams) {
        accumulatedStreamChunkCount += stream.numChunks;
        streamChunkCounts.push_back(accumulatedStreamChunkCount);
        NIMBLE_CHECK_EQ(stream.numChunks, stream.chunkRows.size());
        uint32_t accumulatedRows = 0;
        for (const auto& row : stream.chunkRows) {
          accumulatedRows += row;
          streamChunkRows.push_back(accumulatedRows);
        }
        NIMBLE_CHECK_EQ(stream.numChunks, stream.chunkOffsets.size());
        for (const auto& offset : stream.chunkOffsets) {
          streamChunkOffsets.push_back(offset);
        }
      }
    }

    const uint32_t numStreams = streamChunkCounts.size() / groupStripeCount;

    auto stripeChunkIndex = serialization::CreateStripeChunkIndex(
        chunkBuilder,
        numStreams,
        chunkBuilder.CreateVector(streamChunkCounts),
        chunkBuilder.CreateVector(streamChunkRows),
        chunkBuilder.CreateVector(streamChunkOffsets));
    chunkBuilder.Finish(stripeChunkIndex);

    result.chunkIndexGroupSizes.push_back(chunkBuilder.GetSize());
    result.chunkIndexGroups.append(
        reinterpret_cast<const char*>(chunkBuilder.GetBufferPointer()),
        chunkBuilder.GetSize());

    // Build empty StripeClusterIndex for root ClusterIndex to work.
    flatbuffers::FlatBufferBuilder groupBuilder;
    groupBuilder.Finish(serialization::CreateStripeClusterIndex(groupBuilder));

    uint64_t offset = result.indexGroups.size();
    uint32_t size = groupBuilder.GetSize();
    result.indexGroups.append(
        reinterpret_cast<const char*>(groupBuilder.GetBufferPointer()), size);

    auto metadataSection = serialization::CreateMetadataSection(
        builder, offset, size, serialization::CompressionType_Uncompressed);
    clusterIndexGroupMetadataOffsets.push_back(metadataSection);
  }

  auto stripeCountsVector = builder.CreateVector(stripeCounts);
  auto stripeIndexesVector =
      builder.CreateVector(clusterIndexGroupMetadataOffsets);

  auto index = serialization::CreateClusterIndex(
      builder,
      0 /* stripe_keys = null */,
      0 /* index_columns = null */,
      0 /* sort_orders = null */,
      stripeCountsVector,
      stripeIndexesVector);

  builder.Finish(index);

  result.rootIndex = std::string(
      reinterpret_cast<const char*>(builder.GetBufferPointer()),
      builder.GetSize());

  return result;
}

std::unique_ptr<ClusterIndex> ClusterIndexTestBase::createClusterIndex(
    const IndexBuffers& indexBuffers) {
  Section indexSection{MetadataBuffer(
      *pool_, indexBuffers.rootIndex, CompressionType::Uncompressed)};
  return ClusterIndex::create(std::move(indexSection));
}

std::shared_ptr<ClusterIndexGroup>
ClusterIndexTestBase::createClusterIndexGroup(
    const IndexBuffers& indexBuffers,
    uint32_t stripeGroupIndex) {
  MetadataBuffer rootIndexBuffer(
      *pool_, indexBuffers.rootIndex, CompressionType::Uncompressed);

  auto clusterIndex = createClusterIndex(indexBuffers);
  auto metadata = clusterIndex->groupMetadata(stripeGroupIndex);

  auto groupIndexBuffer = std::make_unique<MetadataBuffer>(
      *pool_,
      std::string_view(
          indexBuffers.indexGroups.data() + metadata.offset(), metadata.size()),
      metadata.compressionType());

  return ClusterIndexGroup::create(
      stripeGroupIndex, rootIndexBuffer, std::move(groupIndexBuffer));
}

std::shared_ptr<ChunkIndexGroup> ClusterIndexTestBase::createChunkIndex(
    const IndexBuffers& indexBuffers,
    uint32_t stripeGroupIndex) {
  // Compute firstStripe and stripeCount from stripe_counts in root index.
  const auto* indexRoot = flatbuffers::GetRoot<serialization::ClusterIndex>(
      indexBuffers.rootIndex.data());
  const auto* stripeCounts = indexRoot->stripe_counts();
  NIMBLE_CHECK_NOT_NULL(stripeCounts);
  NIMBLE_CHECK_LT(stripeGroupIndex, stripeCounts->size());

  const uint32_t firstStripe =
      stripeGroupIndex == 0 ? 0 : stripeCounts->Get(stripeGroupIndex - 1);
  const uint32_t endStripe = stripeCounts->Get(stripeGroupIndex);
  const uint32_t stripeCount = endStripe - firstStripe;

  // Find the ChunkIndexGroup data for this group using recorded sizes.
  NIMBLE_CHECK_LT(stripeGroupIndex, indexBuffers.chunkIndexGroupSizes.size());
  size_t offset = 0;
  for (uint32_t g = 0; g < stripeGroupIndex; ++g) {
    offset += indexBuffers.chunkIndexGroupSizes[g];
  }

  auto chunkIndexBuffer = std::make_unique<MetadataBuffer>(
      *pool_,
      std::string_view(
          indexBuffers.chunkIndexGroups.data() + offset,
          indexBuffers.chunkIndexGroupSizes[stripeGroupIndex]),
      CompressionType::Uncompressed);

  return ChunkIndexGroup::create(
      firstStripe, stripeCount, std::move(chunkIndexBuffer));
}

} // namespace facebook::nimble::index::test
