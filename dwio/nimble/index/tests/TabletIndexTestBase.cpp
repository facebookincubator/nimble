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
#include "dwio/nimble/index/tests/TabletIndexTestBase.h"

#include "dwio/nimble/tablet/IndexGenerated.h"

namespace facebook::nimble::test {

void TabletIndexTestBase::SetUpTestCase() {
  velox::memory::MemoryManager::initialize({});
}

TabletIndexTestBase::IndexBuffers TabletIndexTestBase::createTestTabletIndex(
    const std::vector<std::string>& indexColumns,
    const std::string& minKey,
    const std::vector<Stripe>& stripes,
    const std::vector<int>& stripeGroups) {
  IndexBuffers result;

  flatbuffers::FlatBufferBuilder builder;

  // Build stripe_group_indices - for each stripe, which group it belongs to.
  // Size equals number of stripes.
  std::vector<uint32_t> stripeGroupIndices;
  stripeGroupIndices.reserve(stripes.size());
  for (uint32_t groupIdx = 0; groupIdx < stripeGroups.size(); ++groupIdx) {
    for (int i = 0; i < stripeGroups[groupIdx]; ++i) {
      stripeGroupIndices.push_back(groupIdx);
    }
  }

  // Build StripeIndexGroup for each group and serialize to buffer
  std::vector<flatbuffers::Offset<serialization::MetadataSection>>
      stripeIndexGroupMetadataOffsets;
  stripeIndexGroupMetadataOffsets.reserve(stripeGroups.size());

  uint32_t stripeIdx = 0;
  for (const auto& groupStripeCount : stripeGroups) {
    // Create a separate builder for each StripeIndexGroup
    flatbuffers::FlatBufferBuilder groupBuilder;

    // Collect data for all stripes in this group
    std::vector<uint32_t> keyStreamOffsets;
    std::vector<uint32_t> keyStreamSizes;
    std::vector<uint32_t> keyStreamChunkCounts;
    std::vector<uint32_t> keyStreamChunkRows;
    std::vector<flatbuffers::Offset<flatbuffers::String>> keyStreamChunkKeys;
    std::vector<uint32_t> keyStreamChunkOffsets;

    std::vector<uint32_t> streamChunkCounts;
    std::vector<uint32_t> streamChunkIndexes;
    std::vector<uint32_t> streamChunkRows;
    std::vector<uint32_t> streamChunkOffsets;

    uint32_t currentChunkIndex = 0;
    // chunkCounts are accumulated values for key stream only
    uint32_t accumulatedKeyStreamChunkCount = 0;
    for (int i = 0; i < groupStripeCount; ++i, ++stripeIdx) {
      NIMBLE_CHECK_LT(stripeIdx, stripes.size());
      const auto& stripe = stripes[stripeIdx];

      // Add key stream data for this stripe
      const auto& keyStream = stripe.keyStream;
      keyStreamOffsets.push_back(keyStream.streamOffset);
      keyStreamSizes.push_back(keyStream.streamSize);
      accumulatedKeyStreamChunkCount += keyStream.stream.numChunks;
      keyStreamChunkCounts.push_back(accumulatedKeyStreamChunkCount);
      NIMBLE_CHECK_EQ(
          keyStream.stream.numChunks, keyStream.stream.chunkRows.size());
      // chunkRows are accumulated values (cumulative row counts)
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

      // Add position index data for each stream in this stripe
      for (const auto& stream : stripe.streams) {
        streamChunkCounts.push_back(stream.numChunks);
        streamChunkIndexes.push_back(currentChunkIndex);
        NIMBLE_CHECK_EQ(stream.numChunks, stream.chunkRows.size());
        // chunkRows are accumulated values (cumulative row counts)
        uint32_t accumulatedRows = 0;
        for (const auto& row : stream.chunkRows) {
          accumulatedRows += row;
          streamChunkRows.push_back(accumulatedRows);
        }
        NIMBLE_CHECK_EQ(stream.numChunks, stream.chunkOffsets.size());
        for (const auto& offset : stream.chunkOffsets) {
          streamChunkOffsets.push_back(offset);
        }
        currentChunkIndex += stream.numChunks;
      }
    }

    // Build StripeValueIndex for this group
    auto valueIndex = serialization::CreateStripeValueIndex(
        groupBuilder,
        groupBuilder.CreateVector(keyStreamOffsets),
        groupBuilder.CreateVector(keyStreamSizes),
        groupBuilder.CreateVector(keyStreamChunkCounts),
        groupBuilder.CreateVector(keyStreamChunkRows),
        groupBuilder.CreateVector(keyStreamChunkKeys),
        groupBuilder.CreateVector(keyStreamChunkOffsets));

    // Build StripePositionIndex for this group
    auto positionIndex = serialization::CreateStripePositionIndex(
        groupBuilder,
        groupBuilder.CreateVector(streamChunkCounts),
        groupBuilder.CreateVector(streamChunkIndexes),
        groupBuilder.CreateVector(streamChunkRows),
        groupBuilder.CreateVector(streamChunkOffsets));

    // Build StripeIndexGroup combining all stripes in this group
    auto stripeIndexGroup = serialization::CreateStripeIndexGroup(
        groupBuilder, groupStripeCount, valueIndex, positionIndex);
    groupBuilder.Finish(stripeIndexGroup);

    // Record offset before appending
    uint64_t offset = result.indexGroups.size();
    uint32_t size = groupBuilder.GetSize();

    // Append serialized StripeIndexGroup to the buffer
    result.indexGroups.append(
        reinterpret_cast<const char*>(groupBuilder.GetBufferPointer()), size);

    // Create MetadataSection for this group in the main builder
    auto metadataSection = serialization::CreateMetadataSection(
        builder, offset, size, serialization::CompressionType_Uncompressed);
    stripeIndexGroupMetadataOffsets.push_back(metadataSection);
  }
  // Build stripe_keys vector
  // First element is the min key, rest are max keys of each stripe
  // Max key of each stripe is the last key in keyStream.chunkKeys
  std::vector<flatbuffers::Offset<flatbuffers::String>> stripeKeyOffsets;
  stripeKeyOffsets.reserve(stripes.size() + 1);
  stripeKeyOffsets.push_back(builder.CreateString(minKey));
  for (const auto& stripe : stripes) {
    const auto& chunkKeys = stripe.keyStream.chunkKeys;
    NIMBLE_CHECK(!chunkKeys.empty());
    stripeKeyOffsets.push_back(builder.CreateString(chunkKeys.back()));
  }

  // Build index_columns vector
  std::vector<flatbuffers::Offset<flatbuffers::String>> indexColumnOffsets;
  indexColumnOffsets.reserve(indexColumns.size());
  for (const auto& column : indexColumns) {
    indexColumnOffsets.push_back(builder.CreateString(column));
  }

  // Create all vectors right before CreateIndex to avoid offset invalidation
  auto stripeKeysVector = builder.CreateVector(stripeKeyOffsets);
  auto indexColumnsVector = builder.CreateVector(indexColumnOffsets);
  auto stripeGroupIndicesVector = builder.CreateVector(stripeGroupIndices);
  auto stripeIndexGroupsVector =
      builder.CreateVector(stripeIndexGroupMetadataOffsets);

  // Build the Index table
  auto index = serialization::CreateIndex(
      builder,
      stripeKeysVector,
      indexColumnsVector,
      stripeGroupIndicesVector,
      stripeIndexGroupsVector);

  builder.Finish(index);

  result.rootIndex = std::string(
      reinterpret_cast<const char*>(builder.GetBufferPointer()),
      builder.GetSize());

  return result;
}

std::unique_ptr<TabletIndex> TabletIndexTestBase::createTabletIndex(
    const IndexBuffers& indexBuffers) {
  auto indexSection = std::make_unique<MetadataBuffer>(
      *pool_, indexBuffers.rootIndex, CompressionType::Uncompressed);
  return TabletIndex::create(std::move(indexSection));
}

std::shared_ptr<StripeIndexGroup> TabletIndexTestBase::createStripeIndexGroup(
    const IndexBuffers& indexBuffers,
    uint32_t stripeGroupIndex) {
  // Create the root index MetadataBuffer
  MetadataBuffer rootIndexBuffer(
      *pool_, indexBuffers.rootIndex, CompressionType::Uncompressed);

  // Get the metadata for the specified stripe group from the root index
  auto tabletIndex = createTabletIndex(indexBuffers);
  auto metadata = tabletIndex->indexGroupMetadata(stripeGroupIndex);

  // Create the group index MetadataBuffer from the indexGroups buffer
  // using the offset and size from the metadata
  auto groupIndexBuffer = std::make_unique<MetadataBuffer>(
      *pool_,
      std::string_view(
          indexBuffers.indexGroups.data() + metadata.offset(), metadata.size()),
      metadata.compressionType());

  return StripeIndexGroup::create(
      stripeGroupIndex, rootIndexBuffer, std::move(groupIndexBuffer));
}

} // namespace facebook::nimble::test
