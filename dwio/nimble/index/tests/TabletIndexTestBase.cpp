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
#include "folly/json/json.h"
#include "velox/common/testutil/TestValue.h"

namespace facebook::nimble::index::test {

void TabletIndexTestBase::SetUpTestCase() {
  velox::memory::MemoryManager::initialize({});
  velox::common::testutil::TestValue::enable();
}

TabletIndexTestBase::IndexBuffers TabletIndexTestBase::createTestTabletIndex(
    const std::vector<std::string>& indexColumns,
    const std::vector<std::string>& sortOrders,
    const std::string& minKey,
    const std::vector<Stripe>& stripes,
    const std::vector<int>& stripeGroups) {
  NIMBLE_CHECK_EQ(indexColumns.size(), sortOrders.size());
  // Build stripe_counts - accumulated number of stripes up to each group.
  std::vector<uint32_t> stripeCounts;
  stripeCounts.reserve(stripeGroups.size());
  uint32_t accumulatedStripes = 0;
  for (uint32_t groupIdx = 0; groupIdx < stripeGroups.size(); ++groupIdx) {
    accumulatedStripes += stripeGroups[groupIdx];
    stripeCounts.push_back(accumulatedStripes);
  }

  flatbuffers::FlatBufferBuilder builder;
  IndexBuffers result;

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
    std::vector<uint32_t> streamChunkRows;
    std::vector<uint32_t> streamChunkOffsets;

    // chunkCounts are accumulated values for key stream only
    uint32_t accumulatedKeyStreamChunkCount = 0;
    // streamChunkCounts are accumulated globally in (stripe, stream) order
    uint32_t accumulatedStreamChunkCount = 0;
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
      // streamChunkCounts are accumulated globally in (stripe, stream) order
      for (const auto& stream : stripe.streams) {
        accumulatedStreamChunkCount += stream.numChunks;
        streamChunkCounts.push_back(accumulatedStreamChunkCount);
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
      }
    }

    // Build StripeValueIndex for this group
    auto valueIndex = serialization::CreateStripeValueIndex(
        groupBuilder,
        groupBuilder.CreateVector(keyStreamOffsets),
        groupBuilder.CreateVector(keyStreamSizes),
        groupBuilder.CreateVector(keyStreamChunkCounts),
        groupBuilder.CreateVector(keyStreamChunkRows),
        groupBuilder.CreateVector(keyStreamChunkOffsets),
        groupBuilder.CreateVector(keyStreamChunkKeys));

    // Build StripePositionIndex for this group
    auto positionIndex = serialization::CreateStripePositionIndex(
        groupBuilder,
        groupBuilder.CreateVector(streamChunkCounts),
        groupBuilder.CreateVector(streamChunkRows),
        groupBuilder.CreateVector(streamChunkOffsets));

    // Build StripeIndexGroup combining all stripes in this group
    auto stripeIndexGroup = serialization::CreateStripeIndexGroup(
        groupBuilder, valueIndex, positionIndex);
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
  // Max key of each stripe is the last key in keyStream.keys
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

  // Build sort_orders vector
  std::vector<flatbuffers::Offset<flatbuffers::String>> sortOrderOffsets;
  sortOrderOffsets.reserve(sortOrders.size());
  for (const auto& sortOrder : sortOrders) {
    sortOrderOffsets.push_back(builder.CreateString(sortOrder));
  }

  // Create all vectors right before CreateIndex to avoid offset invalidation
  auto stripeKeysVector = builder.CreateVector(stripeKeyOffsets);
  auto indexColumnsVector = builder.CreateVector(indexColumnOffsets);
  auto sortOrdersVector = builder.CreateVector(sortOrderOffsets);
  auto stripeCountsVector = builder.CreateVector(stripeCounts);
  auto stripeIndexGroupsVector =
      builder.CreateVector(stripeIndexGroupMetadataOffsets);

  // Build the Index table
  auto index = serialization::CreateIndex(
      builder,
      stripeKeysVector,
      indexColumnsVector,
      sortOrdersVector,
      stripeCountsVector,
      stripeIndexGroupsVector);

  builder.Finish(index);

  result.rootIndex = std::string(
      reinterpret_cast<const char*>(builder.GetBufferPointer()),
      builder.GetSize());

  return result;
}

TabletIndexTestBase::IndexBuffers TabletIndexTestBase::createTestTabletIndex(
    const std::vector<std::string>& indexColumns,
    const std::string& minKey,
    const std::vector<Stripe>& stripes,
    const std::vector<int>& stripeGroups) {
  // Default to serialized "ASC NULLS FIRST" sort orders for all columns
  std::vector<std::string> defaultSortOrders;
  defaultSortOrders.reserve(indexColumns.size());
  for (size_t i = 0; i < indexColumns.size(); ++i) {
    defaultSortOrders.push_back(
        folly::toJson(velox::core::kAscNullsFirst.serialize()));
  }
  return createTestTabletIndex(
      indexColumns, defaultSortOrders, minKey, stripes, stripeGroups);
}

std::unique_ptr<TabletIndex> TabletIndexTestBase::createTabletIndex(
    const IndexBuffers& indexBuffers) {
  Section indexSection{MetadataBuffer(
      *pool_, indexBuffers.rootIndex, CompressionType::Uncompressed)};
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
  auto metadata = tabletIndex->groupIndexMetadata(stripeGroupIndex);

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

} // namespace facebook::nimble::index::test
