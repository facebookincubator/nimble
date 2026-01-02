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
#include "dwio/nimble/index/StripeIndexGroup.h"

#include "dwio/nimble/tablet/IndexGenerated.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"

#include <algorithm>

namespace facebook::nimble::index {

namespace {
template <typename T>
const T* asFlatBuffersRoot(std::string_view content) {
  return flatbuffers::GetRoot<T>(content.data());
}

uint32_t getStripeCount(
    const MetadataBuffer& rootIndex,
    uint32_t stripeGroupIndex) {
  const auto* indexRoot =
      asFlatBuffersRoot<serialization::Index>(rootIndex.content());
  auto* stripeCounts = indexRoot->stripe_counts();
  NIMBLE_CHECK_NOT_NULL(stripeCounts);
  NIMBLE_CHECK_LT(stripeGroupIndex, stripeCounts->size());
  const uint32_t endStripe = stripeCounts->Get(stripeGroupIndex);
  const uint32_t startStripe =
      stripeGroupIndex == 0 ? 0 : stripeCounts->Get(stripeGroupIndex - 1);
  return endStripe - startStripe;
}

uint32_t getFirstStripe(
    const MetadataBuffer& rootIndex,
    uint32_t stripeGroupIndex) {
  const auto* indexRoot =
      asFlatBuffersRoot<serialization::Index>(rootIndex.content());
  auto* stripeCounts = indexRoot->stripe_counts();
  NIMBLE_CHECK_NOT_NULL(stripeCounts);
  NIMBLE_CHECK_LT(stripeGroupIndex, stripeCounts->size());
  return stripeGroupIndex == 0 ? 0 : stripeCounts->Get(stripeGroupIndex - 1);
}

uint32_t getStreamCount(const MetadataBuffer* metadata, uint32_t stripeCount) {
  const auto* root =
      asFlatBuffersRoot<serialization::StripeIndexGroup>(metadata->content());
  NIMBLE_CHECK_GT(stripeCount, 0);
  const auto* positionIndex = root->position_index();
  NIMBLE_CHECK_NOT_NULL(positionIndex);
  const auto* streamChunkCounts = positionIndex->stream_chunk_counts();
  NIMBLE_CHECK_NOT_NULL(streamChunkCounts);
  NIMBLE_CHECK_GE(streamChunkCounts->size(), stripeCount);
  NIMBLE_CHECK_EQ(
      streamChunkCounts->size() % stripeCount,
      0,
      "stream_chunk_counts size must be divisible by stripe_count");
  return streamChunkCounts->size() / stripeCount;
}
} // namespace

std::shared_ptr<StripeIndexGroup> StripeIndexGroup::create(
    uint32_t stripeGroupIndex,
    const MetadataBuffer& rootIndex,
    std::unique_ptr<MetadataBuffer> groupIndex) {
  const uint32_t firstStripe = getFirstStripe(rootIndex, stripeGroupIndex);
  const uint32_t stripeCount = getStripeCount(rootIndex, stripeGroupIndex);

  return std::shared_ptr<StripeIndexGroup>(new StripeIndexGroup(
      stripeGroupIndex, firstStripe, stripeCount, std::move(groupIndex)));
}

StripeIndexGroup::StripeIndexGroup(
    uint32_t groupIndex,
    uint32_t firstStripe,
    uint32_t stripeCount,
    std::unique_ptr<MetadataBuffer> metadata)
    : metadata_{std::move(metadata)},
      groupIndex_{groupIndex},
      firstStripe_{firstStripe},
      stripeCount_{stripeCount},
      streamCount_{getStreamCount(metadata_.get(), stripeCount)} {}

std::optional<ChunkLocation> StripeIndexGroup::lookupChunk(
    uint32_t stripe,
    std::string_view encodedKey) const {
  const uint32_t stripeOffset = this->stripeOffset(stripe);
  const auto* root =
      asFlatBuffersRoot<serialization::StripeIndexGroup>(metadata_->content());

  // Get value index (single object for the entire group)
  const auto* valueIndex = root->value_index();
  NIMBLE_CHECK_NOT_NULL(valueIndex);

  // Get chunk counts to determine the search range for this stripe
  const auto* chunkCounts = valueIndex->key_stream_chunk_counts();
  NIMBLE_CHECK_NOT_NULL(chunkCounts);
  NIMBLE_CHECK_EQ(stripeCount_, chunkCounts->size());

  // Determine the chunk range for this stripe
  // chunkCounts contains accumulated counts, so:
  // - Start index: chunkCounts[stripeOffset - 1] (or 0 for first stripe)
  // - End index: chunkCounts[stripeOffset]
  const uint32_t startChunkIndex =
      stripeOffset == 0 ? 0 : chunkCounts->Get(stripeOffset - 1);
  const uint32_t endChunkIndex = chunkCounts->Get(stripeOffset);

  const auto* chunkKeys = valueIndex->key_stream_chunk_keys();
  NIMBLE_CHECK_NOT_NULL(chunkKeys);
  NIMBLE_CHECK_LE(endChunkIndex, chunkKeys->size());

  // Binary search for the chunk key within this stripe's range
  auto it = std::lower_bound(
      chunkKeys->begin() + startChunkIndex,
      chunkKeys->begin() + endChunkIndex,
      encodedKey,
      [](const flatbuffers::String* a, std::string_view b) {
        return a->string_view() < b;
      });
  if (it == chunkKeys->begin() + endChunkIndex) {
    return std::nullopt;
  }

  const uint32_t chunkIndex = it - chunkKeys->begin();

  const auto* chunkRows = valueIndex->key_stream_chunk_rows();
  NIMBLE_CHECK_NOT_NULL(chunkRows);
  NIMBLE_CHECK_EQ(chunkRows->size(), chunkKeys->size());

  const auto* chunkOffsets = valueIndex->key_stream_chunk_offsets();
  NIMBLE_CHECK_NOT_NULL(chunkOffsets);
  NIMBLE_CHECK_EQ(chunkOffsets->size(), chunkKeys->size());

  // Return the found chunk location.
  const uint32_t rowOffset =
      chunkIndex == startChunkIndex ? 0 : chunkRows->Get(chunkIndex - 1);
  return ChunkLocation{chunkOffsets->Get(chunkIndex), rowOffset};
}

std::optional<ChunkLocation> StripeIndexGroup::lookupChunk(
    uint32_t stripe,
    uint32_t streamId,
    uint32_t rowId) const {
  NIMBLE_CHECK_LT(streamId, streamCount_, "streamId out of range");

  const uint32_t stripeOffset = this->stripeOffset(stripe);
  const auto* root =
      asFlatBuffersRoot<serialization::StripeIndexGroup>(metadata_->content());

  // Get position index (single object for the entire group)
  const auto* positionIndex = root->position_index();
  NIMBLE_CHECK_NOT_NULL(positionIndex);

  // Use accumulated stream_chunk_counts to find the range
  // for this specific stream in this stripe
  const auto* streamChunkCounts = positionIndex->stream_chunk_counts();
  NIMBLE_CHECK_NOT_NULL(streamChunkCounts);

  // Calculate the index in the flattened stream_chunk_counts array
  const uint32_t streamChunkCountIndex = stripeOffset * streamCount_ + streamId;
  NIMBLE_CHECK_LT(streamChunkCountIndex, streamChunkCounts->size());

  // stream_chunk_counts is accumulated, so:
  // - Start index: streamChunkCounts[streamChunkCountIndex - 1] (or 0 for
  // first)
  // - End index: streamChunkCounts[streamChunkCountIndex]
  const uint32_t endChunkIndex = streamChunkCounts->Get(streamChunkCountIndex);
  const uint32_t startChunkIndex = streamChunkCountIndex == 0
      ? 0
      : streamChunkCounts->Get(streamChunkCountIndex - 1);

  const auto* chunkRows = positionIndex->stream_chunk_rows();
  NIMBLE_CHECK_NOT_NULL(chunkRows);
  NIMBLE_CHECK_LE(endChunkIndex, chunkRows->size());

  // Binary search within the specific stream's chunk range
  const auto beginIt = chunkRows->begin() + startChunkIndex;
  const auto endIt = chunkRows->begin() + endChunkIndex;
  const auto it = std::lower_bound(beginIt, endIt, rowId + 1);
  if (it == endIt) {
    return std::nullopt;
  }

  const uint32_t chunkIndex = it - chunkRows->begin();
  const auto* chunkOffsets = positionIndex->stream_chunk_offsets();
  NIMBLE_CHECK_NOT_NULL(chunkOffsets);
  const uint32_t rowOffset =
      chunkIndex == startChunkIndex ? 0 : chunkRows->Get(chunkIndex - 1);
  return ChunkLocation{chunkOffsets->Get(chunkIndex), rowOffset};
}

std::unique_ptr<StreamIndex> StripeIndexGroup::createStreamIndex(
    uint32_t stripe,
    uint32_t streamId) const {
  return StreamIndex::create(this, stripe, streamId);
}

uint32_t StripeIndexGroup::rowCount(uint32_t stripe, uint32_t streamId) const {
  NIMBLE_CHECK_LT(streamId, streamCount_, "streamId out of range");

  const uint32_t stripeOffset = this->stripeOffset(stripe);
  const auto* root =
      asFlatBuffersRoot<serialization::StripeIndexGroup>(metadata_->content());

  const auto* positionIndex = root->position_index();
  NIMBLE_CHECK_NOT_NULL(positionIndex);

  const auto* streamChunkCounts = positionIndex->stream_chunk_counts();
  NIMBLE_CHECK_NOT_NULL(streamChunkCounts);

  const uint32_t streamChunkCountIndex = stripeOffset * streamCount_ + streamId;
  NIMBLE_CHECK_LT(streamChunkCountIndex, streamChunkCounts->size());

  const uint32_t endChunkIndex = streamChunkCounts->Get(streamChunkCountIndex);
  const uint32_t startChunkIndex = streamChunkCountIndex == 0
      ? 0
      : streamChunkCounts->Get(streamChunkCountIndex - 1);

  // If no chunks for this stream, return 0
  if (endChunkIndex == startChunkIndex) {
    return 0;
  }

  const auto* chunkRows = positionIndex->stream_chunk_rows();
  NIMBLE_CHECK_NOT_NULL(chunkRows);
  NIMBLE_CHECK_LE(endChunkIndex, chunkRows->size());

  // The last chunk's accumulated row count is the total row count
  return chunkRows->Get(endChunkIndex - 1);
}

std::optional<velox::common::Region> StripeIndexGroup::keyStreamRegion(
    uint32_t stripeIndex) const {
  const uint32_t stripeOffset = this->stripeOffset(stripeIndex);
  const auto* root =
      asFlatBuffersRoot<serialization::StripeIndexGroup>(metadata_->content());

  const auto* valueIndex = root->value_index();
  NIMBLE_CHECK_NOT_NULL(valueIndex);

  const auto* offsets = valueIndex->key_stream_offsets();
  NIMBLE_CHECK_NOT_NULL(offsets);
  const auto* sizes = valueIndex->key_stream_sizes();
  NIMBLE_CHECK_NOT_NULL(sizes);
  NIMBLE_CHECK_EQ(
      offsets->size(),
      sizes->size(),
      "key_stream_offsets and key_stream_sizes must have the same size");
  NIMBLE_CHECK_LT(stripeOffset, offsets->size());

  const uint32_t keyStreamOffset = offsets->Get(stripeOffset);
  const uint32_t keyStreamSize = sizes->Get(stripeOffset);
  NIMBLE_CHECK_GT(keyStreamSize, 0);
  return velox::common::Region{keyStreamOffset, keyStreamSize};
}

std::unique_ptr<StreamIndex> StreamIndex::create(
    const StripeIndexGroup* stripeIndexGroup,
    uint32_t stripe,
    uint32_t streamId) {
  return std::unique_ptr<StreamIndex>(
      new StreamIndex(stripeIndexGroup, stripe, streamId));
}

StreamIndex::StreamIndex(
    const StripeIndexGroup* stripeIndexGroup,
    uint32_t stripe,
    uint32_t streamId)
    : stripeIndexGroup_(stripeIndexGroup),
      stripe_(stripe),
      streamId_(streamId) {
  NIMBLE_CHECK_NOT_NULL(stripeIndexGroup_);
}

std::optional<ChunkLocation> StreamIndex::lookupChunk(uint32_t rowId) const {
  return stripeIndexGroup_->lookupChunk(stripe_, streamId_, rowId);
}

uint32_t StreamIndex::rowCount() const {
  return stripeIndexGroup_->rowCount(stripe_, streamId_);
}

} // namespace facebook::nimble::index
