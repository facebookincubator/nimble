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
#include "dwio/nimble/index/ClusterIndexGroup.h"

#include "dwio/nimble/tablet/ClusterIndexGenerated.h"
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
      asFlatBuffersRoot<serialization::ClusterIndex>(rootIndex.content());
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
      asFlatBuffersRoot<serialization::ClusterIndex>(rootIndex.content());
  auto* stripeCounts = indexRoot->stripe_counts();
  NIMBLE_CHECK_NOT_NULL(stripeCounts);
  NIMBLE_CHECK_LT(stripeGroupIndex, stripeCounts->size());
  return stripeGroupIndex == 0 ? 0 : stripeCounts->Get(stripeGroupIndex - 1);
}

/// Parsed chunk index for a stripe's chunk range.
struct StripeChunkRange {
  uint32_t startChunkIndex;
  uint32_t endChunkIndex;
  const flatbuffers::Vector<uint32_t>* chunkRows;
  const flatbuffers::Vector<uint32_t>* chunkOffsets;
  const serialization::KeyStreamChunkIndex* chunkIndex;

  ChunkLocation locationAt(uint32_t chunkOffset) const {
    const uint32_t rowOffset =
        chunkOffset == startChunkIndex ? 0 : chunkRows->Get(chunkOffset - 1);
    return ChunkLocation{chunkOffsets->Get(chunkOffset), rowOffset};
  }
};

/// Returns the chunk range for a stripe, or nullopt if the stripe has no
/// chunk index (single chunk per stripe).
std::optional<StripeChunkRange> getStripeChunkRange(
    const serialization::StripeClusterIndex* root,
    uint32_t stripeOffset,
    uint32_t stripeCount) {
  const auto* chunkIndex = root->chunk_index();
  if (chunkIndex == nullptr) {
    return std::nullopt;
  }

  const auto* chunkCounts = chunkIndex->chunk_counts();
  NIMBLE_CHECK_NOT_NULL(chunkCounts);
  NIMBLE_CHECK_EQ(stripeCount, chunkCounts->size());

  const uint32_t startChunkIndex =
      stripeOffset == 0 ? 0 : chunkCounts->Get(stripeOffset - 1);
  const uint32_t endChunkIndex = chunkCounts->Get(stripeOffset);

  const auto* chunkRows = chunkIndex->chunk_rows();
  NIMBLE_CHECK_NOT_NULL(chunkRows);

  const auto* chunkOffsets = chunkIndex->chunk_offsets();
  NIMBLE_CHECK_NOT_NULL(chunkOffsets);

  return StripeChunkRange{
      startChunkIndex, endChunkIndex, chunkRows, chunkOffsets, chunkIndex};
}

} // namespace

std::shared_ptr<ClusterIndexGroup> ClusterIndexGroup::create(
    uint32_t stripeGroupIndex,
    const MetadataBuffer& rootIndex,
    std::unique_ptr<MetadataBuffer> groupIndex) {
  const uint32_t firstStripe = getFirstStripe(rootIndex, stripeGroupIndex);
  const uint32_t stripeCount = getStripeCount(rootIndex, stripeGroupIndex);

  return std::shared_ptr<ClusterIndexGroup>(new ClusterIndexGroup(
      stripeGroupIndex, firstStripe, stripeCount, std::move(groupIndex)));
}

ClusterIndexGroup::ClusterIndexGroup(
    uint32_t groupIndex,
    uint32_t firstStripe,
    uint32_t stripeCount,
    std::unique_ptr<MetadataBuffer> metadata)
    : metadata_{std::move(metadata)},
      groupIndex_{groupIndex},
      firstStripe_{firstStripe},
      stripeCount_{stripeCount} {}

std::optional<ChunkLocation> ClusterIndexGroup::lookupChunk(
    uint32_t stripe,
    std::string_view encodedKey) const {
  const auto* root = asFlatBuffersRoot<serialization::StripeClusterIndex>(
      metadata_->content());
  auto range = getStripeChunkRange(root, stripeOffset(stripe), stripeCount_);
  if (!range.has_value()) {
    return ChunkLocation{0, 0};
  }

  const auto* chunkKeys = range->chunkIndex->chunk_keys();
  NIMBLE_CHECK_NOT_NULL(chunkKeys);
  NIMBLE_CHECK_LE(range->endChunkIndex, chunkKeys->size());

  // Binary search for the chunk key within this stripe's range.
  // Chunk keys are the last key in each chunk. lower_bound finds the first
  // chunk whose last key >= encodedKey, meaning the chunk that may contain
  // the target key.
  auto it = std::lower_bound(
      chunkKeys->begin() + range->startChunkIndex,
      chunkKeys->begin() + range->endChunkIndex,
      encodedKey,
      [](const flatbuffers::String* a, std::string_view b) {
        return a->string_view() < b;
      });
  if (it == chunkKeys->begin() + range->endChunkIndex) {
    return std::nullopt;
  }
  return range->locationAt(it - chunkKeys->begin());
}

std::optional<ChunkLocation> ClusterIndexGroup::lookupChunk(
    uint32_t stripe,
    uint32_t row) const {
  const auto* root = asFlatBuffersRoot<serialization::StripeClusterIndex>(
      metadata_->content());
  auto range = getStripeChunkRange(root, stripeOffset(stripe), stripeCount_);
  if (!range.has_value()) {
    return ChunkLocation{0, 0};
  }

  // Binary search on cumulative chunk_rows to find the chunk containing the
  // target row.
  auto it = std::lower_bound(
      range->chunkRows->begin() + range->startChunkIndex,
      range->chunkRows->begin() + range->endChunkIndex,
      row + 1,
      [](uint32_t chunkRowCount, uint32_t target) {
        return chunkRowCount < target;
      });
  if (it == range->chunkRows->begin() + range->endChunkIndex) {
    return std::nullopt;
  }
  return range->locationAt(it - range->chunkRows->begin());
}

velox::common::Region ClusterIndexGroup::keyStreamRegion(
    uint32_t stripeIndex,
    uint64_t stripeBaseOffset) const {
  const uint32_t stripeOffset = this->stripeOffset(stripeIndex);
  const auto* root = asFlatBuffersRoot<serialization::StripeClusterIndex>(
      metadata_->content());

  const auto* keyStream = root->key_stream();
  NIMBLE_CHECK_NOT_NULL(keyStream);

  const auto* offsets = keyStream->offsets();
  NIMBLE_CHECK_NOT_NULL(offsets);
  const auto* sizes = keyStream->sizes();
  NIMBLE_CHECK_NOT_NULL(sizes);
  NIMBLE_CHECK_EQ(
      offsets->size(),
      sizes->size(),
      "offsets and sizes must have the same size");
  NIMBLE_CHECK_LT(stripeOffset, offsets->size());

  const uint32_t keyStreamOffset = offsets->Get(stripeOffset);
  const uint32_t keyStreamSize = sizes->Get(stripeOffset);
  NIMBLE_CHECK_GT(keyStreamSize, 0);

  return velox::common::Region{
      stripeBaseOffset + keyStreamOffset, keyStreamSize};
}

} // namespace facebook::nimble::index
