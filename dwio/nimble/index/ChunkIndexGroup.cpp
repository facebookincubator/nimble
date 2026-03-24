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
#include "dwio/nimble/index/ChunkIndexGroup.h"

#include "dwio/nimble/tablet/ChunkIndexGenerated.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"

#include <algorithm>

namespace facebook::nimble::index {

namespace {

template <typename T>
const T* asFlatBuffersRoot(std::string_view content) {
  return flatbuffers::GetRoot<T>(content.data());
}

} // namespace

std::shared_ptr<ChunkIndexGroup> ChunkIndexGroup::create(
    uint32_t firstStripe,
    uint32_t stripeCount,
    std::unique_ptr<MetadataBuffer> metadata) {
  return std::shared_ptr<ChunkIndexGroup>(
      new ChunkIndexGroup(firstStripe, stripeCount, std::move(metadata)));
}

ChunkIndexGroup::ChunkIndexGroup(
    uint32_t firstStripe,
    uint32_t stripeCount,
    std::unique_ptr<MetadataBuffer> metadata)
    : metadata_{std::move(metadata)},
      firstStripe_{firstStripe},
      stripeCount_{stripeCount},
      streamCount_{asFlatBuffersRoot<serialization::StripeChunkIndex>(
                       metadata_->content())
                       ->stream_count()} {}

std::shared_ptr<StreamIndex> ChunkIndexGroup::createStreamIndex(
    uint32_t stripe,
    uint32_t streamId) const {
  if (streamId >= streamCount_) {
    return nullptr;
  }

  const uint32_t stripeOff = stripeOffset(stripe);
  const auto* root =
      asFlatBuffersRoot<serialization::StripeChunkIndex>(metadata_->content());

  const auto* streamChunkCounts = root->stream_chunk_counts();
  NIMBLE_CHECK_NOT_NULL(streamChunkCounts);

  const uint32_t streamChunkCountIndex = stripeOff * streamCount_ + streamId;
  NIMBLE_CHECK_LT(streamChunkCountIndex, streamChunkCounts->size());

  const uint32_t endChunkOffset = streamChunkCounts->Get(streamChunkCountIndex);
  const uint32_t startChunkOffset = streamChunkCountIndex == 0
      ? 0
      : streamChunkCounts->Get(streamChunkCountIndex - 1);

  // Single-chunk streams don't need index lookup.
  if (endChunkOffset - startChunkOffset <= 1) {
    return nullptr;
  }

  return StreamIndex::create(
      shared_from_this(), streamId, startChunkOffset, endChunkOffset);
}

std::shared_ptr<StreamIndex> StreamIndex::create(
    std::shared_ptr<const ChunkIndexGroup> chunkIndex,
    uint32_t streamId,
    uint32_t startChunkOffset,
    uint32_t endChunkOffset) {
  return std::shared_ptr<StreamIndex>(new StreamIndex(
      std::move(chunkIndex), streamId, startChunkOffset, endChunkOffset));
}

StreamIndex::StreamIndex(
    std::shared_ptr<const ChunkIndexGroup> chunkIndex,
    uint32_t streamId,
    uint32_t startChunkOffset,
    uint32_t endChunkOffset)
    : chunkIndex_(std::move(chunkIndex)),
      streamId_(streamId),
      startChunkOffset_(startChunkOffset),
      endChunkOffset_(endChunkOffset) {
  NIMBLE_CHECK_NOT_NULL(chunkIndex_);
}

ChunkLocation StreamIndex::lookupChunk(uint32_t rowId) const {
  const auto* root = asFlatBuffersRoot<serialization::StripeChunkIndex>(
      chunkIndex_->metadata_->content());

  const auto* chunkRows = root->stream_chunk_rows();
  NIMBLE_CHECK_NOT_NULL(chunkRows);
  NIMBLE_CHECK_LE(endChunkOffset_, chunkRows->size());

  const auto beginIt = chunkRows->begin() + startChunkOffset_;
  const auto endIt = chunkRows->begin() + endChunkOffset_;
  const auto it = std::lower_bound(beginIt, endIt, rowId + 1);
  NIMBLE_CHECK(
      it != endIt,
      "Row ID {} is beyond the last chunk in stream {}",
      rowId,
      streamId_);

  const uint32_t chunkOffset = it - chunkRows->begin();
  const auto* chunkOffsets = root->stream_chunk_offsets();
  NIMBLE_CHECK_NOT_NULL(chunkOffsets);
  const uint32_t rowOffset =
      chunkOffset == startChunkOffset_ ? 0 : chunkRows->Get(chunkOffset - 1);
  return ChunkLocation{chunkOffsets->Get(chunkOffset), rowOffset};
}

uint32_t StreamIndex::rowCount() const {
  if (endChunkOffset_ == startChunkOffset_) {
    return 0;
  }

  const auto* root = asFlatBuffersRoot<serialization::StripeChunkIndex>(
      chunkIndex_->metadata_->content());

  const auto* chunkRows = root->stream_chunk_rows();
  NIMBLE_CHECK_NOT_NULL(chunkRows);
  NIMBLE_CHECK_LE(endChunkOffset_, chunkRows->size());

  // stream_chunk_rows stores per-stream accumulated row counts, so the last
  // entry in this stream's chunk range is the total row count.
  return chunkRows->Get(endChunkOffset_ - 1);
}

} // namespace facebook::nimble::index
