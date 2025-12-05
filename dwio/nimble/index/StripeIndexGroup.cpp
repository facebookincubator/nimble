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
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/MetadataBuffer.h"
#include "dwio/nimble/tablet/IndexGenerated.h"

#include <algorithm>

namespace facebook::nimble {

namespace {
template <typename T>
const T* asFlatBuffersRoot(std::string_view content) {
  return flatbuffers::GetRoot<T>(content.data());
}
} // namespace

std::shared_ptr<StripeIndexGroup> StripeIndexGroup::create(
    uint32_t stripeIndexGroupId,
    std::unique_ptr<MetadataBuffer> metadata) {
  return std::shared_ptr<StripeIndexGroup>(
      new StripeIndexGroup(stripeIndexGroupId, std::move(metadata)));
}

StripeIndexGroup::StripeIndexGroup(
    uint32_t stripeIndexGroupId,
    std::unique_ptr<MetadataBuffer> metadata)
    : metadata_{std::move(metadata)}, stripeIndexGroupId_{stripeIndexGroupId} {}

std::optional<ChunkLocation> StripeIndexGroup::lookupChunk(
    uint32_t stripe,
    std::string_view encodedKey) const {
  const auto* root =
      asFlatBuffersRoot<serialization::StripeIndexGroup>(metadata_->content());

  // Get value index for the stripe
  const auto* valueIndices = root->value_index();
  NIMBLE_CHECK_NOT_NULL(valueIndices);
  NIMBLE_CHECK_LT(
      stripe, valueIndices->size(), "Stripe out of range for value index");

  const auto* stripeValueIndex = valueIndices->Get(stripe);
  NIMBLE_CHECK_NOT_NULL(stripeValueIndex);

  const auto* chunkKeys = stripeValueIndex->chunk_keys();
  NIMBLE_CHECK_NOT_NULL(chunkKeys);

  // Binary search for the chunk key
  auto it = std::lower_bound(
      chunkKeys->begin(),
      chunkKeys->end(),
      encodedKey,
      [](const flatbuffers::String* a, std::string_view b) {
        return a->string_view() < b;
      });
  if (it == chunkKeys->end()) {
    return std::nullopt;
  }

  const uint32_t chunkIndex = it - chunkKeys->begin();

  // Get position index for the stripe
  const auto* positionIndices = root->position_index();
  NIMBLE_CHECK_NOT_NULL(positionIndices);
  NIMBLE_CHECK_LT(
      stripe,
      positionIndices->size(),
      "Stripe out of range for position index");
  NIMBLE_CHECK_EQ(positionIndices->size(), valueIndices->size());

  const auto* stripePositionIndex = positionIndices->Get(stripe);
  NIMBLE_CHECK_NOT_NULL(stripePositionIndex);

  const auto* streamsIndex = stripePositionIndex->streams();
  NIMBLE_CHECK_NOT_NULL(streamsIndex);
  const uint32_t keyStreamId = 0;
  NIMBLE_CHECK_LT(keyStreamId, streamsIndex->size());
  const auto* keyStreamIndex = streamsIndex->Get(keyStreamId);
  NIMBLE_CHECK_NOT_NULL(keyStreamIndex);

  // Note: For key-based lookup, we assume the position index stores chunk
  // information for the key stream at chunkIndex. The specific stream ID
  // should be determined by the caller based on their schema.
  const auto* chunkOffsets = keyStreamIndex->chunk_offsets();
  NIMBLE_CHECK_NOT_NULL(chunkOffsets);
  NIMBLE_CHECK_LT(chunkIndex, chunkOffsets->size(), "Chunk index out of range");
  const auto* chunkRows = keyStreamIndex->chunk_rows();
  NIMBLE_CHECK_EQ(chunkOffsets->size(), chunkRows->size());
  return ChunkLocation{
      chunkOffsets->Get(chunkIndex),
      chunkIndex == 0 ? 0 : chunkRows->Get(chunkIndex - 1)};
}

std::optional<ChunkLocation> StripeIndexGroup::lookupChunk(
    uint32_t stripe,
    uint32_t streamId,
    uint32_t rowId) const {
  const auto* root =
      asFlatBuffersRoot<serialization::StripeIndexGroup>(metadata_->content());

  // Get position index for the stripe
  const auto* positionIndices = root->position_index();
  NIMBLE_CHECK_NOT_NULL(positionIndices);
  NIMBLE_CHECK_LT(
      stripe,
      positionIndices->size(),
      "Stripe index out of range for position index");

  const auto* stripePositionIndex = positionIndices->Get(stripe);
  NIMBLE_CHECK_NOT_NULL(stripePositionIndex);
  const auto* streamsIndex = stripePositionIndex->streams();
  NIMBLE_CHECK_NOT_NULL(streamsIndex);
  NIMBLE_CHECK_LT(streamId, streamsIndex->size());
  const auto* streamIndex = streamsIndex->Get(streamId);
  NIMBLE_CHECK_NOT_NULL(streamIndex);

  const auto* chunkRows = streamIndex->chunk_rows();
  NIMBLE_CHECK_NOT_NULL(chunkRows);

  // Binary search for the chunk containing the row
  auto it = std::lower_bound(chunkRows->begin(), chunkRows->end(), rowId);
  if (it == chunkRows->end()) {
    return std::nullopt;
  }

  const uint32_t chunkIndex = it - chunkRows->begin();
  const auto* chunkOffsets = streamIndex->chunk_offsets();
  NIMBLE_CHECK_NOT_NULL(chunkOffsets);
  NIMBLE_CHECK_EQ(chunkRows->size(), chunkOffsets->size());
  return ChunkLocation{
      chunkOffsets->Get(chunkIndex),
      chunkIndex == 0 ? 0 : chunkRows->Get(chunkIndex - 1)};
}

std::shared_ptr<StreamIndex> StripeIndexGroup::createStreamIndex(
    uint32_t stripe,
    uint32_t streamId) const {
  return StreamIndex::create(this, stripe, streamId);
}

StreamIndex::StreamIndex(
    const StripeIndexGroup* stripeIndexGroup,
    uint32_t stripe,
    uint32_t streamId)
    : stripeIndexGroup_(stripeIndexGroup),
      stripe_(stripe),
      streamId_(streamId) {}

std::shared_ptr<StreamIndex> StreamIndex::create(
    const StripeIndexGroup* stripeIndexGroup,
    uint32_t stripe,
    uint32_t streamId) {
  return std::make_shared<StreamIndex>(stripeIndexGroup, stripe, streamId);
}

std::optional<ChunkLocation> StreamIndex::lookupChunk(uint32_t rowId) const {
  return stripeIndexGroup_->lookupChunk(stripe_, streamId_, rowId);
}

} // namespace facebook::nimble
