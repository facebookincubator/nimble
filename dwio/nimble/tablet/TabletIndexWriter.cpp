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

#include "dwio/nimble/tablet/TabletIndexWriter.h"

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/IndexGenerated.h"
#include "dwio/nimble/tablet/TabletWriter.h"
#include "folly/String.h"
#include "folly/json/json.h"

namespace facebook::nimble {

namespace {

std::string_view asView(const flatbuffers::FlatBufferBuilder& builder) {
  return {
      reinterpret_cast<const char*>(builder.GetBufferPointer()),
      builder.GetSize()};
}

} // namespace

std::unique_ptr<TabletIndexWriter> TabletIndexWriter::create(
    const std::optional<TabletIndexConfig>& config,
    velox::memory::MemoryPool& pool) {
  if (!config.has_value()) {
    return nullptr;
  }
  NIMBLE_USER_CHECK(
      !config->columns.empty(),
      "Index columns must not be empty in TabletIndexConfig");
  NIMBLE_USER_CHECK_EQ(
      config->columns.size(),
      config->sortOrders.size(),
      "Index columns and sort orders must have the same size in TabletIndexConfig");
  return std::unique_ptr<TabletIndexWriter>(
      new TabletIndexWriter(config.value(), pool));
}

TabletIndexWriter::TabletIndexWriter(
    const TabletIndexConfig& config,
    velox::memory::MemoryPool& pool)
    : pool_{&pool}, config_{config} {}

void TabletIndexWriter::ensureStripeWrite(size_t streamCount) {
  checkNotFinalized();
  if (rootIndex_ == nullptr) {
    rootIndex_ = std::make_unique<RootIndex>();
    rootIndex_->encodingBuffer = std::make_unique<Buffer>(*pool_);
  }
  if (groupIndex_ == nullptr) {
    groupIndex_ = std::make_unique<GroupIndex>();
    groupIndex_->encodingBuffer = std::make_unique<Buffer>(*pool_);
  }
  auto& stripeIndex = groupIndex_->stripes.emplace_back();
  stripeIndex.streams.resize(streamCount);
}

void TabletIndexWriter::addStripeKey(const KeyStream& keyStream) {
  checkNotFinalized();
  const auto& chunks = keyStream.chunks;
  NIMBLE_CHECK(!chunks.empty());

  const auto firstStripe = rootIndex_->stripeKeys.empty();
  // For the first stripe, insert both the first key (min key of the file)
  // and the last key. For subsequent stripes, insert only the last key.
  if (firstStripe) {
    rootIndex_->stripeKeys.emplace_back(
        rootIndex_->encodingBuffer->writeString(chunks.front().firstKey));
  }
  const auto& newKey = chunks.back().lastKey;

  // Verify key ordering based on configuration:
  // - enforceKeyOrder: keys must be in ascending order (duplicates allowed)
  // - noDuplicateKey: when combined with enforceKeyOrder, keys must be strictly
  //   ascending (no duplicates)
  //
  // For the first stripe, we compare firstKey (min) vs lastKey (max) within
  // the same stripe. If noDuplicateKey is set, we only allow equality when
  // the stripe has exactly one row (single-row stripe).
  //
  // For subsequent stripes, we compare across stripe boundaries: the current
  // stripe's lastKey must be > previous stripe's lastKey when noDuplicateKey
  // is set.
  if (config_.enforceKeyOrder) {
    NIMBLE_CHECK(!rootIndex_->stripeKeys.empty());
    const auto& prevKey = rootIndex_->stripeKeys.back();
    if (firstStripe) {
      // First stripe: comparing min vs max within same stripe
      // Calculate total rows to determine if single-row stripe
      uint32_t stripeRowCount = 0;
      for (const auto& chunk : chunks) {
        stripeRowCount += chunk.rowCount;
      }
      if (config_.noDuplicateKey && stripeRowCount > 1) {
        // Multiple rows with noDuplicateKey: require strictly ascending
        NIMBLE_USER_CHECK_GT(
            newKey,
            prevKey,
            "Stripe keys must be in strictly ascending order (duplicates are not allowed). "
            "lastKey: {}, firstKey: {}",
            folly::hexlify(newKey),
            folly::hexlify(prevKey));
      } else {
        // Single row or enforceKeyOrder only: allow equality
        NIMBLE_USER_CHECK_GE(
            newKey,
            prevKey,
            "Stripe keys must be in ascending order (firstKey <= lastKey). "
            "lastKey: {}, firstKey: {}",
            folly::hexlify(newKey),
            folly::hexlify(prevKey));
      }
    } else if (config_.noDuplicateKey) {
      // Subsequent stripes with noDuplicateKey: strictly ascending across
      // stripes
      NIMBLE_USER_CHECK_GT(
          newKey,
          prevKey,
          "Stripe keys must be in strictly ascending order (duplicates are not allowed). "
          "newKey: {}, previousKey: {}",
          folly::hexlify(newKey),
          folly::hexlify(prevKey));
    } else {
      // Subsequent stripes with enforceKeyOrder only: ascending (duplicates OK)
      NIMBLE_USER_CHECK_GE(
          newKey,
          prevKey,
          "Stripe keys must be in ascending order. "
          "newKey: {}, previousKey: {}",
          folly::hexlify(newKey),
          folly::hexlify(prevKey));
    }
  }
  rootIndex_->stripeKeys.emplace_back(
      rootIndex_->encodingBuffer->writeString(newKey));
}

template <typename ChunkT>
/* static */ void TabletIndexWriter::updateStreamIndex(
    const std::vector<ChunkT>& chunks,
    StreamIndex& index) {
  uint32_t accumulatedRows{0};
  uint32_t accumulatedOffset{0};
  for (const auto& chunk : chunks) {
    accumulatedRows += chunk.rowCount;
    index.chunkRows.emplace_back(accumulatedRows);
    index.chunkOffsets.emplace_back(accumulatedOffset);
    accumulatedOffset += chunk.contentSize();
    ++index.chunkCount;
  }
}

// Explicit template instantiations
template void TabletIndexWriter::updateStreamIndex(
    const std::vector<Chunk>& chunks,
    StreamIndex& index);
template void TabletIndexWriter::updateStreamIndex(
    const std::vector<KeyChunk>& chunks,
    StreamIndex& index);

void TabletIndexWriter::addStreamIndex(
    uint32_t streamIndex,
    const std::vector<Chunk>& chunks) {
  checkNotFinalized();
  NIMBLE_CHECK_NOT_NULL(groupIndex_);
  NIMBLE_CHECK(!groupIndex_->empty());
  NIMBLE_CHECK_LT(streamIndex, groupIndex_->stripes.back().streams.size());
  updateStreamIndex(chunks, groupIndex_->stripes.back().streams[streamIndex]);
}

uint64_t TabletIndexWriter::writeKeyStream(
    uint64_t keyStreamOffset,
    const std::vector<KeyChunk>& keyChunks,
    const WriteWithChecksumFn& writeWithChecksum) {
  checkNotFinalized();
  for (const auto& chunk : keyChunks) {
    for (const auto& content : chunk.content) {
      writeWithChecksum(content);
    }
  }

  // Calculate total bytes written
  uint64_t bytesWritten{0};
  for (const auto& chunk : keyChunks) {
    bytesWritten += chunk.contentSize();
  }

  // Update key stream chunk index for the current stripe.
  NIMBLE_CHECK(!groupIndex_->empty());
  auto& keyStream = groupIndex_->stripes.back().keyStream;
  updateStreamIndex(keyChunks, keyStream);
  for (const auto& chunk : keyChunks) {
    keyStream.chunkKeys.emplace_back(
        groupIndex_->encodingBuffer->writeString(chunk.lastKey));
  }
  NIMBLE_CHECK_EQ(keyStream.chunkKeys.size(), keyStream.chunkRows.size());

  keyStream.streamOffset = keyStreamOffset;
  keyStream.streamSize = bytesWritten;

  return bytesWritten;
}

void TabletIndexWriter::writeIndexGroup(
    size_t streamCount,
    size_t stripeCount,
    const CreateMetadataSectionFn& createMetadataSection) {
  checkNotFinalized();
  NIMBLE_CHECK_NOT_NULL(groupIndex_);
  SCOPE_EXIT {
    groupIndex_.reset();
  };

  NIMBLE_CHECK_EQ(stripeCount, groupIndex_->stripes.size());
  NIMBLE_CHECK_GT(stripeCount, 0);
  NIMBLE_CHECK_GT(streamCount, 0);

  flatbuffers::FlatBufferBuilder indexBuilder(kInitialFooterSize);

  // Flatten key stream data from all stripes
  std::vector<uint32_t> keyStreamOffsets;
  std::vector<uint32_t> keyStreamSizes;
  std::vector<uint32_t> keyStreamChunkCounts;
  std::vector<uint32_t> keyStreamChunkRows;
  std::vector<uint32_t> keyStreamChunkOffsets;
  std::vector<std::string_view> keyStreamKeys;

  keyStreamOffsets.reserve(stripeCount);
  keyStreamSizes.reserve(stripeCount);
  keyStreamChunkCounts.reserve(stripeCount);

  uint32_t accumulatedKeyChunkCount{0};
  for (const auto& stripe : groupIndex_->stripes) {
    keyStreamOffsets.push_back(stripe.keyStream.streamOffset);
    keyStreamSizes.push_back(stripe.keyStream.streamSize);
    accumulatedKeyChunkCount += stripe.keyStream.chunkCount;
    keyStreamChunkCounts.push_back(accumulatedKeyChunkCount);
    NIMBLE_CHECK_EQ(
        stripe.keyStream.chunkCount, stripe.keyStream.chunkRows.size());
    keyStreamChunkRows.reserve(
        keyStreamChunkRows.size() + stripe.keyStream.chunkRows.size());
    keyStreamChunkRows.insert(
        keyStreamChunkRows.end(),
        stripe.keyStream.chunkRows.begin(),
        stripe.keyStream.chunkRows.end());
    NIMBLE_CHECK_EQ(
        stripe.keyStream.chunkCount, stripe.keyStream.chunkOffsets.size());
    keyStreamChunkOffsets.reserve(
        keyStreamChunkOffsets.size() + stripe.keyStream.chunkOffsets.size());
    keyStreamChunkOffsets.insert(
        keyStreamChunkOffsets.end(),
        stripe.keyStream.chunkOffsets.begin(),
        stripe.keyStream.chunkOffsets.end());
    NIMBLE_CHECK_EQ(
        stripe.keyStream.chunkCount, stripe.keyStream.chunkKeys.size());
    keyStreamKeys.reserve(
        keyStreamKeys.size() + stripe.keyStream.chunkKeys.size());
    keyStreamKeys.insert(
        keyStreamKeys.end(),
        stripe.keyStream.chunkKeys.begin(),
        stripe.keyStream.chunkKeys.end());
  }

  // Create StripeValueIndex with key stream data
  const size_t numKeyStreamKeys = keyStreamKeys.size();
  auto valueIndex = serialization::CreateStripeValueIndex(
      indexBuilder,
      indexBuilder.CreateVector(keyStreamOffsets),
      indexBuilder.CreateVector(keyStreamSizes),
      indexBuilder.CreateVector(keyStreamChunkCounts),
      indexBuilder.CreateVector(keyStreamChunkRows),
      indexBuilder.CreateVector(keyStreamChunkOffsets),
      indexBuilder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(
          numKeyStreamKeys,
          [&indexBuilder, streamKeys = std::move(keyStreamKeys)](size_t i) {
            return indexBuilder.CreateString(
                streamKeys[i].data(), streamKeys[i].size());
          }));

  // Create StripePositionIndex with flattened stream chunk data
  const size_t totalStreams = stripeCount * streamCount;
  std::vector<uint32_t> flattenedStreamChunkCounts;
  flattenedStreamChunkCounts.reserve(totalStreams);

  uint32_t accumulatedChunkCount{0};
  for (const auto& stripe : groupIndex_->stripes) {
    for (size_t streamId = 0; streamId < streamCount; ++streamId) {
      const uint32_t chunkCount = streamId < stripe.streams.size()
          ? stripe.streams[streamId].chunkCount
          : 0;
      accumulatedChunkCount += chunkCount;
      flattenedStreamChunkCounts.push_back(accumulatedChunkCount);
    }
  }

  // Flatten stream_chunk_rows and stream_chunk_offsets
  std::vector<uint32_t> flattenedChunkRows;
  std::vector<uint32_t> flattenedChunkOffsets;
  flattenedChunkRows.reserve(accumulatedChunkCount);
  flattenedChunkOffsets.reserve(accumulatedChunkCount);

  for (const auto& stripe : groupIndex_->stripes) {
    for (const auto& stream : stripe.streams) {
      flattenedChunkRows.insert(
          flattenedChunkRows.end(),
          stream.chunkRows.begin(),
          stream.chunkRows.end());
      flattenedChunkOffsets.insert(
          flattenedChunkOffsets.end(),
          stream.chunkOffsets.begin(),
          stream.chunkOffsets.end());
    }
  }

  auto positionIndex = serialization::CreateStripePositionIndex(
      indexBuilder,
      indexBuilder.CreateVector(flattenedStreamChunkCounts),
      indexBuilder.CreateVector(flattenedChunkRows),
      indexBuilder.CreateVector(flattenedChunkOffsets));

  // Create StripeIndexGroup with both value and position indexes
  indexBuilder.Finish(
      serialization::CreateStripeIndexGroup(
          indexBuilder, valueIndex, positionIndex));

  rootIndex_->stripeIndexGroups.push_back(
      createMetadataSection(asView(indexBuilder)));

  groupIndex_.reset();
}

void TabletIndexWriter::writeRootIndex(
    const std::vector<uint32_t>& stripeGroupIndices,
    const WriteOptionalSectionFn& writeOptionalSection) {
  checkNotFinalized();
  SCOPE_EXIT {
    rootIndex_.reset();
    finalized_ = true;
  };

  // Handle empty file case: write config only (columns, sort orders)
  // but no stripe keys or stripe index groups.
  if (rootIndex_ == nullptr) {
    writeEmptyRootIndex(writeOptionalSection);
    return;
  }

  // If rootIndex_ exists, stripeKeys must not be empty. stripeKeys contains:
  // first key of first stripe + last key for each stripe. For N stripes: size =
  // 1 + N = N + 1, which is always >= 2 for N >= 1.
  NIMBLE_CHECK(
      !rootIndex_->stripeKeys.empty(),
      "stripeKeys must not be empty when rootIndex exists");
  // Size should be numStripes + 1 (first key + one last key per stripe).
  // Since we add first key only for first stripe, and last key for all stripes:
  // size = 1 (first key) + numStripes (last keys) = numStripes + 1
  const size_t numStripes = rootIndex_->stripeKeys.size() - 1;
  NIMBLE_CHECK_GT(numStripes, 0, "Must have at least one stripe");

  flatbuffers::FlatBufferBuilder builder(kInitialFooterSize);

  // Create stripe keys vector
  auto stripeKeysVector =
      builder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(
          rootIndex_->stripeKeys.size(), [&builder, this](size_t i) {
            return builder.CreateString(
                rootIndex_->stripeKeys[i].data(),
                rootIndex_->stripeKeys[i].size());
          });

  // Create index columns vector from index config
  auto indexColumnsVector =
      builder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(
          config_.columns.size(), [&builder, this](size_t i) {
            return builder.CreateString(config_.columns[i]);
          });

  // Create sort orders vector from index config
  auto sortOrdersVector =
      builder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(
          config_.sortOrders.size(), [&builder, this](size_t i) {
            return builder.CreateString(
                folly::toJson(config_.sortOrders[i].serialize()));
          });

  // Compute accumulated stripe counts per group from stripeGroupIndices.
  std::vector<uint32_t> stripeCounts;
  if (!stripeGroupIndices.empty()) {
    const uint32_t numGroups = stripeGroupIndices.back() + 1;
    stripeCounts.resize(numGroups, 0);
    for (const auto groupIdx : stripeGroupIndices) {
      ++stripeCounts[groupIdx];
    }
    // Convert to accumulated counts (prefix sums)
    for (uint32_t i = 1; i < numGroups; ++i) {
      stripeCounts[i] += stripeCounts[i - 1];
    }
  }
  auto stripeCountsVector = builder.CreateVector(stripeCounts);

  // Create stripe index groups metadata sections
  auto stripeIndexGroupsVector =
      builder.CreateVector<flatbuffers::Offset<serialization::MetadataSection>>(
          rootIndex_->stripeIndexGroups.size(), [&builder, this](size_t i) {
            return serialization::CreateMetadataSection(
                builder,
                rootIndex_->stripeIndexGroups[i].offset(),
                rootIndex_->stripeIndexGroups[i].size(),
                static_cast<serialization::CompressionType>(
                    rootIndex_->stripeIndexGroups[i].compressionType()));
          });

  builder.Finish(
      serialization::CreateIndex(
          builder,
          stripeKeysVector,
          indexColumnsVector,
          sortOrdersVector,
          stripeCountsVector,
          stripeIndexGroupsVector));
  writeOptionalSection(std::string(kIndexSection), asView(builder));
}

void TabletIndexWriter::writeEmptyRootIndex(
    const WriteOptionalSectionFn& writeOptionalSection) {
  flatbuffers::FlatBufferBuilder builder(kInitialFooterSize);

  auto stripeKeysVector =
      builder.CreateVector<flatbuffers::Offset<flatbuffers::String>>({});

  auto indexColumnsVector =
      builder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(
          config_.columns.size(), [&builder, this](size_t i) {
            return builder.CreateString(config_.columns[i]);
          });

  auto sortOrdersVector =
      builder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(
          config_.sortOrders.size(), [&builder, this](size_t i) {
            return builder.CreateString(
                folly::toJson(config_.sortOrders[i].serialize()));
          });

  auto stripeCountsVector = builder.CreateVector<uint32_t>({});

  auto stripeIndexGroupsVector =
      builder.CreateVector<flatbuffers::Offset<serialization::MetadataSection>>(
          {});

  builder.Finish(
      serialization::CreateIndex(
          builder,
          stripeKeysVector,
          indexColumnsVector,
          sortOrdersVector,
          stripeCountsVector,
          stripeIndexGroupsVector));
  writeOptionalSection(std::string(kIndexSection), asView(builder));
}

} // namespace facebook::nimble
