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

#include "dwio/nimble/tablet/ChunkIndexWriter.h"

#include <algorithm>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/tablet/Chunk.h"
#include "dwio/nimble/tablet/ChunkIndexGenerated.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/TabletWriter.h"
#include "flatbuffers/flatbuffers.h"
#include "folly/ScopeGuard.h"

namespace facebook::nimble {

namespace {

std::string_view asView(const flatbuffers::FlatBufferBuilder& builder) {
  return {
      reinterpret_cast<const char*>(builder.GetBufferPointer()),
      builder.GetSize()};
}

flatbuffers::Offset<serialization::StreamChunkStats> serializeStreamStats(
    flatbuffers::FlatBufferBuilder& builder,
    const std::vector<ChunkStats>& chunks) {
  if (chunks.empty()) {
    return serialization::CreateStreamChunkStats(builder);
  }
  const auto n = chunks.size();

  std::vector<uint8_t> hasNulls(n);
  std::vector<int64_t> nullCounts(n);
  for (size_t i = 0; i < n; ++i) {
    hasNulls[i] = chunks[i].hasNulls ? 1 : 0;
    nullCounts[i] = static_cast<int64_t>(chunks[i].nullCount);
  }

  // Determine value_size and validate that all chunks have consistent sizes.
  // If any chunk has a different size or is missing min/max while others have
  // it, drop min/max for the entire stream to avoid false filter bounds.
  uint32_t valueSize = 0;
  bool seenEmpty = false;
  for (const auto& c : chunks) {
    if (c.minValue.empty()) {
      seenEmpty = true;
    } else {
      if (seenEmpty) {
        // Earlier chunk(s) had empty min/max — inconsistent.
        valueSize = 0;
        break;
      }
      if (valueSize == 0) {
        valueSize = static_cast<uint32_t>(c.minValue.size());
      }
      if (c.minValue.size() != valueSize || c.maxValue.size() != valueSize) {
        valueSize = 0;
        break;
      }
    }
  }
  if (seenEmpty) {
    valueSize = 0;
  }

  // Concatenate min/max bytes.
  flatbuffers::Offset<flatbuffers::Vector<uint8_t>> minsVec = 0;
  flatbuffers::Offset<flatbuffers::Vector<uint8_t>> maxsVec = 0;
  if (valueSize > 0) {
    std::vector<uint8_t> allMins, allMaxs;
    allMins.reserve(n * valueSize);
    allMaxs.reserve(n * valueSize);
    for (const auto& c : chunks) {
      allMins.insert(allMins.end(), c.minValue.begin(), c.minValue.end());
      allMaxs.insert(allMaxs.end(), c.maxValue.begin(), c.maxValue.end());
    }
    minsVec = builder.CreateVector(allMins);
    maxsVec = builder.CreateVector(allMaxs);
  }

  return serialization::CreateStreamChunkStats(
      builder,
      /*has_nulls=*/builder.CreateVector(hasNulls),
      /*min_values=*/minsVec,
      /*max_values=*/maxsVec,
      /*value_size=*/valueSize,
      /*null_counts=*/builder.CreateVector(nullCounts));
}

} // namespace

ChunkIndexWriter::ChunkIndexWriter(
    velox::memory::MemoryPool& pool,
    float minAvgChunksPerStream,
    bool enableChunkStats)
    : pool_{&pool},
      minAvgChunksPerStream_{minAvgChunksPerStream},
      enableChunkStats_{enableChunkStats} {}

void ChunkIndexWriter::newStripe(size_t streamCount) {
  NIMBLE_CHECK(!finalized_, "ChunkIndexWriter has been finalized");
  if (groupIndex_ == nullptr) {
    groupIndex_ = std::make_unique<GroupIndex>();
  }
  auto& stripeIndex = groupIndex_->stripes.emplace_back();
  stripeIndex.streams.resize(streamCount);
}

void ChunkIndexWriter::addStream(
    uint32_t streamIndex,
    const std::vector<Chunk>& chunks) {
  NIMBLE_CHECK(!finalized_, "ChunkIndexWriter has been finalized");
  NIMBLE_CHECK_NOT_NULL(groupIndex_);
  NIMBLE_CHECK(!groupIndex_->empty());
  NIMBLE_CHECK_LT(streamIndex, groupIndex_->stripes.back().streams.size());

  auto& stream = groupIndex_->stripes.back().streams[streamIndex];
  uint32_t accumulatedRows{0};
  uint32_t accumulatedOffset{0};
  for (const auto& chunk : chunks) {
    accumulatedRows += chunk.rowCount;
    stream.chunkRows.emplace_back(accumulatedRows);
    stream.chunkOffsets.emplace_back(accumulatedOffset);
    accumulatedOffset += chunk.contentSize();
    ++stream.chunkCount;
    if (enableChunkStats_) {
      if (chunk.stats.has_value()) {
        stream.chunkStats.emplace_back(*chunk.stats);
      } else {
        stream.chunkStats.emplace_back();
      }
    }
  }
}

void ChunkIndexWriter::writeGroup(
    size_t streamCount,
    size_t stripeCount,
    const CreateMetadataSectionFn& createMetadataSection) {
  NIMBLE_CHECK(!finalized_, "ChunkIndexWriter has been finalized");
  NIMBLE_CHECK_NOT_NULL(groupIndex_);
  NIMBLE_CHECK_EQ(stripeCount, groupIndex_->stripes.size());
  NIMBLE_CHECK_GT(stripeCount, 0);
  NIMBLE_CHECK_GT(streamCount, 0);

  SCOPE_EXIT {
    groupIndex_.reset();
  };

  // Compute total number of chunks across all streams and stripes.
  uint32_t totalNumChunks{0};
  for (const auto& stripe : groupIndex_->stripes) {
    for (size_t streamId = 0; streamId < streamCount; ++streamId) {
      if (streamId < stripe.streams.size()) {
        totalNumChunks += stripe.streams[streamId].chunkCount;
      }
    }
  }

  // Check threshold: skip chunk index for this group if average chunks
  // per stream is below threshold.
  if (minAvgChunksPerStream_ > 0) {
    const float avgChunks =
        static_cast<float>(totalNumChunks) / static_cast<float>(streamCount);
    if (avgChunks < minAvgChunksPerStream_) {
      chunkIndexSections_.emplace_back(0, 0, CompressionType::Uncompressed);
      return;
    }
  }

  // Flatten chunk position data for all streams.
  std::vector<uint32_t> flattenedStreamChunkCounts;
  flattenedStreamChunkCounts.reserve(stripeCount * streamCount);

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

  std::vector<uint32_t> flattenedChunkRows;
  std::vector<uint32_t> flattenedChunkOffsets;
  flattenedChunkRows.reserve(accumulatedChunkCount);
  flattenedChunkOffsets.reserve(accumulatedChunkCount);

  for (const auto& stripe : groupIndex_->stripes) {
    for (size_t streamId = 0; streamId < streamCount; ++streamId) {
      if (streamId < stripe.streams.size()) {
        const auto& stream = stripe.streams[streamId];
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
  }

  flatbuffers::FlatBufferBuilder builder(kInitialFooterSize);

  flatbuffers::Offset<
      flatbuffers::Vector<flatbuffers::Offset<serialization::StreamChunkStats>>>
      streamStatsVec = 0;
  if (enableChunkStats_) {
    std::vector<flatbuffers::Offset<serialization::StreamChunkStats>>
        streamStats;
    streamStats.reserve(stripeCount * streamCount);
    for (const auto& stripe : groupIndex_->stripes) {
      for (size_t sid = 0; sid < streamCount; ++sid) {
        if (sid < stripe.streams.size()) {
          streamStats.push_back(
              serializeStreamStats(builder, stripe.streams[sid].chunkStats));
        } else {
          streamStats.push_back(serializeStreamStats(builder, {}));
        }
      }
    }
    streamStatsVec = builder.CreateVector(streamStats);
  }

  auto chunkIndex = serialization::CreateStripeChunkIndex(
      builder,
      static_cast<uint32_t>(streamCount),
      builder.CreateVector(flattenedStreamChunkCounts),
      builder.CreateVector(flattenedChunkRows),
      builder.CreateVector(flattenedChunkOffsets),
      streamStatsVec);
  builder.Finish(chunkIndex);

  chunkIndexSections_.push_back(createMetadataSection(asView(builder)));
}

void ChunkIndexWriter::writeRoot(
    const WriteOptionalSectionFn& writeOptionalSection) {
  NIMBLE_CHECK(!finalized_, "ChunkIndexWriter has been finalized");
  SCOPE_EXIT {
    finalized_ = true;
  };

  // Skip writing the chunk_index section if all groups were skipped or no
  // groups were written.
  const bool hasNonEmptyGroup = std::any_of(
      chunkIndexSections_.begin(),
      chunkIndexSections_.end(),
      [](const auto& section) { return section.size() > 0; });
  if (!hasNonEmptyGroup) {
    return;
  }

  flatbuffers::FlatBufferBuilder builder(kInitialFooterSize);

  auto stripeIndexesVector =
      builder.CreateVector<flatbuffers::Offset<serialization::MetadataSection>>(
          chunkIndexSections_.size(), [&builder, this](size_t i) {
            return serialization::CreateMetadataSection(
                builder,
                chunkIndexSections_[i].offset(),
                chunkIndexSections_[i].size(),
                static_cast<serialization::CompressionType>(
                    chunkIndexSections_[i].compressionType()),
                chunkIndexSections_[i].uncompressedSize().value_or(
                    chunkIndexSections_[i].size()));
          });

  builder.Finish(serialization::CreateChunkIndex(builder, stripeIndexesVector));
  writeOptionalSection(std::string(kChunkIndexSection), asView(builder));
}

} // namespace facebook::nimble
