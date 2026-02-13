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

#include "dwio/nimble/index/tests/IndexTestUtils.h"

#include "dwio/nimble/tablet/IndexGenerated.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "velox/common/file/FileSystems.h"

namespace facebook::nimble::index::test {

std::vector<velox::RowVectorPtr> generateData(
    const velox::RowTypePtr& rowType,
    size_t numVectors,
    velox::vector_size_t numRowsPerVector,
    const KeyColumnGeneratorMap& keyColumnGenerators,
    velox::memory::MemoryPool* pool,
    double nullRatio) {
  velox::VectorFuzzer::Options options;
  options.vectorSize = numRowsPerVector;
  options.nullRatio = nullRatio;
  velox::VectorFuzzer fuzzer(options, pool);

  std::vector<velox::RowVectorPtr> result;
  result.reserve(numVectors);
  for (size_t vecIdx = 0; vecIdx < numVectors; ++vecIdx) {
    std::vector<velox::VectorPtr> children;
    children.reserve(rowType->size());
    for (auto i = 0; i < rowType->size(); ++i) {
      const auto& name = rowType->nameOf(i);
      auto it = keyColumnGenerators.find(name);
      if (it != keyColumnGenerators.end()) {
        children.push_back(it->second(vecIdx, numRowsPerVector));
      } else {
        children.push_back(fuzzer.fuzz(rowType->childAt(i)));
      }
    }
    result.push_back(
        std::make_shared<velox::RowVector>(
            pool, rowType, nullptr, numRowsPerVector, std::move(children)));
  }
  return result;
}

void writeFile(
    const std::string& filePath,
    const std::vector<velox::RowVectorPtr>& data,
    IndexConfig indexConfig,
    velox::memory::MemoryPool& pool,
    std::function<std::unique_ptr<FlushPolicy>()> flushPolicyFactory) {
  NIMBLE_CHECK(!data.empty(), "Data must not be empty");

  VeloxWriterOptions options;
  options.enableChunking = true;
  options.indexConfig = std::move(indexConfig);
  if (flushPolicyFactory) {
    options.flushPolicyFactory = std::move(flushPolicyFactory);
  }

  auto fs = velox::filesystems::getFileSystem(filePath, {});
  auto writeFile = fs->openFileForWrite(
      filePath,
      {.shouldCreateParentDirectories = true,
       .shouldThrowOnFileAlreadyExists = false});

  auto rowType = velox::asRowType(data[0]->type());
  VeloxWriter writer(rowType, std::move(writeFile), pool, std::move(options));
  for (const auto& vector : data) {
    writer.write(vector);
  }
  writer.close();
}

KeyStream createKeyStream(
    Buffer& buffer,
    const std::vector<KeyChunkSpec>& chunkSpecs) {
  KeyStream keyStream;

  for (const auto& spec : chunkSpecs) {
    auto contentSize = spec.firstKey.size() + spec.lastKey.size();
    auto pos = buffer.reserve(contentSize);
    std::memcpy(pos, spec.firstKey.data(), spec.firstKey.size());
    std::memcpy(
        pos + spec.firstKey.size(), spec.lastKey.data(), spec.lastKey.size());

    KeyChunk chunk;
    chunk.rowCount = spec.rowCount;
    chunk.content = {std::string_view(pos, contentSize)};

    auto firstKeyPos = buffer.reserve(spec.firstKey.size());
    std::memcpy(firstKeyPos, spec.firstKey.data(), spec.firstKey.size());
    auto lastKeyPos = buffer.reserve(spec.lastKey.size());
    std::memcpy(lastKeyPos, spec.lastKey.data(), spec.lastKey.size());

    chunk.firstKey = std::string_view(firstKeyPos, spec.firstKey.size());
    chunk.lastKey = std::string_view(lastKeyPos, spec.lastKey.size());
    keyStream.chunks.push_back(std::move(chunk));
  }

  return keyStream;
}

std::vector<Chunk> createChunks(
    Buffer& buffer,
    const std::vector<ChunkSpec>& chunkSpecs) {
  std::vector<Chunk> chunks;
  for (const auto& spec : chunkSpecs) {
    auto pos = buffer.reserve(spec.size);
    std::memset(pos, 'X', spec.size);
    chunks.push_back(
        {.rowCount = spec.rowCount, .content = {{pos, spec.size}}});
  }
  return chunks;
}

Stream createStream(Buffer& buffer, const StreamSpec& spec) {
  return Stream{
      .offset = spec.offset, .chunks = createChunks(buffer, spec.chunks)};
}

KeyStreamStats StripeIndexGroupTestHelper::keyStreamStats() const {
  KeyStreamStats stats;

  const auto* root = flatbuffers::GetRoot<serialization::StripeIndexGroup>(
      indexGroup_->metadata_->content().data());

  const auto* valueIndex = root->value_index();
  if (valueIndex == nullptr) {
    return stats;
  }

  // Copy key stream offsets per stripe.
  if (const auto* offsets = valueIndex->key_stream_offsets()) {
    stats.offsets.reserve(offsets->size());
    for (const auto offset : *offsets) {
      stats.offsets.push_back(offset);
    }
  }

  // Copy key stream sizes per stripe.
  if (const auto* sizes = valueIndex->key_stream_sizes()) {
    stats.sizes.reserve(sizes->size());
    for (const auto size : *sizes) {
      stats.sizes.push_back(size);
    }
  }

  // Copy accumulated chunk counts per stripe.
  if (const auto* chunkCounts = valueIndex->key_stream_chunk_counts()) {
    stats.chunkCounts.reserve(chunkCounts->size());
    for (const auto count : *chunkCounts) {
      stats.chunkCounts.push_back(count);
    }
  }

  // Copy accumulated row counts per chunk.
  if (const auto* chunkRows = valueIndex->key_stream_chunk_rows()) {
    stats.chunkRows.reserve(chunkRows->size());
    for (const auto row : *chunkRows) {
      stats.chunkRows.push_back(row);
    }
  }

  // Copy chunk offsets.
  if (const auto* chunkOffsets = valueIndex->key_stream_chunk_offsets()) {
    stats.chunkOffsets.reserve(chunkOffsets->size());
    for (const auto offset : *chunkOffsets) {
      stats.chunkOffsets.push_back(offset);
    }
  }

  // Copy chunk keys.
  if (const auto* keys = valueIndex->key_stream_chunk_keys()) {
    stats.chunkKeys.reserve(keys->size());
    for (const auto* key : *keys) {
      stats.chunkKeys.push_back(key->str());
    }
  }

  return stats;
}

StreamStats StripeIndexGroupTestHelper::streamStats(uint32_t streamId) const {
  StreamStats stats;

  const auto* root = flatbuffers::GetRoot<serialization::StripeIndexGroup>(
      indexGroup_->metadata_->content().data());

  const auto* positionIndex = root->position_index();
  if (positionIndex == nullptr) {
    return stats;
  }

  const auto* streamChunkCounts = positionIndex->stream_chunk_counts();
  NIMBLE_CHECK_NOT_NULL(streamChunkCounts);
  const auto* chunkRows = positionIndex->stream_chunk_rows();
  NIMBLE_CHECK_NOT_NULL(chunkRows);
  const auto* chunkOffsets = positionIndex->stream_chunk_offsets();
  NIMBLE_CHECK_NOT_NULL(chunkOffsets);

  const uint32_t stripeCount = indexGroup_->stripeCount_;
  const uint32_t streamCount = indexGroup_->streamCount_;

  // Extract chunk data for this stream across all stripes.
  // streamChunkCounts is stored in (stripe, stream) order as prefix sums.
  // We need to:
  // 1. Calculate accumulated chunk count for this stream only
  // 2. Extract chunk rows and offsets for this stream's chunks
  uint32_t accumulatedChunkCountForStream = 0;
  for (uint32_t stripeOffset = 0; stripeOffset < stripeCount; ++stripeOffset) {
    const uint32_t idx = stripeOffset * streamCount + streamId;
    const uint32_t globalAccumulatedCount = streamChunkCounts->Get(idx);
    const uint32_t prevGlobalAccumulatedCount =
        idx == 0 ? 0 : streamChunkCounts->Get(idx - 1);
    const uint32_t chunksInThisStripe =
        globalAccumulatedCount - prevGlobalAccumulatedCount;
    // Accumulate chunk count for this stream
    accumulatedChunkCountForStream += chunksInThisStripe;
    stats.chunkCounts.push_back(accumulatedChunkCountForStream);

    // Extract chunk rows and offsets for this stripe's chunks
    const uint32_t startChunkIndex = prevGlobalAccumulatedCount;
    for (uint32_t i = 0; i < chunksInThisStripe; ++i) {
      const uint32_t chunkIdx = startChunkIndex + i;
      stats.chunkRows.push_back(chunkRows->Get(chunkIdx));
      stats.chunkOffsets.push_back(chunkOffsets->Get(chunkIdx));
    }
  }

  return stats;
}

uint32_t StripeIndexGroupTestHelper::keyChunkLength(
    uint32_t stripeIndex,
    uint32_t chunkStreamOffset,
    uint32_t keyStreamLength) const {
  const auto* root = flatbuffers::GetRoot<serialization::StripeIndexGroup>(
      indexGroup_->metadata_->content().data());

  const auto* valueIndex = root->value_index();
  NIMBLE_CHECK_NOT_NULL(valueIndex);

  const auto* chunkCounts = valueIndex->key_stream_chunk_counts();
  NIMBLE_CHECK_NOT_NULL(chunkCounts);
  const auto* chunkOffsets = valueIndex->key_stream_chunk_offsets();
  NIMBLE_CHECK_NOT_NULL(chunkOffsets);

  const uint32_t stripeOffset = indexGroup_->stripeOffset(stripeIndex);

  // Determine the chunk range for this stripe
  const uint32_t startChunkIndex =
      stripeOffset == 0 ? 0 : chunkCounts->Get(stripeOffset - 1);
  const uint32_t endChunkIndex = chunkCounts->Get(stripeOffset);

  // Find the chunk with the matching offset
  for (uint32_t i = startChunkIndex; i < endChunkIndex; ++i) {
    if (chunkOffsets->Get(i) == chunkStreamOffset) {
      // Found the chunk, calculate its length
      if (i + 1 < endChunkIndex) {
        // Use next chunk's offset
        return chunkOffsets->Get(i + 1) - chunkStreamOffset;
      } else {
        // Last chunk in stripe, use stream length
        return keyStreamLength - chunkStreamOffset;
      }
    }
  }

  NIMBLE_UNREACHABLE(
      fmt::format(
          "Chunk with offset {} not found in stripe {}",
          chunkStreamOffset,
          stripeIndex));
}

} // namespace facebook::nimble::index::test
