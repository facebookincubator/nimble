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

#include "dwio/nimble/tablet/ChunkIndexGenerated.h"
#include "dwio/nimble/tablet/ClusterIndexGenerated.h"
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

KeyStreamStats ClusterIndexGroupTestHelper::keyStreamStats() const {
  KeyStreamStats stats;

  const auto* root = flatbuffers::GetRoot<serialization::StripeClusterIndex>(
      indexGroup_->metadata_->content().data());

  if (const auto* keyStream = root->key_stream()) {
    if (const auto* offsets = keyStream->offsets()) {
      stats.offsets.reserve(offsets->size());
      for (const auto offset : *offsets) {
        stats.offsets.push_back(offset);
      }
    }

    if (const auto* sizes = keyStream->sizes()) {
      stats.sizes.reserve(sizes->size());
      for (const auto size : *sizes) {
        stats.sizes.push_back(size);
      }
    }
  }

  if (const auto* chunkIndex = root->chunk_index()) {
    if (const auto* chunkCounts = chunkIndex->chunk_counts()) {
      stats.chunkCounts.reserve(chunkCounts->size());
      for (const auto count : *chunkCounts) {
        stats.chunkCounts.push_back(count);
      }
    }

    if (const auto* chunkRows = chunkIndex->chunk_rows()) {
      stats.chunkRows.reserve(chunkRows->size());
      for (const auto row : *chunkRows) {
        stats.chunkRows.push_back(row);
      }
    }

    if (const auto* chunkOffsets = chunkIndex->chunk_offsets()) {
      stats.chunkOffsets.reserve(chunkOffsets->size());
      for (const auto offset : *chunkOffsets) {
        stats.chunkOffsets.push_back(offset);
      }
    }

    if (const auto* keys = chunkIndex->chunk_keys()) {
      stats.chunkKeys.reserve(keys->size());
      for (const auto* key : *keys) {
        stats.chunkKeys.push_back(key->str());
      }
    }
  }

  return stats;
}

uint32_t ClusterIndexGroupTestHelper::keyChunkLength(
    uint32_t stripeIndex,
    uint32_t chunkStreamOffset,
    uint32_t keyStreamLength) const {
  const auto* root = flatbuffers::GetRoot<serialization::StripeClusterIndex>(
      indexGroup_->metadata_->content().data());

  const auto* chunkIdx = root->chunk_index();
  if (chunkIdx == nullptr) {
    // Single chunk per stripe — the entire key stream is one chunk.
    NIMBLE_CHECK_EQ(chunkStreamOffset, 0);
    return keyStreamLength;
  }

  const auto* chunkCounts = chunkIdx->chunk_counts();
  NIMBLE_CHECK_NOT_NULL(chunkCounts);
  const auto* chunkOffsets = chunkIdx->chunk_offsets();
  NIMBLE_CHECK_NOT_NULL(chunkOffsets);

  const uint32_t stripeOffset = indexGroup_->stripeOffset(stripeIndex);

  const uint32_t startChunkIndex =
      stripeOffset == 0 ? 0 : chunkCounts->Get(stripeOffset - 1);
  const uint32_t endChunkIndex = chunkCounts->Get(stripeOffset);

  for (uint32_t i = startChunkIndex; i < endChunkIndex; ++i) {
    if (chunkOffsets->Get(i) == chunkStreamOffset) {
      if (i + 1 < endChunkIndex) {
        return chunkOffsets->Get(i + 1) - chunkStreamOffset;
      } else {
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

StreamStats ChunkIndexTestHelper::streamStats(uint32_t streamId) const {
  StreamStats stats;

  const auto* root = flatbuffers::GetRoot<serialization::StripeChunkIndex>(
      chunkIndex_->metadata_->content().data());

  const auto* streamChunkCounts = root->stream_chunk_counts();
  if (streamChunkCounts == nullptr) {
    return stats;
  }

  const uint32_t streamCount = chunkIndex_->streamCount_;
  if (streamId >= streamCount) {
    return stats;
  }

  const auto* chunkRows = root->stream_chunk_rows();
  NIMBLE_CHECK_NOT_NULL(chunkRows);
  const auto* chunkOffsets = root->stream_chunk_offsets();
  NIMBLE_CHECK_NOT_NULL(chunkOffsets);

  const uint32_t stripeCount = chunkIndex_->stripeCount_;

  uint32_t accumulatedChunkCountForStream = 0;
  for (uint32_t stripeOffset = 0; stripeOffset < stripeCount; ++stripeOffset) {
    const uint32_t idx = stripeOffset * streamCount + streamId;
    const uint32_t globalAccumulatedCount = streamChunkCounts->Get(idx);
    const uint32_t prevGlobalAccumulatedCount =
        idx == 0 ? 0 : streamChunkCounts->Get(idx - 1);
    const uint32_t chunksInThisStripe =
        globalAccumulatedCount - prevGlobalAccumulatedCount;
    accumulatedChunkCountForStream += chunksInThisStripe;
    stats.chunkCounts.push_back(accumulatedChunkCountForStream);

    const uint32_t startChunkIndex = prevGlobalAccumulatedCount;
    for (uint32_t i = 0; i < chunksInThisStripe; ++i) {
      const uint32_t chunkIndex = startChunkIndex + i;
      stats.chunkRows.push_back(chunkRows->Get(chunkIndex));
      stats.chunkOffsets.push_back(chunkOffsets->Get(chunkIndex));
    }
  }

  return stats;
}

} // namespace facebook::nimble::index::test
