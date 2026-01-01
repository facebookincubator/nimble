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
#include "dwio/nimble/velox/IndexWriter.h"

#include "dwio/nimble/index/IndexConstants.h"
#include "dwio/nimble/velox/BufferGrowthPolicy.h"
#include "dwio/nimble/velox/ChunkedStreamWriter.h"
#include "dwio/nimble/velox/StreamChunker.h"

namespace facebook::nimble::index {

namespace {
// The key stream uses a placeholder offset value (kKeyStreamId) that does
// not correspond to any actual stream position. This offset is not used for
// data retrieval but is required to conform to Nimble's stream typing
// framework, which expects all streams to have an associated offset.
// Returns a reference to a static descriptor to ensure lifetime validity for
// ContentStreamData which stores a reference to the descriptor.
const StreamDescriptorBuilder& keyStreamDescriptor() {
  static const StreamDescriptorBuilder descriptor{
      kKeyStreamId, ScalarKind::Binary};
  return descriptor;
}

// Returns a reference to a static growth policy to ensure lifetime validity for
// ContentStreamData which stores a reference to the policy.
const InputBufferGrowthPolicy& keyStreamGrowthPolicy() {
  static const auto policy =
      DefaultInputBufferGrowthPolicy::withDefaultRanges();
  return *policy;
}

} // namespace

std::unique_ptr<IndexWriter> IndexWriter::create(
    const std::optional<IndexConfig>& config,
    const velox::TypePtr& inputType,
    velox::memory::MemoryPool* pool) {
  if (!config.has_value()) {
    return nullptr;
  }
  NIMBLE_CHECK_NOT_NULL(pool, "memory pool must not be null");
  return std::unique_ptr<IndexWriter>(
      new IndexWriter(config.value(), velox::asRowType(inputType), pool));
}

IndexWriter::IndexWriter(
    const IndexConfig& config,
    const velox::RowTypePtr& inputType,
    velox::memory::MemoryPool* pool)
    : pool_{pool},
      keyEncoder_{velox::serializer::KeyEncoder::create(
          config.columns,
          inputType,
          std::vector<velox::core::SortOrder>{
              config.columns.size(),
              velox::core::SortOrder{true, true}},
          pool_)},
      keyStream_{std::make_unique<ContentStreamData<std::string_view>>(
          *pool_,
          keyStreamDescriptor(),
          keyStreamGrowthPolicy())},
      encodingLayout_{config.encodingLayout},
      enforceKeyOrder_{config.enforceKeyOrder},
      minChunkSize_{config.minChunkRawSize},
      maxChunkSize_{config.maxChunkRawSize} {
  // Key stream encoding only supports trivial encoding without nested children.
  NIMBLE_CHECK_EQ(
      encodingLayout_.encodingType(),
      EncodingType::Trivial,
      "Key stream encoding only supports Trivial encoding");
}

std::unique_ptr<EncodingSelectionPolicy<std::string_view>>
IndexWriter::createEncodingPolicy() const {
  return std::make_unique<ReplayedEncodingSelectionPolicy<std::string_view>>(
      encodingLayout_,
      CompressionOptions{},
      [](DataType) -> std::unique_ptr<EncodingSelectionPolicyBase> {
        return nullptr;
      });
}

void IndexWriter::write(const velox::VectorPtr& input, Buffer& buffer) {
  const auto prevSize = keyStream_->mutableData().size();
  const auto newSize = prevSize + input->size();
  keyStream_->ensureMutableDataCapacity(newSize);

  keyEncoder_->encode(input, keyStream_->mutableData(), [&buffer](size_t size) {
    return buffer.reserve(size);
  });

  // Verify that all new encoded keys (plus last previous key if exists) are in
  // non-descending order.
  if (enforceKeyOrder_) {
    const auto& keys = keyStream_->mutableData();
    const auto startIdx = prevSize > 0 ? prevSize - 1 : 0;
    for (auto i = startIdx + 1; i < newSize; ++i) {
      NIMBLE_USER_CHECK(
          keys[i] >= keys[i - 1],
          "Encoded keys must be in non-descending order");
    }
  }
}

void IndexWriter::encodeStream(Buffer& buffer) {
  const auto& keys = keyStream_->mutableData();
  if (keys.empty()) {
    return;
  }

  auto& chunk = encodedKeyStream_.chunks.emplace_back();
  encodeChunkData(
      std::span<const std::string_view>(keys.data(), keys.size()),
      keys[0],
      keys[keys.size() - 1],
      buffer,
      chunk);
  keyStream_->reset();
}

bool IndexWriter::encodeChunk(
    bool ensureFullChunks,
    bool lastChunk,
    Buffer& buffer) {
  if (keyStream_->mutableData().empty()) {
    return false;
  }

  // Use minChunkSize = 0 for last chunk to flush remaining keys
  const auto minChunkSize = lastChunk ? 0 : minChunkSize_;

  auto chunker = getStreamChunker(
      *keyStream_,
      StreamChunkerOptions{
          .minChunkSize = minChunkSize,
          .maxChunkSize = maxChunkSize_,
          .ensureFullChunks = ensureFullChunks,
          .isFirstChunk = encodedKeyStream_.chunks.empty()});

  bool writtenChunk{false};
  while (auto chunkView = chunker->next()) {
    const auto& keys = keyStream_->mutableData();
    const auto keyCount = chunkView->rowCount();

    auto& chunk = encodedKeyStream_.chunks.emplace_back();
    encodeChunkData(
        std::span<const std::string_view>(keys.data(), keyCount),
        keys[0],
        keys[keyCount - 1],
        buffer,
        chunk);

    chunker->compact();
    writtenChunk = true;
  }
  return writtenChunk;
}

std::optional<KeyStream> IndexWriter::finishStripe(Buffer& buffer) {
  NIMBLE_CHECK(keyStream_->empty(), "Key stream must be empty at stripe end");
  SCOPE_EXIT {
    reset();
  };
  if (encodedKeyStream_.chunks.empty()) {
    return std::nullopt;
  }
  auto result = std::move(encodedKeyStream_);
  return result;
}

uint32_t IndexWriter::encodeChunkData(
    std::span<const std::string_view> data,
    std::string_view firstKey,
    std::string_view lastKey,
    Buffer& buffer,
    KeyChunk& chunk) {
  const auto encoded = EncodingFactory::encode<std::string_view>(
      createEncodingPolicy(), data, buffer);
  NIMBLE_CHECK(!encoded.empty());

  chunk.rowCount = data.size();
  // Reserve buffer space for both keys at once to avoid two allocations.
  const auto totalKeySize = firstKey.size() + lastKey.size();
  auto* keyData = buffer.reserve(totalKeySize);
  std::memcpy(keyData, firstKey.data(), firstKey.size());
  std::memcpy(keyData + firstKey.size(), lastKey.data(), lastKey.size());
  chunk.firstKey = std::string_view{keyData, firstKey.size()};
  chunk.lastKey = std::string_view{keyData + firstKey.size(), lastKey.size()};

  uint32_t chunkBytes{0};
  ChunkedStreamWriter chunkWriter{buffer};
  for (auto& contentBuffer : chunkWriter.encode(encoded)) {
    chunkBytes += contentBuffer.size();
    chunk.content.push_back(std::move(contentBuffer));
  }
  return chunkBytes;
}

bool IndexWriter::hasKeys() const {
  return !keyStream_->empty() || !encodedKeyStream_.chunks.empty();
}

void IndexWriter::reset() {
  keyStream_->reset();
  encodedKeyStream_ = KeyStream{};
}

void IndexWriter::close() {
  reset();
}
} // namespace facebook::nimble::index
