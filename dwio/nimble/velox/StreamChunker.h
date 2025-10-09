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

#pragma once

#include "dwio/nimble/velox/StreamData.h"

namespace facebook::nimble {
namespace {
bool omitStream(
    const StreamData& streamData,
    uint64_t minChunkSize,
    bool isNullStream,
    bool hasEmptyStreamContent) {
  bool shouldChunkStream;
  if (!isNullStream) {
    // When all values are null, the values stream is omitted.
    shouldChunkStream = streamData.data().size() > minChunkSize;
    if (!shouldChunkStream && !hasEmptyStreamContent) {
      shouldChunkStream = (minChunkSize == 0) && !streamData.nonNulls().empty();
    }
  } else {
    // When all values are non-nulls, we omit the entire null stream.
    shouldChunkStream =
        streamData.hasNulls() && streamData.nonNulls().size() > minChunkSize;
    if (!shouldChunkStream && !hasEmptyStreamContent) {
      shouldChunkStream = (minChunkSize == 0) && !streamData.empty();
    }
  }
  return !shouldChunkStream;
}
} // namespace
class StreamChunker;

template <typename T>
std::unique_ptr<StreamChunker> encodeStreamTyped(
    StreamData& streamData,
    uint64_t maxChunkSize,
    uint64_t minChunkSize,
    bool emptyStreamContent,
    bool isNullStream);

std::unique_ptr<StreamChunker> getStreamChunker(
    StreamData& streamData,
    uint64_t maxChunkSize,
    uint64_t minChunkSize,
    bool emptyStreamContent,
    bool isNullStream);

/**
 * Breaks streams into manageable chunks based on size thresholds.
 */
class StreamChunker {
 public:
  // Returns the next chunk of stream data.
  virtual StreamDataView next() = 0;

  // Checks if more chunks are available.
  virtual bool hasNext() = 0;

  // Removes processed data to reclaim memory.
  virtual void compact() = 0;

  virtual ~StreamChunker() = default;
};

template <typename T>
class ContentStreamChunker final : public StreamChunker {
 public:
  explicit ContentStreamChunker(
      ContentStreamData<T>& streamData,
      uint64_t maxChunkSize,
      uint64_t minChunkSize)
      : streamData_{streamData},
        maxChunkSize_{maxChunkSize},
        minChunkSize_{minChunkSize},
        dataOffset_{0},
        extraMemory_{streamData_.extraMemory()} {}

  StreamDataView next() override {
    const auto chunkSize = nextChunkSize();
    if (chunkSize == 0) {
      NIMBLE_UNREACHABLE("next() called when hasNext() is false");
    }

    std::string_view chunkData = {
        reinterpret_cast<const char*>(
            streamData_.mutableData().data() + dataOffset_),
        chunkSize * sizeof(T)};
    dataOffset_ += chunkSize;

    return StreamDataView{
        streamData_.descriptor(),
        chunkData,
        /*nonNulls=*/std::span<const bool>{},
        /*hasNulls=*/false};
  }

  bool hasNext() override {
    return nextChunkSize() > 0;
  }

  void compact() override {
    auto& data = streamData_.mutableData();
    data.trimLeft(dataOffset_);
    dataOffset_ = 0;
    if constexpr (std::is_same_v<T, std::string_view>) {
      streamData_.extraMemory() = extraMemory_;
    }
  }

 private:
  size_t nextChunkSize() {
    size_t remainingSize = streamData_.mutableData().size() - dataOffset_;
    size_t nextChunkSize = std::min(maxChunkSize_ / sizeof(T), remainingSize);
    if (nextChunkSize < minChunkSize_ / sizeof(T)) {
      nextChunkSize = 0;
    }
    return nextChunkSize;
  }

  ContentStreamData<T>& streamData_;
  uint64_t maxChunkSize_;
  uint64_t minChunkSize_;
  size_t dataOffset_;
  size_t extraMemory_;
};

template <>
inline bool ContentStreamChunker<std::string_view>::hasNext() {
  const auto& data = streamData_.mutableData();
  const auto streamDataCount = data.size();
  if (dataOffset_ >= streamDataCount) {
    return false;
  }

  uint64_t remainingMemory = 0;
  for (auto i = dataOffset_; i < streamDataCount; ++i) {
    remainingMemory += data[i].size() + sizeof(std::string_view);
    if (remainingMemory >= minChunkSize_) {
      return true;
    }
  }

  return false;
}

template <>
inline StreamDataView ContentStreamChunker<std::string_view>::next() {
  auto& data = streamData_.mutableData();
  if (dataOffset_ >= data.size()) {
    NIMBLE_UNREACHABLE("next() called when hasNext() is false");
  }

  size_t chunkSize = 0;
  uint64_t rollingChunkSize = 0;
  uint64_t rollingExtraMemory = 0;
  for (size_t i = dataOffset_; i < data.size(); ++i) {
    const auto& str = data[i];
    uint64_t strSize = str.size() + sizeof(std::string_view);
    if ((rollingChunkSize + strSize > maxChunkSize_)) {
      break;
    }

    rollingExtraMemory += str.size();
    rollingChunkSize += strSize;
    chunkSize += 1;
  }

  if ((rollingChunkSize < minChunkSize_)) {
    NIMBLE_UNREACHABLE("next() called when hasNext() is false");
  }

  std::string_view chunkData = {
      reinterpret_cast<const char*>(
          streamData_.mutableData().data() + dataOffset_),
      chunkSize * sizeof(std::string_view)};
  dataOffset_ += chunkSize;
  streamData_.extraMemory() -= rollingExtraMemory;

  return StreamDataView{
      streamData_.descriptor(),
      chunkData,
      /*nonNulls=*/std::span<const bool>{},
      /*hasNulls=*/false,
      rollingExtraMemory};
}

class NullsStreamChunker final : public StreamChunker {
 public:
  explicit NullsStreamChunker(
      NullsStreamData& streamData,
      uint64_t maxChunkSize,
      uint64_t minChunkSize,
      bool emptyStreamContent = true)
      : streamData_{streamData},
        maxChunkElements_{maxChunkSize / sizeof(bool)},
        minChunkElements_{minChunkSize / sizeof(bool)},
        nonNullsOffset_{0},
        omitStream_{
            omitStream(streamData, minChunkSize, true, emptyStreamContent)} {
    streamData.materialize();
  }

  StreamDataView next() override {
    const auto chunkSize = nextChunkSize();
    if (chunkSize == 0) {
      NIMBLE_UNREACHABLE("next() called when hasNext() is false");
    }

    std::span<const bool> nonNullsChunk(
        streamData_.mutableNonNulls().data() + nonNullsOffset_, chunkSize);
    nonNullsOffset_ += chunkSize;
    return StreamDataView{
        streamData_.descriptor(),
        /*data=*/std::string_view{},
        nonNullsChunk,
        streamData_.hasNulls()};
  }

  bool hasNext() override {
    return !omitStream_ && nextChunkSize() > 0;
  }

  void compact() override {
    auto& nonNulls = streamData_.mutableNonNulls();
    nonNulls.trimLeft(nonNullsOffset_);
    streamData_.bufferedCount() = static_cast<uint32_t>(nonNulls.size());
    nonNullsOffset_ = 0;
  }

 private:
  size_t nextChunkSize() {
    size_t remainingSize = streamData_.memoryUsed() - nonNullsOffset_;
    size_t nextChunkSize = std::min(maxChunkElements_, remainingSize);
    if (nextChunkSize < minChunkElements_) {
      nextChunkSize = 0;
    }
    return nextChunkSize;
  }

  NullsStreamData& streamData_;
  uint64_t maxChunkElements_;
  uint64_t minChunkElements_;
  size_t nonNullsOffset_;
  bool omitStream_;
};

template <typename T>
class NullableContentStreamChunker final : public StreamChunker {
 public:
  explicit NullableContentStreamChunker(
      NullableContentStreamData<T>& streamData,
      uint64_t maxChunkSize,
      uint64_t minChunkSize,
      bool emptyStreamContent = true,
      bool isNullStream = false)
      : streamData_{streamData},
        maxChunkSize_{maxChunkSize},
        minChunkSize_{minChunkSize},
        dataOffset_{0},
        nonNullsOffset_{0},
        isNullStream_{isNullStream},
        extraMemory_{streamData_.extraMemory()},
        omitStream_{omitStream(
            streamData,
            minChunkSize,
            isNullStream,
            emptyStreamContent)} {
    streamData.materialize();
  }

  StreamDataView next() override {
    const auto result = nextChunkSizes();
    if (result.nullSize == 0) {
      NIMBLE_UNREACHABLE("next() called when hasNext() is false");
    }

    // Chunk content
    std::string_view chunkData = {
        reinterpret_cast<const char*>(
            streamData_.mutableData().data() + dataOffset_),
        result.dataSize * sizeof(T)};

    // Chunk nulls
    std::span<const bool> nonNullsChunk(
        streamData_.mutableNonNulls().data() + nonNullsOffset_,
        result.nullSize);

    dataOffset_ += result.dataSize;
    nonNullsOffset_ += result.nullSize;

    return StreamDataView{
        streamData_.descriptor(),
        chunkData,
        nonNullsChunk,
        streamData_.hasNulls()};
  }

  bool hasNext() override {
    return !omitStream_ && nextChunkSizes().nullSize > 0;
  }

  void compact() override {
    auto& data = streamData_.mutableData();
    auto& nonNulls = streamData_.mutableNonNulls();
    data.trimLeft(dataOffset_);
    nonNulls.trimLeft(nonNullsOffset_);
    streamData_.extraMemory() = extraMemory_;
    streamData_.bufferedCount() = static_cast<uint32_t>(nonNulls.size());
    dataOffset_ = 0;
    nonNullsOffset_ = 0;
  }

 private:
  struct ChunkSizes {
    size_t dataSize = 0;
    size_t nullSize = 0;
    uint64_t extraMemory = 0;
  };

  ChunkSizes nextChunkSizes() {
    const auto& nonNulls = streamData_.mutableNonNulls();
    const auto bufferedCount = nonNulls.size();

    ChunkSizes result;
    uint64_t rollingChunkSize = 0;

    // Calculate how many entries we can fit in the chunk
    for (size_t i = nonNullsOffset_; i < bufferedCount; ++i) {
      uint64_t entrySize = sizeof(bool); // Always account for null indicator
      if (nonNulls[i]) {
        entrySize += sizeof(T); // Add data size if non-null
      }

      if (rollingChunkSize + entrySize > maxChunkSize_) {
        break;
      }

      if (nonNulls[i]) {
        result.dataSize += 1;
      }
      result.nullSize += 1;
      rollingChunkSize += entrySize;
    }

    // Check if we meet minimum size requirements
    if (rollingChunkSize < minChunkSize_) {
      result = ChunkSizes{0, 0, 0};
    }

    return result;
  }

  NullableContentStreamData<T>& streamData_;
  uint64_t maxChunkSize_;
  uint64_t minChunkSize_;
  size_t dataOffset_;
  size_t nonNullsOffset_;
  bool isNullStream_;
  size_t extraMemory_;
  bool omitStream_;
};

template <>
inline NullableContentStreamChunker<std::string_view>::ChunkSizes
NullableContentStreamChunker<std::string_view>::nextChunkSizes() {
  const auto& data = streamData_.mutableData();
  const auto& nonNulls = streamData_.mutableNonNulls();
  const auto bufferedCount = nonNulls.size();

  ChunkSizes result;
  uint64_t rollingChunkSize = 0;

  // Calculate how many entries we can fit in the chunk
  for (size_t i = nonNullsOffset_; i < bufferedCount; ++i) {
    uint64_t strSize = 0;
    if (nonNulls[i]) {
      const auto& str = data[dataOffset_ + result.dataSize];
      strSize = str.size() + sizeof(std::string_view);
    }

    if (rollingChunkSize + sizeof(bool) + strSize > maxChunkSize_) {
      break;
    }

    rollingChunkSize += sizeof(bool) + strSize;
    result.nullSize += 1;

    if (nonNulls[i]) {
      result.extraMemory += data[dataOffset_ + result.dataSize].size();
      result.dataSize += 1;
    }
  }

  // Check if we meet minimum size requirements
  if (rollingChunkSize < minChunkSize_) {
    result = ChunkSizes{0, 0, 0};
  }

  return result;
}

template <>
inline StreamDataView NullableContentStreamChunker<std::string_view>::next() {
  const auto result = nextChunkSizes();
  if (result.nullSize == 0) {
    NIMBLE_UNREACHABLE("next() called when hasNext() is false");
  }

  // Chunk content
  std::string_view chunkData = {
      reinterpret_cast<const char*>(
          streamData_.mutableData().data() + dataOffset_),
      result.dataSize * sizeof(std::string_view)};

  // Chunk nulls
  std::span<const bool> nonNullsChunk(
      streamData_.mutableNonNulls().data() + nonNullsOffset_, result.nullSize);

  dataOffset_ += result.dataSize;
  nonNullsOffset_ += result.nullSize;
  streamData_.extraMemory() -= result.extraMemory;

  return StreamDataView{
      streamData_.descriptor(),
      chunkData,
      nonNullsChunk,
      streamData_.hasNulls(),
      result.extraMemory};
}
} // namespace facebook::nimble
