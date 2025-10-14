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
// When we create new buffers, it's likely that these buffers will end up
// growing to the size that previously triggered chunking. As a trafeoff between
// minimizing reallocations (for performance) and actually releasing memory (to
// relieve memory pressure), we heuristically set the new buffer size to be
// maxChunkSize_ plus 50% of the difference between the current buffer capacity
// and maxChunkSize_. This ensures the buffer is not too small (causing frequent
// reallocations) nor too large (wasting memory).
template <typename T>
uint64_t getNewBufferCapacity(
    uint64_t maxChunkSize,
    uint64_t currentCapacity,
    uint64_t newBufferSize) {
  if (currentCapacity < maxChunkSize) {
    return currentCapacity;
  }
  return std::max<uint64_t>(
      (maxChunkSize + (currentCapacity - maxChunkSize) * 0.5),
      newBufferSize * sizeof(T));
}

/**
 * Breaks streams into manageable chunks based on size thresholds.
 * When out of scope processed StreamData values are erased to reclaim memory.
 */
class StreamChunker {
 public:
  StreamChunker() = default;
  StreamChunker(const StreamChunker&) = delete;
  StreamChunker(StreamChunker&&) = delete;
  StreamChunker& operator=(const StreamChunker&) = delete;
  StreamChunker& operator=(StreamChunker&&) = delete;

  // Returns the next chunk of stream data.
  virtual std::optional<StreamDataView> next() = 0;

  virtual ~StreamChunker() = default;

 protected:
  // Erases processed data to reclaim memory.
  virtual void compact() = 0;

  // Helper struct to hold result of nextChunkSize helper methods.
  struct ChunkSize {
    size_t dataSize = 0;
    size_t rollingChunkSize = 0;
    uint64_t extraMemory = 0;
    size_t nullSize = 0;
  };
};

std::unique_ptr<StreamChunker> getStreamChunker(
    StreamData& streamData,
    uint64_t maxChunkSize,
    uint64_t minChunkSize,
    bool ensureFullChunks,
    bool emptyStreamContent,
    bool isNullStream,
    bool isLastChunk);

bool omitStream(
    const StreamData& streamData,
    uint64_t minChunkSize,
    bool isNullStream,
    bool hasEmptyStreamContent,
    bool isLastChunk);

template <typename T>
class ContentStreamChunker final : public StreamChunker {
 public:
  explicit ContentStreamChunker(
      ContentStreamData<T>& streamData,
      uint64_t maxChunkSize,
      uint64_t minChunkSize,
      bool ensureFullChunks,
      bool lastChunk)
      : streamData_{streamData},
        maxChunkSize_{maxChunkSize},
        minChunkSize_{lastChunk ? 0 : minChunkSize},
        dataOffset_{0},
        extraMemory_{streamData_.extraMemory()},
        ensureFullChunks_{lastChunk ? false : ensureFullChunks} {}

  ContentStreamChunker(const ContentStreamChunker&) = delete;
  ContentStreamChunker(ContentStreamChunker&&) = delete;
  ContentStreamChunker& operator=(const ContentStreamChunker&) = delete;
  ContentStreamChunker& operator=(ContentStreamChunker&&) = delete;

  ~ContentStreamChunker() override {
    compact();
  }

  std::optional<StreamDataView> next() override {
    const auto& chunkSize = nextChunkSize();
    if (chunkSize.rollingChunkSize == 0) {
      return std::nullopt;
    }

    std::string_view chunkData = {
        reinterpret_cast<const char*>(
            streamData_.mutableData().data() + dataOffset_),
        chunkSize.dataSize * sizeof(T)};
    dataOffset_ += chunkSize.dataSize;
    extraMemory_ -= chunkSize.extraMemory;

    return StreamDataView{
        streamData_.descriptor(),
        chunkData,
        /*nonNulls=*/std::span<const bool>{},
        /*hasNulls=*/false,
        chunkSize.extraMemory};
  }

 private:
  ChunkSize nextChunkSize() {
    const size_t maxChunkValuesCount = maxChunkSize_ / sizeof(T);
    const size_t remainingValuesCount =
        streamData_.mutableData().size() - dataOffset_;
    if ((ensureFullChunks_ && remainingValuesCount < maxChunkValuesCount) ||
        (remainingValuesCount < minChunkSize_ / sizeof(T))) {
      return ChunkSize{};
    }
    const size_t chunkValuesCount =
        std::min(maxChunkValuesCount, remainingValuesCount);
    return ChunkSize{
        .dataSize = chunkValuesCount,
        .rollingChunkSize = chunkValuesCount * sizeof(T)};
  }

  void compact() override {
    const auto& currentData = streamData_.mutableData();
    const uint64_t remainingDataCount = currentData.size() - dataOffset_;
    const auto newDataCapactity = getNewBufferCapacity<T>(
        maxChunkSize_, currentData.capacity(), remainingDataCount);

    // Move and clear existing buffer
    auto tempData = std::move(currentData);
    streamData_.reset();

    streamData_.mutableData().reserve(newDataCapactity);
    streamData_.mutableData().resize(remainingDataCount);
    std::copy_n(
        tempData.begin() + dataOffset_,
        remainingDataCount,
        streamData_.mutableData().begin());
    dataOffset_ = 0;
    streamData_.extraMemory() = extraMemory_;
  }

  ContentStreamData<T>& streamData_;
  uint64_t maxChunkSize_;
  uint64_t minChunkSize_;
  size_t dataOffset_;
  size_t extraMemory_;
  bool ensureFullChunks_;
};

template <>
inline StreamChunker::ChunkSize
ContentStreamChunker<std::string_view>::nextChunkSize() {
  const auto& data = streamData_.mutableData();
  size_t stringCount = 0;
  uint64_t rollingChunkSize = 0;
  uint64_t rollingExtraMemory = 0;
  for (size_t i = dataOffset_;
       ((i < data.size()) && (rollingChunkSize < maxChunkSize_));
       ++i) {
    const auto& str = data[i];
    uint64_t strSize = str.size() + sizeof(std::string_view);
    if (rollingChunkSize + strSize > maxChunkSize_) {
      break;
    }

    rollingExtraMemory += str.size();
    rollingChunkSize += strSize;
    ++stringCount;
  }

  if ((ensureFullChunks_ && rollingChunkSize < maxChunkSize_) ||
      (rollingChunkSize < minChunkSize_)) {
    return ChunkSize{};
  }

  return ChunkSize{
      .dataSize = stringCount,
      .rollingChunkSize = rollingChunkSize,
      .extraMemory = rollingExtraMemory};
}

class NullsStreamChunker final : public StreamChunker {
 public:
  explicit NullsStreamChunker(
      NullsStreamData& streamData,
      uint64_t maxChunkSize,
      uint64_t minChunkSize,
      bool ensureFullChunks,
      bool emptyStreamContent,
      bool isNullStream,
      bool lastChunk)
      : streamData_{streamData},
        maxChunkSize_{maxChunkSize},
        minChunkSize_{lastChunk ? 0 : minChunkSize},
        nonNullsOffset_{0},
        omitStream_{omitStream(
            streamData,
            minChunkSize,
            isNullStream,
            emptyStreamContent,
            lastChunk)},
        ensureFullChunks_{lastChunk ? false : ensureFullChunks} {
    static_assert(sizeof(bool) == 1);
    streamData.materialize();
  }

  NullsStreamChunker(const NullsStreamChunker&) = delete;
  NullsStreamChunker(NullsStreamChunker&&) = delete;
  NullsStreamChunker& operator=(const NullsStreamChunker&) = delete;
  NullsStreamChunker& operator=(NullsStreamChunker&&) = delete;

  ~NullsStreamChunker() override {
    compact();
  }

  std::optional<StreamDataView> next() override {
    if (omitStream_) {
      return std::nullopt;
    }

    const auto& nonNulls = streamData_.mutableNonNulls();
    size_t remainingNonNulls = nonNulls.size() - nonNullsOffset_;
    size_t nonNullsInChunk = std::min(maxChunkSize_, remainingNonNulls);
    if (nonNullsInChunk == 0 || nonNullsInChunk < minChunkSize_ ||
        (ensureFullChunks_ && nonNullsInChunk < maxChunkSize_)) {
      return std::nullopt;
    }

    std::span<const bool> nonNullsChunk(
        nonNulls.data() + nonNullsOffset_, nonNullsInChunk);
    nonNullsOffset_ += nonNullsInChunk;

    return StreamDataView{
        streamData_.descriptor(),
        /*data=*/std::string_view{},
        nonNullsChunk,
        streamData_.hasNulls()};
  }

 private:
  void compact() override {
    auto& nonNulls = streamData_.mutableNonNulls();
    const auto remainingNonNullsCount = nonNulls.size() - nonNullsOffset_;
    const auto newCapacity = getNewBufferCapacity<bool>(
        maxChunkSize_, nonNulls.capacity(), remainingNonNullsCount);

    // Move and clear existing buffer
    auto tempNonNulls = std::move(nonNulls);
    streamData_.reset();

    streamData_.mutableNonNulls().reserve(newCapacity);
    const bool hasNulls = std::find(
                              tempNonNulls.begin() + nonNullsOffset_,
                              tempNonNulls.end(),
                              false) != tempNonNulls.end();
    streamData_.ensureNullsCapacity(
        hasNulls, static_cast<uint32_t>(remainingNonNullsCount));
    if (hasNulls) {
      streamData_.mutableNonNulls().resize(remainingNonNullsCount);
      std::copy_n(
          tempNonNulls.begin() + nonNullsOffset_,
          remainingNonNullsCount,
          streamData_.mutableNonNulls().begin());
    }
    nonNullsOffset_ = 0;
  }

  NullsStreamData& streamData_;
  uint64_t maxChunkSize_;
  uint64_t minChunkSize_;
  size_t nonNullsOffset_;
  bool omitStream_;
  bool ensureFullChunks_;
};

template <typename T>
class NullableContentStreamChunker final : public StreamChunker {
 public:
  explicit NullableContentStreamChunker(
      NullableContentStreamData<T>& streamData,
      uint64_t maxChunkSize,
      uint64_t minChunkSize,
      bool ensureFullChunks,
      bool emptyStreamContent,
      bool isNullStream,
      bool lastChunk)
      : streamData_{streamData},
        maxChunkSize_{maxChunkSize},
        minChunkSize_{lastChunk ? 0 : minChunkSize},
        dataOffset_{0},
        nonNullsOffset_{0},
        isNullStream_{isNullStream},
        extraMemory_{streamData_.extraMemory()},
        omitStream_{omitStream(
            streamData,
            minChunkSize,
            isNullStream,
            emptyStreamContent,
            lastChunk)},
        ensureFullChunks_{lastChunk ? false : ensureFullChunks} {
    streamData.materialize();
  }

  NullableContentStreamChunker(const NullableContentStreamChunker&) = delete;
  NullableContentStreamChunker(NullableContentStreamChunker&&) = delete;
  NullableContentStreamChunker& operator=(const NullableContentStreamChunker&) =
      delete;
  NullableContentStreamChunker& operator=(NullableContentStreamChunker&&) =
      delete;

  ~NullableContentStreamChunker() override {
    compact();
  }

  std::optional<StreamDataView> next() override {
    if (omitStream_) {
      return std::nullopt;
    }
    const auto& chunkSize = nextChunkSize();
    if (chunkSize.rollingChunkSize == 0) {
      return std::nullopt;
    }

    // Chunk content
    std::string_view chunkData = {
        reinterpret_cast<const char*>(
            streamData_.mutableData().data() + dataOffset_),
        chunkSize.dataSize * sizeof(T)};

    // Chunk nulls
    std::span<const bool> nonNullsChunk(
        streamData_.mutableNonNulls().data() + nonNullsOffset_,
        chunkSize.nullSize);

    dataOffset_ += chunkSize.dataSize;
    nonNullsOffset_ += chunkSize.nullSize;
    streamData_.extraMemory() -= chunkSize.extraMemory;

    return StreamDataView{
        streamData_.descriptor(),
        chunkData,
        nonNullsChunk,
        streamData_.hasNulls(),
        chunkSize.extraMemory};
  }

 private:
  ChunkSize nextChunkSize() {
    const auto& nonNulls = streamData_.mutableNonNulls();
    const auto bufferedCount = nonNulls.size();

    size_t dataSize = 0;
    size_t nullSize = 0;
    uint64_t rollingChunkSize = 0;

    // Calculate how many entries we can fit in the chunk
    for (size_t i = nonNullsOffset_;
         ((i < bufferedCount) && (rollingChunkSize < maxChunkSize_));
         ++i) {
      uint64_t entrySize = sizeof(bool); // Always account for null indicator
      if (nonNulls[i]) {
        entrySize += sizeof(T); // Add data size if non-null
      }

      if (rollingChunkSize + entrySize > maxChunkSize_) {
        break;
      }

      if (nonNulls[i]) {
        dataSize += 1;
      }
      nullSize += 1;
      rollingChunkSize += entrySize;
    }

    if ((ensureFullChunks_ && rollingChunkSize < maxChunkSize_) ||
        (rollingChunkSize < minChunkSize_)) {
      return ChunkSize{};
    }

    return ChunkSize{
        .dataSize = dataSize,
        .rollingChunkSize = rollingChunkSize,
        .extraMemory = 0,
        .nullSize = nullSize};
  }

  void compact() override {
    auto& currentNonNulls = streamData_.mutableNonNulls();
    auto& currentData = streamData_.mutableData();
    const auto remainingNonNullsCount =
        currentNonNulls.size() - nonNullsOffset_;
    const auto remainingDataCount = currentData.size() - dataOffset_;

    const auto newNonNullsCapacity = getNewBufferCapacity<bool>(
        maxChunkSize_, currentNonNulls.capacity(), remainingNonNullsCount);
    const auto newDataCapactity = getNewBufferCapacity<T>(
        maxChunkSize_, currentData.capacity(), remainingDataCount);

    // Move and clear existing buffers
    auto tempNonNulls = std::move(currentNonNulls);
    auto tempData = std::move(currentData);
    streamData_.reset();

    streamData_.mutableData().reserve(newDataCapactity);
    streamData_.mutableData().resize(remainingDataCount);
    std::copy_n(
        tempData.begin() + dataOffset_,
        remainingDataCount,
        streamData_.mutableData().begin());

    streamData_.mutableNonNulls().reserve(newNonNullsCapacity);
    const bool hasNulls = std::find(
                              tempNonNulls.begin() + nonNullsOffset_,
                              tempNonNulls.end(),
                              false) != tempNonNulls.end();
    streamData_.ensureNullsCapacity(
        hasNulls, static_cast<uint32_t>(remainingNonNullsCount));
    if (hasNulls) {
      streamData_.mutableNonNulls().resize(remainingNonNullsCount);
      std::copy_n(
          tempNonNulls.begin() + nonNullsOffset_,
          remainingNonNullsCount,
          streamData_.mutableNonNulls().begin());
    }
    streamData_.extraMemory() = extraMemory_;
    dataOffset_ = 0;
    nonNullsOffset_ = 0;
  }

  NullableContentStreamData<T>& streamData_;
  uint64_t maxChunkSize_;
  uint64_t minChunkSize_;
  size_t dataOffset_;
  size_t nonNullsOffset_;
  bool isNullStream_;
  size_t extraMemory_;
  bool omitStream_;
  bool ensureFullChunks_;
};

template <>
inline StreamChunker::ChunkSize
NullableContentStreamChunker<std::string_view>::nextChunkSize() {
  const auto& data = streamData_.mutableData();
  const auto& nonNulls = streamData_.mutableNonNulls();
  const auto bufferedCount = nonNulls.size();

  size_t dataSize = 0;
  size_t nullSize = 0;
  uint64_t rollingChunkSize = 0;
  uint64_t extraMemory = 0;

  // Calculate how many entries we can fit in the chunk
  for (size_t i = nonNullsOffset_;
       ((i < bufferedCount) && (rollingChunkSize < maxChunkSize_));
       ++i) {
    uint64_t strSize = 0;
    if (nonNulls[i]) {
      const auto& str = data[dataOffset_ + dataSize];
      strSize = str.size() + sizeof(std::string_view);
    }

    if (rollingChunkSize + sizeof(bool) + strSize > maxChunkSize_) {
      break;
    }

    rollingChunkSize += sizeof(bool) + strSize;
    nullSize += 1;

    if (nonNulls[i]) {
      extraMemory += data[dataOffset_ + dataSize].size();
      dataSize += 1;
    }
  }

  if ((ensureFullChunks_ && rollingChunkSize < maxChunkSize_) ||
      (rollingChunkSize < minChunkSize_)) {
    return ChunkSize{};
  }

  return ChunkSize{
      .dataSize = dataSize,
      .rollingChunkSize = rollingChunkSize,
      .extraMemory = extraMemory,
      .nullSize = nullSize};
}
} // namespace facebook::nimble
