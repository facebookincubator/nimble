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

#include <memory>
#include <span>
#include <string_view>

#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/velox/SchemaBuilder.h"

namespace facebook::nimble {

class StreamDataView;

// Stream data is a generic interface representing a stream of data, allowing
// generic access to the content to be used by writers
class StreamData {
 public:
  explicit StreamData(const StreamDescriptorBuilder& descriptor)
      : descriptor_{descriptor} {}

  StreamData(const StreamData&) = delete;
  StreamData(StreamData&&) = delete;
  StreamData& operator=(const StreamData&) = delete;
  StreamData& operator=(StreamData&&) = delete;

  virtual std::string_view data() const = 0;
  virtual std::span<const bool> nonNulls() const = 0;
  virtual bool hasNulls() const = 0;
  virtual bool empty() const = 0;
  virtual uint64_t memoryUsed() const = 0;

  virtual void reset() = 0;
  virtual void materialize() {}

  // Materializes and returns the next chunk of stream data up to specified byte
  // size, or nullptr if unsuccessful. This method enables chunking of large
  // streams to manage memory usage by processing data in smaller pieces.
  virtual std::unique_ptr<const StreamDataView> nextChunk(uint64_t) = 0;

  virtual std::unique_ptr<const StreamDataView> lastChunk() {
    materialize();
    return nextChunk(memoryUsed());
  }

  const StreamDescriptorBuilder& descriptor() const {
    return descriptor_;
  }

  virtual ~StreamData() = default;

 private:
  const StreamDescriptorBuilder& descriptor_;
};

// Represents a view into a chunk of stream data.
// Provides a lightweight, non-owning view into a portion of stream data,
// containing references to the data content and null indicators for efficient
// processing of large streams without copying data.
class StreamDataView {
 public:
  StreamDataView(
      const StreamDescriptorBuilder& descriptor,
      std::string_view data,
      std::span<const bool> nonNulls,
      bool hasNulls,
      uint64_t extraMemory = 0)
      : data_{data},
        nonNulls_{nonNulls},
        hasNulls_{hasNulls},
        extraMemory_{extraMemory},
        descriptor_{descriptor} {}

  std::string_view data() const {
    return data_;
  }

  std::span<const bool> nonNulls() const {
    return nonNulls_;
  }

  bool hasNulls() const {
    return hasNulls_;
  }

  uint64_t memoryUsed() const {
    return data_.size() + nonNulls_.size() * sizeof(bool) + extraMemory_;
  }

  uint64_t extraMemory() const {
    return extraMemory_;
  }

  const StreamDescriptorBuilder& descriptor() const {
    return descriptor_;
  }

 private:
  std::string_view data_;
  std::span<const bool> nonNulls_;
  bool hasNulls_;
  uint64_t extraMemory_;
  const StreamDescriptorBuilder& descriptor_;
};

// Content only data stream.
// Used when a stream doesn't contain nulls.
template <typename T>
class ContentStreamData final : public StreamData {
 public:
  ContentStreamData(
      velox::memory::MemoryPool& memoryPool,
      const StreamDescriptorBuilder& descriptor)
      : StreamData(descriptor),
        data_{&memoryPool},
        extraMemory_{0},
        chunkOffset_{0},
        memoryPool_{memoryPool} {}

  inline virtual std::string_view data() const override {
    return {
        reinterpret_cast<const char*>(data_.data() + chunkOffset_),
        (data_.size() - chunkOffset_) * sizeof(T)};
  }

  std::unique_ptr<const StreamDataView> nextChunk(
      uint64_t maxChunkSize) override {
    size_t nextChunkSize = maxChunkSize / sizeof(T);
    if (chunkOffset_ + nextChunkSize > data_.size()) {
      return nullptr;
    }
    std::string_view chunkData = {
        reinterpret_cast<const char*>(data_.data() + chunkOffset_),
        nextChunkSize * sizeof(T)};
    chunkOffset_ += nextChunkSize;
    return std::make_unique<StreamDataView>(
        descriptor(),
        chunkData,
        /*nonNulls=*/std::span<const bool>{},
        /*hasNulls=*/false);
  }

  inline virtual std::span<const bool> nonNulls() const override {
    return {};
  }

  inline virtual bool hasNulls() const override {
    return false;
  }

  inline virtual bool empty() const override {
    return data_.empty();
  }

  inline virtual uint64_t memoryUsed() const override {
    return ((data_.size() - chunkOffset_) * sizeof(T)) + extraMemory_;
  }

  inline Vector<T>& mutableData() {
    return data_;
  }

  inline uint64_t& extraMemory() {
    return extraMemory_;
  }

  inline virtual void reset() override {
    data_.clear();
    extraMemory_ = 0;
    chunkOffset_ = 0;
  }

 private:
  Vector<T> data_;
  uint64_t extraMemory_;
  size_t chunkOffset_;
  velox::memory::MemoryPool& memoryPool_;
};

// Nulls only data stream.
// Used in cases where boolean data (representing nulls) is needed.
// NOTE: ContentStreamData<bool> can also be used to represent these data
// streams, however, for these "null streams", we have special optimizations,
// where if all data is non-null, we omit the stream. This class specialization
// helps with reusing enabling this optimization.
class NullsStreamData : public StreamData {
 public:
  NullsStreamData(
      velox::memory::MemoryPool& memoryPool,
      const StreamDescriptorBuilder& descriptor)
      : StreamData(descriptor),
        nonNulls_{&memoryPool},
        hasNulls_{false},
        bufferedCount_{0},
        memoryPool_{memoryPool},
        chunkNullOffset_{0} {}

  inline virtual std::string_view data() const override {
    return {};
  }

  std::unique_ptr<const StreamDataView> nextChunk(
      uint64_t maxChunkSize) override {
    materialize();
    if (memoryUsed() < maxChunkSize) {
      return nullptr;
    }
    size_t nextChunkSize = maxChunkSize / sizeof(bool);

    std::span<const bool> nonNullsChunk(
        nonNulls_.data() + chunkNullOffset_, nextChunkSize);
    chunkNullOffset_ += nextChunkSize;
    return std::make_unique<StreamDataView>(
        descriptor(),
        /*data=*/std::string_view{},
        nonNullsChunk,
        hasNulls_);
  }

  inline virtual std::span<const bool> nonNulls() const override {
    // non-materialized
    if (nonNulls_.size() < bufferedCount_) {
      return nonNulls_;
    }
    return {
        nonNulls_.data() + chunkNullOffset_, bufferedCount_ - chunkNullOffset_};
  }

  inline virtual bool hasNulls() const override {
    return hasNulls_;
  }

  inline virtual bool empty() const override {
    return nonNulls_.empty() && bufferedCount_ == 0;
  }

  inline virtual uint64_t memoryUsed() const override {
    return nonNulls_.size() - chunkNullOffset_;
  }

  inline Vector<bool>& mutableNonNulls() {
    return nonNulls_;
  }

  inline virtual void reset() override {
    nonNulls_.clear();
    hasNulls_ = false;
    bufferedCount_ = 0;
    chunkNullOffset_ = 0;
  }

  void materialize() override {
    if (nonNulls_.size() < bufferedCount_) {
      const auto offset = nonNulls_.size();
      nonNulls_.resize(bufferedCount_);
      std::fill(
          nonNulls_.data() + offset, nonNulls_.data() + bufferedCount_, true);
    }
  }

  void ensureNullsCapacity(bool mayHaveNulls, uint32_t size);

 protected:
  Vector<bool> nonNulls_;
  bool hasNulls_;
  uint32_t bufferedCount_;
  velox::memory::MemoryPool& memoryPool_;
  size_t chunkNullOffset_;
};

// Nullable content data stream.
// Used in all cases where data may contain nulls.
template <typename T>
class NullableContentStreamData final : public NullsStreamData {
 public:
  NullableContentStreamData(
      velox::memory::MemoryPool& memoryPool,
      const StreamDescriptorBuilder& descriptor)
      : NullsStreamData(memoryPool, descriptor),
        data_{&memoryPool},
        extraMemory_{0},
        chunkDataOffset_{0} {}

  inline virtual std::string_view data() const override {
    return {
        reinterpret_cast<const char*>(data_.data() + chunkDataOffset_),
        (data_.size() - chunkDataOffset_) * sizeof(T)};
  }

  std::unique_ptr<const StreamDataView> nextChunk(
      uint64_t maxChunkSize) override {
    materialize();
    if (memoryUsed() < maxChunkSize) {
      return nullptr;
    }

    size_t chunkDataSize = 0;
    size_t chunkNullSize = 0;
    uint64_t rollingChunkSize = 0;

    // Calculate how many entries we can fit in the chunk
    for (size_t i = chunkNullOffset_; i < bufferedCount_; ++i) {
      uint64_t entrySize = sizeof(bool); // Always account for null indicator
      if (nonNulls_[i]) {
        entrySize += sizeof(T); // Add data size if non-null
      }

      if (rollingChunkSize + entrySize > maxChunkSize) {
        break;
      }

      if (nonNulls_[i]) {
        chunkDataSize += 1;
      }
      chunkNullSize += 1;
      rollingChunkSize += entrySize;
    }

    if (chunkNullSize == 0) {
      return nullptr;
    }

    // Chunk nulls
    std::span<const bool> nonNullsChunk(
        nonNulls_.data() + chunkNullOffset_, chunkNullSize);

    // Chunk content
    std::string_view chunkData = {
        reinterpret_cast<const char*>(data_.data() + chunkDataOffset_),
        chunkDataSize * sizeof(T)};

    chunkNullOffset_ += chunkNullSize;
    chunkDataOffset_ += chunkDataSize;

    return std::make_unique<StreamDataView>(
        descriptor(), chunkData, nonNullsChunk, hasNulls_);
  }

  inline virtual bool empty() const override {
    return NullsStreamData::empty() && data_.empty();
  }

  inline virtual uint64_t memoryUsed() const override {
    return ((data_.size() - chunkDataOffset_) * sizeof(T)) + extraMemory_ +
        NullsStreamData::memoryUsed();
  }

  inline Vector<T>& mutableData() {
    return data_;
  }

  inline uint64_t& extraMemory() {
    return extraMemory_;
  }

  inline virtual void reset() override {
    NullsStreamData::reset();
    data_.clear();
    extraMemory_ = 0;
    chunkDataOffset_ = 0;
  }

 private:
  Vector<T> data_;
  uint64_t extraMemory_;
  uint64_t chunkDataOffset_;
};

// Template specialization for std::string_view to handle extra memory properly
template <>
inline std::unique_ptr<const StreamDataView>
ContentStreamData<std::string_view>::nextChunk(uint64_t maxChunkSize) {
  if (memoryUsed() < maxChunkSize) {
    return nullptr;
  }

  // TODO: Expensive operation. We should consider not re-calculating stats.
  size_t chunkSize = 0;
  uint64_t rollingChunkSize = 0;
  uint64_t rollingExtraMemory = 0;
  for (size_t i = chunkOffset_; i < data_.size(); ++i) {
    const auto& str = data_[i];
    uint64_t strSize = str.size() + sizeof(std::string_view);
    if (rollingChunkSize + strSize > maxChunkSize) {
      break;
    }
    rollingExtraMemory += str.size();
    rollingChunkSize += strSize;
    chunkSize += 1;
  }

  if (chunkOffset_ + chunkSize > data_.size()) {
    return nullptr;
  }
  std::string_view chunkData = {
      reinterpret_cast<const char*>(data_.data() + chunkOffset_),
      chunkSize * sizeof(std::string_view)};
  chunkOffset_ += chunkSize;
  extraMemory_ -= rollingExtraMemory;
  return std::make_unique<StreamDataView>(
      descriptor(),
      chunkData,
      /*nonNulls=*/std::span<const bool>{},
      /*hasNulls=*/false,
      rollingExtraMemory);
}

template <>
inline std::unique_ptr<const StreamDataView>
NullableContentStreamData<std::string_view>::nextChunk(uint64_t maxChunkSize) {
  materialize();
  if (memoryUsed() < maxChunkSize) {
    return nullptr;
  }

  // TODO: Expensive operation. We should consider not re-calculating stats.
  size_t chunkDataSize = 0;
  size_t chunkNullSize = 0;
  uint64_t rollingChunkSize = 0;
  uint64_t rollingExtraMemory = 0;
  for (size_t i = chunkNullOffset_; i < bufferedCount_; ++i) {
    uint64_t strSize = 0;
    if (nonNulls_[i]) {
      const auto& str = data_[chunkDataOffset_ + chunkDataSize];
      strSize = str.size() + sizeof(std::string_view);
    }

    if (rollingChunkSize + sizeof(bool) + strSize > maxChunkSize) {
      break;
    }

    rollingChunkSize += sizeof(bool) + strSize;
    chunkNullSize += 1;

    if (nonNulls_[i]) {
      rollingExtraMemory += data_[chunkDataOffset_ + chunkDataSize].size();
      chunkDataSize += 1;
    }
  }

  if (chunkNullSize == 0) {
    return nullptr;
  }

  // Chunk content
  std::string_view chunkData = {
      reinterpret_cast<const char*>(data_.data() + chunkDataOffset_),
      chunkDataSize * sizeof(std::string_view)};

  // Chunk nulls
  std::span<const bool> nonNullsChunk(
      nonNulls_.data() + chunkNullOffset_, chunkNullSize);

  chunkDataOffset_ += chunkDataSize;
  chunkNullOffset_ += chunkNullSize;
  extraMemory_ -= rollingExtraMemory;
  return std::make_unique<StreamDataView>(
      descriptor(), chunkData, nonNullsChunk, hasNulls_, rollingExtraMemory);
}

} // namespace facebook::nimble
