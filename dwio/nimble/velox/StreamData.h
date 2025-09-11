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

  // Break the stream data into chunks, each respecting the maximum size limit.
  virtual std::unique_ptr<StreamData> popChunk(uint64_t maxChunkSize) = 0;

  const StreamDescriptorBuilder& descriptor() const {
    return descriptor_;
  }

  virtual ~StreamData() = default;

 private:
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
        memoryPool_{memoryPool} {}

  inline virtual std::string_view data() const override {
    return {
        reinterpret_cast<const char*>(data_.data()), data_.size() * sizeof(T)};
  }

  inline virtual std::unique_ptr<StreamData> popChunk(
      uint64_t maxChunkSize) override {
    size_t chunkSize = maxChunkSize / sizeof(T);
    if (chunkSize > data_.size()) {
      return nullptr;
    }

    Vector<T> dataToBeReturned(
        &memoryPool_, data_.begin(), data_.begin() + chunkSize);
    Vector<T> dataToBePreserved(
        &memoryPool_, data_.begin() + chunkSize, data_.end());
    auto chunk =
        std::make_unique<ContentStreamData<T>>(memoryPool_, descriptor());
    chunk->mutableData() = std::move(dataToBeReturned);
    chunk->extraMemory_ = extraMemory_;
    data_ = std::move(dataToBePreserved);
    return chunk;
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
    return (data_.size() * sizeof(T)) + extraMemory_;
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
  }

 private:
  Vector<T> data_;
  uint64_t extraMemory_;
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
        memoryPool_{memoryPool} {}

  inline virtual std::string_view data() const override {
    return {};
  }

  inline virtual std::unique_ptr<StreamData> popChunk(
      uint64_t maxChunkSize) override {
    uint64_t chunkSize = maxChunkSize / sizeof(bool);
    if (chunkSize > bufferedCount_) {
      return nullptr;
    }
    materialize();

    Vector<bool> nonNullsToBeReturned(
        &memoryPool_, nonNulls_.begin(), nonNulls_.begin() + chunkSize);
    Vector<bool> nonNullsToBePreserved(
        &memoryPool_, nonNulls_.begin() + chunkSize, nonNulls_.end());

    auto chunk = std::make_unique<NullsStreamData>(memoryPool_, descriptor());
    chunk->mutableNonNulls() = std::move(nonNullsToBeReturned);
    chunk->bufferedCount_ = static_cast<uint32_t>(chunkSize);

    nonNulls_ = std::move(nonNullsToBePreserved);
    bufferedCount_ -= chunkSize;

    // Assume hasNulls_ is equal for both chunks to avoid costly iteration.
    chunk->hasNulls_ = hasNulls_;
    return chunk;
  }

  inline virtual std::span<const bool> nonNulls() const override {
    return nonNulls_;
  }

  inline virtual bool hasNulls() const override {
    return hasNulls_;
  }

  inline virtual bool empty() const override {
    return nonNulls_.empty() && bufferedCount_ == 0;
  }

  inline virtual uint64_t memoryUsed() const override {
    return nonNulls_.size();
  }

  inline Vector<bool>& mutableNonNulls() {
    return nonNulls_;
  }

  inline virtual void reset() override {
    nonNulls_.clear();
    hasNulls_ = false;
    bufferedCount_ = 0;
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
        extraMemory_{0} {}

  inline virtual std::string_view data() const override {
    return {
        reinterpret_cast<const char*>(data_.data()), data_.size() * sizeof(T)};
  }

  inline virtual std::unique_ptr<StreamData> popChunk(
      uint64_t maxChunkSize) override {
    uint64_t chunkSize = maxChunkSize / (sizeof(T) + sizeof(bool));
    if (chunkSize > data_.size()) {
      return nullptr;
    }

    auto chunk = std::make_unique<NullableContentStreamData<T>>(
        memoryPool_, descriptor());

    // Chunk Content
    {
      Vector<T> dataToBeReturned(
          &memoryPool_, data_.begin(), data_.begin() + chunkSize);
      Vector<T> dataToBePreserved(
          &memoryPool_, data_.begin() + chunkSize, data_.end());
      chunk->mutableData() = std::move(dataToBeReturned);
      chunk->extraMemory_ = extraMemory_;
      data_ = std::move(dataToBePreserved);
    }

    // Chunk Nulls
    {
      NullsStreamData::materialize();
      Vector<bool> nonNullsToBeReturned(
          &memoryPool_, nonNulls_.begin(), nonNulls_.begin() + chunkSize);
      Vector<bool> nonNullsToBePreserved(
          &memoryPool_, nonNulls_.begin() + chunkSize, nonNulls_.end());

      chunk->mutableNonNulls() = std::move(nonNullsToBeReturned);
      chunk->bufferedCount_ = static_cast<uint32_t>(chunkSize);

      nonNulls_ = std::move(nonNullsToBePreserved);
      bufferedCount_ -= chunkSize;

      // Assume hasNulls_ is equal for both chunks to avoid costly iteration.
      chunk->hasNulls_ = hasNulls_;
    }
    return chunk;
  }

  inline virtual bool empty() const override {
    return NullsStreamData::empty() && data_.empty();
  }

  inline virtual uint64_t memoryUsed() const override {
    return (data_.size() * sizeof(T)) + extraMemory_ +
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
  }

 private:
  Vector<T> data_;
  uint64_t extraMemory_;
};

// Template specialization for std::string_view to handle extra memory properly
template <>
inline std::unique_ptr<StreamData>
ContentStreamData<std::string_view>::popChunk(uint64_t maxChunkSize) {
  if (memoryUsed() < maxChunkSize) {
    return nullptr;
  }

  size_t chunkSize = 0;
  uint64_t rollingChunkSize = 0;
  uint64_t rollingExtraMemory = 0;
  for (size_t i = 0; i < data_.size(); ++i) {
    const auto& str = data_[i];
    uint64_t strSize = str.size() + sizeof(std::string_view);
    if (rollingChunkSize + strSize > maxChunkSize) {
      break;
    }
    rollingExtraMemory += str.size();
    rollingChunkSize += strSize;
    chunkSize = i + 1;
  }

  Vector<std::string_view> dataToBeReturned(
      &memoryPool_, data_.begin(), data_.begin() + chunkSize);
  Vector<std::string_view> dataToBePreserved(
      &memoryPool_, data_.begin() + chunkSize, data_.end());
  auto chunk = std::make_unique<ContentStreamData<std::string_view>>(
      memoryPool_, descriptor());
  chunk->mutableData() = std::move(dataToBeReturned);
  data_ = std::move(dataToBePreserved);

  chunk->extraMemory_ = rollingExtraMemory;
  extraMemory_ -= rollingExtraMemory;
  return chunk;
}

template <>
inline std::unique_ptr<StreamData>
NullableContentStreamData<std::string_view>::popChunk(uint64_t maxChunkSize) {
  if (memoryUsed() < maxChunkSize) {
    return nullptr;
  }

  size_t chunkSize = 0;
  uint64_t rollingChunkSize = 0;
  uint64_t rollingExtraMemory = 0;
  for (size_t i = 0; i < data_.size(); ++i) {
    const auto& str = data_[i];
    uint64_t strSize = str.size() + sizeof(std::string_view) + sizeof(bool);
    if (rollingChunkSize + strSize > maxChunkSize) {
      break;
    }
    rollingExtraMemory += str.size();
    rollingChunkSize += strSize;
    chunkSize = i + 1;
  }

  auto chunk = std::make_unique<NullableContentStreamData<std::string_view>>(
      memoryPool_, descriptor());
  // Chunk content
  {
    Vector<std::string_view> dataToBeReturned(
        &memoryPool_, data_.begin(), data_.begin() + chunkSize);
    Vector<std::string_view> dataToBePreserved(
        &memoryPool_, data_.begin() + chunkSize, data_.end());
    chunk->mutableData() = std::move(dataToBeReturned);
    data_ = std::move(dataToBePreserved);

    chunk->extraMemory_ = rollingExtraMemory;
    extraMemory_ -= rollingExtraMemory;
  }

  // Chunk Nulls
  {
    NullsStreamData::materialize();
    Vector<bool> nonNullsToBeReturned(
        &memoryPool_, nonNulls_.begin(), nonNulls_.begin() + chunkSize);
    Vector<bool> nonNullsToBePreserved(
        &memoryPool_, nonNulls_.begin() + chunkSize, nonNulls_.end());

    chunk->mutableNonNulls() = std::move(nonNullsToBeReturned);
    chunk->bufferedCount_ = static_cast<uint32_t>(chunkSize);

    nonNulls_ = std::move(nonNullsToBePreserved);
    bufferedCount_ -= chunkSize;

    // Assume hasNulls_ is equal for both chunks to avoid costly iteration.
    chunk->hasNulls_ = hasNulls_;
  }
  return chunk;
}

} // namespace facebook::nimble
