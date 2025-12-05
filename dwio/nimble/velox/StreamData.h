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

#include <span>
#include <string_view>

#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/velox/BufferGrowthPolicy.h"
#include "dwio/nimble/velox/SchemaBuilder.h"

namespace facebook::nimble {

/// Stream data is a generic interface representing a stream of data, allowing
/// generic access to the content to be used by writers
class StreamData {
 public:
  explicit StreamData(const StreamDescriptorBuilder& descriptor)
      : descriptor_{descriptor} {}

  StreamData(const StreamData&) = delete;
  StreamData(StreamData&&) = delete;
  StreamData& operator=(const StreamData&) = delete;
  StreamData& operator=(StreamData&&) = delete;
  virtual ~StreamData() = default;

  /// Returns the number of rows in the stream.
  virtual uint32_t numRows() const = 0;

  virtual std::string_view data() const = 0;

  virtual std::span<const bool> nonNulls() const = 0;
  virtual bool hasNulls() const = 0;

  virtual bool empty() const = 0;

  virtual uint64_t memoryUsed() const = 0;

  virtual void reset() = 0;

  virtual void materialize() {}

  const StreamDescriptorBuilder& descriptor() const {
    return descriptor_;
  }

 private:
  const StreamDescriptorBuilder& descriptor_;
};

class MutableStreamData : public StreamData {
 protected:
  MutableStreamData(
      const StreamDescriptorBuilder& descriptor,
      const InputBufferGrowthPolicy& growthPolicy)
      : StreamData(descriptor), growthPolicy_{growthPolicy} {}

  virtual ~MutableStreamData() = default;

  template <typename T>
  void ensureDataCapacity(Vector<T>& data, uint64_t newSize) {
    if (newSize > data.capacity()) {
      const auto newCapacity =
          growthPolicy_.getExtendedCapacity(newSize, data.capacity());
      data.reserve(newCapacity);
    }
  }

 private:
  const InputBufferGrowthPolicy& growthPolicy_;
};

/// Provides a lightweight, non-owning view into a portion of stream data,
/// containing references to the data content and null indicators for efficient
/// processing of large streams without copying data.
class StreamDataView final : public StreamData {
 public:
  StreamDataView(
      const StreamDescriptorBuilder& descriptor,
      std::string_view data,
      uint32_t numRows,
      std::optional<std::span<const bool>> nonNulls = std::nullopt)
      : StreamData(descriptor),
        data_{data},
        numRows_{numRows},
        nonNulls_{nonNulls} {}

  StreamDataView(
      const StreamDescriptorBuilder& descriptor,
      std::string_view data,
      uint32_t numRows,
      std::string_view firstKey,
      std::string_view lastKey)
      : StreamData(descriptor),
        data_{data},
        numRows_{numRows},
        firstKey_{firstKey},
        lastKey_{lastKey} {}

  StreamDataView(StreamDataView&& other) noexcept
      : StreamData(other.descriptor()),
        data_{other.data_},
        numRows_{other.numRows_},
        nonNulls_{other.nonNulls_},
        firstKey_{other.firstKey_},
        lastKey_{other.lastKey_} {}

  StreamDataView(const StreamDataView&) = delete;

  StreamDataView& operator=(const StreamDataView&) = delete;

  StreamDataView& operator=(StreamDataView&& other) noexcept = delete;

  ~StreamDataView() override = default;

  uint32_t numRows() const override {
    return numRows_;
  }

  std::string_view data() const override {
    return data_;
  }

  std::span<const bool> nonNulls() const override {
    return nonNulls_.value_or(std::span<const bool>{});
  }

  bool hasNulls() const override {
    return nonNulls_.has_value();
  }

  std::string_view firstKey() const {
    NIMBLE_CHECK(firstKey_.has_value(), "firstKey is not set");
    return firstKey_.value();
  }

  std::string_view lastKey() const {
    NIMBLE_CHECK(lastKey_.has_value(), "lastKey is not set");
    return lastKey_.value();
  }

  uint64_t memoryUsed() const override {
    NIMBLE_UNREACHABLE("StreamDataView is non-owning");
  }

  bool empty() const override {
    NIMBLE_UNREACHABLE("StreamDataView is non-owning");
  }

  void reset() override {
    NIMBLE_UNREACHABLE("StreamDataView is non-owning");
  }

 private:
  const std::string_view data_;
  const uint32_t numRows_;
  const std::optional<std::span<const bool>> nonNulls_;
  const std::optional<std::string_view> firstKey_;
  const std::optional<std::string_view> lastKey_;
};

/// Content only data stream.
/// Used when a stream doesn't contain nulls.
template <typename T>
class ContentStreamData final : public MutableStreamData {
 public:
  ContentStreamData(
      velox::memory::MemoryPool& pool,
      const StreamDescriptorBuilder& descriptor,
      const InputBufferGrowthPolicy& growthPolicy)
      : MutableStreamData(descriptor, growthPolicy), data_{&pool} {}

  inline uint32_t numRows() const override {
    return data_.size();
  }

  inline std::string_view data() const override {
    return {
        reinterpret_cast<const char*>(data_.data()), data_.size() * sizeof(T)};
  }

  inline std::span<const bool> nonNulls() const override {
    return {};
  }

  inline bool hasNulls() const override {
    return false;
  }

  inline bool empty() const override {
    return data_.empty();
  }

  inline uint64_t memoryUsed() const override {
    return (data_.size() * sizeof(T)) + extraMemory_;
  }

  inline Vector<T>& mutableData() {
    return data_;
  }

  void ensureMutableDataCapacity(uint64_t newSize) {
    this->ensureDataCapacity(data_, newSize);
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
  uint64_t extraMemory_{0};
};

class KeyStreamData final : public MutableStreamData {
 public:
  KeyStreamData(
      velox::memory::MemoryPool& pool,
      const StreamDescriptorBuilder& descriptor,
      const InputBufferGrowthPolicy& growthPolicy)
      : MutableStreamData(descriptor, growthPolicy), keys_{&pool} {}

  inline uint32_t numRows() const override {
    return keys_.size();
  }

  inline virtual std::string_view data() const override {
    return {
        reinterpret_cast<const char*>(keys_.data()),
        keys_.size() * sizeof(std::string_view)};
  }

  inline std::span<const bool> nonNulls() const override {
    return {};
  }

  inline bool hasNulls() const override {
    return false;
  }

  inline bool empty() const override {
    return keys_.empty();
  }

  inline uint64_t memoryUsed() const override {
    return (keys_.size() * sizeof(std::string_view)) + extraMemory_;
  }

  inline const Vector<std::string_view>& keys() const {
    return keys_;
  }

  inline Vector<std::string_view>& mutableKeys() {
    return keys_;
  }

  inline std::string_view firstKey() const {
    NIMBLE_CHECK(!keys_.empty(), "KeyStreamData is empty");
    return keys_[0];
  }

  inline std::string_view lastKey() const {
    NIMBLE_CHECK(!keys_.empty(), "KeyStreamData is empty");
    return keys_.back();
  }

  void ensureMutableDataCapacity(uint64_t newSize) {
    this->ensureDataCapacity(keys_, newSize);
  }

  inline uint64_t& extraMemory() {
    return extraMemory_;
  }

  inline void reset() override {
    keys_.clear();
    extraMemory_ = 0;
  }

 private:
  Vector<std::string_view> keys_;
  uint64_t extraMemory_{0};
};

/// Nulls only data stream.
/// Used in cases where boolean data (representing nulls) is needed.
///
/// NOTE: ContentStreamData<bool> can also be used to represent these data
/// streams, however, for these "null streams", we have special optimizations,
/// where if all data is non-null, we omit the stream. This class
/// specialization helps with reusing enabling this optimization.
class NullsStreamData : public MutableStreamData {
 public:
  NullsStreamData(
      velox::memory::MemoryPool& pool,
      const StreamDescriptorBuilder& descriptor,
      const InputBufferGrowthPolicy& growthPolicy)
      : MutableStreamData(descriptor, growthPolicy), nonNulls_{&pool} {}

  inline uint32_t numRows() const override {
    return bufferedCount_;
  }

  inline std::string_view data() const override {
    return {};
  }

  inline std::span<const bool> nonNulls() const override {
    return nonNulls_;
  }

  inline bool hasNulls() const override {
    return hasNulls_;
  }

  inline bool empty() const override {
    return nonNulls_.empty() && bufferedCount_ == 0;
  }

  inline uint64_t memoryUsed() const override {
    return nonNulls_.size();
  }

  inline Vector<bool>& mutableNonNulls() {
    return nonNulls_;
  }

  inline void reset() override {
    nonNulls_.clear();
    hasNulls_ = false;
    bufferedCount_ = 0;
  }

  void materialize() override {
    const auto offset = nonNulls_.size();
    NIMBLE_CHECK_LE(offset, bufferedCount_);
    if (offset >= bufferedCount_) {
      return;
    }
    nonNulls_.resize(bufferedCount_);
    std::fill(
        nonNulls_.data() + offset, nonNulls_.data() + bufferedCount_, true);
  }

  void ensureAdditionalNullsCapacity(
      bool mayHaveNulls,
      uint64_t additionalSize);

 protected:
  Vector<bool> nonNulls_;
  bool hasNulls_{false};
  uint32_t bufferedCount_{0};
};

/// Nullable content data stream.
/// Used in all cases where data may contain nulls.
template <typename T>
class NullableContentStreamData final : public NullsStreamData {
 public:
  NullableContentStreamData(
      velox::memory::MemoryPool& memoryPool,
      const StreamDescriptorBuilder& descriptor,
      const InputBufferGrowthPolicy& growthPolicy)
      : NullsStreamData(memoryPool, descriptor, growthPolicy),
        data_{&memoryPool},
        extraMemory_{0} {}

  inline std::string_view data() const override {
    return {
        reinterpret_cast<const char*>(data_.data()), data_.size() * sizeof(T)};
  }

  inline bool empty() const override {
    return NullsStreamData::empty() && data_.empty();
  }

  inline uint64_t memoryUsed() const override {
    return (data_.size() * sizeof(T)) + extraMemory_ +
        NullsStreamData::memoryUsed();
  }

  inline Vector<T>& mutableData() {
    return data_;
  }

  void ensureMutableDataCapacity(uint64_t newSize) {
    this->ensureDataCapacity(data_, newSize);
  }

  inline uint64_t& extraMemory() {
    return extraMemory_;
  }

  inline void reset() override {
    NullsStreamData::reset();
    data_.clear();
    extraMemory_ = 0;
  }

 private:
  Vector<T> data_;
  uint64_t extraMemory_;
};

} // namespace facebook::nimble
