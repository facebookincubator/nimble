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
  virtual ~StreamData() = default;

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

// Content only data stream.
// Used when a stream doesn't contain nulls.
template <typename T>
class ContentStreamData final : public MutableStreamData {
 public:
  ContentStreamData(
      velox::memory::MemoryPool& memoryPool,
      const StreamDescriptorBuilder& descriptor,
      const InputBufferGrowthPolicy& growthPolicy)
      : MutableStreamData(descriptor, growthPolicy),
        data_{&memoryPool},
        extraMemory_{0} {}

  inline virtual std::string_view data() const override {
    return {
        reinterpret_cast<const char*>(data_.data()), data_.size() * sizeof(T)};
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
  uint64_t extraMemory_;
};

// Nulls only data stream.
// Used in cases where boolean data (representing nulls) is needed.
// NOTE: ContentStreamData<bool> can also be used to represent these data
// streams, however, for these "null streams", we have special optimizations,
// where if all data is non-null, we omit the stream. This class specialization
// helps with reusing enabling this optimization.
class NullsStreamData : public MutableStreamData {
 public:
  NullsStreamData(
      velox::memory::MemoryPool& memoryPool,
      const StreamDescriptorBuilder& descriptor,
      const InputBufferGrowthPolicy& growthPolicy)
      : MutableStreamData(descriptor, growthPolicy),
        nonNulls_{&memoryPool},
        hasNulls_{false},
        bufferedCount_{0} {}

  inline virtual std::string_view data() const override {
    return {};
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

  void ensureAdditionalNullsCapacity(
      bool mayHaveNulls,
      uint64_t additionalSize);

 protected:
  Vector<bool> nonNulls_;
  bool hasNulls_;
  uint32_t bufferedCount_;
};

// Nullable content data stream.
// Used in all cases where data may contain nulls.
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

  inline virtual std::string_view data() const override {
    return {
        reinterpret_cast<const char*>(data_.data()), data_.size() * sizeof(T)};
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

  void ensureMutableDataCapacity(uint64_t newSize) {
    this->ensureDataCapacity(data_, newSize);
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

} // namespace facebook::nimble
