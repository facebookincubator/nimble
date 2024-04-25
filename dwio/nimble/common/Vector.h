/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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

#include "dwio/nimble/common/Exceptions.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/memory/Memory.h"

#include <array>
#include <memory>

// Basically equivalent to std::vector, but without the edge case for booleans,
// i.e. data() returns T* for all T. This allows for implicit conversion to
// std::span for all T.

namespace facebook::nimble {

template <typename T>
class Vector {
  using InnerType =
      typename std::conditional<std::is_same_v<T, bool>, uint8_t, T>::type;

 public:
  Vector(velox::memory::MemoryPool* memoryPool, size_t size, T value)
      : memoryPool_{memoryPool} {
    init(size);
    std::fill(dataRawPtr_, dataRawPtr_ + size_, value);
  }

  Vector(velox::memory::MemoryPool* memoryPool, size_t size)
      : memoryPool_{memoryPool} {
    init(size);
  }

  explicit Vector(velox::memory::MemoryPool* memoryPool)
      : memoryPool_{memoryPool} {
    capacity_ = 0;
    size_ = 0;
    data_ = nullptr;
    dataRawPtr_ = nullptr;
#ifndef NDEBUG
    dataRawPtr_ = placeholder_.data();
#endif
  }

  template <typename It>
  Vector(velox::memory::MemoryPool* memoryPool, It first, It last)
      : memoryPool_{memoryPool} {
    auto size = last - first;
    init(size);
    std::copy(first, last, dataRawPtr_);
  }

  Vector(const Vector& other) {
    *this = other;
  }

  Vector& operator=(const Vector& other) {
    if (this != &other) {
      size_ = other.size();
      capacity_ = other.capacity_;
      memoryPool_ = other.memoryPool_;
      allocateBuffer();
      std::copy(other.dataRawPtr_, other.dataRawPtr_ + size_, dataRawPtr_);
    }
    return *this;
  }

  Vector(Vector&& other) noexcept {
    *this = std::move(other);
  }

  Vector& operator=(Vector&& other) noexcept {
    if (this != &other) {
      size_ = other.size();
      capacity_ = other.capacity_;
      data_ = std::move(other.data_);
#ifndef NDEBUG
      dataRawPtr_ = placeholder_.data();
#endif
      if (data_ != nullptr) {
        dataRawPtr_ = reinterpret_cast<T*>(data_->asMutable<InnerType>());
      }
      memoryPool_ = other.memoryPool_;
      other.size_ = 0;
      other.capacity_ = 0;
    }
    return *this;
  }

  Vector(velox::memory::MemoryPool* memoryPool, std::initializer_list<T> l)
      : memoryPool_{memoryPool} {
    init(l.size());
    std::copy(l.begin(), l.end(), dataRawPtr_);
  }

  inline velox::memory::MemoryPool* memoryPool() {
    return memoryPool_;
  }

  uint64_t size() const {
    return size_;
  }
  bool empty() const {
    return size_ == 0;
  }
  uint64_t capacity() const {
    return capacity_;
  }
  T& operator[](uint64_t i) {
    return dataRawPtr_[i];
  }
  const T& operator[](uint64_t i) const {
    return dataRawPtr_[i];
  }
  T* begin() {
    return dataRawPtr_;
  }
  T* end() {
    return dataRawPtr_ + size_;
  }
  const T* begin() const {
    return dataRawPtr_;
  }
  const T* end() const {
    return dataRawPtr_ + size_;
  }
  T& back() {
    return dataRawPtr_[size_ - 1];
  }
  const T& back() const {
    return dataRawPtr_[size_ - 1];
  }

  // Directly updates the size_ to |size|. Useful if you've filled in some data
  // directly using the underlying raw pointers.
  void update_size(uint64_t size) {
    size_ = size;
  }

  // Fills all of data_ with T().
  void zero_out() {
    std::fill(dataRawPtr_, dataRawPtr_ + capacity_, T());
  }

  // Fills all of data_ with the given value.
  void fill(T value) {
    std::fill(dataRawPtr_, dataRawPtr_ + capacity_, value);
  }

  // Resets *this to a newly constructed empty state.
  void clear() {
    capacity_ = 0;
    size_ = 0;
    data_.reset();
    dataRawPtr_ = nullptr;
  }

  void insert(T* output, const T* inputStart, const T* inputEnd) {
    const uint64_t inputSize = inputEnd - inputStart;
    const uint64_t distanceToEnd = end() - output;
    if (inputSize > distanceToEnd) {
      const uint64_t sizeChange = inputSize - distanceToEnd;
      const uint64_t distanceFromBegin = output - begin();
      resize(size_ + sizeChange);
      std::move(inputStart, inputEnd, begin() + distanceFromBegin);
    } else {
      std::move(inputStart, inputEnd, output);
    }
  }

  // Add |copies| copies of |value| to the end of the vector.
  void extend(uint64_t copies, T value) {
    reserve(size_ + copies);
    std::fill(end(), end() + copies, value);
    size_ += copies;
  }

  T* data() noexcept {
    return dataRawPtr_;
  }

  const T* data() const noexcept {
    return dataRawPtr_;
  }

  void push_back(T value) {
    if (size_ == capacity_) {
      reserve(calculateNewSize(capacity_));
    }
    dataRawPtr_[size_] = value;
    ++size_;
  }

  template <typename... Args>
  void emplace_back(Args&&... args) {
    if (size_ == capacity_) {
      reserve(calculateNewSize(capacity_));
    }
    // use placement new to construct the object.
    new (dataRawPtr_ + size_) InnerType(std::forward<Args>(args)...);
    ++size_;
  }

  // Ensures that *this can hold |size| elements. Does NOT shrink to
  // fit if |size| is less than size(), and does NOT initialize any new
  // values.
  void reserve(uint64_t size) {
    if (size > capacity_) {
      capacity_ = size;
      auto newData =
          velox::AlignedBuffer::allocate<InnerType>(capacity_, memoryPool_);
      if (data_ != nullptr && size_ > 0) {
        std::move(
            dataRawPtr_,
            dataRawPtr_ + size_,
            reinterpret_cast<T*>(newData->template asMutable<InnerType>()));
      }
      data_ = std::move(newData);
      dataRawPtr_ = reinterpret_cast<T*>(data_->asMutable<InnerType>());
    }
  }

  // Changes size_ to |size|. Does NOT shrink to fit the new size, and
  // does NOT initialize any new elements if |size| is greater than size_.
  void resize(uint64_t size) {
    reserve(size);
    size_ = size;
  }

  // Changes size_ to |newSize|. Does NOT shrink to fit the new size. Initialize
  // any new elements to |value| if |newSize| is greater than size_.
  void resize(uint64_t newSize, const T& value) {
    auto initialSize = size_;
    resize(newSize);

    if (size_ > initialSize) {
      std::fill(dataRawPtr_ + initialSize, end(), value);
    }
  }

  velox::BufferPtr releaseOwnership() {
    velox::BufferPtr tmp = std::move(data_);
    tmp->setSize(size_);
    capacity_ = 0;
    size_ = 0;
    data_ = nullptr;
    dataRawPtr_ = nullptr;
#ifndef NDEBUG
    dataRawPtr_ = placeholder_.data();
#endif
    return tmp;
  }

 private:
  inline void init(size_t size) {
    capacity_ = size;
    size_ = size;
    allocateBuffer();
  }

  inline size_t calculateNewSize(size_t size) {
    auto newSize = size <<= 1;
    if (newSize == 0) {
      return velox::AlignedBuffer::kSizeofAlignedBuffer;
    }

    return newSize;
  }

  inline void allocateBuffer() {
    data_ = velox::AlignedBuffer::allocate<InnerType>(capacity_, memoryPool_);
    dataRawPtr_ = reinterpret_cast<T*>(data_->asMutable<InnerType>());
  }

  velox::memory::MemoryPool* memoryPool_;
  velox::BufferPtr data_;
  uint64_t capacity_;
  uint64_t size_;
  T* dataRawPtr_;
#ifndef NDEBUG
  inline static std::array<T, sizeof(size_t)> placeholder_;
#endif
};

} // namespace facebook::nimble
