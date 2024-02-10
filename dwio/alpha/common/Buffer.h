// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include "velox/buffer/Buffer.h"
#include "velox/common/memory/Memory.h"

#include <bits/unique_ptr.h>
#include <cstring>
#include <mutex>
#include <string_view>
#include <vector>

// Basic memory buffer interface, aka arena.
//
// Standard usage:
//   char* pos = buffer->reserve(100);
//   write data to [pos, pos + 100)
//   pos = buffer->Reserve(27);
//   write data to [pos, pos + 27)
//   and so on

namespace facebook::alpha {

// Internally manages memory in chunks. Releases memory only upon destruction.
// Buffer is NOT threadsafe: external locking is required.
class Buffer {
  using MemoryPool = facebook::velox::memory::MemoryPool;

 public:
  explicit Buffer(
      MemoryPool& memoryPool,
      uint64_t initialChunkSize = kMinChunkSize)
      : memoryPool_(memoryPool) {
    addChunk(initialChunkSize);
    reserveEnd_ = pos_;
  }

  // Returns a pointer to a block of memory of size bytes that can be written
  // to, and guarantees for the lifetime of *this that that region will remain
  // valid. Does NOT guarantee that the region is initially 0'd.
  char* reserve(uint64_t bytes) {
    // Lock to ensure can encode streams in parallel
    // TODO: update with a better solution as this is critical flush path
    std::scoped_lock<std::mutex> l(mutex_);
    if (reserveEnd_ + bytes <= chunkEnd_) {
      pos_ = reserveEnd_;
      reserveEnd_ += bytes;
    } else {
      addChunk(bytes);
    }
    return pos_;
  }

  // Copies |data| into the chunk, returning a view to the copied data.
  std::string_view writeString(std::string_view data) {
    char* pos = reserve(data.size());
    memcpy(pos, data.data(), data.size());
    return {pos, data.size()};
  }

  MemoryPool& getMemoryPool() {
    return memoryPool_;
  }

  std::string_view takeOwnership(velox::BufferPtr&& bufferPtr) {
    std::string_view chunk{bufferPtr->as<char>(), bufferPtr->size()};
    chunks_.push_back(std::move(bufferPtr));
    return chunk;
  }

 private:
  static constexpr uint64_t kMinChunkSize = 1LL << 20;

  void addChunk(uint64_t bytes) {
    const uint64_t chunkSize = std::max(bytes, kMinChunkSize);
    auto bufferPtr =
        velox::AlignedBuffer::allocate<char>(chunkSize, &memoryPool_);
    pos_ = bufferPtr->asMutable<char>();
    chunkEnd_ = pos_ + chunkSize;
    reserveEnd_ = pos_ + bytes;
    chunks_.push_back(std::move(bufferPtr));
  }

  char* chunkEnd_;
  char* pos_;
  char* reserveEnd_;
  // TODO: T168114889 evaluate if we can remove the mutex from here
  mutable std::mutex mutex_;
  std::vector<velox::BufferPtr> chunks_;
  MemoryPool& memoryPool_;
};

} // namespace facebook::alpha
