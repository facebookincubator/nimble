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

#include "velox/buffer/Buffer.h"
#include "velox/common/memory/Memory.h"

#include <cstring>
#include <memory>
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

namespace facebook::nimble {

// Internally manages memory in chunks. releases memory when destroyed
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
    // @lint-ignore CLANGSECURITY facebook-security-vulnerable-memcpy
    std::memcpy(pos, data.data(), data.size());
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
  std::vector<velox::BufferPtr> chunks_;
  MemoryPool& memoryPool_;
};

} // namespace facebook::nimble
