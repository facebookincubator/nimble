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

#include "dwio/nimble/common/Buffer.h"

namespace facebook::nimble {

char* Buffer::reserve(uint64_t bytes) {
  std::scoped_lock<std::mutex> l(mutex_);
  if (reserveEnd_ + bytes <= chunkEnd_) {
    pos_ = reserveEnd_;
    reserveEnd_ += bytes;
  } else if (!tryAdvanceToNextChunk(bytes)) {
    addChunk(bytes);
  }
  return pos_;
}

void Buffer::reset() {
  std::scoped_lock<std::mutex> l(mutex_);
  chunkIndex_ = 0;
  pos_ = chunks_.front()->asMutable<char>();
  chunkEnd_ = pos_ + chunks_.front()->capacity();
  reserveEnd_ = pos_;
}

bool Buffer::tryAdvanceToNextChunk(uint64_t bytes) {
  while (chunkIndex_ + 1 < chunks_.size()) {
    ++chunkIndex_;
    auto& chunk = chunks_[chunkIndex_];
    if (chunk->capacity() >= bytes) {
      pos_ = chunk->asMutable<char>();
      chunkEnd_ = pos_ + chunk->capacity();
      reserveEnd_ = pos_ + bytes;
      return true;
    }
  }
  return false;
}

void Buffer::addChunk(uint64_t bytes) {
  const uint64_t chunkSize = std::max(bytes, kMinChunkSize);
  auto bufferPtr = velox::AlignedBuffer::allocate<char>(chunkSize, memoryPool_);
  pos_ = bufferPtr->asMutable<char>();
  chunkEnd_ = pos_ + chunkSize;
  reserveEnd_ = pos_ + bytes;
  chunks_.push_back(std::move(bufferPtr));
  chunkIndex_ = chunks_.size() - 1;
}

} // namespace facebook::nimble
