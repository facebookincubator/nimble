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

#include <functional>
#include <memory>
#include <string_view>

#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/tablet/Tablet.h"
#include "folly/io/IOBuf.h"

namespace facebook::nimble {

class ChunkedStream {
 public:
  virtual ~ChunkedStream() = default;

  virtual bool hasNext() = 0;

  virtual std::string_view nextChunk() = 0;

  virtual CompressionType peekCompressionType() = 0;

  virtual void reset() = 0;
};

class InMemoryChunkedStream : public ChunkedStream {
 public:
  InMemoryChunkedStream(
      velox::memory::MemoryPool& memoryPool,
      std::unique_ptr<StreamLoader> streamLoader)
      : streamLoader_{std::move(streamLoader)},
        pos_{nullptr},
        uncompressed_{&memoryPool} {}

  bool hasNext() override;

  std::string_view nextChunk() override;

  CompressionType peekCompressionType() override;

  void reset() override;

 private:
  void ensureLoaded();

  std::unique_ptr<StreamLoader> streamLoader_;
  std::string_view stream_;
  const char* pos_;
  Vector<char> uncompressed_;
};

} // namespace facebook::nimble
