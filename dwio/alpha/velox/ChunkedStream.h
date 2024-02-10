// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <functional>
#include <memory>
#include <string_view>

#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/common/Vector.h"
#include "dwio/alpha/tablet/Tablet.h"
#include "folly/io/IOBuf.h"

namespace facebook::alpha {

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

} // namespace facebook::alpha
