// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <functional>
#include <memory>
#include <string_view>

#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/common/Vector.h"
#include "folly/io/IOBuf.h"

namespace facebook::alpha {

class StreamInput {
 public:
  virtual ~StreamInput() = default;

  virtual uint32_t size() const = 0;

  virtual bool hasNext() const = 0;

  virtual std::string_view nextChunk() = 0;

  virtual CompressionType peekCompressionType() const = 0;

  virtual void reset() = 0;

  virtual void extract(std::function<void(std::string_view)>) const = 0;
};

class InMemoryStreamInput : public StreamInput {
 public:
  InMemoryStreamInput(
      velox::memory::MemoryPool& memoryPool,
      std::string_view input)
      : InMemoryStreamInput{{&memoryPool, input.cbegin(), input.cend()}} {}

  explicit InMemoryStreamInput(Vector<char> input)
      : data_{std::move(input)},
        pos_{data_.data()},
        uncompressed_{data_.memoryPool()} {}

  InMemoryStreamInput(velox::memory::MemoryPool& memoryPool, folly::IOBuf input)
      : data_{toVector(memoryPool, std::move(input))},
        pos_{data_.data()},
        uncompressed_{&memoryPool} {}

  uint32_t size() const override {
    return data_.size();
  }

  bool hasNext() const override;

  std::string_view nextChunk() override;

  CompressionType peekCompressionType() const override;

  void reset() override;

  void extract(std::function<void(std::string_view)>) const override;

 private:
  static Vector<char> toVector(
      velox::memory::MemoryPool& memoryPool,
      folly::IOBuf input);

  Vector<char> data_;
  const char* pos_;
  Vector<char> uncompressed_;
};

} // namespace facebook::alpha
