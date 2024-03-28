// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/common/MetricsLogger.h"
#include "dwio/alpha/encodings/Encoding.h"
#include "dwio/alpha/velox/ChunkedStream.h"
#include "dwio/alpha/velox/Decoder.h"

namespace facebook::alpha {

class StreamInputDecoder : public Decoder {
 public:
  StreamInputDecoder(
      velox::memory::MemoryPool& pool,
      std::unique_ptr<ChunkedStream> stream,
      const MetricsLogger& logger)
      : pool_{pool}, stream_{std::move(stream)}, logger_{logger} {}

  uint32_t next(
      uint32_t count,
      void* output,
      std::function<void*()> nulls = nullptr,
      const bits::Bitmap* scatterBitmap = nullptr) override;

  void skip(uint32_t count) override;

  void reset() override;

 private:
  void ensureLoaded();

  velox::memory::MemoryPool& pool_;
  std::unique_ptr<ChunkedStream> stream_;
  std::unique_ptr<Encoding> encoding_;
  uint32_t remaining_{0};
  const MetricsLogger& logger_;
};

} // namespace facebook::alpha
