// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/nimble/common/MetricsLogger.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/velox/ChunkedStream.h"
#include "dwio/nimble/velox/Decoder.h"

namespace facebook::nimble {

class ChunkedStreamDecoder : public Decoder {
 public:
  ChunkedStreamDecoder(
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
  std::vector<Vector<char>> stringBuffers_;
};

} // namespace facebook::nimble
