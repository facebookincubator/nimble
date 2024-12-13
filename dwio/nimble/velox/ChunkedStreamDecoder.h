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

  void ensureLoaded();

  const Encoding* encoding() const override {
    return encoding_.get();
  }

 private:
  velox::memory::MemoryPool& pool_;
  std::unique_ptr<ChunkedStream> stream_;
  std::unique_ptr<Encoding> encoding_;
  uint32_t remaining_{0};
  const MetricsLogger& logger_;
  std::vector<Vector<char>> stringBuffers_;
};

} // namespace facebook::nimble
