// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <functional>
#include "dwio/alpha/common/Bits.h"

namespace facebook::alpha {

class Decoder {
 public:
  virtual ~Decoder() = default;

  virtual uint32_t next(
      uint32_t count,
      void* output,
      std::function<void*()> nulls = nullptr,
      const bits::Bitmap* scatterBitmap = nullptr) = 0;

  virtual void skip(uint32_t count) = 0;

  virtual void reset() = 0;
};

} // namespace facebook::alpha
