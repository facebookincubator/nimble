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

#include <cstdint>
#include <functional>
#include "dwio/nimble/common/Bits.h"

namespace facebook::nimble {

class Encoding;

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

  virtual const Encoding* encoding() const = 0;
};

} // namespace facebook::nimble
