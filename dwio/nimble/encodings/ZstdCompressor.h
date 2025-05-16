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

#include "dwio/nimble/encodings/Compression.h"

namespace facebook::nimble {

class ZstdCompressor : public ICompressor {
 public:
  CompressionResult compress(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      DataType dataType,
      int bitWidth,
      const CompressionPolicy& compressionPolicy) override;

  Vector<char> uncompress(
      velox::memory::MemoryPool& memoryPool,
      CompressionType compressionType,
      std::string_view data) override;

  std::optional<size_t> uncompressedSize(std::string_view data) const override;

  CompressionType compressionType() override;
};

} // namespace facebook::nimble
