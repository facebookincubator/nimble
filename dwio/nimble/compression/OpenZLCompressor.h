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

#include <memory>

#include "dwio/nimble/compression/Compression.h"

namespace facebook::nimble {

/// ICompressor backed by custom graphs implemented using OpenZL.
///
/// The OpenZL numeric graphs are built once per instance and shared (read-only)
/// across all compress() calls, so per-call setup is limited to a CCtx that
/// refs the prebuilt graphs. Instances are expected to be long-lived (a single
/// instance is registered in the compressor registry).
class OpenZLCompressor : public ICompressor {
 public:
  OpenZLCompressor();
  ~OpenZLCompressor() override;

  CompressionResult compress(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      DataType dataType,
      int bitWidth,
      const CompressionPolicy& compressionPolicy) override;

  velox::BufferPtr uncompress(
      velox::memory::MemoryPool& pool,
      CompressionType compressionType,
      DataType dataType,
      std::string_view data,
      velox::BufferPool* bufferPool = nullptr) override;

  std::optional<size_t> uncompressedSize(std::string_view data) const override;

  CompressionType compressionType() override;

 private:
  // Holds the prebuilt OpenZL Compressor and its per-DataType graphs. Defined
  // in the .cpp to keep OpenZL headers out of this header.
  struct GraphCache;
  std::unique_ptr<GraphCache> graphCache_;
};

} // namespace facebook::nimble
