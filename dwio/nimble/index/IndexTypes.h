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

#include <cstdint>

namespace facebook::nimble::index {

/// Represents the location of a chunk within a stream.
/// Both offsets are relative to the containing unit:
/// - For chunk index: relative to the stripe (stream start offset and stripe
///   start row).
/// - For cluster index: relative to the index partition (key data blob offset
///   and partition start row).
struct ChunkLocation {
  uint32_t chunkOffset;
  uint32_t chunkSize;
  uint32_t rowOffset;

  ChunkLocation(uint32_t _chunkOffset, uint32_t _chunkSize, uint32_t _rowOffset)
      : chunkOffset(_chunkOffset),
        chunkSize(_chunkSize),
        rowOffset(_rowOffset) {}
};

} // namespace facebook::nimble::index
