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
/// Both offsets are relative:
/// - streamOffset: relative to the stream start offset within the stripe
/// - rowOffset: relative to the stripe start row id
struct ChunkLocation {
  uint32_t streamOffset;
  uint32_t rowOffset;

  ChunkLocation(uint32_t _streamOffset, uint32_t _rowOffset)
      : streamOffset(_streamOffset), rowOffset(_rowOffset) {}
};

} // namespace facebook::nimble::index
