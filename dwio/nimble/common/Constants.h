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

namespace facebook::nimble {
/// WARNING: These values have been derived experimentally.

/// Attempt to compress a data stream only if the data size is equal or greater
/// than this threshold.
constexpr uint32_t kMetaInternalMinCompressionSize = 40;
constexpr uint32_t kZstdMinCompressionSize = 25;

/// Default options for the ChunkFlushPolicy.
/// Threshold to trigger chunking to relieve memory pressure
constexpr uint64_t kChunkingWriterMemoryHighThreshold = 400 << 20; // 400MB
/// Threshold below which chunking stops.
constexpr uint64_t kChunkingWriterMemoryLowThreshold = 300 << 20; // 300MB
/// Target size for encoded stripes.
constexpr uint64_t kChunkingWriterTargetStripeStorageSize = 100 << 20; // 100MB
/// Expected ratio of raw to encoded data.
constexpr double kChunkingWriterEstimatedCompressionFactor = 3.7;
} // namespace facebook::nimble
