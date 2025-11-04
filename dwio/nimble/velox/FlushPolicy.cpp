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
#include "dwio/nimble/velox/FlushPolicy.h"

namespace facebook::nimble {
// Relieve memory pressure with chunking.
bool ChunkFlushPolicy::shouldChunk(const StripeProgress& stripeProgress) {
  const uint64_t inMemoryBytes =
      stripeProgress.stripeRawSize + stripeProgress.stripeEncodedSize;
  const auto writerMemoryThreshold = (lastChunkDecision_ == false)
      ? config_.writerMemoryHighThresholdBytes
      : config_.writerMemoryLowThresholdBytes;
  lastChunkDecision_ = inMemoryBytes > writerMemoryThreshold;
  return lastChunkDecision_;
}

// Optimize for expected storage stripe size.
bool ChunkFlushPolicy::shouldFlush(const StripeProgress& stripeProgress) {
  // When chunking is unable to relieve memory pressure, we flush stripe.
  if (stripeProgress.stripeRawSize + stripeProgress.stripeEncodedSize >
      config_.writerMemoryHighThresholdBytes) {
    return true;
  }

  double compressionFactor = config_.estimatedCompressionFactor;
  // Use historical compression ratio as a heuristic when available.
  if (stripeProgress.stripeEncodedSize > 0) {
    compressionFactor =
        static_cast<double>(stripeProgress.stripeEncodedLogicalSize) /
        stripeProgress.stripeEncodedSize;
  }
  double expectedEncodedStripeSize = stripeProgress.stripeEncodedSize +
      stripeProgress.stripeRawSize / std::max(compressionFactor, 1.0);
  return (expectedEncodedStripeSize >= config_.targetStripeSizeBytes);
}

} // namespace facebook::nimble
