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

FlushDecision StripeRawSizeFlushPolicy::shouldFlush(
    const StripeProgress& stripeProgress) {
  return stripeProgress.stripeRawSize >= stripeRawSize_ ? FlushDecision::Stripe
                                                        : FlushDecision::None;
}

// Relieve memory pressure with chunking. Tracks state between calls.
ChunkDecision ChunkFlushPolicy::shouldChunk(
    const StripeProgress& stripeProgress) {
  const auto relieveMemoryPressure = [&]() {
    uint64_t inMemoryByteSize =
        stripeProgress.stripeRawSize + stripeProgress.stripeEncodedSize;
    if (lastChunkDecision_ == ChunkDecision::None) {
      return inMemoryByteSize > config_->writerMemoryHighThreshold
          ? ChunkDecision::Chunk
          : ChunkDecision::None;
    }
    chunkingFailed_ = lastStripeRawSize_ <= stripeProgress.stripeRawSize;
    if (chunkingFailed_ ||
        inMemoryByteSize < config_->writerMemoryLowThreshold) {
      return ChunkDecision::None;
    }
    return ChunkDecision::Chunk;
  };

  lastChunkDecision_ = relieveMemoryPressure();
  lastStripeRawSize_ = stripeProgress.stripeRawSize;
  return lastChunkDecision_;
}

// Optimize for expected storage stripe size.
// Does not track state between calls.
FlushDecision ChunkFlushPolicy::shouldFlush(
    const StripeProgress& stripeProgress) {
  // When chunking is unable to relieve memory pressure, we flush stripe.
  if (chunkingFailed_) {
    return FlushDecision::Stripe;
  }

  // Use historical compression ratio as a heuristic when available.
  double historicalCompressionRatio = 1.0;
  if (stripeProgress.stripeEncodedSize > 0) {
    historicalCompressionRatio =
        static_cast<double>(stripeProgress.stripeEncodedLogicalSize) /
        stripeProgress.stripeEncodedSize;
  }
  double expectedEncodedStripeSize = stripeProgress.stripeEncodedSize +
      stripeProgress.stripeRawSize /
          (historicalCompressionRatio *
           std::max(config_->compressionRatioFactor, 1.0));
  return (expectedEncodedStripeSize >= config_->targetStripeSizeBytes)
      ? FlushDecision::Stripe
      : FlushDecision::None;
}

} // namespace facebook::nimble
