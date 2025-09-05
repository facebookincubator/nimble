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

FlushDecision RawStripeSizeFlushPolicy::shouldFlush(
    const StripeProgress& stripeProgress) {
  return stripeProgress.stripeRawSize >= rawStripeSize_ ? FlushDecision::Stripe
                                                        : FlushDecision::None;
}

FlushDecision ChunkFlushPolicy::shouldFlush(
    const StripeProgress& stripeProgress) {
  const auto relieveMemoryPressure = [&]() {
    const uint64_t inMemoryByteSize =
        stripeProgress.stripeRawSize + stripeProgress.stripeEncodedSize;
    // Determine when we need to start relieving memory pressure with chunking
    if (lastFlushDecision_ == FlushDecision::None &&
        inMemoryByteSize > config_.writerMaxMemoryBytes) {
      return FlushDecision::Chunk;
    }

    // Try chunking when possible to relieve memory pressure
    else if (
        lastFlushDecision_ == FlushDecision::Chunk &&
        stripeProgress.successfullyChunked &&
        inMemoryByteSize > config_.writerMinMemoryBytes) {
      return FlushDecision::Chunk;
    }

    // When chunking is unable to relieve memory pressure, we flush
    else if (
        lastFlushDecision_ == FlushDecision::Chunk &&
        !stripeProgress.successfullyChunked &&
        inMemoryByteSize > config_.writerMinMemoryBytes) {
      return FlushDecision::Stripe;
    }

    return FlushDecision::None;
  };

  lastFlushDecision_ = relieveMemoryPressure();
  if (lastFlushDecision_ != FlushDecision::None) {
    return lastFlushDecision_;
  }

  // When no writer memory pressure, optimize for storage stripe size
  const auto optimizeStorageSize = [&]() {
    double compressionRatio = config_.compressionRatioFactor;
    // Use historical compression ratio as a heuristic when available.
    if (stripeProgress.stripeEncodedSize > 0) {
      compressionRatio *= stripeProgress.stripeEncodedRawSize /
          stripeProgress.stripeEncodedSize;
    }
    double expectedEncodedStripeSize = stripeProgress.stripeEncodedSize +
        compressionRatio * stripeProgress.stripeRawSize;
    return expectedEncodedStripeSize >= config_.targetStripeSizeBytes
        ? FlushDecision::Stripe
        : FlushDecision::None;
  };
  lastFlushDecision_ = optimizeStorageSize();

  return lastFlushDecision_;
}

void ChunkFlushPolicy::reset() {
  lastFlushDecision_ = FlushDecision::None;
}

} // namespace facebook::nimble
