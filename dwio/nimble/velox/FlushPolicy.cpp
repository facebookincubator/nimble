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

// Relieve memory pressure with chunking.
FlushDecision ChunkFlushPolicy::shouldChunk(
    const StripeProgress& stripeProgress) {
  const auto relieveMemoryPressure = [&]() {
    const uint64_t inMemoryByteSize =
        stripeProgress.stripeRawSize + stripeProgress.stripeEncodedSize;
    if (lastFlushDecision_ == FlushDecision::None &&
        inMemoryByteSize > config_.writerMaxMemoryBytes) {
      return FlushDecision::Chunk;
    }

    // Determine if chunking was successful.
    bool successfullyChunked = (lastFlushDecision_ == FlushDecision::Chunk) &&
        (lastStripeRawSize_ > stripeProgress.stripeRawSize);

    // Try chunking when possible to relieve memory pressure.
    if (successfullyChunked &&
        inMemoryByteSize > config_.writerMinMemoryBytes) {
      return FlushDecision::Chunk;
    }

    // When chunking is unable to relieve memory pressure, we flush stripe.
    if (lastFlushDecision_ == FlushDecision::Chunk && !successfullyChunked &&
        inMemoryByteSize > config_.writerMinMemoryBytes) {
      return FlushDecision::Stripe;
    }

    return FlushDecision::None;
  };

  lastFlushDecision_ = relieveMemoryPressure();
  lastStripeRawSize_ = stripeProgress.stripeRawSize;
  return lastFlushDecision_;
}

// Optimize for expected storage stripe size.
FlushDecision ChunkFlushPolicy::shouldFlush(
    const StripeProgress& stripeProgress) {
  // Use historical compression ratio as a heuristic when available.
  double historicalCompressionRatio = 1.0;
  if (stripeProgress.stripeEncodedSize > 0) {
    historicalCompressionRatio =
        static_cast<double>(stripeProgress.stripeEncodedRawSize) /
        stripeProgress.stripeEncodedSize;
  }
  double expectedEncodedStripeSize = stripeProgress.stripeEncodedSize +
      stripeProgress.stripeRawSize /
          (historicalCompressionRatio *
           std::max(config_.compressionRatio, 1.0));
  return expectedEncodedStripeSize >= config_.targetStripeSizeBytes
      ? FlushDecision::Stripe
      : FlushDecision::None;
}

} // namespace facebook::nimble
