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
#include <functional>

namespace facebook::nimble {

// TODO: Set default values for these parameters.
struct ChunkFlushPolicyConfig {
  // Threshold to trigger chunking to relieve memory pressure
  const uint64_t writerMaxMemoryBytes;
  // Threshold below which chunking stops and stripe size optimization resumes
  const uint64_t writerMinMemoryBytes;
  // Target size for encoded stripes
  const uint64_t targetStripeSizeBytes;
  // Expected ratio of raw to encoded data
  const double compressionRatio{1.0};
};

enum class FlushDecision : uint8_t {
  None = 0,
  Stripe = 1,
  Chunk = 2,
};

struct StripeProgress {
  // Size of the stripe data when it's fully decompressed and decoded
  const uint64_t stripeRawSize;
  // Size of the stripe after buffered data is encoded and optionally compressed
  const uint64_t stripeEncodedSize;
  // Previous raw size of the now encoded stripe data
  const uint64_t stripeEncodedRawSize;
};

class FlushPolicy {
 public:
  virtual ~FlushPolicy() = default;
  virtual FlushDecision shouldFlush(const StripeProgress& stripeProgress) = 0;
  virtual FlushDecision shouldChunk(const StripeProgress&) {
    return FlushDecision::None;
  }
};

class StripeRawSizeFlushPolicy final : public FlushPolicy {
 public:
  explicit StripeRawSizeFlushPolicy(uint64_t stripeRawSize)
      : stripeRawSize_{stripeRawSize} {}

  FlushDecision shouldFlush(const StripeProgress& stripeProgress) override;

 private:
  const uint64_t stripeRawSize_;
};

class LambdaFlushPolicy : public FlushPolicy {
 public:
  explicit LambdaFlushPolicy(
      std::function<FlushDecision(const StripeProgress&)> lambda)
      : lambda_{std::move(lambda)} {}

  FlushDecision shouldFlush(const StripeProgress& stripeProgress) override {
    return lambda_(stripeProgress);
  }

 private:
  std::function<FlushDecision(const StripeProgress&)> lambda_;
};

class ChunkFlushPolicy : public FlushPolicy {
 public:
  explicit ChunkFlushPolicy(const ChunkFlushPolicyConfig& config)
      : config_{config} {}

  // Optimize for expected storage stripe size.
  FlushDecision shouldFlush(const StripeProgress& stripeProgress) override;

  // Relieve memory pressure with chunking.
  FlushDecision shouldChunk(const StripeProgress& stripeProgress) override;

 private:
  ChunkFlushPolicyConfig config_;
  FlushDecision lastFlushDecision_{FlushDecision::None};
  uint64_t lastStripeRawSize_{0};
};

} // namespace facebook::nimble
