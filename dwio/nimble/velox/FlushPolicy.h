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
#include <memory>

namespace facebook::nimble {

// TODO: Set default values for these parameters based on DISCO experiments.
// Use abitrary values for now.
struct ChunkFlushPolicyConfig {
  // Threshold to trigger chunking to relieve memory pressure
  const uint64_t writerMemoryHighThreshold{250 * 1024L * 1024L};
  // Threshold below which chunking stops and stripe size optimization resumes
  const uint64_t writerMemoryLowThreshold{200 * 1024L * 1024L};
  // Target size for encoded stripes
  const uint64_t targetStripeSizeBytes{384 * 1024L * 1024L};
  // Expected ratio of raw to encoded data
  const double compressionRatioFactor{1.0};
};

// TODO: Make sure JNI is not broken by these changes.
enum class FlushDecision : bool {
  None = false,
  Stripe = true,
};

enum class ChunkDecision : bool {
  None = false,
  Chunk = true,
};

struct StripeProgress {
  // Size of the stripe data when it's fully decompressed and decoded
  const uint64_t stripeRawSize;
  // Size of the stripe after buffered data is encoded and optionally compressed
  const uint64_t stripeEncodedSize;
  // Previous logical size of the now encoded stripe data
  const uint64_t stripeEncodedLogicalSize;
};

class FlushPolicy {
 public:
  virtual ~FlushPolicy() = default;
  virtual FlushDecision shouldFlush(const StripeProgress& stripeProgress) = 0;
  virtual ChunkDecision shouldChunk(const StripeProgress&) {
    return ChunkDecision::None;
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
      std::function<FlushDecision(const StripeProgress&)> flushLambda =
          [](const StripeProgress&) { return FlushDecision::None; },
      std::function<ChunkDecision(const StripeProgress&)> chunkLambda =
          [](const StripeProgress&) { return ChunkDecision::None; })
      : flushLambda_{std::move(flushLambda)},
        chunkLambda_{std::move(chunkLambda)} {}

  FlushDecision shouldFlush(const StripeProgress& stripeProgress) override {
    return flushLambda_(stripeProgress);
  }

  ChunkDecision shouldChunk(const StripeProgress& stripeProgress) override {
    return chunkLambda_(stripeProgress);
  }

 private:
  std::function<FlushDecision(const StripeProgress&)> flushLambda_;
  std::function<ChunkDecision(const StripeProgress&)> chunkLambda_;
};

class ChunkFlushPolicy : public FlushPolicy {
 public:
  explicit ChunkFlushPolicy(
      std::shared_ptr<const ChunkFlushPolicyConfig> config)
      : config_{std::move(config)},
        lastChunkDecision_{ChunkDecision::None},
        lastStripeRawSize_{0},
        chunkingFailed_{false} {}

  // Optimize for expected storage stripe size.
  // Does not track state between calls.
  FlushDecision shouldFlush(const StripeProgress& stripeProgress) override;

  // Relieve memory pressure with chunking. Tracks state between calls.
  ChunkDecision shouldChunk(const StripeProgress& stripeProgress) override;

 private:
  std::shared_ptr<const ChunkFlushPolicyConfig> config_;
  ChunkDecision lastChunkDecision_;
  uint64_t lastStripeRawSize_;
  bool chunkingFailed_;
};

} // namespace facebook::nimble
