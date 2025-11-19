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

// TODO: Set default values for these parameters based on DISCO experiments.
// Use abitrary values for now.
struct ChunkFlushPolicyConfig {
  // Threshold to trigger chunking to relieve memory pressure
  const uint64_t writerMemoryHighThresholdBytes{200 * 1024L * 1024L};
  // Threshold below which chunking stops and stripe size optimization resumes
  const uint64_t writerMemoryLowThresholdBytes{100 * 1024L * 1024L};
  // Target size for encoded stripes
  const uint64_t targetStripeSizeBytes{100 * 1024L * 1024L};
  // Expected ratio of raw to encoded data
  const double estimatedCompressionFactor{3.7};
};

struct StripeProgress {
  // Size of the stripe data when it's fully decompressed and decoded
  const uint64_t stripeRawSize;
  // Size of the stripe after buffered data is encoded and optionally compressed
  const uint64_t stripeEncodedSize;
  // Logical size of the now encoded stripe data
  const uint64_t stripeEncodedLogicalSize;
};

class FlushPolicy {
 public:
  virtual ~FlushPolicy() = default;
  virtual bool shouldFlush(const StripeProgress& stripeProgress) = 0;
  virtual bool shouldChunk(const StripeProgress& stripeProgress) = 0;
};

class StripeRawSizeFlushPolicy final : public FlushPolicy {
 public:
  explicit StripeRawSizeFlushPolicy(uint64_t stripeRawSize)
      : stripeRawSize_{stripeRawSize} {}

  bool shouldFlush(const StripeProgress& stripeProgress) override {
    return stripeProgress.stripeRawSize >= stripeRawSize_;
  }

  bool shouldChunk(const StripeProgress&) override {
    return false;
  }

 private:
  const uint64_t stripeRawSize_;
};

class LambdaFlushPolicy : public FlushPolicy {
 public:
  explicit LambdaFlushPolicy(
      std::function<bool(const StripeProgress&)> flushLambda =
          [](const StripeProgress&) { return false; },
      std::function<bool(const StripeProgress&)> chunkLambda =
          [](const StripeProgress&) { return false; })
      : flushLambda_{std::move(flushLambda)},
        chunkLambda_{std::move(chunkLambda)} {}

  bool shouldFlush(const StripeProgress& stripeProgress) override {
    return flushLambda_(stripeProgress);
  }

  bool shouldChunk(const StripeProgress& stripeProgress) override {
    return chunkLambda_(stripeProgress);
  }

 private:
  std::function<bool(const StripeProgress&)> flushLambda_;
  std::function<bool(const StripeProgress&)> chunkLambda_;
};

class ChunkFlushPolicy : public FlushPolicy {
 public:
  explicit ChunkFlushPolicy(ChunkFlushPolicyConfig config)
      : config_{std::move(config)}, lastChunkDecision_{false} {}

  // Optimize for expected storage stripe size.
  bool shouldFlush(const StripeProgress& stripeProgress) override;

  // Relieve memory pressure with chunking.
  bool shouldChunk(const StripeProgress& stripeProgress) override;

  const ChunkFlushPolicyConfig& getConfig() const {
    return config_;
  }

 private:
  const ChunkFlushPolicyConfig config_;
  bool lastChunkDecision_;
};

} // namespace facebook::nimble
