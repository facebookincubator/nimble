// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <functional>

namespace facebook::nimble {

struct StripeProgress {
  // Size of the stripe data when it's fully decompressed and decoded
  const uint64_t rawStripeSize;
  // Size of the stripe after buffered data is encoded and optionally compressed
  const uint64_t stripeSize;
  // Size of the allocated buffer in the writer
  const uint64_t bufferSize;
};

enum class FlushDecision : uint8_t {
  None = 0,
  Stripe = 1,
  Chunk = 2,
};

class FlushPolicy {
 public:
  virtual ~FlushPolicy() = default;
  virtual FlushDecision shouldFlush(const StripeProgress& stripeProgress) = 0;
  // Required for memory pressure coordination for now. Will remove in the
  // future.
  virtual void onClose() = 0;
};

class RawStripeSizeFlushPolicy final : public FlushPolicy {
 public:
  explicit RawStripeSizeFlushPolicy(uint64_t rawStripeSize)
      : rawStripeSize_{rawStripeSize} {}

  FlushDecision shouldFlush(const StripeProgress& stripeProgress) override;

  void onClose() override;

 private:
  const uint64_t rawStripeSize_;
};

class LambdaFlushPolicy : public FlushPolicy {
 public:
  explicit LambdaFlushPolicy(
      std::function<FlushDecision(const StripeProgress&)> lambda)
      : lambda_{lambda} {}

  FlushDecision shouldFlush(const StripeProgress& stripeProgress) override {
    return lambda_(stripeProgress);
  }

  void onClose() override {
    // No-op
  }

 private:
  std::function<FlushDecision(const StripeProgress&)> lambda_;
};

} // namespace facebook::nimble
