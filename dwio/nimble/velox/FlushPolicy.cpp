// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/nimble/velox/FlushPolicy.h"

namespace facebook::nimble {

FlushDecision RawStripeSizeFlushPolicy::shouldFlush(
    const StripeProgress& stripeProgress) {
  return stripeProgress.rawStripeSize >= rawStripeSize_ ? FlushDecision::Stripe
                                                        : FlushDecision::None;
}

void RawStripeSizeFlushPolicy::onClose() {
  // No-op
}

} // namespace facebook::nimble
