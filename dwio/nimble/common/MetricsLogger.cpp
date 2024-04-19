// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/nimble/common/MetricsLogger.h"

namespace facebook::nimble {

folly::dynamic StripeLoadMetrics::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["stripeIndex"] = stripeIndex;
  obj["rowsInStripe"] = rowsInStripe;
  obj["streamCount"] = streamCount;
  obj["totalStreamSize"] = totalStreamSize;
  return obj;
}

folly::dynamic StripeFlushMetrics::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["inputSize"] = inputSize;
  obj["rowCount"] = rowCount;
  obj["stripeSize"] = stripeSize;
  obj["trackedMemory"] = trackedMemory;
  return obj;
}

folly::dynamic FileCloseMetrics::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["rowCount"] = rowCount;
  obj["inputSize"] = inputSize;
  obj["stripeCount"] = stripeCount;
  obj["fileSize"] = fileSize;
  obj["totalFlushCpuUsec"] = totalFlushCpuUsec;
  obj["totalFlushWallTimeUsec"] = totalFlushWallTimeUsec;
  return obj;
}

LoggingScope::Context& LoggingScope::Context::get() {
  thread_local static Context ctx;
  return ctx;
}

} // namespace facebook::nimble
