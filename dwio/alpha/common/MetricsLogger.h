// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <folly/json/dynamic.h>

namespace facebook::alpha {

struct StripeLoadMetrics {
  uint32_t stripeIndex;
  uint32_t rowsInStripe;
  uint32_t streamCount{0};
  uint32_t totalStreamSize{0};
  // TODO: add IO sizes.

  // TODO: add encoding summary

  size_t cpuUsec;
  size_t wallTimeUsec;

  folly::dynamic serialize() const;
};

// Might be good to capture via some kind of run stats struct.
// We can then adapt the run stats to file writer run stats.
struct StripeFlushMetrics {
  // Stripe shape summary.
  uint64_t inputSize;
  uint64_t rowCount;
  uint64_t stripeSize;

  // We would add flush policy states here when wired up in the future.
  // uint64_t inputChunkSize_;

  // TODO: add some type of encoding summary in a follow-up diff.

  // Memory footprint
  uint64_t trackedMemory;
  // uint64_t residentMemory;

  // Perf stats.
  uint64_t flushCpuUsec;
  uint64_t flushWallTimeUsec;
  // Add IOStatistics when we have finished WS api consolidations.

  folly::dynamic serialize() const;
};

struct FileCloseMetrics {
  uint64_t rowCount;
  uint64_t inputSize;
  uint64_t stripeCount;
  uint64_t fileSize;

  // Perf stats.
  uint64_t totalFlushCpuUsec;
  uint64_t totalFlushWallTimeUsec;
  // Add IOStatistics when we have finished WS api consolidations.

  folly::dynamic serialize() const;
};

class MetricsLogger {
 public:
  constexpr static std::string_view kStripeLoadOperation{"STRIPE_LOAD"};
  constexpr static std::string_view kStripeFlushOperation{"STRIPE_FLUSH"};
  constexpr static std::string_view kFileCloseOperation{"FILE_CLOSE"};
  constexpr static std::string_view kZstrong{"ZSTRONG"};

  virtual ~MetricsLogger() = default;

  virtual void logException(
      std::string_view /* operation */,
      const std::string& /* errorMessage */) const {}

  virtual void logStripeLoad(const StripeLoadMetrics& /* metrics */) const {}
  virtual void logStripeFlush(const StripeFlushMetrics& /* metrics */) const {}
  virtual void logFileClose(const FileCloseMetrics& /* metrics */) const {}
  virtual void logZstrongContext(const std::string&) const {}
};

class LoggingScope {
 public:
  explicit LoggingScope(const MetricsLogger& logger) {
    Context::get().logger = &logger;
  }

  ~LoggingScope() {
    Context::get().logger = nullptr;
  }

  static const MetricsLogger* getLogger() {
    return Context::get().logger;
  }

 private:
  struct Context {
    const MetricsLogger* logger;

    static Context& get();
  };
};

} // namespace facebook::alpha
