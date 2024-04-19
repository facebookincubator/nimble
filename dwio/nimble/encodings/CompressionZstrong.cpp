// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/nimble/encodings/CompressionInternal.h"

#include <folly/Chrono.h>
#include <folly/Likely.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <optional>
#include "data_compression/experimental/zstrong_compressors/xldb/lib/xldb_compressor.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/MetricsLogger.h"

// Temporary flags to enable testing.
DEFINE_int32(
    nimble_zstrong_override_compression_level,
    -1,
    "Override the compression level passed to Zstrong.");
DEFINE_int32(
    nimble_zstrong_override_decompression_level,
    -1,
    "Override the decompression level passed to Zstrong.");

namespace facebook::nimble {
namespace {

// Rate limit the error logs because they can explode in volume.
// We don't rate limit the version logs in this layer because it has its own
// logic for rate limiting.
class ErrorLogRateLimiter {
  using Clock = folly::chrono::coarse_steady_clock;
  static Clock::duration constexpr kDefaultResetInterval =
      std::chrono::hours(1);
  static constexpr long kDefaultLogQuota = 100;

 public:
  bool shouldLog() {
    const long lastResetTime = lastResetTime_.load(std::memory_order_acquire);
    const long currentTime = Clock::now().time_since_epoch().count();

    if (currentTime - lastResetTime >= kDefaultResetInterval.count()) {
      LOG(WARNING) << "Resetting log quota";
      logCount_ = 0;
      lastResetTime_.store(currentTime, std::memory_order_release);
    }
    return ++logCount_ <= kDefaultLogQuota;
  }

 private:
  std::atomic_long logCount_{0};
  std::atomic<Clock::rep> lastResetTime_{0};
};

static bool shouldLogError() {
  static ErrorLogRateLimiter limiter;
  return limiter.shouldLog();
}

class ZstrongLogger : public zstrong::compressors::xldb::XLDBLogger {
  void log(std::string_view msg) {
    try {
      auto logger = LoggingScope::getLogger();
      // It's possible for logger to be nullptr in unit test context.
      if (LIKELY(logger != nullptr)) {
        logger->logZstrongContext(std::string(msg));
      }
    } catch (...) {
      LOG(WARNING) << "Failed to log Zstrong context.";
    }
  }

 public:
  void logCompressionVersion(int formatVersion) override {
    log(generateJSONLog(
        LogType::VERSION_LOG, Operation::COMPRESS, formatVersion));
  }

  void logDecompressionVersion(int formatVersion) override {
    log(generateJSONLog(
        LogType::VERSION_LOG, Operation::DECOMPRESS, formatVersion));
  }

  void logCompressionError(
      std::string_view msg,
      std::optional<int> formatVersion = std::nullopt) override {
    if (shouldLogError()) {
      LOG(WARNING) << fmt::format("Logging zstrong compression error: {}", msg);
      log(generateJSONLog(
          LogType::ERROR, Operation::COMPRESS, formatVersion, msg));
    }
  }

  void logDecompressionError(
      std::string_view msg,
      std::optional<int> formatVersion = std::nullopt) override {
    if (shouldLogError()) {
      LOG(WARNING) << fmt::format(
          "Logging zstrong decompression error: {}", msg);
      log(generateJSONLog(
          LogType::ERROR, Operation::DECOMPRESS, formatVersion, msg));
    }
  }

  ~ZstrongLogger() override = default;
};

} // namespace

CompressionResult compressZstrong(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    DataType dataType,
    int bitWidth,
    const CompressionPolicy& compressionPolicy,
    const ZstrongCompressionParameters& zstrongParams) {
  zstrong::compressors::xldb::DataType dt =
      zstrong::compressors::xldb::DataType::Unknown;
  switch (dataType) {
    case DataType::Uint64:
    case DataType::Int64:
      bitWidth = 64;
      dt = zstrong::compressors::xldb::DataType::U64;
      break;
    case DataType::Uint32:
    case DataType::Int32:
      bitWidth = 32;
      dt = zstrong::compressors::xldb::DataType::Int;
      break;
    case DataType::Uint16:
    case DataType::Int16:
      bitWidth = 16;
      dt = zstrong::compressors::xldb::DataType::Int;
      break;
    case DataType::Uint8:
    case DataType::Int8:
      bitWidth = 8;
      dt = zstrong::compressors::xldb::DataType::Int;
      break;
    // Used for bit packed data.
    case DataType::Undefined: {
      if (zstrongParams.useVariableBitWidthCompressor) {
        dt = zstrong::compressors::xldb::DataType::Int;
      }
      break;
    }
    case DataType::Float:
      dt = zstrong::compressors::xldb::DataType::F32;
      break;
    default:
      // TODO: support other datatypes
      break;
  }
  zstrong::compressors::xldb::Compressor compressor(
      std::make_unique<ZstrongLogger>());
  const int32_t compressionLevel =
      FLAGS_nimble_zstrong_override_compression_level > 0
      ? FLAGS_nimble_zstrong_override_compression_level
      : zstrongParams.compressionLevel;
  const int32_t decompressionLevel =
      FLAGS_nimble_zstrong_override_decompression_level > 0
      ? FLAGS_nimble_zstrong_override_decompression_level
      : zstrongParams.decompressionLevel;
  zstrong::compressors::xldb::CompressParams params = {
      .level = zstrong::compressors::xldb::Level(
          compressionLevel, decompressionLevel),
      .datatype = dt,
      .integerBitWidth = folly::to<size_t>(bitWidth),
      .bruteforce = false,
  };

  const auto uncompressedSize = data.size();
  const auto bound = compressor.compressBound(uncompressedSize);
  Vector<char> buffer{&memoryPool, bound};
  size_t compressedSize = 0;
  compressedSize = compressor.compress(
      buffer.data(), buffer.size(), data.data(), data.size(), params);

  if (!compressionPolicy.shouldAccept(
          CompressionType::Zstrong, uncompressedSize, compressedSize) ||
      compressedSize >= bound) {
    return {
        .compressionType = CompressionType::Uncompressed,
        .buffer = std::nullopt,
    };
  }

  buffer.resize(compressedSize);
  return {
      .compressionType = CompressionType::Zstrong,
      .buffer = std::move(buffer),
  };
}

Vector<char> uncompressZstrong(
    velox::memory::MemoryPool& memoryPool,
    CompressionType compressionType,
    std::string_view data) {
  auto compressor =
      zstrong::compressors::xldb::Compressor(std::make_unique<ZstrongLogger>());
  const auto uncompressedSize =
      compressor.decompressedSize(data.data(), data.length());
  Vector<char> buffer{&memoryPool, uncompressedSize};
  const auto actualSize = compressor.decompress(
      buffer.data(), buffer.size(), data.data(), data.size());
  NIMBLE_CHECK(
      actualSize == uncompressedSize,
      fmt::format(
          "Corrupted stream. Decompressed object does not match expected size. Expected: {}, Actual: {}.",
          uncompressedSize,
          actualSize));
  return buffer;
}

} // namespace facebook::nimble
