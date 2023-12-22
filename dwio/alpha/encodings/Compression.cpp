// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <folly/Chrono.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <zstd.h>
#include <zstd_errors.h>
#include <optional>

#include "data_compression/experimental/zstrong_compressors/xldb/lib/xldb_compressor.h"
#include "dwio/alpha/common/EncodingPrimitives.h"
#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/common/MetricsLogger.h"
#include "dwio/alpha/encodings/Compression.h"
#include "folly/Likely.h"
#include "folly/compression/Compression.h"

namespace facebook::alpha {

// Temporary flags to enable testing.
DEFINE_int32(
    alpha_zstrong_override_compression_level,
    -1,
    "Override the compression level passed to Zstrong.");
DEFINE_int32(
    alpha_zstrong_override_decompression_level,
    -1,
    "Override the decompression level passed to Zstrong.");

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

/* static */ CompressionResult Compression::compress(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    DataType dataType,
    int bitWidth,
    const CompressionPolicy& compressionPolicy) {
  auto compression = compressionPolicy.compression();
  switch (compression.compressionType) {
    case CompressionType::Zstd: {
      Vector<char> buffer{&memoryPool, data.size() + sizeof(uint32_t)};
      auto pos = buffer.data();
      encoding::writeUint32(data.size(), pos);
      auto ret = ZSTD_compress(
          pos,
          data.size(),
          data.data(),
          data.size(),
          compression.parameters.zstd.compressionLevel);
      if (ZSTD_isError(ret)) {
        ALPHA_ASSERT(
            ZSTD_getErrorCode(ret) ==
                ZSTD_ErrorCode::ZSTD_error_dstSize_tooSmall,
            fmt::format(
                "Error while compressing data: {}", ZSTD_getErrorName(ret)));
        return {
            .compressionType = CompressionType::Uncompressed,
            .buffer = std::nullopt,
        };
      }

      buffer.resize(ret + sizeof(uint32_t));
      return {
          .compressionType = CompressionType::Zstd,
          .buffer = std::move(buffer),
      };
    }
    case CompressionType::Zstrong: {
      const auto& zstrongParams = compression.parameters.zstrong;
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
          FLAGS_alpha_zstrong_override_compression_level > 0
          ? FLAGS_alpha_zstrong_override_compression_level
          : zstrongParams.compressionLevel;
      const int32_t decompressionLevel =
          FLAGS_alpha_zstrong_override_decompression_level > 0
          ? FLAGS_alpha_zstrong_override_decompression_level
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
    default:
      ALPHA_NOT_SUPPORTED(fmt::format(
          "Unsupported compression type: {}.",
          toString(compression.compressionType)));
  }
}

/* static */ Vector<char> Compression::uncompress(
    velox::memory::MemoryPool& memoryPool,
    CompressionType compressionType,
    std::string_view data) {
  switch (compressionType) {
    case CompressionType::Uncompressed: {
      ALPHA_UNREACHABLE(
          "uncompress() shouldn't be called on uncompressed buffer.");
    }
    case CompressionType::Zstd: {
      auto pos = data.data();
      const uint32_t uncompressedSize = encoding::readUint32(pos);
      Vector<char> buffer{&memoryPool, uncompressedSize};
      auto ret = ZSTD_decompress(
          buffer.data(), buffer.size(), pos, data.size() - sizeof(uint32_t));
      ALPHA_CHECK(
          !ZSTD_isError(ret),
          fmt::format("Error uncompressing data: {}", ZSTD_getErrorName(ret)));
      return buffer;
    }
    case CompressionType::Zstrong: {
      auto compressor = zstrong::compressors::xldb::Compressor(
          std::make_unique<ZstrongLogger>());
      const auto uncompressedSize =
          compressor.decompressedSize(data.data(), data.length());
      Vector<char> buffer{&memoryPool, uncompressedSize};
      const auto actualSize = compressor.decompress(
          buffer.data(), buffer.size(), data.data(), data.size());
      ALPHA_CHECK(
          actualSize == uncompressedSize,
          fmt::format(
              "Corrupted stream. Decompressed object does not match expected size. Expected: {}, Actual: {}.",
              uncompressedSize,
              actualSize));
      return buffer;
    }
    default:
      ALPHA_NOT_SUPPORTED(fmt::format(
          "Unsupported decompression type: {}.", toString(compressionType)));
  }
}

} // namespace facebook::alpha
