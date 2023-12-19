// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <folly/dynamic.h>

#include "dwio/alpha/velox/VeloxReader.h"
#include "dwio/alpha/velox/VeloxWriterOptions.h"
#include "dwio/catalog/fbhive/HiveCatalog.h"
#include "dwio/common/request/AccessDescriptor.h"
#include "folly/stats/StreamingStats.h"
#include "velox/dwio/common/Options.h"

namespace facebook::alpha::tools {

// FeatureProjectionConfig for read feature projection performance testing.
// Caller is in charge of generating the feature ordering with all the relevant
// features. When there's no prioritized features (such as for feature
// reordering) we expect all features to be passed in.
struct FeatureProjectionConfig {
  std::unordered_map<std::string, std::vector<int64_t>> featureOrdering;
  float percentage;

  folly::F14FastMap<std::string, alpha::FeatureSelection> toFeatureSelector()
      const {
    folly::F14FastMap<std::string, alpha::FeatureSelection> featureSelector;
    for (const auto& column : featureOrdering) {
      size_t featureCount = std::ceil(column.second.size() * percentage);
      std::vector<std::string> featureIds{};
      featureIds.reserve(featureCount);
      for (size_t i = 0; i < featureCount; ++i) {
        featureIds.emplace_back(folly::to<std::string>(column.second[i]));
      }

      featureSelector.emplace(
          column.first,
          alpha::FeatureSelection{
              .features = featureIds, .mode = alpha::SelectionMode::Include});
    }
    return featureSelector;
  }
};

struct WriteResult {
  size_t origFileSize;
  folly::StreamingStats<time_t> cpuStats;
  folly::StreamingStats<time_t> flushCpuStats;
  folly::StreamingStats<time_t> flushWallTimeStats;
  uint64_t fileSize;
  uint32_t stripeCount;
  uint64_t rowCount;
  std::unordered_map<std::string, std::vector<int64_t>> columnFeatures;
  uint64_t inputBufferReallocCount;
  uint64_t inputBufferReallocItemCount;
  int64_t writePeakMemory;
  int64_t writeAvgMemory;

  folly::dynamic serialize() const {
    folly::dynamic json = folly::dynamic::object;
    json["fileSizeBytes"] = fileSize;
    json["stripeCount"] = stripeCount;
    json["rowCount"] = rowCount;
    json["cpuAvgNanos"] = cpuStats.count() ? cpuStats.mean() : 0;
    json["cpuStdDevNanos"] =
        cpuStats.count() > 1 ? cpuStats.sampleStandardDeviation() : 0;
    json["flushCpuAvgNanos"] = flushCpuStats.count() ? flushCpuStats.mean() : 0;
    json["flushCpuStdDevNanos"] =
        flushCpuStats.count() > 1 ? flushCpuStats.sampleStandardDeviation() : 0;
    json["flushWallTimeAvgNanos"] =
        flushWallTimeStats.count() ? flushWallTimeStats.mean() : 0;
    json["flushWallTimeStdDevNanos"] = flushWallTimeStats.count() > 1
        ? flushWallTimeStats.sampleStandardDeviation()
        : 0;
    json["inputBufferReallocCount"] = inputBufferReallocCount;
    json["inputBufferReallocItemCount"] = inputBufferReallocItemCount;
    json["peakMemoryBytes"] = writePeakMemory;
    json["avgMemoryBytes"] = writeAvgMemory;
    return json;
  }
};

struct ReadResult {
  folly::StreamingStats<time_t> cpuStats;
  int64_t readPeakMemory;
  int64_t readAvgMemory;

  folly::dynamic serialize() const {
    folly::dynamic json = folly::dynamic::object;
    json["cpuAvgNanos"] = cpuStats.count() ? cpuStats.mean() : 0;
    json["cpuStdDevNanos"] =
        cpuStats.count() > 1 ? cpuStats.sampleStandardDeviation() : 0;
    json["peakMemoryBytes"] = readPeakMemory;
    json["avgMemoryBytes"] = readAvgMemory;
    return json;
  }
};

struct PerfSummary {
  size_t origFileSize;
  WriteResult writeResult;
  std::unordered_map<float, ReadResult> readResults;
  // TODO: add read bytes/IO stats after consolidation of IO layers.
  // TODO: add encoding histogram
  // TODO: compression stats

  folly::dynamic serialize() const {
    folly::dynamic json = folly::dynamic::object;
    json["writeResult"] = writeResult.serialize();
    folly::dynamic readResultsJson = folly::dynamic::object;
    for (const auto& readResult : readResults) {
      readResultsJson[folly::to<std::string>(readResult.first)] =
          readResult.second.serialize();
    }
    json["readResults"] = readResultsJson;
    return json;
  }
};

class PerfTuningUtils {
 public:
  static PerfSummary evaluatePerf(
      const std::string& inputPath,
      velox::dwio::common::FileFormat inputFileFormat,
      const dwio::common::request::AccessDescriptor& accessDescriptor,
      const std::string& outPath,
      const std::map<std::string, std::string>& serdeParams,
      std::function<void(alpha::VeloxWriterOptions&)> optionOverrides,
      size_t writeIters,
      size_t readIters,
      size_t featureShuffleSeed = time(nullptr),
      size_t readBatchSize = 1000,
      const Apache::Hadoop::Hive::FeatureOrdering& featureOrdering =
          Apache::Hadoop::Hive::FeatureOrdering{});

 private:
  // Takes an alpha file and rewrites it.
  // TODO: Maybe we should move the whole thing to dwio/tools/ so that we could
  // use dwio/api libs. NOTE: we should always take a serde map and have input
  // file only provide content instead of configuration hint to be more robust.
  static WriteResult rewriteFile(
      const std::string& inputPath,
      velox::dwio::common::FileFormat inputFileFormat,
      const dwio::common::request::AccessDescriptor& accessDescriptor,
      const std::string& outPath,
      const std::map<std::string, std::string>& serdeParams,
      std::function<void(alpha::VeloxWriterOptions&)> optionOverrides,
      size_t iters);
  static ReadResult readFile(
      const std::string& inputPath,
      const dwio::common::request::AccessDescriptor& accessDescriptor,
      const FeatureProjectionConfig& featureProjectionConfig,
      size_t batchSize,
      size_t iters);
};

} // namespace facebook::alpha::tools
