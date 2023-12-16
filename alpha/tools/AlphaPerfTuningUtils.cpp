// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/tools/AlphaPerfTuningUtils.h"

#include <folly/experimental/FunctionScheduler.h>
#include <random>

#include "dwio/alpha/velox/VeloxReader.h"
#include "dwio/alpha/velox/VeloxWriter.h"
#include "dwio/api/AlphaWriterOptionBuilder.h"
#include "dwio/common/filesystem/FileSystem.h"
#include "dwio/utils/BufferedWriteFile.h"
#include "dwio/utils/FileSink.h"
#include "dwio/utils/InputStreamFactory.h"
#include "velox/common/time/CpuWallTimer.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"

using namespace facebook::velox;

constexpr uint32_t kAlphaWriteBufferSize = 72U * 1024 * 1024;

namespace facebook::alpha::tools {

namespace {
class DataAdapter {
 public:
  virtual ~DataAdapter() = default;

  virtual bool next(uint64_t rowCount, velox::VectorPtr& result) = 0;
  virtual const std::shared_ptr<const velox::RowType>& getSchema() = 0;
  virtual size_t getFileSize() const = 0;
};

class DWRFDataAdapter : public DataAdapter {
 public:
  DWRFDataAdapter(
      const std::string& path,
      velox::memory::MemoryPool* memoryPool,
      const dwio::common::request::AccessDescriptor& accessDescriptor)
      : reader_{velox::dwrf::DwrfReader::create(
            std::make_unique<velox::dwio::common::BufferedInput>(
                dwio::utils::InputStreamFactory::create(
                    path,
                    accessDescriptor,
                    /*stats=*/nullptr),
                *memoryPool),
            velox::dwio::common::ReaderOptions{memoryPool})},
        rowReader_{reader_->createDwrfRowReader()} {}

  bool next(uint64_t rowCount, velox::VectorPtr& result) override {
    return rowReader_->next(rowCount, result);
  }

  const std::shared_ptr<const velox::RowType>& getSchema() override {
    return reader_->rowType();
  }

  size_t getFileSize() const override {
    return reader_->getFileLength();
  }

 private:
  std::unique_ptr<velox::dwrf::DwrfReader> reader_;
  std::unique_ptr<velox::dwrf::DwrfRowReader> rowReader_;
};

class AlphaDataAdapter : public DataAdapter {
 public:
  AlphaDataAdapter(
      const std::string& path,
      velox::memory::MemoryPool* memoryPool,
      const dwio::common::request::AccessDescriptor& accessDescriptor)
      : reader_{std::make_unique<alpha::VeloxReader>(
            *memoryPool,
            dwio::file_system::FileSystem::openForRead(
                path,
                accessDescriptor))} {}

  bool next(uint64_t rowCount, velox::VectorPtr& result) override {
    return reader_->next(rowCount, result);
  }

  const std::shared_ptr<const velox::RowType>& getSchema() override {
    return reader_->getType();
  }

  size_t getFileSize() const override {
    return reader_->getTabletView().fileSize();
  }

 private:
  std::unique_ptr<alpha::VeloxReader> reader_;
};

class DataAdapterFactory {
 public:
  static std::unique_ptr<DataAdapter> create(
      const std::string& path,
      velox::dwio::common::FileFormat format,
      velox::memory::MemoryPool* memoryPool,
      const dwio::common::request::AccessDescriptor& accessDescriptor) {
    switch (format) {
      case velox::dwio::common::FileFormat::DWRF:
        return std::make_unique<DWRFDataAdapter>(
            path, memoryPool, accessDescriptor);
      case velox::dwio::common::FileFormat::ALPHA:
        return std::make_unique<AlphaDataAdapter>(
            path, memoryPool, accessDescriptor);
      default:
        ALPHA_UNREACHABLE("Unexpected file format");
    }
  }
};

// Returns empty map if empty feature ordering is supplied.
std::unordered_map<std::string, std::vector<int64_t>> extractFeatureOrdering(
    const std::shared_ptr<const velox::RowType>& type,
    const Apache::Hadoop::Hive::FeatureOrdering& featureOrdering) {
  std::unordered_map<std::string, std::vector<int64_t>> extractedFeatures;
  for (const auto& order : featureOrdering.get_featureOrdering()) {
    const auto& columnId = order.columnId();
    extractedFeatures.emplace(
        type->nameOf(columnId->get_columnIdentifier()),
        order.get_featureOrder());
  }
  return extractedFeatures;
}

// Generate feature list from alpha file schema.
std::unordered_map<std::string, std::vector<int64_t>> generateFeatureList(
    const std::string& path,
    const dwio::common::request::AccessDescriptor& accessDescriptor) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  alpha::VeloxReader reader{
      *pool,
      dwio::file_system::FileSystem::openForRead(path, accessDescriptor),
      /*selector=*/nullptr};
  auto schema = reader.schema();
  auto rowType = schema->asRow();
  std::unordered_map<std::string, std::vector<int64_t>> featuresPerColumn;

  for (size_t i = 0; i < rowType.childrenCount(); ++i) {
    auto type = rowType.childAt(i);
    if (type->kind() == alpha::Kind::FlatMap) {
      auto flatMap = type->asFlatMap();
      auto childrenCount = flatMap.childrenCount();
      auto keyScalarKind = flatMap.keyScalarKind();
      ALPHA_CHECK(
          keyScalarKind != alpha::ScalarKind::Bool &&
              keyScalarKind != alpha::ScalarKind::String &&
              keyScalarKind != alpha::ScalarKind::Binary &&
              keyScalarKind != alpha::ScalarKind::Float &&
              keyScalarKind != alpha::ScalarKind::Double &&
              keyScalarKind != alpha::ScalarKind::Undefined,
          "Unsupported key types in feature projection");
      // Skip placeholder elements in empty flatmaps.
      if (childrenCount == 1 && flatMap.nameAt(0).empty()) {
        continue;
      }
      featuresPerColumn[rowType.nameAt(i)].reserve(childrenCount);
      for (size_t j = 0; j < childrenCount; ++j) {
        featuresPerColumn[rowType.nameAt(i)].emplace_back(
            folly::to<int64_t>(flatMap.nameAt(j)));
      }
    }
  }
  return featuresPerColumn;
}
} // namespace

/* static */ WriteResult PerfTuningUtils::rewriteFile(
    const std::string& inputPath,
    velox::dwio::common::FileFormat inputFileFormat,
    const dwio::common::request::AccessDescriptor& accessDescriptor,
    const std::string& outPath,
    const std::map<std::string, std::string>& serdeParams,
    std::function<void(alpha::VeloxWriterOptions&)> optionOverrides,
    size_t iters) {
  // TODO: batch size should actually approximate koski writer or
  // trabant patterns.
  constexpr uint64_t batchSize = 25;
  auto rootPool = velox::memory::deprecatedDefaultMemoryManager().addRootPool(
      "alpha_perf_tuning_rewriteFile");
  auto readerPool = rootPool->addLeafChild("reader");
  auto writerPool = rootPool->addLeafChild("writer");

  folly::StreamingStats<time_t> cpuStats;
  folly::StreamingStats<time_t> flushCpuStats;
  folly::StreamingStats<time_t> flushWallTimeStats;
  size_t origFileSize = 0;
  uint64_t fileSize;
  uint32_t stripeCount;
  uint64_t rowCount;
  uint64_t inputBufferReallocCount = 0;
  uint64_t inputBufferReallocItemCount = 0;
  folly::StreamingStats<int64_t> memoryPerMs;
  for (size_t i = 0; i < iters; ++i) {
    std::remove(outPath.c_str());
    auto dataAdapter = DataAdapterFactory::create(
        inputPath, inputFileFormat, readerPool.get(), accessDescriptor);
    origFileSize = dataAdapter->getFileSize();
    auto schema = dataAdapter->getSchema();
    auto writerOption = dwio::api::AlphaWriterOptionBuilder{}
                            .withSerdeParams(schema, serdeParams)
                            .build();
    optionOverrides(writerOption);
    folly::FunctionScheduler funcScheduler;
    funcScheduler.addFunction(
        [&]() { memoryPerMs.add(writerPool->currentBytes()); },
        std::chrono::milliseconds(1),
        "Average Memory Aggregator",
        std::chrono::milliseconds(1));
    funcScheduler.start();
    alpha::VeloxWriter writer{
        *rootPool,
        schema,
        std::make_unique<dwio::api::BufferedWriteFile>(
            writerPool,
            kAlphaWriteBufferSize,
            dwio::file_system::FileSystem::openForUnbufferedWrite(
                outPath, accessDescriptor)),
        writerOption};
    velox::CpuWallTiming writerTiming;
    velox::VectorPtr vec;
    while (dataAdapter->next(batchSize, vec)) {
      velox::CpuWallTimer timer{writerTiming};
      writer.write(vec);
    }
    {
      velox::CpuWallTimer timer{writerTiming};
      writer.close();
    }
    funcScheduler.shutdown();
    auto runStats = writer.getRunStats();
    fileSize = runStats.bytesWritten;
    stripeCount = runStats.stripeCount;
    const auto& rowsPerStripe = runStats.rowsPerStripe;
    inputBufferReallocCount = runStats.inputBufferReallocCount;
    inputBufferReallocItemCount = runStats.inputBufferReallocItemCount;
    rowCount = std::accumulate(rowsPerStripe.begin(), rowsPerStripe.end(), 0);
    cpuStats.add(writerTiming.cpuNanos);
    flushCpuStats.add(runStats.flushCpuTimeUsec * 1000);
    flushWallTimeStats.add(runStats.flushWallTimeUsec * 1000);
  }
  return WriteResult{
      .origFileSize = origFileSize,
      .cpuStats = cpuStats,
      .flushCpuStats = flushCpuStats,
      .flushWallTimeStats = flushWallTimeStats,
      .fileSize = fileSize,
      .stripeCount = stripeCount,
      .rowCount = rowCount,
      .columnFeatures = generateFeatureList(outPath, accessDescriptor),
      .inputBufferReallocCount = inputBufferReallocCount,
      .inputBufferReallocItemCount = inputBufferReallocItemCount,
      .writePeakMemory = writerPool->peakBytes(),
      .writeAvgMemory = std::llround(memoryPerMs.mean())};
}

/* static */ ReadResult PerfTuningUtils::readFile(
    const std::string& inputPath,
    const dwio::common::request::AccessDescriptor& accessDescriptor,
    const FeatureProjectionConfig& featureProjectionConfig,
    size_t batchSize,
    size_t iters) {
  auto rootPool = velox::memory::deprecatedDefaultMemoryManager().addRootPool(
      "alpha_perf_tuning_readFile");
  auto pool = rootPool->addLeafChild("reader");
  folly::StreamingStats<time_t> cpuStats;
  folly::StreamingStats<int64_t> memoryPerMs;
  for (size_t i = 0; i < iters; ++i) {
    folly::FunctionScheduler funcScheduler;
    funcScheduler.addFunction(
        [&]() { memoryPerMs.add(pool->currentBytes()); },
        std::chrono::milliseconds(1),
        "Average Memory Aggregator",
        std::chrono::milliseconds(1));
    velox::CpuWallTiming readTiming;
    {
      alpha::VeloxReadParams readParams;
      readParams.flatMapFeatureSelector =
          featureProjectionConfig.toFeatureSelector();
      funcScheduler.start();
      alpha::VeloxReader reader{
          *pool,
          dwio::file_system::FileSystem::openForRead(
              inputPath, accessDescriptor),
          /*selector=*/nullptr,
          readParams};

      velox::VectorPtr vec;
      velox::CpuWallTimer timer{readTiming};
      while (reader.next(batchSize, vec)) {
        // No-op
      }
    }
    funcScheduler.shutdown();
    cpuStats.add(readTiming.cpuNanos);
  }
  return ReadResult{
      .cpuStats = cpuStats,
      .readPeakMemory = pool->peakBytes(),
      .readAvgMemory = std::llround(memoryPerMs.mean())};
}
/* static */ PerfSummary PerfTuningUtils::evaluatePerf(
    const std::string& inputPath,
    velox::dwio::common::FileFormat inputFileFormat,
    const dwio::common::request::AccessDescriptor& accessDescriptor,
    const std::string& outPath,
    const std::map<std::string, std::string>& serdeParams,
    std::function<void(alpha::VeloxWriterOptions&)> optionOverrides,
    size_t writeIters,
    size_t readIters,
    size_t featureShuffleSeed,
    size_t readBatchSize,
    const Apache::Hadoop::Hive::FeatureOrdering& featureOrdering) {
  auto writeResult = rewriteFile(
      inputPath,
      inputFileFormat,
      accessDescriptor,
      outPath,
      serdeParams,
      [&](alpha::VeloxWriterOptions& options) {
        optionOverrides(options);
        auto ordering = featureOrdering.featureOrdering_ref();
        if (!ordering->empty()) {
          std::vector<std::tuple<size_t, std::vector<int64_t>>> target;
          target.reserve(ordering->size());
          for (const auto& column : ordering.value()) {
            std::vector<int64_t> features;
            features.reserve(column.featureOrder_ref()->size());
            std::copy(
                column.featureOrder_ref()->cbegin(),
                column.featureOrder_ref()->cend(),
                std::back_inserter(features));
            target.emplace_back(
                column.columnId_ref()->columnIdentifier_ref().value(),
                std::move(features));
          }

          options.featureReordering.emplace(std::move(target));
        }
      },
      writeIters);

  if (readIters == 0) {
    return PerfSummary{
        .writeResult = std::move(writeResult), .readResults = {}};
  }

  std::unordered_map<std::string, std::vector<int64_t>> columnFeaturesShuffled;
  const auto& columnFeatures = writeResult.columnFeatures;
  LOG(INFO) << "Shuffling column features with random seed: "
            << featureShuffleSeed;
  for (const auto& featuresPerColumn : columnFeatures) {
    std::vector<int64_t> features{
        featuresPerColumn.second.begin(), featuresPerColumn.second.end()};
    std::mt19937 gen;
    gen.seed(featureShuffleSeed);
    std::shuffle(features.begin(), features.end(), gen);
    // TODO: in the case when we have feature reordering config, we should
    // prioritize those feeature ids somehow.
    columnFeaturesShuffled.emplace(
        featuresPerColumn.first, std::move(features));
  }

  std::vector<float> percentages{0.05f, 0.1f, 0.25f, 1.0f};
  std::unordered_map<float, ReadResult> readResults;
  for (const auto percentage : percentages) {
    // TODO: Future shuffling should be biased toward the feature ordering.
    auto readResult = readFile(
        outPath,
        accessDescriptor,
        FeatureProjectionConfig{
            .featureOrdering = columnFeaturesShuffled,
            .percentage = percentage},
        readBatchSize,
        readIters);
    readResults.emplace(percentage, std::move(readResult));
  }
  return PerfSummary{
      .writeResult = std::move(writeResult),
      .readResults = std::move(readResults)};
}

} // namespace facebook::alpha::tools
