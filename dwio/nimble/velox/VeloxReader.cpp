/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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
#include "dwio/nimble/velox/VeloxReader.h"
#include <chrono>
#include <cstdint>
#include <optional>
#include <vector>
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/velox/ChunkedStreamDecoder.h"
#include "dwio/nimble/velox/MetadataGenerated.h"
#include "dwio/nimble/velox/SchemaReader.h"
#include "dwio/nimble/velox/SchemaSerialization.h"
#include "dwio/nimble/velox/SchemaTypes.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "velox/common/time/CpuWallTimer.h"
#include "velox/dwio/common/OnDemandUnitLoader.h"
#include "velox/dwio/common/UnitLoader.h"
#include "velox/type/Type.h"

namespace facebook::nimble {

namespace {

std::shared_ptr<const Type> loadSchema(const TabletReader& tabletReader) {
  auto section = tabletReader.loadOptionalSection(std::string(kSchemaSection));
  NIMBLE_CHECK(section.has_value(), "Schema not found.");
  return SchemaDeserializer::deserialize(section->content().data());
}

std::map<std::string, std::string> loadMetadata(
    const TabletReader& tabletReader) {
  std::map<std::string, std::string> result;
  auto section =
      tabletReader.loadOptionalSection(std::string(kMetadataSection));

  if (!section.has_value()) {
    return result;
  }

  auto metadata =
      flatbuffers::GetRoot<serialization::Metadata>(section->content().data());
  auto entryCount = metadata->entries()->size();
  for (auto i = 0; i < entryCount; ++i) {
    auto* entry = metadata->entries()->Get(i);
    result.insert({entry->key()->str(), entry->value()->str()});
  }

  return result;
}

std::shared_ptr<const velox::Type> createFlatType(
    const std::vector<std::string>& selectedFeatures,
    const velox::TypePtr& veloxType) {
  NIMBLE_ASSERT(
      !selectedFeatures.empty(),
      "Empty feature selection not allowed for struct encoding.");

  auto& valueType = veloxType->asMap().valueType();
  return velox::ROW(
      std::vector<std::string>(selectedFeatures),
      std::vector<std::shared_ptr<const velox::Type>>(
          selectedFeatures.size(), valueType));
}

class NimbleUnit : public velox::dwio::common::LoadUnit {
 public:
  NimbleUnit(
      uint32_t stripeId,
      const TabletReader& tabletReader,
      std::shared_ptr<const Type> schema,
      const std::vector<uint32_t>& streamIdentifiers)
      : tabletReader_{tabletReader},
        schema_{std::move(schema)},
        streamLabels_{schema_},
        stripeId_{stripeId},
        streamIdentifiers_{streamIdentifiers} {}

  // Perform the IO (read)
  void load() override;

  // Unload the unit to free memory
  void unload() override;

  // Number of rows in the unit
  uint64_t getNumRows() override {
    return tabletReader_.stripeRowCount(stripeId_);
  }

  // Number of bytes that the IO will read
  uint64_t getIoSize() override;

  std::vector<std::unique_ptr<StreamLoader>> extractStreamLoaders() {
    return std::move(streamLoaders_);
  }

  const StripeLoadMetrics& getMetrics() const {
    return metrics_;
  }

 private:
  const TabletReader& tabletReader_;
  std::shared_ptr<const Type> schema_;
  StreamLabels streamLabels_;
  uint32_t stripeId_;
  const std::vector<uint32_t>& streamIdentifiers_;

  // Lazy
  std::optional<uint64_t> ioSize_;

  std::optional<TabletReader::StripeIdentifier> stripeIdentifier_;
  // Will be loaded on load() and moved away in extractStreamLoaders()
  std::vector<std::unique_ptr<StreamLoader>> streamLoaders_;
  StripeLoadMetrics metrics_;
};

void NimbleUnit::load() {
  velox::CpuWallTiming timing{};
  {
    velox::CpuWallTimer timer{timing};
    if (!stripeIdentifier_.has_value()) {
      stripeIdentifier_ = tabletReader_.getStripeIdentifier(stripeId_);
    }
    streamLoaders_ = tabletReader_.load(
        stripeIdentifier_.value(),
        streamIdentifiers_,
        [this](offset_size offset) {
          return streamLabels_.streamLabel(offset);
        });
  }
  metrics_.cpuUsec = timing.cpuNanos / 1000;
  metrics_.wallTimeUsec = timing.wallNanos / 1000;
}

void NimbleUnit::unload() {
  streamLoaders_.clear();
  stripeIdentifier_.reset();
}

uint64_t NimbleUnit::getIoSize() {
  if (ioSize_.has_value()) {
    return ioSize_.value();
  }
  if (!stripeIdentifier_.has_value()) {
    stripeIdentifier_ = tabletReader_.getStripeIdentifier(stripeId_);
  }
  ioSize_ = tabletReader_.getTotalStreamSize(
      stripeIdentifier_.value(), streamIdentifiers_);
  return ioSize_.value();
}

} // namespace

const std::vector<std::string>& VeloxReader::preloadedOptionalSections() {
  static std::vector<std::string> sections{std::string(kSchemaSection)};
  return sections;
}

VeloxReader::VeloxReader(
    velox::memory::MemoryPool& pool,
    velox::ReadFile* file,
    std::shared_ptr<const velox::dwio::common::ColumnSelector> selector,
    VeloxReadParams params)
    : VeloxReader(
          pool,
          std::make_shared<const TabletReader>(
              pool,
              file, /* preloadOptionalSections */
              preloadedOptionalSections()),
          std::move(selector),
          std::move(params)) {}

VeloxReader::VeloxReader(
    velox::memory::MemoryPool& pool,
    std::shared_ptr<velox::ReadFile> file,
    std::shared_ptr<const velox::dwio::common::ColumnSelector> selector,
    VeloxReadParams params)
    : VeloxReader(
          pool,
          std::make_shared<const TabletReader>(
              pool,
              std::move(file), /* preloadOptionalSections */
              preloadedOptionalSections()),
          std::move(selector),
          std::move(params)) {}

VeloxReader::VeloxReader(
    velox::memory::MemoryPool& pool,
    std::shared_ptr<const TabletReader> tabletReader,
    std::shared_ptr<const velox::dwio::common::ColumnSelector> selector,
    VeloxReadParams params)
    : pool_{pool},
      tabletReader_{std::move(tabletReader)},
      parameters_{std::move(params)},
      schema_{loadSchema(*tabletReader_)},
      type_{
          selector ? selector->getSchema()
                   : std::dynamic_pointer_cast<const velox::RowType>(
                         convertToVeloxType(*schema_))},
      barrier_{
          params.decodingExecutor
              ? std::make_unique<velox::dwio::common::ExecutorBarrier>(
                    params.decodingExecutor)
              : nullptr},
      logger_{
          parameters_.metricsLogger ? parameters_.metricsLogger
                                    : std::make_shared<MetricsLogger>()} {
  static_assert(std::is_same_v<velox::vector_size_t, int32_t>);

  if (!selector) {
    selector = std::make_shared<velox::dwio::common::ColumnSelector>(type_);
  }
  auto schemaWithId = selector->getSchemaWithId();
  rootFieldReaderFactory_ = FieldReaderFactory::create(
      parameters_,
      pool_,
      schema_,
      schemaWithId,
      offsets_,
      [selector](auto nodeId) { return selector->shouldReadNode(nodeId); },
      barrier_.get());

  // We scope down the allowed stripes based on the passed in offset ranges.
  // These ranges represent file splits.
  // File splits contain a file path and a range of bytes (not rows) withing
  // that file. It is possible that multiple file splits map to the same file
  // (but each covers a diffrent range within this file).
  // File splits are guaranteed to not overlap and also to cover the entire file
  // range.
  // We want to guarantee that each row in the file is processed by
  // exactly one split. When a reader is created, we only know about a single
  // split (the current range passed in), and we have no additional context
  // about other splits being processed by other readers. Therefore, we apply
  // the following heuristics to guarantee uniqueness across splits:
  // 1. We transpose the passed in range to match stripe boundaries.
  // 2. We consider a stripe to be part of this range, only if the stripe
  // beginning offset falls inside the range.
  // NOTE: With these heuristics, it is possible that a file split will map to
  // zero rows in a file (for example, if the file split is falls completely
  // inside a single stripe, without covering byte 0). This is perfectly ok, as
  // usually the caller will then fetch another file split to process and other
  // file splits will cover the rest of the file.

  firstStripe_ = tabletReader_->stripeCount();
  lastStripe_ = 0;
  firstRow_ = 0;
  lastRow_ = 0;
  uint64_t rows = 0;
  for (auto i = 0; i < tabletReader_->stripeCount(); ++i) {
    auto stripeOffset = tabletReader_->stripeOffset(i);
    if ((stripeOffset >= parameters_.fileRangeStartOffset) &&
        (stripeOffset < parameters_.fileRangeEndOffset)) {
      if (i < firstStripe_) {
        firstStripe_ = i;
        firstRow_ = rows;
      }
      if (i >= lastStripe_) {
        lastStripe_ = i + 1;
        lastRow_ = rows + tabletReader_->stripeRowCount(i);
      }
    }

    rows += tabletReader_->stripeRowCount(i);
  }

  nextStripe_ = firstStripe_;

  if (parameters_.stripeCountCallback) {
    if (firstStripe_ <= lastStripe_) {
      parameters_.stripeCountCallback(lastStripe_ - firstStripe_);
    } else {
      parameters_.stripeCountCallback(0);
    }
  }

  VLOG(1) << "TabletReader handling stripes: " << firstStripe_ << " -> "
          << lastStripe_ << " (rows " << firstRow_ << " -> " << lastRow_
          << "). Total stripes: " << tabletReader_->stripeCount()
          << ". Total rows: " << tabletReader_->tabletRowCount();

  unitLoader_ = getUnitLoader();
}

void VeloxReader::loadStripeIfAny() {
  if (nextStripe_ < lastStripe_) {
    loadNextStripe();
  }
}

void VeloxReader::loadNextStripe() {
  if (loadedStripe_.has_value() && loadedStripe_.value() == nextStripe_) {
    // We are not reloading the current stripe, but we expect all
    // decoders/readers to be reset after calling loadNextStripe(), therefore,
    // we need to explicitly reset all decoders and readers.
    rootReader_->reset();

    rowsRemainingInStripe_ = tabletReader_->stripeRowCount(nextStripe_);
    unitLoader_->onSeek(
        getUnitIndex(loadedStripe_.value()), /* rowInStripe */ 0);
    ++nextStripe_;
    return;
  }

  try {
    StripeLoadMetrics metrics;
    velox::CpuWallTiming timing{};
    {
      auto& unit = unitLoader_->getLoadedUnit(getUnitIndex(nextStripe_));

      velox::CpuWallTimer timer{timing};
      auto* nimbleUnit = dynamic_cast<NimbleUnit*>(&unit);
      NIMBLE_ASSERT(nimbleUnit, "Should be a NimbleUnit");
      rowsRemainingInStripe_ = nimbleUnit->getNumRows();
      metrics = nimbleUnit->getMetrics();
      metrics.totalStreamSize = nimbleUnit->getIoSize();

      auto streams = nimbleUnit->extractStreamLoaders();
      for (uint32_t i = 0; i < streams.size(); ++i) {
        if (!streams[i]) {
          // As this stream is not present in current stripe (might be present
          // in previous one) we set to nullptr, One of the case is where you
          // are projecting more fields in FlatMap than the stripe actually
          // has.
          decoders_[offsets_[i]] = nullptr;
        } else {
          ++metrics.streamCount;
          decoders_[offsets_[i]] = std::make_unique<ChunkedStreamDecoder>(
              pool_,
              std::make_unique<InMemoryChunkedStream>(
                  pool_, std::move(streams[i])),
              *logger_);
        }
      }
      loadedStripe_ = nextStripe_++;
      rootReader_ = rootFieldReaderFactory_->createReader(decoders_);
    }
    metrics.stripeIndex = loadedStripe_.value();
    metrics.rowsInStripe = rowsRemainingInStripe_;
    metrics.cpuUsec += timing.cpuNanos / 1000;
    metrics.wallTimeUsec += timing.wallNanos / 1000;
    logger_->logStripeLoad(metrics);
  } catch (const std::exception& e) {
    logger_->logException(LogOperation::StripeLoad, e.what());
    throw;
  } catch (...) {
    logger_->logException(
        LogOperation::StripeLoad,
        folly::to<std::string>(folly::exceptionStr(std::current_exception())));
    throw;
  }
}

bool VeloxReader::next(uint64_t rowCount, velox::VectorPtr& result) {
  if (rowsRemainingInStripe_ == 0) {
    if (nextStripe_ < lastStripe_) {
      loadNextStripe();
    } else {
      return false;
    }
  }
  uint64_t rowsToRead = std::min(rowsRemainingInStripe_, rowCount);
  std::optional<std::chrono::steady_clock::time_point> startTime;
  if (parameters_.decodingTimeCallback) {
    startTime = std::chrono::steady_clock::now();
  }
  unitLoader_->onRead(
      getUnitIndex(loadedStripe_.value()), getCurrentRowInStripe(), rowsToRead);
  rootReader_->next(rowsToRead, result, nullptr /*scatterBitmap*/);
  if (barrier_) {
    // Wait for all reader tasks to complete.
    barrier_->waitAll();
  }
  if (startTime.has_value()) {
    parameters_.decodingTimeCallback(
        std::chrono::steady_clock::now() - startTime.value());
  }

  // Update reader state
  rowsRemainingInStripe_ -= rowsToRead;
  return true;
}

const TabletReader& VeloxReader::tabletReader() const {
  return *tabletReader_;
}

const std::shared_ptr<const velox::RowType>& VeloxReader::type() const {
  return type_;
}

const std::shared_ptr<const Type>& VeloxReader::schema() const {
  return schema_;
}

const std::map<std::string, std::string>& VeloxReader::metadata() const {
  if (!metadata_.has_value()) {
    metadata_ = loadMetadata(*tabletReader_);
  }

  return metadata_.value();
}

uint64_t VeloxReader::seekToRow(uint64_t rowNumber) {
  if (isEmptyFile()) {
    return 0;
  }

  if (rowNumber < firstRow_) {
    LOG(INFO) << "Trying to seek to row " << rowNumber
              << " which is outside of the allowed range [" << firstRow_ << ", "
              << lastRow_ << ").";

    nextStripe_ = firstStripe_;
    rowsRemainingInStripe_ = 0;
    return firstRow_;
  }

  if (rowNumber >= lastRow_) {
    LOG(INFO) << "Trying to seek to row " << rowNumber
              << " which is outside of the allowed range [" << firstRow_ << ", "
              << lastRow_ << ").";

    nextStripe_ = lastStripe_;
    rowsRemainingInStripe_ = 0;
    return lastRow_;
  }

  auto rowsSkipped = skipStripes(0, rowNumber);
  loadNextStripe();
  skipInCurrentStripe(rowNumber - rowsSkipped);
  return rowNumber;
}

uint64_t VeloxReader::skipRows(uint64_t numberOfRowsToSkip) {
  if (isEmptyFile() || numberOfRowsToSkip == 0) {
    LOG(INFO) << "Nothing to skip!";
    return 0;
  }

  // When we skipped or exhausted the whole file we can return 0
  if (rowsRemainingInStripe_ == 0 && nextStripe_ == lastStripe_) {
    LOG(INFO) << "Current index is beyond EOF. Nothing to skip.";
    return 0;
  }

  // Skips remaining rows in stripe
  if (rowsRemainingInStripe_ >= numberOfRowsToSkip) {
    skipInCurrentStripe(numberOfRowsToSkip);
    return numberOfRowsToSkip;
  }

  uint64_t rowsSkipped = rowsRemainingInStripe_;
  auto rowsToSkip = numberOfRowsToSkip;
  // Skip the leftover rows from currently loaded stripe
  rowsToSkip -= rowsRemainingInStripe_;
  rowsSkipped += skipStripes(nextStripe_, rowsToSkip);
  if (nextStripe_ >= lastStripe_) {
    LOG(INFO) << "Skipped to last allowed row in the file.";
    return rowsSkipped;
  }

  loadNextStripe();
  skipInCurrentStripe(numberOfRowsToSkip - rowsSkipped);
  return numberOfRowsToSkip;
}

uint64_t VeloxReader::skipStripes(
    uint32_t startStripeIndex,
    uint64_t rowsToSkip) {
  NIMBLE_DCHECK(
      startStripeIndex <= lastStripe_,
      fmt::format("Invalid stripe {}.", startStripeIndex));

  uint64_t totalRowsToSkip = rowsToSkip;
  while (startStripeIndex < lastStripe_ &&
         rowsToSkip >= tabletReader_->stripeRowCount(startStripeIndex)) {
    rowsToSkip -= tabletReader_->stripeRowCount(startStripeIndex);
    ++startStripeIndex;
  }

  nextStripe_ = startStripeIndex;
  rowsRemainingInStripe_ = nextStripe_ >= lastStripe_
      ? 0
      : tabletReader_->stripeRowCount(nextStripe_);

  return totalRowsToSkip - rowsToSkip;
}

void VeloxReader::skipInCurrentStripe(uint64_t rowsToSkip) {
  NIMBLE_DCHECK(
      rowsToSkip <= rowsRemainingInStripe_,
      "Not Enough rows to skip in stripe!");
  rowsRemainingInStripe_ -= rowsToSkip;
  unitLoader_->onSeek(
      getUnitIndex(loadedStripe_.value()), getCurrentRowInStripe());
  rootReader_->skip(rowsToSkip);
}

VeloxReader::~VeloxReader() = default;

std::unique_ptr<velox::dwio::common::UnitLoader> VeloxReader::getUnitLoader() {
  if (lastStripe_ <= firstStripe_) {
    return nullptr;
  }

  std::vector<std::unique_ptr<velox::dwio::common::LoadUnit>> units;
  units.reserve(lastStripe_ - firstStripe_);
  for (uint32_t stripe = firstStripe_; stripe < lastStripe_; ++stripe) {
    units.push_back(std::make_unique<NimbleUnit>(
        stripe, *tabletReader_, schema_, offsets_));
  }
  if (parameters_.unitLoaderFactory) {
    return parameters_.unitLoaderFactory->create(std::move(units), 0);
  }
  velox::dwio::common::OnDemandUnitLoaderFactory factory(
      parameters_.blockedOnIoCallback);
  return factory.create(std::move(units), 0);
}

uint32_t VeloxReader::getUnitIndex(uint32_t stripeIndex) const {
  return stripeIndex - firstStripe_;
}

uint32_t VeloxReader::getCurrentRowInStripe() const {
  return tabletReader_->stripeRowCount(loadedStripe_.value()) -
      static_cast<uint32_t>(rowsRemainingInStripe_);
}

} // namespace facebook::nimble
