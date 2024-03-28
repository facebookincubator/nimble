// Copyright 2004-present Facebook. All Rights Reserved.

#include "dwio/alpha/velox/VeloxReader.h"
#include <cstdint>
#include <optional>
#include <vector>
#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/velox/ChunkedStreamDecoder.h"
#include "dwio/alpha/velox/MetadataGenerated.h"
#include "dwio/alpha/velox/SchemaReader.h"
#include "dwio/alpha/velox/SchemaSerialization.h"
#include "dwio/alpha/velox/SchemaTypes.h"
#include "dwio/alpha/velox/SchemaUtils.h"
#include "dwio/alpha/velox/TabletSections.h"
#include "velox/common/time/CpuWallTimer.h"
#include "velox/type/Type.h"

namespace facebook::alpha {

namespace {

std::shared_ptr<const Type> loadSchema(const Tablet& tablet) {
  auto section = tablet.loadOptionalSection(std::string(kSchemaSection));
  ALPHA_CHECK(section.has_value(), "Schema not found.");
  return SchemaDeserializer::deserialize(section->content().data());
}

std::map<std::string, std::string> loadMetadata(const Tablet& tablet) {
  std::map<std::string, std::string> result;
  auto section = tablet.loadOptionalSection(std::string(kMetadataSection));

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
  ALPHA_ASSERT(
      !selectedFeatures.empty(),
      "Empty feature selection not allowed for struct encoding.");

  auto& valueType = veloxType->asMap().valueType();
  return velox::ROW(
      std::vector<std::string>(selectedFeatures),
      std::vector<std::shared_ptr<const velox::Type>>(
          selectedFeatures.size(), valueType));
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
          std::make_shared<const Tablet>(
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
          std::make_shared<const Tablet>(
              pool,
              std::move(file), /* preloadOptionalSections */
              preloadedOptionalSections()),
          std::move(selector),
          std::move(params)) {}

VeloxReader::VeloxReader(
    velox::memory::MemoryPool& pool,
    std::shared_ptr<const Tablet> tablet,
    std::shared_ptr<const velox::dwio::common::ColumnSelector> selector,
    VeloxReadParams params)
    : pool_{pool},
      tablet_{std::move(tablet)},
      parameters_{std::move(params)},
      schema_{loadSchema(*tablet_)},
      streamLabels_{schema_},
      type_{
          selector ? selector->getSchema()
                   : std::dynamic_pointer_cast<const velox::RowType>(
                         convertToVeloxType(*schema_))},
      barrier_{
          params.decodingExecutor
              ? std::make_unique<velox::dwio::common::ExecutorBarrier>(
                    params.decodingExecutor)
              : nullptr},
      logger_{parameters_.metricsLogger} {
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

  firstStripe_ = tablet_->stripeCount();
  lastStripe_ = 0;
  firstRow_ = 0;
  lastRow_ = 0;
  uint64_t rows = 0;
  for (auto i = 0; i < tablet_->stripeCount(); ++i) {
    auto stripeOffset = tablet_->stripeOffset(i);
    if ((stripeOffset >= parameters_.fileRangeStartOffset) &&
        (stripeOffset < parameters_.fileRangeEndOffset)) {
      if (i < firstStripe_) {
        firstStripe_ = i;
        firstRow_ = rows;
      }
      if (i >= lastStripe_) {
        lastStripe_ = i + 1;
        lastRow_ = rows + tablet_->stripeRowCount(i);
      }
    }

    rows += tablet_->stripeRowCount(i);
  }

  nextStripe_ = firstStripe_;

  if (parameters_.stripeCountCallback) {
    parameters_.stripeCountCallback(lastStripe_ - firstStripe_);
  }

  VLOG(1) << "Tablet handling stripes: " << firstStripe_ << " -> "
          << lastStripe_ << " (rows " << firstRow_ << " -> " << lastRow_
          << "). Total stripes: " << tablet_->stripeCount()
          << ". Total rows: " << tablet_->tabletRowCount();

  if (!logger_) {
    logger_ = std::make_shared<MetricsLogger>();
  }
}

void VeloxReader::loadStripeIfAny() {
  if (nextStripe_ < lastStripe_) {
    loadStripe();
  }
}

void VeloxReader::loadStripe() {
  try {
    if (loadedStripe_ != std::numeric_limits<uint32_t>::max() &&
        loadedStripe_ == nextStripe_) {
      // We are not reloading the current stripe, but we expect all
      // decoders/readers to be reset after calling loadStripe(), therefore, we
      // need to explicitly reset all decoders and readers.
      rootReader_->reset();

      rowsRemainingInStripe_ = tablet_->stripeRowCount(nextStripe_);
      ++nextStripe_;
      return;
    }

    StripeLoadMetrics metrics{};
    velox::CpuWallTiming timing{};
    {
      velox::CpuWallTimer timer{timing};
      // LoadAll returns all the stream available in a stripe.
      // The streams returned might be a subset of the total streams available
      // in the file, as the current stripe might have captured/encountered less
      // streams than later stripes.
      // In the extreme case, a stripe can return zero streams (for example, if
      // all the streams in that stripe were contained all nulls).
      auto streams =
          tablet_->load(nextStripe_, offsets_, [this](offset_size offset) {
            return streamLabels_.streamLabel(offset);
          });
      for (uint32_t i = 0; i < streams.size(); ++i) {
        if (!streams[i]) {
          // As this stream is not present in current stripe (might be present
          // in previous one) we set to nullptr, One of the case is where you
          // are projecting more fields in FlatMap than the stripe actually
          // has.
          decoders_[offsets_[i]] = nullptr;
        } else {
          metrics.totalStreamSize += streams[i]->getStream().size();
          decoders_[offsets_[i]] = std::make_unique<ChunkedStreamDecoder>(
              pool_,
              std::make_unique<InMemoryChunkedStream>(
                  pool_, std::move(streams[i])),
              *logger_);
          ++metrics.streamCount;
        }
      }
      rowsRemainingInStripe_ = tablet_->stripeRowCount(nextStripe_);
      loadedStripe_ = nextStripe_;
      ++nextStripe_;
      rootReader_ = rootFieldReaderFactory_->createReader(decoders_);
    }
    metrics.stripeIndex = loadedStripe_;
    metrics.rowsInStripe = rowsRemainingInStripe_;
    metrics.cpuUsec = timing.cpuNanos / 1000;
    metrics.wallTimeUsec = timing.wallNanos / 1000;
    if (parameters_.blockedOnIoMsCallback) {
      parameters_.blockedOnIoMsCallback(timing.wallNanos / 1000000);
    }
    logger_->logStripeLoad(metrics);
  } catch (const std::exception& e) {
    logger_->logException(MetricsLogger::kStripeLoadOperation, e.what());
    throw;
  } catch (...) {
    logger_->logException(
        MetricsLogger::kStripeLoadOperation,
        folly::to<std::string>(folly::exceptionStr(std::current_exception())));
    throw;
  }
}

bool VeloxReader::next(uint64_t rowCount, velox::VectorPtr& result) {
  if (rowsRemainingInStripe_ == 0) {
    if (nextStripe_ < lastStripe_) {
      loadStripe();
    } else {
      return false;
    }
  }
  uint64_t rowsToRead = std::min(rowsRemainingInStripe_, rowCount);
  std::optional<std::chrono::steady_clock::time_point> startTime;
  if (parameters_.decodingTimeUsCallback) {
    startTime = std::chrono::steady_clock::now();
  }
  rootReader_->next(rowsToRead, result, nullptr /*scatterBitmap*/);
  if (barrier_) {
    // Wait for all reader tasks to complete.
    barrier_->waitAll();
  }
  if (startTime.has_value()) {
    parameters_.decodingTimeUsCallback(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - startTime.value())
            .count());
  }

  // Update reader state
  rowsRemainingInStripe_ -= rowsToRead;
  return true;
}

const Tablet& VeloxReader::getTabletView() const {
  return *tablet_;
}

const std::shared_ptr<const velox::RowType>& VeloxReader::getType() const {
  return type_;
}

const std::shared_ptr<const Type>& VeloxReader::schema() const {
  return schema_;
}

const std::map<std::string, std::string>& VeloxReader::metadata() const {
  if (!metadata_.has_value()) {
    metadata_ = loadMetadata(*tablet_);
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
  loadStripe();
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

  loadStripe();
  skipInCurrentStripe(numberOfRowsToSkip - rowsSkipped);
  return numberOfRowsToSkip;
}

uint64_t VeloxReader::skipStripes(
    uint32_t startStripeIndex,
    uint64_t rowsToSkip) {
  ALPHA_DCHECK(
      startStripeIndex <= lastStripe_,
      fmt::format("Invalid stripe {}.", startStripeIndex));

  uint64_t totalRowsToSkip = rowsToSkip;
  while (startStripeIndex < lastStripe_ &&
         rowsToSkip >= tablet_->stripeRowCount(startStripeIndex)) {
    rowsToSkip -= tablet_->stripeRowCount(startStripeIndex);
    ++startStripeIndex;
  }

  nextStripe_ = startStripeIndex;
  rowsRemainingInStripe_ =
      nextStripe_ >= lastStripe_ ? 0 : tablet_->stripeRowCount(nextStripe_);

  return totalRowsToSkip - rowsToSkip;
}

void VeloxReader::skipInCurrentStripe(uint64_t rowsToSkip) {
  ALPHA_DCHECK(
      rowsToSkip <= rowsRemainingInStripe_,
      "Not Enough rows to skip in stripe!");
  rowsRemainingInStripe_ -= rowsToSkip;
  rootReader_->skip(rowsToSkip);
}

VeloxReader::~VeloxReader() = default;

} // namespace facebook::alpha
