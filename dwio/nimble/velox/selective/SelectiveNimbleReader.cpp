/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

#include "dwio/nimble/velox/selective/SelectiveNimbleReader.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/legacy/EncodingFactory.h"
#include "dwio/nimble/index/IndexFilter.h"
#include "dwio/nimble/index/IndexReader.h"

#include "dwio/nimble/velox/SchemaUtils.h"
#include "dwio/nimble/velox/selective/ColumnReader.h"
#include "dwio/nimble/velox/selective/ReaderBase.h"
#include "dwio/nimble/velox/selective/RowSizeTracker.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/serializers/KeyEncoder.h"

namespace facebook::nimble {
namespace detail {

// Provide external initialization code for a selective reader instance.
void initHook();

} // namespace detail

using namespace facebook::velox;

namespace {

// Converts index column names from nimble schema (internal file names) to
// file schema (user-facing names). The nimble schema and file schema have
// the same structure but potentially different column names due to the
// options.fileSchema() mapping applied during file schema construction.
//
// @param nimbleIndexColumns Index column names from TabletIndex (nimble schema)
// @param nimbleSchema The nimble schema from the file
// @param fileSchema The file schema (table schema with name mapping applied)
// @return Vector of index column names in file schema
std::vector<std::string> convertIndexColumnsToFileSchema(
    const std::vector<std::string>& nimbleIndexColumns,
    const std::shared_ptr<const Type>& nimbleSchema,
    const RowTypePtr& fileSchema) {
  const auto nimbleRowType = asRowType(convertToVeloxType(*nimbleSchema));

  std::vector<std::string> convertedIndexColumns;
  convertedIndexColumns.reserve(nimbleIndexColumns.size());
  for (const auto& nimbleColName : nimbleIndexColumns) {
    const auto colIndex = nimbleRowType->getChildIdxIfExists(nimbleColName);
    NIMBLE_CHECK(
        colIndex.has_value(),
        "Index column '{}' not found in nimble schema: {}",
        nimbleColName,
        nimbleRowType->toString());
    convertedIndexColumns.push_back(fileSchema->nameOf(colIndex.value()));
  }
  return convertedIndexColumns;
}

// Converts nimble SortOrders to velox::core::SortOrders.
// Only converts the first numBoundColumns sort orders, which corresponds to the
// columns that were converted to index bounds (a prefix of all index columns).
std::vector<velox::core::SortOrder> toVeloxSortOrders(
    const std::vector<SortOrder>& sortOrders,
    size_t numBoundColumns) {
  std::vector<velox::core::SortOrder> veloxSortOrders;
  veloxSortOrders.reserve(numBoundColumns);
  for (size_t i = 0; i < numBoundColumns; ++i) {
    veloxSortOrders.push_back(sortOrders[i].toVeloxSortOrder());
  }
  return veloxSortOrders;
}

} // namespace

namespace {

class SelectiveNimbleRowReader : public dwio::common::RowReader {
 public:
  SelectiveNimbleRowReader(
      const std::shared_ptr<ReaderBase>& readerBase,
      const dwio::common::RowReaderOptions& options)
      : readerBase_{readerBase},
        options_{options},
        streams_(readerBase_),
        rowSizeTracker_{
            std::make_unique<RowSizeTracker>(readerBase->fileSchemaWithId())} {
    initReadRange();
    initIndex();
    if (options.eagerFirstStripeLoad()) {
      nextRowNumber();
    }
  }

  ~SelectiveNimbleRowReader() override {
    restoreFilters();
  }

  int64_t nextRowNumber() override;

  int64_t nextReadSize(uint64_t size) override;

  uint64_t next(
      uint64_t size,
      VectorPtr& result,
      const dwio::common::Mutation* mutation) override;

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override;

  void resetFilterCaches() override;

  std::optional<size_t> estimatedRowSize() const override;

  bool allPrefetchIssued() const final;

  // Resets the row reader for a new query with different scan spec/index
  // bounds. The split boundaries (splitStartStripe_, splitEndStripe_) remain
  // the same, but the actual read boundaries (startStripe_, endStripe_) may
  // change based on new index bounds.
  void reset() override;

 private:
  // Initializes the stripe range to read based on row offset bounds
  // specified in options. Sets startStripe_ and endStripe_.
  void initReadRange();

  // Initializes the tablet index if index is enabled and available.
  // Sets tabletIndex_ and indexColumns_ which are reused across reset() calls.
  void initIndex();

  // Loads the current stripe by initializing streams and column readers.
  // Also calls setStripeRowRange() to apply index-based row range filtering.
  void loadCurrentStripe();

  // Sets up index bounds for key-based filtering. Sets up
  // encodedKeyBounds_ if cluster index bounds are specified in the scan spec.
  void maybeSetIndexBounds();

  // Returns true if cluster index bounds are specified for key-based
  // filtering.
  bool hasIndexBounds() const;

  // Updates the start stripe based on the lower index bound.
  // Uses TabletIndex lookup to find the first stripe containing keys at or
  // after the lower bound. If no matching stripe is found, sets startStripe_
  // to endStripe_ to skip reading.
  void updateStartStripeFromLowerIndexBound();

  // Updates the end stripe based on the upper index bound.
  // Uses TabletIndex lookup to find the last stripe containing keys at or
  // before the upper bound. Sets endStripe_ to one past the matching stripe
  // to create an exclusive upper bound.
  void updateEndStripeFromUpperIndexBound();

  // Advances to the next stripe by incrementing the stripe index and resetting
  // the row position state.
  void advanceToNextStripe();

  // Builds the index reader for the current stripe. Only called when index
  // bounds are set.
  void buildIndexReader(NimbleParams& params);

  // Sets the row range to read within the current stripe based on cluster
  // index bounds. For the first stripe, adjusts rowInCurrentStripe_ to the
  // lower bound position. For the last stripe, sets endRowInCurrentStripe_ to
  // the upper bound position. Also skips to the starting row position if
  // needed.
  void setStripeRowRange();

  // Updates the random skip tracker when rows are skipped due to index bounds.
  void maybeUpdateRandomSkip(int64_t rowsSkipped);

  // Sets nextRowNumber_ to kAtEnd and restores any filters that were removed
  // during index bound conversion.
  void setAtEnd();

  // Restores filters that were removed during index bound conversion.
  // Called from both setAtEnd() and the destructor.
  void restoreFilters();

  const std::shared_ptr<ReaderBase> readerBase_;
  const dwio::common::RowReaderOptions options_;

  StripeStreams streams_;
  std::vector<int64_t> stripeRowOffsets_;

  // The split stripe boundaries - fixed after initReadRange(), represent the
  // data split boundaries from options_.offset() and options_.limit().
  int32_t splitStartStripe_{};
  int32_t splitEndStripe_{};

  // The actual stripe range to read - may be narrowed by index bounds.
  // These are reset to split boundaries on reset().
  // The inclusive lower bound of the stripe range to read.
  int32_t startStripe_{};
  // The exclusive upper bound of the stripe range to read.
  int32_t endStripe_{};

  // Index related fields.
  const TabletIndex* tabletIndex_{nullptr};
  // Index column names converted from nimble schema to file schema.
  // Set once during initIndex() and reused across reset() calls.
  std::vector<std::string> indexColumns_;
  std::optional<velox::serializer::EncodedKeyBounds> encodedKeyBounds_;
  // Index reader for seeking to row positions based on cluster index bounds.
  // Only created for the first and last stripes when index bounds are set.
  std::unique_ptr<index::IndexReader> indexReader_;

  // The current stripe being read.
  int32_t currentStripe_{};
  // The current row position within the current stripe (0-based).
  int64_t rowInCurrentStripe_{};
  // Optional end row position for the current stripe, set when reading the
  // last stripe with upper index bounds.
  std::optional<int64_t> endRowInCurrentStripe_;
  std::optional<int64_t> nextRowNumber_;
  // Flag to track whether index bounds have been set for the current scan.
  // Reset to false on each reset() call.
  bool hasSetIndexBounds_{false};

  int32_t skippedStripes_{0};
  // Tracks the number of rows from trailing stripes filtered out by upper index
  // bound. These rows are combined with rows skipped at the end of the last
  // stripe when updating the random skip tracker.
  int64_t trailingSkippedRows_{0};

  std::unique_ptr<dwio::common::SelectiveColumnReader> columnReader_;
  dwio::common::ColumnReaderStatistics columnReaderStatistics_;
  std::unique_ptr<RowSizeTracker> rowSizeTracker_;

  // Filters that were removed from the scan spec during index bound conversion.
  // These need to be restored when the row reader is destroyed, as the scan
  // spec may be shared across multiple split readers.
  std::vector<std::pair<std::string, std::shared_ptr<common::Filter>>>
      filtersToRestore_;
};

int64_t SelectiveNimbleRowReader::nextRowNumber() {
  if (nextRowNumber_.has_value()) {
    return *nextRowNumber_;
  }
  maybeSetIndexBounds();
  while (currentStripe_ < endStripe_) {
    auto numStripeRows = readerBase_->tablet().stripeRowCount(currentStripe_);
    if (rowInCurrentStripe_ == 0) {
      // Apply random skip at stripe level (before loading the stripe).
      if (readerBase_->randomSkip() &&
          readerBase_->randomSkip()->nextSkip() >= numStripeRows) {
        readerBase_->randomSkip()->consume(numStripeRows);
        ++skippedStripes_;
        goto advanceToNextStripe;
      }
      loadCurrentStripe();
    }
    if (endRowInCurrentStripe_.has_value()) {
      NIMBLE_CHECK_LE(endRowInCurrentStripe_.value(), numStripeRows);
      numStripeRows = endRowInCurrentStripe_.value();
    }
    if (rowInCurrentStripe_ < numStripeRows) {
      nextRowNumber_ = stripeRowOffsets_[currentStripe_] + rowInCurrentStripe_;
      return *nextRowNumber_;
    }
  advanceToNextStripe:
    advanceToNextStripe();
  }
  // Update random skip tracker for trailing rows that were skipped due to upper
  // index bound filtering (includes both entire trailing stripes and rows at
  // the end of the last stripe).
  maybeUpdateRandomSkip(trailingSkippedRows_);

  setAtEnd();
  return kAtEnd;
}

void SelectiveNimbleRowReader::advanceToNextStripe() {
  ++currentStripe_;
  rowInCurrentStripe_ = 0;
  endRowInCurrentStripe_.reset();
}

int64_t SelectiveNimbleRowReader::nextReadSize(uint64_t size) {
  NIMBLE_DCHECK_GT(size, 0, "Read size must be greater than 0");
  if (nextRowNumber() == kAtEnd) {
    return kAtEnd;
  }
  auto numStripeRows = readerBase_->tablet().stripeRowCount(currentStripe_);
  if (endRowInCurrentStripe_.has_value()) {
    numStripeRows = endRowInCurrentStripe_.value();
  }
  const auto rowsToRead =
      std::min<int64_t>(size, numStripeRows - rowInCurrentStripe_);
  NIMBLE_DCHECK_GT(rowsToRead, 0);
  return rowsToRead;
}

uint64_t SelectiveNimbleRowReader::next(
    uint64_t size,
    VectorPtr& result,
    const dwio::common::Mutation* mutation) {
  const auto rowsToRead = nextReadSize(size);
  if (rowsToRead == kAtEnd) {
    return 0;
  }
  columnReader_->setCurrentRowNumber(nextRowNumber());
  if (options_.rowNumberColumnInfo().has_value()) {
    readWithRowNumber(
        columnReader_, options_, nextRowNumber(), rowsToRead, mutation, result);
  } else {
    columnReader_->next(rowsToRead, result, mutation);
  }
  nextRowNumber_.reset();
  rowInCurrentStripe_ += rowsToRead;
  return rowsToRead;
}

void SelectiveNimbleRowReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& stats) const {
  stats.skippedStrides += skippedStripes_;
}

void SelectiveNimbleRowReader::resetFilterCaches() {
  if (columnReader_) {
    columnReader_->resetFilterCaches();
  }
}

std::optional<size_t> SelectiveNimbleRowReader::estimatedRowSize() const {
  // TODO: Use column statistics once ready.
  if (const_cast<SelectiveNimbleRowReader*>(this)->nextRowNumber() == kAtEnd) {
    return std::nullopt;
  }
  size_t byteSize, rowCount;
  if (!columnReader_->estimateMaterializedSize(byteSize, rowCount)) {
    return options_.trackRowSize() ? rowSizeTracker_->getCurrentMaxRowSize()
                                   : 1UL << 20;
  }
  return rowCount == 0 ? 0 : byteSize / rowCount;
}

bool SelectiveNimbleRowReader::allPrefetchIssued() const {
  return true;
}

void SelectiveNimbleRowReader::reset() {
  // Restore any filters that were removed during previous index bound
  // conversion. This is important because the scan spec may have filters
  // removed when converting to index bounds.
  restoreFilters();

  // Reset actual stripe range to split boundaries.
  startStripe_ = splitStartStripe_;
  endStripe_ = splitEndStripe_;
  currentStripe_ = startStripe_;

  // Reset row positions.
  rowInCurrentStripe_ = 0;
  endRowInCurrentStripe_.reset();
  nextRowNumber_.reset();

  // Reset index-related state (tabletIndex_ is not reset since it's tied to
  // the file, not the query).
  encodedKeyBounds_.reset();
  indexReader_.reset();
  hasSetIndexBounds_ = false;

  // Reset statistics.
  skippedStripes_ = 0;
  trailingSkippedRows_ = 0;

  // Reset column reader (will be rebuilt on next loadCurrentStripe()).
  columnReader_.reset();
  // Reset buffered input to release cached data and prefetch state for the
  // previous scan. This allows the next scan to start fresh with new I/O
  // scheduling.
  readerBase_->input().reset();
}

void SelectiveNimbleRowReader::initReadRange() {
  const auto& tablet = readerBase_->tablet();
  splitStartStripe_ = tablet.stripeCount();
  splitEndStripe_ = 0;
  int64_t numRows = 0;
  stripeRowOffsets_.resize(tablet.stripeCount());
  const auto low = options_.offset();
  const auto high = options_.limit();
  for (int i = 0; i < tablet.stripeCount(); ++i) {
    stripeRowOffsets_[i] = numRows;
    if (low <= tablet.stripeOffset(i) && tablet.stripeOffset(i) < high) {
      splitStartStripe_ = std::min(splitStartStripe_, i);
      splitEndStripe_ = std::max(splitEndStripe_, i + 1);
    }
    numRows += tablet.stripeRowCount(i);
  }
  // Initialize actual read boundaries to split boundaries.
  startStripe_ = splitStartStripe_;
  endStripe_ = splitEndStripe_;
  currentStripe_ = startStripe_;
  rowInCurrentStripe_ = 0;
}

void SelectiveNimbleRowReader::initIndex() {
  NIMBLE_CHECK_NULL(tabletIndex_);

  // Check if index filtering is disabled via row reader options.
  if (!options_.indexEnabled()) {
    return;
  }

  // Verify that the file has a cluster index.
  if (!readerBase_->tablet().hasIndex()) {
    return;
  }

  tabletIndex_ = readerBase_->tablet().index();
  NIMBLE_CHECK_NOT_NULL(tabletIndex_);

  // Convert index column names from nimble schema to file schema.
  // This is cached and reused across reset() calls.
  indexColumns_ = convertIndexColumnsToFileSchema(
      tabletIndex_->indexColumns(),
      readerBase_->nimbleSchema(),
      readerBase_->fileSchema());
}

void SelectiveNimbleRowReader::loadCurrentStripe() {
  addThreadLocalRuntimeStat(kNumStripeLoads, velox::RuntimeCounter(1));

  streams_.setStripe(currentStripe_, hasIndexBounds());
  NimbleParams params(
      *readerBase_->pool(),
      columnReaderStatistics_,
      readerBase_->nimbleSchema(),
      streams_,
      options_.trackRowSize() ? rowSizeTracker_.get() : nullptr,
      options_.passStringBuffersFromDecoder()
          ? [](velox::memory::MemoryPool& pool,
               std::string_view data,
               std::function<void*(uint32_t)> stringBufferFactory)
                -> std::unique_ptr<Encoding> {
        return EncodingFactory::decode(pool, data, stringBufferFactory);
      }
          : [](velox::memory::MemoryPool& pool,
               std::string_view data,
               std::function<void*(uint32_t)> stringBufferFactory)
                -> std::unique_ptr<Encoding> {
        return legacy::EncodingFactory::decode(pool, data, stringBufferFactory);
      },
      options_.passStringBuffersFromDecoder(),
      options_.preserveFlatMapsInMemory());
  columnReader_ = buildColumnReader(
      options_.requestedType() ? options_.requestedType()
                               : readerBase_->fileSchema(),
      readerBase_->fileSchemaWithId(),
      params,
      *options_.scanSpec(),
      true);
  rowSizeTracker_->finalizeProjection();
  columnReader_->setIsTopLevel();
  // Enqueue the key stream before the stripe IO load for potential IO
  // coalescing with data streams.
  buildIndexReader(params);
  streams_.load();
  setStripeRowRange();
}

bool SelectiveNimbleRowReader::hasIndexBounds() const {
  return encodedKeyBounds_.has_value();
}

void SelectiveNimbleRowReader::maybeSetIndexBounds() {
  // Early return if index is not available.
  if (tabletIndex_ == nullptr) {
    return;
  }

  // Skip if already set for this scan.
  if (hasSetIndexBounds_) {
    return;
  }
  hasSetIndexBounds_ = true;

  // Early return if there are no stripes to read based on the read range
  if (currentStripe_ >= endStripe_) {
    return;
  }

  // Convert filters from scanSpec to index bounds.
  const auto& sortOrders = tabletIndex_->sortOrders();
  auto result = convertFilterToIndexBounds(
      indexColumns_,
      sortOrders,
      readerBase_->fileSchema(),
      *options_.scanSpec(),
      readerBase_->pool());

  if (!result.has_value()) {
    return;
  }

  filtersToRestore_ = std::move(result->removedFilters);
  options_.scanSpec()->resetCachedValues(/*doReorder=*/false);

  addThreadLocalRuntimeStat(
      kNumIndexFilterConversions,
      velox::RuntimeCounter(result->indexBounds.indexColumns.size()));

  auto keyEncoder = velox::serializer::KeyEncoder::create(
      result->indexBounds.indexColumns,
      asRowType(result->indexBounds.type()),
      toVeloxSortOrders(sortOrders, result->indexBounds.indexColumns.size()),
      readerBase_->pool());
  encodedKeyBounds_ = keyEncoder->encodeIndexBounds(result->indexBounds);

  updateStartStripeFromLowerIndexBound();
  updateEndStripeFromUpperIndexBound();
  // Reset currentStripe_ to startStripe_ since startStripe_ may have been
  // updated by updateStartStripeFromLowerIndexBound().
  currentStripe_ = startStripe_;
}

void SelectiveNimbleRowReader::updateStartStripeFromLowerIndexBound() {
  NIMBLE_CHECK(encodedKeyBounds_.has_value());
  if (!encodedKeyBounds_->lowerKey.has_value()) {
    return;
  }

  const auto originalStartStripe = startStripe_;

  // Lookup the first stripe that matches the lower bound
  const auto lowerLocation =
      tabletIndex_->lookup(encodedKeyBounds_->lowerKey.value());
  if (lowerLocation.has_value()) {
    startStripe_ =
        std::max(startStripe_, static_cast<int>(lowerLocation->stripeIndex));
  } else if (encodedKeyBounds_->lowerKey.value() > tabletIndex_->maxKey()) {
    // If the lower bound key is greater than the max key, skip all the
    // stripes entirely.
    startStripe_ = endStripe_;
  }

  // Update random skip tracker for rows in stripes that are filtered out
  // entirely.
  if (startStripe_ > originalStartStripe) {
    int64_t rowsSkipped{0};
    for (int stripe = originalStartStripe; stripe < startStripe_; ++stripe) {
      rowsSkipped += readerBase_->tablet().stripeRowCount(stripe);
    }
    maybeUpdateRandomSkip(rowsSkipped);
  }
}

void SelectiveNimbleRowReader::updateEndStripeFromUpperIndexBound() {
  NIMBLE_CHECK(encodedKeyBounds_.has_value());
  if (!encodedKeyBounds_->upperKey.has_value()) {
    return;
  }
  // Early return if the lower bound filtering already filtered out all stripes.
  if (startStripe_ >= endStripe_) {
    return;
  }

  const auto originalEndStripe = endStripe_;

  const auto& upperKey = encodedKeyBounds_->upperKey.value();
  // Lookup the last stripe that matches the upper bound
  const auto upperLocation = tabletIndex_->lookup(upperKey);
  if (upperLocation.has_value()) {
    endStripe_ =
        std::min(endStripe_, static_cast<int>(upperLocation->stripeIndex + 1));
  } else if (encodedKeyBounds_->upperKey.value() <= tabletIndex_->minKey()) {
    // If the upper bound key is less than the min key, skip all the stripes
    // entirely.
    endStripe_ = startStripe_;
  }

  // Track rows from trailing stripes that are filtered out entirely.
  // These will be combined with rows skipped at the end of the last stripe
  // when updating the random skip tracker.
  if (endStripe_ < originalEndStripe) {
    for (int stripe = endStripe_; stripe < originalEndStripe; ++stripe) {
      trailingSkippedRows_ += readerBase_->tablet().stripeRowCount(stripe);
    }
  }
}

void SelectiveNimbleRowReader::buildIndexReader(NimbleParams& params) {
  if (!hasIndexBounds()) {
    return;
  }
  // We only need row position seek in the first and the last stripe to find
  // the start and end row positions based on index bounds. Note that the first
  // and the last stripe can also be the same one.
  if ((currentStripe_ != startStripe_) && (currentStripe_ != endStripe_ - 1)) {
    return;
  }
  NIMBLE_CHECK_NOT_NULL(tabletIndex_);
  indexReader_ = index::IndexReader::create(
      params.streams().enqueueKeyStream(),
      params.streams().stripeIndex(),
      params.streams().indexGroup(),
      tabletIndex_->noDuplicateKey(),
      &params.pool());
}

void SelectiveNimbleRowReader::maybeUpdateRandomSkip(int64_t rowsSkipped) {
  if (readerBase_->randomSkip() && rowsSkipped > 0) {
    const auto skip = readerBase_->randomSkip()->nextSkip();
    readerBase_->randomSkip()->consume(
        std::min(static_cast<int64_t>(skip), rowsSkipped));
  }
}

void SelectiveNimbleRowReader::restoreFilters() {
  // Restore filters that were removed during index bound conversion.
  // This is needed because the scan spec may be shared across multiple
  // split readers in multiple split execution modes.
  if (filtersToRestore_.empty()) {
    return;
  }
  NIMBLE_CHECK_NOT_NULL(
      tabletIndex_,
      "filtersToRestore_ should only be set when index bounds are used");
  for (auto& [columnName, filter] : filtersToRestore_) {
    auto* childSpec = options_.scanSpec()->childByName(columnName);
    if (childSpec != nullptr) {
      childSpec->setFilter(std::move(filter));
    }
  }
  filtersToRestore_.clear();
  options_.scanSpec()->resetCachedValues(/*doReorder=*/false);
}

void SelectiveNimbleRowReader::setAtEnd() {
  restoreFilters();
  nextRowNumber_ = kAtEnd;
}

void SelectiveNimbleRowReader::setStripeRowRange() {
  if (indexReader_ == nullptr) {
    return;
  }

  // For the first stripe, seek to the lower bound position
  if (currentStripe_ == startStripe_ &&
      encodedKeyBounds_->lowerKey.has_value()) {
    const auto startRow =
        indexReader_->seekAtOrAfter(encodedKeyBounds_->lowerKey.value());
    NIMBLE_CHECK(
        startRow.has_value(),
        "Failed to seek to lower bound key in stripe {}",
        currentStripe_);
    // Update current stripe row to start from the position at or after
    // lower bound
    rowInCurrentStripe_ = startRow.value();
    maybeUpdateRandomSkip(rowInCurrentStripe_);
  }

  // For the last stripe, set the end row based on upper bound
  if (currentStripe_ == endStripe_ - 1 &&
      encodedKeyBounds_->upperKey.has_value()) {
    const auto endRow =
        indexReader_->seekAtOrAfter(encodedKeyBounds_->upperKey.value());
    if (endRow.has_value()) {
      // Set the end row for the current stripe
      endRowInCurrentStripe_ = endRow.value();
      const auto numStripeRows =
          readerBase_->tablet().stripeRowCount(currentStripe_);
      // Accumulate rows skipped at the end of the last stripe.
      // Combined with trailingSkippedRows_ and updated in nextRowNumber()
      // when we reach kAtEnd.
      trailingSkippedRows_ += numStripeRows - endRow.value();
    }
  }

  // Skip to the starting row position within the stripe. This only happens when
  // cluster index bounds are set and the lower bound maps to a non-zero row
  // position within the first stripe.
  if (rowInCurrentStripe_ > 0 &&
      (!endRowInCurrentStripe_.has_value() ||
       endRowInCurrentStripe_.value() > rowInCurrentStripe_)) {
    columnReader_->seekTo(rowInCurrentStripe_, /*readsNullsOnly=*/false);
  }
}

class SelectiveNimbleReader : public dwio::common::Reader {
 public:
  SelectiveNimbleReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options)
      : readerBase_(ReaderBase::create(std::move(input), options)),
        options_(options) {
    detail::initHook();
  }

  std::optional<uint64_t> numberOfRows() const override;

  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t index) const override;

  const RowTypePtr& rowType() const override;

  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId()
      const override;

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options) const override;

 private:
  const std::shared_ptr<ReaderBase> readerBase_;
  const dwio::common::ReaderOptions options_;
};

std::optional<uint64_t> SelectiveNimbleReader::numberOfRows() const {
  return readerBase_->tablet().tabletRowCount();
}

std::unique_ptr<dwio::common::ColumnStatistics>
SelectiveNimbleReader::columnStatistics(uint32_t /*index*/) const {
  return nullptr;
}

const RowTypePtr& SelectiveNimbleReader::rowType() const {
  return readerBase_->fileSchema();
}

const std::shared_ptr<const dwio::common::TypeWithId>&
SelectiveNimbleReader::typeWithId() const {
  return readerBase_->fileSchemaWithId();
}

std::unique_ptr<dwio::common::RowReader> SelectiveNimbleReader::createRowReader(
    const dwio::common::RowReaderOptions& options) const {
  return std::make_unique<SelectiveNimbleRowReader>(readerBase_, options);
}

} // namespace

std::unique_ptr<dwio::common::Reader>
SelectiveNimbleReaderFactory::createReader(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options) {
  return std::make_unique<SelectiveNimbleReader>(std::move(input), options);
}

} // namespace facebook::nimble
