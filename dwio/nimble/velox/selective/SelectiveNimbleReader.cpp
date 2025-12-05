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
#include "dwio/nimble/index/KeyEncoder.h"
#include "dwio/nimble/index/KeyReader.h"
#include "dwio/nimble/tablet/FooterGenerated.h"
#include "dwio/nimble/velox/selective/ColumnReader.h"
#include "dwio/nimble/velox/selective/ReaderBase.h"
#include "dwio/nimble/velox/selective/RowSizeTracker.h"

namespace facebook::nimble {
namespace detail {

// Provide external initialization code for a selective reader instance.
void initHook();

} // namespace detail

using namespace facebook::velox;

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
    initIndexBounds();
    if (options.eagerFirstStripeLoad()) {
      nextRowNumber();
    }
  }

  int64_t nextRowNumber() override;

  int64_t nextReadSize(uint64_t size) override;

  uint64_t next(
      uint64_t size,
      VectorPtr& result,
      const dwio::common::Mutation* mutation) override;

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override {
    stats.skippedStrides += skippedStripes_;
  }

  void resetFilterCaches() override {
    if (columnReader_) {
      columnReader_->resetFilterCaches();
    }
  }

  std::optional<size_t> estimatedRowSize() const override;

  bool allPrefetchIssued() const final {
    return true;
  }

 private:
  /// Initializes the stripe range to read based on row offset bounds
  /// specified in options. Sets startStripe_ and endStripe_.
  void initReadRange();

  /// Initializes index bounds for key-based filtering. Sets up tabletIndex_,
  /// keyEncoder_, and encodedKeyBounds_ if cluster index bounds are specified
  /// in the scan spec.
  void initIndexBounds();

  /// Loads the current stripe by initializing streams and column readers.
  /// Also calls setStripeRowRange() to apply index-based row range filtering.
  void loadCurrentStripe();

  /// Returns true if cluster index bounds are specified for key-based
  /// filtering.
  bool hasIndexBounds() const;

  /// Sets the row range to read within the current stripe based on cluster
  /// index bounds. For the first stripe, adjusts currentRowInStripe_ to the
  /// lower bound position. For the last stripe, sets currentEndRowInStripe_ to
  /// the upper bound position.
  void setStripeRowRange(NimbleParams& params);

  /// Updates the start stripe based on the lower index bound.
  /// Uses TabletIndex lookup to find the first stripe containing keys at or
  /// after the lower bound. If no matching stripe is found, sets startStripe_
  /// to endStripe_ to skip reading.
  void updateStartStripeFromLowerIndexBound();

  /// Updates the end stripe based on the upper index bound.
  /// Uses TabletIndex lookup to find the last stripe containing keys at or
  /// before the upper bound. Sets endStripe_ to one past the matching stripe
  /// to create an exclusive upper bound.
  void updateEndStripeFromUpperIndexBound();

  const std::shared_ptr<ReaderBase> readerBase_;
  const dwio::common::RowReaderOptions options_;
  StripeStreams streams_;
  std::vector<int64_t> stripeRowOffsets_;
  // The inclusive lower bound of the stripe range to read.
  int32_t startStripe_;
  // The exclusive upper bound of the stripe range to read.
  int32_t endStripe_;
  TabletIndex* tabletIndex_;
  std::unique_ptr<KeyEncoder> keyEncoder_;
  std::optional<EncodedKeyBounds> encodedKeyBounds_;

  // The current stripe being read.
  int32_t currentStripe_;
  // The current row position within the current stripe (0-based).
  int64_t currentRowInStripe_;
  // Optional end row position for the current stripe, set when reading the
  // last stripe with upper index bounds.
  std::optional<int64_t> currentEndRowInStripe_;
  std::optional<int64_t> nextRowNumber_;
  int32_t skippedStripes_{0};
  std::unique_ptr<dwio::common::SelectiveColumnReader> columnReader_;
  dwio::common::ColumnReaderStatistics columnReaderStatistics_;
  std::unique_ptr<RowSizeTracker> rowSizeTracker_;
  std::unique_ptr<KeyReader> keyReader_;
};

bool SelectiveNimbleRowReader::hasIndexBounds() const {
  return tabletIndex_ != nullptr;
}

int64_t SelectiveNimbleRowReader::nextRowNumber() {
  if (nextRowNumber_.has_value()) {
    return *nextRowNumber_;
  }
  while (currentStripe_ < endStripe_) {
    auto numStripeRows = readerBase_->tablet().stripeRowCount(currentStripe_);
    NIMBLE_CHECK(!readerBase_->randomSkip() || !hasIndexBounds());
    if (currentRowInStripe_ == 0) {
      if (readerBase_->randomSkip() &&
          readerBase_->randomSkip()->nextSkip() >= numStripeRows) {
        readerBase_->randomSkip()->consume(numStripeRows);
        ++skippedStripes_;
        goto advanceToNextStripe;
      }
      loadCurrentStripe();
    }
    if (currentEndRowInStripe_.has_value()) {
      NIMBLE_CHECK_LE(currentEndRowInStripe_.value(), numStripeRows);
      numStripeRows = currentEndRowInStripe_.value();
    }
    if (currentRowInStripe_ < numStripeRows) {
      nextRowNumber_ = stripeRowOffsets_[currentStripe_] + currentRowInStripe_;
      return *nextRowNumber_;
    }
  advanceToNextStripe:
    ++currentStripe_;
    currentRowInStripe_ = 0;
    currentEndRowInStripe_.reset();
  }
  nextRowNumber_ = kAtEnd;
  return kAtEnd;
}

int64_t SelectiveNimbleRowReader::nextReadSize(uint64_t size) {
  VELOX_DCHECK_GT(size, 0);
  if (nextRowNumber() == kAtEnd) {
    return kAtEnd;
  }
  const auto rowsToRead = std::min<int64_t>(
      size,
      readerBase_->tablet().stripeRowCount(currentStripe_) -
          currentRowInStripe_);
  VELOX_DCHECK_GT(rowsToRead, 0);
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
  currentRowInStripe_ += rowsToRead;
  return rowsToRead;
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

void SelectiveNimbleRowReader::initReadRange() {
  const auto& tablet = readerBase_->tablet();
  startStripe_ = tablet.stripeCount();
  endStripe_ = 0;
  int64_t numRows = 0;
  stripeRowOffsets_.resize(tablet.stripeCount());
  const auto low = options_.offset();
  const auto high = options_.limit();
  for (int i = 0; i < tablet.stripeCount(); ++i) {
    stripeRowOffsets_[i] = numRows;
    if (low <= tablet.stripeOffset(i) && tablet.stripeOffset(i) < high) {
      startStripe_ = std::min(startStripe_, i);
      endStripe_ = std::max(endStripe_, i + 1);
    }
    numRows += tablet.stripeRowCount(i);
  }
  currentStripe_ = startStripe_;
  currentRowInStripe_ = 0;
}

void SelectiveNimbleRowReader::initIndexBounds() {
  NIMBLE_CHECK_NULL(tabletIndex_);
  // Early return if there are no stripes to read based on the read range
  if (currentStripe_ >= endStripe_) {
    return;
  }
  // Check if index bounds are specified in ScanSpec
  if (!options_.scanSpec()->indexBounds().has_value()) {
    return;
  }
  // Verify that the file has a cluster index
  if (!readerBase_->tablet().hasIndex()) {
    NIMBLE_FAIL("Index bounds specified but file does not have index");
  }

  // Load the cluster index from the file
  tabletIndex_ = readerBase_->tablet().index();
  NIMBLE_CHECK_NOT_NULL(tabletIndex_);

  const auto& indexBounds = options_.scanSpec()->indexBounds().value();
  const auto& keyColumns = tabletIndex_->indexColumns();
  for (size_t i = 0; i < keyColumns.size(); ++i) {
    NIMBLE_CHECK_EQ(
        keyColumns[i],
        indexBounds.indexColumns[i],
        "Index key column mismatch at position {}. TabletIndex key columns: [{}] vs ScanSpec index columns: [{}]",
        i,
        folly::join(", ", keyColumns),
        folly::join(", ", indexBounds.indexColumns));
  }

  keyEncoder_ = KeyEncoder::create(
      indexBounds.indexColumns,
      indexBounds.type(),
      std::vector<velox::core::SortOrder>{
          indexBounds.indexColumns.size(), velox::core::SortOrder{true, true}},
      readerBase_->pool());

  encodedKeyBounds_ = keyEncoder_->encodeIndexBounds(indexBounds);

  updateStartStripeFromLowerIndexBound();
  updateEndStripeFromUpperIndexBound();

  currentStripe_ = startStripe_;
}

void SelectiveNimbleRowReader::updateStartStripeFromLowerIndexBound() {
  NIMBLE_CHECK(encodedKeyBounds_.has_value());
  if (!encodedKeyBounds_->lowerKey.has_value()) {
    return;
  }
  // Lookup the first stripe that matches the lower bound
  const auto lowerLocation =
      tabletIndex_->lookup(encodedKeyBounds_->lowerKey.value());
  if (lowerLocation.has_value()) {
    startStripe_ =
        std::max(startStripe_, static_cast<int>(lowerLocation->stripeIndex));
    return;
  }
  if (encodedKeyBounds_->lowerKey.value() > tabletIndex_->maxKey()) {
    // If the lower bound key is greater than the max key, skip all the stripes
    // entirely.
    startStripe_ = endStripe_;
  }
}

void SelectiveNimbleRowReader::updateEndStripeFromUpperIndexBound() {
  NIMBLE_CHECK(encodedKeyBounds_.has_value());
  if (!encodedKeyBounds_->upperKey.has_value()) {
    return;
  }
  const auto& upperKey = encodedKeyBounds_->upperKey.value();
  // Lookup the last stripe that matches the upper bound
  const auto upperLocation = tabletIndex_->lookup(upperKey);
  if (upperLocation.has_value()) {
    endStripe_ =
        std::max(endStripe_, static_cast<int>(upperLocation->stripeIndex + 1));
    return;
  }
  if (encodedKeyBounds_->upperKey.value() <= tabletIndex_->minKey()) {
    // If the upper bound key is less than the min key, skip all the stripes
    // entirely.
    endStripe_ = startStripe_;
  }
}

void SelectiveNimbleRowReader::loadCurrentStripe() {
  streams_.setStripe(currentStripe_);
  NimbleParams params(
      *readerBase_->pool(),
      columnReaderStatistics_,
      readerBase_->nimbleSchema(),
      streams_,
      options_.trackRowSize() ? rowSizeTracker_.get() : nullptr,
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
  setStripeRowRange(params);
  streams_.load();
}

std::unique_ptr<KeyReader> buildKeyReader(NimbleParams& params) {
  return KeyReader::create(
      params.streams().enqueue(
          params.streams().stripeIndexGroup()->keyStreamId()),
      params.streams().stripeIndex(),
      params.streams().stripeGroupIndex(),
      params.pool());
}

void SelectiveNimbleRowReader::setStripeRowRange(NimbleParams& params) {
  if (!hasIndexBounds()) {
    return;
  }
  if ((currentStripe_ != startStripe_) && (currentStripe_ != endStripe_ - 1)) {
    return;
  }

  keyReader_ = buildKeyReader(params);
  // For the first stripe, seek to the lower bound position
  if (currentStripe_ == startStripe_ &&
      encodedKeyBounds_->lowerKey.has_value()) {
    bool exactMatch{false};
    const auto startRow = keyReader_->seekAtOrAfter(
        encodedKeyBounds_->lowerKey.value(), exactMatch);
    NIMBLE_CHECK(startRow.has_value());
    // Update current stripe row to start from the position at or after
    // lower bound
    currentRowInStripe_ = startRow.value();
  }

  // For the last stripe, set the end row based on upper bound
  if (currentStripe_ == endStripe_ - 1 &&
      encodedKeyBounds_->upperKey.has_value()) {
    bool exactMatch = false;
    const auto endRow = keyReader_->seekAtOrAfter(
        encodedKeyBounds_->upperKey.value(), exactMatch);
    if (endRow.has_value()) {
      // Set the end row for the current stripe
      currentEndRowInStripe_ = endRow.value();
    }
  }
  if (currentRowInStripe_ > 0 &&
      (!currentEndRowInStripe_.has_value() ||
       currentEndRowInStripe_.value() > currentRowInStripe_)) {
    // Skip the column reader to the starting position
    columnReader_->skip(currentRowInStripe_);
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

  std::optional<uint64_t> numberOfRows() const override {
    return readerBase_->tablet().tabletRowCount();
  }

  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t /*index*/) const override {
    return nullptr;
  }

  const RowTypePtr& rowType() const override {
    return readerBase_->fileSchema();
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId()
      const override {
    return readerBase_->fileSchemaWithId();
  }

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options) const override {
    return std::make_unique<SelectiveNimbleRowReader>(readerBase_, options);
  }

 private:
  const std::shared_ptr<ReaderBase> readerBase_;
  const dwio::common::ReaderOptions options_;
};

} // namespace

std::unique_ptr<dwio::common::Reader>
SelectiveNimbleReaderFactory::createReader(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options) {
  return std::make_unique<SelectiveNimbleReader>(std::move(input), options);
}

} // namespace facebook::nimble
