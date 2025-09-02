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
#include "dwio/nimble/velox/selective/ColumnReader.h"
#include "dwio/nimble/velox/selective/ReaderBase.h"

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
      : readerBase_(readerBase), options_(options), streams_(readerBase_) {
    initReadRange();
    if (options.eagerFirstStripeLoad()) {
      nextRowNumber();
    }
  }

  int64_t nextRowNumber() override {
    if (nextRowNumber_.has_value()) {
      return *nextRowNumber_;
    }
    while (currentStripe_ < lastStripe_) {
      auto numStripeRows = readerBase_->tablet().stripeRowCount(currentStripe_);
      if (currentRowInStripe_ == 0) {
        if (readerBase_->randomSkip() &&
            readerBase_->randomSkip()->nextSkip() >= numStripeRows) {
          readerBase_->randomSkip()->consume(numStripeRows);
          ++skippedStripes_;
          goto advanceToNextStripe;
        }
        loadCurrentStripe();
      }
      if (currentRowInStripe_ < numStripeRows) {
        nextRowNumber_ =
            firstRowOfStripe_[currentStripe_] + currentRowInStripe_;
        return *nextRowNumber_;
      }
    advanceToNextStripe:
      ++currentStripe_;
      currentRowInStripe_ = 0;
    }
    nextRowNumber_ = kAtEnd;
    return kAtEnd;
  }

  int64_t nextReadSize(uint64_t size) override {
    VELOX_DCHECK_GT(size, 0);
    if (nextRowNumber() == kAtEnd) {
      return kAtEnd;
    }
    auto rowsToRead = std::min<int64_t>(
        size,
        readerBase_->tablet().stripeRowCount(currentStripe_) -
            currentRowInStripe_);
    VELOX_DCHECK_GT(rowsToRead, 0);
    return rowsToRead;
  }

  uint64_t next(
      uint64_t size,
      VectorPtr& result,
      const dwio::common::Mutation* mutation) override {
    const auto rowsToRead = nextReadSize(size);
    if (rowsToRead == kAtEnd) {
      return 0;
    }
    columnReader_->setCurrentRowNumber(nextRowNumber());
    if (options_.rowNumberColumnInfo().has_value()) {
      readWithRowNumber(
          columnReader_,
          options_,
          nextRowNumber(),
          rowsToRead,
          mutation,
          result);
    } else {
      columnReader_->next(rowsToRead, result, mutation);
    }
    nextRowNumber_.reset();
    currentRowInStripe_ += rowsToRead;
    return rowsToRead;
  }

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override {
    stats.skippedStrides += skippedStripes_;
  }

  void resetFilterCaches() override {
    if (columnReader_) {
      columnReader_->resetFilterCaches();
    }
  }

  std::optional<size_t> estimatedRowSize() const override {
    // TODO: Use column statistics once ready.
    if (const_cast<SelectiveNimbleRowReader*>(this)->nextRowNumber() ==
        kAtEnd) {
      return std::nullopt;
    }
    size_t byteSize, rowCount;
    if (!columnReader_->estimateMaterializedSize(byteSize, rowCount)) {
      return 1 << 20;
    }
    return rowCount == 0 ? 0 : byteSize / rowCount;
  }

  bool allPrefetchIssued() const final {
    return true;
  }

 private:
  void initReadRange() {
    auto& tablet = readerBase_->tablet();
    currentStripe_ = tablet.stripeCount();
    lastStripe_ = 0;
    int64_t numRows = 0;
    firstRowOfStripe_.resize(tablet.stripeCount());
    const auto low = options_.offset(), high = options_.limit();
    for (int i = 0; i < tablet.stripeCount(); ++i) {
      firstRowOfStripe_[i] = numRows;
      if (low <= tablet.stripeOffset(i) && tablet.stripeOffset(i) < high) {
        currentStripe_ = std::min(currentStripe_, i);
        lastStripe_ = std::max(lastStripe_, i + 1);
      }
      numRows += tablet.stripeRowCount(i);
    }
    currentRowInStripe_ = 0;
  }

  void loadCurrentStripe() {
    streams_.setStripe(currentStripe_);
    NimbleParams params(
        *readerBase_->memoryPool(),
        columnReaderStatistics_,
        readerBase_->nimbleSchema(),
        streams_,
        options_.preserveFlatMapsInMemory());
    columnReader_ = buildColumnReader(
        options_.requestedType() ? options_.requestedType()
                                 : readerBase_->fileSchema(),
        readerBase_->fileSchemaWithId(),
        params,
        *options_.scanSpec(),
        true);
    columnReader_->setIsTopLevel();
    streams_.load();
  }

  std::shared_ptr<ReaderBase> const readerBase_;
  const dwio::common::RowReaderOptions options_;
  StripeStreams streams_;
  std::vector<int64_t> firstRowOfStripe_;
  int currentStripe_;
  int lastStripe_;
  int64_t currentRowInStripe_;
  std::optional<int64_t> nextRowNumber_;
  int skippedStripes_ = 0;
  std::unique_ptr<dwio::common::SelectiveColumnReader> columnReader_;
  dwio::common::ColumnReaderStatistics columnReaderStatistics_;
};

class SelectiveNimbleReader : public dwio::common::Reader {
 public:
  SelectiveNimbleReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options)
      : readerBase_(std::make_shared<ReaderBase>(std::move(input), options)),
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
  std::shared_ptr<ReaderBase> const readerBase_;
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
