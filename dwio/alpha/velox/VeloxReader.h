// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <optional>
#include "dwio/alpha/common/MetricsLogger.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/common/Vector.h"
#include "dwio/alpha/tablet/Tablet.h"
#include "dwio/alpha/velox/FieldReader.h"
#include "dwio/alpha/velox/SchemaReader.h"
#include "dwio/alpha/velox/StreamInputDecoder.h"
#include "dwio/alpha/velox/StreamLabels.h"
#include "folly/container/F14Set.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/ExecutorBarrier.h"
#include "velox/dwio/common/FlatMapHelper.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

// The VeloxReader reads (a projection from) a file into a velox VectorPtr.
// The current implementation only uses FlatVector rather than any of the
// fancier vectors (dictionary, etc), and only supports the types needed for ML.

namespace facebook::alpha {

struct VeloxReadParams : public FieldReaderParams {
  uint64_t fileRangeStartOffset = 0;
  uint64_t fileRangeEndOffset = std::numeric_limits<uint64_t>::max();

  // Optional reader decoding executor. When supplied, decoding into a Velox
  // vector will be parallelized by this executor, if the column type supports
  // parallel decoding.
  std::shared_ptr<folly::Executor> decodingExecutor;

  // Metric logger with pro-populated access info.
  std::shared_ptr<MetricsLogger> metricsLogger;
};

class VeloxReader {
 public:
  VeloxReader(
      velox::memory::MemoryPool& pool,
      velox::ReadFile* file,
      std::shared_ptr<const velox::dwio::common::ColumnSelector> selector =
          nullptr,
      VeloxReadParams params = {});

  VeloxReader(
      velox::memory::MemoryPool& pool,
      std::shared_ptr<velox::ReadFile> file,
      std::shared_ptr<const velox::dwio::common::ColumnSelector> selector =
          nullptr,
      VeloxReadParams params = {});

  VeloxReader(
      velox::memory::MemoryPool& pool,
      std::shared_ptr<const Tablet> tablet,
      std::shared_ptr<const velox::dwio::common::ColumnSelector> selector =
          nullptr,
      VeloxReadParams params = {});

  ~VeloxReader();

  // Fills |result| with up to rowCount new rows, returning whether any new rows
  // were read. |result| may be nullptr, in which case it will be allocated via
  // pool_. If it is not nullptr its type must match type_.
  bool next(uint64_t rowCount, velox::VectorPtr& result);

  const Tablet& getTabletView() const;

  const std::shared_ptr<const velox::RowType>& getType() const;

  const std::shared_ptr<const Type>& schema() const;

  const std::map<std::string, std::string>& metadata() const;

  velox::memory::MemoryPool& getMemoryPool() const {
    return pool_;
  }

  // Seeks to |rowNumber| from the beginning of the file (row 0).
  // If |rowNumber| is greater than the number of rows in the file, the
  // seek will stop at the end of file and following reads will return
  // false.
  // Returns the current row number the reader is pointing to. If
  // |rowNumber| is greater than the number of rows in the file, this will
  // return the last row number.
  uint64_t seekToRow(uint64_t rowNumber);

  // Skips |numberOfRowsToSkip| rows from current row index.
  // If |numberOfRowsToSkip| is greater than the remaining rows in the
  // file, skip will stop at the end of file, and following reads will
  // return false.
  // Returns the number of rows skipped. If |numberOfRowsToSkip| is
  // greater than the remaining rows in the file, this will return the
  // total number of rows skipped, until reaching the end of file.
  uint64_t skipRows(uint64_t numberOfRowsToSkip);

  // Loads the next stripe if any
  void loadStripeIfAny();

 private:
  // Loads the next stripe's streams.
  void loadStripe();

  // True if the file contain zero rows.
  bool isEmptyFile() const {
    return ((lastRow_ - firstRow_) == 0);
  }

  // Skips |rowsToSkip| in the currently loaded stripe. |rowsToSkip| must be
  // less than rowsRemaining_.
  void skipInCurrentStripe(uint64_t rowsToSkip);

  // Skips over multiple stripes, starting from a stripe with index
  // |startStripeIndex|. Keeps skipping stripes for as long as remaining rows to
  // skip is greater than the next stripe's row count.
  // This method does not load the last stripe, or skips inside the last stripe.
  // Returns the total number of rows skipped.
  uint64_t skipStripes(uint32_t startStripeIndex, uint64_t rowsToSkip);

  static const std::vector<std::string>& preloadedOptionalSections();

  velox::memory::MemoryPool& pool_;
  std::shared_ptr<const Tablet> tablet_;
  const VeloxReadParams parameters_;
  std::shared_ptr<const Type> schema_;
  StreamLabels streamLabels_;
  std::shared_ptr<const velox::RowType> type_;
  std::vector<uint32_t> offsets_;
  folly::F14FastMap<offset_size, std::unique_ptr<Decoder>> decoders_;
  std::unique_ptr<FieldReaderFactory> rootFieldReaderFactory_;
  std::unique_ptr<FieldReader> rootReader_;
  mutable std::optional<std::map<std::string, std::string>> metadata_;
  uint32_t firstStripe_;
  uint32_t lastStripe_;
  uint64_t firstRow_;
  uint64_t lastRow_;

  // Reading state for reader
  uint32_t nextStripe_ = 0;
  uint64_t rowsRemainingInStripe_ = 0;
  // stripe currently loaded. Initially state is no stripe loaded (INT_MAX)
  uint32_t loadedStripe_ = std::numeric_limits<uint32_t>::max();

  std::unique_ptr<velox::dwio::common::ExecutorBarrier> barrier_;
  // Right now each reader is considered its own session if not passed from
  // writer option.
  std::shared_ptr<MetricsLogger> logger_;

  friend class VeloxReaderHelper;
};

} // namespace facebook::alpha
