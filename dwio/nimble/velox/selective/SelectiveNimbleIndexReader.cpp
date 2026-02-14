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

#include "dwio/nimble/velox/selective/SelectiveNimbleIndexReader.h"

#include <utility>

#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/legacy/EncodingFactory.h"
#include "dwio/nimble/index/IndexReader.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "dwio/nimble/velox/selective/ColumnReader.h"
#include "dwio/nimble/velox/selective/ReaderBase.h"
#include "dwio/nimble/velox/selective/RowSizeTracker.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/serializers/KeyEncoder.h"

namespace facebook::nimble {

using namespace facebook::velox;

namespace {

// Converts index column names from nimble schema (internal file names) to
// file schema (user-facing names).
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

size_t SelectiveNimbleIndexReader::RowRangeHash::operator()(
    const RowRange& range) const {
  return folly::hash::hash_combine(range.startRow, range.endRow);
}

void SelectiveNimbleIndexReader::RequestState::incPendingStripes() {
  ++pendingStripes;
}

void SelectiveNimbleIndexReader::RequestState::decPendingStripes() {
  VELOX_CHECK_GT(pendingStripes, 0);
  --pendingStripes;
}

std::string SelectiveNimbleIndexReader::ReadSegment::toString() const {
  std::string requestIndicesStr;
  for (size_t i = 0; i < requestIndices.size(); ++i) {
    if (i > 0) {
      requestIndicesStr += ", ";
    }
    requestIndicesStr += std::to_string(requestIndices[i]);
  }
  return fmt::format(
      "ReadSegment{{rowRange: {}, requestIndices: [{}]}}",
      rowRange.toString(),
      requestIndicesStr);
}

void SelectiveNimbleIndexReader::OutputChunk::dropRef() {
  NIMBLE_CHECK_GT(refCount, 0);
  if (--refCount == 0) {
    data.reset();
  }
}

SelectiveNimbleIndexReader::SelectiveNimbleIndexReader(
    std::shared_ptr<ReaderBase> readerBase,
    const dwio::common::RowReaderOptions& options)
    : readerBase_(std::move(readerBase)),
      options_(options),
      rowSizeTracker_(
          std::make_unique<RowSizeTracker>(readerBase_->fileSchemaWithId())),
      hasFilters_(options.scanSpec()->hasFilter()),
      outputType_(
          options.requestedType() ? options.requestedType()
                                  : readerBase_->fileSchema()),
      tabletIndex_{readerBase_->tablet().index()},
      indexColumns_{convertIndexColumnsToFileSchema(
          tabletIndex_->indexColumns(),
          readerBase_->nimbleSchema(),
          readerBase_->fileSchema())},
      streams_{readerBase_} {
  initReadRange();
}

void SelectiveNimbleIndexReader::startLookup(
    const velox::serializer::IndexBounds& indexBounds,
    const Options& options) {
  reset();

  numRequests_ = indexBounds.numRows();
  // Encode the index bounds.
  encodedKeyBounds_ = encodeIndexBounds(indexBounds);
  VELOX_CHECK_EQ(
      encodedKeyBounds_.size(),
      numRequests_,
      "Encoded key bounds size mismatch");

  // Cache options for use during output tracking.
  requestOptions_ = options;

  // Build stripe to request mapping.
  mapRequestsToStripes();

  // Initialize request output tracking.
  nextOutputRequest_ = 0;
  lastReadyOutputRequest_ = -1;
  readyOutputRows_ = 0;

  // Report lookup stats.
  addThreadLocalRuntimeStat(
      dwio::common::IndexReader::kNumIndexLookupRequests,
      velox::RuntimeCounter(numRequests_));
  addThreadLocalRuntimeStat(
      dwio::common::IndexReader::kNumIndexLookupStripes,
      velox::RuntimeCounter(stripes_.size()));
}

bool SelectiveNimbleIndexReader::hasNext() const {
  return numRequests_ > 0 &&
      (stripeIndex_ < stripes_.size() || readyOutputRows_ > 0 ||
       nextOutputRequest_ <= lastReadyOutputRequest_);
}

std::unique_ptr<connector::IndexSource::Result>
SelectiveNimbleIndexReader::next(vector_size_t maxOutputRows) {
  NIMBLE_CHECK_GT(numRequests_, 0, "No lookup in progress");

  // Process stripes until we can produce output or finish.
  while (stripeIndex_ < stripes_.size() && !canProduceOutput(maxOutputRows)) {
    if (!loadStripe()) {
      continue;
    }
    NIMBLE_CHECK(stripeLoaded_);

    // Process segments within this stripe.
    while (readSegmentIndex_ < readSegments_.size() &&
           !canProduceOutput(maxOutputRows)) {
      auto& segment = readSegments_[readSegmentIndex_];
      RowVectorPtr segmentOutput;
      const uint64_t readRows = readStripeFragment(segmentOutput);
      VELOX_CHECK_EQ(readRows, segment.rowRange.numRows());
      addStripeSegmentOutput(segment, segmentOutput);
      advanceStripeReadSegment();
    }

    advanceStripe();
  }

  return produceOutput();
}

void SelectiveNimbleIndexReader::initReadRange() {
  const auto& tablet = readerBase_->tablet();
  // Index reader always reads from the entire file.
  numStripes_ = tablet.stripeCount();
  int64_t numRows = 0;
  stripeRowOffsets_.resize(numStripes_);
  // Sanity check: options should cover the entire file.
  const auto low = options_.offset();
  const auto high = options_.limit();
  for (int i = 0; i < numStripes_; ++i) {
    NIMBLE_CHECK(
        low <= tablet.stripeOffset(i) && tablet.stripeOffset(i) < high,
        "Index reader requires options to cover the entire file, but stripe {} "
        "at offset {} is outside range [{}, {})",
        i,
        tablet.stripeOffset(i),
        low,
        high);
    stripeRowOffsets_[i] = numRows;
    numRows += tablet.stripeRowCount(i);
  }
}

std::vector<velox::serializer::EncodedKeyBounds>
SelectiveNimbleIndexReader::encodeIndexBounds(
    const velox::serializer::IndexBounds& indexBounds) {
  NIMBLE_CHECK_NOT_NULL(tabletIndex_);
  if (keyEncoder_ == nullptr) {
    const auto& sortOrders = tabletIndex_->sortOrders();
    keyEncoder_ = velox::serializer::KeyEncoder::create(
        indexBounds.indexColumns,
        asRowType(indexBounds.type()),
        toVeloxSortOrders(sortOrders, indexBounds.indexColumns.size()),
        readerBase_->pool());
  }
  return keyEncoder_->encodeIndexBounds(indexBounds);
}

void SelectiveNimbleIndexReader::mapRequestsToStripes() {
  stripes_.clear();
  stripeToRequests_.clear();
  stripeIndex_ = 0;
  requestStates_.resize(numRequests_);

  // Early exit if the index is empty (no stripes).
  if (tabletIndex_->empty()) {
    return;
  }

  const auto& tablet = readerBase_->tablet();
  for (size_t requestIdx = 0; requestIdx < numRequests_; ++requestIdx) {
    const auto& bounds = encodedKeyBounds_[requestIdx];
    std::vector<uint32_t> requestStripes;

    // Determine start stripe from lower bound.
    uint32_t startStripe = 0;
    if (bounds.lowerKey.has_value()) {
      const auto lowerLocation = tabletIndex_->lookup(bounds.lowerKey.value());
      if (lowerLocation.has_value()) {
        startStripe = static_cast<uint32_t>(lowerLocation->stripeIndex);
      } else if (bounds.lowerKey.value() > tabletIndex_->maxKey()) {
        continue; // No stripes match.
      }
    }

    // Determine end stripe from upper bound.
    uint32_t endStripe = numStripes_;
    if (bounds.upperKey.has_value()) {
      const auto upperLocation = tabletIndex_->lookup(bounds.upperKey.value());
      if (upperLocation.has_value()) {
        endStripe = std::min(
            endStripe, static_cast<uint32_t>(upperLocation->stripeIndex) + 1);
      } else if (bounds.upperKey.value() < tabletIndex_->minKey()) {
        continue; // No stripes match.
      }
    }
    VELOX_CHECK_LT(startStripe, endStripe);

    // Add stripes to the mapping.
    // For no-filter case, we can prune stripes based on pre-filter row counts
    // since input rows == output rows. However, we only consider middle stripes
    // (not first or last) for truncation, because first and last stripes may be
    // partial and their actual row counts are unknown until lookupRowRanges().
    // For filter case, we cannot prune stripes since we don't know output rows
    // ahead of time. Instead, truncation based on actual output rows happens
    // during output tracking in trackStripeSegmentOutputRefs().
    uint64_t fullStripeRows = 0;
    for (uint32_t stripe = startStripe; stripe < endStripe; ++stripe) {
      stripeToRequests_[stripe].push_back(requestIdx);
      requestStates_[requestIdx].incPendingStripes();

      // Only consider middle stripes for truncation since first and last
      // stripes may be partial (unknown row counts until lookupRowRanges).

      if (!hasFilters_ && requestOptions_.maxRowsPerRequest > 0) {
        const bool isMiddleStripe =
            stripe != startStripe && stripe != endStripe - 1;
        fullStripeRows += isMiddleStripe ? tablet.stripeRowCount(stripe) : 0;
        if (fullStripeRows >= requestOptions_.maxRowsPerRequest) {
          break;
        }
      }
    }
  }

  // Collect and sort unique stripes.
  stripes_.reserve(stripeToRequests_.size());
  for (const auto& [stripe, _] : stripeToRequests_) {
    stripes_.push_back(stripe);
  }
  std::sort(stripes_.begin(), stripes_.end());
}

bool SelectiveNimbleIndexReader::loadStripe() {
  if (stripeLoaded_) {
    return true;
  }
  // Skip stripes if all requests have been removed due to stripe pruning.
  while (stripeIndex_ < stripes_.size()) {
    const uint32_t stripeIndex = stripes_[stripeIndex_];
    auto it = stripeToRequests_.find(stripeIndex);
    VELOX_CHECK(it != stripeToRequests_.end());
    if (it->second.empty()) {
      VELOX_CHECK_GT(requestOptions_.maxRowsPerRequest, 0);
      ++stripeIndex_;
      continue;
    }
    prepareStripeReading(stripeIndex);
    if (readSegments_.empty()) {
      ++stripeIndex_;
      continue;
    }
    stripeLoaded_ = true;
    return true;
  }
  return false;
}

void SelectiveNimbleIndexReader::prepareStripeReading(uint32_t stripeIndex) {
  readSegments_.clear();
  readSegmentIndex_ = 0;

  auto it = stripeToRequests_.find(stripeIndex);
  NIMBLE_CHECK(it != stripeToRequests_.end());
  NIMBLE_CHECK(!it->second.empty());

  auto& requestRows = it->second;

  // Collect encoded bounds for requests in this stripe.
  std::vector<velox::serializer::EncodedKeyBounds> stripeKeyBounds;
  stripeKeyBounds.reserve(requestRows.size());
  for (vector_size_t requestRow : requestRows) {
    stripeKeyBounds.push_back(encodedKeyBounds_[requestRow]);
  }

  // Load the stripe and build index reader.
  loadStripeWithIndex(stripeIndex);

  // Get row ranges for all requests.
  auto stripeRowRanges = lookupRowRanges(stripeIndex, stripeKeyBounds);
  NIMBLE_CHECK_EQ(stripeRowRanges.size(), requestRows.size());

  // Filter out empty ranges and store per-request row ranges.
  // For no-filter case, also truncate row ranges based on
  // requestOptions_.maxRowsPerRequest since input rows == output rows.
  std::vector<std::pair<vector_size_t, RowRange>> requestRanges;
  requestRanges.reserve(requestRows.size());
  for (size_t i = 0; i < requestRows.size(); ++i) {
    auto& state = requestStates_[requestRows[i]];
    VELOX_CHECK_GT(state.pendingStripes, 0);
    auto range = stripeRowRanges[i];
    if (range.empty()) {
      // Decrement pending stripes for requests with empty row ranges.
      state.decPendingStripes();
      continue;
    }

    // For no-filter case, truncate row range if accumulated output would
    // exceed requestOptions_.maxRowsPerRequest. This avoids reading
    // unnecessary data.
    if (!hasFilters_ && requestOptions_.maxRowsPerRequest > 0) {
      VELOX_CHECK_GT(requestOptions_.maxRowsPerRequest, state.outputRows);
      const auto remainingRows =
          requestOptions_.maxRowsPerRequest - state.outputRows;
      const auto rangeRows = range.numRows();
      if (rangeRows >= static_cast<vector_size_t>(remainingRows)) {
        // Truncate the row range to fit within the limit.
        range.endRow = range.startRow + remainingRows;
        // All the rows will be processed within this stripe, so remove the
        // request from subsequent stripes if there are any.
        removeRequestFromSubsequentStripes(requestRows[i], stripeIndex_ + 1);
      }
    }

    requestRanges.emplace_back(requestRows[i], range);
    requestStates_[requestRows[i]].rowRange = std::move(range);
  }
  // Update ready output requests after decrementing pending stripes for empty
  // row ranges. This ensures requests with no matching rows in this stripe
  // can still be marked as ready if all their stripes have been processed.
  updateReadyOutputRequests();

  // Update requestRows to only contain non-empty requests.
  requestRows.clear();
  for (const auto& [requestIdx, _] : requestRanges) {
    requestRows.push_back(requestIdx);
  }

  if (requestRows.empty()) {
    return;
  }

  buildStripeReadSegments(requestRanges);
}

void SelectiveNimbleIndexReader::buildStripeReadSegments(
    const std::vector<std::pair<vector_size_t, RowRange>>& requestRanges) {
  if (!hasFilters_) {
    mergeStripeRowRanges(requestRanges);
  } else {
    splitStripeRowRanges(requestRanges);
  }
  VELOX_CHECK(!readSegments_.empty());

  // Report read segment stats.
  addThreadLocalRuntimeStat(
      dwio::common::IndexReader::kNumIndexLookupReadSegments,
      velox::RuntimeCounter(readSegments_.size()));

  readSegmentIndex_ = 0;
  prepareStripeSegmentRead();
}

void SelectiveNimbleIndexReader::mergeStripeRowRanges(
    const std::vector<std::pair<vector_size_t, RowRange>>& requestRanges) {
  VELOX_CHECK(!hasFilters_);
  // No filter case: merge row ranges for efficient reading with seek.
  // Each request will reference the appropriate portion of the merged read.

  // Sort ranges by start row.
  std::vector<RowRange> sortedRanges;
  sortedRanges.reserve(requestRanges.size());
  for (const auto& [_, range] : requestRanges) {
    sortedRanges.push_back(range);
  }
  std::sort(
      sortedRanges.begin(),
      sortedRanges.end(),
      [](const auto& a, const auto& b) { return a.startRow < b.startRow; });

  // Merge overlapping ranges.
  std::vector<RowRange> mergedRanges;
  RowRange current = sortedRanges[0];
  for (size_t i = 1; i < sortedRanges.size(); ++i) {
    const auto& range = sortedRanges[i];
    if (range.startRow <= current.endRow) {
      current.endRow = std::max(current.endRow, range.endRow);
    } else {
      mergedRanges.push_back(current);
      current = range;
    }
  }
  mergedRanges.push_back(current);

  // Create one read segment per merged range.
  // Each segment references all requests that overlap with it.
  for (const auto& mergedRange : mergedRanges) {
    ReadSegment segment;
    segment.rowRange = mergedRange;
    for (const auto& [requestIdx, range] : requestRanges) {
      // Check if merged range contains the request range.
      if (mergedRange.contains(range)) {
        segment.requestIndices.push_back(requestIdx);
      }
    }
    // Each merged range must have at least one contained request.
    NIMBLE_CHECK(
        !segment.requestIndices.empty(),
        "Merged range [{}, {}) has no contained requests",
        mergedRange.startRow,
        mergedRange.endRow);
    readSegments_.push_back(std::move(segment));
  }
}

void SelectiveNimbleIndexReader::splitStripeRowRanges(
    const std::vector<std::pair<vector_size_t, RowRange>>& requestRanges) {
  VELOX_CHECK(hasFilters_);
  // Filter case: split overlapping ranges into non-overlapping segments.
  // Example: [0,100) and [50,150) -> [0,50), [50,100), [100,150)
  // Each segment is read separately, and requests reference their segments.

  // Collect all unique boundary points.
  std::vector<vector_size_t> boundaries;
  boundaries.reserve(requestRanges.size() * 2);
  for (const auto& [_, range] : requestRanges) {
    boundaries.push_back(range.startRow);
    boundaries.push_back(range.endRow);
  }
  std::sort(boundaries.begin(), boundaries.end());
  boundaries.erase(
      std::unique(boundaries.begin(), boundaries.end()), boundaries.end());

  // Create segments from consecutive boundary points.
  for (size_t i = 0; i + 1 < boundaries.size(); ++i) {
    RowRange segmentRange{boundaries[i], boundaries[i + 1]};
    if (segmentRange.empty()) {
      continue;
    }

    // Find which requests contain this segment.
    std::vector<vector_size_t> containingRequests;
    for (const auto& [requestIdx, range] : requestRanges) {
      if (range.contains(segmentRange)) {
        containingRequests.push_back(requestIdx);
      }
    }

    if (!containingRequests.empty()) {
      ReadSegment segment;
      segment.rowRange = segmentRange;
      segment.requestIndices = std::move(containingRequests);
      readSegments_.push_back(std::move(segment));
    }
  }
}

void SelectiveNimbleIndexReader::loadStripeWithIndex(uint32_t stripeIndex) {
  addThreadLocalRuntimeStat(
      dwio::common::RowReader::kNumStripeLoads, velox::RuntimeCounter(1));

  streams_.setStripe(stripeIndex, /*loadIndex=*/true);
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
        return EncodingFactory::decode(pool, data, std::move(stringBufferFactory));
      }
          : [](velox::memory::MemoryPool& pool,
               std::string_view data,
               std::function<void*(uint32_t)> stringBufferFactory)
                -> std::unique_ptr<Encoding> {
        return legacy::EncodingFactory::decode(pool, data, std::move(stringBufferFactory));
      },
      options_.passStringBuffersFromDecoder(),
      options_.preserveFlatMapsInMemory());

  columnReader_ = buildColumnReader(
      outputType_,
      readerBase_->fileSchemaWithId(),
      params,
      *options_.scanSpec(),
      true);
  rowSizeTracker_->finalizeProjection();
  columnReader_->setIsTopLevel();

  // Build index reader.
  indexReader_ = index::IndexReader::create(
      params.streams().enqueueKeyStream(),
      params.streams().stripeIndex(),
      params.streams().indexGroup(),
      &params.pool());

  streams_.load();
}

std::vector<SelectiveNimbleIndexReader::RowRange>
SelectiveNimbleIndexReader::lookupRowRanges(
    uint32_t stripeIndex,
    const std::vector<velox::serializer::EncodedKeyBounds>& keyBounds) {
  NIMBLE_CHECK_NOT_NULL(indexReader_);

  const auto numStripeRows = static_cast<velox::vector_size_t>(
      readerBase_->tablet().stripeRowCount(stripeIndex));
  std::vector<RowRange> result;
  result.reserve(keyBounds.size());

  for (const auto& bounds : keyBounds) {
    velox::vector_size_t startRow = 0;
    if (bounds.lowerKey.has_value()) {
      const auto lowerRow =
          indexReader_->seekAtOrAfter(bounds.lowerKey.value());
      if (lowerRow.has_value()) {
        startRow = static_cast<velox::vector_size_t>(lowerRow.value());
      } else {
        // No rows match the lower bound and skip the request.
        result.emplace_back();
        continue;
      }
    }

    velox::vector_size_t endRow = numStripeRows;
    if (bounds.upperKey.has_value()) {
      const auto upperRow =
          indexReader_->seekAtOrAfter(bounds.upperKey.value());
      if (upperRow.has_value()) {
        endRow = static_cast<velox::vector_size_t>(upperRow.value());
      }
    }
    result.emplace_back(startRow, endRow);
  }
  return result;
}

uint64_t SelectiveNimbleIndexReader::readStripeFragment(RowVectorPtr& output) {
  if (readSegmentIndex_ >= readSegments_.size()) {
    return 0;
  }

  const auto& segment = readSegments_[readSegmentIndex_];
  const auto rowsToRead = segment.rowRange.endRow - segment.rowRange.startRow;
  VELOX_CHECK_GT(rowsToRead, 0);

  VectorPtr readOutput =
      BaseVector::create(outputType_, 0, readerBase_->pool());
  columnReader_->next(rowsToRead, readOutput, /*mutation=*/nullptr);
  if (readOutput->size() > 0) {
    readOutput->loadedVector();
  }
  output = checkedPointerCast<RowVector>(readOutput);
  return rowsToRead;
}

void SelectiveNimbleIndexReader::advanceStripeReadSegment() {
  ++readSegmentIndex_;
  prepareStripeSegmentRead();
}

void SelectiveNimbleIndexReader::prepareStripeSegmentRead() {
  if (readSegmentIndex_ < readSegments_.size()) {
    columnReader_->seekTo(
        readSegments_[readSegmentIndex_].rowRange.startRow,
        /*readsNullsOnly=*/false);
  }
}

void SelectiveNimbleIndexReader::advanceStripe() {
  if (readSegmentIndex_ >= readSegments_.size()) {
    ++stripeIndex_;
    stripeLoaded_ = false;
    readSegments_.clear();
    readSegmentIndex_ = 0;
    columnReader_.reset();
    indexReader_.reset();
  }
}

bool SelectiveNimbleIndexReader::canProduceOutput(
    vector_size_t maxOutputRows) const {
  return readyOutputRows_ >= maxOutputRows;
}

void SelectiveNimbleIndexReader::addStripeSegmentOutput(
    const ReadSegment& segment,
    RowVectorPtr output) {
  VELOX_CHECK_NOT_NULL(output);
  const auto outputRows = output->size();
  if (outputRows == 0) {
    updatePendingStripes(segment);
  } else {
    outputSegments_.emplace_back(std::move(output));
    const size_t outputIndex = outputSegments_.size() - 1;
    trackStripeSegmentOutputRefs(segment, outputIndex, outputRows);
    NIMBLE_CHECK_GT(outputSegments_[outputIndex].refCount, 0);
  }
  updateReadyOutputRequests();
}

void SelectiveNimbleIndexReader::trackStripeSegmentOutputRefs(
    const ReadSegment& segment,
    size_t outputIndex,
    vector_size_t outputRows) {
  VELOX_CHECK_GT(outputRows, 0);
  const auto startRow = segment.rowRange.startRow;
  const auto readRows = segment.rowRange.numRows();

  if (!hasFilters_) {
    // No filter: outputRows == readRows. Each request extracts its portion
    // based on its row range. Row range truncation is already done in
    // prepareStripeReading.
    NIMBLE_CHECK_EQ(outputRows, readRows);
    const auto readEndRow = segment.rowRange.endRow;
    for (vector_size_t requestIdx : segment.requestIndices) {
      auto& state = requestStates_[requestIdx];
      const auto& range = state.rowRange;
      const auto overlapStart = std::max(startRow, range.startRow);
      const auto overlapEnd = std::min(readEndRow, range.endRow);
      const auto overlapRows = overlapEnd - overlapStart;

      state.outputRefs.emplace_back(
          RequestState::OutputReference{
              outputIndex,
              RowRange{overlapStart - startRow, overlapEnd - startRow}});
      outputSegments_[outputIndex].addRef();
      state.outputRows += overlapRows;
      VELOX_DCHECK(
          requestOptions_.maxRowsPerRequest == 0 ||
              state.outputRows <= requestOptions_.maxRowsPerRequest,
          "Output rows {} exceeds maxRowsPerRequest {} for request {}",
          state.outputRows,
          requestOptions_.maxRowsPerRequest,
          requestIdx);
    }
  } else {
    // With filter: outputRows <= readRows. Each request in the segment
    // gets the filtered output. Apply truncation if
    // requestOptions_.maxRowsPerRequest is set.
    NIMBLE_CHECK_LE(outputRows, readRows);

    for (vector_size_t requestIdx : segment.requestIndices) {
      auto& state = requestStates_[requestIdx];

      // Calculate how many rows to add, respecting the limit.
      auto rowsToAdd = outputRows;
      if (requestOptions_.maxRowsPerRequest > 0) {
        if (requestOptions_.maxRowsPerRequest == state.outputRows) {
          continue;
        }
        const auto remainingRows =
            requestOptions_.maxRowsPerRequest - state.outputRows;
        if (outputRows >= static_cast<vector_size_t>(remainingRows)) {
          rowsToAdd = remainingRows;
          removeRequestFromSubsequentStripes(requestIdx, stripeIndex_ + 1);
        }
      }

      state.outputRefs.emplace_back(
          RequestState::OutputReference{outputIndex, RowRange{0, rowsToAdd}});
      outputSegments_[outputIndex].addRef();
      state.outputRows += rowsToAdd;
    }
  }
  updatePendingStripes(segment);
}

void SelectiveNimbleIndexReader::updatePendingStripes(
    const ReadSegment& segment) {
  for (vector_size_t requestIdx : segment.requestIndices) {
    const auto& requestRange = requestStates_[requestIdx].rowRange;
    if (segment.rowRange.endRow >= requestRange.endRow) {
      --requestStates_[requestIdx].pendingStripes;
    }
  }
}

void SelectiveNimbleIndexReader::updateReadyOutputRequests() {
  for (auto i = lastReadyOutputRequest_ + 1;
       i < static_cast<vector_size_t>(numRequests_);
       ++i) {
    if (requestStates_[i].pendingStripes != 0) {
      break;
    }
    lastReadyOutputRequest_ = i;
    readyOutputRows_ += requestStates_[i].outputRows;
    // Skip empty output request.
    if (requestStates_[i].empty() && i == nextOutputRequest_) {
      ++nextOutputRequest_;
    }
  }
}

std::unique_ptr<connector::IndexSource::Result>
SelectiveNimbleIndexReader::produceOutput() {
  if (readyOutputRows_ == 0) {
    VELOX_CHECK_EQ(stripeIndex_, stripes_.size());
    reset();
    return nullptr;
  }

  VELOX_CHECK_LE(nextOutputRequest_, lastReadyOutputRequest_);

  // Count total output rows from all ready requests.
  vector_size_t totalOutputRows = 0;
  for (auto i = nextOutputRequest_; i <= lastReadyOutputRequest_; ++i) {
    totalOutputRows += requestStates_[i].outputRows;
  }
  VELOX_CHECK_EQ(totalOutputRows, readyOutputRows_);

  // Allocate output buffers for all ready requests.
  RowVectorPtr output = BaseVector::create<RowVector>(
      outputType_, totalOutputRows, readerBase_->pool());
  auto inputHits = AlignedBuffer::allocate<vector_size_t>(
      totalOutputRows, readerBase_->pool());
  auto* rawInputHits = inputHits->asMutable<vector_size_t>();

  // Build output for all ready requests.
  vector_size_t outputOffset = 0;
  for (auto requestIdx = nextOutputRequest_;
       requestIdx <= lastReadyOutputRequest_;
       ++requestIdx) {
    const auto& state = requestStates_[requestIdx];
    if (state.empty()) {
      continue;
    }

    for (const auto& ref : state.outputRefs) {
      auto& chunk = outputSegments_[ref.chunkIndex];
      const auto numRows = ref.rowRange.endRow - ref.rowRange.startRow;
      output->copy(
          chunk.data.get(), outputOffset, ref.rowRange.startRow, numRows);

      // Fill inputHits with the request index for each output row.
      std::fill(
          rawInputHits + outputOffset,
          rawInputHits + outputOffset + numRows,
          requestIdx);

      outputOffset += numRows;
      chunk.dropRef();
    }
  }
  NIMBLE_CHECK_EQ(outputOffset, totalOutputRows);

  nextOutputRequest_ = lastReadyOutputRequest_ + 1;
  readyOutputRows_ = 0;
  return std::make_unique<connector::IndexSource::Result>(
      std::move(inputHits), std::move(output));
}

void SelectiveNimbleIndexReader::removeRequestFromSubsequentStripes(
    velox::vector_size_t requestIdx,
    size_t startStripeIdx) {
  auto& state = requestStates_[requestIdx];
  // We must have at least one pending stripe (the current stripe) that will
  // be processed. We only remove from subsequent stripes.
  VELOX_CHECK_GE(state.pendingStripes, 1);
  for (size_t i = startStripeIdx;
       i < stripes_.size() && state.pendingStripes > 1;
       ++i) {
    const auto stripe = stripes_[i];
    auto& requests = stripeToRequests_[stripe];
    auto it = std::find(requests.begin(), requests.end(), requestIdx);
    VELOX_CHECK(
        it != requests.end(),
        "Request {} must exist in stripe {} as stripes are contiguous",
        requestIdx,
        stripe);
    requests.erase(it);
    state.decPendingStripes();
  }
}

void SelectiveNimbleIndexReader::reset() {
  numRequests_ = 0;
  requestOptions_ = {};
  encodedKeyBounds_.clear();
  stripes_.clear();
  stripeToRequests_.clear();
  stripeIndex_ = 0;
  stripeLoaded_ = false;
  readSegments_.clear();
  readSegmentIndex_ = 0;
  requestStates_.clear();
  outputSegments_.clear();
  nextOutputRequest_ = 0;
  lastReadyOutputRequest_ = -1;
  readyOutputRows_ = 0;
  columnReader_.reset();
  indexReader_.reset();
}
} // namespace facebook::nimble
