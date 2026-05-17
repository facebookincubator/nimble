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

#include "dwio/nimble/velox/index/NimbleIndexProjector.h"

#include <algorithm>

#include "dwio/nimble/index/ClusterIndex.h"
#include "dwio/nimble/serializer/SerializerImpl.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "folly/ScopeGuard.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/dwio/common/BufferedInput.h"

namespace facebook::nimble {

using namespace facebook::velox; // NOLINT(google-build-using-namespace)

std::string NimbleIndexProjector::Stats::toString() const {
  return fmt::format(
      "Stats(numReadStripes={}, numScannedRows={}, numProjectedRows={}, numReadRows={}, "
      "numReadBytes={}, rawBytesRead={}, rawOverreadBytes={}, numStorageReads={}, "
      "lookupTiming=[{}], scanTiming=[{}], projectionTiming=[{}])",
      numReadStripes,
      numScannedRows,
      numProjectedRows,
      numReadRows,
      velox::succinctBytes(numReadBytes),
      velox::succinctBytes(rawBytesRead),
      velox::succinctBytes(rawOverreadBytes),
      numStorageReads,
      lookupTiming.toString(),
      scanTiming.toString(),
      projectionTiming.toString());
}

NimbleIndexProjector::NimbleIndexProjector(
    std::shared_ptr<velox::ReadFile> readFile,
    const std::vector<Subfield>& projectedSubfields,
    const velox::dwio::common::ReaderOptions& options)
    : ioStatistics_{std::make_shared<velox::io::IoStatistics>()},
      readerBase_{ReaderBase::create(
          std::make_unique<velox::dwio::common::BufferedInput>(
              std::move(readFile),
              options.memoryPool(),
              velox::dwio::common::MetricsLog::voidLog(),
              ioStatistics_.get(),
              /*ioStats=*/nullptr,
              options.maxCoalesceDistance()),
          options)},
      pool_{readerBase_->pool()},
      clusterIndex_{readerBase_->tablet().clusterIndex()},
      numStripes_{readerBase_->tablet().stripeCount()},
      streams_{readerBase_} {
  NIMBLE_CHECK_NOT_NULL(
      clusterIndex_, "NimbleIndexProjector requires a tablet with an index");

  projectedNimbleType_ = buildProjectedNimbleType(
      readerBase_->nimbleSchema().get(),
      projectedSubfields,
      projectedStreamOffsets_);
}

NimbleIndexProjector::Result NimbleIndexProjector::project(
    const Request& request,
    const Options& options) {
  NIMBLE_CHECK_NULL(request_, "project() is not reentrant");
  NIMBLE_CHECK_NULL(options_, "project() is not reentrant");
  request_ = &request;
  options_ = &options;
  SCOPE_EXIT {
    request_ = nullptr;
    options_ = nullptr;
    rowsPerRequest_.clear();
    stripeRowRanges_.clear();
    stripeRangeMap_.clear();
  };

  NIMBLE_CHECK_GT(request_->keyBounds.size(), 0, "keyBounds must not be empty");

  lookupStripes();

  numReadRows_ = 0;
  const auto numRequests = request_->keyBounds.size();
  rowsPerRequest_.assign(numRequests, 0);
  Result result;
  result.responses.resize(numRequests);

  for (uint32_t i = 0; i < stripeRangeMap_.numStripes; ++i) {
    const uint32_t stripeIndex = stripeRangeMap_.startStripe + i;
    const auto stripeRowRanges = stripeRangeMap_.stripeRowRanges(stripeIndex);
    if (!pruneStripeRowRanges(stripeRowRanges)) {
      continue;
    }
    if (!processStripe(stripeIndex, stripeRowRanges_, result)) {
      setResumeKeys(stripeIndex, stripeRowRanges_, result);
      break;
    }
  }

  updateIoStats();
  return result;
}

void NimbleIndexProjector::lookupStripes() {
  velox::CpuWallTimer timer(stats_.lookupTiming);

  const auto result = clusterIndex_->lookup(
      index::IndexLookup::LookupRequest::rangeScan(request_->keyBounds));

  // Find the stripe range across all requests.
  const auto numRequests = request_->keyBounds.size();
  uint32_t minStripe = numStripes_;
  uint32_t maxStripe = 0;

  struct RequestStripeRange {
    uint32_t startStripe;
    uint32_t endStripe;
    // File-level row range from the cluster index lookup.
    RowRange rowRange;

    bool empty() const {
      return startStripe >= endStripe;
    }
  };
  std::vector<RequestStripeRange> requestRanges;
  requestRanges.reserve(numRequests);

  uint32_t numRowRanges = 0;
  const auto& tablet = readerBase_->tablet();
  for (size_t requestIdx = 0; requestIdx < numRequests; ++requestIdx) {
    const auto rowRanges = result[requestIdx];
    if (rowRanges.empty()) {
      requestRanges.emplace_back(RequestStripeRange{0, 0, {}});
      continue;
    }
    NIMBLE_CHECK_EQ(
        rowRanges.size(), 1, "Expected single row range per lookup");
    const auto& rowRange = rowRanges[0];
    NIMBLE_CHECK(!rowRange.empty());

    const uint32_t startStripe = tablet.rowToStripe(rowRange.startRow);
    const uint32_t endStripe = tablet.rowToStripe(rowRange.endRow - 1) + 1;
    NIMBLE_CHECK_LE(endStripe, numStripes_);
    NIMBLE_CHECK_LT(startStripe, endStripe);
    requestRanges.emplace_back(
        RequestStripeRange{startStripe, endStripe, rowRange});

    numRowRanges += endStripe - startStripe;
    minStripe = std::min(minStripe, startStripe);
    maxStripe = std::max(maxStripe, endStripe);
  }

  if (numRowRanges == 0) {
    return;
  }

  stripeRangeMap_.startStripe = minStripe;
  stripeRangeMap_.numStripes = maxStripe - minStripe;

  // Build CSR layout: count entries per stripe, then fill.
  stripeRangeMap_.stripeOffsets.resize(stripeRangeMap_.numStripes + 1, 0);
  for (const auto& requestRange : requestRanges) {
    if (requestRange.empty()) {
      continue;
    }
    for (uint32_t stripe = requestRange.startStripe;
         stripe < requestRange.endStripe;
         ++stripe) {
      ++stripeRangeMap_.stripeOffsets[stripe - minStripe + 1];
    }
  }

  // Convert counts to prefix sums.
  for (uint32_t i = 1; i <= stripeRangeMap_.numStripes; ++i) {
    stripeRangeMap_.stripeOffsets[i] += stripeRangeMap_.stripeOffsets[i - 1];
  }

  // Fill entries using a copy of offsets as write cursors.
  stripeRangeMap_.rowRanges.resize(numRowRanges);
  auto writeCursors = stripeRangeMap_.stripeOffsets;
  for (size_t requestIdx = 0; requestIdx < numRequests; ++requestIdx) {
    const auto& requestRange = requestRanges[requestIdx];
    if (requestRange.empty()) {
      continue;
    }
    for (uint32_t stripe = requestRange.startStripe;
         stripe < requestRange.endStripe;
         ++stripe) {
      const auto stripeOffset = stripe - minStripe;
      stripeRangeMap_.rowRanges[writeCursors[stripeOffset]++] = StripeRowRange{
          static_cast<uint32_t>(requestIdx),
          stripeRowRange(stripe, requestRange.rowRange)};
    }
  }
}

bool NimbleIndexProjector::pruneStripeRowRanges(
    std::span<const StripeRowRange> stripeRowRanges) {
  stripeRowRanges_.assign(stripeRowRanges.begin(), stripeRowRanges.end());
  if (stripeRowRanges_.empty()) {
    return false;
  }
  if (options_->maxRowsPerRequest > 0) {
    std::erase_if(stripeRowRanges_, [&](const auto& rowRange) {
      return rowsPerRequest_[rowRange.requestIndex] >=
          options_->maxRowsPerRequest;
    });
  }
  return !stripeRowRanges_.empty();
}

bool NimbleIndexProjector::processStripe(
    uint32_t stripeIndex,
    std::span<const StripeRowRange> stripeRowRanges,
    Result& result) {
  streams_.setStripe(stripeIndex);

  // TODO: Support fine-grained row range fetches from a stripe. For now, we
  // always fetch entire projected column streams and build the serialized
  // values. The result references a portion of it based on the row range.
  auto inputStreams = loadStripe();
  auto stripeChunk = serializeStripe(stripeIndex, inputStreams);
  return buildStripeResult(
      stripeIndex, stripeRowRanges, std::move(stripeChunk), result);
}

NimbleIndexProjector::InputStreams NimbleIndexProjector::loadStripe() {
  velox::CpuWallTimer timer(stats_.scanTiming);
  ++stats_.numReadStripes;
  InputStreams inputStreams;
  inputStreams.reserve(projectedStreamOffsets_.size());
  for (const auto streamId : projectedStreamOffsets_) {
    inputStreams.emplace_back(streams_.enqueue(streamId));
  }
  streams_.load();
  return inputStreams;
}

NimbleIndexProjector::Chunk NimbleIndexProjector::serializeStripe(
    uint32_t stripeIndex,
    InputStreams& inputStreams) {
  velox::CpuWallTimer timer(stats_.projectionTiming);

  const auto numStripeRows = stripeRowCount(stripeIndex);

  // Build kTabletRaw output as chained IOBufs:
  // [header] → [raw stream data...] → [trailer].
  // kTabletRaw passes raw tablet bytes through including chunk headers.
  // The Deserializer handles chunk header stripping on the client side.
  std::string headerBuf;
  serde::detail::writeHeader(
      headerBuf, SerializationVersion::kTabletRaw, numStripeRows);
  auto output = folly::IOBuf::copyBuffer(headerBuf.data(), headerBuf.size());

  // Read all stream data into a single contiguous buffer to avoid
  // per-segment IOBuf allocations (one IOBuf::create instead of many
  // IOBuf::copyBuffer calls).
  std::vector<uint32_t> streamSizes(projectedStreamOffsets_.size(), 0);
  std::vector<std::pair<const void*, int>> segments;
  uint32_t totalBytes = 0;
  for (size_t i = 0; i < inputStreams.size(); ++i) {
    if (inputStreams[i] == nullptr) {
      continue;
    }
    const void* data;
    int size;
    while (inputStreams[i]->Next(&data, &size)) {
      segments.emplace_back(data, size);
      streamSizes[i] += static_cast<uint32_t>(size);
      totalBytes += static_cast<uint32_t>(size);
    }
  }

  std::string trailerBuf;
  serde::detail::writeRawTrailer(
      streamSizes, EncodingType::Trivial, trailerBuf);

  // Single allocation for header + data + trailer.
  const size_t totalSize = headerBuf.size() + totalBytes + trailerBuf.size();
  output = folly::IOBuf::create(totalSize);
  auto* dest = output->writableData();

  std::memcpy(dest, headerBuf.data(), headerBuf.size());
  dest += headerBuf.size();

  for (const auto& [data, size] : segments) {
    std::memcpy(dest, data, size);
    dest += size;
  }

  std::memcpy(dest, trailerBuf.data(), trailerBuf.size());
  output->append(totalSize);

  Chunk chunk;
  chunk.data = std::move(*output);
  chunk.numRows = numStripeRows;

  stats_.numScannedRows += numStripeRows;
  stats_.numProjectedRows += numStripeRows;
  stats_.numReadBytes += chunk.data.computeChainDataLength();
  return chunk;
}

bool NimbleIndexProjector::buildStripeResult(
    uint32_t stripeIndex,
    std::span<const StripeRowRange> stripeRowRanges,
    Chunk&& chunk,
    Result& result) {
  const auto chunkIndex = static_cast<uint32_t>(result.chunks.size());

  for (const auto& request : stripeRowRanges) {
    NIMBLE_CHECK(!request.rowRange.empty());
    auto rowRange = request.rowRange;

    // Saturated requests are filtered out by the caller in project().
    if (options_->maxRowsPerRequest > 0) {
      NIMBLE_CHECK_GT(
          options_->maxRowsPerRequest, rowsPerRequest_[request.requestIndex]);
      const auto remaining =
          options_->maxRowsPerRequest - rowsPerRequest_[request.requestIndex];
      if (static_cast<uint64_t>(rowRange.numRows()) > remaining) {
        rowRange.endRow = rowRange.startRow + static_cast<uint32_t>(remaining);
      }
    }

    stats_.numReadRows += rowRange.numRows();
    numReadRows_ += rowRange.numRows();
    result.responses[request.requestIndex].slices.emplace_back(
        ChunkSlice{chunkIndex, rowRange});
    rowsPerRequest_[request.requestIndex] += rowRange.numRows();
  }

  result.chunks.emplace_back(std::move(chunk));

  return !(options_->maxRows > 0 && numReadRows_ >= options_->maxRows);
}

void NimbleIndexProjector::setResumeKeys(
    uint32_t stripeIndex,
    std::span<const StripeRowRange> stripeRowRanges,
    Result& result) {
  // No next stripe in the range map — all mapped requests end at this stripe.
  const auto nextStripe = stripeIndex + 1;
  if (nextStripe >= stripeRangeMap_.startStripe + stripeRangeMap_.numStripes) {
    return;
  }

  // No next stripe in the file — all requests are fully satisfied.
  const auto stripeStartRow =
      static_cast<uint32_t>(readerBase_->tablet().stripeStartRow(stripeIndex));
  const auto nextStripeStartRow = stripeStartRow + stripeRowCount(stripeIndex);
  if (nextStripeStartRow >= readerBase_->tablet().tabletRowCount()) {
    return;
  }

  auto resumeKey = clusterIndex_->keyAtRow(nextStripeStartRow);
  const auto nextRanges = stripeRangeMap_.stripeRowRanges(nextStripe);

  for (const auto& request : stripeRowRanges) {
    auto& response = result.responses[request.requestIndex];
    NIMBLE_CHECK(!response.resumeKey.has_value());
    if (std::any_of(
            nextRanges.begin(), nextRanges.end(), [&](const auto& range) {
              return range.requestIndex == request.requestIndex;
            })) {
      response.resumeKey = resumeKey;
    }
  }

  // For requests that were never started (empty slices, no resume key),
  // set resume key to their original lower key so the caller can retry.
  for (size_t i = 0; i < result.responses.size(); ++i) {
    auto& response = result.responses[i];
    if (response.slices.empty() && !response.resumeKey.has_value()) {
      const auto& keyBounds = request_->keyBounds[i];
      NIMBLE_CHECK(
          keyBounds.lowerKey.has_value(),
          "Request {} has no lowerKey: unbounded lower requests start from "
          "stripe 0 and should have been processed before truncation",
          i);
      response.resumeKey = *keyBounds.lowerKey;
    }
  }
}

void NimbleIndexProjector::updateIoStats() {
  stats_.rawBytesRead = ioStatistics_->rawBytesRead();
  stats_.rawOverreadBytes = ioStatistics_->rawOverreadBytes();
  stats_.numStorageReads = ioStatistics_->read().count();
}

} // namespace facebook::nimble
