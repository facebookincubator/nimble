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
#include "dwio/nimble/tablet/TabletReaderCache.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "folly/ScopeGuard.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/DirectBufferedInput.h"

namespace facebook::nimble {

using namespace facebook::velox; // NOLINT(google-build-using-namespace)

namespace {

void validateReaderOptions(const velox::dwio::common::ReaderOptions& options) {
  NIMBLE_CHECK_NOT_NULL(
      options.dataIoStats(),
      "NimbleIndexProjector requires ReaderOptions::dataIoStats to be set");
  NIMBLE_CHECK_NOT_NULL(
      options.metadataIoStats(),
      "NimbleIndexProjector requires ReaderOptions::metadataIoStats to be set");
  NIMBLE_CHECK_NOT_NULL(
      options.indexIoStats(),
      "NimbleIndexProjector requires ReaderOptions::indexIoStats to be set");
}

} // namespace

std::string NimbleIndexProjector::Stats::toString() const {
  return fmt::format(
      "Stats(numReadStripes={}, numScannedRows={}, numProjectedRows={}, numReadRows={}, "
      "numReadBytes={}, "
      "lookupTiming=[{}], scanTiming=[{}], projectionTiming=[{}])",
      numReadStripes,
      numScannedRows,
      numProjectedRows,
      numReadRows,
      velox::succinctBytes(numReadBytes),
      lookupTiming.toString(),
      scanTiming.toString(),
      projectionTiming.toString());
}

namespace {
std::unique_ptr<velox::dwio::common::BufferedInput> createBufferedInput(
    const velox::FileHandle& fileHandle,
    velox::cache::AsyncDataCache* cache,
    const velox::dwio::common::ReaderOptions& options) {
  if (cache != nullptr && options.cacheData()) {
    NIMBLE_CHECK(
        fileHandle.uuid.hasValue() && fileHandle.groupId.hasValue(),
        "FileHandle must have valid uuid and groupId when cache is provided");
    return std::make_unique<velox::dwio::common::CachedBufferedInput>(
        fileHandle.file,
        velox::dwio::common::MetricsLog::voidLog(),
        fileHandle.uuid,
        cache,
        /*tracker=*/nullptr,
        fileHandle.groupId,
        options.dataIoStats(),
        /*ioStats=*/nullptr,
        /*executor=*/nullptr,
        options);
  }
  return std::make_unique<velox::dwio::common::DirectBufferedInput>(
      fileHandle.file,
      velox::dwio::common::MetricsLog::voidLog(),
      fileHandle.uuid,
      /*tracker=*/nullptr,
      fileHandle.groupId,
      options.dataIoStats(),
      /*ioStats=*/nullptr,
      /*executor=*/nullptr,
      options);
}
} // namespace

std::unique_ptr<NimbleIndexProjector> NimbleIndexProjector::create(
    const velox::FileHandle& fileHandle,
    velox::cache::AsyncDataCache* cache,
    const std::vector<Subfield>& projectedSubfields,
    const velox::dwio::common::ReaderOptions& options) {
  validateReaderOptions(options);
  auto bufferedInput = createBufferedInput(fileHandle, cache, options);
  return std::unique_ptr<NimbleIndexProjector>(new NimbleIndexProjector(
      ReaderBase::create(std::move(bufferedInput), options),
      projectedSubfields,
      options));
}

std::unique_ptr<NimbleIndexProjector> NimbleIndexProjector::create(
    TabletReaderCache& tabletReaderCache,
    const velox::FileHandle& fileHandle,
    velox::cache::AsyncDataCache* cache,
    const std::vector<Subfield>& projectedSubfields,
    const velox::dwio::common::ReaderOptions& options) {
  validateReaderOptions(options);
  auto cached = tabletReaderCache.get(
      fileHandle.file, TabletReader::configureOptions(options));
  auto bufferedInput = createBufferedInput(fileHandle, cache, options);
  return std::unique_ptr<NimbleIndexProjector>(new NimbleIndexProjector(
      ReaderBase::create(std::move(bufferedInput), std::move(cached), options),
      projectedSubfields,
      options));
}

NimbleIndexProjector::NimbleIndexProjector(
    std::shared_ptr<ReaderBase> readerBase,
    const std::vector<Subfield>& projectedSubfields,
    const velox::dwio::common::ReaderOptions& options)
    : readerBase_{std::move(readerBase)},
      ioStats_{options.dataIoStats()},
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
  initRequest(request, options);
  SCOPE_EXIT {
    clearRequest();
  };

  lookupStripes();

  Result result;
  result.responses.resize(numRequests_);

  for (uint32_t i = 0; i < stripeRangeMap_.numStripes; ++i) {
    const uint32_t stripeIndex = stripeRangeMap_.startStripe + i;
    const auto stripeRowRanges = stripeRangeMap_.stripeRowRanges(stripeIndex);
    if (!pruneStripeRowRanges(stripeRowRanges)) {
      continue;
    }
    if (!processStripe(stripeIndex, stripeRowRanges_)) {
      setResumeKeys(stripeIndex, stripeRowRanges_, result);
      break;
    }
  }

  finalizeResult(result);
  return result;
}

void NimbleIndexProjector::initRequest(
    const Request& request,
    const Options& options) {
  NIMBLE_CHECK_NULL(request_, "project() is not reentrant");
  NIMBLE_CHECK_NULL(options_, "project() is not reentrant");
  NIMBLE_CHECK_GT(request.keyBounds.size(), 0, "keyBounds must not be empty");
  request_ = &request;
  options_ = &options;
  numRequests_ = static_cast<uint32_t>(request.keyBounds.size());
  numReadRows_ = 0;
  numReadBytes_ = 0;
  rowsPerRequest_.assign(numRequests_, 0);
  resultChunks_.assign(numRequests_, {});
}

void NimbleIndexProjector::clearRequest() {
  request_ = nullptr;
  options_ = nullptr;
  numRequests_ = 0;
  rowsPerRequest_.clear();
  resultChunks_.clear();
  stripeRowRanges_.clear();
  stripeRangeMap_.clear();
}

void NimbleIndexProjector::lookupStripes() {
  velox::CpuWallTimer timer(stats_.lookupTiming);

  const auto result = clusterIndex_->lookup(
      index::IndexLookup::LookupRequest::rangeScan(request_->keyBounds));

  // Find the stripe range across all requests.
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
  requestRanges.reserve(numRequests_);

  uint32_t numRowRanges = 0;
  const auto& tablet = readerBase_->tablet();
  for (size_t requestIdx = 0; requestIdx < numRequests_; ++requestIdx) {
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
  for (size_t requestIdx = 0; requestIdx < numRequests_; ++requestIdx) {
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
    std::span<const StripeRowRange> stripeRowRanges) {
  streams_.setStripe(stripeIndex);

  // TODO: Support fine-grained row range fetches from a stripe. For now, we
  // always fetch entire projected column streams and build the serialized
  // values. The result references a portion of it based on the row range.
  auto inputStreams = loadStripe();
  auto stripeChunk = serializeStripe(stripeIndex, inputStreams);
  return buildStripeResult(stripeRowRanges, std::move(stripeChunk));
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

NimbleIndexProjector::ResultChunk NimbleIndexProjector::serializeStripe(
    uint32_t stripeIndex,
    InputStreams& inputStreams) {
  velox::CpuWallTimer timer(stats_.projectionTiming);

  const auto numRows = stripeRowCount(stripeIndex);

  // Build the shared body+trailer for this stripe:
  //   [stream_data...][encodingType][stream_sizes][trailer_size:u32].
  // The version/rowCount/row-range/resume-key header is per-slice and is
  // built later by finalizeResult().

  // Collect raw stream segments without copying. Sizes are needed for the
  // trailer.
  std::vector<uint32_t> streamSizes(projectedStreamOffsets_.size(), 0);
  std::vector<std::pair<const void*, int>> segments;
  uint32_t totalStreamBytes = 0;
  for (size_t i = 0; i < inputStreams.size(); ++i) {
    if (inputStreams[i] == nullptr) {
      continue;
    }
    const void* data;
    int size;
    while (inputStreams[i]->Next(&data, &size)) {
      segments.emplace_back(data, size);
      streamSizes[i] += static_cast<uint32_t>(size);
      totalStreamBytes += static_cast<uint32_t>(size);
    }
  }

  std::string trailerBuf;
  serde::detail::writeTrailer(streamSizes, EncodingType::Trivial, trailerBuf);

  // Single allocation for stream data + trailer.
  const size_t bodySize = totalStreamBytes + trailerBuf.size();
  auto body = folly::IOBuf::create(bodySize);
  auto* dest = body->writableData();

  for (const auto& [data, size] : segments) {
    std::memcpy(dest, data, size);
    dest += size;
  }

  std::memcpy(dest, trailerBuf.data(), trailerBuf.size());
  body->append(bodySize);

  ResultChunk stripeChunk;
  stripeChunk.sharedBody = std::move(*body);
  stripeChunk.numRows = numRows;

  stats_.numScannedRows += numRows;
  stats_.numProjectedRows += numRows;
  stats_.numReadBytes += stripeChunk.sharedBody.length();
  return stripeChunk;
}

bool NimbleIndexProjector::buildStripeResult(
    std::span<const StripeRowRange> stripeRowRanges,
    ResultChunk&& stripeChunk) {
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
    NIMBLE_CHECK_GT(rowRange.numRows(), 0);

    // Queue a per-slice clone of the shared body+trailer. The header node
    // (version + rowCount + row range + resume-key-length [+ resume key])
    // is materialized and chained on top in finalizeResult().
    stats_.numReadRows += rowRange.numRows();
    numReadRows_ += rowRange.numRows();
    resultChunks_[request.requestIndex].push_back(
        ResultChunk{
            stripeChunk.sharedBody.cloneOneAsValue(),
            stripeChunk.numRows,
            rowRange});
    rowsPerRequest_[request.requestIndex] += rowRange.numRows();
  }

  // Track body bytes only. Header bytes are small relative to stream data
  // and are not worth the accounting complexity.
  const auto bodyBytes = stripeChunk.sharedBody.length();
  numReadBytes_ += bodyBytes;

  return !checkOutputLimit();
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

  // For requests that were never started (no result chunks for this scan,
  // no resume key), set resume key to their original lower key so the
  // caller can retry. Note: response.chunks is populated later by
  // finalizeResult(), so we check resultChunks_ here.
  for (size_t i = 0; i < result.responses.size(); ++i) {
    auto& response = result.responses[i];
    if (resultChunks_[i].empty() && !response.resumeKey.has_value()) {
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

namespace {

/// Builds a kTablet IOBuf chain: [header] -> [shared body+trailer].
folly::IOBuf assembleChunk(
    uint32_t numRows,
    RowRange rowRange,
    folly::IOBuf body,
    std::optional<std::string> resumeKey = std::nullopt) {
  serde::TabletChunkHeader header{
      .rowCount = numRows,
      .rowRange = rowRange,
      .resumeKey = std::move(resumeKey),
  };
  auto serializedHeader =
      std::make_unique<folly::IOBuf>(serde::createTabletChunkHeader(header));
  serializedHeader->appendToChain(
      std::make_unique<folly::IOBuf>(std::move(body)));
  return std::move(*serializedHeader);
}

} // namespace

void NimbleIndexProjector::finalizeResult(Result& result) {
  NIMBLE_CHECK_EQ(resultChunks_.size(), result.responses.size());

  for (size_t responseIdx = 0; responseIdx < result.responses.size();
       ++responseIdx) {
    auto& response = result.responses[responseIdx];
    auto& chunks = resultChunks_[responseIdx];
    const auto numChunks = chunks.size();
    response.chunks.reserve(numChunks);

    for (size_t chunkIdx = 0; chunkIdx < numChunks; ++chunkIdx) {
      auto& chunk = chunks[chunkIdx];
      const bool isLastChunk = chunkIdx + 1 == numChunks;
      response.chunks.emplace_back(assembleChunk(
          chunk.numRows,
          chunk.rowRange,
          std::move(chunk.sharedBody),
          isLastChunk ? response.resumeKey : std::nullopt));
    }
    chunks.clear();
  }
}

} // namespace facebook::nimble
