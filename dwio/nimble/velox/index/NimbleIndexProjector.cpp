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
      "Stats(numReadStripes={}, numReadRows={}, numProjectedRows={}, "
      "numOutputBytes={}, "
      "lookupTiming=[{}], scanTiming=[{}], projectionTiming=[{}])",
      numReadStripes,
      numReadRows,
      numProjectedRows,
      velox::succinctBytes(numOutputBytes),
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
  NIMBLE_CHECK_GT(numStripes_, 0, "NimbleIndexProjector requires stripes");

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
  prepareStripes();
  loadStripes();
  return processStripes();
}

void NimbleIndexProjector::pruneStripeRanges(
    std::vector<StripeRange>& stripeRanges,
    const std::vector<uint64_t>& rowsPerRequest) {
  if (ctx_.options->maxRowsPerRequest > 0) {
    std::erase_if(stripeRanges, [&](const auto& range) {
      return rowsPerRequest[range.requestIndex] >=
          ctx_.options->maxRowsPerRequest;
    });
  }
}

void NimbleIndexProjector::prepareStripes() {
  uint64_t totalRows{0};
  uint64_t totalBytes{0};
  std::vector<uint64_t> rowsPerRequest(ctx_.numRequests, 0);
  ctx_.hasStripeRanges.assign(ctx_.numRequests, false);

  for (uint32_t offset = 0; offset < ctx_.stripeRanges.numStripes; ++offset) {
    auto spanRanges = ctx_.stripeRanges.getRanges(offset);
    std::vector<StripeRange> stripeRanges(spanRanges.begin(), spanRanges.end());
    pruneStripeRanges(stripeRanges, rowsPerRequest);
    if (stripeRanges.empty()) {
      continue;
    }

    uint64_t stripeRows{0};
    for (auto& stripeRange : stripeRanges) {
      const auto numRows =
          static_cast<uint64_t>(stripeRange.rowRange.numRows());
      if (ctx_.options->maxRowsPerRequest > 0) {
        const auto remaining = ctx_.options->maxRowsPerRequest -
            rowsPerRequest[stripeRange.requestIndex];
        const auto rowsToRead = std::min(numRows, remaining);
        rowsPerRequest[stripeRange.requestIndex] += rowsToRead;
        stripeRows += rowsToRead;
        if (rowsToRead < numRows) {
          stripeRange.rowRange.endRow =
              stripeRange.rowRange.startRow + static_cast<uint32_t>(rowsToRead);
        }
      } else {
        stripeRows += numRows;
      }
    }

    for (const auto& range : stripeRanges) {
      ctx_.hasStripeRanges[range.requestIndex] = true;
    }

    const uint32_t stripeIndex = ctx_.stripeRanges.startStripe + offset;
    StripePlan stripePlan;
    stripePlan.stripeIndex = stripeIndex;
    stripePlan.stripeRanges = std::move(stripeRanges);
    locateStripeStreams(stripePlan);

    totalRows += stripeRows;
    totalBytes += stripePlan.projectedBytes;
    ctx_.plan.stripePlans.push_back(std::move(stripePlan));
    if ((ctx_.options->maxRows > 0 && totalRows >= ctx_.options->maxRows) ||
        (ctx_.options->maxBytes > 0 && totalBytes >= ctx_.options->maxBytes)) {
      ctx_.plan.truncated = true;
      break;
    }
  }
}

void NimbleIndexProjector::initRequest(
    const Request& request,
    const Options& options) {
  NIMBLE_CHECK_NULL(ctx_.request, "project() is not reentrant");
  NIMBLE_CHECK_NULL(ctx_.options, "project() is not reentrant");
  NIMBLE_CHECK_GT(request.keyBounds.size(), 0, "keyBounds must not be empty");
  ctx_.request = &request;
  ctx_.options = &options;
  ctx_.numRequests = static_cast<uint32_t>(request.keyBounds.size());
}

void NimbleIndexProjector::clearRequest() {
  ctx_ = {};
}

RowRange NimbleIndexProjector::stripeRowRange(
    uint32_t stripe,
    const RowRange& rowRangeLimit) const {
  const auto stripeStart =
      static_cast<uint32_t>(readerBase_->tablet().stripeStartRow(stripe));
  const auto stripeEnd = stripeStart + stripeRowCount(stripe);
  const auto startRow = std::max(rowRangeLimit.startRow, stripeStart);
  const auto endRow = std::min(rowRangeLimit.endRow, stripeEnd);
  if (startRow >= endRow) {
    return RowRange{};
  }
  return RowRange(startRow - stripeStart, endRow - stripeStart);
}

void NimbleIndexProjector::lookupStripes() {
  velox::CpuWallTimer timer(stats_.lookupTiming);

  const auto result = clusterIndex_->lookup(
      index::IndexLookup::LookupRequest::rangeScan(ctx_.request->keyBounds));

  struct ResolvedRequest {
    uint32_t requestIndex;
    uint32_t startStripe;
    uint32_t endStripe;
    RowRange rowRange;
  };

  uint32_t minStripe = numStripes_;
  uint32_t maxStripe = 0;
  std::vector<ResolvedRequest> resolvedRequests;
  resolvedRequests.reserve(ctx_.numRequests);
  const auto& tablet = readerBase_->tablet();
  for (uint32_t requestIndex = 0; requestIndex < ctx_.numRequests;
       ++requestIndex) {
    const auto& ranges = result[requestIndex];
    if (ranges.empty()) {
      continue;
    }
    NIMBLE_CHECK_EQ(ranges.size(), 1, "Expected single row range per lookup");
    const auto& range = ranges[0];
    NIMBLE_CHECK(!range.empty());

    const uint32_t startStripe = tablet.rowToStripe(range.startRow);
    const uint32_t endStripe = tablet.rowToStripe(range.endRow - 1) + 1;
    NIMBLE_CHECK_LE(endStripe, numStripes_);
    NIMBLE_CHECK_LT(startStripe, endStripe);

    resolvedRequests.push_back({requestIndex, startStripe, endStripe, range});
    minStripe = std::min(minStripe, startStripe);
    maxStripe = std::max(maxStripe, endStripe);
  }

  if (resolvedRequests.empty()) {
    return;
  }

  ctx_.stripeRanges.startStripe = minStripe;
  ctx_.stripeRanges.numStripes = maxStripe - minStripe;

  std::vector<std::vector<StripeRange>> rangesByStripe(
      ctx_.stripeRanges.numStripes);
  ctx_.stripeRanges.offsets.resize(ctx_.stripeRanges.numStripes + 1, 0);

  for (const auto& request : resolvedRequests) {
    for (uint32_t stripe = request.startStripe; stripe < request.endStripe;
         ++stripe) {
      const auto stripeOffset = stripe - minStripe;
      rangesByStripe[stripeOffset].push_back(
          StripeRange{
              request.requestIndex, stripeRowRange(stripe, request.rowRange)});
    }
  }

  uint32_t numRanges = 0;
  for (uint32_t i = 0; i < ctx_.stripeRanges.numStripes; ++i) {
    numRanges += static_cast<uint32_t>(rangesByStripe[i].size());
    ctx_.stripeRanges.offsets[i + 1] = numRanges;
  }

  ctx_.stripeRanges.ranges.reserve(numRanges);
  for (const auto& ranges : rangesByStripe) {
    ctx_.stripeRanges.ranges.insert(
        ctx_.stripeRanges.ranges.end(), ranges.begin(), ranges.end());
  }
}

void NimbleIndexProjector::loadStripes() {
  const auto& stripePlans = ctx_.plan.stripePlans;
  velox::CpuWallTimer timer(stats_.scanTiming);

  ctx_.loadedStripes.resize(stripePlans.size());
  for (size_t stripeOffset = 0; stripeOffset < stripePlans.size();
       ++stripeOffset) {
    ctx_.loadedStripes[stripeOffset].resize(projectedStreamOffsets_.size());
    const auto& streams = stripePlans[stripeOffset].projectedStreams;
    for (size_t streamIndex = 0; streamIndex < streams.size(); ++streamIndex) {
      const auto& stream = streams[streamIndex];
      if (!stream.has_value()) {
        continue;
      }
      dwio::common::StreamIdentifier sid(stream->streamId);
      ctx_.loadedStripes[stripeOffset][streamIndex] =
          readerBase_->input().enqueue(stream->region, &sid);
    }
  }

  readerBase_->input().load(velox::dwio::common::LogType::STREAM_BUNDLE);
}

NimbleIndexProjector::Result NimbleIndexProjector::processStripes() {
  velox::CpuWallTimer timer(stats_.projectionTiming);
  Result result;
  result.responses.resize(ctx_.numRequests);
  ctx_.stripeBodies.resize(ctx_.plan.stripePlans.size());
  for (size_t i = 0; i < ctx_.plan.stripePlans.size(); ++i) {
    const auto& stripePlan = ctx_.plan.stripePlans[i];
    ++stats_.numReadStripes;
    ctx_.stripeBodies[i] =
        serializeStripe(stripePlan.stripeIndex, ctx_.loadedStripes[i]);
  }
  setResumeKeys(result);
  buildResult(result);
  return result;
}

void NimbleIndexProjector::locateStripeStreams(StripePlan& stripePlan) {
  NIMBLE_CHECK(
      stripePlan.projectedStreams.empty() && stripePlan.projectedBytes == 0,
      "StripePlan already has located streams");
  stripePlan.numRows = stripeRowCount(stripePlan.stripeIndex);
  streams_.setStripe(stripePlan.stripeIndex);
  stripePlan.projectedStreams = streams_.locateStreams(projectedStreamOffsets_);
  for (const auto& stream : stripePlan.projectedStreams) {
    if (stream.has_value()) {
      stripePlan.projectedBytes += stream->region.length;
    }
  }
}

folly::IOBuf NimbleIndexProjector::serializeStripe(
    uint32_t stripeIndex,
    InputStreams& inputStreams) {
  const auto numRows = stripeRowCount(stripeIndex);

  // Build the shared body+trailer for this stripe:
  //   [stream_data...][encodingType][stream_sizes][trailer_size:u32].
  // The version/rowCount/row-range/resume-key header is per-slice and is
  // built later by buildResult().

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

  stats_.numReadRows += numRows;
  stats_.numOutputBytes += bodySize;
  return std::move(*body);
}

void NimbleIndexProjector::setResumeKeys(Result& result) {
  if (!ctx_.plan.truncated) {
    return;
  }
  const auto& lastPlan = ctx_.plan.stripePlans.back();
  const auto stripeIndex = lastPlan.stripeIndex;
  const auto& stripeRanges = lastPlan.stripeRanges;

  // No next stripe in the range map — all mapped requests end at this stripe.
  const auto nextStripe = stripeIndex + 1;
  if (nextStripe >=
      ctx_.stripeRanges.startStripe + ctx_.stripeRanges.numStripes) {
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
  const auto nextRanges =
      ctx_.stripeRanges.getRanges(nextStripe - ctx_.stripeRanges.startStripe);

  for (const auto& request : stripeRanges) {
    auto& response = result.responses[request.requestIndex];
    NIMBLE_CHECK(!response.resumeKey.has_value());
    if (std::any_of(
            nextRanges.begin(), nextRanges.end(), [&](const auto& range) {
              return range.requestIndex == request.requestIndex;
            })) {
      response.resumeKey = resumeKey;
    }
  }

  // For requests that were never started (no stripe ranges in any plan),
  // set resume key to their original lower key so the caller can retry.
  for (size_t i = 0; i < result.responses.size(); ++i) {
    auto& response = result.responses[i];
    if (!ctx_.hasStripeRanges[i] && !response.resumeKey.has_value()) {
      const auto& keyBounds = ctx_.request->keyBounds[i];
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
folly::IOBuf assembleStripeSlice(
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

void NimbleIndexProjector::buildResult(Result& result) {
  // Build per-response slice counts for reserve.
  std::vector<size_t> sliceCounts(ctx_.numRequests, 0);
  for (size_t i = 0; i < ctx_.plan.stripePlans.size(); ++i) {
    for (const auto& range : ctx_.plan.stripePlans[i].stripeRanges) {
      ++sliceCounts[range.requestIndex];
    }
  }
  for (size_t i = 0; i < ctx_.numRequests; ++i) {
    result.responses[i].slices.reserve(sliceCounts[i]);
  }

  // Track per-response how many slices have been emitted so we know when
  // we're at the last one (for embedding the resume key).
  std::vector<size_t> slicesEmitted(ctx_.numRequests, 0);

  for (size_t i = 0; i < ctx_.plan.stripePlans.size(); ++i) {
    const auto& stripePlan = ctx_.plan.stripePlans[i];
    auto& stripeBody = ctx_.stripeBodies[i];

    for (const auto& range : stripePlan.stripeRanges) {
      NIMBLE_CHECK(!range.rowRange.empty());
      stats_.numProjectedRows += range.rowRange.numRows();
      ++slicesEmitted[range.requestIndex];
      const bool isLastSlice =
          slicesEmitted[range.requestIndex] == sliceCounts[range.requestIndex];
      auto& response = result.responses[range.requestIndex];
      response.slices.emplace_back(assembleStripeSlice(
          stripePlan.numRows,
          range.rowRange,
          stripeBody.cloneOneAsValue(),
          isLastSlice ? response.resumeKey : std::nullopt));
    }
  }
}

} // namespace facebook::nimble
