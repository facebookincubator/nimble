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

std::unique_ptr<DataInput> createDataInput(
    const velox::FileHandle& fileHandle,
    const velox::dwio::common::ReaderOptions& options) {
  DirectDataInput::Options dataInputOptions;
  dataInputOptions.pool = &options.memoryPool();
  dataInputOptions.ioStats = options.dataIoStats();
  dataInputOptions.maxCoalesceDistance = options.maxCoalesceDistance();
  dataInputOptions.maxCoalesceBytes = options.maxCoalesceBytes();
  return std::make_unique<DirectDataInput>(
      fileHandle.file.get(), dataInputOptions);
}

void freeDataHandle(void* /*buf*/, void* userData) {
  delete static_cast<DataInput::Handle*>(userData);
}

} // namespace

std::unique_ptr<NimbleIndexProjector> NimbleIndexProjector::create(
    TabletReaderCache& tabletReaderCache,
    const velox::FileHandle& fileHandle,
    const std::vector<Subfield>& projectedSubfields,
    const velox::dwio::common::ReaderOptions& options) {
  validateReaderOptions(options);
  NIMBLE_CHECK(
      !options.cacheData(),
      "NimbleIndexProjector does not support data caching");
  auto cached = tabletReaderCache.get(
      fileHandle.file, TabletReader::configureOptions(options));
  return std::unique_ptr<NimbleIndexProjector>(new NimbleIndexProjector(
      std::move(cached.tablet),
      std::move(cached.nimbleSchema),
      fileHandle.file,
      createDataInput(fileHandle, options),
      projectedSubfields,
      &options.memoryPool(),
      options.dataIoStats()));
}

NimbleIndexProjector::NimbleIndexProjector(
    std::shared_ptr<TabletReader> tablet,
    std::shared_ptr<const Type> nimbleSchema,
    std::shared_ptr<velox::ReadFile> file,
    std::unique_ptr<DataInput> dataInput,
    const std::vector<Subfield>& projectedSubfields,
    velox::memory::MemoryPool* pool,
    std::shared_ptr<velox::io::IoStatistics> ioStats)
    : file_{std::move(file)},
      tablet_{std::move(tablet)},
      ioStats_{std::move(ioStats)},
      pool_{pool},
      dataInput_{std::move(dataInput)},
      clusterIndex_{tablet_->clusterIndex()},
      numStripes_{tablet_->stripeCount()} {
  NIMBLE_CHECK_NOT_NULL(
      clusterIndex_, "NimbleIndexProjector requires a tablet with an index");
  NIMBLE_CHECK_GT(numStripes_, 0, "NimbleIndexProjector requires stripes");

  projectedNimbleType_ = buildProjectedNimbleType(
      nimbleSchema.get(), projectedSubfields, projectedStreamOffsets_);
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
  ctx_.request = nullptr;
  ctx_.options = nullptr;
  ctx_.numRequests = 0;
  ctx_.stripeRanges.clear();
  ctx_.plan.stripePlans.clear();
  ctx_.plan.truncated = false;
  ctx_.hasStripeRanges.clear();
  ctx_.dataInputIndices.clear();
  ctx_.dataHandle.reset();
  ctx_.stripeBodies.clear();
  dataInput_->clear();
}

RowRange NimbleIndexProjector::stripeRowRange(
    uint32_t stripe,
    const RowRange& rowRangeLimit) const {
  const auto stripeStart =
      static_cast<uint32_t>(tablet_->stripeStartRow(stripe));
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
  for (uint32_t requestIndex = 0; requestIndex < ctx_.numRequests;
       ++requestIndex) {
    const auto& ranges = result[requestIndex];
    if (ranges.empty()) {
      continue;
    }
    NIMBLE_CHECK_EQ(ranges.size(), 1, "Expected single row range per lookup");
    const auto& range = ranges[0];
    NIMBLE_CHECK(!range.empty());

    const uint32_t startStripe = tablet_->rowToStripe(range.startRow);
    const uint32_t endStripe = tablet_->rowToStripe(range.endRow - 1) + 1;
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

  // Build CSR layout with write-cursor pattern: count entries per stripe,
  // prefix-sum into offsets, then fill entries by index.
  ctx_.stripeRanges.offsets.assign(ctx_.stripeRanges.numStripes + 1, 0);
  for (const auto& request : resolvedRequests) {
    for (uint32_t stripe = request.startStripe; stripe < request.endStripe;
         ++stripe) {
      ++ctx_.stripeRanges.offsets[stripe - minStripe + 1];
    }
  }
  for (uint32_t i = 1; i <= ctx_.stripeRanges.numStripes; ++i) {
    ctx_.stripeRanges.offsets[i] += ctx_.stripeRanges.offsets[i - 1];
  }

  const auto numRanges = ctx_.stripeRanges.offsets.back();
  ctx_.stripeRanges.ranges.resize(numRanges);
  auto writeCursors = ctx_.stripeRanges.offsets;
  for (const auto& request : resolvedRequests) {
    for (uint32_t stripe = request.startStripe; stripe < request.endStripe;
         ++stripe) {
      const auto stripeOffset = stripe - minStripe;
      ctx_.stripeRanges.ranges[writeCursors[stripeOffset]++] = StripeRange{
          request.requestIndex, stripeRowRange(stripe, request.rowRange)};
    }
  }
}

void NimbleIndexProjector::loadStripes() {
  const auto& stripePlans = ctx_.plan.stripePlans;
  if (stripePlans.empty()) {
    return;
  }
  velox::CpuWallTimer timer(stats_.scanTiming);

  uint32_t totalStreams = 0;
  for (const auto& plan : stripePlans) {
    totalStreams += plan.numStreams;
  }
  dataInput_->reserve(totalStreams);

  const auto numProjectedStreams = projectedStreamOffsets_.size();
  ctx_.dataInputIndices.resize(stripePlans.size() * numProjectedStreams);
  for (size_t stripeOffset = 0; stripeOffset < stripePlans.size();
       ++stripeOffset) {
    dataInput_->startGroup();
    const auto base = stripeOffset * numProjectedStreams;
    const auto& streams = stripePlans[stripeOffset].projectedStreams;
    for (size_t streamIndex = 0; streamIndex < streams.size(); ++streamIndex) {
      const auto& stream = streams[streamIndex];
      if (!stream.has_value()) {
        continue;
      }
      ctx_.dataInputIndices[base + streamIndex] = dataInput_->enqueue(*stream);
    }
  }
  ctx_.dataHandle = dataInput_->load();
}

NimbleIndexProjector::Result NimbleIndexProjector::processStripes() {
  velox::CpuWallTimer timer(stats_.projectionTiming);
  Result result;
  result.responses.resize(ctx_.numRequests);
  ctx_.stripeBodies.resize(ctx_.plan.stripePlans.size());
  for (size_t i = 0; i < ctx_.plan.stripePlans.size(); ++i) {
    ++stats_.numReadStripes;
    ctx_.stripeBodies[i] = packStripe(i);
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

  const auto stripeId = tablet_->stripeIdentifier(stripePlan.stripeIndex);
  const auto streamCount = tablet_->streamCount(stripeId);
  const auto stripeOffset = tablet_->stripeOffset(stripePlan.stripeIndex);
  const auto& streamOffsets = tablet_->streamOffsets(stripeId);
  const auto& streamSizes = tablet_->streamSizes(stripeId);

  stripePlan.projectedStreams.resize(projectedStreamOffsets_.size());
  for (size_t i = 0; i < projectedStreamOffsets_.size(); ++i) {
    const auto streamId = projectedStreamOffsets_[i];
    if (streamId >= streamCount || streamSizes[streamId] == 0) {
      continue;
    }
    stripePlan.projectedStreams[i] = velox::common::Region{
        stripeOffset + streamOffsets[streamId], streamSizes[streamId]};
    ++stripePlan.numStreams;
    stripePlan.projectedBytes += streamSizes[streamId];
  }
}

folly::IOBuf NimbleIndexProjector::packStripe(size_t stripeOffset) {
  const auto& stripePlan = ctx_.plan.stripePlans[stripeOffset];
  const auto numRows = stripeRowCount(stripePlan.stripeIndex);
  const auto numProjectedStreams = projectedStreamOffsets_.size();
  const auto base = stripeOffset * numProjectedStreams;

  // Build stream sizes from the precomputed projected regions.
  std::vector<uint32_t> streamSizes(numProjectedStreams, 0);
  for (size_t i = 0; i < numProjectedStreams; ++i) {
    if (stripePlan.projectedStreams[i].has_value()) {
      streamSizes[i] =
          static_cast<uint32_t>(stripePlan.projectedStreams[i]->length);
    }
  }

  // First stream IOBuf holds the DataInput::Handle via takeOwnership,
  // keeping the loaded data alive. Subsequent streams use wrapBuffer
  // (no ownership) since the chain is always cloned/destroyed as a unit.
  std::unique_ptr<folly::IOBuf> chain;
  for (size_t i = 0; i < numProjectedStreams; ++i) {
    if (!ctx_.dataInputIndices[base + i].has_value()) {
      continue;
    }
    const auto& ref = dataInput_->bufferRef(*ctx_.dataInputIndices[base + i]);
    if (chain == nullptr) {
      chain = folly::IOBuf::takeOwnership(
          const_cast<char*>(ref.data),
          ref.length,
          freeDataHandle,
          new DataInput::Handle(ctx_.dataHandle));
    } else {
      chain->appendToChain(folly::IOBuf::wrapBuffer(ref.data, ref.length));
    }
  }

  std::string trailerBuf;
  serde::detail::writeTrailer(
      streamSizes,
      EncodingType::FixedBitWidth,
      EncodingType::FixedBitWidth,
      trailerBuf);

  NIMBLE_CHECK_NOT_NULL(chain);
  auto trailer = folly::IOBuf::copyBuffer(trailerBuf);
  chain->appendToChain(std::move(trailer));

  const size_t bodySize = stripePlan.projectedBytes + trailerBuf.size();
  stats_.numReadRows += numRows;
  stats_.numOutputBytes += bodySize;
  return std::move(*chain);
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
      static_cast<uint32_t>(tablet_->stripeStartRow(stripeIndex));
  const auto nextStripeStartRow = stripeStartRow + stripeRowCount(stripeIndex);
  if (nextStripeStartRow >= tablet_->tabletRowCount()) {
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
          stripeBody.cloneAsValue(),
          isLastSlice ? response.resumeKey : std::nullopt));
    }
  }
}

} // namespace facebook::nimble
