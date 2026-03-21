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

#include "dwio/nimble/index/IndexReader.h"
#include "dwio/nimble/serializer/SerializerImpl.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "folly/ScopeGuard.h"

namespace facebook::nimble {

using namespace facebook::velox; // NOLINT(google-build-using-namespace)

std::string NimbleIndexProjector::Stats::toString() const {
  return fmt::format(
      "Stats(numReadStripes={}, numScannedRows={}, numProjectedRows={}, numReadRows={}, "
      "numReadBytes={}, lookupTiming=[{}], scanTiming=[{}], projectionTiming=[{}])",
      numReadStripes,
      numScannedRows,
      numProjectedRows,
      numReadRows,
      numReadBytes,
      lookupTiming.toString(),
      scanTiming.toString(),
      projectionTiming.toString());
}

NimbleIndexProjector::NimbleIndexProjector(
    std::shared_ptr<ReaderBase> readerBase,
    const std::vector<Subfield>& projectedSubfields)
    : readerBase_(std::move(readerBase)),
      pool_{readerBase_->pool()},
      tabletIndex_{readerBase_->tablet().index()},
      numStripes_{readerBase_->tablet().stripeCount()},
      streams_{readerBase_} {
  NIMBLE_CHECK_NOT_NULL(
      tabletIndex_, "NimbleIndexProjector requires a tablet with an index");

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
  };

  const auto numRequests = request_->keyBounds.size();
  Result result;
  result.responses.resize(numRequests);

  if (numRequests == 0 || tabletIndex_->empty()) {
    return result;
  }

  StripeMapping stripeMapping;
  {
    velox::CpuWallTimer timer(stats_.lookupTiming);
    stripeMapping = lookupStripes();
  }

  rowsPerRequest_.assign(numRequests, 0);

  for (auto& [stripeIndex, requestIndices] : stripeMapping) {
    // Remove requests that have already read maxRowsPerRequest.
    if (options_->maxRowsPerRequest > 0) {
      std::erase_if(requestIndices, [&](auto idx) {
        return rowsPerRequest_[idx] >= options_->maxRowsPerRequest;
      });
      if (requestIndices.empty()) {
        continue;
      }
    }
    processStripe(stripeIndex, requestIndices, result);
  }
  return result;
}

NimbleIndexProjector::StripeMapping NimbleIndexProjector::lookupStripes() {
  const auto& encodedKeyBounds = request_->keyBounds;
  const auto numRequests = encodedKeyBounds.size();

  StripeMapping mapping;
  for (size_t requestIdx = 0; requestIdx < numRequests; ++requestIdx) {
    const auto& bounds = encodedKeyBounds[requestIdx];

    uint32_t startStripe = 0;
    if (bounds.lowerKey.has_value()) {
      const auto stripeLocation = tabletIndex_->lookup(bounds.lowerKey.value());
      if (stripeLocation.has_value()) {
        startStripe = static_cast<uint32_t>(stripeLocation->stripeIndex);
      } else if (bounds.lowerKey.value() > tabletIndex_->maxKey()) {
        continue;
      }
    }

    uint32_t endStripe = numStripes_;
    if (bounds.upperKey.has_value()) {
      const auto stripeLocation = tabletIndex_->lookup(bounds.upperKey.value());
      if (stripeLocation.has_value()) {
        endStripe = std::min(
            endStripe, static_cast<uint32_t>(stripeLocation->stripeIndex) + 1);
      } else if (bounds.upperKey.value() < tabletIndex_->minKey()) {
        continue;
      }
    }

    if (startStripe >= endStripe) {
      continue;
    }

    for (uint32_t stripe = startStripe; stripe < endStripe; ++stripe) {
      mapping[stripe].emplace_back(requestIdx);
    }
  }
  return mapping;
}

void NimbleIndexProjector::processStripe(
    uint32_t stripeIndex,
    const std::vector<vector_size_t>& requestIndices,
    Result& result) {
  auto requestRanges = lookupRowRanges(stripeIndex, requestIndices);
  if (requestRanges.empty()) {
    return;
  }

  // TODO: Support fine-grained row range fetches from a stripe. For now, we
  // always fetch entire projected column streams and build the serialized
  // values. The result references a portion of it based on the row range.
  auto inputStreams = loadStripe();
  auto stripeChunk = serializeStripe(stripeIndex, inputStreams);
  buildStripeResult(std::move(stripeChunk), requestRanges, result);
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

  std::vector<uint32_t> streamSizes(projectedStreamOffsets_.size(), 0);
  for (size_t i = 0; i < inputStreams.size(); ++i) {
    if (inputStreams[i] == nullptr) {
      continue;
    }

    // Read raw tablet bytes and append to output chain.
    const void* data;
    int size;
    while (inputStreams[i]->Next(&data, &size)) {
      output->appendToChain(folly::IOBuf::copyBuffer(data, size));
      streamSizes[i] += static_cast<uint32_t>(size);
    }
  }

  std::string trailerBuf;
  serde::detail::writeRawTrailer(
      streamSizes, EncodingType::Trivial, trailerBuf);
  output->appendToChain(
      folly::IOBuf::copyBuffer(trailerBuf.data(), trailerBuf.size()));

  Chunk chunk;
  chunk.data = std::move(*output);
  chunk.numRows = numStripeRows;

  stats_.numScannedRows += numStripeRows;
  stats_.numProjectedRows += numStripeRows;
  stats_.numReadBytes += chunk.data.computeChainDataLength();
  return chunk;
}

void NimbleIndexProjector::buildStripeResult(
    Chunk&& chunk,
    const std::vector<RequestRange>& requestRanges,
    Result& result) {
  const auto chunkIndex = static_cast<uint32_t>(result.chunks.size());

  for (const auto& requestRange : requestRanges) {
    ChunkSlice slice;
    slice.chunkIndex = chunkIndex;
    slice.rows = requestRange.rowRange;
    result.responses[requestRange.requestIndex].slices.emplace_back(slice);
    rowsPerRequest_[requestRange.requestIndex] +=
        requestRange.rowRange.numRows();
  }

  result.chunks.emplace_back(std::move(chunk));
}

std::vector<NimbleIndexProjector::RequestRange>
NimbleIndexProjector::lookupRowRanges(
    uint32_t stripeIndex,
    const std::vector<vector_size_t>& requestIndices) {
  velox::CpuWallTimer timer(stats_.lookupTiming);

  // Set up the stripe with index metadata.
  streams_.setStripe(stripeIndex, /*loadIndex=*/true);

  // Enqueue and load key stream for index lookup.
  auto indexReader = index::IndexReader::create(
      streams_.enqueueKeyStream(),
      streams_.stripeIndex(),
      streams_.indexGroup(),
      pool_);
  streams_.load();

  const auto numStripeRows =
      static_cast<vector_size_t>(stripeRowCount(stripeIndex));

  std::vector<RequestRange> result;
  result.reserve(requestIndices.size());

  for (size_t i = 0; i < requestIndices.size(); ++i) {
    const auto& bounds = request_->keyBounds[requestIndices[i]];

    vector_size_t startRow = 0;
    if (bounds.lowerKey.has_value()) {
      const auto lowerRow = indexReader->seekAtOrAfter(bounds.lowerKey.value());
      // The tablet index already verified lowerKey <= stripe's max key, so
      // there must be at least one row with key >= lowerKey in this stripe.
      NIMBLE_CHECK(
          lowerRow.has_value(),
          "seekAtOrAfter returned nullopt for stripe {} with lowerKey that "
          "passed tablet index lookup",
          stripeIndex);
      startRow = static_cast<vector_size_t>(lowerRow.value());
    }

    vector_size_t endRow = numStripeRows;
    if (bounds.upperKey.has_value()) {
      const auto upperRow = indexReader->seekAtOrAfter(bounds.upperKey.value());
      if (upperRow.has_value()) {
        endRow = static_cast<vector_size_t>(upperRow.value());
      }
    }

    RowRange range(startRow, endRow);
    // Empty range can happen for point lookups where the key doesn't exist in
    // this stripe: seekAtOrAfter returns the same position for both bounds.
    if (range.empty()) {
      continue;
    }

    const auto requestIdx = requestIndices[i];
    if (options_->maxRowsPerRequest > 0) {
      const auto remaining =
          options_->maxRowsPerRequest - rowsPerRequest_[requestIdx];

      // Saturated requests are already filtered out by the caller in
      // project(), so remaining should always be positive here.
      NIMBLE_CHECK_GT(remaining, 0);

      if (static_cast<uint64_t>(range.numRows()) > remaining) {
        range.endRow = range.startRow + static_cast<vector_size_t>(remaining);
      }
    }
    stats_.numReadRows += range.numRows();
    result.emplace_back(RequestRange{requestIdx, range});
  }
  return result;
}

} // namespace facebook::nimble
