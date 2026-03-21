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

#pragma once

#include <folly/io/IOBuf.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "dwio/nimble/index/TabletIndex.h"
#include "dwio/nimble/velox/RowRange.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "dwio/nimble/velox/selective/ReaderBase.h"
#include "velox/common/time/CpuWallTimer.h"
#include "velox/serializers/KeyEncoder.h"
#include "velox/type/Subfield.h"

namespace facebook::nimble {

using Subfield = velox::common::Subfield;

/// NimbleIndexProjector takes a batch of index lookup requests (point lookups
/// or range scans with already-encoded keys) and column projections, uses the
/// Nimble cluster index to locate relevant stripes and row ranges, then reads
/// and serializes the projected columns for transport.
///
/// The output uses kTabletRaw serialization format, which passes raw tablet
/// stream bytes through (including chunk headers). The Deserializer handles
/// chunk header stripping on the client side, shifting CPU cost from the
/// centralized server to distributed clients.
///
/// Usage:
///   auto result = projector.project(request, options);
///   for (size_t i = 0; i < result.responses.size(); ++i) {
///     const auto& response = result.responses[i];
///     for (const auto& slice : response.slices) {
///       const auto& chunk = result.chunks[slice.chunkIndex];
///       // process chunk.data (IOBuf), rows slice.rows
///     }
///   }
/// NOTE: NimbleIndexProjector is not thread-safe. Each thread must use its
/// own instance.
class NimbleIndexProjector {
 public:
  // TODO: projectedSubfields currently must match file schema column names.
  // Add table-to-file column name mapping for schema evolution support.
  NimbleIndexProjector(
      std::shared_ptr<ReaderBase> readerBase,
      const std::vector<Subfield>& projectedSubfields);

  ~NimbleIndexProjector() = default;

  /// Options for controlling projection behavior.
  struct Options {
    /// Maximum number of rows per request. 0 means no limit.
    uint64_t maxRowsPerRequest{0};
  };

  /// Request for a batch of index lookups.
  struct Request {
    /// Pre-encoded key bounds for each lookup.
    std::vector<velox::serializer::EncodedKeyBounds> keyBounds;
  };

  /// Serialized data for a contiguous row range.
  struct Chunk {
    /// kTabletRaw serialized data containing projected columns.
    folly::IOBuf data;
    /// Number of rows in this chunk.
    uint32_t numRows{0};
  };

  /// Slice into a shared chunk for a specific request.
  struct ChunkSlice {
    /// Index into Result::chunks.
    uint32_t chunkIndex{0};
    /// Row range within the chunk.
    RowRange rows;
  };

  /// Response for a single request.
  struct Response {
    /// Slices into Result::chunks for this request. Empty for miss.
    std::vector<ChunkSlice> slices;

    /// If the result was truncated (e.g. by maxResultBytes), the encoded
    /// resume key for the next project() call. nullopt if complete or miss.
    std::optional<velox::serializer::EncodedKeyBounds> resumeKey;
  };

  /// Result of a project() call.
  struct Result {
    /// Serialized chunks (shared across requests that overlap).
    std::vector<Chunk> chunks;
    /// One entry per request, in request order.
    std::vector<Response> responses;
  };

  /// Projects the requested columns for the given batch of index lookups.
  /// Returns one Response per request, in order. Processes all relevant
  /// stripes internally.
  Result project(const Request& request, const Options& options);

  /// Returns the projected nimble schema. Preserves encoding-specific types
  /// (ArrayWithOffsets, SlidingWindowMap, FlatMap) from the file schema.
  /// Clients need this to build a Deserializer for the output data.
  const std::shared_ptr<const Type>& projectedNimbleType() const {
    return projectedNimbleType_;
  }

  /// Statistics captured during project().
  struct Stats {
    /// Number of stripes read from the tablet.
    uint32_t numReadStripes{0};
    /// Number of rows scanned from stripes (entire stripe row counts).
    uint64_t numScannedRows{0};
    /// Number of rows in the projected result. Currently equals
    /// numScannedRows since we project entire stripes. With fine-grained
    /// row range fetches (value fetch), this will be smaller.
    uint64_t numProjectedRows{0};
    /// Number of rows read by request row ranges (may be less than matched
    /// rows when truncated by maxRowsPerRequest).
    uint64_t numReadRows{0};
    /// Total bytes read from tablet stream data (raw encoded bytes, excluding
    /// serialization overhead like headers and trailers).
    uint64_t numReadBytes{0};

    /// Time spent looking up stripes and row ranges via the tablet index.
    velox::CpuWallTiming lookupTiming;
    /// Time spent loading stripe stream data from tablet.
    velox::CpuWallTiming scanTiming;
    /// Time spent serializing projected streams into kTabletRaw format.
    velox::CpuWallTiming projectionTiming;

    std::string toString() const;
  };

  /// Returns cumulative statistics across all project() calls.
  const Stats& stats() const {
    return stats_;
  }

 private:
  // Map from stripe index to request indices that need data from it.
  // Sorted by stripe index for sequential I/O.
  using StripeMapping = std::map<uint32_t, std::vector<velox::vector_size_t>>;

  // Maps requests to stripes using the tablet index.
  StripeMapping lookupStripes();

  // Processes a single stripe: loads index + data, looks up row ranges,
  // reads and serializes segments.
  void processStripe(
      uint32_t stripeIndex,
      const std::vector<velox::vector_size_t>& requestIndices,
      Result& result);

  // Row range for a specific request within a stripe.
  struct RequestRange {
    velox::vector_size_t requestIndex{};
    RowRange rowRange;
  };

  // Sets up the stripe with index metadata, creates an index reader, and looks
  // up row ranges for requests within the stripe. Indexes into
  // request_->keyBounds via requestIndices. Filters empty ranges and applies
  // maxRowsPerRequest truncation.
  std::vector<RequestRange> lookupRowRanges(
      uint32_t stripeIndex,
      const std::vector<velox::vector_size_t>& requestIndices);

  using InputStreams =
      std::vector<std::unique_ptr<velox::dwio::common::SeekableInputStream>>;

  // Enqueues and loads projected data streams from the tablet.
  InputStreams loadStripe();

  // Stitches loaded raw tablet bytes into kTabletRaw format without
  // decode/re-encode. Updates scan/projection stats.
  Chunk serializeStripe(uint32_t stripeIndex, InputStreams& inputStreams);

  // Maps the serialized stripe chunk to request results based on row ranges.
  void buildStripeResult(
      Chunk&& chunk,
      const std::vector<RequestRange>& requestRanges,
      Result& result);

  inline uint32_t stripeRowCount(uint32_t stripeIndex) const {
    return static_cast<uint32_t>(
        readerBase_->tablet().stripeRowCount(stripeIndex));
  }

  const std::shared_ptr<ReaderBase> readerBase_;
  velox::memory::MemoryPool* const pool_;
  const TabletIndex* const tabletIndex_;
  const uint32_t numStripes_{0};

  // Projected nimble schema built from file nimble schema. Preserves
  // encoding-specific types (ArrayWithOffsets, SlidingWindowMap, FlatMap).
  std::shared_ptr<const Type> projectedNimbleType_;
  std::vector<uint32_t> projectedStreamOffsets_;

  StripeStreams streams_;

  Stats stats_;

  // Set during project() and cleared on return.
  const Request* request_{nullptr};
  const Options* options_{nullptr};
  std::vector<uint64_t> rowsPerRequest_;
};

} // namespace facebook::nimble
