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
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "dwio/nimble/index/ClusterIndex.h"
#include "dwio/nimble/velox/RowRange.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "dwio/nimble/velox/selective/ReaderBase.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileHandle.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/time/CpuWallTimer.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/serializers/KeyEncoder.h"
#include "velox/type/Subfield.h"

namespace facebook::nimble {

class TabletReaderCache;

using Subfield = velox::common::Subfield;

/// NimbleIndexProjector takes a batch of index lookup requests (point lookups
/// or range scans with already-encoded keys) and column projections, uses the
/// Nimble cluster index to locate relevant stripes and row ranges, then reads
/// and serializes the projected columns for transport.
///
/// The output uses kTablet serialization format with a fixed-shape
/// per-slice header in front of the stripe body+trailer:
///   NODE 1 (per-slice header):
///     [version:1B=3][rowCount:varint]
///     [startRow:varint][endRow:varint]
///     [resumeKeyLength:varint]   0 = no key; N>0 = key of length N-1
///     [resumeKey bytes]          present when resumeKeyLength > 0
///   NODE 2 (shared stripe body + trailer):
///     [stream_data_0...][encodingType:1B][stream_sizes][trailer_size:u32]
///
/// The stripe slice IOBuf is a 2-node chain: a per-slice header node and a
/// shared body+trailer node. Multiple requests that hit the same stripe with
/// different row ranges share the body+trailer bytes via
/// `folly::IOBuf::cloneOne` (refcounted SharedInfo); only the header node
/// is unique per slice.
///
/// Usage:
///   auto result = projector.project(request, options);
///   for (size_t i = 0; i < result.responses.size(); ++i) {
///     const auto& response = result.responses[i];
///     for (const auto& slice : response.slices) {
///       // `slice` is a self-describing kTablet IOBuf chain with
///       // the request's row range and (on the last slice of a truncated
///       // response) the resume key embedded in the header.
///     }
///   }
/// NOTE: NimbleIndexProjector is not thread-safe. Each thread must use its
/// own instance.
class NimbleIndexProjector {
 public:
  /// Creates a NimbleIndexProjector with appropriate BufferedInput based on
  /// whether a cache is provided. Uses CachedBufferedInput when cache is
  /// non-null, DirectBufferedInput otherwise.
  // TODO: projectedSubfields currently must match file schema column names.
  // Add table-to-file column name mapping for schema evolution support.
  static std::unique_ptr<NimbleIndexProjector> create(
      const velox::FileHandle& fileHandle,
      velox::cache::AsyncDataCache* cache,
      const std::vector<Subfield>& projectedSubfields,
      const velox::dwio::common::ReaderOptions& options);

  /// Creates a NimbleIndexProjector sharing a TabletReader from the
  /// provided TabletReaderCache.
  static std::unique_ptr<NimbleIndexProjector> create(
      TabletReaderCache& tabletReaderCache,
      const velox::FileHandle& fileHandle,
      velox::cache::AsyncDataCache* cache,
      const std::vector<Subfield>& projectedSubfields,
      const velox::dwio::common::ReaderOptions& options);

  ~NimbleIndexProjector() = default;

  /// Options for controlling projection behavior.
  struct Options {
    /// Soft limit on total rows across all requests. 0 means no limit.
    /// When the running total exceeds this limit mid-stripe, the entire
    /// stripe is still included (stripe-boundary soft limit). Processing
    /// stops after that stripe completes.
    uint64_t maxRows{0};
    /// Soft limit on total serialized bytes across all requests. 0 means no
    /// limit. Like maxRows, operates at stripe granularity: at least one
    /// stripe is always included, then processing stops after the stripe
    /// that exceeds the budget.
    uint64_t maxBytes{0};
    /// Hard per-request row limit. 0 means no limit. Each request's row
    /// range is clipped so that it never exceeds this many rows total.
    /// No resume key is set — the request is considered fulfilled.
    uint64_t maxRowsPerRequest{0};
  };

  /// Request for a batch of index lookups.
  struct Request {
    /// Pre-encoded key bounds for each lookup.
    std::vector<velox::serializer::EncodedKeyBounds> keyBounds;
  };

  /// Response for a single request.
  struct Response {
    /// One self-describing kTablet IOBuf chain per (request × stripe)
    /// intersection. Each entry covers a contiguous row range within one
    /// stripe; the row range is embedded in the chain's header. Empty for
    /// miss. Slices for overlapping requests share the body+trailer bytes
    /// via refcounted SharedInfo on the second IOBuf node.
    ///
    /// If the response is truncated and has a resume key, it is embedded in
    /// the header of the last slice. Use `rocks::readResultResumeKey()` on
    /// the last slice to extract it.
    std::vector<folly::IOBuf> slices;

    /// If the result was truncated by maxRows or maxBytes, the encoded resume
    /// key for continuation. The caller constructs new key bounds using this
    /// as lowerKey with their original upperKey. nullopt if complete or miss.
    ///
    /// Also embedded in the last slice's per-slice header (when slices
    /// is non-empty) so consumers that hold an IOBuf can recover the key
    /// without keeping the Response struct around.
    std::optional<std::string> resumeKey;
  };

  /// Result of a project() call.
  struct Result {
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
    /// Total rows read from storage (entire stripe row counts). Includes
    /// rows outside the requested row ranges that are read because we
    /// fetch entire projected streams per stripe.
    uint64_t numReadRows{0};
    /// Total rows in the output result (only the requested row ranges).
    /// The difference numReadRows - numProjectedRows is over-fetched rows.
    uint64_t numProjectedRows{0};
    /// Total serialized output bytes.
    uint64_t numOutputBytes{0};

    /// Time spent looking up stripes and row ranges via the tablet index.
    velox::CpuWallTiming lookupTiming;
    /// Time spent loading stripe stream data from tablet.
    velox::CpuWallTiming scanTiming;
    /// Time spent serializing projected streams into kTablet format.
    velox::CpuWallTiming projectionTiming;

    std::string toString() const;
  };

  /// Returns cumulative statistics across all project() calls.
  const Stats& stats() const {
    return stats_;
  }

 private:
  NimbleIndexProjector(
      std::shared_ptr<ReaderBase> readerBase,
      const std::vector<Subfield>& projectedSubfields,
      const velox::dwio::common::ReaderOptions& options);

  // A request index paired with its stripe-relative row range.
  struct StripeRange {
    uint32_t requestIndex{};
    // Stripe-relative row range, already intersected with stripe boundaries.
    RowRange rowRange;
  };

  // CSR (compressed sparse row) layout mapping stripes to request row ranges.
  // All StripeRange entries are stored in a single flat vector (`entries`),
  // with `offsets[i]` marking where stripe i's entries begin.
  struct StripeRanges {
    uint32_t startStripe{0};
    uint32_t numStripes{0};
    // Flat storage of all per-stripe row ranges, grouped by stripe.
    std::vector<StripeRange> ranges;
    // offsets[i] = start index in ranges for stripe i (relative to
    // startStripe). Size = numStripes + 1.
    std::vector<uint32_t> offsets;

    std::span<const StripeRange> getRanges(uint32_t stripeIndex) const {
      NIMBLE_CHECK_LT(stripeIndex, numStripes);
      return {
          ranges.data() + offsets[stripeIndex],
          offsets[stripeIndex + 1] - offsets[stripeIndex]};
    }

    void clear() {
      startStripe = 0;
      numStripes = 0;
      ranges.clear();
      offsets.clear();
    }
  };

  // Initializes per-project() state: stores request/options pointers and
  // resets running totals.
  void initRequest(const Request& request, const Options& options);

  // Clears all per-project() state set by initRequest(). Invoked on scope
  // exit so a subsequent project() call starts from a clean slate.
  void clearRequest();

  // Looks up all requests via the cluster index and maps them to stripes.
  // Populates ctx_.stripeRanges.
  void lookupStripes();

  using InputStreams =
      std::vector<std::unique_ptr<velox::dwio::common::SeekableInputStream>>;

  // Per-stripe plan produced by prepareStripes(). Contains the stripe's
  // projected stream locations (for IO) and the per-request row ranges
  // (for result building).
  struct StripePlan {
    uint32_t stripeIndex{};
    // Total rows in this stripe.
    uint32_t numRows{0};
    // Total bytes across all projected streams in this stripe.
    uint64_t projectedBytes{0};
    // Stream locations for projected columns. Indexed by projected stream
    // offset; nullopt for streams absent in this stripe.
    std::vector<std::optional<StreamLocation>> projectedStreams;
    // Per-request row ranges intersected with this stripe, after pruning
    // saturated requests.
    std::vector<StripeRange> stripeRanges;
  };

  // Output of prepareStripes(): the set of stripes to process and whether
  // global limits (maxRows/maxBytes) caused early termination.
  struct ProjectionPlan {
    std::vector<StripePlan> stripePlans;
    bool truncated{false};
  };

  // Locates projected streams for the stripe and populates projectedStreams
  // and projectedBytes.
  void locateStripeStreams(StripePlan& stripePlan);

  // Removes row ranges for requests that have reached their maxRowsPerRequest
  // budget.
  void pruneStripeRanges(
      std::vector<StripeRange>& stripeRanges,
      const std::vector<uint64_t>& rowsPerRequest);

  // Applies row and byte limits to the looked-up stripe ranges, computes stream
  // metadata for selected stripes, and populates ctx_.plan.
  void prepareStripes();

  // Enqueues all projected streams from ctx_.plan into BufferedInput
  // and issues a single coalesced load() call. Populates ctx_.loadedStripes.
  void loadStripes();

  // Serializes each stripe's loaded streams, builds per-request results,
  // and finalizes the result.
  Result processStripes();

  // Stitches loaded raw stream bytes into the shared kTablet body+trailer
  // (stream data + trailer) without decode/re-encode.
  folly::IOBuf serializeStripe(
      uint32_t stripeIndex,
      InputStreams& inputStreams);

  // If ctx_.plan.truncated, sets resume keys on requests that have data in the
  // next unprocessed stripe.
  void setResumeKeys(Result& result);

  // Iterates ctx_.plan.stripePlans and ctx_.stripeBodies to build per-request
  // output slices. Each slice clones the shared stripe body and prepends a
  // per-request header with the row range and (on the last slice of a
  // truncated response) the resume key.
  void buildResult(Result& result);

  inline uint32_t stripeRowCount(uint32_t stripe) const {
    return static_cast<uint32_t>(readerBase_->tablet().stripeRowCount(stripe));
  }

  // Computes the stripe-relative row range by intersecting the file-level
  // rowRangeLimit with the stripe boundaries.
  RowRange stripeRowRange(uint32_t stripe, const RowRange& rowRangeLimit) const;

  const std::shared_ptr<ReaderBase> readerBase_;
  const std::shared_ptr<velox::io::IoStatistics> ioStats_;
  velox::memory::MemoryPool* const pool_;
  const ClusterIndex* const clusterIndex_;
  const uint32_t numStripes_{0};

  // Projected nimble schema built from file nimble schema. Preserves
  // encoding-specific types (ArrayWithOffsets, SlidingWindowMap, FlatMap).
  std::shared_ptr<const Type> projectedNimbleType_;
  std::vector<uint32_t> projectedStreamOffsets_;

  StripeStreams streams_;

  // Per-project() call state. Set by initRequest(), populated through the
  // pipeline (lookupStripes → prepareStripes → loadStripes → processStripes),
  // and reset on return.
  struct ProjectionContext {
    const Request* request{nullptr};
    const Options* options{nullptr};
    uint32_t numRequests{0};
    // CSR mapping from stripes to request row ranges. Populated by
    // lookupStripes(), read-only during stripe processing.
    StripeRanges stripeRanges;
    // Populated by prepareStripes().
    ProjectionPlan plan;
    // Per-request flag: true if the request has ranges in any StripePlan.
    // Set by prepareStripes(), used by setResumeKeys().
    std::vector<bool> hasStripeRanges;
    // Populated by loadStripes(), consumed during processStripes().
    std::vector<InputStreams> loadedStripes;
    // Serialized stripe bodies, one per StripePlan. Populated by
    // processStripes(), consumed during buildResult().
    std::vector<folly::IOBuf> stripeBodies;
  };
  ProjectionContext ctx_;

  Stats stats_;
};

} // namespace facebook::nimble
