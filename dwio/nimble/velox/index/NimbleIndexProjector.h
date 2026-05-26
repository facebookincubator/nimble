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
/// The chunk slice IOBuf is a 2-node chain: a per-slice header node and a
/// shared body+trailer node. Multiple requests that hit the same stripe with
/// different row ranges share the body+trailer bytes via
/// `folly::IOBuf::cloneOne` (refcounted SharedInfo); only the header node
/// is unique per slice.
///
/// Usage:
///   auto result = projector.project(request, options);
///   for (size_t i = 0; i < result.responses.size(); ++i) {
///     const auto& response = result.responses[i];
///     for (const auto& chunkSlice : response.chunks) {
///       // `chunkSlice` is a self-describing kTablet IOBuf chain with
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
    /// miss. Chunk slices for overlapping requests share the body+trailer
    /// bytes via refcounted SharedInfo on the second IOBuf node.
    ///
    /// If the response is truncated and has a resume key, it is embedded in
    /// the header of the last slice. Use `rocks::readResultResumeKey()` on
    /// the last slice to extract it.
    std::vector<folly::IOBuf> chunks;

    /// If the result was truncated by maxRows or maxBytes, the encoded resume
    /// key for continuation. The caller constructs new key bounds using this
    /// as lowerKey with their original upperKey. nullopt if complete or miss.
    ///
    /// Also embedded in the last slice's per-slice header (when chunks
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
    /// Number of rows scanned from stripes (entire stripe row counts).
    uint64_t numScannedRows{0};
    /// Number of rows in the projected result. Currently equals
    /// numScannedRows since we project entire stripes. With fine-grained
    /// row range fetches (value fetch), this will be smaller.
    uint64_t numProjectedRows{0};
    /// Number of rows read by request row ranges (may exceed maxRows due to
    /// stripe-boundary soft limit).
    uint64_t numReadRows{0};
    /// Total bytes read from tablet stream data (raw encoded bytes, excluding
    /// serialization overhead like headers and trailers).
    uint64_t numReadBytes{0};

    /// Total bytes read from disk, including coalesced/merged regions and
    /// index/key streams. Only available when created with the ReadFile
    /// overload; 0 otherwise.
    uint64_t rawBytesRead{0};
    /// Bytes read from disk but not requested by the projector, due to
    /// BufferedInput merging adjacent regions (coalescing overhead).
    uint64_t rawOverreadBytes{0};
    /// Number of storage read operations (pread syscalls).
    uint64_t numStorageReads{0};
    /// Number of cache hits from the AsyncDataCache (RAM).
    uint64_t numCacheHits{0};
    /// Total bytes served from cache hits.
    uint64_t cacheHitBytes{0};

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
      std::unique_ptr<velox::dwio::common::BufferedInput> bufferedInput,
      const std::vector<Subfield>& projectedSubfields,
      const velox::dwio::common::ReaderOptions& options);

  // A request index paired with its stripe-relative row range.
  struct StripeRowRange {
    uint32_t requestIndex{};
    // Stripe-relative row range, already intersected with stripe boundaries.
    RowRange rowRange;
  };

  // CSR (compressed sparse row) layout mapping stripes to request row ranges.
  // All StripeRowRanges stored in a single flat vector, with stripeOffsets
  // indicating where each stripe's entries begin.
  struct StripeRangeMap {
    uint32_t startStripe{0};
    uint32_t numStripes{0};
    // All stripe row ranges, grouped by stripe in order.
    std::vector<StripeRowRange> rowRanges;
    // stripeOffsets[i] = start index in rowRanges for stripe (startStripe + i).
    // Size = numStripes + 1.
    std::vector<uint32_t> stripeOffsets;

    std::span<const StripeRowRange> stripeRowRanges(uint32_t stripeIdx) const {
      NIMBLE_CHECK_GE(stripeIdx, startStripe);
      NIMBLE_CHECK_LT(stripeIdx, startStripe + numStripes);
      const auto stripeOffset = stripeIdx - startStripe;
      return {
          rowRanges.data() + stripeOffsets[stripeOffset],
          stripeOffsets[stripeOffset + 1] - stripeOffsets[stripeOffset]};
    }

    void clear() {
      startStripe = 0;
      numStripes = 0;
      rowRanges.clear();
      stripeOffsets.clear();
    }
  };

  // Initializes per-project() state: stores request/options pointers, sizes
  // rowsPerRequest_, and resets running totals. Caches numRequests_.
  void initRequest(const Request& request, const Options& options);

  // Clears all per-project() state set by initRequest(). Invoked on scope
  // exit so a subsequent project() call starts from a clean slate.
  void clearRequest();

  // Returns true if a global output limit (maxRows or maxBytes) has been hit.
  bool checkOutputLimit() const {
    return (options_->maxRows > 0 && numReadRows_ >= options_->maxRows) ||
        (options_->maxBytes > 0 && numReadBytes_ >= options_->maxBytes);
  }

  // Looks up all requests via the cluster index and maps them to stripes.
  // Populates stripeRangeMap_.
  void lookupStripes();

  // Prunes stripe row ranges for requests that have exceeded their
  // maxRowsPerRequest budget. Returns false if no active requests remain.
  bool pruneStripeRowRanges(std::span<const StripeRowRange> stripeRowRanges);

  // Loads projected streams for the stripe, serializes them into kTablet
  // format, and maps the result to per-request row ranges. Returns false if
  // a global limit (maxRows or maxBytes) was hit, signaling the caller to
  // stop and set resume keys.
  bool processStripe(
      uint32_t stripeIndex,
      std::span<const StripeRowRange> stripeRowRanges);

  using InputStreams =
      std::vector<std::unique_ptr<velox::dwio::common::SeekableInputStream>>;

  // Enqueues projected streams into StripeStreams and loads them from disk.
  InputStreams loadStripe();

  // Collected data from one stripe for one request. sharedBody holds the
  // projected stream data and trailer (no header), shared across requests
  // for the same stripe via refcounted IOBuf clones:
  //   [stream_data...][encodingType][stream_sizes][trailer_size:u32]
  // finalizeResult() prepends a per-request header with the row range via
  // assembleChunk().
  struct ResultChunk {
    folly::IOBuf sharedBody;
    uint32_t numRows{0};
    RowRange rowRange;
  };

  // Stitches loaded raw stream bytes into the shared kTablet body+trailer
  // (stream data + trailer) without decode/re-encode. The returned chunk has
  // a default-constructed rowRange that buildStripeResult fills in per request.
  ResultChunk serializeStripe(uint32_t stripeIndex, InputStreams& inputStreams);

  // Queues per-request result chunks for the stripe. Each chunk captures a
  // cloneOne() of the shared body+trailer, the stripe row count, and the
  // per-request row range. The chunk slice IOBuf chain is finalized later by
  // finalizeResult() once we know whether the response carries a
  // resume key. Applies maxRowsPerRequest hard clipping and accumulates
  // numReadRows_. Returns false if a global limit (maxRows or maxBytes) was
  // hit after this stripe.
  bool buildStripeResult(
      std::span<const StripeRowRange> stripeRowRanges,
      ResultChunk&& stripeChunk);

  // Sets resume keys on all truncated requests that have data in the next
  // unprocessed stripe.
  void setResumeKeys(
      uint32_t stripeIndex,
      std::span<const StripeRowRange> stripeRowRanges,
      Result& result);

  // Materializes the per-slice header node for every queued chunk and
  // assembles the final chunk slice IOBuf chain (header -> body+trailer).
  // The resume key is folded into the header of the last slice of any
  // truncated response.
  void finalizeResult(Result& result);

  inline uint32_t stripeRowCount(uint32_t stripe) const {
    return static_cast<uint32_t>(readerBase_->tablet().stripeRowCount(stripe));
  }

  // Computes the stripe-relative row range by intersecting the file-level
  // rowRangeLimit with the stripe boundaries.
  RowRange stripeRowRange(uint32_t stripe, const RowRange& rowRangeLimit)
      const {
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

  void updateIoStats();

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

  Stats stats_;

  // Set during project() and cleared on return.
  const Request* request_{nullptr};
  const Options* options_{nullptr};
  // Number of requests in the current project() call. Cached from
  // request_->keyBounds.size() to avoid repeated pointer chases.
  uint32_t numRequests_{0};

  // Running total of rows read across all requests for maxRows enforcement.
  uint64_t numReadRows_{0};
  // Running total of serialized bytes across all stripes for maxBytes
  // enforcement.
  uint64_t numReadBytes_{0};
  // Accumulated rows per request for maxRowsPerRequest enforcement.
  std::vector<uint64_t> rowsPerRequest_;

  // Per-response, per-slice chunks awaiting finalization.
  std::vector<std::vector<ResultChunk>> resultChunks_;
  // Current stripe's row ranges after pruning saturated requests.
  std::vector<StripeRowRange> stripeRowRanges_;
  // CSR mapping from stripes to request row ranges. Populated by
  // lookupStripes(), read-only during stripe processing.
  StripeRangeMap stripeRangeMap_;
};

} // namespace facebook::nimble
