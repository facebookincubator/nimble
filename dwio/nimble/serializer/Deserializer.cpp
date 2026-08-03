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
#include "dwio/nimble/serializer/Deserializer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/serializer/DeserializerImpl.h"
#include "dwio/nimble/velox/Decoder.h"
#include "dwio/nimble/velox/SchemaReader.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "folly/Likely.h"
#include "folly/ScopeGuard.h"
#include "folly/container/F14Set.h"
#include "velox/buffer/Buffer.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/TypeWithId.h"

#include <algorithm>
#include <limits>
#include <optional>

namespace facebook::nimble {

namespace {

inline uint32_t getTypeStorageWidth(const Type& type) {
  switch (type.kind()) {
    case Kind::Scalar: {
      const auto scalarKind = type.asScalar().scalarDescriptor().scalarKind();
      switch (scalarKind) {
        case ScalarKind::Bool:
        case ScalarKind::Int8:
        case ScalarKind::UInt8:
          return 1;
        case ScalarKind::Int16:
        case ScalarKind::UInt16:
          return 2;
        case ScalarKind::Int32:
        case ScalarKind::Float:
        case ScalarKind::UInt32:
          return 4;
        case ScalarKind::Int64:
        case ScalarKind::UInt64:
        case ScalarKind::Double:
          return 8;
        case ScalarKind::String:
        case ScalarKind::Binary:
        case ScalarKind::Undefined:
          // Variable-length types return 0 to signal special handling path.
          return 0;
      }
      break;
    }
    case Kind::TimestampMicroNano:
      return 10;
    case Kind::Row:
    case Kind::FlatMap:
      return 1;
    case Kind::Array:
    case Kind::ArrayWithOffsets:
    case Kind::Map:
    case Kind::SlidingWindowMap:
      return 4;
  }
}

// Get the ScalarKind for a type based on its storage format.
inline ScalarKind getScalarKindForType(const Type& type) {
  if (type.isScalar()) {
    return type.asScalar().scalarDescriptor().scalarKind();
  } else if (type.isRow() || type.isFlatMap()) {
    // Row/FlatMap nulls streams are boolean.
    return ScalarKind::Bool;
  } else if (type.isArray() || type.isMap()) {
    // Array/Map lengths streams are uint32_t.
    return ScalarKind::UInt32;
  }
  NIMBLE_UNSUPPORTED("Unsupported type: {}", toString(type.kind()));
}

// Empty scattered reads still need to mark every output row as absent.
inline void markEmptyScatteredOutputNulls(
    const std::function<void*()>& getOutputNulls,
    const velox::bits::Bitmap* scatterOutputBitmap) {
  if (scatterOutputBitmap == nullptr) {
    return;
  }
  NIMBLE_CHECK_EQ(
      velox::bits::countBits(
          static_cast<const uint64_t*>(scatterOutputBitmap->bits()),
          0,
          scatterOutputBitmap->size()),
      0,
      "Empty scattered reads require an empty scatterOutputBitmap");
  NIMBLE_CHECK_NOT_NULL(
      getOutputNulls, "Scattered reads require output nulls callback");
  velox::bits::fillBits(
      static_cast<uint64_t*>(getOutputNulls()),
      0,
      scatterOutputBitmap->size(),
      velox::bits::kNull);
}

// Decoder for one logical stream assembled from per-batch segments.
class SegmentedStreamDecoder : public Decoder {
 public:
  SegmentedStreamDecoder(
      const Type* type,
      bool isInMapStream,
      size_t bufferPoolCapacity,
      velox::memory::MemoryPool* pool)
      : type_{type},
        pool_{pool},
        isInMapStream_{isInMapStream},
        scalarKind_{getScalarKindForType(*type)},
        typeStorageWidth_{getTypeStorageWidth(*type)},
        bufferPool_{
            bufferPoolCapacity > 0
                ? std::make_unique<velox::BufferPool>(bufferPoolCapacity)
                : nullptr} {
    NIMBLE_CHECK(
        !isInMapStream_ || typeStorageWidth_ == sizeof(bool),
        "FlatMap in-map stream should be bool");
  }

  uint32_t next(
      uint32_t count,
      void* output,
      std::vector<velox::BufferPtr>& stringBuffers,
      std::function<void*()> getOutputNulls = nullptr,
      const velox::bits::Bitmap* scatterOutputBitmap = nullptr) override {
    NIMBLE_CHECK(
        scatterOutputBitmap == nullptr || !isInMapStream(),
        "scatterOutputBitmap not used for FlatMap in-map streams");

    if (count == 0) {
      markEmptyScatteredOutputNulls(getOutputNulls, scatterOutputBitmap);
      return 0;
    }

    uint32_t nonNullCount;
    if (scatterOutputBitmap != nullptr) {
      nonNullCount = scatteredRead(
          count, output, getOutputNulls, scatterOutputBitmap, stringBuffers);
    } else if (isInMapStream()) {
      nonNullCount = inMapRead(count, output, stringBuffers);
    } else {
      nonNullCount = denseRead(count, output, getOutputNulls, stringBuffers);
    }
    currentRow_ += count;
    return nonNullCount;
  }

  void skip(uint32_t count) override {
    if (count == 0) {
      return;
    }

    // For non-in-map streams, an empty `streamSegments_` is only valid
    // for Row/FlatMap null streams that the writer omitted (all-non-null).
    // Nothing decoded → nothing to advance; just bump the cursor.
    if (FOLLY_UNLIKELY(!isInMapStream() && streamSegments_.empty())) {
      NIMBLE_CHECK(
          type_->isRow() || type_->isFlatMap(),
          "Empty streamSegments_ only valid for Row/FlatMap null streams");
      currentRow_ += count;
      return;
    }

    // `skipStringBuffers_` is a persistent per-decoder buffer given to
    // `ensureStreamData`. It must outlive the encoding created by skip,
    // which may hold `string_view`s into it and be re-used by a
    // subsequent `next()`.
    if (isInMapStream()) {
      skipInMap(count, skipStringBuffers_);
    } else {
      skipEncoded(count, skipStringBuffers_);
    }
    currentRow_ += count;
  }

  void reset() override {
    clear();
  }

  void clear() {
    streamSegments_.clear();
    presentInMapSegments_.clear();
    // Reset streamData_ (and hence its encoding_) BEFORE dropping
    // skipStringBuffers_ — the encoding may hold string_views into buffers
    // stored there, and those views must not outlive the buffers.
    streamData_.reset();
    skipStringBuffers_.clear();
    streamSegmentIndex_ = 0;
    presentSegmentIndex_ = 0;
    currentRow_ = 0;
  }

  const Encoding* encoding() const override {
    NIMBLE_UNREACHABLE("unexpected call");
  }

  static inline SegmentedStreamDecoder* as(Decoder* d) {
    return static_cast<SegmentedStreamDecoder*>(d);
  }

  // Queues a physical stream segment for one batch. `startRow` is the
  // top-level row where the batch begins in the concatenated run; it's
  // read back by FlatMap in-map reads to detect and fill gaps when
  // earlier batches omitted the stream. Other streams just concatenate
  // in payload order.
  //
  // The segment is stored as raw bytes. The encoding is constructed
  // lazily by `ensureStreamData` the first time this segment is decoded.
  void addBatch(
      uint32_t startRow,
      std::string_view data,
      SerializationVersion version) {
    NIMBLE_CHECK(!data.empty(), "Physical stream segment must be non-empty");
    streamSegments_.emplace_back(
        StreamSegment{.startRow = startRow, .data = data, .version = version});
  }

  // Records a batch range where this FlatMap key is present in every row.
  // Called for batches whose in-map stream was omitted on the wire
  // (writer's all-true optimization). Merges into the previous segment
  // when contiguous to keep `presentInMapSegments_` compact; `fillInMapGap`
  // clamps segments that extend past the current read's gap.
  void addPresentInMapBatch(uint32_t startRow, uint32_t rowCount) {
    NIMBLE_CHECK(isInMapStream(), "Expected FlatMap in-map stream");
    NIMBLE_CHECK_GT(
        rowCount, 0, "All-present in-map segment must be non-empty");
    const uint32_t endRow = startRow + rowCount;
    if (!presentInMapSegments_.empty() &&
        presentInMapSegments_.back().endRow == startRow) {
      presentInMapSegments_.back().endRow = endRow;
    } else {
      presentInMapSegments_.emplace_back(InMapSegment{startRow, endRow});
    }
  }

  // Records an all-present FlatMap key range for a null-barrier batch.
  // The read's effective end row (not any per-batch rowCount) determines
  // how many rows are present, so `endRow` stores the sentinel
  // `kPresentInMapEndRow`; the read side clamps as needed.
  void addPresentInMapBatch() {
    NIMBLE_CHECK(isInMapStream(), "Expected FlatMap in-map stream");
    NIMBLE_CHECK(
        streamSegments_.empty(),
        "All-present in-map segment must not be mixed with physical batches");
    presentInMapSegments_.emplace_back(
        InMapSegment{.startRow = 0, .endRow = kPresentInMapEndRow});
  }

 private:
  // Sentinel value stored in `InMapSegment::endRow` when the segment's
  // extent isn't known until the read side (i.e. the parameter-less
  // `addPresentInMapBatch()` used by null-barrier batches). The read side
  // clamps the segment to the current gap when it encounters this value.
  static constexpr uint32_t kPresentInMapEndRow =
      std::numeric_limits<uint32_t>::max();

  // Physical stream data for one batch.
  struct StreamSegment {
    // Top-level row where this batch starts. Only relevant for FlatMap in-map
    // streams to detect gaps when decoding across multiple chunks.
    uint32_t startRow;
    std::string_view data;
    SerializationVersion version;
  };

  // Row range where a FlatMap key is present in every requested row and the
  // in-map stream was omitted from the physical payload.
  struct InMapSegment {
    uint32_t startRow;
    uint32_t endRow;
  };

  // True for the FlatMap child-presence stream, not for the FlatMap
  // value/null stream itself.
  bool isInMapStream() const {
    return isInMapStream_;
  }

  // Returns the `StreamData` for the current segment, creating it lazily
  // on first access. `stringBuffers` is where any string content decoded
  // out of this segment will be pushed; caller retains ownership.
  //
  // On a cache hit (a prior `skip` already built the encoding using our
  // scratch vector), transfer any buffers the skip path pushed into
  // `skipStringBuffers_` into the caller's vector so the output takes
  // shared ownership before the end-of-run `clear()` drops our scratch,
  // then redirect the encoding's factory target for lazy string
  // encodings whose future page allocations must land in the caller's
  // vector, not ours.
  serde::StreamData& ensureStreamData(
      std::vector<velox::BufferPtr>& stringBuffers) {
    if (streamData_.has_value()) {
      if (!skipStringBuffers_.empty() &&
          &stringBuffers != &skipStringBuffers_) {
        stringBuffers.insert(
            stringBuffers.end(),
            std::make_move_iterator(skipStringBuffers_.begin()),
            std::make_move_iterator(skipStringBuffers_.end()));
        skipStringBuffers_.clear();
      }
      streamData_->setStringBuffers(&stringBuffers);
      return *streamData_;
    }

    NIMBLE_CHECK_LT(streamSegmentIndex_, streamSegments_.size());
    const auto& segment = streamSegments_[streamSegmentIndex_];
    streamData_.emplace(
        scalarKind_,
        segment.data,
        stringBuffers,
        pool_,
        serde::StreamData::Options{
            .version = segment.version,
            .bufferPool = bufferPool_.get(),
            .decompressionBuffer = &decompressionBuffer_});
    return *streamData_;
  }

  // Advances to the next segment. ensureStreamData() will create StreamData for
  // the new segment before decoding it.
  void advanceSegment() {
    streamData_.reset();
    ++streamSegmentIndex_;
  }

  // Fills the in-map output for rows `[rowOffset, gapEndRow)` where
  // `gapEndRow = min(rowOffset + rowCount, next stream segment's startRow)`.
  // Defaults every row to `kInMapAbsent`, then overlays with `kInMapPresent`
  // for every presence segment in `presentInMapSegments_` that overlaps.
  // Returns the number of rows filled.
  //
  // Parameters:
  //   * `rowOffset`   — absolute row in the concatenated batch-run row
  //                     domain (matches `StreamSegment::startRow` and
  //                     `InMapSegment::startRow`). Drives all range math.
  //   * `rowCount`    — upper bound on rows to fill; the actual count is
  //                     capped at the next stream segment's start.
  //   * `outputOffset` — where in `output` to start writing, in element
  //                      slots (multiplied by `typeStorageWidth_`).
  //                      Independent of `rowOffset` because `skip()` moves
  //                      the row cursor without moving the output cursor.
  //   * `output`      — destination buffer.
  //
  // Presence-segment handling:
  //   * A segment fully inside the gap is written and consumed
  //     (`++presentSegmentIndex_`).
  //   * A segment that extends past `gapEndRow` is partially written
  //     (clamped to the gap) and its `startRow` is advanced to `gapEndRow`
  //     so the next call resumes where this one stopped.
  //   * A segment starting before `rowOffset` (a prior skip landed inside
  //     it) is clamped on the low end via `max(segment.startRow, rowOffset)`.
  uint32_t fillInMapGap(
      uint32_t rowOffset,
      uint32_t rowCount,
      uint32_t outputOffset,
      void* output) {
    NIMBLE_CHECK(isInMapStream(), "Expected FlatMap in-map stream");
    const auto requestEndRow = rowOffset + rowCount;
    const auto gapEndRow = streamSegmentIndex_ < streamSegments_.size()
        ? std::min(requestEndRow, streamSegments_[streamSegmentIndex_].startRow)
        : requestEndRow;
    NIMBLE_CHECK_GT(
        gapEndRow,
        rowOffset,
        "FlatMap in-map gap fill requires a non-empty output range");
    const auto numGapRows = gapEndRow - rowOffset;
    auto* const outputBools =
        static_cast<char*>(output) + outputOffset * typeStorageWidth_;
    constexpr char kInMapAbsent = 0;
    constexpr char kInMapPresent = 1;
    std::memset(outputBools, kInMapAbsent, numGapRows * typeStorageWidth_);
    while (presentSegmentIndex_ < presentInMapSegments_.size()) {
      auto& segment = presentInMapSegments_[presentSegmentIndex_];
      if (segment.startRow >= gapEndRow) {
        break;
      }
      // Clamp both ends. A segment can start before `rowOffset` if a
      // prior skip landed inside it, and can end past `gapEndRow` when
      // the current read only covers a prefix (or when the segment is
      // the null-barrier sentinel `kPresentInMapEndRow`).
      const auto presentStartRow = std::max(segment.startRow, rowOffset);
      const auto presentEndRow = std::min(segment.endRow, gapEndRow);
      std::memset(
          outputBools + (presentStartRow - rowOffset) * typeStorageWidth_,
          kInMapPresent,
          (presentEndRow - presentStartRow) * typeStorageWidth_);
      if (segment.endRow > gapEndRow) {
        // Not fully consumed — advance its start so the next call resumes
        // where this one left off. `kPresentInMapEndRow` sentinel stays
        // in `endRow`.
        segment.startRow = gapEndRow;
        break;
      }
      ++presentSegmentIndex_;
    }
    return numGapRows;
  }

  serde::StreamData::DecodeResult readLegacyStreamSegment(
      serde::StreamData& streamData,
      void* output,
      uint32_t offset,
      uint32_t count) {
    const auto width = typeStorageWidth_;
    if (width > 0) {
      return streamData.decodeLegacy(output, offset, count, width);
    }

    auto* dest = static_cast<std::string_view*>(output) + offset;
    return streamData.decodeStrings(count, dest);
  }

  serde::StreamData::DecodeResult readSegment(
      void* output,
      uint32_t offset,
      uint32_t count,
      const std::function<void*()>& getOutputNulls,
      const velox::bits::Bitmap* scatterOutputBitmap,
      std::vector<velox::BufferPtr>& stringBuffers) {
    NIMBLE_CHECK(
        scatterOutputBitmap == nullptr || !isInMapStream(),
        "scatterOutputBitmap not used for FlatMap in-map streams");

    NIMBLE_CHECK_LT(streamSegmentIndex_, streamSegments_.size());
    auto& streamData = ensureStreamData(stringBuffers);
    if (!streamData.hasEncoding()) {
      NIMBLE_CHECK_NULL(
          scatterOutputBitmap,
          "scatterOutputBitmap is only used for encoded streams");
      return readLegacyStreamSegment(streamData, output, offset, count);
    }

    const auto width = typeStorageWidth_;
    return streamData.decode(
        output, offset, count, width, getOutputNulls, scatterOutputBitmap);
  }

  // Skips up to `numRows` rows from the segment at `streamSegmentIndex_`.
  // Advances `streamSegmentIndex_` past the segment when its remaining
  // rows fit inside `numRows`; otherwise leaves the cursor mid-segment.
  // Returns the number of rows actually skipped (never more than
  // `numRows`, may be less if the segment has fewer remaining).
  uint32_t skipSegment(
      uint32_t numRows,
      std::vector<velox::BufferPtr>& stringBuffers) {
    NIMBLE_CHECK_LT(
        streamSegmentIndex_,
        streamSegments_.size(),
        "SegmentedStreamDecoder::skip past end of decoder queue");
    auto& streamData = ensureStreamData(stringBuffers);
    NIMBLE_CHECK(
        streamData.hasEncoding(),
        "SegmentedStreamDecoder::skip requires encoded segments");
    const auto remainingRows = streamData.remainingRows();
    NIMBLE_CHECK_GT(remainingRows, 0, "Current segment has no rows");
    const auto toSkip = std::min<uint32_t>(remainingRows, numRows);
    streamData.skip(toSkip);
    if (toSkip == remainingRows) {
      advanceSegment();
    }
    return toSkip;
  }

  // Skips `numRows` rows for non-in-map columns (dense scalars, FlatMap
  // value / scattered columns). Walks `streamSegments_` from the current
  // cursor, consuming each segment fully until `numRows` are covered.
  void skipEncoded(
      uint32_t numRows,
      std::vector<velox::BufferPtr>& stringBuffers) {
    uint32_t skippedRows = 0;
    while (skippedRows < numRows) {
      skippedRows += skipSegment(numRows - skippedRows, stringBuffers);
    }
  }

  // Skips `numRows` rows for FlatMap in-map streams. Alternates between
  // presence-gap regions (advance `presentSegmentIndex_` via
  // `advanceInMapPresentSegmentIndex`) and encoded segments (advance via
  // `skipSegment`), mirroring the read-side control flow in
  // `inMapRead` but writing nothing.
  void skipInMap(
      uint32_t numRows,
      std::vector<velox::BufferPtr>& stringBuffers) {
    const uint32_t targetRow = currentRow_ + numRows;
    uint32_t skippedRows = 0;
    while (skippedRows < numRows) {
      const uint32_t currentRow = currentRow_ + skippedRows;
      if (streamSegmentIndex_ >= streamSegments_.size()) {
        // No more encoded segments — everything left is presence-gap.
        advanceInMapPresentSegmentIndex(targetRow);
        break;
      }
      const auto nextStreamStartRow =
          streamSegments_[streamSegmentIndex_].startRow;
      if (nextStreamStartRow > currentRow) {
        // Presence gap up to the next encoded segment (or the skip
        // target, whichever comes first).
        const uint32_t gapEndRow = std::min(targetRow, nextStreamStartRow);
        advanceInMapPresentSegmentIndex(gapEndRow);
        skippedRows += gapEndRow - currentRow;
        continue;
      }
      skippedRows += skipSegment(numRows - skippedRows, stringBuffers);
    }
  }

  // Advances `presentSegmentIndex_` past every presence segment fully
  // contained in `[..., targetRow]` (i.e. `segment.endRow <= targetRow`).
  // A segment that extends past `targetRow` stays as the current segment;
  // partial consumption is tracked implicitly (the read side will clamp
  // it when needed).
  void advanceInMapPresentSegmentIndex(uint32_t targetRow) {
    NIMBLE_CHECK(isInMapStream(), "Expected FlatMap in-map stream");
    while (presentSegmentIndex_ < presentInMapSegments_.size()) {
      const auto& segment = presentInMapSegments_[presentSegmentIndex_];
      if (segment.endRow > targetRow) {
        break;
      }
      ++presentSegmentIndex_;
    }
  }

  // Reads `count` non-in-map values into dense output row positions.
  uint32_t denseRead(
      uint32_t count,
      void* output,
      const std::function<void*()>& getOutputNulls,
      std::vector<velox::BufferPtr>& stringBuffers) {
    const auto width = typeStorageWidth_;
    if (FOLLY_UNLIKELY(streamSegments_.empty())) {
      NIMBLE_CHECK(
          type_->isRow() || type_->isFlatMap(),
          "streamSegments_ is empty for unexpected stream type={}",
          type_->kind());
      NIMBLE_CHECK_EQ(
          width, sizeof(bool), "Row/FlatMap null stream should be bool");
      // All-non-null Row/FlatMap null streams are omitted on the wire and
      // reconstructed as all-true here (no null rows).
      std::fill_n(static_cast<bool*>(output), count, true);
      return count;
    }

    uint32_t rowsRead{0};
    uint32_t nonNullCount{0};
    bool nullsInitialized{false};
    while (rowsRead < count) {
      NIMBLE_CHECK_LT(
          streamSegmentIndex_,
          streamSegments_.size(),
          "Non-in-map stream ended before requested rows were decoded");
      const uint32_t rowsToRead = count - rowsRead;
      const auto result = readSegment(
          output,
          rowsRead,
          rowsToRead,
          getOutputNulls,
          /*scatterOutputBitmap=*/nullptr,
          stringBuffers);
      NIMBLE_CHECK_GT(
          result.numOutputRows, 0, "Current segment returned no rows");
      NIMBLE_CHECK_LE(
          result.nonNullOutputRows,
          result.numOutputRows,
          "non-null row count exceeds row count");
      const bool segmentAllNonNull =
          result.nonNullOutputRows == result.numOutputRows;
      const bool needsNullHandling = !segmentAllNonNull || nullsInitialized;
      if (FOLLY_UNLIKELY(needsNullHandling)) {
        NIMBLE_CHECK_NOT_NULL(
            getOutputNulls, "nullable segment requires output nulls callback");
        if (!segmentAllNonNull && !nullsInitialized) {
          velox::bits::fillBits(
              static_cast<uint64_t*>(getOutputNulls()),
              0,
              rowsRead,
              velox::bits::kNotNull);
          nullsInitialized = true;
        } else if (segmentAllNonNull && nullsInitialized) {
          // Nullable decoding does not touch the null bitmap for all-non-null
          // segments, so keep the stitched output range explicitly non-null.
          velox::bits::fillBits(
              static_cast<uint64_t*>(getOutputNulls()),
              rowsRead,
              rowsRead + result.numOutputRows,
              velox::bits::kNotNull);
        }
      }
      rowsRead += result.numOutputRows;
      nonNullCount += result.nonNullOutputRows;
      if (FOLLY_LIKELY(result.segmentExhausted)) {
        advanceSegment();
      }
    }

    NIMBLE_CHECK_EQ(
        rowsRead,
        count,
        "Incomplete read: typeKind={} inMap={} segments={} streamSegmentIndex={}",
        toString(type_->kind()),
        isInMapStream_,
        streamSegments_.size(),
        streamSegmentIndex_);
    return nonNullCount;
  }

  // FlatMap in-map streams still materialize dense bool output. Their physical
  // stream can be omitted for all-absent/all-present batch ranges, so this path
  // reconstructs those gaps while normal dense reads avoid the in-map branches.
  uint32_t inMapRead(
      uint32_t count,
      void* output,
      std::vector<velox::BufferPtr>& stringBuffers) {
    uint32_t rowsRead{0};
    uint32_t nonNullCount{0};
    while (rowsRead < count) {
      const uint32_t currentRow = currentRow_ + rowsRead;
      if (streamSegmentIndex_ >= streamSegments_.size()) {
        const auto rows =
            fillInMapGap(currentRow, count - rowsRead, rowsRead, output);
        rowsRead += rows;
        nonNullCount += rows;
        break;
      }

      const auto nextStreamStartRow =
          streamSegments_[streamSegmentIndex_].startRow;
      if (nextStreamStartRow > currentRow) {
        const auto rows =
            fillInMapGap(currentRow, count - rowsRead, rowsRead, output);
        NIMBLE_CHECK_EQ(
            rows,
            std::min(count + currentRow_, nextStreamStartRow) - currentRow,
            "FlatMap in-map gap fill returned unexpected row count");
        rowsRead += rows;
        nonNullCount += rows;
        continue;
      }

      const uint32_t rowsToRead = count - rowsRead;
      const auto result = readSegment(
          output,
          rowsRead,
          rowsToRead,
          /*getOutputNulls=*/nullptr,
          /*scatterOutputBitmap=*/nullptr,
          stringBuffers);
      NIMBLE_CHECK_GT(
          result.numOutputRows, 0, "Current in-map segment returned no rows");
      NIMBLE_CHECK_EQ(
          result.nonNullOutputRows,
          result.numOutputRows,
          "FlatMap in-map stream must not contain nulls");
      rowsRead += result.numOutputRows;
      nonNullCount += result.numOutputRows;
      if (FOLLY_LIKELY(result.segmentExhausted)) {
        advanceSegment();
      }
    }

    NIMBLE_CHECK_EQ(
        rowsRead,
        count,
        "Incomplete in-map read: segments={} streamSegmentIndex={}",
        streamSegments_.size(),
        streamSegmentIndex_);
    return nonNullCount;
  }

  // Decode directly to positions where scatterOutputBitmap bits are set. Used
  // for FlatMap value columns where some rows don't have certain keys
  // (inMap=false).
  uint32_t scatteredRead(
      uint32_t count,
      void* output,
      const std::function<void*()>& getOutputNulls,
      const velox::bits::Bitmap* scatterOutputBitmap,
      std::vector<velox::BufferPtr>& stringBuffers) {
    NIMBLE_CHECK(
        !type_->isFlatMap(),
        "scatterOutputBitmap not used for FlatMap null streams");

    const auto outputSize = scatterOutputBitmap->size();
    // Fast path: if bitmap is dense (all bits set), read directly to output.
    // This avoids temp buffer allocation and scatter overhead.
    if (count == outputSize) {
      return denseRead(count, output, getOutputNulls, stringBuffers);
    }

    uint32_t rowsRead = 0;
    uint32_t nonNullCount = 0;

    NIMBLE_CHECK_NOT_NULL(
        getOutputNulls,
        "Output nulls callback is required for scattered reads");
    uint32_t offset = 0;
    bool hasNulls = false;

    while (rowsRead < count && streamSegmentIndex_ < streamSegments_.size()) {
      auto& streamData = ensureStreamData(stringBuffers);
      NIMBLE_CHECK(
          streamData.hasEncoding(),
          "Scattered reads require encoded stream data");
      const auto requestRows = count - rowsRead;
      const auto rowsToRead = std::min(requestRows, streamData.remainingRows());
      NIMBLE_CHECK_GT(rowsToRead, 0, "Current scattered segment has no rows");

      const auto endOffset = velox::bits::findSetBit(
          static_cast<const char*>(scatterOutputBitmap->bits()),
          offset,
          outputSize,
          rowsToRead + 1);
      velox::bits::Bitmap segmentScatterBitmap{
          scatterOutputBitmap->bits(), endOffset};
      const auto result = readSegment(
          output,
          offset,
          rowsToRead,
          getOutputNulls,
          &segmentScatterBitmap,
          stringBuffers);
      NIMBLE_CHECK_EQ(
          result.numOutputRows,
          rowsToRead,
          "Incomplete scattered segment read");

      const auto segmentRows = endOffset - offset;
      const bool segmentHasNulls = result.nonNullOutputRows != segmentRows;
      if (segmentHasNulls && !hasNulls) {
        velox::bits::BitmapBuilder nullBits{getOutputNulls(), offset};
        nullBits.set(0, offset);
      }
      if (hasNulls && !segmentHasNulls) {
        velox::bits::BitmapBuilder nullBits{getOutputNulls(), endOffset};
        nullBits.set(offset, endOffset);
      }
      hasNulls |= segmentHasNulls;

      rowsRead += result.numOutputRows;
      nonNullCount += result.nonNullOutputRows;
      offset = endOffset;
      if (FOLLY_LIKELY(result.segmentExhausted)) {
        advanceSegment();
      }
    }

    NIMBLE_CHECK_EQ(
        rowsRead,
        count,
        "Incomplete scattered read: typeKind={} segments={} streamSegmentIndex={}",
        toString(type_->kind()),
        streamSegments_.size(),
        streamSegmentIndex_);
    return nonNullCount;
  }

  // --- Const members (set at construction, never modified) ---
  const Type* const type_;
  velox::memory::MemoryPool* const pool_;
  // True when this decoder reads a FlatMap child in-map presence stream rather
  // than the FlatMap value/null stream.
  const bool isInMapStream_;
  // Cached from type at construction to avoid per-call dispatch.
  const ScalarKind scalarKind_;
  const uint32_t typeStorageWidth_;
  // Pool for encoding scratch buffers (e.g. MainlyConstant's isCommon and
  // otherValues buffers). Persists across reset()/addBatch() cycles so buffers
  // are reused instead of being allocated/freed through MemoryPool each time.
  // Null when buffer pooling is disabled via DeserializerOptions.
  const std::unique_ptr<velox::BufferPool> bufferPool_;
  // Decompression buffer reused across StreamData lifetimes. Persists across
  // reset()/addBatch() cycles so the buffer capacity is reused instead of
  // freed and re-allocated on each segment transition.
  velox::BufferPtr decompressionBuffer_;

  // --- Stream decode state (cleared by reset()) ---
  size_t streamSegmentIndex_{0};
  std::vector<StreamSegment> streamSegments_;

  // --- FlatMap in-map state (cleared by reset()) ---
  size_t presentSegmentIndex_{0};
  std::vector<InMapSegment> presentInMapSegments_;

  // Lazily-created StreamData wrapper reused across physical segments for this
  // stream decoder.
  std::optional<serde::StreamData> streamData_;

  // Row cursor in this decoder's row domain (matches
  // `StreamSegment::startRow` for in-map streams; encoded-row domain
  // otherwise). Bumped by every `next()` and `skip()`. Read by
  // `fillInMapGap` and `skipInMap` for presence-position math. Reset to
  // 0 in `clear()`.
  uint32_t currentRow_{0};

  // Backing storage for `ensureStreamData` when called from `skip*`.
  // Some string encodings allocate their content buffers into this vector
  // at construction time and keep `string_view`s into them; the vector
  // must outlive the encoding that references it, so it lives with the
  // decoder and is cleared inside `clear()` AFTER `streamData_` (and
  // hence the encoding) is destroyed.
  std::vector<velox::BufferPtr> skipStringBuffers_;
};

const StreamDescriptor& getMainDescriptor(const Type& type) {
  switch (type.kind()) {
    case Kind::Scalar:
      return type.asScalar().scalarDescriptor();
    case Kind::TimestampMicroNano:
      return type.asTimestampMicroNano().microsDescriptor();
    case Kind::Array:
      return type.asArray().lengthsDescriptor();
    case Kind::Map:
      return type.asMap().lengthsDescriptor();
    case Kind::Row:
      return type.asRow().nullsDescriptor();
    case Kind::FlatMap:
      return type.asFlatMap().nullsDescriptor();
    default:
      // ArrayWithOffsets and SlidingWindowMap are not supported.
      NIMBLE_UNSUPPORTED(
          "Schema type {} is not supported.", toString(type.kind()));
  }
}

bool checkColumnProjectionSubfield(
    const RowType& row,
    const Deserializer::Subfield& subfield) {
  const auto& path = subfield.path();
  NIMBLE_USER_CHECK(
      subfield.valid(),
      "Column projection deserialize requires a named subfield path: {}",
      subfield);
  auto childIndex = row.findChild(subfield.baseName());
  NIMBLE_USER_CHECK(
      childIndex.has_value(),
      "Column projection subfield does not exist in schema: {}",
      subfield);
  const auto* nestedType = row.childAt(childIndex.value()).get();
  for (size_t i = 1; i < path.size(); ++i) {
    if (nestedType->isFlatMap()) {
      NIMBLE_USER_CHECK(
          path[i]->is(velox::common::SubfieldKind::kStringSubscript) ||
              path[i]->is(velox::common::SubfieldKind::kLongSubscript),
          "FlatMap projection requires a string or integer key: {}",
          subfield);
      NIMBLE_USER_CHECK_EQ(
          i + 1,
          path.size(),
          "Nested projection inside a FlatMap value is not supported: {}",
          subfield);
      return true;
    }
    NIMBLE_USER_CHECK(
        path[i]->is(velox::common::SubfieldKind::kNestedField),
        "Column projection deserialize only supports named fields. Path: {}, element: {}",
        subfield,
        path[i]->toString());
    NIMBLE_USER_CHECK(
        nestedType->isRow(),
        "Column projection deserialize only supports nested Row fields. Path: {}, type: {}",
        subfield,
        nestedType->kind());
    const auto& nestedName =
        path[i]->asChecked<velox::common::Subfield::NestedField>()->name();
    childIndex = nestedType->asRow().findChild(nestedName);
    NIMBLE_USER_CHECK(
        childIndex.has_value(),
        "Column projection subfield does not exist in schema: {}",
        subfield);
    nestedType = nestedType->asRow().childAt(childIndex.value()).get();
  }
  return false;
}

// One reader operation. Executed as `reader_->skip(numRows)` when
// `skip == true`, or `reader_->next(numRows, ...)` when `skip == false`.
struct DecodeOp {
  bool skip;
  uint32_t numRows;
};

// Turns per-batch rowRanges (in run-local coordinates) into the minimal
// sequence of skip/read ops that visits each range exactly once.
// Adjacent ranges with no gap fold into a single read op. Empty ranges
// are dropped. Returns `[{read, 0}]` when the result would otherwise be
// empty, so callers always emit at least one `reader_->next` and produce
// a non-null output vector.
std::vector<DecodeOp> buildDecodeOps(
    const std::vector<nimble::RowRange>& ranges) {
  std::vector<DecodeOp> ops;
  // Worst case is skip+read per range (all disjoint, non-contiguous).
  ops.reserve(2 * ranges.size());
  uint32_t cursor{0};
  for (const auto& range : ranges) {
    // Empty range = "no rows from this batch". Common when the caller
    // uses the rowRanges overload to skip whole batches, or when a
    // batch's rowCount is 0. Nothing to emit; move on.
    if (range.numRows() == 0) {
      continue;
    }
    if (range.startRow > cursor) {
      ops.push_back({/*skip=*/true, range.startRow - cursor});
    }
    // Fold into the preceding read op when this range is contiguous with
    // it; otherwise start a fresh read (either `ops` is empty or the last
    // op was the skip we just pushed).
    if (!ops.empty() && !ops.back().skip) {
      ops.back().numRows += range.numRows();
    } else {
      ops.push_back({/*skip=*/false, range.numRows()});
    }
    cursor = range.endRow;
  }
  // Every input range was empty (or `ranges` itself was). Emit one
  // zero-length read so the caller still runs a `reader_->next` and
  // produces a non-null empty output vector (see ProjectorFormatTest
  // .emptyInput and equivalents).
  if (ops.empty()) {
    ops.push_back({/*skip=*/false, 0});
  }
  return ops;
}

} // namespace

Deserializer::ProjectedField* Deserializer::ProjectedField::ensureChild(
    const std::string& name) {
  auto& selectedChild = children[name];
  if (selectedChild == nullptr) {
    selectedChild = std::make_unique<ProjectedField>();
  }
  return selectedChild.get();
}

velox::TypePtr Deserializer::buildProjectedType(
    const velox::TypePtr& source,
    const ProjectedField& selected,
    Deserializer::OutputProjection& projection) {
  if (selected.selectWholeField) {
    return source;
  }
  const auto& sourceRow = source->asRow();
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(selected.children.size());
  types.reserve(selected.children.size());
  std::vector<std::string> selectedNames;
  selectedNames.reserve(selected.children.size());
  for (const auto& [name, _] : selected.children) {
    selectedNames.emplace_back(name);
  }
  std::sort(selectedNames.begin(), selectedNames.end());
  for (const auto& name : selectedNames) {
    const auto sourceChannel = sourceRow.getChildIdx(name);
    const auto& selectedChild = *selected.children.at(name);
    projection.identityProjections.emplace_back(sourceChannel, names.size());
    names.emplace_back(name);
    auto& childProjection = projection.childProjections.emplace_back();
    if (selectedChild.selectWholeField) {
      types.emplace_back(sourceRow.childAt(sourceChannel));
    } else {
      types.emplace_back(buildProjectedType(
          sourceRow.childAt(sourceChannel), selectedChild, childProjection));
    }
  }
  return velox::ROW(std::move(names), std::move(types));
}

velox::RowTypePtr Deserializer::buildProjectedType(
    const velox::RowTypePtr& sourceType,
    const std::vector<Deserializer::Subfield>& selectedSubfields,
    Deserializer::OutputProjection& outputProjection) {
  ProjectedField root;
  for (const auto& subfield : selectedSubfields) {
    auto* selected = root.ensureChild(subfield.baseName());
    const auto& path = subfield.path();
    for (size_t i = 1; i < path.size(); ++i) {
      if (path[i]->is(velox::common::SubfieldKind::kStringSubscript) ||
          path[i]->is(velox::common::SubfieldKind::kLongSubscript)) {
        NIMBLE_CHECK_EQ(
            i,
            1,
            "FlatMap key projection is only supported for top-level fields: {}",
            subfield);
        selected->selectWholeField = true;
        break;
      }
      const auto& name =
          path[i]->asChecked<velox::common::Subfield::NestedField>()->name();
      selected = selected->ensureChild(name);
    }
    selected->selectWholeField = true;
  }

  return velox::checkedPointerCast<const velox::RowType>(
      buildProjectedType(sourceType, root, outputProjection));
}

FieldReaderParams Deserializer::createFieldReaderParams() const {
  FieldReaderParams params;
  params.flatMapFeatureSelector = flatMapFeatureSelector_;
  params.decodeExecutor = options_.decodeExecutor;
  params.maxDecodeParallelism = options_.maxDecodeParallelism;
  params.minStreamsPerDecodeUnit = options_.minStreamsPerDecodeUnit;
  if (options_.outputType == nullptr) {
    return params;
  }

  NIMBLE_CHECK(
      schema_->isRow(),
      "outputType requires Row schema root, got {}",
      toString(schema_->kind()));

  const auto& rootRow = schema_->asRow();
  NIMBLE_CHECK_EQ(
      rootRow.childrenCount(),
      options_.outputType->size(),
      "Output type field count must match schema field count");

  for (size_t i = 0; i < rootRow.childrenCount(); ++i) {
    if (!rootRow.childAt(i)->isFlatMap()) {
      continue;
    }
    const auto& outputFieldType = options_.outputType->childAt(i);
    if (outputFieldType->kind() != velox::TypeKind::ROW) {
      continue;
    }

    const auto& columnName = rootRow.nameAt(i);
    params.readFlatMapFieldAsStruct.insert(columnName);

    const auto& rowType = outputFieldType->asRow();
    std::vector<std::string> features;
    features.reserve(rowType.size());
    for (size_t j = 0; j < rowType.size(); ++j) {
      features.push_back(rowType.nameOf(j));
    }
    params.flatMapFeatureSelector[columnName] = FeatureSelection{
        .features = std::move(features),
        .mode = SelectionMode::Include,
    };
  }
  return params;
}

Deserializer::Deserializer(
    std::shared_ptr<const Type> schema,
    velox::memory::MemoryPool* pool)
    : Deserializer{std::move(schema), pool, {}} {}

Deserializer::Deserializer(
    std::shared_ptr<const Type> schema,
    velox::memory::MemoryPool* pool,
    DeserializerOptions options)
    : Deserializer{
          std::move(schema),
          /*selectedSubfields=*/{},
          pool,
          std::move(options)} {}

Deserializer::Deserializer(
    std::shared_ptr<const Type> schema,
    const std::vector<Deserializer::Subfield>& selectedSubfields,
    velox::memory::MemoryPool* pool,
    DeserializerOptions options)
    : schema_{std::move(schema)},
      pool_{pool},
      options_{std::move(options)},
      hasColumnProjection_{!selectedSubfields.empty()} {
  auto veloxType = convertToVeloxType(*schema_);
  if (!hasColumnProjection_) {
    initialize(
        velox::dwio::common::TypeWithId::create(veloxType),
        [](uint32_t) { return true; });
    return;
  }

  initializeColumnProjection(veloxType, selectedSubfields);
}

void Deserializer::initializeColumnProjection(
    const velox::TypePtr& veloxType,
    const std::vector<Deserializer::Subfield>& selectedSubfields) {
  NIMBLE_CHECK(hasColumnProjection_, "Column projection is not enabled");
  const auto rowType =
      velox::checkedPointerCast<const velox::RowType>(veloxType);
  std::vector<std::string> projectedColumnPaths;
  folly::F14FastSet<std::string> selectedSubfieldSet;
  folly::F14FastSet<std::string> projectedColumnPathSet;
  folly::F14FastMap<std::string, folly::F14FastSet<std::string>>
      flatMapFeatureSets;
  projectedColumnPaths.reserve(selectedSubfields.size());
  selectedSubfieldSet.reserve(selectedSubfields.size());
  projectedColumnPathSet.reserve(selectedSubfields.size());
  flatMapFeatureSets.reserve(selectedSubfields.size());
  for (const auto& subfield : selectedSubfields) {
    const bool selectsFlatMapKey =
        checkColumnProjectionSubfield(schema_->asRow(), subfield);
    const auto& path = subfield.path();
    const auto selectedPath = subfield.toString();
    NIMBLE_USER_CHECK(
        selectedSubfieldSet.insert(selectedPath).second,
        "Duplicate column projection subfield: {}",
        subfield);
    std::string columnPath;
    if (selectsFlatMapKey) {
      columnPath = subfield.baseName();
      auto feature = path[1]->is(velox::common::SubfieldKind::kStringSubscript)
          ? path[1]
                ->asChecked<velox::common::Subfield::StringSubscript>()
                ->index()
          : std::to_string(
                path[1]
                    ->asChecked<velox::common::Subfield::LongSubscript>()
                    ->index());
      const bool newFlatMapFeature =
          flatMapFeatureSets[columnPath].insert(feature).second;
      NIMBLE_USER_CHECK(
          newFlatMapFeature, "Duplicate FlatMap projection key: {}", subfield);
      flatMapFeatureSelector_[columnPath].features.emplace_back(
          std::move(feature));
    } else {
      columnPath = subfield.toString();
    }
    const bool newColumnPath = projectedColumnPathSet.insert(columnPath).second;
    if (newColumnPath) {
      projectedColumnPaths.emplace_back(columnPath);
    }
  }

  outputProjection_ = std::make_unique<OutputProjection>();
  outputType_ =
      buildProjectedType(rowType, selectedSubfields, *outputProjection_);
  auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(
      rowType, projectedColumnPaths);
  initialize(selector->getSchemaWithId(), [selector](auto nodeId) {
    return selector->shouldReadNode(nodeId);
  });
}

void Deserializer::initialize(
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& schemaWithId,
    const std::function<bool(uint32_t)>& isSelected) {
  const auto params = createFieldReaderParams();
  parser_ = std::make_unique<serde::StreamDataParser>(pool_, options_);

  std::vector<uint32_t> offsets;
  rootFactory_ = FieldReaderFactory::create(
      params, schema_, schemaWithId, offsets, isSelected, pool_);

  if (hasColumnProjection_) {
    const auto maxSelectedOffset =
        *std::max_element(offsets.begin(), offsets.end());
    selectedStreamOffsetFlags_.resize(maxSelectedOffset + 1, false);
    for (const auto offset : offsets) {
      selectedStreamOffsetFlags_[offset] = true;
    }
  }

  SchemaReader::traverseSchema(schema_, [this](auto depth, auto& type, auto&) {
    createDeserializersForType(type, depth);
  });

  reader_ = rootFactory_->createReader(deserializerMap_);

  // Build flat vector for O(1) stream offset lookup during deserialize().
  uint32_t maxOffset = 0;
  for (const auto& [offset, _] : deserializerMap_) {
    maxOffset = std::max(maxOffset, offset);
  }
  deserializers_.resize(maxOffset + 1, nullptr);
  for (auto& [offset, decoder] : deserializerMap_) {
    deserializers_[offset] = decoder.get();
  }

  // Pre-size stream presence-tracking state once. Both vectors are bounded
  // by maxOffset because every value-stream anchor offset is a Type main
  // descriptor offset already in deserializerMap_. Sizing here (rather than
  // grow-on-demand inside createDeserializersForType) avoids repeated
  // reallocations and lets the per-batch hot path skip a bounds check.
  if (!inMapChildTypes_.empty()) {
    streamPresentFlags_.resize(maxOffset + 1, false);
    valueOffsetToInMap_.resize(maxOffset + 1, kInvalidInMapOffset);
    // Populate the reverse-lookup table: for each top-level FlatMap child,
    // record its inMap stream offset at every one of its value-stream
    // anchors. The per-batch in-map inference reads this to map a present
    // value anchor back to its owning child without re-walking the schema.
    //
    // visitValueStreamLeaves visits ALL value-stream offsets in the child
    // subtree (Row recurses all children; FlatMap recurses all children).
    // Relies on RowFieldWriter writing every field over the same
    // OrderedRanges, so sibling Row children populate in lockstep — if any
    // sibling's value stream is present in a batch, all are. If a future
    // writer ever made Row children conditionally absent, the in-map
    // inference below would over-attribute presence to keys whose first
    // child was absent but a sibling was present.
    for (const auto& [inMapOffset, childType] : inMapChildTypes_) {
      visitValueStreamLeaves(
          *childType,
          [this, _inMapOffset = inMapOffset](offset_size valueOffset) {
            valueOffsetToInMap_[valueOffset] = _inMapOffset;
            return false;
          });
    }
  }
}

Deserializer::~Deserializer() = default;

void Deserializer::createDeserializersForType(
    const Type& type,
    uint32_t depth) {
  const auto streamOffset = getMainDescriptor(type).offset();
  if (shouldDecodeStream(streamOffset)) {
    deserializerMap_[streamOffset] = std::make_unique<SegmentedStreamDecoder>(
        &type,
        /*isInMapStream=*/false,
        options_.bufferPoolCapacity,
        pool_);
  }
  // FlatMap is only supported at depth 1 (top-level columns). Register each
  // child in-map stream so it is decoded like other physical streams.
  if (type.isFlatMap()) {
    NIMBLE_CHECK_EQ(
        depth, 1, "FlatMap is only supported as a top-level column (depth 1)");
    auto& flatMap = type.asFlatMap();
    for (size_t i = 0; i < flatMap.childrenCount(); ++i) {
      const auto inMapOffset = flatMap.inMapDescriptorAt(i).offset();
      if (!shouldDecodeStream(inMapOffset)) {
        continue;
      }
      deserializerMap_[inMapOffset] = std::make_unique<SegmentedStreamDecoder>(
          &type,
          /*isInMapStream=*/true,
          options_.bufferPoolCapacity,
          pool_);
      inMapChildTypes_[inMapOffset] = flatMap.childAt(i).get();
    }
  }
}

void Deserializer::deserialize(std::string_view data, velox::VectorPtr& output)
    const {
  deserialize(folly::Range<const std::string_view*>(&data, 1), output);
}

void Deserializer::deserialize(
    const std::vector<std::string_view>& data,
    velox::VectorPtr& output) const {
  deserialize(
      folly::Range<const std::string_view*>(data.data(), data.size()), output);
}

void Deserializer::appendToOutput(
    velox::VectorPtr&& decoded,
    velox::VectorPtr& output) const {
  if (FOLLY_LIKELY(output == nullptr)) {
    output = std::move(decoded);
    return;
  }
  output->append(decoded.get());
}

velox::VectorPtr Deserializer::projectOutput(velox::VectorPtr&& decoded) const {
  if (!hasColumnProjection_) {
    return std::move(decoded);
  }
  NIMBLE_CHECK_NOT_NULL(
      outputProjection_, "Output projection must be initialized");
  NIMBLE_CHECK_NOT_NULL(outputType_, "Output type must be initialized");

  return projectOutput(std::move(decoded), outputType_, *outputProjection_);
}

velox::VectorPtr Deserializer::projectOutput(
    velox::VectorPtr&& source,
    const velox::TypePtr& projectedType,
    const OutputProjection& projection) const {
  auto* decodedRow = source->asChecked<velox::RowVector>();
  const auto& projectedRowType = projectedType->asRow();
  NIMBLE_CHECK_EQ(
      projection.identityProjections.size(), projectedRowType.size());
  NIMBLE_CHECK_EQ(projection.childProjections.size(), projectedRowType.size());
  std::vector<velox::VectorPtr> children(projectedRowType.size());
  for (const auto& identity : projection.identityProjections) {
    const auto inputChannel = identity.inputChannel;
    const auto outputChannel = identity.outputChannel;
    const auto& childProjection = projection.childProjections[outputChannel];
    auto decodedChild = decodedRow->childAt(inputChannel);
    NIMBLE_CHECK_NOT_NULL(
        decodedChild,
        "Projected field was not decoded: {}",
        projectedRowType.nameOf(outputChannel));
    if (childProjection.identityProjections.empty()) {
      children[outputChannel] = std::move(decodedChild);
    } else {
      children[outputChannel] = projectOutput(
          std::move(decodedChild),
          projectedRowType.childAt(outputChannel),
          childProjection);
    }
  }
  return std::make_shared<velox::RowVector>(
      pool_,
      projectedType,
      decodedRow->nulls(),
      decodedRow->size(),
      std::move(children),
      std::nullopt);
}

void Deserializer::decodeRun(DecodeRun& run, velox::VectorPtr& output) const {
  if (FOLLY_UNLIKELY(run.batches == 0)) {
    return;
  }
  const auto ops = buildDecodeOps(runRanges_);
  for (const auto& op : ops) {
    if (op.skip) {
      reader_->skip(op.numRows);
      continue;
    }
    velox::VectorPtr decoded;
    reader_->next(op.numRows, decoded, nullptr);
    decoded = projectOutput(std::move(decoded));
    appendToOutput(std::move(decoded), output);
  }
  run = {};
  runRanges_.clear();
  reader_->reset();
}

void Deserializer::appendStreamSegments(
    uint32_t rowCount,
    uint32_t startRow,
    bool requiresBarrier) const {
  const auto maxStreamOffset = deserializers_.size() - 1;
  const auto version = parser_->version();
  const bool hasInMapChildren = !inMapChildTypes_.empty();
  if (hasInMapChildren) {
    std::fill(streamPresentFlags_.begin(), streamPresentFlags_.end(), false);
    presentStreamOffsets_.clear();
  }
  parser_->iterateStreams([&](uint32_t offset, std::string_view streamData) {
    if (FOLLY_UNLIKELY(offset > maxStreamOffset)) {
      return;
    }
    if (FOLLY_UNLIKELY(!shouldDecodeStream(offset))) {
      return;
    }
    if (hasInMapChildren) {
      if (!streamPresentFlags_[offset]) {
        streamPresentFlags_[offset] = true;
        presentStreamOffsets_.emplace_back(offset);
      }
    }
    auto* decoder = deserializers_[offset];
    NIMBLE_CHECK_NOT_NULL(decoder, "Missing decoder for stream");
    SegmentedStreamDecoder::as(decoder)->addBatch(
        startRow, streamData, version);
  });

  if (!hasInMapChildren) {
    return;
  }
  const auto presentStreamCount = presentStreamOffsets_.size();
  for (size_t i = 0; i < presentStreamCount; ++i) {
    const auto inMapOffset = valueOffsetToInMap_[presentStreamOffsets_[i]];
    if (inMapOffset == kInvalidInMapOffset ||
        streamPresentFlags_[inMapOffset]) {
      continue;
    }
    auto* decoder = deserializers_[inMapOffset];
    NIMBLE_CHECK_NOT_NULL(decoder, "Missing FlatMap in-map decoder");
    auto* segmentedDecoder = SegmentedStreamDecoder::as(decoder);
    if (requiresBarrier) {
      segmentedDecoder->addPresentInMapBatch();
    } else {
      segmentedDecoder->addPresentInMapBatch(startRow, rowCount);
    }
    streamPresentFlags_[inMapOffset] = true;
  }
}

void Deserializer::appendBatch(
    std::string_view batch,
    std::optional<nimble::RowRange> rowRange,
    DecodeRun& run,
    velox::VectorPtr& output) const {
  const auto rowCount = parser_->initialize(batch);
  const auto requiresBarrier = parser_->requiresNullBarrier();
  if (FOLLY_UNLIKELY(requiresBarrier)) {
    decodeRun(run, output);
  }

  // Base range from the batch: parser's rowRange (kTablet header) or
  // full-batch default if the header didn't encode one.
  nimble::RowRange range =
      parser_->rowRange().value_or(nimble::RowRange{0, rowCount});
  // Caller override wins if provided.
  if (rowRange.has_value()) {
    NIMBLE_USER_CHECK(
        !requiresBarrier,
        "override rowRange not supported for null-barrier batches");
    NIMBLE_USER_CHECK_LE(
        rowRange->startRow,
        rowRange->endRow,
        "override rowRange startRow must be <= endRow");
    NIMBLE_USER_CHECK_LE(
        rowRange->endRow,
        rowCount,
        "override rowRange endRow exceeds batch rowCount");
    range = *rowRange;
  }

  appendStreamSegments(rowCount, /*startRow=*/run.rows, requiresBarrier);
  runRanges_.push_back({run.rows + range.startRow, run.rows + range.endRow});
  run.rows += rowCount;
  ++run.batches;
  if (FOLLY_UNLIKELY(requiresBarrier)) {
    decodeRun(run, output);
    parser_->reset();
  }
}

void Deserializer::deserialize(
    folly::Range<const std::string_view*> data,
    velox::VectorPtr& output) const {
  deserializeImpl(data, {}, output);
}

void Deserializer::deserialize(
    const std::vector<std::string_view>& data,
    const std::vector<nimble::RowRange>& rowRanges,
    velox::VectorPtr& output) const {
  deserializeImpl(
      folly::Range<const std::string_view*>(data.data(), data.size()),
      folly::Range<const nimble::RowRange*>(rowRanges.data(), rowRanges.size()),
      output);
}

void Deserializer::deserializeImpl(
    folly::Range<const std::string_view*> data,
    folly::Range<const nimble::RowRange*> rowRanges,
    velox::VectorPtr& output) const {
  NIMBLE_USER_CHECK(!data.empty(), "Expected at least one serialized batch");
  if (!rowRanges.empty()) {
    NIMBLE_USER_CHECK_EQ(
        data.size(),
        rowRanges.size(),
        "data and rowRanges must have the same size");
  }
  // `runRanges_` must be empty across `deserialize*` calls — check on
  // entry, clear on exit (including exceptions) via SCOPE_EXIT.
  NIMBLE_CHECK(
      runRanges_.empty(), "runRanges_ must be empty on deserialize entry");
  SCOPE_EXIT {
    runRanges_.clear();
  };

  output = nullptr;
  DecodeRun run;
  runRanges_.reserve(data.size());
  for (size_t i = 0; i < data.size(); ++i) {
    std::optional<nimble::RowRange> rowRange;
    if (!rowRanges.empty()) {
      rowRange = rowRanges[i];
    }
    appendBatch(data[i], rowRange, run, output);
  }
  decodeRun(run, output);
  parser_->reset();
}

} // namespace facebook::nimble
