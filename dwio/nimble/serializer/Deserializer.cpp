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
#include "velox/buffer/Buffer.h"
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

    if (scatterOutputBitmap != nullptr) {
      return scatteredRead(
          count, output, getOutputNulls, scatterOutputBitmap, stringBuffers);
    }
    if (isInMapStream()) {
      return inMapRead(count, output, stringBuffers);
    }
    return denseRead(count, output, getOutputNulls, stringBuffers);
  }

  void skip(uint32_t /* count */) override {
    NIMBLE_UNREACHABLE("unexpected call");
  }

  void reset() override {
    clear();
  }

  void clear() {
    streamSegments_.clear();
    presentInMapSegments_.clear();
    streamData_.reset();
    streamSegmentIndex_ = 0;
    presentSegmentIndex_ = 0;
  }

  const Encoding* encoding() const override {
    NIMBLE_UNREACHABLE("unexpected call");
  }

  static inline SegmentedStreamDecoder* as(Decoder* d) {
    return static_cast<SegmentedStreamDecoder*>(d);
  }

  // Stores raw data without creating encoding objects. Encodings are created
  // lazily when the segment is first read, ensuring only one encoding tree
  // exists at a time. This avoids the memory locality and allocation overhead
  // of creating hundreds of encoding trees simultaneously in batch decode.
  // Appends a physical stream segment. Only FlatMap in-map streams use
  // `startRow` to reconstruct gaps for batches that omitted the stream.
  // Other streams concatenate by payload order.
  void addBatch(
      uint32_t startRow,
      std::string_view data,
      SerializationVersion version) {
    NIMBLE_CHECK(!data.empty(), "Physical stream segment must be non-empty");
    streamSegments_.emplace_back(
        StreamSegment{.startRow = startRow, .data = data, .version = version});
  }

  // Records a batch range where this FlatMap key is present in every row.
  void addPresentInMapBatch(uint32_t startRow, uint32_t rowCount) {
    NIMBLE_CHECK(isInMapStream(), "Expected FlatMap in-map stream");
    NIMBLE_CHECK_GT(
        rowCount, 0, "All-present in-map segment must be non-empty");
    const uint32_t endRow = startRow + rowCount;
    // Merge with the previous segment if contiguous.
    if (!presentInMapSegments_.empty() &&
        presentInMapSegments_.back().endRow == startRow) {
      presentInMapSegments_.back().endRow = endRow;
    } else {
      presentInMapSegments_.emplace_back(InMapSegment{startRow, endRow});
    }
  }

  // Records an all-present FlatMap key range for a null-barrier batch, where
  // the read request determines the effective end row.
  void addPresentInMapBatch() {
    NIMBLE_CHECK(isInMapStream(), "Expected FlatMap in-map stream");
    NIMBLE_CHECK(
        streamSegments_.empty(),
        "All-present in-map segment must not be mixed with physical batches");
    presentInMapSegments_.emplace_back(
        InMapSegment{.startRow = 0, .endRow = kPresentInMapEndRow});
  }

 private:
  // Sentinel end row for all-present FlatMap in-map ranges whose actual row
  // count comes from the current read request. Null-barrier batches use this
  // because FlatMap child reads are scoped by the parent Row/FlatMap null
  // stream, not the physical batch row count.
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

  // Lazily creates StreamData for the current physical segment. String buffers
  // from encoding are pushed directly into the caller's stringBuffers vector,
  // so no explicit release is needed.
  serde::StreamData& ensureStreamData(
      std::vector<velox::BufferPtr>& stringBuffers) {
    if (streamData_.has_value()) {
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

  uint32_t fillInMapGap(uint32_t rowOffset, uint32_t rowCount, void* output) {
    NIMBLE_CHECK(isInMapStream(), "Expected FlatMap in-map stream");
    // rowOffset and rowCount are in the same concatenated batch-run row domain
    // as StreamSegment::startRow.
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
        static_cast<char*>(output) + rowOffset * typeStorageWidth_;
    constexpr char kInMapAbsent = 0;
    constexpr char kInMapPresent = 1;
    std::memset(outputBools, kInMapAbsent, numGapRows * typeStorageWidth_);
    while (presentSegmentIndex_ < presentInMapSegments_.size()) {
      const auto& segment = presentInMapSegments_[presentSegmentIndex_];
      if (segment.startRow >= gapEndRow) {
        break;
      }
      NIMBLE_CHECK_GE(
          segment.startRow,
          rowOffset,
          "Present in-map segment starts before absent row range");
      const auto presentEndRow = std::min(segment.endRow, gapEndRow);
      std::memset(
          outputBools + (segment.startRow - rowOffset) * typeStorageWidth_,
          kInMapPresent,
          (presentEndRow - segment.startRow) * typeStorageWidth_);
      if (segment.endRow > gapEndRow) {
        // Synthetic all-present ranges can span past the current read request;
        // physical present ranges are expected to end within this gap.
        NIMBLE_CHECK_EQ(
            segment.endRow,
            kPresentInMapEndRow,
            "Only all-present in-map segment can extend beyond gap range");
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
      if (streamSegmentIndex_ >= streamSegments_.size()) {
        const auto rows = fillInMapGap(rowsRead, count - rowsRead, output);
        rowsRead += rows;
        nonNullCount += rows;
        break;
      }

      const auto nextStreamStartRow =
          streamSegments_[streamSegmentIndex_].startRow;
      if (nextStreamStartRow > rowsRead) {
        const auto rows = fillInMapGap(rowsRead, count - rowsRead, output);
        NIMBLE_CHECK_EQ(
            rows,
            std::min(count, nextStreamStartRow) - rowsRead,
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

} // namespace

FieldReaderParams Deserializer::createFieldReaderParams() const {
  FieldReaderParams params;
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
    : schema_{std::move(schema)}, pool_{pool}, options_{std::move(options)} {
  const auto params = createFieldReaderParams();
  parser_ = std::make_unique<serde::StreamDataParser>(pool_, options_);

  std::shared_ptr<const velox::dwio::common::TypeWithId> schemaWithId =
      velox::dwio::common::TypeWithId::create(convertToVeloxType(*schema_));
  std::vector<uint32_t> offsets;
  rootFactory_ = FieldReaderFactory::create(
      params, schema_, schemaWithId, offsets, [](auto) { return true; }, pool_);
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
  deserializerMap_[getMainDescriptor(type).offset()] =
      std::make_unique<SegmentedStreamDecoder>(
          &type,
          /*isInMapStream=*/false,
          options_.bufferPoolCapacity,
          pool_);
  // FlatMap is only supported at depth 1 (top-level columns). Register each
  // child in-map stream so it is decoded like other physical streams.
  if (type.isFlatMap()) {
    NIMBLE_CHECK_EQ(
        depth, 1, "FlatMap is only supported as a top-level column (depth 1)");
    auto& flatMap = type.asFlatMap();
    for (size_t i = 0; i < flatMap.childrenCount(); ++i) {
      const auto inMapOffset = flatMap.inMapDescriptorAt(i).offset();
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

void Deserializer::decodeRun(DecodeRun& run, velox::VectorPtr& output) const {
  if (FOLLY_UNLIKELY(run.batches == 0)) {
    return;
  }

  velox::VectorPtr decoded;
  reader_->next(run.rows, decoded, nullptr);
  run = {};
  appendToOutput(std::move(decoded), output);
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
    DecodeRun& run,
    velox::VectorPtr& output) const {
  const auto rowCount = parser_->initialize(batch);
  const auto requiresBarrier = parser_->requiresNullBarrier();
  if (FOLLY_UNLIKELY(requiresBarrier)) {
    decodeRun(run, output);
  }

  appendStreamSegments(rowCount, /*startRow=*/run.rows, requiresBarrier);
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
  NIMBLE_CHECK(!data.empty(), "Expected at least one serialized batch");

  output = nullptr;
  DecodeRun run;
  for (const auto batch : data) {
    appendBatch(batch, run, output);
  }
  decodeRun(run, output);
  parser_->reset();
}

} // namespace facebook::nimble
