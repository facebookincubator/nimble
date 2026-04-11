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
#include "velox/buffer/Buffer.h"
#include "velox/dwio/common/TypeWithId.h"

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

// Decoder implementation for deserializing stream data from multiple batches.
class DeserializerImpl : public Decoder {
 public:
  // inMapStream: true for FlatMap inMap streams (fills with 'false' when
  // missing), false for nulls streams (fills with 'true' when missing).
  // FlatMap is only supported at depth 1 (top-level columns), so gap detection
  // is enabled whenever the type is FlatMap.
  DeserializerImpl(
      const Type* type,
      bool inMapStream,
      bool enableBufferPool,
      std::optional<bool> useVarintRowCountOverride,
      velox::memory::MemoryPool* pool)
      : type_{type},
        pool_{pool},
        inMapStream_{inMapStream},
        scalarKind_{getScalarKindForType(*type)},
        typeStorageWidth_{getTypeStorageWidth(*type)},
        useVarintRowCountOverride_{useVarintRowCountOverride},
        bufferPool_{
            enableBufferPool ? std::make_unique<velox::BufferPool>()
                             : nullptr} {}

  uint32_t next(
      uint32_t count,
      void* output,
      std::vector<velox::BufferPtr>& stringBuffers,
      std::function<void*()> /* nulls */ = nullptr,
      const velox::bits::Bitmap* scatterBitmap = nullptr) override {
    if (count == 0) {
      return 0;
    }

    // Three read paths based on stream type:
    // readFlatMap: For FlatMap nulls/inMap with gap detection
    // scatteredRead: For scatterBitmap (StructFlatMapFieldReader values)
    // contiguousRead: For nested types - simple contiguous read
    if (type_->isFlatMap()) {
      NIMBLE_CHECK_NULL(
          scatterBitmap, "scatterBitmap not used for FlatMap streams");
      return readFlatMap(count, output, typeStorageWidth_, stringBuffers);
    }
    if (scatterBitmap != nullptr) {
      return scatteredRead(
          count, output, typeStorageWidth_, scatterBitmap, stringBuffers);
    }
    return contiguousRead(count, output, typeStorageWidth_, stringBuffers);
  }

  void skip(uint32_t /* count */) override {
    NIMBLE_UNREACHABLE("unexpected call");
  }

  void reset() override {
    NIMBLE_UNREACHABLE("unexpected call");
  }

  const Encoding* encoding() const override {
    NIMBLE_UNREACHABLE("unexpected call");
  }

  static inline DeserializerImpl* toDecoderImpl(Decoder* d) {
    return static_cast<DeserializerImpl*>(d);
  }

  // Clear all state (called at the start of deserialization).
  void clear() {
    batchSegments_.clear();
    streamData_.reset();
    presentInMapSegments_.clear();
    topLevelRows_ = 0;
    currentFlatMapRow_ = 0;
    currentSegment_ = 0;
    currentInMapSegment_ = 0;
  }

  // Add data starting at the given row offset. Stores the raw data without
  // creating encoding objects. Encodings are created lazily when the segment
  // is first read, ensuring only one encoding tree exists at a time. This
  // avoids the memory locality and allocation overhead of creating hundreds
  // of encoding trees simultaneously in batch decode.
  void addBatch(
      uint32_t rowOffset,
      std::string_view data,
      SerializationVersion version) {
    if (data.empty()) {
      return;
    }
    batchSegments_.emplace_back(BatchSegment{rowOffset, data, version});
  }

  // Record a segment where this key is present in every row (in-map stream
  // skipped by the serializer). Used by fillMissingFlatMapRows to fill gaps
  // with true (present) instead of false (absent).
  void addPresentInMapSegment(uint32_t startRow, uint32_t rowCount) {
    NIMBLE_CHECK(
        type_->isFlatMap() && inMapStream_,
        "addPresentInMapSegment requires FlatMap in-map stream");
    const uint32_t endRow = startRow + rowCount;
    // Merge with the previous segment if contiguous.
    if (!presentInMapSegments_.empty() &&
        presentInMapSegments_.back().endRow == startRow) {
      presentInMapSegments_.back().endRow = endRow;
    } else {
      presentInMapSegments_.emplace_back(InMapSegment{startRow, endRow});
    }
  }

  void setTopLevelRows(uint32_t rows) {
    topLevelRows_ = rows;
  }

 private:
  // Lazily creates StreamData for the given segment index.
  // Destroys the previous StreamData so the allocator reuses the same
  // cache-hot memory (matching non-batch mode's sequential pattern).
  // Ensures streamData_ is initialized for the current segment. Creates a new
  // StreamData lazily on first access or after advanceSegment() resets it.
  // String buffers from encoding are pushed directly into the caller's
  // stringBuffers vector, so no explicit release is needed.
  serde::StreamData& ensureStreamData(
      std::vector<velox::BufferPtr>& stringBuffers) {
    if (streamData_.has_value()) {
      return *streamData_;
    }

    NIMBLE_CHECK_LT(currentSegment_, batchSegments_.size());
    const auto& segment = batchSegments_[currentSegment_];
    const serde::StreamData::Options options{
        .version = segment.version,
        .bufferPool = bufferPool_.get(),
        .useVarintRowCountOverride = useVarintRowCountOverride_,
    };
    streamData_.emplace(
        scalarKind_, segment.data, stringBuffers, pool_, options);
    return *streamData_;
  }

  // Advances to the next segment. Destroys current StreamData so the next
  // ensureStreamData() creates a fresh one for the new segment.
  void advanceSegment() {
    streamData_.reset();
    ++currentSegment_;
  }

  uint32_t readFromBatchSegment(
      void* output,
      uint32_t offset,
      uint32_t count,
      uint32_t width,
      std::vector<velox::BufferPtr>& stringBuffers) {
    auto& streamData = ensureStreamData(stringBuffers);

    // Nimble encoding path: decode dispatches by type width.
    // Returns actual count decoded (may be less than requested if encoding
    // has fewer remaining rows).
    if (streamData.hasEncoding()) {
      return streamData.decode(output, offset, count, width);
    }

    // Legacy path.
    if (width > 0) {
      auto* dest = static_cast<char*>(output) + offset * width;
      const auto copied = streamData.copyTo(dest, count * width);
      return copied / width;
    } else {
      // String type.
      auto* dest = static_cast<std::string_view*>(output) + offset;
      return streamData.decodeStrings(count, dest);
    }
  }

  // Simple contiguous read for nested types. Reads `count` values from
  // segments directly to output without gap detection or scattering.
  uint32_t contiguousRead(
      uint32_t count,
      void* output,
      uint32_t width,
      std::vector<velox::BufferPtr>& stringBuffers) {
    NIMBLE_CHECK(!type_->isFlatMap(), "contiguousRead not used for FlatMap");

    // Handle empty batchSegments_ for Row nulls streams. The serializer omits
    // Row nulls when all values are non-null. Fill with true (all non-null).
    if (batchSegments_.empty()) {
      NIMBLE_CHECK(
          type_->isRow(),
          "batchSegments_ is empty for unexpected type={}",
          toString(type_->kind()));
      fillMissingRows(output, /*offset=*/0, count, width);
      return count;
    }

    uint32_t valuesRead{0};
    while (valuesRead < count && currentSegment_ < batchSegments_.size()) {
      const uint32_t toRead = count - valuesRead;
      const uint32_t read = readFromBatchSegment(
          output, valuesRead, toRead, width, stringBuffers);
      if (read == 0) {
        advanceSegment();
      } else {
        valuesRead += read;
        if (read < toRead) {
          advanceSegment();
        }
      }
    }
    NIMBLE_CHECK_EQ(valuesRead, count, "Incomplete read");
    return count;
  }

  // Read for FlatMap nulls/inMap streams with gap detection.
  // Detects gaps between segments (where certain keys are missing) and fills
  // with placeholder data. Uses currentFlatMapRow_ to track position and
  // compare with segment startRow.
  uint32_t readFlatMap(
      uint32_t count,
      void* output,
      uint32_t width,
      std::vector<velox::BufferPtr>& stringBuffers) {
    NIMBLE_CHECK(type_->isFlatMap(), "readFlatMap requires FlatMap type");
    // Only used for FlatMap nulls/inMap streams which are boolean.
    NIMBLE_CHECK_EQ(width, sizeof(bool), "readFlatMap expects bool width");

    uint32_t rowsRead = 0;
    while (rowsRead < count) {
      // Gap detection: fill missing rows before current segment starts.
      if (currentSegment_ < batchSegments_.size() &&
          currentFlatMapRow_ < batchSegments_[currentSegment_].startRow) {
        const uint32_t segmentStartRow =
            batchSegments_[currentSegment_].startRow;
        const uint32_t numMissingRows =
            std::min(segmentStartRow - currentFlatMapRow_, count - rowsRead);
        fillMissingFlatMapRows(output, rowsRead, numMissingRows, width);
        rowsRead += numMissingRows;
        currentFlatMapRow_ += numMissingRows;
        continue;
      }

      if (currentSegment_ >= batchSegments_.size()) {
        // No more segments - fill remaining with placeholder.
        if (currentFlatMapRow_ < topLevelRows_) {
          const uint32_t numMissingRows =
              std::min(topLevelRows_ - currentFlatMapRow_, count - rowsRead);
          fillMissingFlatMapRows(output, rowsRead, numMissingRows, width);
          rowsRead += numMissingRows;
          currentFlatMapRow_ += numMissingRows;
          continue;
        }
        NIMBLE_FAIL("Incomplete read: no more segments and beyond totalRows");
      }

      // Read from current segment.
      const uint32_t rowsToRead = count - rowsRead;
      const uint32_t numRowsRead = readFromBatchSegment(
          output, /*offset=*/rowsRead, rowsToRead, width, stringBuffers);
      if (numRowsRead == 0) {
        advanceSegment();
      } else {
        rowsRead += numRowsRead;
        currentFlatMapRow_ += numRowsRead;
        if (numRowsRead < rowsToRead) {
          advanceSegment();
        }
      }
    }
    return count;
  }

  // Ensure scatterBuffer_ has at least the requested capacity.
  // Only grows the buffer, never shrinks, to avoid repeated allocations.
  char* ensureScatterBuffer(size_t bytes) {
    if (scatterBuffer_ == nullptr || scatterBuffer_->capacity() < bytes) {
      scatterBuffer_ = velox::AlignedBuffer::allocate<char>(bytes, pool_);
    }
    return scatterBuffer_->asMutable<char>();
  }

  // Read values contiguously then scatter to positions where scatterBitmap
  // bits are set. Used for FlatMap value columns where some rows don't have
  // certain keys (inMap=false).
  uint32_t scatteredRead(
      uint32_t count,
      void* output,
      uint32_t width,
      const velox::bits::Bitmap* scatterBitmap,
      std::vector<velox::BufferPtr>& stringBuffers) {
    const auto outputSize = scatterBitmap->size();
    // Fast path: if bitmap is dense (all bits set), read directly to output.
    // This avoids temp buffer allocation and scatter overhead.
    if (count == outputSize) {
      return contiguousRead(count, output, width, stringBuffers);
    }

    if (width > 0) {
      // Fixed-width types: read to temp buffer, then scatter.
      auto* buffer = ensureScatterBuffer((size_t)count * width);
      uint32_t valuesRead = 0;
      while (valuesRead < count && currentSegment_ < batchSegments_.size()) {
        auto& streamData = ensureStreamData(stringBuffers);
        auto* dest = buffer + valuesRead * width;
        const auto toRead = count - valuesRead;
        const auto copied = streamData.copyTo(dest, toRead * width);
        const auto read = copied / width;
        if (read == 0) {
          advanceSegment();
        } else {
          valuesRead += read;
          if (read < toRead) {
            advanceSegment();
          }
        }
      }
      NIMBLE_CHECK_EQ(valuesRead, count, "Incomplete read");

      // Scatter to output positions where bitmap is set.
      const char* src = buffer;
      auto* dst = static_cast<char*>(output);
      for (uint32_t pos = 0; pos < outputSize; ++pos) {
        if (scatterBitmap->test(pos)) {
          std::memcpy(dst + pos * width, src, width);
          src += width;
        }
      }
    } else {
      // String types: read to temp buffer, then scatter.
      auto* stringBuffer = reinterpret_cast<std::string_view*>(
          ensureScatterBuffer(count * sizeof(std::string_view)));
      uint32_t valuesRead = 0;
      while (valuesRead < count && currentSegment_ < batchSegments_.size()) {
        auto& streamData = ensureStreamData(stringBuffers);
        const auto toRead = count - valuesRead;
        const auto read =
            streamData.decodeStrings(toRead, stringBuffer + valuesRead);
        if (read == 0) {
          advanceSegment();
        } else {
          valuesRead += read;
          if (read < toRead) {
            advanceSegment();
          }
        }
      }
      NIMBLE_CHECK_EQ(valuesRead, count, "Incomplete read");

      // Scatter to output positions where bitmap is set.
      const std::string_view* src = stringBuffer;
      auto* dst = static_cast<std::string_view*>(output);
      for (uint32_t pos = 0; pos < outputSize; ++pos) {
        if (scatterBitmap->test(pos)) {
          dst[pos] = *src++;
        }
      }
    }

    return count;
  }

  // Fill missing rows for FlatMap streams (nulls or in-map).
  // For nulls streams, delegates to fillMissingRows (fills with true).
  // For in-map streams, a gap can span multiple segments — some with key
  // present in every row (serializer skipped), some absent (key missing).
  // Default fills with false, then overlays present segments.
  void fillMissingFlatMapRows(
      void* output,
      uint32_t offset,
      uint32_t count,
      uint32_t width) {
    NIMBLE_CHECK(
        type_->isFlatMap(), "fillMissingFlatMapRows requires FlatMap type");

    if (!inMapStream_ || presentInMapSegments_.empty()) {
      fillMissingRows(output, offset, count, width);
      return;
    }

    auto* bools = static_cast<bool*>(output) + offset;
    const uint32_t startRow = currentFlatMapRow_;
    const uint32_t endRow = startRow + count;

    // Fast path: single present segment covers the entire gap.
    if (currentInMapSegment_ < presentInMapSegments_.size()) {
      const auto& segment = presentInMapSegments_[currentInMapSegment_];
      if (segment.startRow <= startRow && segment.endRow >= endRow) {
        std::memset(bools, 1, count);
        return;
      }
    }

    // General path: default fill with false, then overlay present segments.
    // Segments are sorted by startRow, so advance currentInMapSegment_ past
    // consumed segments to avoid re-scanning.
    std::memset(bools, 0, count);
    while (currentInMapSegment_ < presentInMapSegments_.size()) {
      const auto& segment = presentInMapSegments_[currentInMapSegment_];
      if (segment.startRow >= endRow) {
        break;
      }
      if (segment.endRow > startRow) {
        const uint32_t overlapStart = std::max(segment.startRow, startRow);
        const uint32_t overlapEnd = std::min(segment.endRow, endRow);
        std::memset(
            bools + (overlapStart - startRow), 1, overlapEnd - overlapStart);
      }
      // Advance past segments fully consumed by this gap.
      if (segment.endRow <= endRow) {
        ++currentInMapSegment_;
      } else {
        break;
      }
    }
  }

  void fillMissingRows(
      void* output,
      uint32_t offset,
      uint32_t count,
      uint32_t width) {
    if (type_->isRow() || type_->isFlatMap()) {
      // For Row/FlatMap nulls stream, fill with true (all non-null).
      // For FlatMap inMap streams, fill with false (key not present).
      NIMBLE_CHECK_EQ(width, sizeof(bool), "Unexpected width for Row/FlatMap");
      auto* bools = static_cast<bool*>(output) + offset;
      std::memset(bools, inMapStream_ ? 0 : 1, count);
    } else if (type_->isScalar()) {
      // For Scalar types, fill with zeros.
      if (width > 0) {
        std::memset(
            static_cast<char*>(output) + offset * width, 0, count * width);
      }
      // For strings (width == 0), leave as empty string_views.
    } else {
      // For other types (Array, Map, etc.), fill lengths with zeros.
      NIMBLE_CHECK_EQ(
          width, sizeof(uint32_t), "Unexpected width for Array/Map");
      std::memset(
          static_cast<char*>(output) + offset * width, 0, count * width);
    }
  }

  // Batch segment storing raw data for lazy StreamData creation.
  // Encoding objects are created on-demand when the segment is first read,
  // ensuring only one encoding tree exists at a time for better cache locality.
  struct BatchSegment {
    uint32_t startRow; // Row offset where this segment starts.
    std::string_view data; // Raw stream data (valid for lifetime of input).
    SerializationVersion version;
  };

  // Row range [startRow, endRow) for segments where this key is present in
  // every row. The serializer skips the in-map stream for these segments, and
  // fillMissingFlatMapRows fills with true (present) instead of false (absent).
  struct InMapSegment {
    uint32_t startRow;
    uint32_t endRow;
  };

  // --- Const members (set at construction, never modified) ---
  const Type* const type_;
  velox::memory::MemoryPool* const pool_;
  // True for inMap streams (fills with 'false' when missing), false for nulls
  // streams (fills with 'true' when missing).
  const bool inMapStream_;
  // Cached from type at construction to avoid per-call dispatch.
  const ScalarKind scalarKind_;
  const uint32_t typeStorageWidth_;
  // Override for varint row count format in kTabletRaw streams.
  const std::optional<bool> useVarintRowCountOverride_;
  // Pool for encoding scratch buffers (e.g. MainlyConstant's isCommon and
  // otherValues buffers). Persists across clear()/addBatch() cycles so buffers
  // are reused instead of being allocated/freed through MemoryPool each time.
  // Null when buffer pooling is disabled via DeserializerOptions.
  const std::unique_ptr<velox::BufferPool> bufferPool_;

  // --- Batch decode state (reset in clear()) ---
  // Total top-level rows across all batches. Used for FlatMap gap detection to
  // fill missing rows at the end.
  uint32_t topLevelRows_{0};
  std::vector<BatchSegment> batchSegments_;
  size_t currentSegment_{0}; // Current index into batchSegments_

  // Lazily-created StreamData. Only one exists at a time — destroyed before
  // creating the next so the allocator reuses cache-hot memory.
  std::optional<serde::StreamData> streamData_;

  // --- FlatMap state (reset in clear()) ---
  // FlatMap is only supported at depth 1 (top-level columns). Gap detection is
  // enabled whenever type_->isFlatMap(). These fields are unused for
  // non-FlatMap types.

  // Current read position for FlatMap gap detection.
  uint32_t currentFlatMapRow_{0};
  // Segments where this key is present in every row (in-map stream skipped).
  // Used by fillMissingFlatMapRows to fill with true (present).
  std::vector<InMapSegment> presentInMapSegments_;
  size_t currentInMapSegment_{0}; // Current index into presentInMapSegments_

  // Temp buffer for scattered reads (reused to avoid repeated allocations).
  // Used for both fixed-width types (as char*) and strings (as string_view*).
  velox::BufferPtr scatterBuffer_;
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

  std::shared_ptr<const velox::dwio::common::TypeWithId> schemaWithId =
      velox::dwio::common::TypeWithId::create(convertToVeloxType(*schema_));
  std::vector<uint32_t> offsets;
  rootFactory_ = FieldReaderFactory::create(
      params, schema_, schemaWithId, offsets, [](auto) { return true; }, pool_);
  SchemaReader::traverseSchema(schema_, [this](auto depth, auto& type, auto&) {
    createDeserializersForType(type, depth);
  });

  rootReader_ = rootFactory_->createReader(deserializerMap_);
  inputBuffer_.resize(1);

  // Build flat vector for O(1) stream offset lookup during deserialize().
  uint32_t maxOffset = 0;
  for (const auto& [offset, _] : deserializerMap_) {
    maxOffset = std::max(maxOffset, offset);
  }
  deserializers_.resize(maxOffset + 1, nullptr);
  for (auto& [offset, decoder] : deserializerMap_) {
    deserializers_[offset] = decoder.get();
  }
  // Size inMapPresentOffsets_ to match for flatmap present tracking.
  if (!inMapChildTypes_.empty()) {
    inMapPresentOffsets_.resize(maxOffset + 1, false);
  }
}

void Deserializer::createDeserializersForType(
    const Type& type,
    uint32_t depth) {
  const auto useVarintOverride =
      options_.useVarintRowCount ? std::optional<bool>{true} : std::nullopt;
  deserializerMap_[getMainDescriptor(type).offset()] =
      std::make_unique<DeserializerImpl>(
          &type,
          /*inMapStream=*/false,
          options_.enableBufferPool,
          useVarintOverride,
          pool_);
  // FlatMap is only supported at depth 1 (top-level columns). FlatMap keys can
  // vary across batches, causing gaps in nulls/inMap streams. Gap detection is
  // enabled in DeserializerImpl whenever type->isFlatMap().
  if (type.isFlatMap()) {
    NIMBLE_CHECK_EQ(
        depth, 1, "FlatMap is only supported as a top-level column (depth 1)");
    auto& flatMap = type.asFlatMap();
    for (size_t i = 0; i < flatMap.childrenCount(); ++i) {
      const auto inMapOffset = flatMap.inMapDescriptorAt(i).offset();
      deserializerMap_[inMapOffset] = std::make_unique<DeserializerImpl>(
          &type,
          /*inMapStream=*/true,
          options_.enableBufferPool,
          useVarintOverride,
          pool_);
      inMapChildTypes_[inMapOffset] = flatMap.childAt(i).get();
    }
  }
}

void Deserializer::deserialize(std::string_view data, velox::VectorPtr& vector)
    const {
  inputBuffer_[0] = data;
  deserialize(inputBuffer_, vector);
}

void Deserializer::deserialize(
    const std::vector<std::string_view>& data,
    velox::VectorPtr& vector) const {
  // Clear deserializer state from previous calls.
  for (auto& [_, decoder] : deserializerMap_) {
    DeserializerImpl::toDecoderImpl(decoder.get())->clear();
  }
  const bool hasInMapChildren = !inMapChildTypes_.empty();
  const auto maxStreamOffset = deserializers_.size() - 1;

  // Iterate batches and add stream data with row offsets. Streams missing from
  // a batch will have gaps that are filled later during reading.
  uint32_t rowOffset{0};
  serde::StreamDataReader reader{pool_, options_};
  for (auto sv : data) {
    const auto batchRows = reader.initialize(sv);
    const auto version = reader.version();
    // Reset present tracking from previous batch.
    if (hasInMapChildren && !inMapPresentOffsetsList_.empty()) {
      for (auto off : inMapPresentOffsetsList_) {
        inMapPresentOffsets_[off] = false;
      }
      inMapPresentOffsetsList_.clear();
    }
    reader.iterateStreams([&](uint32_t offset, std::string_view streamData) {
      if (offset <= maxStreamOffset) {
        if (hasInMapChildren) {
          inMapPresentOffsets_[offset] = true;
          inMapPresentOffsetsList_.push_back(offset);
        }
        auto* decoder = deserializers_[offset];
        if (decoder != nullptr) {
          DeserializerImpl::toDecoderImpl(decoder)->addBatch(
              rowOffset, streamData, version);
        }
      }
    });

    // Detect present in-map streams: in-map skipped + value streams present.
    for (const auto& [inMapOffset, childType] : inMapChildTypes_) {
      if (!inMapPresentOffsets_[inMapOffset] &&
          hasValueStreams(*childType, [&](offset_size offset) {
            return offset <= maxStreamOffset && inMapPresentOffsets_[offset];
          })) {
        DeserializerImpl::toDecoderImpl(deserializers_[inMapOffset])
            ->addPresentInMapSegment(rowOffset, batchRows);
      }
    }

    rowOffset += batchRows;
  }

  // Set total top-level rows so deserializers can fill missing FlatMap data.
  for (auto& [_, decoder] : deserializerMap_) {
    DeserializerImpl::toDecoderImpl(decoder.get())->setTopLevelRows(rowOffset);
  }

  rootReader_->next(rowOffset, vector, /*scatterBitmap=*/nullptr);
}

} // namespace facebook::nimble
