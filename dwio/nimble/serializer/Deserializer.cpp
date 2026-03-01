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
  // topLevelFlatMap: true for top-level FlatMap streams (depth 1) where row
  // count matches top-level row count. Enables gap detection and filling for
  // keys missing from some batches. Nested FlatMaps (depth > 1) are not
  // supported as their row counts depend on parent inMap streams.
  //
  // inMapStream: true for FlatMap inMap streams (fills with 'false' when
  // missing), false for nulls streams (fills with 'true' when missing).
  explicit DeserializerImpl(
      const Type* type,
      velox::memory::MemoryPool* pool,
      bool topLevelFlatMap = false,
      bool inMapStream = false,
      bool enableEncoding = false)
      : type_{type},
        pool_{pool},
        topLevelFlatMap_{topLevelFlatMap},
        inMapStream_{inMapStream},
        encodingEnabled_{enableEncoding} {}

  uint32_t next(
      uint32_t count,
      void* output,
      std::vector<velox::BufferPtr>& /* stringBuffers */,
      std::function<void*()> /* nulls */ = nullptr,
      const bits::Bitmap* scatterBitmap = nullptr) override {
    if (count == 0) {
      return 0;
    }

    const auto typeStorageWidth = getTypeStorageWidth(*type_);

    // Three read paths based on stream type:
    // readTopLevelFlatMap: For top-level FlatMap nulls/inMap with gap detection
    // (handles empty batchSegments_ internally)
    // scatteredRead: For scatterBitmap (StructFlatMapFieldReader values)
    // contiguousRead: For nested types - simple contiguous read
    if (topLevelFlatMap_) {
      NIMBLE_CHECK_NULL(
          scatterBitmap, "scatterBitmap not used for topLevelFlatMap streams");
      return readTopLevelFlatMap(count, output, typeStorageWidth);
    }

    if (scatterBitmap != nullptr) {
      return scatteredRead(count, output, typeStorageWidth, scatterBitmap);
    }

    return contiguousRead(count, output, typeStorageWidth);
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
    topLevelRows_ = 0;
    flatMapCurrentRow_ = 0;
    currentSegment_ = 0;
  }

  // Add data starting at the given row offset.
  void addBatch(uint32_t rowOffset, std::string_view data) {
    if (data.empty()) {
      return;
    }
    // Get the ScalarKind for proper decoding based on type.
    // - Scalar: use the descriptor's scalarKind
    // - Row/FlatMap: nulls streams are boolean
    // - Array/Map: lengths streams are uint32_t
    // - String/Binary: handled separately (no nimble encoding)
    const ScalarKind scalarKind = getScalarKindForType(*type_);
    NIMBLE_CHECK_NE(scalarKind, ScalarKind::Undefined);
    batchSegments_.push_back(
        {rowOffset,
         serde::StreamData(scalarKind, encodingEnabled_, data, pool_)});
  }

  void setTopLevelRows(uint32_t rows) {
    topLevelRows_ = rows;
  }

 private:
  uint32_t readFromBatchSegment(
      void* output,
      uint32_t offset,
      uint32_t count,
      uint32_t width) {
    NIMBLE_CHECK_LT(currentSegment_, batchSegments_.size());
    auto& segment = batchSegments_[currentSegment_];

    // Nimble encoding path: decode dispatches by type width.
    // Returns actual count decoded (may be less than requested if encoding
    // has fewer remaining rows).
    if (segment.data.hasEncoding()) {
      return segment.data.decode(output, offset, count, width);
    }

    // Legacy path.
    if (width > 0) {
      auto* dest = static_cast<char*>(output) + offset * width;
      const auto copied = segment.data.copyTo(dest, count * width);
      return copied / width;
    } else {
      // String type.
      auto* dest = static_cast<std::string_view*>(output) + offset;
      return segment.data.decodeStrings(count, dest);
    }
  }

  // Simple contiguous read for nested types. Reads `count` values from
  // segments directly to output without gap detection or scattering.
  uint32_t contiguousRead(uint32_t count, void* output, uint32_t width) {
    NIMBLE_CHECK(!topLevelFlatMap_, "contiguousRead not used for FlatMap");

    // Handle empty batchSegments_ for Row/FlatMap nulls streams.
    // The serializer doesn't write Row/FlatMap nulls when all values are
    // non-null (to save space). Fill with true (all non-null) in this case.
    if (batchSegments_.empty()) {
      NIMBLE_CHECK(
          type_->isRow() || type_->isFlatMap(),
          "batchSegments_ is empty for unexpected type={}",
          toString(type_->kind()));
      fillMissingRows(output, /*offset=*/0, count, width);
      return count;
    }

    uint32_t valuesRead{0};
    while (valuesRead < count && currentSegment_ < batchSegments_.size()) {
      const uint32_t toRead = count - valuesRead;
      const uint32_t read =
          readFromBatchSegment(output, valuesRead, toRead, width);
      if (read == 0) {
        ++currentSegment_;
      } else {
        valuesRead += read;
        if (read < toRead) {
          ++currentSegment_;
        }
      }
    }
    NIMBLE_CHECK_EQ(valuesRead, count, "Incomplete read");
    return count;
  }

  // Read for top-level FlatMap nulls/inMap streams with gap detection.
  // Detects gaps between batches (where certain keys are missing) and fills
  // with placeholder data. Uses flatMapCurrentRow_ to track position and
  // compare with segment startRow.
  uint32_t readTopLevelFlatMap(uint32_t count, void* output, uint32_t width) {
    NIMBLE_CHECK(topLevelFlatMap_, "readTopLevelFlatMap not used for FlatMap");
    // Only used for FlatMap nulls/inMap streams which are boolean.
    NIMBLE_CHECK_EQ(
        width, sizeof(bool), "readTopLevelFlatMap expects bool width");

    uint32_t rowsRead = 0;
    while (rowsRead < count) {
      // Gap detection: fill missing rows before current segment starts.
      if (currentSegment_ < batchSegments_.size() &&
          flatMapCurrentRow_ < batchSegments_[currentSegment_].startRow) {
        const uint32_t segmentStartRow =
            batchSegments_[currentSegment_].startRow;
        const uint32_t numMissingRows =
            std::min(segmentStartRow - flatMapCurrentRow_, count - rowsRead);
        fillMissingRows(output, /*offset=*/rowsRead, numMissingRows, width);
        rowsRead += numMissingRows;
        flatMapCurrentRow_ += numMissingRows;
        continue;
      }

      if (currentSegment_ >= batchSegments_.size()) {
        // No more segments - fill remaining with placeholder.
        if (flatMapCurrentRow_ < topLevelRows_) {
          const uint32_t numMissingRows =
              std::min(topLevelRows_ - flatMapCurrentRow_, count - rowsRead);
          fillMissingRows(output, /*offset=*/rowsRead, numMissingRows, width);
          rowsRead += numMissingRows;
          flatMapCurrentRow_ += numMissingRows;
          continue;
        }
        NIMBLE_FAIL("Incomplete read: no more segments and beyond totalRows");
      }

      // Read from current segment.
      const uint32_t rowsToRead = count - rowsRead;
      const uint32_t numRowsRead =
          readFromBatchSegment(output, /*offset=*/rowsRead, rowsToRead, width);
      if (numRowsRead == 0) {
        ++currentSegment_;
      } else {
        rowsRead += numRowsRead;
        flatMapCurrentRow_ += numRowsRead;
        if (numRowsRead < rowsToRead) {
          ++currentSegment_;
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
      const bits::Bitmap* scatterBitmap) {
    const auto outputSize = scatterBitmap->size();
    // Fast path: if bitmap is dense (all bits set), read directly to output.
    // This avoids temp buffer allocation and scatter overhead.
    if (count == outputSize) {
      return contiguousRead(count, output, width);
    }

    if (width > 0) {
      // Fixed-width types: read to temp buffer, then scatter.
      auto* buffer = ensureScatterBuffer((size_t)count * width);
      uint32_t valuesRead = 0;
      while (valuesRead < count && currentSegment_ < batchSegments_.size()) {
        auto& segment = batchSegments_[currentSegment_];
        auto* dest = buffer + valuesRead * width;
        const auto toRead = count - valuesRead;
        const auto copied = segment.data.copyTo(dest, toRead * width);
        const auto read = copied / width;
        if (read == 0) {
          ++currentSegment_;
        } else {
          valuesRead += read;
          if (read < toRead) {
            ++currentSegment_;
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
        auto& segment = batchSegments_[currentSegment_];
        const auto toRead = count - valuesRead;
        const auto read =
            segment.data.decodeStrings(toRead, stringBuffer + valuesRead);
        if (read == 0) {
          ++currentSegment_;
        } else {
          valuesRead += read;
          if (read < toRead) {
            ++currentSegment_;
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

  // Batch segment with row offset for gap detection in multi-batch scenarios.
  // When FlatMap keys are missing in some batches, we detect gaps by comparing
  // segment start rows and fill missing data.
  struct BatchSegment {
    uint32_t startRow; // Row offset where this segment starts
    serde::StreamData data; // Actual stream data
  };

  const Type* const type_;
  velox::memory::MemoryPool* const pool_;
  // True for top-level FlatMap streams where row count matches top-level count.
  // Enables gap detection and filling placeholder data for keys missing from
  // some batches.
  const bool topLevelFlatMap_;
  // True for inMap streams (fills with 'false' when missing), false for nulls
  // streams (fills with 'true' when missing).
  const bool inMapStream_;
  // True when nimble encoding is used for scalar streams.
  const bool encodingEnabled_;

  // --- Batch decode state (reset in clear()) ---
  // Total top-level rows across all batches. Used for gap detection in
  // top-level FlatMap streams to fill missing rows at the end.
  uint32_t topLevelRows_{0};
  std::vector<BatchSegment> batchSegments_;
  // Current read position for top-level FlatMap gap detection.
  uint32_t flatMapCurrentRow_{0};
  size_t currentSegment_{0}; // Current index into batchSegments_
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

Deserializer::Deserializer(
    std::shared_ptr<const Type> schema,
    velox::memory::MemoryPool* pool)
    : Deserializer{std::move(schema), pool, {}} {}

Deserializer::Deserializer(
    std::shared_ptr<const Type> schema,
    velox::memory::MemoryPool* pool,
    DeserializerOptions options)
    : schema_{std::move(schema)}, pool_{pool}, options_{std::move(options)} {
  FieldReaderParams params;
  std::shared_ptr<const velox::dwio::common::TypeWithId> schemaWithId =
      velox::dwio::common::TypeWithId::create(convertToVeloxType(*schema_));
  std::vector<uint32_t> offsets;
  rootFactory_ = FieldReaderFactory::create(
      params, *pool_, schema_, schemaWithId, offsets);
  SchemaReader::traverseSchema(schema_, [this](auto depth, auto& type, auto&) {
    createDeserializersForType(type, depth);
  });

  rootReader_ = rootFactory_->createReader(deserializers_);
  inputBuffer_.resize(1);
}

void Deserializer::createDeserializersForType(
    const Type& type,
    uint32_t depth) {
  // topLevelFlatMap only for FlatMap columns at depth 1.
  // FlatMap keys can vary across batches, causing gaps in nulls/inMap streams.
  // - Root Row (depth 0): No gaps - every batch has Row's nulls stream.
  // - FlatMap at depth 1: Gaps possible - keys may be missing in some batches.
  // - Nested types (depth > 1): Row counts don't match top-level row offset.
  const bool topLevelFlatMap = type.isFlatMap() && depth == 1;
  deserializers_[getMainDescriptor(type).offset()] =
      std::make_unique<DeserializerImpl>(
          &type,
          pool_,
          topLevelFlatMap,
          /*inMapStream=*/false,
          options_.enableEncoding());
  // FlatMap has additional inMap streams for each child that need
  // deserializers. Pass isFlatMapInMapStream=true so placeholder fills with
  // 'false' (key not present) instead of 'true'.
  if (type.isFlatMap()) {
    auto& flatMap = type.asFlatMap();
    for (size_t i = 0; i < flatMap.childrenCount(); ++i) {
      deserializers_[flatMap.inMapDescriptorAt(i).offset()] =
          std::make_unique<DeserializerImpl>(
              &type,
              pool_,
              topLevelFlatMap,
              /*inMapStream=*/true,
              options_.enableEncoding());
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
  for (auto& [_, deserializer] : deserializers_) {
    DeserializerImpl::toDecoderImpl(deserializer.get())->clear();
  }

  // Iterate batches and add stream data with row offsets. Streams missing from
  // a batch will have gaps that are filled later during reading.
  uint32_t rowOffset{0};
  serde::StreamDataReader reader{options_};
  for (auto sv : data) {
    const auto batchRows = reader.initialize(sv);
    reader.iterateStreams([&](uint32_t offset, std::string_view streamData) {
      auto it = deserializers_.find(offset);
      if (it != deserializers_.end()) {
        DeserializerImpl::toDecoderImpl(it->second.get())
            ->addBatch(rowOffset, streamData);
      }
    });
    rowOffset += batchRows;
  }

  // Set total top-level rows so deserializers can fill missing FlatMap data.
  for (auto& [_, deserializer] : deserializers_) {
    DeserializerImpl::toDecoderImpl(deserializer.get())
        ->setTopLevelRows(rowOffset);
  }

  rootReader_->next(rowOffset, vector, /*scatterBitmap=*/nullptr);
}

} // namespace facebook::nimble
