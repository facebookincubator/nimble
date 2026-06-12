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

#include <optional>

#include <zstd.h>

#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/serializer/SerializationHeader.h"
#include "dwio/nimble/serializer/SerializerImpl.h"
#include "dwio/nimble/serializer/legacy/TrailerReader.h"
#include "dwio/nimble/velox/RowRange.h"
#include "dwio/nimble/velox/SchemaTypes.h"
#include "velox/buffer/Buffer.h"
#include "velox/buffer/BufferPool.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble::serde {

class StreamData {
 public:
  /// Decode configuration for stream data.
  struct Options {
    /// Serialization version. Determines encoding and row count format.
    SerializationVersion version;
    /// Optional pool for encoding scratch buffers.
    velox::BufferPool* bufferPool{nullptr};
    /// Externally-owned decompression buffer. Required (must not be null).
    /// Owned by DeserializerImpl to persist across segment transitions so the
    /// buffer capacity is reused.
    velox::BufferPtr* decompressionBuffer{nullptr};
  };

  /// Constructor for thrift decoder: creates an empty stream that will be
  /// populated later via reset().
  /// @param kind Scalar kind for the stream data.
  /// @param pool Memory pool for encoding buffer allocation. Required when
  ///             reset() will be called with encodingEnabled=true.
  /// @param stringBuffers External vector where string buffers from encoding
  ///        are stored. The caller must keep the vector alive while
  ///        string_views from this StreamData are in use.
  StreamData(
      ScalarKind kind,
      std::vector<velox::BufferPtr>& stringBuffers,
      velox::memory::MemoryPool* pool,
      velox::BufferPtr* decompressionBuffer)
      : kind_{kind},
        pool_{pool},
        encodingEnabled_{false},
        decompressionBuffer_{decompressionBuffer},
        stringBuffers_{&stringBuffers} {
    NIMBLE_CHECK_NOT_NULL(
        decompressionBuffer_, "Decompression buffer required");
  }

  /// @param kind Scalar kind for the stream data.
  /// @param data Stream data to initialize with.
  /// @param pool Memory pool for encoding buffer allocation.
  /// @param options Decode configuration (version, bufferPool).
  /// @param stringBuffers External vector where string buffers from encoding
  ///        are stored. The caller must keep the vector alive while
  ///        string_views from this StreamData are in use.
  StreamData(
      ScalarKind kind,
      std::string_view data,
      std::vector<velox::BufferPtr>& stringBuffers,
      velox::memory::MemoryPool* pool,
      const Options& options);

  uint32_t copyTo(char* output, uint32_t bufferSize);

  uint32_t decodeStrings(uint32_t count, std::string_view* output);

  /// Decode nimble-encoded data to output. Dispatches to typed materialize
  /// based on width. Only valid when hasEncoding() is true.
  /// Returns the number of values actually decoded (may be less than count
  /// if encoding has fewer remaining rows).
  uint32_t
  decode(void* output, uint32_t offset, uint32_t count, uint32_t width);

  /// Simplified decode for thrift decoder: decodes 'count' values to output.
  /// Uses sizeof(T) as width and offset 0.
  template <typename T>
  void decode(T* output, uint32_t count) {
    decode(output, /*offset=*/0, count, sizeof(T));
  }

  /// Reset the stream with new data. Used by thrift decoder between rows.
  /// @param data New stream data to initialize with.
  /// @param version Serialization version determining encoding and row count
  ///        format.
  void reset(std::string_view data, SerializationVersion version);

  ScalarKind kind() const {
    return kind_;
  }

  bool hasEncoding() const {
    return encoding_ != nullptr;
  }

 private:
  // Initialize with data. For encoding path, creates Encoding object.
  // For legacy path, decompresses if not string/binary type.
  void init(std::string_view data);

  // Decompress legacy zstd-compressed data. Reads compression type prefix and
  // decompresses into decompressionBuffer_ if needed.
  void decompress();

  // Prepare nimble-encoded data for reading. Creates an Encoding object that
  // can materialize values on demand.
  void prepareForDecoding(std::string_view data);

  // Returns the number of rows remaining in the encoding.
  uint32_t remainingRows() const {
    NIMBLE_CHECK_NOT_NULL(encoding_);
    return encoding_->rowCount() - readRows_;
  }

  // Materialize values from nimble encoding to typed output.
  template <typename T>
  void materialize(uint32_t count, T* output);

  velox::BufferPtr& decompressionBuf() {
    return *decompressionBuffer_;
  }

  void ensureDecompressionBuffer(size_t minBytes);

  const ScalarKind kind_{ScalarKind::Undefined};
  velox::memory::MemoryPool* const pool_{nullptr};
  // Whether nimble encoding is enabled. Non-const to allow reset() to change.
  bool encodingEnabled_{false};
  // Whether encoding headers use varint row counts (true for kLegacyCompact) or
  // fixed u32 (false for kTablet). Non-const to allow
  // reset() to change.
  bool useVarintRowCount_{true};
  // Optional pool for encoding scratch buffers. Owned externally
  // (typically by DeserializerImpl) to persist across StreamData lifetimes.
  velox::BufferPool* const bufferPool_{nullptr};
  // Externally-owned decompression buffer. Always non-null — checked in both
  // constructors. Owned by DeserializerImpl or thrift Decoder to persist
  // across StreamData lifetimes.
  velox::BufferPtr* const decompressionBuffer_{nullptr};

  const char* pos_{nullptr};
  const char* end_{nullptr};
  std::unique_ptr<Encoding> encoding_;
  // Track consumed rows for nimble encoding path.
  uint32_t readRows_{0};
  // External storage for string buffers from encoding. Owned by the caller
  // (DeserializerImpl or thrift Decoder). Each buffer is allocated separately
  // to avoid pointer invalidation when the vector grows.
  std::vector<velox::BufferPtr>* const stringBuffers_;
};

template <typename T>
void StreamData::materialize(uint32_t count, T* output) {
  if (count == 0) {
    return;
  }
  NIMBLE_CHECK_NOT_NULL(encoding_);
  encoding_->materialize(count, output);
  readRows_ += count;
}

class StreamDataReader {
 public:
  StreamDataReader(
      velox::memory::MemoryPool* pool,
      const DeserializerOptions& options);

  /// Returns number of rows serialized.
  /// Validates that the version in serialized data matches options.
  ///
  /// PRECONDITION (kTablet only): the per-slice header
  /// (`[version][rowCount:varint][startRow:varint][endRow:varint]`
  /// `[resumeKeyLength:varint][resumeKey]`) must live contiguously within
  /// `data`. The projector emits the header in a single allocation; consumers
  /// that hand chunk slices to this method should coalesce the IOBuf chain
  /// first (`folly::IOBuf::cloneCoalescedAsValue()`).
  uint32_t initialize(std::string_view data);

  /// Walks every non-empty stream in the current blob, invoking
  /// `callback(offset, data)` per stream. For kTablet, `data` is the
  /// chunk-stripped (and decompressed, if needed) payload; for kLegacyCompact
  /// and kLegacy it is the raw stream bytes. Empty streams are skipped.
  /// Must be called at most once per `initialize()` (consumes the per-blob
  /// cursor).
  ///
  /// PERFORMANCE: This is a function template (rather than a function taking
  /// `std::function`/`folly::FunctionRef`) on purpose. The callback's call
  /// type is concrete at the call site, so `callback(offset, data)` becomes
  /// a direct call the compiler can inline into the loop body. The earlier
  /// `std::function`-based variant paid an indirect-call dispatch
  /// (`_Function_handler::_M_invoke`) per stream — measurable on per-batch
  /// hot paths with many streams (the Deserializer's flatmap deserialization
  /// over hundreds of keys).
  template <typename Callback>
  void iterateStreams(Callback&& callback) {
    if (nonLegacyFormat(version_)) {
      // kLegacyCompact/kTablet/kSerialization/kProjection: stream sizes are
      // packed into a sparse trailer at the end of the blob. Production blobs
      // at kLegacyCompact are decoded via the legacy reader (frozen snapshot of
      // the pre-two-array wire format); all newer versions go through the new
      // two-array sparse trailer reader. Reusable member buffers keep this
      // path alloc-free across blobs.
      if (usesLegacyTrailer(version_)) {
        legacy::readLegacyTrailerStreamMetadata(
            end_, streamIndices_, streamSizes_);
      } else {
        detail::readTrailerStreamMetadata(end_, streamIndices_, streamSizes_);
      }
      const bool isTablet = isTabletVersion(version_);
      const size_t numStreams = streamIndices_.size();
      for (size_t entryIdx = 0; entryIdx < numStreams; ++entryIdx) {
        const uint32_t streamOffset = streamIndices_[entryIdx];
        const uint32_t streamSize = streamSizes_[entryIdx];
        // Writer invariant: the sparse trailer only encodes non-zero stream
        // slots (SerializerImpl.h writeTrailer skips streamSizes[i]==0).
        // Debug-only assert documents that invariant; release elides it.
        NIMBLE_DCHECK_GT(
            streamSize, 0, "Sparse trailer must not encode zero-sized stream");
        std::string_view streamData(pos_, streamSize);
        pos_ += streamSize;
        if (isTablet) {
          // kTablet: stream data includes tablet chunk headers:
          // [chunkSize:u32][compressionType:1B][encoded_data...]
          // Strip headers and decompress if needed before handing off.
          callback(streamOffset, stripChunkHeaders(streamData));
        } else {
          callback(streamOffset, streamData);
        }
      }
      pos_ = end_; // Skip past trailer.
    } else {
      // kLegacy: streams in order with inline u32 sizes.
      uint32_t offset = 0;
      while (pos_ < end_) {
        uint32_t size = encoding::readUint32(pos_);
        std::string_view streamData(pos_, size);
        pos_ += size;
        if (!streamData.empty()) {
          callback(offset, streamData);
        }
        ++offset;
      }
    }

    NIMBLE_CHECK_EQ(pos_, end_, "Unexpected trailing data");
  }

  /// Returns the auto-detected serialization version.
  /// Only valid after initialize() has been called.
  SerializationVersion version() const {
    return version_;
  }

  /// Returns true if nimble encoding is enabled based on the auto-detected
  /// version. Only valid after initialize() has been called.
  bool encodingEnabled() const {
    return nonLegacyFormat(version_);
  }

  /// Returns the row range embedded in the per-slice header for kTablet
  /// chunks (always present on the wire for kTablet). nullopt for all
  /// other serialization versions. Only valid after initialize() has been
  /// called.
  const std::optional<RowRange>& rowRange() const {
    return rowRange_;
  }

 private:
  // Strips tablet chunk headers from stream data for kTablet format.
  // Each chunk is: [chunkSize:u32][compressionType:1B][encoded_data...]
  // Returns a view into the original data for single uncompressed chunks
  // (zero-copy), or a view into chunkStrippingBuffer_ for compressed/
  // multi-chunk streams.
  std::string_view stripChunkHeaders(std::string_view streamData);

  // Slow path: decompress/concatenate all chunks into chunkStrippingBuffer_.
  std::string_view slowChunkHeaderStrip(const char* pos, const char* end);

  // Returns a zero-copy view if the stream is a single uncompressed chunk.
  // Returns std::nullopt if decompression or concatenation is needed.
  std::optional<std::string_view> tryFastChunkHeaderStrip(
      const char* pos,
      const char* end);

  // Appends chunk data to chunkStrippingBuffer_, decompressing if needed.
  void appendChunkData(
      CompressionType compression,
      const char* data,
      uint32_t length);

  const DeserializerOptions& options_;
  velox::memory::MemoryPool* const pool_;

  // Serialization version detected from data. If the data has a version
  // header, this is read from the first byte; otherwise defaults to kLegacy.
  // When options specify a version, the data version is validated against it.
  SerializationVersion version_{SerializationVersion::kLegacy};
  // Per-request row range embedded in the kTablet header (post-rowCount,
  // before stream data). nullopt for non-kTablet formats or when the
  // producer did not embed a row range.
  std::optional<RowRange> rowRange_;
  const char* pos_{nullptr};
  const char* end_{nullptr};
  // Reusable buffer for chunk header stripping (compressed/multi-chunk case).
  Vector<char> chunkStrippingBuffer_;
  // Reusable parallel buffers for the per-blob sparse trailer. Refilled by
  // iterateStreams() on the kLegacyCompact/kTablet path: streamIndices_
  // holds the offsets of non-zero stream slots (sorted ascending);
  // streamSizes_ holds the corresponding byte sizes. Sized to the same
  // length, cleared+filled in place each blob to keep the hot path
  // alloc-free across invocations.
  std::vector<uint32_t> streamIndices_;
  std::vector<uint32_t> streamSizes_;
};

} // namespace facebook::nimble::serde
