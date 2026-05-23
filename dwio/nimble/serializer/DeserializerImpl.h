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

#include <functional>
#include <optional>

#include <zstd.h>

#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/serializer/SerializerImpl.h"
#include "dwio/nimble/velox/RowRange.h"
#include "dwio/nimble/velox/SchemaTypes.h"
#include "velox/buffer/Buffer.h"
#include "velox/buffer/BufferPool.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble::serde {

/// Reads the kTabletRaw chunk slice header starting at `*pos` and advances
/// `*pos` past it (including any resume key bytes). NIMBLE_CHECKs that the
/// version is kTabletRaw and that the row range satisfies
/// startRow <= endRow <= rowCount.
///
/// Counterpart to the writer-side `createTabletChunkHeader` in
/// SerializerImpl.h. Used both by NimbleTable.h helpers (extract row range /
/// resume key from a chunk slice) and by StreamDataReader::initialize (when
/// the data is kTabletRaw).
TabletChunkHeader readTabletChunkHeader(const char*& pos, const char* end);

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
  // Whether encoding headers use varint row counts (true for kCompact/
  // kCompactRaw) or fixed u32 (false for kTabletRaw). Non-const to allow
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
  /// PRECONDITION (kTabletRaw only): the per-slice header
  /// (`[version][rowCount:varint][startRow:varint][endRow:varint]`
  /// `[resumeKeyLength:varint][resumeKey]`) must live contiguously within
  /// `data`. The projector emits the header in a single allocation; consumers
  /// that hand chunk slices to this method should coalesce the IOBuf chain
  /// first (`folly::IOBuf::cloneCoalescedAsValue()`).
  uint32_t initialize(std::string_view data);

  void iterateStreams(
      const std::function<void(uint32_t offset, std::string_view data)>&
          callback);

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

  /// Returns the row range embedded in the per-slice header for kTabletRaw
  /// chunks (always present on the wire for kTabletRaw). nullopt for all
  /// other serialization versions. Only valid after initialize() has been
  /// called.
  const std::optional<RowRange>& rowRange() const {
    return rowRange_;
  }

 private:
  // Reads and validates the serialization version from the data header.
  // Sets version_ from the first byte if hasHeader is true, otherwise defaults
  // to kLegacy. Advances pos_ past the version byte.
  void readVersion();

  // Strips tablet chunk headers from stream data for kTabletRaw format.
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
  // Per-request row range embedded in the kTabletRaw header (post-rowCount,
  // before stream data). nullopt for non-kTabletRaw formats or when the
  // producer did not embed a row range.
  std::optional<RowRange> rowRange_;
  const char* pos_{nullptr};
  const char* end_{nullptr};
  // Reusable buffer for chunk header stripping (compressed/multi-chunk case).
  Vector<char> chunkStrippingBuffer_;
};

} // namespace facebook::nimble::serde
