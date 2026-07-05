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

#include <cstdint>
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

  StreamData(
      ScalarKind kind,
      std::vector<velox::BufferPtr>& stringBuffers,
      velox::memory::MemoryPool* pool,
      velox::BufferPtr* decompressionBuffer);

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

  struct DecodeResult {
    // Decoded output rows produced from the stream segment. For scattered
    // decode, this is the number of selected output rows, not the wider output
    // span including absent positions.
    uint32_t numOutputRows{0};
    // Non-null rows in the output range covered by this decode. For scattered
    // decode, this is in the output row domain, not the encoded row domain.
    uint32_t nonNullOutputRows{0};
    // True when this decode consumed the current physical stream segment.
    bool segmentExhausted{false};
  };

  uint32_t copyTo(char* output, uint32_t bufferSize);

  DecodeResult decodeStrings(uint32_t count, std::string_view* output);

  // Decode legacy raw fixed-width data. Legacy string streams use
  // decodeStrings().
  DecodeResult
  decodeLegacy(void* output, uint32_t offset, uint32_t count, uint32_t width);

  /// Decode nimble-encoded data to output. Dispatches to typed materialize
  /// based on width. Only valid when hasEncoding() is true.
  /// Returns decoded output rows and non-null rows in the output range.
  /// numOutputRows may be less than count if the encoding has fewer remaining
  /// rows.
  ///
  /// For nullable encodings or scattered output, getOutputNulls must return the
  /// mutable output null bitmap. If scatterOutputBitmap is provided, decoded
  /// rows and null bits are written using that scattered output layout;
  /// otherwise output is dense starting at offset.
  DecodeResult decode(
      void* output,
      uint32_t offset,
      uint32_t count,
      uint32_t width,
      const std::function<void*()>& getOutputNulls = nullptr,
      const velox::bits::Bitmap* scatterOutputBitmap = nullptr);

  /// Simplified decode for thrift decoder: decodes 'count' values to output.
  /// Uses sizeof(T) as width and offset 0.
  template <typename T>
  void decode(T* output, uint32_t count) {
    decode(output, /*offset=*/0, count, sizeof(T));
  }

  void reset(std::string_view data, SerializationVersion version);

  ScalarKind kind() const {
    return kind_;
  }

  bool hasEncoding() const {
    return encoding_ != nullptr;
  }

  /// Remaining encoded rows in the current stream segment.
  uint32_t remainingRows() const {
    NIMBLE_DCHECK_NOT_NULL(encoding_);
    return encoding_->rowCount() - readRows_;
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

  // Decode nullable encoded data, optionally using scatterOutputBitmap to map
  // encoded rows into sparse output positions.
  uint32_t decodeNullable(
      void* output,
      uint32_t offset,
      uint32_t readCount,
      uint32_t width,
      const std::function<void*()>& getOutputNulls,
      const velox::bits::Bitmap* scatterOutputBitmap);

  // Decode non-null encoded data into dense output positions.
  void decodeNonNull(
      void* output,
      uint32_t offset,
      uint32_t readCount,
      uint32_t width);

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
  // Externally-owned decompression buffer. Always non-null; owned by
  // DeserializerImpl or thrift Decoder to persist across StreamData lifetimes.
  velox::BufferPtr* const decompressionBuffer_{nullptr};

  const char* pos_{nullptr};
  const char* end_{nullptr};
  std::unique_ptr<Encoding> encoding_;
  // Track consumed rows for nimble encoding path.
  uint32_t readRows_{0};
  // External storage for string buffers from encoding. Owned by the caller
  // (DeserializerImpl or thrift Decoder). Each buffer is allocated separately
  // to avoid pointer invalidation when the vector grows.
  std::vector<velox::BufferPtr>* stringBuffers_;
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

class StreamDataParser {
 public:
  StreamDataParser(
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
    if (isTabletVersion(version_)) {
      // kTablet: the trailer stores stream ids, per-stream size indices, and
      // unique sizes. Per-slot sizes and body offsets are reconstructed from
      // (sizeIndex, uniqueSizes); duplicate slots resolve to a single body
      // extent, so streams are addressed by the reconstructed offset rather
      // than walked sequentially.
      detail::readTrailerStreamMetadata(
          end_,
          streamIds_,
          streamOffsets_,
          streamSizes_,
          uniqueStreamSizesScratch_);
      const char* const bodyBase = pos_;
      const size_t numStreams = streamIds_.size();
      for (size_t entryIdx = 0; entryIdx < numStreams; ++entryIdx) {
        const uint32_t streamId = streamIds_[entryIdx];
        const uint32_t streamSize = streamSizes_[entryIdx];
        std::string_view streamData(
            bodyBase + streamOffsets_[entryIdx], streamSize);
        // kTablet stream data includes tablet chunk headers:
        // [chunkSize:u32][compressionType:1B][encoded_data...]. Strip headers
        // and decompress if needed before handing off.
        callback(streamId, stripChunkHeaders(streamData));
      }
      pos_ = end_; // Skip past trailer.
    } else if (nonLegacyFormat(version_)) {
      // kLegacyCompact/kLegacySerialization/kSerialization/kProjection:
      // sizes-only sparse trailer.
      // Each stream's body offset is the prefix sum of preceding sizes, so
      // streams are walked sequentially. Production blobs at kLegacyCompact are
      // decoded via the legacy reader (frozen snapshot of the pre-two-array
      // wire format); newer versions use the two-array sparse trailer reader.
      // Reusable member buffers keep this path alloc-free across blobs.
      if (usesLegacyTrailer(version_)) {
        legacy::readLegacyTrailerStreamMetadata(end_, streamIds_, streamSizes_);
      } else {
        detail::readTrailerStreamMetadata(end_, streamIds_, streamSizes_);
      }
      const size_t numStreams = streamIds_.size();
      for (size_t entryIdx = 0; entryIdx < numStreams; ++entryIdx) {
        const uint32_t streamId = streamIds_[entryIdx];
        const uint32_t streamSize = streamSizes_[entryIdx];
        // Writer invariant: the sparse trailer only encodes non-zero stream
        // slots (SerializerImpl.h writeTrailer skips streamSizes[i]==0).
        // Debug-only assert documents that invariant; release elides it.
        NIMBLE_DCHECK_GT(
            streamSize, 0, "Sparse trailer must not encode zero-sized stream");
        std::string_view streamData(pos_, streamSize);
        pos_ += streamSize;
        callback(streamId, streamData);
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

    NIMBLE_CHECK(
        pos_ == end_,
        "Unexpected trailing data: pos={} end={}",
        reinterpret_cast<uintptr_t>(pos_),
        reinterpret_cast<uintptr_t>(end_));
  }

  /// Returns the auto-detected serialization version.
  /// Only valid after initialize() has been called.
  SerializationVersion version() const {
    return version_;
  }

  /// Returns true when Row/FlatMap null streams contain real nulls, forcing
  /// this batch to be deserialized via the per-batch barrier path. Always
  /// false for versions without a flags byte. Only valid after initialize().
  bool requiresNullBarrier() const {
    return requiresNullBarrier_;
  }

  /// Returns true if nimble encoding is enabled based on the auto-detected
  /// version. Only valid after initialize() has been called.
  bool encodingEnabled() const {
    return nonLegacyFormat(version_);
  }

  /// Releases owned kTablet stream payload buffers after a decode run consumes
  /// the string_views returned by iterateStreams(). This does not reset the
  /// current initialized blob cursor/header because callers may initialize the
  /// next batch before flushing the previous run.
  void reset() {
    strippedStreamBuffers_.clear();
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
  // (zero-copy), or a view into strippedStreamBuffers_ for compressed/
  // multi-chunk streams. Retained buffers are not cleared by initialize()
  // because callers may append streams from multiple initialized batches
  // before decoding the stored views.
  std::string_view stripChunkHeaders(std::string_view streamData);

  // Slow path: decompress/concatenate all chunks into an owned buffer.
  std::string_view slowChunkHeaderStrip(const char* pos, const char* end);

  // Returns a zero-copy view if the stream is a single uncompressed chunk.
  // Returns std::nullopt if decompression or concatenation is needed.
  std::optional<std::string_view> tryFastChunkHeaderStrip(
      const char* pos,
      const char* end);

  // Returns the payload bytes needed after removing per-chunk headers and
  // expanding compressed chunks.
  size_t strippedStreamSize(const char* pos, const char* end);

  // Returns the uncompressed byte size for a single chunk payload.
  size_t decodedChunkSize(
      CompressionType compression,
      const char* data,
      uint32_t length);

  // Copies or decompresses one chunk into output and advances output past the
  // appended bytes.
  void appendChunkData(
      CompressionType compression,
      const char* data,
      uint32_t length,
      char*& output);

  const DeserializerOptions& options_;
  velox::memory::MemoryPool* const pool_;

  // Serialization version detected from data. If the data has a version
  // header, this is read from the first byte; otherwise defaults to kLegacy.
  // When options specify a version, the data version is validated against it.
  SerializationVersion version_{SerializationVersion::kLegacy};
  // True when Row/FlatMap null streams contain real nulls (read from the header
  // flags byte). Defaults false for versions without a flags byte.
  bool requiresNullBarrier_{false};
  // Per-request row range embedded in the kTablet header (post-rowCount,
  // before stream data). nullopt for non-kTablet formats or when the
  // producer did not embed a row range.
  std::optional<RowRange> rowRange_;
  const char* pos_{nullptr};
  const char* end_{nullptr};
  // Owns slow-stripped kTablet stream payloads returned as string_views.
  // A deserializer can append several batches before materializing the run,
  // so each slow-stripped stream gets a stable backing allocation.
  std::vector<velox::BufferPtr> strippedStreamBuffers_;
  // Reusable parallel buffers for the per-blob trailer. Refilled by
  // iterateStreams(): streamIds_ holds the ids of non-zero stream slots (sorted
  // ascending) and streamSizes_ their byte sizes. For kTablet,
  // streamOffsets_ additionally holds each slot's body offset; with the
  // kTablet dedup trailer those offsets and sizes are reconstructed from the
  // per-slot size indices and the unique-size table (duplicate slots resolve
  // to a shared offset). The sizes-only formats leave streamOffsets_ empty and
  // derive offsets by prefix-summing sizes. uniqueStreamSizesScratch_ is reused
  // across blobs for the unique stream table: decoded as sizes, then
  // transformed in place into unique-offset prefix sums to keep the hot path
  // alloc-free.
  std::vector<uint32_t> streamIds_;
  std::vector<uint32_t> streamOffsets_;
  std::vector<uint32_t> streamSizes_;
  std::vector<uint32_t> uniqueStreamSizesScratch_;
};

} // namespace facebook::nimble::serde
