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

#include <algorithm>
#include <optional>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/velox/StreamData.h"
#include "folly/io/Cursor.h"
#include "folly/io/IOBuf.h"
#include "velox/common/Casts.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble::serde {
namespace detail {

/// Get total size of a string field
uint32_t getStringsTotalSize(std::string_view input);

/// Encode a string field with supplied total size
void encodeStrings(std::string_view input, uint32_t size, char* output);

/// Encode non-string field
uint32_t
encode(const SerializerOptions& options, std::string_view input, char* output);

/// Write zeros for missing streams in kLegacy format.
/// Each missing stream is a zero-length stream (size=0, u32 = 4 bytes).
template <typename T>
void writeMissingStreams(T& buffer, uint32_t lastStream, uint32_t nextStream) {
  NIMBLE_CHECK_LE(lastStream + 1, nextStream, "unexpected stream offset");
  const auto missingStreamCount = nextStream - lastStream - 1;
  if (missingStreamCount > 0) {
    const auto oldByteSize = buffer.size();
    buffer.resize(oldByteSize + missingStreamCount * sizeof(uint32_t));
    auto begin = reinterpret_cast<uint32_t*>(buffer.data() + oldByteSize);
    std::fill(begin, begin + missingStreamCount, 0);
  }
}

template <typename T>
char* extend(T& buffer, uint32_t size) {
  const auto oldSize = buffer.size();
  buffer.resize(oldSize + size);
  return buffer.data() + oldSize;
}

/// Encode typed values using a given encoding selection policy factory.
/// When encodingLayout is provided, replays the captured encoding.
/// Otherwise, uses the policy factory to select encoding.
template <typename T>
std::string_view encodeTyped(
    std::span<const T> values,
    nimble::Buffer& encodingBuffer,
    const EncodingSelectionPolicyFactory& policyFactory,
    const CompressionOptions& compressionOptions = {},
    const EncodingLayout* encodingLayout = nullptr) {
  std::unique_ptr<EncodingSelectionPolicy<T>> typedPolicy;
  if (encodingLayout != nullptr) {
    typedPolicy = std::make_unique<ReplayedEncodingSelectionPolicy<T>>(
        *encodingLayout, compressionOptions, policyFactory);
  } else {
    auto policy = policyFactory(TypeTraits<T>::dataType);
    auto* rawTypedPolicy =
        dynamic_cast<EncodingSelectionPolicy<T>*>(policy.release());
    NIMBLE_CHECK_NOT_NULL(
        rawTypedPolicy,
        "Policy type mismatch for {}",
        toString(TypeTraits<T>::dataType));
    typedPolicy.reset(rawTypedPolicy);
  }
  return EncodingFactory::encode<T>(
      std::move(typedPolicy),
      values,
      encodingBuffer,
      Encoding::Options{.useVarintRowCount = true});
}

/// Reads the trailer size (u32) from the last 4 bytes of the buffer.
/// For kCompact: the encoded stream sizes byte count.
/// For kCompactRaw: the raw trailer byte count (including encodingType).
inline uint32_t readTrailerSize(const char* end) {
  const char* pos = end - sizeof(uint32_t);
  return encoding::readUint32(pos);
}

/// Returns the byte size of the version header (0 or 1).
inline size_t versionSize(std::optional<SerializationVersion> version) {
  return version.has_value() ? sizeof(uint8_t) : 0;
}

/// Returns the byte size of the row count encoding for the given version.
inline size_t rowCountSize(
    std::optional<SerializationVersion> version,
    uint32_t rowCount) {
  return isCompactFormat(version) ? varint::varintSize(rowCount)
                                  : sizeof(uint32_t);
}

/// Returns the exact byte size of the serialization header.
/// Use to pre-allocate buffers before calling writeHeader().
inline size_t estimateHeaderSize(
    std::optional<SerializationVersion> version,
    uint32_t rowCount) {
  return versionSize(version) + rowCountSize(version, rowCount);
}

/// Writes serialization header to buffer.
/// Works with any buffer type that has size(), resize(), and data() methods.
///
/// kLegacy wire format:
///   [version:1B][rowCount:u32][size_0:u32][stream_data_0]...[size_N:u32][stream_data_N]
///
/// kCompact wire format:
///   [version:1B][rowCount:varint][stream_data_0][stream_data_1]...[encoded_stream_sizes][stream_sizes_encoded_size:u32]
///
/// For kLegacy: writes [optional_version:1B][rowCount:u32]
/// For kCompact: writes [version:1B][rowCount:varint]
///
/// @param buffer Output buffer (std::string, velox::Buffer, etc.)
/// @param version Serialization format version (nullopt = no version byte)
/// @param rowCount Number of rows
template <typename T>
void writeHeader(
    T& buffer,
    std::optional<SerializationVersion> version,
    uint32_t rowCount) {
  // Write version byte if provided.
  if (version.has_value()) {
    auto* versionPos = extend(buffer, 1);
    *versionPos = static_cast<char>(version.value());
  }

  if (isCompactFormat(version)) {
    auto* rowCountPos = extend(buffer, varint::varintSize(rowCount));
    varint::writeVarint(rowCount, &rowCountPos);
  } else {
    auto* rowCountPos = extend(buffer, sizeof(uint32_t));
    encoding::writeUint32(rowCount, rowCountPos);
  }
}

/// Returns an upper-bound estimate of the kCompact trailer size.
/// The actual size depends on the encoding selected for stream sizes, so
/// this is a conservative estimate (trivial encoding = raw uint32 values +
/// encoding header overhead + trailing u32).
///
/// @param numStreams Number of streams (size of the streamSizes array).
inline size_t estimateCompactTrailerSize(size_t numStreams) {
  // Encoding header overhead (encoding type, row count varint, nested headers).
  constexpr size_t kEncodingHeaderOverhead = 32;
  // Trivial encoding: raw uint32 values.
  // Trailing u32: stores the encoded sizes byte count.
  return numStreams * sizeof(uint32_t) + kEncodingHeaderOverhead +
      sizeof(uint32_t);
}

/// Returns the exact byte size of the kCompactRaw trailer.
inline size_t estimateRawTrailerSize(
    size_t numStreams,
    std::optional<EncodingType> encodingType = std::nullopt) {
  const auto resolvedType = getRawEncodingType(encodingType);
  // [encodingType:1B][payload][trailer_size:u32]
  size_t payloadSize{0};
  switch (resolvedType) {
    case EncodingType::Trivial:
      payloadSize = numStreams * sizeof(uint32_t);
      break;
    case EncodingType::Varint:
      // Upper bound: count varint + N varints (max 5 bytes each).
      payloadSize = 5 + numStreams * 5;
      break;
    default:
      NIMBLE_FAIL("Unsupported EncodingType for kCompactRaw: {}", resolvedType);
  }
  return sizeof(uint8_t) + payloadSize + sizeof(uint32_t);
}

/// Returns an upper-bound estimate of the trailer size for the given
/// serialization version (kCompact or kCompactRaw).
inline size_t estimateTrailerSize(
    SerializationVersion outputVersion,
    size_t numStreams,
    std::optional<EncodingType> encodingType = std::nullopt) {
  if (outputVersion == SerializationVersion::kCompactRaw) {
    return estimateRawTrailerSize(numStreams, encodingType);
  }
  return estimateCompactTrailerSize(numStreams);
}

/// Writes the kCompactRaw trailer: appends
/// [encodingType:1B][payload][trailer_size:u32] to buffer.
template <typename T>
void writeRawTrailer(
    const std::vector<uint32_t>& streamSizes,
    std::optional<EncodingType> encodingType,
    T& buffer) {
  const auto resolvedType = getRawEncodingType(encodingType);
  const auto streamCount = streamSizes.size();

  switch (resolvedType) {
    case EncodingType::Trivial: {
      const uint32_t payloadSize = streamCount * sizeof(uint32_t);
      const uint32_t trailerSize = sizeof(uint8_t) + payloadSize;
      // [encodingType:1B][size_0:u32]...[size_N:u32][trailer_size:u32]
      auto* pos = extend(buffer, trailerSize + sizeof(uint32_t));
      *pos++ = static_cast<char>(resolvedType);
      if (payloadSize > 0) {
        std::memcpy(pos, streamSizes.data(), payloadSize);
        pos += payloadSize;
      }
      encoding::writeUint32(trailerSize, pos);
      break;
    }
    case EncodingType::Varint: {
      const auto countVarintSize =
          varint::varintSize(static_cast<uint32_t>(streamCount));
      const auto dataVarintSize = varint::bulkVarintSize32(streamSizes);
      const uint32_t trailerSize =
          sizeof(uint8_t) + countVarintSize + dataVarintSize;
      auto* pos = extend(buffer, trailerSize + sizeof(uint32_t));
      *pos++ = static_cast<char>(resolvedType);
      varint::writeVarint(static_cast<uint32_t>(streamCount), &pos);
      for (const auto size : streamSizes) {
        varint::writeVarint(size, &pos);
      }
      encoding::writeUint32(trailerSize, pos);
      break;
    }
    default:
      NIMBLE_FAIL("Unsupported EncodingType for kCompactRaw: {}", resolvedType);
  }
}

/// Writes the kCompact trailer: appends
/// [encoded_stream_sizes][stream_sizes_encoded_size:u32] to buffer.
///
/// @param streamSizes Dense stream sizes array. sizes[i] = byte size of
///        stream i (0 for missing).
/// @param encodingType Optional encoding type override.
/// @param encodingBuffer Encoding buffer for nimble encoding output.
///        Caller must call reset() before each call if reusing.
/// @param buffer Output buffer.
/// @param encodingLayout Optional encoding layout for replaying captured
///        encoding. Skips encoding selection and statistics computation.
template <typename T>
void writeCompactTrailer(
    const std::vector<uint32_t>& streamSizes,
    std::optional<EncodingType> encodingType,
    nimble::Buffer& encodingBuffer,
    T& buffer,
    const EncodingLayout* encodingLayout = nullptr) {
  encodingBuffer.reset();
  auto factory = encodingType.has_value()
      ? ManualEncodingSelectionPolicyFactory({{*encodingType, 1.0}})
      : ManualEncodingSelectionPolicyFactory();
  auto encodedStreamSizes = encodeTyped<uint32_t>(
      streamSizes,
      encodingBuffer,
      [&factory](DataType dataType) { return factory.createPolicy(dataType); },
      /*compressionOptions=*/{},
      encodingLayout);
  const uint32_t encodedSize = encodedStreamSizes.size();
  auto* encodedStreamSizesPos = extend(buffer, encodedSize);
  std::memcpy(encodedStreamSizesPos, encodedStreamSizes.data(), encodedSize);

  auto* encodedSizePos = extend(buffer, sizeof(uint32_t));
  encoding::writeUint32(encodedSize, encodedSizePos);
}

/// Writes the stream sizes trailer for the given serialization version.
/// Dispatches to writeRawTrailer (kCompactRaw) or writeTrailer
/// (kCompact) based on outputVersion.
///
/// @param outputVersion Must be kCompact or kCompactRaw.
/// @param streamSizes Dense stream sizes array.
/// @param encodingType Optional encoding type override.
/// @param encodingBuffer Encoding buffer for kCompact nimble encoding.
/// @param buffer Output buffer.
/// @param encodingLayout Optional encoding layout for replaying captured
///        encoding. Only used for kCompact. Ignored for kCompactRaw.
template <typename T>
void writeTrailer(
    SerializationVersion outputVersion,
    const std::vector<uint32_t>& streamSizes,
    std::optional<EncodingType> encodingType,
    nimble::Buffer& encodingBuffer,
    T& buffer,
    const EncodingLayout* encodingLayout = nullptr) {
  if (outputVersion == SerializationVersion::kCompactRaw) {
    writeRawTrailer(streamSizes, encodingType, buffer);
    return;
  }

  NIMBLE_CHECK_EQ(
      outputVersion,
      SerializationVersion::kCompact,
      "writeTrailer requires kCompact or kCompactRaw, got {}",
      outputVersion);
  writeCompactTrailer(
      streamSizes, encodingType, encodingBuffer, buffer, encodingLayout);
}

/// Writes a single stream to the buffer.
/// Writes [size][data...] where size is varint (useVarint=true) or u32
/// (useVarint=false).
///
/// @tparam useVarint True for varint size prefix, false for u32
/// @param streamData The stream data to write
/// @param buffer Output buffer (std::string, velox::Buffer, etc.)
template <bool useVarint, typename T>
void writeStream(std::string_view streamData, T& buffer) {
  const uint32_t dataSize = streamData.size();
  uint32_t prefixSize;
  if constexpr (useVarint) {
    prefixSize = varint::varintSize(dataSize);
  } else {
    prefixSize = sizeof(uint32_t);
  }
  auto* pos = extend(buffer, prefixSize + dataSize);
  if constexpr (useVarint) {
    varint::writeVarint(dataSize, &pos);
  } else {
    encoding::writeUint32(dataSize, pos);
  }
  if (dataSize > 0) {
    std::memcpy(pos, streamData.data(), dataSize);
  }
}

/// Reads a single stream from the buffer.
/// Reads [size][data...] where size is varint (useVarint=true) or u32
/// (useVarint=false). Advances pos past the stream.
///
/// @tparam useVarint True for varint size prefix, false for u32
/// @param pos Pointer to current position (updated after read)
/// @return View of the stream data
template <bool useVarint>
std::string_view readStream(const char*& pos) {
  uint32_t size;
  if constexpr (useVarint) {
    size = varint::readVarint32(&pos);
  } else {
    size = encoding::readUint32(pos);
  }
  std::string_view data(pos, size);
  pos += size;
  return data;
}

/// Skips a single stream in the buffer without reading its data.
/// Advances pos past [size][data...].
template <bool useVarint>
void skipStream(const char*& pos) {
  uint32_t size;
  if constexpr (useVarint) {
    size = varint::readVarint32(&pos);
  } else {
    size = encoding::readUint32(pos);
  }
  pos += size;
}

/// Reads stream sizes from the trailer for kCompact or kCompactRaw format.
/// The last 4 bytes store the trailer size. For kCompact, the trailer is
/// nimble-encoded. For kCompactRaw, it starts with an EncodingType byte.
std::vector<uint32_t> readStreamSizes(
    const char* end,
    SerializationVersion version,
    velox::memory::MemoryPool* pool);

/// IOBuf overload: reads stream sizes from the trailer of a (possibly chained)
/// IOBuf. Tries the fast path first: if the tail segment contains the entire
/// trailer, delegates to the contiguous readStreamSizes. Falls back to
/// cursor + pull() when the trailer spans a chain boundary.
std::vector<uint32_t> readStreamSizes(
    const folly::IOBuf& input,
    SerializationVersion version,
    velox::memory::MemoryPool* pool);

/// Parses all streams from a serialized buffer.
/// Returns a vector of stream data indexed by their original offset.
///
/// For kLegacy: Returns streams in order with inline u32 sizes.
/// For kCompact: Returns streams indexed by their offset from sizes header.
///
/// @param pos Pointer past the header (version + rowCount already read)
/// @param end End of buffer
/// @param version Serialization format version
/// @param pool Memory pool for decoding nimble-encoded sizes.
/// @return Vector of stream data (may have gaps for kLegacy format)
inline std::vector<std::string_view> parseStreams(
    const char* pos,
    const char* end,
    SerializationVersion version,
    velox::memory::MemoryPool* pool) {
  NIMBLE_CHECK_NOT_NULL(pool, "Memory pool cannot be null");

  std::vector<std::string_view> streams;

  if (isCompactFormat(version)) {
    auto streamSizes = readStreamSizes(end, version, pool);
    streams.resize(streamSizes.size());

    for (uint32_t i = 0; i < streamSizes.size(); ++i) {
      streams[i] = std::string_view(pos, streamSizes[i]);
      pos += streamSizes[i];
    }
  } else {
    NIMBLE_CHECK_EQ(
        version,
        SerializationVersion::kLegacy,
        "unexpected version {}",
        version);
    // kLegacy format: inline [size:u32][data]...
    while (pos < end) {
      streams.emplace_back(readStream<false>(pos));
    }
  }

  return streams;
}

/// Encode scalar data using nimble encoding framework with serializer options.
template <typename T, typename Buffer>
std::string_view encodeTyped(
    const SerializerOptions& options,
    std::string_view data,
    velox::memory::MemoryPool& pool,
    nimble::Buffer& encodingBuffer,
    const EncodingLayout* encodingLayout) {
  const auto count = data.size() / sizeof(T);
  std::span<const T> values{reinterpret_cast<const T*>(data.data()), count};
  return encodeTyped<T>(
      values,
      encodingBuffer,
      options.encodingSelectionPolicyFactory,
      options.compressionOptions,
      encodingLayout);
}

/// Dispatch to typed nimble encoding based on ScalarKind.
template <typename Buffer>
std::string_view encodeScalar(
    const SerializerOptions& options,
    ScalarKind scalarKind,
    std::string_view data,
    velox::memory::MemoryPool& pool,
    nimble::Buffer& encodingBuffer,
    const EncodingLayout* encodingLayout) {
  switch (scalarKind) {
    case ScalarKind::Bool:
      return encodeTyped<bool, Buffer>(
          options, data, pool, encodingBuffer, encodingLayout);
    case ScalarKind::Int8:
      return encodeTyped<int8_t, Buffer>(
          options, data, pool, encodingBuffer, encodingLayout);
    case ScalarKind::UInt8:
      return encodeTyped<uint8_t, Buffer>(
          options, data, pool, encodingBuffer, encodingLayout);
    case ScalarKind::Int16:
      return encodeTyped<int16_t, Buffer>(
          options, data, pool, encodingBuffer, encodingLayout);
    case ScalarKind::UInt16:
      return encodeTyped<uint16_t, Buffer>(
          options, data, pool, encodingBuffer, encodingLayout);
    case ScalarKind::Int32:
      return encodeTyped<int32_t, Buffer>(
          options, data, pool, encodingBuffer, encodingLayout);
    case ScalarKind::UInt32:
      return encodeTyped<uint32_t, Buffer>(
          options, data, pool, encodingBuffer, encodingLayout);
    case ScalarKind::Int64:
      return encodeTyped<int64_t, Buffer>(
          options, data, pool, encodingBuffer, encodingLayout);
    case ScalarKind::UInt64:
      return encodeTyped<uint64_t, Buffer>(
          options, data, pool, encodingBuffer, encodingLayout);
    case ScalarKind::Float:
      return encodeTyped<float, Buffer>(
          options, data, pool, encodingBuffer, encodingLayout);
    case ScalarKind::Double:
      return encodeTyped<double, Buffer>(
          options, data, pool, encodingBuffer, encodingLayout);
    case ScalarKind::String:
    case ScalarKind::Binary:
      return encodeTyped<std::string_view, Buffer>(
          options, data, pool, encodingBuffer, encodingLayout);
    default:
      NIMBLE_UNSUPPORTED(
          "Unsupported scalar kind for nimble encoding: {}",
          toString(scalarKind));
  }
}
} // namespace detail

template <typename T>
class StreamDataWriter {
 public:
  /// Constructor. For kLegacy, writes the header immediately. For kCompact,
  /// writes the header prefix (version + rowCount).
  ///
  /// @param pool Memory pool for encoding buffer allocation.
  /// @param streamEncodingLayouts Optional encoding layouts for replaying
  ///        captured encodings. When provided, looks up EncodingLayout by
  ///        stream offset and uses ReplayedEncodingSelectionPolicy.
  StreamDataWriter(
      const SerializerOptions& options,
      T& buffer,
      uint32_t rowCount,
      velox::memory::MemoryPool* pool,
      const std::unordered_map<uint32_t, const EncodingLayout*>*
          streamEncodingLayouts);

  /// Write encoded data for a single stream.
  /// For both formats, writes directly to buffer.
  /// For kCompact, also tracks stream sizes for the trailer.
  void writeData(const nimble::StreamData& streamData);

  /// Close the writer. For kLegacy, fills trailing zeros up to nodeCount.
  /// For kCompact, writes the trailer (encoded sizes).
  void close(uint32_t nodeCount = 0);

 private:
  void encodeStream(
      ScalarKind scalarKind,
      std::string_view data,
      uint32_t streamOffset);

  // --- Const members ---
  const SerializerOptions& options_;
  // Memory pool for encoding buffer allocation.
  velox::memory::MemoryPool* const pool_;
  // Buffer for nimble encoding output.
  const std::unique_ptr<nimble::Buffer> encodingBuffer_;
  // Optional map from stream offset to encoding layout for replaying captured
  // encodings. Only set when options_.encodingLayoutTree is specified.
  const std::unordered_map<uint32_t, const EncodingLayout*>* const
      streamEncodingLayouts_;

  // --- Mutable members ---
  T& buffer_;
  // Track last stream offset for kLegacy format zero-filling.
  uint32_t lastStream_{0xffffffff};
  // Dense stream sizes for kCompact trailer. streamSizes_[i] = byte size of
  // stream i (0 for missing/empty).
  std::vector<uint32_t> streamSizes_;
};

template <typename T>
StreamDataWriter<T>::StreamDataWriter(
    const SerializerOptions& options,
    T& buffer,
    uint32_t rowCount,
    velox::memory::MemoryPool* pool,
    const std::unordered_map<uint32_t, const EncodingLayout*>*
        streamEncodingLayouts)
    : options_{options},
      pool_{pool},
      encodingBuffer_{
          options.enableEncoding() ? std::make_unique<nimble::Buffer>(*pool)
                                   : nullptr},
      streamEncodingLayouts_{streamEncodingLayouts},
      buffer_{buffer} {
  NIMBLE_CHECK(
      streamEncodingLayouts_ == nullptr || options_.enableEncoding(),
      "streamEncodingLayouts can only be set when encoding is enabled");
  NIMBLE_CHECK_NOT_NULL(pool, "Memory pool cannot be null");

  std::optional<SerializationVersion> version;
  if (options_.hasVersionHeader()) {
    version = options_.serializationVersion();
  }
  detail::writeHeader(buffer_, version, rowCount);
}

template <typename T>
void StreamDataWriter<T>::writeData(const nimble::StreamData& streamData) {
  const auto nonNulls = streamData.nonNulls();
  const auto data = streamData.data();

  if (data.empty() && nonNulls.empty()) {
    return;
  }
  NIMBLE_CHECK(
      nonNulls.empty() ||
          std::all_of(
              nonNulls.begin(),
              nonNulls.end(),
              [](bool notNull) { return notNull; }),
      "nulls not supported");

  const auto scalarKind = streamData.descriptor().scalarKind();
  const auto streamOffset = streamData.descriptor().offset();

  if (options_.enableEncoding()) {
    // kCompact: encode stream and buffer the result.
    encodeStream(scalarKind, data, streamOffset);
    return;
  }

  // kLegacy: fill zeros for missing streams before writing.
  NIMBLE_CHECK_LE(lastStream_ + 1, streamOffset, "unexpected stream offset");
  detail::writeMissingStreams(buffer_, lastStream_, streamOffset);
  lastStream_ = streamOffset;

  encodeStream(scalarKind, data, streamOffset);
}

template <typename T>
void StreamDataWriter<T>::encodeStream(
    ScalarKind scalarKind,
    std::string_view data,
    uint32_t streamOffset) {
  if (options_.enableEncoding()) {
    // Look up encoding layout for this stream if available.
    const EncodingLayout* encodingLayout = nullptr;
    if (streamEncodingLayouts_ != nullptr) {
      auto it = streamEncodingLayouts_->find(streamOffset);
      if (it != streamEncodingLayouts_->end()) {
        encodingLayout = it->second;
      }
    }

    // Use nimble encoding framework for optimal compression.
    auto encoded = detail::encodeScalar<T>(
        options_, scalarKind, data, *pool_, *encodingBuffer_, encodingLayout);

    // kCompact: write directly to buffer and track size for trailer.
    if (streamOffset >= streamSizes_.size()) {
      streamSizes_.resize(streamOffset + 1, 0);
    }
    streamSizes_[streamOffset] = static_cast<uint32_t>(encoded.size());
    auto* pos = detail::extend(buffer_, encoded.size());
    std::memcpy(pos, encoded.data(), encoded.size());
  } else if (
      scalarKind == ScalarKind::String || scalarKind == ScalarKind::Binary) {
    // Legacy string encoding: [total_size:u32][len_0:u32][data_0]...
    const auto size = detail::getStringsTotalSize(data);
    auto* pos = detail::extend(buffer_, size + sizeof(uint32_t));
    detail::encodeStrings(data, size, pos);
  } else {
    // Legacy scalar encoding: [size:u32][compression_type:i8][data...]
    const auto bufferStart = buffer_.size();
    const uint32_t maxSize = data.size() + sizeof(uint32_t) + 1;
    auto* pos = detail::extend(buffer_, maxSize);
    const auto encodedSize = detail::encode(options_, data, pos);
    if (encodedSize < maxSize) {
      buffer_.resize(bufferStart + encodedSize);
    }
  }
}

template <typename T>
void StreamDataWriter<T>::close(uint32_t nodeCount) {
  if (options_.enableEncoding()) {
    detail::writeTrailer(
        options_.serializationVersion(),
        streamSizes_,
        options_.streamSizesEncodingType,
        *encodingBuffer_,
        buffer_);
  } else {
    // kLegacy: fill trailing zeros up to nodeCount.
    detail::writeMissingStreams(buffer_, lastStream_, nodeCount);
  }
}

} // namespace facebook::nimble::serde
