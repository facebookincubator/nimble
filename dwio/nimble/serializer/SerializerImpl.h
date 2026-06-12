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
#include <bit>
#include <cstring>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/common/Zigzag.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/selection/EncodingSelectionPolicy.h"
#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/serializer/SerializationHeader.h"
#include "dwio/nimble/velox/RowRange.h"
#include "dwio/nimble/velox/StreamData.h"
#include "folly/io/Cursor.h"
#include "folly/io/IOBuf.h"
#include "velox/common/Casts.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble::serde {
namespace detail {

/// Returns a default encoding selection policy for the given data type.
/// Uses a static factory to avoid recreating the read factors vector on every
/// call.
FOLLY_ALWAYS_INLINE std::unique_ptr<EncodingSelectionPolicyBase>
createDefaultEncodingPolicy(DataType dataType) {
  static const ManualEncodingSelectionPolicyFactory factory{
      ManualEncodingSelectionPolicyFactory::defaultReadFactors(),
      /*compressionOptions=*/std::nullopt};
  return factory.createPolicy(dataType);
}

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

/// Encode typed values using a given encoding selection policy factory.
/// When encodingLayout is provided, replays the captured encoding with
/// compressionOptions. policyFactory is used as fallback for nested encodings
/// not captured in the layout tree. When encodingLayout is not provided, uses
/// policyFactory directly and compressionOptions is ignored.
template <typename T>
std::string_view encodeTyped(
    std::span<const T> values,
    nimble::Buffer& encodingBuffer,
    const EncodingSelectionPolicyFactory& policyFactory,
    const EncodingLayout* encodingLayout = nullptr,
    std::optional<CompressionOptions> compressionOptions = std::nullopt) {
  std::unique_ptr<EncodingSelectionPolicy<T>> typedPolicy;
  if (encodingLayout != nullptr) {
    // Replay the captured encoding layout. policyFactory is used as fallback
    // for nested encodings not in the layout tree.
    typedPolicy = std::make_unique<ReplayedEncodingSelectionPolicy<T>>(
        *encodingLayout, compressionOptions, policyFactory);
  } else {
    typedPolicy = velox::checkedPointerCast<EncodingSelectionPolicy<T>>(
        policyFactory(TypeTraits<T>::dataType));
  }
  return EncodingFactory::encode<T>(
      std::move(typedPolicy),
      values,
      encodingBuffer,
      Encoding::Options{.useVarintRowCount = true});
}

/// Reads the trailer size (u32) from the last 4 bytes of the buffer.
inline uint32_t readTrailerSize(const char* end) {
  const char* pos = end - sizeof(uint32_t);
  return encoding::readUint32(pos);
}

/// Returns an upper-bound estimate of the trailer size for the two-array
/// sparse layout. Conservatively assumes every stream slot is non-zero
/// (worst case for sparseness) and that sizes can be up to UINT32_MAX.
size_t estimateTrailerSize(
    size_t numStreams,
    EncodingType indicesEncodingType,
    EncodingType sizesEncodingType);

/// Overload that accepts SerializationVersion for API compatibility.
inline size_t estimateTrailerSize(
    SerializationVersion /* outputVersion */,
    size_t numStreams,
    std::optional<EncodingType> indicesEncodingType = std::nullopt,
    std::optional<EncodingType> sizesEncodingType = std::nullopt) {
  return estimateTrailerSize(
      numStreams,
      indicesEncodingType.value_or(EncodingType::FixedBitWidth),
      sizesEncodingType.value_or(EncodingType::FixedBitWidth));
}

/// Writes the Trivial section payload: count varint + raw u32 array.
/// Wire: [count:varint][v_0:u32]...[v_N:u32]
template <typename T>
void writeTrivialSection(const std::vector<uint32_t>& values, T& buffer) {
  const auto count = static_cast<uint32_t>(values.size());
  const auto countVarintSize = varint::varintSize(count);
  const uint32_t payloadBytes = count * sizeof(uint32_t);
  auto* pos = extend(buffer, countVarintSize + payloadBytes);
  varint::writeVarint(count, &pos);
  if (payloadBytes > 0) {
    std::memcpy(pos, values.data(), payloadBytes);
  }
}

/// Writes the Varint section payload: count varint + each value as varint.
/// Wire: [count:varint][v_0:varint]...[v_N:varint]
template <typename T>
void writeVarintSection(const std::vector<uint32_t>& values, T& buffer) {
  const auto count = static_cast<uint32_t>(values.size());
  const auto countVarintSize = varint::varintSize(count);
  const auto dataVarintSize =
      static_cast<uint32_t>(varint::bulkVarintSize32(values));
  auto* pos = extend(buffer, countVarintSize + dataVarintSize);
  varint::writeVarint(count, &pos);
  for (const auto v : values) {
    varint::writeVarint(v, &pos);
  }
}

/// Writes the Delta section payload: count varint + first value + per-element
/// deltas. Wire: [count:varint][first:varint][delta_1:varint]...
template <typename T>
void writeDeltaSection(const std::vector<uint32_t>& values, T& buffer) {
  const auto count = static_cast<uint32_t>(values.size());
  const auto countVarintSize = varint::varintSize(count);
  uint32_t dataVarintSize = 0;
  if (count > 0) {
    dataVarintSize = varint::varintSize(values[0]);
    for (uint32_t i = 1; i < count; ++i) {
      const auto delta = values[i] - values[i - 1];
      dataVarintSize += varint::varintSize(delta);
    }
  }
  auto* pos = extend(buffer, countVarintSize + dataVarintSize);
  varint::writeVarint(count, &pos);
  if (count > 0) {
    varint::writeVarint(values[0], &pos);
    for (uint32_t i = 1; i < count; ++i) {
      const auto delta = values[i] - values[i - 1];
      varint::writeVarint(delta, &pos);
    }
  }
}

/// Writes the FixedBitWidth section payload: bitWidth + count + bit-packed.
/// Wire: [bitWidth:1B][count:varint][bit-packed data]
template <typename T>
void writeFixedBitWidthSection(const std::vector<uint32_t>& values, T& buffer) {
  const auto count = static_cast<uint32_t>(values.size());
  uint32_t maxVal = 0;
  for (const auto v : values) {
    maxVal = std::max(maxVal, v);
  }
  const uint8_t bitWidth =
      maxVal == 0 ? 0 : static_cast<uint8_t>(std::bit_width(maxVal));
  const uint32_t packedBytes = (bitWidth > 0 && count > 0)
      ? static_cast<uint32_t>(FixedBitArray::bufferSize(count, bitWidth))
      : 0;
  const auto countVarintSize = varint::varintSize(count);
  auto* pos = extend(buffer, sizeof(uint8_t) + countVarintSize + packedBytes);
  *pos++ = static_cast<char>(bitWidth);
  varint::writeVarint(count, &pos);
  if (bitWidth > 0 && count > 0) {
    std::memset(pos, 0, packedBytes);
    FixedBitArray arr{pos, static_cast<int>(bitWidth)};
    arr.bulkSet32(0, count, values.data());
  }
}

/// Dispatches a section write to the encoding-specific writer.
template <typename T>
void writeSection(
    EncodingType encodingType,
    const std::vector<uint32_t>& values,
    T& buffer) {
  switch (getTrailerEncodingType(encodingType)) {
    case EncodingType::Trivial:
      writeTrivialSection(values, buffer);
      break;
    case EncodingType::Varint:
      writeVarintSection(values, buffer);
      break;
    case EncodingType::Delta:
      writeDeltaSection(values, buffer);
      break;
    case EncodingType::FixedBitWidth:
      writeFixedBitWidthSection(values, buffer);
      break;
    default:
      NIMBLE_FAIL(
          "Unsupported EncodingType for stream sizes trailer section: {}",
          encodingType);
  }
}

/// Writes the two-array sparse stream-sizes trailer. Walks
/// `denseStreamSizes` once to collect the stream IDs of non-zero entries and
/// their sizes, then encodes each section with the caller-specified encoding
/// type.
/// Wire: [indicesEncType:1B][indicesPayload]
///       [sizesEncType:1B][sizesPayload][trailer_size:u32]
template <typename T>
void writeTrailer(
    const std::vector<uint32_t>& denseStreamSizes,
    EncodingType indicesEncodingType,
    EncodingType sizesEncodingType,
    T& buffer) {
  const auto streamCount = denseStreamSizes.size();
  std::vector<uint32_t> streamIds;
  std::vector<uint32_t> streamSizes;
  streamIds.reserve(streamCount);
  streamSizes.reserve(streamCount);
  for (uint32_t i = 0; i < streamCount; ++i) {
    if (denseStreamSizes[i] != 0) {
      streamIds.emplace_back(i);
      streamSizes.emplace_back(denseStreamSizes[i]);
    }
  }

  const auto trailerStartOffset = buffer.size();
  auto* indicesTypePos = extend(buffer, sizeof(uint8_t));
  *indicesTypePos = static_cast<char>(indicesEncodingType);
  writeSection(indicesEncodingType, streamIds, buffer);

  auto* sizesTypePos = extend(buffer, sizeof(uint8_t));
  *sizesTypePos = static_cast<char>(sizesEncodingType);
  writeSection(sizesEncodingType, streamSizes, buffer);

  const uint32_t trailerSize =
      static_cast<uint32_t>(buffer.size() - trailerStartOffset);
  auto* sizePos = extend(buffer, sizeof(uint32_t));
  encoding::writeUint32(trailerSize, sizePos);
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

/// Reads the two-array sparse stream-sizes trailer from the end of a
/// contiguous buffer. Fills `streamIndices` (offsets of non-zero stream
/// slots, sorted ascending) and `streamSizes` (their byte sizes), parallel
/// arrays of identical length. Both vectors are reusable buffers owned by
/// the caller (e.g. members on `StreamDataReader`) to keep the per-blob hot
/// path alloc-free across invocations.
void readTrailerStreamMetadata(
    const char* end,
    std::vector<uint32_t>& streamIndices,
    std::vector<uint32_t>& streamSizes);

/// Value-returning convenience overload for cold-path consumers
/// (tests, dump tools). Returns parallel (streamIndices, streamSizes) arrays.
std::pair<std::vector<uint32_t>, std::vector<uint32_t>>
readTrailerStreamMetadata(const char* end);

/// IOBuf overload: reads the trailer from a (possibly chained) IOBuf.
/// Tries the fast path first: if the tail segment contains the entire
/// trailer, delegates to the contiguous overload. Falls back to
/// cursor + pull() when the trailer spans a chain boundary.
std::pair<std::vector<uint32_t>, std::vector<uint32_t>>
readTrailerStreamMetadata(const folly::IOBuf& input);

/// Parses all streams from a serialized buffer.
/// Returns a vector of stream data indexed by their original offset.
///
/// For kLegacy: Returns streams in order with inline u32 sizes.
/// For kLegacyCompact: Returns streams indexed by their offset from sizes
/// header.
///
/// @param pos Pointer past the header (version + rowCount already read)
/// @param end End of buffer
/// @param version Serialization format version
/// @param pool Memory pool for decoding nimble-encoded sizes.
/// @return Vector of stream data (may have gaps for kLegacy format)
std::vector<std::string_view> parseStreams(
    const char* pos,
    const char* end,
    SerializationVersion version,
    velox::memory::MemoryPool* pool);

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
      encodingLayout,
      options.compressionOptions);
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
  /// Constructor. For kLegacy, writes the header immediately. For
  /// kLegacyCompact, writes the header prefix (version + rowCount).
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
  /// For kLegacyCompact, also tracks stream sizes for the trailer.
  void writeData(const nimble::StreamData& streamData);

  /// Close the writer. For kLegacy, fills trailing zeros up to nodeCount.
  /// For kLegacyCompact, writes the trailer (encoded sizes).
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
  // Dense stream sizes. streamSizes_[i] = byte size of stream i (0 for
  // missing/empty).
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
  writeSerializationHeader(buffer_, version, rowCount);
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

    // Track size for trailer.
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
    // Legacy scalar encoding:
    //   Zstd: [size:u32][compType:i8][data...]
    //   LZ4:  [size:u32][compType:i8][origSize:u32][data...]
    const auto bufferStart = buffer_.size();
    const uint32_t maxSize = data.size() + 2 * sizeof(uint32_t) + 1;
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
        streamSizes_,
        options_.streamIndicesEncodingType,
        options_.streamSizesEncodingType,
        buffer_);
  } else {
    // kLegacy: fill trailing zeros up to nodeCount.
    detail::writeMissingStreams(buffer_, lastStream_, nodeCount);
  }
}

} // namespace facebook::nimble::serde
