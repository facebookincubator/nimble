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

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/velox/StreamData.h"
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

/// Write zeros for missing streams in dense format (version 0).
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

/// Writes serialization header to buffer.
/// Works with any buffer type that has size(), resize(), and data() methods.
///
/// @param buffer Output buffer (std::string, velox::Buffer, etc.)
/// @param version Serialization format version (nullopt = no version byte)
/// @param rowCount Number of rows
/// @param streamOffsets Stream offsets for sparse format (empty for dense)
template <typename T>
void writeHeader(
    T& buffer,
    std::optional<SerializationVersion> version,
    uint32_t rowCount,
    const std::vector<uint32_t>& streamOffsets) {
  // Write version byte if provided.
  bool sparseFormat = false;
  if (version.has_value()) {
    auto* versionPos = extend(buffer, 1);
    *versionPos = static_cast<char>(version.value());
    sparseFormat =
        (version.value() == SerializationVersion::kSparse ||
         version.value() == SerializationVersion::kSparseEncoded);
  }

  // Write row count.
  auto* rowCountPos = extend(buffer, sizeof(uint32_t));
  encoding::writeUint32(rowCount, rowCountPos);

  // Sparse format: write stream count and offsets.
  if (sparseFormat) {
    auto* streamCountPos = extend(buffer, sizeof(uint32_t));
    encoding::writeUint32(
        static_cast<uint32_t>(streamOffsets.size()), streamCountPos);

    for (uint32_t offset : streamOffsets) {
      auto* offsetPos = extend(buffer, sizeof(uint32_t));
      encoding::writeUint32(offset, offsetPos);
    }
  }
}

/// Writes a single stream to the buffer.
/// Writes [size:u32][data...] format used by all serialization versions.
///
/// @param buffer Output buffer (std::string, velox::Buffer, etc.)
/// @param streamData The stream data to write
template <typename T>
void writeStream(T& buffer, std::string_view streamData) {
  auto* sizePos = extend(buffer, sizeof(uint32_t));
  encoding::writeUint32(streamData.size(), sizePos);

  if (!streamData.empty()) {
    auto* dataPos = extend(buffer, streamData.size());
    std::memcpy(dataPos, streamData.data(), streamData.size());
  }
}

/// Reads a single stream from the buffer.
/// Reads [size:u32][data...] format used by all serialization versions.
/// Advances pos past the stream.
///
/// @param pos Pointer to current position (updated after read)
/// @return View of the stream data
inline std::string_view readStream(const char*& pos) {
  const uint32_t size = encoding::readUint32(pos);
  std::string_view data(pos, size);
  pos += size;
  return data;
}

/// Parses all streams from a serialized buffer.
/// Returns a vector of stream data indexed by their original offset.
///
/// For dense format: Returns streams in order (index = offset).
/// For sparse format: Returns streams indexed by their offset from header.
///
/// @param pos Pointer past the header (version + rowCount already read)
/// @param end End of buffer
/// @param version Serialization format version
/// @return Vector of stream data (may have gaps for dense format)
inline std::vector<std::string_view>
parseStreams(const char* pos, const char* end, SerializationVersion version) {
  std::vector<std::string_view> streams;

  const bool sparseFormat =
      (version == SerializationVersion::kSparse ||
       version == SerializationVersion::kSparseEncoded);
  if (sparseFormat) {
    // Sparse format: [stream_count:u32][offsets...][stream_data...]
    const uint32_t streamCount = encoding::readUint32(pos);
    std::vector<uint32_t> offsets(streamCount);
    uint32_t maxOffset = 0;
    for (uint32_t i = 0; i < streamCount; ++i) {
      offsets[i] = encoding::readUint32(pos);
      maxOffset = std::max(maxOffset, offsets[i]);
    }
    if (streamCount > 0) {
      streams.resize(maxOffset + 1);
    }

    // Read streams and place at their offsets.
    for (uint32_t i = 0; i < streamCount; ++i) {
      streams[offsets[i]] = readStream(pos);
    }
  } else {
    NIMBLE_CHECK(
        version == SerializationVersion::kDense ||
            version == SerializationVersion::kDenseEncoded,
        "unexpected version {}",
        version);
    // Dense format: [stream_0][stream_1]...
    // Each stream: [size:u32][data...]
    while (pos < end) {
      streams.emplace_back(readStream(pos));
    }
  }

  return streams;
}

/// Encode scalar data using nimble encoding framework.
/// When encodingLayout is provided, replays the captured encoding.
/// Otherwise, uses encodingSelectionPolicyFactory to select encoding.
/// Returns the encoded data as string_view into the encoding buffer.
template <typename T, typename Buffer>
std::string_view encodeTyped(
    const SerializerOptions& options,
    std::string_view data,
    velox::memory::MemoryPool& pool,
    nimble::Buffer& encodingBuffer,
    const EncodingLayout* encodingLayout) {
  const auto count = data.size() / sizeof(T);
  std::span<const T> values{reinterpret_cast<const T*>(data.data()), count};

  std::unique_ptr<EncodingSelectionPolicy<T>> typedPolicy;
  if (encodingLayout != nullptr) {
    // Replay captured encoding from encodingLayoutTree.
    typedPolicy = std::make_unique<ReplayedEncodingSelectionPolicy<T>>(
        *encodingLayout,
        options.compressionOptions,
        options.encodingSelectionPolicyFactory);
  } else {
    // Use encoding selection policy factory to determine encoding.
    auto policy =
        options.encodingSelectionPolicyFactory(TypeTraits<T>::dataType);
    auto* rawTypedPolicy =
        dynamic_cast<EncodingSelectionPolicy<T>*>(policy.release());
    NIMBLE_CHECK_NOT_NULL(
        rawTypedPolicy,
        "Policy type mismatch for {}",
        toString(TypeTraits<T>::dataType));
    typedPolicy.reset(rawTypedPolicy);
  }

  return EncodingFactory::encode<T>(
      std::move(typedPolicy), values, encodingBuffer);
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
  /// Constructor writes the header:
  /// - No version header: [rowCount]...
  /// - With version header: [version][rowCount]...
  /// - Sparse format adds: [stream_count][offsets...]
  ///
  /// @param nonEmptyStreams Required for kSparse format (to write stream_count
  ///        and offsets in header). Ignored for kDense format.
  /// @param pool Memory pool for encoding buffer allocation.
  /// @param encodingLayoutTree Optional encoding layout tree for replaying
  ///        captured encodings. When provided, looks up EncodingLayout by
  ///        stream offset and uses ReplayedEncodingSelectionPolicy.
  StreamDataWriter(
      const SerializerOptions& options,
      T& buffer,
      uint32_t rowCount,
      const std::vector<const nimble::StreamData*>& nonEmptyStreams,
      velox::memory::MemoryPool* pool,
      const std::unordered_map<uint32_t, const EncodingLayout*>*
          streamEncodingLayouts);

  /// Write encoded data for a single stream directly to buffer.
  void writeData(const nimble::StreamData& streamData);

  /// Close the writer. For dense format, fills trailing zeros up to nodeCount.
  /// For sparse format, this is a no-op (header already written).
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
  // Track last stream offset for dense format zero-filling.
  uint32_t lastStream_{0xffffffff};
};

template <typename T>
StreamDataWriter<T>::StreamDataWriter(
    const SerializerOptions& options,
    T& buffer,
    uint32_t rowCount,
    const std::vector<const nimble::StreamData*>& nonEmptyStreams,
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

  // Build stream offsets for sparse format.
  std::vector<uint32_t> streamOffsets;
  if (options_.sparseFormat()) {
    streamOffsets.reserve(nonEmptyStreams.size());
    for (const auto* streamData : nonEmptyStreams) {
      streamOffsets.emplace_back(streamData->descriptor().offset());
    }
  }

  // Write header using shared implementation.
  std::optional<SerializationVersion> version;
  if (options_.hasVersionHeader()) {
    version = options_.serializationVersion();
  }
  detail::writeHeader(buffer_, version, rowCount, streamOffsets);
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

  if (!options_.sparseFormat()) {
    // Dense format: fill zeros for missing streams before writing.
    NIMBLE_CHECK_LE(lastStream_ + 1, streamOffset, "unexpected stream offset");
    detail::writeMissingStreams(buffer_, lastStream_, streamOffset);
    lastStream_ = streamOffset;
  }

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
    // Wire format: [size:u32][nimble_encoded_data...]
    // The nimble encoding is self-describing with its own header.
    auto encoded = detail::encodeScalar<T>(
        options_, scalarKind, data, *pool_, *encodingBuffer_, encodingLayout);

    // Write size prefix followed by encoded data.
    detail::writeStream(buffer_, encoded);
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
  if (!options_.sparseFormat()) {
    // Dense format: fill trailing zeros up to nodeCount.
    detail::writeMissingStreams(buffer_, lastStream_, nodeCount);
  }
  // Sparse format: no-op (header and data already written).
}

} // namespace facebook::nimble::serde
