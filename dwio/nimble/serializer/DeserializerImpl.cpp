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

#include "dwio/nimble/serializer/DeserializerImpl.h"

#include <zstd.h>

#include "dwio/nimble/common/ChunkHeader.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/serializer/SerializerImpl.h"

namespace facebook::nimble::serde {

StreamData::StreamData(
    ScalarKind kind,
    std::string_view data,
    velox::memory::MemoryPool* pool,
    const Options& options)
    : kind_{kind},
      pool_{pool},
      encodingEnabled_{nonLegacyFormat(options.version)},
      useVarintRowCount_{!isTabletRawFormat(options.version)},
      bufferPool_{options.bufferPool} {
  NIMBLE_CHECK_NOT_NULL(pool_, "Memory pool required for encoding");
  init(data);
}

// Copy legacy stream data to output buffer.
// Data is already decompressed by init() -> decompress().
uint32_t StreamData::copyTo(char* output, uint32_t bufferSize) {
  const uint32_t length =
      std::min(static_cast<uint32_t>(end_ - pos_), bufferSize);
  std::copy(pos_, pos_ + length, output);
  pos_ += length;
  return length;
}

uint32_t StreamData::decodeStrings(uint32_t count, std::string_view* output) {
  if (encodingEnabled_) {
    return decode(output, /*offset=*/0, count, /*width=*/0);
  }
  uint32_t index = 0;
  while (pos_ < end_ && index < count) {
    output[index++] = encoding::readString(pos_);
  }
  return index;
}

void StreamData::reset(std::string_view data, SerializationVersion version) {
  readRows_ = 0;
  encoding_.reset();
  stringBuffers_.clear();
  encodingEnabled_ = nonLegacyFormat(version);
  useVarintRowCount_ = !isTabletRawFormat(version);
  // Re-initialize with new data.
  init(data);
}

void StreamData::init(std::string_view data) {
  if (data.empty()) {
    pos_ = nullptr;
    end_ = nullptr;
    return;
  }
  pos_ = data.begin();
  end_ = data.end();
  if (encodingEnabled_) {
    prepareForDecoding({pos_, end_});
  } else {
    decompress();
  }
}

void StreamData::decompress() {
  NIMBLE_CHECK(!encodingEnabled_);

  // Skip for string/binary - they don't have a compression prefix.
  if (kind_ == ScalarKind::String || kind_ == ScalarKind::Binary) {
    return;
  }

  auto compression = static_cast<CompressionType>(encoding::readChar(pos_));
  switch (compression) {
    case CompressionType::Uncompressed: {
      break;
    }
    case CompressionType::Zstd: {
      const auto compressedSize = static_cast<size_t>(end_ - pos_);
      const auto decompressedSize =
          ZSTD_getFrameContentSize(pos_, compressedSize);
      NIMBLE_CHECK(
          decompressedSize != ZSTD_CONTENTSIZE_ERROR &&
              decompressedSize != ZSTD_CONTENTSIZE_UNKNOWN,
          "Error determining decompressed size");
      decompressionBuffer_.resize(decompressedSize);
      const auto ret = ZSTD_decompress(
          decompressionBuffer_.data(), decompressedSize, pos_, compressedSize);
      NIMBLE_CHECK(!ZSTD_isError(ret), "Error decompressing data");
      pos_ = decompressionBuffer_.data();
      end_ = pos_ + decompressionBuffer_.size();
      break;
    }
    default:
      NIMBLE_UNSUPPORTED("Unsupported compression {}", compression);
  }
}

void StreamData::prepareForDecoding(std::string_view data) {
  NIMBLE_CHECK(encodingEnabled_);
  NIMBLE_CHECK_NULL(encoding_, "Encoding already set");

  // Use nimble EncodingFactory to decode the data.
  // The encoded data is self-describing with type information.
  // Encoded serializer versions always use varint for encoding prefix row
  // counts.
  // For string types, provide a stringBufferFactory that allocates separate
  // buffers using velox::AlignedBuffer for memory tracking.
  Encoding::Options options{
      .useVarintRowCount = useVarintRowCount_, .bufferPool = bufferPool_};
  encoding_ =
      EncodingFactory(options).create(*pool_, data, [this](uint32_t size) {
        auto& buffer = stringBuffers_.emplace_back(
            velox::AlignedBuffer::allocate<char>(size, pool_));
        return buffer->asMutable<void>();
      });
}

uint32_t StreamData::decode(
    void* output,
    uint32_t offset,
    uint32_t count,
    uint32_t width) {
  if (encodingEnabled_) {
    // Nimble encoding path: use materialize() to decode.
    NIMBLE_CHECK_NOT_NULL(encoding_);
    // Only decode as many values as remain in the encoding.
    const uint32_t readCount = std::min(count, remainingRows());
    switch (width) {
      case 0: {
        // String type: output is std::string_view*
        auto* dest = static_cast<std::string_view*>(output) + offset;
        materialize(readCount, dest);
        break;
      }
      case 1: {
        auto* dest = static_cast<char*>(output) + offset * width;
        materialize(readCount, reinterpret_cast<int8_t*>(dest));
        break;
      }
      case 2: {
        auto* dest = static_cast<char*>(output) + offset * width;
        materialize(readCount, reinterpret_cast<int16_t*>(dest));
        break;
      }
      case 4: {
        auto* dest = static_cast<char*>(output) + offset * width;
        materialize(readCount, reinterpret_cast<int32_t*>(dest));
        break;
      }
      case 8: {
        auto* dest = static_cast<char*>(output) + offset * width;
        materialize(readCount, reinterpret_cast<int64_t*>(dest));
        break;
      }
      default:
        NIMBLE_FAIL("Unexpected width {} for nimble decoding", width);
    }
    return readCount;
  }

  // Legacy compression path: read raw bytes from pos_.
  NIMBLE_CHECK_NE(width, 0, "String type not supported for legacy path");
  if (count == 0) {
    return 0;
  }
  const size_t bytesToRead = count * width;
  NIMBLE_CHECK_LE(
      pos_ + bytesToRead, end_, "Not enough data for legacy decode");
  auto* dest = static_cast<char*>(output) + offset * width;
  std::memcpy(dest, pos_, bytesToRead);
  pos_ += bytesToRead;
  return count;
}

StreamDataReader::StreamDataReader(
    velox::memory::MemoryPool* pool,
    const DeserializerOptions& options)
    : options_{options}, pool_{pool}, chunkStrippingBuffer_{pool_} {
  NIMBLE_CHECK_NOT_NULL(pool_);
}

uint32_t StreamDataReader::initialize(std::string_view data) {
  pos_ = data.data();
  end_ = data.end();
  readVersion();
  // All non-legacy formats use varint row count.
  if (usesVarintRowCount(version_)) {
    return varint::readVarint32(&pos_);
  }
  return encoding::readUint32(pos_);
}

void StreamDataReader::readVersion() {
  if (!options_.hasHeader) {
    version_ = SerializationVersion::kLegacy;
    return;
  }
  version_ = static_cast<SerializationVersion>(*pos_);
  NIMBLE_CHECK(
      version_ == SerializationVersion::kLegacy ||
          version_ == SerializationVersion::kCompact ||
          version_ == SerializationVersion::kCompactRaw ||
          version_ == SerializationVersion::kTabletRaw,
      "Unsupported version {}",
      static_cast<uint8_t>(version_));
  ++pos_;
}

void StreamDataReader::iterateStreams(
    const std::function<void(uint32_t offset, std::string_view data)>&
        callback) {
  if (nonLegacyFormat(version_)) {
    // kCompact/kCompactRaw/kTabletRaw format: read stream sizes from trailer.
    const auto streamSizes = detail::readStreamSizes(end_, version_, pool_);
    const bool tabletRaw = isTabletRawFormat(version_);

    for (uint32_t i = 0; i < streamSizes.size(); ++i) {
      std::string_view streamData(pos_, streamSizes[i]);
      pos_ += streamSizes[i];
      if (!streamData.empty()) {
        if (tabletRaw) {
          // kTabletRaw: stream data includes tablet chunk headers:
          // [chunkSize:u32][compressionType:1B][encoded_data...]
          // Strip headers and decompress if needed before passing to callback.
          callback(i, stripChunkHeaders(streamData));
        } else {
          callback(i, streamData);
        }
      }
    }
    pos_ = end_; // Skip past trailer.
  } else {
    // kLegacy format: streams in order with inline u32 sizes.
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

std::string_view StreamDataReader::stripChunkHeaders(
    std::string_view streamData) {
  const auto* pos = streamData.data();
  const auto* end = pos + streamData.size();

  NIMBLE_CHECK_GE(
      streamData.size(), kChunkHeaderSize, "Truncated chunk header in stream");

  if (auto result = tryFastChunkHeaderStrip(pos, end)) {
    return *result;
  }
  return slowChunkHeaderStrip(pos, end);
}

std::string_view StreamDataReader::slowChunkHeaderStrip(
    const char* pos,
    const char* end) {
  // TODO: Consider using IOBuf chain to avoid concatenation for multi-chunk
  // streams.
  chunkStrippingBuffer_.clear();
  while (pos < end) {
    NIMBLE_CHECK_GE(
        static_cast<size_t>(end - pos),
        kChunkHeaderSize,
        "Truncated chunk header in stream");
    const auto [chunkLength, compressionType] = readChunkHeader(pos);
    NIMBLE_CHECK_LE(
        chunkLength,
        static_cast<uint32_t>(end - pos),
        "Chunk data exceeds stream boundary");
    appendChunkData(compressionType, pos, chunkLength);
    pos += chunkLength;
  }
  return {chunkStrippingBuffer_.data(), chunkStrippingBuffer_.size()};
}

std::optional<std::string_view> StreamDataReader::tryFastChunkHeaderStrip(
    const char* pos,
    const char* end) {
  const auto [chunkLength, compressionType] = readChunkHeader(pos);
  NIMBLE_CHECK_LE(
      chunkLength,
      static_cast<uint32_t>(end - pos),
      "Chunk data exceeds stream boundary");
  // Single uncompressed chunk: return a view into the original data
  // (zero-copy).
  if (pos + chunkLength == end &&
      compressionType == CompressionType::Uncompressed) {
    return std::string_view{pos, chunkLength};
  }
  return std::nullopt;
}

void StreamDataReader::appendChunkData(
    CompressionType compression,
    const char* data,
    uint32_t length) {
  switch (compression) {
    case CompressionType::Uncompressed: {
      const auto offset = chunkStrippingBuffer_.size();
      chunkStrippingBuffer_.resize(offset + length);
      std::memcpy(chunkStrippingBuffer_.data() + offset, data, length);
      break;
    }
    case CompressionType::Zstd: {
      const auto decompressedSize = ZSTD_getFrameContentSize(data, length);
      NIMBLE_CHECK(
          decompressedSize != ZSTD_CONTENTSIZE_ERROR &&
              decompressedSize != ZSTD_CONTENTSIZE_UNKNOWN,
          "Error determining decompressed size");
      const auto offset = chunkStrippingBuffer_.size();
      chunkStrippingBuffer_.resize(offset + decompressedSize);
      const auto ret = ZSTD_decompress(
          chunkStrippingBuffer_.data() + offset,
          decompressedSize,
          data,
          length);
      NIMBLE_CHECK(!ZSTD_isError(ret), "Error decompressing chunk data");
      break;
    }
    default:
      NIMBLE_UNSUPPORTED("Unsupported chunk compression {}", compression);
  }
}

} // namespace facebook::nimble::serde
