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

#include <lz4.h>

#include "dwio/nimble/common/ChunkHeader.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/serializer/SerializationHeader.h"

namespace facebook::nimble::serde {

namespace {
ZSTD_DCtx* getThreadLocalDCtx() {
  struct DCtxDeleter {
    void operator()(ZSTD_DCtx* ctx) const {
      ZSTD_freeDCtx(ctx);
    }
  };
  static thread_local std::unique_ptr<ZSTD_DCtx, DCtxDeleter> ctx{
      ZSTD_createDCtx()};
  NIMBLE_CHECK(ctx != nullptr, "Failed to create ZSTD decompression context");
  return ctx.get();
}
} // namespace

StreamData::StreamData(
    ScalarKind kind,
    std::vector<velox::BufferPtr>& stringBuffers,
    velox::memory::MemoryPool* pool,
    velox::BufferPtr* decompressionBuffer)
    : kind_{kind},
      pool_{pool},
      decompressionBuffer_{decompressionBuffer},
      stringBuffers_{&stringBuffers} {
  NIMBLE_CHECK_NOT_NULL(pool_, "Memory pool required for encoding");
  NIMBLE_CHECK_NOT_NULL(decompressionBuffer_, "Decompression buffer required");
}

StreamData::StreamData(
    ScalarKind kind,
    std::string_view data,
    std::vector<velox::BufferPtr>& stringBuffers,
    velox::memory::MemoryPool* pool,
    const Options& options)
    : kind_{kind},
      pool_{pool},
      encodingEnabled_{nonLegacyFormat(options.version)},
      useVarintRowCount_{!isTabletVersion(options.version)},
      bufferPool_{options.bufferPool},
      decompressionBuffer_{options.decompressionBuffer},
      stringBuffers_{&stringBuffers} {
  NIMBLE_CHECK_NOT_NULL(pool_, "Memory pool required for encoding");
  NIMBLE_CHECK_NOT_NULL(decompressionBuffer_, "Decompression buffer required");
  init(data);
}

void StreamData::reset(std::string_view data, SerializationVersion version) {
  readRows_ = 0;
  encoding_.reset();
  encodingEnabled_ = nonLegacyFormat(version);
  useVarintRowCount_ = !isTabletVersion(version);
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

StreamData::DecodeResult StreamData::decodeStrings(
    uint32_t count,
    std::string_view* output) {
  if (encodingEnabled_) {
    return decode(output, /*offset=*/0, count, /*width=*/0);
  }
  uint32_t index = 0;
  while (pos_ < end_ && index < count) {
    output[index++] = encoding::readString(pos_);
  }
  return {
      .numOutputRows = index,
      .nonNullOutputRows = index,
      .segmentExhausted = pos_ == end_};
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

void StreamData::ensureDecompressionBuffer(size_t minBytes) {
  auto& buffer = decompressionBuf();
  if (buffer == nullptr || !buffer->unique() || buffer->capacity() < minBytes) {
    buffer = velox::AlignedBuffer::allocateExact<char>(minBytes, pool_);
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
      ensureDecompressionBuffer(decompressedSize);
      auto& buffer = decompressionBuf();
      const auto ret = ZSTD_decompressDCtx(
          getThreadLocalDCtx(),
          buffer->asMutable<char>(),
          decompressedSize,
          pos_,
          compressedSize);
      NIMBLE_CHECK(!ZSTD_isError(ret), "Error decompressing data");
      pos_ = buffer->as<char>();
      end_ = pos_ + decompressedSize;
      break;
    }
    case CompressionType::Lz4: {
      // LZ4 block data is preceded by the uncompressed size (uint32).
      // Wire format: [origSize:u32][lz4_data...]
      const auto decompressedSize = encoding::readUint32(pos_);
      const auto compressedSize = static_cast<size_t>(end_ - pos_);
      ensureDecompressionBuffer(decompressedSize);
      auto& buffer = decompressionBuf();
      const auto ret = LZ4_decompress_safe(
          pos_,
          buffer->asMutable<char>(),
          static_cast<int>(compressedSize),
          static_cast<int>(decompressedSize));
      NIMBLE_CHECK_EQ(
          ret,
          static_cast<int>(decompressedSize),
          "LZ4 decompressed size mismatch");
      pos_ = buffer->as<char>();
      end_ = pos_ + decompressedSize;
      break;
    }
    default:
      NIMBLE_UNSUPPORTED("Unsupported compression {}", compression);
  }
}

void StreamData::prepareForDecoding(std::string_view data) {
  NIMBLE_CHECK(encodingEnabled_);
  NIMBLE_CHECK_NULL(encoding_, "Encoding already set");
  NIMBLE_CHECK_NOT_NULL(
      stringBuffers_, "String buffer storage required for encoded stream data");

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
        auto& buffer = stringBuffers_->emplace_back(
            velox::AlignedBuffer::allocate<char>(size, pool_));
        return buffer->asMutable<void>();
      });
}

StreamData::DecodeResult StreamData::decode(
    void* output,
    uint32_t offset,
    uint32_t count,
    uint32_t width,
    const std::function<void*()>& getOutputNulls,
    const velox::bits::Bitmap* scatterOutputBitmap) {
  // Nimble encoding path: use materialize() to decode.
  NIMBLE_CHECK_NOT_NULL(
      encoding_,
      "Legacy StreamData must be decoded through copyTo() or decodeStrings()");
  // Only decode as many values as remain in the encoding.
  const uint32_t remainingRows = this->remainingRows();
  const uint32_t readCount = std::min(count, remainingRows);
  const bool segmentExhausted = readCount == remainingRows;
  // materializeNullable() is also the scatter-output API. Use it when the
  // encoding carries nulls, or when a parent in-map stream selected sparse
  // output rows that must be filled with decoded values and absent-row nulls.
  if (encoding_->isNullable() || scatterOutputBitmap != nullptr) {
    const auto nonNulls = decodeNullable(
        output, offset, readCount, width, getOutputNulls, scatterOutputBitmap);
    return {
        .numOutputRows = readCount,
        .nonNullOutputRows = nonNulls,
        .segmentExhausted = segmentExhausted};
  }
  decodeNonNull(output, offset, readCount, width);
  return {
      .numOutputRows = readCount,
      .nonNullOutputRows = readCount,
      .segmentExhausted = segmentExhausted};
}

uint32_t StreamData::decodeNullable(
    void* output,
    uint32_t offset,
    uint32_t readCount,
    uint32_t width,
    const std::function<void*()>& getOutputNulls,
    const velox::bits::Bitmap* scatterOutputBitmap) {
  NIMBLE_CHECK_NOT_NULL(getOutputNulls, "Null buffer callback required");
  switch (width) {
    case 0: {
      const auto nonNulls = encoding_->materializeNullable(
          readCount, output, getOutputNulls, scatterOutputBitmap, offset);
      readRows_ += readCount;
      return nonNulls;
    }
    case 1: {
      const auto nonNulls = encoding_->materializeNullable(
          readCount,
          reinterpret_cast<int8_t*>(output),
          getOutputNulls,
          scatterOutputBitmap,
          offset);
      readRows_ += readCount;
      return nonNulls;
    }
    case 2: {
      const auto nonNulls = encoding_->materializeNullable(
          readCount,
          reinterpret_cast<int16_t*>(output),
          getOutputNulls,
          scatterOutputBitmap,
          offset);
      readRows_ += readCount;
      return nonNulls;
    }
    case 4: {
      const auto nonNulls = encoding_->materializeNullable(
          readCount,
          reinterpret_cast<int32_t*>(output),
          getOutputNulls,
          scatterOutputBitmap,
          offset);
      readRows_ += readCount;
      return nonNulls;
    }
    case 8: {
      const auto nonNulls = encoding_->materializeNullable(
          readCount,
          reinterpret_cast<int64_t*>(output),
          getOutputNulls,
          scatterOutputBitmap,
          offset);
      readRows_ += readCount;
      return nonNulls;
    }
    default:
      NIMBLE_FAIL("Unexpected width {} for nimble decoding", width);
  }
}

void StreamData::decodeNonNull(
    void* output,
    uint32_t offset,
    uint32_t readCount,
    uint32_t width) {
  switch (width) {
    case 0: {
      // String type: output is std::string_view*
      auto* dest = static_cast<std::string_view*>(output) + offset;
      materialize(readCount, dest);
      return;
    }
    case 1: {
      auto* dest = static_cast<char*>(output) + offset * width;
      materialize(readCount, reinterpret_cast<int8_t*>(dest));
      return;
    }
    case 2: {
      auto* dest = static_cast<char*>(output) + offset * width;
      materialize(readCount, reinterpret_cast<int16_t*>(dest));
      return;
    }
    case 4: {
      auto* dest = static_cast<char*>(output) + offset * width;
      materialize(readCount, reinterpret_cast<int32_t*>(dest));
      return;
    }
    case 8: {
      auto* dest = static_cast<char*>(output) + offset * width;
      materialize(readCount, reinterpret_cast<int64_t*>(dest));
      return;
    }
    default:
      NIMBLE_FAIL("Unexpected width {} for nimble decoding", width);
  }
}

StreamData::DecodeResult StreamData::decodeLegacy(
    void* output,
    uint32_t offset,
    uint32_t count,
    uint32_t width) {
  NIMBLE_CHECK(!encodingEnabled_, "decodeLegacy called for encoded stream");
  NIMBLE_CHECK_NE(width, 0, "String type not supported for legacy path");
  if (count == 0) {
    return {.segmentExhausted = pos_ == end_};
  }
  auto* dest = static_cast<char*>(output) + offset * width;
  const auto copied = copyTo(dest, count * width);
  NIMBLE_CHECK_EQ(copied % width, 0, "Legacy stream ended mid-value");
  const auto rows = copied / width;
  return {
      .numOutputRows = rows,
      .nonNullOutputRows = rows,
      .segmentExhausted = pos_ == end_};
}

StreamDataParser::StreamDataParser(
    velox::memory::MemoryPool* pool,
    const DeserializerOptions& options)
    : options_{options}, pool_{pool} {
  NIMBLE_CHECK_NOT_NULL(pool_);
}

uint32_t StreamDataParser::initialize(std::string_view data) {
  pos_ = data.data();
  end_ = data.end();
  auto header = readSerializationHeader(pos_, end_, options_.hasHeader);
  version_ = header.version;
  requiresNullBarrier_ = header.requiresNullBarrier;
  rowRange_ = header.rowRange;
  return header.rowCount;
}

std::string_view StreamDataParser::stripChunkHeaders(
    std::string_view streamData) {
  const auto* pos = streamData.data();
  const auto* end = pos + streamData.size();

  NIMBLE_CHECK_GE(
      streamData.size(), kChunkHeaderSize, "Truncated chunk header in stream");

  if (auto result = tryFastChunkHeaderStrip(pos, end)) {
    NIMBLE_CHECK(
        !result->empty(), "Chunked stream must have a non-empty payload");
    return *result;
  }
  return slowChunkHeaderStrip(pos, end);
}

std::string_view StreamDataParser::slowChunkHeaderStrip(
    const char* pos,
    const char* end) {
  // TODO: Consider using IOBuf chain to avoid concatenation for multi-chunk
  // streams.
  const auto payloadSize = strippedStreamSize(pos, end);
  NIMBLE_CHECK_GT(
      payloadSize, 0, "Chunked stream must have a non-empty payload");
  auto buffer = velox::AlignedBuffer::allocateExact<char>(payloadSize, pool_);
  auto* output = buffer->asMutable<char>();
  auto* const outputEnd = output + payloadSize;
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
    appendChunkData(compressionType, pos, chunkLength, output);
    pos += chunkLength;
  }
  NIMBLE_CHECK_EQ(output, outputEnd, "Stripped chunk size mismatch");
  const auto* data = buffer->as<char>();
  strippedStreamBuffers_.emplace_back(std::move(buffer));
  return {data, payloadSize};
}

std::optional<std::string_view> StreamDataParser::tryFastChunkHeaderStrip(
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

size_t StreamDataParser::strippedStreamSize(const char* pos, const char* end) {
  size_t size = 0;
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
    size += decodedChunkSize(compressionType, pos, chunkLength);
    pos += chunkLength;
  }
  return size;
}

size_t StreamDataParser::decodedChunkSize(
    CompressionType compression,
    const char* data,
    uint32_t length) {
  switch (compression) {
    case CompressionType::Uncompressed:
      return length;
    case CompressionType::Zstd: {
      const auto decompressedSize = ZSTD_getFrameContentSize(data, length);
      NIMBLE_CHECK(
          decompressedSize != ZSTD_CONTENTSIZE_ERROR &&
              decompressedSize != ZSTD_CONTENTSIZE_UNKNOWN,
          "Error determining decompressed size");
      return decompressedSize;
    }
    case CompressionType::Lz4: {
      NIMBLE_CHECK_GE(
          length, sizeof(uint32_t), "Truncated LZ4 chunk size header");
      const auto* pos = data;
      return encoding::readUint32(pos);
    }
    default:
      NIMBLE_UNSUPPORTED("Unsupported chunk compression {}", compression);
  }
}

void StreamDataParser::appendChunkData(
    CompressionType compression,
    const char* data,
    uint32_t length,
    char*& output) {
  switch (compression) {
    case CompressionType::Uncompressed: {
      std::memcpy(output, data, length);
      output += length;
      break;
    }
    case CompressionType::Zstd: {
      const auto decompressedSize = decodedChunkSize(compression, data, length);
      const auto ret = ZSTD_decompressDCtx(
          getThreadLocalDCtx(), output, decompressedSize, data, length);
      NIMBLE_CHECK(!ZSTD_isError(ret), "Error decompressing chunk data");
      NIMBLE_CHECK_EQ(
          ret, decompressedSize, "ZSTD chunk decompressed size mismatch");
      output += decompressedSize;
      break;
    }
    case CompressionType::Lz4: {
      const auto decompressedSize = decodedChunkSize(compression, data, length);
      const auto* pos = data;
      encoding::readUint32(pos);
      const auto compressedSize =
          static_cast<size_t>(length - sizeof(uint32_t));
      const auto ret = LZ4_decompress_safe(
          pos,
          output,
          static_cast<int>(compressedSize),
          static_cast<int>(decompressedSize));
      NIMBLE_CHECK_EQ(
          ret,
          static_cast<int>(decompressedSize),
          "LZ4 chunk decompressed size mismatch");
      output += decompressedSize;
      break;
    }
    default:
      NIMBLE_UNSUPPORTED("Unsupported chunk compression {}", compression);
  }
}

} // namespace facebook::nimble::serde
