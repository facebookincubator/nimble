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

#include "dwio/nimble/serializer/SerializerImpl.h"

#include <bit>
#include <limits>

#include "dwio/nimble/serializer/SerializationHeader.h"

#include <lz4.h>
#include <lz4hc.h>
#include <zstd.h>
#include <zstd_errors.h>

#include <folly/io/Cursor.h>

namespace facebook::nimble::serde::detail {

namespace {

std::vector<uint32_t> decodeTrivialTrailer(
    const char*& payload,
    uint32_t payloadSize) {
  const uint32_t count = payloadSize / sizeof(uint32_t);
  std::vector<uint32_t> sizes(count);
  if (count > 0) {
    std::memcpy(sizes.data(), payload, count * sizeof(uint32_t));
    payload += count * sizeof(uint32_t);
  }
  return sizes;
}

std::vector<uint32_t> decodeVarintTrailer(const char*& payload) {
  const uint32_t count = varint::readVarint32(&payload);
  std::vector<uint32_t> sizes(count);
  for (uint32_t i = 0; i < count; ++i) {
    sizes[i] = varint::readVarint32(&payload);
  }
  return sizes;
}

std::vector<uint32_t> decodeDeltaTrailer(const char*& payload) {
  const uint32_t count = varint::readVarint32(&payload);
  std::vector<uint32_t> sizes(count);
  if (count > 0) {
    sizes[0] = varint::readVarint32(&payload);
    for (uint32_t i = 1; i < count; ++i) {
      const auto delta = varint::readVarint32(&payload);
      sizes[i] = sizes[i - 1] + delta;
    }
  }
  return sizes;
}

std::vector<uint32_t> decodeFixedBitWidthTrailer(const char*& payload) {
  const uint8_t bitWidth = static_cast<uint8_t>(*payload++);
  const uint32_t count = varint::readVarint32(&payload);
  std::vector<uint32_t> sizes(count);
  if (bitWidth > 0 && count > 0) {
    const uint32_t packedBytes =
        static_cast<uint32_t>(FixedBitArray::bufferSize(count, bitWidth));
    FixedBitArray arr{const_cast<char*>(payload), static_cast<int>(bitWidth)};
    arr.bulkGet32(0, count, sizes.data());
    payload += packedBytes;
  }
  return sizes;
}

std::vector<uint32_t> decodeMainlyConstantTrailer(const char*& payload) {
  const uint32_t streamCount = varint::readVarint32(&payload);
  const uint32_t nonZeroCount = varint::readVarint32(&payload);
  NIMBLE_CHECK_LE(
      nonZeroCount,
      streamCount,
      "MainlyConstant nonZeroCount exceeds streamCount");
  // Constant is fixed at 0 and not stored on the wire.
  std::vector<uint32_t> sizes(streamCount, 0);
  if (nonZeroCount == 0) {
    // idxBitWidth/valBitWidth bytes and packed arrays are omitted when
    // there are no non-zero entries.
    return sizes;
  }

  const uint8_t idxBitWidth = static_cast<uint8_t>(*payload++);
  NIMBLE_CHECK_LE(
      idxBitWidth, 32, "MainlyConstant idxBitWidth exceeds 32 bits");
  std::vector<uint32_t> indices(nonZeroCount);
  const uint32_t packedIndicesBytes = static_cast<uint32_t>(
      FixedBitArray::bufferSize(nonZeroCount, idxBitWidth));
  FixedBitArray idxArr{
      const_cast<char*>(payload), static_cast<int>(idxBitWidth)};
  idxArr.bulkGet32(0, nonZeroCount, indices.data());
  payload += packedIndicesBytes;

  const uint8_t valBitWidth = static_cast<uint8_t>(*payload++);
  NIMBLE_CHECK_LE(
      valBitWidth, 32, "MainlyConstant valBitWidth exceeds 32 bits");
  std::vector<uint32_t> values(nonZeroCount);
  const uint32_t packedValuesBytes = static_cast<uint32_t>(
      FixedBitArray::bufferSize(nonZeroCount, valBitWidth));
  FixedBitArray valArr{
      const_cast<char*>(payload), static_cast<int>(valBitWidth)};
  valArr.bulkGet32(0, nonZeroCount, values.data());
  payload += packedValuesBytes;

  for (uint32_t i = 0; i < nonZeroCount; ++i) {
    NIMBLE_CHECK_LT(
        indices[i], streamCount, "MainlyConstant trailer index out of range");
    sizes[indices[i]] = values[i];
  }
  return sizes;
}

// Upper-bound payload-size estimators (one per trailer encoding). Each
// returns the payload bytes between the encoding-type byte and the trailing
// trailer_size u32. estimateTrailerSize() adds the encoding-type byte and
// the trailer_size suffix.

// Compresses 'input' into 'compPos' (which already has the compression-type
// byte written). Returns the number of bytes written if compression was
// beneficial; std::nullopt if the caller should fall back to uncompressed.

std::optional<uint32_t> compressZstd(
    const SerializerOptions& options,
    std::string_view input,
    char* compPos,
    uint32_t size) {
  const auto ret = ZSTD_compress(
      compPos, size, input.data(), size, options.compressionLevel);
  if (ZSTD_isError(ret)) {
    NIMBLE_CHECK_EQ(
        static_cast<int>(ZSTD_getErrorCode(ret)),
        static_cast<int>(ZSTD_error_dstSize_tooSmall),
        "zstd error");
    return std::nullopt;
  }
  return static_cast<uint32_t>(ret);
}

std::optional<uint32_t> compressLz4(
    const SerializerOptions& options,
    std::string_view input,
    char* compPos,
    uint32_t size) {
  // LZ4 block mode is not self-descriptive (unlike ZSTD frames), so we
  // prepend the uncompressed size as a uint32 before the compressed data.
  // Wire format: [size:u32][compType:i8=3][origSize:u32][lz4_data...]
  const auto origSize = static_cast<uint32_t>(input.size());
  encoding::writeUint32(origSize, compPos);
  constexpr int kMinHcLevel = LZ4HC_CLEVEL_MIN;
  const auto compressedSize = options.compressionLevel >= kMinHcLevel
      ? LZ4_compress_HC(
            input.data(),
            compPos,
            static_cast<int>(input.size()),
            static_cast<int>(size),
            options.compressionLevel)
      : LZ4_compress_default(
            input.data(),
            compPos,
            static_cast<int>(input.size()),
            static_cast<int>(size));
  if (compressedSize == 0 ||
      static_cast<uint32_t>(compressedSize) >= origSize) {
    return std::nullopt;
  }
  return sizeof(uint32_t) + static_cast<uint32_t>(compressedSize);
}

size_t estimateTrivialTrailerPayloadSize(size_t numStreams) {
  return numStreams * sizeof(uint32_t);
}

size_t estimateVarintTrailerPayloadSize(size_t numStreams) {
  // count varint + N varints (max 5 bytes each).
  return 5 + numStreams * 5;
}

size_t estimateDeltaTrailerPayloadSize(size_t numStreams) {
  // count varint + first offset varint + (N-1) delta varints (max 5 bytes
  // each).
  return 5 + numStreams * 5;
}

size_t estimateFixedBitWidthTrailerPayloadSize(size_t numStreams) {
  // bitWidth:1B + count varint + bufferSize (32 bits per element max).
  return 1 + 5 + FixedBitArray::bufferSize(numStreams, /*bitWidth=*/32);
}

// MainlyConstant: idxBitWidth matches the writer's formula (max(1,
// bit_width(numStreams - 1)) when there are indices); valBitWidth is
// unbounded up to 32 since values are uint32_t.
size_t estimateMainlyConstantTrailerPayloadSize(size_t numStreams) {
  const size_t idxBitWidth =
      numStreams == 0 ? 0 : std::max<size_t>(1, std::bit_width(numStreams - 1));
  // 5 (streamCount varint) + 5 (nonZeroCount varint) + 1 (idxBitWidth) +
  // packed indices + 1 (valBitWidth) + packed values.
  return 5 + 5 + 1 +
      FixedBitArray::bufferSize(numStreams, static_cast<int>(idxBitWidth)) + 1 +
      FixedBitArray::bufferSize(numStreams, /*bitWidth=*/32);
}

std::vector<uint32_t> decodeTrailerStreamSizes(
    const char* trailerStart,
    uint32_t trailerSize) {
  const auto* end = trailerStart + trailerSize;
  const auto encodingType =
      static_cast<EncodingType>(static_cast<uint8_t>(*trailerStart));
  const char* payload = trailerStart + sizeof(uint8_t);
  const uint32_t payloadSize = trailerSize - sizeof(uint8_t);
  std::vector<uint32_t> sizes;

  switch (encodingType) {
    case EncodingType::Trivial:
      sizes = decodeTrivialTrailer(payload, payloadSize);
      break;
    case EncodingType::Varint:
      sizes = decodeVarintTrailer(payload);
      break;
    case EncodingType::Delta:
      sizes = decodeDeltaTrailer(payload);
      break;
    case EncodingType::FixedBitWidth:
      sizes = decodeFixedBitWidthTrailer(payload);
      break;
    case EncodingType::MainlyConstant:
      sizes = decodeMainlyConstantTrailer(payload);
      break;
    default:
      NIMBLE_FAIL(
          "Unsupported EncodingType for stream sizes trailer: {}",
          encodingType);
  }

  NIMBLE_CHECK_EQ(
      payload,
      end,
      "Trailer size mismatch: read {} bytes, expected {}",
      payload - trailerStart,
      trailerSize);
  return sizes;
}

} // namespace

uint32_t getStringsTotalSize(std::string_view input) {
  const auto strData = reinterpret_cast<const std::string_view*>(input.data());
  const auto strDataEnd =
      reinterpret_cast<const std::string_view*>(input.end());
  uint32_t size = 0;
  for (auto sv = strData; sv < strDataEnd; ++sv) {
    size += sizeof(uint32_t);
    size += sv->size();
  }
  return size;
}

void encodeStrings(std::string_view input, uint32_t size, char* output) {
  const auto strData = reinterpret_cast<const std::string_view*>(input.data());
  const auto strDataEnd =
      reinterpret_cast<const std::string_view*>(input.end());
  encoding::writeUint32(size, output);
  for (auto sv = strData; sv < strDataEnd; ++sv) {
    encoding::writeString(*sv, output);
  }
}

uint32_t
encode(const SerializerOptions& options, std::string_view input, char* output) {
  // Size prefix + compression type + actual content
  uint32_t size = input.size();
  const auto compression = options.compressionType;
  bool writeUncompressed{true};
  if (compression != CompressionType::Uncompressed &&
      size >= options.compressionThreshold) {
    auto* compPos = output + sizeof(uint32_t);
    encoding::writeChar(static_cast<int8_t>(compression), compPos);
    std::optional<uint32_t> compressedSize;
    switch (compression) {
      case CompressionType::Zstd:
        compressedSize = compressZstd(options, input, compPos, size);
        break;
      case CompressionType::Lz4:
        compressedSize = compressLz4(options, input, compPos, size);
        break;
      default:
        NIMBLE_UNSUPPORTED("Unsupported compression {}", toString(compression));
    }
    if (compressedSize.has_value()) {
      size = *compressedSize;
      writeUncompressed = false;
    }
  }

  if (writeUncompressed) {
    auto* compPos = output + sizeof(uint32_t);
    encoding::writeChar(
        static_cast<int8_t>(CompressionType::Uncompressed), compPos);
    std::copy(input.data(), input.end(), compPos);
  }
  encoding::writeUint32(size + 1, output);
  return size + sizeof(uint32_t) + 1;
}

std::vector<uint32_t> readTrailerStreamSizes(const char* end) {
  const uint32_t trailerSize = readTrailerSize(end);
  const char* trailerStart = end - sizeof(uint32_t) - trailerSize;
  return decodeTrailerStreamSizes(trailerStart, trailerSize);
}

std::vector<uint32_t> readTrailerStreamSizes(const folly::IOBuf& input) {
  // Fast path: trailer fits in the tail segment.
  const auto* tail = input.prev();
  if (tail->length() >= sizeof(uint32_t)) {
    const auto* tailEnd =
        reinterpret_cast<const char*>(tail->data()) + tail->length();
    const uint32_t trailerSize = readTrailerSize(tailEnd);
    if (tail->length() >= sizeof(uint32_t) + trailerSize) {
      return readTrailerStreamSizes(tailEnd);
    }
  }

  // Fallback: trailer spans chain boundary — use cursor + pull().
  const auto totalLength = input.computeChainDataLength();
  folly::io::Cursor trailerCursor(&input);
  trailerCursor.skip(totalLength - sizeof(uint32_t));
  const uint32_t trailerSize = trailerCursor.read<uint32_t>();

  folly::io::Cursor sizesCursor(&input);
  sizesCursor.skip(totalLength - sizeof(uint32_t) - trailerSize);
  std::string trailerBuf(trailerSize, '\0');
  sizesCursor.pull(trailerBuf.data(), trailerSize);
  return decodeTrailerStreamSizes(trailerBuf.data(), trailerSize);
}

size_t estimateTrailerSize(size_t numStreams, EncodingType encodingType) {
  const auto resolvedType = getTrailerEncodingType(encodingType);
  // [encodingType:1B][payload][trailer_size:u32]
  size_t payloadSize{0};
  switch (resolvedType) {
    case EncodingType::Trivial:
      payloadSize = estimateTrivialTrailerPayloadSize(numStreams);
      break;
    case EncodingType::Varint:
      payloadSize = estimateVarintTrailerPayloadSize(numStreams);
      break;
    case EncodingType::Delta:
      payloadSize = estimateDeltaTrailerPayloadSize(numStreams);
      break;
    case EncodingType::FixedBitWidth:
      payloadSize = estimateFixedBitWidthTrailerPayloadSize(numStreams);
      break;
    case EncodingType::MainlyConstant:
      payloadSize = estimateMainlyConstantTrailerPayloadSize(numStreams);
      break;
    default:
      NIMBLE_FAIL(
          "Unsupported EncodingType for stream sizes trailer: {}",
          resolvedType);
  }
  return sizeof(uint8_t) + payloadSize + sizeof(uint32_t);
}

std::vector<std::string_view> parseStreams(
    const char* pos,
    const char* end,
    SerializationVersion version,
    velox::memory::MemoryPool* pool) {
  NIMBLE_CHECK_NOT_NULL(pool, "Memory pool cannot be null");

  std::vector<std::string_view> streams;

  if (isCompactFormat(version)) {
    const auto streamSizes = readTrailerStreamSizes(end);
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

} // namespace facebook::nimble::serde::detail
