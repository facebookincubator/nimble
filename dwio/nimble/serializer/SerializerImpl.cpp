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

#include <limits>

#include "dwio/nimble/serializer/SerializationHeader.h"

#include <lz4.h>
#include <lz4hc.h>
#include <zstd.h>
#include <zstd_errors.h>

#include <folly/io/Cursor.h>

namespace facebook::nimble::serde::detail {
namespace {

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
    case EncodingType::Trivial: {
      const uint32_t count = payloadSize / sizeof(uint32_t);
      sizes.resize(count);
      if (count > 0) {
        std::memcpy(sizes.data(), payload, count * sizeof(uint32_t));
        payload += count * sizeof(uint32_t);
      }
      break;
    }
    case EncodingType::Varint: {
      const uint32_t count = varint::readVarint32(&payload);
      sizes.resize(count);
      for (uint32_t i = 0; i < count; ++i) {
        sizes[i] = varint::readVarint32(&payload);
      }
      break;
    }
    case EncodingType::Delta: {
      const uint32_t count = varint::readVarint32(&payload);
      sizes.resize(count);
      if (count > 0) {
        sizes[0] = varint::readVarint32(&payload);
        for (uint32_t i = 1; i < count; ++i) {
          const auto delta = varint::readVarint32(&payload);
          sizes[i] = sizes[i - 1] + delta;
        }
      }
      break;
    }
    case EncodingType::FixedBitWidth: {
      const uint8_t bitWidth = static_cast<uint8_t>(*payload++);
      const uint32_t count = varint::readVarint32(&payload);
      sizes.resize(count);
      if (bitWidth > 0 && count > 0) {
        const uint32_t packedBytes =
            static_cast<uint32_t>(FixedBitArray::bufferSize(count, bitWidth));
        FixedBitArray arr{
            const_cast<char*>(payload), static_cast<int>(bitWidth)};
        arr.bulkGet32(0, count, sizes.data());
        payload += packedBytes;
      }
      break;
    }
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
    // TODO: share compression implementation
    switch (compression) {
      case CompressionType::Zstd: {
        const auto ret = ZSTD_compress(
            compPos, size, input.data(), size, options.compressionLevel);
        if (ZSTD_isError(ret)) {
          NIMBLE_CHECK_EQ(
              static_cast<int>(ZSTD_getErrorCode(ret)),
              static_cast<int>(ZSTD_error_dstSize_tooSmall),
              "zstd error");
          // Fall back to uncompressed
        } else {
          size = ret;
          writeUncompressed = false;
        }
        break;
      }
      case CompressionType::Lz4: {
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
          // Fall back to uncompressed
        } else {
          size = sizeof(uint32_t) + compressedSize;
          writeUncompressed = false;
        }
        break;
      }
      default:
        NIMBLE_UNSUPPORTED("Unsupported compression {}", toString(compression));
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
      payloadSize = numStreams * sizeof(uint32_t);
      break;
    case EncodingType::Varint:
      // Upper bound: count varint + N varints (max 5 bytes each).
      payloadSize = 5 + numStreams * 5;
      break;
    case EncodingType::Delta:
      // Upper bound: count varint + first offset varint + (N-1) delta
      // varints (max 5 bytes each).
      payloadSize = 5 + numStreams * 5;
      break;
    case EncodingType::FixedBitWidth:
      // Upper bound: bitWidth:1B + count varint + bufferSize (32 bits per
      // element max).
      payloadSize =
          1 + 5 + FixedBitArray::bufferSize(numStreams, /*bitWidth=*/32);
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
