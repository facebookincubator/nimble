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

#include <zstd.h>
#include <zstd_errors.h>

#include <folly/io/Cursor.h>

namespace facebook::nimble::serde::detail {
namespace {

std::vector<uint32_t> decodeCompactStreamSizes(
    std::string_view encodedSizes,
    velox::memory::MemoryPool* pool) {
  NIMBLE_CHECK_NOT_NULL(pool);
  auto encoding = EncodingFactory::decode(
      *pool,
      encodedSizes,
      nullptr,
      Encoding::Options{.useVarintRowCount = true});
  const uint32_t count = encoding->rowCount();
  std::vector<uint32_t> values(count);
  encoding->materialize(count, values.data());
  return values;
}

std::vector<uint32_t> decodeRawStreamSizes(
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
    default:
      NIMBLE_FAIL("Unsupported EncodingType for kCompactRaw: {}", encodingType);
  }

  NIMBLE_CHECK_EQ(
      payload,
      end,
      "Raw trailer size mismatch: read {} bytes, expected {}",
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

std::vector<uint32_t> readStreamSizes(
    const char* end,
    SerializationVersion version,
    velox::memory::MemoryPool* pool) {
  const uint32_t trailerSize = readTrailerSize(end);
  const char* trailerStart = end - sizeof(uint32_t) - trailerSize;
  if (isRawFormat(version)) {
    return decodeRawStreamSizes(trailerStart, trailerSize);
  }
  return decodeCompactStreamSizes({trailerStart, trailerSize}, pool);
}

std::vector<uint32_t> readStreamSizes(
    const folly::IOBuf& input,
    SerializationVersion version,
    velox::memory::MemoryPool* pool) {
  // Fast path: trailer fits in the tail segment.
  const auto* tail = input.prev();
  if (tail->length() >= sizeof(uint32_t)) {
    const auto* tailEnd =
        reinterpret_cast<const char*>(tail->data()) + tail->length();
    const uint32_t trailerSize = readTrailerSize(tailEnd);
    if (tail->length() >= sizeof(uint32_t) + trailerSize) {
      return readStreamSizes(tailEnd, version, pool);
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
  if (isRawFormat(version)) {
    return decodeRawStreamSizes(trailerBuf.data(), trailerSize);
  }
  return decodeCompactStreamSizes(trailerBuf, pool);
}

size_t estimateRawTrailerSize(size_t numStreams, EncodingType encodingType) {
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

std::vector<std::string_view> parseStreams(
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

} // namespace facebook::nimble::serde::detail
