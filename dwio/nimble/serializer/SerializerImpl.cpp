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

#include "dwio/nimble/serializer/legacy/TrailerReader.h"

#include "dwio/nimble/serializer/SerializationHeader.h"

#include <lz4.h>
#include <lz4hc.h>
#include <zstd.h>
#include <zstd_errors.h>

#include <folly/io/Cursor.h>

namespace facebook::nimble::serde::detail {

namespace {

// Sanity cap on the trailing trailer-size u32 at the public reader entries.
// Production trailers are well under this (typical low-MB at worst). Anything
// past this is treated as buffer corruption rather than a legitimate
// allocation request, so a single bit flip in the trailing u32 can't drive
// a multi-GB allocation before the post-decode mismatch check fires.
constexpr uint32_t kMaxTrailerBytes = 1u << 26; // 64 MiB.

// Per-section decoders for the two-array sparse trailer layout. Each consumes
// the encoding-specific payload (no encoding-type byte) and advances `payload`
// past the bytes it read. The caller is responsible for reading the
// encoding-type byte and dispatching to the right decoder.
//
// `remainingBytes` is the upper bound on payload bytes the decoder may
// consume (i.e. `trailerEnd - payload` at entry). It is used only for a
// single pre-loop sanity check on `count` so a corrupted count varint
// cannot drive a multi-GB `resize`. Loop bodies remain free of
// per-iteration bound checks to keep the hot path branch-free.

void decodeTrivialSection(
    const char*& payload,
    uint32_t remainingBytes,
    std::vector<uint32_t>& values) {
  const uint32_t count = varint::readVarint32(&payload);
  NIMBLE_CHECK_LE(
      count,
      remainingBytes,
      "Trivial section count exceeds remaining trailer bytes");
  values.resize(count);
  if (count > 0) {
    std::memcpy(values.data(), payload, count * sizeof(uint32_t));
    payload += count * sizeof(uint32_t);
  }
}

void decodeVarintSection(
    const char*& payload,
    uint32_t remainingBytes,
    std::vector<uint32_t>& values) {
  const uint32_t count = varint::readVarint32(&payload);
  NIMBLE_CHECK_LE(
      count,
      remainingBytes,
      "Varint section count exceeds remaining trailer bytes");
  values.resize(count);
  for (uint32_t i = 0; i < count; ++i) {
    values[i] = varint::readVarint32(&payload);
  }
}

void decodeDeltaSection(
    const char*& payload,
    uint32_t remainingBytes,
    std::vector<uint32_t>& values) {
  const uint32_t count = varint::readVarint32(&payload);
  NIMBLE_CHECK_LE(
      count,
      remainingBytes,
      "Delta section count exceeds remaining trailer bytes");
  values.resize(count);
  if (count > 0) {
    values[0] = varint::readVarint32(&payload);
    for (uint32_t i = 1; i < count; ++i) {
      const auto delta = varint::readVarint32(&payload);
      values[i] = values[i - 1] + delta;
    }
  }
}

void decodeFixedBitWidthSection(
    const char*& payload,
    uint32_t remainingBytes,
    std::vector<uint32_t>& values) {
  const uint8_t bitWidth = static_cast<uint8_t>(*payload++);
  NIMBLE_CHECK_LE(
      bitWidth, 32, "FixedBitWidth section bitWidth exceeds 32 bits");
  const uint32_t count = varint::readVarint32(&payload);
  // Each value occupies >= 1 bit on the wire, so count <= remainingBytes * 8
  // is a generous sanity ceiling. Cast to uint64_t to avoid overflow when
  // remainingBytes is large.
  NIMBLE_CHECK_LE(
      static_cast<uint64_t>(count),
      static_cast<uint64_t>(remainingBytes) * 8u,
      "FixedBitWidth section count exceeds remaining trailer bit-capacity");
  values.assign(count, 0);
  if (bitWidth > 0 && count > 0) {
    const uint32_t packedBytes =
        static_cast<uint32_t>(FixedBitArray::bufferSize(count, bitWidth));
    FixedBitArray arr{const_cast<char*>(payload), static_cast<int>(bitWidth)};
    arr.bulkGet32(0, count, values.data());
    payload += packedBytes;
  }
}

void decodeSection(
    EncodingType encodingType,
    const char*& payload,
    const char* trailerEnd,
    std::vector<uint32_t>& values) {
  const auto remainingBytes = static_cast<uint32_t>(trailerEnd - payload);
  // NOLINTNEXTLINE(clang-diagnostic-switch-enum)
  switch (getTrailerEncodingType(encodingType)) {
    case EncodingType::Trivial:
      decodeTrivialSection(payload, remainingBytes, values);
      break;
    case EncodingType::Varint:
      decodeVarintSection(payload, remainingBytes, values);
      break;
    case EncodingType::Delta:
      decodeDeltaSection(payload, remainingBytes, values);
      break;
    case EncodingType::FixedBitWidth:
      decodeFixedBitWidthSection(payload, remainingBytes, values);
      break;
    default:
      NIMBLE_FAIL(
          "Unsupported EncodingType for stream sizes trailer section: {}",
          encodingType);
  }
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

// Upper-bound estimators for the encoded payload of one section. `count` is the
// worst-case number of values to emit; `maxValue` is the largest value that
// can appear (used by Delta/FixedBitWidth; Trivial/Varint ignore it).
size_t estimateTrivialSectionSize(size_t count) {
  // count varint (max 5 bytes) + N u32s.
  return 5 + count * sizeof(uint32_t);
}

size_t estimateVarintSectionSize(size_t count) {
  // count varint + N varints (max 5 bytes each).
  return 5 + count * 5;
}

size_t estimateDeltaSectionSize(size_t count) {
  // count varint + first value varint + (N-1) delta varints (max 5 each).
  return 5 + count * 5;
}

size_t estimateFixedBitWidthSectionSize(size_t count) {
  // bitWidth:1B + count varint + bufferSize (32 bits per element max).
  return 1 + 5 + FixedBitArray::bufferSize(count, /*bitWidth=*/32);
}

size_t estimateSectionSize(EncodingType encodingType, size_t count) {
  // NOLINTNEXTLINE(clang-diagnostic-switch-enum)
  switch (getTrailerEncodingType(encodingType)) {
    case EncodingType::Trivial:
      return estimateTrivialSectionSize(count);
    case EncodingType::Varint:
      return estimateVarintSectionSize(count);
    case EncodingType::Delta:
      return estimateDeltaSectionSize(count);
    case EncodingType::FixedBitWidth:
      return estimateFixedBitWidthSectionSize(count);
    default:
      NIMBLE_FAIL(
          "Unsupported EncodingType for stream sizes trailer section: {}",
          encodingType);
  }
}

// Decodes a two-array sparse trailer into parallel `streamIds` and
// `streamSizes` arrays. Validates that the per-section decoders consume
// exactly the trailer payload and that `streamIds.size() ==
// streamSizes.size()`.
void decodeTrailerStreamMetadata(
    const char* trailerStart,
    uint32_t trailerSize,
    std::vector<uint32_t>& streamIds,
    std::vector<uint32_t>& streamSizes) {
  const auto* trailerEnd = trailerStart + trailerSize;
  const char* payload = trailerStart;
  const auto indicesEncodingType =
      static_cast<EncodingType>(static_cast<uint8_t>(*payload++));
  decodeSection(indicesEncodingType, payload, trailerEnd, streamIds);
  const auto sizesEncodingType =
      static_cast<EncodingType>(static_cast<uint8_t>(*payload++));
  decodeSection(sizesEncodingType, payload, trailerEnd, streamSizes);
  NIMBLE_CHECK_EQ(
      payload,
      trailerEnd,
      "Trailer size mismatch: read {} bytes, expected {}",
      payload - trailerStart,
      trailerSize);
  NIMBLE_CHECK_EQ(
      streamIds.size(),
      streamSizes.size(),
      "Trailer indices/sizes section count mismatch");
}

// Decodes the kTablet dedup trailer layout (streamIds, sizeIndices,
// uniqueSizes) and reconstructs the parallel `streamIds`, `streamOffsets`,
// and `streamSizes` arrays. The sizeIndices section stores, for each present
// stream slot, the unique body stream it aliases. `streamOffsets` is reused as
// scratch for those unique-stream indices before being overwritten with
// reconstructed body offsets; `uniqueStreamSizes` receives the decoded
// unique-size table and is then transformed in place into exclusive prefix-sum
// offsets.
void decodeTrailerStreamMetadata(
    const char* trailerStart,
    uint32_t trailerSize,
    std::vector<uint32_t>& streamIds,
    std::vector<uint32_t>& streamOffsets,
    std::vector<uint32_t>& streamSizes,
    std::vector<uint32_t>& uniqueStreamSizes) {
  const auto* trailerEnd = trailerStart + trailerSize;
  const char* payload = trailerStart;
  const auto streamIdsEncodingType =
      static_cast<EncodingType>(static_cast<uint8_t>(*payload++));
  decodeSection(streamIdsEncodingType, payload, trailerEnd, streamIds);
  const auto sizeIndicesEncodingType =
      static_cast<EncodingType>(static_cast<uint8_t>(*payload++));
  // streamOffsets temporarily holds the per-slot unique stream index; it is
  // overwritten with the reconstructed body offset below.
  decodeSection(sizeIndicesEncodingType, payload, trailerEnd, streamOffsets);
  const auto uniqueSizesEncodingType =
      static_cast<EncodingType>(static_cast<uint8_t>(*payload++));
  decodeSection(
      uniqueSizesEncodingType, payload, trailerEnd, uniqueStreamSizes);
  NIMBLE_CHECK_EQ(
      payload,
      trailerEnd,
      "Trailer size mismatch: read {} bytes, expected {}",
      payload - trailerStart,
      trailerSize);
  NIMBLE_CHECK_EQ(
      streamIds.size(),
      streamOffsets.size(),
      "Trailer streamIds/sizeIndices section count mismatch");

  const auto numStreams = streamIds.size();
  const auto numUniqueStreams = uniqueStreamSizes.size();
  streamSizes.resize(numStreams);
  // Expand per-slot sizes from the unique-size table.
  for (size_t i = 0; i < numStreams; ++i) {
    const auto uniqueStreamIndex = streamOffsets[i];
    NIMBLE_CHECK_LT(
        uniqueStreamIndex,
        numUniqueStreams,
        "Unique stream index out of range");
    streamSizes[i] = uniqueStreamSizes[uniqueStreamIndex];
  }
  // Transform uniqueStreamSizes in place into exclusive prefix-sum body
  // offsets.
  uint32_t nextBodyOffset{0};
  for (size_t i = 0; i < numUniqueStreams; ++i) {
    const auto sizeValue = uniqueStreamSizes[i];
    uniqueStreamSizes[i] = nextBodyOffset;
    nextBodyOffset += sizeValue;
  }
  // Expand per-slot body offsets from the unique-offset table.
  // streamOffsets[i] currently holds the unique stream index; read it, then
  // overwrite with the resolved offset.
  for (size_t i = 0; i < numStreams; ++i) {
    streamOffsets[i] = uniqueStreamSizes[streamOffsets[i]];
  }
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

void readTrailerStreamMetadata(
    const char* end,
    std::vector<uint32_t>& streamIds,
    std::vector<uint32_t>& streamSizes) {
  const uint32_t trailerSize = readTrailerSize(end);
  NIMBLE_CHECK_LE(
      trailerSize,
      kMaxTrailerBytes,
      "Trailer size sanity cap exceeded (likely buffer corruption)");
  const char* trailerStart = end - sizeof(uint32_t) - trailerSize;
  decodeTrailerStreamMetadata(
      trailerStart, trailerSize, streamIds, streamSizes);
}

std::pair<std::vector<uint32_t>, std::vector<uint32_t>>
readTrailerStreamMetadata(const char* end) {
  std::vector<uint32_t> streamIds;
  std::vector<uint32_t> streamSizes;
  readTrailerStreamMetadata(end, streamIds, streamSizes);
  return {std::move(streamIds), std::move(streamSizes)};
}

void readTrailerStreamMetadata(
    const char* end,
    std::vector<uint32_t>& streamIds,
    std::vector<uint32_t>& streamOffsets,
    std::vector<uint32_t>& streamSizes,
    std::vector<uint32_t>& uniqueStreamSizesScratch) {
  const uint32_t trailerSize = readTrailerSize(end);
  NIMBLE_CHECK_LE(
      trailerSize,
      kMaxTrailerBytes,
      "Trailer size sanity cap exceeded (likely buffer corruption)");
  const char* trailerStart = end - sizeof(uint32_t) - trailerSize;
  decodeTrailerStreamMetadata(
      trailerStart,
      trailerSize,
      streamIds,
      streamOffsets,
      streamSizes,
      uniqueStreamSizesScratch);
}

std::pair<std::vector<uint32_t>, std::vector<uint32_t>>
readTrailerStreamMetadata(const folly::IOBuf& input) {
  // Fast path: trailer fits in the tail segment.
  const auto* tail = input.prev();
  if (tail->length() >= sizeof(uint32_t)) {
    const auto* tailEnd =
        reinterpret_cast<const char*>(tail->data()) + tail->length();
    const uint32_t trailerSize = readTrailerSize(tailEnd);
    if (tail->length() >= sizeof(uint32_t) + trailerSize) {
      return readTrailerStreamMetadata(tailEnd);
    }
  }

  // Fallback: trailer spans chain boundary — use cursor + pull().
  const auto totalLength = input.computeChainDataLength();
  folly::io::Cursor trailerCursor(&input);
  trailerCursor.skip(totalLength - sizeof(uint32_t));
  const uint32_t trailerSize = trailerCursor.read<uint32_t>();
  NIMBLE_CHECK_LE(
      trailerSize,
      kMaxTrailerBytes,
      "Trailer size sanity cap exceeded (likely buffer corruption)");
  // Bound trailerSize against the actual IOBuf chain length so a corrupted
  // value cannot drive a huge std::string allocation.
  NIMBLE_CHECK_LE(
      static_cast<uint64_t>(trailerSize) + sizeof(uint32_t),
      totalLength,
      "Trailer size exceeds IOBuf chain length");

  folly::io::Cursor sizesCursor(&input);
  sizesCursor.skip(totalLength - sizeof(uint32_t) - trailerSize);
  std::string trailerBuf(trailerSize, '\0');
  sizesCursor.pull(trailerBuf.data(), trailerSize);
  std::vector<uint32_t> streamIds;
  std::vector<uint32_t> streamSizes;
  decodeTrailerStreamMetadata(
      trailerBuf.data(), trailerSize, streamIds, streamSizes);
  return {std::move(streamIds), std::move(streamSizes)};
}

size_t estimateTrailerSize(
    size_t numStreams,
    EncodingType indicesEncodingType,
    EncodingType sizesEncodingType) {
  // [indicesEncType:1B][indicesPayload][sizesEncType:1B][sizesPayload]
  // [trailer_size:u32]
  // Worst case: every stream slot is non-zero, so both axes carry numStreams
  // entries.
  return sizeof(uint8_t) +
      estimateSectionSize(indicesEncodingType, numStreams) + sizeof(uint8_t) +
      estimateSectionSize(sizesEncodingType, numStreams) + sizeof(uint32_t);
}

size_t estimateTrailerSize(
    size_t numPresentStreams,
    size_t numUniqueStreams,
    EncodingType streamIdsEncodingType,
    EncodingType sizeIndicesEncodingType,
    EncodingType uniqueSizesEncodingType) {
  // [streamIdsEncType:1B][streamIdsPayload]
  // [sizeIndicesEncType:1B][sizeIndicesPayload]
  // [uniqueSizesEncType:1B][uniqueSizesPayload][trailer_size:u32]
  return sizeof(uint8_t) +
      estimateSectionSize(streamIdsEncodingType, numPresentStreams) +
      sizeof(uint8_t) +
      estimateSectionSize(sizeIndicesEncodingType, numPresentStreams) +
      sizeof(uint8_t) +
      estimateSectionSize(uniqueSizesEncodingType, numUniqueStreams) +
      sizeof(uint32_t);
}

std::vector<std::string_view> parseStreams(
    const char* pos,
    const char* end,
    SerializationVersion version,
    velox::memory::MemoryPool* pool) {
  NIMBLE_CHECK_NOT_NULL(pool, "Memory pool cannot be null");

  std::vector<std::string_view> streams;

  if (nonLegacyFormat(version) && !isTabletVersion(version)) {
    // Walk the sparse (streamIds, streamSizes) trailer and place each
    // stream at its offset slot. Gaps remain empty string_views. Legacy
    // kLegacyCompact blobs are decoded via the frozen legacy reader.
    auto [streamIds, streamSizes] = usesLegacyTrailer(version)
        ? legacy::readLegacyTrailerStreamMetadata(end)
        : readTrailerStreamMetadata(end);
    if (!streamIds.empty()) {
      streams.resize(streamIds.back() + 1);
      for (size_t i = 0; i < streamIds.size(); ++i) {
        // NOLINTNEXTLINE(facebook-hte-LocalUncheckedArrayBounds)
        streams[streamIds[i]] = std::string_view(pos, streamSizes[i]);
        pos += streamSizes[i];
      }
    }
  } else {
    NIMBLE_CHECK_EQ(
        version,
        SerializationVersion::kLegacy,
        "Unexpected serialization version");
    // kLegacy format: inline [size:u32][data]...
    while (pos < end) {
      streams.emplace_back(readStream<false>(pos));
    }
  }

  return streams;
}

} // namespace facebook::nimble::serde::detail
