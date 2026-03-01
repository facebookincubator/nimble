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

#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/EncodingFactory.h"

namespace facebook::nimble::serde {

StreamData::StreamData(
    ScalarKind kind,
    bool encodingEnabled,
    std::string_view data,
    velox::memory::MemoryPool* pool)
    : kind_{kind}, pool_{pool}, encodingEnabled_{encodingEnabled} {
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
  uint32_t index = 0;
  while (pos_ < end_ && index < count) {
    output[index++] = encoding::readString(pos_);
  }
  return index;
}

void StreamData::reset(std::string_view data, bool encodingEnabled) {
  readRows_ = 0;
  encoding_.reset();
  stringBuffers_.clear();
  encodingEnabled_ = encodingEnabled;
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
  // For string types, provide a stringBufferFactory that allocates separate
  // buffers using velox::AlignedBuffer for memory tracking.
  encoding_ = EncodingFactory::decode(*pool_, data, [this](uint32_t size) {
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

StreamDataReader::StreamDataReader(const DeserializerOptions& options)
    : options_{options} {}

uint32_t StreamDataReader::initialize(std::string_view data) {
  pos_ = data.data();
  end_ = data.end();
  if (options_.hasVersionHeader()) {
    const auto version = static_cast<SerializationVersion>(*pos_);
    NIMBLE_CHECK_LE(
        version,
        SerializationVersion::kSparseEncoded,
        "Unsupported version {}",
        version);
    // Verify the version read from serialized data matches options.
    NIMBLE_CHECK_EQ(
        version,
        *options_.version,
        "Version mismatch: data has version {}, options expect {}",
        version,
        *options_.version);
    ++pos_;
  }
  return encoding::readUint32(pos_);
}

void StreamDataReader::iterateStreams(
    const std::function<void(uint32_t offset, std::string_view data)>&
        callback) {
  if (options_.sparseFormat()) {
    // Sparse format: [stream_count][offsets...][data...]
    NIMBLE_CHECK_LE(
        pos_ + sizeof(uint32_t), end_, "Truncated data: missing stream count");
    const uint32_t streamCount = encoding::readUint32(pos_);

    // Sanity check: stream count should be reasonable.
    const size_t remainingBytes = end_ - pos_;
    NIMBLE_CHECK_LE(
        streamCount,
        remainingBytes / sizeof(uint32_t),
        "Invalid stream count exceeds remaining data");

    std::vector<uint32_t> offsets(streamCount);
    for (uint32_t i = 0; i < streamCount; ++i) {
      offsets[i] = encoding::readUint32(pos_);
    }

    for (uint32_t i = 0; i < streamCount; ++i) {
      auto streamData = encoding::readString(pos_);
      callback(offsets[i], streamData);
    }
  } else {
    NIMBLE_CHECK(options_.denseFormat());
    // Dense format (version 0): streams in order with zeros for missing.
    uint32_t offset = 0;
    while (pos_ < end_) {
      auto streamData = encoding::readString(pos_);
      if (!streamData.empty()) {
        callback(offset, streamData);
      }
      ++offset;
    }
  }

  NIMBLE_CHECK_EQ(pos_, end_, "Unexpected trailing data");
}

} // namespace facebook::nimble::serde
