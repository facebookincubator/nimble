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

#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/EncodingSelection.h"
#include "folly/io/IOBuf.h"

namespace facebook::nimble {

struct CompressionResult {
  CompressionType compressionType;
  std::optional<Vector<char>> buffer;
};

struct ICompressor {
  virtual CompressionResult compress(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      DataType dataType,
      int bitWidth,
      const CompressionPolicy& compressionPolicy) = 0;

  virtual Vector<char> uncompress(
      velox::memory::MemoryPool& memoryPool,
      CompressionType compressionType,
      DataType dataType,
      std::string_view data) = 0;

  virtual std::optional<size_t> uncompressedSize(
      std::string_view data) const = 0;

  virtual CompressionType compressionType() = 0;

  virtual ~ICompressor() = default;
};

class Compression {
 public:
  static CompressionResult compress(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      DataType dataType,
      int bitWidth,
      const CompressionPolicy& compressionPolicy);

  static Vector<char> uncompress(
      velox::memory::MemoryPool& memoryPool,
      CompressionType compressionType,
      DataType dataType,
      std::string_view data);

  static std::optional<size_t> uncompressedSize(
      CompressionType compressionType,
      std::string_view data);

  static void registerCompressor(std::unique_ptr<ICompressor>&& compressor);
};

// Encodings using compression repeat the same pattern, which involves trying to
// compress the data, and throwing it away if compression policy decides to
// throw it away. This class tries to extract this common logic into one place.
// Note: There are actually two sub-patterns, therefore, there are two CTors in
// this class. More on this below.
template <typename T>
class CompressionEncoder {
 public:
  // This CTor handles the sub-pattern where the source data is already encoded
  // correctly, so no extra encoding is needed.
  CompressionEncoder(
      velox::memory::MemoryPool& memoryPool,
      const CompressionPolicy& compressionPolicy,
      DataType dataType,
      std::string_view uncompressedBuffer,
      int bitWidth = 0)
      : dataSize_{uncompressedBuffer.size()},
        compressionType_{CompressionType::Uncompressed} {
    if (uncompressedBuffer.size() == 0 ||
        compressionPolicy.compression().compressionType ==
            CompressionType::Uncompressed) {
      // No compression, just use the original buffer.
      data_ = uncompressedBuffer;
      return;
    }

    auto compressionResult = Compression::compress(
        memoryPool, uncompressedBuffer, dataType, bitWidth, compressionPolicy);

    if (compressionResult.compressionType == CompressionType::Uncompressed) {
      // Compression declined. Use the original buffer.
      data_ = uncompressedBuffer;
      return;
    }

    // Compression accepted. Use the compressed buffer.
    compressed_ = std::move(compressionResult.buffer);
    data_ = {compressed_->data(), compressed_->size()};
    dataSize_ = compressed_->size();
    compressionType_ = compressionResult.compressionType;
  }

  // This CTor handles the sub-pattern where the source data requires special
  // encoding before it is compressed/written.
  // Note that in this case, the target buffer for the newly encoded data is
  // different if compression is applied (it is written to a temp buffer), or if
  // compression is skipped (written directly to the stream buffer).
  CompressionEncoder(
      velox::memory::MemoryPool& memoryPool,
      const CompressionPolicy& compressionPolicy,
      DataType dataType,
      int bitWidth,
      size_t uncompressedSize,
      std::function<std::span<char>()> allocateUncompressedBuffer,
      std::function<void(char*&)> encoder)
      : dataSize_{uncompressedSize},
        compressionType_{CompressionType::Uncompressed},
        encoder_{encoder} {
    if (uncompressedSize == 0 ||
        compressionPolicy.compression().compressionType ==
            CompressionType::Uncompressed) {
      // No compression. Do not encode the data yet. It will be encoded later
      // (in write()) directly into the output buffer.
      return;
    }

    // Compression is attempted. Encode the data before compressing it, into a
    // temp buffer.
    auto uncompressed = allocateUncompressedBuffer();
    char* pos = uncompressed.data();
    encoder_(pos);
    auto compressionResult = Compression::compress(
        memoryPool,
        {uncompressed.data(), uncompressed.size()},
        dataType,
        bitWidth,
        compressionPolicy);

    if (compressionResult.compressionType == CompressionType::Uncompressed) {
      // Compression declined. Since we already encoded the data, remember the
      // temp buffer for later.
      // Note: data size is still the same uncompressed size.
      data_ = uncompressed;
      return;
    }

    // Compression accepted. Use the compressed buffer.
    compressed_ = std::move(compressionResult.buffer);
    data_ = {compressed_->data(), compressed_->size()};
    dataSize_ = compressed_->size();
    compressionType_ = compressionResult.compressionType;
  }

  size_t getSize() {
    return dataSize_;
  }

  void write(char*& pos) {
    if (!data_.has_value()) {
      // If we are here, it means we handle uncompressed data that needs to be
      // encoded directly into the target buffer.
      encoder_(pos);
      return;
    }

    if (data_->data() == nullptr) {
      return;
    }

    std::copy(data_->begin(), data_->end(), pos);
    pos += data_->size();
  }

  CompressionType compressionType() {
    return compressionType_;
  }

 private:
  size_t dataSize_;
  std::optional<std::span<const char>> data_;
  std::optional<Vector<char>> compressed_;
  CompressionType compressionType_;
  std::function<void(char*&)> encoder_;
};

} // namespace facebook::nimble
