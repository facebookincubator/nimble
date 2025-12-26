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

#include <string_view>
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "folly/io/IOBuf.h"

namespace facebook::nimble {

class MetadataBuffer {
 public:
  MetadataBuffer(
      velox::memory::MemoryPool& pool,
      std::string_view ref,
      CompressionType type = CompressionType::Uncompressed);

  MetadataBuffer(
      velox::memory::MemoryPool& pool,
      const folly::IOBuf& iobuf,
      size_t offset,
      size_t length,
      CompressionType type = CompressionType::Uncompressed);

  MetadataBuffer(
      velox::memory::MemoryPool& pool,
      const folly::IOBuf& iobuf,
      CompressionType type = CompressionType::Uncompressed);

  std::string_view content() const {
    return {buffer_.data(), buffer_.size()};
  }

 private:
  Vector<char> buffer_;
};

class Section {
 public:
  explicit Section(MetadataBuffer&& buffer) : buffer_{std::move(buffer)} {}

  std::string_view content() const {
    return buffer_.content();
  }
  explicit operator std::string_view() const {
    return content();
  }

  /// Returns a reference to the underlying MetadataBuffer.
  const MetadataBuffer& buffer() const {
    return buffer_;
  }

 private:
  MetadataBuffer buffer_;
};

class MetadataSection {
 public:
  MetadataSection(
      uint64_t offset,
      uint32_t size,
      CompressionType compressionType)
      : offset_{offset}, size_{size}, compressionType_{compressionType} {}

  MetadataSection()
      : offset_{0}, size_{0}, compressionType_{CompressionType::Uncompressed} {}

  uint64_t offset() const {
    return offset_;
  }

  uint32_t size() const {
    return size_;
  }

  CompressionType compressionType() const {
    return compressionType_;
  }

 private:
  uint64_t offset_;
  uint32_t size_;
  CompressionType compressionType_;
};

} // namespace facebook::nimble
