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

#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "dwio/nimble/tablet/Compression.h"
#include "folly/io/Cursor.h"

namespace facebook::nimble {

namespace {

folly::IOBuf
cloneAndCoalesce(const folly::IOBuf& src, size_t offset, size_t size) {
  folly::io::Cursor cursor(&src);
  NIMBLE_CHECK_GE(cursor.totalLength(), offset, "Offset out of range");
  cursor.skip(offset);
  NIMBLE_CHECK_GE(cursor.totalLength(), size, "Size out of range");
  folly::IOBuf result;
  cursor.clone(result, size);
  result.coalesceWithHeadroomTailroom(0, 0);
  return result;
}

std::string_view toStringView(const folly::IOBuf& buf) {
  return {reinterpret_cast<const char*>(buf.data()), buf.length()};
}

} // namespace

MetadataBuffer::MetadataBuffer(
    velox::memory::MemoryPool& pool,
    std::string_view ref,
    CompressionType type)
    : buffer_{&pool} {
  switch (type) {
    case CompressionType::Uncompressed: {
      buffer_.resize(ref.size());
      std::copy(ref.cbegin(), ref.cend(), buffer_.begin());
      break;
    }
    case CompressionType::Zstd: {
      buffer_ = ZstdCompression::uncompress(pool, ref);
      break;
    }
    default:
      NIMBLE_UNREACHABLE("Unexpected stream compression type: {}", type);
  }
}

MetadataBuffer::MetadataBuffer(
    velox::memory::MemoryPool& pool,
    const folly::IOBuf& iobuf,
    size_t offset,
    size_t length,
    CompressionType type)
    : buffer_{&pool} {
  switch (type) {
    case CompressionType::Uncompressed: {
      buffer_.resize(length);
      folly::io::Cursor cursor(&iobuf);
      cursor.skip(offset);
      cursor.pull(buffer_.data(), length);
      break;
    }
    case CompressionType::Zstd: {
      auto compressed = cloneAndCoalesce(iobuf, offset, length);
      buffer_ = ZstdCompression::uncompress(pool, toStringView(compressed));
      break;
    }
    default:
      NIMBLE_UNREACHABLE("Unexpected stream compression type: {}", type);
  }
}

MetadataBuffer::MetadataBuffer(
    velox::memory::MemoryPool& pool,
    const folly::IOBuf& iobuf,
    CompressionType type)
    : MetadataBuffer{pool, iobuf, 0, iobuf.computeChainDataLength(), type} {}

} // namespace facebook::nimble
