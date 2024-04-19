/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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
#include "dwio/nimble/velox/Serializer.h"
#include <zstd.h>
#include <zstd_errors.h>
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/SchemaUtils.h"

namespace facebook::nimble {

namespace {

ScalarKind getScalarKind(const Type& type) {
  switch (type.kind()) {
    case Kind::Scalar:
      return type.asScalar().scalarDescriptor().scalarKind();
    case Kind::Row:
    case Kind::Array:
    case Kind::ArrayWithOffsets:
    case Kind::Map:
    case Kind::FlatMap:
      return ScalarKind::Undefined;
  }
}

void writeMissingStreams(
    Vector<char>& buffer,
    uint32_t prevIndex,
    uint32_t endIndex) {
  if (endIndex != prevIndex + 1) {
    buffer.extend((endIndex - prevIndex - 1) * sizeof(uint32_t), 0);
  }
}

} // namespace

std::string_view Serializer::serialize(
    const velox::VectorPtr& vector,
    const OrderedRanges& ranges) {
  buffer_.resize(sizeof(uint32_t));
  auto pos = buffer_.data();
  encoding::writeUint32(ranges.size(), pos);
  writer_->write(vector, ranges);
  uint32_t lastStream = 0xffffffff;

  for (auto& streamData : context_.streams()) {
    auto stream = streamData->descriptor().offset();
    auto nonNulls = streamData->nonNulls();
    auto data = streamData->data();

    if (data.empty() && nonNulls.empty()) {
      continue;
    }

    // Current implementation has strong assumption that schema traversal is
    // pre-order, hence handles types with offsets in increasing order. This
    // assumption may not be true if schema changes based on data shape (ie.
    // flatmap). We need to implement different ways of maintaining the order if
    // need to support that.
    NIMBLE_CHECK(
        lastStream + 1 <= stream,
        fmt::format("unexpected stream offset {}", stream));
    // We expect streams to arrive in ascending offset order. If there is an
    // offset gap, it means that those streams are missing (and will not show up
    // later on), so we fill zeros for all of the missing streams.
    writeMissingStreams(buffer_, lastStream, stream);
    lastStream = stream;

    NIMBLE_CHECK(
        nonNulls.empty() ||
            std::all_of(
                nonNulls.begin(),
                nonNulls.end(),
                [](bool notNull) { return notNull; }),
        "nulls not supported");
    auto oldSize = buffer_.size();
    auto scalarKind = streamData->descriptor().scalarKind();
    if (scalarKind == ScalarKind::String || scalarKind == ScalarKind::Binary) {
      // TODO: handle string compression
      const auto strData =
          reinterpret_cast<const std::string_view*>(data.data());
      const auto strDataEnd =
          reinterpret_cast<const std::string_view*>(data.end());
      uint32_t size = 0;
      for (auto sv = strData; sv < strDataEnd; ++sv) {
        size += (sv->size() + sizeof(uint32_t));
      }
      buffer_.resize(oldSize + size + sizeof(uint32_t));
      auto pos = buffer_.data() + oldSize;
      encoding::writeUint32(size, pos);
      for (auto sv = strData; sv < strDataEnd; ++sv) {
        encoding::writeString(*sv, pos);
      }
    } else {
      // Size prefix + compression type + actual content
      const auto size = data.size();
      buffer_.resize(oldSize + size + sizeof(uint32_t) + 1);

      auto compression = options_.compressionType;
      bool writeUncompressed = true;
      if (compression != CompressionType::Uncompressed &&
          size >= options_.compressionThreshold) {
        auto pos = buffer_.data() + oldSize + sizeof(uint32_t);
        encoding::writeChar(static_cast<int8_t>(compression), pos);
        // TODO: share compression implementation
        switch (compression) {
          case CompressionType::Zstd: {
            auto ret = ZSTD_compress(
                pos, size, data.data(), size, options_.compressionLevel);
            if (ZSTD_isError(ret)) {
              NIMBLE_ASSERT(
                  ZSTD_getErrorCode(ret) ==
                      ZSTD_ErrorCode::ZSTD_error_dstSize_tooSmall,
                  "zstd error");
              // fall back to uncompressed
            } else {
              pos = buffer_.data() + oldSize;
              encoding::writeUint32(ret + 1, pos);
              // reflect the compressed size
              buffer_.resize(oldSize + ret + sizeof(uint32_t) + 1);
              writeUncompressed = false;
            }
            break;
          }
          default:
            NIMBLE_NOT_SUPPORTED(fmt::format(
                "Unsupported compression {}", toString(compression)));
        }
      }

      if (writeUncompressed) {
        auto pos = buffer_.data() + oldSize;
        encoding::writeUint32(size + 1, pos);
        encoding::writeChar(
            static_cast<int8_t>(CompressionType::Uncompressed), pos);
        std::copy(data.data(), data.end(), pos);
      }
    }
  }

  writer_->reset();

  // Write missing streams similar to above
  writeMissingStreams(buffer_, lastStream, context_.schemaBuilder.nodeCount());
  return {buffer_.data(), buffer_.size()};
}

} // namespace facebook::nimble
