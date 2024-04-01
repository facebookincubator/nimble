// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/Serializer.h"
#include <zstd.h>
#include <zstd_errors.h>
#include "dwio/alpha/common/EncodingPrimitives.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/velox/SchemaBuilder.h"
#include "dwio/alpha/velox/SchemaUtils.h"

namespace facebook::alpha {

namespace {

ScalarKind getScalarKind(const Type& type) {
  switch (type.kind()) {
    case Kind::Scalar:
      return type.asScalar().scalarKind();
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
  auto data = buffer_.data();
  encoding::writeUint32(ranges.size(), data);
  writer_->write(vector, ranges);
  uint32_t lastStream = 0xffffffff;
  writer_->flush([this, &lastStream](
                     const StreamDescriptor& streamDescriptor,
                     std::span<const bool>* nonNulls,
                     std::string_view values) {
    auto stream = streamDescriptor.offset();
    // Current implementation has strong assumption that schema traversal is
    // pre-order, hence handles types with offsets in increasing order. This
    // assumption may not be true if schema changes based on data shape (ie.
    // flatmap). We need to implement different ways of maintaining the order if
    // need to support that.
    ALPHA_CHECK(
        lastStream + 1 <= stream,
        fmt::format("unexpected stream offset {}", stream));
    // We expect streams to arrive in ascending offset order. If there is an
    // offset gap, it means that those streams are missing (and will not show up
    // later on), so we fill zeros for all of the missing streams.
    writeMissingStreams(buffer_, lastStream, stream);
    lastStream = stream;

    ALPHA_CHECK(
        !nonNulls ||
            std::all_of(
                nonNulls->begin(),
                nonNulls->end(),
                [](bool notNull) { return notNull; }),
        "nulls not supported");
    auto oldSize = buffer_.size();
    auto scalarKind = streamDescriptor.scalarKind();
    if (scalarKind == ScalarKind::String || scalarKind == ScalarKind::Binary) {
      // TODO: handle string compression
      auto strData = reinterpret_cast<const std::string_view*>(values.data());
      auto strDataEnd = reinterpret_cast<const std::string_view*>(values.end());
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
      auto size = values.size();
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
                pos, size, values.data(), size, options_.compressionLevel);
            if (ZSTD_isError(ret)) {
              ALPHA_ASSERT(
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
            ALPHA_NOT_SUPPORTED(fmt::format(
                "Unsupported compression {}", toString(compression)));
        }
      }

      if (writeUncompressed) {
        auto pos = buffer_.data() + oldSize;
        encoding::writeUint32(size + 1, pos);
        encoding::writeChar(
            static_cast<int8_t>(CompressionType::Uncompressed), pos);
        std::copy(values.data(), values.end(), pos);
      }
    }
  });

  // Write missing streams similar to above
  writeMissingStreams(buffer_, lastStream, context_.schemaBuilder.nodeCount());
  return {buffer_.data(), buffer_.size()};
}

} // namespace facebook::alpha
