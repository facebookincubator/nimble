// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/common/Vector.h"
#include "dwio/alpha/velox/FieldWriter.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/vector/BaseVector.h"

namespace facebook::alpha {

struct SerializerOptions {
  CompressionType compressionType{CompressionType::Uncompressed};
  uint32_t compressionThreshold{0};
  int32_t compressionLevel{0};
};

class Serializer {
 public:
  Serializer(
      SerializerOptions options,
      velox::memory::MemoryPool& pool,
      const std::shared_ptr<const velox::Type>& type)
      : options_{std::move(options)},
        context_{pool},
        writer_{FieldWriter::create(
            context_,
            velox::dwio::common::TypeWithId::create(type))},
        buffer_{context_.bufferMemoryPool.get()} {}

  std::string_view serialize(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges);

  const std::shared_ptr<const TypeBuilder>& alphaSchema() const {
    return context_.schemaBuilder.getRoot();
  }

 private:
  SerializerOptions options_;
  FieldWriterContext context_;
  std::unique_ptr<FieldWriter> writer_;
  Vector<char> buffer_;
};

} // namespace facebook::alpha
