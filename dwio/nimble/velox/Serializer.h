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
#pragma once

#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/velox/FieldWriter.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/vector/BaseVector.h"

namespace facebook::nimble {

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

  const SchemaBuilder& schemaBuilder() const {
    return context_.schemaBuilder;
  }

 private:
  SerializerOptions options_;
  FieldWriterContext context_;
  std::unique_ptr<FieldWriter> writer_;
  Vector<char> buffer_;
};

} // namespace facebook::nimble
