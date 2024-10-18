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

#include <span>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/velox/BufferGrowthPolicy.h"
#include "dwio/nimble/velox/OrderedRanges.h"
#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/StreamData.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::nimble {

struct InputBufferGrowthStats {
  std::atomic<uint64_t> count{0};
  // realloc bytes would be interesting, but requires a bit more
  // trouble to get.
  std::atomic<uint64_t> itemCount{0};
};

struct FieldWriterContext {
  class LocalDecodedVector;

  explicit FieldWriterContext(
      velox::memory::MemoryPool& memoryPool,
      std::unique_ptr<velox::memory::MemoryReclaimer> reclaimer = nullptr,
      std::function<void(void)> vectorDecoderVisitor = []() {})
      : bufferMemoryPool{memoryPool.addLeafChild(
            "field_writer_buffer",
            true,
            std::move(reclaimer))},
        inputBufferGrowthPolicy{
            DefaultInputBufferGrowthPolicy::withDefaultRanges()},
        vectorDecoderVisitor(std::move(vectorDecoderVisitor)) {
    resetStringBuffer();
  }

  std::shared_ptr<velox::memory::MemoryPool> bufferMemoryPool;
  SchemaBuilder schemaBuilder;

  folly::F14FastSet<uint32_t> flatMapNodeIds;
  folly::F14FastSet<uint32_t> dictionaryArrayNodeIds;
  folly::F14FastSet<uint32_t> deduplicatedMapNodeIds;

  std::unique_ptr<InputBufferGrowthPolicy> inputBufferGrowthPolicy;
  InputBufferGrowthStats inputBufferGrowthStats;

  std::function<void(const TypeBuilder&, std::string_view, const TypeBuilder&)>
      flatmapFieldAddedEventHandler;

  std::function<void(const TypeBuilder&)> typeAddedHandler =
      [](const TypeBuilder&) {};

  std::function<void(void)> vectorDecoderVisitor;

  LocalDecodedVector getLocalDecodedVector();
  velox::SelectivityVector& getSelectivityVector(velox::vector_size_t size);

  Buffer& stringBuffer() {
    return *buffer_;
  }

  // Reset writer context for use by next stripe.
  void resetStringBuffer() {
    buffer_ = std::make_unique<Buffer>(*bufferMemoryPool);
  }

  const std::vector<std::unique_ptr<StreamData>>& streams() {
    return streams_;
  }

  template <typename T>
  NullsStreamData& createNullsStreamData(
      const StreamDescriptorBuilder& descriptor) {
    return static_cast<NullsStreamData&>(*streams_.emplace_back(
        std::make_unique<NullsStreamData>(*bufferMemoryPool, descriptor)));
  }

  template <typename T>
  ContentStreamData<T>& createContentStreamData(
      const StreamDescriptorBuilder& descriptor) {
    return static_cast<ContentStreamData<T>&>(*streams_.emplace_back(
        std::make_unique<ContentStreamData<T>>(*bufferMemoryPool, descriptor)));
  }

  template <typename T>
  NullableContentStreamData<T>& createNullableContentStreamData(
      const StreamDescriptorBuilder& descriptor) {
    return static_cast<NullableContentStreamData<T>&>(
        *streams_.emplace_back(std::make_unique<NullableContentStreamData<T>>(
            *bufferMemoryPool, descriptor)));
  }

 private:
  std::unique_ptr<velox::DecodedVector> getDecodedVector();
  void releaseDecodedVector(std::unique_ptr<velox::DecodedVector>&& vector);

  std::unique_ptr<Buffer> buffer_;
  std::vector<std::unique_ptr<velox::DecodedVector>> decodedVectorPool_;
  std::unique_ptr<velox::SelectivityVector> selectivity_;
  std::vector<std::unique_ptr<StreamData>> streams_;
};

using OrderedRanges = range_helper::OrderedRanges<velox::vector_size_t>;

class FieldWriter {
 public:
  FieldWriter(
      FieldWriterContext& context,
      std::shared_ptr<TypeBuilder> typeBuilder)
      : context_{context}, typeBuilder_{std::move(typeBuilder)} {}

  virtual ~FieldWriter() = default;

  // Writes the vector to internal buffers.
  virtual void write(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges) = 0;

  // Clears interanl state and any accumulated data in internal buffers.
  virtual void reset() = 0;

  // Called when all writes are done, allowing field writers to finalize
  // internal state.
  virtual void close() {}

  const std::shared_ptr<TypeBuilder>& typeBuilder() {
    return typeBuilder_;
  }

  static std::unique_ptr<FieldWriter> create(
      FieldWriterContext& context,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type);

  static std::unique_ptr<FieldWriter> create(
      FieldWriterContext& context,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type,
      std::function<void(const TypeBuilder&)> typeAddedHandler);

 protected:
  FieldWriterContext& context_;
  std::shared_ptr<TypeBuilder> typeBuilder_;

  FieldWriterContext::LocalDecodedVector decode(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges);
};

} // namespace facebook::nimble
