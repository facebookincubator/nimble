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

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/velox/BufferGrowthPolicy.h"
#include "dwio/nimble/velox/OrderedRanges.h"
#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/StreamData.h"
#include "dwio/nimble/velox/stats/ColumnStatsUtils.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::nimble {

struct InputBufferGrowthStats {
  std::atomic<uint64_t> count{0};
  // realloc bytes would be interesting, but requires a bit more
  // trouble to get.
  std::atomic<uint64_t> itemCount{0};
};

// A pool of decoding contexts.  Decoding contexts are used to decode a vector
// and its associated selectivity vector.  The pool is used to avoid
// repeated allocations of decoding context.
class DecodingContextPool {
 public:
  class DecodingContext {
   public:
    explicit DecodingContext(
        DecodingContextPool& pool,
        std::unique_ptr<velox::DecodedVector> decodedVector,
        std::unique_ptr<velox::SelectivityVector> selectivityVector);

    ~DecodingContext();

    velox::DecodedVector& decode(
        const velox::VectorPtr& vector,
        const range_helper::OrderedRanges<velox::vector_size_t>& ranges);

   private:
    DecodingContextPool& pool_;
    std::unique_ptr<velox::DecodedVector> decodedVector_;
    std::unique_ptr<velox::SelectivityVector> selectivityVector_;
  };

  explicit DecodingContextPool(
      std::function<void(void)> vectorDecoderVisitor = []() {});

  DecodingContext reserveContext();
  size_t size() const;

 private:
  std::mutex mutex_;
  std::vector<std::pair<
      std::unique_ptr<velox::DecodedVector>,
      std::unique_ptr<velox::SelectivityVector>>>
      pool_;
  std::function<void(void)> vectorDecoderVisitor_;

  void addContext(
      std::unique_ptr<velox::DecodedVector> decodedVector,
      std::unique_ptr<velox::SelectivityVector> selectivityVector);
};

class MemoryPoolHolder {
 public:
  velox::memory::MemoryPool& operator*() {
    return *poolPtr_;
  }

  velox::memory::MemoryPool* get() const {
    return poolPtr_;
  }

  velox::memory::MemoryPool* operator->() const {
    return get();
  }

  template <typename Creator>
  static MemoryPoolHolder create(
      velox::memory::MemoryPool& pool,
      const Creator& creator) {
    MemoryPoolHolder holder;
    if (pool.isLeaf()) {
      holder.poolPtr_ = &pool;
    } else {
      holder.ownedPool_ = creator(pool);
      holder.poolPtr_ = holder.ownedPool_.get();
    }
    return holder;
  }

 private:
  MemoryPoolHolder() = default;

  std::shared_ptr<velox::memory::MemoryPool> ownedPool_;
  velox::memory::MemoryPool* poolPtr_{nullptr};
};

/// NOTE: this class is not thread-safe.
class FieldWriterContext {
 public:
  explicit FieldWriterContext(
      velox::memory::MemoryPool& memoryPool,
      std::unique_ptr<velox::memory::MemoryReclaimer> reclaimer = nullptr,
      std::function<void(void)> vectorDecoderVisitor = []() {})
      : bufferMemoryPool_{MemoryPoolHolder::create(
            memoryPool,
            [&](auto& pool) {
              return pool.addLeafChild(
                  "field_writer_buffer", true, std::move(reclaimer));
            })},
        inputBufferGrowthPolicy_{
            DefaultInputBufferGrowthPolicy::withDefaultRanges()},
        decodingContextPool_{std::move(vectorDecoderVisitor)} {
    resetStringBuffer();
  }

  inline MemoryPoolHolder& bufferMemoryPool() {
    return bufferMemoryPool_;
  }

  inline const MemoryPoolHolder& bufferMemoryPool() const {
    return bufferMemoryPool_;
  }

  inline std::mutex& flatMapSchemaMutex() {
    return flatMapSchemaMutex_;
  }

  inline SchemaBuilder& schemaBuilder() {
    return schemaBuilder_;
  }

  inline const SchemaBuilder& schemaBuilder() const {
    return schemaBuilder_;
  }

  inline bool hasFlatMapNodeId(uint32_t nodeId) const {
    return flatMapNodeIds_.contains(nodeId);
  }

  inline void clearAndReserveFlatMapNodeIds(size_t size) {
    flatMapNodeIds_.clear();
    flatMapNodeIds_.reserve(size);
  }

  inline void addFlatMapNodeId(uint32_t nodeId) {
    flatMapNodeIds_.insert(nodeId);
  }

  inline folly::F14FastSet<uint32_t>& dictionaryArrayNodeIds() {
    return dictionaryArrayNodeIds_;
  }

  inline const folly::F14FastSet<uint32_t>& dictionaryArrayNodeIds() const {
    return dictionaryArrayNodeIds_;
  }

  inline void clearAndReserveDictionaryArrayNodeIds(size_t size) {
    dictionaryArrayNodeIds_.clear();
    dictionaryArrayNodeIds_.reserve(size);
  }

  inline bool hasDictionaryArrayNodeId(uint32_t nodeId) const {
    return dictionaryArrayNodeIds_.contains(nodeId);
  }

  inline folly::F14FastSet<uint32_t>& deduplicatedMapNodeIds() {
    return deduplicatedMapNodeIds_;
  }

  inline bool hasDeduplicatedMapNodeId(uint32_t nodeId) const {
    return deduplicatedMapNodeIds_.contains(nodeId);
  }

  inline const folly::F14FastSet<uint32_t>& deduplicatedMapNodeIds() const {
    return deduplicatedMapNodeIds_;
  }

  inline void clearAndReserveDeduplicatedMapNodeIds(size_t size) {
    deduplicatedMapNodeIds_.clear();
    deduplicatedMapNodeIds_.reserve(size);
  }

  inline bool ignoreTopLevelNulls() const {
    return ignoreTopLevelNulls_;
  }

  inline void setIgnoreTopLevelNulls(bool value) {
    ignoreTopLevelNulls_ = value;
  }

  inline std::unique_ptr<InputBufferGrowthPolicy>& inputBufferGrowthPolicy() {
    return inputBufferGrowthPolicy_;
  }

  inline const std::unique_ptr<InputBufferGrowthPolicy>&
  inputBufferGrowthPolicy() const {
    return inputBufferGrowthPolicy_;
  }

  inline InputBufferGrowthStats& inputBufferGrowthStats() {
    return inputBufferGrowthStats_;
  }

  inline const InputBufferGrowthStats& inputBufferGrowthStats() const {
    return inputBufferGrowthStats_;
  }

  inline ColumnStats& columnStats(offset_size offset) {
    return columnStats_[offset];
  }

  inline std::unordered_map<offset_size, ColumnStats>& columnStats() {
    return columnStats_;
  }

  void handleFlatmapFieldAddEvent(
      const TypeBuilder& typeBuilder,
      std::string_view flatmapKey,
      const TypeBuilder& flatmapTypeBuilder) {
    if (flatmapFieldAddedEventHandler_) {
      flatmapFieldAddedEventHandler_(
          typeBuilder, flatmapKey, flatmapTypeBuilder);
    }
  }

  inline void setFlatmapFieldAddedEventHandler(
      std::function<
          void(const TypeBuilder&, std::string_view, const TypeBuilder&)>
          handler) {
    flatmapFieldAddedEventHandler_ = std::move(handler);
  }

  inline void handleAddedType(const TypeBuilder& typeBuilder) const {
    typeAddedHandler_(typeBuilder);
  }

  inline void setTypeAddedHandler(
      std::function<void(const TypeBuilder&)>&& handler) {
    typeAddedHandler_ = std::move(handler);
  }

  inline DecodingContextPool::DecodingContext decodingContext() {
    return decodingContextPool_.reserveContext();
  }

  inline Buffer& stringBuffer() {
    return *buffer_;
  }

  inline void resetStringBuffer() {
    buffer_ = std::make_unique<Buffer>(*bufferMemoryPool_);
  }

  inline const std::vector<std::unique_ptr<StreamData>>& streams() {
    return streams_;
  }

  inline NullsStreamData& createNullsStreamData(
      const StreamDescriptorBuilder& descriptor) {
    return static_cast<NullsStreamData&>(*streams_.emplace_back(
        std::make_unique<NullsStreamData>(
            *bufferMemoryPool_, descriptor, *inputBufferGrowthPolicy_)));
  }

  template <typename T>
  ContentStreamData<T>& createContentStreamData(
      const StreamDescriptorBuilder& descriptor) {
    return static_cast<ContentStreamData<T>&>(*streams_.emplace_back(
        std::make_unique<ContentStreamData<T>>(
            *bufferMemoryPool_, descriptor, *inputBufferGrowthPolicy_)));
  }

  template <typename T>
  NullableContentStreamData<T>& createNullableContentStreamData(
      const StreamDescriptorBuilder& descriptor) {
    return static_cast<NullableContentStreamData<T>&>(*streams_.emplace_back(
        std::make_unique<NullableContentStreamData<T>>(
            *bufferMemoryPool_, descriptor, *inputBufferGrowthPolicy_)));
  }

 protected:
  MemoryPoolHolder bufferMemoryPool_;
  std::mutex flatMapSchemaMutex_;
  SchemaBuilder schemaBuilder_;

  folly::F14FastSet<uint32_t> flatMapNodeIds_;
  folly::F14FastSet<uint32_t> dictionaryArrayNodeIds_;
  folly::F14FastSet<uint32_t> deduplicatedMapNodeIds_;
  bool ignoreTopLevelNulls_{false};

  std::unique_ptr<InputBufferGrowthPolicy> inputBufferGrowthPolicy_;
  InputBufferGrowthStats inputBufferGrowthStats_;

  std::unordered_map<offset_size, ColumnStats> columnStats_;

  std::function<void(const TypeBuilder&, std::string_view, const TypeBuilder&)>
      flatmapFieldAddedEventHandler_;

  std::function<void(const TypeBuilder&)> typeAddedHandler_{
      [](const TypeBuilder&) {}};

 private:
  std::unique_ptr<Buffer> buffer_;
  DecodingContextPool decodingContextPool_;
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
      const OrderedRanges& ranges,
      folly::Executor* executor = nullptr) = 0;

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
};

} // namespace facebook::nimble
