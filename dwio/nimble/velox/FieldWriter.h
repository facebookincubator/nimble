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
#include "dwio/nimble/velox/stats/ColumnStatistics.h"
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

  virtual ~FieldWriterContext() = default;

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

  inline const folly::F14FastSet<uint32_t>& flatMapNodeIds() const {
    return flatMapNodeIds_;
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

  inline bool hasPassthroughFlatMapWrites() const {
    return hasPassthroughFlatMapWrites_;
  }

  inline void setHasPassthroughFlatMapWrites(bool value) {
    hasPassthroughFlatMapWrites_ = value;
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

  inline std::vector<ColumnStatistics*> columnStats() {
    if (!statsFinalized_) {
      return {};
    }
    // TODO: add a build method for this pattern.
    std::vector<ColumnStatistics*> statsViews;
    statsViews.reserve(statsCollectors_.size());
    for (auto& collector : statsCollectors_) {
      // FIXME: don't need this branching.
      statsViews.push_back(
          collector->isShared()
              ? collector->as<SharedStatisticsCollector>()->getBaseStatistics()
              : collector->getStatsView());
    }
    return statsViews;
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

  inline const std::vector<std::pair<uint32_t, std::unique_ptr<StreamData>>>&
  streams() {
    return streams_;
  }

  inline NullsStreamData& createNullsStreamData(
      const StreamDescriptorBuilder& descriptor,
      uint32_t nodeId) {
    return static_cast<NullsStreamData&>(
        *streams_
             .emplace_back(
                 nodeId,
                 std::make_unique<NullsStreamData>(
                     *bufferMemoryPool_, descriptor, *inputBufferGrowthPolicy_))
             .second);
  }

  template <typename T>
  ContentStreamData<T>& createContentStreamData(
      const StreamDescriptorBuilder& descriptor,
      uint32_t nodeId) {
    return static_cast<ContentStreamData<T>&>(
        *streams_
             .emplace_back(
                 nodeId,
                 std::make_unique<ContentStreamData<T>>(
                     *bufferMemoryPool_, descriptor, *inputBufferGrowthPolicy_))
             .second);
  }

  template <typename T>
  NullableContentStreamData<T>& createNullableContentStreamData(
      const StreamDescriptorBuilder& descriptor,
      uint32_t nodeId) {
    return static_cast<NullableContentStreamData<T>&>(
        *streams_
             .emplace_back(
                 nodeId,
                 std::make_unique<NullableContentStreamData<T>>(
                     *bufferMemoryPool_, descriptor, *inputBufferGrowthPolicy_))
             .second);
  }

  nimble::NullableContentStringStreamData&
  createNullableContentStringStreamData(
      const nimble::StreamDescriptorBuilder& descriptor,
      uint32_t nodeId) {
    return static_cast<nimble::NullableContentStringStreamData&>(
        *streams_
             .emplace_back(
                 nodeId,
                 std::make_unique<nimble::NullableContentStringStreamData>(
                     *bufferMemoryPool_,
                     descriptor,
                     *inputBufferGrowthPolicy_,
                     *stringBufferGrowthPolicy_))
             .second);
  }

  inline bool disableSharedStringBuffers() const {
    return disableSharedStringBuffers_;
  }

  void createStatsCollector(
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type) {
    statsCollectors_.emplace_back(StatisticsCollector::create(type));
    for (const auto& child : type->getChildren()) {
      createStatsCollector(child);
    }
  }

  void wrapSharedStatsCollector(
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type,
      bool shared) {
    shared = shared || hasFlatMapNodeId(type->id());
    if (shared) {
      statsCollectors_[type->id()] = SharedStatisticsCollector::wrap(
          std::move(statsCollectors_[type->id()]));
    }

    for (const auto& child : type->getChildren()) {
      wrapSharedStatsCollector(child, shared);
    }
  }

  void initStatsCollectors(
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type) {
    createStatsCollector(type);

    // NOTE: When supporting deduplicated stats, we set just the stats on
    // the deduplicated type writers only. Those nodes record logical stats as
    // is, but then record the children's logical nodes as deduplicated at
    // finalization.
    for (auto id : dictionaryArrayNodeIds()) {
      statsCollectors_[id] = DeduplicatedStatisticsCollector::wrap(
          std::move(statsCollectors_[id]));
    }

    for (auto id : deduplicatedMapNodeIds()) {
      statsCollectors_[id] = DeduplicatedStatisticsCollector::wrap(
          std::move(statsCollectors_[id]));
    }

    // Wrap shared stats collector for flatmap nodes last to fully protected
    // concurrent stats updates across flatmap value writers.
    wrapSharedStatsCollector(type, false);

    schemaWithId_ = type;
  }

  StatisticsCollector* getStatsCollector(uint32_t nodeId) const {
    NIMBLE_CHECK_LT(
        nodeId,
        statsCollectors_.size(),
        "Mismatched cardinality for stats and schema. Likely uninitialized stats collectors.");
    return statsCollectors_[nodeId].get();
  }

  // After potential parallel process of field writers and their stats,
  // sequentially roll up and fix up the stats according to the hierarchy.
  // 1. roll up logical and physical sizes
  // 2. wrap all ancestors of deduplicated types
  // 3. backfill both known and newly wrapped deduplicated stats
  void finalizeStatsCollectors() {
    NIMBLE_CHECK(!statsFinalized_);
    finalizeStatsCollector(schemaWithId_);
    statsFinalized_ = true;
  }

 protected:
  MemoryPoolHolder bufferMemoryPool_;
  std::mutex flatMapSchemaMutex_;
  SchemaBuilder schemaBuilder_;

  folly::F14FastSet<uint32_t> flatMapNodeIds_;
  folly::F14FastSet<uint32_t> dictionaryArrayNodeIds_;
  folly::F14FastSet<uint32_t> deduplicatedMapNodeIds_;
  bool ignoreTopLevelNulls_{false};
  bool disableSharedStringBuffers_{false};
  // Set to true when a flatmap passthrough write is detected
  // (ROW vector written as MAP via flatMapColumns configuration).
  bool hasPassthroughFlatMapWrites_{false};

  std::unique_ptr<InputBufferGrowthPolicy> inputBufferGrowthPolicy_;
  std::unique_ptr<InputBufferGrowthPolicy> stringBufferGrowthPolicy_;
  InputBufferGrowthStats inputBufferGrowthStats_;

  std::function<void(const TypeBuilder&, std::string_view, const TypeBuilder&)>
      flatmapFieldAddedEventHandler_;

  std::function<void(const TypeBuilder&)> typeAddedHandler_{
      [](const TypeBuilder&) {}};

 private:
  void finalizeStatsCollector(
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type) {
    auto statsCollector = statsCollectors_[type->id()].get();

    // Known deduplicated nodes record logical stats as is, but then record the
    // children's logical sizes as deduplicated at finalization, to avoid double
    // counting.
    if (statsCollector->getType() == StatType::DEDUPLICATED) {
      // Handle the case where the collector might be wrapped in a
      // SharedStatisticsCollector.
      if (statsCollector->isShared()) {
        auto sharedCollector = statsCollector->as<SharedStatisticsCollector>();
        for (const auto& child : type->getChildren()) {
          finalizeStatsCollector(child);
          auto childStatsCollector = getStatsCollector(child->id());
          sharedCollector->updateBaseCollector([&](StatisticsCollector*
                                                       baseCollector) {
            auto dedupedStatsCollector =
                baseCollector->as<DeduplicatedStatisticsCollector>();
            NIMBLE_DCHECK_NOT_NULL(
                dedupedStatsCollector,
                "Expected DeduplicatedStatisticsCollector for DEDUPLICATED type");
            // Only add children's logical sizes for flatmaps, which don't
            // include children's sizes in their own collectStatistics.
            // Sliding window maps and array with offsets use
            // getRawSizeFromVector which already includes children's sizes.
            // TODO(huamengjiang): fix the behavior of deduplicated stats for
            // flatmaps when properly supporting them.
            if (hasFlatMapNodeId(type->id())) {
              dedupedStatsCollector->addLogicalSize(
                  childStatsCollector->getLogicalSize());
            }
            dedupedStatsCollector->recordDeduplicatedStats(
                childStatsCollector->getValueCount(),
                childStatsCollector->getLogicalSize());
            dedupedStatsCollector->addPhysicalSize(
                childStatsCollector->getPhysicalSize());
          });
        }
      } else {
        auto dedupedStatsCollector =
            statsCollector->as<DeduplicatedStatisticsCollector>();
        NIMBLE_DCHECK_NOT_NULL(
            dedupedStatsCollector,
            "Expected DeduplicatedStatisticsCollector for DEDUPLICATED type");
        for (const auto& child : type->getChildren()) {
          finalizeStatsCollector(child);
          auto childStatsCollector = getStatsCollector(child->id());
          dedupedStatsCollector->recordDeduplicatedStats(
              childStatsCollector->getValueCount(),
              childStatsCollector->getLogicalSize());
          dedupedStatsCollector->addPhysicalSize(
              childStatsCollector->getPhysicalSize());
        }
      }

      return;
    }

    // Roll up logical and physical sizes. Find out if any child has
    // deduplicated stats.
    bool isDeduplicated = false;
    for (const auto& child : type->getChildren()) {
      finalizeStatsCollector(child);
      auto childStatsCollector = getStatsCollector(child->id());
      isDeduplicated |=
          (childStatsCollector->getType() == StatType::DEDUPLICATED);

      statsCollector->addLogicalSize(childStatsCollector->getLogicalSize());
      statsCollector->addPhysicalSize(childStatsCollector->getPhysicalSize());
    }

    // Backfill deduplicated ancestors.
    if (isDeduplicated &&
        statsCollectors_[type->id()]->getType() != StatType::DEDUPLICATED) {
      statsCollectors_[type->id()] = DeduplicatedStatisticsCollector::wrap(
          std::move(statsCollectors_[type->id()]));

      auto dedupedStatsCollector =
          statsCollectors_[type->id()]->as<DeduplicatedStatisticsCollector>();
      for (const auto& child : type->getChildren()) {
        auto childStatsCollector = getStatsCollector(child->id());
        if (childStatsCollector->getType() == StatType::DEDUPLICATED) {
          if (childStatsCollector->isShared()) {
            auto dedupedStats =
                childStatsCollector->as<SharedStatisticsCollector>()
                    ->getBaseStatistics()
                    ->as<DeduplicatedColumnStatistics>();
            dedupedStatsCollector->recordDeduplicatedStats(
                dedupedStats->getDedupedCount(),
                dedupedStats->getDedupedLogicalSize());
          } else {
            auto dedupedStats =
                childStatsCollector->as<DeduplicatedStatisticsCollector>()
                    ->getStatsView()
                    ->as<DeduplicatedColumnStatistics>();
            dedupedStatsCollector->recordDeduplicatedStats(
                dedupedStats->getDedupedCount(),
                dedupedStats->getDedupedLogicalSize());
          }
        } else {
          dedupedStatsCollector->recordDeduplicatedStats(
              childStatsCollector->getValueCount(),
              childStatsCollector->getLogicalSize());
        }
      }
    }
  }

  std::unique_ptr<Buffer> buffer_;
  DecodingContextPool decodingContextPool_;
  std::vector<std::pair<uint32_t, std::unique_ptr<StreamData>>> streams_;
  std::shared_ptr<const velox::dwio::common::TypeWithId> schemaWithId_;
  std::vector<std::unique_ptr<StatisticsCollector>> statsCollectors_;
  bool statsFinalized_{false};
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

  // Collects stats and clears interanl state and any accumulated data in
  // internal buffers.
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
