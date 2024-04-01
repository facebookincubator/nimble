// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <span>
#include "dwio/alpha/common/Buffer.h"
#include "dwio/alpha/common/Vector.h"
#include "dwio/alpha/velox/BufferGrowthPolicy.h"
#include "dwio/alpha/velox/OrderedRanges.h"
#include "dwio/alpha/velox/SchemaBuilder.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::alpha {

struct InputBufferGrowthStats {
  std::atomic<uint64_t> count{0};
  // realloc bytes would be interesting, but requires a bit more
  // trouble to get.
  std::atomic<uint64_t> itemCount{0};
};

struct FieldWriterContext {
  std::shared_ptr<velox::memory::MemoryPool> bufferMemoryPool;
  SchemaBuilder schemaBuilder;
  std::unique_ptr<InputBufferGrowthPolicy> inputBufferGrowthPolicy;
  InputBufferGrowthStats inputBufferGrowthStats;
  // TODO: Replace node ids with a new "language" to describe nodes by relative
  // ordinals. Until we have this language we use node ids. Node ids are not
  // ideal, because they might change when the schema changes. Therefore, it is
  // impossible to pass them in, as the caller might use a (future) table schema
  // which is different than the (older) file schema, producing incorrect node
  // ids.
  // In the meantime, we use the file schema to perform config conversions to
  // node ids.
  folly::F14FastSet<uint32_t> flatMapNodeIds;
  folly::F14FastSet<uint32_t> dictionaryArrayNodeIds;

  std::function<void(const TypeBuilder&, std::string_view, const TypeBuilder&)>
      flatmapFieldAddedEventHandler;

  explicit FieldWriterContext(
      velox::memory::MemoryPool& memoryPool,
      std::unique_ptr<velox::memory::MemoryReclaimer> reclaimer = nullptr)
      : bufferMemoryPool{memoryPool.addLeafChild(
            "field_writer_buffer",
            true,
            std::move(reclaimer))},
        inputBufferGrowthPolicy{
            DefaultInputBufferGrowthPolicy::withDefaultRanges()} {
    resetStringBuffer();
  }

  class LocalDecodedVector;

  LocalDecodedVector getLocalDecodedVector();

  velox::SelectivityVector& getSelectivityVector(velox::vector_size_t size);

  Buffer& stringBuffer() {
    return *buffer_;
  }

  // Reset writer context for use by next stripe.
  void resetStringBuffer() {
    buffer_ = std::make_unique<Buffer>(*bufferMemoryPool);
  }

 private:
  std::unique_ptr<velox::DecodedVector> getDecodedVector();

  void releaseDecodedVector(std::unique_ptr<velox::DecodedVector>&& vector);

  std::unique_ptr<Buffer> buffer_;
  std::vector<std::unique_ptr<velox::DecodedVector>> decodedVectorPool_;
  std::unique_ptr<velox::SelectivityVector> selectivity_;
};

using OrderedRanges = range_helper::OrderedRanges<velox::vector_size_t>;
using StreamCollector = std::function<void(
    const TypeBuilder* /* type */,
    std::span<const bool>* /* nonNulls */,
    std::string_view /* cols */)>;

class FieldWriter {
 public:
  FieldWriter(
      FieldWriterContext& context,
      std::shared_ptr<TypeBuilder> typeBuilder)
      : context_{context},
        typeBuilder_{std::move(typeBuilder)},
        nonNulls_(context.bufferMemoryPool.get()) {}

  virtual ~FieldWriter() = default;

  // Writes the vector to internal buffers.
  virtual uint64_t write(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges) = 0;

  // Flushes any buffered data. `reset` specifies if internal state should be
  // reset. Writer may be used to flush multiple times, and after each flush, it
  // may still carry internal state for subsequent write/flush to work. When
  // `reset` is set to true, internal state will be cleared.
  virtual void flush(const StreamCollector& collector, bool reset = true) = 0;

  virtual void close() {}

  const std::shared_ptr<TypeBuilder>& typeBuilder() {
    return typeBuilder_;
  }

  static std::unique_ptr<FieldWriter> create(
      FieldWriterContext& context,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type);

 protected:
  FieldWriterContext& context_;
  std::shared_ptr<TypeBuilder> typeBuilder_;

  uint64_t bufferedValueCount_{0};
  uint64_t flushedValueCount_{0};
  bool hasNulls_{false};
  Vector<bool> nonNulls_;

  void reset() {
    bufferedValueCount_ = 0;
    flushedValueCount_ = 0;
    hasNulls_ = false;
    nonNulls_.clear();
  }

  void ensureCapacity(bool mayHaveNulls, velox::vector_size_t size);

  uint64_t nullBitmapSize(velox::vector_size_t size) const {
    return hasNulls_ ? size : 0;
  }

  FieldWriterContext::LocalDecodedVector decode(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges);

  template <bool addNulls, typename Vector, typename Op>
  uint64_t iterateIndices(
      const OrderedRanges& ranges,
      const Vector& vector,
      const Op& op);

  template <typename Vector, typename Op>
  uint64_t iterateValues(
      const OrderedRanges& ranges,
      const Vector& vector,
      const Op& op);

  template <typename T>
  void flushStream(
      const StreamCollector& collector,
      bool hasNulls,
      Vector<T>& data,
      bool forceFlushingNulls,
      const TypeBuilder* typeBuilder = nullptr);
};

} // namespace facebook::alpha
