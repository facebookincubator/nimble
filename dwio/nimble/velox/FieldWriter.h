// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <span>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/velox/BufferGrowthPolicy.h"
#include "dwio/nimble/velox/OrderedRanges.h"
#include "dwio/nimble/velox/SchemaBuilder.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::nimble {

// Stream data is a generic interface representing a stream of data, allowing
// generic access to the content to be used by writers
class StreamData {
 public:
  explicit StreamData(const StreamDescriptorBuilder& descriptor)
      : descriptor_{descriptor} {}

  StreamData(const StreamData&) = delete;
  StreamData(StreamData&&) = delete;
  StreamData& operator=(const StreamData&) = delete;
  StreamData& operator=(StreamData&&) = delete;

  virtual std::string_view data() const = 0;
  virtual std::span<const bool> nonNulls() const = 0;
  virtual bool hasNulls() const = 0;
  virtual bool empty() const = 0;
  virtual uint64_t memoryUsed() const = 0;

  virtual void reset() = 0;
  virtual void materialize() {}

  const StreamDescriptorBuilder& descriptor() const {
    return descriptor_;
  }

  virtual ~StreamData() = default;

 private:
  const StreamDescriptorBuilder& descriptor_;
};

// Content only data stream.
// Used when a stream doesn't contain nulls.
template <typename T>
class ContentStreamData final : public StreamData {
 public:
  ContentStreamData(
      velox::memory::MemoryPool& memoryPool,
      const StreamDescriptorBuilder& descriptor)
      : StreamData(descriptor), data_{&memoryPool}, extraMemory_{0} {}

  inline virtual std::string_view data() const override {
    return {
        reinterpret_cast<const char*>(data_.data()), data_.size() * sizeof(T)};
  }

  inline virtual std::span<const bool> nonNulls() const override {
    return {};
  }

  inline virtual bool hasNulls() const override {
    return false;
  }

  inline virtual bool empty() const override {
    return data_.empty();
  }

  inline virtual uint64_t memoryUsed() const override {
    return (data_.size() * sizeof(T)) + extraMemory_;
  }

  inline Vector<T>& mutableData() {
    return data_;
  }

  inline uint64_t& extraMemory() {
    return extraMemory_;
  }

  inline virtual void reset() override {
    data_.clear();
    extraMemory_ = 0;
  }

 private:
  Vector<T> data_;
  uint64_t extraMemory_;
};

// Nulls only data stream.
// Used in cases where boolean data (representing nulls) is needed.
// NOTE: ContentStreamData<bool> can also be used to represent these data
// streams, however, for these "null streams", we have special optimizations,
// where if all data is non-null, we omit the stream. This class specialization
// helps with reusing enabling this optimization.
class NullsStreamData : public StreamData {
 public:
  NullsStreamData(
      velox::memory::MemoryPool& memoryPool,
      const StreamDescriptorBuilder& descriptor)
      : StreamData(descriptor),
        nonNulls_{&memoryPool},
        hasNulls_{false},
        bufferedCount_{0} {}

  inline virtual std::string_view data() const override {
    return {};
  }

  inline virtual std::span<const bool> nonNulls() const override {
    return nonNulls_;
  }

  inline virtual bool hasNulls() const override {
    return hasNulls_;
  }

  inline virtual bool empty() const override {
    return nonNulls_.empty() && bufferedCount_ == 0;
  }

  inline virtual uint64_t memoryUsed() const override {
    return nonNulls_.size();
  }

  inline Vector<bool>& mutableNonNulls() {
    return nonNulls_;
  }

  inline virtual void reset() override {
    nonNulls_.clear();
    hasNulls_ = false;
    bufferedCount_ = 0;
  }

  void materialize() override {
    if (nonNulls_.size() < bufferedCount_) {
      const auto offset = nonNulls_.size();
      nonNulls_.resize(bufferedCount_);
      std::fill(
          nonNulls_.data() + offset, nonNulls_.data() + bufferedCount_, true);
    }
  }

  void ensureNullsCapacity(bool mayHaveNulls, velox::vector_size_t size);

 protected:
  Vector<bool> nonNulls_;
  bool hasNulls_;
  uint32_t bufferedCount_;
};

// Nullable content data stream.
// Used in all cases where data may contain nulls.
template <typename T>
class NullableContentStreamData final : public NullsStreamData {
 public:
  NullableContentStreamData(
      velox::memory::MemoryPool& memoryPool,
      const StreamDescriptorBuilder& descriptor)
      : NullsStreamData(memoryPool, descriptor),
        data_{&memoryPool},
        extraMemory_{0} {}

  inline virtual std::string_view data() const override {
    return {
        reinterpret_cast<const char*>(data_.data()), data_.size() * sizeof(T)};
  }

  inline virtual bool empty() const override {
    return NullsStreamData::empty() && data_.empty();
  }

  inline virtual uint64_t memoryUsed() const override {
    return (data_.size() * sizeof(T)) + extraMemory_ +
        NullsStreamData::memoryUsed();
  }

  inline Vector<T>& mutableData() {
    return data_;
  }

  inline uint64_t& extraMemory() {
    return extraMemory_;
  }

  inline virtual void reset() override {
    NullsStreamData::reset();
    data_.clear();
    extraMemory_ = 0;
  }

 private:
  Vector<T> data_;
  uint64_t extraMemory_;
};

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
      std::unique_ptr<velox::memory::MemoryReclaimer> reclaimer = nullptr)
      : bufferMemoryPool{memoryPool.addLeafChild(
            "field_writer_buffer",
            true,
            std::move(reclaimer))},
        inputBufferGrowthPolicy{
            DefaultInputBufferGrowthPolicy::withDefaultRanges()} {
    resetStringBuffer();
  }

  std::shared_ptr<velox::memory::MemoryPool> bufferMemoryPool;
  SchemaBuilder schemaBuilder;

  folly::F14FastSet<uint32_t> flatMapNodeIds;
  folly::F14FastSet<uint32_t> dictionaryArrayNodeIds;

  std::unique_ptr<InputBufferGrowthPolicy> inputBufferGrowthPolicy;
  InputBufferGrowthStats inputBufferGrowthStats;

  std::function<void(const TypeBuilder&, std::string_view, const TypeBuilder&)>
      flatmapFieldAddedEventHandler;

  std::function<void(const TypeBuilder&)> typeAddedHandler =
      [](const TypeBuilder&) {};

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
