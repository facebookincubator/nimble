// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/FieldWriter.h"
#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/velox/SchemaBuilder.h"
#include "dwio/alpha/velox/SchemaTypes.h"
#include "velox/common/base/CompareFlags.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::alpha {

class FieldWriterContext::LocalDecodedVector {
 public:
  explicit LocalDecodedVector(FieldWriterContext& context)
      : context_(context), vector_(context_.getDecodedVector()) {}

  LocalDecodedVector(LocalDecodedVector&& other) noexcept
      : context_{other.context_}, vector_{std::move(other.vector_)} {}

  LocalDecodedVector& operator=(LocalDecodedVector&& other) = delete;

  ~LocalDecodedVector() {
    if (vector_) {
      context_.releaseDecodedVector(std::move(vector_));
    }
  }

  velox::DecodedVector& get() {
    return *vector_;
  }

 private:
  FieldWriterContext& context_;
  std::unique_ptr<velox::DecodedVector> vector_;
};

FieldWriterContext::LocalDecodedVector
FieldWriterContext::getLocalDecodedVector() {
  return LocalDecodedVector{*this};
}

velox::SelectivityVector& FieldWriterContext::getSelectivityVector(
    velox::vector_size_t size) {
  if (LIKELY(selectivity_.get() != nullptr)) {
    selectivity_->resize(size);
  } else {
    selectivity_ = std::make_unique<velox::SelectivityVector>(size);
  }
  return *selectivity_;
}

std::unique_ptr<velox::DecodedVector> FieldWriterContext::getDecodedVector() {
  if (decodedVectorPool_.empty()) {
    return std::make_unique<velox::DecodedVector>();
  }
  auto vector = std::move(decodedVectorPool_.back());
  decodedVectorPool_.pop_back();
  return vector;
}

void FieldWriterContext::releaseDecodedVector(
    std::unique_ptr<velox::DecodedVector>&& vector) {
  decodedVectorPool_.push_back(std::move(vector));
}

FieldWriterContext::LocalDecodedVector FieldWriter::decode(
    const velox::VectorPtr& vector,
    const OrderedRanges& ranges) {
  auto& selectivityVector = context_.getSelectivityVector(vector->size());
  // initialize selectivity vector
  selectivityVector.clearAll();
  ranges.apply([&](auto offset, auto size) {
    selectivityVector.setValidRange(offset, offset + size, true);
  });
  selectivityVector.updateBounds();

  auto localDecoded = context_.getLocalDecodedVector();
  localDecoded.get().decode(*vector, selectivityVector);
  return localDecoded;
}

void FieldWriter::ensureCapacity(bool mayHaveNulls, velox::vector_size_t size) {
  if (mayHaveNulls || hasNulls_) {
    auto newSize = bufferedValueCount_ + size;
    nonNulls_.reserve(newSize);
    if (!hasNulls_) {
      hasNulls_ = true;
      std::fill(nonNulls_.data(), nonNulls_.data() + bufferedValueCount_, true);
      nonNulls_.update_size(bufferedValueCount_);
    }
    if (!mayHaveNulls) {
      std::fill(
          nonNulls_.data() + bufferedValueCount_,
          nonNulls_.data() + newSize,
          true);
      nonNulls_.update_size(newSize);
    }
  }
  bufferedValueCount_ += size;
}

namespace {

template <bool addNulls, typename Vector, typename Consumer, typename IndexOp>
uint64_t iterate(
    const OrderedRanges& ranges,
    alpha::Vector<bool>& nonNulls,
    const Vector& vector,
    const Consumer& consumer,
    const IndexOp& indexOp) {
  uint64_t nonNullCount = 0;
  if (vector.hasNulls()) {
    ranges.applyEach([&](auto offset) {
      auto notNull = !vector.isNullAt(offset);
      if constexpr (addNulls) {
        nonNulls.push_back(notNull);
      }
      if (notNull) {
        ++nonNullCount;
        consumer(indexOp(offset));
      }
    });
  } else {
    ranges.applyEach([&](auto offset) { consumer(indexOp(offset)); });
    nonNullCount = ranges.size();
  }
  return nonNullCount;
}

template <typename T>
std::string_view convert(const Vector<T>& input) {
  return {
      reinterpret_cast<const char*>(input.data()), input.size() * sizeof(T)};
}

} // namespace

template <bool addNulls, typename Vector, typename Op>
uint64_t FieldWriter::iterateIndices(
    const OrderedRanges& ranges,
    const Vector& vector,
    const Op& op) {
  return iterate<addNulls>(ranges, nonNulls_, vector, op, [&](auto offset) {
    return vector.index(offset);
  });
}

template <typename Vector, typename Op>
uint64_t FieldWriter::iterateValues(
    const OrderedRanges& ranges,
    const Vector& vector,
    const Op& op) {
  return iterate<true>(ranges, nonNulls_, vector, op, [&](auto offset) {
    return vector.valueAt(offset);
  });
}

template <typename T>
void FieldWriter::flushStream(
    const StreamCollector& collector,
    bool hasNulls,
    Vector<T>& data,
    bool forceFlushingNulls,
    const StreamDescriptorBuilder& streamDescriptor) {
  // When there is non-null values to be flushed, it always need to flush both
  // data and nulls and then clear the state.
  // When there is no value, there are two cases:
  // 1. All values are null. There is no need to flush nulls if all values
  // written to this writer is null, which we won't know until it's the last
  // flush (when `forceFlushingNulls` is set) or if there is already values
  // flushed previous (indicated by `flushedValueCount_ > 0`).
  // 2. Array/map has no child. No need to flush.
  if (data.size() > 0 ||
      (forceFlushingNulls && nonNulls_.size() > 0 && flushedValueCount_ > 0)) {
    std::span<const bool> nonNulls(nonNulls_);
    collector(streamDescriptor, hasNulls ? &nonNulls : nullptr, convert(data));
    flushedValueCount_ += bufferedValueCount_;
    bufferedValueCount_ = 0;
    nonNulls_.clear();
    data.clear();
  }
}

namespace {

template <velox::TypeKind KIND>
struct AlphaTypeTraits {};

template <>
struct AlphaTypeTraits<velox::TypeKind::BOOLEAN> {
  static constexpr ScalarKind scalarKind = ScalarKind::Bool;
};

template <>
struct AlphaTypeTraits<velox::TypeKind::TINYINT> {
  static constexpr ScalarKind scalarKind = ScalarKind::Int8;
};

template <>
struct AlphaTypeTraits<velox::TypeKind::SMALLINT> {
  static constexpr ScalarKind scalarKind = ScalarKind::Int16;
};

template <>
struct AlphaTypeTraits<velox::TypeKind::INTEGER> {
  static constexpr ScalarKind scalarKind = ScalarKind::Int32;
};

template <>
struct AlphaTypeTraits<velox::TypeKind::BIGINT> {
  static constexpr ScalarKind scalarKind = ScalarKind::Int64;
};

template <>
struct AlphaTypeTraits<velox::TypeKind::REAL> {
  static constexpr ScalarKind scalarKind = ScalarKind::Float;
};

template <>
struct AlphaTypeTraits<velox::TypeKind::DOUBLE> {
  static constexpr ScalarKind scalarKind = ScalarKind::Double;
};

template <>
struct AlphaTypeTraits<velox::TypeKind::VARCHAR> {
  static constexpr ScalarKind scalarKind = ScalarKind::String;
};

template <>
struct AlphaTypeTraits<velox::TypeKind::VARBINARY> {
  static constexpr ScalarKind scalarKind = ScalarKind::Binary;
};

template <>
struct AlphaTypeTraits<velox::TypeKind::TIMESTAMP> {
  static constexpr ScalarKind scalarKind = ScalarKind::Int64;
};

// Adapters to handle flat or decoded vector using same interfaces.
template <typename T = int8_t>
class Flat {
  static constexpr auto kIsBool = std::is_same_v<T, bool>;

 public:
  explicit Flat(const velox::VectorPtr& vector)
      : vector_{vector}, nulls_{vector->rawNulls()} {
    if constexpr (!kIsBool) {
      if (auto casted = vector->asFlatVector<T>()) {
        values_ = casted->rawValues();
      }
    }
  }

  bool hasNulls() const {
    return vector_->mayHaveNulls();
  }

  bool isNullAt(velox::vector_size_t index) const {
    return velox::bits::isBitNull(nulls_, index);
  }

  T valueAt(velox::vector_size_t index) const {
    if constexpr (kIsBool) {
      return static_cast<const velox::FlatVector<T>*>(vector_.get())
          ->valueAtFast(index);
    } else {
      return values_[index];
    }
  }

  velox::vector_size_t index(velox::vector_size_t index) const {
    return index;
  }

 private:
  const velox::VectorPtr& vector_;
  const uint64_t* nulls_;
  const T* values_;
};

template <typename T = int8_t>
class Decoded {
 public:
  explicit Decoded(const velox::DecodedVector& decoded) : decoded_{decoded} {}

  bool hasNulls() const {
    return decoded_.mayHaveNulls();
  }

  bool isNullAt(velox::vector_size_t index) const {
    return decoded_.isNullAt(index);
  }

  T valueAt(velox::vector_size_t index) const {
    return decoded_.valueAt<T>(index);
  }

  velox::vector_size_t index(velox::vector_size_t index) const {
    return decoded_.index(index);
  }

 private:
  const velox::DecodedVector& decoded_;
};

template <typename T>
bool equalDecodedVectorIndices(
    const velox::DecodedVector& vec,
    velox::vector_size_t index,
    velox::vector_size_t otherIndex) {
  bool otherNull = vec.isNullAt(otherIndex);
  bool thisNull = vec.isNullAt(index);

  if (thisNull && otherNull) {
    return true;
  }

  if (thisNull || otherNull) {
    return false;
  }

  return vec.valueAt<T>(index) == vec.valueAt<T>(otherIndex);
}

template <typename T>
bool compareDecodedVectorToCache(
    const velox::DecodedVector& thisVec,
    velox::vector_size_t index,
    velox::FlatVector<T>* cachedFlatVec,
    velox::vector_size_t cacheIndex,
    velox::CompareFlags flags) {
  bool thisNull = thisVec.isNullAt(index);
  bool otherNull = cachedFlatVec->isNullAt(cacheIndex);

  if (thisNull && otherNull) {
    return true;
  }

  if (thisNull || otherNull) {
    return false;
  }

  return thisVec.valueAt<T>(index) == cachedFlatVec->valueAt(cacheIndex);
}

template <typename T, typename = std::enable_if_t<std::is_trivial_v<T>>>
struct IdentityConverter {
  static T convert(T t, Buffer&, uint64_t&) {
    return t;
  }
};

struct StringConverter {
  static std::string_view
  convert(velox::StringView sv, Buffer& buffer, uint64_t& memoryUsed) {
    memoryUsed += sv.size();
    return buffer.writeString({sv.data(), sv.size()});
  }
};

struct TimestampConverter {
  static int64_t convert(velox::Timestamp ts, Buffer&, uint64_t&) {
    return ts.toMillis();
  }
};

template <
    velox::TypeKind K,
    typename C = IdentityConverter<typename velox::TypeTraits<K>::NativeType>>
class SimpleFieldWriter : public FieldWriter {
  using SourceType = typename velox::TypeTraits<K>::NativeType;
  using TargetType = decltype(C::convert(
      SourceType(),
      std::declval<Buffer&>(),
      std::declval<uint64_t&>()));

 public:
  explicit SimpleFieldWriter(FieldWriterContext& context)
      : FieldWriter(
            context,
            context.schemaBuilder.createScalarTypeBuilder(
                AlphaTypeTraits<K>::scalarKind)),
        vec_{context.bufferMemoryPool.get()} {}

  uint64_t write(const velox::VectorPtr& vector, const OrderedRanges& ranges)
      override {
    auto size = ranges.size();
    auto& buffer = context_.stringBuffer();
    uint64_t stringMemoryUsed = 0;
    uint32_t nonNullCount = 0;
    if (auto flat = vector->asFlatVector<SourceType>()) {
      ensureCapacity(flat->mayHaveNulls(), size);
      bool rangeCopied = false;
      if (!flat->mayHaveNulls()) {
        if constexpr (
            std::is_same_v<C, IdentityConverter<SourceType, void>> &&
            K != velox::TypeKind::BOOLEAN) {
          // In low memory mode we can afford to be exact for raw data
          // buffering.
          auto newSize = vec_.size() + size;
          if (newSize > vec_.capacity()) {
            auto newCapacity =
                context_.inputBufferGrowthPolicy->getExtendedCapacity(
                    newSize, vec_.capacity());
            ++context_.inputBufferGrowthStats.count;
            context_.inputBufferGrowthStats.itemCount += newCapacity;
            vec_.reserve(newCapacity);
          }
          ranges.apply([&](auto offset, auto count) {
            vec_.insert(
                vec_.end(),
                flat->rawValues() + offset,
                flat->rawValues() + offset + count);
          });
          nonNullCount = size;
          rangeCopied = true;
        }
      }

      if (!rangeCopied) {
        nonNullCount = iterateValues(
            ranges, Flat<SourceType>{vector}, [&](SourceType value) {
              vec_.push_back(C::convert(value, buffer, stringMemoryUsed));
            });
      }
    } else {
      auto localDecoded = decode(vector, ranges);
      auto& decoded = localDecoded.get();
      ensureCapacity(decoded.mayHaveNulls(), size);
      nonNullCount = iterateValues(
          ranges, Decoded<SourceType>{decoded}, [&](SourceType value) {
            vec_.push_back(C::convert(value, buffer, stringMemoryUsed));
          });
    }
    return sizeof(TargetType) * nonNullCount + stringMemoryUsed +
        nullBitmapSize(size);
  }

  void flush(const StreamCollector& collector, bool reset) override {
    // Need to solve reader problem before we can enable chunking for strings.
    if (!std::is_same_v<C, StringConverter> || reset) {
      flushStream(
          collector,
          hasNulls_,
          vec_,
          reset,
          typeBuilder_->asScalar().scalarDescriptor());
    }
    if (reset) {
      FieldWriter::reset();
    }
  }

 private:
  // NOTE: this is currently expensive to grow during a long sequence of ingest
  // operators. We currently achieve a balance via a buffer growth policy.
  // Another factor that can help us reduce this cost is to also consider the
  // stripe progress. However, naive progress based policies can't be combined
  // with the size based policies, and are thus currently not included.
  Vector<TargetType> vec_;
};

class RowFieldWriter : public FieldWriter {
 public:
  RowFieldWriter(
      FieldWriterContext& context,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type)
      : FieldWriter{
            context,
            context.schemaBuilder.createRowTypeBuilder(type->size())} {
    auto rowType =
        std::dynamic_pointer_cast<const velox::RowType>(type->type());

    fields_.reserve(rowType->size());
    for (auto i = 0; i < rowType->size(); ++i) {
      fields_.push_back(FieldWriter::create(context, type->childAt(i)));
      typeBuilder_->asRow().addChild(
          rowType->nameOf(i), fields_.back()->typeBuilder());
    }
  }

  uint64_t write(const velox::VectorPtr& vector, const OrderedRanges& ranges)
      override {
    auto size = ranges.size();
    uint64_t memoryUsed = 0;
    OrderedRanges childRanges;
    const OrderedRanges* childRangesPtr;
    const velox::RowVector* row = vector->as<velox::RowVector>();
    if (row) {
      ALPHA_CHECK(fields_.size() == row->childrenSize(), "schema mismatch");
      ensureCapacity(vector->mayHaveNulls(), size);
      if (row->mayHaveNulls()) {
        childRangesPtr = &childRanges;
        iterateIndices<true>(ranges, Flat{vector}, [&](auto offset) {
          childRanges.add(offset, 1);
        });
      } else {
        childRangesPtr = &ranges;
      }
    } else {
      auto localDecoded = decode(vector, ranges);
      auto& decoded = localDecoded.get();
      row = decoded.base()->as<velox::RowVector>();
      ALPHA_ASSERT(row, "Unexpected vector type");
      ALPHA_CHECK(fields_.size() == row->childrenSize(), "schema mismatch");
      childRangesPtr = &childRanges;
      ensureCapacity(decoded.mayHaveNulls(), size);
      iterateIndices<true>(ranges, Decoded{decoded}, [&](auto offset) {
        childRanges.add(offset, 1);
      });
    }
    for (auto i = 0; i < fields_.size(); ++i) {
      memoryUsed += fields_[i]->write(row->childAt(i), *childRangesPtr);
    }
    return memoryUsed + nullBitmapSize(size);
  }

  void flush(const StreamCollector& collector, bool reset) override {
    if (hasNulls_) {
      flushStream(
          collector,
          false,
          nonNulls_,
          reset,
          typeBuilder_->asRow().nullsDescriptor());
    }
    for (auto& field : fields_) {
      field->flush(collector, reset);
    }
    if (reset) {
      FieldWriter::reset();
    }
  }

  void close() override {
    for (auto& field : fields_) {
      field->close();
    }
  }

 private:
  std::vector<std::unique_ptr<FieldWriter>> fields_;
};

class MultiValueFieldWriter : public FieldWriter {
 public:
  MultiValueFieldWriter(
      FieldWriterContext& context,
      std::shared_ptr<TypeBuilder> typeBuilder)
      : FieldWriter{context, std::move(typeBuilder)},
        lengths_{context.bufferMemoryPool.get()} {}

 protected:
  template <typename T>
  const T* ingestLengths(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges,
      uint64_t& memoryUsed,
      OrderedRanges& childRanges) {
    auto size = ranges.size();
    const T* casted = vector->as<T>();
    const velox::vector_size_t* offsets;
    const velox::vector_size_t* lengths;
    auto proc = [&](velox::vector_size_t index) {
      auto length = lengths[index];
      if (length > 0) {
        childRanges.add(offsets[index], length);
      }
      lengths_.push_back(length);
    };

    uint32_t nonNullCount = 0;
    if (casted) {
      offsets = casted->rawOffsets();
      lengths = casted->rawSizes();

      ensureCapacity(casted->mayHaveNulls(), size);
      nonNullCount = iterateIndices<true>(ranges, Flat{vector}, proc);
    } else {
      auto localDecoded = decode(vector, ranges);
      auto& decoded = localDecoded.get();
      casted = decoded.base()->as<T>();
      ALPHA_ASSERT(casted, "Unexpected vector type");
      offsets = casted->rawOffsets();
      lengths = casted->rawSizes();

      ensureCapacity(decoded.mayHaveNulls(), size);
      nonNullCount = iterateIndices<true>(ranges, Decoded{decoded}, proc);
    }
    memoryUsed +=
        (sizeof(velox::vector_size_t) * nonNullCount + nullBitmapSize(size));
    return casted;
  }

  Vector<velox::vector_size_t> lengths_;
};

class ArrayFieldWriter : public MultiValueFieldWriter {
 public:
  ArrayFieldWriter(
      FieldWriterContext& context,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type)
      : MultiValueFieldWriter{
            context,
            context.schemaBuilder.createArrayTypeBuilder()} {
    auto arrayType =
        std::dynamic_pointer_cast<const velox::ArrayType>(type->type());

    ALPHA_DASSERT(type->size() == 1, "Invalid array type.");
    elements_ = FieldWriter::create(context, type->childAt(0));

    typeBuilder_->asArray().setChildren(elements_->typeBuilder());
  }

  uint64_t write(const velox::VectorPtr& vector, const OrderedRanges& ranges)
      override {
    uint64_t memoryUsed = 0;
    OrderedRanges childRanges;
    auto array = ingestLengths<velox::ArrayVector>(
        vector, ranges, memoryUsed, childRanges);
    if (childRanges.size() > 0) {
      memoryUsed += elements_->write(array->elements(), childRanges);
    }
    return memoryUsed;
  }

  void flush(const StreamCollector& collector, bool reset) override {
    flushStream(
        collector,
        hasNulls_,
        lengths_,
        reset,
        typeBuilder_->asArray().lengthsDescriptor());
    elements_->flush(collector, reset);
    if (reset) {
      FieldWriter::reset();
    }
  }

  void close() override {
    elements_->close();
  }

 private:
  std::unique_ptr<FieldWriter> elements_;
};

class MapFieldWriter : public MultiValueFieldWriter {
 public:
  MapFieldWriter(
      FieldWriterContext& context,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type)
      : MultiValueFieldWriter{
            context,
            context.schemaBuilder.createMapTypeBuilder()} {
    auto mapType =
        std::dynamic_pointer_cast<const velox::MapType>(type->type());

    ALPHA_DASSERT(type->size() == 2, "Invalid map type.");
    keys_ = FieldWriter::create(context, type->childAt(0));
    values_ = FieldWriter::create(context, type->childAt(1));
    typeBuilder_->asMap().setChildren(
        keys_->typeBuilder(), values_->typeBuilder());
  }

  uint64_t write(const velox::VectorPtr& vector, const OrderedRanges& ranges)
      override {
    uint64_t memoryUsed = 0;
    OrderedRanges childRanges;
    auto map = ingestLengths<velox::MapVector>(
        vector, ranges, memoryUsed, childRanges);
    if (childRanges.size() > 0) {
      memoryUsed += keys_->write(map->mapKeys(), childRanges);
      memoryUsed += values_->write(map->mapValues(), childRanges);
    }
    return memoryUsed;
  }

  void flush(const StreamCollector& collector, bool reset) override {
    flushStream(
        collector,
        hasNulls_,
        lengths_,
        reset,
        typeBuilder_->asMap().lengthsDescriptor());
    keys_->flush(collector, reset);
    values_->flush(collector, reset);
    if (reset) {
      FieldWriter::reset();
    }
  }

  void close() override {
    keys_->close();
    values_->close();
  }

 private:
  std::unique_ptr<FieldWriter> keys_;
  std::unique_ptr<FieldWriter> values_;
};

class FlatMapValueFieldWriter {
 public:
  FlatMapValueFieldWriter(
      FieldWriterContext& context,
      const StreamDescriptorBuilder& inMapDescriptor,
      std::unique_ptr<FieldWriter> valueField)
      : inMapDescriptor_{inMapDescriptor},
        valueField_{std::move(valueField)},
        inMapBuffer_(context.bufferMemoryPool.get()) {}

  // Clear the ranges and extend the inMapBuffer
  void prepare(uint32_t numValues) {
    inMapBuffer_.reserve(inMapBuffer_.size() + numValues);
    std::fill(inMapBuffer_.end(), inMapBuffer_.end() + numValues, false);
  }

  void add(velox::vector_size_t offset, uint32_t mapIndex) {
    auto index = mapIndex + inMapBuffer_.size();
    ALPHA_CHECK(inMapBuffer_.empty() || !inMapBuffer_[index], "Duplicated key");
    ranges_.add(offset, 1);
    inMapBuffer_[index] = true;
  }

  uint64_t write(const velox::VectorPtr& vector, uint32_t mapCount) {
    inMapBuffer_.update_size(inMapBuffer_.size() + mapCount);
    // TODO: currently bool uses 1 byte memory, update once we move to dense
    uint64_t memoryUsed = mapCount;
    if (ranges_.size() > 0) {
      memoryUsed += valueField_->write(vector, ranges_);
    }
    ranges_.clear();
    return memoryUsed;
  }

  void flush(const StreamCollector& collector, bool reset) {
    if (inMapBuffer_.size() > 0) {
      collector(inMapDescriptor_, nullptr, convert(inMapBuffer_));
      inMapBuffer_.clear();
    }
    valueField_->flush(collector, reset);
  }

  void backfill(uint32_t count, uint32_t reserve) {
    inMapBuffer_.resize(count, false);
    prepare(reserve);
  }

  void close() {
    valueField_->close();
  }

 private:
  const StreamDescriptorBuilder& inMapDescriptor_;
  std::unique_ptr<FieldWriter> valueField_;
  Vector<bool> inMapBuffer_;
  OrderedRanges ranges_;
};

template <velox::TypeKind K>
class FlatMapFieldWriter : public FieldWriter {
  using KeyType = typename velox::TypeTraits<K>::NativeType;

 public:
  FlatMapFieldWriter(
      FieldWriterContext& context,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type)
      : FieldWriter(
            context,
            context.schemaBuilder.createFlatMapTypeBuilder(
                AlphaTypeTraits<K>::scalarKind)),
        valueType_{type->childAt(1)} {}

  uint64_t write(const velox::VectorPtr& vector, const OrderedRanges& ranges)
      override {
    auto size = ranges.size();
    uint64_t memoryUsed = 0;
    const velox::vector_size_t* offsets;
    const velox::vector_size_t* lengths;
    uint32_t nonNullCount = 0;
    OrderedRanges keyRanges;

    // Lambda that iterates keys of a map and records the offsets to write to
    // particular value node.
    auto processMap = [&](velox::vector_size_t index, auto& keysVector) {
      for (auto begin = offsets[index], end = begin + lengths[index];
           begin < end;
           ++begin) {
        auto valueField = getValueFieldWriter(keysVector.valueAt(begin), size);
        // Add the value to the buffer by recording its offset in the values
        // vector.
        valueField->add(begin, nonNullCount);
      }
      ++nonNullCount;
    };

    // Lambda that calculates child ranges
    auto computeKeyRanges = [&](velox::vector_size_t index) {
      keyRanges.add(offsets[index], lengths[index]);
    };

    // Lambda that iterates the vector
    auto processVector = [&](const auto& map, const auto& vector) {
      auto& mapKeys = map->mapKeys();
      if (auto flatKeys = mapKeys->template asFlatVector<KeyType>()) {
        // Keys are flat.
        Flat<KeyType> keysVector{mapKeys};
        iterateIndices<true>(ranges, vector, [&](auto offset) {
          processMap(offset, keysVector);
        });
      } else {
        // Keys are encoded. Decode.
        iterateIndices<false>(ranges, vector, computeKeyRanges);
        auto localDecodedKeys = decode(mapKeys, keyRanges);
        auto& decodedKeys = localDecodedKeys.get();
        Decoded<KeyType> keysVector{decodedKeys};
        iterateIndices<true>(ranges, vector, [&](auto offset) {
          processMap(offset, keysVector);
        });
      }
    };

    // Reset existing value fields for next batch
    for (auto& pair : currentValueFields_) {
      pair.second->prepare(size);
    }

    const velox::MapVector* map = vector->as<velox::MapVector>();
    if (map) {
      // Map is flat
      offsets = map->rawOffsets();
      lengths = map->rawSizes();

      ensureCapacity(map->mayHaveNulls(), size);
      processVector(map, Flat{vector});
    } else {
      // Map is encoded. Decode.
      auto localDecodedMap = decode(vector, ranges);
      auto& decodedMap = localDecodedMap.get();
      map = decodedMap.base()->template as<velox::MapVector>();
      ALPHA_ASSERT(map, "Unexpected vector type");
      offsets = map->rawOffsets();
      lengths = map->rawSizes();

      ensureCapacity(decodedMap.mayHaveNulls(), size);
      processVector(map, Decoded{decodedMap});
    }

    // Now actually ingest the map values
    if (nonNullCount > 0) {
      auto& values = map->mapValues();
      for (auto& pair : currentValueFields_) {
        memoryUsed += pair.second->write(values, nonNullCount);
      }
    }
    nonNullCount_ += nonNullCount;
    return memoryUsed + nullBitmapSize(size);
  }

  void flush(const StreamCollector& collector, bool reset) override {
    if (hasNulls_) {
      flushStream(
          collector,
          false,
          nonNulls_,
          reset,
          typeBuilder_->asFlatMap().nullsDescriptor());
    }
    for (auto& pair : currentValueFields_) {
      pair.second->flush(collector, reset);
    }
    if (reset) {
      FieldWriter::reset();
      nonNullCount_ = 0;
      currentValueFields_.clear();
    }
  }

  void close() override {
    // Add dummy node so we can preserve schema of an empty flat map.
    if (allValueFields_.empty()) {
      auto valueField = FieldWriter::create(context_, valueType_);
      typeBuilder_->asFlatMap().addChild("", valueField->typeBuilder());
    } else {
      for (auto& pair : allValueFields_) {
        pair.second->close();
      }
    }
  }

 private:
  FlatMapValueFieldWriter* getValueFieldWriter(KeyType key, uint32_t size) {
    auto it = currentValueFields_.find(key);
    if (it != currentValueFields_.end()) {
      return it->second;
    }

    auto stringKey = folly::to<std::string>(key);
    ALPHA_DASSERT(!stringKey.empty(), "String key cannot be empty for flatmap");

    // check whether the typebuilder for this key is already present
    auto flatFieldIt = allValueFields_.find(key);
    if (flatFieldIt == allValueFields_.end()) {
      auto valueFieldWriter = FieldWriter::create(context_, valueType_);
      const auto& inMapDescriptor = typeBuilder_->asFlatMap().addChild(
          stringKey, valueFieldWriter->typeBuilder());
      if (context_.flatmapFieldAddedEventHandler) {
        context_.flatmapFieldAddedEventHandler(
            *typeBuilder_, stringKey, *valueFieldWriter->typeBuilder());
      }
      auto flatMapValueField = std::make_unique<FlatMapValueFieldWriter>(
          context_, inMapDescriptor, std::move(valueFieldWriter));
      flatFieldIt =
          allValueFields_.emplace(key, std::move(flatMapValueField)).first;
    }
    // TODO: assert on not having too many keys?
    it = currentValueFields_.emplace(key, flatFieldIt->second.get()).first;

    // At this point we will have at max nonNullCount_ for the field which we
    // backfill to false, later when ingest is completed (using scatter write)
    // we update the inMapBuffer to nonNullCount which represnet the actual
    // values written in file
    it->second->backfill(nonNullCount_, size);
    return it->second;
  }

  // This map store the FlatMapValue fields used in current flush unit.
  folly::F14FastMap<KeyType, FlatMapValueFieldWriter*> currentValueFields_;
  const std::shared_ptr<const velox::dwio::common::TypeWithId>& valueType_;
  uint64_t nonNullCount_ = 0;
  // This map store all FlatMapValue fields encountered by the VeloxWriter
  // across the whole file.
  folly::F14FastMap<KeyType, std::unique_ptr<FlatMapValueFieldWriter>>
      allValueFields_;
};

std::unique_ptr<FieldWriter> createFlatMapFieldWriter(
    FieldWriterContext& context,
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& type) {
  ALPHA_DASSERT(
      type->type()->kind() == velox::TypeKind::MAP,
      "Unexpected flat-map field type.");
  ALPHA_DASSERT(type->size() == 2, "Invalid flat-map field type.");
  auto kind = type->childAt(0)->type()->kind();
  switch (kind) {
    case velox::TypeKind::TINYINT:
      return std::make_unique<FlatMapFieldWriter<velox::TypeKind::TINYINT>>(
          context, type);
    case velox::TypeKind::SMALLINT:
      return std::make_unique<FlatMapFieldWriter<velox::TypeKind::SMALLINT>>(
          context, type);
    case velox::TypeKind::INTEGER:
      return std::make_unique<FlatMapFieldWriter<velox::TypeKind::INTEGER>>(
          context, type);
    case velox::TypeKind::BIGINT:
      return std::make_unique<FlatMapFieldWriter<velox::TypeKind::BIGINT>>(
          context, type);
    case velox::TypeKind::VARCHAR:
      return std::make_unique<FlatMapFieldWriter<velox::TypeKind::VARCHAR>>(
          context, type);
    case velox::TypeKind::VARBINARY:
      return std::make_unique<FlatMapFieldWriter<velox::TypeKind::VARBINARY>>(
          context, type);
    default:
      ALPHA_NOT_SUPPORTED(fmt::format(
          "Unsupported flat map key type {}.",
          type->childAt(0)->type()->toString()));
  }
}

template <velox::TypeKind K>
class ArrayWithOffsetsFieldWriter : public FieldWriter {
  using SourceType = typename velox::TypeTraits<K>::NativeType;
  using OffsetType = uint32_t;

 public:
  ArrayWithOffsetsFieldWriter(
      FieldWriterContext& context,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type)
      : FieldWriter{context, context.schemaBuilder.createArrayWithOffsetsTypeBuilder()},
        lengths_{context.bufferMemoryPool.get()},
        offsets_{context.bufferMemoryPool.get()},
        cached_(false),
        cachedValue_(nullptr),
        cachedSize_(0) {
    elements_ = FieldWriter::create(context, type->childAt(0));

    typeBuilder_->asArrayWithOffsets().setChildren(elements_->typeBuilder());

    cachedValue_ = velox::ArrayVector::create(
        type->type(), 1, context.bufferMemoryPool.get());
  }

  uint64_t write(const velox::VectorPtr& vector, const OrderedRanges& ranges)
      override {
    uint64_t memoryUsed = 0;
    OrderedRanges childFilteredRanges;
    auto array =
        ingestLengthsOffsets(vector, ranges, memoryUsed, childFilteredRanges);
    if (childFilteredRanges.size() > 0) {
      memoryUsed += elements_->write(array->elements(), childFilteredRanges);
    }
    return memoryUsed + nullBitmapSize(ranges.size());
  }

  void flush(const StreamCollector& collector, bool reset) override {
    flushStream(
        collector,
        hasNulls_,
        offsets_,
        reset,
        typeBuilder_->asArrayWithOffsets().offsetsDescriptor());
    if (lengths_.size() > 0) {
      collector(
          typeBuilder_->asArrayWithOffsets().lengthsDescriptor(),
          nullptr,
          convert(lengths_));
      lengths_.clear();
    }
    elements_->flush(collector, reset);
    if (reset) {
      FieldWriter::reset();
      cached_ = false;
      nextOffset_ = 0;
    }
  }

  void close() override {
    elements_->close();
  }

 private:
  std::unique_ptr<FieldWriter> elements_;
  Vector<int32_t> lengths_; /** lengths of the each deduped data */
  Vector<OffsetType> offsets_; /** offsets for each data after dedup */
  OffsetType nextOffset_{0}; /** next available offset for dedup storing */

  bool cached_;
  velox::VectorPtr cachedValue_;
  velox::vector_size_t cachedSize_;

  template <typename Vector>
  uint64_t ingestLengthsOffsetsByElements(
      const velox::ArrayVector* array,
      const Vector& iterableVector,
      const OrderedRanges& ranges,
      const OrderedRanges& childRanges,
      OrderedRanges& filteredRanges) {
    const velox::vector_size_t* rawOffsets = array->rawOffsets();
    const velox::vector_size_t* rawLengths = array->rawSizes();
    velox::vector_size_t prevIndex = -1;
    velox::vector_size_t numLengths = 0;

    std::function<bool(velox::vector_size_t, velox::vector_size_t)>
        compareConsecutive;

    std::function<bool(velox::vector_size_t)> compareToCache;

    /** dedup arrays by consecutive elements */
    auto dedupProc = [&](velox::vector_size_t index) {
      auto const length = rawLengths[index];

      bool match = false;
      /// Don't compare on the first run
      if (prevIndex >= 0) {
        match =
            (index == prevIndex ||
             (length == rawLengths[prevIndex] &&
              compareConsecutive(index, prevIndex)));
      } else if (cached_) { // check cache here
        match = (length == cachedSize_ && compareToCache(index));
      }

      if (!match) {
        if (length > 0) {
          filteredRanges.add(rawOffsets[index], length);
        }
        lengths_.push_back(length);
        ++numLengths;
        ++nextOffset_;
      }

      prevIndex = index;
      offsets_.push_back(nextOffset_ - 1);
    };

    uint32_t numOffsets;
    auto& vectorElements = array->elements();
    if (auto flat = vectorElements->asFlatVector<SourceType>()) {
      /** compare array at index and prevIndex to be equal */
      compareConsecutive = [&](velox::vector_size_t index,
                               velox::vector_size_t prevIndex) {
        bool match = true;
        velox::CompareFlags flags;
        for (velox::vector_size_t idx = 0; idx < rawLengths[index]; ++idx) {
          match = flat->compare(
                          flat,
                          rawOffsets[index] + idx,
                          rawOffsets[prevIndex] + idx,
                          flags)
                      .value_or(-1) == 0;
          if (!match) {
            break;
          }
        }
        return match;
      };

      compareToCache = [&](velox::vector_size_t index) {
        velox::CompareFlags flags;
        return array->compare(cachedValue_.get(), index, 0, flags)
                   .value_or(-1) == 0;
      };

      numOffsets = iterateIndices<false>(ranges, iterableVector, dedupProc);
    } else {
      auto localDecoded = decode(vectorElements, childRanges);
      auto& decoded = localDecoded.get();
      /** compare array at index and prevIndex to be equal */
      compareConsecutive = [&](velox::vector_size_t index,
                               velox::vector_size_t prevIndex) {
        bool match = true;
        for (velox::vector_size_t idx = 0; idx < rawLengths[index]; ++idx) {
          match = equalDecodedVectorIndices<SourceType>(
              decoded, rawOffsets[index] + idx, rawOffsets[prevIndex] + idx);
          if (!match) {
            break;
          }
        }
        return match;
      };

      auto cachedElements =
          (cachedValue_->as<velox::ArrayVector>())->elements();
      auto cachedFlat = cachedElements->asFlatVector<SourceType>();
      compareToCache = [&](velox::vector_size_t index) {
        bool match = true;
        velox::CompareFlags flags;
        for (velox::vector_size_t idx = 0; idx < rawLengths[index]; ++idx) {
          match = compareDecodedVectorToCache<SourceType>(
              decoded, rawOffsets[index] + idx, cachedFlat, idx, flags);
          if (!match) {
            break;
          }
        }
        return match;
      };
      numOffsets = iterateIndices<false>(ranges, iterableVector, dedupProc);
    }

    // Copy the last valid element into the cache.
    // Cache is saved across calls to write(), as long as the same FieldWriter
    // object is used.
    if (prevIndex != -1 && lengths_.size() > 0) {
      cached_ = true;
      cachedSize_ = lengths_[lengths_.size() - 1];
      ALPHA_ASSERT(
          lengths_[lengths_.size() - 1] == rawLengths[prevIndex],
          "Unexpected index: Prev index is not the last item in the list.");
      cachedValue_->prepareForReuse();
      velox::BaseVector::CopyRange cacheRange{
          static_cast<velox::vector_size_t>(prevIndex) /* source index*/,
          0 /* target index*/,
          1 /* count*/};
      cachedValue_->copyRanges(array, folly::Range(&cacheRange, 1));
    }
    return sizeof(velox::vector_size_t) * numLengths +
        sizeof(OffsetType) * numOffsets;
  }

  const velox::ArrayVector* ingestLengthsOffsets(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges,
      uint64_t& memoryUsed,
      OrderedRanges& filteredRanges) {
    auto size = ranges.size();
    const velox::ArrayVector* arrayVector = vector->as<velox::ArrayVector>();
    const velox::vector_size_t* rawOffsets;
    const velox::vector_size_t* rawLengths;
    OrderedRanges childRanges;

    auto proc = [&](velox::vector_size_t index) {
      auto length = rawLengths[index];
      if (length > 0) {
        childRanges.add(rawOffsets[index], length);
      }
    };

    if (arrayVector) {
      rawOffsets = arrayVector->rawOffsets();
      rawLengths = arrayVector->rawSizes();

      ensureCapacity(arrayVector->mayHaveNulls(), size);
      Flat iterableVector{vector};
      iterateIndices<true>(ranges, iterableVector, proc);
      memoryUsed = ingestLengthsOffsetsByElements(
          arrayVector, iterableVector, ranges, childRanges, filteredRanges);
    } else {
      auto localDecoded = decode(vector, ranges);
      auto& decoded = localDecoded.get();
      arrayVector = decoded.base()->template as<velox::ArrayVector>();
      ALPHA_ASSERT(arrayVector, "Unexpected vector type");
      rawOffsets = arrayVector->rawOffsets();
      rawLengths = arrayVector->rawSizes();

      ensureCapacity(decoded.mayHaveNulls(), size);
      Decoded iterableVector{decoded};
      iterateIndices<true>(ranges, iterableVector, proc);
      memoryUsed = ingestLengthsOffsetsByElements(
          arrayVector, iterableVector, ranges, childRanges, filteredRanges);
    }
    return arrayVector;
  }
};

std::unique_ptr<FieldWriter> createArrayWithOffsetsFieldWriter(
    FieldWriterContext& context,
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& type) {
  ALPHA_DASSERT(
      type->type()->kind() == velox::TypeKind::ARRAY,
      "Unexpected offset-array field type.");
  ALPHA_DASSERT(type->size() == 1, "Invalid offset-array field type.");
  auto kind = type->childAt(0)->type()->kind();
  switch (kind) {
    case velox::TypeKind::TINYINT:
      return std::make_unique<
          ArrayWithOffsetsFieldWriter<velox::TypeKind::TINYINT>>(context, type);
    case velox::TypeKind::SMALLINT:
      return std::make_unique<
          ArrayWithOffsetsFieldWriter<velox::TypeKind::SMALLINT>>(
          context, type);
    case velox::TypeKind::INTEGER:
      return std::make_unique<
          ArrayWithOffsetsFieldWriter<velox::TypeKind::INTEGER>>(context, type);
    case velox::TypeKind::BIGINT:
      return std::make_unique<
          ArrayWithOffsetsFieldWriter<velox::TypeKind::BIGINT>>(context, type);
    case velox::TypeKind::REAL:
      return std::make_unique<
          ArrayWithOffsetsFieldWriter<velox::TypeKind::REAL>>(context, type);
    case velox::TypeKind::DOUBLE:
      return std::make_unique<
          ArrayWithOffsetsFieldWriter<velox::TypeKind::DOUBLE>>(context, type);
    default:
      ALPHA_NOT_SUPPORTED(fmt::format(
          "Unsupported dedup array element type {}.",
          type->childAt(0)->type()->toString()));
  }
}

} // namespace

std::unique_ptr<FieldWriter> FieldWriter::create(
    FieldWriterContext& context,
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& type) {
  switch (type->type()->kind()) {
    case velox::TypeKind::BOOLEAN:
      return std::make_unique<SimpleFieldWriter<velox::TypeKind::BOOLEAN>>(
          context);
    case velox::TypeKind::TINYINT:
      return std::make_unique<SimpleFieldWriter<velox::TypeKind::TINYINT>>(
          context);
    case velox::TypeKind::SMALLINT:
      return std::make_unique<SimpleFieldWriter<velox::TypeKind::SMALLINT>>(
          context);
    case velox::TypeKind::INTEGER:
      return std::make_unique<SimpleFieldWriter<velox::TypeKind::INTEGER>>(
          context);
    case velox::TypeKind::BIGINT:
      return std::make_unique<SimpleFieldWriter<velox::TypeKind::BIGINT>>(
          context);
    case velox::TypeKind::REAL:
      return std::make_unique<SimpleFieldWriter<velox::TypeKind::REAL>>(
          context);
    case velox::TypeKind::DOUBLE:
      return std::make_unique<SimpleFieldWriter<velox::TypeKind::DOUBLE>>(
          context);
    case velox::TypeKind::VARCHAR:
      return std::make_unique<
          SimpleFieldWriter<velox::TypeKind::VARCHAR, StringConverter>>(
          context);
    case velox::TypeKind::VARBINARY:
      return std::make_unique<
          SimpleFieldWriter<velox::TypeKind::VARBINARY, StringConverter>>(
          context);
    case velox::TypeKind::TIMESTAMP:
      return std::make_unique<
          SimpleFieldWriter<velox::TypeKind::TIMESTAMP, TimestampConverter>>(
          context);
    case velox::TypeKind::ROW:
      return std::make_unique<RowFieldWriter>(context, type);
    case velox::TypeKind::ARRAY:
      return context.dictionaryArrayNodeIds.contains(type->id())
          ? createArrayWithOffsetsFieldWriter(context, type)
          : std::make_unique<ArrayFieldWriter>(context, type);
    case velox::TypeKind::MAP: {
      return context.flatMapNodeIds.contains(type->id())
          ? createFlatMapFieldWriter(context, type)
          : std::make_unique<MapFieldWriter>(context, type);
    }
    default:
      ALPHA_NOT_SUPPORTED(
          fmt::format("Unsupported kind: {}.", type->type()->kind()));
  }
}

} // namespace facebook::alpha
