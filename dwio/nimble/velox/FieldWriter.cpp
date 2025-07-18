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
#include "dwio/nimble/velox/FieldWriter.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/velox/DeduplicationUtils.h"
#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/SchemaTypes.h"
#include "velox/common/base/CompareFlags.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::nimble {

namespace {

template <velox::TypeKind KIND>
struct NimbleTypeTraits {};

template <>
struct NimbleTypeTraits<velox::TypeKind::BOOLEAN> {
  static constexpr ScalarKind scalarKind = ScalarKind::Bool;
};

template <>
struct NimbleTypeTraits<velox::TypeKind::TINYINT> {
  static constexpr ScalarKind scalarKind = ScalarKind::Int8;
};

template <>
struct NimbleTypeTraits<velox::TypeKind::SMALLINT> {
  static constexpr ScalarKind scalarKind = ScalarKind::Int16;
};

template <>
struct NimbleTypeTraits<velox::TypeKind::INTEGER> {
  static constexpr ScalarKind scalarKind = ScalarKind::Int32;
};

template <>
struct NimbleTypeTraits<velox::TypeKind::BIGINT> {
  static constexpr ScalarKind scalarKind = ScalarKind::Int64;
};

template <>
struct NimbleTypeTraits<velox::TypeKind::REAL> {
  static constexpr ScalarKind scalarKind = ScalarKind::Float;
};

template <>
struct NimbleTypeTraits<velox::TypeKind::DOUBLE> {
  static constexpr ScalarKind scalarKind = ScalarKind::Double;
};

template <>
struct NimbleTypeTraits<velox::TypeKind::VARCHAR> {
  static constexpr ScalarKind scalarKind = ScalarKind::String;
};

template <>
struct NimbleTypeTraits<velox::TypeKind::VARBINARY> {
  static constexpr ScalarKind scalarKind = ScalarKind::Binary;
};

template <>
struct NimbleTypeTraits<velox::TypeKind::TIMESTAMP> {
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

template <bool addNulls, typename Vector, typename Consumer, typename IndexOp>
uint64_t iterateNonNulls(
    const OrderedRanges& ranges,
    nimble::Vector<bool>& nonNulls,
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

template <bool addNulls, typename Vector, typename Op>
uint64_t iterateNonNullIndices(
    const OrderedRanges& ranges,
    nimble::Vector<bool>& nonNulls,
    const Vector& vector,
    const Op& op) {
  return iterateNonNulls<addNulls>(
      ranges, nonNulls, vector, op, [&](auto offset) {
        return vector.index(offset);
      });
}

template <typename Vector, typename Op>
uint64_t iterateNonNullValues(
    const OrderedRanges& ranges,
    nimble::Vector<bool>& nonNulls,
    const Vector& vector,
    const Op& op) {
  return iterateNonNulls<true>(ranges, nonNulls, vector, op, [&](auto offset) {
    return vector.valueAt(offset);
  });
}

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

template <typename T>
std::string_view convert(const Vector<T>& input) {
  return {
      reinterpret_cast<const char*>(input.data()), input.size() * sizeof(T)};
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
                NimbleTypeTraits<K>::scalarKind)),
        valuesStream_{context.createNullableContentStreamData<TargetType>(
            typeBuilder_->asScalar().scalarDescriptor())},
        columnStats_{context.columnStats[valuesStream_.descriptor().offset()]} {
  }

  void write(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges,
      folly::Executor*) override {
    auto size = ranges.size();
    auto& buffer = context_.stringBuffer();
    auto& data = valuesStream_.mutableData();
    uint64_t nullCount = 0;

    if (auto flat = vector->asFlatVector<SourceType>()) {
      valuesStream_.ensureNullsCapacity(flat->mayHaveNulls(), size);
      bool rangeCopied = false;
      if (!flat->mayHaveNulls()) {
        if constexpr (
            std::is_same_v<C, IdentityConverter<SourceType, void>> &&
            K != velox::TypeKind::BOOLEAN) {
          // NOTE: this is currently expensive to grow during a long sequence of
          // ingest
          // operators. We currently achieve a balance via a buffer growth
          // policy. Another factor that can help us reduce this cost is to also
          // consider the stripe progress. However, naive progress based
          // policies can't be combined with the size based policies, and are
          // thus currently not included.
          auto newSize = data.size() + size;
          if (newSize > data.capacity()) {
            auto newCapacity =
                context_.inputBufferGrowthPolicy->getExtendedCapacity(
                    newSize, data.capacity());
            ++context_.inputBufferGrowthStats.count;
            context_.inputBufferGrowthStats.itemCount += newCapacity;
            data.reserve(newCapacity);
          }
          ranges.apply([&](auto offset, auto count) {
            data.insert(
                data.end(),
                flat->rawValues() + offset,
                flat->rawValues() + offset + count);
          });
          rangeCopied = true;
        }
      }

      if (!rangeCopied) {
        auto nonNullCount = iterateNonNullValues(
            ranges,
            valuesStream_.mutableNonNulls(),
            Flat<SourceType>{vector},
            [&](SourceType value) {
              data.push_back(
                  C::convert(value, buffer, valuesStream_.extraMemory()));
            });
        nullCount = size - nonNullCount;
      }
    } else {
      auto decodingContext = context_.getDecodingContext();
      auto& decoded = decodingContext.decode(vector, ranges);
      valuesStream_.ensureNullsCapacity(decoded.mayHaveNulls(), size);
      auto nonNullCount = iterateNonNullValues(
          ranges,
          valuesStream_.mutableNonNulls(),
          Decoded<SourceType>{decoded},
          [&](SourceType value) {
            data.push_back(
                C::convert(value, buffer, valuesStream_.extraMemory()));
          });
      nullCount = size - nonNullCount;
    }

    columnStats_.logicalSize += nullCount +
        ((K == velox::TypeKind::VARCHAR || K == velox::TypeKind::VARBINARY)
             ? valuesStream_.extraMemory()
             : valuesStream_.data().size());
    columnStats_.nullCount += nullCount;
    columnStats_.valueCount += size;
  }

  void reset() override {
    valuesStream_.reset();
  }

 private:
  NullableContentStreamData<TargetType>& valuesStream_;
  ColumnStats& columnStats_;
};

class RowFieldWriter : public FieldWriter {
 public:
  RowFieldWriter(
      FieldWriterContext& context,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type)
      : FieldWriter{context, context.schemaBuilder.createRowTypeBuilder(type->size())},
        nullsStream_{context_.createNullsStreamData<bool>(
            typeBuilder_->asRow().nullsDescriptor())},
        columnStats_{context.columnStats[nullsStream_.descriptor().offset()]} {
    auto rowType =
        std::dynamic_pointer_cast<const velox::RowType>(type->type());

    fields_.reserve(rowType->size());
    for (auto i = 0; i < rowType->size(); ++i) {
      fields_.push_back(FieldWriter::create(context, type->childAt(i)));
      typeBuilder_->asRow().addChild(
          rowType->nameOf(i), fields_.back()->typeBuilder());
    }
  }

  void write(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges,
      folly::Executor* executor = nullptr) override {
    auto size = ranges.size();
    OrderedRanges childRanges;
    const OrderedRanges* childRangesPtr;
    const velox::RowVector* row = vector->as<velox::RowVector>();
    uint64_t nullCount = 0;

    if (row) {
      NIMBLE_CHECK(
          fields_.size() == row->childrenSize(),
          fmt::format(
              "Schema mismatch: expected {} fields, but got {} fields",
              fields_.size(),
              row->childrenSize()));
      nullsStream_.ensureNullsCapacity(vector->mayHaveNulls(), size);
      if (row->mayHaveNulls()) {
        childRangesPtr = &childRanges;
        auto nonNullCount = iterateNonNullIndices<true>(
            ranges,
            nullsStream_.mutableNonNulls(),
            Flat{vector},
            [&](auto offset) { childRanges.add(offset, 1); });
        nullCount = size - nonNullCount;
      } else {
        childRangesPtr = &ranges;
      }
    } else {
      auto decodingContext = context_.getDecodingContext();
      auto& decoded = decodingContext.decode(vector, ranges);
      row = decoded.base()->as<velox::RowVector>();
      NIMBLE_ASSERT(row, "Unexpected vector type");
      NIMBLE_CHECK(
          fields_.size() == row->childrenSize(),
          fmt::format(
              "Schema mismatch: expected {} fields, but got {} fields",
              fields_.size(),
              row->childrenSize()));
      childRangesPtr = &childRanges;
      nullsStream_.ensureNullsCapacity(decoded.mayHaveNulls(), size);
      auto nonNullCount = iterateNonNullIndices<true>(
          ranges,
          nullsStream_.mutableNonNulls(),
          Decoded{decoded},
          [&](auto offset) { childRanges.add(offset, 1); });
      nullCount = size - nonNullCount;
    }

    if (executor) {
      for (auto i = 0; i < fields_.size(); ++i) {
        executor->add([&field = fields_[i],
                       &childVector = row->childAt(i),
                       childRanges = *childRangesPtr]() {
          field->write(childVector, childRanges);
        });
      }
    } else {
      for (auto i = 0; i < fields_.size(); ++i) {
        fields_[i]->write(row->childAt(i), *childRangesPtr);
      }
    }

    columnStats_.logicalSize += nullCount;
    columnStats_.nullCount += nullCount;
    columnStats_.valueCount += size;
  }

  void reset() override {
    nullsStream_.reset();

    for (auto& field : fields_) {
      field->reset();
    }
  }

  void close() override {
    for (auto& field : fields_) {
      field->close();
    }
  }

 private:
  std::vector<std::unique_ptr<FieldWriter>> fields_;
  NullsStreamData& nullsStream_;
  ColumnStats& columnStats_;
};

class MultiValueFieldWriter : public FieldWriter {
 public:
  MultiValueFieldWriter(
      FieldWriterContext& context,
      std::shared_ptr<LengthsTypeBuilder> typeBuilder)
      : FieldWriter{context, std::move(typeBuilder)},
        lengthsStream_{context.createNullableContentStreamData<uint32_t>(
            static_cast<LengthsTypeBuilder&>(*typeBuilder_)
                .lengthsDescriptor())},
        columnStats_{
            context.columnStats[lengthsStream_.descriptor().offset()]} {}

  void reset() override {
    lengthsStream_.reset();
  }

 protected:
  template <typename T>
  const T* ingestLengths(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges,
      OrderedRanges& childRanges) {
    auto size = ranges.size();
    const T* casted = vector->as<T>();
    const velox::vector_size_t* offsets;
    const velox::vector_size_t* lengths;
    auto& data = lengthsStream_.mutableData();

    auto proc = [&](velox::vector_size_t index) {
      auto length = lengths[index];
      if (length > 0) {
        childRanges.add(offsets[index], length);
      }
      data.push_back(length);
    };

    uint64_t nullCount = 0;
    if (casted) {
      offsets = casted->rawOffsets();
      lengths = casted->rawSizes();

      lengthsStream_.ensureNullsCapacity(casted->mayHaveNulls(), size);
      auto nonNullCount = iterateNonNullIndices<true>(
          ranges, lengthsStream_.mutableNonNulls(), Flat{vector}, proc);
      nullCount = size - nonNullCount;
    } else {
      auto decodingContext = context_.getDecodingContext();
      auto& decoded = decodingContext.decode(vector, ranges);
      casted = decoded.base()->as<T>();
      NIMBLE_ASSERT(casted, "Unexpected vector type");
      offsets = casted->rawOffsets();
      lengths = casted->rawSizes();

      lengthsStream_.ensureNullsCapacity(decoded.mayHaveNulls(), size);
      auto nonNullCount = iterateNonNullIndices<true>(
          ranges, lengthsStream_.mutableNonNulls(), Decoded{decoded}, proc);
      nullCount = size - nonNullCount;
    }
    columnStats_.logicalSize += nullCount;
    columnStats_.nullCount += nullCount;
    columnStats_.valueCount += size;

    return casted;
  }

  NullableContentStreamData<uint32_t>& lengthsStream_;
  ColumnStats& columnStats_;
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

    NIMBLE_DASSERT(type->size() == 1, "Invalid array type.");
    elements_ = FieldWriter::create(context, type->childAt(0));

    typeBuilder_->asArray().setChildren(elements_->typeBuilder());
  }

  void write(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges,
      folly::Executor*) override {
    OrderedRanges childRanges;
    auto array = ingestLengths<velox::ArrayVector>(vector, ranges, childRanges);
    if (childRanges.size() > 0) {
      elements_->write(array->elements(), childRanges);
    }
  }

  void reset() override {
    MultiValueFieldWriter::reset();
    elements_->reset();
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

    NIMBLE_DASSERT(type->size() == 2, "Invalid map type.");
    keys_ = FieldWriter::create(context, type->childAt(0));
    values_ = FieldWriter::create(context, type->childAt(1));
    typeBuilder_->asMap().setChildren(
        keys_->typeBuilder(), values_->typeBuilder());
  }

  void write(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges,
      folly::Executor*) override {
    OrderedRanges childRanges;
    auto map = ingestLengths<velox::MapVector>(vector, ranges, childRanges);
    if (childRanges.size() > 0) {
      keys_->write(map->mapKeys(), childRanges);
      values_->write(map->mapValues(), childRanges);
    }
  }

  void reset() override {
    MultiValueFieldWriter::reset();
    keys_->reset();
    values_->reset();
  }

  void close() override {
    keys_->close();
    values_->close();
  }

 private:
  std::unique_ptr<FieldWriter> keys_;
  std::unique_ptr<FieldWriter> values_;
};

class SlidingWindowMapFieldWriter : public FieldWriter {
 public:
  SlidingWindowMapFieldWriter(
      FieldWriterContext& context,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type)
      : FieldWriter{context, context.schemaBuilder.createSlidingWindowMapTypeBuilder()},
        offsetsStream_{context.createNullableContentStreamData<uint32_t>(
            typeBuilder_->asSlidingWindowMap().offsetsDescriptor())},
        lengthsStream_{context.createContentStreamData<uint32_t>(
            typeBuilder_->asSlidingWindowMap().lengthsDescriptor())},
        columnStats_{context.columnStats[lengthsStream_.descriptor().offset()]},
        currentOffset_(0),
        cached_{false},
        cachedLength_{0} {
    NIMBLE_DASSERT(type->size() == 2, "Invalid map type.");
    keys_ = FieldWriter::create(context, type->childAt(0));
    values_ = FieldWriter::create(context, type->childAt(1));
    typeBuilder_->asSlidingWindowMap().setChildren(
        keys_->typeBuilder(), values_->typeBuilder());
    cachedValue_ = velox::MapVector::create(
        type->type(), 1, context.bufferMemoryPool.get());
  }

  void write(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges,
      folly::Executor*) override {
    OrderedRanges childFilteredRanges;
    auto map = ingestOffsetsAndLengthsDeduplicated(
        vector, ranges, childFilteredRanges);
    if (childFilteredRanges.size() > 0) {
      keys_->write(map->mapKeys(), childFilteredRanges);
      values_->write(map->mapValues(), childFilteredRanges);
    }
  }

  void reset() override {
    offsetsStream_.reset();
    lengthsStream_.reset();
    keys_->reset();
    values_->reset();
    currentOffset_ = 0;
    cached_ = false;
    cachedLength_ = 0;
  }

  void close() override {
    keys_->close();
    values_->close();
  }

 private:
  std::unique_ptr<FieldWriter> keys_;
  std::unique_ptr<FieldWriter> values_;
  NullableContentStreamData<uint32_t>& offsetsStream_;
  ContentStreamData<uint32_t>& lengthsStream_;
  ColumnStats& columnStats_;
  uint32_t currentOffset_; /* Global Offset for the data */
  bool cached_;
  velox::vector_size_t cachedLength_;
  velox::VectorPtr cachedValue_;

  const velox::MapVector* ingestOffsetsAndLengthsDeduplicated(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges,
      OrderedRanges& filteredRanges) {
    const auto size = ranges.size();
    const velox::MapVector* mapVector = vector->as<velox::MapVector>();
    const velox::vector_size_t* rawOffsets;
    const velox::vector_size_t* rawLengths;
    velox::vector_size_t lastCompareIndex = -1;
    auto& offsetsData = offsetsStream_.mutableData();
    auto& lengthsData = lengthsStream_.mutableData();

    auto processMapIndex = [&](velox::vector_size_t index) {
      auto const length = rawLengths[index];

      bool match = false;
      // Compare with the last element if not the first elemment
      if (lastCompareIndex >= 0) {
        match = length == rawLengths[lastCompareIndex];
        if (length > 0) {
          match = match &&
              DeduplicationUtils::CompareMapsAtIndex(
                      *mapVector, index, *mapVector, lastCompareIndex);
        }
        // If this is the first element, compare with the cached element from
        // the last batch
      } else if (cached_) {
        match =
            (length == cachedLength_ &&
             DeduplicationUtils::CompareMapsAtIndex(
                 *static_cast<velox::MapVector*>(cachedValue_.get()),
                 0,
                 *mapVector,
                 index));
      }

      if (!match && length > 0) {
        filteredRanges.add(rawOffsets[index], length);
        currentOffset_ += length;
      }
      lengthsData.push_back(length);
      offsetsData.push_back(currentOffset_ - length);
      lastCompareIndex = index;
    };

    uint64_t nullCount = 0;
    if (mapVector) {
      rawOffsets = mapVector->rawOffsets();
      rawLengths = mapVector->rawSizes();
      offsetsStream_.ensureNullsCapacity(mapVector->mayHaveNulls(), size);
      Flat iterableVector{vector};
      auto nonNullCount = iterateNonNullIndices<true>(
          ranges,
          offsetsStream_.mutableNonNulls(),
          iterableVector,
          processMapIndex);
      nullCount = size - nonNullCount;
    } else {
      auto decodingContext = context_.getDecodingContext();
      auto& decoded = decodingContext.decode(vector, ranges);
      mapVector = decoded.base()->template as<velox::MapVector>();
      NIMBLE_ASSERT(mapVector, "Unexpected vector type");
      rawOffsets = mapVector->rawOffsets();
      rawLengths = mapVector->rawSizes();
      offsetsStream_.ensureNullsCapacity(decoded.mayHaveNulls(), size);
      Decoded iterableVector{decoded};
      auto nonNullCount = iterateNonNullIndices<true>(
          ranges,
          offsetsStream_.mutableNonNulls(),
          iterableVector,
          processMapIndex);
      nullCount = size - nonNullCount;
    }

    // Copy the last valid element into the cache.
    const velox::vector_size_t idxOfLastElement = lastCompareIndex;
    if (idxOfLastElement != -1) {
      cached_ = true;
      cachedLength_ = rawLengths[idxOfLastElement];
      cachedValue_->prepareForReuse();
      velox::BaseVector::CopyRange cacheRange{
          idxOfLastElement /* source index*/,
          0 /* target index*/,
          1 /* count*/};
      cachedValue_->copyRanges(mapVector, folly::Range(&cacheRange, 1));
    }

    // Calculate non-deduplicated logical size.
    {
      RawSizeContext context;
      // This code currently uses getRawSizeFromVector to calculate the
      // non-deduplicated logical size. In the future, we should replace this
      // with a more efficient approach that passes OrderedRanges objects to
      // child FieldWriters, allowing them to directly calculate
      // non-deduplicated sizes from deduplicated vectors without needing to
      // rely on an external util.
      columnStats_.logicalSize += getRawSizeFromVector(vector, ranges, context);
      columnStats_.nullCount += nullCount;
      columnStats_.valueCount += size;
    }
    return mapVector;
  }
};

class FlatMapPassthroughValueFieldWriter {
 public:
  FlatMapPassthroughValueFieldWriter(
      FieldWriterContext& context,
      const StreamDescriptorBuilder& inMapDescriptor,
      std::unique_ptr<FieldWriter> valueField)
      : valueField_{std::move(valueField)},
        inMapStream_{context.createContentStreamData<bool>(inMapDescriptor)},
        // TODO(T226402409): Reuse same stats object for all flatmap fields.
        columnStats_{context.columnStats[inMapStream_.descriptor().offset()]} {}

  void write(const velox::VectorPtr& vector, const OrderedRanges& ranges) {
    auto& data = inMapStream_.mutableData();
    data.resize(data.size() + ranges.size(), true);
    valueField_->write(vector, ranges);
    columnStats_.valueCount += ranges.size();
  }

  void reset() {
    inMapStream_.reset();
    valueField_->reset();
  }

  void close() {
    valueField_->close();
  }

 private:
  std::unique_ptr<FieldWriter> valueField_;
  ContentStreamData<bool>& inMapStream_;
  ColumnStats& columnStats_;
};

class FlatMapValueFieldWriter {
 public:
  FlatMapValueFieldWriter(
      FieldWriterContext& context,
      const StreamDescriptorBuilder& inMapDescriptor,
      std::unique_ptr<FieldWriter> valueField)
      : valueField_{std::move(valueField)},
        inMapStream_{context.createContentStreamData<bool>(inMapDescriptor)},
        // TODO(T226402409): Reuse same stats object for all flatmap fields.
        columnStats_{context.columnStats[inMapStream_.descriptor().offset()]} {}

  // Clear the ranges and extend the inMapBuffer
  void prepare(uint32_t numValues) {
    auto& data = inMapStream_.mutableData();
    data.reserve(data.size() + numValues);
    std::fill(data.end(), data.end() + numValues, false);
  }

  // Returns whether the offset is successfully recorded.
  // NOTE: this method is always called after calling prepare(), so
  // we can access the raw in map stream data without size checks.
  bool add(velox::vector_size_t offset, uint32_t mapIndex) {
    auto& data = inMapStream_.mutableData();
    auto index = mapIndex + data.size();
    // The index being already populated means we have a key duplication.
    // In order to avoid another branching here, we perform the rest of the
    // method regardless, knowning that the whole write will be aborted
    // upon key duplication, and the rest of the states wouldn't matter.
    bool keyDuplicated = data[index];
    ranges_.add(offset, 1);
    data[index] = true;
    return !keyDuplicated;
  }

  void write(const velox::VectorPtr& vector, uint32_t mapCount) {
    auto& data = inMapStream_.mutableData();
    data.update_size(data.size() + mapCount);

    if (ranges_.size() > 0) {
      valueField_->write(vector, ranges_);
    }

    columnStats_.valueCount += ranges_.size();
    ranges_.clear();
  }

  void backfill(uint32_t count, uint32_t reserve) {
    inMapStream_.mutableData().resize(count, false);
    prepare(reserve);
  }

  void reset() {
    inMapStream_.reset();
    valueField_->reset();
  }

  void close() {
    valueField_->close();
  }

 private:
  std::unique_ptr<FieldWriter> valueField_;
  ContentStreamData<bool>& inMapStream_;
  ColumnStats& columnStats_;
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
                NimbleTypeTraits<K>::scalarKind)),
        nullsStream_{context_.createNullsStreamData<bool>(
            typeBuilder_->asFlatMap().nullsDescriptor())},
        columnStats_{context_.columnStats[nullsStream_.descriptor().offset()]},
        valueType_{type->childAt(1)},
        nodeId_{type->id()} {}

  void write(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges,
      folly::Executor* executor = nullptr) override {
    // Check if the vector received is already flattened
    const auto isFlatMap = vector->type()->kind() == velox::TypeKind::ROW;
    if (isFlatMap) {
      ingestFlattenedMap(
          velox::RowVector::pushDictionaryToRowVectorLeaves(
              velox::BaseVector::loadedVectorShared(vector)),
          ranges);
    } else {
      ingestMap(vector, ranges, executor);
    }
  }

  FlatMapPassthroughValueFieldWriter& createPassthroughValueFieldWriter(
      const std::string& key) {
    auto fieldWriter = FieldWriter::create(context_, valueType_);
    auto& inMapDescriptor =
        typeBuilder_->asFlatMap().addChild(key, fieldWriter->typeBuilder());
    if (context_.flatmapFieldAddedEventHandler) {
      context_.flatmapFieldAddedEventHandler(
          *typeBuilder_, key, *fieldWriter->typeBuilder());
    }
    auto it = currentPassthroughFields_
                  .insert(
                      {key,
                       std::make_unique<FlatMapPassthroughValueFieldWriter>(
                           context_, inMapDescriptor, std::move(fieldWriter))})
                  .first;
    return *it->second;
  }

  FlatMapPassthroughValueFieldWriter& findPassthroughValueFieldWriter(
      const std::string& key) {
    auto existingPair = currentPassthroughFields_.find(key);
    NIMBLE_ASSERT(
        existingPair != currentPassthroughFields_.end(),
        "Field writer must already exist in map");
    return *existingPair->second;
  }

  void ingestFlattenedMap(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges) {
    NIMBLE_ASSERT(
        currentValueFields_.empty() && allValueFields_.empty(),
        "Mixing map and flatmap vectors in the FlatMapFieldWriter is not supported");
    const auto& flatMap = vector->as<velox::RowVector>();
    NIMBLE_ASSERT(
        flatMap,
        "Unexpected vector type. Vector must be a decoded ROW vector.");
    const auto size = ranges.size();
    nullsStream_.ensureNullsCapacity(flatMap->mayHaveNulls(), size);
    const auto& keys = flatMap->type()->asRow().names();
    const auto& values = flatMap->children();

    OrderedRanges childRanges;
    uint64_t nonNullCount = iterateNonNullIndices<true>(
        ranges, nullsStream_.mutableNonNulls(), Flat{vector}, [&](auto offset) {
          childRanges.add(offset, 1);
        });

    columnStats_.nullCount += size - nonNullCount;
    columnStats_.logicalSize += columnStats_.nullCount;
    columnStats_.valueCount += size;
    // early bail out if no ranges at the top level row vector
    if (childRanges.size() == 0) {
      return;
    }

    // Only create keys on first call to write (with valid ranges).
    // Subsequent calls must have the same set of keys,
    // otherwise writer will throw.
    bool populateMap = currentPassthroughFields_.empty();

    for (int i = 0; i < keys.size(); ++i) {
      const auto& key = keys[i];
      auto& writer = populateMap ? createPassthroughValueFieldWriter(key)
                                 : findPassthroughValueFieldWriter(key);
      writer.write(values[i], childRanges);
    }
  }

  void ingestMap(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges,
      folly::Executor* executor = nullptr) {
    NIMBLE_ASSERT(
        currentPassthroughFields_.empty(),
        "Mixing map and flatmap vectors in the FlatMapFieldWriter is not supported");
    auto size = ranges.size();
    const velox::vector_size_t* offsets;
    const velox::vector_size_t* lengths;
    uint32_t nonNullCount = 0;
    OrderedRanges keyRanges;

    // Lambda that iterates keys of a map and records the offsets to write to
    // particular value node.
    auto processMap = [&](velox::vector_size_t index, auto& keysVector) {
      for (auto elementIdx = offsets[index], end = elementIdx + lengths[index];
           elementIdx < end;
           ++elementIdx) {
        const auto& keyVector = keysVector.valueAt(elementIdx);
        // Accumulate logical key size.
        columnStats_.logicalSize += sizeof(KeyType);
        auto valueField = getValueFieldWriter(keyVector, size);
        // Add the value to the buffer by recording its offset in the values
        // vector.
        NIMBLE_CHECK(
            valueField->add(elementIdx, nonNullCount),
            fmt::format(
                "Duplicate key: {} at flatmap with node id {}",
                folly::to<std::string>(keyVector),
                nodeId_));
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
        iterateNonNullIndices<true>(
            ranges, nullsStream_.mutableNonNulls(), vector, [&](auto offset) {
              processMap(offset, keysVector);
            });
      } else {
        // Keys are encoded. Decode.
        iterateNonNullIndices<false>(
            ranges, nullsStream_.mutableNonNulls(), vector, computeKeyRanges);
        auto decodingContext = context_.getDecodingContext();
        auto& decodedKeys = decodingContext.decode(mapKeys, keyRanges);
        Decoded<KeyType> keysVector{decodedKeys};
        iterateNonNullIndices<true>(
            ranges, nullsStream_.mutableNonNulls(), vector, [&](auto offset) {
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

      nullsStream_.ensureNullsCapacity(map->mayHaveNulls(), size);
      processVector(map, Flat{vector});
    } else {
      // Map is encoded. Decode.
      auto decodingContext = context_.getDecodingContext();
      auto& decodedMap = decodingContext.decode(vector, ranges);
      map = decodedMap.base()->template as<velox::MapVector>();
      NIMBLE_ASSERT(map, "Unexpected vector type");
      offsets = map->rawOffsets();
      lengths = map->rawSizes();

      nullsStream_.ensureNullsCapacity(decodedMap.mayHaveNulls(), size);
      processVector(map, Decoded{decodedMap});
    }

    // Now actually ingest the map values
    if (nonNullCount > 0) {
      auto& values = map->mapValues();

      if (executor) {
        for (auto& pair : currentValueFields_) {
          executor->add([&]() { pair.second->write(values, nonNullCount); });
        }
      } else {
        for (auto& pair : currentValueFields_) {
          pair.second->write(values, nonNullCount);
        }
      }
    }
    nonNullCount_ += nonNullCount;
    columnStats_.nullCount += size - nonNullCount;
    columnStats_.logicalSize += columnStats_.nullCount;
    columnStats_.valueCount += size;
  }

  void reset() override {
    for (auto& field : currentValueFields_) {
      field.second->reset();
    }

    for (auto& field : currentPassthroughFields_) {
      field.second->reset();
    }

    nullsStream_.reset();
    nonNullCount_ = 0;
    currentValueFields_.clear();
  }

  void close() override {
    // Add dummy node so we can preserve schema of an empty flat map
    // when no fields are written
    if (allValueFields_.empty() && currentPassthroughFields_.empty()) {
      auto valueField = FieldWriter::create(context_, valueType_);
      typeBuilder_->asFlatMap().addChild("", valueField->typeBuilder());
    } else {
      for (auto& pair : allValueFields_) {
        pair.second->close();
      }
      for (auto& pair : currentPassthroughFields_) {
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
    NIMBLE_DASSERT(
        !stringKey.empty(), "String key cannot be empty for flatmap");

    // check whether the typebuilder for this key is already present
    auto flatFieldIt = allValueFields_.find(key);
    if (flatFieldIt == allValueFields_.end()) {
      std::scoped_lock<std::mutex> lock{context_.flatMapSchemaMutex};

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

  NullsStreamData& nullsStream_;
  ColumnStats& columnStats_;
  // This map store the FlatMapValue fields used in current flush unit.
  folly::F14FastMap<KeyType, FlatMapValueFieldWriter*> currentValueFields_;

  // This map stores the FlatMapPassthrough fields.
  folly::F14FastMap<
      std::string,
      std::unique_ptr<FlatMapPassthroughValueFieldWriter>>
      currentPassthroughFields_;
  const std::shared_ptr<const velox::dwio::common::TypeWithId>& valueType_;
  const uint32_t nodeId_;
  uint64_t nonNullCount_ = 0;
  // This map store all FlatMapValue fields encountered by the VeloxWriter
  // across the whole file.
  folly::F14FastMap<KeyType, std::unique_ptr<FlatMapValueFieldWriter>>
      allValueFields_;
};

std::unique_ptr<FieldWriter> createFlatMapFieldWriter(
    FieldWriterContext& context,
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& type) {
  NIMBLE_DASSERT(
      type->type()->kind() == velox::TypeKind::MAP,
      "Unexpected flat-map field type.");
  NIMBLE_DASSERT(type->size() == 2, "Invalid flat-map field type.");
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
      NIMBLE_NOT_SUPPORTED(fmt::format(
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
        offsetsStream_{context.createNullableContentStreamData<uint32_t>(
            typeBuilder_->asArrayWithOffsets().offsetsDescriptor())},
        lengthsStream_{context.createContentStreamData<uint32_t>(
            typeBuilder_->asArrayWithOffsets().lengthsDescriptor())},
        columnStats_{
            context_.columnStats[lengthsStream_.descriptor().offset()]},
        cached_(false),
        cachedValue_(nullptr),
        cachedSize_(0) {
    elements_ = FieldWriter::create(context, type->childAt(0));

    typeBuilder_->asArrayWithOffsets().setChildren(elements_->typeBuilder());

    cachedValue_ = velox::ArrayVector::create(
        type->type(), 1, context.bufferMemoryPool.get());
  }

  void write(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges,
      folly::Executor*) override {
    OrderedRanges childFilteredRanges;
    const velox::ArrayVector* array;
    // To unwrap the dictionaryVector we need to cast into ComplexType before
    // extracting value arrayVector
    const auto dictionaryVector =
        vector->as<velox::DictionaryVector<velox::ComplexType>>();
    if (dictionaryVector &&
        dictionaryVector->valueVector()->template as<velox::ArrayVector>() &&
        isDictionaryValidRunLengthEncoded(*dictionaryVector)) {
      array = ingestLengthsOffsetsAlreadyEncoded(
          *dictionaryVector, ranges, childFilteredRanges);
    } else {
      array = ingestLengthsOffsets(vector, ranges, childFilteredRanges);
    }
    if (childFilteredRanges.size() > 0) {
      elements_->write(array->elements(), childFilteredRanges);
    }

    // Calculate non-deduplicated logical size.
    {
      RawSizeContext context;
      // This code currently uses getRawSizeFromVector to calculate the
      // non-deduplicated logical size. In the future, we should replace this
      // with a more efficient approach that passes OrderedRanges objects to
      // child FieldWriters, allowing them to directly calculate
      // non-deduplicated sizes from deduplicated vectors without needing to
      // rely on an external util.
      columnStats_.logicalSize += getRawSizeFromVector(vector, ranges, context);
      columnStats_.nullCount += context.nullCount;
      columnStats_.valueCount += ranges.size();
    }
  }

  void reset() override {
    offsetsStream_.reset();
    lengthsStream_.reset();
    elements_->reset();

    cached_ = false;
    nextOffset_ = 0;
  }

  void close() override {
    elements_->close();
  }

 private:
  std::unique_ptr<FieldWriter> elements_;
  NullableContentStreamData<uint32_t>&
      offsetsStream_; /** offsets for each data after dedup */
  ContentStreamData<uint32_t>&
      lengthsStream_; /** lengths of the each deduped data */
  ColumnStats& columnStats_;
  OffsetType nextOffset_{0}; /** next available offset for dedup storing */

  bool cached_;
  velox::VectorPtr cachedValue_;
  velox::vector_size_t cachedSize_;

  /*
   * Check if the dictionary is valid run length encoded.
   * A dictionary is valid if its offsets in order are
   * increasing or equal. Two or more offsets are equal
   * when the dictionary has been deduped (the values
   * vec will be smaller as a result)
   * The read side expects offsets to be ordered for caching,
   * so we need to ensure that they are ordered if we are going to
   * passthrough the dictionary without applying any offset dedup logic.
   * Dictionaries of 0 or size 1 are always considered dictionary length
   * encoded since there are 0 or 1 offsets to validate.
   */
  bool isDictionaryValidRunLengthEncoded(
      const velox::DictionaryVector<velox::ComplexType>& dictionaryVector) {
    const velox::vector_size_t* indices =
        dictionaryVector.indices()->template as<velox::vector_size_t>();
    for (int i = 1; i < dictionaryVector.size(); ++i) {
      if (indices[i] < indices[i - 1]) {
        return false;
      }
    }

    return true;
  }

  velox::ArrayVector* ingestLengthsOffsetsAlreadyEncoded(
      const velox::DictionaryVector<velox::ComplexType>& dictionaryVector,
      const OrderedRanges& ranges,
      OrderedRanges& filteredRanges) {
    auto size = ranges.size();
    offsetsStream_.ensureNullsCapacity(dictionaryVector.mayHaveNulls(), size);

    auto& offsetsData = offsetsStream_.mutableData();
    auto& lengthsData = lengthsStream_.mutableData();
    auto& nonNulls = offsetsStream_.mutableNonNulls();

    const velox::vector_size_t* offsets =
        dictionaryVector.indices()->template as<velox::vector_size_t>();
    auto valuesArrayVector =
        dictionaryVector.valueVector()->template as<velox::ArrayVector>();

    auto previousOffset = -1;
    bool newElementIngested = false;
    auto ingestDictionaryIndex = [&](auto index) {
      bool match = false;
      // Only write length if first element or if consecutive offset is
      // different, meaning we have reached a new value element.
      if (previousOffset >= 0) {
        match = (offsets[index] == previousOffset);
      } else if (cached_) {
        velox::CompareFlags flags;
        match =
            (valuesArrayVector->sizeAt(offsets[index]) == cachedSize_ &&
             valuesArrayVector
                     ->compare(cachedValue_.get(), offsets[index], 0, flags)
                     .value_or(-1) == 0);
      }

      if (!match) {
        auto arrayOffset = valuesArrayVector->offsetAt(offsets[index]);
        auto length = valuesArrayVector->sizeAt(offsets[index]);
        lengthsData.push_back(length);
        newElementIngested = true;
        if (length > 0) {
          filteredRanges.add(arrayOffset, length);
        }
        ++nextOffset_;
      }

      offsetsData.push_back(nextOffset_ - 1);
      previousOffset = offsets[index];
    };

    if (dictionaryVector.mayHaveNulls()) {
      ranges.applyEach([&](auto index) {
        auto notNull = !dictionaryVector.isNullAt(index);
        nonNulls.push_back(notNull);
        if (notNull) {
          ingestDictionaryIndex(index);
        }
      });
    } else {
      ranges.applyEach([&](auto index) { ingestDictionaryIndex(index); });
    }

    // insert last element discovered into cache
    if (newElementIngested) {
      cached_ = true;
      cachedSize_ = lengthsData[lengthsData.size() - 1];
      cachedValue_->prepareForReuse();
      velox::BaseVector::CopyRange cacheRange{
          static_cast<velox::vector_size_t>(previousOffset) /* source index*/,
          0 /* target index*/,
          1 /* count*/};
      cachedValue_->copyRanges(valuesArrayVector, folly::Range(&cacheRange, 1));
    }

    return valuesArrayVector;
  }

  template <typename Vector>
  void ingestLengthsOffsetsByElements(
      const velox::ArrayVector* array,
      const Vector& iterableVector,
      const OrderedRanges& ranges,
      const OrderedRanges& childRanges,
      OrderedRanges& filteredRanges) {
    const velox::vector_size_t* rawOffsets = array->rawOffsets();
    const velox::vector_size_t* rawLengths = array->rawSizes();
    velox::vector_size_t prevIndex = -1;
    auto& lengthsData = lengthsStream_.mutableData();
    auto& offsetsData = offsetsStream_.mutableData();
    auto& nonNulls = offsetsStream_.mutableNonNulls();

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
        lengthsData.push_back(length);
        ++nextOffset_;
      }

      prevIndex = index;
      offsetsData.push_back(nextOffset_ - 1);
    };

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

      iterateNonNullIndices<false>(ranges, nonNulls, iterableVector, dedupProc);
    } else {
      auto decodingContext = context_.getDecodingContext();
      auto& decoded = decodingContext.decode(vectorElements, childRanges);
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
      iterateNonNullIndices<false>(ranges, nonNulls, iterableVector, dedupProc);
    }

    // Copy the last valid element into the cache.
    // Cache is saved across calls to write(), as long as the same FieldWriter
    // object is used.
    if (prevIndex != -1 && lengthsData.size() > 0) {
      cached_ = true;
      cachedSize_ = lengthsData[lengthsData.size() - 1];
      NIMBLE_ASSERT(
          lengthsData[lengthsData.size() - 1] == rawLengths[prevIndex],
          "Unexpected index: Prev index is not the last item in the list.");
      cachedValue_->prepareForReuse();
      velox::BaseVector::CopyRange cacheRange{
          static_cast<velox::vector_size_t>(prevIndex) /* source index*/,
          0 /* target index*/,
          1 /* count*/};
      cachedValue_->copyRanges(array, folly::Range(&cacheRange, 1));
    }
  }

  const velox::ArrayVector* ingestLengthsOffsets(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges,
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

      offsetsStream_.ensureNullsCapacity(arrayVector->mayHaveNulls(), size);
      Flat iterableVector{vector};
      iterateNonNullIndices<true>(
          ranges, offsetsStream_.mutableNonNulls(), iterableVector, proc);
      ingestLengthsOffsetsByElements(
          arrayVector, iterableVector, ranges, childRanges, filteredRanges);
    } else {
      auto decodingContext = context_.getDecodingContext();
      auto& decoded = decodingContext.decode(vector, ranges);
      arrayVector = decoded.base()->template as<velox::ArrayVector>();
      NIMBLE_ASSERT(arrayVector, "Unexpected vector type");
      rawOffsets = arrayVector->rawOffsets();
      rawLengths = arrayVector->rawSizes();

      offsetsStream_.ensureNullsCapacity(decoded.mayHaveNulls(), size);
      Decoded iterableVector{decoded};
      iterateNonNullIndices<true>(
          ranges, offsetsStream_.mutableNonNulls(), iterableVector, proc);
      ingestLengthsOffsetsByElements(
          arrayVector, iterableVector, ranges, childRanges, filteredRanges);
    }
    return arrayVector;
  }
};

std::unique_ptr<FieldWriter> createArrayWithOffsetsFieldWriter(
    FieldWriterContext& context,
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& type) {
  NIMBLE_DASSERT(
      type->type()->kind() == velox::TypeKind::ARRAY,
      "Unexpected offset-array field type.");
  NIMBLE_DASSERT(type->size() == 1, "Invalid offset-array field type.");
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
      NIMBLE_NOT_SUPPORTED(fmt::format(
          "Unsupported dedup array element type {}.",
          type->childAt(0)->type()->toString()));
  }
}

} // namespace

DecodingContextPool::DecodingContext::DecodingContext(
    DecodingContextPool& pool,
    std::unique_ptr<velox::DecodedVector> decodedVector,
    std::unique_ptr<velox::SelectivityVector> selectivityVector)
    : pool_{pool},
      decodedVector_{std::move(decodedVector)},
      selectivityVector_{std::move(selectivityVector)} {}

DecodingContextPool::DecodingContext::~DecodingContext() {
  pool_.addContext(std::move(decodedVector_), std::move(selectivityVector_));
}

velox::DecodedVector& DecodingContextPool::DecodingContext::decode(
    const velox::VectorPtr& vector,
    const OrderedRanges& ranges) {
  selectivityVector_->resize(vector->size());
  selectivityVector_->clearAll();
  ranges.apply([&](auto offset, auto size) {
    selectivityVector_->setValidRange(offset, offset + size, true);
  });
  selectivityVector_->updateBounds();

  decodedVector_->decode(*vector, *selectivityVector_);
  return *decodedVector_;
}

DecodingContextPool::DecodingContextPool(
    std::function<void(void)> vectorDecoderVisitor)
    : vectorDecoderVisitor_{std::move(vectorDecoderVisitor)} {
  NIMBLE_CHECK(vectorDecoderVisitor_, "vectorDecoderVisitor must be set");
  pool_.reserve(std::thread::hardware_concurrency());
}

void DecodingContextPool::addContext(
    std::unique_ptr<velox::DecodedVector> decodedVector,
    std::unique_ptr<velox::SelectivityVector> selectivityVector) {
  std::scoped_lock<std::mutex> lock{mutex_};
  pool_.push_back(
      std::pair(std::move(decodedVector), std::move(selectivityVector)));
}

DecodingContextPool::DecodingContext DecodingContextPool::reserveContext() {
  vectorDecoderVisitor_();

  std::scoped_lock<std::mutex> lock{mutex_};
  if (pool_.empty()) {
    return DecodingContext{
        *this,
        std::make_unique<velox::DecodedVector>(),
        std::make_unique<velox::SelectivityVector>()};
  }

  auto pair = std::move(pool_.back());
  pool_.pop_back();
  return DecodingContext{*this, std::move(pair.first), std::move(pair.second)};
}

size_t DecodingContextPool::size() const {
  return pool_.size();
}

std::unique_ptr<FieldWriter> FieldWriter::create(
    FieldWriterContext& context,
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& type,
    std::function<void(const TypeBuilder&)> typeAddedHandler) {
  context.typeAddedHandler = std::move(typeAddedHandler);
  return create(context, type);
}

std::unique_ptr<FieldWriter> FieldWriter::create(
    FieldWriterContext& context,
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& type) {
  std::unique_ptr<FieldWriter> field;
  switch (type->type()->kind()) {
    case velox::TypeKind::BOOLEAN: {
      field = std::make_unique<SimpleFieldWriter<velox::TypeKind::BOOLEAN>>(
          context);
      break;
    }
    case velox::TypeKind::TINYINT: {
      field = std::make_unique<SimpleFieldWriter<velox::TypeKind::TINYINT>>(
          context);
      break;
    }
    case velox::TypeKind::SMALLINT: {
      field = std::make_unique<SimpleFieldWriter<velox::TypeKind::SMALLINT>>(
          context);
      break;
    }
    case velox::TypeKind::INTEGER: {
      field = std::make_unique<SimpleFieldWriter<velox::TypeKind::INTEGER>>(
          context);
      break;
    }
    case velox::TypeKind::BIGINT: {
      field =
          std::make_unique<SimpleFieldWriter<velox::TypeKind::BIGINT>>(context);
      break;
    }
    case velox::TypeKind::REAL: {
      field =
          std::make_unique<SimpleFieldWriter<velox::TypeKind::REAL>>(context);
      break;
    }
    case velox::TypeKind::DOUBLE: {
      field =
          std::make_unique<SimpleFieldWriter<velox::TypeKind::DOUBLE>>(context);
      break;
    }
    case velox::TypeKind::VARCHAR: {
      field = std::make_unique<
          SimpleFieldWriter<velox::TypeKind::VARCHAR, StringConverter>>(
          context);
      break;
    }
    case velox::TypeKind::VARBINARY: {
      field = std::make_unique<
          SimpleFieldWriter<velox::TypeKind::VARBINARY, StringConverter>>(
          context);
      break;
    }
    case velox::TypeKind::TIMESTAMP: {
      field = std::make_unique<
          SimpleFieldWriter<velox::TypeKind::TIMESTAMP, TimestampConverter>>(
          context);
      break;
    }
    case velox::TypeKind::ROW: {
      field = std::make_unique<RowFieldWriter>(context, type);
      break;
    }
    case velox::TypeKind::ARRAY: {
      field = context.dictionaryArrayNodeIds.contains(type->id())
          ? createArrayWithOffsetsFieldWriter(context, type)
          : std::make_unique<ArrayFieldWriter>(context, type);
      break;
    }
    case velox::TypeKind::MAP: {
      // A map can both be a flat map and a deduplicated map.
      // Flat map takes precedence over deduplicated map, i.e. the outer map
      // will be a flat map whereas the child maps will be deduplicated.
      if (context.flatMapNodeIds.contains(type->id())) {
        field = createFlatMapFieldWriter(context, type);
      } else if (context.deduplicatedMapNodeIds.contains(type->id())) {
        field = std::make_unique<SlidingWindowMapFieldWriter>(context, type);
      } else {
        field = std::make_unique<MapFieldWriter>(context, type);
      }
      break;
    }
    default:
      NIMBLE_NOT_SUPPORTED(
          fmt::format("Unsupported kind: {}.", type->type()->kind()));
  }

  context.typeAddedHandler(*field->typeBuilder());

  return field;
}

} // namespace facebook::nimble
