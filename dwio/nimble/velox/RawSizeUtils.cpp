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
#include "dwio/nimble/velox/RawSizeUtils.h"

#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatMapVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::nimble {

// Timestamp logical size in bytes.
// This matches DWRF's approach: 8 bytes for seconds (int64) + 4 bytes for nanos
// (int32). Note: sizeof(velox::Timestamp) is 16 bytes (8+8), but we use 12
// bytes for raw size to match DWRF behavior and Nimble FieldWriter's
// kTimestampLogicalSize.
constexpr uint64_t kTimestampLogicalSize = 12;

// Returns the size in bytes for a given TypeKind.
// Used for calculating key sizes in passthrough flatmaps and for
// handling type mismatches between vector types and schema types.
// Returns std::nullopt for variable-length types (string or complex types)
std::optional<size_t> getTypeSizeFromKind(velox::TypeKind kind) {
  switch (kind) {
    case velox::TypeKind::BOOLEAN:
      return sizeof(bool);
    case velox::TypeKind::TINYINT:
      return sizeof(int8_t);
    case velox::TypeKind::SMALLINT:
      return sizeof(int16_t);
    case velox::TypeKind::INTEGER:
      return sizeof(int32_t);
    case velox::TypeKind::BIGINT:
      return sizeof(int64_t);
    case velox::TypeKind::REAL:
      return sizeof(float);
    case velox::TypeKind::DOUBLE:
      return sizeof(double);
    case velox::TypeKind::TIMESTAMP:
      return kTimestampLogicalSize;
    default:
      // Variable-length types (string or complex types)
      // don't have a fixed size
      return std::nullopt;
  }
}

namespace {

// Adapter for flat vectors - provides unified interface for null checking and
// index mapping.
template <typename VectorType>
class FlatAdapter {
 public:
  explicit FlatAdapter(const VectorType& vector) : vector_(vector) {}

  bool mayHaveNulls() const {
    return vector_.mayHaveNulls();
  }

  bool isNullAt(velox::vector_size_t idx) const {
    return vector_.isNullAt(idx);
  }

  velox::vector_size_t index(velox::vector_size_t idx) const {
    return idx;
  }

  template <typename T>
  T valueAt(velox::vector_size_t idx) const {
    return vector_.valueAt(idx);
  }

 private:
  const VectorType& vector_;
};

// Adapter for decoded vectors - provides unified interface for null checking
// and index mapping. IgnoreNulls template parameter controls whether nulls are
// checked (like FieldWriter.cpp's Decoded<T, IgnoreNulls>).
template <bool IgnoreNulls = false>
class DecodedAdapter {
 public:
  explicit DecodedAdapter(const velox::DecodedVector& decoded)
      : decoded_(decoded) {}

  bool mayHaveNulls() const {
    if constexpr (IgnoreNulls) {
      return false;
    } else {
      return decoded_.mayHaveNulls();
    }
  }

  bool isNullAt(velox::vector_size_t idx) const {
    if constexpr (IgnoreNulls) {
      return false;
    } else {
      return decoded_.isNullAt(idx);
    }
  }

  velox::vector_size_t index(velox::vector_size_t idx) const {
    return decoded_.index(idx);
  }

  template <typename T>
  T valueAt(velox::vector_size_t idx) const {
    return decoded_.valueAt<T>(idx);
  }

 private:
  const velox::DecodedVector& decoded_;
};

// Helper to count nulls over a range using an adapter.
template <typename Adapter>
uint64_t countNulls(
    const Adapter& adapter,
    const velox::common::Ranges& ranges) {
  uint64_t nullCount = 0;
  if (adapter.mayHaveNulls()) {
    for (const auto& row : ranges) {
      if (adapter.isNullAt(row)) {
        ++nullCount;
      }
    }
  }
  return nullCount;
}

// Computes total string size and null count for string vectors.
// Works with both FlatAdapter and DecodedAdapter.
template <typename Adapter>
std::pair<uint64_t, uint64_t> computeStringSizeAndNulls(
    const Adapter& adapter,
    const velox::common::Ranges& ranges) {
  uint64_t nullCount = 0;
  uint64_t totalStringSize = 0;
  if (adapter.mayHaveNulls()) {
    for (const auto& row : ranges) {
      if (adapter.isNullAt(row)) {
        ++nullCount;
      } else {
        totalStringSize +=
            adapter.template valueAt<velox::StringView>(row).size();
      }
    }
  } else {
    for (const auto& row : ranges) {
      totalStringSize +=
          adapter.template valueAt<velox::StringView>(row).size();
    }
  }
  return {totalStringSize, nullCount};
}

// Builds child ranges, counting nulls. Works with both FlatAdapter and
// DecodedAdapter. ProcessRow is a lambda that takes the mapped index and
// processes it. Returns the number of nulls encountered.
template <typename Adapter, typename ProcessRow>
uint64_t buildChildRanges(
    const Adapter& adapter,
    const velox::common::Ranges& ranges,
    ProcessRow&& processRow) {
  uint64_t nullCount = 0;
  if (adapter.mayHaveNulls()) {
    for (const auto& row : ranges) {
      if (adapter.isNullAt(row)) {
        ++nullCount;
      } else {
        processRow(adapter.index(row));
      }
    }
  } else {
    for (const auto& row : ranges) {
      processRow(adapter.index(row));
    }
  }
  return nullCount;
}

} // namespace

// Computes raw size for fixed-width scalar vectors.
// The requestTypeWidth parameter allows overriding the physical sizeof(T) with
// a different width (e.g., TIMESTAMP uses 12 bytes instead of 16, or when
// upcasting from smaller to larger integer types).
template <velox::TypeKind K>
uint64_t getRawSizeFromFixedWidthVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    std::optional<uint64_t> requestTypeWidth = std::nullopt) {
  VELOX_CHECK_NOT_NULL(vector);
  VELOX_DCHECK(
      K == velox::TypeKind::BOOLEAN || K == velox::TypeKind::TINYINT ||
          K == velox::TypeKind::SMALLINT || K == velox::TypeKind::INTEGER ||
          K == velox::TypeKind::BIGINT || K == velox::TypeKind::REAL ||
          K == velox::TypeKind::DOUBLE || K == velox::TypeKind::TIMESTAMP,
      "Wrong vector type. Expected BOOLEAN | TINYINT | SMALLINT | INTEGER | BIGINT | REAL | DOUBLE | TIMESTAMP.");
  using T = typename velox::TypeTraits<K>::NativeType;

  // Use provided request type width or default to sizeof(T)
  const uint64_t elementSize = requestTypeWidth.value_or(sizeof(T));

  const auto& encoding = vector->encoding();
  switch (encoding) {
    case velox::VectorEncoding::Simple::FLAT: {
      const auto* flatVector = vector->asFlatVector<T>();
      VELOX_CHECK_NOT_NULL(
          flatVector,
          "Encoding mismatch on FlatVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      const uint64_t nullCount = countNulls(FlatAdapter(*flatVector), ranges);
      context.nullCount = nullCount;
      return ((ranges.size() - nullCount) * elementSize) +
          (nullCount * kNullSize);
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      const auto* constVector = vector->as<velox::ConstantVector<T>>();
      VELOX_CHECK_NOT_NULL(
          constVector,
          "Encoding mismatch on ConstantVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      context.nullCount = constVector->mayHaveNulls() ? ranges.size() : 0;
      return constVector->mayHaveNulls() ? ranges.size() * kNullSize
                                         : ranges.size() * elementSize;
    }
    default: {
      // Decode the vector to handle any encoding (FLAT, DICTIONARY, etc.)
      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*vector);

      const uint64_t nullCount =
          countNulls(DecodedAdapter(decodedVector), ranges);
      context.nullCount = nullCount;
      return ((ranges.size() - nullCount) * elementSize) +
          (nullCount * kNullSize);
    }
  }
}

uint64_t getRawSizeFromStringVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context) {
  VELOX_CHECK_NOT_NULL(vector);
  VELOX_DCHECK(
      vector->typeKind() == velox::TypeKind::VARCHAR ||
          vector->typeKind() == velox::TypeKind::VARBINARY,
      "Wrong vector type. Expected VARCHAR | VARBINARY.");

  const auto& encoding = vector->encoding();
  switch (encoding) {
    case velox::VectorEncoding::Simple::FLAT: {
      const auto* flatVector = vector->asFlatVector<velox::StringView>();
      VELOX_CHECK_NOT_NULL(
          flatVector,
          "Encoding mismatch on FlatVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      auto [totalStringSize, nullCount] =
          computeStringSizeAndNulls(FlatAdapter(*flatVector), ranges);

      context.nullCount = nullCount;
      return totalStringSize + (nullCount * kNullSize);
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      const auto* constVector =
          vector->as<velox::ConstantVector<velox::StringView>>();
      VELOX_CHECK_NOT_NULL(
          constVector,
          "Encoding mismatch on ConstantVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      context.nullCount = constVector->mayHaveNulls() ? ranges.size() : 0;
      return constVector->mayHaveNulls()
          ? ranges.size() * kNullSize
          : ranges.size() * constVector->value().size();
    }
    default: {
      // Decode the vector to handle any encoding (FLAT, DICTIONARY, etc.)
      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*vector);

      auto [totalStringSize, nullCount] =
          computeStringSizeAndNulls(DecodedAdapter(decodedVector), ranges);

      context.nullCount = nullCount;
      return totalStringSize + (nullCount * kNullSize);
    }
  }
}

// When type is nullptr, uses vector's actual type for calculations.
// When type is non-null, uses schema type for size calculations.
uint64_t getRawSizeFromArrayVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId* type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds) {
  VELOX_CHECK_NOT_NULL(vector);
  const auto& encoding = vector->encoding();

  const velox::ArrayVector* arrayVector;
  const velox::vector_size_t* offsets;
  const velox::vector_size_t* sizes;
  velox::common::Ranges childRanges;
  uint64_t nullCount = 0;
  auto processRow = [&](size_t row) {
    auto begin = offsets[row];
    auto end = begin + sizes[row];
    // Ensure valid size
    if (sizes[row] > 0) {
      VELOX_CHECK_LE(end, arrayVector->elements()->size());
      childRanges.add(begin, end);
    }
  };

  switch (encoding) {
    case velox::VectorEncoding::Simple::CONSTANT: {
      const auto* constantVector =
          vector->as<velox::ConstantVector<velox::ComplexType>>();
      VELOX_CHECK_NOT_NULL(
          constantVector,
          "Encoding mismatch on ConstantVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      const auto& valueVector = constantVector->valueVector();
      const auto& index = constantVector->index();

      // Get raw size for the single constant value
      uint64_t singleRowSize = getRawSizeFromArrayVector(
          valueVector,
          velox::common::Ranges::of(index, index + 1),
          context,
          type,
          flatMapNodeIds);

      context.nullCount = 0;
      return singleRowSize * ranges.size();
    }
    case velox::VectorEncoding::Simple::ARRAY: {
      arrayVector = vector->as<velox::ArrayVector>();
      VELOX_CHECK_NOT_NULL(
          arrayVector,
          "Encoding mismatch on ArrayVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());
      offsets = arrayVector->rawOffsets();
      sizes = arrayVector->rawSizes();

      nullCount =
          buildChildRanges(FlatAdapter(*arrayVector), ranges, processRow);

      break;
    }
    default: {
      // Decode the vector to handle any encoding (ARRAY, DICTIONARY, etc.)
      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*vector);

      arrayVector = decodedVector.base()->as<velox::ArrayVector>();
      VELOX_CHECK_NOT_NULL(
          arrayVector,
          "Encoding mismatch on ArrayVector. Encoding: {}. TypeKind: {}.",
          decodedVector.base()->encoding(),
          decodedVector.base()->typeKind());

      offsets = arrayVector->rawOffsets();
      sizes = arrayVector->rawSizes();

      nullCount =
          buildChildRanges(DecodedAdapter(decodedVector), ranges, processRow);

      break;
    }
  }

  uint64_t rawSize = 0;
  if (childRanges.size()) {
    // Use schema's element type for computing sizes
    rawSize += getRawSizeFromVector(
        arrayVector->elements(),
        childRanges,
        context,
        type ? type->childAt(0).get() : nullptr,
        flatMapNodeIds);
  }

  context.nullCount = nullCount;
  if (nullCount) {
    rawSize += nullCount * kNullSize;
  }

  return rawSize;
}

namespace {
uint64_t getRawSizeFromMapVector(
    const velox::MapVector& mapVector,
    const velox::common::Ranges& childRanges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId* type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds) {
  uint64_t rawSize = 0;
  // Use schema's key/value types for computing sizes
  rawSize += getRawSizeFromVector(
      mapVector.mapKeys(),
      childRanges,
      context,
      type ? type->childAt(0).get() : nullptr,
      flatMapNodeIds);
  rawSize += getRawSizeFromVector(
      mapVector.mapValues(),
      childRanges,
      context,
      type ? type->childAt(1).get() : nullptr,
      flatMapNodeIds);
  return rawSize;
}

// For flat map vectors, we need to merge the base ranges with the "valid"
// ranges for each key. Valid ranges for a key are controled by `inMap` buffers,
// which dictate in which rows a particular key is present/active.
uint64_t getRawSizeFromFlatMapVector(
    const velox::FlatMapVector& flatMapVector,
    const velox::common::Ranges& baseRanges,
    RawSizeContext& context) {
  uint64_t rawSize = 0;

  if (baseRanges.size()) {
    velox::common::Ranges keyRanges;

    for (size_t i = 0; i < flatMapVector.numDistinctKeys(); ++i) {
      keyRanges.clear();

      const uint32_t keySize = getRawSizeFromVector(
          flatMapVector.distinctKeys(),
          velox::common::Ranges::of(i, i + 1),
          context);

      // Process the keys and values for the rows where the key is present.
      if (auto& inMaps = flatMapVector.inMapsAt(i)) {
        const auto* rawInMaps = inMaps->as<uint64_t>();
        for (const auto& row : baseRanges) {
          if (velox::bits::isBitSet(rawInMaps, row)) {
            keyRanges.add(row, row + 1);
          }
        }

        rawSize += getRawSizeFromVector(
            flatMapVector.mapValuesAt(i), keyRanges, context);
        rawSize += keySize * keyRanges.size();
      }
      // If there is no inMap buffer, process all rows.
      else {
        rawSize += getRawSizeFromVector(
            flatMapVector.mapValuesAt(i), baseRanges, context);
        rawSize += keySize * baseRanges.size();
      }
    }
  }
  return rawSize;
}

} // namespace

uint64_t getRawSizeFromMap(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId* type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds) {
  VELOX_CHECK_NOT_NULL(vector);
  auto encoding = vector->encoding();

  if (encoding == velox::VectorEncoding::Simple::CONSTANT) {
    const auto* constantVector =
        vector->as<velox::ConstantVector<velox::ComplexType>>();
    VELOX_CHECK_NOT_NULL(
        constantVector,
        "Encoding mismatch on ConstantVector. Encoding: {}. TypeKind: {}.",
        encoding,
        vector->typeKind());

    const auto& valueVector = constantVector->valueVector();
    const auto& index = constantVector->index();
    velox::common::Ranges childRanges;
    childRanges.add(index, index + 1);

    // Get raw size for the single constant value
    uint64_t singleRowSize = getRawSizeFromMap(
        valueVector, childRanges, context, type, flatMapNodeIds);

    context.nullCount = 0;
    return singleRowSize * ranges.size();
  }

  const velox::MapVector* mapVector;

  const velox::vector_size_t* offsets;
  const velox::vector_size_t* sizes;
  velox::common::Ranges childRanges;

  uint64_t rawSize = 0;
  uint64_t nullCount = 0;

  auto processMapRow = [&](size_t row) {
    auto begin = offsets[row];
    auto end = begin + sizes[row];
    // Ensure valid size
    if (sizes[row] > 0) {
      VELOX_CHECK_LE(end, mapVector->mapKeys()->size());
      VELOX_CHECK_LE(end, mapVector->mapValues()->size());
      childRanges.add(begin, end);
    }
  };

  const velox::BaseVector* decoded = vector.get();
  auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
      context.getDecodedVectorManager());
  velox::DecodedVector& decodedVector = localDecodedVector.get();
  decodedVector.decode(*vector);
  decoded = decodedVector.base();
  encoding = decoded->encoding();

  switch (encoding) {
    // Handle regular map vectors.
    case velox::VectorEncoding::Simple::MAP: {
      mapVector = decoded->as<velox::MapVector>();
      VELOX_CHECK_NOT_NULL(
          mapVector,
          "Encoding mismatch on MapVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      offsets = mapVector->rawOffsets();
      sizes = mapVector->rawSizes();

      nullCount = buildChildRanges(
          DecodedAdapter(decodedVector), ranges, processMapRow);
      rawSize += getRawSizeFromMapVector(
          *mapVector, childRanges, context, type, flatMapNodeIds);
      break;
    }

    // Handle flat map vectors.
    case velox::VectorEncoding::Simple::FLAT_MAP: {
      auto flatMapVector = decoded->as<velox::FlatMapVector>();
      VELOX_CHECK_NOT_NULL(
          flatMapVector,
          "Encoding mismatch on FlatMapVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      auto processFlatMapRow = [&](size_t baseIndex) {
        childRanges.add(baseIndex, baseIndex + 1);
      };
      nullCount = buildChildRanges(
          DecodedAdapter(decodedVector), ranges, processFlatMapRow);

      rawSize +=
          getRawSizeFromFlatMapVector(*flatMapVector, childRanges, context);
      break;
    }
    default: {
      VELOX_FAIL("Unexpected encoding for decoded vector base: {}.", encoding);
    }
  }

  context.nullCount = nullCount;
  if (nullCount) {
    rawSize += nullCount * kNullSize;
  }

  return rawSize;
}

// Passthrough flatmap handling: Nimble treats ROW vectors at certain node IDs
// as "passthrough flatmaps" - maps where keys are field names and values
// correspond to the map value type. This is indicated when:
// 1. The node's id is in flatMapNodeIds, AND
// 2. The vector at that level is a ROW vector
//
// For passthrough flatmaps, we need to compute:
// - Key sizes: Fixed-size based on declared key type in schema (VARCHAR,
// BIGINT, etc.)
// - Value sizes: Computed recursively for each field, using the map's value
// type
//   from schema
//
// The raw size is: sum of (key_size * field_count + value_sizes) for each row
// where the field is present, plus null handling.
uint64_t getRawSizeFromPassthroughFlatMap(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId& type,
    const bool topLevel) {
  VELOX_CHECK_NOT_NULL(vector);
  const auto& encoding = vector->encoding();
  const velox::RowVector* rowVector = nullptr;
  uint64_t nullCount = 0;
  velox::common::Ranges childRanges;

  switch (encoding) {
    case velox::VectorEncoding::Simple::CONSTANT: {
      const auto* constantVector =
          vector->as<velox::ConstantVector<velox::ComplexType>>();
      VELOX_CHECK_NOT_NULL(
          constantVector,
          "Encoding mismatch on ConstantVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      const auto& valueVector = constantVector->valueVector();
      const auto& index = constantVector->index();

      // Get raw size for the single constant value
      uint64_t singleRowSize = getRawSizeFromPassthroughFlatMap(
          valueVector,
          velox::common::Ranges::of(index, index + 1),
          context,
          type,
          topLevel);

      context.nullCount = 0;
      if (topLevel) {
        // Scale up all column sizes by the number of rows
        for (size_t i = 0; i < context.columnCount(); ++i) {
          context.setSizeAt(i, context.sizeAt(i) * ranges.size());
          context.setNullsAt(i, context.nullsAt(i) * ranges.size());
        }
      }
      return singleRowSize * ranges.size();
    }
    default: {
      // Decode the vector to handle any encoding (ROW, DICTIONARY, etc.)
      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*vector);

      rowVector = decodedVector.base()->as<velox::RowVector>();
      VELOX_CHECK_NOT_NULL(
          rowVector,
          "Encoding mismatch on RowVector. Encoding: {}. TypeKind: {}.",
          decodedVector.base()->encoding(),
          decodedVector.base()->typeKind());

      // For passthrough flatmap, we NEVER ignore top-level nulls
      // because the ROW represents a map entry, and null means "empty map"
      auto processRow = [&](velox::vector_size_t baseIndex) {
        childRanges.add(baseIndex, baseIndex + 1);
      };
      nullCount =
          buildChildRanges(DecodedAdapter(decodedVector), ranges, processRow);
      break;
    }
  }

  // For passthrough flatmap: type is MAP, and we treat ROW children as
  // map entries.
  // Schema: MAP<keyType, valueType>
  // - type.childAt(0) is the key type (determines key size)
  // - type.childAt(1) is the value type (used for computing value sizes)
  const auto& schemaKeyType = type.childAt(0)->type();
  const auto& schemaValueType = type.childAt(1);
  auto keyTypeSize = getTypeSizeFromKind(schemaKeyType->kind());

  uint64_t rawSize = 0;
  const auto nonNullCount = childRanges.size();

  if (nonNullCount) {
    // Add key sizes: each ROW field name is written as a key for each non-null
    // row
    const auto childrenSize = rowVector->childrenSize();
    if (keyTypeSize.has_value()) {
      rawSize += *keyTypeSize * childrenSize * nonNullCount;
    }

    // Add value sizes: all children use the same value type from the MAP schema
    for (size_t i = 0; i < childrenSize; ++i) {
      const auto& child = rowVector->childAt(i);

      // Compute value sizes using schema's value type
      uint64_t childRawSize = getRawSizeFromVector(
          child, childRanges, context, schemaValueType.get(), {});
      rawSize += childRawSize;

      if (topLevel) {
        context.appendSize(childRawSize);
        context.appendNullCount(context.nullCount);
      }
    }
  } else if (topLevel) {
    // No non-null rows, but we still need to record sizes for each child
    const auto childrenSize = rowVector->childrenSize();
    for (size_t i = 0; i < childrenSize; ++i) {
      context.appendSize(0);
      context.appendNullCount(0);
    }
  }

  context.nullCount = nullCount;
  if (nullCount) {
    rawSize += nullCount * kNullSize;
  }

  return rawSize;
}

// ignoreNulls: when true, only nulls at this level are ignored
uint64_t getRawSizeFromRegularRowVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId* type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds,
    const bool topLevel,
    bool ignoreNulls) {
  VELOX_CHECK_NOT_NULL(vector);
  const auto& encoding = vector->encoding();

  const velox::RowVector* rowVector;
  uint64_t nullCount = 0;
  velox::common::Ranges childRanges;

  switch (encoding) {
    case velox::VectorEncoding::Simple::CONSTANT: {
      const auto* constantVector =
          vector->as<velox::ConstantVector<velox::ComplexType>>();
      VELOX_CHECK_NOT_NULL(
          constantVector,
          "Encoding mismatch on ConstantVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      const auto& valueVector = constantVector->valueVector();
      const auto& index = constantVector->index();

      // Get raw size for the single constant value
      uint64_t singleRowSize = getRawSizeFromRegularRowVector(
          valueVector,
          velox::common::Ranges::of(index, index + 1),
          context,
          type,
          flatMapNodeIds,
          topLevel,
          ignoreNulls);

      context.nullCount = 0;
      if (topLevel) {
        // Scale up all column sizes by the number of rows
        for (size_t i = 0; i < context.columnCount(); ++i) {
          context.setSizeAt(i, context.sizeAt(i) * ranges.size());
          context.setNullsAt(i, context.nullsAt(i) * ranges.size());
        }
      }
      return singleRowSize * ranges.size();
    }
    default: {
      // Decode the vector to handle any encoding (ROW, DICTIONARY, etc.)
      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*vector);

      rowVector = decodedVector.base()->as<velox::RowVector>();
      VELOX_CHECK_NOT_NULL(
          rowVector,
          "Encoding mismatch on RowVector. Encoding: {}. TypeKind: {}.",
          decodedVector.base()->encoding(),
          decodedVector.base()->typeKind());

      auto processRow = [&](velox::vector_size_t baseIndex) {
        childRanges.add(baseIndex, baseIndex + 1);
      };

      if (ignoreNulls) {
        // When ignoring nulls, use DecodedAdapter<true> to skip null checking
        nullCount = buildChildRanges(
            DecodedAdapter<true>(decodedVector), ranges, processRow);
      } else {
        nullCount = buildChildRanges(
            DecodedAdapter<>(decodedVector), ranges, processRow);
      }
      break;
    }
  }

  uint64_t rawSize = 0;
  const auto nonNullCount = childRanges.size();

  // Determine children count - use schema if available, otherwise use vector
  const size_t childrenSize = type ? type->size() : rowVector->childrenSize();

  if (nonNullCount) {
    for (size_t i = 0; i < childrenSize; ++i) {
      uint64_t childRawSize;
      if (type) {
        // Use schema's child type for computing sizes
        childRawSize = getRawSizeFromVector(
            rowVector->childAt(i),
            childRanges,
            context,
            type->childAt(i).get(),
            flatMapNodeIds);
      } else {
        // No schema, use vector's actual type
        childRawSize = getRawSizeFromVector(
            rowVector->childAt(i),
            childRanges,
            context,
            nullptr,
            flatMapNodeIds);
      }
      rawSize += childRawSize;
      if (topLevel) {
        context.appendSize(childRawSize);
        context.appendNullCount(context.nullCount);
      }
    }
  } else if (topLevel) {
    for (size_t i = 0; i < childrenSize; ++i) {
      context.appendSize(0);
      context.appendNullCount(0);
    }
  }

  context.nullCount = nullCount;
  if (nullCount) {
    rawSize += nullCount * kNullSize;
  }

  return rawSize;
}

// Dispatches to either passthrough flatmap or regular row handling.
// ignoreTopLevelNulls: when true and topLevel is true, nulls at the top level
// are ignored
uint64_t getRawSizeFromRowVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId* type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds,
    const bool topLevel,
    bool ignoreTopLevelNulls) {
  VELOX_CHECK_NOT_NULL(vector);

  // Check if this is a passthrough flatmap (only when we have schema info):
  // 1. The type's node ID is in flatMapNodeIds (configured as flatmap)
  // 2. The vector at that level is a ROW vector
  const bool isPassthroughFlatMap = type &&
      flatMapNodeIds.contains(type->id()) &&
      vector->typeKind() == velox::TypeKind::ROW;

  if (isPassthroughFlatMap) {
    return getRawSizeFromPassthroughFlatMap(
        vector, ranges, context, *type, topLevel);
  } else {
    // When ignoreTopLevelNulls is true and this is the top level, treat all
    // rows as non-null
    const bool ignoreNulls = ignoreTopLevelNulls && topLevel;
    return getRawSizeFromRegularRowVector(
        vector, ranges, context, type, flatMapNodeIds, topLevel, ignoreNulls);
  }
}

// When type is nullptr, uses vector's actual type for calculations.
// When type is non-null, uses schema type for size calculations (handles type
// mismatches like int32_t vector with BIGINT schema).
uint64_t getRawSizeFromVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId* type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds,
    bool ignoreTopLevelNulls) {
  VELOX_CHECK_NOT_NULL(vector);

  auto vectorTypeKind = vector->typeKind();

  // If we have schema info and there's a type mismatch, handle it.
  // Type mismatch is only allowed for leaf (scalar) types.
  // Complex type mismatches (e.g., ROW vs MAP for passthrough flatmaps) are
  // handled in the type-specific functions.

  switch (vectorTypeKind) {
    case velox::TypeKind::BOOLEAN: {
      return getRawSizeFromFixedWidthVector<velox::TypeKind::BOOLEAN>(
          vector, ranges, context);
    }
    case velox::TypeKind::TINYINT: {
      return getRawSizeFromFixedWidthVector<velox::TypeKind::TINYINT>(
          vector, ranges, context);
    }
    case velox::TypeKind::SMALLINT: {
      return getRawSizeFromFixedWidthVector<velox::TypeKind::SMALLINT>(
          vector, ranges, context);
    }
    case velox::TypeKind::INTEGER: {
      return getRawSizeFromFixedWidthVector<velox::TypeKind::INTEGER>(
          vector, ranges, context);
    }
    case velox::TypeKind::BIGINT: {
      return getRawSizeFromFixedWidthVector<velox::TypeKind::BIGINT>(
          vector, ranges, context);
    }
    case velox::TypeKind::REAL: {
      return getRawSizeFromFixedWidthVector<velox::TypeKind::REAL>(
          vector, ranges, context);
    }
    case velox::TypeKind::DOUBLE: {
      return getRawSizeFromFixedWidthVector<velox::TypeKind::DOUBLE>(
          vector, ranges, context);
    }
    case velox::TypeKind::TIMESTAMP: {
      // TIMESTAMP uses a logical size of 12 bytes (8 + 4) instead of
      // sizeof(Timestamp) = 16
      return getRawSizeFromFixedWidthVector<velox::TypeKind::TIMESTAMP>(
          vector, ranges, context, kTimestampLogicalSize);
    }
    case velox::TypeKind::VARCHAR:
    case velox::TypeKind::VARBINARY: {
      return getRawSizeFromStringVector(vector, ranges, context);
    }
    case velox::TypeKind::ARRAY: {
      return getRawSizeFromArrayVector(
          vector, ranges, context, type, flatMapNodeIds);
    }
    case velox::TypeKind::MAP: {
      return getRawSizeFromMap(vector, ranges, context, type, flatMapNodeIds);
    }
    case velox::TypeKind::ROW: {
      // Determine if this is a top-level ROW:
      // - If no schema, treat node 0 (first call) as top level
      // - If schema, use type->id() == 0 or passthrough flatmap detection
      const bool isTopLevel = type
          ? (type->id() == 0 || flatMapNodeIds.contains(type->id()))
          : true; // Without schema, first ROW is top level
      return getRawSizeFromRowVector(
          vector,
          ranges,
          context,
          type,
          flatMapNodeIds,
          isTopLevel,
          isTopLevel ? ignoreTopLevelNulls : false);
    }
    default: {
      VELOX_FAIL("Unsupported type: {}.", vectorTypeKind);
    }
  }
}

// Convenience overload with context but without schema
uint64_t getRawSizeFromVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context) {
  return getRawSizeFromVector(vector, ranges, context, nullptr, {});
}

// Convenience overload for RowVector without schema
uint64_t getRawSizeFromRowVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    bool topLevel) {
  return getRawSizeFromRowVector(
      vector, ranges, context, nullptr, {}, topLevel, false);
}

// Convenience overload without context for simple use cases
uint64_t getRawSizeFromVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges) {
  RawSizeContext context;
  return getRawSizeFromVector(vector, ranges, context, nullptr, {});
}

} // namespace facebook::nimble
