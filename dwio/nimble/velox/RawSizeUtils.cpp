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
// Returns std::nullopt for variable-length types (VARCHAR, VARBINARY, etc.)
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
      // Variable-length types (VARCHAR, VARBINARY) or complex types
      // don't have a fixed size
      return std::nullopt;
  }
}

namespace {

// Checks if upcasting from vectorType to schemaType is valid.
// Only allows same-family promotions: integer->integer, float->float
// where the schema type is larger or equal.
bool isValidUpcast(velox::TypeKind vectorType, velox::TypeKind schemaType) {
  auto vectorSize = getTypeSizeFromKind(vectorType);
  auto schemaSize = getTypeSizeFromKind(schemaType);

  if (!vectorSize.has_value() || !schemaSize.has_value()) {
    return false;
  }

  // Integer type family
  const std::unordered_set<velox::TypeKind> integerTypes = {
      velox::TypeKind::BOOLEAN,
      velox::TypeKind::TINYINT,
      velox::TypeKind::SMALLINT,
      velox::TypeKind::INTEGER,
      velox::TypeKind::BIGINT,
  };

  // Floating point type family
  const std::unordered_set<velox::TypeKind> floatTypes = {
      velox::TypeKind::REAL,
      velox::TypeKind::DOUBLE,
  };

  // Integer to integer upcast
  if (integerTypes.contains(vectorType) && integerTypes.contains(schemaType)) {
    return *schemaSize >= *vectorSize;
  }

  // Float to float upcast
  if (floatTypes.contains(vectorType) && floatTypes.contains(schemaType)) {
    return *schemaSize >= *vectorSize;
  }

  return false;
}

// Computes raw size for scalar types when the vector type differs from the
// schema type. Counts nulls from the vector's actual data but uses the schema
// type's size for computing the final raw size. Only supports scalar types
// (fixed-width numeric types).
uint64_t getUpcastedRawSizeFromVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    size_t schemaTypeSize) {
  VELOX_CHECK_NOT_NULL(vector);

  const auto& encoding = vector->encoding();
  uint64_t nullCount = 0;

  switch (encoding) {
    case velox::VectorEncoding::Simple::FLAT: {
      if (vector->mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (vector->isNullAt(row)) {
            ++nullCount;
          }
        }
      }
      break;
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      nullCount = vector->mayHaveNulls() ? ranges.size() : 0;
      break;
    }
    default: {
      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*vector);
      if (decodedVector.mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (decodedVector.isNullAt(row)) {
            ++nullCount;
          }
        }
      }
    }
  }

  context.nullCount = nullCount;
  uint64_t nonNullCount = ranges.size() - nullCount;
  return (nonNullCount * schemaTypeSize) + (nullCount * kNullSize);
}

} // namespace

template <velox::TypeKind K>
uint64_t getRawSizeFromFixedWidthVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context) {
  VELOX_CHECK_NOT_NULL(vector);
  VELOX_DCHECK(
      K == velox::TypeKind::BOOLEAN || K == velox::TypeKind::TINYINT ||
          K == velox::TypeKind::SMALLINT || K == velox::TypeKind::INTEGER ||
          K == velox::TypeKind::BIGINT || K == velox::TypeKind::REAL ||
          K == velox::TypeKind::DOUBLE,
      "Wrong vector type. Expected BOOLEAN | TINYINT | SMALLINT | INTEGER | BIGINT | REAL | DOUBLE.");
  using T = typename velox::TypeTraits<K>::NativeType;

  const auto& encoding = vector->encoding();
  switch (encoding) {
    case velox::VectorEncoding::Simple::FLAT: {
      const auto* flatVector = vector->asFlatVector<T>();
      VELOX_CHECK_NOT_NULL(
          flatVector,
          "Encoding mismatch on FlatVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      uint64_t nullCount = 0;
      if (flatVector->mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (flatVector->isNullAt(row)) {
            ++nullCount;
          }
        }
      }

      context.nullCount = nullCount;
      return ((ranges.size() - nullCount) * sizeof(T)) +
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
                                         : ranges.size() * sizeof(T);
    }
    default: {
      // Decode the vector to handle any encoding (FLAT, DICTIONARY, etc.)
      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*vector);

      uint64_t nullCount = 0;
      if (decodedVector.mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (decodedVector.isNullAt(row)) {
            ++nullCount;
          }
        }
      }
      context.nullCount = nullCount;
      return ((ranges.size() - nullCount) * sizeof(T)) +
          (nullCount * kNullSize);
    }
  }
}

uint64_t getRawSizeFromTimestampVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context) {
  VELOX_CHECK_NOT_NULL(vector);
  VELOX_DCHECK(
      vector->typeKind() == velox::TypeKind::TIMESTAMP,
      "Wrong vector type. Expected TIMESTAMP.");
  using T = typename velox::TypeTraits<velox::TypeKind::TIMESTAMP>::NativeType;
  const auto& encoding = vector->encoding();
  switch (encoding) {
    case velox::VectorEncoding::Simple::FLAT: {
      const auto* flatVector = vector->asFlatVector<T>();
      VELOX_CHECK_NOT_NULL(
          flatVector,
          "Encoding mismatch on FlatVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      uint64_t nullCount = 0;
      if (flatVector->mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (flatVector->isNullAt(row)) {
            ++nullCount;
          }
        }
      }

      context.nullCount = nullCount;
      return ((ranges.size() - nullCount) * kTimestampLogicalSize) +
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
      return constVector->mayHaveNulls()
          ? ranges.size() * kNullSize
          : ranges.size() * kTimestampLogicalSize;
    }
    default: {
      // Decode the vector to handle any encoding (FLAT, DICTIONARY, etc.)
      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*vector);

      uint64_t nullCount = 0;
      if (decodedVector.mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (decodedVector.isNullAt(row)) {
            ++nullCount;
          }
        }
      }
      context.nullCount = nullCount;
      return ((ranges.size() - nullCount) * kTimestampLogicalSize) +
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

      uint64_t nullCount = 0;
      uint64_t totalStringSize = 0;
      if (flatVector->mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (flatVector->isNullAt(row)) {
            ++nullCount;
          } else {
            totalStringSize += flatVector->valueAt(row).size();
          }
        }
      } else {
        for (const auto& row : ranges) {
          totalStringSize += flatVector->valueAt(row).size();
        }
      }

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

      uint64_t nullCount = 0;
      uint64_t totalStringSize = 0;
      if (decodedVector.mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (decodedVector.isNullAt(row)) {
            ++nullCount;
          } else {
            totalStringSize +=
                decodedVector.valueAt<velox::StringView>(row).size();
          }
        }
      } else {
        for (const auto& row : ranges) {
          totalStringSize +=
              decodedVector.valueAt<velox::StringView>(row).size();
        }
      }

      context.nullCount = nullCount;
      return totalStringSize + (nullCount * kNullSize);
    }
  }
}

uint64_t getRawSizeFromConstantComplexVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId* type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds,
    const bool topLevel = false) {
  VELOX_CHECK_NOT_NULL(vector);
  const auto& encoding = vector->encoding();
  VELOX_DCHECK(
      encoding == velox::VectorEncoding::Simple::CONSTANT,
      "Expected encoding CONSTANT. Encoding: {}.",
      encoding);

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

  uint64_t rawSize = 0;
  if (topLevel && vector->typeKind() == velox::TypeKind::ROW) {
    VELOX_CHECK_EQ(
        velox::TypeKind::ROW,
        valueVector->typeKind(),
        "Value vector should be a RowVector");
    rawSize = getRawSizeFromRowVector(
        valueVector,
        childRanges,
        context,
        type,
        flatMapNodeIds,
        /*topLevel=*/true);
  } else {
    rawSize = getRawSizeFromVector(
        valueVector, childRanges, context, type, flatMapNodeIds);
  }

  context.nullCount = 0;
  if (topLevel) {
    // Scale up all column sizes by the number of rows
    for (size_t i = 0; i < context.columnCount(); ++i) {
      context.setSizeAt(i, context.sizeAt(i) * ranges.size());
      context.setNullsAt(i, context.nullsAt(i) * ranges.size());
    }
  }
  return rawSize * ranges.size();
}

// Unified getRawSizeFromArrayVector that works with optional TypeWithId*
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
    case velox::VectorEncoding::Simple::ARRAY: {
      arrayVector = vector->as<velox::ArrayVector>();
      VELOX_CHECK_NOT_NULL(
          arrayVector,
          "Encoding mismatch on ArrayVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());
      offsets = arrayVector->rawOffsets();
      sizes = arrayVector->rawSizes();

      if (arrayVector->mayHaveNulls()) {
        const uint64_t* nulls = arrayVector->rawNulls();
        for (const auto& row : ranges) {
          if (velox::bits::isBitNull(nulls, row)) {
            ++nullCount;
          } else {
            processRow(row);
          }
        }
      } else {
        for (const auto& row : ranges) {
          processRow(row);
        }
      }

      break;
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      return getRawSizeFromConstantComplexVector(
          vector, ranges, context, type, flatMapNodeIds);
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

      if (decodedVector.mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (decodedVector.isNullAt(row)) {
            ++nullCount;
          } else {
            processRow(decodedVector.index(row));
          }
        }
      } else {
        for (const auto& row : ranges) {
          processRow(decodedVector.index(row));
        }
      }

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

// Unified getRawSizeFromMapVector that works with optional TypeWithId*
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

// Unified getRawSizeFromMap that works with optional TypeWithId*
uint64_t getRawSizeFromMap(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId* type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds) {
  VELOX_CHECK_NOT_NULL(vector);
  auto encoding = vector->encoding();
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

  if (encoding == velox::VectorEncoding::Simple::CONSTANT) {
    return getRawSizeFromConstantComplexVector(
        vector, ranges, context, type, flatMapNodeIds);
  }

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

      if (decodedVector.mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (decodedVector.isNullAt(row)) {
            ++nullCount;
          } else {
            processMapRow(decodedVector.index(row));
          }
        }
      } else {
        for (const auto& row : ranges) {
          processMapRow(decodedVector.index(row));
        }
      }
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

      if (decodedVector.mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (decodedVector.isNullAt(row)) {
            ++nullCount;
          } else {
            auto baseIndex = decodedVector.index(row);
            childRanges.add(baseIndex, baseIndex + 1);
          }
        }
      } else {
        for (const auto& row : ranges) {
          auto baseIndex = decodedVector.index(row);
          childRanges.add(baseIndex, baseIndex + 1);
        }
      }

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

  if (encoding == velox::VectorEncoding::Simple::CONSTANT) {
    return getRawSizeFromConstantComplexVector(
        vector, ranges, context, &type, {}, topLevel);
  }

  // Decode the vector to handle any encoding (ROW, DICTIONARY, etc.)
  auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
      context.getDecodedVectorManager());
  velox::DecodedVector& decodedVector = localDecodedVector.get();
  decodedVector.decode(*vector);

  const velox::RowVector* rowVector =
      decodedVector.base()->as<velox::RowVector>();
  VELOX_CHECK_NOT_NULL(
      rowVector,
      "Encoding mismatch on RowVector. Encoding: {}. TypeKind: {}.",
      decodedVector.base()->encoding(),
      decodedVector.base()->typeKind());

  uint64_t nullCount = 0;
  velox::common::Ranges childRanges;

  // For passthrough flatmap, we NEVER ignore top-level nulls
  // because the ROW represents a map entry, and null means "empty map"
  if (decodedVector.mayHaveNulls()) {
    for (const auto& row : ranges) {
      if (decodedVector.isNullAt(row)) {
        ++nullCount;
      } else {
        auto baseIndex = decodedVector.index(row);
        childRanges.add(baseIndex, baseIndex + 1);
      }
    }
  } else {
    for (const auto& row : ranges) {
      auto baseIndex = decodedVector.index(row);
      childRanges.add(baseIndex, baseIndex + 1);
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
    // ROW children represent the "values" in the passthrough flatmap
    // Each field in the ROW is a key-value pair
    const auto childrenSize = rowVector->childrenSize();
    for (size_t i = 0; i < childrenSize; ++i) {
      const auto& child = rowVector->childAt(i);
      // Key size is fixed based on schema key type
      // We add key size for each present (non-null) value
      velox::common::Ranges presentRanges;
      if (child->mayHaveNulls()) {
        for (const auto& row : childRanges) {
          if (!child->isNullAt(row)) {
            presentRanges.add(row, row + 1);
          }
        }
      } else {
        presentRanges = childRanges;
      }

      // Add key size for present values
      if (keyTypeSize.has_value()) {
        rawSize += *keyTypeSize * presentRanges.size();
      } else {
        // For VARCHAR keys, use the field name length
        const auto& fieldName = rowVector->type()->asRow().nameOf(i);
        rawSize += fieldName.size() * presentRanges.size();
      }

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

// Unified getRawSizeFromRegularRowVector that works with optional TypeWithId*
// ignoreNulls: when true, nulls at this level are ignored
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

  if (encoding == velox::VectorEncoding::Simple::CONSTANT) {
    return getRawSizeFromConstantComplexVector(
        vector, ranges, context, type, flatMapNodeIds, topLevel);
  }

  // Decode the vector to handle any encoding (ROW, DICTIONARY, etc.)
  auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
      context.getDecodedVectorManager());
  velox::DecodedVector& decodedVector = localDecodedVector.get();
  decodedVector.decode(*vector);

  const velox::RowVector* rowVector =
      decodedVector.base()->as<velox::RowVector>();
  VELOX_CHECK_NOT_NULL(
      rowVector,
      "Encoding mismatch on RowVector. Encoding: {}. TypeKind: {}.",
      decodedVector.base()->encoding(),
      decodedVector.base()->typeKind());

  uint64_t nullCount = 0;
  velox::common::Ranges childRanges;

  if (!ignoreNulls && decodedVector.mayHaveNulls()) {
    for (const auto& row : ranges) {
      if (decodedVector.isNullAt(row)) {
        ++nullCount;
      } else {
        auto baseIndex = decodedVector.index(row);
        childRanges.add(baseIndex, baseIndex + 1);
      }
    }
  } else {
    for (const auto& row : ranges) {
      auto baseIndex = decodedVector.index(row);
      childRanges.add(baseIndex, baseIndex + 1);
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

// Unified getRawSizeFromRowVector that works with optional TypeWithId*
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

// Unified getRawSizeFromVector that works with optional TypeWithId*
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
  if (type) {
    const auto schemaTypeKind = type->type()->kind();
    if (vectorTypeKind != schemaTypeKind) {
      auto schemaTypeSize = getTypeSizeFromKind(schemaTypeKind);

      // Only validate and handle type mismatch for scalar (leaf) types.
      // Complex types (ROW, MAP, ARRAY) are allowed to mismatch for cases like
      // passthrough flatmaps and we let upstream decide whether to run the
      // compatibility check.
      if (schemaTypeSize.has_value()) {
        // Validate that the upcast is valid (schema type must be same or larger
        // in same type family)
        VELOX_CHECK(
            isValidUpcast(vectorTypeKind, schemaTypeKind),
            "Invalid type coercion from {} to {}. Only upcasting within the same type family is allowed.",
            vectorTypeKind,
            schemaTypeKind);

        // Use the dedicated upcasting function to count nulls from the vector's
        // actual data and compute size using the schema type.
        return getUpcastedRawSizeFromVector(
            vector, ranges, context, *schemaTypeSize);
      }
    }
  }

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
      return getRawSizeFromTimestampVector(vector, ranges, context);
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

// Convenience overload without context for simple use cases
uint64_t getRawSizeFromVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges) {
  RawSizeContext context;
  return getRawSizeFromVector(vector, ranges, context, nullptr, {});
}

} // namespace facebook::nimble
