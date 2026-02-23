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

#include "dwio/nimble/velox/VectorAdapters.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatMapVector.h"

namespace facebook::nimble {

// Timestamp logical size in bytes.
// This matches DWRF's approach: 8 bytes for seconds (int64) + 4 bytes for nanos
// (int32). Note: velox::Type::cppSizeInBytes() returns 16 bytes (8+8) for
// Timestamp, but we use 12 bytes for raw size to match DWRF behavior and
// Nimble FieldWriter's kTimestampLogicalSize.
constexpr uint64_t kTimestampLogicalSize = 12;

// Returns the size in bytes for a fixed-width type.
// For TIMESTAMP, returns kTimestampLogicalSize (12 bytes) to match DWRF
// behavior rather than the actual struct size (16 bytes). Returns std::nullopt
// for variable-length types (string or complex types).
std::optional<size_t> getTypeSize(const velox::Type& type) {
  switch (type.kind()) {
    case velox::TypeKind::BOOLEAN:
    case velox::TypeKind::TINYINT:
    case velox::TypeKind::SMALLINT:
    case velox::TypeKind::INTEGER:
    case velox::TypeKind::BIGINT:
    case velox::TypeKind::REAL:
    case velox::TypeKind::DOUBLE:
      return type.cppSizeInBytes();
    case velox::TypeKind::TIMESTAMP:
      return kTimestampLogicalSize;
    default:
      return std::nullopt;
  }
}

namespace {

// Helper to count nulls over a range using an adapter.
template <typename Adapter>
uint64_t countNulls(
    const Adapter& adapter,
    const velox::common::Ranges& ranges) {
  uint64_t nullCount = 0;
  if (adapter.hasNulls()) {
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
  if (adapter.hasNulls()) {
    for (const auto& row : ranges) {
      if (adapter.isNullAt(row)) {
        ++nullCount;
      } else {
        totalStringSize += adapter.valueAt(row).size();
      }
    }
  } else {
    for (const auto& row : ranges) {
      totalStringSize += adapter.valueAt(row).size();
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
  if (adapter.hasNulls()) {
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
      const uint64_t nullCount = countNulls(FlatAdapter<T>(vector), ranges);
      return ((ranges.size() - nullCount) * elementSize) +
          (nullCount * kNullSize);
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      const auto* constVector = vector->asChecked<velox::ConstantVector<T>>();

      return constVector->mayHaveNulls() ? ranges.size() * kNullSize
                                         : ranges.size() * elementSize;
    }
    default: {
      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*vector);

      const uint64_t nullCount =
          countNulls(DecodedAdapter<T>(decodedVector), ranges);
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
      auto [totalStringSize, nullCount] = computeStringSizeAndNulls(
          FlatAdapter<velox::StringView>(vector), ranges);

      return totalStringSize + (nullCount * kNullSize);
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      const auto* constVector =
          vector->asChecked<velox::ConstantVector<velox::StringView>>();

      return constVector->mayHaveNulls()
          ? ranges.size() * kNullSize
          : ranges.size() * constVector->value().size();
    }
    default: {
      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*vector);

      auto [totalStringSize, nullCount] = computeStringSizeAndNulls(
          DecodedAdapter<velox::StringView>(decodedVector), ranges);

      return totalStringSize + (nullCount * kNullSize);
    }
  }
}

// Forward declaration for recursive calls
uint64_t getRawSizeFromVectorInternal(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId* type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds,
    bool ignoreTopLevelNulls = false);

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
          vector->asChecked<velox::ConstantVector<velox::ComplexType>>();

      const auto& valueVector = constantVector->valueVector();
      const auto& index = constantVector->index();

      // Get raw size for the single constant value
      const uint64_t singleRowSize = getRawSizeFromArrayVector(
          valueVector,
          velox::common::Ranges::of(index, index + 1),
          context,
          type,
          flatMapNodeIds);

      return singleRowSize * ranges.size();
    }
    case velox::VectorEncoding::Simple::ARRAY: {
      arrayVector = vector->asChecked<velox::ArrayVector>();
      offsets = arrayVector->rawOffsets();
      sizes = arrayVector->rawSizes();

      nullCount = buildChildRanges(FlatAdapter<>(vector), ranges, processRow);

      break;
    }
    default: {
      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*vector);

      arrayVector = decodedVector.base()->asChecked<velox::ArrayVector>();

      offsets = arrayVector->rawOffsets();
      sizes = arrayVector->rawSizes();

      nullCount =
          buildChildRanges(DecodedAdapter<>(decodedVector), ranges, processRow);

      break;
    }
  }

  uint64_t rawSize = 0;
  if (childRanges.size() > 0) {
    // Use schema's element type for computing sizes
    rawSize += getRawSizeFromVectorInternal(
        arrayVector->elements(),
        childRanges,
        context,
        type ? type->childAt(0).get() : nullptr,
        flatMapNodeIds);
  }

  if (nullCount > 0) {
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
  rawSize += getRawSizeFromVectorInternal(
      mapVector.mapKeys(),
      childRanges,
      context,
      type ? type->childAt(0).get() : nullptr,
      flatMapNodeIds);
  rawSize += getRawSizeFromVectorInternal(
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

  if (baseRanges.size() > 0) {
    velox::common::Ranges keyRanges;

    for (size_t i = 0; i < flatMapVector.numDistinctKeys(); ++i) {
      keyRanges.clear();

      const uint32_t keySize = getRawSizeFromVectorInternal(
          flatMapVector.distinctKeys(),
          velox::common::Ranges::of(i, i + 1),
          context,
          nullptr,
          {});

      // Process the keys and values for the rows where the key is present.
      if (auto& inMaps = flatMapVector.inMapsAt(i)) {
        const auto* rawInMaps = inMaps->as<uint64_t>();
        for (const auto& row : baseRanges) {
          if (velox::bits::isBitSet(rawInMaps, row)) {
            keyRanges.add(row, row + 1);
          }
        }

        rawSize += getRawSizeFromVectorInternal(
            flatMapVector.mapValuesAt(i), keyRanges, context, nullptr, {});
        rawSize += keySize * keyRanges.size();
      }
      // If there is no inMap buffer, process all rows.
      else {
        rawSize += getRawSizeFromVectorInternal(
            flatMapVector.mapValuesAt(i), baseRanges, context, nullptr, {});
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
        vector->asChecked<velox::ConstantVector<velox::ComplexType>>();

    const auto& valueVector = constantVector->valueVector();
    const auto& index = constantVector->index();
    velox::common::Ranges childRanges;
    childRanges.add(index, index + 1);

    // Get raw size for the single constant value
    const uint64_t singleRowSize = getRawSizeFromMap(
        valueVector, childRanges, context, type, flatMapNodeIds);

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
      mapVector = decoded->asChecked<velox::MapVector>();

      offsets = mapVector->rawOffsets();
      sizes = mapVector->rawSizes();

      nullCount = buildChildRanges(
          DecodedAdapter<>(decodedVector), ranges, processMapRow);
      rawSize += getRawSizeFromMapVector(
          *mapVector, childRanges, context, type, flatMapNodeIds);
      break;
    }

    // Handle flat map vectors.
    case velox::VectorEncoding::Simple::FLAT_MAP: {
      auto flatMapVector = decoded->asChecked<velox::FlatMapVector>();

      auto processFlatMapRow = [&](size_t baseIndex) {
        childRanges.add(baseIndex, baseIndex + 1);
      };
      nullCount = buildChildRanges(
          DecodedAdapter<>(decodedVector), ranges, processFlatMapRow);

      rawSize +=
          getRawSizeFromFlatMapVector(*flatMapVector, childRanges, context);
      break;
    }
    default: {
      VELOX_FAIL("Unexpected encoding for decoded vector base: {}.", encoding);
    }
  }

  if (nullCount > 0) {
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
//   type from schema
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
          vector->asChecked<velox::ConstantVector<velox::ComplexType>>();

      const auto& valueVector = constantVector->valueVector();
      const auto& index = constantVector->index();

      // Get raw size for the single constant value
      const uint64_t singleRowSize = getRawSizeFromPassthroughFlatMap(
          valueVector,
          velox::common::Ranges::of(index, index + 1),
          context,
          type,
          topLevel);

      if (topLevel) {
        // Scale up all column sizes by the number of rows
        for (size_t i = 0; i < context.columnCount(); ++i) {
          context.setSizeAt(i, context.sizeAt(i) * ranges.size());
        }
      }
      return singleRowSize * ranges.size();
    }
    default: {
      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*vector);

      rowVector = decodedVector.base()->asChecked<velox::RowVector>();

      // For passthrough flatmap, we NEVER ignore top-level nulls
      // because the ROW represents a map entry, and null means "empty map"
      auto processRow = [&](velox::vector_size_t baseIndex) {
        childRanges.add(baseIndex, baseIndex + 1);
      };
      nullCount =
          buildChildRanges(DecodedAdapter<>(decodedVector), ranges, processRow);
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
  auto keyTypeSize = getTypeSize(*schemaKeyType);

  uint64_t rawSize = 0;
  const auto nonNullCount = childRanges.size();

  if (nonNullCount > 0) {
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
      uint64_t childRawSize = getRawSizeFromVectorInternal(
          child, childRanges, context, schemaValueType.get(), {});
      rawSize += childRawSize;

      if (topLevel) {
        context.appendSize(childRawSize);
      }
    }
  } else if (topLevel) {
    // No non-null rows, but we still need to record sizes for each child
    const auto childrenSize = rowVector->childrenSize();
    for (size_t i = 0; i < childrenSize; ++i) {
      context.appendSize(0);
    }
  }

  if (nullCount > 0) {
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
          vector->asChecked<velox::ConstantVector<velox::ComplexType>>();

      const auto& valueVector = constantVector->valueVector();
      const auto& index = constantVector->index();

      // Get raw size for the single constant value
      const uint64_t singleRowSize = getRawSizeFromRegularRowVector(
          valueVector,
          velox::common::Ranges::of(index, index + 1),
          context,
          type,
          flatMapNodeIds,
          topLevel,
          ignoreNulls);

      if (topLevel) {
        // Scale up all column sizes by the number of rows
        for (size_t i = 0; i < context.columnCount(); ++i) {
          context.setSizeAt(i, context.sizeAt(i) * ranges.size());
        }
      }
      return singleRowSize * ranges.size();
    }
    default: {
      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*vector);
      rowVector = decodedVector.base()->asChecked<velox::RowVector>();

      auto processRow = [&](velox::vector_size_t baseIndex) {
        childRanges.add(baseIndex, baseIndex + 1);
      };

      nullCount = ignoreNulls
          ? buildChildRanges(
                DecodedAdapter<int8_t, true>(decodedVector), ranges, processRow)
          : buildChildRanges(
                DecodedAdapter<int8_t, false>(decodedVector),
                ranges,
                processRow);
      break;
    }
  }

  uint64_t rawSize = 0;
  const auto nonNullCount = childRanges.size();

  // Determine children count - use schema if available, otherwise use vector
  const size_t childrenSize = type ? type->size() : rowVector->childrenSize();

  if (nonNullCount > 0) {
    for (size_t i = 0; i < childrenSize; ++i) {
      uint64_t childRawSize;
      if (type) {
        // Use schema's child type for computing sizes
        childRawSize = getRawSizeFromVectorInternal(
            rowVector->childAt(i),
            childRanges,
            context,
            type->childAt(i).get(),
            flatMapNodeIds);
      } else {
        // No schema, use vector's actual type
        childRawSize = getRawSizeFromVectorInternal(
            rowVector->childAt(i),
            childRanges,
            context,
            nullptr,
            flatMapNodeIds);
      }
      rawSize += childRawSize;
      if (topLevel) {
        context.appendSize(childRawSize);
      }
    }
  } else if (topLevel) {
    for (size_t i = 0; i < childrenSize; ++i) {
      context.appendSize(0);
    }
  }

  if (nullCount > 0) {
    rawSize += nullCount * kNullSize;
  }

  return rawSize;
}

// Dispatches to either passthrough flatmap or regular row handling.
// ignoreTopLevelNulls: when true and topLevel is true, nulls at the top level
// are ignored
uint64_t getRawSizeFromRowVectorInternal(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId* type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds,
    const bool topLevel,
    bool ignoreTopLevelNulls) {
  VELOX_CHECK_NOT_NULL(vector);

  // TODO: maybe we can avoid this check and assume the flatmap passthrough.
  // Upstream call sites should ensure the schema compatibility.
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
uint64_t getRawSizeFromVectorInternal(
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
      // - If schema, use type->id() == 0
      const bool isTopLevel = type ? (type->id() == 0) : true;
      return getRawSizeFromRowVectorInternal(
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

// Public API functions

uint64_t getRawSizeFromVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId* type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds,
    bool ignoreTopLevelNulls) {
  return getRawSizeFromVectorInternal(
      vector, ranges, context, type, flatMapNodeIds, ignoreTopLevelNulls);
}

// Convenience overload with context but without schema
uint64_t getRawSizeFromVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context) {
  return getRawSizeFromVectorInternal(vector, ranges, context, nullptr, {});
}

// Convenience overload for RowVector without schema
uint64_t getRawSizeFromRowVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    bool topLevel) {
  return getRawSizeFromRowVectorInternal(
      vector, ranges, context, nullptr, {}, topLevel, false);
}

uint64_t getRawSizeFromRowVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId* type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds,
    bool topLevel,
    bool ignoreTopLevelNulls) {
  return getRawSizeFromRowVectorInternal(
      vector,
      ranges,
      context,
      type,
      flatMapNodeIds,
      topLevel,
      ignoreTopLevelNulls);
}

// Convenience overload without context for simple use cases
uint64_t getRawSizeFromVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges) {
  RawSizeContext context;
  return getRawSizeFromVectorInternal(vector, ranges, context, nullptr, {});
}

} // namespace facebook::nimble
