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

namespace {

// Returns the size in bytes for a given TypeKind.
// Used for calculating key sizes in passthrough flatmaps.
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
          (nullCount * NULL_SIZE);
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      const auto* constVector = vector->as<velox::ConstantVector<T>>();
      VELOX_CHECK_NOT_NULL(
          constVector,
          "Encoding mismatch on ConstantVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      context.nullCount = constVector->mayHaveNulls() ? ranges.size() : 0;
      return constVector->mayHaveNulls() ? ranges.size() * NULL_SIZE
                                         : ranges.size() * sizeof(T);
    }
    case velox::VectorEncoding::Simple::DICTIONARY: {
      const auto* dictVector = vector->as<velox::DictionaryVector<T>>();
      VELOX_CHECK_NOT_NULL(
          dictVector,
          "Encoding mismatch on DictionaryVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*dictVector);

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
          (nullCount * NULL_SIZE);
    }
    default: {
      VELOX_FAIL("Unsupported encoding: {}.", encoding);
    }
  }
}

// Specialized function for TIMESTAMP vectors that uses kTimestampLogicalSize
// instead of sizeof(velox::Timestamp) to match DWRF and Nimble FieldWriter.
uint64_t getRawSizeFromTimestampVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context) {
  VELOX_CHECK_NOT_NULL(vector);
  using T = velox::Timestamp;

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
          (nullCount * NULL_SIZE);
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
          ? ranges.size() * NULL_SIZE
          : ranges.size() * kTimestampLogicalSize;
    }
    case velox::VectorEncoding::Simple::DICTIONARY: {
      const auto* dictVector = vector->as<velox::DictionaryVector<T>>();
      VELOX_CHECK_NOT_NULL(
          dictVector,
          "Encoding mismatch on DictionaryVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*dictVector);

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
          (nullCount * NULL_SIZE);
    }
    default: {
      VELOX_FAIL("Unsupported encoding: {}.", encoding);
    }
  }
}

uint64_t getRawSizeFromStringVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context) {
  VELOX_CHECK_NOT_NULL(vector);
  const auto& encoding = vector->encoding();
  switch (encoding) {
    case velox::VectorEncoding::Simple::FLAT: {
      const auto* flatVector = vector->asFlatVector<velox::StringView>();
      VELOX_CHECK_NOT_NULL(
          flatVector,
          "Encoding mismatch on FlatVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      uint64_t rawSize = 0;
      uint64_t nullCount = 0;
      if (flatVector->mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (flatVector->isNullAt(row)) {
            ++nullCount;
          } else {
            rawSize += flatVector->valueAt(row).size();
          }
        }
      } else {
        for (const auto& row : ranges) {
          rawSize += flatVector->valueAt(row).size();
        }
      }

      context.nullCount = nullCount;
      return rawSize + (nullCount * NULL_SIZE);
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
          ? ranges.size() * NULL_SIZE
          : ranges.size() * constVector->value().size();
    }
    case velox::VectorEncoding::Simple::DICTIONARY: {
      const auto* dictVector =
          vector->as<velox::DictionaryVector<velox::StringView>>();
      VELOX_CHECK_NOT_NULL(
          dictVector,
          "Encoding mismatch on DictionaryVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*dictVector);

      uint64_t rawSize = 0;
      uint64_t nullCount = 0;
      const auto* indices = decodedVector.indices();
      const auto decodedVectorData = decodedVector.data<velox::StringView>();
      if (decodedVector.mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (decodedVector.isNullAt(row)) {
            ++nullCount;
          } else {
            rawSize += decodedVectorData[indices[row]].size();
          }
        }
      } else {
        for (const auto& row : ranges) {
          rawSize += decodedVectorData[indices[row]].size();
        }
      }

      context.nullCount = nullCount;
      return rawSize + (nullCount * NULL_SIZE);
    }
    default: {
      VELOX_FAIL("Unsupported encoding: {}", encoding);
    }
  }
}

uint64_t getRawSizeFromConstantComplexVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    bool topLevelRow = false) {
  VELOX_CHECK_NOT_NULL(vector);
  VELOX_DCHECK(
      velox::VectorEncoding::Simple::CONSTANT == vector->encoding(),
      "Wrong vector encoding. Expected VectorEncoding::Simple::CONSTANT.");

  const auto* constantVector =
      vector->as<velox::ConstantVector<velox::ComplexType>>();
  VELOX_CHECK_NOT_NULL(
      constantVector,
      "Encoding mismatch on ConstantVector. Encoding: {}. TypeKind: {}.",
      vector->encoding(),
      vector->typeKind());

  const auto& valueVector = constantVector->valueVector();
  const auto& index = constantVector->index();
  velox::common::Ranges childRanges;
  childRanges.add(index, index + 1);

  uint64_t rawSize = 0;
  if (topLevelRow) {
    VELOX_CHECK_EQ(
        velox::TypeKind::ROW,
        valueVector->typeKind(),
        "Value vector should be a RowVector");
    rawSize = getRawSizeFromRowVector(
        valueVector, childRanges, context, /*topLevel=*/true);
    for (int idx = 0; idx < context.columnCount(); ++idx) {
      context.setSizeAt(idx, context.sizeAt(idx) * ranges.size());
      context.setNullsAt(idx, context.nullsAt(idx) * ranges.size());
    }
  } else {
    rawSize = getRawSizeFromVector(valueVector, childRanges, context);
  }
  context.nullCount = constantVector->mayHaveNulls() ? ranges.size() : 0;
  return rawSize * ranges.size();
}

uint64_t getRawSizeFromArrayVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context) {
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
      return getRawSizeFromConstantComplexVector(vector, ranges, context);
    }
    case velox::VectorEncoding::Simple::DICTIONARY: {
      const auto* dictionaryArrayVector =
          vector->as<velox::DictionaryVector<velox::ComplexType>>();
      VELOX_CHECK_NOT_NULL(
          dictionaryArrayVector,
          "Encoding mismatch on DictionaryVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*dictionaryArrayVector);

      // Decoded ComplexVectors are stored in baseVector.
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
    default: {
      VELOX_FAIL("Unsupported encoding: {}.", encoding);
    }
  }

  // ARRAY and DICTIONARY encodings should only reach here
  uint64_t rawSize = 0;
  if (childRanges.size()) {
    rawSize +=
        getRawSizeFromVector(arrayVector->elements(), childRanges, context);
  }

  context.nullCount = nullCount;
  if (nullCount) {
    rawSize += nullCount * NULL_SIZE;
  }

  return rawSize;
}

namespace {

uint64_t getRawSizeFromMapVector(
    const velox::MapVector& mapVector,
    const velox::common::Ranges& childRanges,
    RawSizeContext& context) {
  uint64_t rawSize = 0;
  rawSize += getRawSizeFromVector(mapVector.mapKeys(), childRanges, context);
  rawSize += getRawSizeFromVector(mapVector.mapValues(), childRanges, context);
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
    RawSizeContext& context) {
  VELOX_CHECK_NOT_NULL(vector);
  const auto& encoding = vector->encoding();
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

  switch (encoding) {
    // Handle top-level (regular) Map vectors.
    case velox::VectorEncoding::Simple::MAP: {
      mapVector = vector->as<velox::MapVector>();
      VELOX_CHECK_NOT_NULL(
          mapVector,
          "Encoding mismatch on MapVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      offsets = mapVector->rawOffsets();
      sizes = mapVector->rawSizes();

      if (mapVector->mayHaveNulls()) {
        const uint64_t* nulls = mapVector->rawNulls();
        for (const auto& row : ranges) {
          if (velox::bits::isBitNull(nulls, row)) {
            ++nullCount;
          } else {
            processMapRow(row);
          }
        }
      } else {
        for (const auto& row : ranges) {
          processMapRow(row);
        }
      }
      rawSize += getRawSizeFromMapVector(*mapVector, childRanges, context);
      break;
    }

    // Handle top-level Flat Map vectors.
    case velox::VectorEncoding::Simple::FLAT_MAP: {
      auto flatMapVector = vector->as<velox::FlatMapVector>();
      VELOX_CHECK_NOT_NULL(
          flatMapVector,
          "Encoding mismatch on FlatMapVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      if (flatMapVector->mayHaveNulls()) {
        const uint64_t* nulls = flatMapVector->rawNulls();
        for (const auto& row : ranges) {
          if (velox::bits::isBitNull(nulls, row)) {
            ++nullCount;
          } else {
            childRanges.add(row, row + 1);
          }
        }
        rawSize +=
            getRawSizeFromFlatMapVector(*flatMapVector, childRanges, context);
      } else {
        rawSize += getRawSizeFromFlatMapVector(*flatMapVector, ranges, context);
      }
      break;
    }

    // Cases when maps or flat maps are wrapped by a constant.
    case velox::VectorEncoding::Simple::CONSTANT: {
      return getRawSizeFromConstantComplexVector(vector, ranges, context);
    }

    // Cases when maps or flat maps are wrapped by a dictionary.
    case velox::VectorEncoding::Simple::DICTIONARY: {
      const auto* dictionaryMapVector =
          vector->as<velox::DictionaryVector<velox::ComplexType>>();
      VELOX_CHECK_NOT_NULL(
          dictionaryMapVector,
          "Encoding mismatch on DictionaryVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*dictionaryMapVector);

      // Now switch on the inner type of the dictionary; must be either a map
      // or a flat map.
      switch (decodedVector.base()->encoding()) {
        // Dictionary wrapped around a map:
        case velox::VectorEncoding::Simple::MAP: {
          mapVector = decodedVector.base()->as<velox::MapVector>();
          VELOX_CHECK_NOT_NULL(
              mapVector,
              "Encoding mismatch on FlatVector. MapVector: {}. TypeKind: {}.",
              decodedVector.base()->encoding(),
              decodedVector.base()->typeKind());

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
          rawSize += getRawSizeFromMapVector(*mapVector, childRanges, context);
          break;
        }
        // Dictionary wrapped around a flat map:
        case velox::VectorEncoding::Simple::FLAT_MAP: {
          auto flatMapVector = decodedVector.base()->as<velox::FlatMapVector>();
          VELOX_CHECK_NOT_NULL(
              flatMapVector,
              "Encoding mismatch on FlatMapVector. Encoding: {}. TypeKind: {}.",
              decodedVector.base()->encoding(),
              decodedVector.base()->typeKind());

          if (decodedVector.mayHaveNulls()) {
            for (const auto& row : ranges) {
              if (decodedVector.isNullAt(row)) {
                ++nullCount;
              } else {
                auto idx = decodedVector.index(row);
                childRanges.add(idx, idx + 1);
              }
            }
          } else {
            for (const auto& row : ranges) {
              auto idx = decodedVector.index(row);
              childRanges.add(idx, idx + 1);
            }
          }
          rawSize +=
              getRawSizeFromFlatMapVector(*flatMapVector, childRanges, context);
          break;
        }
        default:
          VELOX_FAIL(
              "Unsupported map encoding wrapped by DICTIONARY: {}.", encoding);
      }
      break;
    }
    default:
      VELOX_FAIL("Unsupported map encoding: {}.", encoding);
  }

  context.nullCount = nullCount;
  if (nullCount) {
    rawSize += nullCount * NULL_SIZE;
  }
  return rawSize;
}

uint64_t getRawSizeFromRowVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const bool topLevel) {
  VELOX_CHECK_NOT_NULL(vector);
  const auto& encoding = vector->encoding();
  const velox::RowVector* rowVector;
  uint64_t nullCount = 0;
  velox::common::Ranges childRanges;
  const velox::common::Ranges* childRangesPtr;

  switch (encoding) {
    case velox::VectorEncoding::Simple::ROW: {
      rowVector = vector->as<velox::RowVector>();
      VELOX_CHECK_NOT_NULL(
          rowVector,
          "Encoding mismatch on RowVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      childRangesPtr = &childRanges;
      if (rowVector->mayHaveNulls()) {
        const auto& nulls = rowVector->rawNulls();
        for (const auto& row : ranges) {
          if (velox::bits::isBitNull(nulls, row)) {
            ++nullCount;
          } else {
            childRanges.add(row, row + 1);
          }
        }
      } else {
        childRangesPtr = &ranges;
      }
      break;
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      return getRawSizeFromConstantComplexVector(
          vector, ranges, context, topLevel);
    }
    case velox::VectorEncoding::Simple::DICTIONARY: {
      const auto* dictionaryRowVector =
          vector->as<velox::DictionaryVector<velox::ComplexType>>();
      VELOX_CHECK_NOT_NULL(
          dictionaryRowVector,
          "Encoding mismatch on DictionaryVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*dictionaryRowVector);

      rowVector = decodedVector.base()->as<velox::RowVector>();
      VELOX_CHECK_NOT_NULL(
          rowVector,
          "Encoding mismatch on RowVector. Encoding: {}. TypeKind: {}.",
          decodedVector.base()->encoding(),
          decodedVector.base()->typeKind());

      childRangesPtr = &childRanges;
      if (decodedVector.mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (decodedVector.isNullAt(row)) {
            ++nullCount;
          } else {
            childRanges.add(
                decodedVector.index(row), decodedVector.index(row) + 1);
          }
        }
      } else {
        for (auto& row : ranges) {
          childRanges.add(
              decodedVector.index(row), decodedVector.index(row) + 1);
        }
      }

      break;
    }
    default: {
      VELOX_FAIL("Unsupported encoding: {}.", encoding);
    }
  }

  uint64_t rawSize = 0;
  const auto nonNullCount = (*childRangesPtr).size();
  if (nonNullCount) {
    const auto childrenSize = rowVector->childrenSize();

    for (size_t i = 0; i < childrenSize; ++i) {
      auto childRawSize =
          getRawSizeFromVector(rowVector->childAt(i), *childRangesPtr, context);
      rawSize += childRawSize;
      if (topLevel) {
        context.appendSize(childRawSize);
        context.appendNullCount(context.nullCount);
      }
    }
  } else if (topLevel) {
    for (size_t i = 0; i < rowVector->childrenSize(); ++i) {
      context.appendSize(0);
      context.appendNullCount(0);
    }
  }

  context.nullCount = nullCount;
  if (nullCount) {
    rawSize += nullCount * NULL_SIZE;
  }

  return rawSize;
}

// Returns uint64_t bytes of raw data in the vector.
uint64_t getRawSizeFromVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context) {
  VELOX_CHECK_NOT_NULL(vector);
  const auto& typeKind = vector->typeKind();
  switch (typeKind) {
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
      return getRawSizeFromArrayVector(vector, ranges, context);
    }
    case velox::TypeKind::MAP: {
      return getRawSizeFromMap(vector, ranges, context);
    }
    case velox::TypeKind::ROW: {
      return getRawSizeFromRowVector(vector, ranges, context);
    }
    default: {
      VELOX_FAIL("Unsupported type: {}.", typeKind);
    }
  }
}

uint64_t getRawSizeFromVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges) {
  RawSizeContext context;
  return getRawSizeFromVector(vector, ranges, context);
}

// Computes raw size for a passthrough flatmap.
// A passthrough flatmap is a ROW vector that represents a MAP in the schema,
// where each ROW field name becomes a map key and each ROW child becomes the
// corresponding map value.
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
  const velox::common::Ranges* childRangesPtr = nullptr;

  // For passthrough flatmaps, the schema type is MAP.
  // Get the key type from type.childAt(0) which is the MAP's key type.
  const auto& keyType = type.childAt(0);
  std::optional<size_t> keyTypeSize =
      getTypeSizeFromKind(keyType->type()->kind());

  // For VARCHAR keys, compute the total string key size from ROW field names
  size_t stringKeySize = 0;
  if (!keyTypeSize.has_value() &&
      keyType->type()->kind() == velox::TypeKind::VARCHAR) {
    const velox::RowVector* passthroughRow = nullptr;
    if (encoding == velox::VectorEncoding::Simple::ROW) {
      passthroughRow = vector->as<velox::RowVector>();
    } else if (encoding == velox::VectorEncoding::Simple::DICTIONARY) {
      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*vector);
      passthroughRow = decodedVector.base()->as<velox::RowVector>();
    }
    if (passthroughRow) {
      const auto& rowType = passthroughRow->type()->asRow();
      for (size_t i = 0; i < rowType.size(); ++i) {
        stringKeySize += rowType.nameOf(i).size();
      }
    }
  }

  switch (encoding) {
    case velox::VectorEncoding::Simple::ROW: {
      rowVector = vector->as<velox::RowVector>();
      VELOX_CHECK_NOT_NULL(
          rowVector,
          "Encoding mismatch on RowVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      childRangesPtr = &childRanges;
      if (rowVector->mayHaveNulls()) {
        const auto& nulls = rowVector->rawNulls();
        for (const auto& row : ranges) {
          if (velox::bits::isBitNull(nulls, row)) {
            ++nullCount;
          } else {
            childRanges.add(row, row + 1);
          }
        }
      } else {
        childRangesPtr = &ranges;
      }
      break;
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      return getRawSizeFromConstantComplexVector(
          vector, ranges, context, topLevel);
    }
    case velox::VectorEncoding::Simple::DICTIONARY: {
      const auto* dictionaryRowVector =
          vector->as<velox::DictionaryVector<velox::ComplexType>>();
      VELOX_CHECK_NOT_NULL(
          dictionaryRowVector,
          "Encoding mismatch on DictionaryVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*dictionaryRowVector);

      rowVector = decodedVector.base()->as<velox::RowVector>();
      VELOX_CHECK_NOT_NULL(
          rowVector,
          "Encoding mismatch on RowVector. Encoding: {}. TypeKind: {}.",
          decodedVector.base()->encoding(),
          decodedVector.base()->typeKind());

      childRangesPtr = &childRanges;
      if (decodedVector.mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (decodedVector.isNullAt(row)) {
            ++nullCount;
          } else {
            childRanges.add(
                decodedVector.index(row), decodedVector.index(row) + 1);
          }
        }
      } else {
        for (auto& row : ranges) {
          childRanges.add(
              decodedVector.index(row), decodedVector.index(row) + 1);
        }
      }
      break;
    }
    default: {
      VELOX_FAIL("Unsupported encoding: {}.", encoding);
    }
  }

  uint64_t rawSize = 0;
  const auto nonNullCount = (*childRangesPtr).size();
  // Use vector's children count (each child is a key/value pair)
  const auto childrenSize = rowVector->childrenSize();

  if (nonNullCount) {
    // Add key sizes: each ROW field name is written as a key for each non-null
    // row
    if (keyTypeSize.has_value()) {
      rawSize += *keyTypeSize * childrenSize * nonNullCount;
    } else if (stringKeySize > 0) {
      // For VARCHAR keys, use the total string key size multiplied by non-null
      // count
      rawSize += stringKeySize * nonNullCount;
    }

    // Add value sizes: all children use the same value type from the MAP schema
    for (size_t i = 0; i < childrenSize; ++i) {
      // Value types are not flatmaps themselves, so use the non-flatmap version
      uint64_t childRawSize =
          getRawSizeFromVector(rowVector->childAt(i), *childRangesPtr, context);
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
    rawSize += nullCount * NULL_SIZE;
  }

  return rawSize;
}

// Computes raw size for a regular ROW vector (not a passthrough flatmap).
// ignoreNulls: when true, nulls at this level are ignored
uint64_t getRawSizeFromRegularRowVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId& type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds,
    const bool topLevel,
    bool ignoreNulls) {
  VELOX_CHECK_NOT_NULL(vector);
  const auto& encoding = vector->encoding();
  const velox::RowVector* rowVector = nullptr;
  uint64_t nullCount = 0;
  velox::common::Ranges childRanges;
  const velox::common::Ranges* childRangesPtr = nullptr;

  switch (encoding) {
    case velox::VectorEncoding::Simple::ROW: {
      rowVector = vector->as<velox::RowVector>();
      VELOX_CHECK_NOT_NULL(
          rowVector,
          "Encoding mismatch on RowVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      childRangesPtr = &childRanges;
      if (!ignoreNulls && rowVector->mayHaveNulls()) {
        const auto& nulls = rowVector->rawNulls();
        for (const auto& row : ranges) {
          if (velox::bits::isBitNull(nulls, row)) {
            ++nullCount;
          } else {
            childRanges.add(row, row + 1);
          }
        }
      } else {
        childRangesPtr = &ranges;
      }
      break;
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      return getRawSizeFromConstantComplexVector(
          vector, ranges, context, topLevel);
    }
    case velox::VectorEncoding::Simple::DICTIONARY: {
      const auto* dictionaryRowVector =
          vector->as<velox::DictionaryVector<velox::ComplexType>>();
      VELOX_CHECK_NOT_NULL(
          dictionaryRowVector,
          "Encoding mismatch on DictionaryVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      auto localDecodedVector = DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());
      velox::DecodedVector& decodedVector = localDecodedVector.get();
      decodedVector.decode(*dictionaryRowVector);

      rowVector = decodedVector.base()->as<velox::RowVector>();
      VELOX_CHECK_NOT_NULL(
          rowVector,
          "Encoding mismatch on RowVector. Encoding: {}. TypeKind: {}.",
          decodedVector.base()->encoding(),
          decodedVector.base()->typeKind());

      childRangesPtr = &childRanges;
      if (!ignoreNulls && decodedVector.mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (decodedVector.isNullAt(row)) {
            ++nullCount;
          } else {
            childRanges.add(
                decodedVector.index(row), decodedVector.index(row) + 1);
          }
        }
      } else {
        for (auto& row : ranges) {
          childRanges.add(
              decodedVector.index(row), decodedVector.index(row) + 1);
        }
      }
      break;
    }
    default: {
      VELOX_FAIL("Unsupported encoding: {}.", encoding);
    }
  }

  uint64_t rawSize = 0;
  const auto nonNullCount = (*childRangesPtr).size();
  // Use schema's children count (input may have extra columns not in schema)
  const auto childrenSize = type.size();

  if (nonNullCount) {
    // Each child has its own type in the schema
    for (size_t i = 0; i < childrenSize; ++i) {
      uint64_t childRawSize = getRawSizeFromVector(
          rowVector->childAt(i),
          *childRangesPtr,
          context,
          *type.childAt(i),
          flatMapNodeIds);
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
    rawSize += nullCount * NULL_SIZE;
  }

  return rawSize;
}

// TypeWithId version of getRawSizeFromRowVector.
// Dispatches to either passthrough flatmap or regular row handling.
// ignoreTopLevelNulls: when true and topLevel is true, nulls at the top level
// are ignored
uint64_t getRawSizeFromRowVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId& type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds,
    const bool topLevel,
    bool ignoreTopLevelNulls) {
  VELOX_CHECK_NOT_NULL(vector);

  // Check if this is a passthrough flatmap:
  // 1. The node's id is in flatMapNodeIds (configured as flatmap)
  // 2. The vector at that level is a ROW vector
  const bool isPassthroughFlatMap = flatMapNodeIds.contains(type.id()) &&
      vector->typeKind() == velox::TypeKind::ROW;

  if (isPassthroughFlatMap) {
    return getRawSizeFromPassthroughFlatMap(
        vector, ranges, context, type, topLevel);
  } else {
    // When ignoreTopLevelNulls is true and this is the top level, treat all
    // rows as non-null
    const bool ignoreNulls = ignoreTopLevelNulls && topLevel;
    return getRawSizeFromRegularRowVector(
        vector, ranges, context, type, flatMapNodeIds, topLevel, ignoreNulls);
  }
}

// TypeWithId version of getRawSizeFromVector that passes schema and
// flatMapNodeIds recursively.
uint64_t getRawSizeFromVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges,
    RawSizeContext& context,
    const velox::dwio::common::TypeWithId& type,
    const folly::F14FastSet<uint32_t>& flatMapNodeIds,
    bool ignoreTopLevelNulls) {
  VELOX_CHECK_NOT_NULL(vector);

  const auto& typeKind = vector->typeKind();
  switch (typeKind) {
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
      // For arrays, pass the element type to child
      // Note: Current implementation doesn't fully support TypeWithId for
      // arrays This can be enhanced later to pass type.childAt(0) to elements
      return getRawSizeFromArrayVector(vector, ranges, context);
    }
    case velox::TypeKind::MAP: {
      // For maps, pass key/value types to children
      // Note: Current implementation doesn't fully support TypeWithId for maps
      // This can be enhanced later
      return getRawSizeFromMap(vector, ranges, context);
    }
    case velox::TypeKind::ROW: {
      // type.id() == 0 means this is the root/top-level ROW
      // Handle passthrough flatmap: node id is in flatMapNodeIds AND vector is
      // ROW
      // Passthrough flatmaps are treated as top-level for stats purposes.
      const bool isTopLevel =
          (type.id() == 0 || flatMapNodeIds.contains(type.id()));
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
      VELOX_FAIL("Unsupported type: {}.", typeKind);
    }
  }
}

} // namespace facebook::nimble
