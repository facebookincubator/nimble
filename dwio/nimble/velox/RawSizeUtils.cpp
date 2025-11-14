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
          K == velox::TypeKind::DOUBLE || K == velox::TypeKind::TIMESTAMP,
      "Wrong vector type. Expected BOOLEAN | TINYINT | SMALLINT | INTEGER | BIGINT | REAL | DOUBLE | TIMESTAMP.");
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
  if ((*childRangesPtr).size()) {
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
      return getRawSizeFromFixedWidthVector<velox::TypeKind::TIMESTAMP>(
          vector, ranges, context);
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

} // namespace facebook::nimble
