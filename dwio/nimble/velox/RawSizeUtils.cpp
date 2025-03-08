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
#include "velox/vector/FlatVector.h"

namespace facebook::nimble {

template <typename T>
uint64_t getRawSizeFromFixedWidthVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges) {
  VELOX_DCHECK(
      (std::disjunction_v<
          std::is_same<bool, T>,
          std::is_same<int8_t, T>,
          std::is_same<int16_t, T>,
          std::is_same<int32_t, T>,
          std::is_same<int64_t, T>,
          std::is_same<float, T>,
          std::is_same<double, T>>),
      "Wrong vector type. Expected bool | int8_t | int16_t | int32_t | int64_t | float | double.");

  const auto& encoding = vector->encoding();
  switch (encoding) {
    case velox::VectorEncoding::Simple::FLAT: {
      const auto* flatVector = vector->asFlatVector<T>();
      VELOX_CHECK_NOT_NULL(
          flatVector,
          "Failed vector cast to FlatVector. Encoding: {}. TypeKind: {}.",
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

      return ((ranges.size() - nullCount) * sizeof(T)) +
          (nullCount * NULL_SIZE);
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      const auto* constVector = vector->as<velox::ConstantVector<T>>();
      VELOX_CHECK_NOT_NULL(
          constVector,
          "Failed vector cast to ConstantVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      return constVector->mayHaveNulls() ? ranges.size() * NULL_SIZE
                                         : ranges.size() * sizeof(T);
    }
    case velox::VectorEncoding::Simple::DICTIONARY: {
      const auto* dictVector = vector->as<velox::DictionaryVector<T>>();
      VELOX_CHECK_NOT_NULL(
          dictVector,
          "Failed vector cast to DictionaryVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      // TODO: Optimize use of DecodedVector.
      velox::SelectivityVector selectVector(dictVector->size());
      velox::DecodedVector decodedVector(*dictVector, selectVector);

      uint64_t nullCount = 0;
      if (decodedVector.mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (decodedVector.isNullAt(row)) {
            ++nullCount;
          }
        }
      }

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
    const velox::common::Ranges& ranges) {
  const auto& encoding = vector->encoding();
  switch (encoding) {
    case velox::VectorEncoding::Simple::FLAT: {
      const auto* flatVector = vector->asFlatVector<velox::StringView>();
      VELOX_CHECK_NOT_NULL(
          flatVector,
          "Failed vector cast to FlatVector. Encoding: {}. TypeKind: {}.",
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

      return rawSize + (nullCount * NULL_SIZE);
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      const auto* constVector =
          vector->as<velox::ConstantVector<velox::StringView>>();
      VELOX_CHECK_NOT_NULL(
          constVector,
          "Failed vector cast to ConstantVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      return constVector->mayHaveNulls()
          ? ranges.size() * NULL_SIZE
          : ranges.size() * constVector->value().size();
    }
    case velox::VectorEncoding::Simple::DICTIONARY: {
      const auto* dictVector =
          vector->as<velox::DictionaryVector<velox::StringView>>();
      VELOX_CHECK_NOT_NULL(
          dictVector,
          "Failed vector cast to DictionaryVector. Encoding: {}. TypeKind: {}.",
          encoding,
          vector->typeKind());

      // TODO: Optimize use of DecodedVector.
      velox::SelectivityVector selectVector(dictVector->size());
      velox::DecodedVector decodedVector(*dictVector, selectVector);

      uint64_t rawSize = 0;
      uint64_t nullCount = 0;
      if (decodedVector.mayHaveNulls()) {
        for (const auto& row : ranges) {
          if (decodedVector.isNullAt(row)) {
            ++nullCount;
          } else {
            rawSize += decodedVector.valueAt<velox::StringView>(row).size();
          }
        }
      } else {
        for (const auto& row : ranges) {
          rawSize += decodedVector.valueAt<velox::StringView>(row).size();
        }
      }

      return rawSize + (nullCount * NULL_SIZE);
    }
    default: {
      VELOX_FAIL("Unsupported encoding: {}", encoding);
    }
  }
}

uint64_t getRawSizeFromConstantComplexVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges) {
  VELOX_CHECK_NOT_NULL(vector, "Vector is null.");
  VELOX_DCHECK(
      velox::VectorEncoding::Simple::CONSTANT == vector->encoding(),
      "Wrong vector encoding. Expected VectorEncoding::Simple::CONSTANT.");

  const auto* constantVector =
      vector->as<velox::ConstantVector<velox::ComplexType>>();
  VELOX_CHECK_NOT_NULL(
      vector,
      "Failed vector cast to ConstantVector. Encoding: {}. TypeKind: {}.",
      vector->encoding(),
      vector->typeKind());

  const auto& valueVector = constantVector->valueVector();
  const auto& index = constantVector->index();
  velox::common::Ranges childRanges;
  childRanges.add(index, index + 1);

  uint64_t rawSize = getRawSizeFromVector(valueVector, childRanges);

  return rawSize * ranges.size();
}

uint64_t getRawSizeFromArrayVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges) {
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
    if (end > begin) {
      VELOX_CHECK_LE(end, arrayVector->elements()->size());
      childRanges.add(begin, end);
    }
  };

  switch (encoding) {
    case velox::VectorEncoding::Simple::ARRAY: {
      arrayVector = vector->as<velox::ArrayVector>();
      VELOX_CHECK_NOT_NULL(
          arrayVector,
          "Failed vector cast to ArrayVector. Encoding: {}. TypeKind: {}.",
          encoding,
          arrayVector->typeKind());
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
      return getRawSizeFromConstantComplexVector(vector, ranges);
    }
    case velox::VectorEncoding::Simple::DICTIONARY: {
      const auto* dictionaryArrayVector =
          vector->as<velox::DictionaryVector<velox::ComplexType>>();
      VELOX_CHECK_NOT_NULL(
          dictionaryArrayVector,
          "Failed vector cast to DictionaryVector. Encoding: {}. TypeKind: {}.",
          encoding,
          dictionaryArrayVector->typeKind());

      // TODO: Optimize use of DecodedVector.
      velox::SelectivityVector selectVector(dictionaryArrayVector->size());
      velox::DecodedVector decodedVector(*dictionaryArrayVector, selectVector);

      // Decoded ComplexVectors are stored in baseVector.
      arrayVector = decodedVector.base()->as<velox::ArrayVector>();
      VELOX_CHECK_NOT_NULL(
          arrayVector,
          "Failed vector cast to ArrayVector. Encoding: {}. TypeKind: {}.",
          arrayVector->encoding(),
          arrayVector->typeKind());

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
    rawSize += getRawSizeFromVector(arrayVector->elements(), childRanges);
  }

  if (nullCount) {
    rawSize += nullCount * NULL_SIZE;
  }

  return rawSize;
}

uint64_t getRawSizeFromMapVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges) {
  const auto& encoding = vector->encoding();
  const velox::MapVector* mapVector;
  const velox::vector_size_t* offsets;
  const velox::vector_size_t* sizes;
  velox::common::Ranges childRanges;
  uint64_t nullCount = 0;
  auto processRow = [&](size_t row) {
    auto begin = offsets[row];
    auto end = begin + sizes[row];
    // Ensure valid size
    if (end > begin) {
      VELOX_CHECK_LE(end, mapVector->mapKeys()->size());
      VELOX_CHECK_LE(end, mapVector->mapValues()->size());
      childRanges.add(begin, end);
    }
  };

  switch (encoding) {
    case velox::VectorEncoding::Simple::MAP: {
      mapVector = vector->as<velox::MapVector>();
      VELOX_CHECK_NOT_NULL(
          mapVector,
          "Failed vector cast to MapVector. Encoding: {}. TypeKind: {}.",
          encoding,
          mapVector->typeKind());

      offsets = mapVector->rawOffsets();
      sizes = mapVector->rawSizes();

      if (mapVector->mayHaveNulls()) {
        const uint64_t* nulls = mapVector->rawNulls();
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
      return getRawSizeFromConstantComplexVector(vector, ranges);
    }
    case velox::VectorEncoding::Simple::DICTIONARY: {
      const auto* dictionaryMapVector =
          vector->as<velox::DictionaryVector<velox::ComplexType>>();
      VELOX_CHECK_NOT_NULL(
          dictionaryMapVector,
          "Failed vector cast to DictionaryVector. Encoding: {}. TypeKind: {}.",
          encoding,
          dictionaryMapVector->typeKind());

      // TODO: Optimize use of DecodedVector.
      velox::SelectivityVector selectVector(dictionaryMapVector->size());
      velox::DecodedVector decodedVector(*dictionaryMapVector, selectVector);

      mapVector = decodedVector.base()->as<velox::MapVector>();
      VELOX_CHECK_NOT_NULL(
          mapVector,
          "Failed vector cast to FlatVector. MapVector: {}. TypeKind: {}.",
          mapVector->encoding(),
          mapVector->typeKind());

      offsets = mapVector->rawOffsets();
      sizes = mapVector->rawSizes();

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

  uint64_t rawSize = 0;
  if (childRanges.size()) {
    rawSize += getRawSizeFromVector(mapVector->mapKeys(), childRanges);
    rawSize += getRawSizeFromVector(mapVector->mapValues(), childRanges);
  }

  if (nullCount) {
    rawSize += nullCount * NULL_SIZE;
  }

  return rawSize;
}

uint64_t getRawSizeFromRowVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges) {
  const auto& encoding = vector->encoding();
  const velox::RowVector* rowVector;
  uint64_t nullCount = 0;
  velox::common::Ranges childRanges;

  switch (encoding) {
    case velox::VectorEncoding::Simple::ROW: {
      rowVector = vector->as<velox::RowVector>();
      VELOX_CHECK_NOT_NULL(
          rowVector,
          "Failed vector cast to RowVector. Encoding: {}. TypeKind: {}.",
          encoding,
          rowVector->typeKind());

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
        // Potentially expensive?
        childRanges = ranges;
      }

      break;
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      return getRawSizeFromConstantComplexVector(vector, ranges);
    }
    case velox::VectorEncoding::Simple::DICTIONARY: {
      const auto* dictionaryRowVector =
          vector->as<velox::DictionaryVector<velox::ComplexType>>();
      VELOX_CHECK_NOT_NULL(
          dictionaryRowVector,
          "Failed vector cast to DictionaryVector. Encoding: {}. TypeKind: {}.",
          encoding,
          dictionaryRowVector->typeKind());

      // TODO: Optimize use of DecodedVector.
      velox::SelectivityVector selectVector(dictionaryRowVector->size());
      velox::DecodedVector decodedVector(*dictionaryRowVector, selectVector);

      rowVector = decodedVector.base()->as<velox::RowVector>();
      VELOX_CHECK_NOT_NULL(
          rowVector,
          "Failed vector cast to RowVector. Encoding: {}. TypeKind: {}.",
          rowVector->encoding(),
          rowVector->typeKind());

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
  if (childRanges.size()) {
    const auto childrenSize = rowVector->childrenSize();
    for (size_t i = 0; i < childrenSize; ++i) {
      rawSize += getRawSizeFromVector(rowVector->childAt(i), childRanges);
    }
  }

  if (nullCount) {
    rawSize += nullCount * NULL_SIZE;
  }

  return rawSize;
}

// Returns uint64_t bytes of raw data in the vector.
uint64_t getRawSizeFromVector(
    const velox::VectorPtr& vector,
    const velox::common::Ranges& ranges) {
  VELOX_CHECK_NOT_NULL(vector, "Vector is null.");
  const auto& typeKind = vector->typeKind();
  switch (typeKind) {
    case velox::TypeKind::BOOLEAN: {
      return getRawSizeFromFixedWidthVector<bool>(vector, ranges);
    }
    case velox::TypeKind::TINYINT: {
      return getRawSizeFromFixedWidthVector<int8_t>(vector, ranges);
    }
    case velox::TypeKind::SMALLINT: {
      return getRawSizeFromFixedWidthVector<int16_t>(vector, ranges);
    }
    case velox::TypeKind::INTEGER: {
      return getRawSizeFromFixedWidthVector<int32_t>(vector, ranges);
    }
    case velox::TypeKind::BIGINT: {
      return getRawSizeFromFixedWidthVector<int64_t>(vector, ranges);
    }
    case velox::TypeKind::REAL: {
      return getRawSizeFromFixedWidthVector<float>(vector, ranges);
    }
    case velox::TypeKind::DOUBLE: {
      return getRawSizeFromFixedWidthVector<double>(vector, ranges);
    }
    case velox::TypeKind::VARCHAR:
    case velox::TypeKind::VARBINARY: {
      return getRawSizeFromStringVector(vector, ranges);
    }
    case velox::TypeKind::ARRAY: {
      return getRawSizeFromArrayVector(vector, ranges);
    }
    case velox::TypeKind::MAP: {
      return getRawSizeFromMapVector(vector, ranges);
    }
    case velox::TypeKind::ROW: {
      return getRawSizeFromRowVector(vector, ranges);
    }
    default: {
      VELOX_FAIL("Unsupported type: {}.", typeKind);
    }
  }
}

} // namespace facebook::nimble
