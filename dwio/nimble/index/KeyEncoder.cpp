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
#include "dwio/nimble/index/KeyEncoder.h"
#include "dwio/nimble/common/Exceptions.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/FlatVector.h"

namespace facebook::nimble {

namespace {

static constexpr int64_t DOUBLE_EXP_BIT_MASK = 0x7FF0000000000000L;
static constexpr int64_t DOUBLE_SIGNIF_BIT_MASK = 0x000FFFFFFFFFFFFFL;

static constexpr int64_t FLOAT_EXP_BIT_MASK = 0x7F800000;
static constexpr int64_t FLOAT_SIGNIF_BIT_MASK = 0x007FFFFF;

static constexpr size_t kNullByteSize = 1;

FOLLY_ALWAYS_INLINE int64_t doubleToLong(double value) {
  const int64_t* result = reinterpret_cast<const int64_t*>(&value);

  if (((*result & DOUBLE_EXP_BIT_MASK) == DOUBLE_EXP_BIT_MASK) &&
      (*result & DOUBLE_SIGNIF_BIT_MASK) != 0L) {
    return 0x7ff8000000000000L;
  }
  return *result;
}

FOLLY_ALWAYS_INLINE int32_t floatToInt(float value) {
  const int32_t* result = reinterpret_cast<const int32_t*>(&value);

  if (((*result & FLOAT_EXP_BIT_MASK) == FLOAT_EXP_BIT_MASK) &&
      (*result & FLOAT_SIGNIF_BIT_MASK) != 0L) {
    return 0x7fc00000;
  }
  return *result;
}

FOLLY_ALWAYS_INLINE void encodeByte(int8_t value, bool descending, char*& out) {
  if (descending) {
    *out = 0xff ^ value;
  } else {
    *out = value;
  }
  ++out;
}

FOLLY_ALWAYS_INLINE void
encodeLong(int64_t value, bool descending, char*& out) {
  // Flip sign bit for lexicographic ordering
  value ^= (1LL << 63);

  // Convert to big-endian
  value = folly::Endian::big(value);

  // Apply descending transformation if needed (branchless)
  // If descending is true, XOR with all 1s (0xff...ff)
  // If descending is false, XOR with 0
  const uint64_t mask = -static_cast<uint64_t>(descending);
  value ^= mask;

  // Write all 8 bytes at once
  std::memcpy(out, &value, sizeof(value));
  out += sizeof(value);
}

FOLLY_ALWAYS_INLINE void
encodeShort(int16_t value, bool descending, char*& out) {
  // Flip sign bit for lexicographic ordering
  value ^= (1 << 15);

  // Convert to big-endian
  value = folly::Endian::big(value);

  // Apply descending transformation if needed (branchless)
  const uint16_t mask = -static_cast<uint16_t>(descending);
  value ^= mask;

  // Write all 2 bytes at once
  std::memcpy(out, &value, sizeof(value));
  out += sizeof(value);
}

FOLLY_ALWAYS_INLINE void
encodeInteger(int32_t value, bool descending, char*& out) {
  // Flip sign bit for lexicographic ordering
  value ^= (1 << 31);

  // Convert to big-endian
  value = folly::Endian::big(value);

  // Apply descending transformation if needed (branchless)
  // If descending is true, XOR with all 1s (0xffffffff)
  // If descending is false, XOR with 0
  const uint32_t mask = -static_cast<uint32_t>(descending);
  value ^= mask;

  // Write all 4 bytes at once
  std::memcpy(out, &value, sizeof(value));
  out += sizeof(value);
}

FOLLY_ALWAYS_INLINE void
encodeString(const char* data, size_t size, bool descending, char*& out) {
  size_t offset = 0;
  constexpr size_t kBatchSize = xsimd::batch<uint8_t>::size;

  // Precompute XOR mask for descending transformation
  const uint8_t xorMask = descending ? 0xFF : 0x00;

  // Process in SIMD batches to find bytes needing escaping
  if (size >= kBatchSize) {
    const auto zero = xsimd::batch<uint8_t>::broadcast(0);
    const auto one = xsimd::batch<uint8_t>::broadcast(1);
    const auto xorBatch = xsimd::batch<uint8_t>::broadcast(xorMask);

    while (offset + kBatchSize <= size) {
      auto batch = xsimd::batch<uint8_t>::load_unaligned(
          reinterpret_cast<const uint8_t*>(data + offset));

      // Check which bytes need escaping (are 0 or 1)
      const auto needsEscape = (batch == zero) | (batch == one);
      const auto escapeMask = velox::simd::toBitMask(needsEscape);

      if (escapeMask == 0) {
        // Fast path: no bytes need escaping, bulk copy the entire batch
        // Apply descending transformation branchlessly (no-op if xorBatch is
        // all zeros)
        batch = batch ^ xorBatch;
        batch.store_unaligned(reinterpret_cast<uint8_t*>(out));
        out += kBatchSize;
        offset += kBatchSize;
      } else {
        // Some bytes need escaping, process byte-by-byte for this batch
        for (size_t i = 0; i < kBatchSize; ++i) {
          const uint8_t byte = data[offset + i];
          if (byte == 0 || byte == 1) {
            encodeByte(1, descending, out);
            encodeByte(byte, descending, out);
          } else {
            encodeByte(byte, descending, out);
          }
        }
        offset += kBatchSize;
      }
    }
  }

  // Process remaining bytes
  for (; offset < size; ++offset) {
    const uint8_t byte = data[offset];
    if (byte == 0 || byte == 1) {
      encodeByte(1, descending, out);
      encodeByte(byte, descending, out);
    } else {
      encodeByte(byte, descending, out);
    }
  }

  // Write terminator
  encodeByte(0, descending, out);
}

FOLLY_ALWAYS_INLINE size_t stringEncodedSize(const char* data, size_t size) {
  size_t countZeros{0};
  size_t countOnes{0};
  size_t offset{0};

  // Process bytes in SIMD batches
  constexpr size_t kBatchSize = xsimd::batch<uint8_t>::size;
  if (size >= kBatchSize) {
    const auto zero = xsimd::batch<uint8_t>::broadcast(0);
    const auto one = xsimd::batch<uint8_t>::broadcast(1);

    for (; offset + kBatchSize <= size; offset += kBatchSize) {
      auto batch = xsimd::batch<uint8_t>::load_unaligned(
          reinterpret_cast<const uint8_t*>(data + offset));

      // Count bytes that equal 0
      const auto isZero = (batch == zero);
      const auto zeroMask = velox::simd::toBitMask(isZero);
      countZeros += __builtin_popcount(zeroMask);

      // Count bytes that equal 1
      const auto isOne = (batch == one);
      const auto oneMask = velox::simd::toBitMask(isOne);
      countOnes += __builtin_popcount(oneMask);
    }
  }

  // Process remaining bytes using standard algorithms
  const auto* remaining = reinterpret_cast<const uint8_t*>(data + offset);
  const auto remainingSize = size - offset;

  countZeros += std::count(remaining, remaining + remainingSize, 0);
  countOnes += std::count(remaining, remaining + remainingSize, 1);

  // Each 0 or 1 byte needs 2 bytes (escape byte + actual byte)
  // Other bytes need 1 byte
  // Plus 1 terminator byte
  return size + countZeros + countOnes + 1;
}

FOLLY_ALWAYS_INLINE void encodeBool(bool value, char*& out) {
  encodeByte(value, /*descending=*/false, out);
}

// Forward declarations for functions used in estimateEncodedColumnSize
void addDateEncodedSize(
    const folly::Range<const velox::vector_size_t*>& nonNullRows,
    velox::vector_size_t** nonNullSizePtrs);

template <velox::TypeKind KIND>
void addColumnEncodedSizeTyped(
    const velox::DecodedVector& decodedSource,
    const folly::Range<const velox::vector_size_t*>& nonNullRows,
    velox::vector_size_t** nonNullSizePtrs);

void estimateEncodedColumnSize(
    const velox::DecodedVector& decodedSource,
    const folly::Range<const velox::vector_size_t*>& rows,
    std::vector<velox::vector_size_t>& sizes,
    velox::Scratch& scratch) {
  const auto numRows = rows.size();
  folly::Range<const velox::vector_size_t*> nonNullRows = rows;
  velox::vector_size_t** nonNullSizePtrs = nullptr;

  velox::ScratchPtr<uint64_t, 64> nullsHolder(scratch);
  velox::ScratchPtr<velox::vector_size_t, 64> nonNullsHolder(scratch);
  velox::ScratchPtr<velox::vector_size_t*, 64> nonNullSizePtrsHolder(scratch);

  if (decodedSource.mayHaveNulls()) {
    auto* nulls = nullsHolder.get(velox::bits::nwords(numRows));
    for (auto row = 0; row < numRows; ++row) {
      velox::bits::clearNull(nulls, row);
      if (decodedSource.isNullAt(rows[row])) {
        velox::bits::setNull(nulls, row);
        sizes[row] += kNullByteSize;
      }
    }

    auto* nonNulls = nonNullsHolder.get(numRows);
    const auto numNonNull =
        velox::simd::indicesOfSetBits(nulls, 0, numRows, nonNulls);
    if (numNonNull == 0) {
      return;
    }

    auto* const nonNullSizes = sizes.data();
    nonNullSizePtrs = nonNullSizePtrsHolder.get(numNonNull);
    for (int32_t i = 0; i < numNonNull; ++i) {
      nonNullSizePtrs[i] = &nonNullSizes[nonNulls[i]];
    }

    velox::simd::transpose(
        rows.data(),
        folly::Range<const velox::vector_size_t*>(nonNulls, numNonNull),
        nonNulls);
    nonNullRows =
        folly::Range<const velox::vector_size_t*>(nonNulls, numNonNull);
  } else {
    // No nulls, all rows are non-null - create size pointers for all rows
    nonNullSizePtrs = nonNullSizePtrsHolder.get(numRows);
    for (int32_t i = 0; i < numRows; ++i) {
      nonNullSizePtrs[i] = &sizes[i];
    }
  }

  // Process data for non-null rows only (or all rows if no nulls)
  if (decodedSource.base()->type()->isDate()) {
    addDateEncodedSize(nonNullRows, nonNullSizePtrs);
  } else {
    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        addColumnEncodedSizeTyped,
        decodedSource.base()->typeKind(),
        decodedSource,
        nonNullRows,
        nonNullSizePtrs);
  }
}

void addDateEncodedSize(
    const folly::Range<const velox::vector_size_t*>& nonNullRows,
    velox::vector_size_t** nonNullSizePtrs) {
  for (auto i = 0; i < nonNullRows.size(); ++i) {
    *nonNullSizePtrs[i] += (kNullByteSize + sizeof(int32_t));
  }
}

template <velox::TypeKind KIND>
void addColumnEncodedSizeTyped(
    const velox::DecodedVector& decodedSource,
    const folly::Range<const velox::vector_size_t*>& nonNullRows,
    velox::vector_size_t** nonNullSizePtrs) {
  // Handle variable-width types
  if constexpr (
      KIND == velox::TypeKind::VARCHAR || KIND == velox::TypeKind::VARBINARY) {
    for (auto i = 0; i < nonNullRows.size(); ++i) {
      const auto view =
          decodedSource.valueAt<velox::StringView>(nonNullRows[i]);
      *nonNullSizePtrs[i] +=
          kNullByteSize + stringEncodedSize(view.data(), view.size());
    }
  } else if constexpr (KIND == velox::TypeKind::TIMESTAMP) {
    // Timestamp uses int64_t for serialization
    for (auto i = 0; i < nonNullRows.size(); ++i) {
      *nonNullSizePtrs[i] += (kNullByteSize + sizeof(int64_t));
    }
  } else {
    // Fixed-width scalar types
    const auto elementSize = decodedSource.base()->type()->cppSizeInBytes();
    for (auto i = 0; i < nonNullRows.size(); ++i) {
      *nonNullSizePtrs[i] += (kNullByteSize + elementSize);
    }
  }
}

std::vector<velox::vector_size_t> getKeyChannels(
    const velox::RowTypePtr& inputType,
    const std::vector<std::string>& keyColumns) {
  std::vector<velox::vector_size_t> keyChannels;
  keyChannels.reserve(keyColumns.size());
  for (const auto& keyColumn : keyColumns) {
    keyChannels.emplace_back(inputType->getChildIdx(keyColumn));
  }
  return keyChannels;
}

void encodeBigInt(
    const velox::DecodedVector& decodedVector,
    velox::vector_size_t numRows,
    bool descending,
    std::vector<char*>& rowOffsets) {
  const bool mayHaveNulls = decodedVector.mayHaveNulls();
  if (mayHaveNulls) {
    for (auto row = 0; row < numRows; ++row) {
      if (!decodedVector.isNullAt(row)) {
        const auto value = decodedVector.valueAt<int64_t>(row);
        encodeLong(value, descending, rowOffsets[row]);
      }
    }
  } else {
    for (auto row = 0; row < numRows; ++row) {
      const auto value = decodedVector.valueAt<int64_t>(row);
      encodeLong(value, descending, rowOffsets[row]);
    }
  }
}

void encodeBoolean(
    const velox::DecodedVector& decodedVector,
    velox::vector_size_t numRows,
    bool descending,
    std::vector<char*>& rowOffsets) {
  const bool mayHaveNulls = decodedVector.mayHaveNulls();
  if (mayHaveNulls) {
    for (auto row = 0; row < numRows; ++row) {
      if (!decodedVector.isNullAt(row)) {
        const auto value = decodedVector.valueAt<bool>(row);
        encodeByte(
            static_cast<int8_t>(value ? 2 : 1), descending, rowOffsets[row]);
      }
    }
  } else {
    for (auto row = 0; row < numRows; ++row) {
      const auto value = decodedVector.valueAt<bool>(row);
      encodeByte(
          static_cast<int8_t>(value ? 2 : 1), descending, rowOffsets[row]);
    }
  }
}

void encodeDouble(
    const velox::DecodedVector& decodedVector,
    velox::vector_size_t numRows,
    bool descending,
    std::vector<char*>& rowOffsets) {
  const bool mayHaveNulls = decodedVector.mayHaveNulls();
  if (mayHaveNulls) {
    for (auto row = 0; row < numRows; ++row) {
      if (!decodedVector.isNullAt(row)) {
        const auto value = decodedVector.valueAt<double>(row);
        int64_t longValue = doubleToLong(value);
        if ((longValue & (1L << 63)) != 0) {
          longValue = ~longValue;
        } else {
          longValue = longValue ^ (1L << 63);
        }
        encodeLong(longValue, descending, rowOffsets[row]);
      }
    }
  } else {
    for (auto row = 0; row < numRows; ++row) {
      const auto value = decodedVector.valueAt<double>(row);
      int64_t longValue = doubleToLong(value);
      if ((longValue & (1L << 63)) != 0) {
        longValue = ~longValue;
      } else {
        longValue = longValue ^ (1L << 63);
      }
      encodeLong(longValue, descending, rowOffsets[row]);
    }
  }
}

void encodeReal(
    const velox::DecodedVector& decodedVector,
    velox::vector_size_t numRows,
    bool descending,
    std::vector<char*>& rowOffsets) {
  const bool mayHaveNulls = decodedVector.mayHaveNulls();
  if (mayHaveNulls) {
    for (auto row = 0; row < numRows; ++row) {
      if (!decodedVector.isNullAt(row)) {
        const auto value = decodedVector.valueAt<float>(row);
        int32_t intValue = floatToInt(value);
        if ((intValue & (1L << 31)) != 0) {
          intValue = ~intValue;
        } else {
          intValue = intValue ^ (1L << 31);
        }
        encodeInteger(intValue, descending, rowOffsets[row]);
      }
    }
  } else {
    for (auto row = 0; row < numRows; ++row) {
      const auto value = decodedVector.valueAt<float>(row);
      int32_t intValue = floatToInt(value);
      if ((intValue & (1L << 31)) != 0) {
        intValue = ~intValue;
      } else {
        intValue = intValue ^ (1L << 31);
      }
      encodeInteger(intValue, descending, rowOffsets[row]);
    }
  }
}

void encodeTinyInt(
    const velox::DecodedVector& decodedVector,
    velox::vector_size_t numRows,
    bool descending,
    std::vector<char*>& rowOffsets) {
  const bool mayHaveNulls = decodedVector.mayHaveNulls();
  if (mayHaveNulls) {
    for (auto row = 0; row < numRows; ++row) {
      if (!decodedVector.isNullAt(row)) {
        const auto value = decodedVector.valueAt<int8_t>(row);
        encodeByte(
            static_cast<int8_t>(value ^ 0x80), descending, rowOffsets[row]);
      }
    }
  } else {
    for (auto row = 0; row < numRows; ++row) {
      const auto value = decodedVector.valueAt<int8_t>(row);
      encodeByte(
          static_cast<int8_t>(value ^ 0x80), descending, rowOffsets[row]);
    }
  }
}

void encodeSmallInt(
    const velox::DecodedVector& decodedVector,
    velox::vector_size_t numRows,
    bool descending,
    std::vector<char*>& rowOffsets) {
  const bool mayHaveNulls = decodedVector.mayHaveNulls();
  if (mayHaveNulls) {
    for (auto row = 0; row < numRows; ++row) {
      if (!decodedVector.isNullAt(row)) {
        const auto value = decodedVector.valueAt<int16_t>(row);
        encodeShort(value, descending, rowOffsets[row]);
      }
    }
  } else {
    for (auto row = 0; row < numRows; ++row) {
      const auto value = decodedVector.valueAt<int16_t>(row);
      encodeShort(value, descending, rowOffsets[row]);
    }
  }
}

void encodeIntegerType(
    const velox::DecodedVector& decodedVector,
    velox::vector_size_t numRows,
    bool descending,
    std::vector<char*>& rowOffsets) {
  const bool mayHaveNulls = decodedVector.mayHaveNulls();
  if (mayHaveNulls) {
    for (auto row = 0; row < numRows; ++row) {
      if (!decodedVector.isNullAt(row)) {
        const auto value = decodedVector.valueAt<int32_t>(row);
        encodeInteger(value, descending, rowOffsets[row]);
      }
    }
  } else {
    for (auto row = 0; row < numRows; ++row) {
      const auto value = decodedVector.valueAt<int32_t>(row);
      encodeInteger(value, descending, rowOffsets[row]);
    }
  }
}

void encodeStringType(
    const velox::DecodedVector& decodedVector,
    velox::vector_size_t numRows,
    bool descending,
    std::vector<char*>& rowOffsets) {
  const bool mayHaveNulls = decodedVector.mayHaveNulls();
  if (mayHaveNulls) {
    for (auto row = 0; row < numRows; ++row) {
      if (!decodedVector.isNullAt(row)) {
        const auto value = decodedVector.valueAt<velox::StringView>(row);
        encodeString(value.data(), value.size(), descending, rowOffsets[row]);
      }
    }
  } else {
    for (auto row = 0; row < numRows; ++row) {
      const auto value = decodedVector.valueAt<velox::StringView>(row);
      encodeString(value.data(), value.size(), descending, rowOffsets[row]);
    }
  }
}

void encodeTimestamp(
    const velox::DecodedVector& decodedVector,
    velox::vector_size_t numRows,
    bool descending,
    std::vector<char*>& rowOffsets) {
  const bool mayHaveNulls = decodedVector.mayHaveNulls();
  if (mayHaveNulls) {
    for (auto row = 0; row < numRows; ++row) {
      if (!decodedVector.isNullAt(row)) {
        const auto value = decodedVector.valueAt<velox::Timestamp>(row);
        encodeLong(value.toNanos(), descending, rowOffsets[row]);
      }
    }
  } else {
    for (auto row = 0; row < numRows; ++row) {
      const auto value = decodedVector.valueAt<velox::Timestamp>(row);
      encodeLong(value.toNanos(), descending, rowOffsets[row]);
    }
  }
}

// Encodes Date type column for all rows in columnar fashion.
void encodeDate(
    const velox::DecodedVector& decodedVector,
    velox::vector_size_t numRows,
    bool descending,
    std::vector<char*>& rowOffsets) {
  const bool mayHaveNulls = decodedVector.mayHaveNulls();
  if (mayHaveNulls) {
    for (auto row = 0; row < numRows; ++row) {
      if (!decodedVector.isNullAt(row)) {
        const auto value = decodedVector.valueAt<int32_t>(row);
        encodeInteger(value, descending, rowOffsets[row]);
      }
    }
  } else {
    for (auto row = 0; row < numRows; ++row) {
      const auto value = decodedVector.valueAt<int32_t>(row);
      encodeInteger(value, descending, rowOffsets[row]);
    }
  }
}

// Template function to encode column data for a specific type kind
template <velox::TypeKind Kind>
void encodeColumnTyped(
    const velox::DecodedVector& decodedVector,
    velox::vector_size_t numRows,
    bool descending,
    std::vector<char*>& rowOffsets) {
  if constexpr (Kind == velox::TypeKind::BIGINT) {
    encodeBigInt(decodedVector, numRows, descending, rowOffsets);
  } else if constexpr (Kind == velox::TypeKind::BOOLEAN) {
    encodeBoolean(decodedVector, numRows, descending, rowOffsets);
  } else if constexpr (Kind == velox::TypeKind::DOUBLE) {
    encodeDouble(decodedVector, numRows, descending, rowOffsets);
  } else if constexpr (Kind == velox::TypeKind::REAL) {
    encodeReal(decodedVector, numRows, descending, rowOffsets);
  } else if constexpr (Kind == velox::TypeKind::TINYINT) {
    encodeTinyInt(decodedVector, numRows, descending, rowOffsets);
  } else if constexpr (Kind == velox::TypeKind::SMALLINT) {
    encodeSmallInt(decodedVector, numRows, descending, rowOffsets);
  } else if constexpr (Kind == velox::TypeKind::INTEGER) {
    encodeIntegerType(decodedVector, numRows, descending, rowOffsets);
  } else if constexpr (
      Kind == velox::TypeKind::VARCHAR || Kind == velox::TypeKind::VARBINARY) {
    encodeStringType(decodedVector, numRows, descending, rowOffsets);
  } else if constexpr (Kind == velox::TypeKind::TIMESTAMP) {
    encodeTimestamp(decodedVector, numRows, descending, rowOffsets);
  } else {
    NIMBLE_UNSUPPORTED("Unsupported type: {}", Kind);
  }
}

bool incrementStringValue(std::string* value, bool descending) {
  if (!descending) {
    // Ascending order: increment
    for (int i = value->size() - 1; i >= 0; --i) {
      auto& byte = (*value)[i];
      unsigned char uByte = static_cast<unsigned char>(byte);

      // Check if we can increment without overflow
      if (uByte != 0xFF) {
        byte = static_cast<char>(uByte + 1);
        return true;
      }
      // This byte is at max, try next byte
    }
    // All bytes are at max value, append zero byte to get next larger string
    value->push_back('\0');
    return true;
  } else {
    // Descending order: decrement
    for (int i = value->size() - 1; i >= 0; --i) {
      auto& byte = (*value)[i];
      unsigned char uByte = static_cast<unsigned char>(byte);

      // Check if we can decrement without underflow
      if (uByte != 0x00) {
        byte = static_cast<char>(uByte - 1);
        return true;
      }
      // This byte is at min, try next byte
    }
    // All bytes are at min value, remove last byte to get next smaller string
    if (!value->empty()) {
      value->pop_back();
      return true;
    }
    // Empty string, cannot decrement further
    return false;
  }
}

template <velox::TypeKind KIND>
bool setMinValueTyped(velox::VectorPtr& result, velox::vector_size_t row) {
  using T = typename velox::TypeTraits<KIND>::NativeType;

  if constexpr (
      KIND == velox::TypeKind::TINYINT || KIND == velox::TypeKind::SMALLINT ||
      KIND == velox::TypeKind::INTEGER || KIND == velox::TypeKind::BIGINT) {
    auto* flatVector = result->asChecked<velox::FlatVector<T>>();
    flatVector->set(row, std::numeric_limits<T>::min());
    return true;
  }

  if constexpr (
      KIND == velox::TypeKind::VARCHAR || KIND == velox::TypeKind::VARBINARY) {
    auto* flatVector =
        result->asChecked<velox::FlatVector<velox::StringView>>();
    flatVector->set(row, velox::StringView(""));
    return true;
  }

  if constexpr (KIND == velox::TypeKind::TIMESTAMP) {
    auto* flatVector = result->asChecked<velox::FlatVector<velox::Timestamp>>();
    flatVector->set(
        row, velox::Timestamp::fromNanos(std::numeric_limits<int64_t>::min()));
    return true;
  }

  NIMBLE_UNSUPPORTED("Cannot set min value for column type: {}", KIND);
}

template <velox::TypeKind KIND>
bool setMaxValueTyped(velox::VectorPtr& result, velox::vector_size_t row) {
  using T = typename velox::TypeTraits<KIND>::NativeType;

  if constexpr (
      KIND == velox::TypeKind::TINYINT || KIND == velox::TypeKind::SMALLINT ||
      KIND == velox::TypeKind::INTEGER || KIND == velox::TypeKind::BIGINT) {
    auto* flatVector = result->asChecked<velox::FlatVector<T>>();
    flatVector->set(row, std::numeric_limits<T>::max());
    return true;
  }

  if constexpr (
      KIND == velox::TypeKind::VARCHAR || KIND == velox::TypeKind::VARBINARY) {
    // For max string value, we can't represent it directly, but we can use
    // a very large string. However, this is problematic. For now, return false.
    return false;
  }

  if constexpr (KIND == velox::TypeKind::TIMESTAMP) {
    auto* flatVector = result->asChecked<velox::FlatVector<velox::Timestamp>>();
    flatVector->set(
        row, velox::Timestamp::fromNanos(std::numeric_limits<int64_t>::max()));
    return true;
  }

  NIMBLE_UNSUPPORTED("Cannot set max value for column type: {}", KIND);
}

template <velox::TypeKind KIND>
bool incrementColumnValueTyped(
    const velox::VectorPtr& column,
    velox::vector_size_t row,
    velox::VectorPtr& result,
    bool descending) {
  using T = typename velox::TypeTraits<KIND>::NativeType;

  if constexpr (
      KIND == velox::TypeKind::TINYINT || KIND == velox::TypeKind::SMALLINT ||
      KIND == velox::TypeKind::INTEGER || KIND == velox::TypeKind::BIGINT) {
    auto* flatVector = column->as<velox::FlatVector<T>>();
    const auto value = flatVector->valueAt(row);
    if (!descending) {
      // Ascending: increment
      if (value == std::numeric_limits<T>::max()) {
        return false;
      }
      flatVector->set(row, value + 1);
    } else {
      // Descending: decrement
      if (value == std::numeric_limits<T>::min()) {
        return false;
      }
      flatVector->set(row, value - 1);
    }
    return true;
  }

  if constexpr (
      KIND == velox::TypeKind::VARCHAR || KIND == velox::TypeKind::VARBINARY) {
    // For strings, increment/decrement the string representation.
    auto* flatVector = column->as<velox::FlatVector<velox::StringView>>();
    const auto value = flatVector->valueAt(row);
    std::string str(value.data(), value.size());
    if (!incrementStringValue(&str, descending)) {
      return false;
    }
    flatVector->set(row, velox::StringView(str));
    return true;
  }

  if constexpr (KIND == velox::TypeKind::TIMESTAMP) {
    auto* flatVector = column->as<velox::FlatVector<velox::Timestamp>>();
    const auto value = flatVector->valueAt(row);
    const int64_t nanos = value.toNanos();
    if (!descending) {
      // Ascending: increment
      if (nanos == std::numeric_limits<int64_t>::max()) {
        return false;
      }
      flatVector->set(row, velox::Timestamp::fromNanos(nanos + 1));
    } else {
      // Descending: decrement
      if (nanos == std::numeric_limits<int64_t>::min()) {
        return false;
      }
      flatVector->set(row, velox::Timestamp::fromNanos(nanos - 1));
    }
    return true;
  }

  NIMBLE_UNSUPPORTED("Cannot increment column type: {}", KIND);
}

// Handles incrementing or decrementing null column values based on sort order.
// Returns true if operation succeeds, false otherwise.
bool incrementNullColumnValue(
    const velox::VectorPtr& column,
    velox::vector_size_t row,
    bool descending,
    bool nullLast,
    velox::VectorPtr& result) {
  if (!nullLast) {
    // Nulls first: null is at the beginning
    result->setNull(row, false);
    if (descending) {
      // Descending with nulls first: next value is maximum
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          setMaxValueTyped, column->typeKind(), result, row);
    } else {
      // Ascending with nulls first: next value is minimum
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          setMinValueTyped, column->typeKind(), result, row);
    }
  } else {
    // Nulls last: null is at the end, can't increment
    return false;
  }
}

// Increments or decrements a column value at the given row index based on sort
// order. Returns true if increment/decrement succeeds, false if value
// overflows/underflows.
bool incrementColumnValue(
    const velox::VectorPtr& column,
    velox::vector_size_t row,
    bool descending,
    bool nullLast,
    velox::VectorPtr& result) {
  if (column->isNullAt(row)) {
    return incrementNullColumnValue(column, row, descending, nullLast, result);
  }

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      incrementColumnValueTyped,
      column->typeKind(),
      column,
      row,
      result,
      descending);
}
} // namespace

// static.
std::unique_ptr<KeyEncoder> KeyEncoder::create(
    const std::vector<std::string>& keyColumns,
    const velox::RowTypePtr& inputType,
    const std::vector<velox::core::SortOrder>& sortOrders,
    velox::memory::MemoryPool* pool) {
  return std::unique_ptr<KeyEncoder>(
      new KeyEncoder(keyColumns, inputType, sortOrders, pool));
}

KeyEncoder::KeyEncoder(
    const std::vector<std::string>& keyColumns,
    const velox::RowTypePtr& inputType,
    const std::vector<velox::core::SortOrder>& sortOrders,
    velox::memory::MemoryPool* pool)
    : inputType_{inputType},
      sortOrders_{sortOrders},
      keyChannels_{getKeyChannels(inputType_, keyColumns)},
      pool_{pool},
      childDecodedVectors_{keyColumns.size()} {
  NIMBLE_CHECK_EQ(keyChannels_.size(), sortOrders_.size());
}

std::optional<velox::RowVectorPtr> KeyEncoder::createIncrementedBound(
    const velox::RowVectorPtr& bound) const {
  const auto& children = bound->children();
  NIMBLE_CHECK_EQ(bound->size(), 1);
  const auto numKeyColumns = keyChannels_.size();
  NIMBLE_CHECK_EQ(children.size(), keyChannels_.size());

  std::vector<velox::VectorPtr> newChildren;
  newChildren.reserve(numKeyColumns);
  for (const auto& child : children) {
    newChildren.push_back(velox::BaseVector::create(child->type(), 1, pool_));
  }

  // Copy all values from the source row
  for (size_t i = 0; i < numKeyColumns; ++i) {
    newChildren[i]->copy(children[i].get(), 0, 0, 1);
  }

  // Increment from rightmost key column (least significant) to leftmost and
  // quit once we find a column that can be incremented.
  for (int i = numKeyColumns - 1; i >= 0; --i) {
    const bool descending = !sortOrders_[i].isAscending();
    const bool nullLast = !sortOrders_[i].isNullsFirst();
    if (incrementColumnValue(
            children[i], 0, descending, nullLast, newChildren[i])) {
      return std::make_shared<velox::RowVector>(
          pool_, bound->type(), nullptr, 1, std::move(newChildren));
    }
  }

  // All key columns overflowed - cannot increment
  return std::nullopt;
}

void KeyEncoder::encode(
    const velox::VectorPtr& input,
    Vector<std::string_view>& encodedKeys,
    Buffer& buffer) {
  NIMBLE_CHECK_GT(input->size(), 0);
  SCOPE_EXIT {
    encodedSizes_.clear();
  };
  decodedVector_.decode(*input, /*loadLazy=*/true);
  const auto* rowBase = decodedVector_.base()->asChecked<velox::RowVector>();
  const auto& children = rowBase->children();
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    childDecodedVectors_[i].decode(*children[keyChannels_[i]]);
  }
  const auto totalBytes = estimateEncodedSize();
  auto* reserved = buffer.reserve(totalBytes);

  const auto numRows = input->size();
  const auto numKeys = keyChannels_.size();

  // Compute buffer start offsets for each row
  std::vector<char*> rowOffsets(numRows);
  rowOffsets[0] = reserved;
  for (auto row = 1; row < numRows; ++row) {
    rowOffsets[row] = rowOffsets[row - 1] + encodedSizes_[row - 1];
  }

  // Encode column-by-column for better cache locality
  for (auto keyIdx = 0; keyIdx < numKeys; ++keyIdx) {
    const bool nullLast = !sortOrders_[keyIdx].isNullsFirst();
    const bool descending = !sortOrders_[keyIdx].isAscending();
    const auto& decodedVector = childDecodedVectors_[keyIdx];

    // Encode null indicator for all rows
    for (auto row = 0; row < numRows; ++row) {
      encodeBool(!nullLast, rowOffsets[row]);
    }

    // Encode column data for all rows
    const auto typeKind = decodedVector.base()->typeKind();
    if (decodedVector.base()->type()->isDate()) {
      encodeDate(decodedVector, numRows, descending, rowOffsets);
    } else {
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          encodeColumnTyped,
          typeKind,
          decodedVector,
          numRows,
          descending,
          rowOffsets);
    }
  }

  // Build encoded keys string views
  encodedKeys.reserve(encodedKeys.size() + numRows);
  for (auto row = 0; row < numRows; ++row) {
    encodedKeys.emplace_back(
        reserved + (row > 0 ? encodedSizes_[row - 1] : 0), encodedSizes_[row]);
  }
}

uint64_t KeyEncoder::estimateEncodedSize() {
  const auto numRows = decodedVector_.size();
  encodedSizes_.resize(numRows, 0);
  // Initialize base sizes with one byte per key channel.
  for (auto i = 0; i < numRows; ++i) {
    encodedSizes_[i] += keyChannels_.size();
  }

  // Process each key channel in columnar order.
  for (const auto keyChannel : keyChannels_) {
    velox::ScratchPtr<velox::vector_size_t, 128> decodedRowsHolder(scratch_);
    auto* decodedRows = decodedRowsHolder.get(numRows);
    for (auto row = 0; row < numRows; ++row) {
      decodedRows[row] = decodedVector_.index(row);
    }

    estimateEncodedColumnSize(
        childDecodedVectors_[keyChannel],
        folly::Range<const velox::vector_size_t*>(decodedRows, numRows),
        encodedSizes_,
        scratch_);
  }
  return std::accumulate(encodedSizes_.begin(), encodedSizes_.end(), 0);
}

std::vector<std::string> KeyEncoder::encode(const velox::RowVectorPtr& input) {
  Buffer buffer{*pool_};
  Vector<std::string_view> encodedKeys{pool_};
  // Call the public encode method
  encode(input, encodedKeys, buffer);

  // Copy encoded keys to result.
  std::vector<std::string> result;
  result.reserve(encodedKeys.size());
  for (const auto& key : encodedKeys) {
    result.emplace_back(key.data(), key.size());
  }
  return result;
}

EncodedKeyBounds KeyEncoder::encodeIndexBounds(const IndexBounds& indexBounds) {
  NIMBLE_CHECK(indexBounds.validate());

  EncodedKeyBounds result;

  // Encode lower bound if present
  if (indexBounds.lowerBound.has_value()) {
    const auto& lowerBound = indexBounds.lowerBound.value();
    velox::RowVectorPtr lowerBoundToEncode = lowerBound.bound;
    // For exclusive lower bound, increment the row before encoding
    if (!lowerBound.inclusive) {
      const auto incrementedBoundOpt =
          createIncrementedBound(lowerBoundToEncode);
      if (!incrementedBoundOpt.has_value()) {
        NIMBLE_FAIL("Cannot increment lower bound");
      }
      lowerBoundToEncode = incrementedBoundOpt.value();
    }
    auto encodedKeys = encode(lowerBoundToEncode);
    NIMBLE_CHECK_EQ(encodedKeys.size(), 1);
    result.lowerKey = std::move(encodedKeys[0]);
  }

  // Encode upper bound if present
  if (indexBounds.upperBound.has_value()) {
    const auto& upperBound = indexBounds.upperBound.value();
    velox::RowVectorPtr upperBoundToEncode = upperBound.bound;

    // For inclusive upper bound, increment the row before encoding
    if (upperBound.inclusive) {
      const auto incrementedBoundOpt = createIncrementedBound(upperBound.bound);
      // If increment fails (bound is at maximum value), treat as unbounded by
      // setting to nullptr. This prevents setting an upper bound when the
      // inclusive upper bound is already at the maximum possible value.
      if (!incrementedBoundOpt.has_value()) {
        upperBoundToEncode = nullptr;
      } else {
        upperBoundToEncode = incrementedBoundOpt.value();
      }
    }
    if (upperBoundToEncode != nullptr) {
      auto encodedKeys = encode(upperBoundToEncode);
      result.upperKey = std::move(encodedKeys[0]);
    }
  }

  return result;
}

} // namespace facebook::nimble
