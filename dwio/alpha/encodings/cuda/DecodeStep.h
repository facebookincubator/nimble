/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#pragma once

#include "dwio/alpha/common/Types.h"

namespace facebook::alpha::cuda {

/// Describe the type of one GPU decode operation.  This can be some decoding,
/// or pre/post processing other than decoding.
enum class DecodeStep {
  kConstant32,
  kConstant64,
  kConstantChar,
  kConstantBool,
  kConstantBytes,
  kTrivial,
  kTrivialNoOp,
  kMainlyConstant,
  kBitpack32,
  kBitpack64,
  kRleTotalLength,
  kRleBool,
  kRle,
  kDictionary,
  kDictionaryOnBitpack,
  kVarint,
  kNullable,
  kSentinel,
  kSparseBool,
  kMakeScatterIndices,
  kScatter32,
  kScatter64,
  kLengthToOffset,
  kMissing,
  kStruct,
  kArray,
  kMap,
  kFlatMap,
  kFlatMapNode,
  kUnsupported,
};

/// Describes a decoding loop's input and result disposition.
struct GpuDecode {
  DecodeStep step;

  /// Code indicating the result status of decoding operation.  Usually some
  /// error message should be printed to stdout in kernel code to give more
  /// details if the result is not OK.
  enum class StatusCode : int8_t {
    kOk,
    kUnsupportedError,
  } statusCode;

  /// Decode dictionary or direct encoded data, optionally with bitpacking and
  /// baseline shift.
  struct DictionaryOnBitpack {
    // Type of the alphabet and result.
    DataType dataType;
    // Dictionary alphabet.
    const void* alphabet;
    // Indices into the alphabet.
    const uint64_t* indices;
    // Begin position for indices, scatter and result.
    int begin;
    // End position (exclusive) for indices, scatter and result.
    int end;
    // Bit width of each index.
    int bitWidth;
    // If not null, contains the output position relative to result pointer.
    const int32_t* scatter;
    // All indices should be offseted by this baseline.
    int64_t baseline;
    // Starting address of the result.
    void* result;
  };

  /// Extract indices of set or unset bits.
  struct MakeScatterIndices {
    // Input bits.
    const uint8_t* bits;
    // Whether set or unset bits should be found in the input.
    bool findSetBits;
    // Begin bit position of the input.
    int begin;
    // End bit position (exclusive) of the input.
    int end;
    // Address of the result indices.
    int32_t* indices;
    // If non-null, store the number of indices being written.
    int32_t* indicesCount;
  };

  /// Decode values encoded in mainly constant encoding.
  struct MainlyConstant {
    // Type of the values and result.
    DataType dataType;
    // Number of total values that should be written to result.
    int count;
    // Common value that is repeated.
    const void* commonValue;
    // Sparse values that will be picked up when isCommon is false.
    const void* otherValues;
    // Bitmask indicating whether a values is common or not.
    const uint8_t* isCommon;
    // Temporary storage to keep non-common value indices.  Should be
    // preallocated to at least count large.
    int32_t* otherIndices;
    // Starting address of the result.
    void* result;
    // If non-null, the count of non-common elements will be written to this
    // address.
    int32_t* otherCount;
  };

  /// Calculate total length for RLE output.
  struct RleTotalLength {
    // Input data to be summed.
    const int32_t* input;
    // Number of input data.
    int count;
    // The sum of input data.
    int64_t result;
  };

  /// Decode RLE encoded values.
  struct Rle {
    // Type of values and result.
    DataType valueType;
    // Values that will be repeated.
    const void* values;
    // Length of each value.
    const int32_t* lengths;
    // Number of values and lengths.
    int count;
    // Starting address of the result.
    const void* result;
  };

  /// Decode sparse bool encoded values.
  struct SparseBool {
    // Number of bits in the result.
    int totalCount;
    // Sparse value; common value is the opposite of this.
    bool sparseValue;
    // Bit indices where sparse value should be stored.
    const int32_t* sparseIndices;
    // Number of sparse values.
    int sparseCount;
    // Temporary storage to keep bool representation of bits.  Should be at
    // least totalCount large.
    bool* bools;
    // Address of the result bits.  We do not support unaligned result because
    // different blocks could access the same byte and cause racing condition.
    uint8_t* result;
  };

  /// Trivial encoding that copies data.  Handle unaligned input address
  /// properly.  Result address must be aligned.
  struct Trivial {
    // Type of the input and result data.
    DataType dataType;
    // Input data.
    const void* input;
    // Begin position for input, scatter and result.
    int begin;
    // End position (exclusive) for input, scatter and result.
    int end;
    // If not null, contains the output position relative to result pointer.
    const int32_t* scatter;
    // Starting address of the result.
    void* result;
  };

  /// Decode varint values.
  struct Varint {
    // Address of the input data.
    const char* input;
    // Byte size of the input data.
    int size;
    // Temporary storage to keep whether each byte is a terminal for a number.
    // Should be allocated at least "size" large.
    bool* ends;
    // Temporary storage to keep the location of end position of each number.
    // Should be allocated at least "size" large.
    int32_t* endPos;
    // Type of the result number.
    DataType resultType;
    // Starting address of the result.
    void* result;
    // Count of the numbers in the result.
    int resultSize;
  };

  union {
    DictionaryOnBitpack dictionaryOnBitpack;
    MakeScatterIndices makeScatterIndices;
    MainlyConstant mainlyConstant;
    RleTotalLength rleTotalLength;
    Rle rle;
    SparseBool sparseBool;
    Trivial trivial;
    Varint varint;
  } data;
};

} // namespace facebook::alpha::cuda
