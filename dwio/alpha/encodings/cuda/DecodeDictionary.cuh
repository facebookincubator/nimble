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

#include "dwio/alpha/encodings/cuda/DecodeStep.h"

namespace facebook::alpha::cuda::detail {

enum class ResultOp { kDict, kBaseline, kDictScatter, kBaselineScatter };

template <typename T, ResultOp op>
__device__ void storeResult(
    int32_t i,
    int32_t bitfield,
    T baseline,
    const T* dict,
    const int32_t* scatter,
    T* result) {
  if constexpr (op == ResultOp::kBaseline) {
    result[i] = bitfield + baseline;
  } else if constexpr (op == ResultOp::kBaselineScatter) {
    result[scatter[i]] = bitfield + baseline;
  } else if constexpr (op == ResultOp::kDict) {
    result[i] = dict[bitfield + baseline];
  } else if constexpr (op == ResultOp::kDictScatter) {
    result[scatter[i]] = dict[bitfield + baseline];
  }
}

__device__ inline uint64_t getValue(
    const uint64_t* words,
    int alignOffset,
    int bitWidth,
    int index,
    uint64_t mask) {
  int32_t indexBit = index * bitWidth + alignOffset;
  int32_t indexWord = indexBit >> 6;
  int32_t bit = indexBit & 63;
  uint64_t word = words[indexWord];
  uint64_t low = word >> bit;
  if (bitWidth + bit <= 64) {
    return low & mask;
  }
  uint64_t nextWord = words[indexWord + 1];
  int32_t bitsFromNext = bitWidth - (64 - bit);
  return low | ((nextWord & ((1 << bitsFromNext) - 1)) << (64 - bit));
}

template <typename T, ResultOp resultOp>
__device__ void decodeDictionaryOnBitpack(GpuDecode::DictionaryOnBitpack& op) {
  int32_t i = op.begin + threadIdx.x;
  auto end = op.end;
  auto address = reinterpret_cast<uint64_t>(op.indices);
  int32_t alignOffset = (address & 7) * 8;
  address &= ~7UL;
  auto words = reinterpret_cast<uint64_t*>(address);
  const T* dict = reinterpret_cast<const T*>(op.alphabet);
  auto scatter = op.scatter;
  auto baseline = op.baseline;
  auto bitWidth = op.bitWidth;
  uint64_t mask = (1ULL << bitWidth) - 1;
  auto result = reinterpret_cast<T*>(op.result);
  for (; i < end; i += blockDim.x) {
    int32_t index = getValue(words, alignOffset, bitWidth, i, mask);
    storeResult<T, resultOp>(i, index, baseline, dict, scatter, result);
  }
}

template <typename T>
__device__ void decodeDictionaryOnBitpack(GpuDecode::DictionaryOnBitpack& op) {
  if (!op.scatter) {
    if (op.alphabet) {
      decodeDictionaryOnBitpack<T, ResultOp::kDict>(op);
    } else {
      decodeDictionaryOnBitpack<T, ResultOp::kBaseline>(op);
    }
  } else {
    if (op.alphabet) {
      decodeDictionaryOnBitpack<T, ResultOp::kDictScatter>(op);
    } else {
      decodeDictionaryOnBitpack<T, ResultOp::kBaselineScatter>(op);
    }
  }
}

__device__ inline void decodeDictionaryOnBitpack(GpuDecode& plan) {
  auto& op = plan.data.dictionaryOnBitpack;
  switch (op.dataType) {
    case DataType::Int8:
      decodeDictionaryOnBitpack<int8_t>(op);
      break;
    case DataType::Uint8:
      decodeDictionaryOnBitpack<uint8_t>(op);
      break;
    case DataType::Int16:
      decodeDictionaryOnBitpack<int16_t>(op);
      break;
    case DataType::Uint16:
      decodeDictionaryOnBitpack<uint16_t>(op);
      break;
    case DataType::Int32:
      decodeDictionaryOnBitpack<int32_t>(op);
      break;
    case DataType::Uint32:
      decodeDictionaryOnBitpack<uint32_t>(op);
      break;
    case DataType::Int64:
      decodeDictionaryOnBitpack<int64_t>(op);
      break;
    case DataType::Uint64:
      decodeDictionaryOnBitpack<uint64_t>(op);
      break;
    default:
      if (threadIdx.x == 0) {
        printf("ERROR: Unsupported data type for DictionaryOnBitpack\n");
        plan.statusCode = GpuDecode::StatusCode::kUnsupportedError;
      }
  }
}

} // namespace facebook::alpha::cuda::detail
