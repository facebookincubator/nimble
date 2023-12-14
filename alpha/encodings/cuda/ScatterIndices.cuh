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

#include <cub/cub.cuh> // @manual
#include "dwio/alpha/encodings/cuda/DecodeStep.h"

namespace facebook::alpha::cuda::detail {

template <int32_t kBlockSize, typename T>
__device__ int scatterIndices(
    const T* values,
    T value,
    int32_t begin,
    int32_t end,
    int32_t* indices) {
  typedef cub::BlockScan<int32_t, kBlockSize> BlockScan;
  __shared__ typename BlockScan::TempStorage scanStorage;
  int numMatch;
  bool match;
  int32_t k = 0;
  for (auto j = begin; j < end; j += kBlockSize) {
    auto jt = j + threadIdx.x;
    match = (jt < end && values[jt] == value);
    numMatch = match;
    int subtotal;
    BlockScan(scanStorage).ExclusiveSum(numMatch, numMatch, subtotal);
    __syncthreads();
    if (match) {
      indices[k + numMatch] = jt - begin;
    }
    k += subtotal;
  }
  return k;
}

template <int kBlockSize>
__device__ int scatterIndices(
    const uint8_t* bits,
    bool value,
    int32_t begin,
    int32_t end,
    int32_t* indices) {
  typedef cub::BlockScan<int32_t, kBlockSize> BlockScan;
  __shared__ typename BlockScan::TempStorage scanStorage;
  constexpr int kPerThread = 8;
  int numMatch[kPerThread];
  bool match[kPerThread];
  int32_t k = 0;
  constexpr auto kBitsPerBlock = kBlockSize * kPerThread;
  for (auto j = begin; j < end; j += kBitsPerBlock) {
    auto jt = j + threadIdx.x * kPerThread;
    for (auto i = 0; i < kPerThread; ++i) {
      match[i] = jt + i < end && isSet(bits, jt + i) == value;
      numMatch[i] = match[i];
    }
    int subtotal;
    BlockScan(scanStorage).ExclusiveSum(numMatch, numMatch, subtotal);
    __syncthreads();
    for (auto i = 0; i < kPerThread; ++i) {
      if (match[i]) {
        indices[k + numMatch[i]] = jt + i - begin;
      }
    }
    k += subtotal;
  }
  return k;
}

template <int kBlockSize>
__device__ void makeScatterIndices(GpuDecode::MakeScatterIndices& op) {
  auto indicesCount = scatterIndices<kBlockSize>(
      op.bits, op.findSetBits, op.begin, op.end, op.indices);
  if (threadIdx.x == 0 && op.indicesCount) {
    *op.indicesCount = indicesCount;
  }
}

} // namespace facebook::alpha::cuda::detail
