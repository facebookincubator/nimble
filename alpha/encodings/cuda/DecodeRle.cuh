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

template <int kBlockSize, typename T, typename U>
__device__ T sum(const U* values, int size) {
  using Reduce = cub::BlockReduce<T, kBlockSize>;
  __shared__ typename Reduce::TempStorage reduceStorage;
  T total = 0;
  for (int i = 0; i < size; i += kBlockSize) {
    auto numValid = min(size - i, kBlockSize);
    T value;
    if (threadIdx.x < numValid) {
      value = values[i + threadIdx.x];
    }
    total += Reduce(reduceStorage).Sum(value, numValid);
    __syncthreads();
  }
  return total;
}

template <int kBlockSize>
__device__ void rleTotalLength(GpuDecode::RleTotalLength& op) {
  auto result = sum<kBlockSize, int64_t>(op.input, op.count);
  if (threadIdx.x == 0) {
    op.result = result;
  }
}

template <typename T>
__device__ int upperBound(const T* data, int size, T target) {
  int lo = 0, hi = size;
  while (lo < hi) {
    int i = (lo + hi) / 2;
    if (data[i] <= target) {
      lo = i + 1;
    } else {
      hi = i;
    }
  }
  return lo;
}

template <int kBlockSize, typename T>
__device__ void decodeRle(GpuDecode::Rle& op) {
  using BlockScan = cub::BlockScan<int32_t, kBlockSize>;
  __shared__ typename BlockScan::TempStorage scanStorage;
  static_assert(sizeof scanStorage >= sizeof(int32_t) * kBlockSize);
  auto* offsets = (int32_t*)&scanStorage;
  auto* values = (const T*)op.values;
  auto* result = (T*)op.result;
  int total = 0;
  for (int i = 0; i < op.count; i += blockDim.x) {
    auto ti = threadIdx.x + i;
    auto len = ti < op.count ? op.lengths[ti] : 0;
    int32_t offset, subtotal;
    __syncthreads();
    BlockScan(scanStorage).InclusiveSum(len, offset, subtotal);
    __syncthreads();
    offsets[threadIdx.x] = offset;
    __syncthreads();
    for (int j = threadIdx.x; j < subtotal; j += blockDim.x) {
      result[total + j] = values[i + upperBound(offsets, blockDim.x, j)];
    }
    total += subtotal;
  }
}

template <int kBlockSize>
__device__ void decodeRle(GpuDecode& plan) {
  auto& op = plan.data.rle;
  switch (op.valueType) {
    case DataType::Int8:
    case DataType::Uint8:
      decodeRle<kBlockSize, uint8_t>(op);
      break;
    case DataType::Int16:
    case DataType::Uint16:
      decodeRle<kBlockSize, uint16_t>(op);
      break;
    case DataType::Int32:
    case DataType::Uint32:
    case DataType::Float:
      decodeRle<kBlockSize, uint32_t>(op);
      break;
    case DataType::Int64:
    case DataType::Uint64:
    case DataType::Double:
      decodeRle<kBlockSize, uint64_t>(op);
      break;
    default:
      if (threadIdx.x == 0) {
        printf("ERROR: Unsupported value type for Rle\n");
        plan.statusCode = GpuDecode::StatusCode::kUnsupportedError;
      }
  }
}

} // namespace facebook::alpha::cuda::detail
