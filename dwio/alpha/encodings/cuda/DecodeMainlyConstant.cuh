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
#include "dwio/alpha/encodings/cuda/ScatterIndices.cuh"

namespace facebook::alpha::cuda::detail {

template <int kBlockSize, typename T>
__device__ void decodeMainlyConstant(GpuDecode::MainlyConstant& op) {
  auto otherCount = scatterIndices<kBlockSize>(
      op.isCommon, false, 0, op.count, op.otherIndices);
  auto commonValue = *(const T*)op.commonValue;
  auto* otherValues = (const T*)op.otherValues;
  auto* result = (T*)op.result;
  for (int i = threadIdx.x; i < op.count; i += blockDim.x) {
    result[i] = commonValue;
  }
  __syncthreads();
  for (int i = threadIdx.x; i < otherCount; i += blockDim.x) {
    result[op.otherIndices[i]] = otherValues[i];
  }
  if (threadIdx.x == 0 && op.otherCount) {
    *op.otherCount = otherCount;
  }
}

template <int kBlockSize>
__device__ void decodeMainlyConstant(GpuDecode& plan) {
  auto& op = plan.data.mainlyConstant;
  switch (op.dataType) {
    case DataType::Int8:
    case DataType::Uint8:
      decodeMainlyConstant<kBlockSize, uint8_t>(op);
      break;
    case DataType::Int16:
    case DataType::Uint16:
      decodeMainlyConstant<kBlockSize, uint16_t>(op);
      break;
    case DataType::Int32:
    case DataType::Uint32:
    case DataType::Float:
      decodeMainlyConstant<kBlockSize, uint32_t>(op);
      break;
    case DataType::Int64:
    case DataType::Uint64:
    case DataType::Double:
      decodeMainlyConstant<kBlockSize, uint64_t>(op);
      break;
    default:
      if (threadIdx.x == 0) {
        printf("ERROR: Unsupported data type for MainlyConstant\n");
        plan.statusCode = GpuDecode::StatusCode::kUnsupportedError;
      }
  }
}

} // namespace facebook::alpha::cuda::detail
