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

template <typename T>
__device__ void decodeTrivial(GpuDecode::Trivial& op) {
  auto address = reinterpret_cast<uint64_t>(op.input);
  constexpr uint64_t kMask = sizeof(T) - 1;
  int32_t lowShift = (address & kMask) * 8;
  int32_t highShift = sizeof(T) * 8 - lowShift;
  auto source = reinterpret_cast<T*>(address & ~kMask);
  int32_t end = op.end;
  T* result = reinterpret_cast<T*>(op.result);
  auto scatter = op.scatter;
  if (scatter) {
    for (auto i = op.begin + threadIdx.x; i < end; i += blockDim.x) {
      result[scatter[i]] =
          (source[i] >> lowShift) | (source[i + 1] << highShift);
    }
  } else {
    for (auto i = op.begin + threadIdx.x; i < end; i += blockDim.x) {
      result[i] = (source[i] >> lowShift) | (source[i + 1] << highShift);
    }
  }
}

__device__ inline void decodeTrivial(GpuDecode& plan) {
  auto& op = plan.data.trivial;
  switch (op.dataType) {
    case DataType::Int8:
    case DataType::Uint8:
      decodeTrivial<uint8_t>(op);
      break;
    case DataType::Int16:
    case DataType::Uint16:
      decodeTrivial<uint16_t>(op);
      break;
    case DataType::Int32:
    case DataType::Uint32:
    case DataType::Float:
      decodeTrivial<uint32_t>(op);
      break;
    case DataType::Int64:
    case DataType::Uint64:
    case DataType::Double:
      decodeTrivial<uint64_t>(op);
      break;
    default:
      if (threadIdx.x == 0) {
        printf("ERROR: Unsupported data type for Trivial\n");
        plan.statusCode = GpuDecode::StatusCode::kUnsupportedError;
      }
  }
}

} // namespace facebook::alpha::cuda::detail
