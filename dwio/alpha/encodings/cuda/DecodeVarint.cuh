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

__device__ inline uint32_t readVarint32(const char** pos) {
  uint32_t value = (**pos) & 127;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 7;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 14;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 21;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (*((*pos)++) & 127) << 28;
  return value;
}

__device__ inline uint64_t readVarint64(const char** pos) {
  uint64_t value = (**pos) & 127;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 7;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 14;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 21;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 28;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 35;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 42;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 49;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 56;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(*((*pos)++) & 127) << 63;
  return value;
}

template <int kBlockSize, typename T>
__device__ int decodeVarint(
    const char* input,
    int size,
    bool* ends,
    int32_t* endPos,
    T* output) {
  for (auto i = threadIdx.x; i < size; i += blockDim.x) {
    ends[i] = ~input[i] & 0x80;
  }
  auto numOut = scatterIndices<kBlockSize>(ends, true, 0, size, endPos);
  for (auto i = threadIdx.x; i < numOut; i += blockDim.x) {
    auto* pos = input + (i == 0 ? 0 : (endPos[i - 1] + 1));
    if constexpr (sizeof(T) == 4) {
      output[i] = readVarint32(&pos);
    } else {
      static_assert(sizeof(T) == 8);
      output[i] = readVarint64(&pos);
    }
  }
  return numOut;
}

template <int kBlockSize>
__device__ void decodeVarint(GpuDecode& plan) {
  auto& op = plan.data.varint;
  int resultSize;
  switch (op.resultType) {
    case DataType::Uint32:
      resultSize = decodeVarint<kBlockSize, uint32_t>(
          op.input, op.size, op.ends, op.endPos, (uint32_t*)op.result);
      break;
    case DataType::Uint64:
      resultSize = decodeVarint<kBlockSize, uint64_t>(
          op.input, op.size, op.ends, op.endPos, (uint64_t*)op.result);
      break;
    default:
      if (threadIdx.x == 0) {
        printf("ERROR: Unsupported result type for varint decoder\n");
        plan.statusCode = GpuDecode::StatusCode::kUnsupportedError;
      }
  }
  if (threadIdx.x == 0) {
    op.resultSize = resultSize;
  }
}

} // namespace facebook::alpha::cuda::detail
