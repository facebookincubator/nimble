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

__device__ inline void
bitpackBools(const bool* input, int count, uint8_t* out) {
  int nbytes = count / 8;
  for (auto i = threadIdx.x; i < nbytes; i += blockDim.x) {
    uint8_t value = 0;
    for (int j = 0; j < 8; ++j) {
      value |= input[8 * i + j] << j;
    }
    out[i] = value;
  }
  if (threadIdx.x == 0 && count % 8 != 0) {
    auto extra = count % 8;
    for (int j = count - extra; j < count; ++j) {
      setBit(out, j, input[j]);
    }
  }
}

__device__ inline void decodeSparseBool(GpuDecode::SparseBool& op) {
  for (int i = threadIdx.x; i < op.totalCount; i += blockDim.x) {
    op.bools[i] = !op.sparseValue;
  }
  __syncthreads();
  for (int i = threadIdx.x; i < op.sparseCount; i += blockDim.x) {
    op.bools[op.sparseIndices[i]] = op.sparseValue;
  }
  __syncthreads();
  bitpackBools(op.bools, op.totalCount, op.result);
}

} // namespace facebook::alpha::cuda::detail
