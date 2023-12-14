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

#include "dwio/alpha/encodings/cuda/DecodeDictionary.cuh"
#include "dwio/alpha/encodings/cuda/DecodeMainlyConstant.cuh"
#include "dwio/alpha/encodings/cuda/DecodeRle.cuh"
#include "dwio/alpha/encodings/cuda/DecodeSparseBool.cuh"
#include "dwio/alpha/encodings/cuda/DecodeStep.h"
#include "dwio/alpha/encodings/cuda/DecodeTrivial.cuh"
#include "dwio/alpha/encodings/cuda/DecodeVarint.cuh"
#include "dwio/alpha/encodings/cuda/ScatterIndices.cuh"

namespace facebook::alpha::cuda {

namespace detail {

template <int kBlockSize>
__global__ void decodeGlobal(GpuDecode* plan) {
  auto& op = plan[blockIdx.x];
  switch (op.step) {
    case DecodeStep::kTrivial:
    case DecodeStep::kDictionaryOnBitpack:
    case DecodeStep::kSparseBool:
      decodeNoSharedMemory(plan);
      break;
    case DecodeStep::kMakeScatterIndices:
    case DecodeStep::kMainlyConstant:
    case DecodeStep::kVarint:
    case DecodeStep::kRleTotalLength:
    case DecodeStep::kRle:
      decodeWithSharedMemory<kBlockSize>(plan);
      break;
    default:
      if (threadIdx.x == 0) {
        printf("ERROR: Unsupported DecodeStep\n");
        op.statusCode = GpuDecode::StatusCode::kUnsupportedError;
      }
  }
}

} // namespace detail

__device__ inline void decodeNoSharedMemory(GpuDecode* plan) {
  auto& op = plan[blockIdx.x];
  if (threadIdx.x == 0) {
    op.statusCode = GpuDecode::StatusCode::kOk;
  }
  switch (op.step) {
    case DecodeStep::kTrivial:
      detail::decodeTrivial(op);
      break;
    case DecodeStep::kDictionaryOnBitpack:
      detail::decodeDictionaryOnBitpack(op);
      break;
    case DecodeStep::kSparseBool:
      detail::decodeSparseBool(op.data.sparseBool);
      break;
    default:
      if (threadIdx.x == 0) {
        printf("ERROR: Unsupported DecodeStep (no shared memory)\n");
        op.statusCode = GpuDecode::StatusCode::kUnsupportedError;
      }
  }
}

template <int kBlockSize>
__device__ void decodeWithSharedMemory(GpuDecode* plan) {
  auto& op = plan[blockIdx.x];
  if (threadIdx.x == 0) {
    op.statusCode = GpuDecode::StatusCode::kOk;
  }
  switch (op.step) {
    case DecodeStep::kMakeScatterIndices:
      detail::makeScatterIndices<kBlockSize>(op.data.makeScatterIndices);
      break;
    case DecodeStep::kMainlyConstant:
      detail::decodeMainlyConstant<kBlockSize>(op);
      break;
    case DecodeStep::kVarint:
      detail::decodeVarint<kBlockSize>(op);
      break;
    case DecodeStep::kRleTotalLength:
      detail::rleTotalLength<kBlockSize>(op.data.rleTotalLength);
      break;
    case DecodeStep::kRle:
      detail::decodeRle<kBlockSize>(op);
      break;
    default:
      if (threadIdx.x == 0) {
        printf("ERROR: Unsupported DecodeStep (with shared memory)\n");
        op.statusCode = GpuDecode::StatusCode::kUnsupportedError;
      }
  }
}

template <int kBlockSize>
void decodeGlobal(GpuDecode* plan, int numBlocks, cudaStream_t stream) {
  detail::decodeGlobal<kBlockSize><<<numBlocks, kBlockSize, 0, stream>>>(plan);
}

} // namespace facebook::alpha::cuda
