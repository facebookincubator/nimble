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

// Benchmark: sparse decode — FBA per-element vs full block decode + pick.
// Measures the crossover point where decoding the full block becomes
// faster than per-element FixedBitArray::get().
//
// Run: buck2 run @//mode/opt
// fbcode//dwio/nimble/encodings/benchmarks:block_bit_packing_benchmark
//
// Results (Release mode, bw=5, 1024-row block):
//
//  Rows  Selectivity     FBA      FullBlock   PerGroup    Winner
//     5       0.5%       10ns       124ns       24ns      FBA (11x)
//    50         5%       85ns       137ns      142ns      FBA (1.6x)
//   100        10%      153ns       159ns      195ns      ~ crossover
//   200        20%      294ns       222ns      290ns      FullBlock (1.3x)
//   500        49%      702ns       324ns      453ns      FullBlock (2.2x)
//   800        78%     1.12us       425ns      616ns      FullBlock (2.6x)
//  1024       100%     1.42us       502ns      743ns      FullBlock (2.8x)
//
// Conclusion: FBA wins below ~10% selectivity, full block wins above.
// Per-group fastunpack never wins at any selectivity.
// BBP uses a hybrid threshold of kDefaultBlockSize/10 (~102 rows).

#include <algorithm>
#include <numeric>
#include <random>
#include <vector>

#include "dwio/nimble/common/FixedBitArray.h"
#include "folly/Benchmark.h"
#include "velox/dwio/common/Lemire/BitPacking/bitpackinghelpers.h"

using namespace facebook::nimble;

namespace {

constexpr uint32_t kBlockSize = 1024;
constexpr uint8_t kBitWidth = 5;

std::vector<char> packedData;
std::vector<uint32_t> output(kBlockSize);

// Selected row indices within the block.
std::vector<uint32_t> sparseRows5;
std::vector<uint32_t> sparseRows50;
std::vector<uint32_t> sparseRows100;
std::vector<uint32_t> sparseRows200;
std::vector<uint32_t> sparseRows500;
std::vector<uint32_t> sparseRows800;
std::vector<uint32_t> denseRows1024;

std::vector<uint32_t> makeSelectedRows(uint32_t count, uint32_t blockSize) {
  std::vector<uint32_t> all(blockSize);
  std::iota(all.begin(), all.end(), 0);
  std::mt19937 rng(42);
  std::shuffle(all.begin(), all.end(), rng);
  all.resize(count);
  std::sort(all.begin(), all.end());
  return all;
}

void setupData() {
  auto bufSize = FixedBitArray::bufferSize(kBlockSize, kBitWidth);
  packedData.resize(bufSize, 0);
  FixedBitArray fba(packedData.data(), kBitWidth);
  std::mt19937 rng(123);
  uint32_t maxVal = (1u << kBitWidth) - 1;
  for (uint32_t i = 0; i < kBlockSize; ++i) {
    fba.set(i, rng() % (maxVal + 1));
  }

  sparseRows5 = makeSelectedRows(5, kBlockSize);
  sparseRows50 = makeSelectedRows(50, kBlockSize);
  sparseRows100 = makeSelectedRows(100, kBlockSize);
  sparseRows200 = makeSelectedRows(200, kBlockSize);
  sparseRows500 = makeSelectedRows(500, kBlockSize);
  sparseRows800 = makeSelectedRows(800, kBlockSize);
  denseRows1024.resize(kBlockSize);
  std::iota(denseRows1024.begin(), denseRows1024.end(), 0);
}

// Option A: FBA::get() per selected row (current BBP sparse path)
void decodeFBA(const std::vector<uint32_t>& rows) {
  FixedBitArray fba{
      {packedData.data(), FixedBitArray::bufferSize(kBlockSize, kBitWidth)},
      kBitWidth};
  for (uint32_t i = 0; i < rows.size(); ++i) {
    output[i] = static_cast<uint32_t>(fba.get(rows[i]));
  }
  folly::doNotOptimizeAway(output[0]);
}

// Option B: fullUnpack entire block via Lemire, then pick rows (Impulse style)
void decodeFullBlockThenPick(const std::vector<uint32_t>& rows) {
  uint32_t decoded[kBlockSize];
  const auto* input = reinterpret_cast<const uint32_t*>(packedData.data());
  constexpr uint32_t kGroupSize = 32;
  for (uint32_t r = 0; r < kBlockSize; r += kGroupSize) {
    facebook::velox::fastpforlib::fastunpack(
        input + r / kGroupSize * kBitWidth, decoded + r, kBitWidth);
  }
  for (uint32_t i = 0; i < rows.size(); ++i) {
    output[i] = decoded[rows[i]];
  }
  folly::doNotOptimizeAway(output[0]);
}

// Option C: fastunpack only the groups that contain selected rows.
// Decodes 32 values per group, picks only the needed ones.
void decodePerGroup(const std::vector<uint32_t>& rows) {
  constexpr uint32_t kGroupSize = 32;
  const auto* input = reinterpret_cast<const uint32_t*>(packedData.data());
  uint32_t tmp[kGroupSize];
  uint32_t outIdx = 0;
  uint32_t i = 0;
  while (i < rows.size()) {
    const auto groupIdx = rows[i] / kGroupSize;
    facebook::velox::fastpforlib::fastunpack(
        input + groupIdx * kBitWidth, tmp, kBitWidth);
    while (i < rows.size() && rows[i] / kGroupSize == groupIdx) {
      output[outIdx++] = tmp[rows[i] % kGroupSize];
      ++i;
    }
  }
  folly::doNotOptimizeAway(output[0]);
}

} // namespace

// ============================================================================
// 5 rows / 1024 (0.5% selectivity)
// ============================================================================
BENCHMARK(FBA_sparse_5) {
  decodeFBA(sparseRows5);
}
BENCHMARK_RELATIVE(FullBlock_sparse_5) {
  decodeFullBlockThenPick(sparseRows5);
}
BENCHMARK_RELATIVE(PerGroup_sparse_5) {
  decodePerGroup(sparseRows5);
}
BENCHMARK_DRAW_LINE();

// ============================================================================
// 50 rows / 1024 (5% selectivity)
// ============================================================================
BENCHMARK(FBA_sparse_50) {
  decodeFBA(sparseRows50);
}
BENCHMARK_RELATIVE(FullBlock_sparse_50) {
  decodeFullBlockThenPick(sparseRows50);
}
BENCHMARK_RELATIVE(PerGroup_sparse_50) {
  decodePerGroup(sparseRows50);
}
BENCHMARK_DRAW_LINE();

// ============================================================================
// 100 rows / 1024 (10% selectivity)
// ============================================================================
BENCHMARK(FBA_sparse_100) {
  decodeFBA(sparseRows100);
}
BENCHMARK_RELATIVE(FullBlock_sparse_100) {
  decodeFullBlockThenPick(sparseRows100);
}
BENCHMARK_RELATIVE(PerGroup_sparse_100) {
  decodePerGroup(sparseRows100);
}
BENCHMARK_DRAW_LINE();

// ============================================================================
// 200 rows / 1024 (20% selectivity)
// ============================================================================
BENCHMARK(FBA_sparse_200) {
  decodeFBA(sparseRows200);
}
BENCHMARK_RELATIVE(FullBlock_sparse_200) {
  decodeFullBlockThenPick(sparseRows200);
}
BENCHMARK_RELATIVE(PerGroup_sparse_200) {
  decodePerGroup(sparseRows200);
}
BENCHMARK_DRAW_LINE();

// ============================================================================
// 500 rows / 1024 (49% selectivity)
// ============================================================================
BENCHMARK(FBA_sparse_500) {
  decodeFBA(sparseRows500);
}
BENCHMARK_RELATIVE(FullBlock_sparse_500) {
  decodeFullBlockThenPick(sparseRows500);
}
BENCHMARK_RELATIVE(PerGroup_sparse_500) {
  decodePerGroup(sparseRows500);
}
BENCHMARK_DRAW_LINE();

// ============================================================================
// 800 rows / 1024 (78% selectivity)
// ============================================================================
BENCHMARK(FBA_sparse_800) {
  decodeFBA(sparseRows800);
}
BENCHMARK_RELATIVE(FullBlock_sparse_800) {
  decodeFullBlockThenPick(sparseRows800);
}
BENCHMARK_RELATIVE(PerGroup_sparse_800) {
  decodePerGroup(sparseRows800);
}
BENCHMARK_DRAW_LINE();

// ============================================================================
// 1024 rows / 1024 (100% — dense)
// ============================================================================
BENCHMARK(FBA_dense_1024) {
  decodeFBA(denseRows1024);
}
BENCHMARK_RELATIVE(FullBlock_dense_1024) {
  decodeFullBlockThenPick(denseRows1024);
}
BENCHMARK_RELATIVE(PerGroup_dense_1024) {
  decodePerGroup(denseRows1024);
}

// ============================================================================
// Section 2: uint8 decode — fastunpack_quarter vs fastunpack32 + narrow
//
// Option A: Lemire fastunpack_quarter — native 8-bit unpack, 8 values/call,
//   outputs directly to uint8_t*.
// Option B: Lemire fastunpack (32-bit) into uint32_t[32] temp buffer, then
//   narrowing loop to uint8_t.
//
// Results (Release mode, ~8M values):
//
//  bw  Option A (8-bit)  Option B (32-bit+narrow)   A vs B
//   1         2.24ms              3.36ms            A 1.5x faster
//   2         2.14ms              3.73ms            A 1.7x faster
//   3         2.68ms              3.55ms            A 1.3x faster
//   4         1.39ms              3.97ms            A 2.9x faster
//   5         3.02ms              3.60ms            A 1.2x faster
//   6         2.73ms              3.77ms            A 1.4x faster
//   7         2.59ms              3.79ms            A 1.5x faster
//   8         1.39ms              3.99ms            A 2.9x faster
//
// Conclusion: Option A wins 1.3-2.9x across all bit widths.
// ============================================================================

namespace {

constexpr uint32_t kUint8NumBlocks = 8192;
constexpr uint32_t kUint8TotalValues = kBlockSize * kUint8NumBlocks;

struct Uint8PackedData {
  std::vector<char> bits;
  uint32_t bytesPerBlock;
};

std::vector<uint8_t> uint8OutputA(kUint8TotalValues);
std::vector<uint8_t> uint8OutputB(kUint8TotalValues);

Uint8PackedData uint8PackedBw[9]; // index 1..8

Uint8PackedData packUint8Data(uint8_t bitWidth) {
  const auto blockBytes = FixedBitArray::bufferSize(kBlockSize, bitWidth);
  const auto totalBytes = blockBytes * kUint8NumBlocks;
  std::vector<char> bits(totalBytes, 0);
  std::mt19937 rng(42);
  const uint32_t maxVal = (1u << bitWidth) - 1;
  for (uint32_t b = 0; b < kUint8NumBlocks; ++b) {
    FixedBitArray fba(bits.data() + b * blockBytes, bitWidth);
    for (uint32_t i = 0; i < kBlockSize; ++i) {
      fba.set(i, rng() % (maxVal + 1));
    }
  }
  return {std::move(bits), static_cast<uint32_t>(blockBytes)};
}

void decodeQuarter(const Uint8PackedData& data, uint8_t bitWidth) {
  constexpr uint32_t kGroupSize = 8;
  auto* out = uint8OutputA.data();
  for (uint32_t b = 0; b < kUint8NumBlocks; ++b) {
    const auto* input = reinterpret_cast<const uint8_t*>(
        data.bits.data() + b * data.bytesPerBlock);
    for (uint32_t r = 0; r < kBlockSize; r += kGroupSize) {
      facebook::velox::fastpforlib::internal::fastunpack_quarter(
          input, out + b * kBlockSize + r, bitWidth);
      input += bitWidth;
    }
  }
  folly::doNotOptimizeAway(uint8OutputA[0]);
}

void decode32Narrow(const Uint8PackedData& data, uint8_t bitWidth) {
  constexpr uint32_t kGroupSize = 32;
  auto* out = uint8OutputB.data();
  uint32_t tmp[kGroupSize];
  for (uint32_t b = 0; b < kUint8NumBlocks; ++b) {
    const auto* input = reinterpret_cast<const uint32_t*>(
        data.bits.data() + b * data.bytesPerBlock);
    for (uint32_t r = 0; r < kBlockSize; r += kGroupSize) {
      facebook::velox::fastpforlib::fastunpack(input, tmp, bitWidth);
      for (uint32_t i = 0; i < kGroupSize; ++i) {
        out[b * kBlockSize + r + i] = static_cast<uint8_t>(tmp[i]);
      }
      input += bitWidth;
    }
  }
  folly::doNotOptimizeAway(uint8OutputB[0]);
}

void setupUint8Data() {
  for (uint8_t bw = 1; bw <= 8; ++bw) {
    uint8PackedBw[bw] = packUint8Data(bw);
  }
}

} // namespace

#define BENCHMARK_UINT8_BW(bw)             \
  BENCHMARK(quarter_bw##bw) {              \
    decodeQuarter(uint8PackedBw[bw], bw);  \
  }                                        \
  BENCHMARK_RELATIVE(narrow32_bw##bw) {    \
    decode32Narrow(uint8PackedBw[bw], bw); \
  }                                        \
  BENCHMARK_DRAW_LINE();

BENCHMARK_UINT8_BW(1)
BENCHMARK_UINT8_BW(2)
BENCHMARK_UINT8_BW(3)
BENCHMARK_UINT8_BW(4)
BENCHMARK_UINT8_BW(5)
BENCHMARK_UINT8_BW(6)
BENCHMARK_UINT8_BW(7)
BENCHMARK_UINT8_BW(8)

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  setupData();
  setupUint8Data();
  folly::runBenchmarks();
  return 0;
}
