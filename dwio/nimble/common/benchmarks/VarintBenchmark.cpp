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
#include <algorithm>
#include <memory>
#include <vector>

#include "dwio/nimble/common/Varint.h"
#include "folly/Benchmark.h"
#include "folly/Random.h"
#include "folly/Varint.h"

using namespace ::facebook;

const int kNumElements = 1000 * 1000;

// Basically same code as dwrf::IntDecoder::readVuLong.
uint64_t DwrfRead(const char** bufferStart, const char* bufferEnd) {
  if (LIKELY(bufferEnd - *bufferStart >= folly::kMaxVarintLength64)) {
    const char* p = *bufferStart;
    uint64_t val;
    do {
      int64_t b;
      b = *p++;
      val = (b & 0x7f);
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 7;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 14;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 21;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 28;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 35;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 42;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 49;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 56;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x01) << 63;
      if (LIKELY(b >= 0)) {
        break;
      } else {
        throw std::runtime_error{"invalid encoding: likely corrupt data"};
      }
    } while (false);
    *bufferStart = p;
    return val;
  } else {
    // this part isn't the same, but doesn't measurably effect time.
    return nimble::varint::readVarint64(bufferStart);
  }
}

// Makes random data uniform over in bit width over 32 bits.
std::vector<uint32_t> MakeUniformData(int num_elements = kNumElements) {
  std::vector<uint32_t> data(num_elements);
  for (int i = 0; i < num_elements; ++i) {
    const int bit_shift = 1 + folly::Random::secureRand32() % 32;
    data[i] = folly::Random::secureRand32() % (1 << bit_shift);
  }
  return data;
}

// Makes 95% 1 byte, 5% 2 byte data.
std::vector<uint32_t> MakeSkewedData(int num_elements = kNumElements) {
  std::vector<uint32_t> data(num_elements);
  for (int i = 0; i < num_elements; ++i) {
    if (folly::Random::secureRand32() % 20) {
      data[i] = folly::Random::secureRand32() % (1 << 7);
    } else {
      data[i] = folly::Random::secureRand32() % (1 << 14);
    }
  }
  return data;
}

// Makes data where the first value is large (5-byte varint) and the rest
// fit in exactly `tailBytes` varint bytes. Tests fast-path recovery after
// a multi-byte value forces fallback to the general decoder.
std::vector<uint32_t> MakeLargeHeadData32(
    int tailBytes,
    int num_elements = kNumElements) {
  std::vector<uint32_t> data(num_elements);
  // First element: large 5-byte varint.
  data[0] = UINT32_MAX;
  uint32_t lo = (tailBytes == 1) ? 0 : (1u << (7 * (tailBytes - 1)));
  uint32_t hi = (1u << (7 * tailBytes)) - 1;
  for (int i = 1; i < num_elements; ++i) {
    data[i] = lo + folly::Random::secureRand32() % (hi - lo + 1);
  }
  return data;
}

// Makes data where a large (5-byte) varint appears every `interval` elements,
// with the rest fitting in `tailBytes` varint bytes. Tests fast-path recovery
// when large values sporadically interrupt runs of small varints.
std::vector<uint32_t> MakeSporadicLargeData32(
    int tailBytes,
    int interval,
    int num_elements = kNumElements) {
  std::vector<uint32_t> data(num_elements);
  uint32_t lo = (tailBytes == 1) ? 0 : (1u << (7 * (tailBytes - 1)));
  uint32_t hi = (1u << (7 * tailBytes)) - 1;
  for (int i = 0; i < num_elements; ++i) {
    if (i % interval == 0) {
      data[i] = UINT32_MAX;
    } else {
      data[i] = lo + folly::Random::secureRand32() % (hi - lo + 1);
    }
  }
  return data;
}

// Makes data where all values fit in exactly `numBytes` varint bytes.
std::vector<uint32_t> MakeFixedWidthData32(
    int numBytes,
    int num_elements = kNumElements) {
  std::vector<uint32_t> data(num_elements);
  uint32_t lo = (numBytes == 1) ? 0 : (1u << (7 * (numBytes - 1)));
  uint32_t hi = (1u << (7 * numBytes)) - 1;
  if (numBytes == 5) {
    hi = UINT32_MAX;
  }
  for (int i = 0; i < num_elements; ++i) {
    data[i] = lo + folly::Random::secureRand32() % (hi - lo + 1);
  }
  return data;
}

// Makes 64-bit data where all values fit in exactly `numBytes` varint bytes.
std::vector<uint64_t> MakeFixedWidthData64(
    int numBytes,
    int num_elements = kNumElements) {
  std::vector<uint64_t> data(num_elements);
  uint64_t lo = (numBytes == 1) ? 0 : (1ull << (7 * (numBytes - 1)));
  uint64_t hi = (numBytes >= 10) ? UINT64_MAX : ((1ull << (7 * numBytes)) - 1);
  for (int i = 0; i < num_elements; ++i) {
    data[i] = lo + folly::Random::secureRand64() % (hi - lo + 1);
  }
  return data;
}

// Encode data into a varint buffer, returns total encoded size.
template <typename T>
std::unique_ptr<char[]> EncodeData(
    const std::vector<T>& data,
    uint64_t& encodedSize) {
  auto buf = std::make_unique<char[]>(data.size() * folly::kMaxVarintLength64);
  char* pos = buf.get();
  for (auto val : data) {
    nimble::varint::writeVarint(val, &pos);
  }
  encodedSize = pos - buf.get();
  return buf;
}

// ============================================================================
// Original benchmarks (uniform + skewed, 32-bit)
// ============================================================================

BENCHMARK(Encode, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<char[]> buf;
  BENCHMARK_SUSPEND {
    data = MakeUniformData();
    buf = std::make_unique<char[]>(kNumElements * folly::kMaxVarintLength32);
  }
  CHECK(!data.empty());
  while (iters--) {
    char* pos = buf.get();
    for (int i = 0; i < kNumElements; ++i) {
      nimble::varint::writeVarint(data[i], &pos);
    }
    CHECK_GE(pos - buf.get(), kNumElements);
  }
}

BENCHMARK(NimbleBulkDecodeUniform, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<char[]> buf;
  std::vector<uint32_t> recovered;
  BENCHMARK_SUSPEND {
    recovered.resize(kNumElements);
    data = MakeUniformData();
    CHECK(!data.empty());
    buf = std::make_unique<char[]>(kNumElements * folly::kMaxVarintLength32);
    char* pos = buf.get();
    for (int i = 0; i < kNumElements; ++i) {
      nimble::varint::writeVarint(data[i], &pos);
    }
  }
  CHECK(!recovered.empty());
  while (iters--) {
    const char* cpos = buf.get();
    nimble::varint::bulkVarintDecode32(kNumElements, cpos, recovered.data());
    CHECK_EQ(recovered.back(), data.back());
  }
}

// ============================================================================
// Fixed byte-width benchmarks (32-bit): isolate per-width performance
// ============================================================================

BENCHMARK_DRAW_LINE();

BENCHMARK(BulkDecode_1byte, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<char[]> buf;
  std::vector<uint32_t> recovered;
  BENCHMARK_SUSPEND {
    recovered.resize(kNumElements);
    data = MakeFixedWidthData32(1);
    uint64_t sz;
    buf = EncodeData(data, sz);
  }
  while (iters--) {
    const char* cpos = buf.get();
    nimble::varint::bulkVarintDecode32(kNumElements, cpos, recovered.data());
    CHECK_EQ(recovered.back(), data.back());
  }
}

BENCHMARK_DRAW_LINE();

BENCHMARK(BulkDecode_2byte, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<char[]> buf;
  std::vector<uint32_t> recovered;
  BENCHMARK_SUSPEND {
    recovered.resize(kNumElements);
    data = MakeFixedWidthData32(2);
    uint64_t sz;
    buf = EncodeData(data, sz);
  }
  while (iters--) {
    const char* cpos = buf.get();
    nimble::varint::bulkVarintDecode32(kNumElements, cpos, recovered.data());
    CHECK_EQ(recovered.back(), data.back());
  }
}

BENCHMARK_DRAW_LINE();

// ============================================================================
// Large-head benchmarks: first value is 5-byte, rest are 1-byte or 2-byte.
// Tests fast-path recovery after the general decoder handles the first value.
// ============================================================================

BENCHMARK(BulkDecode_largeHead_then1byte, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<char[]> buf;
  std::vector<uint32_t> recovered;
  BENCHMARK_SUSPEND {
    recovered.resize(kNumElements);
    data = MakeLargeHeadData32(1);
    uint64_t sz;
    buf = EncodeData(data, sz);
  }
  CHECK(!recovered.empty());
  CHECK(!data.empty());
  while (iters--) {
    const char* cpos = buf.get();
    nimble::varint::bulkVarintDecode32(kNumElements, cpos, recovered.data());
    CHECK_EQ(recovered.back(), data.back());
  }
}

BENCHMARK_RELATIVE(BulkDecode_largeHead_then2byte, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<char[]> buf;
  std::vector<uint32_t> recovered;
  BENCHMARK_SUSPEND {
    recovered.resize(kNumElements);
    data = MakeLargeHeadData32(2);
    uint64_t sz;
    buf = EncodeData(data, sz);
  }
  CHECK(!recovered.empty());
  CHECK(!data.empty());
  while (iters--) {
    const char* cpos = buf.get();
    nimble::varint::bulkVarintDecode32(kNumElements, cpos, recovered.data());
    CHECK_EQ(recovered.back(), data.back());
  }
}

BENCHMARK_DRAW_LINE();

// ============================================================================
// Sporadic large value benchmarks: a 5-byte varint every N elements,
// rest are 1-byte or 2-byte. Tests how well fast paths recover after
// periodic interruptions by the general decoder.
// ============================================================================

#define SPORADIC_BENCH(TAIL_BYTES, INTERVAL)                                 \
  BENCHMARK(BulkDecode_sporadic##INTERVAL##_then##TAIL_BYTES##byte, iters) { \
    std::vector<uint32_t> data;                                              \
    std::unique_ptr<char[]> buf;                                             \
    std::vector<uint32_t> recovered;                                         \
    BENCHMARK_SUSPEND {                                                      \
      recovered.resize(kNumElements);                                        \
      data = MakeSporadicLargeData32(TAIL_BYTES, INTERVAL);                  \
      uint64_t sz;                                                           \
      buf = EncodeData(data, sz);                                            \
    }                                                                        \
    CHECK(!recovered.empty());                                               \
    CHECK(!data.empty());                                                    \
    while (iters--) {                                                        \
      const char* cpos = buf.get();                                          \
      nimble::varint::bulkVarintDecode32(                                    \
          kNumElements, cpos, recovered.data());                             \
      CHECK_EQ(recovered.back(), data.back());                               \
    }                                                                        \
  }

SPORADIC_BENCH(1, 64)
SPORADIC_BENCH(1, 256)
SPORADIC_BENCH(1, 1024)
SPORADIC_BENCH(1, 4096)

BENCHMARK_DRAW_LINE();

SPORADIC_BENCH(2, 64)
SPORADIC_BENCH(2, 256)
SPORADIC_BENCH(2, 1024)
SPORADIC_BENCH(2, 4096)

BENCHMARK_DRAW_LINE();

// ============================================================================
// Batch size benchmarks: how does bulk decode scale with n?
// ============================================================================

BENCHMARK_DRAW_LINE();

#define BATCH_SIZE_BENCH(N)                                          \
  BENCHMARK(BulkDecode_batch##N, iters) {                            \
    std::vector<uint32_t> data;                                      \
    std::unique_ptr<char[]> buf;                                     \
    std::vector<uint32_t> recovered;                                 \
    BENCHMARK_SUSPEND {                                              \
      recovered.resize(N);                                           \
      data = MakeUniformData(N);                                     \
      uint64_t sz;                                                   \
      buf = EncodeData(data, sz);                                    \
    }                                                                \
    CHECK(!recovered.empty());                                       \
    while (iters--) {                                                \
      const char* cpos = buf.get();                                  \
      nimble::varint::bulkVarintDecode32(N, cpos, recovered.data()); \
      folly::doNotOptimizeAway(recovered.back());                    \
    }                                                                \
  }

BATCH_SIZE_BENCH(4)
BATCH_SIZE_BENCH(8)
BATCH_SIZE_BENCH(16)
BATCH_SIZE_BENCH(64)
BATCH_SIZE_BENCH(256)
BATCH_SIZE_BENCH(1024)
BATCH_SIZE_BENCH(4096)

int main() {
  folly::runBenchmarks();
}
