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

// Micro-benchmarks for FixedBitWidthEncoding decode paths.
// Measures materialize() and bulkGet64WithBaseline() throughput.

#include <memory>
#include <vector>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "folly/Benchmark.h"
#include "folly/Random.h"
#include "velox/common/memory/Memory.h"

using namespace facebook::nimble;

namespace {

constexpr int kNumElements = 100000;

std::shared_ptr<facebook::velox::memory::MemoryPool> pool;

/// Creates a Vector of random values fitting in the given bit width.
template <typename T>
Vector<T> makeData(int bitWidth, int n = kNumElements) {
  Vector<T> data{pool.get()};
  data.resize(n);
  using U = typename std::make_unsigned<T>::type;
  U maxVal = bitWidth >= static_cast<int>(sizeof(T) * 8)
      ? static_cast<U>(~U{0})
      : (static_cast<U>(1) << bitWidth) - 1;
  for (int i = 0; i < n; ++i) {
    if constexpr (sizeof(T) <= 4) {
      data[i] = static_cast<T>(folly::Random::secureRand32() % (maxVal + 1));
    } else {
      data[i] = static_cast<T>(folly::Random::secureRand64() % (maxVal + 1));
    }
  }
  return data;
}

/// Encodes data and returns (encoded string, encoding).
template <typename EncodingType>
std::pair<std::string, std::unique_ptr<Encoding>> encodeAndCreate(
    Buffer& buffer,
    const Vector<typename EncodingType::cppDataType>& data) {
  auto encoded = test::Encoder<EncodingType>::encode(buffer, data);
  std::string encodedStr{encoded.data(), encoded.size()};
  auto encoding = EncodingFactory{}.create(
      *pool, encodedStr, [](uint32_t) -> void* { return nullptr; });
  return {std::move(encodedStr), std::move(encoding)};
}

} // namespace

// ============================================================================
// FixedBitWidthEncoding materialize() — 32-bit
// ============================================================================

BENCHMARK(FBW_Materialize_uint32_8bit, iters) {
  std::string encoded;
  std::vector<uint32_t> output(kNumElements);
  BENCHMARK_SUSPEND {
    Buffer buffer{*pool};
    auto data = makeData<uint32_t>(8);
    encoded =
        test::Encoder<FixedBitWidthEncoding<uint32_t>>::encode(buffer, data);
    encoded = std::string{encoded.data(), encoded.size()};
  }
  while (iters--) {
    auto encoding = EncodingFactory{}.create(
        *pool, encoded, [](uint32_t) -> void* { return nullptr; });
    encoding->materialize(kNumElements, output.data());
  }
}

BENCHMARK(FBW_Materialize_uint32_16bit, iters) {
  std::string encoded;
  std::vector<uint32_t> output(kNumElements);
  BENCHMARK_SUSPEND {
    Buffer buffer{*pool};
    auto data = makeData<uint32_t>(16);
    auto sv =
        test::Encoder<FixedBitWidthEncoding<uint32_t>>::encode(buffer, data);
    encoded = std::string{sv.data(), sv.size()};
  }
  while (iters--) {
    auto encoding = EncodingFactory{}.create(
        *pool, encoded, [](uint32_t) -> void* { return nullptr; });
    encoding->materialize(kNumElements, output.data());
  }
}

BENCHMARK_DRAW_LINE();

// ============================================================================
// FixedBitWidthEncoding materialize() — 64-bit
// These benefit from the new bulkGet64WithBaseline optimization.
// ============================================================================

BENCHMARK(FBW_Materialize_uint64_8bit, iters) {
  std::string encoded;
  std::vector<uint64_t> output(kNumElements);
  BENCHMARK_SUSPEND {
    Buffer buffer{*pool};
    auto data = makeData<uint64_t>(8);
    auto sv =
        test::Encoder<FixedBitWidthEncoding<uint64_t>>::encode(buffer, data);
    encoded = std::string{sv.data(), sv.size()};
  }
  while (iters--) {
    auto encoding = EncodingFactory{}.create(
        *pool, encoded, [](uint32_t) -> void* { return nullptr; });
    encoding->materialize(kNumElements, output.data());
  }
}

BENCHMARK(FBW_Materialize_uint64_16bit, iters) {
  std::string encoded;
  std::vector<uint64_t> output(kNumElements);
  BENCHMARK_SUSPEND {
    Buffer buffer{*pool};
    auto data = makeData<uint64_t>(16);
    auto sv =
        test::Encoder<FixedBitWidthEncoding<uint64_t>>::encode(buffer, data);
    encoded = std::string{sv.data(), sv.size()};
  }
  while (iters--) {
    auto encoding = EncodingFactory{}.create(
        *pool, encoded, [](uint32_t) -> void* { return nullptr; });
    encoding->materialize(kNumElements, output.data());
  }
}

BENCHMARK(FBW_Materialize_uint64_32bit, iters) {
  std::string encoded;
  std::vector<uint64_t> output(kNumElements);
  BENCHMARK_SUSPEND {
    Buffer buffer{*pool};
    auto data = makeData<uint64_t>(32);
    auto sv =
        test::Encoder<FixedBitWidthEncoding<uint64_t>>::encode(buffer, data);
    encoded = std::string{sv.data(), sv.size()};
  }
  while (iters--) {
    auto encoding = EncodingFactory{}.create(
        *pool, encoded, [](uint32_t) -> void* { return nullptr; });
    encoding->materialize(kNumElements, output.data());
  }
}

BENCHMARK(FBW_Materialize_uint64_48bit, iters) {
  std::string encoded;
  std::vector<uint64_t> output(kNumElements);
  BENCHMARK_SUSPEND {
    Buffer buffer{*pool};
    auto data = makeData<uint64_t>(48);
    auto sv =
        test::Encoder<FixedBitWidthEncoding<uint64_t>>::encode(buffer, data);
    encoded = std::string{sv.data(), sv.size()};
  }
  while (iters--) {
    auto encoding = EncodingFactory{}.create(
        *pool, encoded, [](uint32_t) -> void* { return nullptr; });
    encoding->materialize(kNumElements, output.data());
  }
}

BENCHMARK_DRAW_LINE();

// ============================================================================
// FixedBitArray: bulkGet64WithBaseline (NEW) vs per-element get() (OLD)
// The OLD materialize() code path used get() in a loop for 64-bit types
// with bitWidth > 32. The NEW path uses bulkGet64WithBaseline with
// branchless byte-aligned loads.
// Each pair shows baseline (per-element) then relative (bulk) speedup.
// ============================================================================
BENCHMARK(PerElementGet64_16bit, iters) {
  FixedBitArray fba;
  std::vector<char> buffer;
  std::vector<uint64_t> output(kNumElements);
  BENCHMARK_SUSPEND {
    int bw = 16;
    buffer.resize(FixedBitArray::bufferSize(kNumElements, bw), 0);
    fba = FixedBitArray{buffer.data(), bw};
    for (int i = 0; i < kNumElements; ++i) {
      fba.set(i, folly::Random::secureRand64() % (1ULL << bw));
    }
  }
  while (iters--) {
    for (int i = 0; i < kNumElements; ++i) {
      output[i] = fba.get(i);
    }
    folly::doNotOptimizeAway(output.back());
  }
}

BENCHMARK_RELATIVE(BulkGet64_vs_PerElem_16bit, iters) {
  FixedBitArray fba;
  std::vector<char> buffer;
  std::vector<uint64_t> output(kNumElements);
  BENCHMARK_SUSPEND {
    int bw = 16;
    buffer.resize(FixedBitArray::bufferSize(kNumElements, bw), 0);
    fba = FixedBitArray{buffer.data(), bw};
    for (int i = 0; i < kNumElements; ++i) {
      fba.set(i, folly::Random::secureRand64() % (1ULL << bw));
    }
  }
  while (iters--) {
    fba.bulkGet64WithBaseline(0, kNumElements, output.data(), 0);
    folly::doNotOptimizeAway(output.back());
  }
}

BENCHMARK_DRAW_LINE();

BENCHMARK(PerElementGet64_40bit, iters) {
  FixedBitArray fba;
  std::vector<char> buffer;
  std::vector<uint64_t> output(kNumElements);
  BENCHMARK_SUSPEND {
    int bw = 40;
    buffer.resize(FixedBitArray::bufferSize(kNumElements, bw), 0);
    fba = FixedBitArray{buffer.data(), bw};
    for (int i = 0; i < kNumElements; ++i) {
      fba.set(i, folly::Random::secureRand64() % (1ULL << bw));
    }
  }
  while (iters--) {
    for (int i = 0; i < kNumElements; ++i) {
      output[i] = fba.get(i);
    }
    folly::doNotOptimizeAway(output.back());
  }
}

BENCHMARK_RELATIVE(BulkGet64_vs_PerElem_40bit, iters) {
  FixedBitArray fba;
  std::vector<char> buffer;
  std::vector<uint64_t> output(kNumElements);
  BENCHMARK_SUSPEND {
    int bw = 40;
    buffer.resize(FixedBitArray::bufferSize(kNumElements, bw), 0);
    fba = FixedBitArray{buffer.data(), bw};
    for (int i = 0; i < kNumElements; ++i) {
      fba.set(i, folly::Random::secureRand64() % (1ULL << bw));
    }
  }
  while (iters--) {
    fba.bulkGet64WithBaseline(0, kNumElements, output.data(), 0);
    folly::doNotOptimizeAway(output.back());
  }
}

BENCHMARK(PerElementGet64_56bit, iters) {
  FixedBitArray fba;
  std::vector<char> buffer;
  std::vector<uint64_t> output(kNumElements);
  BENCHMARK_SUSPEND {
    int bw = 56;
    buffer.resize(FixedBitArray::bufferSize(kNumElements, bw), 0);
    fba = FixedBitArray{buffer.data(), bw};
    for (int i = 0; i < kNumElements; ++i) {
      fba.set(i, folly::Random::secureRand64() % (1ULL << bw));
    }
  }
  while (iters--) {
    for (int i = 0; i < kNumElements; ++i) {
      output[i] = fba.get(i);
    }
    folly::doNotOptimizeAway(output.back());
  }
}

BENCHMARK_RELATIVE(BulkGet64_vs_PerElem_56bit, iters) {
  FixedBitArray fba;
  std::vector<char> buffer;
  std::vector<uint64_t> output(kNumElements);
  BENCHMARK_SUSPEND {
    int bw = 56;
    buffer.resize(FixedBitArray::bufferSize(kNumElements, bw), 0);
    fba = FixedBitArray{buffer.data(), bw};
    for (int i = 0; i < kNumElements; ++i) {
      fba.set(i, folly::Random::secureRand64() % (1ULL << bw));
    }
  }
  while (iters--) {
    fba.bulkGet64WithBaseline(0, kNumElements, output.data(), 0);
    folly::doNotOptimizeAway(output.back());
  }
}

BENCHMARK(PerElementGet64_57bit, iters) {
  FixedBitArray fba;
  std::vector<char> buffer;
  std::vector<uint64_t> output(kNumElements);
  BENCHMARK_SUSPEND {
    int bw = 57;
    buffer.resize(FixedBitArray::bufferSize(kNumElements, bw), 0);
    fba = FixedBitArray{buffer.data(), bw};
    for (int i = 0; i < kNumElements; ++i) {
      fba.set(i, folly::Random::secureRand64() % (1ULL << bw));
    }
  }
  while (iters--) {
    for (int i = 0; i < kNumElements; ++i) {
      output[i] = fba.get(i);
    }
    folly::doNotOptimizeAway(output.back());
  }
}

BENCHMARK_RELATIVE(BulkGet64_vs_PerElem_57bit, iters) {
  FixedBitArray fba;
  std::vector<char> buffer;
  std::vector<uint64_t> output(kNumElements);
  BENCHMARK_SUSPEND {
    int bw = 57;
    buffer.resize(FixedBitArray::bufferSize(kNumElements, bw), 0);
    fba = FixedBitArray{buffer.data(), bw};
    for (int i = 0; i < kNumElements; ++i) {
      fba.set(i, folly::Random::secureRand64() % (1ULL << bw));
    }
  }
  while (iters--) {
    fba.bulkGet64WithBaseline(0, kNumElements, output.data(), 0);
    folly::doNotOptimizeAway(output.back());
  }
}

int main() {
  facebook::velox::memory::MemoryManager::initialize({});
  pool = facebook::velox::memory::memoryManager()->addLeafPool("benchmark");
  folly::runBenchmarks();
}
