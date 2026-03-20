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

#include "common/init/light.h"
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "folly/Benchmark.h"
#include "velox/common/memory/Memory.h"

#include <fmt/core.h>
#include <random>

namespace facebook::nimble {
namespace {

Vector<int32_t> generateData(
    velox::memory::MemoryPool* pool,
    uint32_t count,
    double commonFraction,
    int32_t commonValue = 42) {
  Vector<int32_t> data(pool, count);
  std::mt19937 rng(12345);
  std::uniform_real_distribution<double> dist(0.0, 1.0);
  std::uniform_int_distribution<int32_t> valueDist(1, 1000000);
  for (uint32_t i = 0; i < count; ++i) {
    if (dist(rng) < commonFraction) {
      data[i] = commonValue;
    } else {
      int32_t v;
      do {
        v = valueDist(rng);
      } while (v == commonValue);
      data[i] = v;
    }
  }
  return data;
}

struct BenchmarkState {
  std::shared_ptr<velox::memory::MemoryPool> pool;
  std::unique_ptr<Encoding> encoding;
  std::unique_ptr<Buffer> encodedBuffer;
  uint32_t rowCount;
};

BenchmarkState setupBenchmark(uint32_t count, double commonFraction) {
  auto pool = velox::memory::memoryManager()->addLeafPool("benchmark");
  auto data = generateData(pool.get(), count, commonFraction);
  Buffer buffer{*pool};
  auto encoded =
      test::Encoder<MainlyConstantEncoding<int32_t>>::encode(buffer, data);

  auto encodedBuffer = std::make_unique<Buffer>(*pool);
  char* copy = encodedBuffer->reserve(encoded.size());
  std::memcpy(copy, encoded.data(), encoded.size());
  std::string_view ownedData{copy, encoded.size()};
  auto encoding = std::make_unique<MainlyConstantEncoding<int32_t>>(
      *pool, ownedData, [](uint32_t) -> void* { return nullptr; });
  return {
      std::move(pool), std::move(encoding), std::move(encodedBuffer), count};
}

void runDecodeBenchmark(uint32_t iters, uint32_t count, double fraction) {
  auto state = setupBenchmark(count, fraction);
  std::vector<int32_t> output(state.rowCount);
  for (uint32_t i = 0; i < iters; ++i) {
    state.encoding->reset();
    state.encoding->materialize(state.rowCount, output.data());
  }
  folly::doNotOptimizeAway(output);
}

void runSkipBenchmark(uint32_t iters, uint32_t count, double fraction) {
  auto state = setupBenchmark(count, fraction);
  for (uint32_t i = 0; i < iters; ++i) {
    state.encoding->reset();
    state.encoding->skip(state.rowCount);
  }
}

// Measures peak memory used by the encoding pool during materialize/skip.
// Creates a dedicated pool so that only the encoding's allocations are tracked.
void measureMemory(
    const std::string& label,
    uint32_t count,
    double commonFraction,
    bool doMaterialize) {
  // Use a separate pool for encoding so we can isolate its memory usage.
  auto rootPool = velox::memory::memoryManager()->addRootPool("mem_measure");
  auto setupPool = rootPool->addLeafChild("setup");
  auto encodingPool = rootPool->addLeafChild("encoding");

  auto data = generateData(setupPool.get(), count, commonFraction);
  Buffer buffer{*setupPool};
  auto encoded =
      test::Encoder<MainlyConstantEncoding<int32_t>>::encode(buffer, data);

  Buffer encodedBuffer{*setupPool};
  char* copy = encodedBuffer.reserve(encoded.size());
  std::memcpy(copy, encoded.data(), encoded.size());
  std::string_view ownedData{copy, encoded.size()};

  auto encoding = std::make_unique<MainlyConstantEncoding<int32_t>>(
      *encodingPool, ownedData, [](uint32_t) -> void* { return nullptr; });

  const auto memBefore = encodingPool->usedBytes();

  if (doMaterialize) {
    std::vector<int32_t> output(count);
    encoding->materialize(count, output.data());
  } else {
    encoding->skip(count);
  }

  const auto memAfter = encodingPool->usedBytes();
  const auto peakMem = encodingPool->peakBytes();

  fmt::print(
      "  {:40s}  used: {:>8d} -> {:>8d} bytes  delta: {:>8d} bytes  peak: {:>8d} bytes\n",
      label,
      memBefore,
      memAfter,
      memAfter - memBefore,
      peakMem);
}

void runMemoryReport(uint32_t count) {
  fmt::print("\nMemory usage report ({} rows):\n", count);
  fmt::print("{:-<100s}\n", "");

  fmt::print("  Materialize:\n");
  measureMemory("materialize 100% common", count, 1.0, true);
  measureMemory("materialize  95% common", count, 0.95, true);
  measureMemory("materialize  75% common", count, 0.75, true);
  measureMemory("materialize  50% common", count, 0.50, true);

  fmt::print("  Skip:\n");
  measureMemory("skip 100% common", count, 1.0, false);
  measureMemory("skip  95% common", count, 0.95, false);
  measureMemory("skip  75% common", count, 0.75, false);
  measureMemory("skip  50% common", count, 0.50, false);

  fmt::print("{:-<100s}\n\n", "");
}

} // namespace
} // namespace facebook::nimble

// Decode benchmarks (materialize)

BENCHMARK(Decode_HighConstancy_95pct_1M, iters) {
  facebook::nimble::runDecodeBenchmark(iters, 1000000, 0.95);
}

BENCHMARK(Decode_ModerateConstancy_75pct_1M, iters) {
  facebook::nimble::runDecodeBenchmark(iters, 1000000, 0.75);
}

BENCHMARK(Decode_LowConstancy_50pct_1M, iters) {
  facebook::nimble::runDecodeBenchmark(iters, 1000000, 0.50);
}

BENCHMARK(Decode_AllCommon_100pct_1M, iters) {
  facebook::nimble::runDecodeBenchmark(iters, 1000000, 1.0);
}

BENCHMARK_DRAW_LINE();

// Skip benchmarks

BENCHMARK(Skip_HighConstancy_95pct_1M, iters) {
  facebook::nimble::runSkipBenchmark(iters, 1000000, 0.95);
}

BENCHMARK(Skip_ModerateConstancy_75pct_1M, iters) {
  facebook::nimble::runSkipBenchmark(iters, 1000000, 0.75);
}

BENCHMARK(Skip_LowConstancy_50pct_1M, iters) {
  facebook::nimble::runSkipBenchmark(iters, 1000000, 0.50);
}

BENCHMARK(Skip_AllCommon_100pct_1M, iters) {
  facebook::nimble::runSkipBenchmark(iters, 1000000, 1.0);
}

int main(int argc, char** argv) {
  facebook::init::initFacebookLight(&argc, &argv);
  facebook::velox::memory::MemoryManager::initialize({});

  // Print memory usage report before running perf benchmarks.
  facebook::nimble::runMemoryReport(1000000);

  folly::runBenchmarks();
  return 0;
}
