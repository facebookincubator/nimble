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

// Folly benchmarks for FixedBitWidthEncoding<T>::encode().
// Exercises the encode path with various data distributions and sizes
// to measure bit-packing throughput.
//
// Compiled with -DENABLE_HW_TIMER to activate fine-grained hardware timer
// instrumentation in the production code paths.

#include <unistd.h>

#include <cstdint>
#include <optional>
#include <random>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <glog/logging.h>

#include "dwio/nimble/benchmarks/BenchmarkSuite.h"
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/encodings/EncodingSelection.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#ifdef ENABLE_HW_TIMER
#include "velox/common/time/HardwareTimer.h"
#endif
#include "velox/common/memory/Memory.h"

FOLLY_GFLAGS_DEFINE_string(
    bm_json_output,
    "",
    "File to write JSON benchmark results to.");

namespace facebook::nimble {
namespace {

// ---------------------------------------------------------------------------
// Data generators — raw uint32_t / uint64_t vectors for encode() benchmarks.
// ---------------------------------------------------------------------------

std::vector<uint32_t> makeSmallRange32(size_t n) {
  // Values in [0, 127] -> 8 bits required (rounded to byte).
  std::mt19937 rng(55);
  std::uniform_int_distribution<uint32_t> dist(0, 127);
  std::vector<uint32_t> vals(n);
  for (auto& v : vals) {
    v = dist(rng);
  }
  return vals;
}

std::vector<uint32_t> makeMediumRange32(size_t n) {
  // Values in [0, 65535] -> 16 bits required.
  std::mt19937 rng(42);
  std::uniform_int_distribution<uint32_t> dist(0, 65535);
  std::vector<uint32_t> vals(n);
  for (auto& v : vals) {
    v = dist(rng);
  }
  return vals;
}

std::vector<uint32_t> makeFullRange32(size_t n) {
  // Values spanning full uint32_t range -> 32 bits required.
  std::mt19937 rng(99);
  std::uniform_int_distribution<uint32_t> dist(0, UINT32_MAX);
  std::vector<uint32_t> vals(n);
  for (auto& v : vals) {
    v = dist(rng);
  }
  return vals;
}

std::vector<uint64_t> makeSmallRange64(size_t n) {
  // Values in [0, 255] -> 8 bits required, stored as uint64_t.
  std::mt19937 rng(77);
  std::uniform_int_distribution<uint64_t> dist(0, 255);
  std::vector<uint64_t> vals(n);
  for (auto& v : vals) {
    v = dist(rng);
  }
  return vals;
}

std::vector<uint64_t> makeMediumRange64(size_t n) {
  // Values in [0, 2^24-1] -> 24 bits required.
  std::mt19937 rng(123);
  std::uniform_int_distribution<uint64_t> dist(0, (1ULL << 24) - 1);
  std::vector<uint64_t> vals(n);
  for (auto& v : vals) {
    v = dist(rng);
  }
  return vals;
}

#ifdef ENABLE_HW_TIMER
// NOLINTNEXTLINE(facebook-avoid-non-const-global-variables)
std::optional<facebook::velox::HardwareTimer> FixedBitWidthEncoding_encode;
std::optional<facebook::velox::HardwareTimer>
    FixedBitWidthEncoding_Immediate_Bench_encode; // NOLINT(facebook-avoid-non-const-global-variables)
std::optional<facebook::velox::HardwareTimer>
    FixedBitWidthEncoding_Stats_Bench_encode; // NOLINT(facebook-avoid-non-const-global-variables)

#endif

// ---------------------------------------------------------------------------
// Helper: encode a span of values using FixedBitWidthEncoding.
// ---------------------------------------------------------------------------

template <typename T>
std::string_view encodeFixedBitWidth(
    std::span<const T> values,
    Buffer& buffer) {
#ifdef ENABLE_HW_TIMER
  HWT_START(*FixedBitWidthEncoding_Stats_Bench_encode);
#endif
  auto stats = Statistics<T>::create(values);
  auto policy = std::make_unique<ManualEncodingSelectionPolicy<T>>(
      ManualEncodingSelectionPolicyFactory::defaultReadFactors(),
      CompressionOptions{},
      std::nullopt);

  EncodingSelection<T> selection{
      EncodingSelectionResult{
          .encodingType = EncodingType::FixedBitWidth,
      },
      std::move(stats),
      std::move(policy)};
#ifdef ENABLE_HW_TIMER
  HWT_END(*FixedBitWidthEncoding_Stats_Bench_encode);
#endif
#ifdef ENABLE_HW_TIMER
  HWT_START(*FixedBitWidthEncoding_Immediate_Bench_encode);
#endif
  auto encodedResult =
      FixedBitWidthEncoding<T>::encode(selection, values, buffer);
#ifdef ENABLE_HW_TIMER
  HWT_END(*FixedBitWidthEncoding_Immediate_Bench_encode);
#endif
  return encodedResult;
}

// ---------------------------------------------------------------------------
// Shared state.
// ---------------------------------------------------------------------------

// NOLINTNEXTLINE(facebook-avoid-non-const-global-variables)
std::shared_ptr<velox::memory::MemoryPool> pool;

void initPool() {
  pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
}

void cleanupPool() {
  pool.reset();
}

// ===========================================================================
// Benchmark functions
// ===========================================================================

// --- uint32_t, small range (8-bit), varying sizes --------------------------

void benchEncode32_SmallRange_500(unsigned iters) {
  std::vector<uint32_t> data;

  BENCHMARK_SUSPEND {
    data = makeSmallRange32(500);
  }

  for (unsigned i = 0; i < iters; ++i) {
    Buffer buffer{*pool};
#ifdef ENABLE_HW_TIMER
    HWT_START((*FixedBitWidthEncoding_encode));
#endif
    auto encoded =
        encodeFixedBitWidth<uint32_t>(std::span<const uint32_t>(data), buffer);
#ifdef ENABLE_HW_TIMER
    HWT_END((*FixedBitWidthEncoding_encode));
#endif
    folly::doNotOptimizeAway(encoded);
  }
}

void benchEncode32_SmallRange_5000(unsigned iters) {
  std::vector<uint32_t> data;

  BENCHMARK_SUSPEND {
    data = makeSmallRange32(5000);
  }

  for (unsigned i = 0; i < iters; ++i) {
    Buffer buffer{*pool};
#ifdef ENABLE_HW_TIMER
    HWT_START((*FixedBitWidthEncoding_encode));
#endif
    auto encoded =
        encodeFixedBitWidth<uint32_t>(std::span<const uint32_t>(data), buffer);
#ifdef ENABLE_HW_TIMER
    HWT_END((*FixedBitWidthEncoding_encode));
#endif
    folly::doNotOptimizeAway(encoded);
  }
}

void benchEncode32_SmallRange_50000(unsigned iters) {
  std::vector<uint32_t> data;

  BENCHMARK_SUSPEND {
    data = makeSmallRange32(50000);
  }

  for (unsigned i = 0; i < iters; ++i) {
    Buffer buffer{*pool};
#ifdef ENABLE_HW_TIMER
    HWT_START((*FixedBitWidthEncoding_encode));
#endif
    auto encoded =
        encodeFixedBitWidth<uint32_t>(std::span<const uint32_t>(data), buffer);
#ifdef ENABLE_HW_TIMER
    HWT_END((*FixedBitWidthEncoding_encode));
#endif
    folly::doNotOptimizeAway(encoded);
  }
}

// --- uint32_t, medium range (16-bit) --------------------------------------

void benchEncode32_MediumRange_5000(unsigned iters) {
  std::vector<uint32_t> data;

  BENCHMARK_SUSPEND {
    data = makeMediumRange32(5000);
  }

  for (unsigned i = 0; i < iters; ++i) {
    Buffer buffer{*pool};
#ifdef ENABLE_HW_TIMER
    HWT_START((*FixedBitWidthEncoding_encode));
#endif
    auto encoded =
        encodeFixedBitWidth<uint32_t>(std::span<const uint32_t>(data), buffer);
#ifdef ENABLE_HW_TIMER
    HWT_END((*FixedBitWidthEncoding_encode));
#endif
    folly::doNotOptimizeAway(encoded);
  }
}

// --- uint32_t, full range (32-bit) ----------------------------------------

void benchEncode32_FullRange_5000(unsigned iters) {
  std::vector<uint32_t> data;

  BENCHMARK_SUSPEND {
    data = makeFullRange32(5000);
  }

  for (unsigned i = 0; i < iters; ++i) {
    Buffer buffer{*pool};
#ifdef ENABLE_HW_TIMER
    HWT_START((*FixedBitWidthEncoding_encode));
#endif
    auto encoded =
        encodeFixedBitWidth<uint32_t>(std::span<const uint32_t>(data), buffer);
#ifdef ENABLE_HW_TIMER
    HWT_END((*FixedBitWidthEncoding_encode));
#endif
    folly::doNotOptimizeAway(encoded);
  }
}

// --- uint64_t, small range (8-bit) ----------------------------------------

void benchEncode64_SmallRange_5000(unsigned iters) {
  std::vector<uint64_t> data;

  BENCHMARK_SUSPEND {
    data = makeSmallRange64(5000);
  }

  for (unsigned i = 0; i < iters; ++i) {
    Buffer buffer{*pool};
#ifdef ENABLE_HW_TIMER
    HWT_START((*FixedBitWidthEncoding_encode));
#endif
    auto encoded =
        encodeFixedBitWidth<uint64_t>(std::span<const uint64_t>(data), buffer);
#ifdef ENABLE_HW_TIMER
    HWT_END((*FixedBitWidthEncoding_encode));
#endif
    folly::doNotOptimizeAway(encoded);
  }
}

// --- uint64_t, medium range (24-bit) --------------------------------------

void benchEncode64_MediumRange_5000(unsigned iters) {
  std::vector<uint64_t> data;

  BENCHMARK_SUSPEND {
    data = makeMediumRange64(5000);
  }

  for (unsigned i = 0; i < iters; ++i) {
    Buffer buffer{*pool};
#ifdef ENABLE_HW_TIMER
    HWT_START((*FixedBitWidthEncoding_encode));
#endif
    auto encoded =
        encodeFixedBitWidth<uint64_t>(std::span<const uint64_t>(data), buffer);
#ifdef ENABLE_HW_TIMER
    HWT_END((*FixedBitWidthEncoding_encode));
#endif
    folly::doNotOptimizeAway(encoded);
  }
}

} // namespace
} // namespace facebook::nimble

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  facebook::nimble::initPool();

  facebook::nimble::BenchmarkSuite suite;
  suite.setTitle("FixedBitWidthEncode");
  if (!FLAGS_bm_json_output.empty()) {
    suite.setJsonOutputPath(FLAGS_bm_json_output);
  }
  suite.setOnStart([](const std::string& name) {
#ifdef ENABLE_HW_TIMER
    facebook::velox::HardwareTimer::init(name);
    facebook::nimble::FixedBitWidthEncoding_encode.emplace(
        "FixedBitWidthEncoding_encode");
    facebook::nimble::FixedBitWidthEncoding_Immediate_Bench_encode.emplace(
        "FixedBitWidthEncoding_Immediate_Bench_encode");
    facebook::nimble::FixedBitWidthEncoding_Stats_Bench_encode.emplace(
        "FixedBitWidthEncoding_Stats_Bench_encode");
#endif
  });
  suite.setOnEnd(
      []([[maybe_unused]] const std::string& name,
         [[maybe_unused]] const folly::detail::BenchmarkResult& result) {
#ifdef ENABLE_HW_TIMER
        // Destroy the timer so its destructor writes to the entries map,
        // then cleanup() prints and clears the map.
        facebook::nimble::FixedBitWidthEncoding_encode.reset();
        facebook::velox::HardwareTimer::cleanup();
#endif
      });

  // uint32_t — small range (8-bit), varying sizes.
  suite.addBenchmark(
      "Encode32_SmallRange_500",
      facebook::nimble::benchEncode32_SmallRange_500);
  suite.addBenchmark(
      "Encode32_SmallRange_5000",
      facebook::nimble::benchEncode32_SmallRange_5000);
  suite.addBenchmark(
      "Encode32_SmallRange_50000",
      facebook::nimble::benchEncode32_SmallRange_50000);

  suite.addSeparator();

  // uint32_t — varying bit widths at 5000 rows.
  suite.addBenchmark(
      "Encode32_MediumRange_5000",
      facebook::nimble::benchEncode32_MediumRange_5000);
  suite.addBenchmark(
      "Encode32_FullRange_5000",
      facebook::nimble::benchEncode32_FullRange_5000);

  suite.addSeparator();

  // uint64_t benchmarks.
  suite.addBenchmark(
      "Encode64_SmallRange_5000",
      facebook::nimble::benchEncode64_SmallRange_5000);
  suite.addBenchmark(
      "Encode64_MediumRange_5000",
      facebook::nimble::benchEncode64_MediumRange_5000);

  suite.run();
  facebook::nimble::cleanupPool();

  fflush(stdout);
  fflush(stderr);
  _exit(0);
}
