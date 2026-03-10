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
#include <fmt/core.h>
#include <fstream>

#include "common/init/light.h"
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/DeltaEncoding.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "folly/Benchmark.h"
#include "folly/json/json.h"
#include "velox/common/memory/Memory.h"

using namespace facebook;

DEFINE_string(
    save_baseline,
    "",
    "Path to save benchmark results as JSON baseline for future comparison.");
DEFINE_string(
    compare_baseline,
    "",
    "Path to a previously saved JSON baseline to compare against.");
DEFINE_bool(skip_storage, false, "Skip the storage comparison section.");
DEFINE_bool(skip_perf, false, "Skip the performance benchmark section.");

constexpr uint32_t kSawtoothPeriod = 1000;

// --- Templatized data generators ---

/// Generates monotonically increasing values with a large base offset.
/// Best case for delta encoding: all deltas are 1 (small), but raw values are
/// large, so the encoding saves significant space.
template <typename T>
nimble::Vector<T> generateMonotonic(
    velox::memory::MemoryPool& pool,
    uint32_t numElements) {
  nimble::Vector<T> values{&pool};
  values.resize(numElements);
  const T base = static_cast<T>(1'000'000'000);
  for (uint32_t i = 0; i < numElements; ++i) {
    values[i] = base + static_cast<T>(i);
  }
  return values;
}

/// Generates a sawtooth pattern with a large base:
/// [base, base+1, ..., base+999, base, base+1, ..., base+999, ...].
/// Realistic mix of deltas and restatements at each period boundary.
template <typename T>
nimble::Vector<T> generateSawtooth(
    velox::memory::MemoryPool& pool,
    uint32_t numElements) {
  nimble::Vector<T> values{&pool};
  values.resize(numElements);
  const T base = static_cast<T>(1'000'000'000);
  for (uint32_t i = 0; i < numElements; ++i) {
    values[i] = base + static_cast<T>(i % kSawtoothPeriod);
  }
  return values;
}

/// Generates timestamps-like data: large base with small random increments.
/// Simulates real-world sorted timestamp columns.
template <typename T>
nimble::Vector<T> generateTimestamps(
    velox::memory::MemoryPool& pool,
    uint32_t numElements) {
  nimble::Vector<T> values{&pool};
  values.resize(numElements);
  T current;
  if constexpr (sizeof(T) >= 8) {
    current = static_cast<T>(1'700'000'000'000LL); // ~epoch millis
  } else {
    current = static_cast<T>(1'000'000'000); // ~epoch seconds
  }
  for (uint32_t i = 0; i < numElements; ++i) {
    values[i] = current;
    current += static_cast<T>(1 + (i * 7 + 13) % 100);
  }
  return values;
}

// --- Templatized benchmark helpers ---

template <typename T>
using GeneratorFn = nimble::Vector<T> (*)(velox::memory::MemoryPool&, uint32_t);

template <typename T>
void benchEncode(
    unsigned int n,
    uint32_t numElements,
    GeneratorFn<T> generator) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Vector<T> values{pool.get()};

  BENCHMARK_SUSPEND {
    values = generator(*pool, numElements);
  }

  for (unsigned int i = 0; i < n; ++i) {
    nimble::Buffer buffer{*pool};
    nimble::test::Encoder<nimble::DeltaEncoding<T>>::encode(buffer, values);
  }
}

// --- Benchmark registration macros ---
// These macros generate thin wrapper functions that forward to the templatized
// benchmark helpers, then register them with Folly's benchmark framework at
// three data sizes (100K, 1M, 10M).

// clang-format off
#define ENCODE_BENCH(Pattern, Type, TypeName)                                  \
  void encode##Pattern##TypeName(unsigned int n, uint32_t numElements) {       \
    benchEncode<Type>(n, numElements, generate##Pattern<Type>);                \
  }                                                                            \
  BENCHMARK_NAMED_PARAM(encode##Pattern##TypeName, 100K, 100'000)              \
  BENCHMARK_NAMED_PARAM(encode##Pattern##TypeName, 1M, 1'000'000)             \
  BENCHMARK_NAMED_PARAM(encode##Pattern##TypeName, 10M, 10'000'000)

#define ENCODE_ALL_PATTERNS(Type, TypeName)                                    \
  ENCODE_BENCH(Monotonic, Type, TypeName)                                      \
  ENCODE_BENCH(Sawtooth, Type, TypeName)                                       \
  ENCODE_BENCH(Timestamps, Type, TypeName)
// clang-format on

// --- Encode benchmarks ---

ENCODE_ALL_PATTERNS(int32_t, Int32)
ENCODE_ALL_PATTERNS(int64_t, Int64)
ENCODE_ALL_PATTERNS(uint32_t, UInt32)
ENCODE_ALL_PATTERNS(uint64_t, UInt64)


// --- Storage comparison ---

/// Encodes values using the specified encoding type with production-like
/// encoding selection (ManualEncodingSelectionPolicy), so child encodings
/// get optimal selection (FixedBitWidth, Varint, SparseBool, etc.).
/// No compression is applied, isolating encoding-level storage savings.
template <typename T>
std::string_view encodeWithPolicy(
    nimble::EncodingType topLevelEncoding,
    std::span<const T> values,
    nimble::Buffer& buffer) {
  nimble::CompressionOptions compressionOptions;
  // Disable compression to measure pure encoding savings.
  compressionOptions.compressionAcceptRatio = 0.0f;

  // Build read factors with only the requested top-level encoding having the
  // lowest cost, so ManualEncodingSelectionPolicy picks it.
  auto defaultFactors =
      nimble::ManualEncodingSelectionPolicyFactory::defaultReadFactors();
  std::vector<std::pair<nimble::EncodingType, float>> readFactors;
  for (const auto& [type, factor] : defaultFactors) {
    if (type == topLevelEncoding) {
      // Give the target encoding a very low cost so it wins.
      readFactors.emplace_back(type, 0.001f);
    } else {
      readFactors.emplace_back(type, factor);
    }
  }

  auto policy = std::make_unique<nimble::ManualEncodingSelectionPolicy<T>>(
      readFactors, compressionOptions, std::nullopt);

  return nimble::EncodingFactory::encode<T>(
      std::move(policy), values, buffer, {});
}

template <typename T>
void printStorageSavingsForType(const char* typeName) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();

  struct Distribution {
    const char* name;
    GeneratorFn<T> generate;
  };

  Distribution distributions[] = {
      {"Monotonic", generateMonotonic<T>},
      {"Sawtooth", generateSawtooth<T>},
      {"Timestamps", generateTimestamps<T>},
  };

  constexpr uint32_t sizes[] = {100'000, 1'000'000, 10'000'000};
  constexpr const char* sizeLabels[] = {"100K", "1M", "10M"};

  for (uint32_t si = 0; si < std::size(sizes); ++si) {
    const uint32_t numElements = sizes[si];
    const size_t rawSize = numElements * sizeof(T);

    fmt::print(
        "\nStorage comparison ({} {} values, no compression):\n",
        sizeLabels[si],
        typeName);
    fmt::print(
        "{:<14} {:>12} {:>12} {:>12} {:>14} {:>14}\n",
        "Distribution",
        "Raw (bytes)",
        "Trivial",
        "Delta",
        "vs Raw",
        "vs Trivial");
    fmt::print("{:-<82}\n", "");

    for (const auto& dist : distributions) {
      auto values = dist.generate(*pool, numElements);
      auto valuesSpan = std::span<const T>(values.data(), values.size());

      nimble::Buffer trivialBuffer{*pool};
      auto trivialEncoded = encodeWithPolicy<T>(
          nimble::EncodingType::Trivial, valuesSpan, trivialBuffer);

      nimble::Buffer deltaBuffer{*pool};
      auto deltaEncoded = encodeWithPolicy<T>(
          nimble::EncodingType::Delta, valuesSpan, deltaBuffer);

      double vsRaw = 100.0 *
          (1.0 -
           static_cast<double>(deltaEncoded.size()) /
               static_cast<double>(rawSize));
      double vsTrivial = 100.0 *
          (1.0 -
           static_cast<double>(deltaEncoded.size()) /
               static_cast<double>(trivialEncoded.size()));

      fmt::print(
          "{:<14} {:>12} {:>12} {:>12} {:>13.1f}% {:>13.1f}%\n",
          dist.name,
          rawSize,
          trivialEncoded.size(),
          deltaEncoded.size(),
          vsRaw,
          vsTrivial);
    }
  }
}

void printStorageSavings() {
  printStorageSavingsForType<int32_t>("int32_t");
  printStorageSavingsForType<int64_t>("int64_t");
  printStorageSavingsForType<uint32_t>("uint32_t");
  printStorageSavingsForType<uint64_t>("uint64_t");
  fmt::print("\n");
}

// --- Baseline save / compare ---

/// Saves benchmark results to a JSON file for future comparison.
void saveBaseline(
    const std::vector<folly::detail::BenchmarkResult>& results,
    const std::string& path) {
  folly::dynamic d = folly::dynamic::array;
  folly::benchmarkResultsToDynamic(results, d);
  std::ofstream out(path);
  if (!out) {
    fmt::print(stderr, "Error: cannot open '{}' for writing.\n", path);
    return;
  }
  out << folly::toPrettyJson(d) << "\n";
  fmt::print(stderr, "Baseline saved to: {}\n", path);
}

/// Loads a previously saved baseline from a JSON file.
std::vector<folly::detail::BenchmarkResult> loadBaseline(
    const std::string& path) {
  std::ifstream in(path);
  if (!in) {
    fmt::print(stderr, "Error: cannot open baseline '{}' for reading.\n", path);
    return {};
  }
  std::string contents(
      (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  auto d = folly::parseJson(contents);
  std::vector<folly::detail::BenchmarkResult> results;
  folly::benchmarkResultsFromDynamic(d, results);
  return results;
}

/// Prints a side-by-side comparison of baseline vs current benchmark results.
/// Shows time/iter for both, the speedup ratio, and a visual indicator.
void printComparison(
    const std::vector<folly::detail::BenchmarkResult>& baseline,
    const std::vector<folly::detail::BenchmarkResult>& current) {
  // Build a map from benchmark name to baseline time.
  std::unordered_map<std::string, double> baselineMap;
  for (const auto& r : baseline) {
    if (!r.name.empty() && r.name[0] != '-') {
      baselineMap[r.name] = r.timeInNs;
    }
  }

  fmt::print("\n{:=<106}\n", "");
  fmt::print(
      "{:<50} {:>14} {:>14} {:>10} {:>10}\n",
      "Benchmark",
      "Baseline",
      "Current",
      "Speedup",
      "Change");
  fmt::print("{:=<106}\n", "");

  for (const auto& r : current) {
    // Skip separator entries (name starts with '-').
    if (r.name.empty() || r.name[0] == '-') {
      continue;
    }

    auto it = baselineMap.find(r.name);
    if (it == baselineMap.end()) {
      fmt::print(
          "{:<50} {:>14} {:>12.2f}ns {:>10} {:>10}\n",
          r.name,
          "(new)",
          r.timeInNs,
          "---",
          "---");
      continue;
    }

    double baseNs = it->second;
    double curNs = r.timeInNs;
    double speedup = baseNs / curNs;
    double pctChange = (baseNs - curNs) / baseNs * 100.0;

    const char* indicator = "";
    if (speedup >= 1.1) {
      indicator = " +++";
    } else if (speedup >= 1.02) {
      indicator = " +";
    } else if (speedup <= 0.9) {
      indicator = " ---";
    } else if (speedup <= 0.98) {
      indicator = " -";
    }

    fmt::print(
        "{:<50} {:>12.2f}ns {:>12.2f}ns {:>7.2f}x {:>+8.1f}%{}\n",
        r.name,
        baseNs,
        curNs,
        speedup,
        pctChange,
        indicator);
  }

  // Report any benchmarks that were in baseline but not in current run.
  for (const auto& [name, timeNs] : baselineMap) {
    bool found = false;
    for (const auto& r : current) {
      if (r.name == name) {
        found = true;
        break;
      }
    }
    if (!found) {
      fmt::print(
          "{:<50} {:>12.2f}ns {:>14} {:>10} {:>10}\n",
          name,
          timeNs,
          "(removed)",
          "---",
          "---");
    }
  }

  fmt::print("{:=<106}\n\n", "");
}

int main(int argc, char** argv) {
  facebook::init::initFacebookLight(&argc, &argv);

  if (!FLAGS_skip_storage) {
    printStorageSavings();
  }

  if (!FLAGS_skip_perf) {
    // Run benchmarks and collect structured results.
    auto results = folly::detail::runBenchmarksWithResults();

    // Print the standard Folly benchmark table.
    fmt::print("{}", folly::detail::benchmarkResultsToString(results));

    // Save results as baseline if requested.
    if (!FLAGS_save_baseline.empty()) {
      saveBaseline(results, FLAGS_save_baseline);
    }

    // Compare against a previously saved baseline if requested.
    if (!FLAGS_compare_baseline.empty()) {
      auto baseline = loadBaseline(FLAGS_compare_baseline);
      if (!baseline.empty()) {
        printComparison(baseline, results);
      }
    }
  }

  return 0;
}
