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

/// Benchmarks the dictionary filter cache optimization for Nimble string
/// column reads. Varies filter type (BytesValues, BytesRange), selectivity
/// (1%, 20%, 80%), and dictionary cardinality (5, 100, 1K, 10K).

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "dwio/nimble/common/tests/NimbleFileWriter.h"
#include "dwio/nimble/velox/selective/SelectiveNimbleReader.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::nimble {
namespace {

using namespace facebook::velox;

constexpr int kTotalRows = 1'000'000;
constexpr int kBatchSize = 10'000;

struct BenchmarkState : public velox::test::VectorTestBase {
  BenchmarkState() {
    registerSelectiveNimbleReaderFactory();
  }

  ~BenchmarkState() {
    unregisterSelectiveNimbleReaderFactory();
  }

  std::string writeFile(int cardinality) {
    auto alphabet = generateAlphabet(cardinality);
    auto c0 = makeFlatVector<std::string>(
        kTotalRows, [&](auto i) { return alphabet[i % cardinality]; });
    return test::createNimbleFile(*pool()->parent(), makeRowVector({c0}));
  }

  void readNoFilter(const std::string& file) {
    auto scanSpec = makeScanSpec();
    auto readers = createReaders(file, scanSpec);
    drainReader(*readers.rowReader);
  }

  void readBytesValues(const std::string& file, int cardinality, int percent) {
    auto scanSpec = makeScanSpec();
    auto alphabet = generateAlphabet(cardinality);
    int acceptCount = std::max(1, cardinality * percent / 100);
    std::vector<std::string> accepted(
        alphabet.begin(), alphabet.begin() + acceptCount);
    scanSpec->childByName("c0")->setFilter(
        std::make_unique<common::BytesValues>(accepted, false));
    auto readers = createReaders(file, scanSpec);
    drainReader(*readers.rowReader);
  }

  void readBytesRange(
      const std::string& file,
      const std::string& lower,
      const std::string& upper) {
    auto scanSpec = makeScanSpec();
    scanSpec->childByName("c0")->setFilter(
        std::make_unique<common::BytesRange>(
            lower, false, false, upper, false, false, false));
    auto readers = createReaders(file, scanSpec);
    drainReader(*readers.rowReader);
  }

 private:
  struct Readers {
    std::unique_ptr<dwio::common::Reader> reader;
    std::unique_ptr<dwio::common::RowReader> rowReader;
  };

  std::shared_ptr<common::ScanSpec> makeScanSpec() {
    auto type = ROW({"c0"}, {VARCHAR()});
    auto scanSpec = std::make_shared<common::ScanSpec>("root");
    scanSpec->addAllChildFields(*type);
    return scanSpec;
  }

  Readers createReaders(
      const std::string& file,
      const std::shared_ptr<common::ScanSpec>& scanSpec) {
    auto type = ROW({"c0"}, {VARCHAR()});
    auto readFile = std::make_shared<InMemoryReadFile>(file);
    auto factory =
        dwio::common::getReaderFactory(dwio::common::FileFormat::NIMBLE);
    dwio::common::ReaderOptions options(pool());
    options.setScanSpec(scanSpec);
    options.setDataIoStats(ioStats_);
    options.setMetadataIoStats(ioStats_);

    Readers readers;
    readers.reader = factory->createReader(
        std::make_unique<dwio::common::BufferedInput>(readFile, *pool()),
        options);

    dwio::common::RowReaderOptions rowOptions;
    rowOptions.setScanSpec(scanSpec);
    rowOptions.setRequestedType(type);
    // Enable dictionary read path (nimble.string-decoder-zero-copy and
    // nimble.preserve-dictionary-encoding in connector config).
    rowOptions.setStringDecoderZeroCopy(true);
    rowOptions.setNimblePreserveDictionaryEncoding(true);
    readers.rowReader = readers.reader->createRowReader(rowOptions);
    return readers;
  }

  void drainReader(dwio::common::RowReader& rowReader) {
    auto type = ROW({"c0"}, {VARCHAR()});
    auto result = BaseVector::create(type, 0, pool());
    while (rowReader.next(kBatchSize, result) > 0) {
      folly::doNotOptimizeAway(result);
    }
  }

  std::shared_ptr<velox::io::IoStatistics> ioStats_ =
      std::make_shared<velox::io::IoStatistics>();

  std::vector<std::string> generateAlphabet(int cardinality) {
    std::vector<std::string> alphabet;
    alphabet.reserve(cardinality);
    for (int i = 0; i < cardinality; ++i) {
      alphabet.push_back("val_" + std::to_string(i));
    }
    return alphabet;
  }
};

BenchmarkState& state() {
  static BenchmarkState s;
  return s;
}

std::string& fileCard5() {
  static auto f = state().writeFile(5);
  return f;
}

std::string& fileCard100() {
  static auto f = state().writeFile(100);
  return f;
}

std::string& fileCard1000() {
  static auto f = state().writeFile(1000);
  return f;
}

std::string& fileCard10000() {
  static auto f = state().writeFile(10000);
  return f;
}

// ============================================================
// Cardinality 5
// ============================================================

BENCHMARK(Card5_NoFilter, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readNoFilter(fileCard5());
  }
}

BENCHMARK_RELATIVE(Card5_BytesValues_Select20pct, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesValues(fileCard5(), 5, 20);
  }
}

BENCHMARK_RELATIVE(Card5_BytesValues_Select80pct, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesValues(fileCard5(), 5, 80);
  }
}

BENCHMARK_RELATIVE(Card5_BytesRange, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesRange(fileCard5(), "val_0", "val_1");
  }
}

BENCHMARK_DRAW_LINE();

// ============================================================
// Cardinality 100
// ============================================================

BENCHMARK(Card100_NoFilter, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readNoFilter(fileCard100());
  }
}

BENCHMARK_RELATIVE(Card100_BytesValues_Select1pct, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesValues(fileCard100(), 100, 1);
  }
}

BENCHMARK_RELATIVE(Card100_BytesValues_Select20pct, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesValues(fileCard100(), 100, 20);
  }
}

BENCHMARK_RELATIVE(Card100_BytesValues_Select80pct, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesValues(fileCard100(), 100, 80);
  }
}

BENCHMARK_RELATIVE(Card100_BytesRange_Narrow, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesRange(fileCard100(), "val_40", "val_49");
  }
}

BENCHMARK_RELATIVE(Card100_BytesRange_Wide, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesRange(fileCard100(), "val_0", "val_79");
  }
}

BENCHMARK_DRAW_LINE();

// ============================================================
// Cardinality 1000
// ============================================================

BENCHMARK(Card1K_NoFilter, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readNoFilter(fileCard1000());
  }
}

BENCHMARK_RELATIVE(Card1K_BytesValues_Select1pct, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesValues(fileCard1000(), 1000, 1);
  }
}

BENCHMARK_RELATIVE(Card1K_BytesValues_Select20pct, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesValues(fileCard1000(), 1000, 20);
  }
}

BENCHMARK_RELATIVE(Card1K_BytesValues_Select80pct, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesValues(fileCard1000(), 1000, 80);
  }
}

BENCHMARK_RELATIVE(Card1K_BytesRange_Narrow, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesRange(fileCard1000(), "val_400", "val_499");
  }
}

BENCHMARK_RELATIVE(Card1K_BytesRange_Wide, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesRange(fileCard1000(), "val_0", "val_799");
  }
}

BENCHMARK_DRAW_LINE();

// ============================================================
// Cardinality 10000
// ============================================================

BENCHMARK(Card10K_NoFilter, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readNoFilter(fileCard10000());
  }
}

BENCHMARK_RELATIVE(Card10K_BytesValues_Select1pct, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesValues(fileCard10000(), 10000, 1);
  }
}

BENCHMARK_RELATIVE(Card10K_BytesValues_Select20pct, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesValues(fileCard10000(), 10000, 20);
  }
}

BENCHMARK_RELATIVE(Card10K_BytesValues_Select80pct, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesValues(fileCard10000(), 10000, 80);
  }
}

BENCHMARK_RELATIVE(Card10K_BytesRange_Narrow, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesRange(fileCard10000(), "val_5000", "val_5099");
  }
}

BENCHMARK_RELATIVE(Card10K_BytesRange_Wide, iters) {
  for (size_t i = 0; i < iters; ++i) {
    state().readBytesRange(fileCard10000(), "val_0", "val_7999");
  }
}

BENCHMARK_DRAW_LINE();

} // namespace
} // namespace facebook::nimble

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  facebook::velox::memory::initializeMemoryManager(
      facebook::velox::memory::MemoryManager::Options{});
  folly::runBenchmarks();
  return 0;
}
