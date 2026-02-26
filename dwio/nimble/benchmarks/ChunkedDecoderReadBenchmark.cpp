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

// Folly benchmarks for ChunkedDecoder::readWithVisitorImpl /
// callReadWithVisitor (ChunkedDecoder.h:333).  Uses data generators from
// EncodingExplorer to exercise the selective read path with realistic data
// distributions under various encoding strategies.
//
// Compiled with -DENABLE_HW_TIMER to activate fine-grained hardware timer
// instrumentation in the production code paths.

#include <unistd.h>

#include <string>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <glog/logging.h>

#include "dwio/nimble/benchmarks/BenchmarkDataGenerators.h"
#include "dwio/nimble/benchmarks/BenchmarkSuite.h"
#include "dwio/nimble/common/tests/NimbleFileWriter.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/velox/selective/SelectiveNimbleReader.h"
#ifdef ENABLE_HW_TIMER
#include "velox/common/time/HardwareTimer.h"
#endif
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/SharedArbitrator.h"

FOLLY_GFLAGS_DEFINE_string(
    bm_json_output,
    "",
    "File to write JSON benchmark results to.");

namespace facebook::nimble {
namespace {

using namespace facebook::velox;

VeloxWriterOptions makeFavoredEncodingOptions(EncodingType favored) {
  std::vector<std::pair<EncodingType, float>> readFactors;
  for (auto enc : ManualEncodingSelectionPolicyFactory::possibleEncodings()) {
    float factor = (enc == favored) ? 0.1f : 100.0f;
    readFactors.emplace_back(enc, factor);
  }

  VeloxWriterOptions opts;
  opts.encodingSelectionPolicyFactory =
      [encodingFactory =
           ManualEncodingSelectionPolicyFactory(std::move(readFactors))](
          DataType dataType) -> std::unique_ptr<EncodingSelectionPolicyBase> {
    return encodingFactory.createPolicy(dataType);
  };
  return opts;
}

// ---------------------------------------------------------------------------
// Shared state — initialized once in main() before benchmarks run.
// ---------------------------------------------------------------------------

std::shared_ptr<memory::MemoryPool> rootPool;
std::shared_ptr<memory::MemoryPool> leafPool;

void initPools() {
  memory::SharedArbitrator::registerFactory();
  memory::MemoryManager::Options options;
  options.arbitratorKind = "SHARED";
  memory::MemoryManager::testingSetInstance(options);
  registerSelectiveNimbleReaderFactory();
  rootPool = memory::memoryManager()->addRootPool("benchmarkRoot");
  leafPool = memory::memoryManager()->addLeafPool("benchmarkLeaf");
}

void cleanupPools() {
  leafPool.reset();
  rootPool.reset();
  unregisterSelectiveNimbleReaderFactory();
}

// ---------------------------------------------------------------------------
// Helper: read all rows from a nimble file through the selective reader.
// Returns the number of rows read.
// ---------------------------------------------------------------------------

uint64_t readAllRows(
    const std::string& file,
    const RowTypePtr& type,
    const std::shared_ptr<common::ScanSpec>& scanSpec,
    memory::MemoryPool* pool,
    int batchSize = 1024,
    const RowTypePtr& resultType = nullptr) {
  auto readFile = std::make_shared<InMemoryReadFile>(file);
  auto factory =
      dwio::common::getReaderFactory(dwio::common::FileFormat::NIMBLE);
  dwio::common::ReaderOptions options(pool);
  options.setScanSpec(scanSpec);
  auto reader = factory->createReader(
      std::make_unique<dwio::common::BufferedInput>(readFile, *pool), options);
  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(scanSpec);
  rowOptions.setRequestedType(type);
  auto rowReader = reader->createRowReader(rowOptions);

  auto vecType = resultType ? resultType : type;
  uint64_t totalRows = 0;
  auto result = BaseVector::create(vecType, 0, pool);
  while (rowReader->next(batchSize, result) > 0) {
    folly::doNotOptimizeAway(result);
    totalRows += result->size();
    result = BaseVector::create(vecType, 0, pool);
  }
  return totalRows;
}

// ===========================================================================
// Benchmark functions — plain functions replacing the BENCHMARK macros.
// ===========================================================================

void benchFavoredFixedBitWidth(unsigned iters) {
  std::string file;
  RowTypePtr type;

  BENCHMARK_SUSPEND {
    VectorMaker vm{leafPool.get()};
    constexpr size_t kN = 500;
    auto input = buildData(vm, kN);
    auto opts = makeFavoredEncodingOptions(EncodingType::FixedBitWidth);
    file = nimble::test::createNimbleFile(*rootPool, input, std::move(opts));
    type = asRowType(input->type());
  }

  for (size_t i = 0; i < iters; ++i) {
    auto scanSpec = std::make_shared<common::ScanSpec>("root");
    scanSpec->addAllChildFields(*type);
    auto rows = readAllRows(file, type, scanSpec, leafPool.get(), 200);
    folly::doNotOptimizeAway(rows);
  }
}

// ---------------------------------------------------------------------------
// Filtered read benchmarks
// ---------------------------------------------------------------------------

void benchFilterMonotonicWithRange(unsigned iters) {
  std::string file;
  RowTypePtr type;

  BENCHMARK_SUSPEND {
    VectorMaker vm{leafPool.get()};
    constexpr size_t kN = 1000;
    auto input = buildData(vm, kN);
    file = nimble::test::createNimbleFile(*rootPool, input);
    type = asRowType(input->type());
  }

  for (size_t i = 0; i < iters; ++i) {
    auto scanSpec = std::make_shared<common::ScanSpec>("root");
    scanSpec->addAllChildFields(*type);
    scanSpec->childByName("monotonic_id")
        ->setFilter(std::make_unique<common::BigintRange>(1050, 2000, false));
    auto rows = readAllRows(file, type, scanSpec, leafPool.get(), 100);
    folly::doNotOptimizeAway(rows);
  }
}

void benchFilterSmallPositiveInts(unsigned iters) {
  std::string file;
  RowTypePtr type;

  BENCHMARK_SUSPEND {
    VectorMaker vm{leafPool.get()};
    constexpr size_t kN = 500;
    auto input = buildData(vm, kN);
    file = nimble::test::createNimbleFile(*rootPool, input);
    type = asRowType(input->type());
  }

  for (size_t i = 0; i < iters; ++i) {
    auto scanSpec = std::make_shared<common::ScanSpec>("root");
    scanSpec->addAllChildFields(*type);
    scanSpec->childByName("small_pos")
        ->setFilter(std::make_unique<common::BigintRange>(0, 50, false));
    auto rows = readAllRows(file, type, scanSpec, leafPool.get(), 100);
    folly::doNotOptimizeAway(rows);
  }
}

// ---------------------------------------------------------------------------
// Multi-chunk read benchmarks
// ---------------------------------------------------------------------------

VeloxWriterOptions makeChunkingOptions() {
  VeloxWriterOptions options;
  options.enableChunking = true;
  options.minStreamChunkRawSize = 0;
  options.flushPolicyFactory = [] {
    return std::make_unique<LambdaFlushPolicy>(
        /*flushLambda=*/[](const StripeProgress&) { return false; },
        /*chunkLambda=*/[](const StripeProgress&) { return true; });
  };
  return options;
}

void benchMultiChunkWithFilter(unsigned iters) {
  std::string file;
  RowTypePtr type;

  BENCHMARK_SUSPEND {
    VectorMaker vm{leafPool.get()};
    auto chunk1 = vm.rowVector(
        {"val", "keep"},
        {vm.flatVector<int64_t>({100, 200, 300, 400, 500}),
         vm.flatVector<bool>({true, false, true, false, true})});
    auto chunk2 = vm.rowVector(
        {"val", "keep"},
        {vm.flatVector<int64_t>({600, 700, 800}),
         vm.flatVector<bool>({false, true, false})});
    auto chunk3 = vm.rowVector(
        {"val", "keep"},
        {vm.flatVector<int64_t>({900, 1000, 1100, 1200}),
         vm.flatVector<bool>({true, true, false, true})});

    file = nimble::test::createNimbleFile(
        *rootPool, {chunk1, chunk2, chunk3}, makeChunkingOptions(), false);
    type = asRowType(chunk1->type());
  }

  for (size_t i = 0; i < iters; ++i) {
    auto scanSpec = std::make_shared<common::ScanSpec>("root");
    scanSpec->addAllChildFields(*type);
    scanSpec->childByName("keep")->setFilter(
        std::make_unique<common::BoolValue>(true, false));
    auto rows = readAllRows(file, type, scanSpec, leafPool.get());
    folly::doNotOptimizeAway(rows);
  }
}

} // namespace
} // namespace facebook::nimble

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  facebook::nimble::initPools();

  facebook::nimble::BenchmarkSuite suite;
  suite.setTitle("ChunkedDecoderBenchmarks");
  if (!FLAGS_bm_json_output.empty()) {
    suite.setJsonOutputPath(FLAGS_bm_json_output);
  }
  suite.setOnStart([](const std::string& name) {
#ifdef ENABLE_HW_TIMER
    facebook::velox::HardwareTimer::init(name);
#endif
  });
  suite.setOnEnd(
      []([[maybe_unused]] const std::string& name,
         [[maybe_unused]] const folly::detail::BenchmarkResult& result) {
#ifdef ENABLE_HW_TIMER
        facebook::velox::HardwareTimer::cleanup();
#endif
      });

  // Favored encoding.
  suite.addBenchmark(
      "FavoredEncoding_FixedBitWidth",
      facebook::nimble::benchFavoredFixedBitWidth);

  suite.addSeparator();

  // Filtered reads.
  suite.addBenchmark(
      "FilterMonotonicWithRange",
      facebook::nimble::benchFilterMonotonicWithRange);
  suite.addBenchmark(
      "FilterSmallPositiveInts",
      facebook::nimble::benchFilterSmallPositiveInts);

  suite.addSeparator();

  // Multi-chunk reads.
  suite.addBenchmark(
      "MultiChunkWithFilter", facebook::nimble::benchMultiChunkWithFilter);

  suite.run();
  facebook::nimble::cleanupPools();

  // Flush stdio buffers before _exit() which skips normal cleanup.
  fflush(nullptr);

  // Use _exit() to skip static destructors. A file-scope static
  // HardwareTimer in EncodingUtils.h (compiled into selective_reader)
  // causes a static destruction order fiasco: it outlives the
  // function-local static EntriesMap it writes to in its destructor.
  _exit(0);
}
