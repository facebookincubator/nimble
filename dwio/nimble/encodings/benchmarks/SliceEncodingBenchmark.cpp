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

#include "dwio/nimble/encodings/ConstantEncoding.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/RLEEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/benchmarks/BenchmarkUtils.h"
#include "folly/Benchmark.h"
#include "folly/init/Init.h"

using namespace facebook::nimble;
using namespace facebook::nimble::benchmarks;

namespace {

constexpr uint32_t kSliceOffset = 12345;
constexpr uint32_t kSliceLength = 4096;

void sliceBenchmark(const std::string& encoded, uint32_t iters) {
  Buffer buffer{*benchmarkPool()};
  while (iters--) {
    buffer.reset();
    const auto sliced =
        EncodingFactory::slice(encoded, kSliceOffset, kSliceLength, buffer);
    folly::doNotOptimizeAway(sliced.data());
    folly::doNotOptimizeAway(sliced.size());
  }
}

template <typename EncodingT, typename T>
void materializeEncodeBenchmark(
    const std::string& encoded,
    EncodingType encodingType,
    uint32_t iters) {
  while (iters--) {
    auto encoding =
        EncodingFactory{}.create(*benchmarkPool(), encoded, nullFactory());
    encoding->skip(kSliceOffset);

    Vector<T> values{benchmarkPool().get(), kSliceLength};
    encoding->materialize(kSliceLength, values.data());

    const auto materialized =
        encodeData<EncodingT>(encodingType, values, Encoding::Options{});
    folly::doNotOptimizeAway(materialized.data());
    folly::doNotOptimizeAway(materialized.size());
  }
}

template <typename T>
void materializeBenchmark(const std::string& encoded, uint32_t iters) {
  Vector<T> values{benchmarkPool().get(), kSliceLength};
  while (iters--) {
    auto encoding =
        EncodingFactory{}.create(*benchmarkPool(), encoded, nullFactory());
    encoding->skip(kSliceOffset);
    encoding->materialize(kSliceLength, values.data());
    folly::doNotOptimizeAway(values.data());
  }
}

} // namespace

#define SLICE_BENCHMARKS(Name, EncodingT, EncodingTypeValue, DataExpr) \
  BENCHMARK(Slice_##Name, iters) {                                     \
    std::string encoded;                                               \
    BENCHMARK_SUSPEND {                                                \
      const auto data = DataExpr;                                      \
      encoded = encodeData<EncodingT>(EncodingTypeValue, data);        \
    }                                                                  \
    sliceBenchmark(encoded, iters);                                    \
  }                                                                    \
  BENCHMARK_RELATIVE(MaterializeEncode_##Name, iters) {                \
    std::string encoded;                                               \
    BENCHMARK_SUSPEND {                                                \
      const auto data = DataExpr;                                      \
      encoded = encodeData<EncodingT>(EncodingTypeValue, data);        \
    }                                                                  \
    materializeEncodeBenchmark<EncodingT, uint32_t>(                   \
        encoded, EncodingTypeValue, iters);                            \
  }                                                                    \
  BENCHMARK_DRAW_LINE()

#define SLICE_MATERIALIZE_BENCHMARKS(                           \
    Name, EncodingT, EncodingTypeValue, DataExpr)               \
  BENCHMARK(SliceCreate_##Name, iters) {                        \
    std::string encoded;                                        \
    BENCHMARK_SUSPEND {                                         \
      const auto data = DataExpr;                               \
      encoded = encodeData<EncodingT>(EncodingTypeValue, data); \
    }                                                           \
    sliceBenchmark(encoded, iters);                             \
  }                                                             \
  BENCHMARK_RELATIVE(MaterializeRange_##Name, iters) {          \
    std::string encoded;                                        \
    BENCHMARK_SUSPEND {                                         \
      const auto data = DataExpr;                               \
      encoded = encodeData<EncodingT>(EncodingTypeValue, data); \
    }                                                           \
    materializeBenchmark<uint32_t>(encoded, iters);             \
  }                                                             \
  BENCHMARK_DRAW_LINE()

SLICE_BENCHMARKS(
    ConstantUint32,
    ConstantEncoding<uint32_t>,
    EncodingType::Constant,
    makeConstant<uint32_t>(42));
SLICE_BENCHMARKS(
    TrivialUint32,
    TrivialEncoding<uint32_t>,
    EncodingType::Trivial,
    makeRandom<uint32_t>());
SLICE_BENCHMARKS(
    RLEUint32,
    RLEEncoding<uint32_t>,
    EncodingType::RLE,
    makeRunLength<uint32_t>());
SLICE_BENCHMARKS(
    FixedBitWidthUint32,
    FixedBitWidthEncoding<uint32_t>,
    EncodingType::FixedBitWidth,
    makeNarrow<uint32_t>(12));

SLICE_MATERIALIZE_BENCHMARKS(
    TrivialUint32,
    TrivialEncoding<uint32_t>,
    EncodingType::Trivial,
    makeRandom<uint32_t>());
SLICE_MATERIALIZE_BENCHMARKS(
    RLEUint32,
    RLEEncoding<uint32_t>,
    EncodingType::RLE,
    makeRunLength<uint32_t>());

#undef SLICE_MATERIALIZE_BENCHMARKS
#undef SLICE_BENCHMARKS

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  facebook::velox::memory::MemoryManager::initialize({});
  folly::runBenchmarks();
}
