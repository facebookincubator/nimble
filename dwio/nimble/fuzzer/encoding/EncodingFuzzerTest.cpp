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

/// Fuzzer tests for all Nimble encodings across supported data types.
///
/// Configuration via CLI flags:
///   --fuzzer_iterations=N     Number of iterations per test (default: 50)
///   --fuzzer_max_rows=N       Maximum rows per iteration (default: 5000)
///   --fuzzer_seed=N           Fixed seed, 0=random (default: 42)
///   --fuzzer_compression      Enable compression testing (default: true)

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "dwio/nimble/encodings/ALPEncoding.h"
#include "dwio/nimble/encodings/BlockBitPackingEncoding.h"
#include "dwio/nimble/encodings/ConstantEncoding.h"
#include "dwio/nimble/encodings/DeltaEncoding.h"
#include "dwio/nimble/encodings/DictionaryEncoding.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/MainlyConstantEncoding.h"
#include "dwio/nimble/encodings/RLEEncoding.h"
#include "dwio/nimble/encodings/SimdForBitpackEncoding.h"
#include "dwio/nimble/encodings/SparseBoolEncoding.h"
#ifdef NIMBLE_ENABLE_EXPERIMENTAL_ENCODINGS
#include "dwio/nimble/encodings/SubIntSplitEncoding.h"
#endif
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/VarintEncoding.h"
#include "dwio/nimble/fuzzer/encoding/EncodingFuzzer.h"

DEFINE_uint32(fuzzer_iterations, 50, "Number of fuzzer iterations per test");
DEFINE_uint32(fuzzer_max_rows, 5000, "Maximum rows per fuzzer iteration");
DEFINE_uint32(fuzzer_seed, 42, "Fuzzer seed (0 = random each run)");
DEFINE_bool(fuzzer_compression, true, "Test with compression enabled");

using namespace facebook::nimble;
using namespace facebook::nimble::test;

// Trivial: all types
using TrivialTypes = ::testing::Types<
    TrivialEncoding<bool>,
    TrivialEncoding<int8_t>,
    TrivialEncoding<uint8_t>,
    TrivialEncoding<int16_t>,
    TrivialEncoding<uint16_t>,
    TrivialEncoding<int32_t>,
    TrivialEncoding<uint32_t>,
    TrivialEncoding<int64_t>,
    TrivialEncoding<uint64_t>,
    TrivialEncoding<float>,
    TrivialEncoding<double>,
    TrivialEncoding<std::string_view>>;

template <typename E>
class TrivialFuzzerTest : public ::testing::Test {};
TYPED_TEST_SUITE(TrivialFuzzerTest, TrivialTypes);

TYPED_TEST(TrivialFuzzerTest, correctness) {
  EncodingFuzzer<TypeParam> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}

// RLE: all types
using RleTypes = ::testing::Types<
    RLEEncoding<bool>,
    RLEEncoding<int8_t>,
    RLEEncoding<uint8_t>,
    RLEEncoding<int16_t>,
    RLEEncoding<uint16_t>,
    RLEEncoding<int32_t>,
    RLEEncoding<uint32_t>,
    RLEEncoding<int64_t>,
    RLEEncoding<uint64_t>,
    RLEEncoding<float>,
    RLEEncoding<double>,
    RLEEncoding<std::string_view>>;

template <typename E>
class RleFuzzerTest : public ::testing::Test {};
TYPED_TEST_SUITE(RleFuzzerTest, RleTypes);

TYPED_TEST(RleFuzzerTest, correctness) {
  EncodingFuzzer<TypeParam> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}

// Dictionary: all types except bool
using DictionaryTypes = ::testing::Types<
    DictionaryEncoding<int8_t>,
    DictionaryEncoding<uint8_t>,
    DictionaryEncoding<int16_t>,
    DictionaryEncoding<uint16_t>,
    DictionaryEncoding<int32_t>,
    DictionaryEncoding<uint32_t>,
    DictionaryEncoding<int64_t>,
    DictionaryEncoding<uint64_t>,
    DictionaryEncoding<float>,
    DictionaryEncoding<double>,
    DictionaryEncoding<std::string_view>>;

template <typename E>
class DictionaryFuzzerTest : public ::testing::Test {};
TYPED_TEST_SUITE(DictionaryFuzzerTest, DictionaryTypes);

TYPED_TEST(DictionaryFuzzerTest, correctness) {
  EncodingFuzzer<TypeParam> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}

// FixedBitWidth: all numeric types
using FixedBitWidthTypes = ::testing::Types<
    FixedBitWidthEncoding<int8_t>,
    FixedBitWidthEncoding<uint8_t>,
    FixedBitWidthEncoding<int16_t>,
    FixedBitWidthEncoding<uint16_t>,
    FixedBitWidthEncoding<int32_t>,
    FixedBitWidthEncoding<uint32_t>,
    FixedBitWidthEncoding<int64_t>,
    FixedBitWidthEncoding<uint64_t>,
    FixedBitWidthEncoding<float>,
    FixedBitWidthEncoding<double>>;

template <typename E>
class FixedBitWidthFuzzerTest : public ::testing::Test {};
TYPED_TEST_SUITE(FixedBitWidthFuzzerTest, FixedBitWidthTypes);

TYPED_TEST(FixedBitWidthFuzzerTest, correctness) {
  EncodingFuzzer<TypeParam> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}

// Delta: integer types
using DeltaTypes = ::testing::Types<
    DeltaEncoding<int8_t>,
    DeltaEncoding<uint8_t>,
    DeltaEncoding<int16_t>,
    DeltaEncoding<uint16_t>,
    DeltaEncoding<int32_t>,
    DeltaEncoding<uint32_t>,
    DeltaEncoding<int64_t>,
    DeltaEncoding<uint64_t>>;

template <typename E>
class DeltaFuzzerTest : public ::testing::Test {};
TYPED_TEST_SUITE(DeltaFuzzerTest, DeltaTypes);

TYPED_TEST(DeltaFuzzerTest, correctness) {
  EncodingFuzzer<TypeParam> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}

// SimdForBitpack: unsigned integer types
using SimdForBitpackTypes = ::testing::Types<
    SimdForBitpackEncoding<uint8_t>,
    SimdForBitpackEncoding<uint16_t>,
    SimdForBitpackEncoding<uint32_t>,
    SimdForBitpackEncoding<uint64_t>>;

template <typename E>
class SimdForBitpackFuzzerTest : public ::testing::Test {};
TYPED_TEST_SUITE(SimdForBitpackFuzzerTest, SimdForBitpackTypes);

TYPED_TEST(SimdForBitpackFuzzerTest, correctness) {
  EncodingFuzzer<TypeParam> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}

// Constant: all types
using ConstantTypes = ::testing::Types<
    ConstantEncoding<bool>,
    ConstantEncoding<int8_t>,
    ConstantEncoding<uint8_t>,
    ConstantEncoding<int16_t>,
    ConstantEncoding<uint16_t>,
    ConstantEncoding<int32_t>,
    ConstantEncoding<uint32_t>,
    ConstantEncoding<int64_t>,
    ConstantEncoding<uint64_t>,
    ConstantEncoding<float>,
    ConstantEncoding<double>,
    ConstantEncoding<std::string_view>>;

template <typename E>
class ConstantFuzzerTest : public ::testing::Test {};
TYPED_TEST_SUITE(ConstantFuzzerTest, ConstantTypes);

TYPED_TEST(ConstantFuzzerTest, correctness) {
  EncodingFuzzer<TypeParam> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}

// SparseBool: bool only
TEST(SparseBoolFuzzerTest, correctness) {
  EncodingFuzzer<SparseBoolEncoding> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}

// Varint: 4+ byte types
using VarintTypes = ::testing::Types<
    VarintEncoding<int32_t>,
    VarintEncoding<uint32_t>,
    VarintEncoding<int64_t>,
    VarintEncoding<uint64_t>,
    VarintEncoding<float>,
    VarintEncoding<double>>;

template <typename E>
class VarintFuzzerTest : public ::testing::Test {};
TYPED_TEST_SUITE(VarintFuzzerTest, VarintTypes);

TYPED_TEST(VarintFuzzerTest, correctness) {
  EncodingFuzzer<TypeParam> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}

// MainlyConstant: all types except bool
using MainlyConstantTypes = ::testing::Types<
    MainlyConstantEncoding<int8_t>,
    MainlyConstantEncoding<uint8_t>,
    MainlyConstantEncoding<int16_t>,
    MainlyConstantEncoding<uint16_t>,
    MainlyConstantEncoding<int32_t>,
    MainlyConstantEncoding<uint32_t>,
    MainlyConstantEncoding<int64_t>,
    MainlyConstantEncoding<uint64_t>,
    MainlyConstantEncoding<float>,
    MainlyConstantEncoding<double>,
    MainlyConstantEncoding<std::string_view>>;

template <typename E>
class MainlyConstantFuzzerTest : public ::testing::Test {};
TYPED_TEST_SUITE(MainlyConstantFuzzerTest, MainlyConstantTypes);

TYPED_TEST(MainlyConstantFuzzerTest, correctness) {
  EncodingFuzzer<TypeParam> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}

// BlockBitPacking: all numeric types
using BlockBitPackingTypes = ::testing::Types<
    BlockBitPackingEncoding<int8_t>,
    BlockBitPackingEncoding<uint8_t>,
    BlockBitPackingEncoding<int16_t>,
    BlockBitPackingEncoding<uint16_t>,
    BlockBitPackingEncoding<int32_t>,
    BlockBitPackingEncoding<uint32_t>,
    BlockBitPackingEncoding<int64_t>,
    BlockBitPackingEncoding<uint64_t>,
    BlockBitPackingEncoding<float>,
    BlockBitPackingEncoding<double>>;

template <typename E>
class BlockBitPackingFuzzerTest : public ::testing::Test {};
TYPED_TEST_SUITE(BlockBitPackingFuzzerTest, BlockBitPackingTypes);

TYPED_TEST(BlockBitPackingFuzzerTest, correctness) {
  EncodingFuzzer<TypeParam> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}

// ALP: float and double only
using ALPTypes = ::testing::Types<ALPEncoding<float>, ALPEncoding<double>>;

template <typename E>
class ALPFuzzerTest : public ::testing::Test {};
TYPED_TEST_SUITE(ALPFuzzerTest, ALPTypes);

TYPED_TEST(ALPFuzzerTest, correctness) {
  EncodingFuzzer<TypeParam> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}

#ifdef NIMBLE_ENABLE_EXPERIMENTAL_ENCODINGS
// SubIntSplit: 32- and 64-bit numeric types (no bool, no string_view)
using SubIntSplitTypes = ::testing::Types<
    SubIntSplitEncoding<int32_t>,
    SubIntSplitEncoding<uint32_t>,
    SubIntSplitEncoding<int64_t>,
    SubIntSplitEncoding<uint64_t>,
    SubIntSplitEncoding<float>,
    SubIntSplitEncoding<double>>;

template <typename E>
class SubIntSplitFuzzerTest : public ::testing::Test {};
TYPED_TEST_SUITE(SubIntSplitFuzzerTest, SubIntSplitTypes);

TYPED_TEST(SubIntSplitFuzzerTest, Correctness) {
  EncodingFuzzer<TypeParam> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}
#endif  // NIMBLE_ENABLE_EXPERIMENTAL_ENCODINGS
