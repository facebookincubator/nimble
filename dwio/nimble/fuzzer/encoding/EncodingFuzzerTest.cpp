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

/// Fuzzer tests for Varint encoding across supported data types.
///
/// Configuration via CLI flags:
///   --fuzzer_iterations=N     Number of iterations per test (default: 50)
///   --fuzzer_max_rows=N       Maximum rows per iteration (default: 5000)
///   --fuzzer_seed=N           Fixed seed, 0=random (default: 42)
///   --fuzzer_compression      Enable compression testing (default: true)

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "dwio/nimble/encodings/MainlyConstantEncoding.h"
#include "dwio/nimble/encodings/VarintEncoding.h"
#include "dwio/nimble/fuzzer/encoding/EncodingFuzzer.h"

DEFINE_uint32(fuzzer_iterations, 50, "Number of fuzzer iterations per test");
DEFINE_uint32(fuzzer_max_rows, 5000, "Maximum rows per fuzzer iteration");
DEFINE_uint32(fuzzer_seed, 42, "Fuzzer seed (0 = random each run)");
DEFINE_bool(fuzzer_compression, true, "Test with compression enabled");

using namespace facebook::nimble;
using namespace facebook::nimble::test;

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

TYPED_TEST(VarintFuzzerTest, Correctness) {
  EncodingFuzzer<TypeParam> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}

// MainlyConstant: all types except bool
using MainlyConstantTypes = ::testing::Types<
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

TYPED_TEST(MainlyConstantFuzzerTest, Correctness) {
  EncodingFuzzer<TypeParam> fuzzer(
      FLAGS_fuzzer_iterations,
      FLAGS_fuzzer_max_rows,
      FLAGS_fuzzer_seed,
      FLAGS_fuzzer_compression);
  fuzzer.run();
}
