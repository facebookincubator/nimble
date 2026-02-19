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

// Data generators — each produces a VectorPtr whose distribution exercises a
// different encoding strength.

#pragma once

#include <random>
#include <string>
#include <vector>

#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::nimble {

using VectorMaker = velox::test::VectorMaker;

inline velox::VectorPtr makeMonotonicInts(VectorMaker& vm, size_t n) {
  std::vector<int64_t> vals(n);
  int64_t v = 1000;
  std::mt19937 rng(42);
  std::uniform_int_distribution<int64_t> step(1, 10);
  for (size_t i = 0; i < n; ++i) {
    v += step(rng);
    vals[i] = v;
  }
  return vm.flatVector<int64_t>(vals);
}

inline velox::VectorPtr makeLowCardInts(VectorMaker& vm, size_t n) {
  std::mt19937 rng(7);
  std::uniform_int_distribution<int32_t> dist(0, 9);
  std::vector<int32_t> vals(n);
  for (auto& v : vals) {
    v = dist(rng);
  }
  return vm.flatVector<int32_t>(vals);
}

inline velox::VectorPtr makeRunHeavyInts(VectorMaker& vm, size_t n) {
  std::mt19937 rng(123);
  std::uniform_int_distribution<int32_t> valDist(0, 4);
  std::uniform_int_distribution<size_t> runDist(50, 200);
  std::vector<int32_t> vals;
  vals.reserve(n);
  while (vals.size() < n) {
    int32_t v = valDist(rng);
    size_t run = std::min(runDist(rng), n - vals.size());
    for (size_t i = 0; i < run; ++i) {
      vals.push_back(v);
    }
  }
  return vm.flatVector<int32_t>(vals);
}

inline velox::VectorPtr makeConstantInts(VectorMaker& vm, size_t n) {
  std::vector<int64_t> vals(n, 42);
  return vm.flatVector<int64_t>(vals);
}

inline velox::VectorPtr makeMainlyConstantInts(VectorMaker& vm, size_t n) {
  std::mt19937 rng(99);
  std::uniform_real_distribution<float> prob(0.0f, 1.0f);
  std::uniform_int_distribution<int64_t> outlierDist(100, 999);
  std::vector<int64_t> vals(n);
  for (auto& v : vals) {
    v = (prob(rng) < 0.98f) ? 7 : outlierDist(rng);
  }
  return vm.flatVector<int64_t>(vals);
}

inline velox::VectorPtr makeSmallPositiveInts(VectorMaker& vm, size_t n) {
  std::mt19937 rng(55);
  std::uniform_int_distribution<int64_t> dist(0, 127);
  std::vector<int64_t> vals(n);
  for (auto& v : vals) {
    v = dist(rng);
  }
  return vm.flatVector<int64_t>(vals);
}

inline velox::VectorPtr makeStrings(VectorMaker& vm, size_t n) {
  static const std::vector<std::string> kPool = {
      "alpha",   "beta", "gamma", "delta",  "epsilon", "zeta",    "eta",
      "theta",   "iota", "kappa", "lambda", "mu",      "nu",      "xi",
      "omicron", "pi",   "rho",   "sigma",  "tau",     "upsilon",
  };
  std::mt19937 rng(77);
  std::uniform_int_distribution<size_t> dist(0, kPool.size() - 1);
  std::vector<velox::StringView> vals(n);
  for (auto& v : vals) {
    v = velox::StringView(kPool[dist(rng)]);
  }
  return vm.flatVector<velox::StringView>(vals);
}

inline velox::RowVectorPtr buildData(VectorMaker& vm, size_t numRows) {
  return vm.rowVector(
      {"monotonic_id",
       "low_card",
       "run_heavy",
       "constant_col",
       "mainly_const",
       "small_pos",
       "label"},
      {makeMonotonicInts(vm, numRows),
       makeLowCardInts(vm, numRows),
       makeRunHeavyInts(vm, numRows),
       makeConstantInts(vm, numRows),
       makeMainlyConstantInts(vm, numRows),
       makeSmallPositiveInts(vm, numRows),
       makeStrings(vm, numRows)});
}

} // namespace facebook::nimble
