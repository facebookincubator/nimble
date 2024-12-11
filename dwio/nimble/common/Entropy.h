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
#pragma once

#include <span>
#include "folly/container/F14Map.h"

// Entropy encoding.

namespace facebook::nimble::entropy {

// Returns the Shannon entropy from symbol counts.
inline double entropy(std::span<const int> counts, int sumOfCount) {
  double entropy = 0;
  for (int count : counts) {
    const double p = double(count) / sumOfCount;
    entropy -= p * log2(p);
  }
  return entropy;
}

// Calculates the Shannon entropy of a data set.
template <typename T>
double computeEntropy(std::span<const T> data) {
  folly::F14FastMap<T, int> counts;
  for (auto datum : data) {
    ++counts[datum];
  }
  std::vector<int> finalCounts;
  finalCounts.reserve(counts.size());
  for (const auto& p : counts) {
    finalCounts.push_back(p.second);
  }
  return entropy(finalCounts, data.size());
}

} // namespace facebook::nimble::entropy
