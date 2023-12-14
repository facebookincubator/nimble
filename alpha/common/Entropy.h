// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <span>
#include "folly/container/F14Map.h"

// Entropy encoding.

namespace facebook::alpha::entropy {

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

} // namespace facebook::alpha::entropy
