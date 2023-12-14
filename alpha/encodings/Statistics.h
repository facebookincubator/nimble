// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <limits>
#include <memory>
#include <span>
#include <type_traits>

#include "absl/container/flat_hash_map.h" // @manual=fbsource//third-party/abseil-cpp:container__flat_hash_map
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/encodings/StatisticsCommon.h"

namespace facebook::alpha {

template <typename T>
class Statistics {
 public:
  using valueType = T;

  template <typename InputType = T>
  static Statistics<T> create(std::span<const InputType> data);

  uint64_t consecutiveRepeatCount() const noexcept {
    return consecutiveRepeatCount_;
  }

  uint64_t minRepeat() const noexcept {
    return minRepeat_;
  }

  uint64_t maxRepeat() const noexcept {
    return maxRepeat_;
  }

  uint64_t totalStringsLength() const noexcept {
    return totalStringsLength_;
  }

  uint64_t totalStringsRepeatLength() const noexcept {
    return totalStringsRepeatLength_;
  }

  T min() const noexcept {
    return min_;
  }

  T max() const noexcept {
    return max_;
  }

  const std::vector<uint64_t>& bucketCounts() const noexcept {
    return bucketCounts_;
  }

  const UniqueValueCounts<T>& uniqueCounts() const noexcept {
    return uniqueCounts_;
  }

 private:
  Statistics() = default;

  uint64_t consecutiveRepeatCount_ = 0;
  uint64_t minRepeat_ = 0;
  uint64_t maxRepeat_ = 0;
  uint64_t totalStringsLength_ = 0;
  uint64_t totalStringsRepeatLength_ = 0;
  T min_ = T();
  T max_ = T();
  std::vector<uint64_t> bucketCounts_;
  UniqueValueCounts<T> uniqueCounts_;
};

} // namespace facebook::alpha
