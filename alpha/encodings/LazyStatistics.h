// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <limits>
#include <memory>
#include <span>
#include <type_traits>
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/encodings/StatisticsCommon.h"

#include "absl/container/flat_hash_map.h" // @manual=fbsource//third-party/abseil-cpp:container__flat_hash_map

namespace facebook::alpha {

template <typename T, typename InputType = T>
class LazyStatistics {
 public:
  using valueType = T;

  static LazyStatistics<T, InputType> create(std::span<const InputType> data);

  uint64_t consecutiveRepeatCount() noexcept {
    if (!consecutiveRepeatCount_.has_value()) {
      populateRepeats();
    }
    return consecutiveRepeatCount_.value();
  }

  uint64_t minRepeat() noexcept {
    if (!minRepeat_.has_value()) {
      populateRepeats();
    }
    return minRepeat_.value();
  }

  uint64_t maxRepeat() noexcept {
    if (!maxRepeat_.has_value()) {
      populateRepeats();
    }
    return maxRepeat_.value();
  }

  uint64_t totalStringsLength() noexcept {
    static_assert(alpha::isStringType<T>());
    if (!totalStringsLength_.has_value()) {
      populateStringLength();
    }
    return totalStringsLength_.value();
  }

  uint64_t totalStringsRepeatLength() noexcept {
    static_assert(alpha::isStringType<T>());
    if (!totalStringsRepeatLength_.has_value()) {
      populateRepeats();
    }
    return totalStringsRepeatLength_.value();
  }

  T min() noexcept {
    static_assert(!alpha::isBoolType<T>());
    if (!min_.has_value()) {
      populateMinMax();
    }
    return min_.value();
  }

  T max() noexcept {
    static_assert(!alpha::isBoolType<T>());
    if (!max_.has_value()) {
      populateMinMax();
    }
    return max_.value();
  }

  std::vector<uint64_t>& bucketCounts() {
    static_assert(alpha::isIntegralType<T>());
    if (!bucketCounts_.has_value()) {
      populateBucketCounts();
    }
    return bucketCounts_.value();
  }

  UniqueValueCounts<T, InputType>& uniqueCounts() noexcept {
    if (!uniqueCounts_.has_value()) {
      populateUniques();
    }
    return uniqueCounts_.value();
  }

 private:
  LazyStatistics() = default;
  std::span<const InputType> data_;

  void populateRepeats();
  void populateUniques();
  void populateMinMax();
  void populateBucketCounts();
  void populateStringLength();

  std::optional<uint64_t> consecutiveRepeatCount_;
  std::optional<uint64_t> minRepeat_;
  std::optional<uint64_t> maxRepeat_;
  std::optional<uint64_t> totalStringsLength_;
  std::optional<uint64_t> totalStringsRepeatLength_;
  std::optional<T> min_;
  std::optional<T> max_;
  std::optional<std::vector<uint64_t>> bucketCounts_;
  std::optional<UniqueValueCounts<T, InputType>> uniqueCounts_;
};

} // namespace facebook::alpha
