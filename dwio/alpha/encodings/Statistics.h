// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <limits>
#include <memory>
#include <span>
#include <type_traits>
#include "dwio/alpha/common/Types.h"

#include "absl/container/flat_hash_map.h" // @manual=fbsource//third-party/abseil-cpp:container__flat_hash_map

namespace facebook::alpha {

template <typename T, typename InputType = T>
class UniqueValueCounts {
 public:
  using MapType = absl::flat_hash_map<T, uint64_t>;

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using value_type = typename MapType::value_type;
    using difference_type = typename MapType::difference_type;
    using const_reference = typename MapType::const_reference;
    using const_iterator = typename MapType::const_iterator;

    const_reference operator*() const {
      return *iterator_;
    }
    const_iterator operator->() const {
      return iterator_;
    }

    // Prefix increment
    Iterator& operator++() {
      ++iterator_;
      return *this;
    }

    // Postfix increment
    Iterator operator++(int) {
      Iterator tmp = *this;
      ++(*this);
      return tmp;
    }

    friend bool operator==(const Iterator& a, const Iterator& b) {
      return a.iterator_ == b.iterator_;
    }
    friend bool operator!=(const Iterator& a, const Iterator& b) {
      return a.iterator_ != b.iterator_;
    }

   private:
    explicit Iterator(typename MapType::const_iterator iterator)
        : iterator_{iterator} {}

    typename MapType::const_iterator iterator_;

    friend class UniqueValueCounts<T, InputType>;
  };

  using const_iterator = Iterator;

  uint64_t at(T key) const noexcept {
    auto it = uniqueCounts_.find(key);
    if (it == uniqueCounts_.end()) {
      return 0;
    }
    return it->second;
  }

  size_t size() const noexcept {
    return uniqueCounts_.size();
  }

  const_iterator begin() const noexcept {
    return Iterator{uniqueCounts_.cbegin()};
  }
  const_iterator cbegin() const noexcept {
    return Iterator{uniqueCounts_.cbegin()};
  }

  const_iterator end() const noexcept {
    return Iterator{uniqueCounts_.cend()};
  }
  const_iterator cend() const noexcept {
    return Iterator{uniqueCounts_.cend()};
  }

  UniqueValueCounts() = default;
  explicit UniqueValueCounts(MapType&& uniqueCounts)
      : uniqueCounts_{std::move(uniqueCounts)} {}

 private:
  MapType uniqueCounts_;
};

template <typename T, typename InputType = T>
class Statistics {
 public:
  using valueType = T;

  static Statistics<T, InputType> create(std::span<const InputType> data);

  uint64_t consecutiveRepeatCount() const noexcept {
    if (!consecutiveRepeatCount_.has_value()) {
      populateRepeats();
    }
    return consecutiveRepeatCount_.value();
  }

  uint64_t minRepeat() const noexcept {
    if (!minRepeat_.has_value()) {
      populateRepeats();
    }
    return minRepeat_.value();
  }

  uint64_t maxRepeat() const noexcept {
    if (!maxRepeat_.has_value()) {
      populateRepeats();
    }
    return maxRepeat_.value();
  }

  uint64_t totalStringsLength() const noexcept {
    static_assert(alpha::isStringType<T>());
    if (!totalStringsLength_.has_value()) {
      populateStringLength();
    }
    return totalStringsLength_.value();
  }

  uint64_t totalStringsRepeatLength() const noexcept {
    static_assert(alpha::isStringType<T>());
    if (!totalStringsRepeatLength_.has_value()) {
      populateRepeats();
    }
    return totalStringsRepeatLength_.value();
  }

  T min() const noexcept {
    static_assert(!alpha::isBoolType<T>());
    if (!min_.has_value()) {
      populateMinMax();
    }
    return min_.value();
  }

  T max() const noexcept {
    static_assert(!alpha::isBoolType<T>());
    if (!max_.has_value()) {
      populateMinMax();
    }
    return max_.value();
  }

  const std::vector<uint64_t>& bucketCounts() const noexcept {
    static_assert(alpha::isIntegralType<T>());
    if (!bucketCounts_.has_value()) {
      populateBucketCounts();
    }
    return bucketCounts_.value();
  }

  const UniqueValueCounts<T, InputType>& uniqueCounts() const noexcept {
    if (!uniqueCounts_.has_value()) {
      populateUniques();
    }
    return uniqueCounts_.value();
  }

 private:
  Statistics() = default;
  std::span<const InputType> data_;

  void populateRepeats() const;
  void populateUniques() const;
  void populateMinMax() const;
  void populateBucketCounts() const;
  void populateStringLength() const;

  mutable std::optional<uint64_t> consecutiveRepeatCount_;
  mutable std::optional<uint64_t> minRepeat_;
  mutable std::optional<uint64_t> maxRepeat_;
  mutable std::optional<uint64_t> totalStringsLength_;
  mutable std::optional<uint64_t> totalStringsRepeatLength_;
  mutable std::optional<T> min_;
  mutable std::optional<T> max_;
  mutable std::optional<std::vector<uint64_t>> bucketCounts_;
  mutable std::optional<UniqueValueCounts<T, InputType>> uniqueCounts_;
};

} // namespace facebook::alpha
