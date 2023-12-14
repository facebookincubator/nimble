// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <complex>
#include <limits>
#include <memory>
#include <type_traits>

#include "absl/container/flat_hash_map.h" // @manual=fbsource//third-party/abseil-cpp:container__flat_hash_map
#include "dwio/alpha/common/Types.h"

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

} // namespace facebook::alpha
