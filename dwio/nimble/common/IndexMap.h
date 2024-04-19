/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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

#include "dwio/nimble/common/NimbleCompare.h"
#include "folly/container/F14Map.h"

// An index map maintain a bijection between a set and each element's unique
// index in [0, n). E.g. the first key introduced to the index map gets index 0,
// the second 1, and so on. Looking up key by index and index by key are both
// possible.

namespace facebook::nimble {

// Don't insert more than INT32_MAX symbols into the map.
template <typename T>
class IndexMap {
 public:
  // Returns the unique index associated with the key, adding the new key to
  // the internal T<->int32_t mapping if it hasn't been seen before.
  int32_t index(const T& key) noexcept {
    auto it = indices_.find(key);
    if (it == indices_.end()) {
      indices_.emplace(key, indices_.size());
      keys_.push_back(key);
      return indices_.size() - 1;
    }
    return it->second;
  }

  // Returns the index of an existing key, or -1 if the key has not been
  // seen previously.
  int32_t readOnlyIndex(const T& key) const noexcept {
    auto it = indices_.find(key);
    if (it == indices_.end()) {
      return -1;
    }
    return it->second;
  }

  // Retrieves a previously inserted key via its index. index must lie in
  // [0, size()).
  const T& key(int32_t index) noexcept {
    DCHECK_LT(index, keys_.size());
    return keys_[index];
  }

  // The current size size will be the index of the next previously unseen key
  // passed to index.
  int32_t size() noexcept {
    return indices_.size();
  }

  // Transfers ownership of the keys_ to the caller. *this should not
  // be used after this is called.
  std::vector<T>&& releaseKeys() {
    indices_.clear();
    return std::move(keys_);
  }

 private:
  folly::
      F14FastMap<T, int32_t, folly::f14::DefaultHasher<T>, NimbleComparator<T>>
          indices_;
  std::vector<T> keys_;
};

} // namespace facebook::nimble
