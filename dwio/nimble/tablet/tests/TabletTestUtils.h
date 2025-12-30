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

#include "dwio/nimble/tablet/TabletReader.h"

namespace facebook::nimble::test {

/// Test helper class for TabletReader that provides access to private members
/// for testing purposes. This follows the same pattern as
/// AsyncDataCacheTestHelper in velox/common/caching/tests/CacheTestUtil.h.
class TabletReaderTestHelper {
 public:
  explicit TabletReaderTestHelper(const TabletReader* tabletReader)
      : tabletReader_(tabletReader) {}

  /// Returns the stripe group index for a given stripe index.
  uint32_t stripeGroupIndex(uint32_t stripeIndex) const {
    return tabletReader_->stripeGroupIndex(stripeIndex);
  }

  /// Returns the number of stripe groups from the tablet metadata.
  size_t numStripeGroups() const {
    return tabletReader_->stripeGroupsMetadata().size();
  }

  /// Returns the number of cached stripe groups.
  size_t cachedStripeGroupCount() const {
    return tabletReader_->stripeGroupCache_.testingCachedCount();
  }

  /// Returns true if the stripe group at the given index is cached.
  bool hasStripeGroupCached(uint32_t groupIndex) const {
    return tabletReader_->stripeGroupCache_.testingHasCachedEntry(groupIndex);
  }

  /// Returns true if the first stripe group is cached and it's the only one.
  /// This is useful for verifying that when the stripe group metadata is
  /// covered by footer IO, the first stripe group is pre-populated in the
  /// cache without additional reads.
  bool hasOnlyFirstStripeGroupCached() const {
    return cachedStripeGroupCount() == 1 && hasStripeGroupCached(0);
  }

  /// Returns the number of cached index groups.
  size_t cachedIndexGroupCount() const {
    return tabletReader_->indexGroupCache_.testingCachedCount();
  }

  /// Returns true if the index group at the given index is cached.
  bool hasIndexGroupCached(uint32_t groupIndex) const {
    return tabletReader_->indexGroupCache_.testingHasCachedEntry(groupIndex);
  }

  /// Returns true if the first index group is cached and it's the only one.
  /// This is useful for verifying that when the index is covered by footer IO,
  /// the first index group is pre-populated in the cache without additional
  /// reads.
  bool hasOnlyFirstIndexGroupCached() const {
    return cachedIndexGroupCount() == 1 && hasIndexGroupCached(0);
  }

  /// Returns the stripe offsets array.
  std::vector<uint64_t> stripeOffsets() const {
    std::vector<uint64_t> offsets;
    offsets.reserve(tabletReader_->stripeCount());
    for (uint32_t i = 0; i < tabletReader_->stripeCount(); ++i) {
      offsets.push_back(tabletReader_->stripeOffsets_[i]);
    }
    return offsets;
  }

  /// Returns the stripe sizes array.
  /// Stripe size is computed as the difference between consecutive offsets,
  /// with the last stripe size computed using the file size.
  std::vector<uint32_t> stripeSizes() const {
    const uint32_t stripeCount = tabletReader_->stripeCount();
    std::vector<uint32_t> sizes;
    sizes.reserve(stripeCount);
    for (uint32_t i = 0; i < stripeCount; ++i) {
      const uint64_t start = tabletReader_->stripeOffsets_[i];
      const uint64_t end = (i + 1 < stripeCount)
          ? tabletReader_->stripeOffsets_[i + 1]
          : tabletReader_->fileSize();
      sizes.push_back(static_cast<uint32_t>(end - start));
    }
    return sizes;
  }

 private:
  const TabletReader* const tabletReader_;
};

} // namespace facebook::nimble::test
