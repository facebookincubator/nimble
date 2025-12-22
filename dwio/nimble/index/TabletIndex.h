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

#include <optional>
#include <string_view>
#include <vector>
#include "dwio/nimble/tablet/MetadataBuffer.h"

namespace facebook::nimble {

/// Represents the global stripe index within a tablet.
struct StripeLocation {
  explicit StripeLocation(uint32_t _stripeIndex) : stripeIndex(_stripeIndex) {}

  uint32_t stripeIndex;

  bool operator==(const StripeLocation& other) const {
    return stripeIndex == other.stripeIndex;
  }
};

/// TabletIndex provides efficient key-based stripe lookup for Nimble tablets.
///
/// The index uses a flatbuffer-backed serialized index structure that maps
/// encoded keys to stripe locations. Internally, the class maintains minimal
/// cached state (stripe count and stripe keys) while accessing other metadata
/// on-demand from the underlying flatbuffer for optimal memory efficiency.
///
/// Key features:
/// - Binary search-based stripe lookup using cached stripe keys
/// - On-demand access to index group metadata
/// - Validates index structure consistency during construction
///
/// Example usage:
///   ...
///   auto index = TabletIndex::create(std::move(indexSection));
///   auto location = index->lookup(encodedKey);
///   if (location) {
///     auto stripeGroupIndex =
///     tabletReader.stripeGroupIndex(location->stripeIndex); auto metadata =
///     index->indexGroupMetadata(stripeGroupIndex);
///   }
///   ... read stripe data ...
///
class TabletIndex {
 public:
  /// Creates a TabletIndex for efficient key-based stripe lookups.
  ///
  /// @param indexSection The index section buffer containing the serialized
  ///        index data
  /// @return A unique pointer to a newly created TabletIndex
  static std::unique_ptr<TabletIndex> create(
      std::unique_ptr<MetadataBuffer> indexSection);

  /// Looks up the stripe location containing the specified encoded key using
  /// binary search.
  ///
  /// The lookup finds the stripe whose key range includes the given encodedKey.
  /// Returns std::nullopt if the key is outside all stripe ranges (before the
  /// first stripe key or after the last stripe key).
  ///
  /// @param encodedKey The binary-encoded search key
  /// @return StripeLocation containing stripe index if found, std::nullopt
  ///         otherwise
  std::optional<StripeLocation> lookup(std::string_view encodedKey) const;

  /// Returns the minimum key across all stripes (first stripe key).
  ///
  /// @return The encoded minimum key value
  std::string_view minKey() const {
    return stripeKeys_.front();
  }

  /// Returns the maximum key across all stripes (last stripe key).
  ///
  /// @return The encoded maximum key value
  std::string_view maxKey() const {
    return stripeKeys_.back();
  }

  /// Returns the encoded key for a specific stripe.
  ///
  /// @param stripeIndex The zero-based stripe index
  /// @return The encoded key for the specified stripe
  std::string_view stripeKey(size_t stripeIndex) const {
    return stripeKeys_[stripeIndex + 1];
  }

  /// Returns the total number of stripes in the tablet.
  ///
  /// @return Number of stripes
  size_t numStripes() const {
    return numStripes_;
  }

  /// Returns the index columns.
  ///
  /// @return Vector of index column names
  const std::vector<std::string>& indexColumns() const {
    return indexColumns_;
  }

  /// Returns metadata for a specific index group.
  ///
  /// @param groupIndex The zero-based stripe group index.
  /// @return Metadata section containing offset, size, and
  ///         compression info for the stripe index group
  MetadataSection indexGroupMetadata(uint32_t groupIndex) const;

 private:
  explicit TabletIndex(std::unique_ptr<MetadataBuffer> indexSection);

  const std::unique_ptr<MetadataBuffer> indexSection_;
  const uint32_t numStripes_;
  const std::vector<std::string_view> stripeKeys_;
  const std::vector<std::string> indexColumns_;
};

} // namespace facebook::nimble
