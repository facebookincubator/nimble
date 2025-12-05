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

#include "dwio/nimble/index/TabletIndex.h"
#include <algorithm>
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/tablet/IndexGenerated.h"

namespace facebook::nimble {

namespace {
const serialization::Index* getIndexRoot(const MetadataBuffer& indexSection) {
  const auto* indexRoot =
      flatbuffers::GetRoot<serialization::Index>(indexSection.content().data());
  NIMBLE_CHECK_NOT_NULL(indexRoot);
  return indexRoot;
}

uint32_t getStripeCount(const MetadataBuffer& indexSection) {
  const auto* indexRoot = getIndexRoot(indexSection);
  const auto* stripeRows = indexRoot->stripe_row_counts();
  NIMBLE_CHECK_NOT_NULL(stripeRows);
  return stripeRows->size();
}

std::vector<std::string_view> getStripeKeys(
    const MetadataBuffer& indexSection) {
  const auto* indexRoot = getIndexRoot(indexSection);
  const auto* stripeKeys = indexRoot->stripe_keys();
  NIMBLE_CHECK_NOT_NULL(stripeKeys);
  std::vector<std::string_view> result;
  result.reserve(stripeKeys->size());
  for (const auto* stripeKey : *stripeKeys) {
    result.emplace_back(stripeKey->string_view());
  }
  return result;
}

std::vector<std::string> getIndexColumns(const MetadataBuffer& indexSection) {
  const auto* indexRoot = getIndexRoot(indexSection);
  const auto* indexColumns = indexRoot->index_columns();
  if (indexColumns == nullptr) {
    return {};
  }
  std::vector<std::string> result;
  result.reserve(indexColumns->size());
  for (const auto* column : *indexColumns) {
    result.emplace_back(column->str(), column->size());
  }
  return result;
}

} // namespace

std::unique_ptr<TabletIndex> TabletIndex::create(
    std::unique_ptr<MetadataBuffer> indexSection) {
  return std::unique_ptr<TabletIndex>(new TabletIndex(std::move(indexSection)));
}

TabletIndex::TabletIndex(std::unique_ptr<MetadataBuffer> indexSection)
    : indexSection_{std::move(indexSection)},
      numStripes_{getStripeCount(*indexSection_)},
      stripeKeys_{getStripeKeys(*indexSection_)},
      indexColumns_{getIndexColumns(*indexSection_)} {
  NIMBLE_CHECK_EQ(numStripes_ + 1, stripeKeys_.size());

  // Validate consistency between stripe group indices and stripe index groups
  const auto* indexRoot = getIndexRoot(*indexSection_);
  const auto* stripeGroupIndices = indexRoot->stripe_group_indices();
  const auto* stripeIndexGroups = indexRoot->stripe_index_groups();
  NIMBLE_CHECK_NOT_NULL(stripeGroupIndices);
  NIMBLE_CHECK_NOT_NULL(stripeIndexGroups);
  NIMBLE_CHECK_EQ(
      stripeGroupIndices->size(),
      numStripes_,
      "Stripe group indices size must match number of stripes");
  NIMBLE_CHECK_GE(
      numStripes_,
      stripeIndexGroups->size(),
      "Number of stripe index groups cannot exceed number of stripes");
}

std::optional<StripeLocation> TabletIndex::lookup(
    std::string_view encodedKey) const {
  // Check if key is before the first stripe key
  if (encodedKey < stripeKeys_[0]) {
    return std::nullopt;
  }

  // Find the target stripe using binary search
  // We search in stripeKeys_[1..numStripes_] to find the first stripe key >
  // encodedKey upper_bound returns iterator to first element > key
  auto it = std::upper_bound(
      stripeKeys_.begin() + 1,
      stripeKeys_.begin() + numStripes_ + 1,
      encodedKey);

  // Calculate which stripe contains the key
  const uint32_t targetStripe = (it - stripeKeys_.begin()) - 1;

  // Check if the key is beyond all stripes
  if (targetStripe >= numStripes_) {
    return std::nullopt;
  }

  const auto* indexRoot = getIndexRoot(*indexSection_);
  const auto* stripeGroupIndices = indexRoot->stripe_group_indices();
  const uint32_t stripeGroupIndex = stripeGroupIndices->data()[targetStripe];
  return StripeLocation{targetStripe, stripeGroupIndex};
}

MetadataSection TabletIndex::stripeIndexGroupMetadata(
    uint32_t stripeIndexGroupId) const {
  const auto* indexRoot = getIndexRoot(*indexSection_);
  const auto* stripeIndexGroupMetadata = indexRoot->stripe_index_groups();
  NIMBLE_CHECK_NOT_NULL(stripeIndexGroupMetadata);
  NIMBLE_CHECK_LT(stripeIndexGroupId, stripeIndexGroupMetadata->size());
  const auto* metadata = stripeIndexGroupMetadata->Get(stripeIndexGroupId);
  return MetadataSection{
      metadata->offset(),
      metadata->size(),
      static_cast<CompressionType>(metadata->compression_type())};
}
} // namespace facebook::nimble
