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
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "folly/json.h"

namespace facebook::nimble::index {

namespace {
const serialization::Index* getIndexRoot(const Section& indexSection) {
  const auto* indexRoot =
      flatbuffers::GetRoot<serialization::Index>(indexSection.content().data());
  NIMBLE_CHECK_NOT_NULL(indexRoot);
  return indexRoot;
}

uint32_t getStripeCount(const Section& indexSection) {
  const auto* indexRoot = getIndexRoot(indexSection);
  const auto* stripeKeys = indexRoot->stripe_keys();
  NIMBLE_CHECK_NOT_NULL(stripeKeys);
  return stripeKeys->size() - 1;
}

uint32_t getIndexGroupCount(const Section& indexSection) {
  const auto* indexRoot = getIndexRoot(indexSection);
  const auto* stripeIndexGroups = indexRoot->stripe_index_groups();
  NIMBLE_CHECK_NOT_NULL(stripeIndexGroups);
  return stripeIndexGroups->size();
}

std::vector<std::string_view> getStripeKeys(const Section& indexSection) {
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

std::vector<std::string> getIndexColumns(const Section& indexSection) {
  const auto* indexRoot = getIndexRoot(indexSection);
  const auto* indexColumns = indexRoot->index_columns();
  NIMBLE_CHECK_NOT_NULL(indexColumns);
  std::vector<std::string> result;
  result.reserve(indexColumns->size());
  for (const auto* column : *indexColumns) {
    result.emplace_back(column->string_view());
  }
  return result;
}

std::vector<velox::core::SortOrder> getSortOrders(const Section& indexSection) {
  const auto* indexRoot = getIndexRoot(indexSection);
  const auto* sortOrders = indexRoot->sort_orders();
  NIMBLE_CHECK_NOT_NULL(sortOrders);
  std::vector<velox::core::SortOrder> result;
  result.reserve(sortOrders->size());
  for (const auto* sortOrder : *sortOrders) {
    result.emplace_back(
        velox::core::SortOrder::deserialize(
            folly::parseJson(sortOrder->string_view())));
  }
  return result;
}

} // namespace

std::unique_ptr<TabletIndex> TabletIndex::create(Section indexSection) {
  return std::unique_ptr<TabletIndex>(new TabletIndex(std::move(indexSection)));
}

TabletIndex::TabletIndex(Section indexSection)
    : indexSection_{std::move(indexSection)},
      numStripes_{getStripeCount(indexSection_)},
      numIndexGroups_{getIndexGroupCount(indexSection_)},
      stripeKeys_{getStripeKeys(indexSection_)},
      indexColumns_{getIndexColumns(indexSection_)},
      sortOrders_{getSortOrders(indexSection_)} {
  NIMBLE_CHECK(!indexColumns_.empty());
  NIMBLE_CHECK_EQ(numStripes_ + 1, stripeKeys_.size());
  NIMBLE_CHECK_EQ(indexColumns_.size(), sortOrders_.size());

  // Validate consistency between stripe group indices and stripe index groups
  NIMBLE_CHECK_GE(
      numStripes_,
      numIndexGroups_,
      "Number of stripe index groups cannot exceed number of stripes");
}

std::optional<StripeLocation> TabletIndex::lookup(
    std::string_view encodedKey) const {
  // Check if key is before the first stripe key
  if (encodedKey < stripeKeys_[0]) {
    return std::nullopt;
  }

  // Find the target stripe using binary search
  // We search in stripeKeys_[1..numStripes_] to find the first stripe's last
  // key >= encodedKey.
  const auto it = std::lower_bound(
      stripeKeys_.begin() + 1,
      stripeKeys_.begin() + numStripes_ + 1,
      encodedKey);

  // Calculate which stripe contains the key
  const uint32_t targetStripe = (it - stripeKeys_.begin()) - 1;

  // Check if the key is beyond all stripes
  if (targetStripe >= numStripes_) {
    return std::nullopt;
  }

  return StripeLocation{targetStripe};
}

MetadataSection TabletIndex::groupIndexMetadata(uint32_t groupIndex) const {
  NIMBLE_CHECK_LT(groupIndex, numIndexGroups_);
  const auto* indexRoot = getIndexRoot(indexSection_);
  const auto* indexGroupSections = indexRoot->stripe_index_groups();
  NIMBLE_CHECK_NOT_NULL(indexGroupSections);
  const auto* metadata = indexGroupSections->Get(groupIndex);
  return MetadataSection{
      metadata->offset(),
      metadata->size(),
      static_cast<CompressionType>(metadata->compression_type())};
}

const MetadataBuffer& TabletIndex::rootIndex() const {
  return indexSection_.buffer();
}
} // namespace facebook::nimble::index
