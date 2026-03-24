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

#include "dwio/nimble/index/ClusterIndex.h"

#include <algorithm>
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/tablet/ClusterIndexGenerated.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "folly/json/json.h"

namespace facebook::nimble::index {

namespace {
const serialization::ClusterIndex* getIndexRoot(const Section& indexSection) {
  const auto* indexRoot = flatbuffers::GetRoot<serialization::ClusterIndex>(
      indexSection.content().data());
  NIMBLE_CHECK_NOT_NULL(indexRoot);
  return indexRoot;
}

uint32_t getStripeCount(const serialization::ClusterIndex* indexRoot) {
  const auto* stripeKeys = indexRoot->stripe_keys();
  NIMBLE_CHECK_NOT_NULL(stripeKeys);
  // Empty file case: no stripe keys means no stripes.
  if (stripeKeys->size() == 0) {
    return 0;
  }
  return stripeKeys->size() - 1;
}

uint32_t getIndexGroupCount(const serialization::ClusterIndex* indexRoot) {
  const auto* stripeIndexes = indexRoot->stripe_indexes();
  NIMBLE_CHECK_NOT_NULL(stripeIndexes);
  return stripeIndexes->size();
}

std::vector<std::string_view> getStripeKeys(
    const serialization::ClusterIndex* indexRoot) {
  const auto* stripeKeys = indexRoot->stripe_keys();
  NIMBLE_CHECK_NOT_NULL(stripeKeys);
  std::vector<std::string_view> result;
  result.reserve(stripeKeys->size());
  for (const auto* stripeKey : *stripeKeys) {
    result.emplace_back(stripeKey->string_view());
  }
  return result;
}

std::vector<std::string> getIndexColumns(
    const serialization::ClusterIndex* indexRoot) {
  const auto* indexColumns = indexRoot->index_columns();
  NIMBLE_CHECK_NOT_NULL(indexColumns);
  std::vector<std::string> result;
  result.reserve(indexColumns->size());
  for (const auto* column : *indexColumns) {
    result.emplace_back(column->string_view());
  }
  return result;
}

std::vector<SortOrder> getSortOrders(
    const serialization::ClusterIndex* indexRoot) {
  const auto* sortOrders = indexRoot->sort_orders();
  NIMBLE_CHECK_NOT_NULL(sortOrders);
  std::vector<SortOrder> result;
  result.reserve(sortOrders->size());
  for (const auto* sortOrder : *sortOrders) {
    result.emplace_back(
        SortOrder::deserialize(folly::parseJson(sortOrder->string_view())));
  }
  return result;
}

} // namespace

std::unique_ptr<ClusterIndex> ClusterIndex::create(Section indexSection) {
  return std::unique_ptr<ClusterIndex>(
      new ClusterIndex(std::move(indexSection)));
}

ClusterIndex::ClusterIndex(Section indexSection)
    : indexSection_{std::move(indexSection)},
      indexRoot_{getIndexRoot(indexSection_)},
      numStripes_{getStripeCount(indexRoot_)},
      numIndexGroups_{getIndexGroupCount(indexRoot_)},
      stripeKeys_{getStripeKeys(indexRoot_)},
      indexColumns_{getIndexColumns(indexRoot_)},
      sortOrders_{getSortOrders(indexRoot_)} {
  NIMBLE_CHECK(!indexColumns_.empty());
  // For empty files: numStripes_ == 0 and stripeKeys_.size() == 0.
  // For non-empty files: stripeKeys_.size() == numStripes_ + 1.
  if (numStripes_ > 0) {
    NIMBLE_CHECK_EQ(numStripes_ + 1, stripeKeys_.size());
  } else {
    NIMBLE_CHECK(stripeKeys_.empty());
  }
  NIMBLE_CHECK_EQ(indexColumns_.size(), sortOrders_.size());

  // Validate consistency between stripe group indices and stripe index groups
  NIMBLE_CHECK_GE(
      numStripes_,
      numIndexGroups_,
      "Number of stripe index groups cannot exceed number of stripes");
}

std::optional<StripeLocation> ClusterIndex::lookup(
    std::string_view encodedKey) const {
  // Handle empty index case.
  if (numStripes_ == 0) {
    return std::nullopt;
  }

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

MetadataSection ClusterIndex::groupMetadata(uint32_t groupIndex) const {
  NIMBLE_CHECK_LT(groupIndex, numIndexGroups_);
  const auto* indexGroupSections = indexRoot_->stripe_indexes();
  NIMBLE_CHECK_NOT_NULL(indexGroupSections);
  const auto* metadata = indexGroupSections->Get(groupIndex);
  return MetadataSection{
      metadata->offset(),
      metadata->size(),
      static_cast<CompressionType>(metadata->compression_type())};
}

const MetadataBuffer& ClusterIndex::rootIndex() const {
  return indexSection_.buffer();
}
} // namespace facebook::nimble::index
