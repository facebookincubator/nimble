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

#include "dwio/nimble/velox/IndexReader.h"
#include <algorithm>

namespace facebook::nimble {

IndexReader::IndexReader(
    std::shared_ptr<TabletIndex> index,
    std::unique_ptr<KeyWriter> keyEncoder)
    : index_{std::move(index)}, keyEncoder_{std::move(keyEncoder)} {
  NIMBLE_CHECK_NOT_NULL(index_);
  NIMBLE_CHECK_NOT_NULL(keyEncoder_);
}

std::optional<RowRange> IndexReader::query(
    std::string_view lowerBound,
    std::string_view upperBound) const {
  // Find the first stripe with key >= lowerBound
  const size_t startStripe = findFirstStripeGE(lowerBound);

  // Find the last stripe with key <= upperBound
  const int64_t endStripe = findLastStripeLE(upperBound);

  // Check if the range is valid
  if (endStripe < 0 || startStripe >= index_->numStripes() ||
      static_cast<int64_t>(startStripe) > endStripe) {
    return std::nullopt;
  }

  // Calculate row range
  const int64_t startRow = getStripeStartRow(startStripe);
  const int64_t endRow =
      getStripeStartRow(endStripe) + index_->stripeRows()[endStripe];

  return RowRange{startRow, endRow};
}

size_t IndexReader::findFirstStripeGE(std::string_view key) const {
  // Binary search for first stripe with key >= search key
  size_t left = 0;
  size_t right = index_->numStripes();

  while (left < right) {
    const size_t mid = left + (right - left) / 2;
    const auto& stripeKey = index_->stripeKeys()[mid];

    if (stripeKey < key) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  return left;
}

int64_t IndexReader::findLastStripeLE(std::string_view key) const {
  // Binary search for last stripe with key <= search key
  int64_t left = 0;
  int64_t right = static_cast<int64_t>(index_->numStripes()) - 1;
  int64_t result = -1;

  while (left <= right) {
    const int64_t mid = left + (right - left) / 2;
    const auto& stripeKey = index_->stripeKeys()[mid];

    if (stripeKey <= key) {
      result = mid;
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  return result;
}

int64_t IndexReader::getStripeStartRow(size_t stripeIndex) const {
  NIMBLE_DCHECK(
      stripeIndex < index_->numStripes(),
      "Stripe index out of bounds: {} >= {}",
      stripeIndex,
      index_->numStripes());

  int64_t startRow = 0;
  for (size_t i = 0; i < stripeIndex; ++i) {
    startRow += index_->stripeRows()[i];
  }
  return startRow;
}

} // namespace facebook::nimble
