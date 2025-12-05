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

#include <memory>
#include <optional>
#include <string_view>
#include "dwio/nimble/index/KeyWriter.h"
#include "dwio/nimble/tablet/TabletReader.h"

namespace facebook::nimble {

struct RowRange {
  int64_t startRow;
  int64_t endRow;

  bool operator==(const RowRange& other) const {
    return startRow == other.startRow && endRow == other.endRow;
  }
};

class IndexReader {
 public:
  IndexReader(
      std::shared_ptr<TabletIndex> index,
      std::unique_ptr<KeyWriter> keyEncoder);

  // Query the index with bounds, returns row range if found.
  // Returns std::nullopt if the bounds are completely out of range.
  // lowerBound: inclusive lower bound key (encoded)
  // upperBound: inclusive upper bound key (encoded)
  std::optional<RowRange> query(
      std::string_view lowerBound,
      std::string_view upperBound) const;

 private:
  // Find the first stripe whose key is >= the given key
  // Returns the stripe index, or numStripes if all stripes are < key
  size_t findFirstStripeGE(std::string_view key) const;

  // Find the last stripe whose key is <= the given key
  // Returns the stripe index, or -1 if all stripes are > key
  int64_t findLastStripeLE(std::string_view key) const;

  // Calculate the starting row number for a given stripe
  int64_t getStripeStartRow(size_t stripeIndex) const;

  std::shared_ptr<TabletIndex> index_;
  std::unique_ptr<KeyWriter> keyEncoder_;
};

} // namespace facebook::nimble
