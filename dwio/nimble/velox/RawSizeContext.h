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

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/velox/DecodedVectorManager.h"

namespace facebook::nimble {

class RawSizeContext {
 public:
  RawSizeContext() = default;

  DecodedVectorManager& getDecodedVectorManager() {
    return decodedVectorManager_;
  }

  void appendSize(uint64_t size) {
    columnSizes_.push_back(size);
  }

  uint64_t sizeAt(uint64_t columnIndex) const {
    NIMBLE_ASSERT(
        columnIndex < columnSizes_.size(),
        fmt::format(
            "Column index {} is out of range. Total number of columns is {}",
            columnIndex,
            columnSizes_.size()));
    return columnSizes_.at(columnIndex);
  }

  void setSizeAt(uint64_t columnIndex, uint64_t size) {
    NIMBLE_ASSERT(
        columnIndex < columnSizes_.size(),
        fmt::format(
            "Column index {} is out of range. Total number of columns is {}",
            columnIndex,
            columnSizes_.size()));
    columnSizes_[columnIndex] = size;
  }

  uint64_t columnCount() const {
    return columnSizes_.size();
  }

  void appendNullCount(uint64_t nulls) {
    columnNullCounts_.push_back(nulls);
  }

  uint64_t nullsAt(uint64_t columnIndex) const {
    NIMBLE_ASSERT(
        columnIndex < columnNullCounts_.size(),
        fmt::format(
            "Column index {} is out of range. Total number of columns is {}",
            columnIndex,
            columnNullCounts_.size()));
    return columnNullCounts_.at(columnIndex);
  }

  void setNullsAt(uint64_t columnIndex, uint64_t nulls) {
    NIMBLE_ASSERT(
        columnIndex < columnNullCounts_.size(),
        fmt::format(
            "Column index {} is out of range. Total number of columns is {}",
            columnIndex,
            columnNullCounts_.size()));
    columnNullCounts_[columnIndex] = nulls;
  }

  // Number of nulls in last visited node
  uint64_t nullCount{0};

 private:
  DecodedVectorManager decodedVectorManager_;
  std::vector<uint64_t> columnSizes_;
  std::vector<uint64_t> columnNullCounts_;
};

} // namespace facebook::nimble
