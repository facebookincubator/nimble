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

#include <folly/container/F14Set.h>
#include "dwio/nimble/velox/selective/ColumnLoader.h"
#include "dwio/nimble/velox/selective/NimbleData.h"
#include "dwio/nimble/velox/selective/RowSizeTracker.h"
#include "velox/dwio/common/SelectiveStructColumnReader.h"

namespace facebook::nimble {

class StructColumnReaderBase
    : public velox::dwio::common::SelectiveStructColumnReaderBase {
 public:
  StructColumnReaderBase(
      const velox::TypePtr& requestedType,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& fileType,
      NimbleParams& params,
      velox::common::ScanSpec& scanSpec,
      bool isRoot)
      : SelectiveStructColumnReaderBase(
            velox::dwio::common::ColumnReaderOptions{},
            requestedType,
            fileType,
            params,
            scanSpec,
            isRoot),
        rowSizeTracker_{params.rowSizeTracker()} {
    VELOX_CHECK_EQ(fileType_->id(), fileType->id());
  }

  void seekTo(int64_t offset, bool readsNullsOnly) override;

  void seekToRowGroup(int64_t /*index*/) final {
    VELOX_UNREACHABLE();
  }

  void advanceFieldReader(SelectiveColumnReader* /*reader*/, int64_t /*offset*/)
      final {
    // No-op, there is no index for fast skipping and we need to skip in the
    // decoders.
  }

  std::unique_ptr<velox::dwio::common::ColumnLoader> makeColumnLoader(
      velox::vector_size_t index) override {
    for (const auto& childSpec : scanSpec_->children()) {
      if (childSpec->subscript() == index && childSpec->hasTransform() &&
          childSpec->extractionType() ==
              velox::common::ScanSpec::ExtractionType::kNone) {
        return std::make_unique<velox::dwio::common::TransformColumnLoader>(
            this, children_[index], numReads_, childSpec->transform());
      }
    }
    return std::make_unique<nimble::TrackedColumnLoader>(
        this, children_[index], numReads_, rowSizeTracker_);
  }

 protected:
  RowSizeTracker* const rowSizeTracker_;
};

class StructColumnReader : public StructColumnReaderBase {
 public:
  StructColumnReader(
      const velox::TypePtr& requestedType,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& fileType,
      NimbleParams& params,
      velox::common::ScanSpec& scanSpec,
      bool isRoot);

  bool estimateMaterializedSize(size_t& byteSize, size_t& rowCount) const final;

 private:
  // Creates a column loader for lazy I/O columns. Lazy columns get a
  // TrackedColumnLoader that triggers LazyInput::load() on first access;
  // non-lazy columns fall through to the base class loader.
  std::unique_ptr<velox::dwio::common::ColumnLoader> makeColumnLoader(
      velox::vector_size_t index) override {
    if (lazyInput_ != nullptr) {
      for (const auto& childSpec : scanSpec_->children()) {
        if (childSpec->subscript() == index &&
            lazyIoColumns_->count(childSpec->fieldName()) > 0) {
          return std::make_unique<nimble::TrackedColumnLoader>(
              this, children_[index], numReads_, rowSizeTracker_, lazyInput_);
        }
      }
    }
    return StructColumnReaderBase::makeColumnLoader(index);
  }

  void addChild(std::unique_ptr<SelectiveColumnReader> child) {
    children_.push_back(child.get());
    childrenOwned_.push_back(std::move(child));
  }

  std::vector<std::unique_ptr<SelectiveColumnReader>> childrenOwned_;
  // Lazy input for all lazy I/O columns in this stripe. Null when
  // no columns use lazy I/O. Owned by StripeStreams.
  LazyInput* lazyInput_{nullptr};
  // Set of top-level column names eligible for lazy I/O. Pointer to the
  // const set owned by SelectiveNimbleRowReader.
  const folly::F14FastSet<std::string>* lazyIoColumns_{nullptr};
};

} // namespace facebook::nimble
