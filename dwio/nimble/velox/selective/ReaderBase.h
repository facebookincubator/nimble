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
#include "dwio/nimble/velox/SchemaReader.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"

namespace facebook::nimble {

class ReaderBase {
 public:
  ReaderBase(
      std::unique_ptr<velox::dwio::common::BufferedInput> input,
      const velox::dwio::common::ReaderOptions& options);

  velox::dwio::common::BufferedInput& input() {
    return *input_;
  }

  const TabletReader& tablet() const {
    return tablet_;
  }

  velox::memory::MemoryPool* memoryPool() const {
    return memoryPool_;
  }

  const std::shared_ptr<velox::random::RandomSkipTracker>& randomSkip() const {
    return randomSkip_;
  }

  const std::shared_ptr<const Type>& nimbleSchema() const {
    return nimbleSchema_;
  }

  const velox::RowTypePtr& fileSchema() const {
    return fileSchema_;
  }

  const std::shared_ptr<const velox::dwio::common::TypeWithId>&
  fileSchemaWithId() const {
    if (!fileSchemaWithId_) {
      fileSchemaWithId_ = scanSpec_
          ? velox::dwio::common::TypeWithId::create(fileSchema_, *scanSpec_)
          : velox::dwio::common::TypeWithId::create(fileSchema_);
    }
    return fileSchemaWithId_;
  }

 private:
  std::unique_ptr<velox::dwio::common::BufferedInput> const input_;
  const TabletReader tablet_;
  velox::memory::MemoryPool* const memoryPool_;
  std::shared_ptr<velox::random::RandomSkipTracker> const randomSkip_;
  const std::shared_ptr<velox::common::ScanSpec> scanSpec_;
  const std::shared_ptr<const Type> nimbleSchema_;
  const velox::RowTypePtr fileSchema_;
  mutable std::shared_ptr<const velox::dwio::common::TypeWithId>
      fileSchemaWithId_;
};

class StripeStreams {
 public:
  explicit StripeStreams(const std::shared_ptr<ReaderBase>& readerBase)
      : readerBase_(readerBase) {}

  void setStripe(int stripe) {
    stripe_ = stripe;
    stripeIdentifier_ = readerBase_->tablet().getStripeIdentifier(stripe_);
  }

  bool hasStream(int streamId) const {
    return getStreamRegion(streamId).has_value();
  }

  std::unique_ptr<velox::dwio::common::SeekableInputStream> enqueue(
      int streamId);

  void load() {
    readerBase_->input().load(velox::dwio::common::LogType::STREAM_BUNDLE);
  }

 private:
  std::optional<velox::common::Region> getStreamRegion(int streamId) const;

  std::shared_ptr<ReaderBase> const readerBase_;
  int stripe_;
  std::optional<TabletReader::StripeIdentifier> stripeIdentifier_;
};

} // namespace facebook::nimble
