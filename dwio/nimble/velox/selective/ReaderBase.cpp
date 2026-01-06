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

#include "dwio/nimble/velox/selective/ReaderBase.h"

#include "dwio/nimble/index/IndexConstants.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/velox/SchemaSerialization.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "velox/dwio/common/Reader.h"

namespace facebook::nimble {

using namespace facebook::velox;

namespace {
const std::string kSchemaSectionString(kSchemaSection);
const std::vector<std::string> kPreloadOptionalSections = {
    kSchemaSectionString};

std::shared_ptr<const facebook::nimble::Type> loadSchema(
    const TabletReader& tablet) {
  auto section = tablet.loadOptionalSection(kSchemaSectionString);
  NIMBLE_CHECK(section.has_value());
  return SchemaDeserializer::deserialize(section->content().data());
}

TypePtr getFileSchema(
    const dwio::common::ReaderOptions& options,
    const TypePtr& fileSchema) {
  if (options.useColumnNamesForColumnMapping() || !options.fileSchema()) {
    return fileSchema;
  }
  return dwio::common::Reader::updateColumnNames(
      fileSchema, options.fileSchema());
}
} // namespace

std::shared_ptr<ReaderBase> ReaderBase::create(
    std::unique_ptr<velox::dwio::common::BufferedInput> input,
    const velox::dwio::common::ReaderOptions& options) {
  // Initialize all members
  auto tablet = TabletReader::create(
      // TODO: Make TabletReader taking BufferedInput.
      input->getReadFile().get(),
      options.memoryPool(),
      kPreloadOptionalSections);

  auto* pool = &options.memoryPool();
  const auto& randomSkip = options.randomSkip();
  const auto& scanSpec = options.scanSpec();
  const auto nimbleSchema = loadSchema(*tablet);
  auto fileSchema =
      asRowType(getFileSchema(options, convertToVeloxType(*nimbleSchema)));

  return std::shared_ptr<ReaderBase>(new ReaderBase(
      std::move(input),
      std::move(tablet),
      pool,
      randomSkip,
      scanSpec,
      nimbleSchema,
      std::move(fileSchema)));
}

ReaderBase::ReaderBase(
    std::unique_ptr<velox::dwio::common::BufferedInput> input,
    std::shared_ptr<TabletReader> tablet,
    velox::memory::MemoryPool* pool,
    const std::shared_ptr<velox::random::RandomSkipTracker>& randomSkip,
    const std::shared_ptr<velox::common::ScanSpec>& scanSpec,
    std::shared_ptr<const Type> nimbleSchema,
    velox::RowTypePtr fileSchema)
    : input_{std::move(input)},
      tablet_{std::move(tablet)},
      pool_{pool},
      randomSkip_{randomSkip},
      scanSpec_{scanSpec},
      nimbleSchema_{std::move(nimbleSchema)},
      fileSchema_{std::move(fileSchema)} {}

std::optional<common::Region> StripeStreams::streamRegion(int streamId) const {
  NIMBLE_CHECK(stripeIdentifier_.has_value());
  const auto& tablet = readerBase_->tablet();
  if (streamId >= tablet.streamCount(*stripeIdentifier_)) {
    return std::nullopt;
  }
  const auto size = tablet.streamSizes(*stripeIdentifier_)[streamId];
  if (size == 0) {
    return std::nullopt;
  }
  common::Region region;
  region.offset = tablet.stripeOffset(stripe_) +
      tablet.streamOffsets(*stripeIdentifier_)[streamId];
  region.length = size;
  return region;
}

std::unique_ptr<dwio::common::SeekableInputStream> StripeStreams::enqueue(
    int streamId) {
  const auto region = streamRegion(streamId);
  if (!region.has_value()) {
    return nullptr;
  }
  dwio::common::StreamIdentifier sid(streamId);
  return readerBase_->input().enqueue(*region, &sid);
}

std::shared_ptr<index::StreamIndex> StripeStreams::streamIndex(
    int streamId) const {
  NIMBLE_CHECK(stripeIdentifier_.has_value());

  const auto& indexGroup = stripeIdentifier_->indexGroup();
  if (indexGroup == nullptr) {
    return nullptr;
  }
  return indexGroup->createStreamIndex(stripe_, streamId);
}

std::unique_ptr<velox::dwio::common::SeekableInputStream>
StripeStreams::enqueueKeyStream() {
  NIMBLE_CHECK(stripeIdentifier_.has_value());

  const auto& indexGroup = stripeIdentifier_->indexGroup();
  NIMBLE_CHECK_NOT_NULL(indexGroup);

  const auto region = indexGroup->keyStreamRegion(stripe_);
  const dwio::common::StreamIdentifier sid(kKeyStreamId);
  return readerBase_->input().enqueue(region, &sid);
}

} // namespace facebook::nimble
