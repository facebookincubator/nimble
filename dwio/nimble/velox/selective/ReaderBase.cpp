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
  VELOX_CHECK(section.has_value());
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

ReaderBase::ReaderBase(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options)
    : input_(std::move(input)),
      tablet_(
          options.memoryPool(),
          // TODO: Make TabletReader taking BufferedInput.
          input_->getReadFile().get(),
          kPreloadOptionalSections),
      memoryPool_(&options.memoryPool()),
      randomSkip_(options.randomSkip()),
      scanSpec_(options.scanSpec()),
      nimbleSchema_(loadSchema(tablet_)),
      fileSchema_(asRowType(
          getFileSchema(options, convertToVeloxType(*nimbleSchema_)))) {}

std::optional<common::Region> StripeStreams::getStreamRegion(
    int streamId) const {
  auto& tablet = readerBase_->tablet();
  VELOX_CHECK(stripeIdentifier_.has_value());
  if (streamId >= tablet.streamCount(*stripeIdentifier_)) {
    return std::nullopt;
  }
  auto size = tablet.streamSizes(*stripeIdentifier_)[streamId];
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
  auto region = getStreamRegion(streamId);
  if (!region.has_value()) {
    return nullptr;
  }
  dwio::common::StreamIdentifier sid(streamId);
  return readerBase_->input().enqueue(*region, &sid);
}

} // namespace facebook::nimble
