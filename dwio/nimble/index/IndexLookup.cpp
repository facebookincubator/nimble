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
#include "dwio/nimble/index/IndexLookup.h"

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/tablet/MetadataInput.h"
#include "velox/common/caching/FileHandle.h"
#include "velox/common/io/Options.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/DirectBufferedInput.h"

namespace facebook::nimble::index {

void IndexLookup::Options::validate() const {
  NIMBLE_CHECK_NOT_NULL(file.get());
  NIMBLE_CHECK_NOT_NULL(ioOptions);
  NIMBLE_CHECK(
      (fileHandle != nullptr) == (cache != nullptr),
      "fileHandle and cache must both be set or neither");
  if (fileHandle != nullptr) {
    NIMBLE_CHECK_EQ(
        static_cast<const void*>(file.get()),
        static_cast<const void*>(fileHandle->file.get()),
        "file and fileHandle->file must point to the same file");
  }
}

std::unique_ptr<MetadataInput> createIndexMetadataInput(
    const IndexLookup::Options& options) {
  return (options.fileHandle != nullptr)
      ? MetadataInput::create(
            options.fileHandle, options.cache, *options.ioOptions)
      : MetadataInput::create(options.file.get(), *options.ioOptions);
}

std::unique_ptr<velox::dwio::common::BufferedInput> createIndexDataInput(
    const IndexLookup::Options& options,
    velox::memory::MemoryPool& pool) {
  auto readFile = options.file;
  auto* rawStats = options.ioOptions->indexIoStats();
  auto indexIoStats = rawStats != nullptr
      ? std::shared_ptr<velox::io::IoStatistics>(
            std::shared_ptr<velox::io::IoStatistics>{}, rawStats)
      : std::make_shared<velox::io::IoStatistics>();
  if (options.cache != nullptr) {
    return std::make_unique<velox::dwio::common::CachedBufferedInput>(
        std::move(readFile),
        velox::dwio::common::MetricsLog::voidLog(),
        options.fileHandle->uuid,
        options.cache,
        nullptr,
        velox::StringIdLease(),
        std::move(indexIoStats),
        nullptr,
        nullptr,
        *options.ioOptions);
  }
  return std::make_unique<velox::dwio::common::DirectBufferedInput>(
      std::move(readFile),
      velox::dwio::common::MetricsLog::voidLog(),
      velox::StringIdLease(),
      nullptr,
      velox::StringIdLease(),
      std::move(indexIoStats),
      nullptr,
      nullptr,
      *options.ioOptions);
}

std::string toString(IndexType indexType) {
  switch (indexType) {
    case IndexType::Cluster:
      return "Cluster";
    case IndexType::Hash:
      return "Hash";
    case IndexType::Sorted:
      return "Sorted";
    default:
      NIMBLE_UNREACHABLE("Unknown IndexType: {}", static_cast<int>(indexType));
  }
}

std::ostream& operator<<(std::ostream& out, IndexType indexType) {
  return out << toString(indexType);
}

} // namespace facebook::nimble::index
