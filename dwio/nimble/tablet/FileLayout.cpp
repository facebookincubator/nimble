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

#include "dwio/nimble/tablet/FileLayout.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "velox/common/file/FileSystems.h"

#include <numeric>

namespace facebook::nimble {

FileLayout FileLayout::create(
    std::shared_ptr<velox::ReadFile> file,
    velox::memory::MemoryPool* pool) {
  auto tablet = TabletReader::create(std::move(file), pool, {});

  FileLayout layout;
  layout.fileSize = tablet->fileSize();

  // Postscript and footer
  layout.postscript = tablet->postscript();
  layout.footer = MetadataSection{
      layout.fileSize - layout.postscript.footerSize() - kPostscriptSize,
      layout.postscript.footerSize(),
      layout.postscript.footerCompressionType()};

  // Stripes metadata
  auto stripesMetadata = tablet->stripesMetadata();
  if (stripesMetadata.has_value()) {
    layout.stripes = *stripesMetadata;
  }

  // Stripe groups
  layout.stripeGroups = tablet->stripeGroupsMetadata();

  // Index groups (from ClusterIndex if present)
  const auto* clusterIndex = tablet->clusterIndex();
  if (clusterIndex != nullptr) {
    layout.indexGroups.reserve(clusterIndex->numIndexGroups());
    for (size_t i = 0; i < clusterIndex->numIndexGroups(); ++i) {
      layout.indexGroups.push_back(clusterIndex->groupMetadata(i));
    }
  }

  // Per-stripe info
  const auto stripeCount = tablet->stripeCount();
  layout.stripesInfo.reserve(stripeCount);
  for (uint32_t i = 0; i < stripeCount; ++i) {
    auto stripeIdentifier = tablet->stripeIdentifier(i);
    auto sizes = tablet->streamSizes(stripeIdentifier);
    auto stripeSize = std::accumulate(sizes.begin(), sizes.end(), 0UL);
    layout.stripesInfo.push_back({
        .offset = tablet->stripeOffset(i),
        .size = stripeSize,
        .stripeGroupIndex = stripeIdentifier.stripeGroup()->index(),
    });
  }

  // Optional sections
  layout.optionalSections = tablet->optionalSections();

  return layout;
}

FileLayout FileLayout::create(
    const std::string& path,
    velox::memory::MemoryPool* pool) {
  auto fs = velox::filesystems::getFileSystem(path, nullptr);
  auto file = fs->openFileForRead(path);
  return create(std::move(file), pool);
}

} // namespace facebook::nimble
