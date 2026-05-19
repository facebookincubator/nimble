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
#include "dwio/nimble/index/DenseIndexRegistry.h"

#include <fmt/ranges.h>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/index/HashIndex.h"
#include "dwio/nimble/index/SortedIndex.h"
#include "dwio/nimble/tablet/HashIndexGenerated.h"
#include "dwio/nimble/tablet/MetadataInput.h"
#include "dwio/nimble/tablet/SortedIndexGenerated.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::nimble::index {

namespace {

bool columnsMatch(
    const std::vector<std::string>& indexColumns,
    const std::vector<std::string>& queryColumns) {
  if (indexColumns.size() != queryColumns.size()) {
    return false;
  }
  for (size_t i = 0; i < indexColumns.size(); ++i) {
    if (indexColumns[i] != queryColumns[i]) {
      return false;
    }
  }
  return true;
}

} // namespace

template <typename FbDirectory>
std::vector<DenseIndexRegistry::IndexDescriptor>
DenseIndexRegistry::parseDescriptors(
    const Section& directorySection,
    const std::string& indexTypeName) {
  const auto* root =
      flatbuffers::GetRoot<FbDirectory>(directorySection.content().data());
  NIMBLE_CHECK_NOT_NULL(root);
  const auto* indices = root->indices();
  NIMBLE_CHECK_NOT_NULL(indices);

  std::vector<DenseIndexRegistry::IndexDescriptor> descriptors;
  descriptors.reserve(indices->size());
  for (const auto* indexSection : *indices) {
    NIMBLE_CHECK_NOT_NULL(indexSection);
    const auto* columns = indexSection->index_columns();
    NIMBLE_CHECK_NOT_NULL(columns);
    NIMBLE_CHECK_GT(columns->size(), 0u, "{} must have columns", indexTypeName);

    std::vector<std::string> columnNames;
    columnNames.reserve(columns->size());
    for (const auto* col : *columns) {
      columnNames.emplace_back(col->string_view());
    }

    for (const auto& descriptor : descriptors) {
      NIMBLE_CHECK(
          !columnsMatch(descriptor.columns, columnNames),
          "Duplicate {} columns: [{}]",
          indexTypeName,
          fmt::join(columnNames, ", "));
    }

    const auto* sectionRef = indexSection->section();
    NIMBLE_CHECK_NOT_NULL(sectionRef);
    descriptors.emplace_back(
        DenseIndexRegistry::IndexDescriptor{
            std::move(columnNames),
            MetadataSection{
                sectionRef->offset(),
                sectionRef->size(),
                static_cast<CompressionType>(sectionRef->compression_type())}});
  }
  return descriptors;
}

std::unique_ptr<DenseIndexRegistry> DenseIndexRegistry::create(
    std::optional<Section> hashSection,
    std::optional<Section> sortedSection,
    const IndexLookup::Options& options,
    velox::memory::MemoryPool* pool) {
  options.validate();
  if (!hashSection.has_value() && !sortedSection.has_value()) {
    return nullptr;
  }
  auto metadataInput = createIndexMetadataInput(options);
  auto dataInput = createIndexDataInput(options);
  auto registry = std::unique_ptr<DenseIndexRegistry>(new DenseIndexRegistry());
  if (hashSection.has_value()) {
    registry->registerHashIndices(
        std::move(hashSection.value()), metadataInput, pool);
  }
  if (sortedSection.has_value()) {
    registry->registerSortedIndices(
        std::move(sortedSection.value()), metadataInput, dataInput, pool);
  }
  registry->validate();
  return registry;
}

void DenseIndexRegistry::validate() const {
  for (const auto& hashDescriptor : hashDescriptors_) {
    for (const auto& sortedDescriptor : sortedDescriptors_) {
      NIMBLE_CHECK(
          !columnsMatch(hashDescriptor.columns, sortedDescriptor.columns),
          "Duplicate dense index columns across hash and sorted: [{}]",
          fmt::join(hashDescriptor.columns, ", "));
    }
  }
}

void DenseIndexRegistry::registerHashIndices(
    Section directorySection,
    std::shared_ptr<MetadataInput> metadataInput,
    velox::memory::MemoryPool* pool) {
  hashDescriptors_ = parseDescriptors<serialization::HashIndexDirectory>(
      directorySection, "Hash index");

  // Always pin: findHashIndex returns raw pointers, so entries must stay alive.
  hashIndexCache_ = std::make_unique<MetadataCache<uint32_t, HashIndex>>(
      [this, metadataInput, pool](
          uint32_t index) -> std::shared_ptr<HashIndex> {
        const auto& descriptor = hashDescriptors_[index];
        auto results = metadataInput->load({&descriptor.section, 1});
        NIMBLE_CHECK_EQ(results.size(), 1);
        auto indexMetadata =
            std::make_unique<MetadataBuffer>(std::move(*results.front()));
        return HashIndex::create(
            descriptor.columns, std::move(indexMetadata), metadataInput, pool);
      },
      /*pinEntries=*/true);
}

void DenseIndexRegistry::registerSortedIndices(
    Section directorySection,
    std::shared_ptr<MetadataInput> metadataInput,
    std::shared_ptr<velox::dwio::common::BufferedInput> dataInput,
    velox::memory::MemoryPool* pool) {
  sortedDescriptors_ = parseDescriptors<serialization::SortedIndexDirectory>(
      directorySection, "Sorted index");

  // Always pin: findSortedIndex returns raw pointers, so entries must stay
  // alive.
  sortedIndexCache_ = std::make_unique<MetadataCache<uint32_t, SortedIndex>>(
      [this, metadataInput, dataInput, pool](
          uint32_t index) -> std::shared_ptr<SortedIndex> {
        const auto& descriptor = sortedDescriptors_[index];
        auto results = metadataInput->load({&descriptor.section, 1});
        NIMBLE_CHECK_EQ(results.size(), 1);
        auto indexMetadata =
            std::make_unique<MetadataBuffer>(std::move(*results.front()));
        return SortedIndex::create(
            descriptor.columns, std::move(indexMetadata), dataInput, pool);
      },
      /*pinEntries=*/true);
}

const HashIndex* DenseIndexRegistry::findHashIndex(
    const std::vector<std::string>& queryColumns) const {
  for (uint32_t i = 0; i < hashDescriptors_.size(); ++i) {
    if (columnsMatch(hashDescriptors_[i].columns, queryColumns)) {
      return hashIndexCache_->getOrCreate(i).get();
    }
  }
  return nullptr;
}

const SortedIndex* DenseIndexRegistry::findSortedIndex(
    const std::vector<std::string>& queryColumns) const {
  for (uint32_t i = 0; i < sortedDescriptors_.size(); ++i) {
    if (columnsMatch(sortedDescriptors_[i].columns, queryColumns)) {
      return sortedIndexCache_->getOrCreate(i).get();
    }
  }
  return nullptr;
}

const IndexLookup* DenseIndexRegistry::findIndex(
    const std::vector<std::string>& queryColumns) const {
  if (auto* hashIdx = findHashIndex(queryColumns)) {
    return hashIdx;
  }
  return findSortedIndex(queryColumns);
}

} // namespace facebook::nimble::index
