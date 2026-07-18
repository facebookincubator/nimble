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
#include <string>
#include <vector>

#include "dwio/nimble/index/IndexLookup.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "dwio/nimble/tablet/MetadataCache.h"

namespace facebook::nimble::index {

class HashIndex;
class SortedIndex;

/// Unified registry for dense indices (hash and sorted) on a Nimble file.
///
/// Loads dense index payloads referenced by the common index manifest and
/// validates no duplicate column sets across index types.
class DenseIndexRegistry {
 public:
  /// Creates a registry from optional hash and sorted index sections.
  /// Returns nullptr if both sections are empty.
  /// Creates with Options — indexes create their own MetadataInput
  /// internally with index-specific IO stats.
  static std::unique_ptr<DenseIndexRegistry> create(
      std::optional<Section> hashSection,
      std::optional<Section> sortedSection,
      const IndexLookup::Options& options,
      velox::memory::MemoryPool* pool);

  /// Finds any dense index matching the given columns.
  const IndexLookup* findIndex(
      const std::vector<std::string>& queryColumns) const;

 private:
  DenseIndexRegistry() = default;

  void registerHashIndices(
      Section directorySection,
      std::shared_ptr<MetadataInput> metadataInput,
      velox::memory::MemoryPool* pool);

  void registerSortedIndices(
      Section directorySection,
      std::shared_ptr<MetadataInput> metadataInput,
      std::shared_ptr<velox::dwio::common::BufferedInput> dataInput,
      velox::memory::MemoryPool* pool);

  struct IndexDescriptor {
    std::vector<std::string> columns;
    MetadataSection section;
  };

  const HashIndex* findHashIndex(
      const std::vector<std::string>& queryColumns) const;

  const SortedIndex* findSortedIndex(
      const std::vector<std::string>& queryColumns) const;

  void validate() const;

  template <typename FbDirectory>
  static std::vector<IndexDescriptor> parseDescriptors(
      const Section& directorySection,
      const std::string& indexTypeName);

  // Hash index state.
  std::vector<IndexDescriptor> hashDescriptors_;
  mutable std::unique_ptr<MetadataCache<uint32_t, HashIndex>> hashIndexCache_;

  // Sorted index state.
  std::vector<IndexDescriptor> sortedDescriptors_;
  mutable std::unique_ptr<MetadataCache<uint32_t, SortedIndex>>
      sortedIndexCache_;
};

} // namespace facebook::nimble::index
