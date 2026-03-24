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

#include <vector>

#include "dwio/nimble/tablet/MetadataBuffer.h"

namespace facebook::nimble::index {

/// ChunkIndex provides access to chunk-level position index metadata for Nimble
/// tablets.
///
/// This is the root-level chunk index that contains per-stripe-group
/// MetadataSection references. Each stripe group's chunk index data
/// (ChunkIndexGroup) can be loaded on demand using the metadata sections
/// provided by this class.
///
/// Mirrors the ClusterIndex pattern: owns the root flatbuffer section and
/// exposes per-group metadata for on-demand loading.
class ChunkIndex {
 public:
  /// Creates a ChunkIndex from the root chunk index optional section.
  ///
  /// @param indexSection The section containing the serialized root chunk index
  /// @return A unique pointer to a newly created ChunkIndex, or nullptr if the
  ///         section contains no stripe indexes
  static std::unique_ptr<ChunkIndex> create(Section indexSection);

  /// Returns the number of stripe groups indexed.
  uint32_t numGroups() const {
    return groupSections_.size();
  }

  /// Returns the metadata section for a specific stripe group's chunk index.
  ///
  /// @param groupIndex The zero-based stripe group index
  /// @return MetadataSection containing offset, size, and compression info
  const MetadataSection& groupMetadata(uint32_t groupIndex) const;

 private:
  explicit ChunkIndex(
      Section indexSection,
      std::vector<MetadataSection> groupSections);

  const Section indexSection_;
  const std::vector<MetadataSection> groupSections_;
};

} // namespace facebook::nimble::index
