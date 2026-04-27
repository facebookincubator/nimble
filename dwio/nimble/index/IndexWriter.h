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
#include <string>
#include <vector>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "velox/serializers/KeyEncoder.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::nimble::index {

/// Base interface for index writers (ClusterIndexWriter, HashIndexWriter).
///
/// Lifecycle:
///   1. Create with config and input schema
///   2. For each batch: write(batch)
///   3. At stripe group boundaries: flush() to write index data
///   4. At file close: close() to finalize, serialize, and release resources
class IndexWriter {
 public:
  virtual ~IndexWriter() = default;

  /// Processes a batch of input data for indexing.
  virtual void write(const velox::VectorPtr& input) = 0;

  /// Flushes index data at stripe group boundaries.
  virtual void flush(
      const WriteDataFn& writeDataFn,
      const CreateMetadataSectionFn& createMetadataFn) {}

  /// Finalizes the index, writes it as an optional section, and releases
  /// resources.
  virtual void close(
      const WriteDataFn& writeDataFn,
      const CreateMetadataSectionFn& createMetadataFn,
      const WriteOptionalSectionFn& writeMetadataFn) = 0;

 protected:
  void checkNotClosed() const {
    NIMBLE_CHECK(!closed_, "IndexWriter has been closed");
  }

  void setClosed() {
    NIMBLE_CHECK(!closed_, "close() already called");
    closed_ = true;
  }

  // Validates that no null values exist in the specified key columns.
  static void validateNoNullKeys(
      const velox::VectorPtr& input,
      const std::vector<velox::column_index_t>& keyColumnIndices);

  // Creates a KeyEncoder with ascending sort order for the given columns.
  static std::unique_ptr<velox::serializer::KeyEncoder> createKeyEncoder(
      const std::vector<std::string>& columns,
      const velox::RowTypePtr& inputType,
      velox::memory::MemoryPool* pool);

  // Returns deduplicated key column indices across multiple column sets.
  static std::vector<velox::column_index_t> getKeyColumnIndices(
      const std::vector<std::vector<std::string>>& columnSets,
      const velox::RowTypePtr& inputType);

 private:
  bool closed_{false};
};

} // namespace facebook::nimble::index
