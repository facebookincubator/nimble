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

#include <string>

#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::nimble::index {

/// Base interface for index writers (ClusterIndexWriter, DenseIndexWriter).
///
/// Lifecycle:
///   1. Create with config and input schema
///   2. For each batch: write(batch)
///   3. At stripe group boundaries: flushPartition() to write index data
///   4. At file close: finalize() to build and serialize the root index
///   5. close() to release resources
class IndexWriter {
 public:
  virtual ~IndexWriter() = default;

  /// Processes a batch of input data for indexing.
  virtual void write(const velox::VectorPtr& input) = 0;

  /// Builds and serializes the index. Called at file close.
  /// Returns the serialized index to be stored as an optional section.
  virtual std::string finalize(
      const CreateMetadataSectionFn& createMetadataSection) = 0;

  /// Releases internal resources. Must be called after finalize().
  virtual void close() = 0;

  /// Flushes partition data at stripe group boundaries.
  /// Writes key stream data via writeData, then writes partition metadata
  /// via createMetadataSection.
  virtual void flushPartition(
      size_t stripeCount,
      const WriteDataFn& writeData,
      const CreateMetadataSectionFn& createMetadataSection) {}
};

} // namespace facebook::nimble::index
