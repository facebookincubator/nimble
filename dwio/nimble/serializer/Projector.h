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
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/velox/SchemaReader.h"
#include "velox/type/Subfield.h"
#include "velox/type/Type.h"

namespace facebook::nimble::serde {

using Subfield = velox::common::Subfield;

/// Projects columns and subfields from serialized Nimble data without decoding.
///
/// Copies only the byte ranges for selected columns/subfields, preserving
/// compression and avoiding decode-encode overhead.
///
/// Supported projections:
/// - Top-level columns: "a", "b", "c"
/// - Nested struct fields: "a.b.c", "struct_col.field1"
/// - FlatMap keys: "flatmap_col[\"key1\"]"
///
/// Not supported (requires re-encoding):
/// - Regular MAP key projection
/// - Array element slicing
///
/// Example usage:
/// @code
///   auto inputSchema = nimble::convertToNimbleType(veloxType);
///   std::vector<Subfield> subfields = {
///       Subfield("a"),
///       Subfield("b.c"),
///       Subfield("flatmap[\"key1\"]"),
///   };
///
///   Projector projector(inputSchema, subfields, {.inputHasVersionHeader =
///   true});
///
///   // Project multiple buffers
///   for (const auto& buffer : buffers) {
///     auto projected = projector.project(buffer);
///   }
///
///   // Get output schema for deserialization
///   auto outputSchema = projector.projectedSchema();
/// @endcode
class Projector {
 public:
  struct Options {
    /// Whether the input buffer has a version header byte.
    /// If false, input is assumed to be legacy dense format (raw encoding).
    bool inputHasVersionHeader{false};

    /// Output serialization format version.
    /// The output always has a version header.
    /// Must be compatible with input encoding type:
    /// - Raw formats (kDense/kSparse) for legacy or raw input
    /// - Encoded formats (kDenseEncoded/kSparseEncoded) for encoded input
    SerializationVersion projectVersion{SerializationVersion::kSparseEncoded};

    /// Optional velox type with up-to-date column names from the current
    /// table schema. After schema evolution (e.g., column renames), the
    /// inputSchema (nimble type from serialization time) may have outdated
    /// names. When set, inputSchema column names are updated to match
    /// projectType via positional matching.
    /// When null, projectSubfield names must match inputSchema exactly.
    velox::TypePtr projectType{nullptr};
  };

  /// @param inputSchema Nimble schema of serialized data.
  /// @param projectSubfields Columns to project (using projectType names if
  ///        set).
  /// @param options Includes optional projectType for schema evolution.
  /// @throws If a subfield path cannot be resolved against the schema.
  Projector(
      std::shared_ptr<const Type> inputSchema,
      const std::vector<Subfield>& projectSubfields,
      Options options);

  /// Projects a single input buffer.
  /// @param input Serialized Nimble buffer.
  /// @return Projected buffer.
  ///
  // TODO: Return folly::IOBuf instead of std::string for network transfer
  // efficiency (avoids copy when sending to client).
  std::string project(std::string_view input) const;

  /// Projects multiple input buffers.
  /// @param inputs Vector of serialized Nimble buffers.
  /// @return Vector of projected buffers.
  std::vector<std::string> project(
      const std::vector<std::string_view>& inputs) const;

  /// Returns the projected schema.
  /// The schema has compact stream indices starting from 0.
  std::shared_ptr<const Type> projectedSchema() const {
    return projectedSchema_;
  }

  /// Returns the input stream indices that will be copied.
  /// Only for testing.
  const std::vector<uint32_t>& testingInputStreamIndices() const {
    return inputStreamIndices_;
  }

 private:
  // Const members (set in init list).
  const Options options_;

  std::shared_ptr<const Type> inputSchema_;
  // Built during construction.
  std::shared_ptr<const Type> projectedSchema_;
  std::vector<uint32_t> inputStreamIndices_;

  // True if all streams are selected and formats match, enabling pass-through
  // (copy input directly to output without parsing/rewriting streams).
  bool passThrough_{false};
};

} // namespace facebook::nimble::serde
