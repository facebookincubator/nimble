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
#include <string_view>
#include <vector>

#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/serializer/StreamSelector.h"
#include "folly/io/IOBuf.h"
#include "velox/common/memory/Memory.h"
#include "velox/type/Type.h"

namespace facebook::nimble::serde {

/// Projects columns and subfields from serialized Nimble data without decoding.
///
/// A Projector wraps a StreamSelector (step 1: select the byte ranges for the
/// requested columns/subfields, zero-copy) and additionally frames the selected
/// streams into a standalone projected blob (step 2: serialization header +
/// stream-sizes trailer). Consumers that only need the selection step (e.g. the
/// Deserializer, which decodes the selected streams directly) use a
/// StreamSelector instead.
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
///   Projector projector(inputSchema, subfields, pool, {});
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
    /// Output serialization format version. Defaults to kProjection (the
    /// Projector-specific version byte for the two-array sparse trailer).
    /// kLegacyCompact is read-only and silently upgraded to kProjection at
    /// construction.
    SerializationVersion projectVersion{SerializationVersion::kProjection};

    /// Encoding type for the indices array of the sparse stream-sizes
    /// trailer. Supported types: Trivial, Varint, Delta, FixedBitWidth.
    EncodingType streamIndicesEncodingType{EncodingType::FixedBitWidth};

    /// Encoding type for the sizes array of the sparse stream-sizes
    /// trailer. Supported types: Trivial, Varint, Delta, FixedBitWidth.
    EncodingType streamSizesEncodingType{EncodingType::FixedBitWidth};

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
  /// @param pool Memory pool for encoding/decoding offsets in sparse format.
  ///        Must not be null.
  /// @param options Includes optional projectType for schema evolution.
  /// @throws If a subfield path cannot be resolved against the schema.
  Projector(
      std::shared_ptr<const Type> inputSchema,
      const std::vector<Subfield>& projectSubfields,
      velox::memory::MemoryPool* pool,
      Options options);

  /// Projects a single input buffer. The output IOBuf chain references the
  /// input memory via zero-copy cloning — the input data must remain valid
  /// while the output is in use.
  /// @param input Serialized Nimble buffer.
  /// @return Projected buffer as IOBuf (possibly chained).
  folly::IOBuf project(std::string_view input) const;

  /// Projects multiple input buffers. Each output references its corresponding
  /// input memory — all inputs must remain valid while outputs are in use.
  /// @param inputs Vector of serialized Nimble buffers.
  /// @return Vector of projected buffers.
  std::vector<folly::IOBuf> project(
      const std::vector<std::string_view>& inputs) const;

  /// Projects a single input IOBuf (may be chained). The output IOBuf chain
  /// shares underlying memory with the input via zero-copy cloning — the input
  /// must remain valid while the output is in use.
  folly::IOBuf project(const folly::IOBuf& input) const;

  /// Projects multiple input IOBufs. Each output shares memory with its
  /// corresponding input — all inputs must remain valid while outputs are in
  /// use.
  std::vector<folly::IOBuf> project(
      const std::vector<folly::IOBuf>& inputs) const;

  /// Returns the projected schema (compact stream indices starting from 0).
  std::shared_ptr<const Type> projectedSchema() const {
    return selector_.projectedSchema();
  }

  /// Returns the input stream indices that will be copied.
  /// Only for testing.
  const std::vector<uint32_t>& testingInputStreamIndices() const {
    return selector_.testingInputStreamIndices();
  }

  /// Returns whether input stream indices are sorted (fast path eligible).
  /// Only for testing.
  bool testingInputStreamsSorted() const {
    return selector_.testingInputStreamsSorted();
  }

 private:
  // Frames selected streams into a projected blob by appending the trailer to
  // the output IOBuf chain (which already carries the header and stream data).
  folly::IOBuf buildProjectedOutput(
      const std::vector<uint32_t>& outputStreamSizes,
      SerializationVersion version,
      std::unique_ptr<folly::IOBuf> output) const;

  // Step 1: selects the projected streams (zero-copy).
  StreamSelector selector_;
  const EncodingType streamIndicesEncodingType_;
  const EncodingType streamSizesEncodingType_;
};

} // namespace facebook::nimble::serde
