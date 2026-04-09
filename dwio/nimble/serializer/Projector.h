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

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "folly/io/Cursor.h"
#include "folly/io/IOBuf.h"
#include "velox/common/memory/Memory.h"
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
    /// Output serialization format version. Must be kCompact or kCompactRaw.
    SerializationVersion projectVersion{SerializationVersion::kCompact};

    /// Encoding type for stream sizes in the sizes header.
    /// For kCompact: forces nimble encoding to this type.
    /// For kCompactRaw: only Trivial and Varint are supported.
    /// Defaults to Trivial.
    EncodingType streamSizesEncodingType{EncodingType::Trivial};

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

  /// Projects input directly into a contiguous output string, avoiding IOBuf
  /// chain construction. The output string is resized as needed but can be
  /// reused across calls to amortize allocation. The input data must remain
  /// valid during this call.
  void project(std::string_view input, std::string& output) const;

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

  /// Returns whether input stream indices are sorted (fast path eligible).
  /// Only for testing.
  bool testingInputStreamsSorted() const {
    return inputStreamsSorted_;
  }

 private:
  // Maps an input stream index to its output stream index.
  struct StreamMapping {
    uint32_t inputStreamIdx;
    size_t outputStreamIdx;
  };

  // Projects a contiguous (non-chained) IOBuf using raw pointer arithmetic.
  folly::IOBuf projectContiguous(
      const folly::IOBuf& input,
      SerializationVersion inputVersion) const;

  // Projects a chained IOBuf using folly::io::Cursor.
  folly::IOBuf projectChained(
      const folly::IOBuf& input,
      SerializationVersion inputVersion) const;

  // Projects selected streams from a contiguous IOBuf in unsorted order.
  static std::vector<uint32_t> projectStreamsContiguousUnsorted(
      const folly::IOBuf& input,
      size_t dataOffset,
      const std::vector<uint32_t>& streamSizes,
      const std::vector<uint32_t>& selectedIndices,
      std::unique_ptr<folly::IOBuf>& output);

  // Projects selected streams from a contiguous IOBuf in sorted order.
  static std::vector<uint32_t> projectStreamsContiguousSorted(
      const folly::IOBuf& input,
      size_t dataOffset,
      const std::vector<uint32_t>& streamSizes,
      const std::vector<uint32_t>& selectedIndices,
      std::unique_ptr<folly::IOBuf>& output);

  // Projects selected streams from a chained IOBuf in sorted order.
  static std::vector<uint32_t> projectStreamsChainedSorted(
      folly::io::Cursor& cursor,
      const std::vector<uint32_t>& streamSizes,
      const std::vector<uint32_t>& selectedIndices,
      std::unique_ptr<folly::IOBuf>& output);

  // Projects selected streams from a chained IOBuf in unsorted order.
  static std::vector<uint32_t> projectStreamsChainedUnsorted(
      folly::io::Cursor& cursor,
      const std::vector<uint32_t>& streamSizes,
      const std::vector<StreamMapping>& sortedStreamMappings,
      std::unique_ptr<folly::IOBuf>& output);

  // Appends the trailer to the output IOBuf chain and returns it.
  folly::IOBuf buildProjectedOutput(
      const std::vector<uint32_t>& outputStreamSizes,
      std::unique_ptr<folly::IOBuf> output) const;

  velox::memory::MemoryPool* const pool_;
  const Options options_;

  // Reusable encoding buffer for stream sizes trailer. Reset before each use.
  mutable nimble::Buffer streamSizesEncodingBuffer_;

  std::shared_ptr<const Type> inputSchema_;
  // Built during construction.
  std::shared_ptr<const Type> projectedSchema_;
  std::vector<uint32_t> inputStreamIndices_;

  // True when inputStreamIndices_ is already sorted (no FlatMap key
  // reordering). Enables fast-path projection that avoids sorting and
  // reordering overhead.
  bool inputStreamsSorted_{false};

  // Cached mapping from input stream index to output stream index, sorted by
  // input stream index. Built once in the constructor when
  // !inputStreamsSorted_. Empty when inputStreamsSorted_ is true.
  // Used by projectStreamsChainedUnsorted() for forward pass extraction.
  std::vector<StreamMapping> sortedStreamMappings_;
};

} // namespace facebook::nimble::serde
