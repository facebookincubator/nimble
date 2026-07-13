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

#include <cstdint>
#include <memory>
#include <vector>

#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "folly/io/IOBuf.h"
#include "velox/type/Subfield.h"
#include "velox/type/Type.h"

namespace facebook::nimble::serde {

using Subfield = velox::common::Subfield;

/// Result of the stream-selection step (step 1 of Projector::project()): the
/// selected streams as zero-copy bytes plus the metadata needed to either
/// assemble a projected blob (Projector::project, step 2) or decode them
/// directly (Deserializer). Keeping this intermediate lets the Deserializer
/// reuse the selection without materializing and re-parsing a full projected
/// blob.
struct ProjectedStreams {
  /// Row count of the batch (from the input serialization header).
  uint32_t rowCount{0};
  /// Serialization version the projected streams should be decoded with (the
  /// selector's output version). Matches the version a full projected blob
  /// would carry in its header.
  SerializationVersion version{SerializationVersion::kProjection};
  /// Whether the projected streams require a null barrier before decoding.
  bool requiresNullBarrier{false};
  /// Byte size of each projected stream, indexed by projected stream offset.
  /// A zero entry means the stream is omitted (absent from the projected data).
  std::vector<uint32_t> streamSizes;
  /// Concatenated bytes of the selected streams in projected-offset order,
  /// contiguous runs merged. Zero-copy sub-ranges of the input; the input must
  /// outlive this. Split back into per-stream segments using `streamSizes`.
  folly::IOBuf streamData;
};

/// Maps an input stream index to its output stream index (used by the unsorted
/// projection path to carry the precomputed input→output mapping).
struct StreamMapping {
  uint32_t inputStreamIdx;
  size_t outputStreamIdx;
};

/// Selects (projects) a subset of streams out of a serialized Nimble blob
/// without decoding them — step 1 of a projection. Copies only the byte ranges
/// for the selected columns/subfields as zero-copy IOBuf sub-ranges, preserving
/// compression and avoiding decode-encode overhead.
///
/// This is the reusable core shared by the Projector (which frames the selected
/// streams into a projected blob, step 2) and the Deserializer (which decodes
/// the selected streams directly). Consumers that only need the selection step
/// depend on this class, not on the blob-assembling Projector.
///
/// Supported projections:
/// - Top-level columns: "a", "b", "c"
/// - Nested struct fields: "a.b.c", "struct_col.field1"
/// - FlatMap keys: "flatmap_col[\"key1\"]"
///
/// Not supported (requires re-encoding):
/// - Regular MAP key projection
/// - Array element slicing
class StreamSelector {
 public:
  struct Options {
    /// Output serialization format version stamped on the selected streams.
    /// Defaults to kProjection (the projection-specific version byte for the
    /// two-array sparse trailer). kLegacyCompact is read-only and silently
    /// upgraded to kProjection at construction.
    SerializationVersion projectVersion{SerializationVersion::kProjection};

    /// Optional velox type with up-to-date column names from the current
    /// table schema. After schema evolution (e.g., column renames), the
    /// inputSchema (nimble type from serialization time) may have outdated
    /// names. When set, inputSchema column names are updated to match
    /// projectType via positional matching.
    /// When null, projectSubfield names must match inputSchema exactly.
    velox::TypePtr projectType{nullptr};
  };

  /// @param inputSchema Nimble schema of serialized data. Must be a RowType.
  /// @param projectSubfields Columns to project (using projectType names if
  ///        set). Must not be empty.
  /// @param options Includes optional projectType for schema evolution.
  /// @throws If a subfield path cannot be resolved against the schema.
  StreamSelector(
      std::shared_ptr<const Type> inputSchema,
      const std::vector<Subfield>& projectSubfields,
      const Options& options);

  /// Selects the projected streams from `input` without assembling a projected
  /// blob. The returned streams are zero-copy sub-ranges of `input`, which must
  /// outlive the result. Used by the Deserializer to decode projected columns
  /// directly, and by Projector::project() which appends a header and trailer
  /// to produce a full projected blob.
  ProjectedStreams selectStreams(const folly::IOBuf& input) const;

  /// Returns the projected schema (compact stream indices starting from 0) that
  /// the selected streams decode against.
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
  // Selects streams from a contiguous (non-chained) IOBuf using raw pointer
  // arithmetic. Returns the selected streams without header or trailer.
  ProjectedStreams selectContiguous(
      const folly::IOBuf& input,
      SerializationVersion inputVersion) const;

  // Selects streams from a chained IOBuf using folly::io::Cursor. Returns the
  // selected streams without header or trailer.
  ProjectedStreams selectChained(
      const folly::IOBuf& input,
      SerializationVersion inputVersion) const;

  // Output serialization version stamped on the selected streams, upgraded from
  // any read-only input version at construction. This is all the projector's
  // Options that is needed past construction; projectType is consumed once to
  // rename inputSchema_ and not retained.
  const SerializationVersion projectVersion_;

  std::shared_ptr<const Type> inputSchema_;

  // Built during construction.
  std::shared_ptr<const Type> projectedSchema_;
  std::vector<uint32_t> inputStreamIndices_;

  // Parallel to inputStreamIndices_ and output stream indices. A true entry
  // means the projected stream is a Row or FlatMap null stream.
  std::vector<bool> rowOrFlatMapNullStreams_;

  // True when inputStreamIndices_ is already sorted (no FlatMap key
  // reordering). Enables fast-path projection that avoids sorting and
  // reordering overhead.
  bool inputStreamsSorted_{false};

  // Cached mapping from input stream index to output stream index, sorted by
  // input stream index. Built once in the constructor when
  // !inputStreamsSorted_. Empty when inputStreamsSorted_ is true.
  // Used by selectStreamsChainedUnsorted() for forward pass extraction.
  std::vector<StreamMapping> sortedStreamMappings_;
};

} // namespace facebook::nimble::serde
