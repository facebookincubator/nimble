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

#include <folly/container/F14Set.h>

#include "dwio/nimble/velox/SchemaReader.h"
#include "velox/type/Subfield.h"
#include "velox/type/Type.h"

namespace facebook::nimble {

velox::TypePtr convertToVeloxType(const Type& type);

std::shared_ptr<const Type> convertToNimbleType(const velox::Type& type);

/// Encoding types for top-level columns extracted from a nimble file schema.
/// Records which columns use encoding-specific types (ArrayWithOffsets,
/// SlidingWindowMap, FlatMap), enabling the Serializer to produce output
/// consistent with the file's encoding.
///
/// FlatMap columns projected with key subscripts (e.g., "map[\"key\"]") are
/// handled automatically. However, projecting an entire FlatMap column without
/// any key subscripts is not supported because the velox type does not carry
/// FlatMap key names/order — use the nimble-schema-based
/// buildProjectedNimbleType overload for full FlatMap column projection.
struct ColumnEncodings {
  folly::F14FastSet<std::string> dictionaryArrayColumns;
  folly::F14FastSet<std::string> deduplicatedMapColumns;
  folly::F14FastSet<std::string> flatMapColumns;
};

/// Converts a projected velox RowType to a nimble schema, including only the
/// projected columns. Supports arbitrary-depth subfield projections:
/// - Full column projection: subfield is just the column name (e.g., "col").
/// - FlatMap key projection: subfield has a subscript (e.g., "map[\"key\"]").
/// - Row child projection: subfield has a nested field (e.g., "struct.field").
/// - Deep projection: "struct.nested_struct.field", "map[\"key\"].field", etc.
///
/// When columnEncodings is provided, creates encoding-specific nimble types
/// (ArrayWithOffsets, SlidingWindowMap, FlatMap) to match data serialized with
/// these encodings. When empty (default), uses plain nimble types.
std::shared_ptr<const Type> buildProjectedNimbleType(
    const velox::RowType& type,
    const std::vector<velox::common::Subfield>& projectedSubfields,
    const ColumnEncodings& columnEncodings = {});

/// Builds a projected nimble schema from an existing nimble schema, assigning
/// output stream offsets sequentially in DFS traversal order. This ensures
/// the offset assignment matches how SchemaBuilder assigns offsets in
/// buildProjectedNimbleType, enabling clients to recover the schema.
///
/// Unlike buildProjectedNimbleType (which builds from velox types and loses
/// encoding-specific info like ArrayWithOffsets/SlidingWindowMap), this
/// function preserves the exact nimble type structure from the input schema.
///
/// FlatMap children are sorted alphabetically by name for canonical ordering.
///
/// @param type The input nimble schema tree.
/// @param projectedSubfields Subfields to project. Each subfield path is
///        resolved against the schema to determine which children to include
///        at each Row/FlatMap node.
/// @param projectedStreamOffsets Output: input stream offsets in DFS traversal
///        order, defining which streams to include and in what order. Must be
///        empty on entry.
std::shared_ptr<const Type> buildProjectedNimbleType(
    const Type* type,
    const std::vector<velox::common::Subfield>& projectedSubfields,
    std::vector<uint32_t>& projectedStreamOffsets);

} // namespace facebook::nimble
