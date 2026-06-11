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

/// Builds a projected nimble schema from a source nimble schema and emits
/// the source stream-offset mapping in one pass. Used by
/// `nimble::serde::Projector` and `nimble::NimbleIndexProjector` to derive
/// everything the byte-copy pipeline needs from `(type, projectedSubfields)`.
///
/// Internally: converts the source nimble schema to velox (FlatMaps collapse
/// to `MAP<K,V>`), derives encoding hints from the source's top-level Kinds,
/// builds the projected schema via the velox-source `buildProjectedNimbleType`
/// overload, and walks the source nimble in the matching DFS pre-order +
/// FlatMap-children-alphabetical traversal to emit one source stream offset
/// per projected stream position.
///
/// Missing FlatMap keys are allowed: each subscript whose key does not exist
/// in the source FlatMap produces a synthetic child in the returned schema
/// (the value subtree is cloned from `flatMap.childAt(0)`'s type as a
/// structural template) and emits `UINT32_MAX` placeholder entries in
/// `projectedStreamOffsets` (>= any real source offset). The projector's
/// stream-copy guard turns those into 0-byte placeholder slots in the
/// projected blob, which the deserializer's gap-fill renders as null
/// columns. An all-missing-keys projection is permitted and yields a
/// FlatMap whose every child is a placeholder.
///
/// @param type                    The source nimble schema; root must be Row.
/// @param projectedSubfields      Subfields to project; must not be empty.
/// @param projectedStreamOffsets  Output: appended in projected-stream-
///                                position order. Must be empty on entry.
std::shared_ptr<const Type> buildProjectedNimbleType(
    const Type* type,
    const std::vector<velox::common::Subfield>& projectedSubfields,
    std::vector<uint32_t>& projectedStreamOffsets);

} // namespace facebook::nimble
