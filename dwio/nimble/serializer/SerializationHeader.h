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
#include <optional>
#include <string>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/velox/RowRange.h"
#include "folly/io/IOBuf.h"

namespace facebook::nimble::serde {

// ---- Generic serialization header (all versions) ----

/// Parsed serialization header common to all versions.
struct SerializationHeader {
  SerializationVersion version{SerializationVersion::kLegacy};
  uint32_t rowCount{0};
  /// Row range embedded in kTablet headers. nullopt for other versions.
  std::optional<RowRange> rowRange;
};

namespace detail {

template <typename T>
char* extend(T& buffer, uint32_t size) {
  const auto oldSize = buffer.size();
  buffer.resize(oldSize + size);
  return buffer.data() + oldSize;
}

} // namespace detail

/// Reads the serialization header from `*pos`, detecting the version from the
/// first byte when `hasHeader` is true (otherwise defaults to kLegacy).
/// Advances `*pos` past all header fields. For kTablet, also reads the
/// row range and resume key length fields.
SerializationHeader
readSerializationHeader(const char*& pos, const char* end, bool hasHeader);

/// Writes a serialization header to buffer.
/// For kLegacy: writes [optional_version:1B][rowCount:u32]
/// For kCompactRaw: writes [version:1B][rowCount:varint]
/// kTablet headers must use createTabletChunkHeader() instead.
template <typename T>
void writeSerializationHeader(
    T& buffer,
    std::optional<SerializationVersion> version,
    uint32_t rowCount) {
  NIMBLE_CHECK(
      !isTabletVersion(version),
      "kTablet headers must use createTabletChunkHeader()");

  if (version.has_value()) {
    auto* versionPos = detail::extend(buffer, 1);
    *versionPos = static_cast<char>(version.value());
  }

  if (usesVarintRowCount(version)) {
    auto* rowCountPos = detail::extend(buffer, varint::varintSize(rowCount));
    varint::writeVarint(rowCount, &rowCountPos);
  } else {
    auto* rowCountPos = detail::extend(buffer, sizeof(uint32_t));
    encoding::writeUint32(rowCount, rowCountPos);
  }
}

/// Returns the exact byte size of the serialization header.
/// Not valid for kTablet — use createTabletChunkHeader() instead.
inline size_t estimateSerializationHeaderSize(
    std::optional<SerializationVersion> version,
    uint32_t rowCount) {
  NIMBLE_CHECK(
      !isTabletVersion(version),
      "kTablet headers must use createTabletChunkHeader()");
  const size_t versionBytes = version.has_value() ? sizeof(uint8_t) : 0;
  const size_t rowCountBytes = usesVarintRowCount(version)
      ? varint::varintSize(rowCount)
      : sizeof(uint32_t);
  return versionBytes + rowCountBytes;
}

// ---- Tablet chunk header (kTablet only) ----

/// Parsed fields of a kTablet chunk slice header.
struct TabletChunkHeader {
  uint32_t rowCount{0};
  RowRange rowRange;
  std::optional<std::string> resumeKey;
};

/// Builds the kTablet chunk slice header as a freshly-allocated IOBuf.
folly::IOBuf createTabletChunkHeader(const TabletChunkHeader& header);

/// Reads a kTablet chunk slice header including the version byte.
TabletChunkHeader readTabletChunkHeader(const char*& pos, const char* end);

} // namespace facebook::nimble::serde
