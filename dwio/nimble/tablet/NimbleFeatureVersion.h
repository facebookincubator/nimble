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
#include <limits>

namespace facebook::nimble {

/// Represents a Nimble file format version as (major, minor).
/// Comparison is lexicographic: major is compared first, then minor.
struct NimbleVersion {
  uint16_t major;
  uint16_t minor;

  constexpr bool operator<(const NimbleVersion& other) const {
    return major < other.major || (major == other.major && minor < other.minor);
  }

  constexpr bool operator<=(const NimbleVersion& other) const {
    return !(other < *this);
  }

  constexpr bool operator>=(const NimbleVersion& other) const {
    return !(*this < other);
  }

  constexpr bool operator==(const NimbleVersion& other) const {
    return major == other.major && minor == other.minor;
  }

  constexpr bool operator!=(const NimbleVersion& other) const {
    return !(*this == other);
  }

  constexpr bool operator>(const NimbleVersion& other) const {
    return other < *this;
  }
};

/// Defines the version range [introduced, deprecated) for a feature.
/// A feature is active when: version >= introduced AND version < deprecated.
/// Use kMaxVersion for deprecated to indicate a feature that is not yet
/// deprecated.
struct NimbleFeatureVersionRange {
  NimbleVersion introduced;
  NimbleVersion deprecated;

  constexpr bool isActive(const NimbleVersion& version) const {
    return version >= introduced && version < deprecated;
  }
};

/// Sentinel version representing "not deprecated" (no upper bound).
constexpr NimbleVersion kMaxVersion{
    std::numeric_limits<uint16_t>::max(),
    std::numeric_limits<uint16_t>::max()};

/// Version ranges for each feature.
/// When adding a new feature:
///   1. Add a constexpr NimbleFeatureVersionRange here with the introduced
///      version and kMaxVersion as the deprecated version.
///   2. Bump kVersionMajor/kVersionMinor in Constants.h to match.
///   3. In the writer, gate the new behavior on the current version.
///   4. In the reader, gate decoding on the file's version.
///
/// When deprecating a feature, set the deprecated version to the version
/// that removes it.

/// Version (0, 1): Baseline Nimble format.

/// Version (0, 2): Varint-encoded row counts in encoding prefixes.
/// When active AND index is enabled, encoding prefixes use variable-length
/// row counts instead of fixed 4-byte uint32.
constexpr NimbleFeatureVersionRange kVersionRangeVarintRowCount{
    {0, 2},
    kMaxVersion};

/// Returns true if the given file version supports varint row counts.
/// Callers on the read side must also check that the file has an index section,
/// since varint row counts are bundled with the index feature for safe rollout.
constexpr bool hasVarintRowCount(uint32_t majorVersion, uint32_t minorVersion) {
  return kVersionRangeVarintRowCount.isActive(
      NimbleVersion{
          static_cast<uint16_t>(majorVersion),
          static_cast<uint16_t>(minorVersion)});
}

} // namespace facebook::nimble
