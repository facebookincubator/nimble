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

#include <fmt/format.h>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>

#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/velox/EncodingLayoutTree.h"
#include "folly/container/F14Set.h"

namespace facebook::nimble {

/// Serialization format version.
///
/// - kLegacy: Simple zstd compression with inline u32 sizes.
///   Wire:
///   [version:1B][rowCount:u32][size_0:u32][stream_data_0]...[size_N:u32][stream_data_N]
///
/// - kCompact: Nimble encoding with a dense sizes trailer.
///   Wire: [version:1B][rowCount:varint][stream_data_0]...[stream_data_N]
///         [encoded_stream_sizes][stream_sizes_encoded_size:u32]
///   The trailer contains nimble-encoded stream sizes (sizes[i] = byte size
///   of stream i, 0 for missing) followed by the encoded sizes length (u32).
///
/// - kCompactRaw: Same as kCompact but uses raw encoding for stream sizes
///   instead of the nimble encoding framework. Stream values still use
///   nimble encoding.
///   Wire: [version:1B][rowCount:varint][stream_data_0]...[stream_data_N]
///         [encodingType:1B][raw_sizes_payload][trailer_size:u32]
///   trailer_size = 1 + len(raw_sizes_payload).
///   Only EncodingType::Trivial and EncodingType::Varint are supported.
///   See getRawEncodingType() for validation.
enum class SerializationVersion : uint8_t {
  kLegacy = 0,
  kCompact = 1,
  kCompactRaw = 2,
};

std::string toString(SerializationVersion version);

/// Returns true if the version uses compact encoding (kCompact or kCompactRaw).
inline bool isCompactFormat(SerializationVersion version) {
  return version == SerializationVersion::kCompact ||
      version == SerializationVersion::kCompactRaw;
}

/// Returns true if the optional version uses compact encoding.
inline bool isCompactFormat(std::optional<SerializationVersion> version) {
  return version.has_value() && isCompactFormat(version.value());
}

/// Validates and returns the EncodingType for kCompactRaw stream sizes.
/// Only Trivial and Varint are supported.
EncodingType getRawEncodingType(EncodingType encodingType);

inline std::ostream& operator<<(
    std::ostream& os,
    SerializationVersion version) {
  return os << toString(version);
}

struct SerializerOptions {
  /// Legacy (kLegacy) compression settings. These only apply when version is
  /// kLegacy or nullopt. For kCompact, compression is controlled by
  /// 'compressionOptions' below.
  CompressionType compressionType{CompressionType::Uncompressed};
  uint32_t compressionThreshold{0};
  int32_t compressionLevel{0};

  /// Serialization format version.
  /// - nullopt (default): Legacy format with no version header (kLegacy).
  /// - kLegacy: Legacy compression format.
  /// - kCompact: Nimble encoding format with dense sizes header.
  std::optional<SerializationVersion> version{};

  /// Columns that should be encoded as flat maps.
  folly::F14FastSet<std::string> flatMapColumns{};

  /// Factory for creating encoding selection policies.
  /// Only used when version is kCompact.
  /// When encodingLayoutTree is specified, used as fallback for streams or
  /// nested encodings not captured in the layout tree. When encodingLayoutTree
  /// is not specified, used directly for all streams.
  EncodingSelectionPolicyFactory encodingSelectionPolicyFactory =
      [](DataType dataType) -> std::unique_ptr<EncodingSelectionPolicyBase> {
    return ManualEncodingSelectionPolicyFactory{
        ManualEncodingSelectionPolicyFactory::defaultReadFactors(),
        /*compressionOptions=*/std::nullopt}
        .createPolicy(dataType);
  };

  /// Optional captured encoding layout tree.
  /// When specified, encodings are replayed from this tree instead of using
  /// encodingSelectionPolicyFactory. This speeds up writes by skipping runtime
  /// encoding selection and can provide better encodings based on historical
  /// data. Falls back to encodingSelectionPolicyFactory for streams not in the
  /// tree.
  std::optional<EncodingLayoutTree> encodingLayoutTree{};

  /// Compression options used with encodingLayoutTree.
  /// When encodingLayoutTree is specified, these options are passed to
  /// ReplayedEncodingSelectionPolicy to control compression during encoding
  /// replay. When nullopt (default), compression is disabled.
  std::optional<CompressionOptions> compressionOptions{};

  /// Encoding type for stream sizes in the sizes header.
  /// Defaults to Trivial encoding.
  EncodingType streamSizesEncodingType{EncodingType::Trivial};

  /// Returns true if the serialized data has a version header byte.
  bool hasVersionHeader() const;

  /// Returns the effective serialization version.
  SerializationVersion serializationVersion() const;

  /// Returns true if nimble encoding is enabled (version is kCompact or
  /// kCompactRaw).
  bool enableEncoding() const;
};

struct DeserializerOptions {
  /// Serialization format version expected in the input.
  /// - nullopt (default): Legacy format (version 0) with no version header.
  /// - kLegacy: Legacy compression format.
  /// - kCompact: Nimble encoding format with dense sizes header.
  /// - kCompactRaw: Raw-encoded stream sizes (no nimble encoding overhead).
  std::optional<SerializationVersion> version{};

  /// Returns true if the serialized data has a version header byte.
  bool hasVersionHeader() const;

  /// Returns the effective serialization version.
  SerializationVersion serializationVersion() const;

  /// Returns true if nimble encoding is enabled (version is kCompact or
  /// kCompactRaw).
  bool enableEncoding() const;
};

} // namespace facebook::nimble

template <>
struct fmt::formatter<facebook::nimble::SerializationVersion>
    : fmt::formatter<std::string> {
  auto format(
      facebook::nimble::SerializationVersion version,
      format_context& ctx) const {
    return fmt::formatter<std::string>::format(
        facebook::nimble::toString(version), ctx);
  }
};
