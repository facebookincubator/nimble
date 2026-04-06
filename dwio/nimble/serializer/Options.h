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
#include "folly/container/F14Map.h"
#include "folly/container/F14Set.h"

#include <set>
#include "velox/type/Type.h"

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
///   Supported EncodingTypes: Trivial, Varint, Delta.
///   See getRawEncodingType() for validation.
///
/// - kTabletRaw: Raw tablet stream passthrough format. Like kCompactRaw but:
///   (1) Each stream's data includes tablet chunk headers:
///       [chunkSize:u32][compressionType:1B][encoded_data...]
///       which the Deserializer strips before decoding.
///   (2) Encoding headers within streams use fixed u32 row counts
///       (useVarintRowCount=false), matching the tablet's default format.
///   Wire: [version:1B][rowCount:varint][stream_data_0]...[stream_data_N]
///         [encodingType:1B][raw_sizes_payload][trailer_size:u32]
enum class SerializationVersion : uint8_t {
  kLegacy = 0,
  kCompact = 1,
  kCompactRaw = 2,
  kTabletRaw = 3,
};

std::string toString(SerializationVersion version);

/// Returns true if the version is any non-legacy format (kCompact, kCompactRaw,
/// or kTabletRaw). All non-legacy formats have a version header, encoded
/// streams, and a stream sizes trailer.
inline bool nonLegacyFormat(SerializationVersion version) {
  return version != SerializationVersion::kLegacy;
}

/// Returns true if the version uses compact encoding (kCompact or kCompactRaw).
/// Note: kTabletRaw is NOT a compact format (has chunk headers in streams).
inline bool isCompactFormat(SerializationVersion version) {
  return version == SerializationVersion::kCompact ||
      version == SerializationVersion::kCompactRaw;
}

/// Returns true if the version uses raw stream sizes (kCompactRaw or
/// kTabletRaw) instead of nimble-encoded stream sizes.
inline bool isRawFormat(SerializationVersion version) {
  return version == SerializationVersion::kCompactRaw ||
      version == SerializationVersion::kTabletRaw;
}

/// Returns true if the optional version uses raw stream sizes.
inline bool isRawFormat(std::optional<SerializationVersion> version) {
  return version.has_value() && isRawFormat(version.value());
}

/// Returns true if the version is the tablet raw passthrough format.
inline bool isTabletRawFormat(SerializationVersion version) {
  return version == SerializationVersion::kTabletRaw;
}

/// Returns true if the version is the tablet raw passthrough format.
inline bool isTabletRawFormat(std::optional<SerializationVersion> version) {
  return version.has_value() && isTabletRawFormat(version.value());
}

/// Returns true if the optional version uses compact encoding.
inline bool isCompactFormat(std::optional<SerializationVersion> version) {
  return version.has_value() && isCompactFormat(version.value());
}

/// Returns true if the version uses varint for the header row count.
/// All non-legacy versioned formats (kCompact, kCompactRaw, kTabletRaw) use
/// varint row counts in the header. The raw stream bodies inside kTabletRaw
/// may use fixed u32 row counts in their encoding headers.
inline bool usesVarintRowCount(SerializationVersion version) {
  return version != SerializationVersion::kLegacy;
}

/// Returns true if the optional version uses varint for the header row count.
inline bool usesVarintRowCount(std::optional<SerializationVersion> version) {
  return version.has_value() && usesVarintRowCount(version.value());
}

/// Validates and returns the EncodingType for kCompactRaw stream sizes.
/// Supported: Trivial, Varint, Delta.
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

  /// Columns that should be encoded as flat maps. Maps column name to a set
  /// of predefined key strings. When the set is empty, the column is
  /// treated as a flat map with dynamic key discovery. When non-empty, keys
  /// are predefined in sorted order to ensure all serializers produce
  /// identical schemas regardless of data arrival order. Unknown keys not in
  /// the set will cause an error during serialization.
  folly::F14FastMap<std::string, std::set<std::string>> flatMapColumns{};

  /// Factory for creating encoding selection policies.
  /// Only used when version is kCompact.
  /// When encodingLayoutTree is specified, used as fallback for streams or
  /// nested encodings not captured in the layout tree. When encodingLayoutTree
  /// is not specified, used directly for all streams.
  EncodingSelectionPolicyFactory encodingSelectionPolicyFactory =
      [factory =
           ManualEncodingSelectionPolicyFactory{
               ManualEncodingSelectionPolicyFactory::defaultReadFactors(),
               /*compressionOptions=*/std::nullopt}](
          DataType dataType) -> std::unique_ptr<EncodingSelectionPolicyBase> {
    return factory.createPolicy(dataType);
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
  /// Whether the serialized data has a header byte.
  /// - false (default): Legacy format (version 0) with no header.
  /// - true: Version is auto-detected from the first byte of serialized data.
  bool hasHeader{false};

  /// Output type for deserializing flatmap columns as struct (ROW).
  /// When provided, each top-level flatmap column whose corresponding field in
  /// outputType is ROW will be deserialized as a struct instead of a map. The
  /// ROW field names specify which flatmap keys to select and their order.
  /// Fields not present in the flatmap schema will be filled with nulls.
  /// When nullopt (default), all flatmap columns are deserialized as maps.
  velox::RowTypePtr outputType{};

  /// Whether to enable BufferPool for recycling encoding scratch buffers.
  /// When true, a BufferPool is created per DeserializerImpl to cache and
  /// reuse buffers across encoding lifetimes, reducing MemoryPool allocation
  /// overhead.
  bool enableBufferPool{true};
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
