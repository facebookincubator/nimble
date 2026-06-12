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
#include "dwio/nimble/encodings/selection/EncodingSelectionPolicy.h"
#include "dwio/nimble/velox/EncodingLayoutTree.h"
#include "folly/Executor.h"
#include "folly/container/F14Map.h"
#include "folly/container/F14Set.h"
#include "velox/buffer/BufferPool.h"

#include <set>
#include "velox/type/Type.h"

namespace facebook::nimble {

/// Serialization format version.
///
/// - kLegacy: Simple zstd compression with inline u32 sizes.
///   Wire:
///   [version:1B][rowCount:u32][size_0:u32][stream_data_0]...[size_N:u32][stream_data_N]
///
/// - kCompactRaw: Nimble encoding with raw-encoded stream sizes trailer.
///   Stream values use nimble encoding; stream sizes use a raw encoding.
///   Wire: [version:1B][rowCount:varint][stream_data_0]...[stream_data_N]
///         [encodingType:1B][raw_sizes_payload][trailer_size:u32]
///   trailer_size = 1 + len(raw_sizes_payload).
///   Supported EncodingTypes: Trivial, Varint, Delta, FixedBitWidth,
///   MainlyConstant. See getTrailerEncodingType() for validation.
///
/// - kTablet: Tablet stream passthrough format. Like kCompactRaw but:
///   (1) Each stream's data includes tablet chunk headers:
///       [chunkSize:u32][compressionType:1B][encoded_data...]
///       which the Deserializer strips before decoding.
///   (2) Encoding headers within streams use fixed u32 row counts
///       (useVarintRowCount=false), matching the tablet's default format.
///   Wire: [version:1B][rowCount:varint][stream_data_0]...[stream_data_N]
///         [encodingType:1B][raw_sizes_payload][trailer_size:u32]
enum class SerializationVersion : uint8_t {
  kLegacy = 0,
  kCompactRaw = 2,
  kTablet = 3,
};

std::string toString(SerializationVersion version);

/// Returns true if the version is any non-legacy format (kCompactRaw or
/// kTablet). All non-legacy formats have a version header, encoded streams,
/// and a stream sizes trailer.
inline bool nonLegacyFormat(SerializationVersion version) {
  return version != SerializationVersion::kLegacy;
}

/// Returns true if the version uses compact encoding (kCompactRaw).
/// Note: kTablet is NOT a compact format (has chunk headers in streams).
inline bool isCompactFormat(SerializationVersion version) {
  return version == SerializationVersion::kCompactRaw;
}

/// Returns true if the version uses raw stream sizes (kCompactRaw or
/// kTablet) instead of nimble-encoded stream sizes.
inline bool isRawFormat(SerializationVersion version) {
  return version == SerializationVersion::kCompactRaw ||
      version == SerializationVersion::kTablet;
}

/// Returns true if the version was written using the (now frozen) legacy
/// trailer wire format decoded by `nimble::serde::legacy::`. All currently
/// defined non-legacy versions (kCompactRaw, kTablet) share that format;
/// future writer versions that emit a different trailer wire format will
/// return false from this helper so dispatchers route them to a different
/// reader.
///
/// kTablet is included here today because in master kTablet and kCompactRaw
/// produce byte-identical trailer layouts (single encoding-type byte +
/// payload + trailer-size u32) and the same reader code path handles both.
/// Treating kTablet as legacy in this diff keeps the refactor purely
/// behavior-preserving.
///
/// The next diff (the two-array sparse trailer change) migrates kTablet
/// onto the new trailer wire format alongside the new kSerialization /
/// kProjection versions, and this helper will be narrowed to return true
/// only for kCompactRaw. kCompactRaw is the only version frozen forever
/// here because it is the only one with existing production blobs that
/// must remain readable.
inline bool usesLegacyTrailer(SerializationVersion version) {
  return version == SerializationVersion::kCompactRaw ||
      version == SerializationVersion::kTablet;
}

/// Returns true if the optional version uses raw stream sizes.
inline bool isRawFormat(std::optional<SerializationVersion> version) {
  return version.has_value() && isRawFormat(version.value());
}

/// Returns true if the version is the tablet passthrough format.
inline bool isTabletVersion(SerializationVersion version) {
  return version == SerializationVersion::kTablet;
}

/// Returns true if the version is the tablet passthrough format.
inline bool isTabletVersion(std::optional<SerializationVersion> version) {
  return version.has_value() && isTabletVersion(version.value());
}

/// Returns true if the optional version uses compact encoding.
inline bool isCompactFormat(std::optional<SerializationVersion> version) {
  return version.has_value() && isCompactFormat(version.value());
}

/// Returns true if the version uses varint for the header row count.
/// All non-legacy versioned formats (kCompactRaw, kTablet) use varint row
/// counts in the header. The raw stream bodies inside kTablet may use fixed
/// u32 row counts in their encoding headers.
inline bool usesVarintRowCount(SerializationVersion version) {
  return version != SerializationVersion::kLegacy;
}

/// Returns true if the optional version uses varint for the header row count.
inline bool usesVarintRowCount(std::optional<SerializationVersion> version) {
  return version.has_value() && usesVarintRowCount(version.value());
}

/// Validates and returns the EncodingType for stream sizes trailer.
/// Supported: Trivial, Varint, Delta, FixedBitWidth, MainlyConstant.
EncodingType getTrailerEncodingType(EncodingType encodingType);

inline std::ostream& operator<<(
    std::ostream& os,
    SerializationVersion version) {
  return os << toString(version);
}

/// Returns the default encoding selection policy factory. The underlying
/// ManualEncodingSelectionPolicyFactory (and its default read-factor vector) is
/// initialized once and shared across all returned factories.
EncodingSelectionPolicyFactory defaultEncodingSelectionPolicyFactory();

struct SerializerOptions {
  /// Legacy (kLegacy) compression settings. These only apply when version is
  /// kLegacy or nullopt. For kCompactRaw, compression is controlled by
  /// 'compressionOptions' below.
  CompressionType compressionType{CompressionType::Uncompressed};
  uint32_t compressionThreshold{0};
  int32_t compressionLevel{0};

  /// Serialization format version.
  /// - nullopt (default): Legacy format with no version header (kLegacy).
  /// - kLegacy: Legacy compression format.
  /// - kCompactRaw: Nimble encoding format with raw sizes trailer.
  std::optional<SerializationVersion> version{};

  /// Columns that should be encoded as flat maps. Maps column name to a set
  /// of predefined key strings. When the set is empty, the column is
  /// treated as a flat map with dynamic key discovery. When non-empty, keys
  /// are predefined in sorted order to ensure all serializers produce
  /// identical schemas regardless of data arrival order. Unknown keys not in
  /// the set will cause an error during serialization.
  folly::F14FastMap<std::string, std::set<std::string>> flatMapColumns{};

  /// Factory for creating encoding selection policies.
  /// Only used when version is kCompactRaw.
  /// When encodingLayoutTree is specified, used as fallback for streams or
  /// nested encodings not captured in the layout tree. When encodingLayoutTree
  /// is not specified, used directly for all streams.
  EncodingSelectionPolicyFactory encodingSelectionPolicyFactory =
      defaultEncodingSelectionPolicyFactory();

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

  /// Encoding type for stream sizes in the trailer.
  /// Supported types: Trivial, Varint, Delta, FixedBitWidth, MainlyConstant.
  EncodingType streamSizesEncodingType{EncodingType::FixedBitWidth};

  /// Returns true if the serialized data has a version header byte.
  bool hasVersionHeader() const;

  /// Returns the effective serialization version.
  SerializationVersion serializationVersion() const;

  /// Returns true if nimble encoding is enabled (version is kCompactRaw).
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

  /// Maximum number of scratch buffers each per-stream BufferPool retains
  /// for reuse across encoding lifetimes. Higher values reduce MemoryPool
  /// allocation churn at the cost of resident memory. 0 disables the pool
  /// entirely (every encoding allocates and frees through MemoryPool).
  size_t bufferPoolCapacity{velox::BufferPool::kDefaultCapacity};

  /// Executor for parallel decoding of child fields.
  /// When set, RowFieldReader and StructFlatMapFieldReader dispatch child reads
  /// as coroutines to this executor, using co_await to yield threads back to
  /// the pool. This prevents deadlock from nested parallelism.
  /// When nullptr (default), child reads are performed sequentially.
  folly::Executor* decodeExecutor{nullptr};

  /// Maximum number of parallel coroutine tasks for child field decoding.
  /// Children are grouped into this many batches, each decoded sequentially
  /// within a single coroutine task. 0 disables parallel decoding.
  uint32_t maxDecodeParallelism{0};

  /// Minimum number of child streams per parallel decode task. Ensures each
  /// coroutine task has enough work to amortize threading overhead.
  uint32_t minStreamsPerDecodeUnit{1};
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
