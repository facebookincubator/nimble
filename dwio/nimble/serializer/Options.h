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
/// Combines stream format (Dense/Sparse) with encoding type (legacy/nimble).
///
/// Stream formats:
/// - Dense: Streams in offset order, zeros for missing streams
/// - Sparse: Explicit stream_count and offsets array
///
/// Encoding types:
/// - Legacy: Simple zstd compression [size:u32][compression_type:i8][data...]
/// - Encoded: Nimble encoding framework (Dictionary, RLE, etc.)
enum class SerializationVersion : uint8_t {
  kDense = 0, // Dense format, legacy compression
  kSparse = 1, // Sparse format, legacy compression
  kDenseEncoded = 2, // Dense format, nimble encoding
  kSparseEncoded = 3, // Sparse format, nimble encoding
};

std::string toString(SerializationVersion version);

inline std::ostream& operator<<(
    std::ostream& os,
    SerializationVersion version) {
  return os << toString(version);
}

struct SerializerOptions {
  CompressionType compressionType{CompressionType::Uncompressed};
  uint32_t compressionThreshold{0};
  int32_t compressionLevel{0};

  /// Serialization format version.
  /// - nullopt (default): Dense format with no version header (kDense).
  /// - kDense/kSparse: Legacy compression format.
  /// - kDenseEncoded/kSparseEncoded: Nimble encoding format.
  std::optional<SerializationVersion> version{};

  /// Columns that should be encoded as flat maps.
  folly::F14FastSet<std::string> flatMapColumns{};

  /// Factory for creating encoding selection policies.
  /// Only used when version is kDenseEncoded or kSparseEncoded.
  /// Falls back to this when encodingLayoutTree doesn't have a layout for a
  /// stream.
  EncodingSelectionPolicyFactory encodingSelectionPolicyFactory =
      [](DataType dataType) -> std::unique_ptr<EncodingSelectionPolicyBase> {
    return ManualEncodingSelectionPolicyFactory{}.createPolicy(dataType);
  };

  /// Compression options for encodings.
  /// Only used when version is kDenseEncoded or kSparseEncoded.
  CompressionOptions compressionOptions{};

  /// Optional captured encoding layout tree.
  /// When specified, encodings are replayed from this tree instead of using
  /// encodingSelectionPolicyFactory. This speeds up writes by skipping runtime
  /// encoding selection and can provide better encodings based on historical
  /// data. Falls back to encodingSelectionPolicyFactory for streams not in the
  /// tree.
  std::optional<EncodingLayoutTree> encodingLayoutTree{};

  // Helper methods for version checks
  bool hasVersionHeader() const;
  SerializationVersion serializationVersion() const;
  bool enableEncoding() const;
  bool denseFormat() const;
  bool sparseFormat() const;
};

struct DeserializerOptions {
  /// Serialization format version expected in the input.
  /// - nullopt (default): Dense format (version 0) with no version header.
  /// - kDense/kSparse: Legacy compression format.
  /// - kDenseEncoded/kSparseEncoded: Nimble encoding format.
  std::optional<SerializationVersion> version{};

  // Helper methods for version checks
  bool hasVersionHeader() const;
  SerializationVersion serializationVersion() const;
  bool enableEncoding() const;
  bool denseFormat() const;
  bool sparseFormat() const;
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
