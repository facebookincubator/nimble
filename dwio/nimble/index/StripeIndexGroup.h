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
#include <optional>
#include <string_view>

namespace facebook::nimble {

class MetadataBuffer;

struct ChunkLocation {
  uint32_t streamOffset;
  uint32_t rowOffset;

  ChunkLocation(uint32_t _streamOffset, uint32_t _rowOffset)
      : streamOffset(_streamOffset), rowOffset(_rowOffset) {}
};

class StreamIndex;

class StripeIndexGroup {
 public:
  static std::shared_ptr<StripeIndexGroup> create(
      uint32_t stripeIndexGroupId,
      std::unique_ptr<MetadataBuffer> metadata);

  /// Lookup chunk by stripe and encoded key
  /// Returns chunk offset and size, or std::nullopt if not found
  std::optional<ChunkLocation> lookupChunk(
      uint32_t stripe,
      std::string_view encodedKey) const;

  /// Lookup chunk by stripe, stream ID, and row ID
  /// Returns chunk offset and size, or std::nullopt if not found
  std::optional<ChunkLocation>
  lookupChunk(uint32_t stripe, uint32_t streamId, uint32_t rowId) const;

  /// Creates a StreamIndex for the specified stripe and stream ID
  std::shared_ptr<StreamIndex> createStreamIndex(
      uint32_t stripe,
      uint32_t streamId) const;

 private:
  StripeIndexGroup(
      uint32_t stripeIndexGroupId,
      std::unique_ptr<MetadataBuffer> metadata);

  const std::unique_ptr<MetadataBuffer> metadata_;
  const uint32_t stripeIndexGroupId_;
};

/// StreamIndex is a convenience wrapper around StripeIndexGroup that
/// pre-configures stripe and streamId, allowing lookups with just rowId.
class StreamIndex {
 public:
  static std::shared_ptr<StreamIndex> create(
      const StripeIndexGroup* stripeIndexGroup,
      uint32_t stripe,
      uint32_t streamId);

  /// Lookup chunk by row ID
  /// Returns chunk offset and size, or std::nullopt if not found
  std::optional<ChunkLocation> lookupChunk(uint32_t rowId) const;

 private:
  StreamIndex(
      const StripeIndexGroup* stripeIndexGroup,
      uint32_t stripe,
      uint32_t streamId);

  const StripeIndexGroup* const stripeIndexGroup_;
  const uint32_t stripe_;
  const uint32_t streamId_;
};

} // namespace facebook::nimble
