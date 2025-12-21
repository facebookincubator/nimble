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

#include "dwio/nimble/common/Exceptions.h"
#include "velox/common/file/Region.h"

namespace facebook::nimble {

class MetadataBuffer;
class StripeGroup;

/// Represents the location of a chunk within a stream.
/// Both offsets are relative:
/// - streamOffset: relative to the stream start offset within the stripe
/// - rowOffset: relative to the stripe start row id
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
      uint32_t stripeGroupIndex,
      const MetadataBuffer& rootIndex,
      std::unique_ptr<MetadataBuffer> groupIndex);

  /// Lookup chunk by stripe and encoded key.
  /// The lookup is performed on the key stream which is stored in the
  /// StripeIndexGroup metadata.
  /// Returns chunk location, or std::nullopt if not found.
  std::optional<ChunkLocation> lookupChunk(
      uint32_t stripe,
      std::string_view encodedKey) const;

  /// Lookup chunk by stripe, stream ID, and row ID.
  /// Returns chunk location, or std::nullopt if not found.
  std::optional<ChunkLocation>
  lookupChunk(uint32_t stripe, uint32_t streamId, uint32_t rowId) const;

  /// Creates a StreamIndex for the specified stripe and stream ID.
  std::unique_ptr<StreamIndex> createStreamIndex(
      uint32_t stripe,
      uint32_t streamId) const;

  /// Returns the region for the key stream of the specified stripe.
  /// The key stream is stored in the StripeIndexGroup metadata.
  /// Returns std::nullopt if the stripe has no key stream.
  std::optional<velox::common::Region> keyStreamRegion(
      uint32_t stripeIndex) const;

 private:
  StripeIndexGroup(
      uint32_t groupIndex,
      uint32_t firstStripe,
      std::unique_ptr<MetadataBuffer> metadata);

  inline uint32_t stripeOffset(uint32_t stripe) const {
    NIMBLE_CHECK_GE(
        stripe, firstStripe_, "Stripe index is before this group's range");
    const uint32_t offset = stripe - firstStripe_;
    NIMBLE_CHECK_LT(
        offset,
        stripeCount_,
        "Stripe offset is out of range for this stripe index group");
    return offset;
  }

  const std::unique_ptr<MetadataBuffer> metadata_;
  const uint32_t groupIndex_;
  const uint32_t firstStripe_;
  const uint32_t stripeCount_;
  const uint32_t streamCount_;
};

/// StreamIndex is a convenience wrapper around StripeIndexGroup for rowId-based
/// chunk location lookup on a specific stream.
class StreamIndex {
 public:
  static std::unique_ptr<StreamIndex> create(
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
