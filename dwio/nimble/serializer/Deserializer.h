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

#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/serializer/StreamSelector.h"
#include "dwio/nimble/velox/FieldReader.h"
#include "folly/Range.h"
#include "folly/container/F14Map.h"
#include "velox/buffer/Buffer.h"
#include "velox/vector/BaseVector.h"

namespace facebook::nimble {

namespace serde {
class StreamDataParser;
} // namespace serde

/// Deserializer converts serialized nimble streams into Velox vectors.
///
/// This class provides a lightweight deserialization interface for
/// materializing one or more serialized batches into a vector matching the
/// configured schema. It supports FlatMap reconstruction and omitted
/// null/in-map streams according to Deserializer::Options.
class Deserializer {
 public:
  using Options = DeserializerOptions;

  Deserializer(
      std::shared_ptr<const Type> schema,
      velox::memory::MemoryPool* pool);

  Deserializer(
      std::shared_ptr<const Type> schema,
      velox::memory::MemoryPool* pool,
      DeserializerOptions options);

  /// Constructs a deserializer that selects (projects) `projectSubfields` out
  /// of each input via an internal StreamSelector, then decodes only the
  /// selected streams directly, in a single call, without materializing or
  /// re-parsing an intermediate projected blob. The decode path depends only on
  /// StreamSelector, not on the blob-assembling Projector. The output schema is
  /// the selector's projected schema; any `options.outputType` must match it.
  Deserializer(
      std::shared_ptr<const Type> inputSchema,
      velox::memory::MemoryPool* pool,
      const std::vector<serde::Subfield>& projectSubfields,
      const serde::StreamSelector::Options& selectorOptions,
      DeserializerOptions options);

  ~Deserializer();

  Deserializer(Deserializer&&) = delete;
  Deserializer& operator=(Deserializer&&) = delete;
  Deserializer(const Deserializer&) = delete;
  Deserializer& operator=(const Deserializer&) = delete;

  void deserialize(std::string_view data, velox::VectorPtr& output) const;

  void deserialize(
      const std::vector<std::string_view>& data,
      velox::VectorPtr& output) const;

 private:
  // Builds the field-reader tree and per-stream decoder lookup from schema_.
  // Shared by all constructors after schema_ is set.
  void initReaders();

  // Creates deserializers for a type and its FlatMap inMap streams.
  void createDeserializersForType(const Type& type, uint32_t depth);

  // Creates FieldReaderParams from DeserializerOptions, including decode
  // executor, parallel decode threshold, and flatmap-as-struct settings.
  FieldReaderParams createFieldReaderParams() const;

  void deserialize(
      folly::Range<const std::string_view*> data,
      velox::VectorPtr& output) const;

  // Projecting decode (selector_ != nullptr): reuses the StreamSelector's
  // stream selection (selectStreams) and decodes the selected streams
  // directly, without materializing or re-parsing an intermediate projected
  // blob.
  void deserializeProjected(
      folly::Range<const std::string_view*> data,
      velox::VectorPtr& output) const;

  // Feeds one batch's projected streams (from StreamSelector::selectStreams)
  // to the per-stream decoders, including FlatMap in-map reconstruction.
  // `coalescedStreamBuffers` retains pool-backed coalesced copies of streams
  // that span IOBuf nodes until the decode run consumes them.
  void appendProjectedStreamSegments(
      const serde::ProjectedStreams& streams,
      std::vector<velox::BufferPtr>& coalescedStreamBuffers,
      uint32_t startRow) const;

  // Open run of non-barrier batches that can be decoded together.
  struct DecodeRun {
    uint32_t rows{0};
    uint32_t batches{0};
  };

  // Adds one serialized batch to the current decode run, decoding before and
  // after batches that require a null barrier.
  void appendBatch(
      std::string_view batch,
      DecodeRun& run,
      velox::VectorPtr& output) const;

  // Adds one batch's selected streams to the current decode run, decoding
  // before and after batches that require a null barrier.
  void appendProjectedBatch(
      const serde::ProjectedStreams& streams,
      std::vector<velox::BufferPtr>& coalescedStreamBuffers,
      DecodeRun& run,
      velox::VectorPtr& output) const;

  // Registers this batch's physical stream segments and reconstructs omitted
  // FlatMap in-map streams when older serializers leave them implicit.
  void appendStreamSegments(
      uint32_t rowCount,
      uint32_t startRow,
      bool requiresBarrier) const;

  // The following three helpers are the shared body of appendStreamSegments and
  // appendProjectedStreamSegments; the two paths differ only in how they
  // enumerate the batch's streams.

  // Clears per-batch stream-presence tracking. No-op when the schema has no
  // FlatMap children (presence tracking only feeds in-map reconstruction).
  void resetInMapPresenceTracking() const;

  // Records one physical stream segment on the decoder at `offset`, skipping
  // indices beyond the projected schema, and marks the stream present for
  // in-map inference.
  void recordStreamSegment(
      uint32_t offset,
      std::string_view streamData,
      SerializationVersion version,
      uint32_t startRow) const;

  // Reconstructs in-map streams the writer omitted: for each FlatMap key whose
  // value stream appeared this batch but whose in-map stream did not, records
  // an all-present in-map segment.
  void reconstructOmittedInMapStreams(
      uint32_t startRow,
      uint32_t rowCount,
      bool requiresBarrier) const;

  // Appends a decoded run to the accumulated output vector.
  void appendToOutput(velox::VectorPtr&& decoded, velox::VectorPtr& output)
      const;

  // Decodes the pending non-barrier batch run and resets reader state for the
  // next run.
  void decodeRun(DecodeRun& run, velox::VectorPtr& output) const;

  // --- Const members (set at construction, never modified) ---
  // Optional stream selector built from the projected subfields (projecting
  // constructor). When set, deserialize() projects each input via selector_ and
  // decodes the selected streams directly; null for the non-projecting
  // constructors. Declared before schema_ so schema_ can be initialized from
  // its projected schema.
  const std::unique_ptr<const serde::StreamSelector> selector_;
  const std::shared_ptr<const Type> schema_;
  velox::memory::MemoryPool* const pool_;
  const DeserializerOptions options_;

  // --- Non-const members (assigned in constructor body) ---
  std::unique_ptr<FieldReaderFactory> rootFactory_;
  std::unique_ptr<FieldReader> reader_;
  mutable std::unique_ptr<serde::StreamDataParser> parser_;

  // --- Mutable members (modified during deserialization) ---
  mutable folly::F14FastMap<uint32_t, std::unique_ptr<Decoder>>
      deserializerMap_;

  // Flat vector indexed by stream offset for O(1) lookup in deserialize().
  // Non-owning pointers; ownership stays in deserializerMap_.
  mutable std::vector<Decoder*> deserializers_;

  // Maps FlatMap in-map stream offsets to their child value types. Used to
  // reconstruct in-map streams omitted by older serializers.
  folly::F14FastMap<uint32_t, const Type*> inMapChildTypes_;

  // Cached !inMapChildTypes_.empty(), computed once in initReaders(). The
  // per-stream decode path skips all in-map presence tracking when false.
  bool hasInMapChildren_{false};

  static constexpr uint32_t kInvalidInMapOffset =
      std::numeric_limits<uint32_t>::max();

  // Reverse lookup from a FlatMap child value-stream offset to its in-map
  // stream offset. Entries that are not FlatMap child value streams use
  // kInvalidInMapOffset.
  std::vector<uint32_t> valueOffsetToInMap_;

  // Per-batch stream presence, indexed by stream offset.
  mutable std::vector<bool> streamPresentFlags_;
  // Stream offsets present in the current batch. Used to find omitted FlatMap
  // in-map streams.
  mutable std::vector<uint32_t> presentStreamOffsets_;
};

} // namespace facebook::nimble
