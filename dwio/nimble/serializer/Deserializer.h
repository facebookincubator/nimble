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
#include "dwio/nimble/velox/FieldReader.h"
#include "folly/Range.h"
#include "folly/container/F14Map.h"
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
  // Creates deserializers for a type and its FlatMap inMap streams.
  void createDeserializersForType(const Type& type, uint32_t depth);

  // Creates FieldReaderParams from DeserializerOptions, including decode
  // executor, parallel decode threshold, and flatmap-as-struct settings.
  FieldReaderParams createFieldReaderParams() const;

  void deserialize(
      folly::Range<const std::string_view*> data,
      velox::VectorPtr& output) const;

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

  // Registers this batch's physical stream segments and synthesizes omitted
  // FlatMap in-map segments when older serializers leave them implicit.
  void appendStreamSegments(
      uint32_t rowCount,
      uint32_t startRow,
      bool requiresBarrier) const;

  // Appends a decoded run to the accumulated output vector.
  void appendToOutput(velox::VectorPtr&& decoded, velox::VectorPtr& output)
      const;

  // Decodes the pending non-barrier batch run and resets reader state for the
  // next run.
  void decodeRun(DecodeRun& run, velox::VectorPtr& output) const;

  // --- Const members (set at construction, never modified) ---
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
