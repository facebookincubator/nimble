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

#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/serializer/StreamDataWriter.h"
#include "dwio/nimble/velox/FieldWriter.h"
#include "dwio/nimble/writer/EncodingLayoutTree.h"
#include "folly/container/F14Set.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/vector/BaseVector.h"

namespace facebook::nimble {

/// Serializer converts Velox vectors into a serialized nimble format.
///
/// This class provides a lightweight serialization interface for converting
/// Velox vectors to nimble encoded byte streams. It supports flat map encoding
/// for specified columns via Serializer::Options::flatMapColumns.
class Serializer {
 public:
  using Options = SerializerOptions;

  Serializer(
      SerializerOptions options,
      const std::shared_ptr<const velox::Type>& type,
      velox::memory::MemoryPool* pool);

  std::string_view serialize(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges) const;

  // Takes any buffer-like object and serialize the vector into it.
  template <typename T>
  void serialize(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges,
      T& buffer) const;

  /// Returns the cached encoding layout. Present after a successful
  /// serialize() call with options.cacheEncodingLayout set, or if a
  /// caller-provided replayed layout was supplied via
  /// options.encodingLayoutTree. nullopt otherwise.
  const std::optional<EncodingLayoutTree>& getCachedEncodingLayout() const {
    return encodingLayoutTree_;
  }

  const SchemaBuilder& schemaBuilder() const {
    return context_.schemaBuilder();
  }

 private:
  // Traverses the EncodingLayoutTree and stamps each stream descriptor's
  // context with the corresponding EncodingLayout for replay at encode time.
  void initializeEncodingLayouts(
      const TypeBuilder& typeBuilder,
      const EncodingLayoutTree& tree);

  // Assembles the cached encoding layout tree by walking the schema and
  // reading each stream descriptor's EncodingLayoutStreamContext slot.
  void assembleEncodingLayoutTree() const;

  // Validates input shapes that the serializer can write but the dense
  // deserializer cannot reconstruct.
  void validateSupportedInput(
      const velox::VectorPtr& vector,
      const OrderedRanges& ranges) const;

  SerializerOptions options_;
  mutable FieldWriterContext context_;
  // Reused across serialize() calls for nested child streams.
  std::unique_ptr<EncodingBufferPool> nestedEncodingBufferPool_;
  // Kept alive because FlatMap field writers hold references to child nodes.
  std::shared_ptr<const velox::dwio::common::TypeWithId> typeWithId_;
  std::unique_ptr<FieldWriter> writer_;
  mutable Vector<char> buffer_;
  // Selected encoding layout assembled at the end of each call to serialize.
  mutable std::optional<EncodingLayoutTree> encodingLayoutTree_;
  // In-map stream offsets for skipping constant FlatMap key-presence streams.
  // Mutable because FlatMap keys can be added during const serialize().
  mutable folly::F14FastSet<uint32_t> inMapStreamOffsets_;
};

template <typename T>
void Serializer::serialize(
    const velox::VectorPtr& vector,
    const OrderedRanges& ranges,
    T& buffer) const {
  validateSupportedInput(vector, ranges);
  writer_->write(vector, ranges);

  serde::StreamDataWriter<T> streamWriter{
      options_,
      buffer,
      static_cast<uint32_t>(ranges.size()),
      context_.bufferMemoryPool().get()};
  for (auto& [_, streamData] : context_.streams()) {
    if (streamData->isNullStream()) {
      // Omit any null stream that carries no actual nulls, even when a
      // validity bitmap was allocated (hasNulls() true but all-true). Such
      // streams are reconstructed as all-true on read. This must match the
      // null-barrier flag (computed from hasNullValues()): writing an all-true
      // null stream would make it present in only some batches of a dense
      // concat run, which the reader cannot stitch.
      if (!streamData->hasNullValues()) {
        continue;
      }
    }
    if (!inMapStreamOffsets_.empty()) {
      const auto streamOffset = streamData->descriptor().offset();
      if (inMapStreamOffsets_.contains(streamOffset)) {
        streamData->materialize();
        if (isConstantBoolStream(streamData->data())) {
          continue;
        }
      }
    }
    streamWriter.writeData(*streamData);
  }
  // Pass nodeCount for kLegacy to fill trailing zeros.
  streamWriter.close(context_.schemaBuilder().nodeCount());
  if (options_.cacheEncodingLayout || options_.encodingLayoutTree.has_value()) {
    assembleEncodingLayoutTree();
  }

  writer_->reset();
  context_.resetStringBuffer();
}

} // namespace facebook::nimble
