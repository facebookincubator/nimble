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
#include "dwio/nimble/serializer/SerializerImpl.h"
#include "dwio/nimble/velox/FieldWriter.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/vector/BaseVector.h"

namespace facebook::nimble {

/// Serializer converts Velox vectors into a serialized nimble format.
///
/// This class provides a lightweight serialization interface for converting
/// Velox vectors to nimble encoded byte streams. It supports flat map encoding
/// for specified columns via Serializer::Options::flatMapColumns.
///
/// NOTE: This serializer does not support null value encoding at the top-level.
/// All input vectors are expected to have no top-level nulls.
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

  const SchemaBuilder& schemaBuilder() const {
    return context_.schemaBuilder();
  }

 private:
  // Build stream encoding layouts map from the encoding layout tree.
  // Also sets up event handler for dynamically discovered FlatMap keys.
  // Called when encodingLayoutTree is specified.
  void buildStreamEncodingLayouts();

  // Helper to traverse the EncodingLayoutTree and populate
  // streamEncodingLayouts_.
  void initEncodingLayouts(
      const EncodingLayoutTree& tree,
      const TypeBuilder& typeBuilder);

  // Returns pointer to streamEncodingLayouts_ if encoding is enabled and we
  // have captured encodings to replay. Otherwise returns nullptr.
  const std::unordered_map<uint32_t, const EncodingLayout*>*
  getStreamEncodingLayouts() const {
    return (options_.enableEncoding() && !streamEncodingLayouts_.empty())
        ? &streamEncodingLayouts_
        : nullptr;
  }

  const SerializerOptions options_;
  velox::memory::MemoryPool* const pool_;
  mutable FieldWriterContext context_;
  std::unique_ptr<FieldWriter> writer_;
  mutable Vector<char> buffer_;
  // Reusable buffer for collecting non-empty streams during serialization.
  mutable std::vector<const StreamData*> nonEmptyStreams_;
  // Map from stream offset to encoding layout for replaying captured encodings.
  // Only populated when options_.encodingLayoutTree is set.
  // Mutable because FlatMap keys can be added during const serialize().
  mutable std::unordered_map<uint32_t, const EncodingLayout*>
      streamEncodingLayouts_;
};

template <typename T>
void Serializer::serialize(
    const velox::VectorPtr& vector,
    const OrderedRanges& ranges,
    T& buffer) const {
  writer_->write(vector, ranges);

  // For kSparse format, collect non-empty streams upfront (needed for header).
  // For kDense format, we iterate over all streams and skip empty ones.
  if (options_.sparseFormat()) {
    NIMBLE_CHECK(nonEmptyStreams_.empty());
    nonEmptyStreams_.reserve(context_.streams().size());
    for (auto& [_, streamData] : context_.streams()) {
      if (!streamData->data().empty() || !streamData->nonNulls().empty()) {
        nonEmptyStreams_.push_back(streamData.get());
      }
    }
  }

  // Write header and stream data.
  serde::StreamDataWriter<T> streamWriter{
      options_,
      buffer,
      static_cast<uint32_t>(ranges.size()),
      nonEmptyStreams_,
      pool_,
      getStreamEncodingLayouts()};
  if (options_.sparseFormat()) {
    for (const auto* streamData : nonEmptyStreams_) {
      streamWriter.writeData(*streamData);
    }
    nonEmptyStreams_.clear();
  } else {
    NIMBLE_CHECK(options_.denseFormat());
    for (auto& [_, streamData] : context_.streams()) {
      streamWriter.writeData(*streamData);
    }
  }
  // Pass nodeCount for SerializationVersion::kDense to fill trailing zeros.
  streamWriter.close(context_.schemaBuilder().nodeCount());

  writer_->reset();
  context_.resetStringBuffer();
}

} // namespace facebook::nimble
