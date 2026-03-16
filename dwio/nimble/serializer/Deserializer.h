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

#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/velox/FieldReader.h"
#include "folly/container/F14Map.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/BaseVector.h"

namespace facebook::nimble {

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

  void deserialize(std::string_view data, velox::VectorPtr& vector) const;

  void deserialize(
      const std::vector<std::string_view>& data,
      velox::VectorPtr& vector) const;

 private:
  // Creates deserializers for a type and its FlatMap inMap streams.
  void createDeserializersForType(const Type& type, uint32_t depth);

  // Populates FieldReaderParams for flatmap-as-struct deserialization based on
  // outputType. For each top-level flatmap column whose corresponding output
  // field is ROW, sets readFlatMapFieldAsStruct and flatMapFeatureSelector.
  void populateFlatMapAsStructParams(FieldReaderParams& params) const;

  // --- Const members (set at construction, never modified) ---
  const std::shared_ptr<const Type> schema_;
  velox::memory::MemoryPool* const pool_;
  const DeserializerOptions options_;

  // --- Non-const members (assigned in constructor body) ---
  std::unique_ptr<FieldReaderFactory> rootFactory_;
  std::unique_ptr<FieldReader> rootReader_;

  // --- Mutable members (modified during deserialization) ---
  mutable folly::F14FastMap<uint32_t, std::unique_ptr<Decoder>> deserializers_;
  mutable std::vector<std::string_view> inputBuffer_;

  // Map from in-map stream offset to the child value type for detecting
  // present in-map streams. When an in-map stream is missing from a batch but
  // its value streams are present, the key is present in every row (skipped by
  // the serializer). Only populated for top-level FlatMap types (depth 1).
  folly::F14FastMap<uint32_t, const Type*> inMapChildTypes_;

  // Reusable set of stream offsets present in the current batch. Cleared per
  // batch in deserialize(). Only used when inMapChildTypes_ is non-empty.
  mutable folly::F14FastSet<uint32_t> presentStreamOffsets_;
};

} // namespace facebook::nimble
