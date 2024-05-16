/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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

#include <folly/Executor.h>
#include <folly/container/F14Map.h>
#include "dwio/nimble/velox/Decoder.h"
#include "dwio/nimble/velox/SchemaReader.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/dwio/common/FlatMapHelper.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/vector/BaseVector.h"

namespace facebook::nimble {

enum class SelectionMode {
  Include = 0,
  Exclude = 1,
};

struct FeatureSelection {
  std::vector<std::string> features;
  // When mode == Include, only features appearing in 'features' will be
  // included in returned map, otherwise,
  // all features from the file will be returned in the map, excluding
  // the features appearing in 'features'.
  SelectionMode mode{SelectionMode::Include};
};

struct FieldReaderParams {
  // Allow selecting subset of features to be included/excluded in flat maps.
  // The key in the map is the flat map (top-level) column name.
  folly::F14FastMap<std::string, FeatureSelection> flatMapFeatureSelector;

  // Contains flatmap field name which we want to return as Struct
  folly::F14FastSet<std::string> readFlatMapFieldAsStruct;

  // Callback to populate feature projection stats when needed
  std::function<void(velox::dwio::common::flatmap::FlatMapKeySelectionStats)>
      keySelectionCallback{nullptr};
};

class FieldReader {
 public:
  FieldReader(
      velox::memory::MemoryPool& pool,
      velox::TypePtr type,
      Decoder* decoder)
      : pool_{pool}, type_{std::move(type)}, decoder_{decoder} {}

  virtual ~FieldReader() = default;

  // Estimation of the per row size of the field on current reading stripe in
  // bytes. Returns a pair containing the number of rows of the field and
  // average row size in bytes of the field. This method will return nullopt if
  // the field or encoding is not supported for estimation.
  //
  // NOTE: This is not the estimation based on the remaining rows, but the
  // entire stripe's rows.
  virtual std::optional<std::pair<uint32_t, uint64_t>> estimatedRowSize()
      const = 0;

  // Place the next 'count' rows of data into the passed in output vector.
  //
  // NOTE: scatterBitmap is not for external selectivity. External callers must
  // leave scatterBitmap nullptr
  virtual void next(
      uint32_t count,
      velox::VectorPtr& output,
      const bits::Bitmap* scatterBitmap = nullptr) = 0;

  virtual void skip(uint32_t count) = 0;

  // Called at the end of stripe
  virtual void reset();

  const velox::TypePtr& type() const {
    return type_;
  }

 protected:
  void ensureNullConstant(
      uint32_t count,
      velox::VectorPtr& output,
      const std::shared_ptr<const velox::Type>& type) const;

  velox::memory::MemoryPool& pool_;
  const velox::TypePtr type_;
  Decoder* decoder_;
};

class FieldReaderFactory {
 public:
  FieldReaderFactory(
      velox::memory::MemoryPool& pool,
      velox::TypePtr veloxType,
      const Type* nimbleType)
      : pool_{pool},
        veloxType_{std::move(veloxType)},
        nimbleType_{nimbleType} {}

  virtual ~FieldReaderFactory() = default;

  virtual std::unique_ptr<FieldReader> createReader(
      const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>&
          decoders) = 0;

  const velox::TypePtr& veloxType() const {
    return veloxType_;
  }

  // Build a field reader factory tree. Will traverse the passed in types and
  // create matching field readers.
  static std::unique_ptr<FieldReaderFactory> create(
      const FieldReaderParams& parameters,
      velox::memory::MemoryPool& pool,
      const std::shared_ptr<const nimble::Type>& nimbleType,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& veloxType,
      std::vector<uint32_t>& offsets,
      const std::function<bool(uint32_t)>& isSelected =
          [](auto) { return true; },
      folly::Executor* executor = nullptr);

 protected:
  std::unique_ptr<FieldReader> createNullColumnReader() const;

  Decoder* FOLLY_NULLABLE getDecoder(
      const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>& decoders,
      const StreamDescriptor& streamDescriptor) const;

  template <typename T, typename... Args>
  std::unique_ptr<FieldReader> createReaderImpl(
      const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>& decoders,
      const StreamDescriptor& nullsDecriptor,
      Args&&... args) const;

  velox::memory::MemoryPool& pool_;
  const velox::TypePtr veloxType_;
  const Type* nimbleType_;
};

} // namespace facebook::nimble
