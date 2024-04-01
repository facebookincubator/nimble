// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <folly/Executor.h>
#include <folly/container/F14Map.h>
#include "dwio/alpha/velox/Decoder.h"
#include "dwio/alpha/velox/SchemaReader.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/dwio/common/FlatMapHelper.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/vector/BaseVector.h"

namespace facebook::alpha {

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

  // Place the next X rows of data into the passed in output vector.
  virtual void next(
      uint32_t count,
      velox::VectorPtr& output,
      const bits::Bitmap* scatterBitmap) = 0;

  virtual void skip(uint32_t count) = 0;

  // Called at the end of stripe
  virtual void reset();

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
      const Type* alphaType)
      : pool_{pool}, veloxType_{std::move(veloxType)}, alphaType_{alphaType} {}

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
      const std::shared_ptr<const alpha::Type>& alphaType,
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
  const Type* alphaType_;
};

} // namespace facebook::alpha
