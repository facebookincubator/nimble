// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/velox/FieldReader.h"
#include "folly/container/F14Map.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/BaseVector.h"

namespace facebook::alpha {

class Deserializer {
 public:
  Deserializer(
      velox::memory::MemoryPool& pool,
      std::shared_ptr<const Type> schema);

  void deserialize(std::string_view data, velox::VectorPtr& vector);

 private:
  velox::memory::MemoryPool& pool_;
  std::shared_ptr<const Type> schema_;
  folly::F14FastMap<uint32_t, std::unique_ptr<Decoder>> deserializers_;
  std::unique_ptr<FieldReaderFactory> rootFactory_;
  std::unique_ptr<FieldReader> rootReader_;
};

} // namespace facebook::alpha
