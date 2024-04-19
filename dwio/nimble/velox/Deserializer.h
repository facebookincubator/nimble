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

#include "dwio/nimble/velox/FieldReader.h"
#include "folly/container/F14Map.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/BaseVector.h"

namespace facebook::nimble {

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

} // namespace facebook::nimble
