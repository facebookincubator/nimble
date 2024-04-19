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

#include <optional>
#include <span>
#include <vector>
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/velox/EncodingLayoutTree.h"
#include "folly/Executor.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble::tools {

class EncodingLayoutTrainer {
 public:
  EncodingLayoutTrainer(
      velox::memory::MemoryPool& memoryPool,
      std::vector<std::string_view> files,
      std::string serializedSerde = "");

  EncodingLayoutTree train(folly::Executor& executor);

 private:
  velox::memory::MemoryPool& memoryPool_;
  const std::vector<std::string_view> files_;
  const std::string serializedSerde_;
};

} // namespace facebook::nimble::tools
