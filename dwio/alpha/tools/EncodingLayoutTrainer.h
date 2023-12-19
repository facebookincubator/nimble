// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <optional>
#include <span>
#include <vector>
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/velox/VeloxWriterOptions.h"
#include "folly/Executor.h"
#include "velox/common/memory/Memory.h"

namespace facebook::alpha::tools {

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

} // namespace facebook::alpha::tools
