// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/common/Vector.h"

namespace facebook::alpha {

class ZstdCompression {
 public:
  static std::optional<Vector<char>> compress(
      velox::memory::MemoryPool& memoryPool,
      std::string_view source,
      int32_t level = 1);

  static Vector<char> uncompress(
      velox::memory::MemoryPool& memoryPool,
      std::string_view source);
};

} // namespace facebook::alpha
