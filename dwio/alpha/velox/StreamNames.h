// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/velox/SchemaReader.h"

namespace facebook::alpha {

class StreamNames {
 public:
  explicit StreamNames(const std::shared_ptr<const Type>& root);

  std::string_view streamName(offset_size offset) const;

 private:
  std::vector<std::string> names_;
  std::vector<size_t> toName_;
};

} // namespace facebook::alpha
