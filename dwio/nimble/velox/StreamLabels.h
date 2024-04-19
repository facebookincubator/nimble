// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/nimble/velox/SchemaReader.h"

namespace facebook::nimble {

class StreamLabels {
 public:
  explicit StreamLabels(const std::shared_ptr<const Type>& root);

  std::string_view streamLabel(offset_size offset) const;

 private:
  std::vector<std::string> labels_;
  std::vector<size_t> offsetToLabel_;
};

} // namespace facebook::nimble
