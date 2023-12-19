// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <optional>
#include "dwio/alpha/encodings/EncodingLayout.h"
#include "dwio/alpha/velox/SchemaTypes.h"

namespace facebook::alpha {

class EncodingLayoutTree {
 public:
  EncodingLayoutTree(
      Kind schemaKind,
      std::optional<EncodingLayout> encodingLayout,
      std::string name,
      std::vector<EncodingLayoutTree> children = {});

  Kind schemaKind() const;
  const std::optional<EncodingLayout>& encodingLayout() const;
  const std::string& name() const;
  uint32_t childrenCount() const;
  const EncodingLayoutTree& child(uint32_t index) const;

  uint32_t serialize(std::span<char> output) const;
  static EncodingLayoutTree create(std::string_view tree);

 private:
  const Kind schemaKind_;
  const std::optional<EncodingLayout> encodingLayout_;
  const std::string name_;
  const std::vector<EncodingLayoutTree> children_;
};

} // namespace facebook::alpha
