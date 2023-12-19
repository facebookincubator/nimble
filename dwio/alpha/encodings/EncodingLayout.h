// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/encodings/EncodingIdentifier.h"

namespace facebook::alpha {

class EncodingLayout {
 public:
  EncodingLayout(
      EncodingType encodingType,
      CompressionType compressionType,
      std::vector<std::optional<const EncodingLayout>> children = {});

  EncodingType encodingType() const;
  CompressionType compressionType() const;
  uint8_t childrenCount() const;
  const std::optional<const EncodingLayout>& child(
      NestedEncodingIdentifier identifier) const;

  int32_t serialize(std::span<char> output) const;
  static std::pair<EncodingLayout, uint32_t> create(std::string_view encoding);

 private:
  EncodingType encodingType_;
  CompressionType compressionType_;
  std::vector<std::optional<const EncodingLayout>> children_;
};

} // namespace facebook::alpha
