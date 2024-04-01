// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <unordered_map>
#include "dwio/alpha/encodings/EncodingLayout.h"
#include "dwio/alpha/velox/SchemaTypes.h"
#include "folly/CppAttributes.h"

namespace facebook::alpha {

class EncodingLayoutTree {
 public:
  using StreamIdentifier = uint8_t;

  struct StreamIdentifiers {
    struct Scalar {
      constexpr static StreamIdentifier ScalarStream = 0;
    };
    struct Array {
      constexpr static StreamIdentifier LengthsStream = 0;
    };
    struct Map {
      constexpr static StreamIdentifier LengthsStream = 0;
    };
    struct Row {
      constexpr static StreamIdentifier NullsStream = 0;
    };
    struct FlatMap {
      constexpr static StreamIdentifier NullsStream = 0;
    };
    struct ArrayWithOffsets {
      constexpr static StreamIdentifier OffsetsStream = 0;
      constexpr static StreamIdentifier LengthsStream = 1;
    };
  };

  EncodingLayoutTree(
      Kind schemaKind,
      std::unordered_map<StreamIdentifier, EncodingLayout> encodingLayouts,
      std::string name,
      std::vector<EncodingLayoutTree> children = {});

  Kind schemaKind() const;
  const EncodingLayout* FOLLY_NULLABLE encodingLayout(StreamIdentifier) const;
  const std::string& name() const;
  uint32_t childrenCount() const;
  const EncodingLayoutTree& child(uint32_t index) const;

  uint32_t serialize(std::span<char> output) const;
  static EncodingLayoutTree create(std::string_view tree);

  std::vector<StreamIdentifier> encodingLayoutIdentifiers() const;

 private:
  const Kind schemaKind_;
  const std::unordered_map<StreamIdentifier, EncodingLayout> encodingLayouts_;
  const std::string name_;
  const std::vector<EncodingLayoutTree> children_;
};

} // namespace facebook::alpha
