// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <optional>
#include <ostream>

namespace facebook::alpha {

using offset_size = uint32_t;

enum class ScalarKind : uint8_t {
  Int8,
  UInt8,
  Int16,
  UInt16,
  Int32,
  UInt32,
  Int64,
  UInt64,
  Float,
  Double,
  Bool,
  String,
  Binary,

  Undefined = 255,
};

enum class Kind : uint8_t {
  Scalar,
  Row,
  Array,
  ArrayWithOffsets,
  Map,
  FlatMap,
};

std::string toString(ScalarKind kind);
std::string toString(Kind kind);

class SchemaNode {
 public:
  SchemaNode(
      Kind kind,
      offset_size offset,
      ScalarKind scalarKind,
      std::optional<std::string> name = std::nullopt,
      size_t childrenCount = 0)
      : kind_{kind},
        offset_{offset},
        name_{std::move(name)},
        scalarKind_{scalarKind},
        childrenCount_{childrenCount} {}

  Kind kind() const {
    return kind_;
  }

  size_t childrenCount() const {
    return childrenCount_;
  }

  offset_size offset() const {
    return offset_;
  }

  std::optional<std::string> name() const {
    return name_;
  }

  ScalarKind scalarKind() const {
    return scalarKind_;
  }

 private:
  Kind kind_;
  offset_size offset_;
  std::optional<std::string> name_;
  ScalarKind scalarKind_;
  size_t childrenCount_;
};

} // namespace facebook::alpha
