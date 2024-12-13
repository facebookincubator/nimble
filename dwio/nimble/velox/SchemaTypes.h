/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

#include <cstdint>
#include <optional>
#include <ostream>

namespace facebook::nimble {

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
  SlidingWindowMap,
};

std::string toString(ScalarKind kind);
std::string toString(Kind kind);

class StreamDescriptor {
 public:
  StreamDescriptor(offset_size offset, ScalarKind scalarKind)
      : offset_{offset}, scalarKind_{scalarKind} {}

  offset_size offset() const {
    return offset_;
  }

  ScalarKind scalarKind() const {
    return scalarKind_;
  }

 private:
  offset_size offset_;
  ScalarKind scalarKind_;
};

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

} // namespace facebook::nimble
