// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/velox/SchemaBuilder.h"
#include "dwio/alpha/velox/SchemaGenerated.h"

namespace facebook::alpha {

class SchemaSerializer {
 public:
  SchemaSerializer();

  std::string_view serialize(const SchemaBuilder& builder);

 private:
  flatbuffers::FlatBufferBuilder builder_;
};

class SchemaDeserializer {
 public:
  static std::shared_ptr<const Type> deserialize(std::string_view schema);
};

} // namespace facebook::alpha
