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

#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/SchemaGenerated.h"
#include "dwio/nimble/velox/SchemaReader.h"

namespace facebook::nimble {

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

} // namespace facebook::nimble
