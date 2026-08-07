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

#include <optional>

#include "dwio/nimble/encodings/common/EncodingLayout.h"
#include "dwio/nimble/velox/SchemaBuilder.h"

namespace facebook::nimble {

class EncodingLayoutStreamContext : public StreamContext {
 public:
  const EncodingLayout* encoding() const {
    return encoding_.has_value() ? &*encoding_ : nullptr;
  }

  void setEncoding(EncodingLayout value) {
    encoding_.emplace(std::move(value));
  }

 private:
  std::optional<EncodingLayout> encoding_;
};

inline EncodingLayoutStreamContext& encodingLayoutContext(
    const StreamDescriptorBuilder& descriptor) {
  auto* ctx = descriptor.context<EncodingLayoutStreamContext>();
  if (ctx != nullptr) {
    return *ctx;
  }
  descriptor.setContext(std::make_unique<EncodingLayoutStreamContext>());
  return *descriptor.context<EncodingLayoutStreamContext>();
}

} // namespace facebook::nimble
