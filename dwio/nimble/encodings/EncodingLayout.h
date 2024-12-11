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
#include <span>
#include <utility>
#include <vector>

#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/EncodingIdentifier.h"

namespace facebook::nimble {

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

} // namespace facebook::nimble
