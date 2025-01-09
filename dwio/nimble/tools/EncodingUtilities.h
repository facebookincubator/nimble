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

#include <functional>
#include <string>
#include <string_view>
#include <vector>
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/velox/ChunkedStream.h"

namespace facebook::nimble::tools {

enum class EncodingPropertyType {
  Compression,
  EncodedSize,
};

std::ostream& operator<<(std::ostream& out, EncodingPropertyType propertyType);

struct EncodingProperty {
  std::string value;
  std::string_view data;
};

void traverseEncodings(
    std::string_view stream,
    std::function<bool(
        EncodingType,
        DataType,
        uint32_t /* level */,
        uint32_t /* index */,
        std::string /* nestedEncodingName */,
        std::unordered_map<
            EncodingPropertyType,
            EncodingProperty> /* properties */)> visitor);

std::string getStreamInputLabel(nimble::ChunkedStream& stream);
std::string getEncodingLabel(std::string_view stream);

} // namespace facebook::nimble::tools
