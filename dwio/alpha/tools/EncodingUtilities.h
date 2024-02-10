// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <functional>
#include <string>
#include <string_view>
#include <vector>
#include "dwio/alpha/common/EncodingPrimitives.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/velox/ChunkedStream.h"

namespace facebook::alpha::tools {

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

std::string getStreamInputLabel(alpha::ChunkedStream& stream);
std::string getEncodingLabel(std::string_view stream);

} // namespace facebook::alpha::tools
