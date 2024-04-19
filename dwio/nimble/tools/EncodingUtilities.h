// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

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
