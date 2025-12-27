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
#include "dwio/nimble/encodings/EncodingLayout.h"
#include <cstdint>
#include <memory>
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"

namespace facebook::nimble {

namespace {
constexpr uint32_t kMinEncodingLayoutBufferSize = 5;
}

EncodingLayout::EncodingLayout(
    EncodingType encodingType,
    CompressionType compressionType,
    std::vector<std::optional<const EncodingLayout>> children)
    : encodingType_{encodingType},
      compressionType_{compressionType},
      children_{std::move(children)} {}

int32_t EncodingLayout::serialize(std::span<char> output) const {
  // Serialized encoding layout is as follows:
  // 1 byte - Encoding Type
  // 1 byte - CompressionType
  // 1 byte - Children (nested encoding) Count
  // 2 bytes - Extra data size (currently always set to zero. Reserved for futre
  //           exra args for compression or encoding)
  // Extra data size bytes - Extra data (currently not used)
  // 1 bytes - 1st (nested encoding) child exists
  // X bytes - 1st (nested encoding) child
  // 1 bytes - 2nd (nested encoding) child exists
  // Y bytes - 2nd (nested encoding) child
  // ...

  // We store at least kMinEncodingLayoutBufferSize bytes: encoding type,
  // compression type, children count and extra data size (2 bytes), plus one
  // byte per child.
  NIMBLE_CHECK(
      output.size() >= kMinEncodingLayoutBufferSize + children_.size(),
      "Captured encoding layout buffer too small.");

  output[0] = static_cast<char>(encodingType_);
  output[1] = static_cast<char>(compressionType_);
  output[2] = static_cast<char>(children_.size());
  // Currently, extra data is not used and always set to zero.
  output[3] = output[4] = 0;

  int32_t size = kMinEncodingLayoutBufferSize;

  for (auto i = 0; i < children_.size(); ++i) {
    const auto& child = children_[i];
    if (child.has_value()) {
      output[size++] = 1;
      size += child->serialize(output.subspan(size));
    } else {
      // Set child size to 0
      output[size++] = 0;
    }
  }

  return size;
}

std::pair<EncodingLayout, uint32_t> EncodingLayout::create(
    std::string_view encoding) {
  NIMBLE_CHECK(
      encoding.size() >= kMinEncodingLayoutBufferSize,
      "Invalid captured encoding layout. Buffer too small.");

  auto pos = encoding.data();
  const auto encodingType = encoding::read<uint8_t, EncodingType>(pos);
  const auto compressionType = encoding::read<uint8_t, CompressionType>(pos);
  const auto childrenCount = encoding::read<uint8_t>(pos);
  [[maybe_unused]] const auto extraDataSize = encoding::read<uint16_t>(pos);

  NIMBLE_DCHECK_EQ(extraDataSize, 0, "Extra data currently not supported.");

  uint32_t offset = kMinEncodingLayoutBufferSize;
  std::vector<std::optional<const EncodingLayout>> children;
  children.reserve(childrenCount);
  for (auto i = 0; i < childrenCount; ++i) {
    auto childExists = encoding::peek<uint8_t>(encoding.data() + offset);
    ++offset;
    if (childExists > 0) {
      auto encodingLayout = EncodingLayout::create(encoding.substr(offset));
      offset += encodingLayout.second;
      children.emplace_back(std::move(encodingLayout.first));
    } else {
      children.emplace_back(std::nullopt);
    }
  }

  return {{encodingType, compressionType, std::move(children)}, offset};
}

EncodingType EncodingLayout::encodingType() const {
  return encodingType_;
}

CompressionType EncodingLayout::compressionType() const {
  return compressionType_;
}

uint8_t EncodingLayout::childrenCount() const {
  return children_.size();
}

const std::optional<const EncodingLayout>& EncodingLayout::child(
    NestedEncodingIdentifier identifier) const {
  NIMBLE_DCHECK_LT(
      identifier,
      children_.size(),
      "Encoding layout identifier is out of range.");

  return children_[identifier];
}

} // namespace facebook::nimble
