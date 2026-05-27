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
#include "dwio/nimble/encodings/common/EncodingLayout.h"
#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/encodings/SubIntSplitConfig.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"

namespace facebook::nimble {

namespace {
constexpr uint32_t kMinEncodingLayoutBufferSize = 5;

uint32_t serializedConfigSize(const EncodingLayout::Config& config) {
  if (config.values().empty()) {
    return 0;
  }

  uint32_t size = sizeof(uint16_t);
  for (const auto& [key, value] : config.values()) {
    size += sizeof(uint16_t) + static_cast<uint32_t>(key.size());
    size += sizeof(uint16_t) + static_cast<uint32_t>(value.size());
  }
  NIMBLE_CHECK_LE(
      size,
      std::numeric_limits<uint16_t>::max(),
      "EncodingLayout config too large.");
  return size;
}

void serializeConfig(const EncodingLayout::Config& config, char*& pos) {
  std::vector<std::pair<std::string_view, std::string_view>> entries;
  entries.reserve(config.values().size());
  for (const auto& [key, value] : config.values()) {
    entries.emplace_back(key, value);
  }
  std::sort(
      entries.begin(), entries.end(), [](const auto& left, const auto& right) {
        return left.first < right.first;
      });

  encoding::write<uint16_t>(static_cast<uint16_t>(entries.size()), pos);
  for (const auto& [key, value] : entries) {
    encoding::write<uint16_t>(static_cast<uint16_t>(key.size()), pos);
    encoding::writeBytes(key, pos);
    encoding::write<uint16_t>(static_cast<uint16_t>(value.size()), pos);
    encoding::writeBytes(value, pos);
  }
}

EncodingLayout::Config deserializeConfig(std::string_view extraData) {
  if (extraData.empty()) {
    return {};
  }

  const char* pos = extraData.data();
  const char* const end = extraData.data() + extraData.size();
  std::unordered_map<std::string, std::string> configs;

  NIMBLE_CHECK_LE(
      static_cast<size_t>(end - pos),
      extraData.size(),
      "Invalid encoding layout config.");
  const uint16_t configCount = encoding::read<uint16_t>(pos);
  configs.reserve(configCount);

  for (uint16_t i = 0; i < configCount; ++i) {
    NIMBLE_CHECK_LE(
        sizeof(uint16_t),
        static_cast<size_t>(end - pos),
        "Invalid encoding layout config.");
    const uint16_t keySize = encoding::read<uint16_t>(pos);
    NIMBLE_CHECK_LE(
        keySize,
        static_cast<size_t>(end - pos),
        "Invalid encoding layout config.");
    std::string key(pos, keySize);
    pos += keySize;

    NIMBLE_CHECK_LE(
        sizeof(uint16_t),
        static_cast<size_t>(end - pos),
        "Invalid encoding layout config.");
    const uint16_t valueSize = encoding::read<uint16_t>(pos);
    NIMBLE_CHECK_LE(
        valueSize,
        static_cast<size_t>(end - pos),
        "Invalid encoding layout config.");
    std::string value(pos, valueSize);
    pos += valueSize;

    configs.emplace(std::move(key), std::move(value));
  }

  NIMBLE_CHECK_EQ(pos, end, "Invalid encoding layout config length.");
  return EncodingLayout::Config{std::move(configs)};
}
} // namespace

std::optional<std::string> EncodingLayout::Config::get(
    const std::string& key) const {
  auto it = configs_.find(key);
  if (it == configs_.end()) {
    return std::nullopt;
  }
  return it->second;
}

EncodingLayout::EncodingLayout(
    EncodingType encodingType,
    Config encodingConfig,
    CompressionType compressionType,
    std::vector<std::optional<const EncodingLayout>> children)
    : encodingType_{encodingType},
      encodingConfig_{std::move(encodingConfig)},
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
  const uint32_t extraDataSize = serializedConfigSize(encodingConfig_);
  NIMBLE_CHECK(
      output.size() >=
          kMinEncodingLayoutBufferSize + extraDataSize + children_.size(),
      "Captured encoding layout buffer too small.");

  char* pos = output.data();
  pos[0] = static_cast<char>(encodingType_);
  pos[1] = static_cast<char>(compressionType_);
  pos[2] = static_cast<char>(children_.size());
  pos += 3;
  encoding::write<uint16_t>(static_cast<uint16_t>(extraDataSize), pos);

  int32_t size = static_cast<int32_t>(pos - output.data());
  if (extraDataSize > 0) {
    char* extraPos = output.data() + size;
    serializeConfig(encodingConfig_, extraPos);
    size += extraDataSize;
  }

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
  const auto extraDataSize = encoding::read<uint16_t>(pos);

  NIMBLE_CHECK_GE(
      encoding.size(),
      kMinEncodingLayoutBufferSize + extraDataSize,
      "Invalid captured encoding layout. Buffer too small.");

  const auto encodingConfig = deserializeConfig(
      {encoding.data() + kMinEncodingLayoutBufferSize, extraDataSize});

  uint32_t offset = kMinEncodingLayoutBufferSize + extraDataSize;
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

  return {
      {encodingType,
       std::move(encodingConfig),
       compressionType,
       std::move(children)},
      offset};
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

const EncodingLayout::Config& EncodingLayout::config() const {
  return encodingConfig_;
}

namespace {

constexpr uint32_t kEncodingPrefixSize = 6;

} // namespace

EncodingLayout EncodingLayoutCapture::capture(std::string_view encoding) {
  NIMBLE_CHECK_GE(
      encoding.size(), kEncodingPrefixSize, "Encoding size too small.");

  const auto encodingType =
      encoding::peek<uint8_t, EncodingType>(encoding.data());
  CompressionType compressionType = CompressionType::Uncompressed;

  if (encodingType == EncodingType::FixedBitWidth ||
      encodingType == EncodingType::Trivial) {
    compressionType = encoding::peek<uint8_t, CompressionType>(
        encoding.data() + kEncodingPrefixSize);
  }

  std::vector<std::optional<const EncodingLayout>> children;
  switch (encodingType) {
    case EncodingType::FixedBitWidth:
    case EncodingType::Varint:
    case EncodingType::Constant:
    case EncodingType::Prefix: {
      // Non nested encodings have zero children
      break;
    }
    case EncodingType::Trivial: {
      const auto dataType =
          encoding::peek<uint8_t, DataType>(encoding.data() + 1);
      if (dataType == DataType::String) {
        const char* pos = encoding.data() + kEncodingPrefixSize + 1;
        const uint32_t lengthsBytes = encoding::readUint32(pos);

        children.reserve(1);
        children.emplace_back(
            EncodingLayoutCapture::capture({pos, lengthsBytes}));
      }
      break;
    }
    case EncodingType::SparseBool: {
      children.reserve(1);
      children.emplace_back(
          EncodingLayoutCapture::capture(
              encoding.substr(kEncodingPrefixSize + 1)));
      break;
    }
    case EncodingType::MainlyConstant: {
      children.reserve(2);

      const char* pos = encoding.data() + kEncodingPrefixSize;
      const uint32_t isCommonBytes = encoding::readUint32(pos);

      children.emplace_back(
          EncodingLayoutCapture::capture({pos, isCommonBytes}));

      pos += isCommonBytes;
      const uint32_t otherValuesBytes = encoding::readUint32(pos);

      children.emplace_back(
          EncodingLayoutCapture::capture({pos, otherValuesBytes}));
      break;
    }
    case EncodingType::Dictionary: {
      children.reserve(2);
      const char* pos = encoding.data() + kEncodingPrefixSize;
      const uint32_t alphabetBytes = encoding::readUint32(pos);

      children.emplace_back(
          EncodingLayoutCapture::capture({pos, alphabetBytes}));

      pos += alphabetBytes;

      children.emplace_back(
          EncodingLayoutCapture::capture(
              {pos, encoding.size() - (pos - encoding.data())}));
      break;
    }
    case EncodingType::RLE: {
      const auto dataType =
          encoding::peek<uint8_t, DataType>(encoding.data() + 1);

      children.reserve(dataType == DataType::Bool ? 1 : 2);

      const char* pos = encoding.data() + kEncodingPrefixSize;
      const uint32_t runLengthBytes = encoding::readUint32(pos);

      children.emplace_back(
          EncodingLayoutCapture::capture({pos, runLengthBytes}));

      if (dataType != DataType::Bool) {
        pos += runLengthBytes;

        children.emplace_back(
            EncodingLayoutCapture::capture(
                {pos, encoding.size() - (pos - encoding.data())}));
      }
      break;
    }
    case EncodingType::Delta: {
      children.reserve(3);

      const char* pos = encoding.data() + kEncodingPrefixSize;
      const uint32_t deltaBytes = encoding::readUint32(pos);
      const uint32_t restatementBytes = encoding::readUint32(pos);

      children.emplace_back(EncodingLayoutCapture::capture({pos, deltaBytes}));

      pos += deltaBytes;

      children.emplace_back(
          EncodingLayoutCapture::capture({pos, restatementBytes}));

      pos += restatementBytes;

      children.emplace_back(
          EncodingLayoutCapture::capture(
              {pos, encoding.size() - (pos - encoding.data())}));
      break;
    }
    case EncodingType::SubIntSplit: {
      // Layout after the standard prefix:
      //   [splitCount(1)][reserved(1)][{bitStart(1),bitEnd(1),encodedSize(4)} ×
      //   N] [section_0_bytes][section_1_bytes]...
      const char* pos = encoding.data() + kEncodingPrefixSize;
      const uint8_t splitCount = encoding::read<uint8_t>(pos);
      encoding::read<uint8_t>(pos); // reserved

      struct SectionMeta {
        uint8_t bitStart;
        uint8_t bitEnd;
        uint32_t encodedSize;
      };

      std::vector<SectionMeta> sectionMeta;
      sectionMeta.reserve(splitCount);
      for (uint8_t s = 0; s < splitCount; ++s) {
        SectionMeta meta{};
        meta.bitStart = encoding::read<uint8_t>(pos);
        meta.bitEnd = encoding::read<uint8_t>(pos);
        meta.encodedSize = encoding::readUint32(pos);
        sectionMeta.push_back(meta);
      }

      children.reserve(splitCount);
      for (uint8_t s = 0; s < splitCount; ++s) {
        children.emplace_back(
            EncodingLayoutCapture::capture({pos, sectionMeta[s].encodedSize}));
        pos += sectionMeta[s].encodedSize;
      }
      std::vector<detail::subintsplit::SegmentPlan> boundaryPlans;
      boundaryPlans.reserve(splitCount);
      for (uint8_t s = 0; s < splitCount; ++s) {
        detail::subintsplit::SegmentPlan segment{};
        segment.bitStart = static_cast<int>(sectionMeta[s].bitStart);
        segment.bitEnd = static_cast<int>(sectionMeta[s].bitEnd);
        boundaryPlans.push_back(segment);
      }
      return {
          EncodingType::SubIntSplit,
          EncodingLayout::Config{
              detail::subintsplit::makePreserveSplitConfig(boundaryPlans)},
          compressionType,
          std::move(children)};
    }
    case EncodingType::Nullable: {
      const char* pos = encoding.data() + kEncodingPrefixSize;
      const uint32_t dataBytes = encoding::readUint32(pos);

      // For nullable encodings we only capture the data encoding part, so we
      // are "overwriting" the current captured node with the nested data node.
      return EncodingLayoutCapture::capture({pos, dataBytes});
    }
    case EncodingType::Sentinel: {
      // For sentinel encodings we only capture the data encoding part, so we
      // are "overwriting" the current captured node with the nested data node.
      return EncodingLayoutCapture::capture(
          encoding.substr(kEncodingPrefixSize + 8));
    }
  }

  return {
      encodingType,
      /*encodingConfig=*/{},
      compressionType,
      std::move(children)};
}

} // namespace facebook::nimble
