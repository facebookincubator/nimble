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
#include <cstdint>
#include <memory>
#include <vector>
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/encodings/ALPEncoding.h"
#include "dwio/nimble/encodings/FsstEncoding.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/common/EncodingUtils.h"

namespace facebook::nimble {

namespace {
constexpr uint32_t kMinEncodingLayoutBufferSize = 5;

// Captures one size-prefixed nested sub-stream into `children` (nullopt when
// the sub-stream is empty)
void captureSizedChild(
    std::vector<std::optional<const EncodingLayout>>& children,
    const char*& cursor) {
  const uint32_t size = encoding::readUint32(cursor);
  children.emplace_back(
      size > 0 ? std::optional<const EncodingLayout>(
                     EncodingLayoutCapture::capture({cursor, size}))
               : std::nullopt);
  cursor += size;
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
  NIMBLE_CHECK_GE(
      output.size(),
      kMinEncodingLayoutBufferSize + children_.size(),
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
  NIMBLE_CHECK_GE(
      encoding.size(),
      kMinEncodingLayoutBufferSize,
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

  return {{encodingType, {}, compressionType, std::move(children)}, offset};
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
    case EncodingType::Prefix:
    case EncodingType::SimdForBitpack:
    case EncodingType::SubIntSplit:
    case EncodingType::FOR:
    case EncodingType::FrequencyPartition:
    case EncodingType::Huffman:
      // Non nested encodings have zero children
      break;
    case EncodingType::ALP: {
      const char* pos = encoding.data() + kEncodingPrefixSize;
      const auto header = detail::alp::readHeader(pos);
      if (header.hasExceptions) {
        varint::readVarint32(&pos); // exceptionCount
      }
      const uint32_t encodedValuesBytes = varint::readVarint32(&pos);

      children.reserve(1);
      children.emplace_back(
          EncodingLayoutCapture::capture({pos, encodedValuesBytes}));
      break;
    }
    case EncodingType::PFOR: {
      const auto dataType =
          encoding::peek<uint8_t, DataType>(encoding.data() + 1);
      const char* pos = encoding.data() + kEncodingPrefixSize;
      pos += detail::dataTypeSize(dataType); // baseline
      encoding::readChar(pos); // baseBitWidth
      encoding::readUint32(pos); // numExceptions

      children.reserve(2);
      captureSizedChild(children, pos); // exception positions
      captureSizedChild(children, pos); // exception values
      break;
    }
    case EncodingType::BlockBitPacking: {
      // BlockBitPacking nests three per-block metadata sub-streams: baselines,
      // bit widths, and data offsets.
      const char* pos = encoding.data() + kEncodingPrefixSize;
      encoding::readChar(pos); // compressionType
      encoding::read<uint16_t>(pos); // blockSize
      encoding::read<uint16_t>(pos); // numBlocks

      children.reserve(3);
      captureSizedChild(children, pos); // baselines
      captureSizedChild(children, pos); // bit widths
      captureSizedChild(children, pos); // data offsets
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
    case EncodingType::Fsst: {
      FsstEncoding::captureNestedEncoding(encoding, children);
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
      /*encodingConfig=*/
      {},
      compressionType,
      std::move(children)};
}

} // namespace facebook::nimble
