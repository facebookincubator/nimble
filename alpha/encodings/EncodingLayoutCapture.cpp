// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/encodings/EncodingLayoutCapture.h"

#include <vector>

#include "dwio/alpha/common/EncodingPrimitives.h"
#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/common/Types.h"

namespace facebook::alpha {

namespace {

constexpr uint32_t kEncodingPrefixSize = 6;

} // namespace

EncodingLayout EncodingLayoutCapture::capture(std::string_view encoding) {
  ALPHA_CHECK(
      encoding.size() >= kEncodingPrefixSize, "Encoding size too small.");

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
    case EncodingType::Constant: {
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
        children.push_back(EncodingLayoutCapture::capture({pos, lengthsBytes}));
      }
      break;
    }
    case EncodingType::SparseBool: {
      children.reserve(1);
      children.push_back(EncodingLayoutCapture::capture(
          encoding.substr(kEncodingPrefixSize + 1)));
      break;
    }
    case EncodingType::MainlyConstant: {
      children.reserve(2);

      const char* pos = encoding.data() + kEncodingPrefixSize;
      const uint32_t isCommonBytes = encoding::readUint32(pos);

      children.push_back(EncodingLayoutCapture::capture({pos, isCommonBytes}));

      pos += isCommonBytes;
      const uint32_t otherValuesBytes = encoding::readUint32(pos);

      children.push_back(
          EncodingLayoutCapture::capture({pos, otherValuesBytes}));
      break;
    }
    case EncodingType::Dictionary: {
      children.reserve(2);
      const char* pos = encoding.data() + kEncodingPrefixSize;
      const uint32_t alphabetBytes = encoding::readUint32(pos);

      children.push_back(EncodingLayoutCapture::capture({pos, alphabetBytes}));

      pos += alphabetBytes;

      children.push_back(EncodingLayoutCapture::capture(
          {pos, encoding.size() - (pos - encoding.data())}));
      break;
    }
    case EncodingType::RLE: {
      const auto dataType =
          encoding::peek<uint8_t, DataType>(encoding.data() + 1);

      children.reserve(dataType == DataType::Bool ? 1 : 2);

      const char* pos = encoding.data() + kEncodingPrefixSize;
      const uint32_t runLengthBytes = encoding::readUint32(pos);

      children.push_back(EncodingLayoutCapture::capture({pos, runLengthBytes}));

      if (dataType != DataType::Bool) {
        pos += runLengthBytes;

        children.push_back(EncodingLayoutCapture::capture(
            {pos, encoding.size() - (pos - encoding.data())}));
      }
      break;
    }
    case EncodingType::Delta: {
      children.reserve(3);

      const char* pos = encoding.data() + kEncodingPrefixSize;
      const uint32_t deltaBytes = encoding::readUint32(pos);
      const uint32_t restatementBytes = encoding::readUint32(pos);

      children.push_back(EncodingLayoutCapture::capture({pos, deltaBytes}));

      pos += deltaBytes;

      children.push_back(
          EncodingLayoutCapture::capture({pos, restatementBytes}));

      pos += restatementBytes;

      children.push_back(EncodingLayoutCapture::capture(
          {pos, encoding.size() - (pos - encoding.data())}));
      break;
    }
    case EncodingType::Nullable: {
      const char* pos = encoding.data() + kEncodingPrefixSize;
      const uint32_t dataBytes = encoding::readUint32(pos);

      // For nullable encodings we only capture the data encoding part, so we
      // are "overwriting" the current captured node with the nested data node.
      return EncodingLayoutCapture::capture({pos, dataBytes});

      break;
    }
    case EncodingType::Sentinel: {
      // For sentinel encodings we only capture the data encoding part, so we
      // are "overwriting" the current captured node with the nested data node.
      return EncodingLayoutCapture::capture(
          encoding.substr(kEncodingPrefixSize + 8));
      break;
    }
  }

  return {encodingType, compressionType, std::move(children)};
}

} // namespace facebook::alpha
