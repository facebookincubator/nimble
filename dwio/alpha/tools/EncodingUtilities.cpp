// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/tools/EncodingUtilities.h"
#include "dwio/alpha/common/Exceptions.h"

namespace facebook::alpha::tools {
namespace {
constexpr uint32_t kEncodingPrefixSize = 6;

void extractCompressionType(
    EncodingType encodingType,
    DataType /* dataType */,
    std::string_view stream,
    std::unordered_map<EncodingPropertyType, EncodingProperty>& properties) {
  switch (encodingType) {
    // Compression type is the byte right after the encoding header for both
    // encodings.
    case EncodingType::Trivial:
    case EncodingType::FixedBitWidth: {
      auto pos = stream.data() + kEncodingPrefixSize;
      properties.insert(
          {EncodingPropertyType::Compression,
           EncodingProperty{
               .value = toString(
                   static_cast<CompressionType>(encoding::readChar(pos))),
           }});
      break;
    }
    case EncodingType::RLE:
    case EncodingType::Dictionary:
    case EncodingType::Sentinel:
    case EncodingType::Nullable:
    case EncodingType::SparseBool:
    case EncodingType::Varint:
    case EncodingType::Delta:
    case EncodingType::Constant:
    case EncodingType::MainlyConstant:
      break;
  }
}

std::unordered_map<EncodingPropertyType, EncodingProperty>
extractEncodingProperties(
    EncodingType encodingType,
    DataType dataType,
    std::string_view stream) {
  std::unordered_map<EncodingPropertyType, EncodingProperty> properties{};
  extractCompressionType(encodingType, dataType, stream, properties);
  properties.insert(
      {EncodingPropertyType::EncodedSize,
       {.value = folly::to<std::string>(stream.size())}});
  return properties;
}

void traverseEncodings(
    std::string_view stream,
    uint32_t level,
    uint32_t index,
    const std::string& nestedEncodingName,
    std::function<bool(
        EncodingType,
        DataType,
        uint32_t /* level */,
        uint32_t /* index */,
        std::string /* nestedEncodingName */,
        std::unordered_map<
            EncodingPropertyType,
            EncodingProperty> /* properties */)> visitor) {
  ALPHA_CHECK(
      stream.size() >= kEncodingPrefixSize, "Unexpected end of stream.");

  const EncodingType encodingType = static_cast<EncodingType>(stream[0]);
  auto dataType = static_cast<DataType>(stream[1]);
  bool continueTraversal = visitor(
      encodingType,
      dataType,
      level,
      index,
      nestedEncodingName,
      extractEncodingProperties(encodingType, dataType, stream));

  if (!continueTraversal) {
    return;
  }

  switch (encodingType) {
    case EncodingType::FixedBitWidth:
    case EncodingType::Varint:
    case EncodingType::Constant: {
      // don't have any nested encoding
      break;
    }
    case EncodingType::Trivial: {
      if (dataType == DataType::String) {
        const char* pos = stream.data() + kEncodingPrefixSize + 1;
        const uint32_t lengthsBytes = encoding::readUint32(pos);
        traverseEncodings(
            {pos, lengthsBytes}, level + 1, 0, "Lengths", visitor);
      }
      break;
    }
    case EncodingType::SparseBool: {
      const char* pos = stream.data() + kEncodingPrefixSize + 1;
      traverseEncodings(
          {pos, stream.size() - (pos - stream.data())},
          level + 1,
          0,
          "Indices",
          visitor);
      break;
    }
    case EncodingType::MainlyConstant: {
      const char* pos = stream.data() + kEncodingPrefixSize;
      const uint32_t isCommonBytes = encoding::readUint32(pos);
      traverseEncodings(
          {pos, isCommonBytes}, level + 1, 0, "IsCommon", visitor);
      pos += isCommonBytes;
      const uint32_t otherValueBytes = encoding::readUint32(pos);
      traverseEncodings(
          {pos, otherValueBytes}, level + 1, 1, "OtherValues", visitor);
      break;
    }
    case EncodingType::Dictionary: {
      const char* pos = stream.data() + kEncodingPrefixSize;
      const uint32_t alphabetBytes = encoding::readUint32(pos);
      traverseEncodings(
          {pos, alphabetBytes}, level + 1, 0, "Alphabet", visitor);
      pos += alphabetBytes;
      traverseEncodings(
          {pos, stream.size() - (pos - stream.data())},
          level + 1,
          1,
          "Indices",
          visitor);
      break;
    }
    case EncodingType::RLE: {
      const char* pos = stream.data() + kEncodingPrefixSize;
      const uint32_t runLengthBytes = encoding::readUint32(pos);
      traverseEncodings(
          {pos, runLengthBytes}, level + 1, 0, "Lengths", visitor);
      if (dataType != DataType::Bool) {
        pos += runLengthBytes;
        traverseEncodings(
            {pos, stream.size() - (pos - stream.data())},
            level + 1,
            1,
            "Values",
            visitor);
      }
      break;
    }
    case EncodingType::Delta: {
      const char* pos = stream.data() + kEncodingPrefixSize;
      const uint32_t deltaBytes = encoding::readUint32(pos);
      const uint32_t restatementBytes = encoding::readUint32(pos);
      traverseEncodings({pos, deltaBytes}, level + 1, 0, "Deltas", visitor);
      pos += deltaBytes;
      traverseEncodings(
          {pos, restatementBytes}, level + 1, 1, "Restatements", visitor);
      pos += restatementBytes;
      traverseEncodings(
          {pos, stream.size() - (pos - stream.data())},
          level + 1,
          2,
          "IsRestatements",
          visitor);
      break;
    }
    case EncodingType::Nullable: {
      const char* pos = stream.data() + kEncodingPrefixSize;
      const uint32_t dataBytes = encoding::readUint32(pos);
      traverseEncodings({pos, dataBytes}, level + 1, 0, "Data", visitor);
      pos += dataBytes;
      traverseEncodings(
          {pos, stream.size() - (pos - stream.data())},
          level + 1,
          1,
          "Nulls",
          visitor);
      break;
    }
    case EncodingType::Sentinel: {
      const char* pos = stream.data() + kEncodingPrefixSize + 8;
      traverseEncodings(
          {pos, stream.size() - (pos - stream.data())},
          level + 1,
          0,
          "Sentinels",
          visitor);
      break;
    }
  }
}

} // namespace

std::ostream& operator<<(std::ostream& out, EncodingPropertyType propertyType) {
  switch (propertyType) {
    case EncodingPropertyType::Compression:
      return out << "Compression";
    default:
      return out << "Unknown";
  }
}

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
            EncodingProperty> /* properties */)> visitor) {
  traverseEncodings(stream, 0, 0, "", visitor);
}

std::string getStreamInputLabel(alpha::ChunkedStream& stream) {
  std::string label;
  uint32_t chunkId = 0;
  while (stream.hasNext()) {
    label += folly::to<std::string>(chunkId++);
    auto compression = stream.peekCompressionType();
    if (compression != CompressionType::Uncompressed) {
      label += "{" + toString(compression) + "}";
    }
    label += ":" + getEncodingLabel(stream.nextChunk());

    if (stream.hasNext()) {
      label += ";";
    }
  }
  return label;
}

std::string getEncodingLabel(std::string_view stream) {
  std::string label;
  uint32_t currentLevel = 0;

  traverseEncodings(
      stream,
      [&](EncodingType encodingType,
          DataType dataType,
          uint32_t level,
          uint32_t index,
          std::string nestedEncodingName,
          std::unordered_map<EncodingPropertyType, EncodingProperty> properties)
          -> bool {
        if (level > currentLevel) {
          label += "[" + nestedEncodingName + ":";
        } else if (level < currentLevel) {
          label += "]";
        }

        if (index > 0) {
          label += "," + nestedEncodingName + ":";
        }

        currentLevel = level;

        label += toString(encodingType) + "<" + toString(dataType);
        const auto& encodedSize =
            properties.find(EncodingPropertyType::EncodedSize);
        if (encodedSize != properties.end()) {
          label += "," + encodedSize->second.value;
        }
        label += ">";

        const auto& compression =
            properties.find(EncodingPropertyType::Compression);
        if (compression != properties.end()
            // If the "Uncompressed" label clutters the output, uncomment the
            // next line.
            // && compression->second.value !=
            // toString(CompressionType::Uncompressed)
        ) {
          label += "{" + compression->second.value + "}";
        }

        return true;
      });
  while (currentLevel-- > 0) {
    label += "]";
  }

  return label;
}

} // namespace facebook::alpha::tools
