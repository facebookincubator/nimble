// Copyright 2004-present Facebook. All Rights Reserved.

#include "dwio/alpha/common/Types.h"

#include <fmt/core.h>
#include "dwio/alpha/common/Exceptions.h"

namespace facebook::alpha {

std::ostream& operator<<(std::ostream& out, EncodingType encodingType) {
  return out << toString(encodingType);
}

std::string toString(EncodingType encodingType) {
  switch (encodingType) {
    case EncodingType::Trivial:
      return "Trivial";
    case EncodingType::RLE:
      return "RLE";
    case EncodingType::Dictionary:
      return "Dictionary";
    case EncodingType::FixedBitWidth:
      return "FixedBitWidth";
    case EncodingType::Nullable:
      return "Nullable";
    case EncodingType::SparseBool:
      return "SparseBool";
    case EncodingType::Varint:
      return "Varint";
    case EncodingType::Delta:
      return "Delta";
    case EncodingType::Constant:
      return "Constant";
    case EncodingType::MainlyConstant:
      return "MainlyConstant";
    case EncodingType::Sentinel:
      return "Sentinel";
  }
  return fmt::format(
      "Unknown encoding type: {}", static_cast<int32_t>(encodingType));
}

std::ostream& operator<<(std::ostream& out, DataType dataType) {
  return out << toString(dataType);
}

std::string toString(DataType dataType) {
  switch (dataType) {
    case DataType::Int8:
      return "Int8";
    case DataType::Int16:
      return "Int16";
    case DataType::Uint8:
      return "Uint8";
    case DataType::Uint16:
      return "Uint16";
    case DataType::Int32:
      return "Int32";
    case DataType::Int64:
      return "Int64";
    case DataType::Uint32:
      return "Uint32";
    case DataType::Uint64:
      return "Uint64";
    case DataType::Float:
      return "Float";
    case DataType::Double:
      return "Double";
    case DataType::Bool:
      return "Bool";
    case DataType::String:
      return "String";
    default:
      return fmt::format(
          "Unknown data type: {}", static_cast<int32_t>(dataType));
  }
}

std::string toString(CompressionType compressionType) {
  switch (compressionType) {
    case CompressionType::Uncompressed:
      return "Uncompressed";
    case CompressionType::Zstd:
      return "Zstd";
    case CompressionType::Zstrong:
      return "Zstrong";
    default:
      return fmt::format(
          "Unknown compression type: {}",
          static_cast<int32_t>(compressionType));
  }
}

std::ostream& operator<<(std::ostream& out, CompressionType compressionType) {
  return out << toString(compressionType);
}

template <>
void Variant<std::string_view>::set(
    VariantType& target,
    std::string_view source) {
  target = std::string(source);
}

template <>
std::string_view Variant<std::string_view>::get(VariantType& source) {
  return std::get<std::string>(source);
}

std::string toString(ChecksumType type) {
  switch (type) {
    case ChecksumType::XXH3_64:
      return "XXH3_64";
    default:
      return fmt::format(
          "Unknown checksum type: {}", static_cast<int32_t>(type));
  }
}

} // namespace facebook::alpha
