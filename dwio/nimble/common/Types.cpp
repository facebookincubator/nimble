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
#include "dwio/nimble/common/Types.h"

#include <array>

#include "dwio/nimble/common/Exceptions.h"

namespace facebook::nimble {
namespace {

constexpr auto kEncodingTypes =
    std::to_array<std::pair<EncodingType, std::string_view>>({
        {EncodingType::Trivial, "Trivial"},
        {EncodingType::RLE, "RLE"},
        {EncodingType::Dictionary, "Dictionary"},
        {EncodingType::FixedBitWidth, "FixedBitWidth"},
        {EncodingType::Sentinel, "Sentinel"},
        {EncodingType::Nullable, "Nullable"},
        {EncodingType::SparseBool, "SparseBool"},
        {EncodingType::Varint, "Varint"},
        {EncodingType::Delta, "Delta"},
        {EncodingType::Constant, "Constant"},
        {EncodingType::MainlyConstant, "MainlyConstant"},
        {EncodingType::Prefix, "Prefix"},
        {EncodingType::ALP, "ALP"},
        {EncodingType::PFOR, "PFOR"},
        {EncodingType::SimdForBitpack, "SimdForBitpack"},
        {EncodingType::BlockBitPacking, "BlockBitPacking"},
        {EncodingType::SubIntSplit, "SubIntSplit"},
        {EncodingType::FrequencyPartition, "FrequencyPartition"},
        {EncodingType::FOR, "FOR"},
        {EncodingType::Fsst, "Fsst"},
        {EncodingType::Huffman, "Huffman"},
        {EncodingType::DeltaBlock, "DeltaBlock"},
    });

constexpr auto kCompressionTypes =
    std::to_array<std::pair<CompressionType, std::string_view>>({
        {CompressionType::Uncompressed, "Uncompressed"},
        {CompressionType::Zstd, "Zstd"},
        {CompressionType::MetaInternal, "MetaInternal"},
        {CompressionType::Lz4, "Lz4"},
        {CompressionType::OpenZL, "OpenZL"},
    });

} // namespace

std::ostream& operator<<(std::ostream& out, EncodingType encodingType) {
  return out << toString(encodingType);
}

std::string toString(EncodingType encodingType) {
  for (const auto& [type, name] : kEncodingTypes) {
    if (encodingType == type) {
      return std::string{name};
    }
  }
  return fmt::format(
      "Unknown encoding type: {}", static_cast<int32_t>(encodingType));
}

std::optional<EncodingType> encodingTypeFromString(std::string_view name) {
  for (const auto& [type, candidate] : kEncodingTypes) {
    if (name == candidate) {
      return type;
    }
  }
  return std::nullopt;
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
  for (const auto& [type, name] : kCompressionTypes) {
    if (compressionType == type) {
      return std::string{name};
    }
  }
  return fmt::format(
      "Unknown compression type: {}", static_cast<int32_t>(compressionType));
}

std::optional<CompressionType> compressionTypeFromString(
    std::string_view name) {
  for (const auto& [type, candidate] : kCompressionTypes) {
    if (name == candidate) {
      return type;
    }
  }
  return std::nullopt;
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

} // namespace facebook::nimble
