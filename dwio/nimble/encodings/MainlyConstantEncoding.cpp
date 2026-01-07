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

#include "dwio/nimble/encodings/MainlyConstantEncoding.h"

namespace facebook::nimble {

MainlyConstantEncoding<std::string_view>::MainlyConstantEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    std::function<void*(uint32_t)> stringBufferFactory)
    : MainlyConstantEncodingBase<std::string_view>(memoryPool, data) {
  const char* pos = data.data() + Encoding::kPrefixSize;
  const uint32_t isCommonBytes = encoding::readUint32(pos);
  isCommon_ = EncodingFactory::decode(
      *this->pool_, {pos, isCommonBytes}, stringBufferFactory);
  pos += isCommonBytes;
  const uint32_t otherValuesBytes = encoding::readUint32(pos);
  otherValues_ = EncodingFactory::decode(
      *this->pool_, {pos, otherValuesBytes}, stringBufferFactory);
  pos += otherValuesBytes;
  commonValue_ = encoding::read<physicalType>(pos);
  NIMBLE_CHECK(pos == data.end(), "Unexpected mainly constant encoding end");
  auto stringBuffer =
      static_cast<char*>(stringBufferFactory(commonValue_.size()));
  std::memcpy(stringBuffer, commonValue_.data(), commonValue_.size());
  commonValue_ = std::string_view(stringBuffer, commonValue_.size());
}

std::string_view MainlyConstantEncoding<std::string_view>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer) {
  if (values.empty()) {
    NIMBLE_INCOMPATIBLE_ENCODING("MainlyConstantEncoding cannot be empty.");
  }

  const auto commonElement = std::max_element(
      selection.statistics().uniqueCounts()->cbegin(),
      selection.statistics().uniqueCounts()->cend(),
      [](const auto& a, const auto& b) { return a.second < b.second; });

  const uint32_t entryCount = values.size();
  const uint32_t uncommonCount = entryCount - commonElement->second;

  Vector<bool> isCommon{&buffer.getMemoryPool(), values.size(), true};
  Vector<physicalType> otherValues(&buffer.getMemoryPool());
  otherValues.reserve(uncommonCount);

  physicalType commonValue = commonElement->first;
  for (auto i = 0; i < values.size(); ++i) {
    physicalType currentValue = values[i];
    if (currentValue != commonValue) {
      isCommon[i] = false;
      otherValues.push_back(std::move(currentValue));
    }
  }

  Buffer tempBuffer{buffer.getMemoryPool()};
  std::string_view serializedIsCommon = selection.template encodeNested<bool>(
      EncodingIdentifiers::MainlyConstant::IsCommon, isCommon, tempBuffer);
  std::string_view serializedOtherValues =
      selection.template encodeNested<physicalType>(
          EncodingIdentifiers::MainlyConstant::OtherValues,
          otherValues,
          tempBuffer);

  uint32_t encodingSize = Encoding::kPrefixSize + 8 +
      serializedIsCommon.size() + serializedOtherValues.size();
  if constexpr (isNumericType<physicalType>()) {
    encodingSize += sizeof(physicalType);
  } else {
    encodingSize += 4 + commonValue.size();
  }
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::MainlyConstant,
      TypeTraits<std::string_view>::dataType,
      entryCount,
      pos);
  encoding::writeString(serializedIsCommon, pos);
  encoding::writeString(serializedOtherValues, pos);
  encoding::write<physicalType>(commonValue, pos);
  NIMBLE_DCHECK_EQ(pos - reserved, encodingSize, "Encoding size mismatch.");
  return {reserved, encodingSize};
}
} // namespace facebook::nimble
