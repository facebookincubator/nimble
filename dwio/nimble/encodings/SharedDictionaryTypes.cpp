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

#include "dwio/nimble/encodings/SharedDictionaryTypes.h"

#include "velox/common/EnumDefine.h"

namespace facebook::nimble {

namespace {
const auto& sharedDictionaryScopeNames() {
  static const folly::F14FastMap<SharedDictionaryScope, std::string_view>
      kNames = {
          {SharedDictionaryScope::Stripe, "Stripe"},
          {SharedDictionaryScope::File, "File"},
          {SharedDictionaryScope::External, "External"},
      };
  return kNames;
}
} // namespace

VELOX_DEFINE_ENUM_NAME(SharedDictionaryScope, sharedDictionaryScopeNames)

SharedDictionaryAlphabet::SharedDictionaryAlphabet(DataType dataType)
    : dataType_{dataType} {}

SharedDictionaryAlphabet::DecodedChunk SharedDictionaryAlphabet::decodedChunk(
    uint32_t begin,
    uint32_t count,
    const void* entries,
    std::shared_ptr<const void> owner) {
  return DecodedChunk{
      .begin = begin,
      .count = count,
      .entries = entries,
      .owner = std::move(owner)};
}

SharedDictionaryAlphabet::EncodedChunk SharedDictionaryAlphabet::encodedChunk(
    uint32_t begin,
    std::shared_ptr<const EncodingView> view) {
  return EncodedChunk{.begin = begin, .view = std::move(view)};
}

DataType SharedDictionaryAlphabet::dataType() const {
  return dataType_;
}

uint32_t SharedDictionaryAlphabet::entryCount() const {
  return entryCount_;
}

uint32_t SharedDictionaryAlphabet::validateDecodedChunks(
    const std::vector<DecodedChunk>& chunks) {
  uint32_t nextBegin{0};
  for (const auto& chunk : chunks) {
    NIMBLE_CHECK_EQ(
        chunk.begin,
        nextBegin,
        "Shared dictionary alphabet chunks must be contiguous.");
    NIMBLE_CHECK_NOT_NULL(chunk.entries);
    NIMBLE_CHECK_NOT_NULL(chunk.owner);
    NIMBLE_CHECK_LE(
        chunk.count,
        kMaxSharedDictionaryEntryCount - nextBegin,
        "Shared dictionary alphabet chunk count overflows.");
    nextBegin += chunk.count;
  }
  return nextBegin;
}

uint32_t SharedDictionaryAlphabet::validateEncodedChunks(
    DataType dataType,
    const std::vector<EncodedChunk>& chunks) {
  uint32_t nextBegin{0};
  for (const auto& chunk : chunks) {
    NIMBLE_CHECK_EQ(
        chunk.begin,
        nextBegin,
        "Shared dictionary alphabet chunks must be contiguous.");
    NIMBLE_CHECK_NOT_NULL(chunk.view);
    NIMBLE_CHECK_EQ(
        chunk.view->dataType(),
        dataType,
        "Shared dictionary encoded chunk has unexpected type.");
    const auto rowCount = chunk.view->rowCount();
    NIMBLE_CHECK_LE(
        rowCount,
        kMaxSharedDictionaryEntryCount - nextBegin,
        "Shared dictionary alphabet chunk count overflows.");
    nextBegin += rowCount;
  }
  return nextBegin;
}

void SharedDictionaryAlphabet::setEntryCount(uint32_t entryCount) {
  entryCount_ = entryCount;
}

std::shared_ptr<const SharedDictionaryAlphabet>
SharedDictionaryAlphabet::createDecoded(
    DataType dataType,
    std::vector<DecodedChunk> chunks) {
  return std::make_shared<DecodedSharedDictionaryAlphabet>(
      dataType, std::move(chunks));
}

std::shared_ptr<const SharedDictionaryAlphabet>
SharedDictionaryAlphabet::createEncoded(
    DataType dataType,
    std::vector<EncodedChunk> chunks) {
  return std::make_shared<EncodedSharedDictionaryAlphabet>(
      dataType, std::move(chunks));
}

} // namespace facebook::nimble
