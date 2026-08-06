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

#include "dwio/nimble/encodings/tests/SharedDictionaryEncodingTestUtils.h"

#include <algorithm>
#include <iterator>
#include <optional>
#include <utility>

#include "dwio/nimble/common/DataTypeDispatch.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"

namespace facebook::nimble::test {

TestSharedDictionaryAlphabet::TestSharedDictionaryAlphabet(
    DataType dataType,
    std::vector<Chunk> chunks)
    : SharedDictionaryAlphabet{dataType}, chunks_{std::move(chunks)} {
  setEntryCount(validateChunks(chunks_));
}

uint32_t TestSharedDictionaryAlphabet::validateChunks(
    const std::vector<Chunk>& chunks) {
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
        kMaxSharedDictionarySize - nextBegin,
        "Shared dictionary alphabet chunk count overflows.");
    nextBegin += chunk.count;
  }
  return nextBegin;
}

void TestSharedDictionaryAlphabet::physicalValueAtImpl(
    uint32_t index,
    void* output) const {
  NIMBLE_RETURN_BY_DATA_TYPE_OR(
      dataType(),
      T,
      (getPhysicalValueTyped<T>(index, output), void()),
      NIMBLE_UNSUPPORTED(
          "{} is not supported by shared dictionary alphabets.", dataType()));
}

void TestSharedDictionaryAlphabet::materializeImpl(
    std::span<const uint32_t> indices,
    void* output) const {
  NIMBLE_RETURN_BY_DATA_TYPE_OR(
      dataType(),
      T,
      (materializeTyped<T>(
           indices, static_cast<typename TypeTraits<T>::physicalType*>(output)),
       void()),
      NIMBLE_UNSUPPORTED(
          "{} is not supported by shared dictionary alphabets.", dataType()));
}

const TestSharedDictionaryAlphabet::Chunk&
TestSharedDictionaryAlphabet::chunkForIndex(uint32_t index) const {
  NIMBLE_CHECK_LT(
      index, entryCount(), "Shared dictionary index exceeds alphabet size.");
  if (chunks_.size() == 1) {
    return chunks_.front();
  }
  const auto it = std::upper_bound(
      chunks_.begin(),
      chunks_.end(),
      index,
      [](uint32_t value, const Chunk& chunk) { return value < chunk.begin; });
  NIMBLE_CHECK(it != chunks_.begin());
  return *std::prev(it);
}

std::shared_ptr<const SharedDictionaryAlphabet>
createTestSharedDictionaryAlphabet(
    DataType dataType,
    std::vector<TestSharedDictionaryAlphabet::Chunk> chunks) {
  return std::make_shared<TestSharedDictionaryAlphabet>(
      dataType, std::move(chunks));
}

TestSharedDictionarySelectionPolicy::TestSharedDictionarySelectionPolicy(
    SharedDictionaryEncodingInput sharedDictionary,
    EncodingSelectionPolicyCreator nestedPolicyCreator)
    : sharedDictionary_{sharedDictionary},
      nestedPolicyCreator_{std::move(nestedPolicyCreator)} {
  NIMBLE_CHECK_NOT_NULL(nestedPolicyCreator_);
}

EncodingSelectionResult TestSharedDictionarySelectionPolicy::select(
    std::span<const physicalType> /*values*/,
    const Statistics<physicalType>& /*statistics*/,
    const Encoding::Options& /*options*/) {
  return {
      .encodingType = EncodingType::SharedDictionary,
      .sharedDictionaryInput = sharedDictionary_};
}

EncodingSelectionResult TestSharedDictionarySelectionPolicy::selectNullable(
    std::span<const physicalType> /*values*/,
    std::span<const bool> /*nulls*/,
    const Statistics<physicalType>& /*statistics*/,
    const Encoding::Options& /*options*/) {
  return {.encodingType = EncodingType::Nullable};
}

std::unique_ptr<EncodingSelectionPolicyBase>
TestSharedDictionarySelectionPolicy::createImpl(
    EncodingType /*parentEncodingType*/,
    NestedEncodingIdentifier /*nestedEncodingIdentifier*/,
    DataType nestedDataType) {
  auto policy = nestedPolicyCreator_(nestedDataType);
  NIMBLE_CHECK_NOT_NULL(policy);
  return policy;
}

std::string_view encodeSharedDictionary(
    Buffer& buffer,
    const std::vector<uint32_t>& indices) {
  std::vector<int32_t> values;
  values.reserve(indices.size());
  for (const auto index : indices) {
    values.push_back(static_cast<int32_t>(index));
  }
  auto options = Encoding::Options{};
  auto nestedPolicyCreator =
      [](DataType dataType) -> std::unique_ptr<EncodingSelectionPolicyBase> {
    const auto encodingType = dataType == DataType::Uint32
        ? EncodingType::FixedBitWidth
        : EncodingType::Trivial;
    ManualEncodingSelectionPolicyFactory factory{
        {{encodingType, 1.0}}, std::nullopt};
    return factory.createPolicy(dataType);
  };
  return EncodingFactory::encode<int32_t>(
      std::make_unique<TestSharedDictionarySelectionPolicy>(
          SharedDictionaryEncodingInput{
              .scope = SharedDictionaryScope::Stripe,
              .dictionaryId = 7,
              .indices = std::span<const uint32_t>{indices}},
          nestedPolicyCreator),
      values,
      buffer,
      options);
}

} // namespace facebook::nimble::test
