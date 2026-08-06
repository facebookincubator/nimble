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
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/SharedDictionaryEncoding.h"
#include "dwio/nimble/encodings/selection/EncodingSelectionPolicy.h"

namespace facebook::nimble::test {

class TestSharedDictionaryAlphabet final : public SharedDictionaryAlphabet {
 public:
  struct Chunk {
    uint32_t begin{};
    uint32_t count{};
    const void* entries{};
    std::shared_ptr<const void> owner;
  };

  TestSharedDictionaryAlphabet(DataType dataType, std::vector<Chunk> chunks);

 private:
  static uint32_t validateChunks(const std::vector<Chunk>& chunks);

  template <typename T>
  void getPhysicalValueTyped(uint32_t index, void* output) const {
    const auto& chunk = chunkForIndex(index);
    *static_cast<typename TypeTraits<T>::physicalType*>(output) =
        static_cast<const typename TypeTraits<T>::physicalType*>(
            chunk.entries)[index - chunk.begin];
  }

  template <typename T>
  void materializeTyped(
      std::span<const uint32_t> indices,
      typename TypeTraits<T>::physicalType* output) const {
    if (chunks_.size() == 1) {
      const auto* entries =
          static_cast<const typename TypeTraits<T>::physicalType*>(
              chunks_[0].entries);
      for (size_t i = 0; i < indices.size(); ++i) {
        NIMBLE_CHECK_LT(
            indices[i],
            entryCount(),
            "Shared dictionary index exceeds alphabet size.");
        output[i] = entries[indices[i]];
      }
      return;
    }
    for (size_t i = 0; i < indices.size(); ++i) {
      const auto& chunk = chunkForIndex(indices[i]);
      output[i] = static_cast<const typename TypeTraits<T>::physicalType*>(
          chunk.entries)[indices[i] - chunk.begin];
    }
  }

  void physicalValueAtImpl(uint32_t index, void* output) const final;

  void materializeImpl(std::span<const uint32_t> indices, void* output)
      const final;

  std::optional<EncodingType> encodingTypeImpl() const final {
    return std::nullopt;
  }

  const Chunk& chunkForIndex(uint32_t index) const;

  const std::vector<Chunk> chunks_;
};

std::shared_ptr<const SharedDictionaryAlphabet>
createTestSharedDictionaryAlphabet(
    DataType dataType,
    std::vector<TestSharedDictionaryAlphabet::Chunk> chunks);

class TestSharedDictionarySelectionPolicy final
    : public EncodingSelectionPolicy<int32_t> {
 public:
  using physicalType = typename TypeTraits<int32_t>::physicalType;

  TestSharedDictionarySelectionPolicy(
      SharedDictionaryEncodingInput sharedDictionary,
      EncodingSelectionPolicyCreator nestedPolicyCreator);

  EncodingSelectionResult select(
      std::span<const physicalType> values,
      const Statistics<physicalType>& statistics,
      const Encoding::Options& options) final;

  EncodingSelectionResult selectNullable(
      std::span<const physicalType> values,
      std::span<const bool> nulls,
      const Statistics<physicalType>& statistics,
      const Encoding::Options& options) final;

 private:
  std::unique_ptr<EncodingSelectionPolicyBase> createImpl(
      EncodingType parentEncodingType,
      NestedEncodingIdentifier nestedEncodingIdentifier,
      DataType nestedDataType) final;

  const SharedDictionaryEncodingInput sharedDictionary_;
  const EncodingSelectionPolicyCreator nestedPolicyCreator_;
};

std::string_view encodeSharedDictionary(
    Buffer& buffer,
    const std::vector<uint32_t>& indices);

} // namespace facebook::nimble::test
