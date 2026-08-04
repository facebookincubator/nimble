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

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <fmt/core.h>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/BlockBitPackingEncoding.h"
#include "dwio/nimble/encodings/DeltaBlockEncoding.h"
#include "dwio/nimble/encodings/SharedDictionaryTypes.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble {
namespace {

enum class AlphabetStorageKind {
  DecodedChunk,
  EncodedView,
};

enum class AlphabetValueKind {
  Int16,
  Uint32,
  Int64,
  String,
};

struct AlphabetMaterializeCase {
  AlphabetStorageKind storage{AlphabetStorageKind::DecodedChunk};
  AlphabetValueKind value{AlphabetValueKind::Int16};
  EncodingType encodingType{EncodingType::Trivial};
  std::vector<std::vector<int64_t>> numberChunks;
  std::vector<std::vector<std::string_view>> stringChunks;
  uint32_t valueAtIndex{};
  std::vector<uint32_t> indices;
};

std::string alphabetStorageKindName(AlphabetStorageKind storage) {
  switch (storage) {
    case AlphabetStorageKind::DecodedChunk:
      return "decodedChunk";
    case AlphabetStorageKind::EncodedView:
      return "encodedView";
  }
  return fmt::format("unknownStorage{}", static_cast<int>(storage));
}

std::string alphabetValueKindName(AlphabetValueKind kind) {
  switch (kind) {
    case AlphabetValueKind::Int16:
      return "int16";
    case AlphabetValueKind::Uint32:
      return "uint32";
    case AlphabetValueKind::Int64:
      return "int64";
    case AlphabetValueKind::String:
      return "string";
  }
  return fmt::format("unknownValue{}", static_cast<int>(kind));
}

std::string alphabetEncodingTypeName(EncodingType encodingType) {
  if (encodingType == EncodingType::Trivial) {
    return "trivial";
  }
  if (encodingType == EncodingType::BlockBitPacking) {
    return "blockBitPacking";
  }
  if (encodingType == EncodingType::DeltaBlock) {
    return "deltaBlock";
  }
  return fmt::format("encoding{}", static_cast<int>(encodingType));
}

std::string alphabetMaterializeCaseName(
    const testing::TestParamInfo<AlphabetMaterializeCase>& info) {
  auto name = alphabetStorageKindName(info.param.storage) + "_" +
      alphabetValueKindName(info.param.value);
  if (info.param.storage == AlphabetStorageKind::EncodedView) {
    name += "_" + alphabetEncodingTypeName(info.param.encodingType);
  }
  return name;
}

std::string alphabetMaterializeCaseDescription(
    const AlphabetMaterializeCase& testCase) {
  return fmt::format(
      "{}_{}_{}",
      alphabetStorageKindName(testCase.storage),
      alphabetValueKindName(testCase.value),
      alphabetEncodingTypeName(testCase.encodingType));
}

uint32_t alphabetMaterializeCaseSeed(const AlphabetMaterializeCase& testCase) {
  return 0x5EED'1000u ^ (static_cast<uint32_t>(testCase.storage) << 16) ^
      (static_cast<uint32_t>(testCase.value) << 8) ^
      static_cast<uint32_t>(testCase.encodingType);
}

constexpr size_t kNumberValuesPerChunk{18};
constexpr size_t kNumberChunkCount{3};
constexpr size_t kRandomReadIterations{64};

std::vector<uint32_t> numberMaterializeIndices() {
  return {17, 0, 28, 3, 45, 21, 53, 12, 37};
}

std::vector<uint32_t> stringMaterializeIndices() {
  return {7, 0, 10, 3, 17, 12, 23};
}

std::vector<std::vector<int64_t>> unsortedSignedNumberChunks() {
  std::vector<std::vector<int64_t>> chunks;
  chunks.reserve(kNumberChunkCount);
  for (size_t chunkIndex{0}; chunkIndex < kNumberChunkCount; ++chunkIndex) {
    auto& chunk = chunks.emplace_back();
    chunk.reserve(kNumberValuesPerChunk);
    for (size_t i{0}; i < kNumberValuesPerChunk; ++i) {
      const auto magnitude =
          static_cast<int64_t>((chunkIndex + 1) * 100 + i * 11);
      chunk.push_back(i % 2 == 0 ? magnitude : -magnitude);
    }
  }
  return chunks;
}

std::vector<std::vector<int64_t>> sortedSignedNumberChunks() {
  std::vector<std::vector<int64_t>> chunks;
  chunks.reserve(kNumberChunkCount);
  for (size_t chunkIndex{0}; chunkIndex < kNumberChunkCount; ++chunkIndex) {
    auto& chunk = chunks.emplace_back();
    chunk.reserve(kNumberValuesPerChunk);
    const auto first = static_cast<int64_t>(chunkIndex * 1'000) - 800;
    for (size_t i{0}; i < kNumberValuesPerChunk; ++i) {
      chunk.push_back(first + static_cast<int64_t>(i * 13));
    }
  }
  return chunks;
}

std::vector<std::vector<int64_t>> unsortedUnsignedNumberChunks() {
  std::vector<std::vector<int64_t>> chunks;
  chunks.reserve(kNumberChunkCount);
  for (size_t chunkIndex{0}; chunkIndex < kNumberChunkCount; ++chunkIndex) {
    auto& chunk = chunks.emplace_back();
    chunk.reserve(kNumberValuesPerChunk);
    for (size_t i{0}; i < kNumberValuesPerChunk; ++i) {
      chunk.push_back(
          static_cast<int64_t>((chunkIndex + 1) * 1'000 + i) +
          static_cast<int64_t>((i * 7) % kNumberValuesPerChunk) * 31);
    }
  }
  return chunks;
}

std::vector<std::vector<std::string_view>> stringChunks() {
  return {
      {"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"},
      {"iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi"},
      {"rho", "sigma", "tau", "upsilon", "phi", "chi", "psi", "omega"}};
}

AlphabetMaterializeCase numberCase(
    AlphabetStorageKind storage,
    AlphabetValueKind value,
    EncodingType encodingType,
    std::vector<std::vector<int64_t>> chunks) {
  AlphabetMaterializeCase testCase;
  testCase.storage = storage;
  testCase.value = value;
  testCase.encodingType = encodingType;
  testCase.numberChunks = std::move(chunks);
  testCase.valueAtIndex = 45;
  testCase.indices = numberMaterializeIndices();
  return testCase;
}

AlphabetMaterializeCase stringCase(
    AlphabetStorageKind storage,
    EncodingType encodingType) {
  AlphabetMaterializeCase testCase;
  testCase.storage = storage;
  testCase.value = AlphabetValueKind::String;
  testCase.encodingType = encodingType;
  testCase.stringChunks = stringChunks();
  testCase.valueAtIndex = 17;
  testCase.indices = stringMaterializeIndices();
  return testCase;
}

class SharedDictionaryTypesTestBase : public testing::Test {
 protected:
  void SetUp() final {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<Buffer>(*pool_);
  }

  template <typename T>
  Vector<T> makeVector(const std::vector<T>& values) {
    Vector<T> out{pool_.get()};
    out.insert(out.end(), values.data(), values.data() + values.size());
    return out;
  }

  SharedDictionaryAlphabet::EncodedChunk makeEncodedChunk(
      uint32_t begin,
      std::string_view encoded,
      const Encoding::Options& options) {
    auto owner = std::make_shared<std::string>(encoded);
    auto view = createEncodingView(
        std::string_view{owner->data(), owner->size()}, pool_.get(), options);
    encodedOwners_.push_back(owner);
    return SharedDictionaryAlphabet::encodedChunk(
        begin, std::shared_ptr<const EncodingView>{std::move(view)});
  }

  template <typename T>
  static typename TypeTraits<T>::physicalType physical(T value) {
    return EncodingPhysicalType<T>::asEncodingPhysicalType(value);
  }

  template <typename T>
  static std::vector<std::vector<T>> valueChunks(
      const AlphabetMaterializeCase& testCase) {
    if constexpr (std::is_same_v<T, std::string_view>) {
      return testCase.stringChunks;
    } else {
      std::vector<std::vector<T>> out;
      out.reserve(testCase.numberChunks.size());
      for (const auto& chunk : testCase.numberChunks) {
        auto& typedChunk = out.emplace_back();
        typedChunk.reserve(chunk.size());
        for (const auto value : chunk) {
          typedChunk.push_back(static_cast<T>(value));
        }
      }
      return out;
    }
  }

  template <typename T>
  static std::vector<T> flattenedValues(
      const AlphabetMaterializeCase& testCase) {
    std::vector<T> out;
    for (const auto& chunk : valueChunks<T>(testCase)) {
      out.insert(out.end(), chunk.begin(), chunk.end());
    }
    return out;
  }

  template <typename T>
  static std::vector<T> expectedValues(
      const AlphabetMaterializeCase& testCase) {
    return expectedValues<T>(
        testCase,
        std::span<const uint32_t>{
            testCase.indices.data(), testCase.indices.size()});
  }

  template <typename T>
  static std::vector<T> expectedValues(
      const AlphabetMaterializeCase& testCase,
      std::span<const uint32_t> indices) {
    const auto values = flattenedValues<T>(testCase);
    std::vector<T> out;
    out.reserve(indices.size());
    for (const auto index : indices) {
      NIMBLE_CHECK_LT(index, values.size());
      out.push_back(values[index]);
    }
    return out;
  }

  template <typename T>
  std::shared_ptr<std::vector<typename TypeTraits<T>::physicalType>>
  makePhysicalVector(const std::vector<T>& values) {
    auto out =
        std::make_shared<std::vector<typename TypeTraits<T>::physicalType>>();
    out->reserve(values.size());
    for (const auto& value : values) {
      out->push_back(physical<T>(value));
    }
    return out;
  }

  template <typename T>
  std::shared_ptr<const SharedDictionaryAlphabet> makeDecodedAlphabet(
      const AlphabetMaterializeCase& testCase) {
    std::vector<SharedDictionaryAlphabet::DecodedChunk> decodedChunks;
    uint32_t begin{0};
    for (const auto& chunk : valueChunks<T>(testCase)) {
      auto entries = makePhysicalVector<T>(chunk);
      decodedChunks.push_back(
          SharedDictionaryAlphabet::decodedChunk(
              begin,
              static_cast<uint32_t>(entries->size()),
              entries->data(),
              entries));
      begin += static_cast<uint32_t>(entries->size());
    }
    return SharedDictionaryAlphabet::createDecoded(
        TypeTraits<T>::dataType, std::move(decodedChunks));
  }

  template <typename T>
  std::string_view encodeWithTrivial(
      const std::vector<T>& values,
      const Encoding::Options& options) {
    return test::Encoder<TrivialEncoding<T>>::encode(
        *buffer_,
        makeVector<T>(values),
        CompressionType::Uncompressed,
        options);
  }

  template <typename T>
  std::string_view encodeWithBlockBitPacking(
      const std::vector<T>& values,
      const Encoding::Options& options) {
    if constexpr (isNumericType<typename TypeTraits<T>::physicalType>()) {
      return test::Encoder<BlockBitPackingEncoding<T>>::encode(
          *buffer_,
          makeVector<T>(values),
          CompressionType::Uncompressed,
          options);
    }
    NIMBLE_UNSUPPORTED(
        "BlockBitPacking test mode does not support {}.",
        TypeTraits<T>::dataType);
  }

  template <typename T>
  std::string_view encodeWithDeltaBlock(
      const std::vector<T>& values,
      const Encoding::Options& options) {
    if constexpr (isIntegralType<T>() && !std::is_same_v<T, bool>) {
      return test::Encoder<DeltaBlockEncoding<T>>::encode(
          *buffer_,
          makeVector<T>(values),
          CompressionType::Uncompressed,
          options);
    }
    NIMBLE_UNSUPPORTED(
        "DeltaBlock test mode does not support {}.", TypeTraits<T>::dataType);
  }

  Encoding::Options encodingOptions(EncodingType encodingType) const {
    Encoding::Options options;
    if (encodingType == EncodingType::DeltaBlock) {
      options.deltaBlockSize = 16;
    }
    return options;
  }

  template <typename T>
  std::string_view encodeValues(
      const std::vector<T>& values,
      EncodingType encodingType,
      const Encoding::Options& options) {
    if (encodingType == EncodingType::Trivial) {
      return encodeWithTrivial<T>(values, options);
    }
    if (encodingType == EncodingType::BlockBitPacking) {
      return encodeWithBlockBitPacking<T>(values, options);
    }
    if (encodingType == EncodingType::DeltaBlock) {
      return encodeWithDeltaBlock<T>(values, options);
    }
    NIMBLE_UNSUPPORTED(
        "{} test mode is not supported by shared dictionary tests.",
        encodingType);
  }

  template <typename T>
  SharedDictionaryAlphabet::EncodedChunk makeEncodedChunk(
      uint32_t begin,
      const std::vector<T>& values,
      EncodingType encodingType) {
    const auto options = encodingOptions(encodingType);
    return makeEncodedChunk(
        begin, encodeValues<T>(values, encodingType, options), options);
  }

  template <typename T>
  std::shared_ptr<const SharedDictionaryAlphabet> makeEncodedAlphabet(
      const AlphabetMaterializeCase& testCase) {
    std::vector<SharedDictionaryAlphabet::EncodedChunk> encodedChunks;
    uint32_t begin{0};
    for (const auto& chunk : valueChunks<T>(testCase)) {
      encodedChunks.push_back(
          makeEncodedChunk<T>(begin, chunk, testCase.encodingType));
      begin += static_cast<uint32_t>(chunk.size());
    }
    return SharedDictionaryAlphabet::createEncoded(
        TypeTraits<T>::dataType, std::move(encodedChunks));
  }

  template <typename T>
  std::shared_ptr<const SharedDictionaryAlphabet> makeAlphabet(
      const AlphabetMaterializeCase& testCase) {
    switch (testCase.storage) {
      case AlphabetStorageKind::DecodedChunk:
        return makeDecodedAlphabet<T>(testCase);
      case AlphabetStorageKind::EncodedView:
        return makeEncodedAlphabet<T>(testCase);
    }
    NIMBLE_UNSUPPORTED(
        "Unsupported alphabet storage kind {}.",
        alphabetStorageKindName(testCase.storage));
  }

  template <typename T>
  static std::vector<T> logicalValues(
      const std::vector<typename TypeTraits<T>::physicalType>& values) {
    std::vector<T> out;
    out.reserve(values.size());
    for (const auto& value : values) {
      out.push_back(EncodingPhysicalType<T>::asEncodingLogicalType(value));
    }
    return out;
  }

  template <typename T>
  void verifyMaterialize(const AlphabetMaterializeCase& testCase) {
    const auto alphabet = makeAlphabet<T>(testCase);

    EXPECT_EQ(alphabet->entryCount(), flattenedValues<T>(testCase).size());
    EXPECT_EQ(
        EncodingPhysicalType<T>::asEncodingLogicalType(
            alphabet->template physicalValueAt<T>(testCase.valueAtIndex)),
        flattenedValues<T>(testCase)[testCase.valueAtIndex]);

    std::vector<typename TypeTraits<T>::physicalType> values(
        testCase.indices.size());
    alphabet->template materialize<T>(
        std::span<const uint32_t>{
            testCase.indices.data(), testCase.indices.size()},
        values.data());
    EXPECT_EQ(logicalValues<T>(values), expectedValues<T>(testCase));
  }

  template <typename T>
  void verifyMaterialize(
      const SharedDictionaryAlphabet& alphabet,
      const AlphabetMaterializeCase& testCase,
      std::span<const uint32_t> indices) {
    std::vector<typename TypeTraits<T>::physicalType> values(indices.size());
    alphabet.template materialize<T>(indices, values.data());
    EXPECT_EQ(logicalValues<T>(values), expectedValues<T>(testCase, indices));
  }

  std::vector<uint32_t> rangeIndices(uint32_t begin, uint32_t count) {
    std::vector<uint32_t> indices;
    indices.reserve(count);
    for (uint32_t offset{0}; offset < count; ++offset) {
      indices.push_back(begin + offset);
    }
    return indices;
  }

  std::vector<uint32_t> randomIndices(
      std::mt19937& random,
      uint32_t entryCount) {
    std::uniform_int_distribution<uint32_t> sizeDistribution{1, entryCount};
    std::uniform_int_distribution<uint32_t> indexDistribution{
        0, entryCount - 1};
    const auto size = sizeDistribution(random);
    std::vector<uint32_t> indices;
    indices.reserve(size);
    for (uint32_t i{0}; i < size; ++i) {
      indices.push_back(indexDistribution(random));
    }
    return indices;
  }

  std::vector<uint32_t> randomRangeIndices(
      std::mt19937& random,
      uint32_t entryCount) {
    std::uniform_int_distribution<uint32_t> beginDistribution{
        0, entryCount - 1};
    const auto begin = beginDistribution(random);
    std::uniform_int_distribution<uint32_t> countDistribution{
        1, entryCount - begin};
    return rangeIndices(begin, countDistribution(random));
  }

  template <typename T>
  void verifyRandomMaterialize(const AlphabetMaterializeCase& testCase) {
    const auto alphabet = makeAlphabet<T>(testCase);
    const auto values = flattenedValues<T>(testCase);
    ASSERT_EQ(alphabet->entryCount(), values.size());
    ASSERT_GT(alphabet->entryCount(), 0);

    auto random = std::mt19937{alphabetMaterializeCaseSeed(testCase)};
    const auto entryCount = alphabet->entryCount();

    const auto verifyIndices = [&](std::string_view label,
                                   const std::vector<uint32_t>& indices) {
      SCOPED_TRACE(
          fmt::format(
              "{} {} size={}",
              alphabetMaterializeCaseDescription(testCase),
              label,
              indices.size()));
      verifyMaterialize<T>(
          *alphabet,
          testCase,
          std::span<const uint32_t>{indices.data(), indices.size()});
    };

    verifyIndices("singleValueRange", rangeIndices(entryCount / 2, 1));
    verifyIndices("fullRange", rangeIndices(/*begin=*/0, entryCount));

    std::uniform_int_distribution<uint32_t> indexDistribution{
        0, entryCount - 1};
    for (size_t i{0}; i < kRandomReadIterations; ++i) {
      SCOPED_TRACE(
          fmt::format(
              "{} iteration={}",
              alphabetMaterializeCaseDescription(testCase),
              i));
      const auto index = indexDistribution(random);
      EXPECT_EQ(
          EncodingPhysicalType<T>::asEncodingLogicalType(
              alphabet->template physicalValueAt<T>(index)),
          values[index]);
      verifyIndices("randomIndices", randomIndices(random, entryCount));
      verifyIndices("randomRange", randomRangeIndices(random, entryCount));
    }
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<Buffer> buffer_;
  std::vector<std::shared_ptr<const std::string>> encodedOwners_;
};

class SharedDictionaryTypesTest
    : public SharedDictionaryTypesTestBase,
      public testing::WithParamInterface<AlphabetMaterializeCase> {
 protected:
  void verifyMaterialize() {
    const auto& testCase = GetParam();
    switch (testCase.value) {
      case AlphabetValueKind::Int16:
        SharedDictionaryTypesTestBase::verifyMaterialize<int16_t>(testCase);
        return;
      case AlphabetValueKind::Uint32:
        SharedDictionaryTypesTestBase::verifyMaterialize<uint32_t>(testCase);
        return;
      case AlphabetValueKind::Int64:
        SharedDictionaryTypesTestBase::verifyMaterialize<int64_t>(testCase);
        return;
      case AlphabetValueKind::String:
        SharedDictionaryTypesTestBase::verifyMaterialize<std::string_view>(
            testCase);
        return;
    }
  }

  void verifyRandomMaterialize() {
    const auto& testCase = GetParam();
    switch (testCase.value) {
      case AlphabetValueKind::Int16:
        SharedDictionaryTypesTestBase::verifyRandomMaterialize<int16_t>(
            testCase);
        return;
      case AlphabetValueKind::Uint32:
        SharedDictionaryTypesTestBase::verifyRandomMaterialize<uint32_t>(
            testCase);
        return;
      case AlphabetValueKind::Int64:
        SharedDictionaryTypesTestBase::verifyRandomMaterialize<int64_t>(
            testCase);
        return;
      case AlphabetValueKind::String:
        SharedDictionaryTypesTestBase::verifyRandomMaterialize<
            std::string_view>(testCase);
        return;
    }
  }
};

TEST_P(SharedDictionaryTypesTest, materializesAcrossChunks) {
  verifyMaterialize();
}

TEST_P(SharedDictionaryTypesTest, materializesRandomIndicesAndRanges) {
  verifyRandomMaterialize();
}

TEST(SharedDictionaryScopeTest, formatsNames) {
  EXPECT_EQ(
      SharedDictionaryScopeName::toName(SharedDictionaryScope::Stripe),
      "Stripe");
  EXPECT_EQ(
      SharedDictionaryScopeName::toSharedDictionaryScope("File"),
      SharedDictionaryScope::File);
  EXPECT_EQ(fmt::format("{}", SharedDictionaryScope::External), "External");
  EXPECT_FALSE(
      SharedDictionaryScopeName::tryToSharedDictionaryScope("Unknown"));
}

INSTANTIATE_TEST_SUITE_P(
    sharedDictionaryAlphabet,
    SharedDictionaryTypesTest,
    testing::Values(
        numberCase(
            AlphabetStorageKind::DecodedChunk,
            AlphabetValueKind::Int16,
            EncodingType::Trivial,
            unsortedSignedNumberChunks()),
        numberCase(
            AlphabetStorageKind::EncodedView,
            AlphabetValueKind::Int16,
            EncodingType::Trivial,
            unsortedSignedNumberChunks()),
        numberCase(
            AlphabetStorageKind::DecodedChunk,
            AlphabetValueKind::Uint32,
            EncodingType::Trivial,
            unsortedUnsignedNumberChunks()),
        numberCase(
            AlphabetStorageKind::EncodedView,
            AlphabetValueKind::Uint32,
            EncodingType::BlockBitPacking,
            unsortedUnsignedNumberChunks()),
        numberCase(
            AlphabetStorageKind::DecodedChunk,
            AlphabetValueKind::Int64,
            EncodingType::Trivial,
            unsortedSignedNumberChunks()),
        numberCase(
            AlphabetStorageKind::EncodedView,
            AlphabetValueKind::Int64,
            EncodingType::DeltaBlock,
            sortedSignedNumberChunks()),
        stringCase(AlphabetStorageKind::DecodedChunk, EncodingType::Trivial),
        stringCase(AlphabetStorageKind::EncodedView, EncodingType::Trivial)),
    alphabetMaterializeCaseName);

} // namespace
} // namespace facebook::nimble
