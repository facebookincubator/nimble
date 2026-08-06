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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/SharedDictionaryEncoding.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingPrefix.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/tests/SharedDictionaryEncodingTestUtils.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble {
namespace {

class TestSharedDictionaryAlphabet final : public SharedDictionaryAlphabet {
 public:
  explicit TestSharedDictionaryAlphabet(
      std::vector<int32_t> values,
      std::optional<EncodingType> encodingType = std::nullopt)
      : SharedDictionaryAlphabet{DataType::Int32},
        values_{std::make_shared<std::vector<int32_t>>(std::move(values))},
        encodingType_{encodingType} {
    setEntryCount(static_cast<uint32_t>(values_->size()));
  }

  const std::vector<uint32_t>& materializedIndices() const {
    return materializedIndices_;
  }

 private:
  void physicalValueAtImpl(uint32_t index, void* output) const final {
    NIMBLE_CHECK_LT(
        index,
        values_->size(),
        "Shared dictionary index exceeds alphabet size.");
    *static_cast<TypeTraits<int32_t>::physicalType*>(output) =
        static_cast<TypeTraits<int32_t>::physicalType>((*values_)[index]);
  }

  void materializeImpl(std::span<const uint32_t> indices, void* output)
      const final {
    materializedIndices_.assign(indices.begin(), indices.end());

    auto* values = static_cast<TypeTraits<int32_t>::physicalType*>(output);
    for (size_t i{0}; i < indices.size(); ++i) {
      NIMBLE_CHECK_LT(
          indices[i],
          values_->size(),
          "Shared dictionary index exceeds alphabet size.");
      values[i] = static_cast<TypeTraits<int32_t>::physicalType>(
          (*values_)[indices[i]]);
    }
  }

  std::optional<EncodingType> encodingTypeImpl() const final {
    return encodingType_;
  }

  const std::shared_ptr<const std::vector<int32_t>> values_;
  const std::optional<EncodingType> encodingType_;
  mutable std::vector<uint32_t> materializedIndices_;
};

class TestSharedDictionaryResolver final : public SharedDictionaryResolver {
 public:
  TestSharedDictionaryResolver(
      uint32_t dictionaryId,
      std::vector<int32_t> alphabet,
      std::optional<EncodingType> encodingType = std::nullopt)
      : dictionaryId_{dictionaryId},
        alphabet_{std::make_shared<TestSharedDictionaryAlphabet>(
            std::move(alphabet),
            encodingType)} {}

  const TestSharedDictionaryAlphabet& alphabet() const {
    return *alphabet_;
  }

  std::shared_ptr<const SharedDictionaryAlphabet> resolve(
      SharedDictionaryScope scope,
      uint32_t dictionaryId,
      DataType dataType) const final {
    if (scope != SharedDictionaryScope::Stripe ||
        dictionaryId != dictionaryId_ || dataType != DataType::Int32) {
      return nullptr;
    }
    return alphabet_;
  }

 private:
  const uint32_t dictionaryId_;
  const std::shared_ptr<const TestSharedDictionaryAlphabet> alphabet_;
};

class SharedDictionaryEncodingTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() final {
    rootPool_ = velox::memory::memoryManager()->addRootPool(
        "SharedDictionaryEncodingTest");
    pool_ = rootPool_->addLeafChild("SharedDictionaryEncodingTestLeaf");
    buffer_ = std::make_unique<Buffer>(*pool_);
  }

  std::unique_ptr<Encoding> createEncoding(std::string_view encoded) {
    return createEncoding(encoded, Encoding::Options{});
  }

  std::unique_ptr<Encoding> createEncoding(
      std::string_view encoded,
      const Encoding::Options& options) {
    return EncodingFactory{options}.create(
        *pool_, encoded, stringBufferFactory());
  }

  std::function<void*(uint32_t)> stringBufferFactory() {
    return [&](uint32_t totalLength) {
      auto& buffer = stringBuffers_.emplace_back(
          velox::AlignedBuffer::allocate<char>(totalLength, pool_.get()));
      return buffer->asMutable<void>();
    };
  }

  std::vector<int32_t> materialize(std::string_view encoded) {
    auto encoding = createEncoding(encoded);
    Vector<int32_t> output{pool_.get(), encoding->rowCount()};
    encoding->materialize(encoding->rowCount(), output.data());
    return {output.begin(), output.end()};
  }

  static std::vector<int32_t> sequentialAlphabet(uint32_t size) {
    std::vector<int32_t> values;
    values.reserve(size);
    for (uint32_t i{0}; i < size; ++i) {
      values.push_back(static_cast<int32_t>(i));
    }
    return values;
  }

  std::string_view dictionaryAlphabet(std::string_view encoded) const {
    const char* pos = encoded.data() +
        EncodingPrefix::prefixSize(encoded, /*useVarint=*/false);
    const uint32_t alphabetBytes = encoding::readUint32(pos);
    return {pos, alphabetBytes};
  }

  std::string_view dictionaryIndices(std::string_view encoded) const {
    const char* pos = encoded.data() +
        EncodingPrefix::prefixSize(encoded, /*useVarint=*/false);
    const uint32_t alphabetBytes = encoding::readUint32(pos);
    pos += alphabetBytes;
    return {pos, static_cast<size_t>(encoded.data() + encoded.size() - pos)};
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<Buffer> buffer_;
  std::vector<velox::BufferPtr> stringBuffers_;
};

TEST_F(SharedDictionaryEncodingTest, publicApiMaterializesAndSkipsRows) {
  const std::vector<uint32_t> indices{2, 0, 3, 1, 2};
  const auto encoded = test::encodeSharedDictionary(*buffer_, indices);

  Encoding::Options options;
  options.sharedDictionaryResolver =
      std::make_shared<TestSharedDictionaryResolver>(
          /*dictionaryId=*/7, std::vector<int32_t>{10, 20, 30, 40});

  auto encoding = createEncoding(encoded, options);
  EXPECT_EQ(encoding->encodingType(), EncodingType::SharedDictionary);
  EXPECT_EQ(encoding->dataType(), DataType::Int32);
  EXPECT_EQ(encoding->rowCount(), indices.size());
  EXPECT_EQ(encoding->dictionarySize(), 4);

  Vector<int32_t> values{pool_.get(), indices.size()};
  encoding->materialize(indices.size(), values.data());
  const std::vector<int32_t> expected{30, 10, 40, 20, 30};
  EXPECT_EQ(std::vector<int32_t>(values.begin(), values.end()), expected);

  encoding->reset();
  encoding->skip(2);
  Vector<int32_t> suffix{pool_.get(), 2};
  encoding->materialize(2, suffix.data());
  const std::vector<int32_t> expectedSuffix{40, 20};
  EXPECT_EQ(std::vector<int32_t>(suffix.begin(), suffix.end()), expectedSuffix);
}

TEST_F(SharedDictionaryEncodingTest, sliceConvertsToLocalDictionary) {
  struct Scenario {
    std::string_view name;
    std::vector<uint32_t> sourceIndices;
    uint32_t offset;
    uint32_t length;
    std::vector<int32_t> alphabetValues;
    std::optional<EncodingType> alphabetEncodingType;
    std::vector<uint32_t> expectedMaterializedIndices;
    std::vector<int32_t> expected;
  };

  const std::vector<Scenario> scenarios{
      {
          "denseSharedIndexRange",
          {2, 1, 2, 3, 1, 4},
          /*offset=*/0,
          /*length=*/6,
          {100, 200, 300, 400, 500, 600},
          EncodingType::Trivial,
          {1, 2, 3, 4},
          {300, 200, 300, 400, 200, 500},
      },
      {
          "fixedAlphabetEncodingHint",
          {0, 1, 0, 2, 3, 1},
          /*offset=*/0,
          /*length=*/4,
          {100, 200, 300, 400, 500, 600},
          EncodingType::FixedBitWidth,
          {0, 1, 2},
          {100, 200, 100, 300},
      },
      {
          "noAlphabetEncodingHint",
          {0, 1, 0, 2, 3, 1},
          /*offset=*/0,
          /*length=*/4,
          {100, 200, 300, 400, 500, 600},
          std::nullopt,
          {0, 1, 2},
          {100, 200, 100, 300},
      },
      {
          "sparseSharedIndexRange",
          {1000, 1, 1000, 500},
          /*offset=*/0,
          /*length=*/4,
          sequentialAlphabet(/*size=*/1001),
          EncodingType::Trivial,
          {1, 500, 1000},
          {1000, 1, 1000, 500},
      },
  };

  for (const auto& testCase : scenarios) {
    SCOPED_TRACE(testCase.name);
    const auto encoded =
        test::encodeSharedDictionary(*buffer_, testCase.sourceIndices);

    Encoding::Options options;
    auto resolver = std::make_shared<TestSharedDictionaryResolver>(
        /*dictionaryId=*/7,
        testCase.alphabetValues,
        testCase.alphabetEncodingType);
    options.sharedDictionaryResolver = resolver;

    const auto sliced = EncodingFactory::slice(
        encoded, testCase.offset, testCase.length, *buffer_, options);
    EXPECT_NE(sliced.data(), encoded.data());
    EXPECT_EQ(EncodingPrefix::encodingType(sliced), EncodingType::Dictionary);
    EXPECT_EQ(EncodingPrefix::dataType(sliced), DataType::Int32);
    EXPECT_EQ(
        EncodingPrefix::readRowCount(sliced, /*useVarint=*/false),
        testCase.length);
    EXPECT_EQ(materialize(sliced), testCase.expected);

    const auto alphabet = dictionaryAlphabet(sliced);
    const auto localIndices = dictionaryIndices(sliced);
    EXPECT_EQ(
        resolver->alphabet().materializedIndices(),
        testCase.expectedMaterializedIndices);
    EXPECT_EQ(
        EncodingPrefix::readRowCount(alphabet, /*useVarint=*/false),
        static_cast<uint32_t>(testCase.expectedMaterializedIndices.size()));
    if (testCase.alphabetEncodingType.has_value()) {
      EXPECT_EQ(
          EncodingPrefix::encodingType(alphabet),
          *testCase.alphabetEncodingType);
    }
    EXPECT_EQ(
        EncodingPrefix::encodingType(localIndices),
        EncodingType::FixedBitWidth);
  }
}

TEST_F(SharedDictionaryEncodingTest, sliceConvertsToConstantEncoding) {
  const std::vector<uint32_t> indices{0, 0, 0, 1};
  const auto encoded = test::encodeSharedDictionary(*buffer_, indices);

  Encoding::Options options;
  options.sharedDictionaryResolver =
      std::make_shared<TestSharedDictionaryResolver>(
          /*dictionaryId=*/7, std::vector<int32_t>{100, 200, 300});

  const auto sliced = EncodingFactory::slice(
      encoded, /*offset=*/0, /*length=*/3, *buffer_, options);
  EXPECT_EQ(EncodingPrefix::encodingType(sliced), EncodingType::Constant);
  EXPECT_EQ(EncodingPrefix::readRowCount(sliced, /*useVarint=*/false), 3);

  const std::vector<int32_t> expected{100, 100, 100};
  EXPECT_EQ(materialize(sliced), expected);
}

} // namespace
} // namespace facebook::nimble
