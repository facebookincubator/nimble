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
#include "dwio/nimble/encodings/ALPEncoding.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/common/tests/NimbleCompare.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/selection/EncodingSelectionPolicy.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "dwio/nimble/tools/EncodingUtilities.h"
#include "fmt/core.h"

#include <limits>
#include <random>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

using namespace facebook;

template <typename DataType, bool UseVarint>
struct TestConfig {
  using data_type = DataType;
  static constexpr bool useVarint = UseVarint;
};

#define TC(T) TestConfig<T, false>, TestConfig<T, true>

template <typename Config>
class ALPEncodingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  template <typename T>
  nimble::Vector<T> toVector(std::initializer_list<T> l) {
    nimble::Vector<T> v{pool_.get()};
    v.insert(v.end(), l.begin(), l.end());
    return v;
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
};

using TestTypes = ::testing::Types<TC(float), TC(double)>;

TYPED_TEST_CASE(ALPEncodingTest, TestTypes);

nimble::EncodingLayout fixedBitWidthLayout() {
  return nimble::EncodingLayout{
      nimble::EncodingType::FixedBitWidth,
      {},
      nimble::CompressionType::Uncompressed};
}

nimble::EncodingLayout alpWithFixedBitWidthPayloadLayout() {
  return nimble::EncodingLayout{
      nimble::EncodingType::ALP,
      {},
      nimble::CompressionType::Uncompressed,
      {fixedBitWidthLayout()}};
}

nimble::EncodingLayout dictionaryWithAlpAlphabetLayout() {
  return nimble::EncodingLayout{
      nimble::EncodingType::Dictionary,
      {},
      nimble::CompressionType::Uncompressed,
      {alpWithFixedBitWidthPayloadLayout(), fixedBitWidthLayout()}};
}

nimble::EncodingSelectionPolicyCreator unusedNestedPolicyCreator() {
  return [](nimble::DataType) {
    return std::unique_ptr<nimble::EncodingSelectionPolicyBase>{};
  };
}

TEST(ALPSizeEstimationTest, invalidSampleRejected) {
  const std::vector<uint32_t> sample = {0, 1};

  NIMBLE_ASSERT_THROW(
      nimble::ALPEncoding<float>::estimateSizeFromSample(
          /*rowCount=*/0, std::span<const uint32_t>{sample.data(), 1}),
      "ALP estimation requires non-empty input.");
  NIMBLE_ASSERT_THROW(
      nimble::ALPEncoding<float>::estimateSizeFromSample(
          /*rowCount=*/1, std::span<const uint32_t>{}),
      "ALP estimation requires a non-empty sample.");
  NIMBLE_ASSERT_THROW(
      nimble::ALPEncoding<float>::estimateSizeFromSample(
          /*rowCount=*/1, sample),
      "ALP sample size cannot exceed the input row count.");
}

TEST(ALPEncodingHeaderTest, compactControlWordRoundTrip) {
  std::array<char, 3> serialized{};
  char* writePosition = serialized.data();
  nimble::detail::alp::writeHeader(
      nimble::detail::alp::Header{
          .exponent = 23, .factor = 17, .hasExceptions = true},
      writePosition);

  const std::array<char, 3> expected = {
      static_cast<char>(0x37),
      static_cast<char>(0x02),
      static_cast<char>(0x01)};
  EXPECT_EQ(serialized, expected);
  EXPECT_EQ(writePosition, serialized.data() + serialized.size());

  const char* readPosition = serialized.data();
  const auto header = nimble::detail::alp::readHeader(readPosition);
  EXPECT_EQ(header.exponent, 23);
  EXPECT_EQ(header.factor, 17);
  EXPECT_TRUE(header.hasExceptions);
  EXPECT_EQ(readPosition, serialized.data() + serialized.size());
}

template <typename D>
nimble::Vector<D> makeRandomDecimalValues(
    velox::memory::MemoryPool* pool,
    uint32_t rowCount,
    uint32_t uniqueCount,
    uint32_t seed) {
  std::mt19937 rng(seed);
  std::uniform_int_distribution<int32_t> valueDistribution{-5000, 5000};
  std::uniform_int_distribution<uint32_t> indexDistribution{0, uniqueCount - 1};

  std::vector<D> dictionary;
  dictionary.reserve(uniqueCount);
  for (uint32_t i = 0; i < uniqueCount; ++i) {
    dictionary.push_back(
        static_cast<D>(valueDistribution(rng)) / static_cast<D>(100));
  }

  nimble::Vector<D> values{pool};
  values.reserve(rowCount);
  for (uint32_t i = 0; i < rowCount; ++i) {
    values.push_back(dictionary[indexDistribution(rng)]);
  }
  return values;
}

void expectEncodingLayout(
    std::string_view serialized,
    const std::vector<
        std::tuple<nimble::EncodingType, nimble::DataType, std::string>>&
        expected) {
  std::vector<std::tuple<nimble::EncodingType, nimble::DataType, std::string>>
      actual;
  nimble::tools::traverseEncodings(
      serialized,
      [&](auto encodingType,
          auto dataType,
          auto /* level */,
          auto /* index */,
          auto nestedEncodingName,
          std::unordered_map<
              nimble::tools::EncodingPropertyType,
              nimble::tools::EncodingProperty> /* properties */) {
        actual.emplace_back(encodingType, dataType, nestedEncodingName);
        return true;
      });

  EXPECT_EQ(actual, expected);
}

std::unique_ptr<nimble::Encoding> createEncoding(
    velox::memory::MemoryPool* pool,
    std::string_view serialized,
    const nimble::Encoding::Options& options,
    std::vector<velox::BufferPtr>& stringBuffers) {
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& stringBuffer = stringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, pool));
    return stringBuffer->template asMutable<void>();
  };
  return nimble::EncodingFactory(options).create(
      *pool, serialized, stringBufferFactory);
}

template <typename D>
std::string_view encodeWithLayout(
    nimble::Buffer& buffer,
    const nimble::Vector<D>& values,
    nimble::EncodingLayout layout,
    const nimble::Encoding::Options& options) {
  auto policy = std::make_unique<nimble::ReplayedEncodingSelectionPolicy<D>>(
      std::move(layout), std::nullopt, unusedNestedPolicyCreator());
  return nimble::EncodingFactory::encode<D>(
      std::move(policy),
      std::span<const D>{values.data(), values.size()},
      buffer,
      options);
}

template <typename D>
void expectInterleavedMaterializeAndSkip(
    nimble::Encoding& encoding,
    const nimble::Vector<D>& values,
    velox::memory::MemoryPool* pool,
    uint32_t seed) {
  std::mt19937 rng(seed);
  std::uniform_int_distribution<uint32_t> skipDistribution{0, 13};
  std::uniform_int_distribution<uint32_t> readDistribution{1, 17};

  nimble::Vector<D> output{pool};
  uint32_t cursor{0};
  while (cursor < values.size()) {
    const auto remainingRows = static_cast<uint32_t>(values.size() - cursor);
    const auto rowsToSkip = std::min(skipDistribution(rng), remainingRows);
    encoding.skip(rowsToSkip);
    cursor += rowsToSkip;

    if (cursor == values.size()) {
      break;
    }

    const auto rowsToRead = std::min(
        readDistribution(rng), static_cast<uint32_t>(values.size() - cursor));
    output.resize(rowsToRead);
    encoding.materialize(rowsToRead, output.data());

    for (uint32_t i = 0; i < rowsToRead; ++i) {
      SCOPED_TRACE(fmt::format("cursor={} i={}", cursor, i));
      EXPECT_TRUE(
          nimble::NimbleCompare<D>::equals(output[i], values[cursor + i]));
    }
    cursor += rowsToRead;
  }

  std::uniform_int_distribution<uint32_t> targetDistribution{
      0, static_cast<uint32_t>(values.size() - 1)};
  for (uint32_t attempt = 0; attempt < 32; ++attempt) {
    const auto target = targetDistribution(rng);
    const auto rowsToRead = std::min(
        readDistribution(rng), static_cast<uint32_t>(values.size() - target));
    encoding.reset();
    encoding.skip(target);
    output.resize(rowsToRead);
    encoding.materialize(rowsToRead, output.data());

    for (uint32_t i = 0; i < rowsToRead; ++i) {
      SCOPED_TRACE(fmt::format("target={} i={}", target, i));
      EXPECT_TRUE(
          nimble::NimbleCompare<D>::equals(output[i], values[target + i]));
    }
  }
}

TYPED_TEST(ALPEncodingTest, roundTrip) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  auto values = this->template toVector<D>({1, 2, 3, 4, 5});

  std::vector<velox::BufferPtr> stringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buf = stringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buf->template asMutable<void>();
  };

  auto encoding = nimble::test::Encoder<nimble::ALPEncoding<D>>::createEncoding(
      *this->buffer_,
      values,
      stringBufferFactory,
      nimble::CompressionType::Uncompressed,
      options);

  EXPECT_EQ(encoding->encodingType(), nimble::EncodingType::ALP);
  EXPECT_EQ(encoding->dataType(), nimble::TypeTraits<D>::dataType);
  EXPECT_EQ(encoding->rowCount(), values.size());

  nimble::Vector<D> result(this->pool_.get(), values.size());
  encoding->materialize(values.size(), result.data());

  for (uint32_t i = 0; i < values.size(); ++i) {
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]));
  }
}

TYPED_TEST(ALPEncodingTest, headerMetadataUsesVarints) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint,
      .fixedBitWidthUseExactBits = true};

  nimble::Vector<D> values{this->pool_.get(), 200};
  values.fill(D{1.25});
  values[150] = std::numeric_limits<D>::infinity();
  const auto serialized = encodeWithLayout<D>(
      *this->buffer_, values, alpWithFixedBitWidthPayloadLayout(), options);

  const char* pos = serialized.data() +
      nimble::EncodingPrefix::readPrefixSize(
                        serialized, options.useVarintRowCount);
  const auto header = nimble::detail::alp::readHeader(pos);
  EXPECT_TRUE(header.hasExceptions);

  const auto* exceptionCountStart = pos;
  EXPECT_EQ(nimble::varint::readVarint32(&pos), 1);
  EXPECT_EQ(pos - exceptionCountStart, 1);

  const auto* encodedValuesSizeStart = pos;
  const auto encodedValuesSize = nimble::varint::readVarint32(&pos);
  EXPECT_EQ(
      pos - encodedValuesSizeStart,
      nimble::varint::varintSize(encodedValuesSize));
  pos += encodedValuesSize;

  const auto* exceptionPositionStart = pos;
  EXPECT_EQ(nimble::encoding::readUint32(pos), 150);
  EXPECT_EQ(pos - exceptionPositionStart, sizeof(uint32_t));

  std::vector<velox::BufferPtr> stringBuffers;
  auto encoding =
      createEncoding(this->pool_.get(), serialized, options, stringBuffers);
  nimble::Vector<D> result{this->pool_.get(), values.size()};
  encoding->materialize(values.size(), result.data());
  for (uint32_t i = 0; i < values.size(); ++i) {
    SCOPED_TRACE(i);
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]));
  }
}

TYPED_TEST(ALPEncodingTest, roundTripSignedDecimals) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  auto values =
      this->template toVector<D>({-12.5, -1.25, -0.5, 0, 0.5, 1.25, 12.5});

  std::vector<velox::BufferPtr> stringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buf = stringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buf->template asMutable<void>();
  };

  auto encoding = nimble::test::Encoder<nimble::ALPEncoding<D>>::createEncoding(
      *this->buffer_,
      values,
      stringBufferFactory,
      nimble::CompressionType::Uncompressed,
      options);

  nimble::Vector<D> result(this->pool_.get(), values.size());
  encoding->materialize(values.size(), result.data());

  for (uint32_t i = 0; i < values.size(); ++i) {
    SCOPED_TRACE(fmt::format("i={}", i));
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]));
  }
}

TYPED_TEST(ALPEncodingTest, manualSelectionUsesAlpEstimate) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = false, .fixedBitWidthUseExactBits = true};

  nimble::Vector<D> values{this->pool_.get()};
  values.reserve(2048);
  for (auto i = 0; i < 2048; ++i) {
    values.push_back(static_cast<D>((i % 129) - 64) / static_cast<D>(10));
  }

  auto policy = std::make_unique<nimble::ManualEncodingSelectionPolicy<D>>(
      std::vector<std::pair<nimble::EncodingType, float>>{
          {nimble::EncodingType::ALP, 1.0},
          {nimble::EncodingType::Trivial, 1.0},
          {nimble::EncodingType::FixedBitWidth, 1.0},
      },
      std::nullopt,
      std::nullopt);

  const auto serialized = nimble::EncodingFactory::encode<D>(
      std::move(policy),
      std::span<const D>{values.data(), values.size()},
      *this->buffer_,
      options);

  std::vector<std::tuple<nimble::EncodingType, nimble::DataType, std::string>>
      actual;
  nimble::tools::traverseEncodings(
      serialized,
      [&](auto encodingType,
          auto dataType,
          auto /* level */,
          auto /* index */,
          auto nestedEncodingName,
          std::unordered_map<
              nimble::tools::EncodingPropertyType,
              nimble::tools::EncodingProperty> /* properties */) {
        actual.emplace_back(encodingType, dataType, nestedEncodingName);
        return true;
      });

  const std::vector<
      std::tuple<nimble::EncodingType, nimble::DataType, std::string>>
      expected{
          {nimble::EncodingType::ALP, nimble::TypeTraits<D>::dataType, ""},
          {nimble::EncodingType::FixedBitWidth,
           nimble::DataType::Uint64,
           "EncodedValues"},
      };
  EXPECT_EQ(actual, expected);

  std::vector<velox::BufferPtr> stringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buf = stringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buf->template asMutable<void>();
  };
  auto encoding = nimble::EncodingFactory(options).create(
      *this->pool_, serialized, stringBufferFactory);

  nimble::Vector<D> result(this->pool_.get(), values.size());
  encoding->materialize(values.size(), result.data());

  for (uint32_t i = 0; i < values.size(); ++i) {
    SCOPED_TRACE(fmt::format("i={}", i));
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]));
  }
}

TYPED_TEST(ALPEncodingTest, dictionaryAlphabetUsesNestedAlpWhenEnabled) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = false,
      .fixedBitWidthUseExactBits = true,
      .allowNestedAlpSelection = true};

  nimble::Vector<D> values{this->pool_.get()};
  for (auto i = 0; i < 256; ++i) {
    values.push_back(static_cast<D>((i % 17) - 8) / static_cast<D>(4));
  }

  auto policy = std::make_unique<nimble::ManualEncodingSelectionPolicy<D>>(
      std::vector<std::pair<nimble::EncodingType, float>>{
          {nimble::EncodingType::Dictionary, 1.0},
      },
      std::nullopt,
      std::nullopt);

  const auto serialized = nimble::EncodingFactory::encode<D>(
      std::move(policy),
      std::span<const D>{values.data(), values.size()},
      *this->buffer_,
      options);

  std::vector<std::tuple<nimble::EncodingType, nimble::DataType, std::string>>
      actual;
  nimble::tools::traverseEncodings(
      serialized,
      [&](auto encodingType,
          auto dataType,
          auto /* level */,
          auto /* index */,
          auto nestedEncodingName,
          std::unordered_map<
              nimble::tools::EncodingPropertyType,
              nimble::tools::EncodingProperty> /* properties */) {
        actual.emplace_back(encodingType, dataType, nestedEncodingName);
        return true;
      });

  const std::vector<
      std::tuple<nimble::EncodingType, nimble::DataType, std::string>>
      expected{
          {nimble::EncodingType::Dictionary,
           nimble::TypeTraits<D>::dataType,
           ""},
          {nimble::EncodingType::ALP,
           nimble::TypeTraits<D>::dataType,
           "Alphabet"},
          {nimble::EncodingType::Trivial,
           nimble::DataType::Uint64,
           "EncodedValues"},
          {nimble::EncodingType::Trivial, nimble::DataType::Uint32, "Indices"},
      };
  EXPECT_EQ(actual, expected);

  std::vector<velox::BufferPtr> stringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buf = stringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buf->template asMutable<void>();
  };
  auto encoding = nimble::EncodingFactory(options).create(
      *this->pool_, serialized, stringBufferFactory);

  nimble::Vector<D> result(this->pool_.get(), values.size());
  encoding->materialize(values.size(), result.data());

  for (uint32_t i = 0; i < values.size(); ++i) {
    SCOPED_TRACE(fmt::format("i={}", i));
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]));
  }
}

TYPED_TEST(ALPEncodingTest, rleRunValuesUseNestedAlpWhenEnabled) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = false,
      .fixedBitWidthUseExactBits = true,
      .allowNestedAlpSelection = true};

  nimble::Vector<D> values{this->pool_.get()};
  for (auto i = 0; i < 128; ++i) {
    const auto value = static_cast<D>((i % 17) - 8) / static_cast<D>(4);
    values.push_back(value);
    values.push_back(value);
    values.push_back(value);
  }

  auto policy = std::make_unique<nimble::ManualEncodingSelectionPolicy<D>>(
      std::vector<std::pair<nimble::EncodingType, float>>{
          {nimble::EncodingType::RLE, 1.0},
      },
      std::nullopt,
      std::nullopt);

  const auto serialized = nimble::EncodingFactory::encode<D>(
      std::move(policy),
      std::span<const D>{values.data(), values.size()},
      *this->buffer_,
      options);

  std::vector<std::tuple<nimble::EncodingType, nimble::DataType, std::string>>
      actual;
  nimble::tools::traverseEncodings(
      serialized,
      [&](auto encodingType,
          auto dataType,
          auto /* level */,
          auto /* index */,
          auto nestedEncodingName,
          std::unordered_map<
              nimble::tools::EncodingPropertyType,
              nimble::tools::EncodingProperty> /* properties */) {
        actual.emplace_back(encodingType, dataType, nestedEncodingName);
        return true;
      });

  const std::vector<
      std::tuple<nimble::EncodingType, nimble::DataType, std::string>>
      expected{
          {nimble::EncodingType::RLE, nimble::TypeTraits<D>::dataType, ""},
          {nimble::EncodingType::Trivial, nimble::DataType::Uint32, "Lengths"},
          {nimble::EncodingType::ALP,
           nimble::TypeTraits<D>::dataType,
           "Values"},
          {nimble::EncodingType::Trivial,
           nimble::DataType::Uint64,
           "EncodedValues"},
      };
  EXPECT_EQ(actual, expected);

  std::vector<velox::BufferPtr> stringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buf = stringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buf->template asMutable<void>();
  };
  auto encoding = nimble::EncodingFactory(options).create(
      *this->pool_, serialized, stringBufferFactory);

  nimble::Vector<D> result(this->pool_.get(), values.size());
  encoding->materialize(values.size(), result.data());

  for (uint32_t i = 0; i < values.size(); ++i) {
    SCOPED_TRACE(fmt::format("i={}", i));
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]));
  }
}

TYPED_TEST(ALPEncodingTest, mainlyConstantOtherValuesUseNestedAlpWhenEnabled) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = false,
      .fixedBitWidthUseExactBits = true,
      .allowNestedAlpSelection = true};

  nimble::Vector<D> values{this->pool_.get()};
  for (auto i = 0; i < 256; ++i) {
    values.push_back(D{0});
    if (i % 8 == 0) {
      values.back() = static_cast<D>((i % 17) - 8) / static_cast<D>(4);
    }
  }

  auto policy = std::make_unique<nimble::ManualEncodingSelectionPolicy<D>>(
      std::vector<std::pair<nimble::EncodingType, float>>{
          {nimble::EncodingType::MainlyConstant, 1.0},
      },
      std::nullopt,
      std::nullopt);

  const auto serialized = nimble::EncodingFactory::encode<D>(
      std::move(policy),
      std::span<const D>{values.data(), values.size()},
      *this->buffer_,
      options);

  std::vector<std::tuple<nimble::EncodingType, nimble::DataType, std::string>>
      actual;
  nimble::tools::traverseEncodings(
      serialized,
      [&](auto encodingType,
          auto dataType,
          auto /* level */,
          auto /* index */,
          auto nestedEncodingName,
          std::unordered_map<
              nimble::tools::EncodingPropertyType,
              nimble::tools::EncodingProperty> /* properties */) {
        actual.emplace_back(encodingType, dataType, nestedEncodingName);
        return true;
      });

  const std::vector<
      std::tuple<nimble::EncodingType, nimble::DataType, std::string>>
      expected{
          {nimble::EncodingType::MainlyConstant,
           nimble::TypeTraits<D>::dataType,
           ""},
          {nimble::EncodingType::Trivial, nimble::DataType::Bool, "IsCommon"},
          {nimble::EncodingType::ALP,
           nimble::TypeTraits<D>::dataType,
           "OtherValues"},
          {nimble::EncodingType::Trivial,
           nimble::DataType::Uint64,
           "EncodedValues"},
      };
  EXPECT_EQ(actual, expected);

  std::vector<velox::BufferPtr> stringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buf = stringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buf->template asMutable<void>();
  };
  auto encoding = nimble::EncodingFactory(options).create(
      *this->pool_, serialized, stringBufferFactory);

  nimble::Vector<D> result(this->pool_.get(), values.size());
  encoding->materialize(values.size(), result.data());

  for (uint32_t i = 0; i < values.size(); ++i) {
    SCOPED_TRACE(fmt::format("i={}", i));
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]));
  }
}

TYPED_TEST(ALPEncodingTest, randomizedFixedLayoutMaterializeAndSkip) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = false, .fixedBitWidthUseExactBits = true};

  struct Scenario {
    std::string name;
    nimble::EncodingLayout layout;
    uint32_t rowCount;
    uint32_t uniqueCount;
    std::vector<std::tuple<nimble::EncodingType, nimble::DataType, std::string>>
        expectedLayout;
  };

  const std::vector<Scenario> scenarios{
      {
          "alp",
          alpWithFixedBitWidthPayloadLayout(),
          4096,
          257,
          {
              {nimble::EncodingType::ALP, nimble::TypeTraits<D>::dataType, ""},
              {nimble::EncodingType::FixedBitWidth,
               nimble::DataType::Uint64,
               "EncodedValues"},
          },
      },
      {
          "dictionary",
          dictionaryWithAlpAlphabetLayout(),
          4096,
          97,
          {
              {nimble::EncodingType::Dictionary,
               nimble::TypeTraits<D>::dataType,
               ""},
              {nimble::EncodingType::ALP,
               nimble::TypeTraits<D>::dataType,
               "Alphabet"},
              {nimble::EncodingType::FixedBitWidth,
               nimble::DataType::Uint64,
               "EncodedValues"},
              {nimble::EncodingType::FixedBitWidth,
               nimble::DataType::Uint32,
               "Indices"},
          },
      },
  };

  for (uint32_t scenarioIndex = 0; scenarioIndex < scenarios.size();
       ++scenarioIndex) {
    const auto& scenario = scenarios[scenarioIndex];
    SCOPED_TRACE(
        fmt::format(
            "scenario={} type={} useVarint={}",
            scenario.name,
            nimble::TypeTraits<D>::dataType,
            options.useVarintRowCount));

    nimble::Buffer buffer{*this->pool_};
    auto values = makeRandomDecimalValues<D>(
        this->pool_.get(),
        scenario.rowCount,
        scenario.uniqueCount,
        0xC0FFEE + scenarioIndex);
    const auto serialized =
        encodeWithLayout<D>(buffer, values, scenario.layout, options);
    expectEncodingLayout(serialized, scenario.expectedLayout);

    std::vector<velox::BufferPtr> stringBuffers;
    auto encoding =
        createEncoding(this->pool_.get(), serialized, options, stringBuffers);
    expectInterleavedMaterializeAndSkip(
        *encoding, values, this->pool_.get(), 0xA11CE + scenarioIndex);
  }
}

TYPED_TEST(ALPEncodingTest, skipAndMaterialize) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  auto values = this->template toVector<D>({10, 20, 30, 40, 50, 60});

  std::vector<velox::BufferPtr> stringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buf = stringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buf->template asMutable<void>();
  };

  auto encoding = nimble::test::Encoder<nimble::ALPEncoding<D>>::createEncoding(
      *this->buffer_,
      values,
      stringBufferFactory,
      nimble::CompressionType::Uncompressed,
      options);

  encoding->skip(2);

  nimble::Vector<D> result(this->pool_.get(), 3);
  encoding->materialize(3, result.data());

  EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[0], D{30}));
  EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[1], D{40}));
  EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[2], D{50}));
}

TYPED_TEST(ALPEncodingTest, resetAndRematerialize) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  auto values = this->template toVector<D>({7, 8, 9});

  std::vector<velox::BufferPtr> stringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buf = stringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buf->template asMutable<void>();
  };

  auto encoding = nimble::test::Encoder<nimble::ALPEncoding<D>>::createEncoding(
      *this->buffer_,
      values,
      stringBufferFactory,
      nimble::CompressionType::Uncompressed,
      options);

  nimble::Vector<D> first(this->pool_.get(), values.size());
  encoding->materialize(values.size(), first.data());

  encoding->reset();

  nimble::Vector<D> second(this->pool_.get(), values.size());
  encoding->materialize(values.size(), second.data());

  for (uint32_t i = 0; i < values.size(); ++i) {
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(first[i], second[i]));
  }
}

TYPED_TEST(ALPEncodingTest, emptyDataRejected) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  nimble::Vector<D> empty{this->pool_.get()};

  std::vector<velox::BufferPtr> stringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buf = stringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buf->template asMutable<void>();
  };

  NIMBLE_ASSERT_USER_THROW(
      nimble::test::Encoder<nimble::ALPEncoding<D>>::createEncoding(
          *this->buffer_,
          empty,
          stringBufferFactory,
          nimble::CompressionType::Uncompressed,
          options),
      "ALP encoding cannot encode empty data.");
}
