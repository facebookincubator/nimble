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
#define NIMBLE_ENCODING_SELECTION_DEBUG

#include <gtest/gtest.h>
#include <limits>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "dwio/nimble/tools/EncodingUtilities.h"

using namespace facebook;

namespace {
constexpr uint32_t commonPrefixSize = 6;

template <typename T, bool FixedByteWidth = false>
std::unique_ptr<nimble::ManualEncodingSelectionPolicy<T, FixedByteWidth>>
getRootManualSelectionPolicy() {
  return std::make_unique<
      nimble::ManualEncodingSelectionPolicy<T, FixedByteWidth>>(
      std::vector<std::pair<nimble::EncodingType, float>>{
          {nimble::EncodingType::Constant, 1.0},
          {nimble::EncodingType::Trivial, 0.7},
          {nimble::EncodingType::FixedBitWidth, 1.05},
          {nimble::EncodingType::MainlyConstant, 1.05},
          {nimble::EncodingType::SparseBool, 1.05},
          {nimble::EncodingType::Dictionary, 1.05},
          {nimble::EncodingType::RLE, 1.05},
          {nimble::EncodingType::Varint, 1.1},
      },
      nimble::CompressionOptions{
          .compressionAcceptRatio = 0.9,
          .internalCompressionLevel = 9,
          .internalDecompressionLevel = 2,
          .useVariableBitWidthCompressor = false,
      },
      std::nullopt);
}
} // namespace

struct EncodingDetails {
  nimble::EncodingType encodingType;
  nimble::DataType dataType;
  uint32_t level;
  std::string nestedEncodingName;
};

void verifyEncodingTree(
    std::string_view stream,
    std::vector<EncodingDetails> expected) {
  ASSERT_GT(expected.size(), 0);
  std::vector<EncodingDetails> actual;
  nimble::tools::traverseEncodings(
      stream,
      [&](auto encodingType,
          auto dataType,
          auto level,
          auto /* index */,
          auto nestedEncodingName,
          std::unordered_map<
              nimble::tools::EncodingPropertyType,
              nimble::tools::EncodingProperty> /* properties */) {
        actual.push_back(
            {.encodingType = encodingType,
             .dataType = dataType,
             .level = level,
             .nestedEncodingName = nestedEncodingName});

        return true;
      });

  ASSERT_EQ(expected.size(), actual.size());
  for (auto i = 0; i < expected.size(); ++i) {
    LOG(INFO) << "Expected: " << expected[i].encodingType << "<"
              << expected[i].dataType << ">[" << expected[i].nestedEncodingName
              << ":" << expected[i].level << "]";
    LOG(INFO) << "Actual: " << actual[i].encodingType << "<"
              << actual[i].dataType << ">[" << actual[i].nestedEncodingName
              << ":" << actual[i].level << "]";
    EXPECT_EQ(expected[i].encodingType, actual[i].encodingType);
    EXPECT_EQ(expected[i].dataType, actual[i].dataType);
    EXPECT_EQ(expected[i].level, actual[i].level);
    EXPECT_EQ(expected[i].nestedEncodingName, actual[i].nestedEncodingName);
  }
}

template <typename T>
typename nimble::TypeTraits<T>::physicalType asPhysicalType(T value) {
  return *reinterpret_cast<typename nimble::TypeTraits<T>::physicalType*>(
      &value);
}

template <typename T, bool FixedByteWidth>
void verifySizeEstimate(
    std::span<const T> values,
    nimble::Buffer& buffer,
    size_t expectedEstimatedSize,
    size_t expectedActualSize,
    nimble::EncodingType encodingTypeForEstimation,
    const std::vector<std::pair<nimble::EncodingType, float>>& readFactors) {
  // Create a policy that uses no compression
  auto policy = std::make_unique<
      nimble::ManualEncodingSelectionPolicy<T, FixedByteWidth>>(
      readFactors,
      nimble::CompressionOptions{
          .compressionAcceptRatio = 0.0,
      },
      std::nullopt);

  // Check the serialized uncompressed size
  auto serialized =
      nimble::EncodingFactory::encode<T>(std::move(policy), values, buffer);
  auto actualSize = serialized.size();
  EXPECT_EQ(actualSize, expectedActualSize);

  // Check the estimated size
  auto estimatedSize =
      nimble::detail::EncodingSizeEstimation<T, FixedByteWidth>::estimateSize(
          encodingTypeForEstimation,
          values.size(),
          nimble::Statistics<T>::create(values));
  EXPECT_EQ(estimatedSize, expectedEstimatedSize);
}

template <typename T>
void test(std::span<const T> values, std::vector<EncodingDetails> expected) {
  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  auto policy = getRootManualSelectionPolicy<T>();
  nimble::Buffer buffer{*pool};

  auto serialized =
      nimble::EncodingFactory::encode<T>(std::move(policy), values, buffer);

  // test getRawDataSize
  auto size =
      facebook::nimble::test::TestUtils::getRawDataSize(*pool, serialized);
  auto expectedSize = values.size_bytes();
  ASSERT_EQ(size, expectedSize);

  LOG(INFO) << "Final size: " << serialized.size();

  ASSERT_GT(expected.size(), 0);
  std::vector<EncodingDetails> actual;
  nimble::tools::traverseEncodings(
      serialized,
      [&](auto encodingType,
          auto dataType,
          auto level,
          auto /* index */,
          auto nestedEncodingName,
          std::unordered_map<
              nimble::tools::EncodingPropertyType,
              nimble::tools::EncodingProperty> /* properties */) {
        actual.push_back(
            {.encodingType = encodingType,
             .dataType = dataType,
             .level = level,
             .nestedEncodingName = nestedEncodingName});

        return true;
      });

  ASSERT_EQ(expected.size(), actual.size());
  for (auto i = 0; i < expected.size(); ++i) {
    LOG(INFO) << "Expected: " << expected[i].encodingType << "<"
              << expected[i].dataType << ">[" << expected[i].nestedEncodingName
              << ":" << expected[i].level << "]";
    LOG(INFO) << "Actual: " << actual[i].encodingType << "<"
              << actual[i].dataType << ">[" << actual[i].nestedEncodingName
              << ":" << actual[i].level << "]";
    EXPECT_EQ(expected[i].encodingType, actual[i].encodingType);
    EXPECT_EQ(expected[i].dataType, actual[i].dataType);
    EXPECT_EQ(expected[i].level, actual[i].level);
    EXPECT_EQ(expected[i].nestedEncodingName, actual[i].nestedEncodingName);
  }

  nimble::Vector<T> materialized{pool.get()};
  auto encoding = nimble::EncodingFactory::decode(*pool, serialized);
  nimble::Vector<T> result{pool.get()};
  result.resize(values.size());
  encoding->materialize(values.size(), result.data());

  for (auto i = 0; i < values.size(); ++i) {
    ASSERT_EQ(asPhysicalType(values[i]), asPhysicalType(result[i])) << i;
  }
}

using NumericTypes = ::testing::Types<
    int8_t,
    uint8_t,
    int16_t,
    uint16_t,
    int32_t,
    uint32_t,
    int64_t,
    uint64_t,
    float,
    double>;

TYPED_TEST_CASE(EncodingSelectionNumericTests, NumericTypes);

template <typename C>
class EncodingSelectionNumericTests : public ::testing::Test {};

TYPED_TEST(EncodingSelectionNumericTests, SelectConst) {
  using T = TypeParam;

  for (const T value :
       {std::numeric_limits<T>::min(),
        static_cast<T>(0),
        std::numeric_limits<T>::max()}) {
    std::vector<T> values;
    values.resize(1000);
    for (auto i = 0; i < values.size(); ++i) {
      values[i] = value;
    }

    test<T>(
        values,
        {
            {.encodingType = nimble::EncodingType::Constant,
             .dataType = nimble::TypeTraits<T>::dataType,
             .level = 0,
             .nestedEncodingName = ""},
        });
  }
}

TYPED_TEST(EncodingSelectionNumericTests, SelectMainlyConst) {
  using T = TypeParam;

  for (const T value : {
           std::numeric_limits<T>::min(),
           static_cast<T>(0),
           std::numeric_limits<T>::max(),
       }) {
    LOG(INFO) << "Verifying type " << folly::demangle(typeid(T))
              << " with data: " << value;

    std::vector<T> values;
    values.resize(1000);
    for (auto i = 0; i < values.size(); ++i) {
      values[i] = value;
    }

    for (auto i = 0; i < values.size(); i += 20) {
      if constexpr (nimble::isFloatingPointType<T>()) {
        values[i] = i;
      } else {
        values[i] = i % std::numeric_limits<T>::max();
      }
    }

    if constexpr (nimble::isFloatingPointType<T>() || sizeof(T) < 4) {
      // Floating point types and small types use Trivial encoding to encode
      // the exception values.
      test<T>(
          values,
          {
              {.encodingType = nimble::EncodingType::MainlyConstant,
               .dataType = nimble::TypeTraits<T>::dataType,
               .level = 0,
               .nestedEncodingName = ""},
              {.encodingType = nimble::EncodingType::SparseBool,
               .dataType = nimble::DataType::Bool,
               .level = 1,
               .nestedEncodingName = "IsCommon"},
              {.encodingType = nimble::EncodingType::FixedBitWidth,
               .dataType = nimble::DataType::Uint32,
               .level = 2,
               .nestedEncodingName = "Indices"},
              {.encodingType = nimble::EncodingType::Trivial,
               .dataType = nimble::TypeTraits<
                   typename nimble::EncodingPhysicalType<T>::type>::dataType,
               .level = 1,
               .nestedEncodingName = "OtherValues"},
          });
    } else {
      // All other numeric types use FixedBitWidth encoding to encode the
      // exception values.
      test<T>(
          values,
          {
              {.encodingType = nimble::EncodingType::MainlyConstant,
               .dataType = nimble::TypeTraits<T>::dataType,
               .level = 0,
               .nestedEncodingName = ""},
              {.encodingType = nimble::EncodingType::SparseBool,
               .dataType = nimble::DataType::Bool,
               .level = 1,
               .nestedEncodingName = "IsCommon"},
              {.encodingType = nimble::EncodingType::FixedBitWidth,
               .dataType = nimble::DataType::Uint32,
               .level = 2,
               .nestedEncodingName = "Indices"},
              {.encodingType = nimble::EncodingType::FixedBitWidth,
               .dataType = nimble::TypeTraits<
                   typename nimble::EncodingPhysicalType<T>::type>::dataType,
               .level = 1,
               .nestedEncodingName = "OtherValues"},
          });
    }
  }
}

TYPED_TEST(EncodingSelectionNumericTests, SelectTrivial) {
  using T = TypeParam;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  std::vector<T> values;
  values.resize(10000);
  for (auto i = 0; i < values.size(); ++i) {
    auto random = folly::Random::rand64(rng);
    values[i] = *reinterpret_cast<const T*>(&random);
  }

  test<T>(
      values,
      {
          {.encodingType = nimble::EncodingType::Trivial,
           .dataType = nimble::TypeTraits<T>::dataType,
           .level = 0,
           .nestedEncodingName = ""},
      });
}

TYPED_TEST(EncodingSelectionNumericTests, SelectFixedBitWidth) {
  using T = TypeParam;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (const auto bitWidth : {3, 4, 5}) {
    LOG(INFO) << "Testing with bit width: " << bitWidth;
    uint32_t maxValue = 1 << bitWidth;

    std::vector<T> values;
    values.resize(10000);
    for (auto i = 0; i < values.size(); ++i) {
      auto random = folly::Random::rand64(rng) % maxValue;
      values[i] = *reinterpret_cast<const T*>(&random);
    }

    test<T>(
        values,
        {
            {.encodingType = nimble::EncodingType::FixedBitWidth,
             .dataType = nimble::TypeTraits<T>::dataType,
             .level = 0,
             .nestedEncodingName = ""},
        });
  }
}

TYPED_TEST(EncodingSelectionNumericTests, SelectDictionary) {
  using T = TypeParam;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  std::array<T, 3> uniqueValues{
      std::numeric_limits<T>::min(), 0, std::numeric_limits<T>::max()};

  std::vector<T> values;
  values.resize(10000);
  for (auto i = 0; i < values.size(); ++i) {
    values[i] = uniqueValues[folly::Random::rand32(rng) % uniqueValues.size()];
  }

  test<T>(
      values,
      {
          {.encodingType = nimble::EncodingType::Dictionary,
           .dataType = nimble::TypeTraits<T>::dataType,
           .level = 0,
           .nestedEncodingName = ""},
          {.encodingType = nimble::EncodingType::Trivial,
           .dataType = nimble::TypeTraits<
               typename nimble::EncodingPhysicalType<T>::type>::dataType,
           .level = 1,
           .nestedEncodingName = "Alphabet"},
          {.encodingType = nimble::EncodingType::FixedBitWidth,
           .dataType = nimble::DataType::Uint32,
           .level = 1,
           .nestedEncodingName = "Indices"},
      });
}

TYPED_TEST(EncodingSelectionNumericTests, SelectRunLength) {
  using T = TypeParam;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  std::array<T, 3> uniqueValues{
      std::numeric_limits<T>::min(), 0, std::numeric_limits<T>::max()};
  std::vector<uint32_t> runLengths;
  runLengths.resize(20);
  uint32_t valueCount = 0;
  for (auto i = 0; i < runLengths.size(); ++i) {
    runLengths[i] = (folly::Random::rand32(rng) % 700) + 20;
    valueCount += runLengths[i];
  }

  std::vector<T> values;
  values.reserve(valueCount);
  auto index = 0;
  for (const auto length : runLengths) {
    for (auto i = 0; i < length; ++i) {
      values.push_back(uniqueValues[index % uniqueValues.size()]);
    }
    ++index;
  }

  if constexpr (
      nimble::isFloatingPointType<T>() || std::is_same_v<int32_t, T> ||
      sizeof(T) > 4) {
    // Floating point types and big types prefer storing the run values as
    // dictionary
    test<T>(
        values,
        {
            {.encodingType = nimble::EncodingType::RLE,
             .dataType = nimble::TypeTraits<T>::dataType,
             .level = 0,
             .nestedEncodingName = ""},
            {.encodingType = nimble::EncodingType::FixedBitWidth,
             .dataType = nimble::DataType::Uint32,
             .level = 1,
             .nestedEncodingName = "Lengths"},
            {.encodingType = nimble::EncodingType::Dictionary,
             .dataType = nimble::TypeTraits<
                 typename nimble::EncodingPhysicalType<T>::type>::dataType,
             .level = 1,
             .nestedEncodingName = "Values"},
            {.encodingType = nimble::EncodingType::Trivial,
             .dataType = nimble::TypeTraits<
                 typename nimble::EncodingPhysicalType<T>::type>::dataType,
             .level = 2,
             .nestedEncodingName = "Alphabet"},
            {.encodingType = nimble::EncodingType::FixedBitWidth,
             .dataType = nimble::DataType::Uint32,
             .level = 2,
             .nestedEncodingName = "Indices"},
        });
  } else {
    test<T>(
        values,
        {
            {.encodingType = nimble::EncodingType::RLE,
             .dataType = nimble::TypeTraits<T>::dataType,
             .level = 0,
             .nestedEncodingName = ""},
            {.encodingType = nimble::EncodingType::FixedBitWidth,
             .dataType = nimble::DataType::Uint32,
             .level = 1,
             .nestedEncodingName = "Lengths"},
            {.encodingType = nimble::EncodingType::Trivial,
             .dataType = nimble::TypeTraits<
                 typename nimble::EncodingPhysicalType<T>::type>::dataType,
             .level = 1,
             .nestedEncodingName = "Values"},
        });
  }
}

TYPED_TEST(EncodingSelectionNumericTests, SelectVarint) {
  using T = TypeParam;

  if constexpr (sizeof(T) < 4) {
    return;
  }

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  std::vector<T> values;
  values.resize(700);
  for (auto i = 0; i < values.size(); ++i) {
    auto value = (std::numeric_limits<T>::max() / 2);
    auto random =
        *reinterpret_cast<const typename nimble::TypeTraits<T>::physicalType*>(
            &value) +
        (folly::Random::rand32(rng) % 128);
    values[i] = *reinterpret_cast<const T*>(&random);
  }

  for (auto i = 0; i < values.size(); i += 30) {
    values[i] = std::numeric_limits<T>::max() - folly::Random::rand32(128, rng);
  }

  test<T>(
      values,
      {
          {.encodingType = nimble::EncodingType::Varint,
           .dataType = nimble::TypeTraits<T>::dataType,
           .level = 0,
           .nestedEncodingName = ""},
      });
}

TEST(EncodingSelectionBoolTests, SelectConst) {
  using T = bool;

  for (const T value : {true, false}) {
    std::array<T, 1000> values;
    for (auto i = 0; i < values.size(); ++i) {
      values[i] = value;
    }

    test<T>(
        values,
        {
            {.encodingType = nimble::EncodingType::Constant,
             .dataType = nimble::TypeTraits<T>::dataType,
             .level = 0,
             .nestedEncodingName = ""},
        });
  }
}

TEST(EncodingSelectionBoolTests, SelectSparseBool) {
  using T = bool;

  for (const T value : {false, true}) {
    std::array<T, 10000> values;
    for (auto i = 0; i < values.size(); ++i) {
      values[i] = value;
    }

    for (auto i = 0; i < values.size(); i += 200) {
      values[i] = !value;
    }

    test<T>(
        values,
        {
            {.encodingType = nimble::EncodingType::SparseBool,
             .dataType = nimble::TypeTraits<T>::dataType,
             .level = 0,
             .nestedEncodingName = ""},
            {.encodingType = nimble::EncodingType::FixedBitWidth,
             .dataType = nimble::DataType::Uint32,
             .level = 1,
             .nestedEncodingName = "Indices"},
        });
  }
}

TEST(EncodingSelectionBoolTests, SelectTrivial) {
  using T = bool;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  std::vector<std::unique_ptr<nimble::EncodingSelectionPolicy<T>>> policies;
  policies.push_back(getRootManualSelectionPolicy<T>());
  policies.push_back(
      std::make_unique<nimble::LearnedEncodingSelectionPolicy<T>>());

  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*pool};

  // Size estimate:
  // 1. Encoding header
  // 2. 1 byte for the compression flag
  // 3. Number of bytes to hold one bit per value rounded up to a full byte
  // 4. 7 extra bytes for the safe vectorized extraction
  size_t encodingOverhead = commonPrefixSize + 1 + 7;

  for (auto& policy : policies) {
    std::array<T, 10003> values{};
    for (auto i = 0; i < values.size(); ++i) {
      values[i] = folly::Random::oneIn(2, rng);
    }

    const uint32_t expectedEncodedDataSize = std::ceil(values.size() / 8.0);
    verifySizeEstimate<T, false>(
        values,
        buffer,
        expectedEncodedDataSize + encodingOverhead,
        expectedEncodedDataSize + encodingOverhead,
        nimble::EncodingType::Trivial,
        {{nimble::EncodingType::Trivial, 1.0}});

    auto serialized =
        nimble::EncodingFactory::encode<T>(std::move(policy), values, buffer);

    // test getRawDataSize
    auto size =
        facebook::nimble::test::TestUtils::getRawDataSize(*pool, serialized);
    auto expectedSize = values.size() * sizeof(T);
    ASSERT_EQ(size, expectedSize);

    LOG(INFO) << "Final size: " << serialized.size();

    verifyEncodingTree(
        serialized,
        {
            {.encodingType = nimble::EncodingType::Trivial,
             .dataType = nimble::TypeTraits<T>::dataType,
             .level = 0,
             .nestedEncodingName = ""},
        });
  }
}

TEST(EncodingSelectionBoolTests, SelectRunLength) {
  using T = bool;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*pool};

  std::vector<uint32_t> runLengths;
  runLengths.resize(20);
  uint32_t valueCount = 0;
  for (auto i = 0; i < runLengths.size(); ++i) {
    runLengths[i] = (folly::Random::rand32(rng) % 700) + 20;
    valueCount += runLengths[i];
  }

  nimble::Vector<bool> values{pool.get()};
  values.reserve(valueCount);
  auto index = 0;
  for (const auto length : runLengths) {
    for (auto i = 0; i < length; ++i) {
      values.push_back(index % 2 == 0);
    }
    ++index;
  }

  auto policy = getRootManualSelectionPolicy<T>();
  auto serialized =
      nimble::EncodingFactory::encode<T>(std::move(policy), values, buffer);

  // test getRawDataSize
  auto size =
      facebook::nimble::test::TestUtils::getRawDataSize(*pool, serialized);
  auto expectedSize = values.size() * sizeof(T);
  ASSERT_EQ(size, expectedSize);

  LOG(INFO) << "Final size: " << serialized.size();

  verifyEncodingTree(
      serialized,
      {
          {.encodingType = nimble::EncodingType::RLE,
           .dataType = nimble::TypeTraits<T>::dataType,
           .level = 0,
           .nestedEncodingName = ""},
          {.encodingType = nimble::EncodingType::FixedBitWidth,
           .dataType = nimble::DataType::Uint32,
           .level = 1,
           .nestedEncodingName = "Lengths"},
      });
}

TEST(EncodingSelectionStringTests, SelectConst) {
  using T = std::string_view;

  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*pool};

  for (const T value :
       {std::string(""), std::string("aaaaa"), std::string(5000, '\0')}) {
    LOG(INFO) << "Testing string with value: " << value;
    auto policy = getRootManualSelectionPolicy<T>();

    std::vector<T> values;
    values.resize(1000);
    for (auto i = 0; i < values.size(); ++i) {
      values[i] = value;
    }

    auto serialized =
        nimble::EncodingFactory::encode<T>(std::move(policy), values, buffer);

    // test getRawDataSize
    auto size =
        facebook::nimble::test::TestUtils::getRawDataSize(*pool, serialized);
    auto expectedSize = value.size() * values.size();
    ASSERT_EQ(size, expectedSize);

    LOG(INFO) << "Final size: " << serialized.size();

    verifyEncodingTree(
        serialized,
        {
            {.encodingType = nimble::EncodingType::Constant,
             .dataType = nimble::TypeTraits<T>::dataType,
             .level = 0,
             .nestedEncodingName = ""},
        });
  }
}

TEST(EncodingSelectionStringTests, SelectMainlyConst) {
  using T = std::string_view;

  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*pool};

  for (const T value : {
           std::string("aaaaa"),
           std::string(5000, '\0'),
       }) {
    std::vector<T> values;
    auto expectedSize = 0;

    auto resize = 1000;
    values.resize(resize);
    for (auto i = 0; i < values.size(); ++i) {
      values[i] = value;
    }
    expectedSize += resize * value.size();

    std::vector<std::string> uncommonValues;
    for (auto i = 0; i < values.size() / 20; ++i) {
      uncommonValues.emplace_back(i, 'b');
    }

    for (auto i = 0; i < uncommonValues.size(); ++i) {
      std::string_view val = uncommonValues[i];
      values[i * 20] = val;
      expectedSize += val.size() - value.size();
    }

    auto policy = getRootManualSelectionPolicy<T>();
    auto serialized =
        nimble::EncodingFactory::encode<T>(std::move(policy), values, buffer);

    // test getRawDataSize
    auto size =
        facebook::nimble::test::TestUtils::getRawDataSize(*pool, serialized);
    ASSERT_EQ(size, expectedSize);

    LOG(INFO) << "Final size: " << serialized.size();

    verifyEncodingTree(
        serialized,
        {
            {.encodingType = nimble::EncodingType::MainlyConstant,
             .dataType = nimble::TypeTraits<T>::dataType,
             .level = 0,
             .nestedEncodingName = ""},
            {.encodingType = nimble::EncodingType::SparseBool,
             .dataType = nimble::DataType::Bool,
             .level = 1,
             .nestedEncodingName = "IsCommon"},
            {.encodingType = nimble::EncodingType::FixedBitWidth,
             .dataType = nimble::DataType::Uint32,
             .level = 2,
             .nestedEncodingName = "Indices"},
            {.encodingType = nimble::EncodingType::Trivial,
             .dataType = nimble::TypeTraits<
                 typename nimble::EncodingPhysicalType<T>::type>::dataType,
             .level = 1,
             .nestedEncodingName = "OtherValues"},
            {.encodingType = nimble::EncodingType::FixedBitWidth,
             .dataType = nimble::DataType::Uint32,
             .level = 2,
             .nestedEncodingName = "Lengths"},
        });
  }
}

TEST(EncodingSelectionStringTests, SelectTrivial) {
  using T = std::string_view;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*pool};

  auto policy = getRootManualSelectionPolicy<T>();

  std::vector<std::string> cache;
  cache.resize(10000);
  for (auto i = 0; i < cache.size(); ++i) {
    std::string value(folly::Random::rand32(128, rng), ' ');
    for (auto j = 0; j < value.size(); ++j) {
      value[j] = static_cast<char>(folly::Random::rand32(256, rng));
    }
    cache[i] = std::move(value);
  }

  std::vector<T> values;
  auto expectedSize = 0;
  values.resize(cache.size());
  for (auto i = 0; i < cache.size(); ++i) {
    values[i] = cache[i];
    expectedSize += cache[i].size();
  }

  auto serialized =
      nimble::EncodingFactory::encode<T>(std::move(policy), values, buffer);

  // test getRawDataSize
  auto size =
      facebook::nimble::test::TestUtils::getRawDataSize(*pool, serialized);
  ASSERT_EQ(size, expectedSize);

  LOG(INFO) << "Final size: " << serialized.size();

  verifyEncodingTree(
      serialized,
      {
          {.encodingType = nimble::EncodingType::Trivial,
           .dataType = nimble::TypeTraits<T>::dataType,
           .level = 0,
           .nestedEncodingName = ""},
          {.encodingType = nimble::EncodingType::FixedBitWidth,
           .dataType = nimble::DataType::Uint32,
           .level = 1,
           .nestedEncodingName = "Lengths"},
      });
}

TEST(EncodingSelectionStringTests, SelectDictionary) {
  using T = std::string_view;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  std::vector<std::string> uniqueValues{"", "abcdef", std::string(5000, '\0')};

  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*pool};

  auto policy = getRootManualSelectionPolicy<T>();

  std::vector<T> values;
  auto expectedSize = 0;
  values.resize(10000);
  for (auto i = 0; i < values.size(); ++i) {
    T val = uniqueValues[folly::Random::rand32(rng) % uniqueValues.size()];
    values[i] = val;
    expectedSize += val.size();
  }

  auto serialized =
      nimble::EncodingFactory::encode<T>(std::move(policy), values, buffer);

  // test getRawDataSize
  auto size =
      facebook::nimble::test::TestUtils::getRawDataSize(*pool, serialized);
  ASSERT_EQ(size, expectedSize);

  LOG(INFO) << "Final size: " << serialized.size();

  verifyEncodingTree(
      serialized,
      {
          {.encodingType = nimble::EncodingType::Dictionary,
           .dataType = nimble::TypeTraits<T>::dataType,
           .level = 0,
           .nestedEncodingName = ""},
          {.encodingType = nimble::EncodingType::Trivial,
           .dataType = nimble::TypeTraits<
               typename nimble::EncodingPhysicalType<T>::type>::dataType,
           .level = 1,
           .nestedEncodingName = "Alphabet"},
          {.encodingType = nimble::EncodingType::Varint,
           .dataType = nimble::DataType::Uint32,
           .level = 2,
           .nestedEncodingName = "Lengths"},
          {.encodingType = nimble::EncodingType::FixedBitWidth,
           .dataType = nimble::DataType::Uint32,
           .level = 1,
           .nestedEncodingName = "Indices"},
      });
}

TEST(EncodingSelectionStringTests, SelectRunLength) {
  using T = std::string_view;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*pool};

  std::vector<uint32_t> runLengths;
  runLengths.resize(20);
  uint32_t valueCount = 0;
  for (auto i = 0; i < runLengths.size(); ++i) {
    runLengths[i] = (folly::Random::rand32(rng) % 700) + 20;
    valueCount += runLengths[i];
  }

  std::vector<T> values;
  auto expectedSize = 0;
  values.reserve(valueCount);
  auto index = 0;
  for (const auto length : runLengths) {
    for (auto i = 0; i < length; ++i) {
      std::string_view val =
          ((index % 2 == 0) ? "abcdefghijklmnopqrstuvwxyz" : "1234567890");
      values.emplace_back(val);
      expectedSize += val.size();
    }
    ++index;
  }

  auto policy = getRootManualSelectionPolicy<T>();
  auto serialized =
      nimble::EncodingFactory::encode<T>(std::move(policy), values, buffer);

  // test getRawDataSize
  auto size =
      facebook::nimble::test::TestUtils::getRawDataSize(*pool, serialized);
  ASSERT_EQ(size, expectedSize);

  LOG(INFO) << "Final size: " << serialized.size();

  verifyEncodingTree(
      serialized,
      {
          {.encodingType = nimble::EncodingType::RLE,
           .dataType = nimble::TypeTraits<T>::dataType,
           .level = 0,
           .nestedEncodingName = ""},
          {.encodingType = nimble::EncodingType::FixedBitWidth,
           .dataType = nimble::DataType::Uint32,
           .level = 1,
           .nestedEncodingName = "Lengths"},
          {.encodingType = nimble::EncodingType::Dictionary,
           .dataType = nimble::TypeTraits<T>::dataType,
           .level = 1,
           .nestedEncodingName = "Values"},
          {.encodingType = nimble::EncodingType::Trivial,
           .dataType = nimble::TypeTraits<T>::dataType,
           .level = 2,
           .nestedEncodingName = "Alphabet"},
          {.encodingType = nimble::EncodingType::Varint,
           .dataType = nimble::DataType::Uint32,
           .level = 3,
           .nestedEncodingName = "Lengths"},
          {.encodingType = nimble::EncodingType::FixedBitWidth,
           .dataType = nimble::DataType::Uint32,
           .level = 2,
           .nestedEncodingName = "Indices"},
      });
}

TEST(EncodingSelectionTests, TestNullable) {
  using T = std::string_view;
  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*pool};
  auto policy = getRootManualSelectionPolicy<T>();
  std::vector<T> data{"abcd", "efg", "hijk", "lmno"};
  std::array<bool, 10> nulls{
      true, false, true, true, false, false, true, true, true, false};

  auto serialized = nimble::EncodingFactory::encodeNullable<T>(
      std::move(policy), data, nulls, buffer);

  // test getRawDataSize
  auto size =
      facebook::nimble::test::TestUtils::getRawDataSize(*pool, serialized);
  auto expectedSize = 15 + 10; // 15 bytes for string data, 10 bytes for nulls
  ASSERT_EQ(size, expectedSize);

  LOG(INFO) << "Final size: " << serialized.size();
}
