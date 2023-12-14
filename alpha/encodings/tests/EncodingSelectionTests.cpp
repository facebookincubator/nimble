// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#define ALPHA_ENCODING_SELECTION_DEBUG

#include <gtest/gtest.h>
#include <limits>
#include "dwio/alpha/common/Buffer.h"
#include "dwio/alpha/encodings/EncodingFactoryNew.h"
#include "dwio/alpha/encodings/EncodingSelectionPolicy.h"
#include "dwio/alpha/encodings/NullableEncoding.h"
#include "dwio/alpha/tools/EncodingUtilities.h"

using namespace ::facebook;

namespace {
template <typename T, bool FixedByteWidth = false>
std::unique_ptr<alpha::ManualEncodingSelectionPolicy<T, FixedByteWidth>>
getRootManualSelectionPolicy() {
  return std::make_unique<
      alpha::ManualEncodingSelectionPolicy<T, FixedByteWidth>>(
      std::vector<std::pair<alpha::EncodingType, float>>{
          {alpha::EncodingType::Constant, 1.0},
          {alpha::EncodingType::Trivial, 0.7},
          {alpha::EncodingType::FixedBitWidth, 1.05},
          {alpha::EncodingType::MainlyConstant, 1.05},
          {alpha::EncodingType::SparseBool, 1.05},
          {alpha::EncodingType::Dictionary, 1.05},
          {alpha::EncodingType::RLE, 1.05},
          {alpha::EncodingType::Varint, 1.1},
      },
      alpha::CompressionOptions{
          .compressionAcceptRatio = 0.9,
          .zstrongCompressionLevel = 9,
          .zstrongDecompressionLevel = 2,
          .useVariableBitWidthCompressor = false,
      },
      std::nullopt);
}
} // namespace

struct EncodingDetails {
  alpha::EncodingType encodingType;
  alpha::DataType dataType;
  uint32_t level;
  std::string nestedEncodingName;
};

void verifyEncodingTree(
    std::string_view stream,
    std::vector<EncodingDetails> expected) {
  ASSERT_GT(expected.size(), 0);
  std::vector<EncodingDetails> actual;
  alpha::tools::traverseEncodings(
      stream,
      [&](auto encodingType,
          auto dataType,
          auto level,
          auto /* index */,
          auto nestedEncodingName,
          std::unordered_map<
              alpha::tools::EncodingPropertyType,
              alpha::tools::EncodingProperty> /* properties */) {
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
typename alpha::TypeTraits<T>::physicalType asPhysicalType(T value) {
  return *reinterpret_cast<typename alpha::TypeTraits<T>::physicalType*>(
      &value);
}

template <typename T>
void test(std::span<const T> values, std::vector<EncodingDetails> expected) {
  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  auto policy = getRootManualSelectionPolicy<T>();
  alpha::Buffer buffer{*pool};

  auto serialized =
      alpha::EncodingFactory::encode<T>(std::move(policy), values, buffer);

  LOG(INFO) << "Final size: " << serialized.size();

  ASSERT_GT(expected.size(), 0);
  std::vector<EncodingDetails> actual;
  alpha::tools::traverseEncodings(
      serialized,
      [&](auto encodingType,
          auto dataType,
          auto level,
          auto /* index */,
          auto nestedEncodingName,
          std::unordered_map<
              alpha::tools::EncodingPropertyType,
              alpha::tools::EncodingProperty> /* properties */) {
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

  alpha::Vector<T> materialized{pool.get()};
  auto encoding = alpha::EncodingFactory::decode(*pool, serialized);
  alpha::Vector<T> result{pool.get()};
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
            {.encodingType = alpha::EncodingType::Constant,
             .dataType = alpha::TypeTraits<T>::dataType,
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
      if constexpr (alpha::isFloatingPointType<T>()) {
        values[i] = i;
      } else {
        values[i] = i % std::numeric_limits<T>::max();
      }
    }

    if constexpr (alpha::isFloatingPointType<T>() || sizeof(T) < 4) {
      // Floating point types and small types use Trivial encoding to encode
      // the exception values.
      test<T>(
          values,
          {
              {.encodingType = alpha::EncodingType::MainlyConstant,
               .dataType = alpha::TypeTraits<T>::dataType,
               .level = 0,
               .nestedEncodingName = ""},
              {.encodingType = alpha::EncodingType::SparseBool,
               .dataType = alpha::DataType::Bool,
               .level = 1,
               .nestedEncodingName = "IsCommon"},
              {.encodingType = alpha::EncodingType::FixedBitWidth,
               .dataType = alpha::DataType::Uint32,
               .level = 2,
               .nestedEncodingName = "Indices"},
              {.encodingType = alpha::EncodingType::Trivial,
               .dataType = alpha::TypeTraits<
                   typename alpha::EncodingPhysicalType<T>::type>::dataType,
               .level = 1,
               .nestedEncodingName = "OtherValues"},
          });
    } else {
      // All other numeric types use FixedBitWidth encoding to encode the
      // exception values.
      test<T>(
          values,
          {
              {.encodingType = alpha::EncodingType::MainlyConstant,
               .dataType = alpha::TypeTraits<T>::dataType,
               .level = 0,
               .nestedEncodingName = ""},
              {.encodingType = alpha::EncodingType::SparseBool,
               .dataType = alpha::DataType::Bool,
               .level = 1,
               .nestedEncodingName = "IsCommon"},
              {.encodingType = alpha::EncodingType::FixedBitWidth,
               .dataType = alpha::DataType::Uint32,
               .level = 2,
               .nestedEncodingName = "Indices"},
              {.encodingType = alpha::EncodingType::FixedBitWidth,
               .dataType = alpha::TypeTraits<
                   typename alpha::EncodingPhysicalType<T>::type>::dataType,
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
          {.encodingType = alpha::EncodingType::Trivial,
           .dataType = alpha::TypeTraits<T>::dataType,
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
            {.encodingType = alpha::EncodingType::FixedBitWidth,
             .dataType = alpha::TypeTraits<T>::dataType,
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
          {.encodingType = alpha::EncodingType::Dictionary,
           .dataType = alpha::TypeTraits<T>::dataType,
           .level = 0,
           .nestedEncodingName = ""},
          {.encodingType = alpha::EncodingType::Trivial,
           .dataType = alpha::TypeTraits<
               typename alpha::EncodingPhysicalType<T>::type>::dataType,
           .level = 1,
           .nestedEncodingName = "Alphabet"},
          {.encodingType = alpha::EncodingType::FixedBitWidth,
           .dataType = alpha::DataType::Uint32,
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
      alpha::isFloatingPointType<T>() || std::is_same_v<int32_t, T> ||
      sizeof(T) > 4) {
    // Floating point types and big types prefer storing the run values as
    // dictionary
    test<T>(
        values,
        {
            {.encodingType = alpha::EncodingType::RLE,
             .dataType = alpha::TypeTraits<T>::dataType,
             .level = 0,
             .nestedEncodingName = ""},
            {.encodingType = alpha::EncodingType::FixedBitWidth,
             .dataType = alpha::DataType::Uint32,
             .level = 1,
             .nestedEncodingName = "Lengths"},
            {.encodingType = alpha::EncodingType::Dictionary,
             .dataType = alpha::TypeTraits<
                 typename alpha::EncodingPhysicalType<T>::type>::dataType,
             .level = 1,
             .nestedEncodingName = "Values"},
            {.encodingType = alpha::EncodingType::Trivial,
             .dataType = alpha::TypeTraits<
                 typename alpha::EncodingPhysicalType<T>::type>::dataType,
             .level = 2,
             .nestedEncodingName = "Alphabet"},
            {.encodingType = alpha::EncodingType::FixedBitWidth,
             .dataType = alpha::DataType::Uint32,
             .level = 2,
             .nestedEncodingName = "Indices"},
        });
  } else {
    test<T>(
        values,
        {
            {.encodingType = alpha::EncodingType::RLE,
             .dataType = alpha::TypeTraits<T>::dataType,
             .level = 0,
             .nestedEncodingName = ""},
            {.encodingType = alpha::EncodingType::FixedBitWidth,
             .dataType = alpha::DataType::Uint32,
             .level = 1,
             .nestedEncodingName = "Lengths"},
            {.encodingType = alpha::EncodingType::Trivial,
             .dataType = alpha::TypeTraits<
                 typename alpha::EncodingPhysicalType<T>::type>::dataType,
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
        *reinterpret_cast<const typename alpha::TypeTraits<T>::physicalType*>(
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
          {.encodingType = alpha::EncodingType::Varint,
           .dataType = alpha::TypeTraits<T>::dataType,
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
            {.encodingType = alpha::EncodingType::Constant,
             .dataType = alpha::TypeTraits<T>::dataType,
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
            {.encodingType = alpha::EncodingType::SparseBool,
             .dataType = alpha::TypeTraits<T>::dataType,
             .level = 0,
             .nestedEncodingName = ""},
            {.encodingType = alpha::EncodingType::FixedBitWidth,
             .dataType = alpha::DataType::Uint32,
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

  std::vector<std::unique_ptr<alpha::EncodingSelectionPolicy<T>>> policies;
  policies.push_back(getRootManualSelectionPolicy<T>());
  // use ml policy
  policies.push_back(
      std::make_unique<alpha::LearnedEncodingSelectionPolicy<T>>());

  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  alpha::Buffer buffer{*pool};

  for (auto& policy : policies) {
    std::array<T, 10000> values;
    for (auto i = 0; i < values.size(); ++i) {
      values[i] = folly::Random::oneIn(2, rng);
    }

    auto serialized =
        alpha::EncodingFactory::encode<T>(std::move(policy), values, buffer);

    LOG(INFO) << "Final size: " << serialized.size();

    verifyEncodingTree(
        serialized,
        {
            {.encodingType = alpha::EncodingType::Trivial,
             .dataType = alpha::TypeTraits<T>::dataType,
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

  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  alpha::Buffer buffer{*pool};

  std::vector<uint32_t> runLengths;
  runLengths.resize(20);
  uint32_t valueCount = 0;
  for (auto i = 0; i < runLengths.size(); ++i) {
    runLengths[i] = (folly::Random::rand32(rng) % 700) + 20;
    valueCount += runLengths[i];
  }

  alpha::Vector<bool> values{pool.get()};
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
      alpha::EncodingFactory::encode<T>(std::move(policy), values, buffer);

  LOG(INFO) << "Final size: " << serialized.size();

  verifyEncodingTree(
      serialized,
      {
          {.encodingType = alpha::EncodingType::RLE,
           .dataType = alpha::TypeTraits<T>::dataType,
           .level = 0,
           .nestedEncodingName = ""},
          {.encodingType = alpha::EncodingType::FixedBitWidth,
           .dataType = alpha::DataType::Uint32,
           .level = 1,
           .nestedEncodingName = "Lengths"},
      });
}

TEST(EncodingSelectionStringTests, SelectConst) {
  using T = std::string_view;

  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  alpha::Buffer buffer{*pool};

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
        alpha::EncodingFactory::encode<T>(std::move(policy), values, buffer);

    LOG(INFO) << "Final size: " << serialized.size();

    verifyEncodingTree(
        serialized,
        {
            {.encodingType = alpha::EncodingType::Constant,
             .dataType = alpha::TypeTraits<T>::dataType,
             .level = 0,
             .nestedEncodingName = ""},
        });
  }
}

TEST(EncodingSelectionStringTests, SelectMainlyConst) {
  using T = std::string_view;

  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  alpha::Buffer buffer{*pool};

  for (const T value : {
           std::string("aaaaa"),
           std::string(5000, '\0'),
       }) {
    std::vector<T> values;
    values.resize(1000);
    for (auto i = 0; i < values.size(); ++i) {
      values[i] = value;
    }

    std::vector<std::string> uncommonValues;
    for (auto i = 0; i < values.size() / 20; ++i) {
      uncommonValues.emplace_back(i, 'b');
    }

    for (auto i = 0; i < uncommonValues.size(); ++i) {
      values[i * 20] = uncommonValues[i];
    }

    auto policy = getRootManualSelectionPolicy<T>();
    auto serialized =
        alpha::EncodingFactory::encode<T>(std::move(policy), values, buffer);

    LOG(INFO) << "Final size: " << serialized.size();

    verifyEncodingTree(
        serialized,
        {
            {.encodingType = alpha::EncodingType::MainlyConstant,
             .dataType = alpha::TypeTraits<T>::dataType,
             .level = 0,
             .nestedEncodingName = ""},
            {.encodingType = alpha::EncodingType::SparseBool,
             .dataType = alpha::DataType::Bool,
             .level = 1,
             .nestedEncodingName = "IsCommon"},
            {.encodingType = alpha::EncodingType::FixedBitWidth,
             .dataType = alpha::DataType::Uint32,
             .level = 2,
             .nestedEncodingName = "Indices"},
            {.encodingType = alpha::EncodingType::Trivial,
             .dataType = alpha::TypeTraits<
                 typename alpha::EncodingPhysicalType<T>::type>::dataType,
             .level = 1,
             .nestedEncodingName = "OtherValues"},
            {.encodingType = alpha::EncodingType::FixedBitWidth,
             .dataType = alpha::DataType::Uint32,
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

  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  alpha::Buffer buffer{*pool};

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
  values.resize(cache.size());
  for (auto i = 0; i < cache.size(); ++i) {
    values[i] = cache[i];
  }

  auto serialized =
      alpha::EncodingFactory::encode<T>(std::move(policy), values, buffer);

  LOG(INFO) << "Final size: " << serialized.size();

  verifyEncodingTree(
      serialized,
      {
          {.encodingType = alpha::EncodingType::Trivial,
           .dataType = alpha::TypeTraits<T>::dataType,
           .level = 0,
           .nestedEncodingName = ""},
          {.encodingType = alpha::EncodingType::FixedBitWidth,
           .dataType = alpha::DataType::Uint32,
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

  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  alpha::Buffer buffer{*pool};

  auto policy = getRootManualSelectionPolicy<T>();

  std::vector<T> values;
  values.resize(10000);
  for (auto i = 0; i < values.size(); ++i) {
    values[i] = uniqueValues[folly::Random::rand32(rng) % uniqueValues.size()];
  }

  auto serialized =
      alpha::EncodingFactory::encode<T>(std::move(policy), values, buffer);

  LOG(INFO) << "Final size: " << serialized.size();

  verifyEncodingTree(
      serialized,
      {
          {.encodingType = alpha::EncodingType::Dictionary,
           .dataType = alpha::TypeTraits<T>::dataType,
           .level = 0,
           .nestedEncodingName = ""},
          {.encodingType = alpha::EncodingType::Trivial,
           .dataType = alpha::TypeTraits<
               typename alpha::EncodingPhysicalType<T>::type>::dataType,
           .level = 1,
           .nestedEncodingName = "Alphabet"},
          {.encodingType = alpha::EncodingType::Varint,
           .dataType = alpha::DataType::Uint32,
           .level = 2,
           .nestedEncodingName = "Lengths"},
          {.encodingType = alpha::EncodingType::FixedBitWidth,
           .dataType = alpha::DataType::Uint32,
           .level = 1,
           .nestedEncodingName = "Indices"},
      });
}

TEST(EncodingSelectionStringTests, SelectRunLength) {
  using T = std::string_view;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  alpha::Buffer buffer{*pool};

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
      values.push_back(
          index % 2 == 0 ? "abcdefghijklmnopqrstuvwxyz" : "1234567890");
    }
    ++index;
  }

  auto policy = getRootManualSelectionPolicy<T>();
  auto serialized =
      alpha::EncodingFactory::encode<T>(std::move(policy), values, buffer);

  LOG(INFO) << "Final size: " << serialized.size();

  verifyEncodingTree(
      serialized,
      {
          {.encodingType = alpha::EncodingType::RLE,
           .dataType = alpha::TypeTraits<T>::dataType,
           .level = 0,
           .nestedEncodingName = ""},
          {.encodingType = alpha::EncodingType::FixedBitWidth,
           .dataType = alpha::DataType::Uint32,
           .level = 1,
           .nestedEncodingName = "Lengths"},
          {.encodingType = alpha::EncodingType::Dictionary,
           .dataType = alpha::TypeTraits<T>::dataType,
           .level = 1,
           .nestedEncodingName = "Values"},
          {.encodingType = alpha::EncodingType::Trivial,
           .dataType = alpha::TypeTraits<T>::dataType,
           .level = 2,
           .nestedEncodingName = "Alphabet"},
          {.encodingType = alpha::EncodingType::Varint,
           .dataType = alpha::DataType::Uint32,
           .level = 3,
           .nestedEncodingName = "Lengths"},
          {.encodingType = alpha::EncodingType::FixedBitWidth,
           .dataType = alpha::DataType::Uint32,
           .level = 2,
           .nestedEncodingName = "Indices"},
      });
}

TEST(EncodingSelectionTests, TestNullable) {
  using T = std::string_view;
  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  alpha::Buffer buffer{*pool};
  auto policy = getRootManualSelectionPolicy<T>();
  std::vector<T> data{"abcd", "efg", "hijk", "lmno"};
  std::array<bool, 10> nulls{
      true, false, true, true, false, false, true, true, true, false};

  auto serialized = alpha::EncodingFactory::encodeNullable<T>(
      std::move(policy), data, nulls, buffer);
  LOG(INFO) << "Final size: " << serialized.size();
}
