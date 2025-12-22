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
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/encodings/ConstantEncoding.h"
#include "dwio/nimble/encodings/DictionaryEncoding.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/MainlyConstantEncoding.h"
#include "dwio/nimble/encodings/NullableEncoding.h"
#include "dwio/nimble/encodings/RleEncoding.h"
#include "dwio/nimble/encodings/SparseBoolEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/VarintEncoding.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "velox/common/memory/Memory.h"

using namespace facebook;

class EncodingSeekTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  nimble::Vector<std::string_view> toVector(
      std::initializer_list<std::string_view> values) {
    nimble::Vector<std::string_view> result{pool_.get()};
    result.insert(result.end(), values.begin(), values.end());
    return result;
  }

  std::unique_ptr<nimble::Encoding> createEncoding(
      const nimble::Vector<std::string_view>& values) {
    return nimble::test::Encoder<nimble::TrivialEncoding<std::string_view>>::
        createEncoding(*buffer_, values);
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
};

class EncodingSeekTestWithCompression
    : public ::testing::TestWithParam<nimble::CompressionType> {
 protected:
  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  nimble::Vector<std::string_view> toVector(
      std::initializer_list<std::string_view> values) {
    nimble::Vector<std::string_view> result{pool_.get()};
    result.insert(result.end(), values.begin(), values.end());
    return result;
  }

  std::unique_ptr<nimble::Encoding> createEncoding(
      const nimble::Vector<std::string_view>& values,
      nimble::CompressionType compressionType) {
    return nimble::test::Encoder<nimble::TrivialEncoding<std::string_view>>::
        createEncoding(*buffer_, values, compressionType);
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
};

TEST_P(EncodingSeekTestWithCompression, seekExactMatch) {
  const auto values =
      toVector({"", "apple", "banana", "cherry", "date", "elderberry"});
  const auto encoding = createEncoding(values, GetParam());

  struct {
    std::string_view seekValue;
    std::optional<uint32_t> expected;

    std::string debugString() const {
      if (expected.has_value()) {
        return fmt::format("seek '{}' -> row {}", seekValue, expected.value());
      }
      return fmt::format("seek '{}' -> not found", seekValue);
    }
  } testCases[] = {
      // Exact matches
      {"", 0},
      {"apple", 1},
      {"banana", 2},
      {"cherry", 3},
      {"date", 4},
      {"elderberry", 5},
      // Between values (should return next value)
      {"carrot", 3},
      {"aaa", 1},
      {"coconut", 4},
      // After all values (not found)
      {"zebra", std::nullopt},
      {"fig", std::nullopt}};

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    encoding->reset();
    auto result = encoding->seekAtOrAfter(&testCase.seekValue);
    ASSERT_EQ(result.has_value(), testCase.expected.has_value());
    if (testCase.expected.has_value()) {
      EXPECT_EQ(result.value(), testCase.expected.value());
    }
  }
}

TEST_P(EncodingSeekTestWithCompression, seekExactMatchWithDuplicateValues) {
  const auto values = toVector(
      {"apple", "apple", "banana", "banana", "banana", "cherry", "cherry"});
  const auto encoding = createEncoding(values, GetParam());

  struct {
    std::string_view seekValue;
    std::optional<uint32_t> expected;

    std::string debugString() const {
      if (expected.has_value()) {
        return fmt::format("seek '{}' -> row {}", seekValue, expected.value());
      }
      return fmt::format("seek '{}' -> not found", seekValue);
    }
  } testCases[] = {
      // Should return first occurrence of duplicate values
      {"apple", 0},
      {"banana", 2},
      {"cherry", 5},
      // Between values (should return first of next value)
      {"aaa", 0},
      {"app", 0},
      {"az", 2},
      {"car", 5},
      // After all values (not found)
      {"date", std::nullopt},
      {"zebra", std::nullopt}};

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    encoding->reset();
    auto result = encoding->seekAtOrAfter(&testCase.seekValue);
    ASSERT_EQ(result.has_value(), testCase.expected.has_value());
    if (testCase.expected.has_value()) {
      EXPECT_EQ(result.value(), testCase.expected.value());
    }
  }
}

TEST_P(EncodingSeekTestWithCompression, seekExactMatchWithEmptyEncoding) {
  const auto values = toVector({});
  const auto encoding = createEncoding(values, GetParam());

  struct {
    std::string_view seekValue;
    std::optional<uint32_t> expected;

    std::string debugString() const {
      if (expected.has_value()) {
        return fmt::format("seek '{}' -> row {}", seekValue, expected.value());
      }
      return fmt::format("seek '{}' -> not found", seekValue);
    }
  } testCases[] = {
      // All seeks should return not found for empty encoding
      {"", std::nullopt},
      {"apple", std::nullopt},
      {"test", std::nullopt},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    encoding->reset();
    auto result = encoding->seekAtOrAfter(&testCase.seekValue);
    ASSERT_EQ(result.has_value(), testCase.expected.has_value());
    if (testCase.expected.has_value()) {
      EXPECT_EQ(result.value(), testCase.expected.value());
    }
  }
}

TEST_P(EncodingSeekTestWithCompression, seekAfterValue) {
  auto compressionType = GetParam();
  auto values = toVector({"apple", "cherry", "elderberry", "grape", "kiwi"});
  auto encoding = createEncoding(values, compressionType);

  struct {
    std::string_view seekValue;
    std::optional<uint32_t> expected;

    std::string debugString() const {
      if (expected.has_value()) {
        return fmt::format("seek '{}' -> row {}", seekValue, expected.value());
      }
      return fmt::format("seek '{}' -> not found", seekValue);
    }
  } testCases[] = {
      // Before first value
      {"", 0},
      {"aaa", 0},
      // Between apple and cherry
      {"banana", 1},
      {"cat", 1},
      // Between cherry and elderberry
      {"date", 2},
      {"duck", 2},
      // Between elderberry and grape
      {"fig", 3},
      // Between grape and kiwi
      {"honey", 4},
      {"ice", 4},
      // After last value (not found)
      {"lemon", std::nullopt},
      {"mango", std::nullopt}};

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    encoding->reset();
    auto result = encoding->seekAtOrAfter(&testCase.seekValue);
    ASSERT_EQ(result.has_value(), testCase.expected.has_value());
    if (testCase.expected.has_value()) {
      EXPECT_EQ(result.value(), testCase.expected.value());
    }
  }
}

TEST_P(EncodingSeekTestWithCompression, seekAfterValueWithDuplicateValues) {
  auto compressionType = GetParam();
  // Duplicates at beginning (apple), middle (cherry), and end (kiwi)
  auto values = toVector(
      {"apple", "apple", "apple", "cherry", "cherry", "grape", "kiwi", "kiwi"});
  auto encoding = createEncoding(values, compressionType);

  struct {
    std::string_view seekValue;
    std::optional<uint32_t> expected;

    std::string debugString() const {
      if (expected.has_value()) {
        return fmt::format("seek '{}' -> row {}", seekValue, expected.value());
      }
      return fmt::format("seek '{}' -> not found", seekValue);
    }
  } testCases[] = {
      // Before first duplicate group
      {"", 0},
      {"aaa", 0},
      // Between apple (end) and cherry (start)
      {"banana", 3},
      {"car", 3},
      // Between cherry (end) and grape
      {"date", 5},
      {"fig", 5},
      // Between grape and kiwi (start of end duplicates)
      {"honey", 6},
      // After last duplicate group (not found)
      {"lemon", std::nullopt},
      {"zebra", std::nullopt},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    encoding->reset();
    auto result = encoding->seekAtOrAfter(&testCase.seekValue);
    ASSERT_EQ(result.has_value(), testCase.expected.has_value());
    if (testCase.expected.has_value()) {
      EXPECT_EQ(result.value(), testCase.expected.value());
    }
  }
}

NIMBLE_INSTANTIATE_TEST_SUITE_P(
    EncodingSeekTestWithCompressionSuite,
    EncodingSeekTestWithCompression,
    ::testing::Values(
        nimble::CompressionType::Uncompressed,
        nimble::CompressionType::Zstd,
        nimble::CompressionType::MetaInternal),
    [](const ::testing::TestParamInfo<nimble::CompressionType>& info) {
      return nimble::toString(info.param);
    });

TEST_F(EncodingSeekTest, seekUnsupportedEncodings) {
  std::string_view seekValue = "test";

  // RLE encoding
  {
    nimble::Vector<std::string_view> values{pool_.get()};
    values.push_back("apple");
    values.push_back("apple");
    values.push_back("banana");
    auto encoding =
        nimble::test::Encoder<nimble::RLEEncoding<std::string_view>>::
            createEncoding(*buffer_, values);
    NIMBLE_ASSERT_THROW(
        encoding->seekAtOrAfter(&seekValue), "seekAtOrAfter is not supported");
  }

  // Dictionary encoding
  {
    nimble::Vector<std::string_view> values{pool_.get()};
    values.push_back("apple");
    values.push_back("banana");
    auto encoding =
        nimble::test::Encoder<nimble::DictionaryEncoding<std::string_view>>::
            createEncoding(*buffer_, values);
    NIMBLE_ASSERT_THROW(
        encoding->seekAtOrAfter(&seekValue), "seekAtOrAfter is not supported");
  }

  // Constant encoding
  {
    nimble::Vector<std::string_view> values{pool_.get()};
    values.push_back("apple");
    auto encoding =
        nimble::test::Encoder<nimble::ConstantEncoding<std::string_view>>::
            createEncoding(*buffer_, values);
    NIMBLE_ASSERT_THROW(
        encoding->seekAtOrAfter(&seekValue), "seekAtOrAfter is not supported");
  }

  // MainlyConstant encoding
  {
    nimble::Vector<std::string_view> values{pool_.get()};
    values.push_back("apple");
    values.push_back("apple");
    values.push_back("banana");
    auto encoding = nimble::test::Encoder<nimble::MainlyConstantEncoding<
        std::string_view>>::createEncoding(*buffer_, values);
    NIMBLE_ASSERT_THROW(
        encoding->seekAtOrAfter(&seekValue), "seekAtOrAfter is not supported");
  }

  // Nullable encoding
  {
    nimble::Vector<std::string_view> values{pool_.get()};
    values.push_back("apple");
    values.push_back("banana");
    nimble::Vector<bool> nulls{pool_.get()};
    nulls.push_back(false);
    nulls.push_back(false);
    auto encoding =
        nimble::test::Encoder<nimble::NullableEncoding<std::string_view>>::
            createNullableEncoding(*buffer_, values, nulls);
    NIMBLE_ASSERT_THROW(
        encoding->seekAtOrAfter(&seekValue), "seekAtOrAfter is not supported");
  }

  // Trivial encoding for non-string types
  {
    nimble::Vector<int32_t> intValues{pool_.get()};
    intValues.push_back(1);
    intValues.push_back(2);
    auto encoding =
        nimble::test::Encoder<nimble::TrivialEncoding<int32_t>>::createEncoding(
            *buffer_, intValues);
    int32_t intSeekValue = 1;
    NIMBLE_ASSERT_THROW(
        encoding->seekAtOrAfter(&intSeekValue),
        "seekAtOrAfter is not supported");
  }

  // FixedBitWidth encoding (integer only)
  {
    nimble::Vector<uint32_t> intValues{pool_.get()};
    intValues.push_back(1);
    intValues.push_back(2);
    auto encoding =
        nimble::test::Encoder<nimble::FixedBitWidthEncoding<uint32_t>>::
            createEncoding(*buffer_, intValues);
    uint32_t intSeekValue = 1;
    NIMBLE_ASSERT_THROW(
        encoding->seekAtOrAfter(&intSeekValue),
        "seekAtOrAfter is not supported");
  }

  // Varint encoding (integer only)
  {
    nimble::Vector<uint32_t> intValues{pool_.get()};
    intValues.push_back(1);
    intValues.push_back(2);
    auto encoding =
        nimble::test::Encoder<nimble::VarintEncoding<uint32_t>>::createEncoding(
            *buffer_, intValues);
    uint32_t intSeekValue = 1;
    NIMBLE_ASSERT_THROW(
        encoding->seekAtOrAfter(&intSeekValue),
        "seekAtOrAfter is not supported");
  }

  // RLE encoding for integers
  {
    nimble::Vector<int32_t> intValues{pool_.get()};
    intValues.push_back(1);
    intValues.push_back(1);
    intValues.push_back(2);
    auto encoding =
        nimble::test::Encoder<nimble::RLEEncoding<int32_t>>::createEncoding(
            *buffer_, intValues);
    int32_t intSeekValue = 1;
    NIMBLE_ASSERT_THROW(
        encoding->seekAtOrAfter(&intSeekValue),
        "seekAtOrAfter is not supported");
  }

  // SparseBool encoding
  {
    nimble::Vector<bool> boolValues{pool_.get()};
    boolValues.push_back(true);
    boolValues.push_back(false);
    boolValues.push_back(true);
    auto encoding =
        nimble::test::Encoder<nimble::SparseBoolEncoding>::createEncoding(
            *buffer_, boolValues);
    bool boolSeekValue = true;
    NIMBLE_ASSERT_THROW(
        encoding->seekAtOrAfter(&boolSeekValue),
        "seekAtOrAfter is not supported");
  }
}
