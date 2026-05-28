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
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <chrono>
#include <thread>
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
#include "dwio/nimble/encodings/VarintEncoding.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "velox/common/memory/Memory.h"

using namespace facebook;

class TrivialEncodingTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    rootPool_ =
        velox::memory::memoryManager()->addRootPool("TrivialEncodingTest");
    pool_ = rootPool_->addLeafChild("TrivialEncodingTestLeaf");
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
      const nimble::Encoding::Options& options = {}) {
    stringBuffers_.clear();
    return nimble::test::Encoder<nimble::TrivialEncoding<std::string_view>>::
        createEncoding(
            *buffer_,
            values,
            [&](uint32_t totalLength) {
              auto& buffer = stringBuffers_.emplace_back(
                  velox::AlignedBuffer::allocate<char>(
                      totalLength, pool_.get()));
              return buffer->asMutable<void>();
            },
            nimble::CompressionType::Uncompressed,
            options);
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
  std::vector<velox::BufferPtr> stringBuffers_;
};

class TrivialEncodingTestWithCompression
    : public ::testing::TestWithParam<nimble::CompressionType> {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    rootPool_ = velox::memory::memoryManager()->addRootPool(
        "TrivialEncodingTestWithCompression");
    pool_ = rootPool_->addLeafChild("TrivialEncodingTestWithCompressionLeaf");
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
    stringBuffers_.clear();
    return nimble::test::Encoder<nimble::TrivialEncoding<std::string_view>>::
        createEncoding(
            *buffer_,
            values,
            [&](uint32_t totalLength) {
              auto& buffer = stringBuffers_.emplace_back(
                  velox::AlignedBuffer::allocate<char>(
                      totalLength, pool_.get()));
              return buffer->asMutable<void>();
            },
            compressionType);
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
  std::vector<velox::BufferPtr> stringBuffers_;
};

TEST_P(TrivialEncodingTestWithCompression, seekExactMatch) {
  const auto values =
      toVector({"", "apple", "banana", "cherry", "date", "elderberry"});
  const auto encoding = createEncoding(values, GetParam());

  struct {
    std::string_view seekValue;
    std::optional<uint32_t> expected;
    std::optional<uint32_t> expectedExclusive;

    std::string debugString() const {
      if (expected.has_value()) {
        return fmt::format("seek '{}' -> row {}", seekValue, expected.value());
      }
      return fmt::format("seek '{}' -> not found", seekValue);
    }
  } testCases[] = {
      // Exact matches: exclusive returns next row after match
      {"", 0, 1},
      {"apple", 1, 2},
      {"banana", 2, 3},
      {"cherry", 3, 4},
      {"date", 4, 5},
      {"elderberry", 5, std::nullopt},
      // Between values (should return next value): same for both
      {"carrot", 3, 3},
      {"aaa", 1, 1},
      {"coconut", 4, 4},
      // After all values (not found): same for both
      {"zebra", std::nullopt, std::nullopt},
      {"fig", std::nullopt, std::nullopt}};

  for (bool inclusive : {true, false}) {
    SCOPED_TRACE(fmt::format("inclusive={}", inclusive));
    for (const auto& testCase : testCases) {
      SCOPED_TRACE(testCase.debugString());
      encoding->reset();
      auto result = encoding->seek(&testCase.seekValue, inclusive);
      const auto& expectedResult =
          inclusive ? testCase.expected : testCase.expectedExclusive;
      ASSERT_EQ(result.has_value(), expectedResult.has_value());
      if (expectedResult.has_value()) {
        EXPECT_EQ(result.value(), expectedResult.value());
      }
    }
  }
}

TEST_P(TrivialEncodingTestWithCompression, seekExactMatchWithDuplicateValues) {
  const auto values = toVector(
      {"apple", "apple", "banana", "banana", "banana", "cherry", "cherry"});
  const auto encoding = createEncoding(values, GetParam());

  struct {
    std::string_view seekValue;
    std::optional<uint32_t> expected;
    std::optional<uint32_t> expectedExclusive;

    std::string debugString() const {
      if (expected.has_value()) {
        return fmt::format("seek '{}' -> row {}", seekValue, expected.value());
      }
      return fmt::format("seek '{}' -> not found", seekValue);
    }
  } testCases[] = {
      // Exact matches: exclusive skips past ALL duplicates
      {"apple", 0, 2},
      {"banana", 2, 5},
      {"cherry", 5, std::nullopt},
      // Between values (should return first of next value): same for both
      {"aaa", 0, 0},
      {"app", 0, 0},
      {"az", 2, 2},
      {"car", 5, 5},
      // After all values (not found): same for both
      {"date", std::nullopt, std::nullopt},
      {"zebra", std::nullopt, std::nullopt}};

  for (bool inclusive : {true, false}) {
    SCOPED_TRACE(fmt::format("inclusive={}", inclusive));
    for (const auto& testCase : testCases) {
      SCOPED_TRACE(testCase.debugString());
      encoding->reset();
      auto result = encoding->seek(&testCase.seekValue, inclusive);
      const auto& expectedResult =
          inclusive ? testCase.expected : testCase.expectedExclusive;
      ASSERT_EQ(result.has_value(), expectedResult.has_value());
      if (expectedResult.has_value()) {
        EXPECT_EQ(result.value(), expectedResult.value());
      }
    }
  }
}

TEST_P(TrivialEncodingTestWithCompression, seekAfterValueWithDuplicateValues) {
  auto compressionType = GetParam();
  // Duplicates at beginning (apple), middle (cherry), and end (kiwi)
  auto values = toVector(
      {"apple", "apple", "apple", "cherry", "cherry", "grape", "kiwi", "kiwi"});
  auto encoding = createEncoding(values, compressionType);

  struct {
    std::string_view seekValue;
    std::optional<uint32_t> expected;
    std::optional<uint32_t> expectedExclusive;

    std::string debugString() const {
      if (expected.has_value()) {
        return fmt::format("seek '{}' -> row {}", seekValue, expected.value());
      }
      return fmt::format("seek '{}' -> not found", seekValue);
    }
  } testCases[] = {
      // Before first duplicate group: non-match, same for both
      {"", 0, 0},
      {"aaa", 0, 0},
      // Between apple (end) and cherry (start): non-match, same for both
      {"banana", 3, 3},
      {"car", 3, 3},
      // Between cherry (end) and grape: non-match, same for both
      {"date", 5, 5},
      {"fig", 5, 5},
      // Between grape and kiwi (start of end duplicates): non-match, same
      {"honey", 6, 6},
      // After last duplicate group (not found): same for both
      {"lemon", std::nullopt, std::nullopt},
      {"zebra", std::nullopt, std::nullopt},
  };

  for (bool inclusive : {true, false}) {
    SCOPED_TRACE(fmt::format("inclusive={}", inclusive));
    for (const auto& testCase : testCases) {
      SCOPED_TRACE(testCase.debugString());
      encoding->reset();
      auto result = encoding->seek(&testCase.seekValue, inclusive);
      const auto& expectedResult =
          inclusive ? testCase.expected : testCase.expectedExclusive;
      ASSERT_EQ(result.has_value(), expectedResult.has_value());
      if (expectedResult.has_value()) {
        EXPECT_EQ(result.value(), expectedResult.value());
      }
    }
  }
}

TEST_P(TrivialEncodingTestWithCompression, seekExactMatchWithEmptyEncoding) {
  const auto values = toVector({});
  const auto encoding = createEncoding(values, GetParam());

  struct {
    std::string_view seekValue;
    std::optional<uint32_t> expected;
    std::optional<uint32_t> expectedExclusive;

    std::string debugString() const {
      if (expected.has_value()) {
        return fmt::format("seek '{}' -> row {}", seekValue, expected.value());
      }
      return fmt::format("seek '{}' -> not found", seekValue);
    }
  } testCases[] = {
      // All seeks should return not found for empty encoding
      {"", std::nullopt, std::nullopt},
      {"apple", std::nullopt, std::nullopt},
      {"test", std::nullopt, std::nullopt},
  };

  for (bool inclusive : {true, false}) {
    SCOPED_TRACE(fmt::format("inclusive={}", inclusive));
    for (const auto& testCase : testCases) {
      SCOPED_TRACE(testCase.debugString());
      encoding->reset();
      auto result = encoding->seek(&testCase.seekValue, inclusive);
      const auto& expectedResult =
          inclusive ? testCase.expected : testCase.expectedExclusive;
      ASSERT_EQ(result.has_value(), expectedResult.has_value());
      if (expectedResult.has_value()) {
        EXPECT_EQ(result.value(), expectedResult.value());
      }
    }
  }
}

TEST_P(TrivialEncodingTestWithCompression, seekAfterValue) {
  auto compressionType = GetParam();
  auto values = toVector({"apple", "cherry", "elderberry", "grape", "kiwi"});
  auto encoding = createEncoding(values, compressionType);

  struct {
    std::string_view seekValue;
    std::optional<uint32_t> expected;
    std::optional<uint32_t> expectedExclusive;

    std::string debugString() const {
      if (expected.has_value()) {
        return fmt::format("seek '{}' -> row {}", seekValue, expected.value());
      }
      return fmt::format("seek '{}' -> not found", seekValue);
    }
  } testCases[] = {
      // Before first value: non-match, same for both
      {"", 0, 0},
      {"aaa", 0, 0},
      // Between apple and cherry: non-match, same for both
      {"banana", 1, 1},
      {"cat", 1, 1},
      // Between cherry and elderberry: non-match, same for both
      {"date", 2, 2},
      {"duck", 2, 2},
      // Between elderberry and grape: non-match, same for both
      {"fig", 3, 3},
      // Between grape and kiwi: non-match, same for both
      {"honey", 4, 4},
      {"ice", 4, 4},
      // After last value (not found): same for both
      {"lemon", std::nullopt, std::nullopt},
      {"mango", std::nullopt, std::nullopt}};

  for (bool inclusive : {true, false}) {
    SCOPED_TRACE(fmt::format("inclusive={}", inclusive));
    for (const auto& testCase : testCases) {
      SCOPED_TRACE(testCase.debugString());
      encoding->reset();
      auto result = encoding->seek(&testCase.seekValue, inclusive);
      const auto& expectedResult =
          inclusive ? testCase.expected : testCase.expectedExclusive;
      ASSERT_EQ(result.has_value(), expectedResult.has_value());
      if (expectedResult.has_value()) {
        EXPECT_EQ(result.value(), expectedResult.value());
      }
    }
  }
}

NIMBLE_INSTANTIATE_TEST_SUITE_P(
    TrivialEncodingTestWithCompressionSuite,
    TrivialEncodingTestWithCompression,
    ::testing::Values(
        nimble::CompressionType::Uncompressed,
        nimble::CompressionType::MetaInternal),
    [](const ::testing::TestParamInfo<nimble::CompressionType>& info) {
      return nimble::toString(info.param);
    });

TEST_F(TrivialEncodingTest, seekUnsupportedEncodings) {
  std::string_view seekValue = "test";
  std::vector<velox::BufferPtr> stringBuffers;

  auto makeStringBufferFactory = [&]() {
    return [&](uint32_t totalLength) {
      auto& buffer = stringBuffers.emplace_back(
          velox::AlignedBuffer::allocate<char>(totalLength, pool_.get()));
      return buffer->asMutable<void>();
    };
  };

  // RLE encoding
  {
    stringBuffers.clear();
    nimble::Vector<std::string_view> values{pool_.get()};
    values.push_back("apple");
    values.push_back("apple");
    values.push_back("banana");
    auto encoding =
        nimble::test::Encoder<nimble::RLEEncoding<std::string_view>>::
            createEncoding(*buffer_, values, makeStringBufferFactory());
    NIMBLE_ASSERT_THROW(
        encoding->seek(&seekValue, /*inclusive=*/true),
        "seek is not supported");
  }

  // Dictionary encoding
  {
    stringBuffers.clear();
    nimble::Vector<std::string_view> values{pool_.get()};
    values.push_back("apple");
    values.push_back("banana");
    auto encoding =
        nimble::test::Encoder<nimble::DictionaryEncoding<std::string_view>>::
            createEncoding(*buffer_, values, makeStringBufferFactory());
    NIMBLE_ASSERT_THROW(
        encoding->seek(&seekValue, /*inclusive=*/true),
        "seek is not supported");
  }

  // Constant encoding
  {
    stringBuffers.clear();
    nimble::Vector<std::string_view> values{pool_.get()};
    values.push_back("apple");
    auto encoding =
        nimble::test::Encoder<nimble::ConstantEncoding<std::string_view>>::
            createEncoding(*buffer_, values, makeStringBufferFactory());
    NIMBLE_ASSERT_THROW(
        encoding->seek(&seekValue, /*inclusive=*/true),
        "seek is not supported");
  }

  // MainlyConstant encoding
  {
    stringBuffers.clear();
    nimble::Vector<std::string_view> values{pool_.get()};
    values.push_back("apple");
    values.push_back("apple");
    values.push_back("banana");
    auto encoding = nimble::test::Encoder<
        nimble::MainlyConstantEncoding<std::string_view>>::
        createEncoding(*buffer_, values, makeStringBufferFactory());
    NIMBLE_ASSERT_THROW(
        encoding->seek(&seekValue, /*inclusive=*/true),
        "seek is not supported");
  }

  // Nullable encoding
  {
    stringBuffers.clear();
    nimble::Vector<std::string_view> values{pool_.get()};
    values.push_back("apple");
    values.push_back("banana");
    nimble::Vector<bool> nulls{pool_.get()};
    nulls.push_back(false);
    nulls.push_back(false);
    auto encoding =
        nimble::test::Encoder<nimble::NullableEncoding<std::string_view>>::
            createNullableEncoding(
                *buffer_, values, nulls, makeStringBufferFactory());
    NIMBLE_ASSERT_THROW(
        encoding->seek(&seekValue, /*inclusive=*/true),
        "seek is not supported");
  }

  // Trivial encoding for non-string types
  {
    nimble::Vector<int32_t> intValues{pool_.get()};
    intValues.push_back(1);
    intValues.push_back(2);
    auto encoding =
        nimble::test::Encoder<nimble::TrivialEncoding<int32_t>>::createEncoding(
            *buffer_, intValues, makeStringBufferFactory());
    int32_t intSeekValue = 1;
    NIMBLE_ASSERT_THROW(
        encoding->seek(&intSeekValue, /*inclusive=*/true),
        "seek is not supported");
  }

  // FixedBitWidth encoding (integer only)
  {
    nimble::Vector<uint32_t> intValues{pool_.get()};
    intValues.push_back(1);
    intValues.push_back(2);
    auto encoding =
        nimble::test::Encoder<nimble::FixedBitWidthEncoding<uint32_t>>::
            createEncoding(*buffer_, intValues, makeStringBufferFactory());
    uint32_t intSeekValue = 1;
    NIMBLE_ASSERT_THROW(
        encoding->seek(&intSeekValue, /*inclusive=*/true),
        "seek is not supported");
  }

  // Varint encoding (integer only)
  {
    nimble::Vector<uint32_t> intValues{pool_.get()};
    intValues.push_back(1);
    intValues.push_back(2);
    auto encoding =
        nimble::test::Encoder<nimble::VarintEncoding<uint32_t>>::createEncoding(
            *buffer_, intValues, makeStringBufferFactory());
    uint32_t intSeekValue = 1;
    NIMBLE_ASSERT_THROW(
        encoding->seek(&intSeekValue, /*inclusive=*/true),
        "seek is not supported");
  }

  // RLE encoding for integers
  {
    nimble::Vector<int32_t> intValues{pool_.get()};
    intValues.push_back(1);
    intValues.push_back(1);
    intValues.push_back(2);
    auto encoding =
        nimble::test::Encoder<nimble::RLEEncoding<int32_t>>::createEncoding(
            *buffer_, intValues, makeStringBufferFactory());
    int32_t intSeekValue = 1;
    NIMBLE_ASSERT_THROW(
        encoding->seek(&intSeekValue, /*inclusive=*/true),
        "seek is not supported");
  }

  // SparseBool encoding
  {
    nimble::Vector<bool> boolValues{pool_.get()};
    boolValues.push_back(true);
    boolValues.push_back(false);
    boolValues.push_back(true);
    auto encoding =
        nimble::test::Encoder<nimble::SparseBoolEncoding>::createEncoding(
            *buffer_, boolValues, makeStringBufferFactory());
    bool boolSeekValue = true;
    NIMBLE_ASSERT_THROW(
        encoding->seek(&boolSeekValue, /*inclusive=*/true),
        "seek is not supported");
  }
}

TEST_F(TrivialEncodingTest, getStringByRow) {
  const auto values = toVector({"alpha", "beta", "gamma", "delta", "epsilon"});
  auto encoding = createEncoding(values);

  for (uint32_t i = 0; i < values.size(); ++i) {
    SCOPED_TRACE(fmt::format("row={}", i));
    std::string_view result;
    encoding->get(i, &result);
    EXPECT_EQ(result, values[i]);
  }
}

TEST_F(TrivialEncodingTest, getStringSingleRow) {
  const auto values = toVector({"only"});
  auto encoding = createEncoding(values);

  std::string_view result;
  encoding->get(0, &result);
  EXPECT_EQ(result, "only");
}

TEST_F(TrivialEncodingTest, getStringRepeatedCalls) {
  const auto values = toVector({"aaa", "bbb", "ccc"});
  auto encoding = createEncoding(values);

  // Repeated get() calls on the same and different rows.
  std::string_view result;
  encoding->get(2, &result);
  EXPECT_EQ(result, "ccc");
  encoding->get(0, &result);
  EXPECT_EQ(result, "aaa");
  encoding->get(2, &result);
  EXPECT_EQ(result, "ccc");
  encoding->get(1, &result);
  EXPECT_EQ(result, "bbb");
}

TEST_F(TrivialEncodingTest, concurrentSeek) {
  constexpr uint32_t kNumThreads = 16;
  constexpr uint32_t kNumValues = 200;
  constexpr auto kDuration = std::chrono::seconds(5);

  nimble::Vector<std::string_view> sortedValues{pool_.get()};
  std::vector<std::string> stringBuffers;
  stringBuffers.reserve(kNumValues);
  for (uint32_t i = 0; i < kNumValues; ++i) {
    stringBuffers.push_back(fmt::format("key_{:03d}", i * 2));
  }
  std::sort(stringBuffers.begin(), stringBuffers.end());
  for (const auto& s : stringBuffers) {
    sortedValues.push_back(s);
  }

  // keyEncoding=true eagerly initializes seekValues_ for thread-safe access.
  auto encoding = createEncoding(
      sortedValues, nimble::Encoding::Options{.keyEncoding = true});

  auto linearSearch = [&sortedValues](
                          std::string_view target,
                          bool inclusive) -> std::optional<uint32_t> {
    for (size_t i = 0; i < sortedValues.size(); ++i) {
      const bool found =
          inclusive ? sortedValues[i] >= target : sortedValues[i] > target;
      if (found) {
        return static_cast<uint32_t>(i);
      }
    }
    return std::nullopt;
  };

  const auto deadline = std::chrono::steady_clock::now() + kDuration;
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (uint32_t t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937 rng(42 + t);
      std::uniform_int_distribution<uint32_t> targetDist(0, kNumValues * 2 + 1);
      while (std::chrono::steady_clock::now() < deadline) {
        const uint32_t targetIdx = targetDist(rng);
        const std::string targetKey = fmt::format("key_{:03d}", targetIdx);
        const std::string_view target = targetKey;
        for (bool inclusive : {true, false}) {
          const auto expected = linearSearch(target, inclusive);
          const auto actual = encoding->seek(&target, inclusive);
          ASSERT_EQ(expected, actual)
              << "key=" << targetKey << " inclusive=" << inclusive;
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(TrivialEncodingTest, concurrentGet) {
  constexpr uint32_t kNumThreads = 16;
  constexpr uint32_t kNumValues = 200;
  constexpr auto kDuration = std::chrono::seconds(5);

  nimble::Vector<std::string_view> sortedValues{pool_.get()};
  std::vector<std::string> stringBuffers;
  stringBuffers.reserve(kNumValues);
  for (uint32_t i = 0; i < kNumValues; ++i) {
    stringBuffers.push_back(fmt::format("key_{:03d}", i));
  }
  std::sort(stringBuffers.begin(), stringBuffers.end());
  for (const auto& s : stringBuffers) {
    sortedValues.push_back(s);
  }

  // keyEncoding=true eagerly initializes seekValues_ for thread-safe access.
  auto encoding = createEncoding(
      sortedValues, nimble::Encoding::Options{.keyEncoding = true});

  const auto deadline = std::chrono::steady_clock::now() + kDuration;
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (uint32_t t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937 rng(42 + t);
      std::uniform_int_distribution<uint32_t> rowDist(0, kNumValues - 1);
      while (std::chrono::steady_clock::now() < deadline) {
        const uint32_t row = rowDist(rng);
        std::string_view result;
        encoding->get(row, &result);
        ASSERT_EQ(result, sortedValues[row]) << "row=" << row;
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(TrivialEncodingTest, getUnsupportedEncodingThrows) {
  nimble::Vector<int32_t> intValues{pool_.get()};
  intValues.push_back(1);
  intValues.push_back(1);
  intValues.push_back(2);
  auto encoding =
      nimble::test::Encoder<nimble::RLEEncoding<int32_t>>::createEncoding(
          *buffer_, intValues, [&](uint32_t len) {
            auto& buf = stringBuffers_.emplace_back(
                velox::AlignedBuffer::allocate<char>(len, pool_.get()));
            return buf->asMutable<void>();
          });
  int32_t result;
  NIMBLE_ASSERT_THROW(
      encoding->get(0, &result), "get is not supported by this encoding type");
}
