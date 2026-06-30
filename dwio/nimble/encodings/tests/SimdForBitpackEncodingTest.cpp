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
#include "dwio/nimble/encodings/SimdForBitpackEncoding.h"

#include <fmt/core.h>
#include <gtest/gtest.h>
#include <random>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingLayout.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"
#include "dwio/nimble/encodings/selection/EncodingSelectionPolicy.h"
#include "velox/common/memory/Memory.h"

using namespace ::facebook;

namespace facebook::nimble::test {

template <typename T>
class SimdForBitpackEncodingTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  }

  std::unique_ptr<EncodingSelectionPolicy<T>> createSelectionPolicy() {
    EncodingLayout layout{
        EncodingType::SimdForBitpack, {}, CompressionType::Uncompressed};
    return std::make_unique<ReplayedEncodingSelectionPolicy<T>>(
        std::move(layout),
        CompressionOptions{},
        encodingSelectionPolicyCreator_);
  }

  std::unique_ptr<Encoding> encodeAndCreate(const std::vector<T>& values) {
    Buffer buffer{*pool_};
    auto encoded = EncodingFactory::encode<T>(
        createSelectionPolicy(),
        std::span<const T>{values.data(), values.size()},
        buffer);
    encodedStorage_.assign(encoded.begin(), encoded.end());
    return EncodingFactory().create(
        *pool_, {encodedStorage_.data(), encodedStorage_.size()}, nullptr);
  }

  void roundTripAndExpect(const std::vector<T>& values) {
    auto encoding = encodeAndCreate(values);
    EXPECT_EQ(encoding->encodingType(), EncodingType::SimdForBitpack);
    EXPECT_EQ(encoding->dataType(), TypeTraits<T>::dataType);
    EXPECT_EQ(encoding->rowCount(), values.size());

    std::vector<T> decoded(values.size());
    encoding->materialize(static_cast<uint32_t>(values.size()), decoded.data());
    for (size_t i = 0; i < values.size(); ++i) {
      EXPECT_EQ(decoded[i], values[i]) << "mismatch at index " << i;
    }
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::vector<char> encodedStorage_;
  ManualEncodingSelectionPolicyFactory manualPolicyFactory_;
  EncodingSelectionPolicyCreator encodingSelectionPolicyCreator_ =
      [this](DataType dataType) {
        return manualPolicyFactory_.createPolicy(dataType);
      };
};

using SimdForBitpackTypes =
    ::testing::Types<uint32_t, uint64_t, uint8_t, uint16_t>;
TYPED_TEST_SUITE(SimdForBitpackEncodingTest, SimdForBitpackTypes);

TYPED_TEST(SimdForBitpackEncodingTest, singleElement) {
  this->roundTripAndExpect({TypeParam{42}});
}

TYPED_TEST(SimdForBitpackEncodingTest, allSameValues) {
  std::vector<TypeParam> values(64, TypeParam{7});
  this->roundTripAndExpect(values);
}

TYPED_TEST(SimdForBitpackEncodingTest, exactlyOneGroup) {
  std::vector<TypeParam> values;
  values.reserve(32);
  for (uint32_t i = 0; i < 32; ++i) {
    values.push_back(static_cast<TypeParam>(100 + i));
  }
  this->roundTripAndExpect(values);
}

TYPED_TEST(SimdForBitpackEncodingTest, partialLastGroup) {
  for (uint32_t n : {1u, 15u, 31u, 33u, 63u, 65u, 100u}) {
    SCOPED_TRACE(fmt::format("n={}", n));
    std::vector<TypeParam> values;
    values.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
      values.push_back(static_cast<TypeParam>(10 + i % 50));
    }
    this->roundTripAndExpect(values);
  }
}

TYPED_TEST(SimdForBitpackEncodingTest, bitWidthZero) {
  std::vector<TypeParam> values(100, TypeParam{42});
  auto encoding = this->encodeAndCreate(values);
  std::vector<TypeParam> decoded(100);
  encoding->materialize(100, decoded.data());
  for (size_t i = 0; i < 100; ++i) {
    EXPECT_EQ(decoded[i], TypeParam{42});
  }
}

TYPED_TEST(SimdForBitpackEncodingTest, fullBitWidthRange) {
  std::vector<TypeParam> values;
  values.reserve(100);
  for (uint32_t i = 0; i < 100; ++i) {
    values.push_back(static_cast<TypeParam>(i));
  }
  values.push_back(std::numeric_limits<TypeParam>::max());
  this->roundTripAndExpect(values);
}

TYPED_TEST(SimdForBitpackEncodingTest, nonZeroBaseline) {
  std::vector<TypeParam> values;
  values.reserve(200);
  const TypeParam base =
      static_cast<TypeParam>(std::numeric_limits<TypeParam>::max() / 2);
  for (uint32_t i = 0; i < 200; ++i) {
    values.push_back(static_cast<TypeParam>(base + i % 30));
  }
  this->roundTripAndExpect(values);
}

TYPED_TEST(SimdForBitpackEncodingTest, skipAndMaterialize) {
  std::vector<TypeParam> values;
  values.reserve(300);
  for (uint32_t i = 0; i < 300; ++i) {
    values.push_back(static_cast<TypeParam>(50 + (i % 100)));
  }
  auto encoding = this->encodeAndCreate(values);

  encoding->reset();
  std::vector<TypeParam> chunk(50);
  encoding->materialize(50, chunk.data());
  for (uint32_t i = 0; i < 50; ++i) {
    EXPECT_EQ(chunk[i], values[i]);
  }

  encoding->skip(75);
  encoding->materialize(50, chunk.data());
  for (uint32_t i = 0; i < 50; ++i) {
    EXPECT_EQ(chunk[i], values[125 + i]) << "i=" << i;
  }

  encoding->skip(75);
  encoding->materialize(50, chunk.data());
  for (uint32_t i = 0; i < 50; ++i) {
    EXPECT_EQ(chunk[i], values[250 + i]) << "i=" << i;
  }

  encoding->reset();
  std::vector<TypeParam> all(values.size());
  encoding->materialize(static_cast<uint32_t>(values.size()), all.data());
  for (size_t i = 0; i < values.size(); ++i) {
    EXPECT_EQ(all[i], values[i]) << "after-reset i=" << i;
  }
}

TYPED_TEST(SimdForBitpackEncodingTest, skipAcrossGroupBoundary) {
  std::vector<TypeParam> values;
  values.reserve(128);
  for (uint32_t i = 0; i < 128; ++i) {
    values.push_back(static_cast<TypeParam>(i % 64));
  }
  auto encoding = this->encodeAndCreate(values);

  encoding->skip(30);
  std::vector<TypeParam> chunk(4);
  encoding->materialize(4, chunk.data());
  for (uint32_t i = 0; i < 4; ++i) {
    EXPECT_EQ(chunk[i], values[30 + i]) << "i=" << i;
  }
}

TYPED_TEST(SimdForBitpackEncodingTest, debugString) {
  std::vector<TypeParam> values;
  values.reserve(8);
  for (uint32_t i = 0; i < 8; ++i) {
    values.push_back(static_cast<TypeParam>(i + 1));
  }
  auto encoding = this->encodeAndCreate(values);
  const std::string debug = encoding->debugString();
  EXPECT_NE(debug.find("SimdForBitpack"), std::string::npos);
  EXPECT_NE(debug.find("bitWidth="), std::string::npos);
}

class SimdForBitpackFuzzerTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  }

  template <typename T>
  void runFuzzer(uint32_t seed, uint32_t numIterations) {
    std::mt19937 rng(seed);

    ManualEncodingSelectionPolicyFactory manualFactory;
    EncodingSelectionPolicyCreator creator =
        [&manualFactory](DataType dataType) {
          return manualFactory.createPolicy(dataType);
        };

    for (uint32_t iter = 0; iter < numIterations; ++iter) {
      SCOPED_TRACE(fmt::format("iteration {}", iter));

      std::uniform_int_distribution<uint32_t> sizeDist(1, 1000);
      const uint32_t numValues = sizeDist(rng);

      std::uniform_int_distribution<uint32_t> valueDist(
          0, sizeof(T) >= 4 ? (1u << 20) : std::numeric_limits<T>::max());
      std::vector<T> values;
      values.reserve(numValues);
      const T offset = static_cast<T>(1000);
      for (uint32_t i = 0; i < numValues; ++i) {
        values.push_back(static_cast<T>(offset + valueDist(rng)));
      }

      Buffer buffer{*pool_};
      EncodingLayout layout{
          EncodingType::SimdForBitpack, {}, CompressionType::Uncompressed};
      auto policy = std::make_unique<ReplayedEncodingSelectionPolicy<T>>(
          std::move(layout), CompressionOptions{}, creator);
      auto encoded = EncodingFactory::encode<T>(
          std::move(policy),
          std::span<const T>{values.data(), values.size()},
          buffer);
      std::vector<char> storage(encoded.begin(), encoded.end());
      auto encoding = EncodingFactory().create(
          *pool_, {storage.data(), storage.size()}, nullptr);

      ASSERT_EQ(encoding->encodingType(), EncodingType::SimdForBitpack);
      ASSERT_EQ(encoding->rowCount(), values.size());
      std::vector<T> decoded(values.size());
      encoding->materialize(
          static_cast<uint32_t>(values.size()), decoded.data());
      for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_EQ(decoded[i], values[i])
            << "iter=" << iter << " i=" << i << " size=" << numValues;
      }

      encoding->reset();
      std::uniform_int_distribution<uint32_t> stepDist(0, numValues);
      uint32_t cursor = 0;
      while (cursor < numValues) {
        const uint32_t remaining = numValues - cursor;
        const uint32_t skipCount = stepDist(rng) % (remaining + 1);
        if (skipCount > 0) {
          encoding->skip(skipCount);
          cursor += skipCount;
        }
        if (cursor >= numValues) {
          break;
        }
        const uint32_t matCount =
            std::max<uint32_t>(1, stepDist(rng) % (numValues - cursor + 1));
        std::vector<T> chunk(matCount);
        encoding->materialize(matCount, chunk.data());
        for (uint32_t i = 0; i < matCount; ++i) {
          ASSERT_EQ(chunk[i], values[cursor + i])
              << "iter=" << iter << " cursor=" << cursor << " i=" << i;
        }
        cursor += matCount;
      }
    }
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

TEST_F(SimdForBitpackFuzzerTest, fuzzerUint8) {
  runFuzzer<uint8_t>(/*seed=*/11111, /*numIterations=*/30);
}

TEST_F(SimdForBitpackFuzzerTest, fuzzerUint16) {
  runFuzzer<uint16_t>(/*seed=*/22222, /*numIterations=*/30);
}

TEST_F(SimdForBitpackFuzzerTest, fuzzerUint32) {
  runFuzzer<uint32_t>(/*seed=*/33333, /*numIterations=*/30);
}

TEST_F(SimdForBitpackFuzzerTest, fuzzerUint64) {
  runFuzzer<uint64_t>(/*seed=*/44444, /*numIterations=*/30);
}

TEST_F(SimdForBitpackFuzzerTest, fuzzerInt32) {
  runFuzzer<int32_t>(/*seed=*/55555, /*numIterations=*/30);
}

TEST_F(SimdForBitpackFuzzerTest, fuzzerInt64) {
  runFuzzer<int64_t>(/*seed=*/66666, /*numIterations=*/30);
}

TEST_F(SimdForBitpackFuzzerTest, encodeRejectsEmpty) {
  Buffer buffer{*pool_};
  EncodingLayout layout{
      EncodingType::SimdForBitpack, {}, CompressionType::Uncompressed};
  ManualEncodingSelectionPolicyFactory manualFactory;
  EncodingSelectionPolicyCreator creator = [&manualFactory](DataType dataType) {
    return manualFactory.createPolicy(dataType);
  };
  auto policy = std::make_unique<ReplayedEncodingSelectionPolicy<uint32_t>>(
      std::move(layout), CompressionOptions{}, creator);
  std::vector<uint32_t> empty;
  NIMBLE_ASSERT_THROW(
      EncodingFactory::encode<uint32_t>(
          std::move(policy),
          std::span<const uint32_t>{empty.data(), empty.size()},
          buffer),
      "SimdForBitpack encoding cannot be used with 0 rows.");
}

} // namespace facebook::nimble::test
