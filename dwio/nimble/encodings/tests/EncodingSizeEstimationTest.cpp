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
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/common/EncodingType.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "velox/common/memory/Memory.h"

using namespace facebook;
using namespace facebook::nimble;

class EncodingSizeEstimationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

TEST_F(EncodingSizeEstimationTest, numericConstant) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data(100, 42);
  auto stats = Statistics<uint32_t>::create(data);

  auto size = Est::estimateNumericSize(EncodingType::Constant, 100, stats);
  ASSERT_TRUE(size.has_value());
  EXPECT_GT(size.value(), 0);
  EXPECT_LT(size.value(), 100 * sizeof(uint32_t));
}

TEST_F(EncodingSizeEstimationTest, numericConstantNonConstantData) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data = {1, 2, 3};
  auto stats = Statistics<uint32_t>::create(data);

  auto size = Est::estimateNumericSize(EncodingType::Constant, 3, stats);
  EXPECT_FALSE(size.has_value());
}

TEST_F(EncodingSizeEstimationTest, numericTrivial) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data = {1, 2, 3, 4, 5};
  auto stats = Statistics<uint32_t>::create(data);

  auto size = Est::estimateNumericSize(EncodingType::Trivial, 5, stats);
  ASSERT_TRUE(size.has_value());
  EXPECT_GE(size.value(), 5 * sizeof(uint32_t));
}

TEST_F(EncodingSizeEstimationTest, numericFixedBitWidth) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data = {100, 101, 102, 103, 104};
  auto stats = Statistics<uint32_t>::create(data);

  auto size = Est::estimateNumericSize(EncodingType::FixedBitWidth, 5, stats);
  ASSERT_TRUE(size.has_value());
  EXPECT_LT(size.value(), 5 * sizeof(uint32_t));
}

TEST_F(EncodingSizeEstimationTest, numericDictionary) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data = {1, 2, 1, 2, 1, 2, 1, 2, 1, 2};
  auto stats = Statistics<uint32_t>::create(data);

  auto size = Est::estimateNumericSize(EncodingType::Dictionary, 10, stats);
  ASSERT_TRUE(size.has_value());
  EXPECT_GT(size.value(), 0);
}

TEST_F(EncodingSizeEstimationTest, numericMainlyConstant) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data(100, 42);
  data[50] = 99;
  auto stats = Statistics<uint32_t>::create(data);

  auto size =
      Est::estimateNumericSize(EncodingType::MainlyConstant, 100, stats);
  ASSERT_TRUE(size.has_value());
  EXPECT_LT(size.value(), 100 * sizeof(uint32_t));
}

TEST_F(EncodingSizeEstimationTest, numericRle) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data;
  for (int i = 0; i < 50; ++i) {
    data.push_back(1);
  }
  for (int i = 0; i < 50; ++i) {
    data.push_back(2);
  }
  auto stats = Statistics<uint32_t>::create(data);

  auto size = Est::estimateNumericSize(EncodingType::RLE, 100, stats);
  ASSERT_TRUE(size.has_value());
  EXPECT_LT(size.value(), 100 * sizeof(uint32_t));
}

TEST_F(EncodingSizeEstimationTest, numericVarint32) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data = {0, 1, 127, 128, 255, 256, 1000};
  auto stats = Statistics<uint32_t>::create(data);

  auto size = Est::estimateNumericSize(EncodingType::Varint, 7, stats);
  ASSERT_TRUE(size.has_value());
  EXPECT_GT(size.value(), 0);
}

TEST_F(EncodingSizeEstimationTest, numericVarint64) {
  using Est = detail::EncodingSizeEstimation<uint64_t, true>;

  std::vector<uint64_t> data = {0, 1, 127, 128, 16384, 1000000};
  auto stats = Statistics<uint64_t>::create(data);

  auto size = Est::estimateNumericSize(EncodingType::Varint, 6, stats);
  ASSERT_TRUE(size.has_value());
  EXPECT_GT(size.value(), 0);
}

TEST_F(EncodingSizeEstimationTest, numericUnsupportedEncoding) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data = {1, 2, 3};
  auto stats = Statistics<uint32_t>::create(data);

  auto size = Est::estimateNumericSize(EncodingType::SparseBool, 3, stats);
  EXPECT_FALSE(size.has_value());
}

TEST_F(EncodingSizeEstimationTest, numericEstimateSizeDispatches) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data = {1, 2, 3, 4, 5};
  auto stats = Statistics<uint32_t>::create(data);

  auto size = Est::estimateSize(EncodingType::Trivial, 5, stats);
  ASSERT_TRUE(size.has_value());

  auto directSize = Est::estimateNumericSize(EncodingType::Trivial, 5, stats);
  EXPECT_EQ(size.value(), directSize.value());
}

TEST_F(EncodingSizeEstimationTest, bitPackedBytesZeroRange) {
  using EstNonFixed = detail::EncodingSizeEstimation<uint32_t, false>;
  auto result = EstNonFixed::bitPackedBytes(0, 0, 100);
  EXPECT_GT(result, 0u);

  using EstFixed = detail::EncodingSizeEstimation<uint32_t, true>;
  auto fixedResult = EstFixed::bitPackedBytes(0, 0, 100);
  EXPECT_GE(fixedResult, result);
}

TEST_F(EncodingSizeEstimationTest, bitPackedBytesSmallRange) {
  using EstNonFixed = detail::EncodingSizeEstimation<uint32_t, false>;
  EXPECT_EQ(EstNonFixed::bitPackedBytes(0, 1, 8), 1);

  using EstFixed = detail::EncodingSizeEstimation<uint32_t, true>;
  auto fixedResult = EstFixed::bitPackedBytes(0, 1, 8);
  EXPECT_GE(fixedResult, 1u);
}

TEST_F(EncodingSizeEstimationTest, bitPackedBytesLargeRange) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;
  EXPECT_GT(Est::bitPackedBytes(0, 255, 100), 0u);
  EXPECT_GT(Est::bitPackedBytes(0, 65535, 100), 0u);
}

TEST_F(EncodingSizeEstimationTest, bitPackedBytesWithBaseline) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;
  auto sizeFromZero = Est::bitPackedBytes(0, 4, 100);
  auto sizeWithBaseline = Est::bitPackedBytes(100, 104, 100);
  EXPECT_EQ(sizeFromZero, sizeWithBaseline);
}

TEST_F(EncodingSizeEstimationTest, bitPackedBytesFixedVsNonFixed) {
  using EstFixed = detail::EncodingSizeEstimation<uint32_t, true>;
  using EstNonFixed = detail::EncodingSizeEstimation<uint32_t, false>;

  auto fixed = EstFixed::bitPackedBytes(0, 5, 100);
  auto nonFixed = EstNonFixed::bitPackedBytes(0, 5, 100);
  EXPECT_GE(fixed, nonFixed);
}

TEST_F(EncodingSizeEstimationTest, getEncodingOverheadAllTypes) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  EXPECT_GT(
      (Est::template getEncodingOverhead<EncodingType::Trivial, uint32_t>()),
      0u);
  EXPECT_GT(
      (Est::template getEncodingOverhead<EncodingType::Constant, uint32_t>()),
      0u);
  EXPECT_GT(
      (Est::template getEncodingOverhead<
          EncodingType::FixedBitWidth,
          uint32_t>()),
      0u);
  EXPECT_GT(
      (Est::template getEncodingOverhead<EncodingType::Dictionary, uint32_t>()),
      0u);
  EXPECT_GT(
      (Est::template getEncodingOverhead<EncodingType::RLE, uint32_t>()), 0u);
  EXPECT_GT(
      (Est::template getEncodingOverhead<EncodingType::Varint, uint32_t>()),
      0u);
  EXPECT_GT(
      (Est::template getEncodingOverhead<
          EncodingType::MainlyConstant,
          uint32_t>()),
      0u);
  EXPECT_GT(
      (Est::template getEncodingOverhead<EncodingType::SparseBool, bool>()),
      0u);
  EXPECT_GT(
      (Est::template getEncodingOverhead<EncodingType::Nullable, uint32_t>()),
      0u);
}

TEST_F(EncodingSizeEstimationTest, trivialSmallerThanDictionaryForUnique) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data;
  for (uint32_t i = 0; i < 100; ++i) {
    data.push_back(i);
  }
  auto stats = Statistics<uint32_t>::create(data);

  auto trivialSize =
      Est::estimateNumericSize(EncodingType::Trivial, 100, stats);
  auto dictSize =
      Est::estimateNumericSize(EncodingType::Dictionary, 100, stats);

  ASSERT_TRUE(trivialSize.has_value());
  ASSERT_TRUE(dictSize.has_value());
  EXPECT_LT(trivialSize.value(), dictSize.value());
}

TEST_F(EncodingSizeEstimationTest, dictionarySmallerForHighRepetition) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data;
  for (int i = 0; i < 1000; ++i) {
    data.push_back(i % 3);
  }
  auto stats = Statistics<uint32_t>::create(data);

  auto trivialSize =
      Est::estimateNumericSize(EncodingType::Trivial, 1000, stats);
  auto dictSize =
      Est::estimateNumericSize(EncodingType::Dictionary, 1000, stats);

  ASSERT_TRUE(trivialSize.has_value());
  ASSERT_TRUE(dictSize.has_value());
  EXPECT_LT(dictSize.value(), trivialSize.value());
}

TEST_F(EncodingSizeEstimationTest, constantSmallestForConstantData) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data(1000, 42);
  auto stats = Statistics<uint32_t>::create(data);

  auto constantSize =
      Est::estimateNumericSize(EncodingType::Constant, 1000, stats);
  auto trivialSize =
      Est::estimateNumericSize(EncodingType::Trivial, 1000, stats);

  ASSERT_TRUE(constantSize.has_value());
  ASSERT_TRUE(trivialSize.has_value());
  EXPECT_LT(constantSize.value(), trivialSize.value());
}

TEST_F(EncodingSizeEstimationTest, rleSmallForLongRuns) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data;
  for (int i = 0; i < 500; ++i) {
    data.push_back(1);
  }
  for (int i = 0; i < 500; ++i) {
    data.push_back(2);
  }
  auto stats = Statistics<uint32_t>::create(data);

  auto rleSize = Est::estimateNumericSize(EncodingType::RLE, 1000, stats);
  auto trivialSize =
      Est::estimateNumericSize(EncodingType::Trivial, 1000, stats);

  ASSERT_TRUE(rleSize.has_value());
  ASSERT_TRUE(trivialSize.has_value());
  EXPECT_LT(rleSize.value(), trivialSize.value());
}

TEST_F(EncodingSizeEstimationTest, fbwSmallForNarrowRange) {
  using Est = detail::EncodingSizeEstimation<uint32_t, true>;

  std::vector<uint32_t> data;
  for (uint32_t i = 0; i < 1000; ++i) {
    data.push_back(1000 + (i % 4));
  }
  auto stats = Statistics<uint32_t>::create(data);

  auto fbwSize =
      Est::estimateNumericSize(EncodingType::FixedBitWidth, 1000, stats);
  auto trivialSize =
      Est::estimateNumericSize(EncodingType::Trivial, 1000, stats);

  ASSERT_TRUE(fbwSize.has_value());
  ASSERT_TRUE(trivialSize.has_value());
  EXPECT_LT(fbwSize.value(), trivialSize.value());
}

TEST_F(EncodingSizeEstimationTest, uint64AllEncodings) {
  using Est = detail::EncodingSizeEstimation<uint64_t, true>;

  std::vector<uint64_t> data;
  for (uint64_t i = 0; i < 100; ++i) {
    data.push_back(i);
  }
  auto stats = Statistics<uint64_t>::create(data);

  ASSERT_TRUE(
      Est::estimateNumericSize(EncodingType::Trivial, 100, stats).has_value());
  ASSERT_TRUE(
      Est::estimateNumericSize(EncodingType::FixedBitWidth, 100, stats)
          .has_value());
  ASSERT_TRUE(
      Est::estimateNumericSize(EncodingType::Dictionary, 100, stats)
          .has_value());
  ASSERT_TRUE(
      Est::estimateNumericSize(EncodingType::RLE, 100, stats).has_value());
  ASSERT_TRUE(
      Est::estimateNumericSize(EncodingType::Varint, 100, stats).has_value());
}

TEST_F(EncodingSizeEstimationTest, pforEstimateVsActualSize) {
  using Est = detail::EncodingSizeEstimation<uint32_t, false>;

  // ~90% values in [0, 100], ~10% outliers near max — Pfor's sweet spot.
  std::vector<uint32_t> data;
  for (uint32_t i = 0; i < 900; ++i) {
    data.push_back(i % 100);
  }
  for (uint32_t i = 0; i < 100; ++i) {
    data.push_back(1'000'000 + i);
  }
  auto stats = Statistics<uint32_t>::create(data);
  const uint32_t numValues = static_cast<uint32_t>(data.size());

  auto estimated =
      Est::estimateNumericSize(EncodingType::Pfor, numValues, stats);
  ASSERT_TRUE(estimated.has_value());

  // Actually encode and measure real size.
  nimble::Buffer buffer{*pool_};
  auto encoded = nimble::test::Encoder<nimble::PforEncoding<uint32_t>>::encode(
      buffer, nimble::Vector<uint32_t>(pool_.get(), data.begin(), data.end()));
  const uint64_t actualSize = encoded.size();

  // Estimate should be in the same ballpark as actual — within 2x.
  EXPECT_GT(estimated.value(), actualSize / 2)
      << "estimate too low: " << estimated.value() << " vs actual "
      << actualSize;
  EXPECT_LT(estimated.value(), actualSize * 2)
      << "estimate too high: " << estimated.value() << " vs actual "
      << actualSize;
}

TEST_F(EncodingSizeEstimationTest, doubleDeltaEstimateVsActualSize) {
  using Est = detail::EncodingSizeEstimation<uint64_t, false>;

  // Monotone timestamps with small jitter — DoubleDelta's sweet spot.
  std::vector<uint64_t> data;
  uint64_t current = 1'000'000;
  for (uint32_t i = 0; i < 200; ++i) {
    data.push_back(current);
    current += 60 + (i % 3);
  }
  auto stats = Statistics<uint64_t>::create(data);
  const uint32_t numValues = static_cast<uint32_t>(data.size());

  auto estimated =
      Est::estimateNumericSize(EncodingType::DoubleDelta, numValues, stats);
  ASSERT_TRUE(estimated.has_value());

  nimble::Buffer buffer{*pool_};
  auto encoded =
      nimble::test::Encoder<nimble::DoubleDeltaEncoding<uint64_t>>::encode(
          buffer,
          nimble::Vector<uint64_t>(pool_.get(), data.begin(), data.end()));
  const uint64_t actualSize = encoded.size();

  // DoubleDelta uses a conservative heuristic (perStepMagnitude × 2),
  // so the estimate can be up to ~3x actual for regular-interval data.
  EXPECT_GT(estimated.value(), actualSize / 3)
      << "estimate too low: " << estimated.value() << " vs actual "
      << actualSize;
  EXPECT_LT(estimated.value(), actualSize * 3)
      << "estimate too high: " << estimated.value() << " vs actual "
      << actualSize;
}
