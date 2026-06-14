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
#include <array>
#include <limits>

#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/BlockBitPackingEncoding.h"
#include "dwio/nimble/encodings/ConstantEncoding.h"
#include "dwio/nimble/encodings/DictionaryEncoding.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/MainlyConstantEncoding.h"
#include "dwio/nimble/encodings/RLEEncoding.h"
#include "dwio/nimble/encodings/SparseBoolEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/VarintEncoding.h"

#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "velox/common/memory/Memory.h"

using namespace facebook;
using namespace facebook::nimble;

class EncodingSizeEstimationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  }

  template <typename T>
  void verifySimdForBitpackEstimateVsActualSize() {
    using Est = detail::EncodingSizeEstimation<T, false>;

    const T min = std::numeric_limits<T>::min();
    const T max = std::numeric_limits<T>::max();
    const T base = static_cast<T>(max / 4);
    const std::vector<std::vector<T>> testCases = {
        std::vector<T>(1000, min),
        std::vector<T>(1000, max),
        {min, max},
        [&] {
          std::vector<T> data;
          data.reserve(1000);
          for (uint32_t i = 0; i < 1000; ++i) {
            data.push_back(static_cast<T>(base + i % 50));
          }
          return data;
        }(),
    };

    for (const auto& data : testCases) {
      SCOPED_TRACE(static_cast<uint64_t>(max));
      SCOPED_TRACE(static_cast<uint64_t>(data.front()));
      auto stats = Statistics<T>::create(data);
      const uint32_t numValues = static_cast<uint32_t>(data.size());

      auto estimated = Est::estimateNumericSize(
          EncodingType::SimdForBitpack, numValues, stats);
      ASSERT_TRUE(estimated.has_value());

      nimble::Buffer buffer{*pool_};
      auto encoded =
          nimble::test::Encoder<nimble::SimdForBitpackEncoding<T>>::encode(
              buffer, nimble::Vector<T>(pool_.get(), data.begin(), data.end()));
      const uint64_t actualSize = encoded.size();

      EXPECT_GT(estimated.value(), actualSize / 2)
          << "estimate too low: " << estimated.value() << " vs actual "
          << actualSize;
      EXPECT_LT(estimated.value(), actualSize * 2)
          << "estimate too high: " << estimated.value() << " vs actual "
          << actualSize;
    }
  }

  template <typename T>
  void verifyBlockBitPackingEstimateVsActualSize() {
    std::vector<T> data;
    data.reserve(2050);
    for (uint32_t i = 0; i < 1024; ++i) {
      data.push_back(static_cast<T>(1000 + i % 4));
    }
    for (uint32_t i = 0; i < 1024; ++i) {
      data.push_back(static_cast<T>(1'000'000 + i % 8));
    }
    data.push_back(std::numeric_limits<T>::min());
    data.push_back(std::numeric_limits<T>::max());

    const auto values = std::span<const T>{data.data(), data.size()};
    const auto estimated = BlockBitPackingEncoding<T>::estimateSize(values);

    nimble::Buffer buffer{*pool_};
    auto encoded =
        nimble::test::Encoder<nimble::BlockBitPackingEncoding<T>>::encode(
            buffer, nimble::Vector<T>(pool_.get(), data.begin(), data.end()));

    // estimateSize() estimates the per-block metadata as Trivial sub-encodings,
    // while the actual encoding routes that metadata through nested encoding
    // selection (whose size is data-dependent), so the estimate is an
    // approximation rather than exact. Allow a 2x band, matching the other
    // estimate checks.
    EXPECT_GT(estimated, encoded.size() / 2)
        << "estimate too low: " << estimated << " vs actual " << encoded.size();
    EXPECT_LT(estimated, encoded.size() * 2)
        << "estimate too high: " << estimated << " vs actual "
        << encoded.size();
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

TEST_F(EncodingSizeEstimationTest, fixedBitWidthEstimateUsesValueRange) {
  const auto sizeFromZero = FixedBitWidthEncoding<uint32_t>::estimateSize(
      /*rowCount=*/100,
      /*minValue=*/0,
      /*maxValue=*/4,
      /*fixedByteWidth=*/true);
  const auto sizeWithBaseline = FixedBitWidthEncoding<uint32_t>::estimateSize(
      /*rowCount=*/100,
      /*minValue=*/100,
      /*maxValue=*/104,
      /*fixedByteWidth=*/true);
  EXPECT_EQ(sizeFromZero, sizeWithBaseline);

  const auto fixedByteWidthSize = FixedBitWidthEncoding<uint32_t>::estimateSize(
      /*rowCount=*/100,
      /*minValue=*/0,
      /*maxValue=*/5,
      /*fixedByteWidth=*/true);
  const auto bitWidthSize = FixedBitWidthEncoding<uint32_t>::estimateSize(
      /*rowCount=*/100,
      /*minValue=*/0,
      /*maxValue=*/5,
      /*fixedByteWidth=*/false);
  EXPECT_GE(fixedByteWidthSize, bitWidthSize);
}

TEST_F(EncodingSizeEstimationTest, encodingEstimateHelpersReturnSizes) {
  const std::vector<uint32_t> numericData = {1, 1, 1, 2, 3, 3};
  auto numericStats = Statistics<uint32_t>::create(numericData);
  const std::array<bool, 4> boolData = {true, true, false, true};
  auto boolStats = Statistics<bool>::create(boolData);

  EXPECT_GT(TrivialEncoding<uint32_t>::estimateSize(numericData.size()), 0u);
  EXPECT_GT(
      ConstantEncoding<uint32_t>::estimateSize(
          Statistics<uint32_t>::create(std::vector<uint32_t>{1, 1}))
          .value(),
      0u);
  EXPECT_GT(
      FixedBitWidthEncoding<uint32_t>::estimateSize(
          numericData.size(), numericStats, true),
      0u);
  EXPECT_GT(
      DictionaryEncoding<uint32_t>::estimateSize(
          numericData.size(), numericStats, true),
      0u);
  EXPECT_GT(
      RLEEncoding<uint32_t>::estimateSize(
          numericData.size(), numericStats, true),
      0u);
  EXPECT_GT(VarintEncoding<uint32_t>::estimateSize(numericStats), 0u);
  EXPECT_GT(
      BlockBitPackingEncoding<uint32_t>::estimateSize(
          std::span<const uint32_t>{numericData.data(), numericData.size()}),
      0u);
  EXPECT_GT(
      MainlyConstantEncoding<uint32_t>::estimateSize(
          numericData.size(), numericStats, true),
      0u);
  EXPECT_GT(
      SparseBoolEncoding::estimateSize(boolData.size(), boolStats, true), 0u);
}

TEST_F(
    EncodingSizeEstimationTest,
    mainlyConstantStringEstimateUsesUncommonLengthRange) {
  const std::string common(100, 'x');
  const std::vector<std::string_view> data = {
      common,
      common,
      common,
      common,
      common,
      "aa",
      "bbb",
  };
  auto stats = Statistics<std::string_view>::create(data);

  const uint64_t expectedSize =
      EncodingPrefix::kFixedPrefixSize + 2 * sizeof(uint32_t) + common.size() +
      sizeof(uint32_t) +
      TrivialEncoding<std::string_view>::estimateSize(
          /*rowCount=*/2,
          /*totalStringsLength=*/5,
          /*minLength=*/2,
          /*maxLength=*/3,
          /*fixedByteWidth=*/false) +
      SparseBoolEncoding::estimateSize(
          data.size(), /*exceptionCount=*/2, /*fixedByteWidth=*/false);

  EXPECT_EQ(
      MainlyConstantEncoding<std::string_view>::estimateSize(
          data.size(), stats, /*fixedByteWidth=*/false),
      expectedSize);
}

TEST_F(EncodingSizeEstimationTest, sparseBoolEstimateIncludesSentinelIndex) {
  const uint64_t indicesSize = FixedBitWidthEncoding<uint32_t>::estimateSize(
      /*rowCount=*/3,
      /*minValue=*/0,
      /*maxValue=*/10,
      /*fixedByteWidth=*/false);
  const uint64_t expectedSize =
      EncodingPrefix::kFixedPrefixSize + sizeof(uint8_t) + indicesSize;

  EXPECT_EQ(
      SparseBoolEncoding::estimateSize(
          /*rowCount=*/10,
          /*exceptionCount=*/2,
          /*fixedByteWidth=*/false),
      expectedSize);
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
      Est::estimateNumericSize(EncodingType::PFOR, numValues, stats);
  ASSERT_TRUE(estimated.has_value());

  // Actually encode and measure real size.
  nimble::Buffer buffer{*pool_};
  auto encoded = nimble::test::Encoder<nimble::PFOREncoding<uint32_t>>::encode(
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

TEST_F(EncodingSizeEstimationTest, simdForBitpackEstimateVsActualSize) {
  verifySimdForBitpackEstimateVsActualSize<uint8_t>();
  verifySimdForBitpackEstimateVsActualSize<uint16_t>();
  verifySimdForBitpackEstimateVsActualSize<uint32_t>();
  verifySimdForBitpackEstimateVsActualSize<uint64_t>();
}

TEST_F(EncodingSizeEstimationTest, blockBitPackingEstimateVsActualSize) {
  verifyBlockBitPackingEstimateVsActualSize<uint32_t>();
  verifyBlockBitPackingEstimateVsActualSize<uint64_t>();
}
