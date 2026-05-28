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
#include "dwio/nimble/encodings/CompactForEncoding.h"

#include <fmt/core.h>
#include <gtest/gtest.h>
#include <limits>
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

// CompactFor is templated on int64_t and uint64_t. The encoder operates
// on the unsigned physical representation; per-type tests therefore run on
// the unsigned physical type directly. End-to-end signed-input coverage is
// exercised by the fuzzer below, which routes int64 through
// EncodingFactory::encode<int64_t>.
template <typename T>
class CompactForEncodingTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  }

  std::unique_ptr<EncodingSelectionPolicy<T>> createSelectionPolicy() {
    EncodingLayout layout{
        EncodingType::CompactFor, {}, CompressionType::Uncompressed};
    return std::make_unique<ReplayedEncodingSelectionPolicy<T>>(
        std::move(layout),
        CompressionOptions{},
        encodingSelectionPolicyFactory_);
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
    EXPECT_EQ(encoding->encodingType(), EncodingType::CompactFor);
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
  EncodingSelectionPolicyFactory encodingSelectionPolicyFactory_ =
      [this](DataType dataType) {
        return manualPolicyFactory_.createPolicy(dataType);
      };
};

using CompactForTypes = ::testing::Types<int64_t, uint64_t>;
TYPED_TEST_SUITE(CompactForEncodingTest, CompactForTypes);

TYPED_TEST(CompactForEncodingTest, singleElement) {
  using T = TypeParam;
  this->roundTripAndExpect({T{42}});
}

TYPED_TEST(CompactForEncodingTest, allEqualCollapsesToBitWidthZero) {
  // All values equal → range=0 → bitWidth=0 → wire is just the header
  // (Encoding prefix + 1B baseline varint + 1B bitWidth). Verifies the
  // bitWidth=0 fast path.
  using T = TypeParam;
  std::vector<T> values(64, T{7});
  this->roundTripAndExpect(values);
  auto encoding = this->encodeAndCreate(values);
  // 6 (Encoding prefix) + 1 (baseline=7 varint, single byte) + 1 (bitWidth=0).
  EXPECT_EQ(this->encodedStorage_.size(), 6u + 1u + 1u);
}

TYPED_TEST(CompactForEncodingTest, regularTimestampsBitWidthFits) {
  // Timestamps stepping by 1000 from 1.7e9. min ~ 1.7e9, max ~ 1.7e9 +
  // 1023*1000. Range ~ 1M → bitWidth = 20 — typical real-world long trait
  // shape, well inside the bitWidth<=56 byte-aligned single-load fast path.
  using T = TypeParam;
  std::vector<T> values;
  values.reserve(1024);
  for (uint32_t i = 0; i < 1024; ++i) {
    values.push_back(static_cast<T>(1'700'000'000ULL + i * 1000ULL));
  }
  this->roundTripAndExpect(values);
}

TYPED_TEST(CompactForEncodingTest, sparseOutliers) {
  // Mostly-narrow stream with a few jumps. LFB has no exception path — every
  // value pays the worst-case bit width. Tests that the bit width is correctly
  // sized to cover the outliers.
  using T = TypeParam;
  std::vector<T> values;
  values.reserve(500);
  for (uint32_t i = 0; i < 500; ++i) {
    T v = static_cast<T>(1'000'000ULL + i * 60ULL);
    if (i == 100 || i == 250 || i == 400) {
      v = static_cast<T>(static_cast<uint64_t>(v) + 5'000'000ULL);
    }
    values.push_back(v);
  }
  this->roundTripAndExpect(values);
}

TYPED_TEST(CompactForEncodingTest, mixedSizes) {
  using T = TypeParam;
  for (uint32_t n :
       {1u, 2u, 3u, 7u, 8u, 9u, 16u, 17u, 64u, 100u, 257u, 1024u}) {
    SCOPED_TRACE(fmt::format("n={}", n));
    std::vector<T> values;
    values.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
      values.push_back(static_cast<T>(1000ULL + i * 37ULL));
    }
    this->roundTripAndExpect(values);
  }
}

TYPED_TEST(CompactForEncodingTest, fullBitWidth64) {
  // Force bitWidth=64: a single max-range value pair. Exercises the bw>56
  // cross-word fallback inside FixedBitArray::bulkGet64WithBaseline.
  using T = TypeParam;
  std::vector<T> values;
  values.reserve(32);
  values.push_back(static_cast<T>(0));
  values.push_back(static_cast<T>(std::numeric_limits<uint64_t>::max()));
  for (uint32_t i = 2; i < 32; ++i) {
    values.push_back(static_cast<T>(i));
  }
  this->roundTripAndExpect(values);
}

TYPED_TEST(CompactForEncodingTest, rangeDecodeForwardAndReverse) {
  // Forward and reverse range decode via skip(start) + materialize(window).
  // Mirrors MRS's testEncoderRoundTrip fwd/rev triplet on a known shape.
  using T = TypeParam;
  std::vector<T> values;
  values.reserve(200);
  for (uint32_t i = 0; i < 200; ++i) {
    values.push_back(static_cast<T>(500'000ULL + i * 7ULL));
  }
  auto encoding = this->encodeAndCreate(values);

  // Forward range decode at several offsets.
  for (uint32_t start :
       {uint32_t{0}, uint32_t{1}, uint32_t{50}, uint32_t{150}, uint32_t{199}}) {
    SCOPED_TRACE(fmt::format("fwd start={}", start));
    const uint32_t windowSize =
        std::min<uint32_t>(13, static_cast<uint32_t>(values.size()) - start);
    encoding->reset();
    if (start > 0) {
      encoding->skip(start);
    }
    std::vector<T> window(windowSize);
    encoding->materialize(windowSize, window.data());
    for (uint32_t i = 0; i < windowSize; ++i) {
      EXPECT_EQ(window[i], values[start + i]) << "i=" << i;
    }
  }

  // Reverse range decode: walk the same offsets in descending order, each
  // time resetting first. LFB has true O(1) random access so this should
  // produce identical results to the forward pass.
  for (int32_t start : {int32_t{199}, int32_t{150}, int32_t{50}, int32_t{1}}) {
    SCOPED_TRACE(fmt::format("rev start={}", start));
    const uint32_t windowSize = std::min<uint32_t>(
        13,
        static_cast<uint32_t>(values.size()) - static_cast<uint32_t>(start));
    encoding->reset();
    if (start > 0) {
      encoding->skip(static_cast<uint32_t>(start));
    }
    std::vector<T> window(windowSize);
    encoding->materialize(windowSize, window.data());
    for (uint32_t i = 0; i < windowSize; ++i) {
      EXPECT_EQ(window[i], values[static_cast<uint32_t>(start) + i])
          << "i=" << i;
    }
  }
}

TYPED_TEST(CompactForEncodingTest, skipAndMaterializeInterleaved) {
  using T = TypeParam;
  std::vector<T> values;
  values.reserve(300);
  for (uint32_t i = 0; i < 300; ++i) {
    values.push_back(static_cast<T>(2'000'000ULL + i * 17ULL + (i % 11)));
  }
  auto encoding = this->encodeAndCreate(values);

  encoding->reset();
  std::vector<T> chunk(50);
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

  // Reset restores the cursor.
  encoding->reset();
  std::vector<T> all(values.size());
  encoding->materialize(static_cast<uint32_t>(values.size()), all.data());
  for (size_t i = 0; i < values.size(); ++i) {
    EXPECT_EQ(all[i], values[i]) << "after-reset i=" << i;
  }
}

TYPED_TEST(CompactForEncodingTest, debugString) {
  using T = TypeParam;
  std::vector<T> values;
  values.reserve(8);
  for (uint32_t i = 0; i < 8; ++i) {
    values.push_back(static_cast<T>(1000 + i * 5));
  }
  auto encoding = this->encodeAndCreate(values);
  const std::string debug = encoding->debugString();
  EXPECT_NE(debug.find("CompactFor"), std::string::npos);
  EXPECT_NE(debug.find("bitWidth="), std::string::npos);
}

class CompactForEncodingFuzzerTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  }

  template <typename T>
  void runFuzzer(uint32_t seed, uint32_t numIterations) {
    std::mt19937_64 rng(seed);

    ManualEncodingSelectionPolicyFactory manualFactory;
    EncodingSelectionPolicyFactory factory =
        [&manualFactory](DataType dataType) {
          return manualFactory.createPolicy(dataType);
        };

    for (uint32_t iter = 0; iter < numIterations; ++iter) {
      SCOPED_TRACE(fmt::format("iteration {}", iter));

      std::uniform_int_distribution<uint32_t> sizeDist(1, 1000);
      const uint32_t numValues = sizeDist(rng);

      // Random profile per iteration. Profile 0 is a constant (bw=0 path),
      // profile 1 is narrow-range (bw<=56 fast path), profile 2 is full-range
      // (covers the cross-word fallback at bw>56).
      std::uniform_int_distribution<int32_t> profileDist(0, 2);
      const int32_t profile = profileDist(rng);

      std::vector<T> values;
      values.reserve(numValues);
      if (profile == 0) {
        const T constantValue = static_cast<T>(rng());
        for (uint32_t i = 0; i < numValues; ++i) {
          values.push_back(constantValue);
        }
      } else if (profile == 1) {
        std::uniform_int_distribution<uint64_t> narrowDist(0, (1ULL << 30) - 1);
        const uint64_t base = static_cast<uint64_t>(rng());
        for (uint32_t i = 0; i < numValues; ++i) {
          values.push_back(static_cast<T>(base + narrowDist(rng)));
        }
      } else {
        std::uniform_int_distribution<uint64_t> wideDist(
            0, std::numeric_limits<uint64_t>::max());
        for (uint32_t i = 0; i < numValues; ++i) {
          values.push_back(static_cast<T>(wideDist(rng)));
        }
      }

      Buffer buffer{*pool_};
      EncodingLayout layout{
          EncodingType::CompactFor, {}, CompressionType::Uncompressed};
      auto policy = std::make_unique<ReplayedEncodingSelectionPolicy<T>>(
          std::move(layout), CompressionOptions{}, factory);
      auto encoded = EncodingFactory::encode<T>(
          std::move(policy),
          std::span<const T>{values.data(), values.size()},
          buffer);
      std::vector<char> storage(encoded.begin(), encoded.end());
      auto encoding = EncodingFactory().create(
          *pool_, {storage.data(), storage.size()}, nullptr);

      ASSERT_EQ(encoding->encodingType(), EncodingType::CompactFor);
      ASSERT_EQ(encoding->dataType(), TypeTraits<T>::dataType);
      ASSERT_EQ(encoding->rowCount(), values.size());
      std::vector<T> decoded(values.size());
      encoding->materialize(
          static_cast<uint32_t>(values.size()), decoded.data());
      for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_EQ(decoded[i], values[i])
            << "iter=" << iter << " i=" << i << " size=" << numValues
            << " profile=" << profile;
      }

      // Random skip/materialize sequence.
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

TEST_F(CompactForEncodingFuzzerTest, fuzzerInt64) {
  runFuzzer<int64_t>(/*seed=*/12345, /*numIterations=*/30);
}

TEST_F(CompactForEncodingFuzzerTest, fuzzerUint64) {
  runFuzzer<uint64_t>(/*seed=*/67890, /*numIterations=*/30);
}

TEST_F(CompactForEncodingFuzzerTest, encodeRejectsEmpty) {
  Buffer buffer{*pool_};
  EncodingLayout layout{
      EncodingType::CompactFor, {}, CompressionType::Uncompressed};
  ManualEncodingSelectionPolicyFactory manualFactory;
  EncodingSelectionPolicyFactory factory = [&manualFactory](DataType dataType) {
    return manualFactory.createPolicy(dataType);
  };
  auto policy = std::make_unique<ReplayedEncodingSelectionPolicy<int64_t>>(
      std::move(layout), CompressionOptions{}, factory);
  std::vector<int64_t> empty;
  NIMBLE_ASSERT_THROW(
      EncodingFactory::encode<int64_t>(
          std::move(policy),
          std::span<const int64_t>{empty.data(), empty.size()},
          buffer),
      "CompactFor encoding cannot be used with 0 rows.");
}

} // namespace facebook::nimble::test
