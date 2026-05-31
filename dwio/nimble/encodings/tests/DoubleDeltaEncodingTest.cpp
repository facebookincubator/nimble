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
#include "dwio/nimble/encodings/DoubleDeltaEncoding.h"

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

// DoubleDelta is templated on int64_t and uint64_t. Both share
// the same wire format (the encoder works on the unsigned physical
// representation), so the per-type tests run on the unsigned physical type
// directly. End-to-end signed-input coverage is exercised by the fuzzer
// below, which routes int64 through EncodingFactory::encode<int64_t>.
template <typename T>
class DoubleDeltaEncodingTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  }

  std::unique_ptr<EncodingSelectionPolicy<T>> createSelectionPolicy() {
    EncodingLayout layout{
        EncodingType::DoubleDelta, {}, CompressionType::Uncompressed};
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
    EXPECT_EQ(encoding->encodingType(), EncodingType::DoubleDelta);
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

using LongDoubleDeltaTypes = ::testing::Types<int64_t, uint64_t>;
TYPED_TEST_SUITE(DoubleDeltaEncodingTest, LongDoubleDeltaTypes);

TYPED_TEST(DoubleDeltaEncodingTest, singleElement) {
  using T = TypeParam;
  this->roundTripAndExpect({T{42}});
}

TYPED_TEST(DoubleDeltaEncodingTest, twoElements) {
  using T = TypeParam;
  this->roundTripAndExpect({T{100}, T{250}});
}

TYPED_TEST(DoubleDeltaEncodingTest, allEqualCollapsesToBitWidthZero) {
  // 64-element constant stream. Δ=0 → Δ²=0 → bitWidth=0 → no packed region
  // and (because of the bitWidth=0 short-circuit in the encoder) no captured
  // checkpoints either. Wire is just the 35-byte header + 4-byte cpCount=0.
  using T = TypeParam;
  std::vector<T> values(64, T{7});
  this->roundTripAndExpect(values);
  auto encoding = this->encodeAndCreate(values);
  // 6 (Encoding prefix) + 25 (kPrefixSize) + 4 (cpCount=0). No packed region,
  // no checkpoint entries.
  EXPECT_EQ(this->encodedStorage_.size(), 6u + 25u + 4u);
}

TYPED_TEST(DoubleDeltaEncodingTest, regularIntervalTimestampsAreTiny) {
  // The killer case: timestamps every 1000 units. Δ=1000 (constant) →
  // Δ²=0 → bitWidth=0. Wire stays at 35B regardless of N because the
  // bitWidth=0 fast path skips both the packed region and the checkpoint
  // capture.
  using T = TypeParam;
  std::vector<T> values;
  values.reserve(1024);
  for (uint32_t i = 0; i < 1024; ++i) {
    values.push_back(static_cast<T>(1'700'000'000 + i * 1000));
  }
  this->roundTripAndExpect(values);
  auto encoding = this->encodeAndCreate(values);
  EXPECT_EQ(this->encodedStorage_.size(), 6u + 25u + 4u);
}

TYPED_TEST(DoubleDeltaEncodingTest, monotoneWithRareGaps) {
  // Mostly-regular intervals with a few large jumps. bitWidth must come up
  // off zero to absorb the irregular Δ², but the encoded size still beats
  // the FixedBitWidth ceiling.
  using T = TypeParam;
  std::vector<T> values;
  values.reserve(500);
  for (uint32_t i = 0; i < 500; ++i) {
    T v = static_cast<T>(1'000'000 + i * 60);
    if (i == 100 || i == 250 || i == 400) {
      v = static_cast<T>(static_cast<uint64_t>(v) + 5'000'000ULL);
    }
    values.push_back(v);
  }
  this->roundTripAndExpect(values);
}

TYPED_TEST(DoubleDeltaEncodingTest, mixedSizes) {
  using T = TypeParam;
  for (uint32_t n :
       {1u, 2u, 3u, 7u, 15u, 16u, 17u, 33u, 64u, 100u, 257u, 1024u}) {
    SCOPED_TRACE(fmt::format("n={}", n));
    std::vector<T> values;
    values.reserve(n);
    // A semi-regular sequence — gives a non-trivial bitWidth without being
    // adversarial. The (i*31+1)%256 wobble prevents Δ² from being always 0.
    for (uint32_t i = 0; i < n; ++i) {
      values.push_back(static_cast<T>(1000 + i * 50 + (i * 31 + 1) % 17));
    }
    this->roundTripAndExpect(values);
  }
}

TYPED_TEST(DoubleDeltaEncodingTest, skipAndMaterialize) {
  using T = TypeParam;
  std::vector<T> values;
  values.reserve(300);
  for (uint32_t i = 0; i < 300; ++i) {
    values.push_back(static_cast<T>(2'000'000 + i * 17 + (i % 11)));
  }
  auto encoding = this->encodeAndCreate(values);

  // Interleaved skips and materialises — exercises the wrap-around delta
  // accumulator across non-trivial gaps.
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

  // Reset must restore all cursors so a second pass produces identical output.
  encoding->reset();
  std::vector<T> all(values.size());
  encoding->materialize(static_cast<uint32_t>(values.size()), all.data());
  for (size_t i = 0; i < values.size(); ++i) {
    EXPECT_EQ(all[i], values[i]) << "after-reset i=" << i;
  }
}

TYPED_TEST(DoubleDeltaEncodingTest, rangeMaterializeViaSkip) {
  // Range decode pattern: skip past a prefix, then materialize a window.
  using T = TypeParam;
  std::vector<T> values;
  values.reserve(200);
  for (uint32_t i = 0; i < 200; ++i) {
    values.push_back(static_cast<T>(500'000 + i * 7));
  }
  auto encoding = this->encodeAndCreate(values);

  for (uint32_t start :
       {uint32_t{0}, uint32_t{1}, uint32_t{2}, uint32_t{50}, uint32_t{199}}) {
    SCOPED_TRACE(fmt::format("start={}", start));
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
}

TYPED_TEST(DoubleDeltaEncodingTest, checkpointSeekCoversBoundaries) {
  // Exercise the checkpoint fast path at all the seek positions that the
  // skip-then-materialize math has to get right: K-boundary anchors,
  // mid-segment offsets, single-element windows, ranges that span multiple
  // checkpoints, plus the L==0 and R==N-1 edges. Each window is compared
  // against the corresponding slice of the full materialized output so the
  // assertion holds regardless of decoder internals.
  using T = TypeParam;
  // 600 rows = 9 full strides of 64 residuals (9 checkpoints captured); needs
  // bitWidth > 0 so the checkpoint segment actually exists. The (i % 13)
  // wobble keeps Δ² off zero.
  constexpr uint32_t kRows = 600;
  std::vector<T> values;
  values.reserve(kRows);
  for (uint32_t i = 0; i < kRows; ++i) {
    values.push_back(static_cast<T>(2'500'000 + i * 41 + (i * 7 % 13)));
  }
  // Full-decode once to get a ground-truth output.
  auto fullEncoding = this->encodeAndCreate(values);
  std::vector<T> expected(kRows);
  fullEncoding->materialize(kRows, expected.data());
  ASSERT_EQ(expected, values);

  // (L, R) inclusive ranges. K=64 in the encoder.
  const std::vector<std::pair<uint32_t, uint32_t>> ranges = {
      {0, 0}, // single-element at the very start (no checkpoint usable)
      {0, 10}, // small range at start
      {66, 66}, // single-element on the first checkpoint boundary (L = K+2)
      {66, 80}, // small range starting at the first checkpoint boundary
      {100, 130}, // mid-segment, lands inside checkpoint #0's reach
      {200, 250}, // window inside checkpoint #2's reach
      {130, 400}, // window that spans multiple checkpoints
      {64, 64}, // single-element just before the K+2 boundary
      {500, 599}, // tail of the stream
      {kRows - 1, kRows - 1}, // single-element at the very end
  };

  for (const auto& [l, r] : ranges) {
    SCOPED_TRACE(fmt::format("range L={} R={}", l, r));
    auto encoding = this->encodeAndCreate(values);
    encoding->reset();
    if (l > 0) {
      encoding->skip(l);
    }
    const uint32_t windowSize = r - l + 1;
    std::vector<T> got(windowSize);
    encoding->materialize(windowSize, got.data());
    const std::vector<T> want(expected.begin() + l, expected.begin() + r + 1);
    EXPECT_EQ(got, want);
  }
}

TYPED_TEST(DoubleDeltaEncodingTest, checkpointSeekMatchesFullDecodeAfterReset) {
  // After a checkpoint-accelerated range-decode, calling reset() and a
  // straight full materialize must still produce the original stream.
  // Catches state-leak bugs in seekToCheckpointFor.
  using T = TypeParam;
  constexpr uint32_t kRows = 400;
  std::vector<T> values;
  values.reserve(kRows);
  for (uint32_t i = 0; i < kRows; ++i) {
    values.push_back(static_cast<T>(1'000'000'000 + i * 23 + (i * 11 % 7)));
  }
  auto encoding = this->encodeAndCreate(values);
  encoding->skip(200);
  std::vector<T> midChunk(50);
  encoding->materialize(50, midChunk.data());
  for (uint32_t i = 0; i < 50; ++i) {
    EXPECT_EQ(midChunk[i], values[200 + i]);
  }
  encoding->reset();
  std::vector<T> all(kRows);
  encoding->materialize(kRows, all.data());
  EXPECT_EQ(all, values);
}

TYPED_TEST(DoubleDeltaEncodingTest, checkpointSegmentSizesCorrectly) {
  // 130 rows with bitWidth>0 => residuals = 128 => checkpointCount = 2.
  // Verify the on-wire byte count includes the checkpoint segment.
  using T = TypeParam;
  constexpr uint32_t kRows = 130;
  std::vector<T> values;
  values.reserve(kRows);
  for (uint32_t i = 0; i < kRows; ++i) {
    values.push_back(static_cast<T>(7'000'000 + i * 13 + (i % 5)));
  }
  this->roundTripAndExpect(values);
  auto encoding = this->encodeAndCreate(values);
  // The exact bitpacked region width depends on bitsRequired(maxResidual),
  // so this asserts on the delta added by the new checkpoint segment vs a
  // baseline with no captured checkpoints. Two checkpoints * 16B + 4B
  // count = 36B trailing.
  EXPECT_GE(this->encodedStorage_.size(), 6u + 25u + 36u);
}

TYPED_TEST(DoubleDeltaEncodingTest, largeMagnitudeOverflowSafe) {
  // Values near the type range exercise the wrap-around delta arithmetic
  // (encoder and decoder both treat the underlying uint64 representation
  // mod 2^64). A signed int64 overflow during delta computation would be UB
  // if we used signed subtraction directly — verifies we do not.
  using T = TypeParam;
  std::vector<T> values;
  values.reserve(8);
  // Intentionally straddle zero / the signed boundary for int64; for
  // uint64 these are simply very-large unsigned values.
  values.push_back(static_cast<T>(std::numeric_limits<int64_t>::min()));
  values.push_back(static_cast<T>(std::numeric_limits<int64_t>::min() + 17));
  values.push_back(static_cast<T>(std::numeric_limits<int64_t>::min() + 42));
  values.push_back(static_cast<T>(0));
  values.push_back(static_cast<T>(100));
  values.push_back(static_cast<T>(std::numeric_limits<int64_t>::max() - 5));
  values.push_back(static_cast<T>(std::numeric_limits<int64_t>::max()));
  values.push_back(static_cast<T>(std::numeric_limits<int64_t>::min() + 1));
  this->roundTripAndExpect(values);
}

TYPED_TEST(DoubleDeltaEncodingTest, debugString) {
  using T = TypeParam;
  std::vector<T> values;
  values.reserve(8);
  for (uint32_t i = 0; i < 8; ++i) {
    values.push_back(static_cast<T>(1000 + i * 5));
  }
  auto encoding = this->encodeAndCreate(values);
  const std::string debug = encoding->debugString();
  EXPECT_NE(debug.find("DoubleDelta"), std::string::npos);
  EXPECT_NE(debug.find("bitWidth="), std::string::npos);
  EXPECT_NE(debug.find("checkpoints="), std::string::npos);
}

class DoubleDeltaEncodingFuzzerTest : public ::testing::Test {
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

      // Pick a random regularity profile per iteration to broadly cover the
      // encoder's input space: perfectly regular, small wobble, and wide
      // random jumps.
      std::uniform_int_distribution<int32_t> profileDist(0, 2);
      const int32_t profile = profileDist(rng);
      std::uniform_int_distribution<int64_t> wobbleDist(-32, 32);
      std::uniform_int_distribution<int64_t> jumpDist(
          std::numeric_limits<int32_t>::min(),
          std::numeric_limits<int32_t>::max());

      std::vector<T> values;
      values.reserve(numValues);
      uint64_t cur = static_cast<uint64_t>(rng());
      values.push_back(static_cast<T>(cur));
      const int64_t baseStep = 1'000;
      for (uint32_t i = 1; i < numValues; ++i) {
        int64_t step = baseStep;
        if (profile == 1) {
          step += wobbleDist(rng);
        } else if (profile == 2) {
          step = jumpDist(rng);
        }
        cur += static_cast<uint64_t>(step);
        values.push_back(static_cast<T>(cur));
      }

      Buffer buffer{*pool_};
      EncodingLayout layout{
          EncodingType::DoubleDelta, {}, CompressionType::Uncompressed};
      auto policy = std::make_unique<ReplayedEncodingSelectionPolicy<T>>(
          std::move(layout), CompressionOptions{}, factory);
      auto encoded = EncodingFactory::encode<T>(
          std::move(policy),
          std::span<const T>{values.data(), values.size()},
          buffer);
      std::vector<char> storage(encoded.begin(), encoded.end());
      auto encoding = EncodingFactory().create(
          *pool_, {storage.data(), storage.size()}, nullptr);

      ASSERT_EQ(encoding->encodingType(), EncodingType::DoubleDelta);
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

      // Random skip/materialize sequence — same churn as the PFOR fuzzer.
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

TEST_F(DoubleDeltaEncodingFuzzerTest, fuzzerInt64) {
  runFuzzer<int64_t>(/*seed=*/12345, /*numIterations=*/30);
}

TEST_F(DoubleDeltaEncodingFuzzerTest, fuzzerUint64) {
  runFuzzer<uint64_t>(/*seed=*/67890, /*numIterations=*/30);
}

TEST_F(DoubleDeltaEncodingFuzzerTest, encodeRejectsEmpty) {
  Buffer buffer{*pool_};
  EncodingLayout layout{
      EncodingType::DoubleDelta, {}, CompressionType::Uncompressed};
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
      "DoubleDelta encoding cannot be used with 0 rows.");
}

} // namespace facebook::nimble::test
