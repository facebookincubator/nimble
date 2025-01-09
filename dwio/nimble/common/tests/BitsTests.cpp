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
#include "dwio/nimble/common/Bits.h"
#include "folly/Random.h"

using namespace facebook::nimble::bits;

template <typename T>
void repeat(int32_t times, const T& t) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);
  while (times-- > 0) {
    t(rng);
  }
}

TEST(BitsTests, setBits) {
  repeat(10, [](auto& rng) {
    auto size = folly::Random::rand32(64 * 1024, rng) + 1;
    auto begin = folly::Random::rand32(size, rng);
    auto end = folly::Random::rand32(begin, size, rng);
    std::vector<char> bitmap(bucketsRequired(size, 64) * 8, 0);
    setBits(begin, end, bitmap.data());
    for (auto i = 0; i < size; ++i) {
      bool expected = (i >= begin && i < end);
      EXPECT_EQ(expected, getBit(i, bitmap.data())) << i;
    }
  });
}

TEST(BitsTests, clearBits) {
  repeat(10, [](auto& rng) {
    auto size = folly::Random::rand32(64 * 1024, rng) + 1;
    auto begin = folly::Random::rand32(size, rng);
    auto end = folly::Random::rand32(begin, size, rng);
    std::vector<char> bitmap(bucketsRequired(size, 64) * 8, 0xff);
    clearBits(begin, end, bitmap.data());
    for (auto i = 0; i < size; ++i) {
      bool expected = (i < begin || i >= end);
      EXPECT_EQ(expected, getBit(i, bitmap.data())) << i;
    }
  });
}

TEST(BitsTests, findSetBit) {
  repeat(10, [](auto& rng) {
    auto size = folly::Random::rand32(64 * 1024, rng) + 1;
    std::vector<char> bitmap(bucketsRequired(size, 64) * 8, 0);
    auto begin = folly::Random::rand32(size, rng);
    auto n = (size - begin) / 3;
    auto setBits = 0;
    auto pos = size;
    for (auto i = begin; i < size; ++i) {
      if (folly::Random::oneIn(3, rng)) {
        setBit(i, bitmap.data());
        if (++setBits == n) {
          pos = i;
        }
      }
    }
    EXPECT_EQ(pos, findSetBit(bitmap.data(), begin, size, n));
  });
}

TEST(BitsTests, copy) {
  repeat(1, [](auto& rng) {
    auto size = folly::Random::rand32(64 * 1024, rng) + 1;
    auto begin = folly::Random::rand32(size, rng);
    auto end = folly::Random::oneIn(2) ? folly::Random::rand32(begin, size, rng)
                                       : size;
    std::vector<char> src(bucketsRequired(size, 64) * 8, 0);
    std::vector<char> dst(src.size(), 0);
    for (auto i = begin; i < end; ++i) {
      maybeSetBit(i, src.data(), folly::Random::oneIn(2, rng));
    }
    Bitmap srcBitmap{src.data(), size};
    BitmapBuilder dstBitmap{dst.data(), size};
    dstBitmap.copy(srcBitmap, begin, end);
    for (auto i = 0; i < end; ++i) {
      if (i < begin) {
        EXPECT_FALSE(getBit(i, dst.data()));
      } else {
        EXPECT_EQ(getBit(i, src.data()), getBit(i, dst.data())) << i;
      }
    }
  });
}
