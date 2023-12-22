// Copyright 2004-present Facebook. All Rights Reserved.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include "dwio/alpha/common/BitEncoder.h"
#include "folly/Random.h"

using namespace ::facebook;

TEST(BitEncoderTests, WriteThenReadDifferentBitLengths) {
  constexpr int elementCount = 1000;
  std::vector<char> buffer(4 * elementCount);
  alpha::BitEncoder bitEncoder(buffer.data());
  for (int i = 0; i < elementCount; ++i) {
    bitEncoder.putBits(i % 31, (i % 31) + 1);
  }
  for (int i = 0; i < elementCount; ++i) {
    ASSERT_EQ(i % 31, bitEncoder.getBits((i % 31) + 1));
  }
}

TEST(BitEncoderTests, WriteAndReadIntermixed) {
  constexpr int elementCount = 1000;
  std::vector<int> data;
  for (int i = 0; i < elementCount; ++i) {
    data.push_back(2 * i);
  }
  std::vector<int> bitLengths;
  for (int i = 0; i < elementCount; ++i) {
    bitLengths.push_back(11 + (i % 10));
  }
  std::vector<char> buffer(4 * elementCount);
  alpha::BitEncoder bitEncoder(buffer.data());
  for (int i = 0; i < elementCount; i += 2) {
    bitEncoder.putBits(data[i], bitLengths[i]);
    bitEncoder.putBits(data[i + 1], bitLengths[i + 1]);
    ASSERT_EQ(data[i >> 1], bitEncoder.getBits(bitLengths[i >> 1]));
  }
  for (int i = elementCount / 2; i < elementCount; ++i) {
    ASSERT_EQ(data[i], bitEncoder.getBits(bitLengths[i]));
  }
}

TEST(BitEncoderTests, WriteThenReadFullBitRange) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  constexpr int elementCount = 1000;
  std::vector<uint64_t> data;
  for (int i = 0; i < elementCount; ++i) {
    data.push_back(folly::Random::rand64(rng));
  }
  for (int bits = 1; bits < 65; ++bits) {
    const uint64_t mask = bits == 64 ? (~0ULL) : ((1ULL << bits) - 1);
    std::vector<char> buffer(8 * elementCount);
    alpha::BitEncoder bitEncoder(buffer.data());
    for (int i = 0; i < elementCount; ++i) {
      bitEncoder.putBits(data[i] & mask, bits);
    }
    for (int i = 0; i < elementCount; ++i) {
      ASSERT_EQ(bitEncoder.getBits(bits), data[i] & mask);
    }
  }
}
