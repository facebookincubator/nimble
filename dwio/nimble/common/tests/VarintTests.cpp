// Copyright 2004-present Facebook. All Rights Reserved.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "dwio/nimble/common/Varint.h"
#include "folly/Random.h"
#include "folly/Range.h"
#include "folly/Varint.h"

using namespace ::facebook;

namespace {
const int kNumElements = 10000;
}

TEST(VarintTests, WriteRead32) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  std::vector<uint32_t> data;
  // Generate data with uniform bit length.
  for (int i = 0; i < kNumElements; ++i) {
    const int bitShift = folly::Random::rand32(rng) % 32;
    data.push_back(folly::Random::rand32(rng) >> bitShift);
  }

  auto buffer =
      std::make_unique<char[]>(kNumElements * folly::kMaxVarintLength32);
  char* pos = buffer.get();
  for (int i = 0; i < kNumElements; ++i) {
    nimble::varint::writeVarint(data[i], &pos);
  }

  auto follyBuffer =
      std::make_unique<uint8_t[]>(kNumElements * folly::kMaxVarintLength32);
  uint8_t* fpos = follyBuffer.get();
  for (int i = 0; i < kNumElements; ++i) {
    fpos += folly::encodeVarint(data[i], fpos);
  }

  ASSERT_EQ(pos - buffer.get(), fpos - follyBuffer.get());

  ASSERT_EQ(nimble::varint::bulkVarintSize32(data), pos - buffer.get());

  const char* cpos = buffer.get();
  for (int i = 0; i < kNumElements; ++i) {
    ASSERT_EQ(data[i], nimble::varint::readVarint32(&cpos));
  }

  const uint8_t* fstart = follyBuffer.get();
  const uint8_t* fend = follyBuffer.get() + (fpos - follyBuffer.get());
  folly::Range<const uint8_t*> frange(fstart, fend);
  for (int i = 0; i < kNumElements; ++i) {
    ASSERT_EQ(data[i], folly::decodeVarint(frange));
  }

  std::vector<uint32_t> bulk(kNumElements);
  cpos = buffer.get();
  nimble::varint::bulkVarintDecode32(kNumElements, cpos, bulk.data());
  for (int i = 0; i < kNumElements; ++i) {
    ASSERT_EQ(data[i], bulk[i]);
  }
}

TEST(VarintTests, WriteRead64) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  std::vector<uint64_t> data;
  // Generate data with uniform bit length.
  for (int i = 0; i < kNumElements; ++i) {
    const int bitShift = folly::Random::rand64(rng) % 64;
    data.push_back(folly::Random::rand64(rng) >> bitShift);
  }

  auto buffer =
      std::make_unique<char[]>(kNumElements * folly::kMaxVarintLength64);
  char* pos = buffer.get();
  for (int i = 0; i < kNumElements; ++i) {
    nimble::varint::writeVarint(data[i], &pos);
  }

  auto follyBuffer =
      std::make_unique<uint8_t[]>(kNumElements * folly::kMaxVarintLength64);
  uint8_t* fpos = follyBuffer.get();
  for (int i = 0; i < kNumElements; ++i) {
    fpos += folly::encodeVarint(data[i], fpos);
  }

  ASSERT_EQ(pos - buffer.get(), fpos - follyBuffer.get());

  ASSERT_EQ(nimble::varint::bulkVarintSize64(data), pos - buffer.get());

  const char* cpos = buffer.get();
  for (int i = 0; i < kNumElements; ++i) {
    ASSERT_EQ(data[i], nimble::varint::readVarint64(&cpos));
  }

  const uint8_t* fstart = follyBuffer.get();
  const uint8_t* fend = follyBuffer.get() + (fpos - follyBuffer.get());
  folly::Range<const uint8_t*> frange(fstart, fend);
  for (int i = 0; i < kNumElements; ++i) {
    ASSERT_EQ(data[i], folly::decodeVarint(frange));
  }

  std::vector<uint64_t> bulk(kNumElements);
  cpos = buffer.get();
  nimble::varint::bulkVarintDecode64(kNumElements, cpos, bulk.data());
  for (int i = 0; i < kNumElements; ++i) {
    ASSERT_EQ(data[i], bulk[i]);
  }
}
