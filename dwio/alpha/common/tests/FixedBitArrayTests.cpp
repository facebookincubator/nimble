// Copyright 2004-present Facebook. All Rights Reserved.

#include <gtest/gtest.h>
#include <memory>

#include "dwio/alpha/common/FixedBitArray.h"
#include "folly/Benchmark.h"
#include "folly/Random.h"

using namespace ::facebook;

TEST(FixedBitArrayTests, SetThenGet) {
  constexpr int bitWidth = 10;
  constexpr int elementCount = 10000;
  auto buffer = std::make_unique<char[]>(
      alpha::FixedBitArray::bufferSize(elementCount, bitWidth));
  alpha::FixedBitArray fixedBitArray(buffer.get(), bitWidth);
  for (int i = 0; i < elementCount; ++i) {
    fixedBitArray.set(i, (i + i * i) % (1 << bitWidth));
  }
  for (int i = 0; i < elementCount; ++i) {
    ASSERT_EQ(fixedBitArray.get(i), (i + i * i) % (1 << bitWidth));
  }
}

TEST(FixedBitArrayTests, zeroAndSet) {
  constexpr int bitWidth = 4;
  constexpr int elementCount = 1234;
  auto buffer = std::make_unique<char[]>(
      alpha::FixedBitArray::bufferSize(elementCount, bitWidth));
  alpha::FixedBitArray fixedBitArray(buffer.get(), bitWidth);
  for (int i = 0; i < elementCount; ++i) {
    fixedBitArray.set(i, (i + i * i) % (1 << bitWidth));
  }
  for (int i = 0; i < elementCount; ++i) {
    ASSERT_EQ(fixedBitArray.get(i), (i + i * i) % (1 << bitWidth));
  }
  for (int i = 0; i < elementCount; ++i) {
    if (i % 3 == 0) {
      fixedBitArray.zeroAndSet(i, i % (1 << bitWidth));
    }
  }
  for (int i = 0; i < elementCount; ++i) {
    if (i % 3 == 0) {
      ASSERT_EQ(fixedBitArray.get(i), i % (1 << bitWidth));
    } else {
      ASSERT_EQ(fixedBitArray.get(i), (i + i * i) % (1 << bitWidth));
    }
  }
}

constexpr int kNumTestsPerBitWidth = 10;
constexpr int kMaxElements = 1000;

TEST(FixedBitArrayTests, SetThenGetRandom) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (int bitWidth = 1; bitWidth <= 64; ++bitWidth) {
    const uint64_t elementMask =
        bitWidth == 64 ? (~0ULL) : ((1ULL << bitWidth) - 1);
    for (int test = 0; test < kNumTestsPerBitWidth; ++test) {
      const int elementCount = folly::Random::rand32(rng) % kMaxElements;
      auto buffer = std::make_unique<char[]>(
          alpha::FixedBitArray::bufferSize(elementCount, bitWidth));
      alpha::FixedBitArray fixedBitArray(buffer.get(), bitWidth);
      std::vector<uint64_t> randomValues(elementCount);
      for (int i = 0; i < elementCount; ++i) {
        randomValues[i] = folly::Random::rand64(rng) & elementMask;
      }
      for (int i = 0; i < elementCount; ++i) {
        fixedBitArray.set(i, randomValues[i]);
      }
      for (int i = 0; i < elementCount; ++i) {
        ASSERT_EQ(fixedBitArray.get(i), randomValues[i]);
      }
    }
  }
}

TEST(FixedBitArrayTests, zeroAndSetThenGetRandom) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (int bitWidth = 1; bitWidth <= 64; ++bitWidth) {
    const uint64_t elementMask =
        bitWidth == 64 ? (~0ULL) : ((1ULL << bitWidth) - 1);
    for (int test = 0; test < kNumTestsPerBitWidth; ++test) {
      const int elementCount = folly::Random::rand32(rng) % kMaxElements;
      std::vector<char> buffer(
          alpha::FixedBitArray::bufferSize(elementCount, bitWidth), 0xFF);
      alpha::FixedBitArray fixedBitArray(buffer.data(), bitWidth);
      std::vector<uint64_t> randomValues(elementCount);
      for (int i = 0; i < elementCount; ++i) {
        randomValues[i] = folly::Random::rand64(rng) & elementMask;
      }
      for (int i = 0; i < elementCount; ++i) {
        fixedBitArray.zeroAndSet(i, randomValues[i]);
      }
      for (int i = 0; i < elementCount; ++i) {
        ASSERT_EQ(fixedBitArray.get(i), randomValues[i]) << bitWidth;
      }
    }
  }
}

TEST(FixedBitArrayTests, Set32ThenGet32Random) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (int bitWidth = 1; bitWidth <= 32; ++bitWidth) {
    const uint64_t elementMask = (1ULL << bitWidth) - 1;
    for (int test = 0; test < kNumTestsPerBitWidth; ++test) {
      const int elementCount = folly::Random::rand32(rng) % kMaxElements;
      auto buffer = std::make_unique<char[]>(
          alpha::FixedBitArray::bufferSize(elementCount, bitWidth));
      alpha::FixedBitArray fixedBitArray(buffer.get(), bitWidth);
      std::vector<uint32_t> randomValues(elementCount);
      for (int i = 0; i < elementCount; ++i) {
        randomValues[i] = folly::Random::rand32(rng) & elementMask;
      }
      for (int i = 0; i < elementCount; ++i) {
        fixedBitArray.set32(i, randomValues[i]);
      }
      for (int i = 0; i < elementCount; ++i) {
        ASSERT_EQ(fixedBitArray.get32(i), randomValues[i])
            << i << " " << bitWidth;
      }
    }
  }
}

TEST(FixedBitArrayTests, bulkGet32) {
  constexpr int kBitWidth = 7;
  constexpr int kNumElements = 27;
  auto buffer = std::make_unique<char[]>(
      alpha::FixedBitArray::bufferSize(kNumElements, kBitWidth));
  alpha::FixedBitArray fixedBitArray(buffer.get(), kBitWidth);
  for (int i = 0; i < kNumElements; ++i) {
    fixedBitArray.set32(i, 100 - i);
  }
  std::vector<uint32_t> values(kNumElements);
  fixedBitArray.bulkGet32(0, kNumElements, values.data());
  for (int i = 0; i < kNumElements; ++i) {
    ASSERT_EQ(values[i], 100 - i);
  }
  std::vector<uint64_t> values64(kNumElements);
  fixedBitArray.bulkGet32Into64(0, kNumElements, values64.data());
  for (int i = 0; i < kNumElements; ++i) {
    ASSERT_EQ(values64[i], 100 - i);
  }
}

TEST(FixedBitArrayTests, BulkGet32Random) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (int bitWidth = 1; bitWidth <= 32; ++bitWidth) {
    const uint64_t elementMask = (1ULL << bitWidth) - 1;
    for (int test = 0; test < kNumTestsPerBitWidth; ++test) {
      const int elementCount = folly::Random::rand32(rng) % kMaxElements;
      auto buffer = std::make_unique<char[]>(
          alpha::FixedBitArray::bufferSize(elementCount, bitWidth));
      alpha::FixedBitArray fixedBitArray(buffer.get(), bitWidth);
      std::vector<uint32_t> randomValues(elementCount);
      for (int i = 0; i < elementCount; ++i) {
        randomValues[i] = folly::Random::rand32(rng) & elementMask;
      }
      for (int i = 0; i < elementCount; ++i) {
        fixedBitArray.set32(i, randomValues[i]);
      }
      std::vector<uint32_t> values(elementCount);
      fixedBitArray.bulkGet32(0, elementCount, values.data());
      for (int i = 0; i < elementCount; ++i) {
        ASSERT_EQ(values[i], randomValues[i]);
      }
      for (int i = 0; i < elementCount; ++i) {
        uint32_t element;
        fixedBitArray.bulkGet32(i, 1, &element);
        ASSERT_EQ(element, values[i]);
      }
      std::vector<uint64_t> values64(elementCount);
      fixedBitArray.bulkGet32Into64(0, elementCount, values64.data());
      for (int i = 0; i < elementCount; ++i) {
        ASSERT_EQ(values64[i], randomValues[i]);
      }
      for (int i = 0; i < elementCount; ++i) {
        uint64_t element;
        fixedBitArray.bulkGet32Into64(i, 1, &element);
        ASSERT_EQ(element, values64[i]);
      }
    }
  }
}

TEST(FixedBitArrayTests, BulkGetWithBaseline32Random) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (int bitWidth = 4; bitWidth <= 32; ++bitWidth) {
    const uint64_t maxElement = (1ULL << bitWidth);
    const uint64_t baseline = folly::Random::rand32(rng) % maxElement;
    for (int test = 0; test < kNumTestsPerBitWidth; ++test) {
      const int elementCount = folly::Random::rand32(rng) % kMaxElements;
      auto buffer = std::make_unique<char[]>(
          alpha::FixedBitArray::bufferSize(elementCount, bitWidth));
      alpha::FixedBitArray fixedBitArray(buffer.get(), bitWidth);
      std::vector<uint32_t> randomValues(elementCount);
      for (int i = 0; i < elementCount; ++i) {
        randomValues[i] = folly::Random::rand32(rng) % (maxElement - baseline);
      }
      for (int i = 0; i < elementCount; ++i) {
        fixedBitArray.set32(i, randomValues[i]);
      }
      std::vector<uint32_t> values(elementCount);
      fixedBitArray.bulkGetWithBaseline32(
          0, elementCount, values.data(), baseline);
      for (int i = 0; i < elementCount; ++i) {
        ASSERT_EQ(values[i], randomValues[i] + baseline)
            << "Bit Width: " << bitWidth << ", i: " << i;
      }
      for (int i = 0; i < elementCount; ++i) {
        uint32_t element;
        fixedBitArray.bulkGetWithBaseline32(i, 1, &element, baseline);
        ASSERT_EQ(element, randomValues[i] + baseline);
      }
      std::vector<uint64_t> values64(elementCount);
      fixedBitArray.bulkGetWithBaseline32Into64(
          0, elementCount, values64.data(), baseline);
      for (int i = 0; i < elementCount; ++i) {
        ASSERT_EQ(values64[i], randomValues[i] + baseline);
      }
      for (int i = 0; i < elementCount; ++i) {
        uint64_t element;
        fixedBitArray.bulkGetWithBaseline32Into64(i, 1, &element, baseline);
        ASSERT_EQ(element, randomValues[i] + baseline);
      }
    }
  }
}

TEST(FixedBitArrayTests, BulkSet32Random) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);
  for (int bitWidth = 1; bitWidth <= 32; ++bitWidth) {
    const uint64_t maxElement = (1ULL << bitWidth);
    for (int test = 0; test < kNumTestsPerBitWidth; ++test) {
      const int elementCount = 1 + folly::Random::rand32(rng) % kMaxElements;
      auto buffer = std::make_unique<char[]>(
          alpha::FixedBitArray::bufferSize(elementCount, bitWidth));
      alpha::FixedBitArray fixedBitArray(buffer.get(), bitWidth);
      std::vector<uint32_t> randomValues(elementCount);
      for (int i = 0; i < elementCount; ++i) {
        randomValues[i] = folly::Random::rand32(rng) % maxElement;
      }
      const int offset = folly::Random::rand32(rng) % elementCount;
      const int size = elementCount - offset;
      fixedBitArray.bulkSet32(offset, size, randomValues.data() + offset);
      for (int i = 0; i < size; ++i) {
        ASSERT_EQ(fixedBitArray.get32(offset + i), randomValues[offset + i]);
      }
    }
  }
}

TEST(FixedBitArrayTests, BulkSet32WithBaselineRandom) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);
  for (int bitWidth = 4; bitWidth <= 32; ++bitWidth) {
    const uint64_t maxElement = (1ULL << bitWidth);
    const uint64_t baseline = folly::Random::rand32(rng) % maxElement;
    for (int test = 0; test < kNumTestsPerBitWidth; ++test) {
      const int elementCount = 1 + folly::Random::rand32(rng) % kMaxElements;
      auto buffer = std::make_unique<char[]>(
          alpha::FixedBitArray::bufferSize(elementCount, bitWidth));
      alpha::FixedBitArray fixedBitArray(buffer.get(), bitWidth);
      std::vector<uint32_t> randomValues(elementCount);
      std::vector<uint32_t> randomValuesWithBaseline(elementCount);
      for (int i = 0; i < elementCount; ++i) {
        randomValues[i] = folly::Random::rand32(rng) % (maxElement - baseline);
        randomValuesWithBaseline[i] = randomValues[i] + baseline;
      }
      const int offset = folly::Random::rand32(rng) % elementCount;
      const int size = elementCount - offset;
      fixedBitArray.bulkSet32WithBaseline(
          offset, size, randomValuesWithBaseline.data() + offset, baseline);
      for (int i = 0; i < size; ++i) {
        ASSERT_EQ(fixedBitArray.get32(offset + i), randomValues[offset + i])
            << "Bit Width: " << bitWidth << ", i: " << i;
      }
    }
  }
}

TEST(FixedBitArrayTests, Equals32Random) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (int bitWidth = 1; bitWidth <= 32; ++bitWidth) {
    const uint64_t elementMask = (1ULL << bitWidth) - 1;
    for (int test = 0; test < kNumTestsPerBitWidth; ++test) {
      const int elementCount = 1 + folly::Random::rand32(rng) % kMaxElements;
      auto buffer = std::make_unique<char[]>(
          alpha::FixedBitArray::bufferSize(elementCount, bitWidth));
      alpha::FixedBitArray fixedBitArray(buffer.get(), bitWidth);
      std::vector<uint32_t> randomValues(elementCount);
      for (int i = 0; i < elementCount; ++i) {
        // Test both the full range and also a case where equals are actually
        // likely.
        randomValues[i] = folly::Random::rand32(rng) & elementMask;
        if (test % 2 == 1) {
          randomValues[i] = randomValues[i] % 10;
        }
        fixedBitArray.set32(i, randomValues[i]);
      }
      // Remember start needs to be a multiple of 64.
      const int start =
          (folly::Random::rand32(rng) % elementCount) & (0xFFFFFF00);
      const int length = elementCount - start;
      auto equalsBuffer =
          std::make_unique<char[]>(alpha::FixedBitArray::bufferSize(length, 1));
      const uint32_t equalsValue =
          randomValues[folly::Random::rand32(rng) % elementCount];
      fixedBitArray.equals32(start, length, equalsValue, equalsBuffer.get());
      for (int i = 0; i < length; ++i) {
        ASSERT_EQ(
            alpha::bits::getBit(i, equalsBuffer.get()),
            randomValues[start + i] == equalsValue);
      }
    }
  }
}
