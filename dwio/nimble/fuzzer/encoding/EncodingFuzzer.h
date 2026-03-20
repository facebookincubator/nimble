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
#pragma once

#include <cmath>
#include <limits>
#include <random>
#include <string_view>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "folly/Random.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble::test {

// ============================================================================
// Data generators
// ============================================================================

template <typename T, typename RNG>
Vector<T> makeMonotonicData(
    velox::memory::MemoryPool& pool,
    RNG& rng,
    uint32_t rowCount,
    [[maybe_unused]] Buffer* buffer) {
  Vector<T> data(&pool);
  data.reserve(rowCount);
  if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>) {
    T current = T{0};
    for (uint32_t i = 0; i < rowCount; ++i) {
      data.push_back(current);
      current += static_cast<T>(1 + folly::Random::rand32(rng) % 10);
    }
  }
  return data;
}

template <typename T, typename RNG>
Vector<T> makeSingleValueData(
    velox::memory::MemoryPool& pool,
    RNG& rng,
    uint32_t rowCount,
    Buffer* buffer) {
  Vector<T> data(&pool);
  data.reserve(rowCount);
  Vector<T> seed(&pool);
  seed.reserve(1);
  nimble::testing::addRandomData<T>(rng, 1, &seed, buffer);
  for (uint32_t i = 0; i < rowCount; ++i) {
    data.push_back(seed[0]);
  }
  return data;
}

template <typename T, typename RNG>
Vector<T> makeBoundaryMixedData(
    velox::memory::MemoryPool& pool,
    RNG& rng,
    uint32_t rowCount,
    Buffer* buffer) {
  Vector<T> data(&pool);
  data.reserve(rowCount);
  nimble::testing::addRandomData<T>(rng, rowCount, &data, buffer);
  if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>) {
    for (uint32_t i = 0; i < rowCount && i < 20; i += 3) {
      if (i < data.size()) {
        data[i] = std::numeric_limits<T>::min();
      }
      if (i + 1 < data.size()) {
        data[i + 1] = std::numeric_limits<T>::max();
      }
      if (i + 2 < data.size()) {
        data[i + 2] = T{0};
      }
    }
  }
  return data;
}

// ============================================================================
// Fuzzer operations
// ============================================================================

enum class FuzzerOp {
  MaterializeAll,
  MaterializeChunked,
  MaterializeOneByOne,
  SkipAndMaterialize,
  ResetAndMaterialize,
  MaterializeVariableBlocks,
};

// ============================================================================
// Core fuzzer
// ============================================================================

/// Generic encoding fuzzer parameterized by encoding class. Performs randomized
/// encode-decode-verify cycles with diverse data and access patterns.
template <typename EncodingClass>
class EncodingFuzzer {
  using T = typename EncodingClass::cppDataType;

 public:
  EncodingFuzzer(
      uint32_t iterations,
      uint32_t maxRows,
      uint32_t seed,
      bool testCompression)
      : iterations_{iterations},
        maxRows_{maxRows},
        seed_{seed},
        testCompression_{testCompression} {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<Buffer>(*pool_);
  }

  void run() {
    uint32_t seed = seed_;
    if (seed == 0) {
      seed = folly::Random::rand32();
    }
    LOG(INFO) << "EncodingFuzzer seed: " << seed << " encoding: "
              << toString(Encoder<EncodingClass>::encodingType())
              << " dtype: " << toString(TypeTraits<T>::dataType)
              << " iterations: " << iterations_ << " maxRows: " << maxRows_;
    std::mt19937 rng(seed);

    for (uint32_t iter = 0; iter < iterations_; ++iter) {
      buffer_ = std::make_unique<Buffer>(*pool_);
      auto datasets = generateDatasets(rng);

      for (auto& data : datasets) {
        if (data.empty()) {
          continue;
        }
        auto compressionModes = testCompression_
            ? std::vector<bool>{false, true}
            : std::vector<bool>{false};

        for (bool compress : compressionModes) {
          runSingleIteration(rng, data, compress);
        }
      }
    }
  }

 private:
  std::vector<Vector<T>> generateDatasets(std::mt19937& rng) {
    std::vector<Vector<T>> datasets;
    uint32_t rowCount = 1 + folly::Random::rand32(rng) % maxRows_;

    // Random data.
    {
      Vector<T> data(pool_.get());
      data.reserve(rowCount);
      nimble::testing::addRandomData<T>(rng, rowCount, &data, buffer_.get());
      datasets.push_back(std::move(data));
    }

    // Single value (constant).
    datasets.push_back(
        makeSingleValueData<T>(*pool_, rng, rowCount, buffer_.get()));

    // Boundary/edge case values mixed in.
    if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>) {
      datasets.push_back(
          makeBoundaryMixedData<T>(*pool_, rng, rowCount, buffer_.get()));
    }

    // Monotonic data (good for varint).
    if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>) {
      datasets.push_back(
          makeMonotonicData<T>(*pool_, rng, rowCount, buffer_.get()));
    }

    // Small data (1-3 rows) for edge cases.
    for (uint32_t sz : {1u, 2u, 3u}) {
      Vector<T> small(pool_.get());
      small.reserve(sz);
      nimble::testing::addRandomData<T>(rng, sz, &small, buffer_.get());
      datasets.push_back(std::move(small));
    }

    return datasets;
  }

  void
  runSingleIteration(std::mt19937& rng, const Vector<T>& data, bool compress) {
    Buffer encodeBuffer(*pool_);
    std::vector<velox::BufferPtr> stringBuffers;
    auto stringBufferFactory = [&](uint32_t totalLength) {
      auto& buf = stringBuffers.emplace_back(
          velox::AlignedBuffer::allocate<char>(totalLength, pool_.get()));
      return buf->template asMutable<void>();
    };

    auto compressionType =
        compress ? CompressionType::Zstd : CompressionType::Uncompressed;

    std::string_view encoded;
    try {
      encoded =
          Encoder<EncodingClass>::encode(encodeBuffer, data, compressionType);
    } catch (const NimbleUserError& e) {
      if (e.errorCode() == error_code::IncompatibleEncoding) {
        return;
      }
      throw;
    }

    auto encoding =
        std::make_unique<EncodingClass>(*pool_, encoded, stringBufferFactory);

    EXPECT_EQ(encoding->dataType(), TypeTraits<T>::dataType);
    EXPECT_EQ(encoding->rowCount(), data.size());

    // Pick random operations to perform.
    std::vector<FuzzerOp> ops = {
        FuzzerOp::MaterializeAll,
        FuzzerOp::MaterializeChunked,
        FuzzerOp::MaterializeOneByOne,
        FuzzerOp::SkipAndMaterialize,
        FuzzerOp::ResetAndMaterialize,
        FuzzerOp::MaterializeVariableBlocks,
    };

    std::shuffle(ops.begin(), ops.end(), rng);
    uint32_t numOps =
        1 + folly::Random::rand32(rng) % static_cast<uint32_t>(ops.size());

    for (uint32_t opIdx = 0; opIdx < numOps; ++opIdx) {
      encoding->reset();

      switch (ops[opIdx]) {
        case FuzzerOp::MaterializeAll:
          verifyMaterializeAll(*encoding, data);
          break;
        case FuzzerOp::MaterializeChunked:
          verifyMaterializeChunked(rng, *encoding, data);
          break;
        case FuzzerOp::MaterializeOneByOne:
          verifyMaterializeOneByOne(*encoding, data);
          break;
        case FuzzerOp::SkipAndMaterialize:
          verifySkipAndMaterialize(rng, *encoding, data);
          break;
        case FuzzerOp::ResetAndMaterialize:
          verifyResetAndMaterialize(*encoding, data);
          break;
        case FuzzerOp::MaterializeVariableBlocks:
          verifyMaterializeVariableBlocks(*encoding, data);
          break;
      }
    }
  }

  void verifyMaterializeAll(Encoding& encoding, const Vector<T>& expected) {
    Vector<T> actual(pool_.get(), expected.size());
    encoding.materialize(expected.size(), actual.data());

    for (uint32_t i = 0; i < expected.size(); ++i) {
      verifyEqual(expected[i], actual[i], i);
    }
  }

  void verifyMaterializeChunked(
      std::mt19937& rng,
      Encoding& encoding,
      const Vector<T>& expected) {
    uint32_t splitPoint = 1 + folly::Random::rand32(rng) % (expected.size());
    if (splitPoint >= expected.size()) {
      splitPoint = expected.size() / 2;
    }
    if (splitPoint == 0) {
      splitPoint = 1;
    }

    Vector<T> actual(pool_.get(), expected.size());

    encoding.materialize(splitPoint, actual.data());
    for (uint32_t i = 0; i < splitPoint; ++i) {
      verifyEqual(expected[i], actual[i], i);
    }

    encoding.materialize(
        static_cast<uint32_t>(expected.size()) - splitPoint, actual.data());
    for (uint32_t i = 0; i < expected.size() - splitPoint; ++i) {
      verifyEqual(expected[splitPoint + i], actual[i], splitPoint + i);
    }
  }

  void verifyMaterializeOneByOne(
      Encoding& encoding,
      const Vector<T>& expected) {
    Vector<T> single(pool_.get(), 1);
    for (uint32_t i = 0; i < expected.size(); ++i) {
      encoding.materialize(1, single.data());
      verifyEqual(expected[i], single[0], i);
    }
  }

  void verifySkipAndMaterialize(
      std::mt19937& rng,
      Encoding& encoding,
      const Vector<T>& expected) {
    uint32_t offset = folly::Random::rand32(rng) % expected.size();
    uint32_t length =
        1 + folly::Random::rand32(rng) % (expected.size() - offset);

    encoding.skip(offset);
    Vector<T> actual(pool_.get(), length);
    encoding.materialize(length, actual.data());

    for (uint32_t i = 0; i < length; ++i) {
      verifyEqual(expected[offset + i], actual[i], offset + i);
    }
  }

  void verifyResetAndMaterialize(
      Encoding& encoding,
      const Vector<T>& expected) {
    Vector<T> first(pool_.get(), expected.size());
    encoding.materialize(expected.size(), first.data());
    for (uint32_t i = 0; i < expected.size(); ++i) {
      verifyEqual(expected[i], first[i], i);
    }

    encoding.reset();
    Vector<T> second(pool_.get(), expected.size());
    encoding.materialize(static_cast<uint32_t>(expected.size()), second.data());
    for (uint32_t i = 0; i < expected.size(); ++i) {
      verifyEqual(expected[i], second[i], i);
    }
  }

  void verifyMaterializeVariableBlocks(
      Encoding& encoding,
      const Vector<T>& expected) {
    Vector<T> actual(pool_.get(), expected.size());
    uint32_t start = 0;
    uint32_t len = 1;
    while (start + len <= expected.size()) {
      encoding.materialize(len, actual.data());
      for (uint32_t j = 0; j < len; ++j) {
        verifyEqual(expected[start + j], actual[j], start + j);
      }
      start += len;
      ++len;
    }
    uint32_t remaining = expected.size() - start;
    if (remaining > 0) {
      encoding.materialize(remaining, actual.data());
      for (uint32_t j = 0; j < remaining; ++j) {
        verifyEqual(expected[start + j], actual[j], start + j);
      }
    }
  }

  void verifyEqual(const T& expected, const T& actual, uint32_t index) {
    if constexpr (std::is_floating_point_v<T>) {
      if (std::isnan(expected)) {
        ASSERT_TRUE(std::isnan(actual))
            << "Expected NaN at index " << index << " but got " << actual;
        using UintType =
            std::conditional_t<std::is_same_v<T, float>, uint32_t, uint64_t>;
        auto expectedBits = *reinterpret_cast<const UintType*>(&expected);
        auto actualBits = *reinterpret_cast<const UintType*>(&actual);
        ASSERT_EQ(expectedBits, actualBits)
            << "NaN bit pattern mismatch at index " << index;
      } else {
        ASSERT_EQ(expected, actual) << "Mismatch at index " << index;
      }
    } else {
      ASSERT_EQ(expected, actual) << "Mismatch at index " << index;
    }
  }

  uint32_t iterations_;
  uint32_t maxRows_;
  uint32_t seed_;
  bool testCompression_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<Buffer> buffer_;
};

} // namespace facebook::nimble::test
