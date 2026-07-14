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
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/ConstantEncoding.h"
#include "dwio/nimble/encodings/DictionaryEncoding.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/memory/Memory.h"

using namespace facebook;

// Covers the Encoding::get<T>() template and its virtual backend getImpl(),
// introduced in D107131568, across the four opted-in encodings:
// ConstantEncoding, TrivialEncoding (non-string / non-bool),
// FixedBitWidthEncoding, DictionaryEncoding. Also verifies the base default
// (NIMBLE_NOT_IMPLEMENTED) fires for encodings that opt out — the
// TrivialEncoding<std::string_view> specialization.
class EncodingGetTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  template <typename T>
  nimble::Vector<T> toNimbleVector(const std::vector<T>& values) {
    nimble::Vector<T> v{pool_.get()};
    v.insert(v.end(), values.data(), values.data() + values.size());
    return v;
  }

  std::function<void*(uint32_t)> stringBufferFactory() {
    return [this](uint32_t totalLength) {
      auto& buf = stringBuffers_.emplace_back(
          velox::AlignedBuffer::allocate<char>(totalLength, pool_.get()));
      return buf->asMutable<void>();
    };
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
  std::vector<velox::BufferPtr> stringBuffers_;
};

// =============================================================================
// ConstantEncoding::get<T>() — returns the single stored value, row ignored.
// =============================================================================

TEST_F(EncodingGetTest, constantGetReturnsConstantForEveryRow) {
  const std::vector<uint32_t> src(8, 42u);
  auto values = toNimbleVector(src);
  auto encoding =
      nimble::test::Encoder<nimble::ConstantEncoding<uint32_t>>::createEncoding(
          *buffer_, values, stringBufferFactory());
  for (uint32_t row = 0; row < src.size(); ++row) {
    EXPECT_EQ(encoding->get<uint32_t>(row), 42u) << "row=" << row;
  }
}

TEST_F(EncodingGetTest, constantGetIgnoresRowIndex) {
  const std::vector<int64_t> src(3, -7);
  auto values = toNimbleVector(src);
  auto encoding =
      nimble::test::Encoder<nimble::ConstantEncoding<int64_t>>::createEncoding(
          *buffer_, values, stringBufferFactory());
  // Row index far beyond rowCount() must still return the constant — Constant
  // encoding stores only one value and never indexes into an array.
  EXPECT_EQ(encoding->get<int64_t>(999'999), -7);
}

TEST_F(EncodingGetTest, constantGetMatchesMaterialize) {
  const std::vector<uint64_t> src(50, std::numeric_limits<uint64_t>::max());
  auto values = toNimbleVector(src);
  auto encoding =
      nimble::test::Encoder<nimble::ConstantEncoding<uint64_t>>::createEncoding(
          *buffer_, values, stringBufferFactory());
  nimble::Vector<uint64_t> materialized(pool_.get(), src.size());
  encoding->materialize(src.size(), materialized.data());
  for (uint32_t row = 0; row < src.size(); ++row) {
    EXPECT_EQ(encoding->get<uint64_t>(row), materialized[row]);
  }
}

// =============================================================================
// TrivialEncoding<numeric>::get<T>() — direct array index into values_.
// =============================================================================

TEST_F(EncodingGetTest, trivialGetReadsValueAtIndex) {
  const std::vector<int32_t> src{10, -20, 30, -40, 50, 0, 1};
  auto values = toNimbleVector(src);
  auto encoding =
      nimble::test::Encoder<nimble::TrivialEncoding<int32_t>>::createEncoding(
          *buffer_, values, stringBufferFactory());
  for (uint32_t row = 0; row < src.size(); ++row) {
    EXPECT_EQ(encoding->get<int32_t>(row), src[row]) << "row=" << row;
  }
}

TEST_F(EncodingGetTest, trivialGetPermutedAccessReturnsCorrectValues) {
  const std::vector<uint32_t> src{100, 200, 300, 400, 500, 600};
  auto values = toNimbleVector(src);
  auto encoding =
      nimble::test::Encoder<nimble::TrivialEncoding<uint32_t>>::createEncoding(
          *buffer_, values, stringBufferFactory());
  // Permuted + repeated indices to catch any implicit cursor advance in get().
  const std::vector<uint32_t> order{5, 0, 3, 1, 4, 2, 3, 0, 5, 5, 0};
  for (uint32_t row : order) {
    EXPECT_EQ(encoding->get<uint32_t>(row), src[row]) << "row=" << row;
  }
}

TEST_F(EncodingGetTest, trivialGetMatchesMaterialize) {
  const std::vector<int64_t> src{-1, 2, -3, 4, -5, 6, -7, 8};
  auto values = toNimbleVector(src);
  auto encoding =
      nimble::test::Encoder<nimble::TrivialEncoding<int64_t>>::createEncoding(
          *buffer_, values, stringBufferFactory());
  nimble::Vector<int64_t> materialized(pool_.get(), src.size());
  encoding->materialize(src.size(), materialized.data());
  for (uint32_t row = 0; row < src.size(); ++row) {
    EXPECT_EQ(encoding->get<int64_t>(row), materialized[row]);
  }
}

// =============================================================================
// FixedBitWidthEncoding::get<T>() — O(1) bit extract + baseline add.
// =============================================================================

TEST_F(EncodingGetTest, fixedBitWidthGetReadsValuesAtEachRow) {
  // min=100 forces a non-zero baseline; the encoded payload stores
  // (value - baseline). A bug that drops the "+ baseline_" term would return
  // small numbers here instead of the original values.
  const std::vector<uint32_t> src{100, 101, 200, 150, 175, 100, 250, 199};
  auto values = toNimbleVector(src);
  auto encoding =
      nimble::test::Encoder<nimble::FixedBitWidthEncoding<uint32_t>>::
          createEncoding(*buffer_, values, stringBufferFactory());
  for (uint32_t row = 0; row < src.size(); ++row) {
    EXPECT_EQ(encoding->get<uint32_t>(row), src[row]) << "row=" << row;
  }
}

TEST_F(EncodingGetTest, fixedBitWidthGetHandlesWideRangeOfBitWidths) {
  // Interleave increasing max values so the encoder selects a bit-width wide
  // enough for 2^20-1. Exercises FixedBitArray::get() bit-extraction paths.
  std::vector<uint32_t> src;
  src.push_back(0);
  for (uint32_t bits = 1; bits <= 20; ++bits) {
    src.push_back((1u << bits) - 1);
    src.push_back(1u);
  }
  auto values = toNimbleVector(src);
  auto encoding =
      nimble::test::Encoder<nimble::FixedBitWidthEncoding<uint32_t>>::
          createEncoding(*buffer_, values, stringBufferFactory());
  for (uint32_t row = 0; row < src.size(); ++row) {
    EXPECT_EQ(encoding->get<uint32_t>(row), src[row]) << "row=" << row;
  }
}

TEST_F(EncodingGetTest, fixedBitWidthGetIsStatelessAcrossMaterialize) {
  // Concurrent StripeGroup readers rely on get() being stateless. Interleave
  // get() with materialize() and skip() (both advance the row cursor) and
  // verify get() is absolute-indexed — a bug that advances an internal cursor
  // in get() would surface as an off-by-N read here.
  const std::vector<uint32_t> src{7, 3, 9, 1, 12, 5, 8, 6, 4, 11};
  auto values = toNimbleVector(src);
  auto encoding =
      nimble::test::Encoder<nimble::FixedBitWidthEncoding<uint32_t>>::
          createEncoding(*buffer_, values, stringBufferFactory());

  EXPECT_EQ(encoding->get<uint32_t>(5), src[5]);

  nimble::Vector<uint32_t> chunk(pool_.get(), 4);
  encoding->materialize(4, chunk.data());
  EXPECT_EQ(encoding->get<uint32_t>(3), src[3]);

  encoding->skip(2);
  EXPECT_EQ(encoding->get<uint32_t>(0), src[0]);
}

TEST_F(EncodingGetTest, fixedBitWidthGetMatchesMaterialize) {
  std::vector<uint32_t> src;
  src.reserve(64);
  for (uint32_t i = 0; i < 64; ++i) {
    src.push_back(1000 + i * 17);
  }
  auto values = toNimbleVector(src);
  auto encoding =
      nimble::test::Encoder<nimble::FixedBitWidthEncoding<uint32_t>>::
          createEncoding(*buffer_, values, stringBufferFactory());
  nimble::Vector<uint32_t> materialized(pool_.get(), src.size());
  encoding->materialize(src.size(), materialized.data());
  encoding->reset();
  for (uint32_t row = 0; row < src.size(); ++row) {
    EXPECT_EQ(encoding->get<uint32_t>(row), materialized[row]) << "row=" << row;
  }
}

// =============================================================================
// DictionaryEncoding::get<T>() — recursive: child indices get() then
// alphabet_[].
// =============================================================================

TEST_F(EncodingGetTest, dictionaryGetSmallAlphabetLookup) {
  // 3 unique values, cycled 8 times. Verifies both the indices path (via the
  // child FixedBitWidth encoding's get()) and the alphabet indirection.
  const std::vector<uint32_t> src{100, 200, 300, 100, 200, 300, 100, 200};
  auto values = toNimbleVector(src);
  auto encoding = nimble::test::Encoder<nimble::DictionaryEncoding<uint32_t>>::
      createEncoding(*buffer_, values, stringBufferFactory());
  for (uint32_t row = 0; row < src.size(); ++row) {
    EXPECT_EQ(encoding->get<uint32_t>(row), src[row]) << "row=" << row;
  }
}

TEST_F(EncodingGetTest, dictionaryGetSingleAlphabetEntry) {
  // Single-entry alphabet: every index is 0. Exercises the alphabet_[0]
  // lookup with a degenerate indices distribution.
  const std::vector<int64_t> src(50, 42);
  auto values = toNimbleVector(src);
  auto encoding = nimble::test::Encoder<nimble::DictionaryEncoding<int64_t>>::
      createEncoding(*buffer_, values, stringBufferFactory());
  for (uint32_t row = 0; row < src.size(); ++row) {
    EXPECT_EQ(encoding->get<int64_t>(row), 42);
  }
}

TEST_F(EncodingGetTest, dictionaryGetLargerAlphabetCyclesCorrectly) {
  // Non-trivial alphabet with values that don't equal their indices — catches
  // a bug that returns the index directly instead of alphabet_[index].
  const std::vector<uint32_t> alphabet{7, 13, 100, 250, 999};
  std::vector<uint32_t> src;
  src.reserve(40);
  for (uint32_t i = 0; i < 40; ++i) {
    src.push_back(alphabet[i % alphabet.size()]);
  }
  auto values = toNimbleVector(src);
  auto encoding = nimble::test::Encoder<nimble::DictionaryEncoding<uint32_t>>::
      createEncoding(*buffer_, values, stringBufferFactory());
  for (uint32_t row = 0; row < src.size(); ++row) {
    EXPECT_EQ(encoding->get<uint32_t>(row), src[row]) << "row=" << row;
  }
}

TEST_F(EncodingGetTest, dictionaryGetMatchesMaterialize) {
  const std::vector<uint32_t> alphabet{7, 13, 100, 250, 999};
  std::vector<uint32_t> src;
  src.reserve(40);
  for (uint32_t i = 0; i < 40; ++i) {
    src.push_back(alphabet[i % alphabet.size()]);
  }
  auto values = toNimbleVector(src);
  auto encoding = nimble::test::Encoder<nimble::DictionaryEncoding<uint32_t>>::
      createEncoding(*buffer_, values, stringBufferFactory());
  nimble::Vector<uint32_t> materialized(pool_.get(), src.size());
  encoding->materialize(src.size(), materialized.data());
  encoding->reset();
  for (uint32_t row = 0; row < src.size(); ++row) {
    EXPECT_EQ(encoding->get<uint32_t>(row), materialized[row]) << "row=" << row;
  }
}

// =============================================================================
// Base default getImpl() — NIMBLE_NOT_IMPLEMENTED. TrivialEncoding<string_view>
// is a full specialization that does NOT override getImpl(), so get<T>() hits
// the base default and must throw.
// =============================================================================

TEST_F(EncodingGetTest, baseDefaultNotImplementedForTrivialString) {
  const std::vector<std::string_view> src{"foo", "bar", "baz"};
  auto values = toNimbleVector(src);
  auto encoding =
      nimble::test::Encoder<nimble::TrivialEncoding<std::string_view>>::
          createEncoding(*buffer_, values, stringBufferFactory());
  EXPECT_THROW(encoding->get<std::string_view>(0), nimble::NimbleInternalError);
}
