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
#include "dwio/nimble/encodings/DeltaEncoding.h"
#include <gtest/gtest.h>
#include <array>
#include <limits>
#include <numeric>
#include <vector>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/tests/NimbleCompare.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"

using namespace facebook;

template <typename DataType, bool UseVarint>
struct TestConfig {
  using data_type = DataType;
  static constexpr bool useVarint = UseVarint;
};

#define TC(T) TestConfig<T, false>, TestConfig<T, true>

template <typename Config>
class DeltaEncodingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  template <typename T>
  nimble::Vector<T> toVector(std::initializer_list<T> l) {
    nimble::Vector<T> v{pool_.get()};
    v.insert(v.end(), l.begin(), l.end());
    return v;
  }

  template <typename T>
  std::vector<nimble::Vector<T>> prepareValues() {
    FAIL() << "unspecialized prepareValues() should not be called";
    return {};
  }

  template <>
  std::vector<nimble::Vector<int32_t>> prepareValues() {
    return {
        // Monotonically increasing — all deltas, no restatements.
        toVector({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
        // Non-monotonic — triggers restatements on decrease.
        toVector({1, 2, 4, 1, 2, 3, 4, 1, 2, 4, 8, 8}),
        // Single element.
        toVector({42}),
        // Constant — all same values, deltas are all 0.
        toVector({5, 5, 5, 5, 5}),
        // Signed: negative to positive — triggers zero-crossing restatement.
        toVector({-5, -3, -1, 2, 4, 6}),
        // Signed: all negative, increasing (toward zero).
        toVector({-10, -8, -6, -4, -2}),
        // Signed: decreasing from positive to negative.
        toVector({5, 3, 1, -1, -3, -5}),
        // Large gaps.
        toVector({0, 1000, 2000, 3000, 100, 200, 300}),
    };
  }

  template <>
  std::vector<nimble::Vector<uint32_t>> prepareValues() {
    return {
        // Monotonically increasing.
        toVector<uint32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
        // Non-monotonic.
        toVector<uint32_t>({10, 20, 30, 5, 15, 25}),
        // Single element.
        toVector<uint32_t>({99}),
        // Constant.
        toVector<uint32_t>({7, 7, 7, 7}),
    };
  }

  template <>
  std::vector<nimble::Vector<int64_t>> prepareValues() {
    return {
        // Monotonically increasing.
        toVector<int64_t>({100, 200, 300, 400, 500}),
        // Non-monotonic with restatements.
        toVector<int64_t>({100, 200, 50, 150, 250}),
        // Signed zero-crossing.
        toVector<int64_t>({-100, -50, 0, 50, 100}),
    };
  }

  template <>
  std::vector<nimble::Vector<uint64_t>> prepareValues() {
    return {
        toVector<uint64_t>({10, 20, 30, 40, 50}),
        toVector<uint64_t>({100, 200, 50, 75, 300}),
    };
  }

  template <>
  std::vector<nimble::Vector<int16_t>> prepareValues() {
    return {
        toVector<int16_t>({1, 2, 3, 4, 5}),
        toVector<int16_t>({5, 3, 1, -1, -3}),
    };
  }

  template <>
  std::vector<nimble::Vector<uint16_t>> prepareValues() {
    return {
        toVector<uint16_t>({1, 2, 3, 4, 5}),
        toVector<uint16_t>({10, 5, 15, 3, 20}),
    };
  }

  template <>
  std::vector<nimble::Vector<int8_t>> prepareValues() {
    return {
        toVector<int8_t>({1, 2, 3, 4, 5}),
        toVector<int8_t>({-5, -3, -1, 1, 3}),
    };
  }

  template <>
  std::vector<nimble::Vector<uint8_t>> prepareValues() {
    return {
        toVector<uint8_t>({1, 2, 3, 4, 5}),
        toVector<uint8_t>({10, 5, 15, 3, 20}),
    };
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
};

#define NUM_TYPES                                                  \
  TC(int8_t), TC(uint8_t), TC(int16_t), TC(uint16_t), TC(int32_t), \
      TC(uint32_t), TC(int64_t), TC(uint64_t)

using TestTypes = ::testing::Types<NUM_TYPES>;

TYPED_TEST_CASE(DeltaEncodingTest, TestTypes);

TYPED_TEST(DeltaEncodingTest, SerializeThenDeserialize) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  auto valueGroups = this->template prepareValues<D>();
  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buffer->template asMutable<void>();
  };
  for (const auto& values : valueGroups) {
    auto encoding =
        nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
            *this->buffer_,
            values,
            stringBufferFactory,
            nimble::CompressionType::Uncompressed,
            options);
    uint32_t rowCount = values.size();
    nimble::Vector<D> result(this->pool_.get(), rowCount);
    encoding->materialize(rowCount, result.data());

    EXPECT_EQ(encoding->encodingType(), nimble::EncodingType::Delta);
    EXPECT_EQ(encoding->dataType(), nimble::TypeTraits<D>::dataType);
    EXPECT_EQ(encoding->rowCount(), rowCount);
    for (uint32_t i = 0; i < rowCount; ++i) {
      EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]))
          << "Mismatch at index " << i << ": expected " << values[i] << " got "
          << result[i];
    }
  }
}

TYPED_TEST(DeltaEncodingTest, ResetAndMaterializeInParts) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  auto valueGroups = this->template prepareValues<D>();
  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buffer->template asMutable<void>();
  };
  for (const auto& values : valueGroups) {
    auto encoding =
        nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
            *this->buffer_,
            values,
            stringBufferFactory,
            nimble::CompressionType::Uncompressed,
            options);
    uint32_t rowCount = values.size();

    // Materialize in two halves.
    uint32_t firstHalf = rowCount / 2;
    uint32_t secondHalf = rowCount - firstHalf;
    nimble::Vector<D> result(this->pool_.get(), rowCount);

    encoding->materialize(firstHalf, result.data());
    for (uint32_t i = 0; i < firstHalf; ++i) {
      EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]))
          << "First half mismatch at index " << i;
    }

    encoding->materialize(secondHalf, result.data());
    for (uint32_t i = 0; i < secondHalf; ++i) {
      EXPECT_TRUE(
          nimble::NimbleCompare<D>::equals(result[i], values[firstHalf + i]))
          << "Second half mismatch at index " << i;
    }

    // Reset and read one at a time.
    encoding->reset();
    for (uint32_t i = 0; i < rowCount; ++i) {
      D val;
      encoding->materialize(1, &val);
      EXPECT_TRUE(nimble::NimbleCompare<D>::equals(val, values[i]))
          << "One-at-a-time mismatch at index " << i;
    }
  }
}

TYPED_TEST(DeltaEncodingTest, SkipThenMaterialize) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  auto valueGroups = this->template prepareValues<D>();
  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buffer->template asMutable<void>();
  };
  for (const auto& values : valueGroups) {
    if (values.size() < 3) {
      continue;
    }
    auto encoding =
        nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
            *this->buffer_,
            values,
            stringBufferFactory,
            nimble::CompressionType::Uncompressed,
            options);
    uint32_t rowCount = values.size();
    uint32_t skipCount = rowCount / 3;
    uint32_t readCount = rowCount - skipCount;

    encoding->skip(skipCount);
    nimble::Vector<D> result(this->pool_.get(), readCount);
    encoding->materialize(readCount, result.data());

    for (uint32_t i = 0; i < readCount; ++i) {
      EXPECT_TRUE(
          nimble::NimbleCompare<D>::equals(result[i], values[skipCount + i]))
          << "Skip+materialize mismatch at index " << i;
    }
  }
}

TYPED_TEST(DeltaEncodingTest, BoundaryValues) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  // Build a vector with min, max, and adjacent values for this type.
  nimble::Vector<D> values{this->pool_.get()};
  values.push_back(std::numeric_limits<D>::min());
  values.push_back(static_cast<D>(std::numeric_limits<D>::min() + 1));
  values.push_back(static_cast<D>(std::numeric_limits<D>::max() - 1));
  values.push_back(std::numeric_limits<D>::max());
  // Drop back to min — forces a restatement.
  values.push_back(std::numeric_limits<D>::min());
  values.push_back(std::numeric_limits<D>::max());

  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buffer->template asMutable<void>();
  };
  auto encoding =
      nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
          *this->buffer_,
          values,
          stringBufferFactory,
          nimble::CompressionType::Uncompressed,
          options);
  uint32_t rowCount = values.size();
  nimble::Vector<D> result(this->pool_.get(), rowCount);
  encoding->materialize(rowCount, result.data());

  for (uint32_t i = 0; i < rowCount; ++i) {
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]))
        << "BoundaryValues mismatch at index " << i;
  }
}

TYPED_TEST(DeltaEncodingTest, AllZeros) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  nimble::Vector<D> values{this->pool_.get()};
  for (int i = 0; i < 20; ++i) {
    values.push_back(static_cast<D>(0));
  }

  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buffer->template asMutable<void>();
  };
  auto encoding =
      nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
          *this->buffer_,
          values,
          stringBufferFactory,
          nimble::CompressionType::Uncompressed,
          options);
  nimble::Vector<D> result(this->pool_.get(), values.size());
  encoding->materialize(values.size(), result.data());

  for (uint32_t i = 0; i < values.size(); ++i) {
    EXPECT_EQ(result[i], static_cast<D>(0)) << "AllZeros mismatch at " << i;
  }
}

TYPED_TEST(DeltaEncodingTest, StrictlyDecreasing) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  // Strictly decreasing — every row after the first is a restatement.
  nimble::Vector<D> values{this->pool_.get()};
  constexpr int kCount = 15;
  for (int i = 0; i < kCount; ++i) {
    values.push_back(static_cast<D>(kCount - i));
  }

  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buffer->template asMutable<void>();
  };
  auto encoding =
      nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
          *this->buffer_,
          values,
          stringBufferFactory,
          nimble::CompressionType::Uncompressed,
          options);
  nimble::Vector<D> result(this->pool_.get(), kCount);
  encoding->materialize(kCount, result.data());

  for (uint32_t i = 0; i < kCount; ++i) {
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]))
        << "StrictlyDecreasing mismatch at " << i;
  }
}

TYPED_TEST(DeltaEncodingTest, AlternatingMinMax) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  // Alternating min/max — restatements on every row after the first.
  nimble::Vector<D> values{this->pool_.get()};
  constexpr int kCount = 10;
  for (int i = 0; i < kCount; ++i) {
    values.push_back(
        i % 2 == 0 ? std::numeric_limits<D>::min()
                   : std::numeric_limits<D>::max());
  }

  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buffer->template asMutable<void>();
  };
  auto encoding =
      nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
          *this->buffer_,
          values,
          stringBufferFactory,
          nimble::CompressionType::Uncompressed,
          options);
  nimble::Vector<D> result(this->pool_.get(), kCount);
  encoding->materialize(kCount, result.data());

  for (uint32_t i = 0; i < kCount; ++i) {
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]))
        << "AlternatingMinMax mismatch at " << i;
  }
}

TYPED_TEST(DeltaEncodingTest, SkipZeroRows) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  auto values = this->template toVector<D>({1, 2, 3, 4, 5});

  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buffer->template asMutable<void>();
  };
  auto encoding =
      nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
          *this->buffer_,
          values,
          stringBufferFactory,
          nimble::CompressionType::Uncompressed,
          options);

  // skip(0) should be a no-op.
  encoding->skip(0);

  nimble::Vector<D> result(this->pool_.get(), 5);
  encoding->materialize(5, result.data());

  for (uint32_t i = 0; i < 5; ++i) {
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]))
        << "SkipZeroRows mismatch at " << i;
  }
}

TYPED_TEST(DeltaEncodingTest, SkipAllThenMaterialize) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  auto values = this->template toVector<D>({1, 2, 3, 4, 5});

  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buffer->template asMutable<void>();
  };
  auto encoding =
      nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
          *this->buffer_,
          values,
          stringBufferFactory,
          nimble::CompressionType::Uncompressed,
          options);

  // Skip all rows.
  encoding->skip(5);

  // Reset, skip some, materialize the rest.
  encoding->reset();
  encoding->skip(3);
  nimble::Vector<D> result(this->pool_.get(), 2);
  encoding->materialize(2, result.data());

  for (uint32_t i = 0; i < 2; ++i) {
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[3 + i]))
        << "SkipAllThenMaterialize mismatch at " << i;
  }
}

TYPED_TEST(DeltaEncodingTest, MultipleSkipMaterializeCycles) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  nimble::Vector<D> values{this->pool_.get()};
  for (int i = 0; i < 12; ++i) {
    values.push_back(static_cast<D>(i + 1));
  }

  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buffer->template asMutable<void>();
  };
  auto encoding =
      nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
          *this->buffer_,
          values,
          stringBufferFactory,
          nimble::CompressionType::Uncompressed,
          options);

  // skip(2), read(2), skip(2), read(2), skip(2), read(2)
  for (int cycle = 0; cycle < 3; ++cycle) {
    encoding->skip(2);
    nimble::Vector<D> result(this->pool_.get(), 2);
    encoding->materialize(2, result.data());
    uint32_t offset = cycle * 4 + 2;
    for (uint32_t i = 0; i < 2; ++i) {
      EXPECT_TRUE(
          nimble::NimbleCompare<D>::equals(result[i], values[offset + i]))
          << "MultipleSkipMaterializeCycles mismatch at cycle=" << cycle
          << " i=" << i;
    }
  }
}

TYPED_TEST(DeltaEncodingTest, ResetAfterPartialSkipAndMaterialize) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  nimble::Vector<D> values{this->pool_.get()};
  for (int i = 0; i < 10; ++i) {
    values.push_back(static_cast<D>(i * 2 + 1));
  }

  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buffer->template asMutable<void>();
  };
  auto encoding =
      nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
          *this->buffer_,
          values,
          stringBufferFactory,
          nimble::CompressionType::Uncompressed,
          options);

  // Partial skip + materialize.
  encoding->skip(4);
  nimble::Vector<D> partial(this->pool_.get(), 3);
  encoding->materialize(3, partial.data());
  for (uint32_t i = 0; i < 3; ++i) {
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(partial[i], values[4 + i]));
  }

  // Reset and verify full stream reads correctly.
  encoding->reset();
  nimble::Vector<D> full(this->pool_.get(), 10);
  encoding->materialize(10, full.data());
  for (uint32_t i = 0; i < 10; ++i) {
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(full[i], values[i]))
        << "ResetAfterPartialSkipAndMaterialize mismatch at " << i;
  }
}

TYPED_TEST(DeltaEncodingTest, EmptyData) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  nimble::Vector<D> values{this->pool_.get()};

  // Encoding 0 rows should throw IncompatibleEncoding.
  EXPECT_THROW(
      nimble::test::Encoder<nimble::DeltaEncoding<D>>::encode(
          *this->buffer_,
          values,
          nimble::CompressionType::Uncompressed,
          options),
      nimble::NimbleUserError);
}

TYPED_TEST(DeltaEncodingTest, LargeRowCount) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  // 100K rows: mostly monotonic with periodic restatements.
  constexpr uint32_t kRowCount = 100'000;
  nimble::Vector<D> values{this->pool_.get()};
  values.reserve(kRowCount);
  for (uint32_t i = 0; i < kRowCount; ++i) {
    // Every 1000 rows, reset to a small value (restatement).
    if (i % 1000 == 0) {
      values.push_back(static_cast<D>(1));
    } else {
      values.push_back(static_cast<D>((i % 1000) + 1));
    }
  }

  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buffer->template asMutable<void>();
  };
  auto encoding =
      nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
          *this->buffer_,
          values,
          stringBufferFactory,
          nimble::CompressionType::Uncompressed,
          options);
  EXPECT_EQ(encoding->rowCount(), kRowCount);

  nimble::Vector<D> result(this->pool_.get(), kRowCount);
  encoding->materialize(kRowCount, result.data());
  for (uint32_t i = 0; i < kRowCount; ++i) {
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]))
        << "LargeRowCount mismatch at " << i;
  }
}

TYPED_TEST(DeltaEncodingTest, CompressionRoundTrip) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  nimble::Vector<D> values{this->pool_.get()};
  for (int i = 0; i < 50; ++i) {
    values.push_back(static_cast<D>(i * 3 + 1));
  }

  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buffer->template asMutable<void>();
  };
  auto encoding =
      nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
          *this->buffer_,
          values,
          stringBufferFactory,
          nimble::CompressionType::Zstd,
          options);
  EXPECT_EQ(encoding->rowCount(), 50);

  nimble::Vector<D> result(this->pool_.get(), 50);
  encoding->materialize(50, result.data());
  for (uint32_t i = 0; i < 50; ++i) {
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]))
        << "CompressionRoundTrip mismatch at " << i;
  }
}

TYPED_TEST(DeltaEncodingTest, SingleRestatementAtEnd) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  // Monotonically increasing then a single drop at the end.
  nimble::Vector<D> values{this->pool_.get()};
  for (int i = 0; i < 10; ++i) {
    values.push_back(static_cast<D>(i + 1));
  }
  // Drop — triggers a single restatement at the end.
  values.push_back(static_cast<D>(1));

  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buffer->template asMutable<void>();
  };
  auto encoding =
      nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
          *this->buffer_,
          values,
          stringBufferFactory,
          nimble::CompressionType::Uncompressed,
          options);
  uint32_t rowCount = values.size();
  nimble::Vector<D> result(this->pool_.get(), rowCount);
  encoding->materialize(rowCount, result.data());

  for (uint32_t i = 0; i < rowCount; ++i) {
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]))
        << "SingleRestatementAtEnd mismatch at " << i;
  }
}

// Verifies round-trip correctness when values increase from negative to
// positive. The negative-to-positive transition must be treated as a
// restatement (not a delta) to avoid signed overflow.
TYPED_TEST(DeltaEncodingTest, ZeroCrossingNegativeToPositive) {
  using D = typename TypeParam::data_type;
  if constexpr (nimble::isSignedIntegralType<D>()) {
    const nimble::Encoding::Options options{
        .useVarintRowCount = TypeParam::useVarint};

    auto values = this->template toVector<D>({-3, -1, 2, 5});

    std::vector<velox::BufferPtr> newStringBuffers;
    const auto stringBufferFactory = [&](uint32_t totalLength) {
      auto& buffer = newStringBuffers.emplace_back(
          velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
      return buffer->template asMutable<void>();
    };
    auto encoding =
        nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
            *this->buffer_,
            values,
            stringBufferFactory,
            nimble::CompressionType::Uncompressed,
            options);
    nimble::Vector<D> result(this->pool_.get(), values.size());
    encoding->materialize(values.size(), result.data());

    for (uint32_t i = 0; i < values.size(); ++i) {
      EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]))
          << "Mismatch at index " << i << ": expected " << values[i] << " got "
          << result[i];
    }
  } else {
    GTEST_SKIP() << "Zero crossing only applies to signed types";
  }
}

// Verifies round-trip when values cross zero multiple times, mixing
// increasing crossings (restatement) and decreasing crossings (also
// restatement due to decrease).
TYPED_TEST(DeltaEncodingTest, ZeroCrossingMultipleCrossings) {
  using D = typename TypeParam::data_type;
  if constexpr (nimble::isSignedIntegralType<D>()) {
    const nimble::Encoding::Options options{
        .useVarintRowCount = TypeParam::useVarint};

    // -5→3 crosses zero upward (restatement), 3→-2 decreases (restatement),
    // -2→7 crosses zero upward (restatement), 7→-1 decreases (restatement),
    // -1→4 crosses zero upward (restatement).
    auto values = this->template toVector<D>({-5, 3, -2, 7, -1, 4});

    std::vector<velox::BufferPtr> newStringBuffers;
    const auto stringBufferFactory = [&](uint32_t totalLength) {
      auto& buffer = newStringBuffers.emplace_back(
          velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
      return buffer->template asMutable<void>();
    };
    auto encoding =
        nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
            *this->buffer_,
            values,
            stringBufferFactory,
            nimble::CompressionType::Uncompressed,
            options);
    nimble::Vector<D> result(this->pool_.get(), values.size());
    encoding->materialize(values.size(), result.data());

    for (uint32_t i = 0; i < values.size(); ++i) {
      EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]))
          << "Mismatch at index " << i << ": expected " << values[i] << " got "
          << result[i];
    }
  } else {
    GTEST_SKIP() << "Zero crossing only applies to signed types";
  }
}

// Values that pass through zero itself ({-2, -1, 0, 1, 2}) should NOT trigger
// the crossesZero path, because 0 is neither > 0 nor < 0. All transitions
// are plain deltas.
TYPED_TEST(DeltaEncodingTest, ZeroCrossingThroughZeroValue) {
  using D = typename TypeParam::data_type;
  if constexpr (nimble::isSignedIntegralType<D>()) {
    const nimble::Encoding::Options options{
        .useVarintRowCount = TypeParam::useVarint};

    auto values = this->template toVector<D>({-2, -1, 0, 1, 2});

    std::vector<velox::BufferPtr> newStringBuffers;
    const auto stringBufferFactory = [&](uint32_t totalLength) {
      auto& buffer = newStringBuffers.emplace_back(
          velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
      return buffer->template asMutable<void>();
    };
    auto encoding =
        nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
            *this->buffer_,
            values,
            stringBufferFactory,
            nimble::CompressionType::Uncompressed,
            options);
    nimble::Vector<D> result(this->pool_.get(), values.size());
    encoding->materialize(values.size(), result.data());

    for (uint32_t i = 0; i < values.size(); ++i) {
      EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]))
          << "Mismatch at index " << i << ": expected " << values[i] << " got "
          << result[i];
    }
  } else {
    GTEST_SKIP() << "Zero crossing only applies to signed types";
  }
}

// Verifies that skip correctly handles zero-crossing restatements.
TYPED_TEST(DeltaEncodingTest, ZeroCrossingWithSkipAndMaterialize) {
  using D = typename TypeParam::data_type;
  if constexpr (nimble::isSignedIntegralType<D>()) {
    const nimble::Encoding::Options options{
        .useVarintRowCount = TypeParam::useVarint};

    // Skip past the zero crossing at index 2 (-1→2), then read the rest.
    auto values = this->template toVector<D>({-5, -1, 2, 4, 6, 8});

    std::vector<velox::BufferPtr> newStringBuffers;
    const auto stringBufferFactory = [&](uint32_t totalLength) {
      auto& buffer = newStringBuffers.emplace_back(
          velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
      return buffer->template asMutable<void>();
    };
    auto encoding =
        nimble::test::Encoder<nimble::DeltaEncoding<D>>::createEncoding(
            *this->buffer_,
            values,
            stringBufferFactory,
            nimble::CompressionType::Uncompressed,
            options);

    // Skip past the zero crossing point.
    encoding->skip(3);
    nimble::Vector<D> result(this->pool_.get(), 3);
    encoding->materialize(3, result.data());

    for (uint32_t i = 0; i < 3; ++i) {
      EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[3 + i]))
          << "Mismatch at index " << i << ": expected " << values[3 + i]
          << " got " << result[i];
    }

    // Reset and materialize one-by-one across the crossing.
    encoding->reset();
    for (uint32_t i = 0; i < values.size(); ++i) {
      D val;
      encoding->materialize(1, &val);
      EXPECT_TRUE(nimble::NimbleCompare<D>::equals(val, values[i]))
          << "One-at-a-time mismatch at index " << i;
    }
  } else {
    GTEST_SKIP() << "Zero crossing only applies to signed types";
  }
}

// Directly tests internal::computeDeltas to verify that zero-crossing
// transitions produce the correct decomposition into deltas, restatements,
// and isRestatements vectors.
TEST(ComputeDeltasTest, ZeroCrossingDecomposition) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();

  // Input: {-3, -1, 2, 5}
  // Transition analysis:
  //   -1 >= -3 (delta, no crossing)        → delta = 2
  //   2 >= -1  (increasing, crosses zero)  → restatement = 2
  //   5 >= 2   (delta, no crossing)        → delta = 3
  const std::array<int32_t, 4> values = {-3, -1, 2, 5};

  nimble::Vector<int32_t> deltas(pool.get());
  nimble::Vector<int32_t> restatements(pool.get());
  nimble::Vector<bool> isRestatements(pool.get());

  nimble::internal::computeDeltas<int32_t>(
      std::span<const int32_t>(values),
      &deltas,
      &restatements,
      &isRestatements);

  ASSERT_EQ(isRestatements.size(), 4);
  EXPECT_TRUE(isRestatements[0]); // first element always restatement
  EXPECT_FALSE(isRestatements[1]); // -1 >= -3, no zero crossing
  EXPECT_TRUE(isRestatements[2]); // 2 >= -1, crosses zero → restatement
  EXPECT_FALSE(isRestatements[3]); // 5 >= 2, no zero crossing

  const nimble::Vector<int32_t> expectedDeltas = [&] {
    nimble::Vector<int32_t> v(pool.get());
    v.push_back(2); // -1 - (-3)
    v.push_back(3); // 5 - 2
    return v;
  }();
  ASSERT_EQ(deltas.size(), expectedDeltas.size());
  for (uint32_t i = 0; i < deltas.size(); ++i) {
    EXPECT_EQ(deltas[i], expectedDeltas[i]) << "delta mismatch at " << i;
  }

  const nimble::Vector<int32_t> expectedRestatements = [&] {
    nimble::Vector<int32_t> v(pool.get());
    v.push_back(-3); // first value
    v.push_back(2); // zero-crossing value
    return v;
  }();
  ASSERT_EQ(restatements.size(), expectedRestatements.size());
  for (uint32_t i = 0; i < restatements.size(); ++i) {
    EXPECT_EQ(restatements[i], expectedRestatements[i])
        << "restatement mismatch at " << i;
  }
}

// Verifies that passing through zero (touching the value 0 exactly) does NOT
// trigger zero-crossing restatements. The crossesZero check requires
// values[i] > 0 && values[i-1] < 0, so transitions involving 0 are plain
// deltas.
TEST(ComputeDeltasTest, ThroughZeroNoZeroCrossing) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();

  // -1 → 0: isDelta=true, crossesZero=false (0 is not > 0) → delta
  // 0 → 1:  isDelta=true, crossesZero=false (0 is not < 0) → delta
  const std::array<int32_t, 3> values = {-1, 0, 1};

  nimble::Vector<int32_t> deltas(pool.get());
  nimble::Vector<int32_t> restatements(pool.get());
  nimble::Vector<bool> isRestatements(pool.get());

  nimble::internal::computeDeltas<int32_t>(
      std::span<const int32_t>(values),
      &deltas,
      &restatements,
      &isRestatements);

  ASSERT_EQ(isRestatements.size(), 3);
  EXPECT_TRUE(isRestatements[0]); // first element always restatement
  EXPECT_FALSE(isRestatements[1]); // -1 → 0, no crossing
  EXPECT_FALSE(isRestatements[2]); // 0 → 1, no crossing

  // All transitions are deltas (none cross zero).
  ASSERT_EQ(deltas.size(), 2);
  EXPECT_EQ(deltas[0], 1); // 0 - (-1)
  EXPECT_EQ(deltas[1], 1); // 1 - 0

  ASSERT_EQ(restatements.size(), 1);
  EXPECT_EQ(restatements[0], -1); // only the first value
}

// Verifies that unsigned types never trigger zero-crossing logic.
TEST(ComputeDeltasTest, UnsignedNoZeroCrossing) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();

  // All increasing — every transition is a delta for unsigned types.
  const std::array<uint32_t, 5> values = {1, 2, 3, 4, 5};

  nimble::Vector<uint32_t> deltas(pool.get());
  nimble::Vector<uint32_t> restatements(pool.get());
  nimble::Vector<bool> isRestatements(pool.get());

  nimble::internal::computeDeltas<uint32_t>(
      std::span<const uint32_t>(values),
      &deltas,
      &restatements,
      &isRestatements);

  ASSERT_EQ(isRestatements.size(), 5);
  EXPECT_TRUE(isRestatements[0]);
  for (uint32_t i = 1; i < 5; ++i) {
    EXPECT_FALSE(isRestatements[i]) << "Unexpected restatement at " << i;
  }

  ASSERT_EQ(deltas.size(), 4);
  for (uint32_t i = 0; i < 4; ++i) {
    EXPECT_EQ(deltas[i], 1u) << "delta mismatch at " << i;
  }

  ASSERT_EQ(restatements.size(), 1);
  EXPECT_EQ(restatements[0], 1u);
}
