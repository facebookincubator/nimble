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
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingSelection.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/memory/Memory.h"

#include <limits>

using namespace ::facebook;

namespace facebook::nimble::test {

namespace {

class TestCompressPolicy : public CompressionPolicy {
 public:
  CompressionInformation compression() const override {
    return {.compressionType = CompressionType::Uncompressed};
  }

  bool shouldAccept(
      CompressionType /* compressionType */,
      uint64_t /* uncompressedSize */,
      uint64_t /* compressedSize */) const override {
    return true;
  }
};

template <typename T>
class TestTrivialSelectionPolicy : public EncodingSelectionPolicy<T> {
  using physicalType = typename TypeTraits<T>::physicalType;

 public:
  EncodingSelectionResult select(
      std::span<const physicalType> /* values */,
      const Statistics<physicalType>& /* statistics */) override {
    return {
        .encodingType = EncodingType::Trivial,
        .compressionPolicyFactory = []() {
          return std::make_unique<TestCompressPolicy>();
        }};
  }

  EncodingSelectionResult selectNullable(
      std::span<const physicalType> /* values */,
      std::span<const bool> /* nulls */,
      const Statistics<physicalType>& /* statistics */) override {
    return {.encodingType = EncodingType::Nullable};
  }

  std::unique_ptr<EncodingSelectionPolicyBase> createImpl(
      EncodingType /* encodingType */,
      NestedEncodingIdentifier /* identifier */,
      DataType type) override {
    UNIQUE_PTR_FACTORY(type, TestTrivialSelectionPolicy);
  }
};

} // namespace

template <typename T>
class DeltaEncodingTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::initialize({});
  }

  void SetUp() override {
    pool_ = velox::memory::memoryManager()->addLeafPool();
    buffer_ = std::make_unique<Buffer>(*pool_);
  }

  Vector<T> toVector(std::initializer_list<T> l) {
    Vector<T> v{pool_.get()};
    v.insert(v.end(), l.begin(), l.end());
    return v;
  }

  // Encode values using DeltaEncoding, then decode and return the encoding.
  std::unique_ptr<Encoding> encodeAndDecode(
      const Vector<T>& values,
      const Encoding::Options& options = {}) {
    using physicalType = typename TypeTraits<T>::physicalType;
    auto physicalValues = std::span<const physicalType>(
        reinterpret_cast<const physicalType*>(values.data()), values.size());
    EncodingSelection<physicalType> selection{
        {.encodingType = EncodingType::Delta,
         .compressionPolicyFactory =
             []() { return std::make_unique<TestCompressPolicy>(); }},
        Statistics<physicalType>::create(physicalValues),
        std::make_unique<TestTrivialSelectionPolicy<T>>()};

    auto encoded =
        DeltaEncoding<T>::encode(selection, physicalValues, *buffer_, options);

    std::vector<velox::BufferPtr> stringBuffers;
    auto stringBufferFactory = [&](uint32_t totalLength) {
      auto& buf = stringBuffers.emplace_back(
          velox::AlignedBuffer::allocate<char>(totalLength, pool_.get()));
      return buf->template asMutable<void>();
    };
    return std::make_unique<DeltaEncoding<T>>(
        *pool_, encoded, stringBufferFactory, options);
  }

  // Helper: encode, decode, and verify roundtrip matches original values.
  void verifyRoundtrip(
      const Vector<T>& values,
      const Encoding::Options& options = {}) {
    auto encoding = encodeAndDecode(values, options);

    EXPECT_EQ(encoding->encodingType(), EncodingType::Delta);
    EXPECT_EQ(encoding->dataType(), TypeTraits<T>::dataType);
    EXPECT_EQ(encoding->rowCount(), values.size());

    Vector<T> result(pool_.get(), values.size());
    encoding->materialize(values.size(), result.data());

    for (uint32_t i = 0; i < values.size(); ++i) {
      EXPECT_EQ(result[i], values[i]) << "Mismatch at index " << i;
    }
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<Buffer> buffer_;
};

using IntegerTypes = ::testing::Types<
    int8_t,
    uint8_t,
    int16_t,
    uint16_t,
    int32_t,
    uint32_t,
    int64_t,
    uint64_t>;

NIMBLE_TYPED_TEST_SUITE(DeltaEncodingTest, IntegerTypes);

// The example from the DeltaEncoding.h header comment.
TYPED_TEST(DeltaEncodingTest, HeaderExample) {
  // 1 2 4 1 2 3 4 1 2 4 8 8
  auto values = this->toVector({1, 2, 4, 1, 2, 3, 4, 1, 2, 4, 8, 8});
  this->verifyRoundtrip(values);
}

// Constant sequence (all deltas are 0).
TYPED_TEST(DeltaEncodingTest, ConstantValues) {
  auto values = this->toVector({5, 5, 5, 5, 5});
  this->verifyRoundtrip(values);
}

// Single value.
TYPED_TEST(DeltaEncodingTest, SingleValue) {
  auto values = this->toVector({42});
  this->verifyRoundtrip(values);
}

// Large deltas within type range.
TYPED_TEST(DeltaEncodingTest, LargeDeltas) {
  using T = TypeParam;
  T maxVal = std::numeric_limits<T>::max();
  // Use values spaced apart but within range.
  T mid = maxVal / 2;
  auto values = this->toVector({0, mid, maxVal});
  this->verifyRoundtrip(values);
}

// Skip then materialize.
TYPED_TEST(DeltaEncodingTest, SkipThenMaterialize) {
  auto values = this->toVector({1, 2, 4, 1, 2, 3, 4, 1, 2, 4, 8, 8});
  auto encoding = this->encodeAndDecode(values);

  // Skip the first 4 values.
  encoding->skip(4);

  // Materialize the remaining 8.
  Vector<TypeParam> result(this->pool_.get(), 8);
  encoding->materialize(8, result.data());

  for (uint32_t i = 0; i < 8; ++i) {
    EXPECT_EQ(result[i], values[i + 4]) << "Mismatch at index " << i;
  }
}

// Materialize in chunks.
TYPED_TEST(DeltaEncodingTest, MaterializeInChunks) {
  auto values = this->toVector({1, 3, 5, 2, 4, 6, 8, 10, 7, 9});
  auto encoding = this->encodeAndDecode(values);

  uint32_t chunks[] = {3, 4, 3};
  uint32_t offset = 0;
  for (auto chunkSize : chunks) {
    Vector<TypeParam> result(this->pool_.get(), chunkSize);
    encoding->materialize(chunkSize, result.data());
    for (uint32_t i = 0; i < chunkSize; ++i) {
      EXPECT_EQ(result[i], values[offset + i]);
    }
    offset += chunkSize;
  }
}

// Reset and re-materialize.
TYPED_TEST(DeltaEncodingTest, ResetAndRematerialize) {
  auto values = this->toVector({5, 10, 15, 8, 12});
  auto encoding = this->encodeAndDecode(values);

  Vector<TypeParam> result1(this->pool_.get(), 5);
  encoding->materialize(5, result1.data());
  encoding->reset();
  Vector<TypeParam> result2(this->pool_.get(), 5);
  encoding->materialize(5, result2.data());

  for (uint32_t i = 0; i < 5; ++i) {
    EXPECT_EQ(result1[i], values[i]);
    EXPECT_EQ(result2[i], values[i]);
  }
}

// Verify varint row count option.
TYPED_TEST(DeltaEncodingTest, VarintRowCount) {
  auto values = this->toVector({1, 2, 4, 1, 2, 3, 4, 1, 2, 4, 8, 8});
  Encoding::Options options{.useVarintRowCount = true};
  this->verifyRoundtrip(values, options);
}

// Empty data should throw.
TYPED_TEST(DeltaEncodingTest, EmptyDataThrows) {
  using physicalType = typename TypeTraits<TypeParam>::physicalType;
  auto physicalValues = std::span<const physicalType>();
  EncodingSelection<physicalType> selection{
      {.encodingType = EncodingType::Delta,
       .compressionPolicyFactory =
           []() { return std::make_unique<TestCompressPolicy>(); }},
      Statistics<physicalType>::create(physicalValues),
      std::make_unique<TestTrivialSelectionPolicy<TypeParam>>()};

  NIMBLE_ASSERT_THROW(
      DeltaEncoding<TypeParam>::encode(
          selection, physicalValues, *this->buffer_),
      "DeltaEncoding can't be used with 0 rows");
}

// Large dataset with monotonic increase.
TYPED_TEST(DeltaEncodingTest, LargeMonotonicDataset) {
  Vector<TypeParam> values(this->pool_.get());
  for (uint32_t i = 0; i < 1000; ++i) {
    values.push_back(static_cast<TypeParam>(i));
  }
  this->verifyRoundtrip(values);
}

// Skip with restatement as the last value in the skipped range.
TYPED_TEST(DeltaEncodingTest, SkipEndingOnRestatement) {
  // Values: 10 20 30 5 15 25
  // isRestatement: T F F T F F
  // Skip 4 values (ends on the restatement at index 3 = value 5).
  auto values = this->toVector({10, 20, 30, 5, 15, 25});
  auto encoding = this->encodeAndDecode(values);
  encoding->skip(4);

  Vector<TypeParam> result(this->pool_.get(), 2);
  encoding->materialize(2, result.data());
  EXPECT_EQ(result[0], 15);
  EXPECT_EQ(result[1], 25);
}

} // namespace facebook::nimble::test
