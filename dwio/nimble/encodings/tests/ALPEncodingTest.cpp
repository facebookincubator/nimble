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
#include "dwio/nimble/encodings/ALPEncoding.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/tests/NimbleCompare.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"

#include <limits>
#include <vector>

using namespace facebook;

template <typename DataType, bool UseVarint>
struct TestConfig {
  using data_type = DataType;
  static constexpr bool useVarint = UseVarint;
};

#define TC(T) TestConfig<T, false>, TestConfig<T, true>

template <typename Config>
class ALPEncodingTest : public ::testing::Test {
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

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
};

using TestTypes = ::testing::Types<TC(float), TC(double)>;

TYPED_TEST_CASE(ALPEncodingTest, TestTypes);

TYPED_TEST(ALPEncodingTest, roundTrip) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  auto values = this->template toVector<D>({1, 2, 3, 4, 5});

  std::vector<velox::BufferPtr> stringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buf = stringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buf->template asMutable<void>();
  };

  auto encoding = nimble::test::Encoder<nimble::ALPEncoding<D>>::createEncoding(
      *this->buffer_,
      values,
      stringBufferFactory,
      nimble::CompressionType::Uncompressed,
      options);

  EXPECT_EQ(encoding->encodingType(), nimble::EncodingType::ALP);
  EXPECT_EQ(encoding->dataType(), nimble::TypeTraits<D>::dataType);
  EXPECT_EQ(encoding->rowCount(), values.size());

  nimble::Vector<D> result(this->pool_.get(), values.size());
  encoding->materialize(values.size(), result.data());

  for (uint32_t i = 0; i < values.size(); ++i) {
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]));
  }
}

TYPED_TEST(ALPEncodingTest, skipAndMaterialize) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  auto values = this->template toVector<D>({10, 20, 30, 40, 50, 60});

  std::vector<velox::BufferPtr> stringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buf = stringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buf->template asMutable<void>();
  };

  auto encoding = nimble::test::Encoder<nimble::ALPEncoding<D>>::createEncoding(
      *this->buffer_,
      values,
      stringBufferFactory,
      nimble::CompressionType::Uncompressed,
      options);

  encoding->skip(2);

  nimble::Vector<D> result(this->pool_.get(), 3);
  encoding->materialize(3, result.data());

  EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[0], D{30}));
  EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[1], D{40}));
  EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[2], D{50}));
}

TYPED_TEST(ALPEncodingTest, resetAndRematerialize) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  auto values = this->template toVector<D>({7, 8, 9});

  std::vector<velox::BufferPtr> stringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buf = stringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buf->template asMutable<void>();
  };

  auto encoding = nimble::test::Encoder<nimble::ALPEncoding<D>>::createEncoding(
      *this->buffer_,
      values,
      stringBufferFactory,
      nimble::CompressionType::Uncompressed,
      options);

  nimble::Vector<D> first(this->pool_.get(), values.size());
  encoding->materialize(values.size(), first.data());

  encoding->reset();

  nimble::Vector<D> second(this->pool_.get(), values.size());
  encoding->materialize(values.size(), second.data());

  for (uint32_t i = 0; i < values.size(); ++i) {
    EXPECT_TRUE(nimble::NimbleCompare<D>::equals(first[i], second[i]));
  }
}

TYPED_TEST(ALPEncodingTest, emptyDataRejected) {
  using D = typename TypeParam::data_type;
  const nimble::Encoding::Options options{
      .useVarintRowCount = TypeParam::useVarint};

  nimble::Vector<D> empty{this->pool_.get()};

  std::vector<velox::BufferPtr> stringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buf = stringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, this->pool_.get()));
    return buf->template asMutable<void>();
  };

  EXPECT_THROW(
      nimble::test::Encoder<nimble::ALPEncoding<D>>::createEncoding(
          *this->buffer_,
          empty,
          stringBufferFactory,
          nimble::CompressionType::Uncompressed,
          options),
      nimble::NimbleUserError);
}
