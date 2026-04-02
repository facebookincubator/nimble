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
#include "dwio/nimble/encodings/MainlyConstantEncoding.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingType.h"
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

// Forward declaration
template <typename Config>
class MainlyConstantEncodingTest;

// Helper to prepare values - must be at namespace scope
template <typename T, typename TestClass>
struct MainlyConstantValuesPreparer {
  static std::vector<nimble::Vector<T>> prepareValues(TestClass* test) {
    FAIL() << "unspecialized prepareValues() should not be called";
    return {};
  }
};

template <typename TestClass>
struct MainlyConstantValuesPreparer<double, TestClass> {
  static std::vector<nimble::Vector<double>> prepareValues(TestClass* test) {
    return {
        test->toVector({0.0}),
        test->toVector({0.0, 0.00, 0.12}),
        test->toVector({-2.1, -2.1, -2.3, -2.1, -2.1}),
        test->toVector({test->dNaN0, test->dNaN0, test->dNaN1, test->dNaN2, test->dNaN0})};
  }
};

template <typename TestClass>
struct MainlyConstantValuesPreparer<float, TestClass> {
  static std::vector<nimble::Vector<float>> prepareValues(TestClass* test) {
    return {
        test->toVector({0.0f}),
        test->toVector({0.0f, 0.00f, 0.12f}),
        test->toVector({-2.1f, -2.1f, -2.3f, -2.1f, -2.1f}),
        test->toVector({test->fNaN0, test->fNaN0, test->fNaN1, test->fNaN2, test->fNaN2})};
  }
};

template <typename TestClass>
struct MainlyConstantValuesPreparer<int32_t, TestClass> {
  static std::vector<nimble::Vector<int32_t>> prepareValues(TestClass* test) {
    return {test->toVector({3, 3, 3, 1, 3})};
  }
};

template <typename Config>
class MainlyConstantEncodingTest : public ::testing::Test {
 protected:
  // Make helper templates friends so they can access protected members
  template <typename T, typename TestClass>
  friend struct MainlyConstantValuesPreparer;

  void SetUp() override {
    pool_ = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  template <typename T>
  std::vector<nimble::Vector<T>> prepareValues() {
    return MainlyConstantValuesPreparer<T, MainlyConstantEncodingTest<Config>>::prepareValues(this);
  }

  template <typename T>
  nimble::Vector<T> toVector(std::initializer_list<T> l) {
    nimble::Vector<T> v{pool_.get()};
    v.insert(v.end(), l.begin(), l.end());
    return v;
  }

  template <typename T>
  using ET = nimble::EncodingPhysicalType<T>;

  double dNaN0 = std::numeric_limits<double>::quiet_NaN();
  double dNaN1 = std::numeric_limits<double>::signaling_NaN();
  double dNaN2 = ET<double>::asEncodingLogicalType(
      (ET<double>::asEncodingPhysicalType(dNaN0) | 0x3));

  float fNaN0 = std::numeric_limits<float>::quiet_NaN();
  float fNaN1 = std::numeric_limits<float>::signaling_NaN();
  float fNaN2 = ET<float>::asEncodingLogicalType(
      (ET<float>::asEncodingPhysicalType(fNaN0) | 0x3));

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
};

#define NUM_TYPES TC(int32_t), TC(double), TC(float)

using TestTypes = ::testing::Types<NUM_TYPES>;

TYPED_TEST_CASE(MainlyConstantEncodingTest, TestTypes);

TYPED_TEST(MainlyConstantEncodingTest, SerializeThenDeserialize) {
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
    auto encoding = nimble::test::Encoder<nimble::MainlyConstantEncoding<D>>::
        createEncoding(
            *this->buffer_,
            values,
            stringBufferFactory,
            nimble::CompressionType::Uncompressed,
            options);

    uint32_t rowCount = values.size();
    nimble::Vector<D> result(this->pool_.get(), rowCount);
    encoding->materialize(rowCount, result.data());

    EXPECT_EQ(encoding->encodingType(), nimble::EncodingType::MainlyConstant);
    EXPECT_EQ(encoding->dataType(), nimble::TypeTraits<D>::dataType);
    EXPECT_EQ(encoding->rowCount(), rowCount);
    for (uint32_t i = 0; i < rowCount; ++i) {
      EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]));
    }
  }
}
