/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/RleEncoding.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"

#include <limits>
#include <vector>

using namespace facebook;

template <typename C>
class RleEncodingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  template <typename T>
  std::vector<nimble::Vector<T>> prepareValues() {
    FAIL() << "unspecialized prepapreValues() should not be called";
    return {};
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
  double dNaN3 = ET<double>::asEncodingLogicalType(
      (ET<double>::asEncodingPhysicalType(dNaN0) | 0x5));

  template <>
  std::vector<nimble::Vector<double>> prepareValues() {
    return {
        toVector({0.0, -0.0}),
        toVector({-0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0}),
        toVector({-0.0, -0.0, -0.0, +0.0, -0.0, +0.0, -0.0, -0.0, -0.0, -0.0}),
        toVector({-2.1, -0.0, -0.0, 3.54, 9.87, -0.0, -0.0, -0.0, -0.0, 10.6}),
        toVector({0.00, 1.11, 2.22, 3.33, 4.44, 5.55, 6.66, 7.77, 8.88, 9.99}),
        toVector(
            {dNaN0, dNaN0, dNaN0, dNaN1, dNaN1, dNaN2, dNaN3, dNaN3, dNaN0})};
  }

  float fNaN0 = std::numeric_limits<float>::quiet_NaN();
  float fNaN1 = std::numeric_limits<float>::signaling_NaN();
  float fNaN2 = ET<float>::asEncodingLogicalType(
      (ET<float>::asEncodingPhysicalType(fNaN0) | 0x3));
  float fNaN3 = ET<float>::asEncodingLogicalType(
      (ET<float>::asEncodingPhysicalType(fNaN0) | 0x5));

  template <>
  std::vector<nimble::Vector<float>> prepareValues() {
    return {
        toVector({0.0f, -0.0f}),
        toVector(
            {-0.0f,
             -0.0f,
             -0.0f,
             -0.0f,
             -0.0f,
             -0.0f,
             -0.0f,
             -0.0f,
             -0.0f,
             -0.0f}),
        toVector(
            {-0.0f,
             -0.0f,
             -0.0f,
             +0.0f,
             -0.0f,
             +0.0f,
             -0.0f,
             -0.0f,
             -0.0f,
             -0.0f}),
        toVector(
            {-2.1f,
             -0.0f,
             -0.0f,
             3.54f,
             9.87f,
             -0.0f,
             -0.0f,
             -0.0f,
             -0.0f,
             10.6f}),
        toVector(
            {0.00f,
             1.11f,
             2.22f,
             3.33f,
             4.44f,
             5.55f,
             6.66f,
             7.77f,
             8.88f,
             9.99f}),
        toVector(
            {fNaN0, fNaN0, fNaN0, fNaN1, fNaN1, fNaN2, fNaN3, fNaN3, fNaN0})};
  }

  template <>
  std::vector<nimble::Vector<int32_t>> prepareValues() {
    return {toVector({2, 3, 3}), toVector({1, 2, 2, 3, 3, 3, 4, 4, 4, 4})};
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
};

#define NUM_TYPES int32_t, double, float

using TestTypes = ::testing::Types<NUM_TYPES>;

TYPED_TEST_CASE(RleEncodingTest, TestTypes);

TYPED_TEST(RleEncodingTest, SerializeThenDeserialize) {
  using D = TypeParam;

  auto valueGroups = this->template prepareValues<D>();
  for (const auto& values : valueGroups) {
    auto encoding =
        nimble::test::Encoder<nimble::RLEEncoding<D>>::createEncoding(
            *this->buffer_, values);
    uint32_t rowCount = values.size();
    nimble::Vector<D> result(this->pool_.get(), rowCount);
    encoding->materialize(rowCount, result.data());

    EXPECT_EQ(encoding->encodingType(), nimble::EncodingType::RLE);
    EXPECT_EQ(encoding->dataType(), nimble::TypeTraits<D>::dataType);
    EXPECT_EQ(encoding->rowCount(), rowCount);
    for (uint32_t i = 0; i < rowCount; ++i) {
      EXPECT_TRUE(nimble::NimbleCompare<D>::equals(result[i], values[i]));
    }
  }
}
