// Copyright 2004-present Facebook. All Rights Reserved.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include "dwio/alpha/common/Buffer.h"
#include "dwio/alpha/common/EncodingType.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/encodings/RleEncoding.h"
#include "dwio/alpha/encodings/tests/TestUtils.h"

#include <limits>
#include <vector>

using namespace facebook;

template <typename C>
class RleEncodingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = facebook::velox::memory::addDefaultLeafMemoryPool();
    buffer_ = std::make_unique<alpha::Buffer>(*pool_);
  }

  template <typename T>
  std::vector<alpha::Vector<T>> prepareValues() {
    FAIL() << "unspecialized prepapreValues() should not be called";
    return {};
  }

  template <typename T>
  alpha::Vector<T> toVector(std::initializer_list<T> l) {
    alpha::Vector<T> v{pool_.get()};
    v.insert(v.end(), l.begin(), l.end());
    return v;
  }

  template <typename T>
  using ET = alpha::EncodingPhysicalType<T>;

  double dNaN0 = std::numeric_limits<double>::quiet_NaN();
  double dNaN1 = std::numeric_limits<double>::signaling_NaN();
  double dNaN2 = ET<double>::asEncodingLogicalType(
      (ET<double>::asEncodingPhysicalType(dNaN0) | 0x3));
  double dNaN3 = ET<double>::asEncodingLogicalType(
      (ET<double>::asEncodingPhysicalType(dNaN0) | 0x5));

  template <>
  std::vector<alpha::Vector<double>> prepareValues() {
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
  std::vector<alpha::Vector<float>> prepareValues() {
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
  std::vector<alpha::Vector<int32_t>> prepareValues() {
    return {toVector({2, 3, 3}), toVector({1, 2, 2, 3, 3, 3, 4, 4, 4, 4})};
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<alpha::Buffer> buffer_;
};

#define NUM_TYPES int32_t, double, float

using TestTypes = ::testing::Types<NUM_TYPES>;

TYPED_TEST_CASE(RleEncodingTest, TestTypes);

TYPED_TEST(RleEncodingTest, SerializeThenDeserialize) {
  using D = TypeParam;

  auto valueGroups = this->template prepareValues<D>();
  for (const auto& values : valueGroups) {
    auto encoding = alpha::test::Encoder<alpha::RLEEncoding<D>>::createEncoding(
        *this->buffer_, values);
    uint32_t rowCount = values.size();
    alpha::Vector<D> result(this->pool_.get(), rowCount);
    encoding->materialize(rowCount, result.data());

    EXPECT_EQ(encoding->encodingType(), alpha::EncodingType::RLE);
    EXPECT_EQ(encoding->dataType(), alpha::TypeTraits<D>::dataType);
    EXPECT_EQ(encoding->rowCount(), rowCount);
    for (uint32_t i = 0; i < rowCount; ++i) {
      EXPECT_TRUE(alpha::AlphaCompare<D>::equals(result[i], values[i]));
    }
  }
}
