// Copyright 2004-present Facebook. All Rights Reserved.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include "dwio/alpha/common/AlphaCompare.h"
#include "dwio/alpha/common/Buffer.h"
#include "dwio/alpha/common/EncodingType.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/encodings/MainlyConstantEncoding.h"
#include "dwio/alpha/encodings/tests/TestUtils.h"

#include <limits>
#include <vector>

using namespace facebook;

template <typename C>
class MainlyConstantEncodingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
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

  template <>
  std::vector<alpha::Vector<double>> prepareValues() {
    return {
        toVector({0.0}),
        toVector({0.0, 0.00, 0.12}),
        toVector({-2.1, -2.1, -2.3, -2.1, -2.1}),
        toVector({dNaN0, dNaN0, dNaN1, dNaN2, dNaN0})};
  }

  float fNaN0 = std::numeric_limits<float>::quiet_NaN();
  float fNaN1 = std::numeric_limits<float>::signaling_NaN();
  float fNaN2 = ET<float>::asEncodingLogicalType(
      (ET<float>::asEncodingPhysicalType(fNaN0) | 0x3));

  template <>
  std::vector<alpha::Vector<float>> prepareValues() {
    return {
        toVector({0.0f}),
        toVector({0.0f, 0.00f, 0.12f}),
        toVector({-2.1f, -2.1f, -2.3f, -2.1f, -2.1f}),
        toVector({fNaN0, fNaN0, fNaN1, fNaN2, fNaN2})};
  }

  template <>
  std::vector<alpha::Vector<int32_t>> prepareValues() {
    return {toVector({3, 3, 3, 1, 3})};
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<alpha::Buffer> buffer_;
};

#define NUM_TYPES int32_t, double, float

using TestTypes = ::testing::Types<NUM_TYPES>;

TYPED_TEST_CASE(MainlyConstantEncodingTest, TestTypes);

TYPED_TEST(MainlyConstantEncodingTest, SerializeThenDeserialize) {
  using D = TypeParam;

  auto valueGroups = this->template prepareValues<D>();
  for (const auto& values : valueGroups) {
    auto encoding =
        alpha::test::Encoder<alpha::MainlyConstantEncoding<D>>::createEncoding(
            *this->buffer_, values);

    uint32_t rowCount = values.size();
    alpha::Vector<D> result(this->pool_.get(), rowCount);
    encoding->materialize(rowCount, result.data());

    EXPECT_EQ(encoding->encodingType(), alpha::EncodingType::MainlyConstant);
    EXPECT_EQ(encoding->dataType(), alpha::TypeTraits<D>::dataType);
    EXPECT_EQ(encoding->rowCount(), rowCount);
    for (uint32_t i = 0; i < rowCount; ++i) {
      EXPECT_TRUE(alpha::AlphaCompare<D>::equals(result[i], values[i]));
    }
  }
}
