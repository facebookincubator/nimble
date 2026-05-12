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

#include <cmath>
#include <random>

#include <gtest/gtest.h>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingSelection.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "velox/common/memory/Memory.h"

using namespace facebook;

namespace {

// Test helper: creates an ALP encoding selection policy.
template <typename T>
class ALPTestPolicy : public nimble::EncodingSelectionPolicy<T> {
  using physicalType = typename nimble::TypeTraits<T>::physicalType;

 public:
  nimble::EncodingSelectionResult select(
      std::span<const physicalType>,
      const nimble::Statistics<physicalType>&) override {
    return {.encodingType = nimble::EncodingType::ALP};
  }

  nimble::EncodingSelectionResult selectNullable(
      std::span<const physicalType>,
      std::span<const bool>,
      const nimble::Statistics<physicalType>&) override {
    return {.encodingType = nimble::EncodingType::Nullable};
  }

  std::unique_ptr<nimble::EncodingSelectionPolicyBase> createImpl(
      nimble::EncodingType,
      nimble::NestedEncodingIdentifier,
      nimble::DataType dataType) override {
    // Create a trivial policy for nested streams.
    return nimble::ManualEncodingSelectionPolicyFactory{
        nimble::ManualEncodingSelectionPolicyFactory::defaultReadFactors(),
        std::nullopt}
        .createPolicy(dataType);
  }
};

} // namespace

template <typename T>
class ALPEncodingTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    rootPool_ = velox::memory::memoryManager()->addRootPool("ALPEncodingTest");
    pool_ = rootPool_->addLeafChild("ALPEncodingTestLeaf");
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  /// Encodes values with ALP, decodes, and verifies bit-exact roundtrip.
  void testRoundtrip(const nimble::Vector<T>& values) {
    using physicalType = typename nimble::TypeTraits<T>::physicalType;

    auto physicalValues = std::span<const physicalType>(
        reinterpret_cast<const physicalType*>(values.data()), values.size());

    auto statistics = nimble::Statistics<physicalType>::create(physicalValues);
    nimble::EncodingSelection<physicalType> selection{
        {.encodingType = nimble::EncodingType::ALP},
        std::move(statistics),
        std::make_unique<ALPTestPolicy<T>>()};

    auto encoded =
        nimble::ALPEncoding<T>::encode(selection, physicalValues, *buffer_);

    ASSERT_EQ(
        static_cast<nimble::EncodingType>(encoded[0]),
        nimble::EncodingType::ALP);

    const nimble::EncodingFactory factory{{}};
    auto encoding = factory.create(*pool_, encoded, nullptr);
    ASSERT_EQ(encoding->rowCount(), values.size());

    nimble::Vector<T> result{pool_.get()};
    result.resize(values.size());
    encoding->materialize(values.size(), result.data());

    for (uint32_t i = 0; i < values.size(); ++i) {
      if (std::isnan(values[i])) {
        EXPECT_TRUE(std::isnan(result[i])) << "NaN mismatch at " << i;
      } else {
        EXPECT_EQ(
            *reinterpret_cast<const physicalType*>(&values[i]),
            *reinterpret_cast<const physicalType*>(&result[i]))
            << "Bit mismatch at " << i << " (" << values[i] << " vs "
            << result[i] << ")";
      }
    }
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
};

using FloatTypes = ::testing::Types<float, double>;
TYPED_TEST_SUITE(ALPEncodingTest, FloatTypes);

TYPED_TEST(ALPEncodingTest, LimitedPrecisionIntegers) {
  nimble::Vector<TypeParam> values{this->pool_.get()};
  for (int i = 0; i < 100; ++i) {
    values.push_back(static_cast<TypeParam>(i));
  }
  this->testRoundtrip(values);
}

TYPED_TEST(ALPEncodingTest, TwoDecimalPlaces) {
  nimble::Vector<TypeParam> values{this->pool_.get()};
  for (int i = 0; i < 100; ++i) {
    values.push_back(static_cast<TypeParam>(i) / static_cast<TypeParam>(100));
  }
  this->testRoundtrip(values);
}

TYPED_TEST(ALPEncodingTest, Prices) {
  // Typical price values with 2 decimal places.
  nimble::Vector<TypeParam> values{this->pool_.get()};
  values.push_back(static_cast<TypeParam>(9.99));
  values.push_back(static_cast<TypeParam>(19.99));
  values.push_back(static_cast<TypeParam>(0.01));
  values.push_back(static_cast<TypeParam>(100.00));
  values.push_back(static_cast<TypeParam>(999.95));
  values.push_back(static_cast<TypeParam>(0.50));
  this->testRoundtrip(values);
}

TYPED_TEST(ALPEncodingTest, ConstantValues) {
  nimble::Vector<TypeParam> values{this->pool_.get()};
  for (int i = 0; i < 50; ++i) {
    values.push_back(static_cast<TypeParam>(42.5));
  }
  this->testRoundtrip(values);
}

TYPED_TEST(ALPEncodingTest, WithFewExceptions) {
  // Mostly limited precision, with a few special values.
  nimble::Vector<TypeParam> values{this->pool_.get()};
  for (int i = 0; i < 100; ++i) {
    values.push_back(static_cast<TypeParam>(i) / static_cast<TypeParam>(10));
  }
  // Add a couple exceptions (infinity, NaN) -- under 5% threshold.
  values[50] = std::numeric_limits<TypeParam>::infinity();
  values[75] = std::numeric_limits<TypeParam>::quiet_NaN();
  this->testRoundtrip(values);
}

TYPED_TEST(ALPEncodingTest, SkipAndMaterialize) {
  using physicalType = typename nimble::TypeTraits<TypeParam>::physicalType;

  nimble::Vector<TypeParam> values{this->pool_.get()};
  for (int i = 0; i < 100; ++i) {
    values.push_back(static_cast<TypeParam>(i) / static_cast<TypeParam>(10));
  }

  auto physicalValues = std::span<const physicalType>(
      reinterpret_cast<const physicalType*>(values.data()), values.size());

  auto statistics = nimble::Statistics<physicalType>::create(physicalValues);
  nimble::EncodingSelection<physicalType> selection{
      {.encodingType = nimble::EncodingType::ALP},
      std::move(statistics),
      std::make_unique<ALPTestPolicy<TypeParam>>()};

  auto encoded = nimble::ALPEncoding<TypeParam>::encode(
      selection, physicalValues, *this->buffer_);

  const nimble::EncodingFactory factory{{}};
  auto encoding = factory.create(*this->pool_, encoded, nullptr);

  // Skip first 37, materialize remaining 63.
  encoding->skip(37);
  nimble::Vector<TypeParam> result{this->pool_.get()};
  result.resize(63);
  encoding->materialize(63, result.data());

  for (uint32_t i = 0; i < 63; ++i) {
    EXPECT_EQ(
        *reinterpret_cast<const physicalType*>(&values[37 + i]),
        *reinterpret_cast<const physicalType*>(&result[i]))
        << "Mismatch at index " << (37 + i);
  }
}

TYPED_TEST(ALPEncodingTest, ResetAndReread) {
  using physicalType = typename nimble::TypeTraits<TypeParam>::physicalType;

  nimble::Vector<TypeParam> values{this->pool_.get()};
  for (int i = 0; i < 50; ++i) {
    values.push_back(static_cast<TypeParam>(i) * static_cast<TypeParam>(0.25));
  }

  auto physicalValues = std::span<const physicalType>(
      reinterpret_cast<const physicalType*>(values.data()), values.size());

  auto statistics = nimble::Statistics<physicalType>::create(physicalValues);
  nimble::EncodingSelection<physicalType> selection{
      {.encodingType = nimble::EncodingType::ALP},
      std::move(statistics),
      std::make_unique<ALPTestPolicy<TypeParam>>()};

  auto encoded = nimble::ALPEncoding<TypeParam>::encode(
      selection, physicalValues, *this->buffer_);

  const nimble::EncodingFactory factory{{}};
  auto encoding = factory.create(*this->pool_, encoded, nullptr);

  // Read first 20.
  nimble::Vector<TypeParam> partial{this->pool_.get()};
  partial.resize(20);
  encoding->materialize(20, partial.data());

  // Reset and re-read all.
  encoding->reset();
  nimble::Vector<TypeParam> result{this->pool_.get()};
  result.resize(50);
  encoding->materialize(50, result.data());

  for (uint32_t i = 0; i < 50; ++i) {
    EXPECT_EQ(
        *reinterpret_cast<const physicalType*>(&values[i]),
        *reinterpret_cast<const physicalType*>(&result[i]))
        << "Mismatch at index " << i;
  }
}
