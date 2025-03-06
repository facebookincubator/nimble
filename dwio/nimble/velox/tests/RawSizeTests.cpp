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

#include "dwio/nimble/velox/RawSizeUtils.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace facebook;

class RawSizeBaseTestFixture : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    velox::memory::MemoryManager::initialize({});
  }

  void SetUp() override {
    pool_ = velox::memory::memoryManager()->addLeafPool();
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

class RawSizeTestFixture : public RawSizeBaseTestFixture {};

template <typename T>
class RawSizeTypedTestFixture : public RawSizeBaseTestFixture {};

TYPED_TEST_SUITE_P(RawSizeTypedTestFixture);

TYPED_TEST_P(RawSizeTypedTestFixture, FlatVector) {
  auto vectorMaker = velox::test::VectorMaker(this->pool_.get());
  auto flatVector =
      vectorMaker.flatVector<TypeParam>({0, 1, 0, 1, 0, 1, 0, 1, 0, 1});
  auto rawSize = nimble::getRawSizeFromVector(flatVector);

  ASSERT_EQ(sizeof(TypeParam) * 10, rawSize);
}

TYPED_TEST_P(RawSizeTypedTestFixture, FlatVectorSomeNull) {
  auto vectorMaker = velox::test::VectorMaker(this->pool_.get());
  auto flatVector = vectorMaker.flatVectorNullable<TypeParam>(
      {0, 1, 0, 1, 0, 1, 0, std::nullopt, std::nullopt, std::nullopt});
  auto rawSize = nimble::getRawSizeFromVector(flatVector);

  ASSERT_EQ(sizeof(TypeParam) * 7 + nimble::NULL_SIZE * 3, rawSize);
}

TYPED_TEST_P(RawSizeTypedTestFixture, FlatVectorAllNull) {
  auto vectorMaker = velox::test::VectorMaker(this->pool_.get());
  auto flatVector = vectorMaker.flatVectorNullable<TypeParam>(
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
  auto rawSize = nimble::getRawSizeFromVector(flatVector);

  ASSERT_EQ(nimble::NULL_SIZE * 10, rawSize);
}

TYPED_TEST_P(RawSizeTypedTestFixture, ConstVector) {
  auto vectorMaker = velox::test::VectorMaker(this->pool_.get());
  auto constVector = vectorMaker.constantVector<TypeParam>({0, 0, 0, 0, 0, 0});
  auto rawSize = nimble::getRawSizeFromVector(constVector);

  ASSERT_EQ(sizeof(TypeParam) * 6, rawSize);
}

TYPED_TEST_P(RawSizeTypedTestFixture, ConstVectorAllNull) {
  auto vectorMaker = velox::test::VectorMaker(this->pool_.get());
  auto constVector = vectorMaker.constantVector<TypeParam>(
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
  auto rawSize = nimble::getRawSizeFromVector(constVector);

  ASSERT_EQ(nimble::NULL_SIZE * 6, rawSize);
}

REGISTER_TYPED_TEST_SUITE_P(
    RawSizeTypedTestFixture,
    FlatVector,
    FlatVectorSomeNull,
    FlatVectorAllNull,
    ConstVector,
    ConstVectorAllNull);

using FixedWidthTypes =
    ::testing::Types<bool, int8_t, int16_t, int32_t, int64_t, float, double>;

INSTANTIATE_TYPED_TEST_SUITE_P(
    RawSizeTestSuite,
    RawSizeTypedTestFixture,
    FixedWidthTypes);

TEST_F(RawSizeTestFixture, FlatString) {
  auto vectorMaker = velox::test::VectorMaker(pool_.get());
  auto flatVector = vectorMaker.flatVector<velox::StringView>(
      {"a", "bbbb", "ccccccccc", "dddddddddddddddd"}); // 1 4 9 16
  auto rawSize = nimble::getRawSizeFromVector(flatVector);

  ASSERT_EQ(30, rawSize);
}

TEST_F(RawSizeTestFixture, FlatStringSomeNull) {
  auto vectorMaker = velox::test::VectorMaker(pool_.get());
  auto flatVector = vectorMaker.flatVectorNullable<velox::StringView>(
      {"a", "bbbb", std::nullopt, "dddddddddddddddd"}); // 1 4 16 + 1
  auto rawSize = nimble::getRawSizeFromVector(flatVector);

  ASSERT_EQ(22, rawSize);
}

TEST_F(RawSizeTestFixture, FlatStringAllNull) {
  auto vectorMaker = velox::test::VectorMaker(pool_.get());
  auto flatVector = vectorMaker.flatVectorNullable<velox::StringView>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
  auto rawSize = nimble::getRawSizeFromVector(flatVector);

  ASSERT_EQ(nimble::NULL_SIZE * 4, rawSize);
}

TEST_F(RawSizeTestFixture, FlatStringVarBinary) {
  auto vectorMaker = velox::test::VectorMaker(pool_.get());
  auto flatVector = vectorMaker.flatVector<velox::StringView>(
      {"a", "bbbb", "ccccccccc", "dddddddddddddddd"},
      velox::VARBINARY()); // 1 4 9 16
  auto rawSize = nimble::getRawSizeFromVector(flatVector);

  ASSERT_EQ(30, rawSize);
}

TEST_F(RawSizeTestFixture, FlatStringVarBinarySomeNull) {
  auto vectorMaker = velox::test::VectorMaker(pool_.get());
  auto flatVector = vectorMaker.flatVectorNullable<velox::StringView>(
      {"a", "bbbb", std::nullopt, "dddddddddddddddd"},
      velox::VARBINARY()); // 1 4 16 + 1
  auto rawSize = nimble::getRawSizeFromVector(flatVector);

  ASSERT_EQ(22, rawSize);
}

TEST_F(RawSizeTestFixture, FlatStringVarBinaryAllNull) {
  auto vectorMaker = velox::test::VectorMaker(pool_.get());
  auto flatVector = vectorMaker.flatVectorNullable<velox::StringView>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      velox::VARBINARY());
  auto rawSize = nimble::getRawSizeFromVector(flatVector);

  ASSERT_EQ(nimble::NULL_SIZE * 4, rawSize);
}

TEST_F(RawSizeTestFixture, ConstString) {
  auto vectorMaker = velox::test::VectorMaker(this->pool_.get());
  auto constVector = vectorMaker.constantVector<velox::StringView>(
      {"foo", "foo", "foo", "foo"});
  auto rawSize = nimble::getRawSizeFromVector(constVector);

  ASSERT_EQ(3 * 4, rawSize);
}

TEST_F(RawSizeTestFixture, ConstStringAllNull) {
  auto vectorMaker = velox::test::VectorMaker(this->pool_.get());
  auto constVector = vectorMaker.constantVector<velox::StringView>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
  auto rawSize = nimble::getRawSizeFromVector(constVector);

  ASSERT_EQ(nimble::NULL_SIZE * 4, rawSize);
}

TEST_F(RawSizeTestFixture, ThrowOnDefaultType) {
  auto unknownVector = facebook::velox::BaseVector::create(
      facebook::velox::UNKNOWN(), 10, pool_.get());

  EXPECT_THROW(
      nimble::getRawSizeFromVector(unknownVector), velox::VeloxRuntimeError);
}

TEST_F(RawSizeTestFixture, ThrowOnDefaultEncodingFixedWidth) {
  auto vectorMaker = velox::test::VectorMaker(pool_.get());
  auto sequenceVector =
      vectorMaker.sequenceVector<int8_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

  EXPECT_THROW(
      nimble::getRawSizeFromVector(sequenceVector), velox::VeloxRuntimeError);
}

TEST_F(RawSizeTestFixture, ThrowOnDefaultEncodingVariableWidth) {
  auto vectorMaker = velox::test::VectorMaker(pool_.get());
  auto sequenceVector = vectorMaker.sequenceVector<velox::StringView>(
      {"a", "bbbb", "ccccccccc", "dddddddddddddddd"});

  EXPECT_THROW(
      nimble::getRawSizeFromVector(sequenceVector), velox::VeloxRuntimeError);
}
