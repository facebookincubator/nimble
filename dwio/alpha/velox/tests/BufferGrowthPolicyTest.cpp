// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "dwio/alpha/velox/BufferGrowthPolicy.h"

namespace facebook::alpha {

struct DefaultInputBufferGrowthPolicyTestCase {
  std::map<uint64_t, float> rangedConfigs;
  uint64_t size;
  uint64_t capacity;
  uint64_t expectedNewCapacity;
};

class DefaultInputBufferGrowthPolicyTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<
          DefaultInputBufferGrowthPolicyTestCase> {};

TEST_P(DefaultInputBufferGrowthPolicyTest, GetExtendedCapacity) {
  const auto& testCase = GetParam();
  DefaultInputBufferGrowthPolicy policy{testCase.rangedConfigs};

  ASSERT_EQ(
      policy.getExtendedCapacity(testCase.size, testCase.capacity),
      testCase.expectedNewCapacity);
}

INSTANTIATE_TEST_CASE_P(
    DefaultInputBufferGrowthPolicyMinCapacityTestSuite,
    DefaultInputBufferGrowthPolicyTest,
    testing::Values(
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}},
            .size = 8,
            .capacity = 0,
            .expectedNewCapacity = 16},
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}},
            .size = 16,
            .capacity = 0,
            .expectedNewCapacity = 16},
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}},
            .size = 8,
            .capacity = 4,
            .expectedNewCapacity = 16},
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}},
            .size = 16,
            .capacity = 10,
            .expectedNewCapacity = 16},
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{4, 4.0f}, {16, 2.0f}, {1024, 1.5f}},
            .size = 2,
            .capacity = 0,
            .expectedNewCapacity = 4}));

INSTANTIATE_TEST_CASE_P(
    DefaultInputBufferGrowthPolicyInRangeGrowthTestSuite,
    DefaultInputBufferGrowthPolicyTest,
    testing::Values(
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}},
            .size = 8,
            .capacity = 8,
            .expectedNewCapacity = 8},
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}},
            .size = 20,
            .capacity = 16,
            .expectedNewCapacity = 32},
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}},
            .size = 24,
            .capacity = 20,
            .expectedNewCapacity = 40},
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}},
            .size = 60,
            .capacity = 20,
            .expectedNewCapacity = 80},
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}, {1024, 1.5f}},
            .size = 24,
            .capacity = 20,
            .expectedNewCapacity = 40},
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}, {1024, 1.5f}},
            .size = 1028,
            .capacity = 1024,
            .expectedNewCapacity = 1536},
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}, {1024, 1.5f}},
            .size = 1028,
            .capacity = 1000,
            .expectedNewCapacity = 1500},
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}, {1024, 1.5f}},
            .size = 512,
            .capacity = 16,
            .expectedNewCapacity = 512}));

// We determine the growth factor only once and grow the
// capacity until it suffices.
INSTANTIATE_TEST_CASE_P(
    DefaultInputBufferGrowthPolicyCrossRangeTestSuite,
    DefaultInputBufferGrowthPolicyTest,
    testing::Values(
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}},
            .size = 20,
            .capacity = 0,
            .expectedNewCapacity = 32},
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}, {1024, 1.5f}},
            .size = 2048,
            .capacity = 1000,
            .expectedNewCapacity = 2250},
        DefaultInputBufferGrowthPolicyTestCase{
            .rangedConfigs = {{16, 2.0f}, {1024, 1.5f}, {2048, 1.2f}},
            .size = 2048,
            .capacity = 1000,
            .expectedNewCapacity = 2073}));
} // namespace facebook::alpha
