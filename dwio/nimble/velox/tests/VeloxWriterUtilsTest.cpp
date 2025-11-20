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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/StreamData.h"
#include "dwio/nimble/velox/VeloxWriterUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/SharedArbitrator.h"

using namespace ::testing;

namespace facebook::nimble {

class VeloxWriterUtilsTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::SharedArbitrator::registerFactory();
    velox::memory::MemoryManager::Options options;
    options.arbitratorKind = "SHARED";
    velox::memory::MemoryManager::testingSetInstance(options);
  }

  void SetUp() override {
    rootPool_ = velox::memory::memoryManager()->addRootPool("default_root");
    leafPool_ = rootPool_->addLeafChild("default_leaf");
    schemaBuilder_ = std::make_unique<SchemaBuilder>();
    inputBufferGrowthPolicy_ =
        DefaultInputBufferGrowthPolicy::withDefaultRanges();
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> leafPool_;
  std::unique_ptr<SchemaBuilder> schemaBuilder_;
  std::unique_ptr<InputBufferGrowthPolicy> inputBufferGrowthPolicy_;
};

TEST_F(VeloxWriterUtilsTest, getStreamIndicesByMemoryUsageTest) {
  // Test 1: Empty streams vector
  {
    std::vector<std::unique_ptr<StreamData>> streams;
    auto result = getStreamIndicesByMemoryUsage(streams);
    EXPECT_TRUE(result.empty());
  }

  // Test 2: Single stream
  {
    std::vector<std::unique_ptr<StreamData>> streams;
    auto scalarTypeBuilder =
        schemaBuilder_->createScalarTypeBuilder(ScalarKind::Int32);
    auto descriptor = &scalarTypeBuilder->scalarDescriptor();
    streams.push_back(
        std::make_unique<ContentStreamData<int32_t>>(
            *leafPool_, *descriptor, *inputBufferGrowthPolicy_));

    auto result = getStreamIndicesByMemoryUsage(streams);
    EXPECT_THAT(result, ElementsAre(0));
  }

  // Test 3: Multiple streams with different memory usage
  // Should be sorted in descending order by memory usage
  {
    std::vector<std::unique_ptr<StreamData>> streams;
    auto scalarTypeBuilder =
        schemaBuilder_->createScalarTypeBuilder(ScalarKind::Int32);
    auto descriptor = &scalarTypeBuilder->scalarDescriptor();

    // Stream 0: Small memory usage (empty)
    streams.push_back(
        std::make_unique<ContentStreamData<int32_t>>(
            *leafPool_, *descriptor, *inputBufferGrowthPolicy_));

    // Stream 1: Medium memory usage
    auto stream1 = std::make_unique<ContentStreamData<int32_t>>(
        *leafPool_, *descriptor, *inputBufferGrowthPolicy_);
    std::vector<int32_t> data1 = {1, 2, 3, 4, 5};
    auto& mutableData1 = stream1->mutableData();
    for (const auto& item : data1) {
      mutableData1.push_back(item);
    }
    streams.push_back(std::move(stream1));

    // Stream 2: Large memory usage
    auto stream2 = std::make_unique<ContentStreamData<int32_t>>(
        *leafPool_, *descriptor, *inputBufferGrowthPolicy_);
    std::vector<int32_t> data2(1000, 42);
    auto& mutableData2 = stream2->mutableData();
    for (const auto& item : data2) {
      mutableData2.push_back(item);
    }
    streams.push_back(std::move(stream2));

    // Stream 3: Large memory usage. Should map to same bucket as stream 2
    auto stream3 = std::make_unique<ContentStreamData<int32_t>>(
        *leafPool_, *descriptor, *inputBufferGrowthPolicy_);
    std::vector<int32_t> data3(900, 42);
    auto& mutableData3 = stream3->mutableData();
    for (const auto& item : data3) {
      mutableData3.push_back(item);
    }
    streams.push_back(std::move(stream3));

    auto result = getStreamIndicesByMemoryUsage(streams);

    // Stream 2 should come first (largest), then stream 1, then stream 0
    EXPECT_THAT(result, ElementsAre(2, 3, 1, 0));
  }
}

} // namespace facebook::nimble
