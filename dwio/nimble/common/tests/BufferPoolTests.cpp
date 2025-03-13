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

#include <gtest/gtest.h>
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/velox/FieldWriter.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/dwio/common/ExecutorBarrier.h"

namespace facebook::nimble::test {
using MemoryPool = velox::memory::MemoryPool;
using ExecutorBarrier = velox::dwio::common::ExecutorBarrier;

class BufferPoolTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::SharedArbitrator::registerFactory();
    velox::memory::MemoryManager::testingSetInstance(
        {.arbitratorKind = "SHARED"});
  }

  void SetUp() override {
    rootPool_ = velox::memory::memoryManager()->addRootPool("default_root");
    leafPool_ = rootPool_->addLeafChild("default_leaf");
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> leafPool_;
};

TEST_F(BufferPoolTest, CreateBufferPoolBadMaxPool) {
  try {
    auto bufferPool = BufferPool{*leafPool_, /* maxPoolSize */ 0};
    FAIL();
  } catch (const NimbleUserError& e) {
    EXPECT_EQ(e.errorMessage(), "max pool size must be > 0");
  }
}

TEST_F(BufferPoolTest, ReserveBuffer) {
  auto bufferPool = BufferPool{*leafPool_, /* maxPoolSize */ 10};
  EXPECT_EQ(bufferPool.size(), 10);
  {
    auto buffer = bufferPool.reserveBuffer();
    EXPECT_EQ(bufferPool.size(), 9);
  }
  EXPECT_EQ(bufferPool.size(), 10);
}

TEST_F(BufferPoolTest, EmptyFillBufferPool) {
  size_t iterations = 10;
  auto bufferPool = BufferPool{*leafPool_, /* maxPoolSize */ iterations};

  for (auto i = 0; i < iterations; ++i) {
    {
      auto buffer1 = bufferPool.reserveBuffer();
      auto buffer2 = bufferPool.reserveBuffer();
      auto buffer3 = bufferPool.reserveBuffer();

      EXPECT_EQ(bufferPool.size(), iterations - 3);
    }
    EXPECT_EQ(bufferPool.size(), iterations);
  }
}

TEST_F(BufferPoolTest, ParallelFillPool) {
  auto parallelismFactor = std::thread::hardware_concurrency();
  auto executor =
      std::make_shared<folly::CPUThreadPoolExecutor>(parallelismFactor);
  ExecutorBarrier barrier{executor};
  auto bufferPool = BufferPool{*leafPool_};
  EXPECT_EQ(bufferPool.size(), parallelismFactor);

  for (auto i = 0; i < parallelismFactor; ++i) {
    barrier.add([&]() {
      for (auto j = 0; j < 100000; ++j) {
        auto buffer = bufferPool.reserveBuffer();
      }
    });
  }

  barrier.waitAll();
  EXPECT_LE(bufferPool.size(), parallelismFactor);
}
} // namespace facebook::nimble::test
