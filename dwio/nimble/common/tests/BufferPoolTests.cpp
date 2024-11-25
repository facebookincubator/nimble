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
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/ExecutorBarrier.h"

namespace facebook::nimble::test {
using MemoryPool = velox::memory::MemoryPool;
using ExecutorBarrier = velox::dwio::common::ExecutorBarrier;

class BufferPoolTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {}

  void SetUp() override {
    memPool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  }

  std::shared_ptr<velox::memory::MemoryPool> memPool_;
};

TEST_F(BufferPoolTest, CreateBufferPool) {
  auto bufferPool = BufferPool{*memPool_};
  EXPECT_EQ(bufferPool.size(), std::thread::hardware_concurrency());
}

TEST_F(BufferPoolTest, CreateBufferPoolBadMaxPool) {
  try {
    auto bufferPool = BufferPool{*memPool_, /* maxPoolSize */ 0};
    FAIL();
  } catch (const NimbleUserError& e) {
    EXPECT_EQ(e.errorMessage(), "max pool size must be > 0");
  }
}

TEST_F(BufferPoolTest, ReserveAddBuffer) {
  auto bufferPool = BufferPool{*memPool_, /* maxPoolSize */ 10};
  auto buffer = bufferPool.reserveBuffer();
  EXPECT_EQ(bufferPool.size(), 9);
  bufferPool.addBuffer(std::move(buffer));
  EXPECT_EQ(bufferPool.size(), 10);
}

TEST_F(BufferPoolTest, EmptyFillBufferPool) {
  size_t iterations = 10;
  std::vector<std::unique_ptr<Buffer>> buffers;
  auto bufferPool = BufferPool{*memPool_, iterations};

  for (auto i = 0; i < iterations; ++i) {
    auto buffer = bufferPool.reserveBuffer();
    buffers.push_back(std::move(buffer));
    EXPECT_EQ(bufferPool.size(), iterations - i - 1);
  }
  EXPECT_EQ(bufferPool.size(), 0);

  for (auto i = 0; i < iterations; ++i) {
    bufferPool.addBuffer(std::move(buffers.back()));
    buffers.pop_back();
    EXPECT_EQ(bufferPool.size(), i + 1);
  }
}

TEST_F(BufferPoolTest, ParallelFillPool) {
  for (auto parallelismFactor : {2U}) {
    folly::CPUThreadPoolExecutor executor{parallelismFactor};
    ExecutorBarrier barrier{executor};
    std::vector<std::unique_ptr<Buffer>> buffers;
    auto bufferPool = BufferPool{*memPool_, parallelismFactor};
    EXPECT_EQ(bufferPool.size(), parallelismFactor);

    for (auto i = 0; i < parallelismFactor; ++i) {
      barrier.add([&]() {
        auto buffer = bufferPool.reserveBuffer();
        buffers.push_back(std::move(buffer));
      });
    }

    barrier.waitAll();
    EXPECT_EQ(bufferPool.size(), 0);

    for (auto i = 0; i < parallelismFactor; ++i) {
      barrier.add([&]() {
        bufferPool.addBuffer(std::move(buffers.back()));
        buffers.pop_back();
      });
    }

    barrier.waitAll();
    EXPECT_EQ(bufferPool.size(), parallelismFactor);
  }
}
} // namespace facebook::nimble::test
