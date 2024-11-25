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
#include "velox/dwio/common/ExecutorBarrier.h"
#include "velox/vector/DecodedVector.h"

namespace {
using DecodingContext = facebook::nimble::DecodingContext;
using DecodingContextPool = facebook::nimble::DecodingContextPool;

TEST(DeocodingContextPoolTest, vectorDecoderVisitorMissingThrows) {
  try {
    auto pool = DecodingContextPool{/* vectorDecoderVisitor*/ nullptr};
    FAIL();
  } catch (facebook::nimble::NimbleUserError& e) {
    ASSERT_EQ(e.errorMessage(), "vectorDecoderVisitor must be set");
  }
}

TEST(DecodingContextPoolTest, EmptyDecodingPairPool) {
  auto pairPool = DecodingContextPool{};
  EXPECT_EQ(pairPool.size(), 0);
}

TEST(DecodingContextPoolTest, ReserveAddPair) {
  auto pool = DecodingContextPool{};
  EXPECT_EQ(pool.size(), 0);
  auto context1 = pool.reserveContext();
  pool.addContext(std::move(context1));
  EXPECT_EQ(pool.size(), 1);
  auto context2 = pool.reserveContext();
  EXPECT_EQ(pool.size(), 0);
}

TEST(DecodingContextPoolTest, FillPool) {
  const size_t iterations = 10;
  std::vector<std::unique_ptr<DecodingContext>> contexts;

  auto pool = DecodingContextPool{};

  for (auto i = 0; i < iterations; ++i) {
    auto context = pool.reserveContext();
    contexts.push_back(std::move(context));
    EXPECT_EQ(pool.size(), 0);
  }
  EXPECT_EQ(contexts.size(), iterations);

  for (auto i = 0; i < iterations; ++i) {
    EXPECT_EQ(pool.size(), i);
    pool.addContext(std::move(contexts.back()));
    contexts.pop_back();
    EXPECT_EQ(pool.size(), i + 1);
  }
  for (auto i = 0; i < iterations; ++i) {
    EXPECT_EQ(pool.size(), iterations - i);
    pool.reserveContext();
    EXPECT_EQ(pool.size(), iterations - i - 1);
  }
}

TEST(DecodingContextPoolTest, ParallelFillPool) {
  const size_t iterations = 10;

  for (auto paralleismFactor : {1U, 2U, std::thread::hardware_concurrency()}) {
    auto pool = DecodingContextPool{};
    auto executor = folly::CPUThreadPoolExecutor{paralleismFactor};
    std::vector<std::unique_ptr<DecodingContext>> contexts;

    facebook::velox::dwio::common::ExecutorBarrier barrier{executor};
    for (auto i = 0; i < iterations; ++i) {
      barrier.add([&]() { contexts.push_back(pool.reserveContext()); });
    }
    barrier.waitAll();
    EXPECT_EQ(pool.size(), 0);

    for (auto i = 0; i < iterations; ++i) {
      barrier.add([&]() {
        pool.addContext(std::move(contexts.back()));
        contexts.pop_back();
      });
    }
    barrier.waitAll();
    EXPECT_EQ(pool.size(), iterations);
  }
}
} // namespace
