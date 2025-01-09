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
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/velox/FieldWriter.h"
#include "folly/executors/CPUThreadPoolExecutor.h"

namespace {
using DecodingContext = facebook::nimble::DecodingContextPool::DecodingContext;
using DecodingContextPool = facebook::nimble::DecodingContextPool;

TEST(DeocodingContextPoolTest, VectorDecoderVisitorMissingThrows) {
  try {
    auto pool = DecodingContextPool{/* vectorDecoderVisitor*/ nullptr};
    FAIL();
  } catch (facebook::nimble::NimbleUserError& e) {
    ASSERT_EQ(e.errorMessage(), "vectorDecoderVisitor must be set");
  }
}

TEST(DecodingContextPoolTest, ReserveAddPair) {
  DecodingContextPool pool;
  EXPECT_EQ(pool.size(), 0);
  {
    // inner scope
    auto context = pool.reserveContext();
    EXPECT_EQ(pool.size(), 0);
  }

  EXPECT_EQ(pool.size(), 1);
}

TEST(DecodingContextPoolTest, FillPool) {
  DecodingContextPool pool;
  EXPECT_EQ(pool.size(), 0);

  { // inner scope
    auto context1 = pool.reserveContext();
    auto context2 = pool.reserveContext();
    auto context3 = pool.reserveContext();
    auto context4 = pool.reserveContext();
    EXPECT_EQ(pool.size(), 0);
  }

  EXPECT_EQ(pool.size(), 4);
}

TEST(DecodingContextPoolTest, ParallelFillPool) {
  auto parallelismFactor = std::thread::hardware_concurrency();
  auto executor = folly::CPUThreadPoolExecutor{parallelismFactor};

  DecodingContextPool pool;
  EXPECT_EQ(pool.size(), 0);

  for (auto i = 0; i < parallelismFactor; ++i) {
    executor.add([&]() {
      for (auto j = 0; j < 100000; ++j) {
        auto context = pool.reserveContext();
      }
    });
  }
  executor.join();
  EXPECT_LE(pool.size(), parallelismFactor);
}
} // namespace
