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
#include "dwio/nimble/common/Vector.h"
#include "velox/common/memory/Memory.h"

DECLARE_bool(velox_enable_memory_usage_track_in_default_memory_pool);

using namespace ::facebook;

class VectorTests : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    FLAGS_velox_enable_memory_usage_track_in_default_memory_pool = true;
  }

  void SetUp() override {
    pool_ = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

TEST(VectorTests, FromRange) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  std::vector<int32_t> source{4, 5, 6};
  nimble::Vector<int32_t> v1(pool.get(), source.begin(), source.end());
  EXPECT_EQ(3, v1.size());
  EXPECT_EQ(4, v1[0]);
  EXPECT_EQ(5, v1[1]);
  EXPECT_EQ(6, v1[2]);
}

TEST(VectorTests, EqualOp1) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Vector<int32_t> v1(pool.get());
  v1.push_back(1);
  v1.emplace_back(2);
  v1.push_back(3);

  nimble::Vector<int32_t> v2(pool.get());
  v2.push_back(4);
  v2.emplace_back(5);

  EXPECT_EQ(3, v1.size());
  EXPECT_EQ(1, v1[0]);
  EXPECT_EQ(2, v1[1]);
  EXPECT_EQ(3, v1[2]);
  EXPECT_EQ(2, v2.size());
  EXPECT_EQ(4, v2[0]);
  EXPECT_EQ(5, v2[1]);

  v1 = v2;
  EXPECT_EQ(2, v1.size());
  EXPECT_EQ(4, v1[0]);
  EXPECT_EQ(5, v1[1]);
  EXPECT_EQ(2, v2.size());
  EXPECT_EQ(4, v2[0]);
  EXPECT_EQ(5, v2[1]);
}

TEST(VectorTests, ExplicitMoveEqualOp) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Vector<int32_t> v1(pool.get());
  v1.push_back(1);
  v1.emplace_back(2);
  v1.push_back(3);

  nimble::Vector<int32_t> v2(pool.get());
  v2.push_back(4);
  v2.emplace_back(5);

  EXPECT_EQ(3, v1.size());
  ASSERT_FALSE(v1.empty());
  EXPECT_EQ(1, v1[0]);
  EXPECT_EQ(2, v1[1]);
  EXPECT_EQ(3, v1[2]);
  EXPECT_EQ(2, v2.size());
  ASSERT_FALSE(v2.empty());
  EXPECT_EQ(4, v2[0]);
  EXPECT_EQ(5, v2[1]);

  v1 = std::move(v2);
  EXPECT_EQ(2, v1.size());
  EXPECT_EQ(4, v1[0]);
  EXPECT_EQ(5, v1[1]);
  // @lint-ignore CLANGTIDY bugprone-use-after-move
  EXPECT_EQ(0, v2.size());
  ASSERT_TRUE(v2.empty());
}

TEST(VectorTests, MoveEqualOp1) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Vector<int32_t> v1(pool.get());
  v1.push_back(1);
  v1.emplace_back(2);
  v1.push_back(3);
  EXPECT_EQ(3, v1.size());
  EXPECT_EQ(1, v1[0]);
  EXPECT_EQ(2, v1[1]);
  EXPECT_EQ(3, v1[2]);
  v1 = nimble::Vector(pool.get(), {4, 5});
  EXPECT_EQ(2, v1.size());
  EXPECT_EQ(4, v1[0]);
  EXPECT_EQ(5, v1[1]);
}

TEST(VectorTests, CopyCtr) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Vector<int32_t> v2(pool.get());
  v2.push_back(3);
  v2.emplace_back(4);
  EXPECT_EQ(2, v2.size());
  EXPECT_EQ(3, v2[0]);
  EXPECT_EQ(4, v2[1]);
  nimble::Vector<int32_t> v1(v2);
  EXPECT_EQ(2, v1.size());
  EXPECT_EQ(3, v1[0]);
  EXPECT_EQ(4, v1[1]);

  // make sure they do not share buffer
  v1[0] = 1;
  v1[1] = 2;
  EXPECT_EQ(2, v1.size());
  EXPECT_EQ(1, v1[0]);
  EXPECT_EQ(2, v1[1]);
  EXPECT_EQ(2, v2.size());
  EXPECT_EQ(3, v2[0]);
  EXPECT_EQ(4, v2[1]);
}

TEST(VectorTests, BoolInitializerList) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Vector<bool> v1(pool.get(), {true, false, true});
  EXPECT_EQ(3, v1.size());
  EXPECT_EQ(true, v1[0]);
  EXPECT_EQ(false, v1[1]);
  EXPECT_EQ(true, v1[2]);
}

TEST(VectorTests, BoolEqualOp1) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Vector<bool> v1(pool.get());
  v1.push_back(false);
  v1.emplace_back(true);
  v1.push_back(true);

  EXPECT_EQ(3, v1.size());
  EXPECT_EQ(false, v1[0]);
  EXPECT_EQ(true, v1[1]);
  EXPECT_EQ(true, v1[2]);

  nimble::Vector<bool> v2(pool.get());
  v2.push_back(true);
  v2.emplace_back(false);

  EXPECT_EQ(2, v2.size());
  EXPECT_EQ(true, v2[0]);
  EXPECT_EQ(false, v2[1]);

  v1 = v2;
  EXPECT_EQ(2, v1.size());
  EXPECT_EQ(true, v1[0]);
  EXPECT_EQ(false, v1[1]);
  EXPECT_EQ(2, v2.size());
  EXPECT_EQ(true, v2[0]);
  EXPECT_EQ(false, v2[1]);
}

TEST(VectorTests, BoolMoveEqualOp1) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Vector<bool> v1(pool.get());
  v1.push_back(true);
  v1.emplace_back(false);
  v1.push_back(false);

  EXPECT_EQ(3, v1.size());
  EXPECT_EQ(true, v1[0]);
  EXPECT_EQ(false, v1[1]);
  EXPECT_EQ(false, v1[2]);

  v1 = nimble::Vector(pool.get(), {false, true});
  EXPECT_EQ(2, v1.size());
  EXPECT_EQ(false, v1[0]);
  EXPECT_EQ(true, v1[1]);
}

TEST(VectorTests, BoolCopyCtr) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Vector<bool> v2(pool.get());
  v2.push_back(true);
  v2.emplace_back(false);
  EXPECT_EQ(2, v2.size());
  EXPECT_EQ(true, v2[0]);
  EXPECT_EQ(false, v2[1]);
  nimble::Vector<bool> v1(v2);
  EXPECT_EQ(2, v1.size());
  EXPECT_EQ(true, v1[0]);
  EXPECT_EQ(false, v1[1]);
  EXPECT_EQ(2, v2.size());
  EXPECT_EQ(true, v2[0]);
  EXPECT_EQ(false, v2[1]);
}

TEST(VectorTests, MemoryCleanup) {
  velox::memory::MemoryManagerOptions options;
  options.trackDefaultUsage = true;
  velox::memory::MemoryManager memoryManager{options};
  auto pool = memoryManager.addLeafPool();
  EXPECT_EQ(0, pool->usedBytes());
  {
    nimble::Vector<int32_t> v(pool.get());
    EXPECT_EQ(0, pool->usedBytes());
    v.resize(1000, 10);
    EXPECT_NE(0, pool->usedBytes());
  }
  EXPECT_EQ(0, pool->usedBytes());
  {
    nimble::Vector<int32_t> v(pool.get());
    EXPECT_EQ(0, pool->usedBytes());
    v.resize(1000, 10);
    EXPECT_NE(0, pool->usedBytes());

    auto vCopy(v);
  }
  EXPECT_EQ(0, pool->usedBytes());
  {
    nimble::Vector<int32_t> v(pool.get());
    EXPECT_EQ(0, pool->usedBytes());
    v.resize(1000, 10);
    EXPECT_NE(0, pool->usedBytes());

    auto vCopy(std::move(v));
  }
  EXPECT_EQ(0, pool->usedBytes());
}
