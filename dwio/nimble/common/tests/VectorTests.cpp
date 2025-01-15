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
#include "velox/flag_definitions/flags.h"

DECLARE_bool(velox_enable_memory_usage_track_in_default_memory_pool);

using namespace ::facebook;

class VectorTests : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    FLAGS_velox_enable_memory_usage_track_in_default_memory_pool = true;
    facebook::velox::translateFlagsToGlobalConfig();
  }

  void SetUp() override {
    pool_ = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
    facebook::velox::translateFlagsToGlobalConfig();
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

TEST_F(VectorTests, InitializerList) {
  nimble::Vector<int32_t> v1(pool_.get(), {3, 4});
  EXPECT_EQ(2, v1.size());
  EXPECT_EQ(3, v1[0]);
  EXPECT_EQ(4, v1[1]);
}

TEST_F(VectorTests, FromRange) {
  std::vector<int32_t> source{4, 5, 6};
  nimble::Vector<int32_t> v1(pool_.get(), source.begin(), source.end());
  EXPECT_EQ(3, v1.size());
  EXPECT_EQ(4, v1[0]);
  EXPECT_EQ(5, v1[1]);
  EXPECT_EQ(6, v1[2]);
}

TEST_F(VectorTests, EqualOp1) {
  nimble::Vector<int32_t> v1(pool_.get());
  v1.push_back(1);
  v1.emplace_back(2);
  v1.push_back(3);

  nimble::Vector<int32_t> v2(pool_.get());
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

TEST_F(VectorTests, ExplicitMoveEqualOp) {
  nimble::Vector<int32_t> v1(pool_.get());
  v1.push_back(1);
  v1.emplace_back(2);
  v1.push_back(3);

  nimble::Vector<int32_t> v2(pool_.get());
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

TEST_F(VectorTests, MoveEqualOp1) {
  nimble::Vector<int32_t> v1(pool_.get());
  v1.push_back(1);
  v1.emplace_back(2);
  v1.push_back(3);
  EXPECT_EQ(3, v1.size());
  EXPECT_EQ(1, v1[0]);
  EXPECT_EQ(2, v1[1]);
  EXPECT_EQ(3, v1[2]);
  v1 = nimble::Vector(pool_.get(), {4, 5});
  EXPECT_EQ(2, v1.size());
  EXPECT_EQ(4, v1[0]);
  EXPECT_EQ(5, v1[1]);
}

TEST_F(VectorTests, CopyCtr) {
  nimble::Vector<int32_t> v2(pool_.get());
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

TEST_F(VectorTests, BoolInitializerList) {
  nimble::Vector<bool> v1(pool_.get(), {true, false, true});
  EXPECT_EQ(3, v1.size());
  EXPECT_EQ(true, v1[0]);
  EXPECT_EQ(false, v1[1]);
  EXPECT_EQ(true, v1[2]);
}

TEST_F(VectorTests, BoolEqualOp1) {
  nimble::Vector<bool> v1(pool_.get());
  v1.push_back(false);
  v1.emplace_back(true);
  v1.push_back(true);

  EXPECT_EQ(3, v1.size());
  EXPECT_EQ(false, v1[0]);
  EXPECT_EQ(true, v1[1]);
  EXPECT_EQ(true, v1[2]);

  nimble::Vector<bool> v2(pool_.get());
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

TEST_F(VectorTests, BoolMoveEqualOp1) {
  nimble::Vector<bool> v1(pool_.get());
  v1.push_back(true);
  v1.emplace_back(false);
  v1.push_back(false);

  EXPECT_EQ(3, v1.size());
  EXPECT_EQ(true, v1[0]);
  EXPECT_EQ(false, v1[1]);
  EXPECT_EQ(false, v1[2]);

  v1 = nimble::Vector(pool_.get(), {false, true});
  EXPECT_EQ(2, v1.size());
  EXPECT_EQ(false, v1[0]);
  EXPECT_EQ(true, v1[1]);
}

TEST_F(VectorTests, BoolCopyCtr) {
  nimble::Vector<bool> v2(pool_.get());
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

TEST_F(VectorTests, MemoryCleanup) {
  EXPECT_EQ(0, pool_->usedBytes());
  {
    nimble::Vector<int32_t> v(pool_.get());
    EXPECT_EQ(0, pool_->usedBytes());
    v.resize(1000, 10);
    EXPECT_NE(0, pool_->usedBytes());
  }
  EXPECT_EQ(0, pool_->usedBytes());
  {
    nimble::Vector<int32_t> v(pool_.get());
    EXPECT_EQ(0, pool_->usedBytes());
    v.resize(1000, 10);
    EXPECT_NE(0, pool_->usedBytes());

    auto vCopy(v);
  }
  EXPECT_EQ(0, pool_->usedBytes());
  {
    nimble::Vector<int32_t> v(pool_.get());
    EXPECT_EQ(0, pool_->usedBytes());
    v.resize(1000, 10);
    EXPECT_NE(0, pool_->usedBytes());

    auto vCopy(std::move(v));
  }
  EXPECT_EQ(0, pool_->usedBytes());
}
