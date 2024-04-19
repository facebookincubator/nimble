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
#include "dwio/nimble/common/IndexMap.h"

using namespace ::facebook;

TEST(IndexMapTests, DoubleIndexMap) {
  {
    // First, verify that flat_hash_map doesn't work with double/float
    folly::F14FastMap<double, int> indices;
    indices.emplace(+0.0, 1);
    int count = indices.size();
    EXPECT_EQ(1, count);
    indices.emplace(-0.0, 2);
    count = indices.size();
    EXPECT_EQ(1, count);
  }

  {
    nimble::IndexMap<double> dIndices;
    dIndices.index(+0.0);
    int count = dIndices.size();
    EXPECT_EQ(1, count);
    dIndices.index(-0.0);
    count = dIndices.size();
    EXPECT_EQ(2, count);
  }
}

TEST(IndexMapTests, FloatIndexMap) {
  {
    // First, verify that flat_hash_map doesn't work with double/float
    folly::F14FastMap<float, int> indices;
    indices.emplace(+0.0f, 1);
    int count = indices.size();
    EXPECT_EQ(1, count);
    indices.emplace(-0.0f, 2);
    count = indices.size();
    EXPECT_EQ(1, count);
  }

  {
    nimble::IndexMap<float> dIndices;
    dIndices.index(+0.0f);
    int count = dIndices.size();
    EXPECT_EQ(1, count);
    dIndices.index(-0.0f);
    count = dIndices.size();
    EXPECT_EQ(2, count);
  }
}
