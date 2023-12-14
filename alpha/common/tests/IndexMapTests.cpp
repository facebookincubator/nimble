// Copyright 2004-present Facebook. All Rights Reserved.

#include <gtest/gtest.h>
#include "dwio/alpha/common/IndexMap.h"

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
    alpha::IndexMap<double> dIndices;
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
    alpha::IndexMap<float> dIndices;
    dIndices.index(+0.0f);
    int count = dIndices.size();
    EXPECT_EQ(1, count);
    dIndices.index(-0.0f);
    count = dIndices.size();
    EXPECT_EQ(2, count);
  }
}
