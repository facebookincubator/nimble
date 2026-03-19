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

#include "dwio/nimble/velox/RowRange.h"

#include <gtest/gtest.h>

namespace facebook::nimble::test {

TEST(RowRangeTest, basic) {
  RowRange range(10, 20);
  EXPECT_EQ(range.startRow, 10);
  EXPECT_EQ(range.endRow, 20);
  EXPECT_EQ(range.numRows(), 10);
  EXPECT_FALSE(range.empty());
}

TEST(RowRangeTest, empty) {
  RowRange defaultRange;
  EXPECT_TRUE(defaultRange.empty());
  EXPECT_EQ(defaultRange.numRows(), 0);

  RowRange sameRange(5, 5);
  EXPECT_TRUE(sameRange.empty());

  RowRange invertedRange(10, 5);
  EXPECT_TRUE(invertedRange.empty());
}

TEST(RowRangeTest, contains) {
  RowRange outer(10, 50);

  EXPECT_TRUE(outer.contains(RowRange(10, 50)));
  EXPECT_TRUE(outer.contains(RowRange(10, 20)));
  EXPECT_TRUE(outer.contains(RowRange(30, 50)));
  EXPECT_TRUE(outer.contains(RowRange(20, 30)));

  EXPECT_FALSE(outer.contains(RowRange(5, 20)));
  EXPECT_FALSE(outer.contains(RowRange(30, 60)));
  EXPECT_FALSE(outer.contains(RowRange(5, 60)));
}

TEST(RowRangeTest, equality) {
  EXPECT_EQ(RowRange(10, 20), RowRange(10, 20));
  EXPECT_NE(RowRange(10, 20), RowRange(10, 30));
  EXPECT_NE(RowRange(10, 20), RowRange(5, 20));
}

TEST(RowRangeTest, toString) {
  EXPECT_EQ(RowRange(10, 20).toString(), "[10, 20)");
  EXPECT_EQ(RowRange(0, 0).toString(), "[0, 0)");
}

TEST(RowRangeTest, hash) {
  RowRangeHash hasher;
  EXPECT_EQ(hasher(RowRange(10, 20)), hasher(RowRange(10, 20)));
  EXPECT_NE(hasher(RowRange(10, 20)), hasher(RowRange(10, 30)));
}

} // namespace facebook::nimble::test
