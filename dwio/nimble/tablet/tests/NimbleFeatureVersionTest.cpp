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

#include "dwio/nimble/tablet/NimbleFeatureVersion.h"
#include <gtest/gtest.h>

namespace facebook::nimble {
namespace {

TEST(NimbleFeatureVersionTest, VersionComparison) {
  // Equal versions.
  EXPECT_EQ((NimbleVersion{0, 1}), (NimbleVersion{0, 1}));
  EXPECT_EQ((NimbleVersion{1, 0}), (NimbleVersion{1, 0}));

  // Less-than: minor version difference.
  EXPECT_LT((NimbleVersion{0, 1}), (NimbleVersion{0, 2}));
  EXPECT_LT((NimbleVersion{0, 0}), (NimbleVersion{0, 1}));

  // Less-than: major version takes precedence.
  EXPECT_LT((NimbleVersion{0, 99}), (NimbleVersion{1, 0}));
  EXPECT_LT((NimbleVersion{0, 2}), (NimbleVersion{1, 0}));

  // Greater-than.
  EXPECT_GT((NimbleVersion{0, 2}), (NimbleVersion{0, 1}));
  EXPECT_GT((NimbleVersion{1, 0}), (NimbleVersion{0, 99}));

  // Less-than-or-equal.
  EXPECT_LE((NimbleVersion{0, 1}), (NimbleVersion{0, 1}));
  EXPECT_LE((NimbleVersion{0, 1}), (NimbleVersion{0, 2}));

  // Greater-than-or-equal.
  EXPECT_GE((NimbleVersion{0, 2}), (NimbleVersion{0, 2}));
  EXPECT_GE((NimbleVersion{0, 2}), (NimbleVersion{0, 1}));

  // Not-equal.
  EXPECT_NE((NimbleVersion{0, 1}), (NimbleVersion{0, 2}));
  EXPECT_NE((NimbleVersion{0, 1}), (NimbleVersion{1, 1}));
}

TEST(NimbleFeatureVersionTest, HasVarintRowCount) {
  // Baseline version (0, 1) does not support varint row count.
  EXPECT_FALSE(hasVarintRowCount(0, 1));
  EXPECT_FALSE(hasVarintRowCount(0, 0));

  // Version (0, 2) and above support varint row count.
  EXPECT_TRUE(hasVarintRowCount(0, 2));
  EXPECT_TRUE(hasVarintRowCount(0, 3));

  // Major version bump also implies support.
  EXPECT_TRUE(hasVarintRowCount(1, 0));
  EXPECT_TRUE(hasVarintRowCount(1, 1));
}

TEST(NimbleFeatureVersionTest, FeatureVersionRange) {
  // Active range: [introduced, deprecated).
  constexpr NimbleFeatureVersionRange range{{0, 2}, {1, 0}};
  EXPECT_FALSE(range.isActive(NimbleVersion{0, 1}));
  EXPECT_TRUE(range.isActive(NimbleVersion{0, 2}));
  EXPECT_TRUE(range.isActive(NimbleVersion{0, 99}));
  // Deprecated at (1, 0).
  EXPECT_FALSE(range.isActive(NimbleVersion{1, 0}));
  EXPECT_FALSE(range.isActive(NimbleVersion{1, 1}));

  // Range with kMaxVersion means never deprecated.
  constexpr NimbleFeatureVersionRange openRange{{0, 2}, kMaxVersion};
  EXPECT_FALSE(openRange.isActive(NimbleVersion{0, 1}));
  EXPECT_TRUE(openRange.isActive(NimbleVersion{0, 2}));
  EXPECT_TRUE(openRange.isActive(NimbleVersion{1, 0}));
  EXPECT_TRUE(openRange.isActive(NimbleVersion{100, 0}));
}

TEST(NimbleFeatureVersionTest, ConstexprEvaluation) {
  // Verify that version operations work at compile time.
  static_assert(NimbleVersion{0, 1} < NimbleVersion{0, 2});
  static_assert(NimbleVersion{0, 2} == NimbleVersion{0, 2});
  static_assert(NimbleVersion{0, 99} < NimbleVersion{1, 0});
  static_assert(hasVarintRowCount(0, 2));
  static_assert(!hasVarintRowCount(0, 1));
  static_assert(kVersionRangeVarintRowCount.introduced == NimbleVersion{0, 2});
  static_assert(kVersionRangeVarintRowCount.deprecated == kMaxVersion);
}

} // namespace
} // namespace facebook::nimble
