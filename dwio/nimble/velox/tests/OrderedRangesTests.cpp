// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <numeric>
#include "dwio/nimble/velox/OrderedRanges.h"

namespace facebook::nimble::tests {
namespace {

using namespace testing;
using range_helper::OrderedRanges;

template <typename T>
std::vector<std::tuple<int32_t, int32_t>> collectRange(const T& t) {
  std::vector<std::tuple<int32_t, int32_t>> ret;
  t.apply([&](auto begin, auto size) { ret.emplace_back(begin, size); });
  return ret;
}

template <typename T>
std::vector<int32_t> collectEach(const T& t) {
  std::vector<int32_t> ret;
  t.applyEach([&](auto val) { ret.emplace_back(val); });
  return ret;
}

TEST(OrderedRanges, apply) {
  OrderedRanges ranges;
  ranges.add(0, 1);
  ranges.add(1, 1);
  ranges.add(100, 1);
  ranges.add(50, 50);
  EXPECT_THAT(
      collectRange(ranges),
      ElementsAre(
          std::make_tuple(0, 2),
          std::make_tuple(100, 1),
          std::make_tuple(50, 50)));
}

TEST(OrderedRanges, applyEach) {
  OrderedRanges ranges;
  ranges.add(0, 5);
  ranges.add(5, 30);
  ranges.add(35, 15);

  std::vector<int32_t> expected(50);
  std::iota(expected.begin(), expected.end(), 0);
  EXPECT_THAT(collectEach(ranges), ElementsAreArray(expected));
}

} // namespace
} // namespace facebook::nimble::tests
