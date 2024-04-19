// Copyright 2004-present Facebook. All Rights Reserved.

#include <gtest/gtest.h>
#include "dwio/nimble/common/StopWatch.h"
#include "folly/Benchmark.h"

using namespace ::facebook;

namespace {
void WasteSomeTime() {
  int tot = 0;
  for (int i = 0; i < 100; ++i) {
    tot += i;
    folly::doNotOptimizeAway(tot);
  }
}
} // namespace

TEST(StopWatchTests, BasicCorrectness) {
  nimble::StopWatch watch;
  watch.start();
  WasteSomeTime();
  watch.stop();

  ASSERT_GE(watch.elapsedNsec(), 0LL);
  ASSERT_GE(watch.elapsed(), 0.0);

  watch.reset();
  ASSERT_EQ(watch.elapsedNsec(), 0);
  ASSERT_EQ(watch.elapsed(), 0.0);

  watch.start();
  WasteSomeTime();
  watch.stop();
  int64_t elapsed1 = watch.elapsedNsec();
  ASSERT_GT(elapsed1, 0LL);
  watch.start();
  WasteSomeTime();
  int64_t elapsed2 = watch.elapsedNsec();
  ASSERT_GT(elapsed2, elapsed1);
  watch.stop();
  int64_t elapsed3 = watch.elapsedNsec();
  WasteSomeTime();
  int64_t elapsed4 = watch.elapsedNsec();
  ASSERT_EQ(elapsed3, elapsed4);

  ASSERT_EQ(elapsed4 / 1000, watch.elapsedUsec());
  ASSERT_EQ(elapsed4 / (1000 * 1000), watch.elapsedMsec());

  watch.reset();
  watch.start();
  watch.start();
  WasteSomeTime();
  watch.stop();
  watch.stop();
  ASSERT_GT(watch.elapsedNsec(), 0LL);
}
