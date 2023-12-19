// Copyright 2004-present Facebook. All Rights Reserved.

#include "dwio/alpha/common/StopWatch.h"
#include "common/time/ClockGettimeNS.h"

namespace facebook::alpha {

void StopWatch::start() {
  if (!running_) {
    startNs_ = fb_clock_gettime_ns(CLOCK_MONOTONIC);
    running_ = true;
  }
}

void StopWatch::stop() {
  if (running_) {
    elapsedNs_ += fb_clock_gettime_ns(CLOCK_MONOTONIC) - startNs_;
    running_ = false;
  }
}

void StopWatch::reset() {
  running_ = false;
  elapsedNs_ = 0;
}

double StopWatch::elapsed() {
  return static_cast<double>(elapsedNsec()) / (1000 * 1000 * 1000);
}

int64_t StopWatch::elapsedNsec() {
  if (running_) {
    stop();
    start();
  }
  return elapsedNs_;
}

int64_t StopWatch::elapsedUsec() {
  return elapsedNsec() / 1000;
}

int64_t StopWatch::elapsedMsec() {
  return elapsedNsec() / (1000 * 1000);
}

} // namespace facebook::alpha
