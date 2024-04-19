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
#include "dwio/nimble/common/StopWatch.h"
#include "common/time/ClockGettimeNS.h"

namespace facebook::nimble {

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

} // namespace facebook::nimble
