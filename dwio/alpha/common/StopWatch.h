// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>

// A simple stopwatch measuring to the nanosecond level.
// Useful for timing code while prototyping. The accuracy should be 'good',
// but its not clear what that actually means, and in any case it will depend
// on the underlying details of the system.

namespace facebook::alpha {

class StopWatch {
 public:
  StopWatch() = default;

  // Start timing. A no-op if we're already timing.
  void start();

  // Stop timing. A no-op if we aren't currently timing.
  void stop();

  // Restore *this to its newly constructed state.
  void reset();

  // Reset and then Start in one.
  void restart() {
    reset();
    start();
  }

  // Returns the elapsed time in seconds without rounding. Does not stop
  // the stopwatch if it is running, but does reduce the accuracy a bit
  // so don't call the Elapsed* functions unnecessarily.
  double elapsed();

  // Returns the elapsed time on the stopwatch in the appropriate units,
  // rounding down. Does not stop the stopwatch if it is running.
  int64_t elapsedNsec();
  int64_t elapsedUsec();
  int64_t elapsedMsec();

 private:
  int64_t startNs_;
  int64_t elapsedNs_ = 0;
  bool running_ = false;
};

} // namespace facebook::alpha
