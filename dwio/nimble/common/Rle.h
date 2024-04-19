// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <span>
#include "dwio/nimble/common/NimbleCompare.h"
#include "dwio/nimble/common/Vector.h"

#include <type_traits>

// Functions related to run length encoding.

namespace facebook::nimble::rle {

// TODO: This utility is only used by RLEEncoding. Consider moving it there.

template <typename T>
void computeRuns(
    std::span<const T> data,
    Vector<uint32_t>* runLengths,
    Vector<T>* runValues) {
  static_assert(!std::is_floating_point_v<T>);
  if (data.empty()) {
    return;
  }
  uint32_t runLength = 1;
  T last = data[0];
  for (int i = 1; i < data.size(); ++i) {
    if (data[i] == last) {
      ++runLength;
    } else {
      runLengths->push_back(runLength);
      runValues->push_back(last);
      last = data[i];
      runLength = 1;
    }
  }
  runLengths->push_back(runLength);
  runValues->push_back(last);
}

} // namespace facebook::nimble::rle
