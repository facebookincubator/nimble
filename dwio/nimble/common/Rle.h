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
#pragma once

#include <span>
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
