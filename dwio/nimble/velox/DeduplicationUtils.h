// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once
#include "velox/common/base/CompareFlags.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::nimble {

class DeduplicationUtils {
 public:
  // Compares elements of two maps at the given index.
  static bool CompareMapsAtIndex(
      const velox::MapVector& leftMap,
      velox::vector_size_t leftIdx,
      const velox::MapVector& rightMap,
      velox::vector_size_t rightIdx);
};

} // namespace facebook::nimble
