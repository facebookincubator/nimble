// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/nimble/velox/DeduplicationUtils.h"

namespace facebook::nimble {

bool DeduplicationUtils::CompareMapsAtIndex(
    const velox::MapVector& leftMap,
    velox::vector_size_t leftIdx,
    const velox::MapVector& rightMap,
    velox::vector_size_t rightIdx) {
  // Compare with Null is not defined, return false
  if (leftMap.isNullAt(leftIdx) || rightMap.isNullAt(rightIdx)) {
    return false;
  }

  if (leftMap.sizeAt(leftIdx) != rightMap.sizeAt(rightIdx)) {
    return false;
  }

  auto leftSortedKeys = leftMap.sortedKeyIndices(leftIdx);
  auto rightSortedKeys = rightMap.sortedKeyIndices(rightIdx);

  const auto& leftKeys = leftMap.mapKeys();
  const auto& leftVals = leftMap.mapValues();
  const auto& rightKeys = rightMap.mapKeys();
  const auto& rightVals = rightMap.mapValues();

  for (velox::vector_size_t i = 0; i < leftSortedKeys.size(); ++i) {
    if (!(leftKeys->equalValueAt(
              rightKeys.get(), leftSortedKeys[i], rightSortedKeys[i]) &&
          leftVals->equalValueAt(
              rightVals.get(), leftSortedKeys[i], rightSortedKeys[i]))) {
      return false;
    }
  }
  return true;
}

} // namespace facebook::nimble
