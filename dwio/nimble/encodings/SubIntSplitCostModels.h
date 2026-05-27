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
#pragma once

#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>

#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/SubIntSplitMetrics.h"

// Per-segment cost models for SubIntSplitEncoding's DP selector.
//
// Each function estimates the compressed size in bits for encoding numValues
// items from a bit-range sub-stream of logical width bitWidth. The models
// correspond to nimble's present encodings and are derived from the same
// assumptions used in EncodingSizeEstimation.h (common prefix 6 bytes, nested
// encoding overhead, etc.).
//
// Deliberately avoids HLL cardinality estimation, entropy, and frame-residual
// tracking — the simplified SegmentMetrics provides enough signal for the DP
// to make directionally correct split decisions.

namespace facebook::nimble::detail::subintsplit {

// Smallest storage width in bits for a logical value of `bw` bits.
// Matches nimble's physical type selection: uint8/16/32/64.
inline constexpr uint8_t storageWidthBits(int bw) noexcept {
  if (bw <= 8) {
    return 8;
  }
  if (bw <= 16) {
    return 16;
  }
  if (bw <= 32) {
    return 32;
  }
  return 64;
}

// Union of MetricFlags needed across all cost models below.
inline MetricFlags allCostModelRequiredFlags() noexcept {
  return MetricFlag::MinMax | MetricFlag::RunStats | MetricFlag::UniqueCount;
}

// Trivial: store each value at its native storage width.
inline double trivialCostBits(
    const SegmentMetrics& /*m*/,
    size_t numValues,
    int bitWidth) noexcept {
  constexpr double kHeaderBits = 7.0 * 8.0; // prefix(6) + compressionType(1)
  return kHeaderBits +
      static_cast<double>(numValues) *
      static_cast<double>(storageWidthBits(bitWidth));
}

// FixedBitWidth: bit-pack using observed range, rounded to byte boundary.
// Required: MinMax
inline double fixedBitWidthCostBits(
    const SegmentMetrics& m,
    size_t numValues,
    int bitWidth) noexcept {
  if (numValues == 0) {
    return 0.0;
  }
  // prefix(6) + compressionType(1) + baseline(storageBytes) + bitWidth(1)
  const double baselineBytes =
      static_cast<double>(storageWidthBits(bitWidth)) / 8.0;
  const double headerBits = (7.0 + baselineBytes + 1.0) * 8.0;

  const uint8_t rangeWidth =
      m.range == 0 ? uint8_t{0} : static_cast<uint8_t>(std::bit_width(m.range));
  const uint8_t packedBits =
      std::min<uint8_t>(static_cast<uint8_t>(bitWidth), rangeWidth);
  // Round up to byte boundary (matches nimble's current FixedBitWidth impl).
  const uint8_t roundedBits = (packedBits + 7u) & ~7u;

  return headerBits +
      static_cast<double>(roundedBits) * static_cast<double>(numValues);
}

// Constant: zero cost when all values are equal, infinity otherwise.
// Required: MinMax
inline double constantCostBits(
    const SegmentMetrics& m,
    size_t numValues,
    int bitWidth) noexcept {
  if (numValues == 0) {
    return 0.0;
  }
  if (m.min != m.max) {
    return std::numeric_limits<double>::infinity();
  }
  // prefix(6) + stored value
  const double headerBits =
      (6.0 + static_cast<double>(storageWidthBits(bitWidth)) / 8.0) * 8.0;
  return headerBits;
}

// Dictionary: unique value table + bit-packed indices.
// Uses observed uniqueCount directly (no HLL blending). Directionally correct
// for the DP's purposes.
// Required: UniqueCount, MinMax
inline double dictionaryCostBits(
    const SegmentMetrics& m,
    size_t numValues,
    int bitWidth) noexcept {
  if (m.uniqueCount == 0 || numValues == 0) {
    return 0.0;
  }
  const size_t uniques = m.uniqueCount;
  const uint8_t valueBits = storageWidthBits(bitWidth);
  const double dictBits =
      static_cast<double>(uniques) * static_cast<double>(valueBits);
  // Index width: ceil(log2(uniques)), clamped to 32
  const uint32_t indexWidth = (uniques <= 1)
      ? 1u
      : std::min(32u, static_cast<uint32_t>(std::bit_width(uniques - 1)));
  // Bit-packed indices rounded up to byte boundary
  const double roundedIndexBits = static_cast<double>((indexWidth + 7u) & ~7u);
  const double indexBits = roundedIndexBits * static_cast<double>(numValues);
  // prefix(6) + alphabetSize(4) + nested header overhead (~15 bytes)
  const double headerBits = (6.0 + 4.0 + 15.0) * 8.0;

  // Penalise when index width ≥ value width (dictionary doesn't compress).
  const double penalty = (indexWidth >= valueBits)
      ? 1.0 + 0.15 * (static_cast<double>(indexWidth) / valueBits - 1.0)
      : 1.0;

  return headerBits + penalty * (dictBits + indexBits);
}

// RLE: run values + bit-packed run lengths.
// Required: RunStats, MinMax
inline double
rleCostBits(const SegmentMetrics& m, size_t numValues, int bitWidth) noexcept {
  if (numValues == 0 || m.avgRunLength <= 0.0) {
    return 0.0;
  }
  // avgRunLength is scale-invariant, so extrapolate run count to full stream.
  const double estimatedRuns = static_cast<double>(numValues) / m.avgRunLength;
  // prefix(6) + runLengthsSize(4) + nested encoding overhead (~16 bytes)
  const double headerBits = (6.0 + 4.0 + 16.0) * 8.0;
  const double runValuesBits =
      estimatedRuns * static_cast<double>(storageWidthBits(bitWidth));
  // Assume 16-bit run lengths (conservative)
  const double runLengthsBits = estimatedRuns * 16.0;
  return headerBits + runValuesBits + runLengthsBits;
}

// Varint: variable-length integer storage. Only useful for ≥32-bit sections.
// Required: MinMax (max value determines byte width)
inline double varintCostBits(
    const SegmentMetrics& m,
    size_t numValues,
    int bitWidth) noexcept {
  if (numValues == 0 || bitWidth < 32) {
    return std::numeric_limits<double>::infinity();
  }
  const uint64_t maxVal = m.max;
  uint8_t varintBytes;
  if (maxVal < (1ULL << 7)) {
    varintBytes = 1;
  } else if (maxVal < (1ULL << 14)) {
    varintBytes = 2;
  } else if (maxVal < (1ULL << 21)) {
    varintBytes = 3;
  } else if (maxVal < (1ULL << 28)) {
    varintBytes = 4;
  } else if (maxVal < (1ULL << 35)) {
    varintBytes = 5;
  } else if (maxVal < (1ULL << 42)) {
    varintBytes = 6;
  } else if (maxVal < (1ULL << 49)) {
    varintBytes = 7;
  } else if (maxVal < (1ULL << 56)) {
    varintBytes = 8;
  } else {
    varintBytes = 9;
  }
  // prefix(6) + baseline
  const double headerBits =
      (6.0 + static_cast<double>(storageWidthBits(bitWidth)) / 8.0) * 8.0;
  return headerBits +
      static_cast<double>(varintBytes) * 8.0 * static_cast<double>(numValues);
}

// Evaluate all cost models and return the minimum cost in bits.
// Also sets `bestEncoding` to the winning EncodingType.
inline double bestCostBits(
    const SegmentMetrics& m,
    size_t numValues,
    int bitWidth,
    EncodingType& bestEncoding) noexcept {
  double best = std::numeric_limits<double>::infinity();
  auto consider = [&](double cost, EncodingType type) noexcept {
    if (cost < best) {
      best = cost;
      bestEncoding = type;
    }
  };

  consider(trivialCostBits(m, numValues, bitWidth), EncodingType::Trivial);
  consider(
      fixedBitWidthCostBits(m, numValues, bitWidth),
      EncodingType::FixedBitWidth);
  consider(constantCostBits(m, numValues, bitWidth), EncodingType::Constant);
  consider(rleCostBits(m, numValues, bitWidth), EncodingType::RLE);
  consider(varintCostBits(m, numValues, bitWidth), EncodingType::Varint);
  // Dictionary only when cardinality << numValues
  if (m.uniqueCount > 0 &&
      (m.uniqueCountCapped || m.uniqueCount < numValues / 2)) {
    consider(
        dictionaryCostBits(m, numValues, bitWidth), EncodingType::Dictionary);
  }
  return best;
}

} // namespace facebook::nimble::detail::subintsplit
