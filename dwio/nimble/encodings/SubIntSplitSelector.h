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

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/SubIntSplitCostModels.h"
#include "dwio/nimble/encodings/SubIntSplitMetrics.h"

// DP-based bit-range split selector for SubIntSplitEncoding.
// Evaluates a grid of bit ranges [l..r] on a sample of uint64_t values,
// runs dynamic programming over bit positions 0..kBits to find the minimum-cost
// partition, and returns a list of SegmentPlan entries.

namespace facebook::nimble::detail::subintsplit {

struct SegmentPlan {
  int bitStart{0};
  int bitEnd{0};
  EncodingType encoding{EncodingType::Trivial};
  double cost{0.0}; // estimated total bits for the full stream
};

struct SelectorConfig {
  int minSegmentWidth{1};
  double splitPenalty{10.0}; // extra bits charged per additional split boundary
};

inline SelectorConfig defaultSelectorConfig() noexcept {
  return SelectorConfig{.minSegmentWidth = 1, .splitPenalty = 10.0};
}

// Incremental bit-range value extractor.
// Builds values[i] = bits [bitStart..bitEnd] of sample[i], extending one bit
// at a time to reuse work across the inner loop of the segment-evaluation grid.
class BitRangeExtractor {
 public:
  explicit BitRangeExtractor(const std::vector<uint64_t>& samples)
      : samples_(samples),
        values_(samples.size(), uint64_t{0}),
        bitStart_(-1),
        bitEnd_(-1) {}

  void reset(int bitStart) {
    bitStart_ = bitStart;
    bitEnd_ = bitStart;
    const size_t n = samples_.size();
    for (size_t i = 0; i < n; ++i) {
      values_[i] = (samples_[i] >> bitStart_) & uint64_t{1};
    }
  }

  void extend(int bitEnd) {
    if (bitEnd <= bitEnd_) {
      return;
    }
    const size_t n = samples_.size();
    for (int b = bitEnd_ + 1; b <= bitEnd; ++b) {
      const int shift = b - bitStart_;
      const uint64_t maskShift = uint64_t{1} << shift;
      for (size_t i = 0; i < n; ++i) {
        const uint64_t bit = (samples_[i] >> b) & uint64_t{1};
        values_[i] |= bit * maskShift;
      }
    }
    bitEnd_ = bitEnd;
  }

  const std::vector<uint64_t>& values() const noexcept {
    return values_;
  }

 private:
  const std::vector<uint64_t>& samples_;
  std::vector<uint64_t> values_;
  int bitStart_;
  int bitEnd_;
};

struct SelectorResult {
  std::vector<SegmentPlan> segments;
  double totalCost{0.0};
};

// One cell of the cost grid: the cheapest encoding for a candidate section and
// its estimated full-stream cost in bits. Only lower-triangular (r >= l) cells
// within the active range are populated.
struct SegmentChoice {
  double cost{std::numeric_limits<double>::infinity()};
  EncodingType encoding{EncodingType::Trivial};
};

// A constant bit-plane run stored as a single Constant section (costs
// ~nothing).
inline SegmentPlan makeConstantSegment(int bitStart, int bitEnd) {
  return {
      .bitStart = bitStart,
      .bitEnd = bitEnd,
      .encoding = EncodingType::Constant,
      .cost = 0.0};
}

// The contiguous range of bit positions that actually vary across the sample.
// `allConstant()` means every sampled value is identical (no varying bits).
struct ActiveBitRange {
  int lo{0};
  int hi{-1};

  bool allConstant() const noexcept {
    return hi < lo;
  }

  int width() const noexcept {
    return hi - lo + 1;
  }
};

// Bit-plane pre-pass: find the lowest and highest bit that is not identical
// across every sample. Bits outside [lo, hi] -- a constant high prefix and/or
// low suffix, the common case for narrow, low-cardinality and bit-structured
// data -- carry no information and become free Constant sections, so the
// O(width^2) DP only has to run over the active range.
inline ActiveBitRange findActiveBitRange(
    const std::vector<uint64_t>& samples,
    int kBits) {
  uint64_t orAll = 0;
  uint64_t andAll = ~uint64_t{0};
  for (const uint64_t s : samples) {
    orAll |= s;
    andAll &= s;
  }
  const uint64_t bitsMask =
      (kBits >= 64) ? ~uint64_t{0} : ((uint64_t{1} << kBits) - 1);
  const uint64_t varying = (orAll & ~andAll) & bitsMask;
  if (varying == 0) {
    return {}; // allConstant()
  }
  return {
      .lo = std::countr_zero(varying), .hi = 63 - std::countl_zero(varying)};
}

// Score every candidate section [l, r] within the active range with the
// cheapest applicable encoding, scaling per-sample costs up to the full stream.
// Returns a flat sz-by-sz grid indexed grid[l * sz + r]; only active cells are
// written.
inline std::vector<SegmentChoice> buildActiveCostGrid(
    const std::vector<uint64_t>& samples,
    int sz,
    const ActiveBitRange& active,
    size_t fullCount) {
  const MetricFlags requiredFlags = allCostModelRequiredFlags();
  MetricCollector collector;
  BitRangeExtractor extractor(samples);
  const size_t numSamples = samples.size();

  std::vector<SegmentChoice> grid(static_cast<size_t>(sz) * sz);
  for (int l = active.lo; l <= active.hi; ++l) {
    extractor.reset(l);
    for (int r = l; r <= active.hi; ++r) {
      extractor.extend(r);
      const SegmentMetrics metrics =
          collector.compute(extractor.values(), requiredFlags);

      EncodingType bestEnc = EncodingType::Trivial;
      const double perSampleCost =
          bestCostBits(metrics, numSamples, r - l + 1, bestEnc);
      const double fullCost = perSampleCost * static_cast<double>(fullCount) /
          static_cast<double>(numSamples);

      grid[l * sz + r] = {fullCost, bestEnc};
    }
  }
  return grid;
}

// Dynamic program over the active range using the precomputed cost grid, then
// backtrack into the chosen segments (absolute bit ranges). `dp[p]` is the min
// cost to cover active bits [active.lo, active.lo + p); a step (j..i) maps to
// absolute bits [active.lo + j, active.lo + i - 1]. Sets `totalCost`; on a
// degenerate (non-finite) cover, falls back to one Trivial segment spanning the
// whole active range.
inline std::vector<SegmentPlan> solveActiveDp(
    const std::vector<SegmentChoice>& grid,
    int sz,
    const ActiveBitRange& active,
    const SelectorConfig& cfg,
    double& totalCost) {
  const int activeWidth = active.width();
  std::vector<double> dp(
      activeWidth + 1, std::numeric_limits<double>::infinity());
  std::vector<int> prev(activeWidth + 1, -1);
  std::vector<EncodingType> chosen(activeWidth + 1, EncodingType::Trivial);
  dp[0] = 0.0;

  for (int i = 1; i <= activeWidth; ++i) {
    for (int j = 0; j < i; ++j) {
      if (i - j < cfg.minSegmentWidth) {
        continue;
      }
      const auto& choice = grid[(active.lo + j) * sz + (active.lo + i - 1)];
      if (!std::isfinite(choice.cost)) {
        continue;
      }
      const double splitCost = (j == 0) ? 0.0 : cfg.splitPenalty;
      const double candidate = dp[j] + choice.cost + splitCost;
      if (candidate < dp[i]) {
        dp[i] = candidate;
        prev[i] = j;
        chosen[i] = choice.encoding;
      }
    }
  }

  totalCost = dp[activeWidth];
  if (!std::isfinite(totalCost)) {
    const double cost = grid[active.lo * sz + active.hi].cost;
    totalCost = cost;
    return {
        {.bitStart = active.lo,
         .bitEnd = active.hi,
         .encoding = EncodingType::Trivial,
         .cost = cost}};
  }

  std::vector<SegmentPlan> segments;
  for (int idx = activeWidth; idx > 0;) {
    const int start = prev[idx];
    if (start < 0) {
      break;
    }
    const int bl = active.lo + start;
    const int br = active.lo + idx - 1;
    segments.push_back(
        {.bitStart = bl,
         .bitEnd = br,
         .encoding = chosen[idx],
         .cost = grid[bl * sz + br].cost});
    idx = start;
  }
  std::reverse(segments.begin(), segments.end());
  return segments;
}

// Run the DP split selector on `samples` (uint64_t values drawn from a
// physical-type stream of `kBits` width).
//
// `fullCount` is the total element count of the *full* stream; cost model
// scores are scaled from the sample size to the full stream so the DP
// produces estimates in the right units.
inline SelectorResult selectSplits(
    const std::vector<uint64_t>& samples,
    int kBits, // number of bits in the physical type (32 or 64)
    size_t fullCount,
    const SelectorConfig& cfg = defaultSelectorConfig()) {
  if (samples.empty() || kBits <= 0) {
    return {};
  }
  const int sz = std::min(kBits, 64);

  const ActiveBitRange active = findActiveBitRange(samples, sz);
  if (active.allConstant()) {
    // Every sampled value is identical: one Constant section covers everything.
    SelectorResult result;
    result.segments.push_back(makeConstantSegment(0, sz - 1));
    return result;
  }

  const std::vector<SegmentChoice> grid =
      buildActiveCostGrid(samples, sz, active, fullCount);

  SelectorResult result;
  std::vector<SegmentPlan> activeSegments =
      solveActiveDp(grid, sz, active, cfg, result.totalCost);

  // Stitch the layout together in ascending bit order: constant low bits, the
  // active segments, then constant high bits.
  if (active.lo > 0) {
    result.segments.push_back(makeConstantSegment(0, active.lo - 1));
  }
  for (const auto& seg : activeSegments) {
    result.segments.push_back(seg);
  }
  if (active.hi < sz - 1) {
    result.segments.push_back(makeConstantSegment(active.hi + 1, sz - 1));
  }
  return result;
}

} // namespace facebook::nimble::detail::subintsplit
