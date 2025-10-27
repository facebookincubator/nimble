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
#ifdef __x86_64__
#include <immintrin.h>
#endif //__x86_64__

#ifdef __aarch64__
#include "common/aarch64/compat.h"
#endif //__aarch64__

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Varint.h"
#include "folly/CpuId.h"

namespace facebook::nimble::varint {

const char* bulkVarintSkip(uint64_t n, const char* pos) {
  const uint64_t* word = reinterpret_cast<const uint64_t*>(pos);
  while (n >= 8) {
    // Zeros in the 8 * ith bits indicate termination of a varint.
    n -= __builtin_popcountll(~(*word++) & 0x8080808080808080ULL);
  }
  pos = reinterpret_cast<const char*>(word);
  while (n--) {
    skipVarint(&pos);
  }
  return pos;
}

uint64_t bulkVarintSize32(std::span<const uint32_t> values) {
  constexpr uint8_t kLookupSizeTable32[32] = {5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4,
                                              3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2,
                                              2, 2, 2, 1, 1, 1, 1, 1, 1, 1};
  uint64_t size = 0;
  for (uint32_t value : values) {
    size += kLookupSizeTable32[__builtin_clz(value | 1U)];
  }
  return size;
}

uint64_t bulkVarintSize64(std::span<const uint64_t> values) {
  constexpr uint8_t kLookupSizeTable64[64] = {
      10, 9, 9, 9, 9, 9, 9, 9, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7,
      6,  6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3,
      3,  3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1};
  uint64_t size = 0;
  for (uint64_t value : values) {
    size += kLookupSizeTable64[__builtin_clzll(value | 1ULL)];
  }
  return size;
}

// Declaration of the function we build via generated code below.
template <typename T>
__attribute__((__target__("bmi2")))
// __attribute__ ((optimize("Os")))
const char* bulkVarintDecodeBmi2(uint64_t n, const char* pos, T* output);

const char* bulkVarintDecode32(uint64_t n, const char* pos, uint32_t* output) {
  static bool hasBmi2 = folly::CpuId().bmi2();
  if (hasBmi2) {
    return bulkVarintDecodeBmi2(n, pos, output);
  }
  for (uint64_t i = 0; i < n; ++i) {
    *output++ = readVarint32(&pos);
  }
  return pos;
}

const char* bulkVarintDecode64(uint64_t n, const char* pos, uint64_t* output) {
  static bool hasBmi2 = folly::CpuId().bmi2();
  if (hasBmi2) {
    return bulkVarintDecodeBmi2(n, pos, output);
  }
  for (uint64_t i = 0; i < n; ++i) {
    *output++ = readVarint64(&pos);
  }
  return pos;
}

// Codegen for the cases below. Useful if we want to try to tweak something
// (different mask length, etc) in the future.

// std::string codegenVarintMask(int endByte, int len) {
//   CHECK_GE(endByte + 1,  len);
//   std::string s = "0x0000000000000000ULL";
//   int offset = 5 + endByte * 2;
//   for (int i = 0; i < len; ++i) {
//     *(s.end() - offset) = '7';
//     *(s.end() - offset + 1) = 'f';
//     offset -= 2;
//   }
//   return s;
// }

// std::string codegen32(uint64_t controlBits, int maskLength) {
//   CHECK(controlBits < (1 << maskLength));
//   std::string s = absl::Substitute(
//       "      case $0ULL: {", controlBits);
//   int lastZero = -1;
//   int numVariants = 0;
//   bool carryoverUsed = false;
//   for (int nextBit = 0; nextBit < maskLength; ++nextBit) {
//     // A zero control bit means we detected the end of a varint, so
//     // we can construct a mask of the bottom 7 bits starting at the end
//     // of the nextBit byte and going back (nextBit - lastZero) bytes.
//     if ((controlBits & (1ULL << nextBit)) == 0) {
//       if (carryoverUsed) {
//         s += absl::Substitute("\n        *output++ = _pext_u64(word, $0);",
//                               CodegenVarintMask(nextBit, nextBit -
//                               lastZero));
//       } else {
//         s += absl::Substitute("\n        const uint64_t firstValue = "
//                               "_pext_u64(word, $0);",
//                               CodegenVarintMask(nextBit, nextBit -
//                               lastZero));
//         s += "\n        *output++ = (firstValue << carryoverBits) |
//         carryover;"; carryoverUsed = true;
//       }
//       lastZero = nextBit;
//       ++numVariants;
//     }
//   }
//   // Ending on a complete varint, not completing any varint, and completing
//   // at least 1 varint but no ending on one are all distinct cases.
//   if (lastZero == -1) {
//     s += absl::Substitute("\n        carryover |= "
//                           "_pext_u64(word, $0) << carryoverBits;",
//                           CodegenVarintMask(maskLength - 1, maskLength));
//     s += absl::Substitute("\n        carryoverBits += $0;", 7 * maskLength);
//   } else if (lastZero == maskLength - 1) {
//     s += "\n        carryover = 0ULL;";
//     s += "\n        carryoverBits = 0;";
//     s += absl::Substitute("\n        n -= $0;", numVariants);
//   } else {
//     s += absl::Substitute("\n        carryover = _pext_u64(word, $0);",
//                           CodegenVarintMask(maskLength - 1,
//                                             maskLength - 1 - lastZero));
//     s += absl::Substitute("\n        carryoverBits = $0;",
//                           7 * (maskLength - 1 - lastZero));
//     s += absl::Substitute("\n        n -= $0;", numVariants);
//   }
//   // s += absl::Substitute("\n        pos += $0;", maskLength);
//   s += "\n        continue;";
//   s += "\n      }";
//   return s;
// }

template <typename T>
const char* bulkVarintDecodeBmi2(uint64_t n, const char* pos, T* output) {
  constexpr uint64_t mask = 0x0000808080808080;
  // Note that we could of course use a maskLength of up to 8. But I found
  // that with maskLength > 6 we start to spill out of the l1i cache in
  // opt mode and that counterbalances the gain. Plus the first run and/or
  // small n are more expensive as we have to load more instructions.
  constexpr int maskLength = 6;
  uint64_t carryover = 0;
  int carryoverBits = 0;
  pos -= maskLength;
  // Also note that a handful of these cases are impossible for 32-bit varints.
  // We coould save a tiny bit of program size by pruning them out.
  while (n >= 8) {
    pos += maskLength;
    uint64_t word = *reinterpret_cast<const uint64_t*>(pos);
    const uint64_t controlBits = _pext_u64(word, mask);
    switch (controlBits) {
      case 0ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 6;
        continue;
      }
      case 1ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 5;
        continue;
      }
      case 2ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f7f00ULL);
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 5;
        continue;
      }
      case 3ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x00000000007f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 4;
        continue;
      }
      case 4ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x000000007f7f0000ULL);
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 5;
        continue;
      }
      case 5ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x000000007f7f0000ULL);
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 4;
        continue;
      }
      case 6ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x000000007f7f7f00ULL);
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 4;
        continue;
      }
      case 7ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000007f7f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 3;
        continue;
      }
      case 8ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        *output++ = _pext_u64(word, 0x0000007f7f000000ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 5;
        continue;
      }
      case 9ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        *output++ = _pext_u64(word, 0x0000007f7f000000ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 4;
        continue;
      }
      case 10ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f7f00ULL);
        *output++ = _pext_u64(word, 0x0000007f7f000000ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 4;
        continue;
      }
      case 11ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x00000000007f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000007f7f000000ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 3;
        continue;
      }
      case 12ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x0000007f7f7f0000ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 4;
        continue;
      }
      case 13ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000007f7f7f0000ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 3;
        continue;
      }
      case 14ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000007f7f7f7f00ULL);
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 3;
        continue;
      }
      case 15ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000007f7f7f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 2;
        continue;
      }
      case 16ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        *output++ = _pext_u64(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 5;
        continue;
      }
      case 17ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        *output++ = _pext_u64(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 4;
        continue;
      }
      case 18ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f7f00ULL);
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        *output++ = _pext_u64(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 4;
        continue;
      }
      case 19ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x00000000007f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        *output++ = _pext_u64(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 3;
        continue;
      }
      case 20ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x000000007f7f0000ULL);
        *output++ = _pext_u64(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 4;
        continue;
      }
      case 21ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x000000007f7f0000ULL);
        *output++ = _pext_u64(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 3;
        continue;
      }
      case 22ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x000000007f7f7f00ULL);
        *output++ = _pext_u64(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 3;
        continue;
      }
      case 23ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000007f7f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 2;
        continue;
      }
      case 24ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        *output++ = _pext_u64(word, 0x00007f7f7f000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 4;
        continue;
      }
      case 25ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        *output++ = _pext_u64(word, 0x00007f7f7f000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 3;
        continue;
      }
      case 26ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f7f00ULL);
        *output++ = _pext_u64(word, 0x00007f7f7f000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 3;
        continue;
      }
      case 27ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x00000000007f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00007f7f7f000000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 2;
        continue;
      }
      case 28ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x00007f7f7f7f0000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 3;
        continue;
      }
      case 29ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00007f7f7f7f0000ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 2;
        continue;
      }
      case 30ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00007f7f7f7f7f00ULL);
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 2;
        continue;
      }
      case 31ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x00007f7f7f7f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        carryover = 0ULL;
        carryoverBits = 0;
        n -= 1;
        continue;
      }
      case 32ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 5;
        continue;
      }
      case 33ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 4;
        continue;
      }
      case 34ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f7f00ULL);
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 4;
        continue;
      }
      case 35ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x00000000007f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 3;
        continue;
      }
      case 36ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x000000007f7f0000ULL);
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 4;
        continue;
      }
      case 37ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x000000007f7f0000ULL);
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 3;
        continue;
      }
      case 38ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x000000007f7f7f00ULL);
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 3;
        continue;
      }
      case 39ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000007f7f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000007f00000000ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 2;
        continue;
      }
      case 40ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        *output++ = _pext_u64(word, 0x0000007f7f000000ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 4;
        continue;
      }
      case 41ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        *output++ = _pext_u64(word, 0x0000007f7f000000ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 3;
        continue;
      }
      case 42ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f7f00ULL);
        *output++ = _pext_u64(word, 0x0000007f7f000000ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 3;
        continue;
      }
      case 43ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x00000000007f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000007f7f000000ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 2;
        continue;
      }
      case 44ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x0000007f7f7f0000ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 3;
        continue;
      }
      case 45ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000007f7f7f0000ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 2;
        continue;
      }
      case 46ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000007f7f7f7f00ULL);
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 2;
        continue;
      }
      case 47ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000007f7f7f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        carryover = _pext_u64(word, 0x00007f0000000000ULL);
        carryoverBits = 7;
        n -= 1;
        continue;
      }
      case 48ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        carryover = _pext_u64(word, 0x00007f7f00000000ULL);
        carryoverBits = 14;
        n -= 4;
        continue;
      }
      case 49ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        carryover = _pext_u64(word, 0x00007f7f00000000ULL);
        carryoverBits = 14;
        n -= 3;
        continue;
      }
      case 50ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f7f00ULL);
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        carryover = _pext_u64(word, 0x00007f7f00000000ULL);
        carryoverBits = 14;
        n -= 3;
        continue;
      }
      case 51ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x00000000007f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x000000007f000000ULL);
        carryover = _pext_u64(word, 0x00007f7f00000000ULL);
        carryoverBits = 14;
        n -= 2;
        continue;
      }
      case 52ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x000000007f7f0000ULL);
        carryover = _pext_u64(word, 0x00007f7f00000000ULL);
        carryoverBits = 14;
        n -= 3;
        continue;
      }
      case 53ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x000000007f7f0000ULL);
        carryover = _pext_u64(word, 0x00007f7f00000000ULL);
        carryoverBits = 14;
        n -= 2;
        continue;
      }
      case 54ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x000000007f7f7f00ULL);
        carryover = _pext_u64(word, 0x00007f7f00000000ULL);
        carryoverBits = 14;
        n -= 2;
        continue;
      }
      case 55ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000007f7f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        carryover = _pext_u64(word, 0x00007f7f00000000ULL);
        carryoverBits = 14;
        n -= 1;
        continue;
      }
      case 56ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        carryover = _pext_u64(word, 0x00007f7f7f000000ULL);
        carryoverBits = 21;
        n -= 3;
        continue;
      }
      case 57ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f0000ULL);
        carryover = _pext_u64(word, 0x00007f7f7f000000ULL);
        carryoverBits = 21;
        n -= 2;
        continue;
      }
      case 58ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x00000000007f7f00ULL);
        carryover = _pext_u64(word, 0x00007f7f7f000000ULL);
        carryoverBits = 21;
        n -= 2;
        continue;
      }
      case 59ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x00000000007f7f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        carryover = _pext_u64(word, 0x00007f7f7f000000ULL);
        carryoverBits = 21;
        n -= 1;
        continue;
      }
      case 60ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        *output++ = _pext_u64(word, 0x0000000000007f00ULL);
        carryover = _pext_u64(word, 0x00007f7f7f7f0000ULL);
        carryoverBits = 28;
        n -= 2;
        continue;
      }
      case 61ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x0000000000007f7fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        carryover = _pext_u64(word, 0x00007f7f7f7f0000ULL);
        carryoverBits = 28;
        n -= 1;
        continue;
      }
      case 62ULL: {
        const uint64_t firstValue = _pext_u64(word, 0x000000000000007fULL);
        *output++ = (firstValue << carryoverBits) | carryover;
        carryover = _pext_u64(word, 0x00007f7f7f7f7f00ULL);
        carryoverBits = 35;
        n -= 1;
        continue;
      }
      case 63ULL: {
        carryover |= _pext_u64(word, 0x00007f7f7f7f7f7fULL) << carryoverBits;
        carryoverBits += 42;
        continue;
      }
      default: {
        NIMBLE_UNREACHABLE("Control bits must be < 64");
      }
    }
  }
  pos += maskLength;
  if (n > 0) {
    if constexpr (std::is_same<T, uint32_t>::value) {
      *output++ = readVarint32(&pos) << carryoverBits | carryover;
      for (uint64_t i = 1; i < n; ++i) {
        *output++ = readVarint32(&pos);
      }
    } else {
      *output++ = readVarint64(&pos) << carryoverBits | carryover;
      for (uint64_t i = 1; i < n; ++i) {
        *output++ = readVarint64(&pos);
      }
    }
  }
  return pos;
}

} // namespace facebook::nimble::varint
