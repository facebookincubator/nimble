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

#include <array>
#include <cstring>

#include <xsimd/xsimd.hpp>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Varint.h"
#include "folly/Likely.h"

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

// Zero-extend 8 consecutive bytes into T-sized output elements using xsimd
// batch construction and store.
template <typename T>
inline void expandByteWord(const uint8_t* bytes, T* output) {
  using batch_type = xsimd::batch<T>;
  constexpr auto kBatchSize = batch_type::size;

  if constexpr (kBatchSize >= 8) {
    batch_type(
        static_cast<T>(bytes[0]),
        static_cast<T>(bytes[1]),
        static_cast<T>(bytes[2]),
        static_cast<T>(bytes[3]),
        static_cast<T>(bytes[4]),
        static_cast<T>(bytes[5]),
        static_cast<T>(bytes[6]),
        static_cast<T>(bytes[7]))
        .store_unaligned(output);
  } else if constexpr (kBatchSize == 4) {
    batch_type(
        static_cast<T>(bytes[0]),
        static_cast<T>(bytes[1]),
        static_cast<T>(bytes[2]),
        static_cast<T>(bytes[3]))
        .store_unaligned(output);
    batch_type(
        static_cast<T>(bytes[4]),
        static_cast<T>(bytes[5]),
        static_cast<T>(bytes[6]),
        static_cast<T>(bytes[7]))
        .store_unaligned(output + 4);
  } else if constexpr (kBatchSize == 2) {
    batch_type(static_cast<T>(bytes[0]), static_cast<T>(bytes[1]))
        .store_unaligned(output);
    batch_type(static_cast<T>(bytes[2]), static_cast<T>(bytes[3]))
        .store_unaligned(output + 2);
    batch_type(static_cast<T>(bytes[4]), static_cast<T>(bytes[5]))
        .store_unaligned(output + 4);
    batch_type(static_cast<T>(bytes[6]), static_cast<T>(bytes[7]))
        .store_unaligned(output + 6);
  }
}

// Process runs of single-byte varints using xsimd for both the high-bit
// check and byte-to-element widening. Works with uint8_t* throughout,
// avoiding reinterpret_cast to uint64_t* (alignment/strict-aliasing issues).
// Returns the number of elements remaining after processing.
template <typename T>
inline uint64_t
bulkDecodeSingleByteRun(uint64_t n, const char*& pos, T*& output) {
  using u8_batch = xsimd::batch<uint8_t>;
  constexpr auto kU8Size = u8_batch::size;
  constexpr uint64_t wordSize = 8;
  constexpr uint64_t kHighBits = 0x8080808080808080ULL;

  const auto* src = reinterpret_cast<const uint8_t*>(pos);
  uint64_t remaining = n;

  // Process kU8BatchSize bytes at a time.
  // Single wide load + vptest
  while (remaining >= kU8Size) {
    auto bytes = u8_batch::load_unaligned(src);
    if (xsimd::any((bytes & u8_batch(0x80)) != u8_batch(0))) {
      break;
    }
    for (size_t i = 0; i < kU8Size; i += wordSize) {
      expandByteWord(src + i, output + i);
    }
    src += kU8Size;
    output += kU8Size;
    remaining -= kU8Size;
  }

  // Process 8 bytes at a time. Use memcpy for the high-bit check to avoid
  // reinterpret_cast<const uint64_t*> strict-aliasing/alignment issues.
  while (remaining >= wordSize) {
    uint64_t word;
    std::memcpy(&word, src, sizeof(word));

    if (word & kHighBits) {
      break;
    }
    expandByteWord(src, output);
    src += wordSize;
    output += wordSize;
    remaining -= wordSize;
  }

  // Handle trailing single-byte varints one at a time.
  while (remaining > 0 && !(src[0] & 0x80)) {
    *output++ = static_cast<T>(src[0]);
    ++src;
    --remaining;
  }

  pos = reinterpret_cast<const char*>(src);
  return remaining;
}

constexpr std::size_t kCacheLineBytes = 64;
constexpr std::size_t kMaxControlBitsValue = 64;
constexpr std::size_t kMaskLength = 6;

// Lookup table entry for table-driven BMI2 varint decode.
struct alignas(kCacheLineBytes) VarintLookupEntry {
  // Extraction masks for up to 6 completed varints. Unused slots are 0
  uint64_t valueMasks[kMaskLength];
  // Extraction mask for carryover bytes (partial varint at end of chunk), and
  // zero when the chunk ends on a clean varint boundary.
  uint64_t carryOverMask;
  uint8_t numCompleted;
  uint8_t carryOverBits;
  uint8_t padding[6];
};

static_assert(
    sizeof(VarintLookupEntry) == kCacheLineBytes,
    "Must fit one cache line");

// We build the full 64-entry/kMaxControlBitsValue for control bit lookup table
// at compile time.
static constexpr auto kDecodeTable = [] {
  std::array<VarintLookupEntry, kMaxControlBitsValue> table{};
  for (int i = 0; i < kMaxControlBitsValue; ++i) {
    VarintLookupEntry entry{};
    uint64_t currentMask = 0;

    int lastZero = -1, numCompleted = 0;
    const uint8_t controlBits = static_cast<uint8_t>(i);
    for (int j = 0; j < kMaskLength; ++j) {
      currentMask |= uint64_t(0x7f) << (j * 8);
      if (!((controlBits >> j) & 1)) {
        entry.valueMasks[numCompleted] = currentMask;
        ++numCompleted;

        lastZero = j;
        currentMask = 0;
      }
    }

    entry.numCompleted = static_cast<uint8_t>(numCompleted);

    entry.carryOverMask = 0;
    entry.carryOverBits = 0;

    if (lastZero < 5) {
      // Partial varint at end of chunk.
      entry.carryOverMask = currentMask;
      entry.carryOverBits = static_cast<uint8_t>(7 * (5 - lastZero));
    } else if (lastZero == -1) {
      // All 6 bytes are continuation bytes (case 63). Accumulate carryover.
      // This is a rare case
      entry.carryOverMask = currentMask;
      entry.carryOverBits = 42;
    }
    table[i] = entry;
  }
  return table;
}();

// Table-driven BMI2 varint decode. Reads extraction masks from a lookup table.
template <typename T>
__attribute__((__target__("bmi2"))) const char*
bulkVarintDecodeBmi2Table(uint64_t n, const char* pos, T* output) {
  constexpr uint64_t kControlMask = 0x0000808080808080ULL;
  constexpr int kChunkLen = 6;

  uint64_t carryover = 0;
  int carryoverBits = 0;
  pos -= kChunkLen;

  while (n >= 8) {
    pos += kChunkLen;

    uint64_t word;
    std::memcpy(&word, pos, sizeof(word));
    const uint64_t cb = _pext_u64(word, kControlMask);

    // Case 63 (all continuation bytes) requires accumulating carryover
    // rather than replacing it. This case is extremely rare
    if (FOLLY_UNLIKELY(cb == 63)) {
      carryover |= _pext_u64(word, 0x00007f7f7f7f7f7fULL) << carryoverBits;
      carryoverBits += 42;
      continue;
    }

    const auto& info = kDecodeTable[cb];

    // Extract and store up to 6 values. Unused mask slots are 0, producing
    // harmless zero writes that will be overwritten by subsequent iterations
    output[0] = static_cast<T>(
        (_pext_u64(word, info.valueMasks[0]) << carryoverBits) | carryover);
    output[1] = static_cast<T>(_pext_u64(word, info.valueMasks[1]));
    output[2] = static_cast<T>(_pext_u64(word, info.valueMasks[2]));
    output[3] = static_cast<T>(_pext_u64(word, info.valueMasks[3]));
    output[4] = static_cast<T>(_pext_u64(word, info.valueMasks[4]));
    output[5] = static_cast<T>(_pext_u64(word, info.valueMasks[5]));

    output += info.numCompleted;
    n -= info.numCompleted;

    // Update carryover. When carryoverMask is 0, _pext returns 0 and
    // carryoverBits is 0, effectively clearing the carryover state.
    carryover = _pext_u64(word, info.carryOverMask);
    carryoverBits = info.carryOverBits;
  }

  pos += kChunkLen;
  if (n > 0) {
    if constexpr (std::is_same_v<T, uint32_t>) {
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

const char* bulkVarintDecode32(uint64_t n, const char* pos, uint32_t* output) {
  auto remaining = bulkDecodeSingleByteRun(n, pos, output);
  if (remaining == 0) {
    return pos;
  }
  return bulkVarintDecodeBmi2Table(remaining, pos, output);
}

const char* bulkVarintDecode64(uint64_t n, const char* pos, uint64_t* output) {
  auto remaining = bulkDecodeSingleByteRun(n, pos, output);
  if (remaining == 0) {
    return pos;
  }
  return bulkVarintDecodeBmi2Table(remaining, pos, output);
}

} // namespace facebook::nimble::varint
