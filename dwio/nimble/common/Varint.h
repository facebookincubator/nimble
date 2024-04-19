// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <span>

// Varint-related encoding methods. Same binary encoding as folly/Varint.h
// but with a different API, a faster decode method thanks to
// to not checking bounds, and bulk decoding methods. No functions in this
// library check bounds or deal with buffer overflow.
//
// The bulk decoding methods are particularly interesting. They range from
// 4x faster than the non-bulk in the best case (all 1 byte varints) to
// about 50% faster in the worst case (random byte lengths), assuming that
// bmi2 is available. See varintBenchmark.h for some details.

namespace facebook::nimble::varint {

// Decode n varints at once. Makes use of bmi2 instruction set if its
// available. Returns pos updated past the n varints.
const char* bulkVarintDecode32(uint64_t n, const char* pos, uint32_t* output);
const char* bulkVarintDecode64(uint64_t n, const char* pos, uint64_t* output);

// Skips n varints, returning pos updated past the n varints.
const char* bulkVarintSkip(uint64_t n, const char* pos);

// Returns the number of bytes the |values| will occupy after varint encoding.
uint64_t bulkVarintSize32(std::span<const uint32_t> values);
uint64_t bulkVarintSize64(std::span<const uint64_t> values);

// Inline non-bulk methods follow below.

template <typename T>
inline void writeVarint(T val, char** pos) noexcept {
  while (val >= 128) {
    *((*pos)++) = 0x80 | (val & 0x7f);
    val >>= 7;
  }
  *((*pos)++) = val;
}

inline void skipVarint(const char** pos) noexcept {
  while (*((*pos)++) & 128) {
  }
}

inline uint32_t readVarint32(const char** pos) noexcept {
  uint32_t value = (**pos) & 127;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 7;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 14;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 21;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (*((*pos)++) & 127) << 28;
  return value;
}

inline uint64_t readVarint64(const char** pos) noexcept {
  uint64_t value = (**pos) & 127;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 7;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 14;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 21;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 28;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 35;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 42;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 49;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 56;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(*((*pos)++) & 127) << 63;
  return value;
}

} // namespace facebook::nimble::varint
