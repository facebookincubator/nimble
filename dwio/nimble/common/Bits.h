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
#include <cstdint>
#include <span>
#include <string>

// Common functions related to bits/bit-packing.
//
// The inlined methods are too simple to test or don't need tests
// as they are for debugging. The non-inlined methods are tested
// indirectly through the nullableColumnTests.cpp.

namespace facebook::nimble::bits {

// Returns the number of bits required to store the value.
// For a value of 0 we return 1.
inline int bitsRequired(uint64_t value) noexcept {
  return 64 - __builtin_clzll(value | 1);
}

// How many buckets are required to hold some elements?
inline uint64_t bucketsRequired(
    uint64_t elementCount,
    uint64_t bucketSize) noexcept {
  return elementCount / bucketSize + (elementCount % bucketSize != 0);
}

// Num bytes required to hold X bits.
inline uint64_t bytesRequired(uint64_t bitCount) noexcept {
  return bucketsRequired(bitCount, 8);
}

// Sets the |n|th bit to true.
inline void setBit(int64_t n, char* c) {
  const int64_t byte = n >> 3;
  const int64_t remainder = n & 7;
  c[byte] |= (1 << remainder);
}

// Sets the |n|th bit to |bit|. Note that if bit is false, this
// is a no-op. The intention is to let you avoid a branch.
inline void maybeSetBit(int64_t n, char* c, bool bit) {
  const int64_t byte = n >> 3;
  const int64_t remainder = n & 7;
  c[byte] |= (bit << remainder);
}

// Sets the |n|th bit to false.
inline void clearBit(int64_t n, char* c) {
  const int64_t byte = n >> 3;
  const int64_t remainder = n & 7;
  c[byte] &= ~(1 << remainder);
}

// Retrieves the |n|th bit.
inline bool getBit(int64_t n, const char* c) {
  const int64_t byte = n >> 3;
  const int64_t remainder = n & 7;
  return c[byte] & (1 << remainder);
}

// Sets bits [|begin|, |end|) in |bitmap| to true. Expects |bitmap| to be word
// aligned.
void setBits(uint64_t begin, uint64_t end, char* bitmap);

// Sets bits [|begin|, |end|) in |bitmap| to false. Expects |bitmap| to be word
// aligned.
void clearBits(uint64_t begin, uint64_t end, char* bitmap);

// Packs |bools| in bitmap. bitmap must point to a region of at least
// FixedBitArray::BufferSize(nulls.size(), 1) bytes. Note that we do not
// first clear the bytes being written to in |bitmap|, so if |bitmap| does
// not point to a zeroed out region after this call bit i will be set if
// bools[i] is true OR bit i was originally true.
void packBitmap(std::span<const bool> bools, char* bitmap);

// Returns how many of the bits in the |bitmap| starting at index |row| and
// going |rowCount| forward are non-null.
uint32_t countSetBits(uint32_t row, uint32_t rowCount, const char* bitmap);

// Finds the indices (relative to the row) of the non-null indices (i.e. set
// bits) among the next |rowCount| rows. |indices| should point to a buffer
// Y enough to hold rowCount values. We return the number of non-nulls.
uint32_t findNonNullIndices(
    uint32_t row,
    uint32_t rowCount,
    const char* bitmap,
    uint32_t* indices);

// Same as above, but returns the indices of the nulls (i.e. unset bits).
uint32_t findNullIndices(
    uint32_t row,
    uint32_t rowCount,
    const char* bitmap,
    uint32_t* indices);

// Finds the index of |n|th set bit in [|begin|, |end|) in |bitmap|. If not
// found, return |end|.
uint32_t
findSetBit(const char* bitmap, uint32_t begin, uint32_t end, uint32_t n);

// Prints out the bits in numeric type in nibbles. Not efficient,
// only should be used for debugging/prototyping.
template <typename T>
std::string printBits(T c) {
  std::string result;
  for (int i = 0; i < (sizeof(T) << 3); ++i) {
    if (i > 0 && i % 4 == 0) {
      result += ' ';
    }
    if (c & 1) {
      result += '1';
    } else {
      result += '0';
    }
    c >>= 1;
  }
  // We actually want little endian order.
  std::reverse(result.begin(), result.end());
  return result;
}

template <typename T>
std::string printHex(T c) {
  std::string result(2 + 2 * sizeof(T), 0);
  result[0] = '0';
  result[1] = 'x';
  char* pos = result.data() + result.size() - 1;
  for (int i = 0; i < 2 * sizeof(T); ++i) {
    const uint32_t nibble = c & 15;
    if (nibble < 10) {
      *pos = nibble + '0';
    } else {
      *pos = (nibble - 10) + 'a';
    }
    c >>= 4;
    --pos;
  }
  return result;
}

class Bitmap {
 public:
  Bitmap(const void* bitmap, uint32_t size)
      : bitmap_{static_cast<char*>(const_cast<void*>(bitmap))}, size_{size} {}

  bool test(uint32_t pos) const {
    return getBit(pos, bitmap_);
  }

  uint32_t size() const {
    return size_;
  }

  const void* bits() const {
    return bitmap_;
  }

 protected:
  char* bitmap_;
  uint32_t size_;
};

class BitmapBuilder : public Bitmap {
 public:
  BitmapBuilder(void* bitmap, uint32_t size) : Bitmap{bitmap, size} {}

  void set(uint32_t pos) {
    setBit(pos, bitmap_);
  }

  void maybeSet(uint32_t pos, bool bit) {
    maybeSetBit(pos, bitmap_, bit);
  }

  void set(uint32_t begin, uint32_t end) {
    setBits(begin, end, bitmap_);
  }

  void clear(uint32_t begin, uint32_t end) {
    clearBits(begin, end, bitmap_);
  }

  // Copy the specified range from the source bitmap into this one. It
  // guarantees |begin| is the beginning bit offset, but may copy more beyond
  // |end|.
  void copy(const Bitmap& other, uint32_t begin, uint32_t end);
};

} // namespace facebook::nimble::bits
