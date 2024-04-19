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

#include "dwio/nimble/common/Bits.h"

// Packs integers into a fixed number (1-64) of bits and retrieves them.
// The 'bulk' API provided is particularly efficient at setting/getting
// ranges of values with small bit widths.
//
// Typical usage:
//   std::vector<uint32_t> data = ...
//   int bitsRequired = BitsRequired(*maxElement(data.begin(), data.end());
//   auto buffer = std::make_unique<char[]>(
//        FixedBitArray::BufferSize(data.size(), bitsRequired));
//   FixedBitArray fixedBitArray(buffer.get(), bitsRequired);
//   fixedBitArray.bulkSet32(0, data.size(), data.data());
//
// And then you'd store the buffer somewhere, and later read the values back,
// recovering the info stored in data. Note that the FBA does not own
// any data.

namespace facebook::nimble {

class FixedBitArray {
 public:
  // Computes the buffer size needed to hold |elementCount| values with the
  // specified |bitWidth|. Note that we allocate a few bytes of 'slop' space
  // beyond what is mathematically required to speed things up.
  static uint64_t bufferSize(uint64_t elementCount, int bitWidth);

  // Not legal to use; included so we can default construct on the stack.
  FixedBitArray() = default;

  // Creates a fixed bit array stored at buffer whose elements can lie within
  // the range [0, 2^|bitWidth|). The |buffer| must already be preallocated to
  // the appropriate size, as given by BufferSize.
  FixedBitArray(char* buffer, int bitWidth);

  // Convenience constructor for reading read-only data. Note that you are NOT
  // prevented by the code from using the non-const calls on a FBA constructed
  // via this method, but to do so is undefined behavior.
  FixedBitArray(std::string_view buffer, int bitWidth)
      : FixedBitArray(const_cast<char*>(buffer.data()), bitWidth) {}

  // Sets the |index|'th slot to the given |value|. If value
  // is >= 2^|bitWidth| behavior is undefined.
  //
  // IMPORTANT NOTE: this does NOT function as normal assignment.
  // We do NOT first 0 out the slot, i.e. we use or semantics.
  // If you need to first 0 it out, use ZeroAndSet.
  void set(uint64_t index, uint64_t value);

  // Zeroes a slot and then sets it (like normal assignment).
  void zeroAndSet(uint64_t index, uint64_t value);

  // Gets the |index|'th value.
  uint64_t get(uint64_t index) const;

  // Versions of set/get that only work with bitWidth <= 32, but are slightly
  // faster. Same assignment caveat applies to set32 as to set.
  void set32(uint64_t index, uint32_t value);
  uint32_t get32(uint64_t index) const;

  // Retrieves a contiguous subarray from slots [start, start + length).
  // Considerably faster than looping a get call. Only callable when
  // bit width <= 32.
  void bulkGet32(uint64_t start, uint64_t length, uint32_t* values) const;

  // Same as above, but outputs into 8-bytes values. Still requires that bit
  // width <= 32.
  void bulkGet32Into64(uint64_t start, uint64_t length, uint64_t* values) const;

  // Same as above. Add baseline to every value.
  void bulkGetWithBaseline32(
      uint64_t start,
      uint64_t length,
      uint32_t* values,
      uint32_t baseline) const;

  // Same as above. Add baseline to every value.
  void bulkGetWithBaseline32Into64(
      uint64_t start,
      uint64_t length,
      uint64_t* values,
      uint64_t baseline) const;

  // Sets a contiguous subarray of slots from [start, start + length).
  // Considerably faster than looping/ a get call. Only callable when bitWidth
  // <= 32. Same semantics as set -- see the warning there.
  void bulkSet32(uint64_t start, uint64_t length, const uint32_t* values);

  // Same as above. Subtracts baseline from every value.
  void bulkSet32WithBaseline(
      uint64_t start,
      uint64_t length,
      const uint32_t* values,
      uint32_t baseline);

  // Fills in the bit-packed |bitVector| with whether each value in
  // [start, start + length) equals |value|. Should be faster than
  // doing bulkGet32 and then doing your own equality check + bitpack.
  //
  // bitVector must point to a buffer of at least size BufferSize(length, 1);
  //
  // REQUIRES: start is a multiple of 64. If you need to run equals on
  // a range where that isn't true, you'll have to manually do the part
  // up to the first multiple of 64 yourself + adjust the results accordingly.
  void equals32(
      uint64_t start,
      uint64_t length,
      uint32_t value,
      char* bitVector) const;

  int bitWidth() const {
    return bitWidth_;
  }

 private:
  char* buffer_;
  int bitWidth_;
  uint64_t mask_;
};

} // namespace facebook::nimble
