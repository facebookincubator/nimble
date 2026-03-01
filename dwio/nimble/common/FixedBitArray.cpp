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
#include "dwio/nimble/common/FixedBitArray.h"
#include <glog/logging.h>

namespace facebook::nimble {

// Warning: do not change this function or lots of horrible data corruption
// will probably happen.
uint64_t FixedBitArray::bufferSize(uint64_t elementCount, int bitWidth) {
  // We may read up to 7 bytes beyond the last theoretically needed byte,
  // as we access whole machine words.
  constexpr int kSlopSize = 7;
  return velox::bits::nbytes(elementCount * bitWidth) + kSlopSize;
}

FixedBitArray::FixedBitArray(char* buffer, int bitWidth)
    : buffer_(buffer), bitWidth_(bitWidth) {
  DCHECK(bitWidth_ >= 0);
  DCHECK(bitWidth_ <= 64);
  // mask_ has the first bitWidth bits set to 1, rest 0.
  mask_ = bitWidth == 64 ? (~0ULL) : ((1ULL << bitWidth_) - 1);
}

uint64_t FixedBitArray::get(uint64_t index) const {
  const uint64_t bits = index * bitWidth_;
  const uint64_t offset = bits >> 3;
  const uint64_t remainder = bits & 7;
  const uint64_t word = *reinterpret_cast<uint64_t*>(buffer_ + offset);
  // For widths > 58 bits, the value may overflow into the next word.
  if (bitWidth_ > 58) {
    const int overflow = bitWidth_ + remainder - 64;
    if (overflow > 0) {
      const uint64_t nextWord =
          *reinterpret_cast<uint64_t*>(buffer_ + offset + 8);
      return ((word >> remainder) | (nextWord << (bitWidth_ - overflow))) &
          mask_;
    }
  }
  return (word >> remainder) & mask_;
}

uint32_t FixedBitArray::get32(uint64_t index) const {
  const uint64_t bits = index * bitWidth_;
  const uint64_t offset = bits >> 3;
  const uint64_t remainder = bits & 7;
  const uint64_t word = *reinterpret_cast<uint64_t*>(buffer_ + offset);
  // Don't have to worry about overflow here since bitWidth_ <= 32.
  return (word >> remainder) & mask_;
}

void FixedBitArray::set(uint64_t index, uint64_t value) {
  const uint64_t bits = index * bitWidth_;
  const uint64_t offset = bits >> 3;
  const uint64_t remainder = bits & 7;
  uint64_t& word = *reinterpret_cast<uint64_t*>(buffer_ + offset);
  // For widths > 58 bits, the value may overflow into the next word.
  if (bitWidth_ > 58) {
    const int overflow = bitWidth_ + remainder - 64;
    if (overflow > 0) {
      uint64_t& nextWord = *reinterpret_cast<uint64_t*>(buffer_ + offset + 8);
      nextWord |= value >> (bitWidth_ - overflow);
    }
  }
  word |= value << remainder;
}

void FixedBitArray::set32(uint64_t index, uint32_t value) {
  const uint64_t bits = index * bitWidth_;
  const uint64_t offset = bits >> 3;
  const uint64_t remainder = bits & 7;
  uint64_t& word = *reinterpret_cast<uint64_t*>(buffer_ + offset);
  // Don't have to worry about overflow here since bitWidth_ <= 32.
  word |= static_cast<uint64_t>(value) << remainder;
}

void FixedBitArray::zeroAndSet(uint64_t index, uint64_t value) {
  const uint64_t bits = index * bitWidth_;
  const uint64_t offset = bits >> 3;
  const uint64_t remainder = bits & 7;
  uint64_t& word = *reinterpret_cast<uint64_t*>(buffer_ + offset);
  word &= ~(mask_ << remainder);
  // For widths > 58 bits, the value may overflow into the next word.
  if (bitWidth_ > 58) {
    const int overflow = bitWidth_ + remainder - 64;
    if (overflow > 0) {
      uint64_t& nextWord = *reinterpret_cast<uint64_t*>(buffer_ + offset + 8);
      nextWord &= 0xFFFFFFFFFFFFFFFF << overflow;
      nextWord |= value >> (bitWidth_ - overflow);
    }
  }
  word |= value << remainder;
}

namespace internal {

// T is the output data types, namely uint32_t or uint64_t.
template <typename T, int bitWidth, int loopPosition, bool withBaseline>
void bulkGet32Loop(
    uint64_t& word,
    const uint64_t** nextWord,
    T** values,
    T baseline) {
  constexpr uint64_t kMask = (1ULL << bitWidth) - 1ULL;
  // Some bits for the next value may still be present in word.
  constexpr uint64_t spillover = (loopPosition * bitWidth) % 64 == 0
      ? 0
      : ((loopPosition + 1) * bitWidth) % 64;
  // Note that this isn't a real branch as its on a constexpr.
  if constexpr (spillover > 0) {
    uint64_t remainder = word;
    word = **nextWord;
    ++(*nextWord);
    if constexpr (withBaseline) {
      **values =
          ((remainder | word << (bitWidth - spillover)) & kMask) + baseline;
    } else {
      **values = (remainder | word << (bitWidth - spillover)) & kMask;
    }
    word >>= spillover;
    ++(*values);
  } else {
    word = **nextWord;
    ++(*nextWord);
  }
  // How many remaining values are in this word?
  constexpr int valueCount = (64 - spillover) / bitWidth;
  for (int i = 0; i < valueCount; ++i) {
    if constexpr (withBaseline) {
      **values = (word & kMask) + baseline;
    } else {
      **values = word & kMask;
    }
    ++(*values);
    word >>= bitWidth;
  }
  constexpr int nextLoopPosition = loopPosition + valueCount + (spillover > 0);
  bulkGet32Loop<T, bitWidth, nextLoopPosition, withBaseline>(
      word, nextWord, values, baseline);
}

// Unfortunately we cannot partially specialize the template for the
// terminal case of loopPosition = 64 so we must explicitly specify them.
#define BULK_GET32_LOOP_TERMINAL_CASE(bitWidth)      \
  template <>                                        \
  void bulkGet32Loop<uint32_t, bitWidth, 64, false>( \
      uint64_t& word,                                \
      const uint64_t** nextWord,                     \
      uint32_t** values,                             \
      uint32_t baseline) {}                          \
  template <>                                        \
  void bulkGet32Loop<uint64_t, bitWidth, 64, false>( \
      uint64_t& word,                                \
      const uint64_t** nextWord,                     \
      uint64_t** values,                             \
      uint64_t baseline) {}                          \
  template <>                                        \
  void bulkGet32Loop<uint32_t, bitWidth, 64, true>(  \
      uint64_t& word,                                \
      const uint64_t** nextWord,                     \
      uint32_t** values,                             \
      uint32_t baseline) {}                          \
  template <>                                        \
  void bulkGet32Loop<uint64_t, bitWidth, 64, true>(  \
      uint64_t& word,                                \
      const uint64_t** nextWord,                     \
      uint64_t** values,                             \
      uint64_t baseline) {}

BULK_GET32_LOOP_TERMINAL_CASE(1)
BULK_GET32_LOOP_TERMINAL_CASE(2)
BULK_GET32_LOOP_TERMINAL_CASE(3)
BULK_GET32_LOOP_TERMINAL_CASE(4)
BULK_GET32_LOOP_TERMINAL_CASE(5)
BULK_GET32_LOOP_TERMINAL_CASE(6)
BULK_GET32_LOOP_TERMINAL_CASE(7)
BULK_GET32_LOOP_TERMINAL_CASE(8)
BULK_GET32_LOOP_TERMINAL_CASE(9)
BULK_GET32_LOOP_TERMINAL_CASE(10)
BULK_GET32_LOOP_TERMINAL_CASE(11)
BULK_GET32_LOOP_TERMINAL_CASE(12)
BULK_GET32_LOOP_TERMINAL_CASE(13)
BULK_GET32_LOOP_TERMINAL_CASE(14)
BULK_GET32_LOOP_TERMINAL_CASE(15)
BULK_GET32_LOOP_TERMINAL_CASE(16)
BULK_GET32_LOOP_TERMINAL_CASE(17)
BULK_GET32_LOOP_TERMINAL_CASE(18)
BULK_GET32_LOOP_TERMINAL_CASE(19)
BULK_GET32_LOOP_TERMINAL_CASE(20)
BULK_GET32_LOOP_TERMINAL_CASE(21)
BULK_GET32_LOOP_TERMINAL_CASE(22)
BULK_GET32_LOOP_TERMINAL_CASE(23)
BULK_GET32_LOOP_TERMINAL_CASE(24)
BULK_GET32_LOOP_TERMINAL_CASE(25)
BULK_GET32_LOOP_TERMINAL_CASE(26)
BULK_GET32_LOOP_TERMINAL_CASE(27)
BULK_GET32_LOOP_TERMINAL_CASE(28)
BULK_GET32_LOOP_TERMINAL_CASE(29)
BULK_GET32_LOOP_TERMINAL_CASE(30)
BULK_GET32_LOOP_TERMINAL_CASE(31)
BULK_GET32_LOOP_TERMINAL_CASE(32)

#undef BULK_GET32_LOOP_TERMINAL_CASE

template <typename T, bool withBaseline>
void bulkGet32Internal(
    const FixedBitArray& fixedBitArray,
    const char* buffer,
    uint64_t start,
    uint64_t length,
    T* values,
    T baseline) {
  // Every 64 elements we know the slot will end on a word boundary.
  // (It might end on a boundary before that, which should technically
  // let us be more efficient if we used that instead because we can use
  // the non-bulk API less, but the benefit is pretty small so we do the
  // simple thing for now).
  //
  // We first use the non-bulk method to align ourselves to a 64-element
  // boundary, then dispatch to the appropriate bit-width-specific code
  // for the 64-element loops, then finish with the non-bulk method.
  const uint64_t alignedStart = velox::bits::divRoundUp(start, 64) << 6;
  if (start + length < alignedStart) {
    for (uint64_t i = start; i < start + length; ++i) {
      if constexpr (withBaseline) {
        // TODO: An alternative would be to have a separate (constexpr) loop
        // adding baselines at the end and hopefully the compiler will vectorize
        // it (with -mavx2?). But it might be slower due to the extra loop
        // condition checks. Need to benchmark it.
        *values = fixedBitArray.get32(i) + baseline;
      } else {
        *values = fixedBitArray.get32(i);
      }
      ++values;
    }
    return;
  }
  for (uint64_t i = start; i < alignedStart; ++i) {
    if constexpr (withBaseline) {
      *values = fixedBitArray.get32(i) + baseline;
    } else {
      *values = fixedBitArray.get32(i);
    }
    ++values;
  }
  const uint64_t loopCount = (length - (alignedStart - start)) >> 6;
  switch (fixedBitArray.bitWidth()) {
#define BULK_GET32_SWITCH_CASE(bitWidth)                          \
  case bitWidth: {                                                \
    const uint64_t* nextWord = reinterpret_cast<const uint64_t*>( \
        buffer + ((alignedStart * bitWidth) >> 3));               \
    uint64_t word;                                                \
    for (uint64_t i = 0; i < loopCount; ++i) {                    \
      bulkGet32Loop<T, bitWidth, 0, withBaseline>(                \
          word, &nextWord, &values, baseline);                    \
    }                                                             \
    break;                                                        \
  }

    BULK_GET32_SWITCH_CASE(1)
    BULK_GET32_SWITCH_CASE(2)
    BULK_GET32_SWITCH_CASE(3)
    BULK_GET32_SWITCH_CASE(4)
    BULK_GET32_SWITCH_CASE(5)
    BULK_GET32_SWITCH_CASE(6)
    BULK_GET32_SWITCH_CASE(7)
    BULK_GET32_SWITCH_CASE(8)
    BULK_GET32_SWITCH_CASE(9)
    BULK_GET32_SWITCH_CASE(10)
    BULK_GET32_SWITCH_CASE(11)
    BULK_GET32_SWITCH_CASE(12)
    BULK_GET32_SWITCH_CASE(13)
    BULK_GET32_SWITCH_CASE(14)
    BULK_GET32_SWITCH_CASE(15)
    BULK_GET32_SWITCH_CASE(16)
    BULK_GET32_SWITCH_CASE(17)
    BULK_GET32_SWITCH_CASE(18)
    BULK_GET32_SWITCH_CASE(19)
    BULK_GET32_SWITCH_CASE(20)
    BULK_GET32_SWITCH_CASE(21)
    BULK_GET32_SWITCH_CASE(22)
    BULK_GET32_SWITCH_CASE(23)
    BULK_GET32_SWITCH_CASE(24)
    BULK_GET32_SWITCH_CASE(25)
    BULK_GET32_SWITCH_CASE(26)
    BULK_GET32_SWITCH_CASE(27)
    BULK_GET32_SWITCH_CASE(28)
    BULK_GET32_SWITCH_CASE(29)
    BULK_GET32_SWITCH_CASE(30)
    BULK_GET32_SWITCH_CASE(31)
    BULK_GET32_SWITCH_CASE(32)

#undef BULK_GET32_SWITCH_CASE

    default:
      LOG(FATAL) << "bit width must lie in [1, 32], got: "
                 << fixedBitArray.bitWidth();
  }
  const uint64_t remainderStart = alignedStart + (loopCount << 6);
  const uint64_t remainderEnd = start + length;
  for (uint64_t i = remainderStart; i < remainderEnd; ++i) {
    if constexpr (withBaseline) {
      *values = fixedBitArray.get32(i) + baseline;
    } else {
      *values = fixedBitArray.get32(i);
    }
    ++values;
  }
  return;
}

} // namespace internal

void FixedBitArray::bulkGet32(uint64_t start, uint64_t length, uint32_t* values)
    const {
  internal::bulkGet32Internal<uint32_t, false>(
      *this, buffer_, start, length, values, 0);
}

void FixedBitArray::bulkGet32Into64(
    uint64_t start,
    uint64_t length,
    uint64_t* values) const {
  internal::bulkGet32Internal<uint64_t, false>(
      *this, buffer_, start, length, values, 0);
}

void FixedBitArray::bulkGetWithBaseline32(
    uint64_t start,
    uint64_t length,
    uint32_t* values,
    uint32_t baseline) const {
  internal::bulkGet32Internal<uint32_t, true>(
      *this, buffer_, start, length, values, baseline);
}

void FixedBitArray::bulkGetWithBaseline32Into64(
    uint64_t start,
    uint64_t length,
    uint64_t* values,
    uint64_t baseline) const {
  internal::bulkGet32Internal<uint64_t, true>(
      *this, buffer_, start, length, values, baseline);
}

template <int bitWidth, int loopPosition, bool withBaseline>
void bulkSet32Loop(
    uint64_t** nextWord,
    const uint32_t** values,
    uint32_t baseline) {
  // Some bits for the next value may need to put be in the current word.
  constexpr int spillover = (loopPosition * bitWidth) % 64 == 0
      ? 0
      : ((loopPosition + 1) * bitWidth) % 64;
  // Note that this isn't a real branch as its on a constexpr.
  if constexpr (spillover > 0) {
    if constexpr (withBaseline) {
      **nextWord |= static_cast<uint64_t>(**values - baseline)
          << (64 - bitWidth + spillover);
      ++(*nextWord);
      **nextWord |=
          static_cast<uint64_t>(**values - baseline) >> (bitWidth - spillover);
      ++(*values);
    } else {
      **nextWord |= static_cast<uint64_t>(**values)
          << (64 - bitWidth + spillover);
      ++(*nextWord);
      **nextWord |= static_cast<uint64_t>(**values) >> (bitWidth - spillover);
      ++(*values);
    }
  } else {
    ++(*nextWord);
  }
  // How many remaining values are in this word?
  constexpr int valueCount = (64 - spillover) / bitWidth;
  int offset = spillover;
  for (int i = 0; i < valueCount; ++i) {
    if constexpr (withBaseline) {
      **nextWord |= static_cast<uint64_t>(**values - baseline) << offset;
    } else {
      **nextWord |= static_cast<uint64_t>(**values) << offset;
    }
    offset += bitWidth;
    ++(*values);
  }
  constexpr int nextLoopPosition = loopPosition + valueCount + (spillover > 0);
  bulkSet32Loop<bitWidth, nextLoopPosition, withBaseline>(
      nextWord, values, baseline);
}

// Unfortunately we cannot partially specialize the template for the
// terminal case of loopPosition = 64 so we must explicitly specify them.
#define BULK_SET32_LOOP_TERMINAL_CASE(bitWidth)                           \
  template <>                                                             \
  void bulkSet32Loop<bitWidth, 64, false>(                                \
      uint64_t** nextWord, const uint32_t** values, uint32_t baseline) {} \
  template <>                                                             \
  void bulkSet32Loop<bitWidth, 64, true>(                                 \
      uint64_t** nextWord, const uint32_t** values, uint32_t baseline) {}

BULK_SET32_LOOP_TERMINAL_CASE(1)
BULK_SET32_LOOP_TERMINAL_CASE(2)
BULK_SET32_LOOP_TERMINAL_CASE(3)
BULK_SET32_LOOP_TERMINAL_CASE(4)
BULK_SET32_LOOP_TERMINAL_CASE(5)
BULK_SET32_LOOP_TERMINAL_CASE(6)
BULK_SET32_LOOP_TERMINAL_CASE(7)
BULK_SET32_LOOP_TERMINAL_CASE(8)
BULK_SET32_LOOP_TERMINAL_CASE(9)
BULK_SET32_LOOP_TERMINAL_CASE(10)
BULK_SET32_LOOP_TERMINAL_CASE(11)
BULK_SET32_LOOP_TERMINAL_CASE(12)
BULK_SET32_LOOP_TERMINAL_CASE(13)
BULK_SET32_LOOP_TERMINAL_CASE(14)
BULK_SET32_LOOP_TERMINAL_CASE(15)
BULK_SET32_LOOP_TERMINAL_CASE(16)
BULK_SET32_LOOP_TERMINAL_CASE(17)
BULK_SET32_LOOP_TERMINAL_CASE(18)
BULK_SET32_LOOP_TERMINAL_CASE(19)
BULK_SET32_LOOP_TERMINAL_CASE(20)
BULK_SET32_LOOP_TERMINAL_CASE(21)
BULK_SET32_LOOP_TERMINAL_CASE(22)
BULK_SET32_LOOP_TERMINAL_CASE(23)
BULK_SET32_LOOP_TERMINAL_CASE(24)
BULK_SET32_LOOP_TERMINAL_CASE(25)
BULK_SET32_LOOP_TERMINAL_CASE(26)
BULK_SET32_LOOP_TERMINAL_CASE(27)
BULK_SET32_LOOP_TERMINAL_CASE(28)
BULK_SET32_LOOP_TERMINAL_CASE(29)
BULK_SET32_LOOP_TERMINAL_CASE(30)
BULK_SET32_LOOP_TERMINAL_CASE(31)
BULK_SET32_LOOP_TERMINAL_CASE(32)

#undef BULK_SET32_LOOP_TERMINAL_CASE

template <bool withBaseline>
void bulkSetInternal32(
    FixedBitArray& fixedBitArray,
    char* buffer,
    uint64_t start,
    uint64_t length,
    const uint32_t* values,
    uint32_t baseline) {
  // Same general logic as BulkGet32. See the comments there.
  switch (fixedBitArray.bitWidth()) {
#define BULK_SET32_SWITCH_CASE(bitWidth)                                      \
  case bitWidth: {                                                            \
    const uint64_t alignedStart = velox::bits::divRoundUp(start, 64) << 6;    \
    if (start + length < alignedStart) {                                      \
      for (uint64_t i = start; i < start + length; ++i) {                     \
        if constexpr (withBaseline) {                                         \
          fixedBitArray.set32(i, (*values) - baseline);                       \
        } else {                                                              \
          fixedBitArray.set32(i, *values);                                    \
        }                                                                     \
        ++values;                                                             \
      }                                                                       \
      return;                                                                 \
    }                                                                         \
    for (uint64_t i = start; i < alignedStart; ++i) {                         \
      if constexpr (withBaseline) {                                           \
        fixedBitArray.set32(i, (*values) - baseline);                         \
      } else {                                                                \
        fixedBitArray.set32(i, *values);                                      \
      }                                                                       \
      ++values;                                                               \
    }                                                                         \
    const uint64_t loopCount = (length - (alignedStart - start)) >> 6;        \
    uint64_t* nextWord = reinterpret_cast<uint64_t*>(                         \
        buffer + ((alignedStart * bitWidth) >> 3) - 8);                       \
    for (uint64_t i = 0; i < loopCount; ++i) {                                \
      bulkSet32Loop<bitWidth, 0, withBaseline>(&nextWord, &values, baseline); \
    }                                                                         \
    const uint64_t remainderStart = alignedStart + (loopCount << 6);          \
    const uint64_t remainderEnd = start + length;                             \
    for (uint64_t i = remainderStart; i < remainderEnd; ++i) {                \
      if constexpr (withBaseline) {                                           \
        fixedBitArray.set32(i, (*values) - baseline);                         \
      } else {                                                                \
        fixedBitArray.set32(i, *values);                                      \
      }                                                                       \
      ++values;                                                               \
    }                                                                         \
    return;                                                                   \
  }

    BULK_SET32_SWITCH_CASE(1)
    BULK_SET32_SWITCH_CASE(2)
    BULK_SET32_SWITCH_CASE(3)
    BULK_SET32_SWITCH_CASE(4)
    BULK_SET32_SWITCH_CASE(5)
    BULK_SET32_SWITCH_CASE(6)
    BULK_SET32_SWITCH_CASE(7)
    BULK_SET32_SWITCH_CASE(8)
    BULK_SET32_SWITCH_CASE(9)
    BULK_SET32_SWITCH_CASE(10)
    BULK_SET32_SWITCH_CASE(11)
    BULK_SET32_SWITCH_CASE(12)
    BULK_SET32_SWITCH_CASE(13)
    BULK_SET32_SWITCH_CASE(14)
    BULK_SET32_SWITCH_CASE(15)
    BULK_SET32_SWITCH_CASE(16)
    BULK_SET32_SWITCH_CASE(17)
    BULK_SET32_SWITCH_CASE(18)
    BULK_SET32_SWITCH_CASE(19)
    BULK_SET32_SWITCH_CASE(20)
    BULK_SET32_SWITCH_CASE(21)
    BULK_SET32_SWITCH_CASE(22)
    BULK_SET32_SWITCH_CASE(23)
    BULK_SET32_SWITCH_CASE(24)
    BULK_SET32_SWITCH_CASE(25)
    BULK_SET32_SWITCH_CASE(26)
    BULK_SET32_SWITCH_CASE(27)
    BULK_SET32_SWITCH_CASE(28)
    BULK_SET32_SWITCH_CASE(29)
    BULK_SET32_SWITCH_CASE(30)
    BULK_SET32_SWITCH_CASE(31)
    BULK_SET32_SWITCH_CASE(32)

#undef BULK_SET32_SWITCH_CASE

    default:
      LOG(FATAL) << "bit width must lie in [1, 32], got: "
                 << fixedBitArray.bitWidth();
  }
}

void FixedBitArray::bulkSet32(
    uint64_t start,
    uint64_t length,
    const uint32_t* values) {
  bulkSetInternal32<false>(*this, buffer_, start, length, values, 0);
}

void FixedBitArray::bulkSet32WithBaseline(
    uint64_t start,
    uint64_t length,
    const uint32_t* values,
    uint32_t baseline) {
  bulkSetInternal32<true>(*this, buffer_, start, length, values, baseline);
}

template <int bitWidth, int loopPosition>
void equals32Loop(
    const uint32_t value,
    const uint64_t equalsMask,
    uint64_t word,
    uint64_t** nextWord,
    uint64_t* outputWord) {
  constexpr uint64_t kMask = (1ULL << bitWidth) - 1ULL;
  constexpr uint64_t spillover = (loopPosition * bitWidth) % 64 == 0
      ? 0
      : ((loopPosition + 1) * bitWidth) % 64;
  if (spillover > 0) {
    uint64_t remainder = word;
    word = **nextWord;
    ++(*nextWord);
    if (((remainder | word << (bitWidth - spillover)) & kMask) == value) {
      *outputWord |= (1ULL << loopPosition);
    }
    word >>= spillover;
  } else {
    word = **nextWord;
    ++(*nextWord);
  }
  constexpr int valueCount = (64 - spillover) / bitWidth;
  uint64_t maskedWord = word ^ equalsMask;
  constexpr int offset = loopPosition + (spillover > 0);
  for (int i = 0; i < valueCount; ++i) {
    *outputWord |= static_cast<uint64_t>((maskedWord & kMask) == 0)
        << (offset + i);
    maskedWord >>= bitWidth;
  }
  constexpr int nextWordShift = valueCount * bitWidth;
  const uint64_t nextSpillWord =
      nextWordShift == 64 ? 0 : (word >> nextWordShift);
  constexpr int nextLoopPosition = offset + valueCount;
  equals32Loop<bitWidth, nextLoopPosition>(
      value, equalsMask, nextSpillWord, nextWord, outputWord);
}

#define EQUALS32_TERMINAL_CASE(bitWidth) \
  template <>                            \
  void equals32Loop<bitWidth, 64>(       \
      uint32_t value,                    \
      uint64_t equalsMask,               \
      uint64_t word,                     \
      uint64_t** nextWord,               \
      uint64_t* outputWord) {}

EQUALS32_TERMINAL_CASE(1)
EQUALS32_TERMINAL_CASE(2)
EQUALS32_TERMINAL_CASE(3)
EQUALS32_TERMINAL_CASE(4)
EQUALS32_TERMINAL_CASE(5)
EQUALS32_TERMINAL_CASE(6)
EQUALS32_TERMINAL_CASE(7)
EQUALS32_TERMINAL_CASE(8)
EQUALS32_TERMINAL_CASE(9)
EQUALS32_TERMINAL_CASE(10)
EQUALS32_TERMINAL_CASE(11)
EQUALS32_TERMINAL_CASE(12)
EQUALS32_TERMINAL_CASE(13)
EQUALS32_TERMINAL_CASE(14)
EQUALS32_TERMINAL_CASE(15)
EQUALS32_TERMINAL_CASE(16)
EQUALS32_TERMINAL_CASE(17)
EQUALS32_TERMINAL_CASE(18)
EQUALS32_TERMINAL_CASE(19)
EQUALS32_TERMINAL_CASE(20)
EQUALS32_TERMINAL_CASE(21)
EQUALS32_TERMINAL_CASE(22)
EQUALS32_TERMINAL_CASE(23)
EQUALS32_TERMINAL_CASE(24)
EQUALS32_TERMINAL_CASE(25)
EQUALS32_TERMINAL_CASE(26)
EQUALS32_TERMINAL_CASE(27)
EQUALS32_TERMINAL_CASE(28)
EQUALS32_TERMINAL_CASE(29)
EQUALS32_TERMINAL_CASE(30)
EQUALS32_TERMINAL_CASE(31)
EQUALS32_TERMINAL_CASE(32)

#undef EQUALS32_TERMINAL_CASE

void FixedBitArray::equals32(
    uint64_t start,
    uint64_t length,
    uint32_t value,
    char* bitVector) const {
  // Per the header comment we require that start be a multiple of 64.
  CHECK_EQ(start & 63, 0);
  // First build the equality mask we'll use during the loop.
  uint64_t equalsMask = 0;
  FixedBitArray maskFixedBitArray((char*)&equalsMask, bitWidth_);
  const int maskSlots = 64 / bitWidth_;
  for (int i = 0; i < maskSlots; ++i) {
    maskFixedBitArray.set(i, value);
  }
  const uint64_t loopCount = length >> 6;
  uint64_t* nextWord =
      reinterpret_cast<uint64_t*>(buffer_ + ((start * bitWidth_) >> 3));
  uint64_t* outputWord = reinterpret_cast<uint64_t*>(bitVector);
  switch (bitWidth_) {
#define EQUALS32_SWITCH_CASE(bitWidth)                                        \
  case bitWidth: {                                                            \
    for (uint64_t i = 0; i < loopCount; ++i) {                                \
      equals32Loop<bitWidth, 0>(value, equalsMask, 0, &nextWord, outputWord); \
      ++outputWord;                                                           \
    }                                                                         \
    break;                                                                    \
  }

    EQUALS32_SWITCH_CASE(1)
    EQUALS32_SWITCH_CASE(2)
    EQUALS32_SWITCH_CASE(3)
    EQUALS32_SWITCH_CASE(4)
    EQUALS32_SWITCH_CASE(5)
    EQUALS32_SWITCH_CASE(6)
    EQUALS32_SWITCH_CASE(7)
    EQUALS32_SWITCH_CASE(8)
    EQUALS32_SWITCH_CASE(9)
    EQUALS32_SWITCH_CASE(10)
    EQUALS32_SWITCH_CASE(11)
    EQUALS32_SWITCH_CASE(12)
    EQUALS32_SWITCH_CASE(13)
    EQUALS32_SWITCH_CASE(14)
    EQUALS32_SWITCH_CASE(15)
    EQUALS32_SWITCH_CASE(16)
    EQUALS32_SWITCH_CASE(17)
    EQUALS32_SWITCH_CASE(18)
    EQUALS32_SWITCH_CASE(19)
    EQUALS32_SWITCH_CASE(20)
    EQUALS32_SWITCH_CASE(21)
    EQUALS32_SWITCH_CASE(22)
    EQUALS32_SWITCH_CASE(23)
    EQUALS32_SWITCH_CASE(24)
    EQUALS32_SWITCH_CASE(25)
    EQUALS32_SWITCH_CASE(26)
    EQUALS32_SWITCH_CASE(27)
    EQUALS32_SWITCH_CASE(28)
    EQUALS32_SWITCH_CASE(29)
    EQUALS32_SWITCH_CASE(30)
    EQUALS32_SWITCH_CASE(31)
    EQUALS32_SWITCH_CASE(32)

#undef EQUALS32_SWITCH_CASE
  }
  const uint64_t remainderStart = start + (loopCount >> 6);
  const uint64_t remainderEnd = start + length;
  // Hrm actually in the case we are talking about here the final piece
  // could be a written as a single word itself.
  for (uint64_t i = remainderStart; i < remainderEnd; ++i) {
    if (get32(i) == value) {
      velox::bits::setBit(reinterpret_cast<uint8_t*>(bitVector), i - start);
    }
  }
  return;
}

} // namespace facebook::nimble
