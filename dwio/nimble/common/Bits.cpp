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
#include "dwio/nimble/common/Bits.h"
#include "velox/common/base/BitUtil.h"

namespace facebook::nimble::bits {

void packBitmap(std::span<const bool> bools, char* bitmap) {
  uint64_t* word = reinterpret_cast<uint64_t*>(bitmap);
  const uint64_t loopCount = bools.size() >> 6;
  const uint64_t remainder = bools.size() - (loopCount << 6);
  const bool* rawBools = bools.data();
  // TODO: use SIMD?
  for (uint64_t i = 0; i < loopCount; ++i) {
    for (int j = 0; j < 64; ++j) {
      *word |= static_cast<uint64_t>(*rawBools++) << j;
    }
    ++word;
  }
  for (int j = 0; j < remainder; ++j) {
    *word |= static_cast<uint64_t>(*rawBools++) << j;
  }
}

uint32_t countSetBits(uint32_t row, uint32_t rowCount, const char* bitmap) {
  // Count bits remaining in current partial word, then count bits in
  // each word, then finish with partial final word.
  uint32_t nonNullCount = 0;
  const uint32_t currentWord = row >> 6;
  const uint32_t currentUsedBits = row - (currentWord << 6);
  const uint64_t* word =
      reinterpret_cast<const uint64_t*>(bitmap) + currentWord;
  if (currentUsedBits) {
    // Do we only need to read bits from this one word?
    if (row + rowCount <= (currentWord << 6) + 64) {
      const uint64_t mask = (1ULL << rowCount) - 1;
      return __builtin_popcountll((*word++ >> currentUsedBits) & mask);
    }
    nonNullCount += __builtin_popcountll(*word++ >> currentUsedBits);
    rowCount -= (64 - currentUsedBits);
  }
  // We're now trued up to a word boundary.
  const uint32_t wordCount = rowCount >> 6;
  const uint32_t remainder = rowCount - (wordCount << 6);
  for (uint32_t i = 0; i < wordCount; ++i) {
    nonNullCount += __builtin_popcountll(*word++);
  }
  // Add in the bits in the remaining partial word.
  if (remainder > 0) {
    const uint64_t mask = (1ULL << remainder) - 1;
    nonNullCount += __builtin_popcountll(*word & mask);
  }
  return nonNullCount;
}

uint32_t findNonNullIndices(
    uint32_t row,
    uint32_t rowCount,
    const char* bitmap,
    uint32_t* indices) {
  uint32_t* nextIndex = indices;
  const uint32_t currentWord = row >> 6;
  const uint32_t currentIndex = currentWord << 6;
  const uint32_t currentUsedBits = row - currentIndex;
  const uint64_t* nextWord =
      reinterpret_cast<const uint64_t*>(bitmap) + currentWord;
  // Consume the remaining bits in the current word.
  int index = -1;
  if (currentUsedBits) {
    uint64_t word = *nextWord++ >> currentUsedBits;
    // Do we only need to read bits from this one word?
    if (row + rowCount <= currentIndex + 64) {
      word &= ((1ULL << rowCount) - 1);
      while (int setBit = __builtin_ffsll(word)) {
        index += setBit;
        word >>= setBit;
        *nextIndex++ = index;
      }
      return nextIndex - indices;
    }
    while (int setBit = __builtin_ffsll(word)) {
      index += setBit;
      word >>= setBit;
      *nextIndex++ = index;
    }
    index = 63 - currentUsedBits;
    rowCount -= (64 - currentUsedBits);
  }
  // We're now trued up to a word boundary.
  const uint32_t wordCount = rowCount >> 6;
  const uint32_t remainder = rowCount - (wordCount << 6);
  for (uint32_t i = 0; i < wordCount; ++i) {
    uint64_t word = *nextWord++;
    uint32_t tempIndex = index;
    constexpr uint64_t shiftEdgeCase = 1ULL << 63;
    if (word == shiftEdgeCase) {
      *nextIndex++ = tempIndex + 64;
    } else {
      while (int setBit = __builtin_ffsll(word)) {
        tempIndex += setBit;
        word >>= setBit;
        *nextIndex++ = tempIndex;
      }
    }
    index += 64;
  }
  // Parse last partial word.
  if (remainder > 0) {
    const uint64_t mask = (1ULL << remainder) - 1;
    uint64_t word = *nextWord & mask;
    while (int setBit = __builtin_ffsll(word)) {
      index += setBit;
      word >>= setBit;
      *nextIndex++ = index;
    }
  }
  return nextIndex - indices;
}

uint32_t findNullIndices(
    uint32_t row,
    uint32_t rowCount,
    const char* bitmap,
    uint32_t* indices) {
  uint32_t* nextIndex = indices;
  for (uint32_t i = 0; i < rowCount; ++i) {
    // TODO: optimize this like FindNonNullIndices.
    if (getBit(row + i, bitmap)) {
      *nextIndex++ = i;
    }
  }
  return nextIndex - indices;
}

void setBits(uint64_t begin, uint64_t end, char* bitmap) {
  auto wordPtr = reinterpret_cast<uint64_t*>(bitmap);
  velox::bits::forEachWord(
      begin,
      end,
      [wordPtr](int32_t idx, uint64_t mask) {
        wordPtr[idx] |= static_cast<uint64_t>(-1) & mask;
      },
      [wordPtr](int32_t idx) { wordPtr[idx] = static_cast<uint64_t>(-1); });
}

void clearBits(uint64_t begin, uint64_t end, char* bitmap) {
  auto wordPtr = reinterpret_cast<uint64_t*>(bitmap);
  velox::bits::forEachWord(
      begin,
      end,
      [wordPtr](int32_t idx, uint64_t mask) { wordPtr[idx] &= ~mask; },
      [wordPtr](int32_t idx) { wordPtr[idx] = 0; });
}

uint32_t
findSetBit(const char* bitmap, uint32_t begin, uint32_t end, uint32_t n) {
  if (begin >= end || n == 0) {
    return begin;
  }

  const uint64_t* wordPtr = reinterpret_cast<const uint64_t*>(bitmap);

  // Handle bits in the first partial word
  uint32_t wordIdx = begin >> 6;
  uint32_t bitOffset = begin & 63;
  uint64_t word = wordPtr[wordIdx];

  // Mask out bits before 'begin'
  word &= ~((1ULL << bitOffset) - 1);

  while (true) {
    // Count set bits in current word
    uint32_t setBitsInWord = __builtin_popcountll(word);

    if (setBitsInWord >= n) {
      // The n'th set bit is in this word
      while (n > 0) {
        uint32_t firstSetBit = __builtin_ffsll(word);
        if (firstSetBit == 0) {
          break; // No more set bits
        }

        // __builtin_ffsll returns the index plus one, so subtract 1
        --firstSetBit;

        word &= ~(1ULL << firstSetBit);
        --n;

        if (n == 0) {
          uint32_t result = (wordIdx << 6) + firstSetBit;
          return result < end ? result : end;
        }
      }
    }

    // Move to next word
    n -= setBitsInWord;
    ++wordIdx;
    bitOffset = 0;

    // Check if we've reached the end
    uint32_t nextWordStart = wordIdx << 6;
    if (nextWordStart >= end) {
      return end;
    }

    word = wordPtr[wordIdx];

    // Mask out bits beyond 'end' if this is the last word
    if (nextWordStart + 64 > end) {
      word &= (1ULL << (end - nextWordStart)) - 1;
    }

    // If no bits set in this word, continue to next word
    if (word == 0) {
      continue;
    }
  }

  return end;
}

void BitmapBuilder::copy(const Bitmap& other, uint32_t begin, uint32_t end) {
  auto source = static_cast<const char*>(other.bits());
  auto dest = static_cast<char*>(bitmap_);
  auto firstByte = begin / 8;
  if (begin % 8) {
    uint8_t mask = (1 << (begin % 8)) - 1;
    dest[firstByte] = (dest[firstByte] & mask) | (source[firstByte] & ~mask);
    ++firstByte;
  }
  // @lint-ignore CLANGSECURITY facebook-security-vulnerable-memcpy
  std::memcpy(
      dest + firstByte,
      source + firstByte,
      bits::bytesRequired(end) - firstByte);
}

} // namespace facebook::nimble::bits
