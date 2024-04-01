// Copyright 2004-present Facebook. All Rights Reserved.

#include "dwio/alpha/common/Bits.h"
#include "velox/common/base/BitUtil.h"

namespace facebook::alpha::bits {

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
  // TODO: maybe there is something fancy in bmi2
  while (begin < end) {
    if (getBit(begin, bitmap) && --n == 0) {
      break;
    }
    ++begin;
  }
  return begin;
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

} // namespace facebook::alpha::bits
