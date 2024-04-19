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

#include <glog/logging.h>
#include <memory>
#include <span>

#include "dwio/nimble/common/BitEncoder.h"
#include "dwio/nimble/common/Bits.h"
#include "dwio/nimble/common/Vector.h"
#include "velox/common/memory/Memory.h"

// Huffman encoder/decoder specialized for speed on small numbers (up to a few
// thousand) of symbols.
//
// A huffman encoding is an entropy encoding of k symbols in the range [0, n).
// We require that the range be dense, i.e. each symbol in the range appears
// at least once. (So clearly k >= n.)
//
// The basic idea is we use the frequencies of those symbols to generate the
// optimal encoding table, which can be easily serialized. We use this table
// to encode the data into a bit stream. On the decompression side we transform
// the serialized encoding table into an lookup table structure and then use
// that structure to quickly pull bits from the stream and reconstruct the
// original entries.
//
// Example usage:
//   std::vector<uint32_t> data = /* your data on dense range [0, n) */
//   std::vector<int> counts = /* calculate the counts of your data */
//   int treeDepth;
//   auto encodingTable = GenerateHuffmanEncodingTable(counts, &treeDepth);
//   std::string encodedStream = HuffmanEncode(encodingTable, data);
//   /* store the stream somewhere, store the encoding table with it
//   /* then load it when you want to read the data...
//   DecodingTable decodingTable(encodingTable, treeDepth);
//   std::vector<uint32_t> recoveredData(data.size());
//   decodingTable.Decode(
//     data.size(), encodedStream.data(), recoveredData.data());
//   /* data and recoveredData are now the same */
//
// This code is pretty fast. For the streamed decode on a small (tree depth 10)
// encoding table, we see decode speeds as fast as 5-6 cycles/symbol.

namespace facebook::nimble::huffman {

// The length of the string returned from HuffmanEncode is the number of
// bytes required to hold the bits, plus 7 for slop space.
constexpr int kHuffmanSlopBytes = 7;

// Each entry in our encoding table is a uint32_t, where the bottom 5 bits
// give the depth of the encoding and the top bits gives the path to the leaf
// representing the entry.
std::vector<uint32_t> generateHuffmanEncodingTable(
    std::span<const uint32_t> counts,
    int* treeDepth);

// Similar to above, but enforces that tree depth be at most |maxDepth| by
// normalizing the counts (if necessary). Note that the minimal max depth is,
// of course, ceil(log2(counts.size()) -- don't ask for a lesser one!
std::vector<uint32_t> generateHuffmanEncodingTableWithMaxDepth(
    std::span<const uint32_t> counts,
    int maxDepth,
    int* treeDepth);

// Uses the provided |encodingTable| to huffman encode the |symbols|.
template <typename T>
std::string huffmanEncode(
    std::span<const uint32_t> encodingTable,
    std::span<const T> symbols);

// This decoding table only allows a max depth of up to 15 and a max encoding
// table size of 4096. Note that a depth of 15 will be considerably slower
// than <= 14 due to not fitting in the 32kb L1 cache.
class DecodingTable {
 public:
  DecodingTable(
      facebook::velox::memory::MemoryPool& memoryPool,
      std::span<const uint32_t> encodingTable,
      int treeDepth);

  // Decodes |n| symbols from |data| into |output|.
  template <typename T>
  void decode(uint32_t n, const char* data, T* output) const;

  // Similar to Decode, but simultaneously decodes two independent data streams
  // at once, extracting n symbols from each and placing them in the respective
  // |output|. This was measured to decode 50-80% more symbols/sec than Decode.
  template <typename T>
  void decodeStreamed(
      uint32_t n,
      const char* data1,
      const char* data2,
      T* output1,
      T* output2) const;

 private:
  const int treeDepth_;
  Vector<uint16_t> lookupTable_;
};

//
// End of pubic API. Implementations follow.
//

template <typename T>
std::string huffmanEncode(
    std::span<const uint32_t> encodingTable,
    std::span<const T> symbols) {
  // We could iterate through the symbols and get the total length, then
  // allocate the string. Or we could just over allocate and shrink. We'll do
  // the latter for now. 2 because our max tree depth is 15 and hence can
  // obviously fit within 2 bytes per symbol.
  std::string buffer(2 * symbols.size() + kHuffmanSlopBytes, 0);
  BitEncoder bitEncoder(buffer.data());
  for (uint32_t symbol : symbols) {
    const uint32_t entry = encodingTable[symbol];
    bitEncoder.putBits(entry >> 5, entry & 31UL);
  }
  // Don't forget the slop bytes!
  const int bytes =
      bits::bytesRequired(bitEncoder.bitsWritten()) + kHuffmanSlopBytes;
  buffer.resize(bytes);
  return buffer;
}

namespace internal {

template <typename T, int treeDepth>
void huffmanDecode(
    uint32_t n,
    const char* data,
    const uint16_t* lookupTable,
    T* output) {
  constexpr uint64_t mask = (1ULL << treeDepth) - 1ULL;
  constexpr int symbolsPerLoop = 64 / treeDepth;
  const uint32_t loopCount = n / symbolsPerLoop;
  const uint32_t remainder = n - symbolsPerLoop * loopCount;
  BitEncoder bitEncoder(data);
  uint64_t currentWord = bitEncoder.getBits(64);
  for (int i = 0; i < loopCount; ++i) {
    int bitsUsed = 0;
    for (int j = 0; j < symbolsPerLoop; ++j) {
      const uint64_t tableSlot = currentWord & mask;
      const uint16_t tableEntry = lookupTable[tableSlot];
      *output++ = tableEntry >> 4;
      bitsUsed += tableEntry & 15;
      currentWord >>= tableEntry & 15;
    }
    currentWord |= (bitEncoder.getBits(bitsUsed) << (64 - bitsUsed));
  }
  for (int i = 0; i < remainder; ++i) {
    const uint64_t tableSlot = currentWord & mask;
    const uint16_t tableEntry = lookupTable[tableSlot];
    *output++ = tableEntry >> 4;
    currentWord >>= tableEntry & 15;
  }
}

} // namespace internal

template <typename T>
void DecodingTable::decode(uint32_t n, const char* data, T* output) const {
  switch (treeDepth_) {
#define CASE(treeDepth)                           \
  case treeDepth: {                               \
    return internal::huffmanDecode<T, treeDepth>( \
        n, data, lookupTable_.data(), output);    \
  }
    CASE(1)
    CASE(2)
    CASE(3)
    CASE(4)
    CASE(5)
    CASE(6)
    CASE(7)
    CASE(8)
    CASE(9)
    CASE(10)
    CASE(11)
    CASE(12)
    CASE(13)
    CASE(14)
    CASE(15)
#undef CASE

    default: {
      LOG(FATAL) << "programming error: encountered DecodingTable w/tree depth "
                 << "output the allowed range of [1, 15]: " << treeDepth_;
    }
  }
}

namespace internal {

// Note that the zstd checks for the "bmi2" instruction set, and if available
// dispatches to a function with __attribute__((__target__("bmi2"))) set. Might
// be worth doing later.
template <typename T, int treeDepth>
void huffmanDecodeStreamed(
    uint32_t n,
    const char* data1,
    const char* data2,
    const uint16_t* lookupTable,
    T* output1,
    T* output2) {
  constexpr uint64_t mask = (1ULL << treeDepth) - 1ULL;
  constexpr int symbolsPerLoop = 64 / treeDepth;
  const uint32_t loopCount = n / symbolsPerLoop;
  const uint32_t remainder = n - symbolsPerLoop * loopCount;
  BitEncoder bitEncoder1(data1);
  uint64_t currentWord1 = bitEncoder1.getBits(64);
  BitEncoder bitEncoder2(data2);
  uint64_t currentWord2 = bitEncoder2.getBits(64);
  for (int i = 0; i < loopCount; ++i) {
    int bitsUsed1 = 0;
    int bitsUsed2 = 0;
    for (int j = 0; j < symbolsPerLoop; ++j) {
      const uint64_t tableSlot1 = currentWord1 & mask;
      const uint16_t tableEntry1 = lookupTable[tableSlot1];
      *output1++ = tableEntry1 >> 4;
      bitsUsed1 += tableEntry1 & 15;
      currentWord1 >>= tableEntry1 & 15;
      const uint64_t tableSlot2 = currentWord2 & mask;
      const uint16_t tableEntry2 = lookupTable[tableSlot2];
      *output2++ = tableEntry2 >> 4;
      bitsUsed2 += tableEntry2 & 15;
      currentWord2 >>= tableEntry2 & 15;
    }
    currentWord1 |= (bitEncoder1.getBits(bitsUsed1) << (64 - bitsUsed1));
    currentWord2 |= (bitEncoder2.getBits(bitsUsed2) << (64 - bitsUsed2));
  }
  for (int i = 0; i < remainder; ++i) {
    const uint64_t tableSlot1 = currentWord1 & mask;
    const uint16_t tableEntry1 = lookupTable[tableSlot1];
    *output1++ = tableEntry1 >> 4;
    currentWord1 >>= tableEntry1 & 15;
    const uint64_t tableSlot2 = currentWord2 & mask;
    const uint16_t tableEntry2 = lookupTable[tableSlot2];
    *output2++ = tableEntry2 >> 4;
    currentWord2 >>= tableEntry2 & 15;
  }
}

} // namespace internal

// It's actually at first a bit mysterious why this is faster than the single
// stream version. The answer seems to be that having the two streams better
// saturates the cpu pipeline (thanks to the OoO optimization window). It's
// interesting to note that 2 streams seems to be optimal: 3 at once is roughly
// the same speed (perhaps a few % faster), but 4+ is slower.
template <typename T>
void DecodingTable::decodeStreamed(
    uint32_t n,
    const char* data1,
    const char* data2,
    T* output1,
    T* output2) const {
  switch (treeDepth_) {
#define CASE(treeDepth)                                          \
  case treeDepth: {                                              \
    return internal::huffmanDecodeStreamed<T, treeDepth>(        \
        n, data1, data2, lookupTable_.data(), output1, output2); \
  }
    CASE(1)
    CASE(2)
    CASE(3)
    CASE(4)
    CASE(5)
    CASE(6)
    CASE(7)
    CASE(8)
    CASE(9)
    CASE(10)
    CASE(11)
    CASE(12)
    CASE(13)
    CASE(14)
    CASE(15)
#undef CASE

    default: {
      LOG(FATAL) << "programming error: encountered DecodingTable w/tree depth "
                 << "output the allowed range of [1, 15]: " << treeDepth_;
    }
  }
}

} // namespace facebook::nimble::huffman
