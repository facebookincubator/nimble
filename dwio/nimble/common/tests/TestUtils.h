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

#include <cstdint>
#include <string_view>
#include <type_traits>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "folly/Random.h"
#include "folly/Synchronized.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"

// Utilities to support testing in nimble.

namespace facebook::nimble::testing {

// Adds random data covering the whole range of the data type.
template <typename T, typename RNG>
void addRandomData(RNG&& rng, int rowCount, Vector<T>* data, Buffer* buffer);

// Draw an int uniformly from [min, max).
class Util {
 public:
  Util(velox::memory::MemoryPool& memoryPool) : memoryPool_(memoryPool) {}

  // Makes between 1 and maxRows random values over T's data range.
  // For strings we improvise a bit.
  template <typename T, typename RNG>
  Vector<T> makeRandomData(RNG&& rng, uint32_t maxRows, Buffer* buffer);

  // A random length vector of all the same data.
  template <typename T, typename RNG>
  Vector<T> makeConstantData(RNG&& rng, uint32_t maxRows, Buffer* buffer);

  // A random length vector of runs of various lengths.
  template <typename T, typename RNG>
  Vector<T> makeRLEData(RNG&& rng, uint32_t maxRows, Buffer* buffer);

  // Makes data compatible with our HuffmanColumn (namely in the
  // range [0, 4096)) for integer data only.
  template <typename T, typename RNG>
  Vector<T> makeHuffmanData(RNG&& rng, uint32_t maxRows, Buffer* buffer);

  // A Vector of all the Make* Vectors.
  template <typename T, typename RNG>
  std::vector<Vector<T>>
  makeDataPatterns(RNG&& rng, uint32_t maxRows, Buffer* buffer);

  // Trims data down so that it will be friendly to sums, i.e. so it won't
  // overflow the sum type. This mainly applies to 64-bit integers.
  template <typename T>
  Vector<T> sumFriendlyData(const Vector<T>& data);

  inline uint64_t uniformRandom(uint64_t min, uint64_t max) {
    CHECK_LT(min, max);
    return min + (folly::Random::rand32() % (max - min));
  }

  // Draw an int uniformly from [0, max);
  inline uint64_t uniformRandom(uint64_t max) {
    return uniformRandom(0, max);
  }

 private:
  velox::memory::MemoryPool& memoryPool_;
};

//
// End of public API. Implementation follows.
//

template <typename T, typename RNG>
inline void
addRandomData(RNG&& rng, int rowCount, Vector<T>* data, Buffer* /* buffer */) {
  // Half the time only add positive data.
  if (!std::is_signed_v<T> || folly::Random::rand32() % 2) {
    for (int i = 0; i < rowCount; ++i) {
      if constexpr (sizeof(T) > 4) {
        const uint64_t rand = folly::Random::rand64(std::forward<RNG>(rng));
        data->push_back(*reinterpret_cast<const T*>(&rand));

      } else {
        const uint32_t rand = folly::Random::rand32(std::forward<RNG>(rng));
        data->push_back(*reinterpret_cast<const T*>(&rand));
      }
    }
  } else {
    for (int i = 0; i < rowCount; ++i) {
      if constexpr (sizeof(T) > 4) {
        const uint64_t rand =
            folly::Random::rand64(std::forward<RNG>(rng)) & ((1ULL << 63) - 1);
        data->push_back(*reinterpret_cast<const T*>(&rand));
      } else {
        const uint32_t rand =
            folly::Random::rand32(std::forward<RNG>(rng)) & ((1U << 31) - 1);
        data->push_back(*reinterpret_cast<const T*>(&rand));
      }
    }
  }
}

template <typename T = float, typename RNG>
inline void addRandomData(
    RNG&& rng,
    int rowCount,
    Vector<float>* data,
    Buffer* /* buffer */) {
  for (int i = 0; i < rowCount; ++i) {
    const uint32_t rand = folly::Random::rand32(std::forward<RNG>(rng));
    data->push_back(static_cast<float>(rand));
  }
}

template <typename T = double, typename RNG>
inline void addRandomData(
    RNG&& rng,
    int rowCount,
    Vector<double>* data,
    Buffer* /* buffer */) {
  for (int i = 0; i < rowCount; ++i) {
    const uint64_t rand = folly::Random::rand64(std::forward<RNG>(rng));
    data->push_back(static_cast<double>(rand));
  }
}

template <typename T = std::string_view, typename RNG>
inline void addRandomData(
    RNG&& rng,
    int rowCount,
    Vector<std::string_view>* data,
    Buffer* buffer) {
  for (int i = 0; i < rowCount; ++i) {
    // This is a bit arbitrary, but lets stick to 50 char max string length.
    // We can have some separate tests for large strings if we want.
    const int len = folly::Random::rand32(std::forward<RNG>(rng)) % 50;
    char* pos = buffer->reserve(len);
    for (int j = 0; j < len; ++j) {
      pos[j] = folly::Random::rand32(std::forward<RNG>(rng)) % 256;
    }
    data->emplace_back(pos, len);
  }
}

template <typename T = bool, typename RNG>
inline void addRandomData(
    RNG&& rng,
    int rowCount,
    Vector<bool>* data,
    Buffer* /* buffer */) {
  for (int i = 0; i < rowCount; ++i) {
    data->push_back(folly::Random::rand32(std::forward<RNG>(rng)) & 1);
  }
}

template <typename T, typename RNG>
Vector<T> Util::makeRandomData(RNG&& rng, uint32_t maxRows, Buffer* buffer) {
  Vector<T> randomData(&memoryPool_);
  const int rowCount =
      1 + folly::Random::rand32(std::forward<RNG>(rng)) % maxRows;
  randomData.reserve(rowCount);
  addRandomData<T, RNG>(std::forward<RNG>(rng), rowCount, &randomData, buffer);
  return randomData;
}

template <typename T, typename RNG>
Vector<T> Util::makeConstantData(RNG&& rng, uint32_t maxRows, Buffer* buffer) {
  Vector<T> data = makeRandomData<T>(std::forward<RNG>(rng), maxRows, buffer);
  for (int i = 1; i < data.size(); ++i) {
    data[i] = data[0];
  }
  return data;
}

template <typename T, typename RNG>
Vector<T> Util::makeRLEData(RNG&& rng, uint32_t maxRows, Buffer* buffer) {
  Vector<T> data = makeRandomData<T>(std::forward<RNG>(rng), maxRows, buffer);
  int index = 0;
  while (index < data.size()) {
    const uint32_t runLength = data.size() - index == 1 ? 1
                                                        : 1 +
            folly::Random::rand32(std::forward<RNG>(rng)) %
                (data.size() - index - 1);
    for (int i = 0; i < runLength; ++i) {
      data[index + i] = data[index];
    }
    index += runLength;
  }
  return data;
}

template <typename T, typename RNG>
Vector<T>
Util::makeHuffmanData(RNG&& rng, uint32_t maxRows, Buffer* /* buffer */) {
  const int rowCount =
      1 + folly::Random::rand32(std::forward<RNG>(rng)) % maxRows;
  Vector<T> huffmanData(&memoryPool_);
  huffmanData.reserve(rowCount);
  // Half the time draw all the symbols from within [0, symbolCount),
  // emulating encoding a dictionary index, and the other half of the time
  // emulate encoding a normal stream by drawing from the full [0, 4096) range.
  const int symbolCount =
      1 + folly::Random::rand32(std::forward<RNG>(rng)) % rowCount;
  const int maxValue =
      (folly::Random::rand32(std::forward<RNG>(rng)) & 1) ? symbolCount : 4096;
  for (uint32_t i = 0; i < rowCount; ++i) {
    huffmanData.push_back(
        folly::Random::rand32(std::forward<RNG>(rng)) % maxValue);
  }
  return huffmanData;
}

template <typename T, typename RNG>
std::vector<Vector<T>>
Util::makeDataPatterns(RNG&& rng, uint32_t maxRows, Buffer* buffer) {
  std::vector<Vector<T>> patterns;
  patterns.push_back(
      makeRandomData<T>(std::forward<RNG>(rng), maxRows, buffer));
  patterns.push_back(
      makeConstantData<T>(std::forward<RNG>(rng), maxRows, buffer));
  patterns.push_back(makeRLEData<T>(std::forward<RNG>(rng), maxRows, buffer));
  if constexpr (isIntegralType<T>()) {
    patterns.push_back(
        makeHuffmanData<T>(std::forward<RNG>(rng), maxRows, buffer));
  }
  return patterns;
}

template <typename T>
Vector<T> sumFriendlyDataHelper(
    velox::memory::MemoryPool&,
    const Vector<T>& data) {
  return data;
}

template <typename T>
Vector<T> Util::sumFriendlyData(const Vector<T>& data) {
  return data;
}

template <>
inline Vector<int64_t> Util::sumFriendlyData(const Vector<int64_t>& data) {
  Vector<int64_t> sumData(&memoryPool_);
  // 15 is somewhat arbitrary shift, but this is testing code, so meh.
  for (int64_t datum : data) {
    sumData.push_back(datum >> 15);
  }
  return sumData;
}

template <>
inline Vector<uint64_t> Util::sumFriendlyData(const Vector<uint64_t>& data) {
  Vector<uint64_t> sumData(&memoryPool_);
  // 15 is somewhat arbitrary shift, but this is testing code, so meh.
  for (uint64_t datum : data) {
    sumData.push_back(datum >> 15);
  }
  return sumData;
}

struct Chunk {
  uint64_t offset;
  uint64_t size;
};

// Wrapper around InMemoryReadFile (can't inherit, as InMemoryReadFile is final)
// which tracks all offsets and sizes being read. This is used to verify Nimble
// reader coalese behavior.
class InMemoryTrackableReadFile final : public velox::ReadFile {
 public:
  explicit InMemoryTrackableReadFile(
      std::string_view file,
      bool shouldProduceChainedBuffers)
      : file_{file},
        shouldProduceChainedBuffers_{shouldProduceChainedBuffers} {}

  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const final {
    chunks_.wlock()->push_back({offset, length});
    return file_.pread(offset, length, buf);
  }

  std::string pread(uint64_t offset, uint64_t length) const final {
    chunks_.wlock()->push_back({offset, length});
    return file_.pread(offset, length);
  }

  uint64_t preadv(
      uint64_t /* offset */,
      const std::vector<folly::Range<char*>>& /* buffers */) const final {
    NIMBLE_NOT_SUPPORTED("Not used by Nimble");
  }

  uint64_t preadv(
      folly::Range<const velox::common::Region*> regions,
      folly::Range<folly::IOBuf*> iobufs) const override {
    VELOX_CHECK_EQ(regions.size(), iobufs.size());
    uint64_t length = 0;
    for (size_t i = 0; i < regions.size(); ++i) {
      const auto& region = regions[i];
      length += region.length;
      auto& output = iobufs[i];
      if (shouldProduceChainedBuffers_) {
        chunks_.wlock()->push_back({region.offset, region.length});
        uint64_t splitPoint = region.length / 2;
        output = folly::IOBuf(folly::IOBuf::CREATE, splitPoint);
        file_.pread(region.offset, splitPoint, output.writableData());
        output.append(splitPoint);
        const uint64_t nextLength = region.length - splitPoint;
        auto next = folly::IOBuf::create(nextLength);
        file_.pread(
            region.offset + splitPoint, nextLength, next->writableData());
        next->append(nextLength);
        output.appendChain(std::move(next));
      } else {
        output = folly::IOBuf(folly::IOBuf::CREATE, region.length);
        pread(region.offset, region.length, output.writableData());
        output.append(region.length);
      }
    }

    return length;
  }

  uint64_t size() const final {
    return file_.size();
  }

  uint64_t memoryUsage() const final {
    return file_.memoryUsage();
  }

  // Mainly for testing. Coalescing isn't helpful for in memory data.
  void setShouldCoalesce(bool shouldCoalesce) {
    file_.setShouldCoalesce(shouldCoalesce);
  }

  bool shouldCoalesce() const final {
    return file_.shouldCoalesce();
  }

  std::vector<Chunk> chunks() {
    return *chunks_.rlock();
  }

  void resetChunks() {
    chunks_.wlock()->clear();
  }

  std::string getName() const override {
    return "<InMemoryTrackableReadFile>";
  }

  uint64_t getNaturalReadSize() const override {
    return 1024;
  }

 private:
  velox::InMemoryReadFile file_;
  bool shouldProduceChainedBuffers_;
  mutable folly::Synchronized<std::vector<Chunk>> chunks_;
};

} // namespace facebook::nimble::testing
