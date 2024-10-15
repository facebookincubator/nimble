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
#include <gtest/gtest.h>

#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "dwio/nimble/velox/ChunkedStreamDecoder.h"
#include "dwio/nimble/velox/ChunkedStreamWriter.h"

using namespace facebook;

namespace {

DEFINE_uint32(seed, 0, "Override test seed.");
DEFINE_uint32(
    min_chunk_count,
    2,
    "Minumum chunks to use in multi-chunk tests.");
DEFINE_uint32(
    max_chunk_count,
    10,
    "Maximum chunks to use in multi-chunk tests.");
DEFINE_uint32(max_chunk_size, 10, "Maximum items to store in each chunk.");

DEFINE_uint32(
    reset_iterations,
    2,
    "How many iterations to run, resetting the decoder in between.");

size_t paddedBitmapByteCount(uint32_t count) {
  return velox::bits::nbytes(count) + velox::simd::kPadding;
}

template <typename T, typename RNG>
nimble::Vector<T>
generateData(RNG& rng, velox::memory::MemoryPool& memoryPool, size_t size) {
  nimble::Vector<T> data{&memoryPool, size};
  for (auto i = 0; i < size; ++i) {
    uint64_t value =
        folly::Random::rand64(std::numeric_limits<uint64_t>::max(), rng);
    data[i] = *reinterpret_cast<T*>(&value);
    LOG(INFO) << "Data[" << i << "]: " << data[i];
  }
  return data;
}

template <typename RNG>
nimble::Vector<bool>
generateNullData(RNG& rng, velox::memory::MemoryPool& memoryPool, size_t size) {
  nimble::Vector<bool> data{&memoryPool, size};
  for (auto i = 0; i < size; ++i) {
    data[i] = static_cast<bool>(folly::Random::oneIn(2, rng));
    LOG(INFO) << "Null[" << i << "]: " << data[i];
  }
  return data;
}

class TestStreamLoader : public nimble::StreamLoader {
 public:
  explicit TestStreamLoader(std::string stream) : stream_{std::move(stream)} {}

  const std::string_view getStream() const override {
    return stream_;
  }

 private:
  const std::string stream_;
};

template <typename E>
std::unique_ptr<nimble::StreamLoader> createStream(
    velox::memory::MemoryPool& memoryPool,
    const std::vector<nimble::Vector<typename E::cppDataType>>& values,
    const std::vector<std::optional<nimble::Vector<bool>>>& nulls,
    nimble::CompressionParams compressionParams = {
        .type = nimble::CompressionType::Uncompressed}) {
  NIMBLE_ASSERT(values.size() == nulls.size(), "Data and nulls size mismatch.");

  std::string stream;

  for (auto i = 0; i < values.size(); ++i) {
    nimble::Buffer buffer{memoryPool};
    nimble::ChunkedStreamWriter writer{buffer, compressionParams};
    std::vector<std::string_view> segments;

    if (nulls[i].has_value()) {
      // Remove null entries from data
      nimble::Vector<typename E::cppDataType> data(
          &memoryPool,
          std::accumulate(
              nulls[i]->data(), nulls[i]->data() + nulls[i]->size(), 0UL));
      uint32_t offset = 0;
      for (auto j = 0; j < nulls[i]->size(); ++j) {
        if (nulls[i].value()[j]) {
          data[offset++] = values[i][j];
        }
      }
      segments = writer.encode(nimble::test::Encoder<E>::encodeNullable(
          buffer, data, nulls[i].value()));
    } else {
      segments =
          writer.encode(nimble::test::Encoder<E>::encode(buffer, values[i]));
    }

    for (const auto& segment : segments) {
      stream += segment;
    }
  }

  return std::make_unique<TestStreamLoader>(std::move(stream));
}

template <typename T>
T getValue(const std::vector<nimble::Vector<T>>& data, size_t offset) {
  size_t i = 0;
  while (data[i].size() <= offset) {
    offset -= data[i].size();
    ++i;
  }

  return data[i][offset];
}

template <typename T>
bool getNullValue(
    const std::vector<nimble::Vector<T>>& data,
    const std::vector<std::optional<nimble::Vector<bool>>>& nulls,
    size_t offset) {
  size_t i = 0;
  while (data[i].size() <= offset) {
    offset -= data[i].size();
    ++i;
  }

  if (!nulls[i].has_value()) {
    return true;
  }

  return nulls[i].value()[offset];
}

template <typename T>
void test(
    bool multipleChunks,
    bool hasNulls,
    bool skips,
    bool scatter,
    bool compress) {
  uint32_t seed = FLAGS_seed == 0 ? folly::Random::rand32() : FLAGS_seed;
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};

  auto memoryPool = velox::memory::deprecatedAddDefaultLeafMemoryPool();

  auto chunkCount = multipleChunks
      ? folly::Random::rand32(FLAGS_min_chunk_count, FLAGS_max_chunk_count, rng)
      : 1;
  std::vector<nimble::Vector<T>> data;
  std::vector<std::optional<nimble::Vector<bool>>> nulls(chunkCount);

  size_t totalSize = 0;
  for (auto i = 0; i < chunkCount; ++i) {
    auto size = folly::Random::rand32(1, FLAGS_max_chunk_size, rng);
    LOG(INFO) << "Chunk: " << i << ", Size: " << size;
    data.push_back(generateData<T>(rng, *memoryPool, size));
    if (hasNulls && folly::Random::oneIn(2, rng)) {
      nulls[i] = generateNullData(rng, *memoryPool, size);
    }
    totalSize += size;
  }

  auto streamLoader = createStream<nimble::TrivialEncoding<T>>(
      *memoryPool,
      data,
      nulls,
      compress
          ? nimble::CompressionParams{.type = nimble::CompressionType::Zstd}
          : nimble::CompressionParams{
                .type = nimble::CompressionType::Uncompressed});

  nimble::ChunkedStreamDecoder decoder{
      *memoryPool,
      std::make_unique<nimble::InMemoryChunkedStream>(
          *memoryPool, std::move(streamLoader)),
      /* metricLogger */ {}};

  for (auto batchSize = 1; batchSize <= totalSize; ++batchSize) {
    for (auto iteration = 0; iteration < FLAGS_reset_iterations; ++iteration) {
      LOG(INFO) << "batchSize: " << batchSize;
      auto offset = 0;
      while (offset < totalSize) {
        const auto outputSize = folly::Random::oneIn(5, rng)
            ? 0
            : std::min<size_t>(batchSize, totalSize - offset);
        if (skips && folly::Random::oneIn(2, rng)) {
          LOG(INFO) << "Skipping " << outputSize;
          decoder.skip(outputSize);
        } else {
          std::vector<uint8_t> scatterMap;
          std::optional<nimble::bits::Bitmap> scatterBitmap;
          auto scatterSize = outputSize;
          if (scatter) {
            scatterSize =
                folly::Random::rand32(outputSize, outputSize * 2, rng);
            scatterMap.resize(paddedBitmapByteCount(scatterSize));
            std::vector<size_t> indices(scatterSize);
            std::iota(indices.begin(), indices.end(), 0);
            std::shuffle(indices.begin(), indices.end(), rng);
            for (auto i = 0; i < outputSize; ++i) {
              nimble::bits::setBit(
                  indices[i], reinterpret_cast<char*>(scatterMap.data()));
            }
            for (auto i = 0; i < scatterSize; ++i) {
              LOG(INFO) << "Scatter[" << i << "]: "
                        << nimble::bits::getBit(
                               i, reinterpret_cast<char*>(scatterMap.data()));
            }
            scatterBitmap.emplace(scatterMap.data(), scatterSize);
          }

          if (hasNulls || scatter) {
            std::vector<T> output(scatterSize);
            std::vector<uint8_t> outputNulls(
                paddedBitmapByteCount(scatterSize));
            uint32_t count = 0;
            const auto nonNullCount = decoder.next(
                outputSize,
                output.data(),
                [&]() {
                  ++count;
                  return outputNulls.data();
                },
                scatterBitmap.has_value() ? &scatterBitmap.value() : nullptr);

            LOG(INFO) << "offset: " << offset << ", batchSize: " << batchSize
                      << ", outputSize: " << outputSize
                      << ", scatterSize: " << scatterSize
                      << ", nonNullCount: " << nonNullCount;
            if (nonNullCount != scatterSize || (outputSize == 0 && scatter)) {
              EXPECT_EQ(1, count);
              EXPECT_EQ(
                  nimble::bits::countSetBits(
                      0,
                      scatterSize,
                      reinterpret_cast<const char*>(outputNulls.data())),
                  nonNullCount);
            } else {
              EXPECT_EQ(0, count);
            }

            if (nonNullCount == scatterSize) {
              for (auto i = 0; i < scatterSize; ++i) {
                EXPECT_EQ(getValue(data, offset + i), output[i])
                    << "Index: " << i << ", Offset: " << offset;
              }
            } else {
              if (scatter) {
                size_t scatterOffset = 0;
                for (auto i = 0; i < scatterSize; ++i) {
                  if (!nimble::bits::getBit(
                          i,
                          reinterpret_cast<const char*>(scatterMap.data()))) {
                    EXPECT_FALSE(nimble::bits::getBit(
                        i, reinterpret_cast<const char*>(outputNulls.data())));
                  } else {
                    const auto isNotNull = nimble::bits::getBit(
                        i, reinterpret_cast<const char*>(outputNulls.data()));
                    EXPECT_EQ(
                        getNullValue(data, nulls, offset + scatterOffset),
                        isNotNull)
                        << "Index: " << i << ", Offset: " << offset
                        << ", scatterOffset: " << scatterOffset;
                    if (isNotNull) {
                      EXPECT_EQ(
                          getValue(data, offset + scatterOffset), output[i])
                          << "Index: " << i << ", Offset: " << offset
                          << ", scatterOffset: " << scatterOffset;
                    }
                    ++scatterOffset;
                  }
                }
              } else {
                for (auto i = 0; i < scatterSize; ++i) {
                  const auto isNotNull = nimble::bits::getBit(
                      i, reinterpret_cast<const char*>(outputNulls.data()));
                  EXPECT_EQ(getNullValue(data, nulls, offset + i), isNotNull)
                      << "Index: " << i << ", Offset: " << offset;
                  if (isNotNull) {
                    EXPECT_EQ(getValue(data, offset + i), output[i])
                        << "Index: " << i << ", Offset: " << offset;
                  }
                }
              }
            }
          } else {
            std::vector<T> output(outputSize);
            EXPECT_EQ(outputSize, decoder.next(outputSize, output.data()));

            for (auto i = 0; i < outputSize; ++i) {
              EXPECT_EQ(getValue(data, offset + i), output[i])
                  << "Index: " << i << ", Offset: " << offset;
            }
          }
        }

        offset += outputSize;
      }

      decoder.reset();
    }
  }
}

} // namespace

TEST(ChunkedStreamDecoderTests, Decode) {
  for (auto multipleChunks : {false, true}) {
    for (auto hasNulls : {false, true}) {
      for (auto skip : {false, true}) {
        for (auto scatter : {false, true}) {
          for (auto compress : {false, true}) {
            for (int i = 0; i < 3; ++i) {
              LOG(INFO) << "Interation: " << i
                        << ", Multiple Chunks: " << multipleChunks
                        << ", Has Nulls: " << hasNulls << ", Skips: " << skip
                        << ", Scatter: " << scatter;
              test<int32_t>(multipleChunks, hasNulls, skip, scatter, compress);
            }
          }
        }
      }
    }
  }
}
