// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>

#include "dwio/alpha/encodings/tests/TestUtils.h"
#include "dwio/alpha/velox/ChunkedStreamDecoder.h"
#include "dwio/alpha/velox/ChunkedStreamWriter.h"

using namespace ::facebook;

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
alpha::Vector<T>
generateData(RNG& rng, velox::memory::MemoryPool& memoryPool, size_t size) {
  alpha::Vector<T> data{&memoryPool, size};
  for (auto i = 0; i < size; ++i) {
    uint64_t value =
        folly::Random::rand64(std::numeric_limits<uint64_t>::max(), rng);
    data[i] = *reinterpret_cast<T*>(&value);
    LOG(INFO) << "Data[" << i << "]: " << data[i];
  }
  return data;
}

template <typename RNG>
alpha::Vector<bool>
generateNullData(RNG& rng, velox::memory::MemoryPool& memoryPool, size_t size) {
  alpha::Vector<bool> data{&memoryPool, size};
  for (auto i = 0; i < size; ++i) {
    data[i] = static_cast<bool>(folly::Random::oneIn(2, rng));
    LOG(INFO) << "Null[" << i << "]: " << data[i];
  }
  return data;
}

template <typename E>
std::string createStream(
    velox::memory::MemoryPool& memoryPool,
    const std::vector<alpha::Vector<typename E::cppDataType>>& values,
    const std::vector<std::optional<alpha::Vector<bool>>>& nulls,
    alpha::CompressionParams compressionParams = {
        .type = alpha::CompressionType::Uncompressed}) {
  ALPHA_ASSERT(values.size() == nulls.size(), "Data and nulls size mismatch.");

  std::string stream;

  for (auto i = 0; i < values.size(); ++i) {
    alpha::Buffer buffer{memoryPool};
    alpha::ChunkedStreamWriter writer{buffer, compressionParams};
    std::vector<std::string_view> segments;

    if (nulls[i].has_value()) {
      // Remove null entries from data
      alpha::Vector<typename E::cppDataType> data(
          &memoryPool,
          std::accumulate(
              nulls[i]->data(), nulls[i]->data() + nulls[i]->size(), 0UL));
      uint32_t offset = 0;
      for (auto j = 0; j < nulls[i]->size(); ++j) {
        if (nulls[i].value()[j]) {
          data[offset++] = values[i][j];
        }
      }
      segments = writer.encode(alpha::test::Encoder<E>::encodeNullable(
          buffer, data, nulls[i].value()));
    } else {
      segments =
          writer.encode(alpha::test::Encoder<E>::encode(buffer, values[i]));
    }

    for (const auto& segment : segments) {
      stream += segment;
    }
  }

  return stream;
}

template <typename T>
T getValue(const std::vector<alpha::Vector<T>>& data, size_t offset) {
  size_t i = 0;
  while (data[i].size() <= offset) {
    offset -= data[i].size();
    ++i;
  }

  return data[i][offset];
}

template <typename T>
bool getNullValue(
    const std::vector<alpha::Vector<T>>& data,
    const std::vector<std::optional<alpha::Vector<bool>>>& nulls,
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
  std::vector<alpha::Vector<T>> data;
  std::vector<std::optional<alpha::Vector<bool>>> nulls(chunkCount);

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

  auto stream = createStream<alpha::TrivialEncoding<T>>(
      *memoryPool,
      data,
      nulls,
      compress ? alpha::CompressionParams{.type = alpha::CompressionType::Zstd}
               : alpha::CompressionParams{
                     .type = alpha::CompressionType::Uncompressed});

  alpha::ChunkedStreamDecoder decoder{
      *memoryPool,
      std::make_unique<alpha::InMemoryChunkedStream>(*memoryPool, stream),
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
          std::optional<alpha::bits::Bitmap> scatterBitmap;
          auto scatterSize = outputSize;
          if (scatter) {
            scatterSize =
                folly::Random::rand32(outputSize, outputSize * 2, rng);
            scatterMap.resize(paddedBitmapByteCount(scatterSize));
            std::vector<size_t> indices(scatterSize);
            std::iota(indices.begin(), indices.end(), 0);
            std::shuffle(indices.begin(), indices.end(), rng);
            for (auto i = 0; i < outputSize; ++i) {
              alpha::bits::setBit(
                  indices[i], reinterpret_cast<char*>(scatterMap.data()));
            }
            for (auto i = 0; i < scatterSize; ++i) {
              LOG(INFO) << "Scatter[" << i << "]: "
                        << alpha::bits::getBit(
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
                  alpha::bits::countSetBits(
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
                  if (!alpha::bits::getBit(
                          i,
                          reinterpret_cast<const char*>(scatterMap.data()))) {
                    EXPECT_FALSE(alpha::bits::getBit(
                        i, reinterpret_cast<const char*>(outputNulls.data())));
                  } else {
                    const auto isNotNull = alpha::bits::getBit(
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
                  const auto isNotNull = alpha::bits::getBit(
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
