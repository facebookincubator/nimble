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

#include "dwio/nimble/velox/ChunkedStream.h"
#include "dwio/nimble/velox/ChunkedStreamWriter.h"

using namespace ::facebook;

namespace {
template <typename RNG>
std::string randomString(RNG rng, uint32_t length) {
  std::string random;
  random.resize(folly::Random::rand32(length, rng));
  for (auto i = 0; i < random.size(); ++i) {
    random[i] = folly::Random::rand32(256, rng);
  }

  return random;
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

template <typename RNG>
std::tuple<std::vector<std::string>, std::unique_ptr<nimble::StreamLoader>>
createChunkedStream(
    RNG rng,
    nimble::Buffer& buffer,
    size_t chunkCount,
    bool compress = false) {
  std::vector<std::string> data;
  data.resize(chunkCount);
  for (auto i = 0; i < data.size(); ++i) {
    data[i] = randomString(rng, 100);
    if (compress) {
      data[i] += std::string(200, 'a');
    }
  }
  std::string result;
  for (auto i = 0; i < data.size(); ++i) {
    std::vector<std::string_view> segments;
    {
      nimble::CompressionParams compressionParams{
          .type = nimble::CompressionType::Uncompressed};
      if (compress) {
        compressionParams.type = nimble::CompressionType::Zstd;
        compressionParams.zstdLevel = 3;
      }
      nimble::ChunkedStreamWriter writer{buffer, std::move(compressionParams)};
      segments = writer.encode(data[i]);
    }
    for (const auto& segment : segments) {
      result += segment;
    }
  }

  return {data, std::make_unique<TestStreamLoader>(result)};
}

} // namespace

TEST(ChunkedStreamTests, SingleChunkNoCompression) {
  uint32_t seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};

  auto memoryPool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*memoryPool};
  auto [data, result] = createChunkedStream(rng, buffer, /* chunkCount */ 1);
  ASSERT_GT(result->getStream().size(), 0);
  EXPECT_EQ(data.size(), 1);

  nimble::InMemoryChunkedStream reader{*memoryPool, std::move(result)};
  // Run multiple times to verify that reset() is working
  for (auto i = 0; i < 3; ++i) {
    ASSERT_TRUE(reader.hasNext());
    EXPECT_EQ(
        nimble::CompressionType::Uncompressed, reader.peekCompressionType());
    auto chunk = reader.nextChunk();
    EXPECT_EQ(data[0], chunk);
    EXPECT_FALSE(reader.hasNext());
    reader.reset();
  }
}

TEST(ChunkedStreamTests, MultiChunkNoCompression) {
  uint32_t seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};

  auto memoryPool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*memoryPool};
  auto [data, result] = createChunkedStream(
      rng,
      buffer,
      /* chunkCount */ std::max(2U, folly::Random::rand32(20, rng)));
  ASSERT_GT(result->getStream().size(), 0);
  EXPECT_GE(data.size(), 2);

  nimble::InMemoryChunkedStream reader{*memoryPool, std::move(result)};
  // Run multiple times to verify that reset() is working
  for (auto i = 0; i < 3; ++i) {
    for (auto j = 0; j < data.size(); ++j) {
      ASSERT_TRUE(reader.hasNext());
      EXPECT_EQ(
          nimble::CompressionType::Uncompressed, reader.peekCompressionType());
      auto chunk = reader.nextChunk();
      EXPECT_EQ(data[j], chunk);
    }
    EXPECT_FALSE(reader.hasNext());
    reader.reset();
  }
}

TEST(ChunkedStreamTests, SingleChunkWithCompression) {
  uint32_t seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};

  auto memoryPool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*memoryPool};
  auto [data, result] =
      createChunkedStream(rng, buffer, /* chunkCount */ 1, /* compress */ true);
  ASSERT_GT(result->getStream().size(), 0);
  EXPECT_EQ(data.size(), 1);

  nimble::InMemoryChunkedStream reader{*memoryPool, std::move(result)};
  // Run multiple times to verify that reset() is working
  for (auto i = 0; i < 3; ++i) {
    ASSERT_TRUE(reader.hasNext());
    EXPECT_EQ(nimble::CompressionType::Zstd, reader.peekCompressionType());
    auto chunk = reader.nextChunk();
    EXPECT_EQ(data[0], chunk);
    EXPECT_FALSE(reader.hasNext());
    reader.reset();
  }
}

TEST(ChunkedStreamTests, MultiChunkWithCompression) {
  uint32_t seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};

  auto memoryPool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*memoryPool};
  auto [data, result] = createChunkedStream(
      rng,
      buffer,
      /* chunkCount */ std::max(2U, folly::Random::rand32(20, rng)),
      /* compress */ true);
  ASSERT_GT(result->getStream().size(), 0);
  EXPECT_GE(data.size(), 2);

  nimble::InMemoryChunkedStream reader{*memoryPool, std::move(result)};
  // Run multiple times to verify that reset() is working
  for (auto i = 0; i < 3; ++i) {
    for (auto j = 0; j < data.size(); ++j) {
      ASSERT_TRUE(reader.hasNext());
      EXPECT_EQ(nimble::CompressionType::Zstd, reader.peekCompressionType());
      auto chunk = reader.nextChunk();
      EXPECT_EQ(data[j], chunk);
    }
    EXPECT_FALSE(reader.hasNext());
    reader.reset();
  }
}
