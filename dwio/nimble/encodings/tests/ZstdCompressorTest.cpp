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
#include "dwio/nimble/encodings/ZstdCompressor.h"
#include <gtest/gtest.h>
#include <cstring>
#include <random>
#include "velox/common/memory/Memory.h"

using namespace facebook::nimble;

namespace {

class TestCompressionPolicy : public CompressionPolicy {
 public:
  explicit TestCompressionPolicy(uint64_t minCompressionSize = 0) {
    compressionInfo_ = {
        .compressionType = CompressionType::Zstd,
        .minCompressionSize = minCompressionSize};
    compressionInfo_.parameters.zstd.compressionLevel = 3;
  }

  CompressionInformation compression() const override {
    return compressionInfo_;
  }

  bool shouldAccept(
      CompressionType /* compressionType */,
      uint64_t uncompressedSize,
      uint64_t compressedSize) const override {
    return compressedSize < uncompressedSize;
  }

 private:
  CompressionInformation compressionInfo_;
};

} // namespace

TEST(ZstdCompressorTest, CompressionType) {
  ZstdCompressor compressor;
  EXPECT_EQ(CompressionType::Zstd, compressor.compressionType());
}

TEST(ZstdCompressorTest, RoundTrip) {
  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  ZstdCompressor compressor;
  TestCompressionPolicy policy;

  // Create compressible data (repeated pattern)
  std::vector<char> original(1024);
  for (size_t i = 0; i < original.size(); ++i) {
    original[i] = static_cast<char>(i % 10);
  }
  std::string_view input(original.data(), original.size());

  auto result = compressor.compress(*pool, input, DataType::Int8, 0, policy);
  EXPECT_EQ(CompressionType::Zstd, result.compressionType);
  ASSERT_TRUE(result.buffer.has_value());

  std::string_view compressed(result.buffer->data(), result.buffer->size());

  auto decompressed = compressor.uncompress(
      *pool, CompressionType::Zstd, DataType::Int8, compressed);
  ASSERT_EQ(original.size(), decompressed.size());
  EXPECT_EQ(
      0, std::memcmp(original.data(), decompressed.data(), original.size()));
}

TEST(ZstdCompressorTest, UncompressedSize) {
  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  ZstdCompressor compressor;
  TestCompressionPolicy policy;

  std::vector<char> original(512);
  for (size_t i = 0; i < original.size(); ++i) {
    original[i] = static_cast<char>(i % 7);
  }
  std::string_view input(original.data(), original.size());

  auto result = compressor.compress(*pool, input, DataType::Int32, 0, policy);
  ASSERT_EQ(CompressionType::Zstd, result.compressionType);
  ASSERT_TRUE(result.buffer.has_value());

  std::string_view compressed(result.buffer->data(), result.buffer->size());
  auto size = compressor.uncompressedSize(compressed);
  ASSERT_TRUE(size.has_value());
  EXPECT_EQ(original.size(), size.value());
}

TEST(ZstdCompressorTest, IncompressibleData) {
  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  ZstdCompressor compressor;
  TestCompressionPolicy policy;

  // Generate random data that doesn't compress well
  std::mt19937 rng(42);
  std::uniform_int_distribution<int> dist(0, 255);
  std::vector<char> randomData(256);
  for (auto& byte : randomData) {
    byte = static_cast<char>(dist(rng));
  }
  std::string_view input(randomData.data(), randomData.size());

  auto result = compressor.compress(*pool, input, DataType::Int8, 0, policy);
  // Random data should not compress well; compressor returns Uncompressed
  EXPECT_EQ(CompressionType::Uncompressed, result.compressionType);
  EXPECT_FALSE(result.buffer.has_value());
}

TEST(ZstdCompressorTest, RoundTripVariousDataTypes) {
  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  ZstdCompressor compressor;
  TestCompressionPolicy policy;

  // Test with int32 data (repeating pattern to ensure compressibility)
  std::vector<int32_t> int32Data(256, 42);
  std::string_view input(
      reinterpret_cast<const char*>(int32Data.data()),
      int32Data.size() * sizeof(int32_t));

  auto result = compressor.compress(*pool, input, DataType::Int32, 0, policy);
  EXPECT_EQ(CompressionType::Zstd, result.compressionType);
  ASSERT_TRUE(result.buffer.has_value());

  std::string_view compressed(result.buffer->data(), result.buffer->size());
  auto decompressed = compressor.uncompress(
      *pool, CompressionType::Zstd, DataType::Int8, compressed);
  ASSERT_EQ(input.size(), decompressed.size());
  EXPECT_EQ(0, std::memcmp(input.data(), decompressed.data(), input.size()));
}

TEST(ZstdCompressorTest, MinCompressionSizeSkipsSmallData) {
  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  ZstdCompressor compressor;

  std::vector<char> data(50, 'a');
  std::string_view input(data.data(), data.size());

  // Use CompressionEncoder with a minCompressionSize larger than data
  TestCompressionPolicy policy(data.size() + 1);
  CompressionEncoder<int8_t> encoder{*pool, policy, DataType::Int8, input};
  EXPECT_EQ(CompressionType::Uncompressed, encoder.compressionType());
}
