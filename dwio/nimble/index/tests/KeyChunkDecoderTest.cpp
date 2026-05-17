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

#include <gtest/gtest.h>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/selection/EncodingSelectionPolicy.h"
#include "dwio/nimble/index/KeyChunkDecoder.h"
#include "dwio/nimble/velox/ChunkedStreamWriter.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::nimble::index {
namespace {

using velox::dwio::common::SeekableArrayInputStream;

class KeyChunkDecoderTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = velox::memory::memoryManager()->addRootPool("KeyChunkDecoderTest");
    leafPool_ = pool_->addLeafChild("leaf");
  }

  // Encodes string keys into a chunk stream blob using Trivial encoding.
  std::string encodeChunk(const std::vector<std::string_view>& keys) {
    Buffer encodingBuffer{*leafPool_};
    auto policy =
        std::make_unique<ManualEncodingSelectionPolicy<std::string_view>>(
            std::vector<std::pair<EncodingType, float>>{
                {EncodingType::Trivial, 1.0}},
            CompressionOptions{},
            std::nullopt);
    auto encoded = EncodingFactory::encode<std::string_view>(
        std::move(policy), keys, encodingBuffer);

    ChunkedStreamWriter writer{encodingBuffer};
    auto segments = writer.encode(encoded);
    std::string result;
    for (const auto& segment : segments) {
      result.append(segment.data(), segment.size());
    }
    return result;
  }

  std::unique_ptr<SeekableArrayInputStream> makeStream(
      const std::string& data) {
    return std::make_unique<SeekableArrayInputStream>(
        reinterpret_cast<const unsigned char*>(data.data()), data.size());
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::shared_ptr<velox::memory::MemoryPool> leafPool_;
  velox::BufferPtr dataBuffer_;
};

TEST_F(KeyChunkDecoderTest, decodeAndMaterialize) {
  std::vector<std::string_view> keys = {"apple", "banana", "cherry"};
  auto chunkData = encodeChunk(keys);

  auto result = decodeKeyChunk(makeStream(chunkData), *leafPool_, dataBuffer_);
  ASSERT_NE(result, nullptr);
  ASSERT_NE(result->encoding, nullptr);
  EXPECT_EQ(result->encoding->rowCount(), 3);
  EXPECT_EQ(result->encoding->encodingType(), EncodingType::Trivial);

  std::vector<std::string_view> materialized(3);
  result->encoding->materialize(3, materialized.data());
  EXPECT_EQ(materialized[0], "apple");
  EXPECT_EQ(materialized[1], "banana");
  EXPECT_EQ(materialized[2], "cherry");
}

TEST_F(KeyChunkDecoderTest, seekAndSelectiveMaterialize) {
  std::vector<std::string_view> keys = {"aaa", "bbb", "ccc", "ddd", "eee"};
  auto chunkData = encodeChunk(keys);

  auto result = decodeKeyChunk(makeStream(chunkData), *leafPool_, dataBuffer_);
  ASSERT_NE(result, nullptr);
  ASSERT_NE(result->encoding, nullptr);
  EXPECT_EQ(result->encoding->rowCount(), 5);

  // Seek to "ccc" (position 2).
  const std::string_view target = "ccc";
  auto pos = result->encoding->seek(&target, /*inclusive=*/true);
  ASSERT_TRUE(pos.has_value());
  EXPECT_EQ(pos.value(), 2);

  // Selectively materialize 2 entries from position 2.
  result->encoding->reset();
  result->encoding->skip(2);
  std::vector<std::string_view> entries(2);
  result->encoding->materialize(2, entries.data());
  EXPECT_EQ(entries[0], "ccc");
  EXPECT_EQ(entries[1], "ddd");
}

TEST_F(KeyChunkDecoderTest, singleEntry) {
  std::vector<std::string_view> keys = {"only"};
  auto chunkData = encodeChunk(keys);

  auto result = decodeKeyChunk(makeStream(chunkData), *leafPool_, dataBuffer_);
  ASSERT_NE(result, nullptr);
  ASSERT_NE(result->encoding, nullptr);
  EXPECT_EQ(result->encoding->rowCount(), 1);

  std::vector<std::string_view> materialized(1);
  result->encoding->materialize(1, materialized.data());
  EXPECT_EQ(materialized[0], "only");
}

} // namespace
} // namespace facebook::nimble::index
