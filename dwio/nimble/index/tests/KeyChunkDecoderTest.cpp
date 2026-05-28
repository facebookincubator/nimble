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
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/selection/EncodingSelectionPolicy.h"
#include "dwio/nimble/index/KeyChunkDecoder.h"
#include "dwio/nimble/velox/ChunkedStreamWriter.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::nimble::index {
namespace {

using velox::dwio::common::SeekableArrayInputStream;

class KeyChunkDecoderTest : public ::testing::TestWithParam<EncodingType> {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = velox::memory::memoryManager()->addRootPool("KeyChunkDecoderTest");
    leafPool_ = pool_->addLeafChild("leaf");
  }

  std::string encodeChunk(const std::vector<std::string_view>& keys) {
    Buffer encodingBuffer{*leafPool_};
    auto policy =
        std::make_unique<ManualEncodingSelectionPolicy<std::string_view>>(
            std::vector<std::pair<EncodingType, float>>{{GetParam(), 1.0}},
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

TEST_P(KeyChunkDecoderTest, decodeAndMaterialize) {
  std::vector<std::string_view> keys = {"apple", "banana", "cherry"};
  auto chunkData = encodeChunk(keys);

  auto result = decodeKeyChunk(makeStream(chunkData), *leafPool_, dataBuffer_);
  ASSERT_NE(result, nullptr);
  ASSERT_NE(result->encoding, nullptr);
  EXPECT_EQ(result->encoding->rowCount(), 3);

  auto materialized = result->encoding->materialize(0, 3);
  EXPECT_EQ(materialized[0], "apple");
  EXPECT_EQ(materialized[1], "banana");
  EXPECT_EQ(materialized[2], "cherry");
}

TEST_P(KeyChunkDecoderTest, seekAndSelectiveMaterialize) {
  std::vector<std::string_view> keys = {"aaa", "bbb", "ccc", "ddd", "eee"};
  auto chunkData = encodeChunk(keys);

  auto result = decodeKeyChunk(makeStream(chunkData), *leafPool_, dataBuffer_);
  ASSERT_NE(result, nullptr);
  ASSERT_NE(result->encoding, nullptr);

  auto pos = result->encoding->seek("ccc", /*inclusive=*/true);
  ASSERT_TRUE(pos.has_value());
  EXPECT_EQ(pos.value(), 2);

  auto entries = result->encoding->materialize(2, 2);
  EXPECT_EQ(entries[0], "ccc");
  EXPECT_EQ(entries[1], "ddd");
}

TEST_P(KeyChunkDecoderTest, singleEntry) {
  std::vector<std::string_view> keys = {"only"};
  auto chunkData = encodeChunk(keys);

  auto result = decodeKeyChunk(makeStream(chunkData), *leafPool_, dataBuffer_);
  ASSERT_NE(result, nullptr);
  ASSERT_NE(result->encoding, nullptr);
  EXPECT_EQ(result->encoding->rowCount(), 1);

  auto materialized = result->encoding->materialize(0, 1);
  EXPECT_EQ(materialized[0], "only");
}

TEST_P(KeyChunkDecoderTest, materializeAfterMove) {
  std::vector<std::string_view> keys = {"foo", "bar", "baz"};
  auto chunkData = encodeChunk(keys);

  std::shared_ptr<DecodedKeyChunk> moved;
  {
    auto decoded =
        decodeKeyChunk(makeStream(chunkData), *leafPool_, dataBuffer_);
    moved = std::move(decoded);
  }

  ASSERT_NE(moved->encoding, nullptr);

  auto materialized = moved->encoding->materialize(0, 3);
  EXPECT_FALSE(moved->stringBuffers.empty());
  EXPECT_EQ(materialized[0], "foo");
  EXPECT_EQ(materialized[1], "bar");
  EXPECT_EQ(materialized[2], "baz");
}

TEST_P(KeyChunkDecoderTest, materializeAfterMoveAssign) {
  std::vector<std::string_view> keys1 = {"alpha", "beta"};
  std::vector<std::string_view> keys2 = {"gamma", "delta", "epsilon"};
  auto chunkData1 = encodeChunk(keys1);
  auto chunkData2 = encodeChunk(keys2);

  auto holder = decodeKeyChunk(makeStream(chunkData1), *leafPool_, dataBuffer_);
  holder = decodeKeyChunk(makeStream(chunkData2), *leafPool_, dataBuffer_);

  ASSERT_NE(holder->encoding, nullptr);
  EXPECT_EQ(holder->encoding->rowCount(), 3);

  auto materialized = holder->encoding->materialize(0, 3);
  EXPECT_FALSE(holder->stringBuffers.empty());
  EXPECT_EQ(materialized[0], "gamma");
  EXPECT_EQ(materialized[1], "delta");
  EXPECT_EQ(materialized[2], "epsilon");
}

INSTANTIATE_TEST_SUITE_P(
    EncodingTypes,
    KeyChunkDecoderTest,
    ::testing::Values(EncodingType::Trivial, EncodingType::Prefix),
    [](const ::testing::TestParamInfo<EncodingType>& info) {
      return info.param == EncodingType::Trivial ? "Trivial" : "Prefix";
    });

} // namespace
} // namespace facebook::nimble::index
