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

#include "dwio/nimble/tablet/MetadataBuffer.h"
#include <gtest/gtest.h>
#include "dwio/nimble/tablet/Compression.h"
#include "folly/io/IOBuf.h"
#include "velox/common/memory/Memory.h"

using namespace ::facebook;

namespace {

class MetadataBufferTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::initialize({});
  }

  void SetUp() override {}

  std::shared_ptr<velox::memory::MemoryPool> rootPool_{
      velox::memory::memoryManager()->addRootPool("MetadataBufferTest")};
  std::shared_ptr<velox::memory::MemoryPool> pool_{
      rootPool_->addLeafChild("MetadataBufferTest")};
};

// Test data for various test cases
const std::string kTestData = "Hello, MetadataBuffer!";
const std::string kLargeTestData = std::string(1000, 'A');

TEST_F(MetadataBufferTest, uncompressedStringView) {
  nimble::MetadataBuffer buffer(
      *pool_, kTestData, nimble::CompressionType::Uncompressed);

  auto content = buffer.content();
  EXPECT_EQ(content, kTestData);
  EXPECT_EQ(content.size(), kTestData.size());
}

TEST_F(MetadataBufferTest, uncompressedEmptyStringView) {
  nimble::MetadataBuffer buffer(
      *pool_, "", nimble::CompressionType::Uncompressed);

  auto content = buffer.content();
  EXPECT_TRUE(content.empty());
  EXPECT_EQ(content.size(), 0);
}

TEST_F(MetadataBufferTest, zstdCompressedStringView) {
  // First compress the data
  auto compressed = nimble::ZstdCompression::compress(*pool_, kLargeTestData);
  ASSERT_TRUE(compressed.has_value());

  std::string_view compressedView{compressed->data(), compressed->size()};

  // Create buffer from compressed data
  nimble::MetadataBuffer buffer(
      *pool_, compressedView, nimble::CompressionType::Zstd);

  auto content = buffer.content();
  EXPECT_EQ(content, kLargeTestData);
  EXPECT_EQ(content.size(), kLargeTestData.size());
}

TEST_F(MetadataBufferTest, uncompressedIOBufFullRange) {
  folly::IOBuf iobuf{folly::IOBuf::COPY_BUFFER, kTestData};

  nimble::MetadataBuffer buffer(
      *pool_, iobuf, nimble::CompressionType::Uncompressed);

  auto content = buffer.content();
  EXPECT_EQ(content, kTestData);
  EXPECT_EQ(content.size(), kTestData.size());
}

TEST_F(MetadataBufferTest, uncompressedIOBufWithOffsetAndLength) {
  std::string data = "PrefixHello, MetadataBuffer!Suffix";
  folly::IOBuf iobuf{folly::IOBuf::COPY_BUFFER, data};

  size_t offset = 6; // Skip "Prefix"
  size_t length = kTestData.size();

  nimble::MetadataBuffer buffer(
      *pool_, iobuf, offset, length, nimble::CompressionType::Uncompressed);

  auto content = buffer.content();
  EXPECT_EQ(content, kTestData);
  EXPECT_EQ(content.size(), kTestData.size());
}

TEST_F(MetadataBufferTest, zstdCompressedIOBuf) {
  // First compress the data
  auto compressed = nimble::ZstdCompression::compress(*pool_, kLargeTestData);
  ASSERT_TRUE(compressed.has_value());

  folly::IOBuf iobuf{
      folly::IOBuf::COPY_BUFFER, compressed->data(), compressed->size()};

  nimble::MetadataBuffer buffer(*pool_, iobuf, nimble::CompressionType::Zstd);

  auto content = buffer.content();
  EXPECT_EQ(content, kLargeTestData);
}

TEST_F(MetadataBufferTest, zstdCompressedIOBufWithOffsetAndLength) {
  // First compress the data
  auto compressed = nimble::ZstdCompression::compress(*pool_, kLargeTestData);
  ASSERT_TRUE(compressed.has_value());

  // Add prefix and suffix
  std::string prefix = "PREFIX";
  std::string suffix = "SUFFIX";
  std::string fullData =
      prefix + std::string(compressed->data(), compressed->size()) + suffix;

  folly::IOBuf iobuf{folly::IOBuf::COPY_BUFFER, fullData};

  nimble::MetadataBuffer buffer(
      *pool_,
      iobuf,
      prefix.size(),
      compressed->size(),
      nimble::CompressionType::Zstd);

  auto content = buffer.content();
  EXPECT_EQ(content, kLargeTestData);
}

TEST_F(MetadataBufferTest, chainedIOBuf) {
  // Create a chained IOBuf
  auto iobuf = folly::IOBuf::create(10);
  iobuf->append(5);
  std::memcpy(iobuf->writableData(), "Hello", 5);

  auto next = folly::IOBuf::create(10);
  next->append(7);
  std::memcpy(next->writableData(), ", World", 7);

  iobuf->appendToChain(std::move(next));

  nimble::MetadataBuffer buffer(
      *pool_, *iobuf, nimble::CompressionType::Uncompressed);

  auto content = buffer.content();
  EXPECT_EQ(content, "Hello, World");
}

TEST_F(MetadataBufferTest, sectionConstruction) {
  nimble::MetadataBuffer buffer(
      *pool_, kTestData, nimble::CompressionType::Uncompressed);

  nimble::Section section{std::move(buffer)};

  auto content = section.content();
  EXPECT_EQ(content, kTestData);
}

TEST_F(MetadataBufferTest, sectionStringViewConversion) {
  nimble::MetadataBuffer buffer(
      *pool_, kTestData, nimble::CompressionType::Uncompressed);

  nimble::Section section{std::move(buffer)};

  std::string_view content = static_cast<std::string_view>(section);
  EXPECT_EQ(content, kTestData);
}

TEST_F(MetadataBufferTest, metadataSectionConstructor) {
  uint64_t offset = 100;
  uint32_t size = 200;
  nimble::CompressionType type = nimble::CompressionType::Zstd;

  nimble::MetadataSection section{offset, size, type};

  EXPECT_EQ(section.offset(), offset);
  EXPECT_EQ(section.size(), size);
  EXPECT_EQ(section.compressionType(), type);
}

TEST_F(MetadataBufferTest, metadataSectionDefaultConstructor) {
  nimble::MetadataSection section;

  EXPECT_EQ(section.offset(), 0);
  EXPECT_EQ(section.size(), 0);
  EXPECT_EQ(section.compressionType(), nimble::CompressionType::Uncompressed);
}

TEST_F(MetadataBufferTest, metadataSectionAccessors) {
  uint64_t offset = 12345;
  uint32_t size = 67890;
  nimble::CompressionType type = nimble::CompressionType::Zstd;

  nimble::MetadataSection section{offset, size, type};

  EXPECT_EQ(section.offset(), offset);
  EXPECT_EQ(section.size(), size);
  EXPECT_EQ(section.compressionType(), type);
}

TEST_F(MetadataBufferTest, largeDataUncompressed) {
  std::string largeData(10000, 'X');

  nimble::MetadataBuffer buffer(
      *pool_, largeData, nimble::CompressionType::Uncompressed);

  auto content = buffer.content();
  EXPECT_EQ(content.size(), largeData.size());
  EXPECT_EQ(content, largeData);
}

TEST_F(MetadataBufferTest, largeDataCompressed) {
  std::string largeData(10000, 'Y');

  // Compress the data
  auto compressed = nimble::ZstdCompression::compress(*pool_, largeData);
  ASSERT_TRUE(compressed.has_value());

  std::string_view compressedView{compressed->data(), compressed->size()};

  // Create buffer from compressed data
  nimble::MetadataBuffer buffer(
      *pool_, compressedView, nimble::CompressionType::Zstd);

  auto content = buffer.content();
  EXPECT_EQ(content, largeData);
}
} // namespace
