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

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/tablet/Compression.h"
#include "folly/io/IOBuf.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MmapAllocator.h"

using namespace ::facebook;

namespace {

velox::BufferPtr toBufferPtr(
    std::string_view data,
    velox::memory::MemoryPool* pool) {
  auto buffer = velox::AlignedBuffer::allocate<char>(data.size(), pool);
  std::memcpy(buffer->asMutable<char>(), data.data(), data.size());
  return buffer;
}

class MetadataBufferTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {}

  std::shared_ptr<velox::memory::MemoryPool> rootPool_{
      velox::memory::memoryManager()->addRootPool("MetadataBufferTest")};
  std::shared_ptr<velox::memory::MemoryPool> pool_{
      rootPool_->addLeafChild("MetadataBufferTest")};
};

const std::string kTestData = "Hello, MetadataBuffer!";
const std::string kLargeTestData = std::string(1'000, 'A');

TEST_F(MetadataBufferTest, decompressBufferPtr) {
  struct TestParam {
    std::string inputData;
    nimble::CompressionType compressionType;
    std::string debugString() const {
      return fmt::format(
          "size {}, compressionType {}",
          inputData.size(),
          static_cast<int>(compressionType));
    }
  };
  for (const auto& testData : std::vector<TestParam>{
           {"", nimble::CompressionType::Uncompressed},
           {"", nimble::CompressionType::Zstd},
           {kTestData, nimble::CompressionType::Uncompressed},
           {kLargeTestData, nimble::CompressionType::Uncompressed},
           {kLargeTestData, nimble::CompressionType::Zstd}}) {
    SCOPED_TRACE(testData.debugString());

    velox::BufferPtr inputBuffer;
    if (testData.compressionType == nimble::CompressionType::Uncompressed) {
      inputBuffer = toBufferPtr(testData.inputData, pool_.get());
    } else {
      auto compressed =
          nimble::ZstdCompression::compress(testData.inputData, pool_.get());
      ASSERT_TRUE(compressed.has_value());
      inputBuffer = std::move(compressed.value());
    }

    nimble::MetadataBuffer buffer(
        nimble::MetadataBuffer::decompress(
            std::move(inputBuffer), testData.compressionType, pool_.get()));

    auto content = buffer.content();
    EXPECT_EQ(content, testData.inputData);
    EXPECT_EQ(content.size(), testData.inputData.size());
  }
}

TEST_F(MetadataBufferTest, decompressIOBuf) {
  struct TestParam {
    std::string inputData;
    nimble::CompressionType compressionType;
    bool compressible;
    std::string prefix;
    std::string suffix;
    bool chained;
    std::string debugString() const {
      return fmt::format(
          "size {}, compressionType {}, compressible {}, prefix '{}', suffix '{}', chained {}",
          inputData.size(),
          static_cast<int>(compressionType),
          compressible,
          prefix,
          suffix,
          chained);
    }
  };
  std::vector<TestParam> testSettings;
  for (const auto& inputData : {std::string(""), kTestData, kLargeTestData}) {
    for (const auto compressionType :
         {nimble::CompressionType::Uncompressed,
          nimble::CompressionType::Zstd}) {
      const bool compressible =
          compressionType == nimble::CompressionType::Uncompressed ||
          nimble::ZstdCompression::compress(inputData, pool_.get()).has_value();
      for (bool withOffset : {false, true}) {
        for (bool chained : {false, true}) {
          testSettings.push_back(
              {inputData,
               compressionType,
               compressible,
               withOffset ? "PREFIX" : "",
               withOffset ? "SUFFIX" : "",
               chained});
        }
      }
    }
  }
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    if (!testData.compressible) {
      auto compressed =
          nimble::ZstdCompression::compress(testData.inputData, pool_.get());
      EXPECT_FALSE(compressed.has_value());
      continue;
    }

    std::string rawData;
    if (testData.compressionType == nimble::CompressionType::Zstd) {
      auto compressed =
          nimble::ZstdCompression::compress(testData.inputData, pool_.get());
      ASSERT_TRUE(compressed.has_value());
      rawData = std::string(
          compressed.value()->as<char>(), compressed.value()->size());
    } else {
      rawData = testData.inputData;
    }

    const auto offset = testData.prefix.size();
    const auto length = rawData.size();
    const std::string fullData = testData.prefix + rawData + testData.suffix;

    std::unique_ptr<folly::IOBuf> iobuf;
    if (testData.chained && fullData.size() >= 2) {
      const auto mid = fullData.size() / 2;
      iobuf = folly::IOBuf::copyBuffer(fullData.data(), mid);
      iobuf->appendToChain(
          folly::IOBuf::copyBuffer(
              fullData.data() + mid, fullData.size() - mid));
    } else {
      iobuf = folly::IOBuf::copyBuffer(fullData);
    }

    nimble::MetadataBuffer buffer(
        nimble::MetadataBuffer::decompress(
            *iobuf, offset, length, testData.compressionType, pool_.get()));

    auto content = buffer.content();
    EXPECT_EQ(content, testData.inputData);
  }
}

TEST_F(MetadataBufferTest, sectionConstruction) {
  nimble::MetadataBuffer buffer(
      nimble::MetadataBuffer::decompress(
          toBufferPtr(kTestData, pool_.get()),
          nimble::CompressionType::Uncompressed,
          pool_.get()));

  nimble::Section section{std::move(buffer)};

  const auto content = section.content();
  EXPECT_EQ(content, kTestData);
}

TEST_F(MetadataBufferTest, sectionStringViewConversion) {
  nimble::MetadataBuffer buffer(
      nimble::MetadataBuffer::decompress(
          toBufferPtr(kTestData, pool_.get()),
          nimble::CompressionType::Uncompressed,
          pool_.get()));

  nimble::Section section{std::move(buffer)};

  std::string_view content = static_cast<std::string_view>(section);
  EXPECT_EQ(content, kTestData);
}

TEST_F(MetadataBufferTest, sectionBuffer) {
  nimble::MetadataBuffer buffer(
      nimble::MetadataBuffer::decompress(
          toBufferPtr(kTestData, pool_.get()),
          nimble::CompressionType::Uncompressed,
          pool_.get()));

  nimble::Section section{std::move(buffer)};

  const nimble::MetadataBuffer& bufferRef = section.buffer();
  EXPECT_EQ(bufferRef.content(), kTestData);
  EXPECT_EQ(bufferRef.content(), section.content());
}

TEST_F(MetadataBufferTest, metadataSection) {
  {
    nimble::MetadataSection section{100, 200, nimble::CompressionType::Zstd};
    EXPECT_EQ(section.offset(), 100);
    EXPECT_EQ(section.size(), 200);
    EXPECT_EQ(section.compressionType(), nimble::CompressionType::Zstd);
    EXPECT_FALSE(section.uncompressedSize().has_value());
  }

  {
    nimble::MetadataSection section;
    EXPECT_EQ(section.offset(), 0);
    EXPECT_EQ(section.size(), 0);
    EXPECT_EQ(section.compressionType(), nimble::CompressionType::Uncompressed);
    EXPECT_FALSE(section.uncompressedSize().has_value());
  }
}

TEST_F(MetadataBufferTest, metadataSectionUncompressedSize) {
  struct TestParam {
    uint32_t size;
    nimble::CompressionType compressionType;
    std::optional<uint32_t> uncompressedSize;
    std::string debugString() const {
      return fmt::format(
          "size {}, compressionType {}, uncompressedSize {}",
          size,
          static_cast<int>(compressionType),
          uncompressedSize.has_value()
              ? std::to_string(uncompressedSize.value())
              : std::string("nullopt"));
    }
  };
  for (const auto& testData : std::vector<TestParam>{
           {50, nimble::CompressionType::Zstd, 200},
           {50, nimble::CompressionType::Zstd, std::nullopt},
           {100, nimble::CompressionType::Uncompressed, 100},
           {100, nimble::CompressionType::Uncompressed, std::nullopt}}) {
    SCOPED_TRACE(testData.debugString());
    nimble::MetadataSection section{
        0, testData.size, testData.compressionType, testData.uncompressedSize};

    EXPECT_EQ(section.size(), testData.size);
    EXPECT_EQ(section.compressionType(), testData.compressionType);
    EXPECT_EQ(section.uncompressedSize(), testData.uncompressedSize);
  }
}

TEST_F(MetadataBufferTest, metadataBufferMove) {
  // Move construction.
  {
    nimble::MetadataBuffer buffer(
        nimble::MetadataBuffer::decompress(
            toBufferPtr(kTestData, pool_.get()),
            nimble::CompressionType::Uncompressed,
            pool_.get()));

    nimble::MetadataBuffer moved{std::move(buffer)};
    EXPECT_EQ(moved.content(), kTestData);
  }

  // Move assignment.
  {
    nimble::MetadataBuffer buffer(
        nimble::MetadataBuffer::decompress(
            toBufferPtr(kTestData, pool_.get()),
            nimble::CompressionType::Uncompressed,
            pool_.get()));

    nimble::MetadataBuffer other(
        nimble::MetadataBuffer::decompress(
            toBufferPtr("other", pool_.get()),
            nimble::CompressionType::Uncompressed,
            pool_.get()));

    other = std::move(buffer);
    EXPECT_EQ(other.content(), kTestData);
  }
}

TEST_F(MetadataBufferTest, metadataSectionErrors) {
  NIMBLE_ASSERT_THROW(
      nimble::MetadataSection(
          0, 100, nimble::CompressionType::Uncompressed, 200),
      "Uncompressed size must equal size for uncompressed data");
  NIMBLE_ASSERT_THROW(
      nimble::MetadataSection(0, 100, nimble::CompressionType::Zstd, 50),
      "Uncompressed size must be >= compressed size");
}

class MetadataBufferCacheTest : public ::testing::Test {
 protected:
  static constexpr uint64_t kCacheSize = 64 << 20;

  void SetUp() override {
    velox::memory::MemoryManager::Options options;
    options.useMmapAllocator = true;
    options.allocatorCapacity = kCacheSize;
    options.arbitratorCapacity = kCacheSize;
    options.trackDefaultUsage = true;
    manager_ = std::make_unique<velox::memory::MemoryManager>(options);
    allocator_ =
        static_cast<velox::memory::MmapAllocator*>(manager_->allocator());
    cache_ = velox::cache::AsyncDataCache::create(allocator_);
    fileId_ = std::make_unique<velox::StringIdLease>(
        velox::fileIds(), "MetadataBufferCacheTest");
  }

  void TearDown() override {
    fileId_.reset();
    if (cache_ != nullptr) {
      cache_->shutdown();
    }
    cache_.reset();
    manager_.reset();
  }

  velox::cache::CachePin makePin(uint64_t cacheOffset, std::string_view data) {
    velox::cache::RawFileCacheKey cacheKey{fileId_->id(), cacheOffset};
    auto pin = cache_->findOrCreate(cacheKey, data.size());
    EXPECT_FALSE(pin.empty());
    EXPECT_TRUE(pin.entry()->isExclusive());
    auto ranges = pin.entry()->dataRanges(data.size());
    size_t offset = 0;
    for (auto& range : ranges) {
      const auto copySize = std::min(range.size(), data.size() - offset);
      std::memcpy(range.data(), data.data() + offset, copySize);
      offset += copySize;
    }
    pin.entry()->setExclusiveToShared();
    return pin;
  }

  std::unique_ptr<velox::memory::MemoryManager> manager_;
  velox::memory::MmapAllocator* allocator_{nullptr};
  std::shared_ptr<velox::cache::AsyncDataCache> cache_;
  std::unique_ptr<velox::StringIdLease> fileId_;
};

TEST_F(MetadataBufferCacheTest, cachePinData) {
  for (const auto& data : {std::string("tiny"), std::string(1'000, 'X')}) {
    SCOPED_TRACE(fmt::format("size {}", data.size()));

    {
      auto pin = makePin(0, data);
      EXPECT_FALSE(pin.empty());
      nimble::MetadataBuffer pinBuffer{std::move(pin)};
      EXPECT_EQ(pinBuffer.content(), data);
    }

    {
      auto pool = manager_->addLeafPool("bufferTest");
      auto buffer = toBufferPtr(data, pool.get());
      nimble::MetadataBuffer bufferObj{std::move(buffer)};
      EXPECT_EQ(bufferObj.content(), data);
    }
  }
}

TEST_F(MetadataBufferCacheTest, metadataBufferMoveWithPin) {
  {
    const std::string data = "move-test";
    auto pin = makePin(100, data);
    nimble::MetadataBuffer buffer{std::move(pin)};
    nimble::MetadataBuffer moved{std::move(buffer)};
    EXPECT_EQ(moved.content(), data);
  }

  {
    const std::string data1 = "first";
    const std::string data2 = "second";
    auto pin1 = makePin(200, data1);
    auto pin2 = makePin(300, data2);
    nimble::MetadataBuffer buffer1{std::move(pin1)};
    nimble::MetadataBuffer buffer2{std::move(pin2)};
    buffer2 = std::move(buffer1);
    EXPECT_EQ(buffer2.content(), data1);
  }
}

TEST_F(MetadataBufferCacheTest, cachePinSection) {
  const std::string data = "section-cache-test";
  auto pin = makePin(400, data);

  nimble::Section section{nimble::MetadataBuffer{std::move(pin)}};
  EXPECT_EQ(section.content(), data);
}

TEST_F(MetadataBufferTest, cloneBufferPtr) {
  auto buffer = toBufferPtr(kTestData, pool_.get());
  nimble::MetadataBuffer original{std::move(buffer)};
  EXPECT_EQ(original.content(), kTestData);

  auto cloned = original.clone();
  EXPECT_EQ(cloned.content(), kTestData);
  EXPECT_EQ(original.content(), cloned.content());
  EXPECT_EQ(original.content().data(), cloned.content().data());
}

TEST_F(MetadataBufferCacheTest, cloneCachePin) {
  const std::string data = "clone-pin-test";
  auto pin = makePin(500, data);
  nimble::MetadataBuffer original{std::move(pin)};
  EXPECT_EQ(original.content(), data);

  auto cloned = original.clone();
  EXPECT_EQ(cloned.content(), data);
  EXPECT_EQ(original.content(), cloned.content());
}

} // namespace
