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

#include "dwio/nimble/tablet/Compression.h"
#include <gtest/gtest.h>
#include "dwio/nimble/tablet/MetadataBuffer.h"
#include "velox/common/memory/Memory.h"

using namespace ::facebook;

namespace {

class CompressionTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {}

  std::shared_ptr<velox::memory::MemoryPool> rootPool_{
      velox::memory::memoryManager()->addRootPool("CompressionTest")};
  std::shared_ptr<velox::memory::MemoryPool> pool_{
      rootPool_->addLeafChild("CompressionTest")};
};

// Generate a large string of random data
const std::string kLargeDataForTest = [] {
  std::mt19937 rng{std::random_device{}()};
  std::uniform_int_distribution<int> dist('A', 'Z'); // uppercase letters

  size_t kLargeTestDataSize = 2001;
  std::string data;
  data.reserve(kLargeTestDataSize);

  for (auto i = 0; i < kLargeTestDataSize; ++i) {
    auto randomChar = dist(rng);
    data.push_back(randomChar);
  }
  return data;
}();

TEST_F(CompressionTest, SimpleCompressionTest) {
  // First compress the data
  auto compressed =
      nimble::ZstdCompression::compress(*pool_, kLargeDataForTest);
  ASSERT_TRUE(compressed.has_value());

  std::string_view compressedView{compressed->data(), compressed->size()};

  // Create buffer from compressed data
  nimble::MetadataBuffer buffer(
      *pool_, compressedView, nimble::CompressionType::Zstd);

  auto content = buffer.content();
  EXPECT_EQ(content, kLargeDataForTest);
  EXPECT_EQ(content.size(), kLargeDataForTest.size());
}

} // namespace
