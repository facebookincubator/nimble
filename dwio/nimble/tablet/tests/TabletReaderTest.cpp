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
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/index/tests/TabletIndexTestUtils.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/tablet/TabletWriter.h"
#include "dwio/nimble/tablet/tests/TabletTestUtils.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"

using namespace facebook;

namespace {

class TabletReaderTest : public ::testing::Test {
 protected:
  using ChunkSpec = nimble::index::test::ChunkSpec;
  using KeyChunkSpec = nimble::index::test::KeyChunkSpec;
  using StreamSpec = nimble::index::test::StreamSpec;

  static void SetUpTestCase() {
    // Initialize a fresh global MemoryManager instance for tests to ensure
    // deterministic behavior and isolation across unit tests.
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    // Per-test setup hook. No-op for now, reserved for future initialization.
  }

  // Test case describing the expected outcome of a key lookup against the
  // tablet index. If expectedStripeIndex is std::nullopt, no matching stripe
  // should be found for the given key.
  struct LookupTestCase {
    std::string key; // lookup key (serialized)
    std::optional<uint32_t>
        expectedStripeIndex; // expected stripe index or nullopt if no match
  };

  static nimble::Stream createStream(
      nimble::Buffer& buffer,
      const StreamSpec& spec) {
    return nimble::index::test::createStream(buffer, spec);
  }

  // Create multiple data Streams from a list of specifications. Reserves
  // capacity to avoid reallocations and uses createStream(...) for each entry.
  static std::vector<nimble::Stream> createStreams(
      nimble::Buffer& buffer,
      const std::vector<StreamSpec>& specs) {
    std::vector<nimble::Stream> streams;
    streams.reserve(specs.size());
    for (const auto& spec : specs) {
      streams.push_back(createStream(buffer, spec));
    }
    return streams;
  }

  // Memory pools used by writer/reader and test buffers. rootPool_ owns pool_.
  std::shared_ptr<velox::memory::MemoryPool> rootPool_{
      velox::memory::memoryManager()->addRootPool("TabletTest")};
  std::shared_ptr<velox::memory::MemoryPool> pool_{
      rootPool_->addLeafChild("TabletTest")};
};

TEST_F(TabletReaderTest, SimpleStreamReader) {
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);

  nimble::TabletIndexConfig indexConfig{
      .columns = {"col1", "col2"},
      .sortOrders = {velox::core::kAscNullsFirst, velox::core::kAscNullsFirst},
      .enforceKeyOrder = true,
  };

  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});

  nimble::Buffer buffer{*pool_};

  {
    auto streams = createStreams(
        buffer,
        {
            {.offset = 0,
             .chunks =
                 {
                     {.rowCount = 50, .size = 10},
                     {.rowCount = 50, .size = 12},
                 }},
        });

    tabletWriter->writeStripe(100, std::move(streams));
  }

  tabletWriter->close();
  writeFile.close();

  nimble::testing::InMemoryTrackableReadFile readFile(file, false);
  auto tablet = nimble::TabletReader::create(&readFile, *pool_);

  nimble::test::TabletReaderTestHelper tabletHelper(tablet.get());
  EXPECT_EQ(tabletHelper.numStripeGroups(), 1)
      << "Expected a single stripe group";

  EXPECT_EQ(tablet->stripeCount(), 1) << "One stripe should have been written";
  EXPECT_EQ(tablet->tabletRowCount(), 100)
      << "Total row count across all stripes";
  EXPECT_EQ(tablet->stripeRowCount(0), 100)
      << "Stripe 0 should contain 100 rows";
}

} // namespace
