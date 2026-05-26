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

#include "dwio/nimble/velox/selective/ReaderBase.h"

#include <gtest/gtest.h>

#include "dwio/nimble/tablet/TabletReaderCache.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace facebook::nimble;
using namespace facebook;

namespace {

enum class CreationMode { kDirect, kCached };

} // namespace

class ReaderBaseTest : public ::testing::TestWithParam<CreationMode> {
 protected:
  static inline const velox::RowTypePtr kSchema =
      velox::ROW({"a", "b"}, {velox::BIGINT(), velox::VARCHAR()});

  static void SetUpTestSuite() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = velox::memory::memoryManager()->addLeafPool("test");
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(4);
    fileData_ = writeTestFile();
  }

  void TearDown() override {
    TabletReaderCache::testingReset();
  }

  std::string writeTestFile() {
    std::string file;
    velox::test::VectorMaker vectorMaker(pool_.get());
    auto vector = vectorMaker.rowVector(
        {"a", "b"},
        {vectorMaker.flatVector<int64_t>(200, [](auto i) { return i; }),
         vectorMaker.flatVector<velox::StringView>(200, [](auto i) {
           return velox::StringView::makeInline("val" + std::to_string(i));
         })});

    VeloxWriterOptions writerOptions;
    auto writer = std::make_unique<VeloxWriter>(
        kSchema,
        std::make_unique<velox::InMemoryWriteFile>(&file),
        *pool_,
        std::move(writerOptions));
    writer->write(vector);
    writer->close();
    return file;
  }

  velox::dwio::common::ReaderOptions makeReaderOptions() {
    velox::dwio::common::ReaderOptions opts(pool_.get());
    opts.setDataIoStats(std::make_shared<velox::io::IoStatistics>());
    opts.setMetadataIoStats(std::make_shared<velox::io::IoStatistics>());
    opts.setIndexIoStats(std::make_shared<velox::io::IoStatistics>());
    return opts;
  }

  std::shared_ptr<ReaderBase> createReaderBase() {
    auto readFile = std::make_shared<velox::InMemoryReadFile>(fileData_);
    auto readerOpts = makeReaderOptions();

    switch (GetParam()) {
      case CreationMode::kDirect: {
        auto input = std::make_unique<velox::dwio::common::BufferedInput>(
            readFile, *pool_);
        return ReaderBase::create(std::move(input), readerOpts);
      }
      case CreationMode::kCached: {
        ensureCache();
        auto tabletOpts = TabletReader::configureOptions(readerOpts);
        auto cached = cache_->get(readFile, tabletOpts);

        auto input = std::make_unique<velox::dwio::common::BufferedInput>(
            readFile, *pool_);
        return ReaderBase::create(
            std::move(input), std::move(cached), readerOpts);
      }
    }
    VELOX_UNREACHABLE();
  }

  void ensureCache() {
    if (cache_ == nullptr) {
      TabletReaderCache::Options cacheOpts;
      cacheOpts.numShards = 2;
      cacheOpts.maxEntries = 10;
      cacheOpts.executor = executor_;
      cache_ = std::make_unique<TabletReaderCache>(cacheOpts);
    }
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::string fileData_;
  std::unique_ptr<TabletReaderCache> cache_;
};

TEST_P(ReaderBaseTest, basic) {
  auto reader = createReaderBase();

  EXPECT_TRUE(reader->fileSchema()->equivalent(*kSchema));
  EXPECT_EQ(reader->fileSchema()->size(), 2);
  EXPECT_EQ(reader->fileSchema()->nameOf(0), "a");
  EXPECT_EQ(reader->fileSchema()->nameOf(1), "b");

  ASSERT_NE(reader->nimbleSchema(), nullptr);
  EXPECT_TRUE(reader->nimbleSchema()->isRow());
  EXPECT_EQ(reader->nimbleSchema()->asRow().childrenCount(), 2);

  EXPECT_EQ(reader->tablet().stripeCount(), 1);
  EXPECT_EQ(reader->tablet().tabletRowCount(), 200);
  EXPECT_EQ(reader->pool(), pool_.get());

  const auto& schemaWithId = reader->fileSchemaWithId();
  ASSERT_NE(schemaWithId, nullptr);
  EXPECT_EQ(schemaWithId->size(), 2);
  EXPECT_EQ(reader->fileSchemaWithId().get(), schemaWithId.get());
}

TEST_P(ReaderBaseTest, equivalentAcrossModes) {
  auto readFile = std::make_shared<velox::InMemoryReadFile>(fileData_);
  auto readerOpts = makeReaderOptions();

  auto inputDirect =
      std::make_unique<velox::dwio::common::BufferedInput>(readFile, *pool_);
  auto direct = ReaderBase::create(std::move(inputDirect), readerOpts);

  ensureCache();
  auto tabletOpts = TabletReader::configureOptions(readerOpts);
  auto cached = cache_->get(readFile, tabletOpts);

  auto inputCached =
      std::make_unique<velox::dwio::common::BufferedInput>(readFile, *pool_);
  auto fromCache =
      ReaderBase::create(std::move(inputCached), std::move(cached), readerOpts);

  EXPECT_TRUE(direct->fileSchema()->equivalent(*fromCache->fileSchema()));
  EXPECT_EQ(
      direct->nimbleSchema()->asRow().childrenCount(),
      fromCache->nimbleSchema()->asRow().childrenCount());
  EXPECT_EQ(direct->tablet().stripeCount(), fromCache->tablet().stripeCount());
  EXPECT_EQ(
      direct->tablet().tabletRowCount(), fromCache->tablet().tabletRowCount());
}

INSTANTIATE_TEST_CASE_P(
    AllCreationModes,
    ReaderBaseTest,
    ::testing::Values(CreationMode::kDirect, CreationMode::kCached),
    [](const ::testing::TestParamInfo<CreationMode>& info) {
      return info.param == CreationMode::kDirect ? "direct" : "cached";
    });
