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
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/index/ClusterIndexFactory.h"
#include "dwio/nimble/index/IndexKeyEncoder.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/io/Options.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/vector/FlatVector.h"

namespace facebook::nimble::index::test {
namespace {

constexpr std::string_view kCustomClusterIndexName{"test.cluster.v1"};

class RenamedIndexWriter final : public IndexWriter {
 public:
  RenamedIndexWriter(std::unique_ptr<IndexWriter> delegate, std::string name)
      : delegate_{std::move(delegate)}, name_{std::move(name)} {}

  void write(const velox::VectorPtr& input) override {
    delegate_->write(input);
  }

  void flush(
      const WriteDataFn& writeDataFn,
      const CreateMetadataSectionFn& createMetadataSectionFn) override {
    delegate_->flush(writeDataFn, createMetadataSectionFn);
  }

  std::optional<IndexDescriptor> close(
      const WriteDataFn& writeDataFn,
      const CreateMetadataSectionFn& createMetadataSectionFn) override {
    auto descriptor = delegate_->close(writeDataFn, createMetadataSectionFn);
    if (descriptor.has_value()) {
      descriptor->name = name_;
    }
    return descriptor;
  }

 private:
  std::unique_ptr<IndexWriter> delegate_;
  const std::string name_;
};

class TestClusterIndexFactory final : public ClusterIndexFactory {
 public:
  std::string_view name() const override {
    return kCustomClusterIndexName;
  }

  std::unique_ptr<IndexWriter> createWriter(
      const ClusterIndexConfig& config,
      const velox::TypePtr& inputType,
      velox::memory::MemoryPool* pool) const override {
    auto builtInConfig = config;
    builtInConfig.indexName = kClusterIndexName;
    return std::make_unique<RenamedIndexWriter>(
        clusterIndexFactory(kClusterIndexName)
            .createWriter(builtInConfig, inputType, pool),
        std::string{name()});
  }

  std::unique_ptr<IndexKeyEncoder> createKeyEncoder(
      const std::vector<std::string>& columns,
      const velox::RowTypePtr& inputType,
      const std::vector<SortOrder>& sortOrders,
      velox::memory::MemoryPool* pool) const override {
    return clusterIndexFactory(kClusterIndexName)
        .createKeyEncoder(columns, inputType, sortOrders, pool);
  }

  std::unique_ptr<ClusterIndex> createReader(
      Section rootSection,
      velox::memory::MemoryPool* pool,
      const IndexLookup::Options& options) const override {
    return clusterIndexFactory(kClusterIndexName)
        .createReader(std::move(rootSection), pool, options);
  }
};

class IndexFactoryTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    velox::filesystems::registerLocalFileSystem();
    velox::memory::MemoryManager::testingSetInstance({});
    registerClusterIndexFactory(
        std::make_shared<const TestClusterIndexFactory>());
  }

  void SetUp() override {
    rootPool_ = velox::memory::memoryManager()->addRootPool("IndexFactoryTest");
    leafPool_ = rootPool_->addLeafChild("leaf");
    tempDirectory_ = velox::common::testutil::TempDirectoryPath::create();
  }

  static velox::RowTypePtr rowType() {
    return velox::ROW({{"id", velox::INTEGER()}});
  }

  velox::RowVectorPtr makeBatch() {
    constexpr int32_t kNumRows{20};
    auto ids =
        velox::BaseVector::create(velox::INTEGER(), kNumRows, leafPool_.get());
    auto* flatIds = ids->asFlatVector<int32_t>();
    for (int32_t i = 0; i < kNumRows; ++i) {
      flatIds->set(i, i);
    }
    return std::make_shared<velox::RowVector>(
        leafPool_.get(),
        rowType(),
        nullptr,
        kNumRows,
        std::vector<velox::VectorPtr>{ids});
  }

  std::string writeFile() {
    const auto path = tempDirectory_->getPath() + "/custom_indexes";
    auto fileSystem = velox::filesystems::getFileSystem(path, {});
    auto file = fileSystem->openFileForWrite(
        path,
        {.shouldCreateParentDirectories = true,
         .shouldThrowOnFileAlreadyExists = false});
    VeloxWriterOptions options;
    options.clusterIndexConfig = ClusterIndexConfig{
        .indexName = std::string{kCustomClusterIndexName},
        .columns = {"id"},
        .enforceKeyOrder = true,
    };
    VeloxWriter writer(rowType(), std::move(file), *rootPool_, options);
    writer.write(makeBatch());
    writer.close();
    return path;
  }

  std::shared_ptr<TabletReader> openFile(
      const std::string& path,
      std::string clusterIndexName) {
    auto fileSystem = velox::filesystems::getFileSystem(path, {});
    TabletReader::Options options;
    options.loadClusterIndex = true;
    options.clusterIndexName = std::move(clusterIndexName);
    options.ioOptions.emplace(leafPool_.get())
        .setMetadataIoStats(std::make_shared<velox::io::IoStatistics>())
        .setIndexIoStats(std::make_shared<velox::io::IoStatistics>());
    return TabletReader::create(
        fileSystem->openFileForRead(path), leafPool_.get(), options);
  }

  std::string encodeKey(int32_t value) {
    auto keyVector =
        velox::BaseVector::create(velox::INTEGER(), 1, leafPool_.get());
    keyVector->asFlatVector<int32_t>()->set(0, value);
    auto input = std::make_shared<velox::RowVector>(
        leafPool_.get(),
        rowType(),
        nullptr,
        1,
        std::vector<velox::VectorPtr>{keyVector});
    auto encoder = clusterIndexFactory(kCustomClusterIndexName)
                       .createKeyEncoder(
                           {"id"},
                           rowType(),
                           {SortOrder{.ascending = true}},
                           leafPool_.get());
    Buffer buffer{*leafPool_};
    std::vector<std::string_view> keys;
    encoder->encode(
        input, keys, [&buffer](size_t size) { return buffer.reserve(size); });
    return std::string{keys.front()};
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> leafPool_;
  std::shared_ptr<velox::common::testutil::TempDirectoryPath> tempDirectory_;
};

TEST_F(IndexFactoryTest, customClusterFactoryEndToEnd) {
  const auto path = writeFile();
  auto tablet = openFile(path, std::string{kCustomClusterIndexName});
  ASSERT_NE(tablet->clusterIndex(), nullptr);
  const auto key = encodeKey(7);
  auto clusterResult = tablet->clusterIndex()->lookup(
      IndexLookup::LookupRequest::pointLookup({key}));
  ASSERT_EQ(clusterResult.size(), 1);
  EXPECT_FALSE(clusterResult[0].empty());

  auto wrongClusterName = openFile(path, std::string{kClusterIndexName});
  EXPECT_EQ(wrongClusterName->clusterIndex(), nullptr);
}

TEST_F(IndexFactoryTest, builtInClusterFactoryIsRegistered) {
  EXPECT_EQ(clusterIndexFactory(kClusterIndexName).name(), kClusterIndexName);
}

TEST_F(IndexFactoryTest, rejectDuplicateClusterFactoryRegistration) {
  NIMBLE_ASSERT_THROW(
      registerClusterIndexFactory(
          std::make_shared<const TestClusterIndexFactory>()),
      "Cluster index factory already registered: test.cluster.v1");
}

TEST_F(IndexFactoryTest, rejectUnknownClusterFactory) {
  NIMBLE_ASSERT_THROW(
      clusterIndexFactory("test.missing.cluster.v1"),
      "Unknown cluster index factory: test.missing.cluster.v1");
}

} // namespace
} // namespace facebook::nimble::index::test
