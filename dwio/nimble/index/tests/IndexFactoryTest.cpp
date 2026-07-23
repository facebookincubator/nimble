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

#include <atomic>
#include <thread>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/index/ClusterIndexFactory.h"
#include "dwio/nimble/index/DenseIndexFactory.h"
#include "dwio/nimble/index/DenseIndexRegistry.h"
#include "dwio/nimble/index/IndexKeyEncoder.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/IndexGenerated.h"
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
constexpr std::string_view kCustomDenseIndexName{"test.dense.v1"};

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

class TestDenseIndexReader final : public DenseIndexReader {
 public:
  const IndexLookup* findIndex(const std::vector<std::string>&) const override {
    return nullptr;
  }

  std::vector<std::vector<std::string>> indexColumns() const override {
    return {{"id"}};
  }
};

class TestDenseIndexFactory final : public DenseIndexFactory {
 public:
  explicit TestDenseIndexFactory(
      std::string name = std::string{kCustomDenseIndexName})
      : name_{std::move(name)} {}

  std::string_view name() const override {
    return name_;
  }

  std::unique_ptr<DenseIndexReader> createReader(
      Section,
      const IndexLookup::Options&,
      velox::memory::MemoryPool*) const override {
    return std::make_unique<TestDenseIndexReader>();
  }

 private:
  const std::string name_;
};

class IndexFactoryTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    velox::filesystems::registerLocalFileSystem();
    velox::memory::MemoryManager::testingSetInstance({});
    registerClusterIndexFactory(
        std::make_shared<const TestClusterIndexFactory>());
    registerDenseIndexFactory(std::make_shared<const TestDenseIndexFactory>());
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

  std::unique_ptr<VeloxWriter> makeWriter(
      std::string_view pathSuffix,
      VeloxWriterOptions options) {
    const auto path = tempDirectory_->getPath() + "/" + std::string{pathSuffix};
    auto fileSystem = velox::filesystems::getFileSystem(path, {});
    auto file = fileSystem->openFileForWrite(
        path,
        {.shouldCreateParentDirectories = true,
         .shouldThrowOnFileAlreadyExists = false});
    return std::make_unique<VeloxWriter>(
        rowType(), std::move(file), *rootPool_, std::move(options));
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
    options.loadDenseIndexes = true;
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

TEST_F(IndexFactoryTest, builtInDenseFactoriesAreRegistered) {
  EXPECT_EQ(
      denseIndexFactory(kDenseHashIndexName)->name(), kDenseHashIndexName);
  EXPECT_EQ(
      denseIndexFactory(kDenseSortedIndexName)->name(), kDenseSortedIndexName);
}

TEST_F(IndexFactoryTest, rejectDuplicateClusterFactoryRegistration) {
  NIMBLE_ASSERT_THROW(
      registerClusterIndexFactory(
          std::make_shared<const TestClusterIndexFactory>()),
      "Index factory 'test.cluster.v1' is already registered for family 'cluster'");
}

TEST_F(IndexFactoryTest, rejectDuplicateDenseFactoryRegistration) {
  NIMBLE_ASSERT_THROW(
      registerDenseIndexFactory(
          std::make_shared<const TestDenseIndexFactory>()),
      "Index factory 'test.dense.v1' is already registered for family 'dense'");
}

TEST_F(IndexFactoryTest, rejectUnknownClusterFactory) {
  NIMBLE_ASSERT_THROW(
      clusterIndexFactory("test.missing.cluster.v1"),
      "Unknown index factory 'test.missing.cluster.v1' for family 'cluster'");
}

TEST_F(IndexFactoryTest, rejectUnknownDenseFactory) {
  EXPECT_EQ(denseIndexFactory("test.missing.dense.v1"), nullptr);
}

TEST_F(IndexFactoryTest, rejectDuplicateDenseIndex) {
  auto file = std::make_shared<velox::InMemoryReadFile>(std::string{});
  auto ioStats = std::make_shared<velox::io::IoStatistics>();
  velox::io::ReaderOptions ioOptions{leafPool_.get()};
  ioOptions.setMetadataIoStats(ioStats);
  ioOptions.setIndexIoStats(ioStats);
  IndexLookup::Options options{.file = file, .ioOptions = &ioOptions};

  auto makeSection = [&] {
    return Section{MetadataBuffer{
        velox::AlignedBuffer::allocate<char>(1, leafPool_.get())}};
  };
  std::vector<DenseIndexRegistry::Entry> entries;
  entries.emplace_back(
      DenseIndexRegistry::Entry{
          std::string{kCustomDenseIndexName}, makeSection()});
  entries.emplace_back(
      DenseIndexRegistry::Entry{
          std::string{kCustomDenseIndexName}, makeSection()});

  NIMBLE_ASSERT_THROW(
      DenseIndexRegistry::create(std::move(entries), options, leafPool_.get()),
      "Duplicate dense index 'test.dense.v1' columns");
}

TEST_F(IndexFactoryTest, concurrentFactoryAccess) {
  constexpr size_t kFactoryCount = 32;
  constexpr size_t kReaderCount = 8;
  std::vector<std::shared_ptr<const TestDenseIndexFactory>> factories;
  factories.reserve(kFactoryCount);
  for (size_t i = 0; i < kFactoryCount; ++i) {
    factories.emplace_back(
        std::make_shared<const TestDenseIndexFactory>(
            "test.concurrent.dense." + std::to_string(i)));
  }

  std::vector<std::thread> threads;
  threads.reserve(kFactoryCount);
  for (const auto& factory : factories) {
    threads.emplace_back([factory] { registerDenseIndexFactory(factory); });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  std::atomic_bool lookupFailed{false};
  threads.clear();
  threads.reserve(kReaderCount);
  for (size_t i = 0; i < kReaderCount; ++i) {
    threads.emplace_back([&factories, &lookupFailed] {
      for (const auto& factory : factories) {
        if (denseIndexFactory(factory->name()) != factory.get()) {
          lookupFailed = true;
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_FALSE(lookupFailed);
}

TEST_F(IndexFactoryTest, preserveTypedIndexDescriptorOrder) {
  VeloxWriterOptions options;
  options.hashIndexConfigs.emplace_back(HashIndexConfig{.columns = {"id"}});
  options.sortedIndexConfigs.emplace_back(SortedIndexConfig{.columns = {"id"}});
  options.clusterIndexConfig =
      ClusterIndexConfig{.columns = {"id"}, .enforceKeyOrder = true};
  auto writer = makeWriter("typed_descriptor_order", std::move(options));
  writer->write(makeBatch());
  writer->close();

  const auto path = tempDirectory_->getPath() + "/typed_descriptor_order";
  auto fileSystem = velox::filesystems::getFileSystem(path, {});
  TabletReader::Options readerOptions;
  readerOptions.ioOptions.emplace(leafPool_.get())
      .setMetadataIoStats(std::make_shared<velox::io::IoStatistics>());
  auto tablet = TabletReader::create(
      fileSystem->openFileForRead(path), leafPool_.get(), readerOptions);
  auto section = tablet->loadOptionalSection(std::string{kIndexSection});
  ASSERT_TRUE(section.has_value());
  const auto* root =
      flatbuffers::GetRoot<serialization::IndexRoot>(section->content().data());
  ASSERT_NE(root->indexes(), nullptr);
  std::vector<std::string> names;
  for (const auto* descriptor : *root->indexes()) {
    names.emplace_back(descriptor->name()->str());
  }
  EXPECT_EQ(
      names,
      (std::vector<std::string>{
          std::string{kDenseHashIndexName},
          std::string{kDenseSortedIndexName},
          std::string{kClusterIndexName}}));
}

} // namespace
} // namespace facebook::nimble::index::test
