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
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/system/HardwareConcurrency.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <map>
#include <set>

#include "velox/common/testutil/TestValue.h"

#include <utility>
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/common/tests/NimbleFileWriter.h"
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/encodings/PrefixEncoding.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingLayout.h"
#include "dwio/nimble/encodings/common/EncodingUtils.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "dwio/nimble/index/KeyEncoding.h"
#include "dwio/nimble/index/tests/ClusterIndexTestUtils.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/FileLayout.h"
#include "dwio/nimble/tablet/tests/TabletTestUtils.h"
#include "dwio/nimble/velox/ChunkedStream.h"
#include "dwio/nimble/velox/EncodingLayoutTree.h"
#include "dwio/nimble/velox/FlushPolicy.h"
#include "dwio/nimble/velox/SchemaReader.h"
#include "dwio/nimble/velox/SchemaSerialization.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "dwio/nimble/velox/StatsGenerated.h"
#include "dwio/nimble/velox/VeloxReader.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "dwio/nimble/velox/stats/VectorizedStatistics.h"
#include "dwio/nimble/velox/tests/VeloxWriterTestUtils.h"
#include "folly/FileUtil.h"
#include "folly/Random.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/serializers/KeyEncoder.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook {
DEFINE_uint32(
    writer_tests_seed,
    0,
    "If provided, this seed will be used when executing tests. "
    "Otherwise, a random seed will be used.");

using nimble::test::makeTestTabletOptions;

class VeloxWriterTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::SharedArbitrator::registerFactory();
    velox::memory::MemoryManager::Options options;
    options.arbitratorKind = "SHARED";
    velox::memory::MemoryManager::testingSetInstance(options);
  }

  void SetUp() override {
    rootPool_ = velox::memory::memoryManager()->addRootPool("default_root");
    leafPool_ = rootPool_->addLeafChild("default_leaf");
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> leafPool_;
};

nimble::EncodingSelectionPolicyCreator makeEncodingSelectionPolicyCreator(
    nimble::EncodingType encodingType) {
  nimble::ManualEncodingSelectionPolicyFactory factory{
      {{encodingType, 1.0}}, std::nullopt};
  return [factory = std::move(factory)](nimble::DataType dataType)
             -> std::unique_ptr<nimble::EncodingSelectionPolicyBase> {
    return factory.createPolicy(dataType);
  };
}

nimble::EncodingLayout captureFirstColumnEncoding(
    const std::string& file,
    velox::memory::MemoryPool* pool) {
  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto tablet =
      nimble::TabletReader::create(readFile, pool, makeTestTabletOptions(pool));
  auto section =
      tablet->loadOptionalSection(std::string(nimble::kSchemaSection));
  NIMBLE_CHECK(section.has_value(), "Schema not found.");
  auto schema =
      nimble::SchemaDeserializer::deserialize(section->content().data());
  const auto& scalarNode = schema->asRow().childAt(0)->asScalar();

  std::vector<uint32_t> streamIdentifiers{
      scalarNode.scalarDescriptor().offset()};
  auto streams = tablet->load(tablet->stripeIdentifier(0), streamIdentifiers);
  nimble::InMemoryChunkedStream chunkedStream{*pool, std::move(streams[0])};
  NIMBLE_CHECK(chunkedStream.hasNext(), "Expected at least one chunk.");
  return nimble::EncodingLayoutCapture::capture(chunkedStream.nextChunk());
}

template <typename T>
void testAlpE2EWriterSelection(
    velox::memory::MemoryPool& rootPool,
    velox::memory::MemoryPool* leafPool) {
  SCOPED_TRACE(fmt::format("type={}", nimble::TypeTraits<T>::dataType));

  velox::test::VectorMaker vectorMaker{leafPool};
  auto vector = vectorMaker.rowVector(
      {"c0"}, {vectorMaker.flatVector<T>(512, [](auto row) {
        return static_cast<T>(static_cast<int32_t>(row % 101) - 50) /
            static_cast<T>(4);
      })});

  {
    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriterOptions options;
    options.encodingSelectionPolicyCreator =
        makeEncodingSelectionPolicyCreator(nimble::EncodingType::ALP);
    nimble::VeloxWriter writer(
        vector->type(), std::move(writeFile), rootPool, std::move(options));
    writer.write(vector);
    writer.close();

    const auto captured = captureFirstColumnEncoding(file, leafPool);
    EXPECT_EQ(captured.encodingType(), nimble::EncodingType::ALP);
  }

  {
    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriterOptions options;
    options.allowNestedAlpSelection = true;
    options.encodingSelectionPolicyCreator =
        makeEncodingSelectionPolicyCreator(nimble::EncodingType::Dictionary);
    nimble::VeloxWriter writer(
        vector->type(), std::move(writeFile), rootPool, std::move(options));
    writer.write(vector);
    writer.close();

    const auto captured = captureFirstColumnEncoding(file, leafPool);
    ASSERT_EQ(captured.encodingType(), nimble::EncodingType::Dictionary);
    const auto& alphabet =
        captured.child(nimble::EncodingIdentifiers::Dictionary::Alphabet);
    ASSERT_TRUE(alphabet.has_value());
    EXPECT_EQ(alphabet->encodingType(), nimble::EncodingType::ALP);
  }
}

TEST_F(VeloxWriterTest, emptyFile) {
  auto type = velox::ROW({{"simple", velox::INTEGER()}});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(type, std::move(writeFile), *rootPool_, {});
  writer.close();

  // Verify FileLayout for empty file using FileLayout::create()
  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto layout = nimble::FileLayout::create(readFile, leafPool_.get());
  EXPECT_EQ(layout.fileSize, file.size());
  EXPECT_EQ(layout.postscript.majorVersion(), nimble::kVersionMajor);
  EXPECT_EQ(layout.postscript.minorVersion(), nimble::kVersionMinor);
  EXPECT_GT(layout.footer.size(), 0);
  EXPECT_TRUE(layout.stripeGroups.empty());
  EXPECT_TRUE(layout.indexPartitions.empty());
  EXPECT_TRUE(layout.stripesInfo.empty());

  nimble::VeloxReader reader(readFile.get(), *leafPool_);
  velox::VectorPtr result;
  ASSERT_FALSE(reader.next(1, result));
}

TEST_F(VeloxWriterTest, buildEncodingOptionsPropagatesEncodingOptions) {
  {
    const nimble::VeloxWriterOptions options;
    const auto encodingOptions = options.buildEncodingOptions();
    EXPECT_FALSE(encodingOptions.fixedBitWidthUseExactBits);
    EXPECT_FALSE(encodingOptions.allowNestedAlpSelection);
  }

  for (const auto useExactBits : {false, true}) {
    SCOPED_TRACE(fmt::format("useExactBits={}", useExactBits));
    for (const auto allowNestedAlpSelection : {false, true}) {
      SCOPED_TRACE(
          fmt::format("allowNestedAlpSelection={}", allowNestedAlpSelection));
      nimble::VeloxWriterOptions options;
      options.fsstCompressionTargetRatio = 0.42;
      options.fixedBitWidthUseExactBits = useExactBits;
      options.allowNestedAlpSelection = allowNestedAlpSelection;

      const auto encodingOptions = options.buildEncodingOptions();

      EXPECT_DOUBLE_EQ(encodingOptions.fsstCompressionTargetRatio, 0.42);
      EXPECT_EQ(encodingOptions.fixedBitWidthUseExactBits, useExactBits);
      EXPECT_EQ(
          encodingOptions.allowNestedAlpSelection, allowNestedAlpSelection);
    }
  }
}

TEST_F(VeloxWriterTest, alpEncodingSelectionControlsWriterEncoding) {
  testAlpE2EWriterSelection<float>(*rootPool_, leafPool_.get());
  testAlpE2EWriterSelection<double>(*rootPool_, leafPool_.get());
}

DEBUG_ONLY_TEST_F(VeloxWriterTest, encodingBufferPoolPassedToEncodeOptions) {
  velox::common::testutil::TestValue::enable();

  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector = vectorMaker.rowVector(
      {"a", "b", "c", "d"},
      {vectorMaker.flatVector<int64_t>(
           128,
           [](auto row) { return static_cast<int64_t>(row % 8); },
           [](auto row) { return row % 7 == 0; }),
       vectorMaker.flatVector<int64_t>(
           128, [](auto row) { return static_cast<int64_t>(row); }),
       vectorMaker.flatVector<int32_t>(
           128, [](auto row) { return static_cast<int32_t>(row % 11); }),
       vectorMaker.flatVector<double>(
           128, [](auto row) { return static_cast<double>(row) * 0.5; })});

  struct TestCase {
    const char* name;
    bool setMaxCachedNestedEncodingBuffers;
    uint32_t maxCachedNestedEncodingBuffers;
    uint32_t maxEncodeParallelism;
    bool expectEncodingBufferPool;
    size_t expectedPoolCount;
  };

  for (const auto& testCase : std::vector<TestCase>{
           {
               .name = "default",
               .setMaxCachedNestedEncodingBuffers = false,
               .maxCachedNestedEncodingBuffers = 0,
               .maxEncodeParallelism = 0,
               .expectEncodingBufferPool = false,
               .expectedPoolCount = 0,
           },
           {
               .name = "cache disabled",
               .setMaxCachedNestedEncodingBuffers = true,
               .maxCachedNestedEncodingBuffers = 0,
               .maxEncodeParallelism = 0,
               .expectEncodingBufferPool = false,
               .expectedPoolCount = 0,
           },
           {
               .name = "cache enabled",
               .setMaxCachedNestedEncodingBuffers = true,
               .maxCachedNestedEncodingBuffers = 1,
               .maxEncodeParallelism = 0,
               .expectEncodingBufferPool = true,
               .expectedPoolCount = 1,
           },
           {
               .name = "parallel cache enabled",
               .setMaxCachedNestedEncodingBuffers = true,
               .maxCachedNestedEncodingBuffers = 1,
               .maxEncodeParallelism = 2,
               .expectEncodingBufferPool = true,
               .expectedPoolCount = 2,
           },
       }) {
    SCOPED_TRACE(testCase.name);

    uint32_t encodeCount{0};
    uint32_t pooledEncodeCount{0};
    std::set<const void*> observedPools;
    SCOPED_TESTVALUE_SET(
        "facebook::nimble::VeloxWriter::encode",
        std::function<void(nimble::Encoding::Options*)>(
            [&](nimble::Encoding::Options* encodingOptions) {
              ++encodeCount;
              if (encodingOptions->encodingBufferPool != nullptr) {
                ++pooledEncodeCount;
                observedPools.insert(encodingOptions->encodingBufferPool);
              }
            }));

    std::shared_ptr<folly::CPUThreadPoolExecutor> executor;
    nimble::VeloxWriterOptions options;
    if (testCase.setMaxCachedNestedEncodingBuffers) {
      options.maxCachedNestedEncodingBuffers =
          testCase.maxCachedNestedEncodingBuffers;
    }
    if (testCase.maxEncodeParallelism > 0) {
      executor = std::make_shared<folly::CPUThreadPoolExecutor>(
          testCase.maxEncodeParallelism);
      options.encodingExecutor = folly::getKeepAliveToken(*executor);
      options.maxEncodeParallelism = testCase.maxEncodeParallelism;
    }

    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriter writer(
        vector->type(), std::move(writeFile), *rootPool_, std::move(options));
    writer.write(vector);
    writer.close();

    EXPECT_GT(encodeCount, 0);
    EXPECT_EQ(
        pooledEncodeCount, testCase.expectEncodingBufferPool ? encodeCount : 0);
    EXPECT_EQ(observedPools.size(), testCase.expectedPoolCount);
  }
}

TEST_F(VeloxWriterTest, fsstEncodingTargetControlsWriterEncoding) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector = vectorMaker.rowVector(
      {vectorMaker.flatVector<std::string>({std::string(2'000, 'x')})});

  struct TestCase {
    std::string name;
    double targetRatio;
    nimble::CompressionType fsstCompressionType;
    nimble::CompressionOptions compressionOptions;
    nimble::EncodingType expectedEncodingType;
    nimble::CompressionType expectedCompressionType;
  };

  nimble::CompressionOptions zstdCompressionOptions;
  zstdCompressionOptions.compressionType = nimble::CompressionType::Zstd;
  zstdCompressionOptions.zstdMinCompressionSize = 0;

  for (const auto& testCase : std::vector<TestCase>{
           {
               .name = "permissive_target_uses_fsst",
               .targetRatio = 10.0,
               .fsstCompressionType = nimble::CompressionType::Uncompressed,
               .expectedEncodingType = nimble::EncodingType::Fsst,
               .expectedCompressionType = nimble::CompressionType::Uncompressed,
           },
           {
               .name = "strict_target_falls_back_to_compressed_trivial",
               .targetRatio = 0.0,
               .fsstCompressionType = nimble::CompressionType::Zstd,
               .compressionOptions = zstdCompressionOptions,
               .expectedEncodingType = nimble::EncodingType::Trivial,
               .expectedCompressionType = nimble::CompressionType::Zstd,
           },
       }) {
    SCOPED_TRACE(testCase.name);

    nimble::EncodingLayout fsstLayout{
        nimble::EncodingType::Fsst,
        {},
        testCase.fsstCompressionType,
        {nimble::EncodingLayout{
            nimble::EncodingType::Trivial,
            {},
            nimble::CompressionType::Uncompressed}}};

    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriter writer(
        vector->type(),
        std::move(writeFile),
        *rootPool_,
        {
            .encodingLayoutTree =
                nimble::EncodingLayoutTree{
                    nimble::Kind::Row,
                    {},
                    "",
                    {nimble::EncodingLayoutTree{
                        nimble::Kind::Scalar,
                        {{nimble::EncodingLayoutTree::StreamIdentifiers::
                              Scalar::ScalarStream,
                          std::move(fsstLayout)}},
                        "c0"}}},
            .compressionOptions = testCase.compressionOptions,
            .fsstCompressionTargetRatio = testCase.targetRatio,
        });
    writer.write(vector);
    writer.close();

    auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
    auto tablet = nimble::TabletReader::create(
        readFile, leafPool_.get(), makeTestTabletOptions(leafPool_.get()));
    auto section =
        tablet->loadOptionalSection(std::string(nimble::kSchemaSection));
    NIMBLE_CHECK(section.has_value(), "Schema not found.");
    auto schema =
        nimble::SchemaDeserializer::deserialize(section->content().data());
    const auto& scalarNode = schema->asRow().childAt(0)->asScalar();

    ASSERT_EQ(tablet->stripeCount(), 1);
    std::vector<uint32_t> streamIdentifiers{
        scalarNode.scalarDescriptor().offset()};
    auto streams = tablet->load(tablet->stripeIdentifier(0), streamIdentifiers);
    nimble::InMemoryChunkedStream chunkedStream{
        *leafPool_, std::move(streams[0])};
    ASSERT_TRUE(chunkedStream.hasNext());
    const auto capture =
        nimble::EncodingLayoutCapture::capture(chunkedStream.nextChunk());

    EXPECT_EQ(capture.encodingType(), testCase.expectedEncodingType);
    EXPECT_EQ(capture.compressionType(), testCase.expectedCompressionType);
    if (testCase.expectedEncodingType == nimble::EncodingType::Fsst) {
      EXPECT_EQ(
          capture.child(nimble::EncodingIdentifiers::Fsst::Lengths)
              ->encodingType(),
          nimble::EncodingType::Trivial);
    }
  }
}

TEST_F(VeloxWriterTest, emptyFileWithIndexEnabled) {
  auto type = velox::ROW({
      {"key_col", velox::INTEGER()},
      {"value_col", velox::VARCHAR()},
  });

  nimble::index::ClusterIndexConfig clusterIndexConfig{
      .columns = {"key_col"},
      .sortOrders = {nimble::SortOrder{.ascending = true}},
      .enforceKeyOrder = true,
  };

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(
      type,
      std::move(writeFile),
      *rootPool_,
      {.clusterIndexConfig = clusterIndexConfig});
  writer.close();

  // Verify FileLayout for empty file with index enabled using
  // FileLayout::create()
  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto layout = nimble::FileLayout::create(readFile, leafPool_.get());
  EXPECT_EQ(layout.fileSize, file.size());
  EXPECT_EQ(layout.stripesInfo.size(), 0);
  EXPECT_EQ(layout.stripeGroups.size(), 0);
  EXPECT_TRUE(layout.stripeGroups.empty());
  // Index groups should be empty for empty file (no stripes to index)
  EXPECT_TRUE(layout.indexPartitions.empty());
  EXPECT_TRUE(layout.stripesInfo.empty());

  nimble::VeloxReader reader(readFile.get(), *leafPool_);
  velox::VectorPtr result;
  ASSERT_FALSE(reader.next(1, result));
}

TEST_F(VeloxWriterTest, exceptionOnClose) {
  class ThrowingWriteFile final : public velox::WriteFile {
   public:
    void append(std::string_view /* data */) final {
      throw std::runtime_error(uniqueErrorMessage());
    }
    void flush() final {
      throw std::runtime_error(uniqueErrorMessage());
    }
    void close() final {
      throw std::runtime_error(uniqueErrorMessage());
    }
    uint64_t size() const final {
      throw std::runtime_error(uniqueErrorMessage());
    }

   private:
    std::string uniqueErrorMessage() const {
      return "error/" + folly::to<std::string>(folly::Random::rand32());
    }
  };

  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector = vectorMaker.rowVector(
      {"col0"}, {vectorMaker.flatVector<int32_t>({1, 2, 3})});

  std::string file;
  auto writeFile = std::make_unique<ThrowingWriteFile>();

  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.flushPolicyFactory = [&]() {
        return std::make_unique<nimble::LambdaFlushPolicy>(
            /*flushLambda=*/[&](auto&) { return true; });
      }});
  std::string error;
  try {
    writer.write(vector);
    FAIL() << "Expecting exception";
  } catch (const std::runtime_error& e) {
    EXPECT_TRUE(std::string{e.what()}.starts_with("error/"));
    error = e.what();
  }

  try {
    writer.write(vector);
    FAIL() << "Expecting exception";
  } catch (const std::runtime_error& e) {
    EXPECT_EQ(error, e.what());
  }

  try {
    writer.flush();
    FAIL() << "Expecting exception";
  } catch (const std::runtime_error& e) {
    EXPECT_EQ(error, e.what());
  }

  try {
    writer.close();
    FAIL() << "Expecting exception";
  } catch (const std::runtime_error& e) {
    EXPECT_EQ(error, e.what());
  }

  try {
    writer.close();
    FAIL() << "Expecting exception";
  } catch (const std::runtime_error& e) {
    EXPECT_EQ(error, e.what());
  }
}

TEST_F(VeloxWriterTest, emptyFileNoSchema) {
  const uint32_t batchSize = 10;
  auto type = velox::ROW({{"simple", velox::INTEGER()}});
  nimble::VeloxWriterOptions writerOptions;

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(
      type, std::move(writeFile), *rootPool_, std::move(writerOptions));
  writer.close();

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);

  velox::VectorPtr result;
  ASSERT_FALSE(reader.next(batchSize, result));
}

TEST_F(VeloxWriterTest, rootHasNulls) {
  auto batchSize = 5;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector = vectorMaker.rowVector(
      {"col0"}, {vectorMaker.flatVector<int32_t>(batchSize, [](auto row) {
        return row;
      })});

  // add nulls
  for (auto i = 0; i < batchSize; ++i) {
    vector->setNull(i, i % 2 == 0);
  }

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(), std::move(writeFile), *rootPool_, {});

  writer.write(vector);
  writer.close();

  // Verify FileLayout for non-empty file without index using
  // FileLayout::create()
  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto layout = nimble::FileLayout::create(readFile, leafPool_.get());
  EXPECT_EQ(layout.fileSize, file.size());
  EXPECT_EQ(layout.stripesInfo.size(), 1);
  EXPECT_EQ(layout.stripeGroups.size(), 1);
  EXPECT_EQ(layout.stripeGroups.size(), 1);
  // No index configured
  EXPECT_TRUE(layout.indexPartitions.empty());
  // Stripes metadata should be valid (stripeGroups not empty)
  EXPECT_GT(layout.stripes.size(), 0);
  EXPECT_LT(layout.stripes.offset(), layout.footer.offset());
  // Per-stripe info
  EXPECT_EQ(layout.stripesInfo.size(), 1);
  EXPECT_EQ(layout.stripesInfo[0].stripeGroupIndex, 0);
  EXPECT_GT(layout.stripesInfo[0].size, 0);

  nimble::VeloxReader reader(readFile.get(), *leafPool_);
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(batchSize, result));
  ASSERT_EQ(result->size(), batchSize);
  for (auto i = 0; i < batchSize; ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, i));
  }
}

TEST_F(VeloxWriterTest, schemaGrowthExtraColumn) {
  // File type has a single column
  const auto type = velox::ROW({"c0"}, {velox::BIGINT()});
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  // Expect the written values to match the file type
  velox::RowVectorPtr expectedVector = vectorMaker.rowVector(
      {"c0"}, {vectorMaker.flatVector<int64_t>({1, 2, 3})});

  // Add extra column into the written vector
  velox::RowVectorPtr vector = vectorMaker.rowVector(
      {"c0", "c1"},
      {vectorMaker.flatVector<int64_t>({1, 2, 3}),
       vectorMaker.flatVector<int64_t>({10, 20, 30})});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(type, std::move(writeFile), *rootPool_, {});
  writer.write(vector);
  writer.close();

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);

  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(3, result));
  ASSERT_EQ(result->size(), 3);
  ASSERT_EQ(*result->type(), *type);
  for (auto i = 0; i < 3; ++i) {
    ASSERT_TRUE(result->equalValueAt(expectedVector.get(), i, i));
  }
}

TEST_F(VeloxWriterTest, schemaGrowthExtraSubField) {
  // File type has a single column of type struct<f1>
  const auto type = velox::ROW({"c0"}, {velox::ROW({"f1"}, {velox::BIGINT()})});
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  // Expect the written values to match the file type
  velox::RowVectorPtr expectedVector = vectorMaker.rowVector(
      {"c0"},
      {vectorMaker.rowVector(
          {"f1"}, {vectorMaker.flatVector<int64_t>({1, 2, 3})})});

  // Add extra sub-field into the column: struct<f1> -> struct<f1, f2>
  velox::RowVectorPtr vector = vectorMaker.rowVector(
      {"c0"},
      {vectorMaker.rowVector(
          {"f1", "f2"},
          {vectorMaker.flatVector<int64_t>({1, 2, 3}),
           vectorMaker.flatVector<int64_t>({10, 20, 30})})});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(type, std::move(writeFile), *rootPool_, {});
  writer.write(vector);
  writer.close();

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);

  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(3, result));
  ASSERT_EQ(result->size(), 3);
  ASSERT_EQ(*result->type(), *type);
  for (auto i = 0; i < 3; ++i) {
    ASSERT_TRUE(result->equalValueAt(expectedVector.get(), i, i));
  }
}

TEST_F(
    VeloxWriterTest,
    FeatureReorderingNonFlatmapColumnIgnoresMismatchedConfig) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector = vectorMaker.rowVector(
      {"map", "flatmap"},
      {vectorMaker.mapVector<int32_t, int32_t>(
           5,
           /* sizeAt */ [](auto row) { return row % 3; },
           /* keyAt */ [](auto /* row */, auto mapIndex) { return mapIndex; },
           /* valueAt */ [](auto row, auto /* mapIndex */) { return row; },
           /* isNullAt */ [](auto /* row */) { return false; }),
       vectorMaker.mapVector<int32_t, int32_t>(
           5,
           /* sizeAt */ [](auto row) { return row % 3; },
           /* keyAt */ [](auto /* row */, auto mapIndex) { return mapIndex; },
           /* valueAt */ [](auto row, auto /* mapIndex */) { return row; },
           /* isNullAt */ [](auto /* row */) { return false; })});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.flatMapColumns = {{"flatmap", {}}},
       .featureReordering =
           std::vector<std::tuple<size_t, std::vector<int64_t>>>{
               {0, {1, 2}}, {1, {3, 4}}}});
  writer.write(vector);
  writer.close();
}

// A single write whose map has more distinct keys than maxFlatMapKeys fails.
TEST_F(VeloxWriterTest, FlatMapKeyLimitExceededInSingleWriteThrows) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  // One row with 10 distinct BIGINT keys against a limit of 4.
  auto vector = vectorMaker.rowVector(
      {"flatmap"},
      {vectorMaker.mapVector<int64_t, int64_t>(
          /* size */
          1,
          /* sizeAt */ [](auto) { return 10; },
          /* keyAt */ [](auto, auto mapIndex) -> int64_t { return mapIndex; },
          /* valueAt */ [](auto, auto mapIndex) -> int64_t { return mapIndex; },
          /* isNullAt */ [](auto) { return false; })});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.flatMapColumns = {{"flatmap", {}}}, .maxFlatMapKeys = 4});

  NIMBLE_ASSERT_USER_THROW(
      writer.write(vector), "Too many flatmap keys for node");
}

// A map whose distinct key count is at/under maxFlatMapKeys writes fine.
TEST_F(VeloxWriterTest, FlatMapKeyLimitWithinLimitSucceeds) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  // 5 distinct keys, well under the limit of 100.
  auto vector = vectorMaker.rowVector(
      {"flatmap"},
      {vectorMaker.mapVector<int64_t, int64_t>(
          /* size */
          10,
          /* sizeAt */ [](auto) { return 5; },
          /* keyAt */ [](auto, auto mapIndex) -> int64_t { return mapIndex; },
          /* valueAt */
          [](auto row, auto mapIndex) -> int64_t {
            return row * 10 + mapIndex;
          },
          /* isNullAt */ [](auto) { return false; })});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.flatMapColumns = {{"flatmap", {}}}, .maxFlatMapKeys = 100});

  EXPECT_NO_THROW(writer.write(vector));
  EXPECT_NO_THROW(writer.close());
}

// The limit is file-wide: distinct keys accumulate across write() calls even
// when no single write exceeds it.
TEST_F(VeloxWriterTest, FlatMapKeyLimitAccumulatesAcrossWrites) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  // Each batch has 8 distinct keys (< limit 10), but the two batches use
  // disjoint key ranges, so the file-wide count (16) exceeds the limit.
  auto makeBatch = [&](int64_t keyOffset) {
    return vectorMaker.rowVector(
        {"flatmap"},
        {vectorMaker.mapVector<int64_t, int64_t>(
            /* size */
            1,
            /* sizeAt */ [](auto) { return 8; },
            /* keyAt */
            [keyOffset](auto, auto mapIndex) -> int64_t {
              return keyOffset + mapIndex;
            },
            /* valueAt */
            [](auto, auto mapIndex) -> int64_t { return mapIndex; },
            /* isNullAt */ [](auto) { return false; })});
  };

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      makeBatch(0)->type(),
      std::move(writeFile),
      *rootPool_,
      {.flatMapColumns = {{"flatmap", {}}}, .maxFlatMapKeys = 10});

  // First batch: 8 distinct keys, under the limit.
  EXPECT_NO_THROW(writer.write(makeBatch(0)));
  // Second batch: 8 new keys push the file-wide total to 16, over the limit.
  NIMBLE_ASSERT_USER_THROW(
      writer.write(makeBatch(100)), "Too many flatmap keys for node");
}

// A maxFlatMapKeys of 0 disables the cap (unlimited keys).
TEST_F(VeloxWriterTest, FlatMapKeyLimitZeroMeansUnlimited) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  // 50 distinct keys, which would exceed any small positive limit.
  auto vector = vectorMaker.rowVector(
      {"flatmap"},
      {vectorMaker.mapVector<int64_t, int64_t>(
          /* size */
          1,
          /* sizeAt */ [](auto) { return 50; },
          /* keyAt */ [](auto, auto mapIndex) -> int64_t { return mapIndex; },
          /* valueAt */ [](auto, auto mapIndex) -> int64_t { return mapIndex; },
          /* isNullAt */ [](auto) { return false; })});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.flatMapColumns = {{"flatmap", {}}}, .maxFlatMapKeys = 0});

  EXPECT_NO_THROW(writer.write(vector));
  EXPECT_NO_THROW(writer.close());
}

TEST_F(VeloxWriterTest, featureReorderingStreamCollocation) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  const std::vector<int64_t> reorderedKeys = {4, 2, 0};
  const int numRows = 1'000;

  for (bool enableIndex : {false, true}) {
    SCOPED_TRACE(fmt::format("enableIndex={}", enableIndex));

    // With index: schema is (key, flatmap); without: just (flatmap).
    // The flatmap column ordinal differs accordingly.
    auto vector = enableIndex
        ? vectorMaker.rowVector(
              {"key", "flatmap"},
              {vectorMaker.flatVector<int64_t>(
                   numRows, [](auto row) { return static_cast<int64_t>(row); }),
               vectorMaker.mapVector<int32_t, int32_t>(
                   numRows,
                   [](auto) { return 5; },
                   [](auto, auto mapIndex) { return mapIndex; },
                   [](auto row, auto mapIndex) { return row * 10 + mapIndex; },
                   [](auto) { return false; })})
        : vectorMaker.rowVector(
              {"flatmap"},
              {vectorMaker.mapVector<int32_t, int32_t>(
                  numRows,
                  [](auto) { return 5; },
                  [](auto, auto mapIndex) { return mapIndex; },
                  [](auto row, auto mapIndex) { return row * 10 + mapIndex; },
                  [](auto) { return false; })});

    const size_t flatmapOrdinal = enableIndex ? 1 : 0;

    std::string file;
    {
      auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
      nimble::VeloxWriterOptions options;
      options.flatMapColumns = {{"flatmap", {}}};
      options.featureReordering =
          std::vector<std::tuple<size_t, std::vector<int64_t>>>{
              {flatmapOrdinal, reorderedKeys}};

      if (enableIndex) {
        nimble::index::ClusterIndexConfig clusterIndexConfig;
        clusterIndexConfig.columns = {"key"};
        clusterIndexConfig.sortOrders = {nimble::SortOrder{.ascending = true}};
        clusterIndexConfig.enforceKeyOrder = true;
        clusterIndexConfig.encodingLayout = nimble::EncodingLayout{
            nimble::EncodingType::Prefix,
            {},
            nimble::CompressionType::Uncompressed};
        options.clusterIndexConfig = std::move(clusterIndexConfig);
      }

      nimble::VeloxWriter writer(
          vector->type(), std::move(writeFile), *rootPool_, std::move(options));
      writer.write(vector);
      writer.close();
    }

    auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
    auto tablet = nimble::TabletReader::create(
        readFile, leafPool_.get(), makeTestTabletOptions(leafPool_.get()));
    ASSERT_GE(tablet->stripeCount(), 1);
    if (enableIndex) {
      ASSERT_NE(tablet->clusterIndex(), nullptr) << "Cluster index must exist";
    }

    auto stripeId = tablet->stripeIdentifier(0);
    auto offsets = tablet->streamOffsets(stripeId);
    auto sizes = tablet->streamSizes(stripeId);

    nimble::VeloxReader reader(readFile.get(), *leafPool_);
    const auto& flatMap =
        reader.schema()->asRow().childAt(flatmapOrdinal)->asFlatMap();

    std::unordered_map<std::string, uint32_t> keyToValueStreamId;
    for (size_t i = 0; i < flatMap.childrenCount(); ++i) {
      keyToValueStreamId[flatMap.nameAt(i)] =
          flatMap.childAt(i)->asScalar().scalarDescriptor().offset();
    }

    // Use value stream offsets for ordering since inMap streams may be
    // constant-encoded and deduplicated when all keys are present in every
    // row.
    auto diskPosition = [&](const std::string& key) -> uint32_t {
      return offsets[keyToValueStreamId.at(key)];
    };

    // Verify reordered keys appear in the specified order on disk.
    for (size_t i = 1; i < reorderedKeys.size(); ++i) {
      auto prevKey = folly::to<std::string>(reorderedKeys[i - 1]);
      auto currKey = folly::to<std::string>(reorderedKeys[i]);
      EXPECT_LT(diskPosition(prevKey), diskPosition(currKey))
          << "Key " << prevKey << " should appear before key " << currKey
          << " on disk";
    }

    // Verify reordered keys' value streams are contiguous (adjacent on disk).
    for (size_t i = 1; i < reorderedKeys.size(); ++i) {
      auto prevKey = folly::to<std::string>(reorderedKeys[i - 1]);
      auto currKey = folly::to<std::string>(reorderedKeys[i]);
      auto prevStreamId = keyToValueStreamId.at(prevKey);
      auto currStreamId = keyToValueStreamId.at(currKey);
      EXPECT_EQ(
          offsets[prevStreamId] + sizes[prevStreamId], offsets[currStreamId])
          << "Key " << prevKey << " value stream should be adjacent to key "
          << currKey;
    }

    // Verify leftover keys (1, 3) appear after all reordered keys.
    auto lastReorderedKey = folly::to<std::string>(reorderedKeys.back());
    auto lastReorderedPos = diskPosition(lastReorderedKey);
    for (const auto& leftoverKey : {"1", "3"}) {
      EXPECT_GT(diskPosition(leftoverKey), lastReorderedPos)
          << "Leftover key " << leftoverKey
          << " should appear after last reordered key " << lastReorderedKey;
    }
  }
}

TEST_F(VeloxWriterTest, duplicateFlatmapKey) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  // Vector with constant but duplicate key set. Potentially omitting in map
  // stream in the future.
  {
    auto vec = vectorMaker.rowVector(
        {"flatmap"},
        {vectorMaker.mapVector<int32_t, int32_t>(
            10,
            /* sizeAt */ [](auto row) { return 6; },
            /* keyAt */
            [](auto /* row */, auto mapIndex) { return mapIndex / 2; },
            /* valueAt */ [](auto row, auto /* mapIndex */) { return row; },
            /* isNullAt */ [](auto /* row */) { return false; })});
    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

    nimble::VeloxWriter writer(
        vec->type(),
        std::move(writeFile),
        *rootPool_,
        {.flatMapColumns = {{"flatmap", {}}}});
    EXPECT_THROW(writer.write(vec), nimble::NimbleInternalError);
    EXPECT_ANY_THROW(writer.close());
  }
  // Vector with a rotating duplicate key set. The more typical layout
  // requiring in map stream to represent.
  {
    auto vec = vectorMaker.rowVector(
        {"flatmap"},
        {vectorMaker.mapVector<int32_t, int32_t>(
            10,
            /* sizeAt */ [](auto row) { return 6; },
            /* keyAt */
            [](auto row, auto mapIndex) { return (row + mapIndex / 2) % 6; },
            /* valueAt */ [](auto row, auto /* mapIndex */) { return row; },
            /* isNullAt */ [](auto /* row */) { return false; })});

    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

    nimble::VeloxWriter writer(
        vec->type(),
        std::move(writeFile),
        *rootPool_,
        {.flatMapColumns = {{"flatmap", {}}}});
    EXPECT_THROW(writer.write(vec), nimble::NimbleInternalError);
    EXPECT_ANY_THROW(writer.close());
  }
}

struct StripeRawSizeFlushPolicyTestCase {
  const size_t batchCount;
  const uint32_t rawStripeSize;
  const uint32_t stripeCount;
};

class StripeRawSizeFlushPolicyTest
    : public VeloxWriterTest,
      public ::testing::WithParamInterface<StripeRawSizeFlushPolicyTestCase> {};

TEST_P(StripeRawSizeFlushPolicyTest, StripeRawSizeFlushPolicy) {
  auto type = velox::ROW({{"simple", velox::INTEGER()}});
  nimble::VeloxWriterOptions writerOptions{.flushPolicyFactory = []() {
    // Buffering 256MB data before encoding stripes.
    return std::make_unique<nimble::StripeRawSizeFlushPolicy>(
        GetParam().rawStripeSize);
  }};

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(
      type, std::move(writeFile), *rootPool_, std::move(writerOptions));
  auto batches =
      generateBatches(type, GetParam().batchCount, 4000, 20221110, *leafPool_);

  for (const auto& batch : batches) {
    writer.write(batch);
  }
  writer.close();

  velox::InMemoryReadFile readFile(file);
  auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(type);
  nimble::VeloxReader reader(&readFile, *leafPool_, std::move(selector));

  EXPECT_EQ(GetParam().stripeCount, reader.tabletReader().stripeCount());
}

namespace {
class MockReclaimer : public velox::memory::MemoryReclaimer {
 public:
  explicit MockReclaimer() : velox::memory::MemoryReclaimer(0) {}
  void setEnterArbitrationFunc(std::function<void()>&& func) {
    enterArbitrationFunc_ = func;
  }
  void enterArbitration() override {
    if (enterArbitrationFunc_) {
      enterArbitrationFunc_();
    }
  }

 private:
  std::function<void()> enterArbitrationFunc_;
};
} // namespace

TEST_F(VeloxWriterTest, memoryReclaimPath) {
  auto rootPool = velox::memory::memoryManager()->addRootPool(
      "root", 4L << 20, velox::memory::MemoryReclaimer::create());
  auto writerPool = rootPool->addAggregateChild(
      "writer", velox::memory::MemoryReclaimer::create());

  auto type = velox::ROW(
      {{"simple_int", velox::INTEGER()}, {"simple_double", velox::DOUBLE()}});
  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  std::atomic_bool reclaimEntered = false;
  nimble::VeloxWriterOptions writerOptions{.reclaimerFactory = [&]() {
    auto reclaimer = std::make_unique<MockReclaimer>();
    reclaimer->setEnterArbitrationFunc([&]() { reclaimEntered = true; });
    return reclaimer;
  }};
  nimble::VeloxWriter writer(
      type, std::move(writeFile), *writerPool, std::move(writerOptions));
  auto batches = generateBatches(type, 100, 4000, 20221110, *leafPool_);

  EXPECT_THROW(
      {
        for (const auto& batch : batches) {
          writer.write(batch);
        }
      },
      velox::VeloxException);
  ASSERT_TRUE(reclaimEntered.load());
}

TEST_F(VeloxWriterTest, flushHugeStrings) {
  nimble::VeloxWriterOptions writerOptions{.flushPolicyFactory = []() {
    return std::make_unique<nimble::StripeRawSizeFlushPolicy>(1 * 1024 * 1024);
  }};

  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  // Each vector contains 99 strings with 36 characters each (36*99=3564) +
  // 100 bytes for null vector + 99 string_views (99*16=1584) for a total of
  // 5248 bytes, so writing 200 batches should exceed the flush theshold of
  // 1MB
  auto vector = vectorMaker.rowVector(
      {"string"},
      {
          vectorMaker.flatVector<std::string>(
              100,
              [](auto /* row */) {
                return std::string("abcdefghijklmnopqrstuvwxyz0123456789");
              },
              [](auto row) { return row == 6; }),
      });

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      std::move(writerOptions));

  // Writing 500 batches should produce 3 stripes, as each 200 vectors will
  // exceed the flush threshold.
  for (auto i = 0; i < 500; ++i) {
    writer.write(vector);
  }
  writer.close();

  velox::InMemoryReadFile readFile(file);
  auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(
      std::dynamic_pointer_cast<const velox::RowType>(vector->type()));
  nimble::VeloxReader reader(&readFile, *leafPool_, std::move(selector));

  EXPECT_EQ(3, reader.tabletReader().stripeCount());
}

TEST_F(VeloxWriterTest, encodingLayout) {
  nimble::EncodingLayoutTree expected{
      nimble::Kind::Row,
      {},
      "",
      {
          {nimble::Kind::Map,
           {
               {
                   0,
                   nimble::EncodingLayout{
                       nimble::EncodingType::Dictionary,
                       {},
                       nimble::CompressionType::Uncompressed,
                       {
                           nimble::EncodingLayout{
                               nimble::EncodingType::FixedBitWidth,
                               {},
                               nimble::CompressionType::MetaInternal},
                           std::nullopt,
                       }},
               },
           },
           "",
           {
               // Map keys
               {nimble::Kind::Scalar, {}, ""},
               // Map Values
               {nimble::Kind::Scalar,
                {
                    {
                        0,
                        nimble::EncodingLayout{
                            nimble::EncodingType::MainlyConstant,
                            {},
                            nimble::CompressionType::Uncompressed,
                            {
                                std::nullopt,
                                nimble::EncodingLayout{
                                    nimble::EncodingType::Trivial,
                                    {},
                                    nimble::CompressionType::MetaInternal},
                            }},
                    },
                },
                ""},
           }},
          {nimble::Kind::FlatMap,
           {},
           "",
           {
               {
                   nimble::Kind::Scalar,
                   {
                       {
                           0,
                           nimble::EncodingLayout{
                               nimble::EncodingType::MainlyConstant,
                               {},
                               nimble::CompressionType::Uncompressed,
                               {
                                   nimble::EncodingLayout{
                                       nimble::EncodingType::Trivial,
                                       {},
                                       nimble::CompressionType::Uncompressed},
                                   nimble::EncodingLayout{
                                       nimble::EncodingType::FixedBitWidth,
                                       {},
                                       nimble::CompressionType::Uncompressed},
                               }},
                       },
                   },
                   "1",
               },
               {
                   nimble::Kind::Scalar,
                   {
                       {
                           0,
                           nimble::EncodingLayout{
                               nimble::EncodingType::Constant,
                               {},
                               nimble::CompressionType::Uncompressed,
                           },
                       },
                   },
                   "2",
               },
           }},
      }};

  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector = vectorMaker.rowVector(
      {"map", "flatmap"},
      {vectorMaker.mapVector<int32_t, int32_t>(
           5,
           /* sizeAt */ [](auto row) { return row % 3; },
           /* keyAt */
           [](auto /* row */, auto mapIndex) { return mapIndex; },
           /* valueAt */ [](auto row, auto /* mapIndex */) { return row; },
           /* isNullAt */ [](auto /* row */) { return false; }),
       vectorMaker.mapVector(
           std::vector<std::optional<
               std::vector<std::pair<int32_t, std::optional<int32_t>>>>>{
               std::vector<std::pair<int32_t, std::optional<int32_t>>>{
                   {0, 2},
                   {2, 3},
               },
               std::nullopt,
               {},
               std::vector<std::pair<int32_t, std::optional<int32_t>>>{
                   {1, 4},
                   {0, std::nullopt},
               },
               std::vector<std::pair<int32_t, std::optional<int32_t>>>{
                   {1, std::nullopt},
               },
           })});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {
          .flatMapColumns = {{"flatmap", {}}},
          .encodingLayoutTree = std::move(expected),
          // Boosting acceptance ratio by 100x to make sure it is always
          // accepted (even if compressed size if bigger than uncompressed
          // size)
          .compressionOptions =
              {.compressionAcceptRatio = 100, .internalMinCompressionSize = 0},
      });

  writer.write(vector);
  writer.close();

  for (auto useChainedBuffers : {false, true}) {
    auto readFile =
        std::make_shared<nimble::testing::InMemoryTrackableReadFile>(
            file, useChainedBuffers);
    auto tablet = nimble::TabletReader::create(
        readFile, leafPool_.get(), makeTestTabletOptions(leafPool_.get()));
    auto section =
        tablet->loadOptionalSection(std::string(nimble::kSchemaSection));
    NIMBLE_CHECK(section.has_value(), "Schema not found.");
    auto schema =
        nimble::SchemaDeserializer::deserialize(section->content().data());
    auto& mapNode = schema->asRow().childAt(0)->asMap();
    auto& mapValuesNode = mapNode.values()->asScalar();
    auto& flatMapNode = schema->asRow().childAt(1)->asFlatMap();
    ASSERT_EQ(3, flatMapNode.childrenCount());

    auto findChild =
        [](const facebook::nimble::FlatMapType& map,
           std::string_view key) -> std::shared_ptr<const nimble::Type> {
      for (auto i = 0; i < map.childrenCount(); ++i) {
        if (map.nameAt(i) == key) {
          return map.childAt(i);
        }
      }
      return nullptr;
    };
    const auto& flatMapKey1Node = findChild(flatMapNode, "1")->asScalar();
    const auto& flatMapKey2Node = findChild(flatMapNode, "2")->asScalar();

    for (auto i = 0; i < tablet->stripeCount(); ++i) {
      auto stripeIdentifier = tablet->stripeIdentifier(i);
      std::vector<uint32_t> identifiers{
          mapNode.lengthsDescriptor().offset(),
          mapValuesNode.scalarDescriptor().offset(),
          flatMapKey1Node.scalarDescriptor().offset(),
          flatMapKey2Node.scalarDescriptor().offset()};
      auto streams = tablet->load(stripeIdentifier, identifiers);
      {
        nimble::InMemoryChunkedStream chunkedStream{
            *leafPool_, std::move(streams[0])};
        ASSERT_TRUE(chunkedStream.hasNext());
        // Verify Map stream
        auto capture =
            nimble::EncodingLayoutCapture::capture(chunkedStream.nextChunk());
        EXPECT_EQ(nimble::EncodingType::Dictionary, capture.encodingType());
        EXPECT_EQ(
            nimble::EncodingType::FixedBitWidth,
            capture.child(nimble::EncodingIdentifiers::Dictionary::Alphabet)
                ->encodingType());
        EXPECT_EQ(
            nimble::CompressionType::MetaInternal,
            capture.child(nimble::EncodingIdentifiers::Dictionary::Alphabet)
                ->compressionType());
      }

      {
        nimble::InMemoryChunkedStream chunkedStream{
            *leafPool_, std::move(streams[1])};
        ASSERT_TRUE(chunkedStream.hasNext());
        // Verify Map Values stream
        auto capture =
            nimble::EncodingLayoutCapture::capture(chunkedStream.nextChunk());
        EXPECT_EQ(nimble::EncodingType::MainlyConstant, capture.encodingType());
        EXPECT_EQ(
            nimble::EncodingType::Trivial,
            capture
                .child(nimble::EncodingIdentifiers::MainlyConstant::OtherValues)
                ->encodingType());
        EXPECT_EQ(
            nimble::CompressionType::MetaInternal,
            capture
                .child(nimble::EncodingIdentifiers::MainlyConstant::OtherValues)
                ->compressionType());
      }

      {
        nimble::InMemoryChunkedStream chunkedStream{
            *leafPool_, std::move(streams[2])};
        ASSERT_TRUE(chunkedStream.hasNext());
        // Verify FlatMap Kay "1" stream
        auto capture =
            nimble::EncodingLayoutCapture::capture(chunkedStream.nextChunk());
        EXPECT_EQ(nimble::EncodingType::MainlyConstant, capture.encodingType());
        EXPECT_EQ(
            nimble::EncodingType::Trivial,
            capture
                .child(nimble::EncodingIdentifiers::MainlyConstant::IsCommon)
                ->encodingType());
        EXPECT_EQ(
            nimble::CompressionType::Uncompressed,
            capture
                .child(nimble::EncodingIdentifiers::MainlyConstant::IsCommon)
                ->compressionType());
        EXPECT_EQ(
            nimble::EncodingType::FixedBitWidth,
            capture
                .child(nimble::EncodingIdentifiers::MainlyConstant::OtherValues)
                ->encodingType());
        EXPECT_EQ(
            nimble::CompressionType::Uncompressed,
            capture
                .child(nimble::EncodingIdentifiers::MainlyConstant::OtherValues)
                ->compressionType());
      }

      {
        nimble::InMemoryChunkedStream chunkedStream{
            *leafPool_, std::move(streams[3])};
        ASSERT_TRUE(chunkedStream.hasNext());
        // Verify FlatMap Kay "2" stream
        auto capture =
            nimble::EncodingLayoutCapture::capture(chunkedStream.nextChunk());
        EXPECT_EQ(nimble::EncodingType::Constant, capture.encodingType());
      }
    }
  }
}

TEST_F(VeloxWriterTest, openZLCompressionNumericRoundTrip) {
  // E2E: force the OpenZL codec, write compressible numeric columns, and assert
  // (a) at least one numeric stream is actually OpenZL-compressed and (b) the
  // data round-trips byte-for-byte through the reader.
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  constexpr velox::vector_size_t kRowCount = 1000;
  auto vector = vectorMaker.rowVector(
      {"i32", "i64", "f32"},
      {
          vectorMaker.flatVector<int32_t>(
              kRowCount,
              [](auto row) { return static_cast<int32_t>(1000 + row % 50); }),
          vectorMaker.flatVector<int64_t>(
              kRowCount,
              [](auto row) { return static_cast<int64_t>(row / 4); }),
          vectorMaker.flatVector<float>(
              kRowCount, [](auto row) { return static_cast<float>(row % 16); }),
      });

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriterOptions writerOptions;
  // The default write path drives compression off the encoding selection policy
  // (not VeloxWriterOptions::compressionOptions), so force OpenZL via the
  // factory. Accept ratio 100 + min size 0 ensure the codec is actually applied
  // even to tiny streams.
  writerOptions.encodingSelectionPolicyCreator =
      [factory =
           nimble::ManualEncodingSelectionPolicyFactory{
               nimble::ManualEncodingSelectionPolicyFactory::
                   defaultEncodingReadFactors(),
               nimble::CompressionOptions{
                   .compressionAcceptRatio = 100,
                   .compressionType = nimble::CompressionType::OpenZL,
                   .openzlMinCompressionSize = 0,
               }}](nimble::DataType dataType)
      -> std::unique_ptr<nimble::EncodingSelectionPolicyBase> {
    return factory.createPolicy(dataType);
  };
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      std::move(writerOptions));
  writer.write(vector);
  writer.close();

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto tablet = nimble::TabletReader::create(
      readFile, leafPool_.get(), makeTestTabletOptions(leafPool_.get()));
  auto section =
      tablet->loadOptionalSection(std::string(nimble::kSchemaSection));
  ASSERT_TRUE(section.has_value());
  auto schema =
      nimble::SchemaDeserializer::deserialize(section->content().data());

  // Recursively checks whether any node in the encoding tree was compressed
  // with the given codec, so the assertion does not depend on which encoding
  // the writer happened to pick.
  std::function<bool(const nimble::EncodingLayout&, nimble::CompressionType)>
      usesCompression = [&](const nimble::EncodingLayout& layout,
                            nimble::CompressionType compressionType) {
        if (layout.compressionType() == compressionType) {
          return true;
        }
        for (uint8_t i = 0; i < layout.childrenCount(); ++i) {
          const auto& child = layout.child(i);
          if (child.has_value() &&
              usesCompression(child.value(), compressionType)) {
            return true;
          }
        }
        return false;
      };

  bool anyOpenZL = false;
  for (auto stripe = 0; stripe < tablet->stripeCount(); ++stripe) {
    auto stripeIdentifier = tablet->stripeIdentifier(stripe);
    for (auto column = 0; column < schema->asRow().childrenCount(); ++column) {
      auto offset = schema->asRow()
                        .childAt(column)
                        ->asScalar()
                        .scalarDescriptor()
                        .offset();
      auto streams =
          tablet->load(stripeIdentifier, std::vector<uint32_t>{offset});
      if (streams.empty() || streams[0] == nullptr) {
        continue;
      }
      nimble::InMemoryChunkedStream chunkedStream{
          *leafPool_, std::move(streams[0])};
      while (chunkedStream.hasNext()) {
        auto capture =
            nimble::EncodingLayoutCapture::capture(chunkedStream.nextChunk());
        if (usesCompression(capture, nimble::CompressionType::OpenZL)) {
          anyOpenZL = true;
        }
      }
    }
  }
  EXPECT_TRUE(anyOpenZL)
      << "Expected at least one numeric stream to be OpenZL-compressed";

  nimble::VeloxReader reader(readFile.get(), *leafPool_);
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kRowCount, result));
  ASSERT_EQ(kRowCount, result->size());
  for (velox::vector_size_t i = 0; i < kRowCount; ++i) {
    EXPECT_TRUE(vector->equalValueAt(result.get(), i, i))
        << "Content mismatch at row " << i;
  }
  ASSERT_FALSE(reader.next(1, result));
}

TEST_F(VeloxWriterTest, encodingLayoutSchemaMismatch) {
  nimble::EncodingLayoutTree expected{
      nimble::Kind::Row,
      {},
      "",
      {
          {
              nimble::Kind::Scalar,
              {
                  {
                      0,
                      nimble::EncodingLayout{
                          nimble::EncodingType::Dictionary,
                          {},
                          nimble::CompressionType::Uncompressed,
                          {
                              nimble::EncodingLayout{
                                  nimble::EncodingType::FixedBitWidth,
                                  {},
                                  nimble::CompressionType::MetaInternal},
                              std::nullopt,
                          }},
                  },
              },
              "",
          },
      }};

  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector = vectorMaker.rowVector(
      {"map"},
      {
          vectorMaker.mapVector<int32_t, int32_t>(
              5,
              /* sizeAt */ [](auto row) { return row % 3; },
              /* keyAt */
              [](auto /* row */, auto mapIndex) { return mapIndex; },
              /* valueAt */ [](auto row, auto /* mapIndex */) { return row; },
              /* isNullAt */ [](auto /* row */) { return false; }),
      });

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  try {
    nimble::VeloxWriter writer(
        vector->type(),
        std::move(writeFile),
        *rootPool_,
        {
            .encodingLayoutTree = std::move(expected),
            .compressionOptions = {.compressionAcceptRatio = 100},
        });
    FAIL() << "Writer should fail on incompatible encoding layout node";
  } catch (const nimble::NimbleInternalError& e) {
    EXPECT_NE(
        std::string(e.what()).find(
            "Incompatible encoding layout node. Expecting map node"),
        std::string::npos);
  }
}

TEST_F(VeloxWriterTest, encodingLayoutSchemaEvolutionMapToFlatmap) {
  nimble::EncodingLayoutTree expected{
      nimble::Kind::Row,
      {},
      "",
      {
          {nimble::Kind::Map,
           {
               {
                   0,
                   nimble::EncodingLayout{
                       nimble::EncodingType::Dictionary,
                       {},
                       nimble::CompressionType::Uncompressed,
                       {
                           nimble::EncodingLayout{
                               nimble::EncodingType::FixedBitWidth,
                               {},
                               nimble::CompressionType::MetaInternal},
                           std::nullopt,
                       }},
               },
           },
           "",
           {
               // Map keys
               {nimble::Kind::Scalar, {}, ""},
               // Map Values
               {nimble::Kind::Scalar,
                {
                    {
                        0,
                        nimble::EncodingLayout{
                            nimble::EncodingType::MainlyConstant,
                            {},
                            nimble::CompressionType::Uncompressed,
                            {
                                std::nullopt,
                                nimble::EncodingLayout{
                                    nimble::EncodingType::Trivial,
                                    {},
                                    nimble::CompressionType::MetaInternal},
                            }},
                    },
                },
                ""},
           }},
      }};

  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector = vectorMaker.rowVector(
      {"map"},
      {
          vectorMaker.mapVector<int32_t, int32_t>(
              5,
              /* sizeAt */ [](auto row) { return row % 3; },
              /* keyAt */
              [](auto /* row */, auto mapIndex) { return mapIndex; },
              /* valueAt */ [](auto row, auto /* mapIndex */) { return row; },
              /* isNullAt */ [](auto /* row */) { return false; }),
      });

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {
          .flatMapColumns = {{"map", {}}},
          .encodingLayoutTree = std::move(expected),
          .compressionOptions = {.compressionAcceptRatio = 100},
      });

  writer.write(vector);
  writer.close();

  // Getting here is good enough for now (as it means we didn't fail on node
  // type mismatch). Once we add metric collection, we can use these to verify
  // that no captured encoding was used.
}

TEST_F(VeloxWriterTest, encodingLayoutSchemaEvolutionFlamapToMap) {
  nimble::EncodingLayoutTree expected{
      nimble::Kind::Row,
      {},
      "",
      {
          {nimble::Kind::FlatMap,
           {},
           "",
           {
               {
                   nimble::Kind::Scalar,
                   {
                       {
                           0,
                           nimble::EncodingLayout{
                               nimble::EncodingType::MainlyConstant,
                               {},
                               nimble::CompressionType::Uncompressed,
                               {
                                   nimble::EncodingLayout{
                                       nimble::EncodingType::Trivial,
                                       {},
                                       nimble::CompressionType::Uncompressed},
                                   nimble::EncodingLayout{
                                       nimble::EncodingType::FixedBitWidth,
                                       {},
                                       nimble::CompressionType::Uncompressed},
                               }},
                       },
                   },
                   "1",
               },
               {
                   nimble::Kind::Scalar,
                   {
                       {
                           0,
                           nimble::EncodingLayout{
                               nimble::EncodingType::Constant,
                               {},
                               nimble::CompressionType::Uncompressed,
                           },
                       },
                   },
                   "2",
               },
           }},
      }};

  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector = vectorMaker.rowVector(
      {"flatmap"},
      {
          vectorMaker.mapVector<int32_t, int32_t>(
              5,
              /* sizeAt */ [](auto row) { return row % 3; },
              /* keyAt */
              [](auto /* row */, auto mapIndex) { return mapIndex; },
              /* valueAt */ [](auto row, auto /* mapIndex */) { return row; },
              /* isNullAt */ [](auto /* row */) { return false; }),
      });

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {
          .encodingLayoutTree = std::move(expected),
          .compressionOptions = {.compressionAcceptRatio = 100},
      });

  writer.write(vector);
  writer.close();

  // Getting here is good enough for now (as it means we didn't fail on node
  // type mismatch). Once we add metric collection, we can use these to verify
  // that no captured encoding was used.
}

TEST_F(VeloxWriterTest, encodingLayoutSchemaEvolutionExpandingRow) {
  nimble::EncodingLayoutTree expected{
      nimble::Kind::Row,
      {},
      "",
      {
          {nimble::Kind::Row,
           {
               {
                   0,
                   nimble::EncodingLayout{
                       nimble::EncodingType::Trivial,
                       {},
                       nimble::CompressionType::Uncompressed},
               },
           },
           "",
           {
               {
                   nimble::Kind::Scalar,
                   {
                       {
                           0,
                           nimble::EncodingLayout{
                               nimble::EncodingType::Trivial,
                               {},
                               nimble::CompressionType::Uncompressed},
                       },
                   },
                   "",
               },
           }},
      }};

  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  // We are adding new top level column and also nested column
  auto vector = vectorMaker.rowVector(
      {"row1", "row2"},
      {
          vectorMaker.rowVector({
              vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5}),
              vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5}),
          }),
          vectorMaker.rowVector({
              vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5}),
              vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5}),
          }),
      });

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {
          .encodingLayoutTree = std::move(expected),
          .compressionOptions = {.compressionAcceptRatio = 100},
      });

  writer.write(vector);
  writer.close();

  // Getting here is good enough for now (as it means we didn't fail on node
  // type mismatch). Once we add metric collection, we can use these to verify
  // that no captured encoding was used.
}

TEST_F(VeloxWriterTest, combineMultipleLayersOfDictionaries) {
  using namespace facebook::velox;
  test::VectorMaker vectorMaker{leafPool_.get()};
  auto wrapInDictionary = [&](const std::vector<vector_size_t>& indices,
                              const VectorPtr& values) {
    auto buf =
        AlignedBuffer::allocate<vector_size_t>(indices.size(), leafPool_.get());
    memcpy(
        buf->asMutable<vector_size_t>(),
        indices.data(),
        sizeof(vector_size_t) * indices.size());
    return BaseVector::wrapInDictionary(nullptr, buf, indices.size(), values);
  };
  auto vector = vectorMaker.rowVector({
      wrapInDictionary(
          {0, 0, 1, 1},
          vectorMaker.rowVector({
              wrapInDictionary(
                  {0, 0}, vectorMaker.arrayVector<int64_t>({{1, 2, 3}})),
          })),
  });
  nimble::VeloxWriterOptions options;
  options.flatMapColumns = {{"c0", {}}};
  options.dictionaryArrayColumns = {"c0"};
  std::string file;
  auto writeFile = std::make_unique<InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      ROW({"c0"}, {MAP(VARCHAR(), ARRAY(BIGINT()))}),
      std::move(writeFile),
      *rootPool_,
      std::move(options));
  writer.write(vector);
  writer.close();
  InMemoryReadFile readFile(file);
  nimble::VeloxReadParams params;
  params.readFlatMapFieldAsStruct = {"c0"};
  params.flatMapFeatureSelector["c0"].features = {"c0"};
  nimble::VeloxReader reader(&readFile, *leafPool_, nullptr, std::move(params));
  VectorPtr result;
  ASSERT_TRUE(reader.next(4, result));
  ASSERT_EQ(result->size(), 4);
  auto* c0 = result->asChecked<RowVector>()->childAt(0)->asChecked<RowVector>();
  auto& dict = c0->childAt(0);
  ASSERT_EQ(dict->encoding(), VectorEncoding::Simple::DICTIONARY);
  ASSERT_EQ(dict->size(), 4);
  auto* indices = dict->wrapInfo()->as<vector_size_t>();
  for (int i = 0; i < 4; ++i) {
    ASSERT_EQ(indices[i], 0);
  }
  auto* values = dict->valueVector()->asChecked<ArrayVector>();
  ASSERT_EQ(values->size(), 1);
  auto* elements = values->elements()->asChecked<SimpleVector<int64_t>>();
  ASSERT_EQ(values->sizeAt(0), 3);
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(elements->valueAt(i + values->offsetAt(0)), 1 + i);
  }
}

#define ASSERT_CHUNK_COUNT(count, chunked) \
  for (auto __i = 0; __i < count; ++__i) { \
    ASSERT_TRUE(chunked.hasNext());        \
    auto chunk = chunked.nextChunk();      \
    EXPECT_LT(0, chunk.size());            \
  }                                        \
  ASSERT_FALSE(chunked.hasNext());

void testChunks(
    velox::memory::MemoryPool& rootPool,
    uint32_t minStreamChunkRawSize,
    uint32_t maxStreamChunkRawSize,
    std::vector<std::tuple<velox::VectorPtr, bool>> vectors,
    std::function<void(const nimble::TabletReader&)> verifier,
    folly::F14FastMap<std::string, std::set<std::string>> flatMapColumns = {}) {
  ASSERT_LT(0, vectors.size());
  auto& type = std::get<0>(vectors[0])->type();

  auto leafPool = rootPool.addLeafChild("chunk_leaf");
  auto expected = velox::BaseVector::create(type, 0, leafPool.get());

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  auto flushDecision = false;
  nimble::VeloxWriter writer(
      type,
      std::move(writeFile),
      rootPool,
      {
          .flatMapColumns = std::move(flatMapColumns),
          .minStreamChunkRawSize = minStreamChunkRawSize,
          .flushPolicyFactory =
              [&]() {
                return std::make_unique<nimble::LambdaFlushPolicy>(
                    /*flushLambda=*/[&](auto&) { return false; },
                    /*chunkLambda=*/[&](auto&) { return flushDecision; });
              },
          .enableChunking = true,
      });

  for (const auto& vector : vectors) {
    flushDecision = std::get<1>(vector);
    writer.write(std::get<0>(vector));
    expected->append(std::get<0>(vector).get());
  }

  writer.close();

  folly::writeFile(file, "/tmp/afile");

  auto tabletReadFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto tablet = nimble::TabletReader::create(
      tabletReadFile, leafPool.get(), makeTestTabletOptions(leafPool.get()));
  verifier(*tablet);

  nimble::VeloxReader reader(
      std::make_shared<velox::InMemoryReadFile>(file), *leafPool);
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(expected->size(), result));
  ASSERT_EQ(expected->size(), result->size());
  for (auto i = 0; i < expected->size(); ++i) {
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i));
  }
  ASSERT_FALSE(reader.next(1, result));

  validateChunkSize(reader, minStreamChunkRawSize, maxStreamChunkRawSize);
}

TEST_F(VeloxWriterTest, omitsAllNonNullRowNullStreams) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto makeVector = [&]() {
    return vectorMaker.rowVector(
        {"nested"},
        {vectorMaker.rowVector(
            {"c1"}, {vectorMaker.flatVector<int32_t>({1, 2, 3})})});
  };
  auto makeVectorWithAllocatedAllNonNullNulls = [&]() {
    constexpr velox::vector_size_t kSize = 3;
    auto nestedType = velox::ROW({"c1"}, {velox::INTEGER()});
    auto nestedNulls = velox::AlignedBuffer::allocate<bool>(
        kSize, leafPool_.get(), velox::bits::kNotNull);
    auto nested = std::make_shared<velox::RowVector>(
        leafPool_.get(),
        nestedType,
        nestedNulls,
        kSize,
        std::vector<velox::VectorPtr>{
            vectorMaker.flatVector<int32_t>({1, 2, 3})});

    auto rootNulls = velox::AlignedBuffer::allocate<bool>(
        kSize, leafPool_.get(), velox::bits::kNotNull);
    return std::make_shared<velox::RowVector>(
        leafPool_.get(),
        velox::ROW({"nested"}, {nestedType}),
        rootNulls,
        kSize,
        std::vector<velox::VectorPtr>{nested});
  };

  auto allNonNullVector = makeVector();
  auto allocatedAllNonNullVector = makeVectorWithAllocatedAllNonNullNulls();
  auto topLevelNullVector = makeVector();
  topLevelNullVector->setNull(1, true);

  struct TestCase {
    std::string_view name;
    velox::RowVectorPtr input;
    velox::RowVectorPtr expected;
    bool ignoreTopLevelNulls;
    bool enableChunking;
    bool expectRootNullStreamOmitted;
  };

  const std::vector<TestCase> testCases{
      {
          .name = "allNonNull",
          .input = allNonNullVector,
          .expected = allNonNullVector,
          .ignoreTopLevelNulls = false,
          .enableChunking = false,
          .expectRootNullStreamOmitted = true,
      },
      {
          .name = "allNonNull",
          .input = allNonNullVector,
          .expected = allNonNullVector,
          .ignoreTopLevelNulls = false,
          .enableChunking = true,
          .expectRootNullStreamOmitted = true,
      },
      {
          .name = "allocatedAllNonNullNulls",
          .input = allocatedAllNonNullVector,
          .expected = allNonNullVector,
          .ignoreTopLevelNulls = false,
          .enableChunking = false,
          .expectRootNullStreamOmitted = true,
      },
      {
          .name = "allocatedAllNonNullNulls",
          .input = allocatedAllNonNullVector,
          .expected = allNonNullVector,
          .ignoreTopLevelNulls = false,
          .enableChunking = true,
          .expectRootNullStreamOmitted = true,
      },
      {
          .name = "preservedTopLevelNulls",
          .input = topLevelNullVector,
          .expected = topLevelNullVector,
          .ignoreTopLevelNulls = false,
          .enableChunking = false,
          .expectRootNullStreamOmitted = false,
      },
      {
          .name = "preservedTopLevelNulls",
          .input = topLevelNullVector,
          .expected = topLevelNullVector,
          .ignoreTopLevelNulls = false,
          .enableChunking = true,
          .expectRootNullStreamOmitted = false,
      },
      {
          .name = "ignoredTopLevelNulls",
          .input = topLevelNullVector,
          .expected = allNonNullVector,
          .ignoreTopLevelNulls = true,
          .enableChunking = false,
          .expectRootNullStreamOmitted = true,
      },
      {
          .name = "ignoredTopLevelNulls",
          .input = topLevelNullVector,
          .expected = allNonNullVector,
          .ignoreTopLevelNulls = true,
          .enableChunking = true,
          .expectRootNullStreamOmitted = true,
      },
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(
        fmt::format(
            "case={}, enableChunking={}",
            testCase.name,
            testCase.enableChunking));

    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

    nimble::VeloxWriterOptions options;
    options.enableChunking = testCase.enableChunking;
    options.ignoreTopLevelNulls = testCase.ignoreTopLevelNulls;

    nimble::VeloxWriter writer(
        testCase.input->type(),
        std::move(writeFile),
        *rootPool_,
        std::move(options));
    writer.write(testCase.input);
    writer.close();

    auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
    auto tablet = nimble::TabletReader::create(
        readFile, leafPool_.get(), makeTestTabletOptions(leafPool_.get()));
    ASSERT_EQ(1, tablet->stripeCount());

    nimble::VeloxReader reader(readFile.get(), *leafPool_);
    const auto& root = reader.schema()->asRow();
    const auto& nested = root.childAt(0)->asRow();
    const auto rootNullOffset = root.nullsDescriptor().offset();
    const auto nestedNullOffset = nested.nullsDescriptor().offset();
    const auto stripeIdentifier = tablet->stripeIdentifier(0);
    const auto streamSizes = tablet->streamSizes(stripeIdentifier);
    ASSERT_GT(streamSizes.size(), static_cast<size_t>(rootNullOffset));
    ASSERT_GT(streamSizes.size(), static_cast<size_t>(nestedNullOffset));

    std::array<uint32_t, 2> nullStreamOffsets{rootNullOffset, nestedNullOffset};
    auto streamLoaders = tablet->load(stripeIdentifier, nullStreamOffsets);
    ASSERT_EQ(2, streamLoaders.size());

    // All-non-null Row null streams are omitted on the wire. The root null
    // stream is omitted only when the root is logically all-non-null (no nulls,
    // or top-level nulls ignored); otherwise it is written and round-trips the
    // top-level nulls. The always-non-null nested row null stream is omitted.
    if (testCase.expectRootNullStreamOmitted) {
      EXPECT_EQ(0, streamSizes[rootNullOffset]);
      EXPECT_EQ(nullptr, streamLoaders[0]);
    } else {
      EXPECT_GT(streamSizes[rootNullOffset], 0);
      EXPECT_NE(nullptr, streamLoaders[0]);
    }
    EXPECT_EQ(0, streamSizes[nestedNullOffset]);
    EXPECT_EQ(nullptr, streamLoaders[1]);

    velox::VectorPtr result;
    ASSERT_TRUE(reader.next(testCase.expected->size(), result));
    ASSERT_EQ(result->size(), testCase.expected->size());
    for (velox::vector_size_t i = 0; i < testCase.expected->size(); ++i) {
      ASSERT_TRUE(result->equalValueAt(testCase.expected.get(), i, i));
    }
  }
}

TEST_F(VeloxWriterTest, chunkedStreamsRowAllNullsNoChunks) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({}),
      });
  vector->appendNulls(5);

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 0,
      /* maxStreamChunkRawSize */ 1024,
      {{vector, false}, {vector, false}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // Logically, there should be two streams in the tablet.
        // However, when writing stripes, we do not write empty streams.
        // In this case, the integer column is empty, and therefore, omitted.
        ASSERT_EQ(1, tablet.streamCount(stripeIdentifier));
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[0]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 1>{0});
        ASSERT_EQ(1, streamLoaders.size());

        // No chunks used, so expecting single chunk
        nimble::InMemoryChunkedStream chunked{
            *leafPool_, std::move(streamLoaders[0])};
        ASSERT_CHUNK_COUNT(1, chunked);
      });
}

TEST_F(VeloxWriterTest, chunkedStreamsRowAllNullsWithChunksMinSizeBig) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({}),
      });
  vector->appendNulls(5);

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 1024,
      /* maxStreamChunkRawSize */ 1024,
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // Logically, there should be two streams in the tablet.
        // However, when writing stripes, we do not write empty streams.
        // In this case, the integer column is empty, and therefore, omitted.
        ASSERT_EQ(1, tablet.streamCount(stripeIdentifier));
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[0]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 1>{0});
        ASSERT_EQ(1, streamLoaders.size());

        // Chunks requested, but min chunk size is too big, so expecting one
        // merged chunk
        nimble::InMemoryChunkedStream chunked{
            *leafPool_, std::move(streamLoaders[0])};
        ASSERT_CHUNK_COUNT(1, chunked);
      });
}

TEST_F(VeloxWriterTest, chunkedStreamsRowAllNullsWithChunksMinSizeZero) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({}),
      });
  vector->appendNulls(5);

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 0,
      /* maxStreamChunkRawSize */ 1024,
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // Logically, there should be two streams in the tablet.
        // However, when writing stripes, we do not write empty streams.
        // In this case, the integer column is empty, and therefore, omitted.
        ASSERT_EQ(1, tablet.streamCount(stripeIdentifier));
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[0]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 1>{0});
        ASSERT_EQ(1, streamLoaders.size());

        // Chunks requested, and min chunk size is zero, so expecting two
        // separate chunks.
        nimble::InMemoryChunkedStream chunked{
            *leafPool_, std::move(streamLoaders[0])};
        ASSERT_CHUNK_COUNT(2, chunked);
      });
}

TEST_F(VeloxWriterTest, chunkedStreamsRowSomeNullsNoChunks) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto nullsVector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({}),
      });
  nullsVector->appendNulls(5);

  auto nonNullsVector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({1, 2, 3}),
      });
  nonNullsVector->setNull(1, /* isNull */ true);

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 0,
      /* maxStreamChunkRawSize */ 1024,
      {{nullsVector, false}, {nonNullsVector, false}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // We have values in stream 2, so it is not optimized away.
        ASSERT_EQ(2, tablet.streamCount(stripeIdentifier));
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[0]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[1]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 2>{0, 1});
        ASSERT_EQ(2, streamLoaders.size());
        {
          // No chunks requested, so expecting single chunk.
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[0])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
        {
          // No chunks requested, so expecting single chunk.
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[1])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
      });
}

TEST_F(VeloxWriterTest, chunkedStreamsRowSomeNullsWithChunksMinSizeBig) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto nullsVector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({}),
      });
  nullsVector->appendNulls(5);

  auto nonNullsVector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({1, 2, 3}),
      });
  nonNullsVector->setNull(1, /* isNull */ true);

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 1024,
      /* maxStreamChunkRawSize */ 1024,
      {{nullsVector, true}, {nonNullsVector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        ASSERT_EQ(2, tablet.streamCount(stripeIdentifier));
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[0]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[1]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 2>{0, 1});
        ASSERT_EQ(2, streamLoaders.size());
        {
          // Chunks requested, but min chunk size is too big, so expecting one
          // merged chunk.
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[0])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
        {
          // Chunks requested, but min chunk size is too big, so expecting one
          // merged chunk.
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[1])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
      });
}

TEST_F(VeloxWriterTest, chunkedStreamsRowSomeNullsWithChunksMinSizeZero) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto nullsVector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({}),
      });
  nullsVector->appendNulls(5);

  auto nonNullsVector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({1, 2, 3}),
      });
  nonNullsVector->setNull(1, /* isNull */ true);

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 0,
      /* maxStreamChunkRawSize */ 1024,
      {{nullsVector, true}, {nonNullsVector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        ASSERT_EQ(2, tablet.streamCount(stripeIdentifier));
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[0]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[1]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 2>{0, 1});
        ASSERT_EQ(2, streamLoaders.size());
        {
          // Chunks requested, and min chunk size is zero, so expecting two
          // separate chunks.
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[0])};
          ASSERT_CHUNK_COUNT(2, chunked);
        }
        {
          // Chunks requested, and min chunk size is zero. However, first
          // write didn't have any data, so no chunk was written.
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[1])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
      });
}

TEST_F(VeloxWriterTest, chunkedStreamsRowNoNullsNoChunks) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({1, 2, 3}),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 0,
      /* maxStreamChunkRawSize */ 1024,
      {{vector, false}, {vector, false}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        ASSERT_EQ(2, tablet.streamCount(stripeIdentifier));

        // When there are no nulls, the nulls stream is omitted.
        EXPECT_EQ(0, tablet.streamSizes(stripeIdentifier)[0]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[1]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 2>{0, 1});
        ASSERT_EQ(2, streamLoaders.size());
        {
          // Nulls stream should be missing, as all values are non-null
          EXPECT_FALSE(streamLoaders[0]);
        }
        {
          // No chunks requested, so expecting one chunk.
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[1])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
      });
}

TEST_F(VeloxWriterTest, chunkedStreamsRowNoNullsWithChunksMinSizeBig) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({1, 2, 3}),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 1024,
      /* maxStreamChunkRawSize */ 2048,
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        ASSERT_EQ(2, tablet.streamCount(stripeIdentifier));

        // When there are no nulls, the nulls stream is omitted.
        EXPECT_EQ(0, tablet.streamSizes(stripeIdentifier)[0]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[1]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 2>{0, 1});
        ASSERT_EQ(2, streamLoaders.size());
        {
          // Nulls stream should be missing, as all values are non-null
          EXPECT_FALSE(streamLoaders[0]);
        }
        {
          // Chunks requested, but min size is too big, so expecting one
          // merged chunk.
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[1])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
      });
}

TEST_F(VeloxWriterTest, chunkedStreamsRowNoNullsWithChunksMinSizeZero) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({1, 2, 3}),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 0,
      /* maxStreamChunkRawSize */ 1024,
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        ASSERT_EQ(2, tablet.streamCount(stripeIdentifier));

        // When there are no nulls, the nulls stream is omitted.
        EXPECT_EQ(0, tablet.streamSizes(stripeIdentifier)[0]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[1]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 2>{0, 1});
        ASSERT_EQ(2, streamLoaders.size());
        {
          // Nulls stream should be missing, as all values are non-null
          EXPECT_FALSE(streamLoaders[0]);
        }
        {
          // Chunks requested, with min size zero, so expecting two chunks.
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[1])};
          ASSERT_CHUNK_COUNT(2, chunked);
        }
      });
}

TEST_F(VeloxWriterTest, chunkedStreamsChildAllNullsNoChunks) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVectorNullable<int32_t>(
              {std::nullopt, std::nullopt, std::nullopt}),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 0,
      /* maxStreamChunkRawSize */ 1024,
      {{vector, false}, {vector, false}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // When all rows are not null, the nulls stream is omitted.
        // When all values are null, the values stream is omitted.
        // Since these are the last two stream, they are optimized away.
        ASSERT_EQ(0, tablet.streamCount(stripeIdentifier));
      });
}

TEST_F(VeloxWriterTest, chunkedStreamsChildAllNullsWithChunksMinSizeBig) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVectorNullable<int32_t>(
              {std::nullopt, std::nullopt, std::nullopt}),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 1024,
      /* maxStreamChunkRawSize */ 2048,
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // When all rows are not null, the nulls stream is omitted.
        // When all values are null, the values stream is omitted.
        // Since these are the last two stream, they are optimized away.
        ASSERT_EQ(0, tablet.streamCount(stripeIdentifier));
      });
}

TEST_F(VeloxWriterTest, chunkedStreamsChildAllNullsWithChunksMinSizeZero) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVectorNullable<int32_t>(
              {std::nullopt, std::nullopt, std::nullopt}),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 0,
      /* maxStreamChunkRawSize */ 1024,
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // When all rows are not null, the nulls stream is omitted.
        // When all values are null, the values stream is omitted.
        // Since these are the last two stream, they are optimized away.
        ASSERT_EQ(0, tablet.streamCount(stripeIdentifier));
      });
}

TEST_F(VeloxWriterTest, chunkedStreamsFlatmapAllNullsNoChunks) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.mapVector<int32_t, int32_t>(
              std::vector<std::optional<
                  std::vector<std::pair<int32_t, std::optional<int32_t>>>>>{
                  std::nullopt,
                  std::nullopt,
                  //   std::vector<std::pair<int32_t,
                  //   std::optional<int32_t>>>{
                  //       {5, 6}},
                  std::nullopt,
              }),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 0,
      /* maxStreamChunkRawSize */ 1024,
      {{vector, false}, {vector, false}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // Expected streams:
        // 0: Row nulls stream (expected empty, as all values are not null)
        // 1: Flatmap nulls stream
        ASSERT_EQ(2, tablet.streamCount(stripeIdentifier));
        EXPECT_EQ(0, tablet.streamSizes(stripeIdentifier)[0]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[1]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 2>{0, 1});
        ASSERT_EQ(2, streamLoaders.size());

        // No chunks used, so expecting single chunk
        nimble::InMemoryChunkedStream chunked{
            *leafPool_, std::move(streamLoaders[1])};
        ASSERT_CHUNK_COUNT(1, chunked);
      },
      /* flatmapColumns */
      (folly::F14FastMap<std::string, std::set<std::string>>){{"c1", {}}});
}

TEST_F(VeloxWriterTest, chunkedStreamsFlatmapAllNullsWithChunksMinSizeBig) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.mapVector<int32_t, int32_t>(
              std::vector<std::optional<
                  std::vector<std::pair<int32_t, std::optional<int32_t>>>>>{
                  std::nullopt,
                  std::nullopt,
                  std::nullopt,
              }),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 1024,
      /* maxStreamChunkRawSize */ 2048,
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // Expected streams:
        // 0: Row nulls stream (expected empty, as all values are not null)
        // 1: Flatmap nulls stream
        ASSERT_EQ(2, tablet.streamCount(stripeIdentifier));
        EXPECT_EQ(0, tablet.streamSizes(stripeIdentifier)[0]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[1]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 2>{0, 1});
        ASSERT_EQ(2, streamLoaders.size());

        // Chunks requested, but min size is too big, so expecting single
        // merged chunk
        nimble::InMemoryChunkedStream chunked{
            *leafPool_, std::move(streamLoaders[1])};
        ASSERT_CHUNK_COUNT(1, chunked);
      },
      /* flatmapColumns */
      (folly::F14FastMap<std::string, std::set<std::string>>){{"c1", {}}});
}

TEST_F(VeloxWriterTest, chunkedStreamsFlatmapAllNullsWithChunksMinSizeZero) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.mapVector<int32_t, int32_t>(
              std::vector<std::optional<
                  std::vector<std::pair<int32_t, std::optional<int32_t>>>>>{
                  std::nullopt,
                  std::nullopt,
                  std::nullopt,
              }),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 0,
      /* maxStreamChunkRawSize */ 1024,
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // Expected streams:
        // 0: Row nulls stream (expected empty, as all values are not null)
        // 1: Flatmap nulls stream
        ASSERT_EQ(2, tablet.streamCount(stripeIdentifier));
        EXPECT_EQ(0, tablet.streamSizes(stripeIdentifier)[0]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[1]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 2>{0, 1});
        ASSERT_EQ(2, streamLoaders.size());

        // Chunks requested, with min size zero, so expecting two chunks
        nimble::InMemoryChunkedStream chunked{
            *leafPool_, std::move(streamLoaders[1])};
        ASSERT_CHUNK_COUNT(2, chunked);
      },
      /* flatmapColumns */
      (folly::F14FastMap<std::string, std::set<std::string>>){{"c1", {}}});
}

TEST_F(VeloxWriterTest, chunkedStreamsFlatmapSomeNullsNoChunks) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto nullsVector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.mapVector<int32_t, int32_t>(
              std::vector<std::optional<
                  std::vector<std::pair<int32_t, std::optional<int32_t>>>>>{
                  std::nullopt,
                  std::nullopt,
                  std::nullopt,
              }),
      });
  auto nonNullsVector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.mapVector<int32_t, int32_t>(
              std::vector<std::optional<
                  std::vector<std::pair<int32_t, std::optional<int32_t>>>>>{
                  std::nullopt,
                  std::vector<std::pair<int32_t, std::optional<int32_t>>>{
                      {5, 6}},
                  std::nullopt,
              }),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 0,
      /* maxStreamChunkRawSize */ 1024,
      {{nullsVector, false}, {nonNullsVector, false}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // Expected streams:
        // 0: Row nulls stream (expected empty, as all values are not null)
        // 1: Flatmap nulls stream
        // 2: Scalar stream (flatmap value for key 5)
        // 3: Scalar stream (flatmap in-map for key 5)
        ASSERT_EQ(4, tablet.streamCount(stripeIdentifier));
        EXPECT_EQ(0, tablet.streamSizes(stripeIdentifier)[0]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[1]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[2]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[3]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 4>{0, 1, 2, 3});
        ASSERT_EQ(4, streamLoaders.size());

        EXPECT_FALSE(streamLoaders[0]);
        EXPECT_TRUE(streamLoaders[1]);
        EXPECT_TRUE(streamLoaders[2]);
        EXPECT_TRUE(streamLoaders[3]);

        {
          // No chunks used, so expecting single chunk
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[1])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
        {
          // No chunks used, so expecting single chunk
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[2])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
        {
          // No chunks used, so expecting single chunk
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[3])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
      },
      /* flatmapColumns */
      (folly::F14FastMap<std::string, std::set<std::string>>){{"c1", {}}});
}

TEST_F(VeloxWriterTest, chunkedStreamsFlatmapSomeNullsWithChunksMinSizeBig) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto nullsVector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.mapVector<int32_t, int32_t>(
              std::vector<std::optional<
                  std::vector<std::pair<int32_t, std::optional<int32_t>>>>>{
                  std::nullopt,
                  std::nullopt,
                  std::nullopt,
              }),
      });
  auto nonNullsVector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.mapVector<int32_t, int32_t>(
              std::vector<std::optional<
                  std::vector<std::pair<int32_t, std::optional<int32_t>>>>>{
                  std::nullopt,
                  std::vector<std::pair<int32_t, std::optional<int32_t>>>{
                      {5, 6}},
                  std::nullopt,
              }),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 1024,
      /* maxStreamChunkRawSize */ 2048,
      {{nullsVector, true}, {nonNullsVector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // Expected streams:
        // 0: Row nulls stream (expected empty, as all values are not null)
        // 1: Flatmap nulls stream
        // 2: Scalar stream (flatmap value for key 5)
        // 3: Scalar stream (flatmap in-map for key 5)
        ASSERT_EQ(4, tablet.streamCount(stripeIdentifier));
        EXPECT_EQ(0, tablet.streamSizes(stripeIdentifier)[0]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[1]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[2]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[3]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 4>{0, 1, 2, 3});
        ASSERT_EQ(4, streamLoaders.size());

        EXPECT_FALSE(streamLoaders[0]);
        EXPECT_TRUE(streamLoaders[1]);
        EXPECT_TRUE(streamLoaders[2]);
        EXPECT_TRUE(streamLoaders[3]);

        {
          // Chunks requested, but min size is big, so expecting single merged
          // chunk
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[1])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
        {
          // Chunks requested, but min size is big, so expecting single merged
          // chunk
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[2])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
        {
          // Chunks requested, but min size is big, so expecting single merged
          // chunk
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[3])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
      },
      /* flatmapColumns */
      (folly::F14FastMap<std::string, std::set<std::string>>){{"c1", {}}});
}

TEST_F(VeloxWriterTest, chunkedStreamsFlatmapSomeNullsWithChunksMinSizeZero) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto nullsVector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.mapVector<int32_t, int32_t>(
              std::vector<std::optional<
                  std::vector<std::pair<int32_t, std::optional<int32_t>>>>>{
                  std::nullopt,
                  std::nullopt,
                  std::nullopt,
              }),
      });
  auto nonNullsVector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.mapVector<int32_t, int32_t>(
              std::vector<std::optional<
                  std::vector<std::pair<int32_t, std::optional<int32_t>>>>>{
                  std::nullopt,
                  std::vector<std::pair<int32_t, std::optional<int32_t>>>{
                      {5, 6}},
                  std::nullopt,
              }),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 0,
      /* maxStreamChunkRawSize */ 1024,
      {{nullsVector, true}, {nonNullsVector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.stripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // Expected streams:
        // 0: Row nulls stream (expected empty, as all values are not null)
        // 1: Flatmap nulls stream
        // 2: Scalar stream (flatmap value for key 5)
        // 3: Scalar stream (flatmap in-map for key 5)
        ASSERT_EQ(4, tablet.streamCount(stripeIdentifier));
        EXPECT_EQ(0, tablet.streamSizes(stripeIdentifier)[0]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[1]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[2]);
        EXPECT_LT(0, tablet.streamSizes(stripeIdentifier)[3]);

        auto streamLoaders =
            tablet.load(stripeIdentifier, std::array<uint32_t, 4>{0, 1, 2, 3});
        ASSERT_EQ(4, streamLoaders.size());

        EXPECT_FALSE(streamLoaders[0]);
        EXPECT_TRUE(streamLoaders[1]);
        EXPECT_TRUE(streamLoaders[2]);
        EXPECT_TRUE(streamLoaders[3]);

        {
          // Chunks requested, with min size zero, so expecting two chunks
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[1])};
          ASSERT_CHUNK_COUNT(2, chunked);
        }
        {
          // Chunks requested, with min size zero, but first write didn't
          // contain any values, so expecting single merged chunk
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[2])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
        {
          // Chunks requested, with min size zero, but first write didn't
          // have any items in the map, so expecting single merged chunk
          nimble::InMemoryChunkedStream chunked{
              *leafPool_, std::move(streamLoaders[3])};
          ASSERT_CHUNK_COUNT(1, chunked);
        }
      },
      /* flatmapColumns */
      (folly::F14FastMap<std::string, std::set<std::string>>){{"c1", {}}});
}

TEST_F(VeloxWriterTest, rawSizeWritten) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  constexpr uint64_t expectedRawSize = sizeof(int32_t) * 20;
  auto vector = vectorMaker.rowVector(
      {"row1", "row2"},
      {
          vectorMaker.rowVector({
              vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5}),
              vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5}),
          }),
          vectorMaker.rowVector({
              vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5}),
              vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5}),
          }),
      });

  // Test with enableVectorizedStats = false (default, uses kStatsSection)
  {
    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriterOptions options{};
    options.enableVectorizedStats = false;
    nimble::VeloxWriter writer(
        vector->type(), std::move(writeFile), *rootPool_, options);
    writer.write(vector);
    writer.close();

    auto statsReadFile = std::make_shared<velox::InMemoryReadFile>(file);
    nimble::TabletReader::Options readerOptions =
        makeTestTabletOptions(leafPool_.get());
    readerOptions.preloadOptionalSections = {
        std::string(facebook::nimble::kStatsSection)};
    auto tablet = facebook::nimble::TabletReader::create(
        statsReadFile, leafPool_.get(), readerOptions);
    auto statsSection =
        tablet->loadOptionalSection(readerOptions.preloadOptionalSections[0]);
    ASSERT_TRUE(statsSection.has_value());

    // Use flatbuffers to deserialize the stats payload
    auto rawSize = flatbuffers::GetRoot<nimble::serialization::Stats>(
                       statsSection->content().data())
                       ->raw_size();
    ASSERT_EQ(expectedRawSize, rawSize);
  }

  // Test with enableVectorizedStats = true (uses kVectorizedStatsSection)
  {
    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriterOptions writerOptions{};
    writerOptions.enableVectorizedStats = true;
    nimble::VeloxWriter writer(
        vector->type(), std::move(writeFile), *rootPool_, writerOptions);
    writer.write(vector);
    writer.close();

    auto vecStatsReadFile = std::make_shared<velox::InMemoryReadFile>(file);
    nimble::TabletReader::Options readerOptions =
        makeTestTabletOptions(leafPool_.get());
    readerOptions.preloadOptionalSections = {
        std::string(facebook::nimble::kVectorizedStatsSection)};
    auto tablet = facebook::nimble::TabletReader::create(
        vecStatsReadFile, leafPool_.get(), readerOptions);
    auto statsSection =
        tablet->loadOptionalSection(readerOptions.preloadOptionalSections[0]);
    ASSERT_TRUE(statsSection.has_value());

    // Use VectorizedFileStats to deserialize the stats payload
    auto fileStats = nimble::VectorizedFileStats::deserialize(
        statsSection->content(), *leafPool_);
    ASSERT_NE(fileStats, nullptr);

    // Convert to column statistics using schema and nimbleType
    auto nimbleType = nimble::convertToNimbleType(*vector->type());
    auto columnStats =
        fileStats->toColumnStatistics(vector->type(), nimbleType);
    ASSERT_FALSE(columnStats.empty());

    // The root column statistics contains the raw size (logical size)
    auto rawSize = columnStats.front()->getLogicalSize();
    ASSERT_EQ(expectedRawSize, rawSize);
  }
}

struct ChunkFlushPolicyTestCase {
  const size_t batchCount{20};
  const bool enableChunking{true};
  const uint64_t targetStripeSizeBytes{250 << 10};
  const uint64_t writerMemoryHighThresholdBytes{80 << 10};
  const uint64_t writerMemoryLowThresholdBytes{75 << 10};
  const double estimatedCompressionFactor{1.3};
  const uint32_t minStreamChunkRawSize{100};
  const uint32_t maxStreamChunkRawSize{128 << 10};
  const uint32_t expectedStripeCount{0};
  const uint32_t expectedMaxChunkCount{0};
  const uint32_t expectedMinChunkCount{0};
  const uint32_t chunkedStreamBatchSize{2};
};

class ChunkFlushPolicyTest
    : public VeloxWriterTest,
      public ::testing::WithParamInterface<ChunkFlushPolicyTestCase> {};

TEST_P(ChunkFlushPolicyTest, ChunkFlushPolicyIntegration) {
  const auto type = velox::ROW(
      {{"BIGINT", velox::BIGINT()}, {"SMALLINT", velox::SMALLINT()}});
  nimble::VeloxWriterOptions writerOptions{
      .minStreamChunkRawSize = GetParam().minStreamChunkRawSize,
      .maxStreamChunkRawSize = GetParam().maxStreamChunkRawSize,
      .chunkedStreamBatchSize = GetParam().chunkedStreamBatchSize,
      .flushPolicyFactory = GetParam().enableChunking
          ? []() -> std::unique_ptr<nimble::FlushPolicy> {
              return std::make_unique<nimble::ChunkFlushPolicy>(
                      nimble::ChunkFlushPolicyConfig{
                          .writerMemoryHighThresholdBytes = GetParam().writerMemoryHighThresholdBytes,
                          .writerMemoryLowThresholdBytes = GetParam().writerMemoryLowThresholdBytes,
                          .targetStripeSizeBytes = GetParam().targetStripeSizeBytes,
                          .estimatedCompressionFactor =
                              GetParam().estimatedCompressionFactor,
                      });
            }
          : []() -> std::unique_ptr<nimble::FlushPolicy> {
              return std::make_unique<nimble::StripeRawSizeFlushPolicy>(
                  GetParam().targetStripeSizeBytes);
            },
      .enableChunking = GetParam().enableChunking,
  };

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(
      type, std::move(writeFile), *rootPool_, std::move(writerOptions));
  const auto batches = generateBatches(
      type,
      GetParam().batchCount,
      /*size=*/4000,
      /*seed=*/20221110,
      *leafPool_);

  for (const auto& batch : batches) {
    writer.write(batch);
  }
  writer.close();
  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);
  ChunkSizeResults result = validateChunkSize(
      reader,
      GetParam().minStreamChunkRawSize,
      GetParam().maxStreamChunkRawSize);

  EXPECT_EQ(GetParam().expectedStripeCount, result.stripeCount);
  EXPECT_EQ(GetParam().expectedMaxChunkCount, result.maxChunkCount);
  EXPECT_EQ(GetParam().expectedMinChunkCount, result.minChunkCount);
}

TEST_F(VeloxWriterTest, batchedChunkingRelievesMemoryPressure) {
  // Verify we stop chunking early when chunking relieves memory pressure.
  const uint32_t seed = FLAGS_writer_tests_seed > 0 ? FLAGS_writer_tests_seed
                                                    : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};
  const uint32_t rowCount =
      std::uniform_int_distribution<uint32_t>(1, 4096)(rng);

  velox::VectorFuzzer fuzzer({.vectorSize = rowCount}, leafPool_.get(), seed);
  const auto stringColumn = fuzzer.fuzzFlat(velox::VARCHAR());
  const auto intColumn = fuzzer.fuzzFlat(velox::INTEGER());

  for (bool disableSharedStringBuffers : {true, false}) {
    LOG(INFO) << "disableSharedStringBuffers: " << disableSharedStringBuffers;
    nimble::RawSizeContext context;
    nimble::OrderedRanges ranges;
    ranges.add(0, rowCount);
    // When shared string buffers are disabled, each row contributes the size
    // of a uint64_t offset; otherwise it contributes the size of a
    // std::string_view.
    const uint64_t perRowOverhead = disableSharedStringBuffers
        ? sizeof(uint64_t)
        : sizeof(std::string_view);
    const uint64_t stringColumnRawSize =
        nimble::getRawSizeFromVector(stringColumn, ranges, context) +
        perRowOverhead * rowCount;
    const uint64_t intColumnRawSize =
        nimble::getRawSizeFromVector(intColumn, ranges, context);

    constexpr size_t kColumnCount = 20;
    constexpr size_t kBatchSize = 4;
    std::vector<velox::VectorPtr> children(kColumnCount);
    std::vector<std::string> columnNames(kColumnCount);
    uint64_t totalRawSize = 0;
    for (size_t i = 0; i < kColumnCount; i += 2) {
      columnNames[i] = fmt::format("string_column_{}", i);
      columnNames[i + 1] = fmt::format("int_column_{}", i);
      children[i] = stringColumn;
      children[i + 1] = intColumn;
      totalRawSize += intColumnRawSize + stringColumnRawSize;
    }

    velox::test::VectorMaker vectorMaker{leafPool_.get()};
    const auto rowVector = vectorMaker.rowVector(columnNames, children);

    // Ensure we can chunk the integer streams into multiple chunks
    const uint64_t minChunkSize = intColumnRawSize / 2;
    // In the aggresive stage, we chunk large streams in the first batch. We set
    // the memoryPressureThreshold with the assumption of at least once max
    // chunk being produced for each stream in that batch.
    const uint64_t maxChunkSize = stringColumnRawSize / 2;
    uint64_t memoryPressureThreshold =
        totalRawSize - (kBatchSize * maxChunkSize);

    std::vector<bool> actualChunkingDecisions;
    nimble::VeloxWriterOptions writerOptions;
    writerOptions.chunkedStreamBatchSize = kBatchSize;
    writerOptions.enableChunking = true;
    writerOptions.disableSharedStringBuffers = disableSharedStringBuffers;
    writerOptions.minStreamChunkRawSize = minChunkSize;
    writerOptions.maxStreamChunkRawSize = maxChunkSize;
    writerOptions.flushPolicyFactory =
        [&]() -> std::unique_ptr<nimble::FlushPolicy> {
      return std::make_unique<nimble::LambdaFlushPolicy>(
          /* shouldFlush */ [](const auto&) { return true; },
          /* shouldChunk */
          [&](const nimble::StripeProgress& stripeProgress) {
            bool shouldChunk =
                stripeProgress.stripeRawSize > memoryPressureThreshold;
            actualChunkingDecisions.push_back(shouldChunk);
            if (!shouldChunk) {
              // Force memory pressure after the initial aggressive stage.
              memoryPressureThreshold = 0;
              // Force beginning of the non-aggressive chunking stage.
              shouldChunk = true;
            }
            return shouldChunk;
          });
    };

    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriter writer(
        rowVector->type(), std::move(writeFile), *rootPool_, writerOptions);
    writer.write(rowVector);
    writer.close();

    // We expect true and then false for the aggressive stage.
    EXPECT_GE(actualChunkingDecisions.size(), 2);
    EXPECT_TRUE(actualChunkingDecisions[0]);
    EXPECT_FALSE(actualChunkingDecisions[1]);

    velox::InMemoryReadFile readFile(file);
    nimble::VeloxReader reader(&readFile, *leafPool_);
    validateChunkSize(
        reader,
        writerOptions.minStreamChunkRawSize,
        writerOptions.maxStreamChunkRawSize);
  }
}

TEST_F(VeloxWriterTest, ignoreTopLevelNulls) {
  auto seed = folly::randomNumberSeed();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  constexpr velox::vector_size_t kSize = 10;
  auto type = velox::ROW({"c1"}, {velox::INTEGER()});
  auto nulls = velox::AlignedBuffer::allocate<bool>(kSize, leafPool_.get());
  auto rawNulls = nulls->asMutable<uint64_t>();
  for (auto i = 0; i < kSize; ++i) {
    velox::bits::setBit(rawNulls, i, folly::Random::oneIn(2, rng));
  }
  auto vector =
      vectorMaker.flatVector<int32_t>(kSize, [](auto row) { return row; });
  velox::VectorPtr rowVector = std::make_shared<velox::RowVector>(
      leafPool_.get(),
      type,
      nulls,
      kSize,
      std::vector<velox::VectorPtr>{vector});

  auto verify =
      [](auto& pool, const auto& input, const auto& expected, auto options) {
        std::string file;
        auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
        nimble::VeloxWriter writer(
            input->type(), std::move(writeFile), pool, std::move(options));
        writer.write(input);
        writer.close();

        velox::InMemoryReadFile readFile(file);
        nimble::VeloxReader reader(&readFile, pool);
        velox::VectorPtr output;
        reader.next(expected->size(), output);
        ASSERT_EQ(output->size(), expected->size());
        for (auto i = 0; i < kSize; ++i) {
          ASSERT_TRUE(output->equalValueAt(expected.get(), i, i));
        }
      };

  // Write with top level nulls
  verify(*leafPool_, rowVector, rowVector, nimble::VeloxWriterOptions{});

  // Write ignoring top level nulls with flat input
  {
    auto expected = std::make_shared<velox::RowVector>(
        leafPool_.get(),
        type,
        nullptr,
        kSize,
        std::vector<velox::VectorPtr>{vector});
    verify(
        *leafPool_,
        rowVector,
        expected,
        nimble::VeloxWriterOptions{
            .ignoreTopLevelNulls = true,
        });
  }

  // Write ignoring top level nulls with encoded input
  {
    auto indices = velox::AlignedBuffer::allocate<velox::vector_size_t>(
        kSize, leafPool_.get());
    auto rawIndices = indices->asMutable<velox::vector_size_t>();
    for (auto i = 0; i < kSize; ++i) {
      rawIndices[i] = i;
    }
    rowVector =
        velox::BaseVector::wrapInDictionary(nullptr, indices, kSize, rowVector);
    auto expected = std::make_shared<velox::RowVector>(
        leafPool_.get(),
        type,
        nullptr,
        kSize,
        std::vector<velox::VectorPtr>{vector});
    verify(
        *leafPool_,
        rowVector,
        expected,
        nimble::VeloxWriterOptions{
            .ignoreTopLevelNulls = true,
        });
  }
}

struct TimestampTestCase {
  std::string name;
  velox::Timestamp timestamp;
};

class TimestampEdgeCaseTest
    : public VeloxWriterTest,
      public ::testing::WithParamInterface<TimestampTestCase> {};

// We rely on fuzz tests in VeloxReaderTests for more complex data shapes.
TEST_P(TimestampEdgeCaseTest, RoundTrip) {
  auto testCase = GetParam();
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"timestamp"},
      {vectorMaker.flatVector<velox::Timestamp>(
          std::vector<velox::Timestamp>{testCase.timestamp})});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(), std::move(writeFile), *rootPool_, {});
  writer.write(vector);
  writer.close();

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(1, result));

  auto actual = result->as<velox::RowVector>()
                    ->childAt(0)
                    ->asFlatVector<velox::Timestamp>()
                    ->valueAt(0);

  EXPECT_EQ(testCase.timestamp.getSeconds(), actual.getSeconds());
  EXPECT_EQ(testCase.timestamp.getNanos(), actual.getNanos());
}

// Test that timestamps exceeding Nimble's microsecond range cause overflow.
// Nimble stores timestamps as int64 microseconds, so seconds values beyond
// INT64_MAX/1'000'000 or INT64_MIN/1'000'000 will overflow during conversion.
TEST_F(VeloxWriterTest, TimestampOverflowMax) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto overflowVector = vectorMaker.rowVector(
      {"timestamp"},
      {vectorMaker.flatVector<velox::Timestamp>(std::vector<velox::Timestamp>{
          velox::Timestamp(INT64_MAX / 1'000'000 + 1, 0)})});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      overflowVector->type(), std::move(writeFile), *rootPool_, {});
  EXPECT_THROW(writer.write(overflowVector), nimble::NimbleUserError);
}

TEST_F(VeloxWriterTest, TimestampOverflowMin) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto overflowVector = vectorMaker.rowVector(
      {"timestamp"},
      {vectorMaker.flatVector<velox::Timestamp>(std::vector<velox::Timestamp>{
          velox::Timestamp(INT64_MIN / 1'000'000 - 1, 0)})});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      overflowVector->type(), std::move(writeFile), *rootPool_, {});
  EXPECT_THROW(writer.write(overflowVector), nimble::NimbleUserError);
}

INSTANTIATE_TEST_CASE_P(
    TimestampEdgeCaseTestSuite,
    TimestampEdgeCaseTest,
    ::testing::Values(
        TimestampTestCase{"Epoch", velox::Timestamp(0, 0)},
        TimestampTestCase{"EpochPlus1Nano", velox::Timestamp::fromNanos(1)},
        TimestampTestCase{"EpochMinus1Nano", velox::Timestamp::fromNanos(-1)},

        TimestampTestCase{"ExactMicrosecond", velox::Timestamp(0, 1'000)},
        TimestampTestCase{"ExactMillisecond", velox::Timestamp(0, 1'000'000)},

        TimestampTestCase{"Negative1ms", velox::Timestamp::fromMillis(-1)},
        TimestampTestCase{"Negative999ms", velox::Timestamp::fromMillis(-999)},
        TimestampTestCase{
            "Negative1000ms",
            velox::Timestamp::fromMillis(-1000)},
        TimestampTestCase{
            "Negative1500ms",
            velox::Timestamp::fromMillis(-1500)},

        TimestampTestCase{
            "SubMicroPrecisionConversion",
            velox::Timestamp(100, 999'999'999)},

        TimestampTestCase{
            "MaxMicros",
            velox::Timestamp(INT64_MAX / 1'000'000, 999)},
        TimestampTestCase{
            "MinMicros",
            velox::Timestamp(INT64_MIN / 1'000'000, 0)}));

INSTANTIATE_TEST_CASE_P(
    StripeRawSizeFlushPolicyTestSuite,
    StripeRawSizeFlushPolicyTest,
    ::testing::Values(
        StripeRawSizeFlushPolicyTestCase{
            .batchCount = 50,
            .rawStripeSize = 256 << 10,
            .stripeCount = 4},
        StripeRawSizeFlushPolicyTestCase{
            .batchCount = 100,
            .rawStripeSize = 256 << 10,
            .stripeCount = 7},
        StripeRawSizeFlushPolicyTestCase{
            .batchCount = 100,
            .rawStripeSize = 256 << 11,
            .stripeCount = 4},
        StripeRawSizeFlushPolicyTestCase{
            .batchCount = 100,
            .rawStripeSize = 256 << 12,
            .stripeCount = 2},
        StripeRawSizeFlushPolicyTestCase{
            .batchCount = 100,
            .rawStripeSize = 256 << 20,
            .stripeCount = 1}));

INSTANTIATE_TEST_CASE_P(
    ChunkFlushPolicyTestSuite,
    ChunkFlushPolicyTest,
    ::testing::Values(
        // Base case (no chunking, RawStripeSizeFlushPolicy)
        ChunkFlushPolicyTestCase{
            .batchCount = 20,
            .enableChunking = false,
            .targetStripeSizeBytes = 250 << 10, // 250KB
            .writerMemoryHighThresholdBytes = 80 << 10,
            .writerMemoryLowThresholdBytes = 75 << 10,
            .estimatedCompressionFactor = 1.3,
            .minStreamChunkRawSize = 100,
            .maxStreamChunkRawSize =
                std::numeric_limits<uint32_t>::max(), // no limit
            .expectedStripeCount = 4,
            .expectedMaxChunkCount = 1,
            .expectedMinChunkCount = 1,
            .chunkedStreamBatchSize = 2,
        },
        // Baseline with default settings (has chunking)
        ChunkFlushPolicyTestCase{
            .batchCount = 20,
            .enableChunking = true,
            .targetStripeSizeBytes = 250 << 10, // 250KB
            .writerMemoryHighThresholdBytes = 80 << 10,
            .writerMemoryLowThresholdBytes = 75 << 10,
            .estimatedCompressionFactor = 1.3,
            .minStreamChunkRawSize = 100,
            .maxStreamChunkRawSize = 128 << 10,
            .expectedStripeCount = 7,
            .expectedMaxChunkCount = 2,
            .expectedMinChunkCount = 1,
            .chunkedStreamBatchSize = 2,
        },
        // Reducing maxStreamChunkRawSize produces more chunks
        ChunkFlushPolicyTestCase{
            .batchCount = 20,
            .enableChunking = true,
            .targetStripeSizeBytes = 250 << 10, // 250KB
            .writerMemoryHighThresholdBytes = 80 << 10,
            .writerMemoryLowThresholdBytes = 75 << 10,
            .estimatedCompressionFactor = 1.0,
            .minStreamChunkRawSize = 100,
            .maxStreamChunkRawSize = 12
                << 10, // -126KB (as opposed to 128KB in other cases)
            .expectedStripeCount = 7,
            .expectedMaxChunkCount = 9, // +7 chunks
            .expectedMinChunkCount = 2, // +1 chunk
            .chunkedStreamBatchSize = 10,
        },
        // High memory regression threshold and no compression
        // Stripe count identical to RawStripeSizeFlushPolicy
        ChunkFlushPolicyTestCase{
            .batchCount = 20,
            .enableChunking = true,
            .targetStripeSizeBytes = 250 << 10, // 250KB
            .writerMemoryHighThresholdBytes = 500
                << 10, // 500KB (as opposed to 80 KB in other cases)
            .writerMemoryLowThresholdBytes = 75 << 10,
            .estimatedCompressionFactor =
                1.0, // No compression (as opposed to 1.3 in other cases)
            .minStreamChunkRawSize = 100,
            .maxStreamChunkRawSize = 128 << 10,
            .expectedStripeCount = 4,
            .expectedMaxChunkCount = 2,
            .expectedMinChunkCount = 1,
            .chunkedStreamBatchSize = 2,
        },
        // Low memory regression threshold
        // Produces file with more min chunks per stripe
        ChunkFlushPolicyTestCase{
            .batchCount = 20,
            .enableChunking = true,
            .targetStripeSizeBytes = 250 << 10,
            .writerMemoryHighThresholdBytes = 40
                << 10, // 40KB (as opposed to 80KB in other cases)
            .writerMemoryLowThresholdBytes = 35
                << 10, // 35KB (as opposed to 75KB in other cases)
            .estimatedCompressionFactor = 1.3,
            .minStreamChunkRawSize = 100,
            .maxStreamChunkRawSize = 128 << 10,
            .expectedStripeCount = 10,
            .expectedMaxChunkCount = 2,
            .expectedMinChunkCount = 2, // +1 chunk
            .chunkedStreamBatchSize = 2,
        },
        // High target stripe size bytes (with disabled memory pressure
        // optimization) produces fewer stripes.
        ChunkFlushPolicyTestCase{
            .batchCount = 20,
            .enableChunking = true,
            .targetStripeSizeBytes = 900
                << 10, // 900KB (as opposed to 250KB in other cases)
            .writerMemoryHighThresholdBytes = 2
                << 20, // 2MB (as opposed to 80KB in other cases)
            .writerMemoryLowThresholdBytes = 1
                << 20, // 1MB (as opposed to 75KB in other cases)
            .estimatedCompressionFactor = 1.3,
            .minStreamChunkRawSize = 100,
            .maxStreamChunkRawSize = 128 << 10,
            .expectedStripeCount = 1,
            .expectedMaxChunkCount = 5,
            .expectedMinChunkCount = 2,
            .chunkedStreamBatchSize = 2,

        },
        // Low target stripe size bytes (with disabled memory pressure
        // optimization) produces more stripes. Single chunks.
        ChunkFlushPolicyTestCase{
            .batchCount = 20,
            .enableChunking = true,
            .targetStripeSizeBytes = 90
                << 10, // 90KB (as opposed to 250KB in other cases)
            .writerMemoryHighThresholdBytes = 2
                << 20, // 2MB (as opposed to 80KB in other cases)
            .writerMemoryLowThresholdBytes = 1
                << 20, // 1MB (as opposed to 75KB in other cases)
            .estimatedCompressionFactor = 1.3,
            .minStreamChunkRawSize = 100,
            .maxStreamChunkRawSize = 128 << 10,
            .expectedStripeCount = 7,
            .expectedMaxChunkCount = 1,
            .expectedMinChunkCount = 1,
            .chunkedStreamBatchSize = 2,

        },
        // Higher chunked stream batch size (no change in policy)
        ChunkFlushPolicyTestCase{
            .batchCount = 20,
            .enableChunking = true,
            .targetStripeSizeBytes = 250 << 10, // 250KB
            .writerMemoryHighThresholdBytes = 80 << 10,
            .writerMemoryLowThresholdBytes = 75 << 10,
            .estimatedCompressionFactor = 1.0,
            .minStreamChunkRawSize = 100,
            .maxStreamChunkRawSize = 128 << 10,
            .expectedStripeCount = 7,
            .expectedMaxChunkCount = 2,
            .expectedMinChunkCount = 1,
            .chunkedStreamBatchSize = 10}));

TEST_F(VeloxWriterTest, chunkSizeStatsPopulatedWhenChunkingTriggered) {
  const auto type = velox::ROW({{"c0", velox::BIGINT()}});

  nimble::VeloxWriterOptions options{
      .minStreamChunkRawSize = 100,
      .maxStreamChunkRawSize = 128 << 10,
      .flushPolicyFactory = []() -> std::unique_ptr<nimble::FlushPolicy> {
        return std::make_unique<nimble::ChunkFlushPolicy>(
            nimble::ChunkFlushPolicyConfig{
                .writerMemoryHighThresholdBytes = 80 << 10,
                .writerMemoryLowThresholdBytes = 75 << 10,
                .targetStripeSizeBytes = 250 << 10,
                .estimatedCompressionFactor = 1.3,
            });
      },
      .enableChunking = true,
  };

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      type, std::move(writeFile), *rootPool_, std::move(options));

  const auto batches = generateBatches(
      type, /*batchCount=*/20, /*size=*/4000, /*seed=*/42, *leafPool_);
  for (const auto& batch : batches) {
    writer.write(batch);
  }
  writer.close();

  const auto stats = writer.stats();
  EXPECT_GT(stats.chunkSizeBytes.count, 0);
  EXPECT_GT(stats.chunkSizeBytes.sum, 0);
  EXPECT_LE(stats.chunkSizeBytes.min, stats.chunkSizeBytes.max);
}

TEST_F(VeloxWriterTest, chunkSizeStatsEmptyWhenChunkingNotTriggered) {
  const auto type = velox::ROW({{"c0", velox::BIGINT()}});

  nimble::VeloxWriterOptions options{
      .flushPolicyFactory = []() -> std::unique_ptr<nimble::FlushPolicy> {
        return std::make_unique<nimble::StripeRawSizeFlushPolicy>(256ULL << 20);
      },
      .enableChunking = false,
  };

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      type, std::move(writeFile), *rootPool_, std::move(options));

  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  writer.write(vectorMaker.rowVector(
      {"c0"}, {vectorMaker.flatVector<int64_t>({1, 2, 3, 4, 5})}));
  writer.close();

  EXPECT_EQ(writer.stats().chunkSizeBytes.count, 0);
}

// Parameterized test fixture for index tests.
// When enableChunking is false, sets very high minChunkRawSize and
// maxChunkRawSize to prevent chunking (one chunk per stream).
// When enableChunking is true, sets very low values to force aggressive
// chunking.
struct IndexTestParams {
  bool enableChunking;
  nimble::EncodingType encodingType;
  std::optional<uint32_t> prefixRestartInterval;

  std::string toString() const {
    std::string name =
        encodingType == nimble::EncodingType::Prefix ? "Prefix" : "Trivial";
    if (prefixRestartInterval.has_value()) {
      name += "_restart" + std::to_string(prefixRestartInterval.value());
    }
    name += enableChunking ? "_WithKeyChunking" : "_NoKeyChunking";
    return name;
  }
};

class VeloxWriterIndexTest
    : public VeloxWriterTest,
      public ::testing::WithParamInterface<IndexTestParams> {
 protected:
  bool enableChunking() const {
    return GetParam().enableChunking;
  }

  nimble::EncodingType encodingType() const {
    return GetParam().encodingType;
  }

  std::optional<uint32_t> prefixRestartInterval() const {
    return GetParam().prefixRestartInterval;
  }

  // Default type with integer key column and multiple scalar/complex non-key
  // columns for comprehensive index testing.
  static velox::RowTypePtr defaultType() {
    return velox::ROW({
        {"key_col", velox::BIGINT()}, // Integer key column for indexing
        // Scalar types
        {"int_col", velox::INTEGER()},
        {"double_col", velox::DOUBLE()},
        {"string_col1", velox::VARCHAR()},
        {"string_col2", velox::VARCHAR()},
        {"bool_col", velox::BOOLEAN()},
        // Complex types
        {"array_col", velox::ARRAY(velox::INTEGER())},
        {"map_col", velox::MAP(velox::VARCHAR(), velox::BIGINT())},
        {"row_col",
         velox::ROW(
             {{"nested_int", velox::INTEGER()},
              {"nested_string", velox::VARCHAR()}})},
    });
  }

  // Generate pre-sorted batches with sorted key column and fuzzed non-key
  // columns. Uses VectorFuzzer to generate non-key columns with various
  // encodings (flat, constant, dictionary) but not lazy vectors.
  std::vector<velox::RowVectorPtr> generateFuzzedSortedBatches(
      const velox::RowTypePtr& type,
      size_t batchCount,
      size_t batchSize,
      uint32_t seed = 12345) {
    folly::Random::DefaultGenerator rng(seed);
    velox::test::VectorMaker vectorMaker{leafPool_.get()};

    // Configure fuzzer to generate various encodings but not lazy vectors
    velox::VectorFuzzer::Options fuzzerOpts;
    fuzzerOpts.vectorSize = batchSize;
    fuzzerOpts.nullRatio = 0.1;
    fuzzerOpts.containerLength = 5;
    fuzzerOpts.stringLength = 20;
    fuzzerOpts.containerVariableLength = true;
    // Note: VectorFuzzer generates flat, constant, and dictionary encodings
    // by default. Lazy vectors are not generated unless explicitly requested.
    velox::VectorFuzzer fuzzer(fuzzerOpts, leafPool_.get(), seed);

    std::vector<velox::RowVectorPtr> batches;
    batches.reserve(batchCount);

    int64_t currentKey = 0;
    for (size_t i = 0; i < batchCount; ++i) {
      // Generate sorted key column
      std::vector<int64_t> keys(batchSize);
      for (size_t j = 0; j < batchSize; ++j) {
        currentKey += folly::Random::rand32(1, 10, rng);
        keys[j] = currentKey;
      }
      auto keyVector = vectorMaker.flatVector<int64_t>(keys);

      // Generate fuzzed non-key columns
      std::vector<velox::VectorPtr> children;
      children.push_back(keyVector);

      for (size_t colIdx = 1; colIdx < type->size(); ++colIdx) {
        auto childType = type->childAt(colIdx);
        children.push_back(fuzzer.fuzz(childType));
      }

      batches.push_back(
          std::make_shared<velox::RowVector>(
              leafPool_.get(),
              type,
              nullptr, // no nulls at top level
              batchSize,
              std::move(children)));
    }

    return batches;
  }

  nimble::index::ClusterIndexConfig createIndexConfig(
      const std::vector<std::string>& columns,
      bool enforceKeyOrder = true) {
    nimble::index::ClusterIndexConfig config{
        .columns = columns,
        .enforceKeyOrder = enforceKeyOrder,
    };

    // Set encoding layout based on encoding type
    if (encodingType() == nimble::EncodingType::Trivial) {
      // Trivial encoding for string data needs a child encoding for lengths.
      config.encodingLayout = nimble::EncodingLayout{
          nimble::EncodingType::Trivial,
          {},
          nimble::CompressionType::Zstd,
          {nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              {},
              nimble::CompressionType::Uncompressed}}};
    } else {
      // Prefix encoding - optionally set restart interval config
      nimble::EncodingLayout::Config layoutConfig;
      if (prefixRestartInterval().has_value()) {
        layoutConfig = nimble::EncodingLayout::Config{
            {{std::string(nimble::PrefixEncoding::kRestartIntervalConfigKey),
              std::to_string(prefixRestartInterval().value())}}};
      }
      config.encodingLayout = nimble::EncodingLayout{
          encodingType(),
          std::move(layoutConfig),
          nimble::CompressionType::Zstd};
    }

    if (enableChunking()) {
      config.maxRowsPerKeyChunk = 100;
    }
    return config;
  }

  nimble::VeloxWriterOptions createWriterOptions(
      const nimble::index::ClusterIndexConfig& clusterIndexConfig,
      const std::function<std::unique_ptr<nimble::FlushPolicy>()>&
          flushPolicyFactory = nullptr) {
    nimble::VeloxWriterOptions options{
        .clusterIndexConfig = std::move(clusterIndexConfig),
    };
    if (enableChunking()) {
      options.minStreamChunkRawSize = 1 << 10; // 1KB
      options.maxStreamChunkRawSize = 4 << 10; // 4KB
      options.enableChunking = true;
    }
    if (flushPolicyFactory) {
      options.flushPolicyFactory = std::move(flushPolicyFactory);
    }
    options.enableStreamDeduplication = true;
    return options;
  }

  // Verifies that file data matches the expected batches row-by-row.
  void verifyFileData(
      const std::string& file,
      const velox::TypePtr& type,
      const std::vector<velox::RowVectorPtr>& expectedBatches,
      uint32_t readBatchSize = 100) {
    velox::InMemoryReadFile readFile(file);
    nimble::VeloxReader reader(&readFile, *leafPool_);

    auto expected = velox::BaseVector::create(type, 0, leafPool_.get());
    for (const auto& batch : expectedBatches) {
      expected->append(batch.get());
    }

    velox::VectorPtr result;
    uint64_t rowOffset = 0;
    while (reader.next(readBatchSize, result)) {
      for (velox::vector_size_t i = 0; i < result->size(); ++i) {
        ASSERT_TRUE(result->equalValueAt(expected.get(), i, rowOffset + i))
            << "Mismatch at row " << (rowOffset + i);
      }
      rowOffset += result->size();
    }
    EXPECT_EQ(rowOffset, expected->size()) << "All rows should be verified";
  }

  // Verifies that the position index correctly stores chunk row counts.
  // For each stripe and stream:
  // 1. Gets the expected chunk row counts from the position index
  // 2. Verifies using two methods:
  //    a. Sequential iteration using InMemoryChunkedStream
  //    b. Direct chunk access using offset/size from index with
  //    SingleChunkDecoder
  void verifyPositionIndex(const nimble::TabletReader& tablet) {
    for (uint32_t stripeIdx = 0; stripeIdx < tablet.stripeCount();
         ++stripeIdx) {
      const auto stripeId = tablet.stripeIdentifier(stripeIdx);

      if (stripeId.chunkIndex() == nullptr) {
        // Group was skipped (no streams with >1 chunk). Skip verification.
        continue;
      }

      nimble::index::test::ChunkIndexTestHelper chunkHelper(
          stripeId.chunkIndex().get());

      const uint32_t stripeOffsetInGroup =
          stripeIdx - chunkHelper.firstStripe();

      // Load all streams for this stripe
      const uint32_t streamCount = tablet.streamCount(stripeId);
      std::vector<uint32_t> streamIds(streamCount);
      std::iota(streamIds.begin(), streamIds.end(), 0);
      auto streamLoaders = tablet.load(stripeId, streamIds);

      for (uint32_t streamId = 0; streamId < streamLoaders.size(); ++streamId) {
        if (!streamLoaders[streamId]) {
          continue;
        }

        auto streamStats = chunkHelper.streamStats(streamId);
        if (streamStats.chunkCounts.empty()) {
          // Stream not indexed (0 or 1 chunk). Skip verification.
          continue;
        }

        // Get chunk count for this stripe from accumulated values
        const uint32_t prevChunkCount = (stripeOffsetInGroup == 0)
            ? 0
            : streamStats.chunkCounts[stripeOffsetInGroup - 1];
        const uint32_t currChunkCount =
            streamStats.chunkCounts[stripeOffsetInGroup];
        const uint32_t numChunksInStripe = currChunkCount - prevChunkCount;

        // Extract per-chunk row counts from accumulated row counts
        std::vector<uint32_t> expectedChunkRowCounts;
        expectedChunkRowCounts.reserve(numChunksInStripe);
        uint32_t prevRowCount = 0;
        for (uint32_t i = 0; i < numChunksInStripe; ++i) {
          const uint32_t accumulatedRows =
              streamStats.chunkRows[prevChunkCount + i];
          expectedChunkRowCounts.push_back(accumulatedRows - prevRowCount);
          prevRowCount = accumulatedRows;
        }

        // Get the raw stream data
        const std::string_view rawStreamData =
            streamLoaders[streamId]->getStream();

        // Method 1: Verify using sequential InMemoryChunkedStream iteration
        {
          nimble::InMemoryChunkedStream chunkedStream{
              *leafPool_,
              std::make_unique<nimble::index::test::TestStreamLoader>(
                  rawStreamData)};
          uint32_t chunkIndex = 0;
          while (chunkedStream.hasNext()) {
            const auto chunkData = chunkedStream.nextChunk();
            std::vector<velox::BufferPtr> stringBuffers;
            auto encoding = nimble::EncodingFactory().create(
                *leafPool_, chunkData, [&](uint32_t totalLength) {
                  auto& buffer = stringBuffers.emplace_back(
                      velox::AlignedBuffer::allocate<char>(
                          totalLength, leafPool_.get()));
                  return buffer->asMutable<void>();
                });
            EXPECT_EQ(encoding->rowCount(), expectedChunkRowCounts[chunkIndex])
                << "Stripe " << stripeIdx << " stream " << streamId << " chunk "
                << chunkIndex << " row count mismatch (sequential)";
            ++chunkIndex;
          }

          EXPECT_EQ(chunkIndex, numChunksInStripe)
              << "Stripe " << stripeIdx << " stream " << streamId
              << " chunk count mismatch";
        }

        // Method 2: Verify using chunk offset/size from index with
        // SingleChunkDecoder
        for (uint32_t i = 0; i < numChunksInStripe; ++i) {
          const uint32_t chunkOffset =
              streamStats.chunkOffsets[prevChunkCount + i];

          // Calculate chunk size from consecutive offsets or to end of stream
          uint32_t chunkSize;
          if (i + 1 < numChunksInStripe) {
            chunkSize =
                streamStats.chunkOffsets[prevChunkCount + i + 1] - chunkOffset;
          } else {
            chunkSize = rawStreamData.size() - chunkOffset;
          }

          // Extract chunk data using offset and size from index
          std::string_view chunkStreamData(
              rawStreamData.data() + chunkOffset, chunkSize);

          // Decode using SingleChunkDecoder (same pattern as
          // verifyValueIndex)
          nimble::index::test::SingleChunkDecoder chunkDecoder(
              *leafPool_, chunkStreamData);
          auto encodingData = chunkDecoder.decode();
          std::vector<velox::BufferPtr> stringBuffers;
          auto encoding = nimble::EncodingFactory().create(
              *leafPool_, encodingData, [&](uint32_t totalLength) {
                auto& buffer = stringBuffers.emplace_back(
                    velox::AlignedBuffer::allocate<char>(
                        totalLength, leafPool_.get()));
                return buffer->asMutable<void>();
              });

          EXPECT_EQ(encoding->rowCount(), expectedChunkRowCounts[i])
              << "Stripe " << stripeIdx << " stream " << streamId << " chunk "
              << i << " row count mismatch (using chunk offset from index)";
        }
      }
    }
  }

  nimble::TabletWriter::Stats duplicateStreamStatsFromLayout(
      const nimble::TabletReader& tablet) {
    nimble::TabletWriter::Stats stats;
    for (uint32_t stripeIndex = 0; stripeIndex < tablet.stripeCount();
         ++stripeIndex) {
      const auto stripeIdentifier = tablet.stripeIdentifier(stripeIndex);
      const auto offsets = tablet.streamOffsets(stripeIdentifier);
      const auto sizes = tablet.streamSizes(stripeIdentifier);

      std::map<std::pair<uint32_t, uint32_t>, uint64_t> duplicateGroups;
      for (size_t streamIndex = 0; streamIndex < sizes.size(); ++streamIndex) {
        if (sizes[streamIndex] == 0) {
          continue;
        }
        ++duplicateGroups[{offsets[streamIndex], sizes[streamIndex]}];
      }

      for (const auto& [offsetAndSize, count] : duplicateGroups) {
        if (count > 1) {
          const auto duplicateCount = count - 1;
          stats.duplicateStreamCount += duplicateCount;
          stats.duplicateStreamBytes += offsetAndSize.second * duplicateCount;
        }
      }
    }
    return stats;
  }

  // Verifies that the value index correctly maps each key to its row
  // position. For each row in the input batches:
  // 1. Encodes the key using KeyEncoder
  // 2. Looks up the stripe via ClusterIndex
  // 3. Gets chunk location within stripe via ClusterIndex::lookupChunk
  // 4. Loads the key stream chunk and decodes it
  // 5. Uses seek to find the exact row within the chunk
  // 6. For duplicate keys, verifies the found row id matches the earliest row
  //    with the same key value
  void verifyValueIndex(
      const nimble::TabletReader& tablet,
      velox::ReadFile* file,
      const velox::RowTypePtr& type,
      const std::vector<velox::RowVectorPtr>& batches,
      const std::vector<std::string>& indexColumns) {
    const auto* index = tablet.clusterIndex();
    ASSERT_NE(index, nullptr) << "Index must exist";

    // Create sort orders for KeyEncoder (all ascending, nulls last)
    std::vector<velox::core::SortOrder> sortOrders(
        indexColumns.size(),
        velox::core::SortOrder(true, false)); // ascending, nulls last

    // Create KeyEncoder for encoding row keys
    auto keyEncoder = velox::serializer::KeyEncoder::create(
        indexColumns, type, sortOrders, leafPool_.get());

    // Pre-encode all keys and build a map from encoded key to earliest row
    // id. This handles duplicate keys where seek returns the first
    // occurrence.
    std::map<std::string, uint64_t> keyToEarliestRowId;
    std::vector<std::string> allEncodedKeys;
    {
      uint64_t rowId = 0;
      for (const auto& batch : batches) {
        velox::HashStringAllocator allocator(leafPool_.get());
        std::vector<std::string_view> encodedKeys;
        keyEncoder->encode(batch, encodedKeys, [&allocator](size_t size) {
          return allocator.allocate(size)->begin();
        });

        for (velox::vector_size_t i = 0; i < batch->size(); ++i) {
          std::string keyStr(encodedKeys[i]);
          allEncodedKeys.push_back(keyStr);
          // Only store the first occurrence (earliest row id) for each key
          if (keyToEarliestRowId.find(keyStr) == keyToEarliestRowId.end()) {
            keyToEarliestRowId[keyStr] = rowId;
          }
          ++rowId;
        }
      }
    }

    // Cache for loaded and decoded key stream chunks
    // Key: chunk file offset (unique across all stripes), Value: decoded
    // key encoding
    std::unordered_map<uint64_t, std::unique_ptr<nimble::index::KeyEncoding>>
        keyStreamCache;
    // Buffer for loaded key stream data (must outlive the encoding)
    std::unordered_map<uint64_t, std::string> keyStreamBufferCache;
    std::vector<velox::BufferPtr> stringBuffers;

    // Verify each row's index lookup
    uint64_t currentRowId = 0;
    for (const auto& batch : batches) {
      // Verify each row
      for (velox::vector_size_t i = 0; i < batch->size(); ++i) {
        const std::string& encodedKey = allEncodedKeys[currentRowId];
        std::string_view encodedKeyView(encodedKey);

        // Look up the row location for this key
        velox::serializer::EncodedKeyBounds bounds{
            .lowerKey = std::string(encodedKeyView), .upperKey = std::nullopt};
        auto lookupResult = index->lookup(
            nimble::index::IndexLookup::LookupRequest::rangeScan({bounds}));
        const auto rowRanges = lookupResult[0];
        ASSERT_EQ(rowRanges.size(), 1)
            << "Key at row " << currentRowId << " should be found in index";

        const auto& rowRange = rowRanges[0];
        ASSERT_LT(rowRange.startRow, tablet.tabletRowCount())
            << "Row range start out of range";

        // Look up chunk by encoded key to get chunk location within the
        // partition. Single partition in this test.
        constexpr uint32_t kPartitionIndex = 0;
        nimble::index::test::ClusterIndexTestHelper indexHelper(index);
        const auto chunkLocation =
            indexHelper.lookupChunk(kPartitionIndex, encodedKeyView);

        // Get key stream region for this partition
        const auto keyStreamRegion =
            indexHelper.keyStreamRegion(kPartitionIndex);

        // Cache key: chunk file offset (unique across all stripes)
        const uint64_t chunkFileOffset =
            keyStreamRegion.offset + chunkLocation.chunkOffset;

        // Load and decode key chunk if not cached
        if (keyStreamCache.find(chunkFileOffset) == keyStreamCache.end()) {
          const uint32_t chunkLength = chunkLocation.chunkSize;

          // Load the key chunk data from file
          velox::common::Region region{
              chunkFileOffset, chunkLength, "keyChunk"};
          folly::IOBuf iobuf;
          file->preadv({&region, 1}, {&iobuf, 1});

          // Copy data to a persistent buffer
          std::string& buffer = keyStreamBufferCache[chunkFileOffset];
          buffer.resize(iobuf.computeChainDataLength());
          size_t offset = 0;
          for (auto range : iobuf) {
            std::memcpy(buffer.data() + offset, range.data(), range.size());
            offset += range.size();
          }

          // Decode the single chunk to get the raw encoding data
          nimble::index::test::SingleChunkDecoder chunkDecoder(
              *leafPool_, buffer);
          auto encodingData = chunkDecoder.decode();

          // Decode the key encoding from the chunk data

          keyStreamCache[chunkFileOffset] = nimble::index::KeyEncoding::create(
              *leafPool_, encodingData, [&](uint32_t totalLength) {
                auto& buf = stringBuffers.emplace_back(
                    velox::AlignedBuffer::allocate<char>(
                        totalLength, leafPool_.get()));
                return buf->asMutable<void>();
              });
        }

        auto* keyEncoding = keyStreamCache[chunkFileOffset].get();
        ASSERT_NE(keyEncoding, nullptr);

        auto seekResult = keyEncoding->seek(encodedKeyView, /*inclusive=*/true);
        ASSERT_TRUE(seekResult.has_value())
            << "seek should find key at row " << currentRowId;

        // Calculate the actual file row id.
        // lookupChunk returns partition-wide row offsets. For single-partition
        // files, the partition start row is 0.
        const uint64_t fileRowId = chunkLocation.rowOffset + seekResult.value();

        // For duplicate keys, seek returns the first occurrence.
        // Verify that the found row id matches the earliest row with the same
        // key.
        const uint64_t expectedFileRowId = keyToEarliestRowId.at(encodedKey);
        EXPECT_EQ(fileRowId, expectedFileRowId)
            << "Row " << currentRowId << " key lookup mismatch: "
            << "expected earliest row " << expectedFileRowId << ", got "
            << fileRowId << " (partition " << kPartitionIndex
            << ", chunk row offset " << chunkLocation.rowOffset
            << ", seek offset " << seekResult.value() << ")";

        ++currentRowId;
      }
    }
  }
};

TEST_P(VeloxWriterIndexTest, singleGroup) {
  // Test writing a file with index using real pre-sorted data with complex
  // types. Uses a large flush threshold to ensure all stripes stay in one
  // group.
  auto type = defaultType();

  nimble::index::ClusterIndexConfig clusterIndexConfig =
      createIndexConfig({"key_col"});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  constexpr int kNumBatches = 5;
  constexpr int kBatchSize = 100;
  nimble::VeloxWriter writer(
      type,
      std::move(writeFile),
      *rootPool_,
      createWriterOptions(clusterIndexConfig, []() {
        // Flush after every batch to create one stripe per batch
        return std::make_unique<nimble::LambdaFlushPolicy>(
            [](auto) { return true; }, [](auto) { return false; });
      }));

  // Generate pre-sorted batches with fuzzed non-key columns
  auto batches = generateFuzzedSortedBatches(type, kNumBatches, kBatchSize);

  for (const auto& batch : batches) {
    writer.write(batch);
  }
  writer.close();

  // Read and verify
  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto tabletOptions = makeTestTabletOptions(leafPool_.get());
  auto tablet =
      nimble::TabletReader::create(readFile, leafPool_.get(), tabletOptions);

  // Verify each batch triggered exactly one stripe
  EXPECT_EQ(tablet->stripeCount(), kNumBatches)
      << "Each batch should trigger exactly one stripe";

  // Verify all stripes are in the same stripe group (index 0)
  // Default metadataFlushThreshold is large, so all stripes stay in one group
  for (uint32_t i = 0; i < tablet->stripeCount(); ++i) {
    auto stripeId = tablet->stripeIdentifier(i);
    EXPECT_EQ(stripeId.stripeGroup()->index(), 0)
        << "Stripe " << i << " should be in stripe group 0";
  }

  // Verify index section exists
  EXPECT_TRUE(tablet->hasOptionalSection(std::string(nimble::kIndexSection)));

  // Verify index is available
  const auto* index = tablet->clusterIndex();
  ASSERT_NE(index, nullptr);

  // Verify index columns
  EXPECT_EQ(index->indexColumns().size(), 1);
  EXPECT_EQ(index->indexColumns()[0], "key_col");

  // Verify sub-partition keys are monotonically increasing
  nimble::index::test::ClusterIndexTestHelper indexHelper(index);
  const auto& partitionKeys = indexHelper.partitionKeys();
  for (size_t i = 1; i < partitionKeys.size(); ++i) {
    EXPECT_LE(partitionKeys[i - 1], partitionKeys[i])
        << "Sub-partition keys should be in non-descending order";
  }

  // Verify lookups work correctly
  // Keys should be found in the correct partitions
  ASSERT_FALSE(partitionKeys.empty());
  auto makeLookup = [](std::string_view key) {
    velox::serializer::EncodedKeyBounds bounds{
        .lowerKey = std::string(key), .upperKey = std::nullopt};
    return nimble::index::IndexLookup::LookupRequest::rangeScan({bounds});
  };
  {
    auto r = index->lookup(makeLookup(partitionKeys.front()));
    EXPECT_FALSE(r[0].empty());
  }
  {
    auto r = index->lookup(makeLookup(partitionKeys.back()));
    EXPECT_FALSE(r[0].empty());
  }

  // Empty key with range scan (no upper bound) matches all rows since
  // "" is less than any encoded key.
  {
    auto r = index->lookup(makeLookup(""));
    EXPECT_FALSE(r[0].empty());
  }

  // Read back all data and verify row-by-row match with written batches
  verifyFileData(file, type, batches, kBatchSize);

  // Verify position index chunk row counts
  verifyPositionIndex(*tablet);

  // Verify value index maps each key to correct row position
  verifyValueIndex(*tablet, readFile.get(), type, batches, {"key_col"});
}

TEST_P(VeloxWriterIndexTest, multipleGroups) {
  // Test writing a file with index and multiple stripe groups.
  // Uses small batches with flush-per-batch policy to create multiple
  // stripes.
  auto type = defaultType();

  nimble::index::ClusterIndexConfig clusterIndexConfig =
      createIndexConfig({"key_col"});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  constexpr int kNumBatches = 5;
  constexpr int kBatchSize = 100;
  nimble::VeloxWriter writer(
      type,
      std::move(writeFile),
      *rootPool_,
      createWriterOptions(clusterIndexConfig, []() {
        // Flush stripe after each batch
        return std::make_unique<nimble::LambdaFlushPolicy>(
            [](auto) { return true; }, [](auto) { return false; });
      }));

  // Generate pre-sorted batches with fuzzed non-key columns
  auto batches = generateFuzzedSortedBatches(type, kNumBatches, kBatchSize);

  for (const auto& batch : batches) {
    writer.write(batch);
  }
  writer.close();

  // Read and verify
  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto tabletOptions = makeTestTabletOptions(leafPool_.get());
  auto tablet =
      nimble::TabletReader::create(readFile, leafPool_.get(), tabletOptions);

  // Verify each batch triggered exactly one stripe
  EXPECT_EQ(tablet->stripeCount(), kNumBatches)
      << "Each batch should trigger exactly one stripe";

  // Note: Without controlling metadataFlushThreshold from VeloxWriterOptions,
  // all stripes end up in the same group (default threshold is 8MB).
  // This test verifies the default behavior where all stripes share group 0.
  for (uint32_t i = 0; i < tablet->stripeCount(); ++i) {
    auto stripeId = tablet->stripeIdentifier(i);
    EXPECT_EQ(stripeId.stripeGroup()->index(), 0)
        << "Stripe " << i << " should be in stripe group 0";
  }

  // Verify index section exists
  EXPECT_TRUE(tablet->hasOptionalSection(std::string(nimble::kIndexSection)));

  // Verify index is available
  const auto* index = tablet->clusterIndex();
  ASSERT_NE(index, nullptr);

  // Verify index columns
  EXPECT_EQ(index->indexColumns().size(), 1);
  EXPECT_EQ(index->indexColumns()[0], "key_col");

  // Verify sub-partition keys are monotonically increasing
  nimble::index::test::ClusterIndexTestHelper indexHelper(index);
  const auto& partitionKeys = indexHelper.partitionKeys();
  for (size_t i = 1; i < partitionKeys.size(); ++i) {
    EXPECT_LE(partitionKeys[i - 1], partitionKeys[i])
        << "Sub-partition keys should be in non-descending order";
  }

  // Verify we can look up by key
  // Get the first key and verify lookup returns a valid location
  ASSERT_FALSE(partitionKeys.empty());
  auto makeLookup2 = [](std::string_view key) {
    velox::serializer::EncodedKeyBounds bounds{
        .lowerKey = std::string(key), .upperKey = std::nullopt};
    return nimble::index::IndexLookup::LookupRequest::rangeScan({bounds});
  };
  auto firstResult = index->lookup(makeLookup2(index->minKey()));
  auto firstRanges = firstResult[0];
  ASSERT_EQ(firstRanges.size(), 1);
  EXPECT_EQ(firstRanges[0].startRow, 0);

  // Get the last key and verify lookup returns a valid location
  auto lastResult = index->lookup(makeLookup2(index->maxKey()));
  auto lastRanges = lastResult[0];
  ASSERT_EQ(lastRanges.size(), 1);
  EXPECT_GT(lastRanges[0].endRow, 0);

  // Verify stripeIdentifier returns index group
  for (uint32_t i = 0; i < tablet->stripeCount(); ++i) {
    auto stripeId = tablet->stripeIdentifier(i);
    EXPECT_NE(stripeId.stripeGroup(), nullptr);
  }

  // Read back all data and verify row-by-row match with written batches
  verifyFileData(file, type, batches);

  // Verify position index chunk row counts
  verifyPositionIndex(*tablet);

  // Verify value index maps each key to correct row position
  verifyValueIndex(*tablet, readFile.get(), type, batches, {"key_col"});
}

TEST_P(VeloxWriterIndexTest, multipleIndexColumns) {
  // Test index with multiple index columns (composite key).
  auto type = velox::ROW({
      {"key1", velox::BIGINT()},
      {"key2", velox::INTEGER()},
      {"data_col", velox::VARCHAR()},
  });

  nimble::index::ClusterIndexConfig clusterIndexConfig =
      createIndexConfig({"key1", "key2"});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(
      type,
      std::move(writeFile),
      *rootPool_,
      createWriterOptions(clusterIndexConfig));

  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  // Create pre-sorted data with composite key (key1, key2)
  // key1 increases slowly, key2 varies within same key1 value
  int64_t key1Val = 0;
  int32_t key2Val = 0;
  for (int batch = 0; batch < 10; ++batch) {
    std::vector<int64_t> key1Values;
    std::vector<int32_t> key2Values;
    std::vector<std::string> dataValues;

    for (int i = 0; i < 100; ++i) {
      key1Values.push_back(key1Val);
      key2Values.push_back(key2Val);
      dataValues.push_back(
          "data_" + std::to_string(key1Val) + "_" + std::to_string(key2Val));

      // Increment keys in sorted order
      ++key2Val;
      if (key2Val >= 5) {
        key2Val = 0;
        ++key1Val;
      }
    }

    auto vector = vectorMaker.rowVector(
        {"key1", "key2", "data_col"},
        {
            vectorMaker.flatVector<int64_t>(key1Values),
            vectorMaker.flatVector<int32_t>(key2Values),
            vectorMaker.flatVector<std::string>(dataValues),
        });
    writer.write(vector);
  }
  writer.close();

  // Read and verify
  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto tabletOptions = makeTestTabletOptions(leafPool_.get());
  auto tablet =
      nimble::TabletReader::create(readFile, leafPool_.get(), tabletOptions);
  const auto* index = tablet->clusterIndex();
  ASSERT_NE(index, nullptr);

  // Verify both columns are indexed
  EXPECT_EQ(index->indexColumns().size(), 2);
  EXPECT_EQ(index->indexColumns()[0], "key1");
  EXPECT_EQ(index->indexColumns()[1], "key2");

  // Verify sub-partition keys exist
  nimble::index::test::ClusterIndexTestHelper multiKeyHelper(index);
  const auto& multiKeySubPartitionKeys = multiKeyHelper.partitionKeys();
  ASSERT_FALSE(multiKeySubPartitionKeys.empty());
  EXPECT_FALSE(multiKeySubPartitionKeys.front().empty());
  EXPECT_FALSE(multiKeySubPartitionKeys.back().empty());

  // Verify value index maps each key to correct row position
  // Need to collect batches to verify
  velox::test::VectorMaker vectorMaker2{leafPool_.get()};
  std::vector<velox::RowVectorPtr> batches;
  key1Val = 0;
  key2Val = 0;
  for (int batch = 0; batch < 10; ++batch) {
    std::vector<int64_t> key1Values;
    std::vector<int32_t> key2Values;
    std::vector<std::string> dataValues;

    for (int i = 0; i < 100; ++i) {
      key1Values.push_back(key1Val);
      key2Values.push_back(key2Val);
      dataValues.push_back(
          "data_" + std::to_string(key1Val) + "_" + std::to_string(key2Val));

      ++key2Val;
      if (key2Val >= 5) {
        key2Val = 0;
        ++key1Val;
      }
    }

    batches.push_back(vectorMaker2.rowVector(
        {"key1", "key2", "data_col"},
        {
            vectorMaker2.flatVector<int64_t>(key1Values),
            vectorMaker2.flatVector<int32_t>(key2Values),
            vectorMaker2.flatVector<std::string>(dataValues),
        }));
  }
  // Verify position index chunk row counts
  verifyPositionIndex(*tablet);

  verifyValueIndex(*tablet, readFile.get(), type, batches, {"key1", "key2"});
}

TEST_F(VeloxWriterTest, indexEnforceKeyOrder) {
  // Test both enforceKeyOrder=true (detects out-of-order and duplicate keys)
  // and enforceKeyOrder=false (allows out-of-order and duplicate keys)
  auto type = velox::ROW(
      {{"key_col", velox::BIGINT()}, {"data_col", velox::INTEGER()}});

  enum class TestCase { kOutOfOrder, kDuplicateKeys };

  for (bool enforceKeyOrder : {true, false}) {
    for (auto testCase : {TestCase::kOutOfOrder, TestCase::kDuplicateKeys}) {
      SCOPED_TRACE(
          fmt::format(
              "enforceKeyOrder={}, testCase={}",
              enforceKeyOrder,
              testCase == TestCase::kOutOfOrder ? "OutOfOrder"
                                                : "DuplicateKeys"));

      nimble::index::ClusterIndexConfig clusterIndexConfig{
          .columns = {"key_col"},
          .enforceKeyOrder = enforceKeyOrder,
          .noDuplicateKey = enforceKeyOrder,
      };

      std::string file;
      auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

      nimble::VeloxWriter writer(
          type,
          std::move(writeFile),
          *rootPool_,
          {.clusterIndexConfig = clusterIndexConfig});

      velox::test::VectorMaker vectorMaker{leafPool_.get()};

      // Write first batch with ascending keys
      auto batch1 = vectorMaker.rowVector(
          {"key_col", "data_col"},
          {
              vectorMaker.flatVector<int64_t>({100, 200, 300}),
              vectorMaker.flatVector<int32_t>({1, 2, 3}),
          });
      writer.write(batch1);

      // Write second batch with invalid key ordering based on test case:
      // - OutOfOrder: keys LESS than previous batch
      // - DuplicateKeys: keys starting with same value as previous batch's
      // last key
      auto batch2 = testCase == TestCase::kOutOfOrder
          ? vectorMaker.rowVector(
                {"key_col", "data_col"},
                {
                    vectorMaker.flatVector<int64_t>({50, 60, 70}),
                    vectorMaker.flatVector<int32_t>({4, 5, 6}),
                })
          : vectorMaker.rowVector(
                {"key_col", "data_col"},
                {
                    vectorMaker.flatVector<int64_t>({300, 400, 500}),
                    vectorMaker.flatVector<int32_t>({4, 5, 6}),
                });

      if (enforceKeyOrder) {
        // Should fail when enforceKeyOrder is true
        NIMBLE_ASSERT_USER_THROW(
            writer.write(batch2),
            "Encoded keys must be in strictly ascending order (duplicates are not allowed)");
      } else {
        // Should succeed when enforceKeyOrder is false
        EXPECT_NO_THROW(writer.write(batch2));
        writer.close();

        // Verify file was written successfully
        velox::InMemoryReadFile readFile(file);
        nimble::VeloxReader reader(&readFile, *leafPool_);
        EXPECT_TRUE(reader.tabletReader().hasOptionalSection(
            std::string(nimble::kIndexSection)));
      }
    }
  }
}

TEST_P(VeloxWriterIndexTest, duplicateKeys) {
  // Test index with duplicate key values (non-unique keys).
  // This is a valid scenario where multiple rows have the same key value.
  auto type = defaultType();

  nimble::index::ClusterIndexConfig clusterIndexConfig =
      createIndexConfig({"key_col"});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  // constexpr int kNumBatches = 10;
  constexpr int kNumBatches = 1;
  // constexpr int kBatchSize = 100;
  constexpr int kBatchSize = 32;
  constexpr uint32_t kSeed = 12345;
  // Use flush-per-batch to create multiple stripes
  nimble::VeloxWriter writer(
      type,
      std::move(writeFile),
      *rootPool_,
      createWriterOptions(clusterIndexConfig, []() {
        return std::make_unique<nimble::LambdaFlushPolicy>(
            [](auto) { return true; }, [](auto) { return false; });
      }));

  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  // Configure fuzzer to generate various encodings for non-key columns
  velox::VectorFuzzer::Options fuzzerOpts;
  fuzzerOpts.vectorSize = kBatchSize;
  fuzzerOpts.nullRatio = 0.1;
  fuzzerOpts.containerLength = 5;
  fuzzerOpts.stringLength = 20;
  fuzzerOpts.containerVariableLength = true;
  velox::VectorFuzzer fuzzer(fuzzerOpts, leafPool_.get(), kSeed);

  // Generate pre-sorted batches with duplicate keys.
  // Each key value repeats 5 times before incrementing.
  std::vector<velox::RowVectorPtr> batches;
  int64_t keyVal = 0;
  for (int batch = 0; batch < kNumBatches; ++batch) {
    std::vector<int64_t> keyValues;

    for (int i = 0; i < kBatchSize; ++i) {
      keyValues.push_back(keyVal);
      // Increment key every 5 rows to create duplicates
      if (((batch * kBatchSize + i + 1) % 5) == 0) {
        ++keyVal;
      }
    }

    // Build children: key column + fuzzed non-key columns
    std::vector<velox::VectorPtr> children;
    children.push_back(vectorMaker.flatVector<int64_t>(keyValues));
    for (size_t colIdx = 1; colIdx < type->size(); ++colIdx) {
      children.push_back(fuzzer.fuzz(type->childAt(colIdx)));
    }

    auto batchVec = std::make_shared<velox::RowVector>(
        leafPool_.get(),
        type,
        nullptr, // no nulls at top level
        kBatchSize,
        std::move(children));
    batches.push_back(batchVec);
    writer.write(batchVec);
  }
  writer.close();

  // Read and verify
  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);

  // Verify FileLayout for non-empty file with index using
  // FileLayout::create()
  {
    auto layout = nimble::FileLayout::create(readFile, leafPool_.get());
    EXPECT_EQ(layout.fileSize, file.size());
    EXPECT_EQ(layout.stripesInfo.size(), kNumBatches);
    EXPECT_EQ(layout.stripeGroups.size(), 1);
    EXPECT_EQ(layout.stripeGroups.size(), 1);
    // With index enabled, should have index groups
    EXPECT_EQ(layout.indexPartitions.size(), 1);
    // Stripes metadata should be valid
    EXPECT_GT(layout.stripes.size(), 0);
    EXPECT_LT(layout.stripes.offset(), layout.footer.offset());
    // Index group should be before stripes section
    EXPECT_LT(layout.indexPartitions[0].offset(), layout.stripes.offset());
    // Per-stripe info
    EXPECT_EQ(layout.stripesInfo.size(), kNumBatches);
    for (size_t i = 0; i < layout.stripesInfo.size(); ++i) {
      EXPECT_EQ(layout.stripesInfo[i].stripeGroupIndex, 0);
      EXPECT_GT(layout.stripesInfo[i].size, 0);
    }
  }
  auto tabletOptions = makeTestTabletOptions(leafPool_.get());
  auto tablet =
      nimble::TabletReader::create(readFile, leafPool_.get(), tabletOptions);

  // Verify index exists
  const auto* index = tablet->clusterIndex();
  ASSERT_NE(index, nullptr);
  EXPECT_EQ(index->indexColumns().size(), 1);
  EXPECT_EQ(index->indexColumns()[0], "key_col");

  // Verify sub-partition keys are monotonically increasing (even with
  // duplicates)
  nimble::index::test::ClusterIndexTestHelper dupKeyHelper(index);
  const auto& dupSubPartitionKeys = dupKeyHelper.partitionKeys();
  for (size_t i = 1; i < dupSubPartitionKeys.size(); ++i) {
    EXPECT_LE(dupSubPartitionKeys[i - 1], dupSubPartitionKeys[i])
        << "Sub-partition keys should be in non-descending order";
  }

  // Verify we can look up by key
  ASSERT_FALSE(dupSubPartitionKeys.empty());
  auto makeLookup3 = [](std::string_view key) {
    velox::serializer::EncodedKeyBounds bounds{
        .lowerKey = std::string(key), .upperKey = std::nullopt};
    return nimble::index::IndexLookup::LookupRequest::rangeScan({bounds});
  };
  auto firstResult = index->lookup(makeLookup3(index->minKey()));
  auto firstRanges = firstResult[0];
  ASSERT_EQ(firstRanges.size(), 1);
  EXPECT_EQ(firstRanges[0].startRow, 0);

  auto lastResult = index->lookup(makeLookup3(index->maxKey()));
  auto lastRanges = lastResult[0];
  ASSERT_EQ(lastRanges.size(), 1);
  EXPECT_GT(lastRanges[0].endRow, 0);

  // Read back all data and verify row-by-row match with written batches
  verifyFileData(file, type, batches);

  // Verify position index chunk row counts
  verifyPositionIndex(*tablet);

  // Verify value index maps each key to correct row position
  // With duplicate keys, lookup should find the first occurrence
  verifyValueIndex(*tablet, readFile.get(), type, batches, {"key_col"});
}

TEST_P(VeloxWriterIndexTest, chunking) {
  // Test index with chunking enabled
  auto type = defaultType();

  nimble::index::ClusterIndexConfig clusterIndexConfig =
      createIndexConfig({"key_col"});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  constexpr int kNumBatches = 50;
  constexpr int kBatchSize = 500;
  auto options = createWriterOptions(clusterIndexConfig, []() {
    return std::make_unique<nimble::StripeRawSizeFlushPolicy>(128 << 10);
  });
  // Always enable chunking for this test
  options.minStreamChunkRawSize = 1 << 10;
  options.maxStreamChunkRawSize = 4 << 10;
  options.enableChunking = true;

  nimble::VeloxWriter writer(type, std::move(writeFile), *rootPool_, options);

  // Generate pre-sorted batches with fuzzed non-key columns
  auto batches = generateFuzzedSortedBatches(type, kNumBatches, kBatchSize);

  for (const auto& batch : batches) {
    writer.write(batch);
  }
  writer.close();

  // Read and verify
  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto tabletOptions = makeTestTabletOptions(leafPool_.get());
  auto tablet =
      nimble::TabletReader::create(readFile, leafPool_.get(), tabletOptions);

  // Verify index exists and works
  const auto* index = tablet->clusterIndex();
  ASSERT_NE(index, nullptr);
  EXPECT_EQ(index->indexColumns().size(), 1);
  EXPECT_EQ(index->indexColumns()[0], "key_col");

  // Verify sub-partition keys are in order
  nimble::index::test::ClusterIndexTestHelper indexHelper(index);
  const auto& partitionKeys = indexHelper.partitionKeys();
  for (size_t i = 1; i < partitionKeys.size(); ++i) {
    EXPECT_LE(partitionKeys[i - 1], partitionKeys[i]);
  }

  // Read back all data and verify row-by-row match with written batches
  verifyFileData(file, type, batches);

  // Verify position index chunk row counts
  verifyPositionIndex(*tablet);

  // Verify value index maps each key to correct row position
  verifyValueIndex(*tablet, readFile.get(), type, batches, {"key_col"});
}

TEST_P(VeloxWriterIndexTest, streamDeduplication) {
  // Test that streams with identical content are deduplicated by the tablet
  // writer. We create batches where string_col1 and string_col2 reference the
  // same underlying vector, which should result in identical stream content
  // that gets deduplicated.
  auto type = velox::ROW({
      {"key_col", velox::BIGINT()},
      {"string_col1", velox::VARCHAR()},
      {"string_col2", velox::VARCHAR()},
  });

  nimble::index::ClusterIndexConfig clusterIndexConfig =
      createIndexConfig({"key_col"});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  constexpr int kNumBatches = 5;
  constexpr int kBatchSize = 100;

  nimble::VeloxWriter writer(
      type,
      std::move(writeFile),
      *rootPool_,
      createWriterOptions(clusterIndexConfig, []() {
        // Flush after every batch to create multiple stripes
        return std::make_unique<nimble::LambdaFlushPolicy>(
            [](auto) { return true; }, [](auto) { return false; });
      }));

  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  // Generate pre-sorted batches where string_col1 and string_col2 share the
  // same underlying vector (to trigger stream deduplication)
  std::vector<velox::RowVectorPtr> batches;
  int64_t keyVal = 0;
  for (int batch = 0; batch < kNumBatches; ++batch) {
    std::vector<int64_t> keyValues;
    keyValues.reserve(kBatchSize);
    for (int i = 0; i < kBatchSize; ++i) {
      keyValues.push_back(keyVal++);
    }

    // Create the shared string vector that will be used for both string
    // columns.
    auto sharedStringVector =
        vectorMaker.flatVector<std::string>(kBatchSize, [batch](auto row) {
          return fmt::format("batch-{}-row-{}", batch, row);
        });

    std::vector<velox::VectorPtr> children;
    children.push_back(vectorMaker.flatVector<int64_t>(keyValues));
    children.push_back(sharedStringVector);
    children.push_back(sharedStringVector);

    auto batchVec = std::make_shared<velox::RowVector>(
        leafPool_.get(),
        type,
        nullptr, // no nulls at top level
        kBatchSize,
        std::move(children));
    batches.push_back(batchVec);
    writer.write(batchVec);
  }
  writer.close();

  // Read and verify
  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto tabletOptions = makeTestTabletOptions(leafPool_.get());
  auto tablet =
      nimble::TabletReader::create(readFile, leafPool_.get(), tabletOptions);
  const auto expectedStats = duplicateStreamStatsFromLayout(*tablet);
  EXPECT_EQ(
      writer.stats().duplicateStreamCount, expectedStats.duplicateStreamCount);
  EXPECT_EQ(
      writer.stats().duplicateStreamBytes, expectedStats.duplicateStreamBytes);
  EXPECT_EQ(expectedStats.duplicateStreamCount, kNumBatches);
  EXPECT_GT(expectedStats.duplicateStreamBytes, 0);

  // Verify index exists
  const auto* index = tablet->clusterIndex();
  ASSERT_NE(index, nullptr);
  EXPECT_EQ(index->indexColumns().size(), 1);
  EXPECT_EQ(index->indexColumns()[0], "key_col");

  // Verify sub-partition keys are monotonically increasing
  nimble::index::test::ClusterIndexTestHelper indexHelper(index);
  const auto& partitionKeys = indexHelper.partitionKeys();
  for (size_t i = 1; i < partitionKeys.size(); ++i) {
    EXPECT_LE(partitionKeys[i - 1], partitionKeys[i])
        << "Sub-partition keys should be in non-descending order";
  }

  // Verify we can look up by key
  ASSERT_FALSE(partitionKeys.empty());
  auto makeLookup4 = [](std::string_view key) {
    velox::serializer::EncodedKeyBounds bounds{
        .lowerKey = std::string(key), .upperKey = std::nullopt};
    return nimble::index::IndexLookup::LookupRequest::rangeScan({bounds});
  };
  auto firstResult = index->lookup(makeLookup4(index->minKey()));
  auto firstRanges = firstResult[0];
  ASSERT_EQ(firstRanges.size(), 1);
  EXPECT_EQ(firstRanges[0].startRow, 0);

  auto lastResult = index->lookup(makeLookup4(index->maxKey()));
  auto lastRanges = lastResult[0];
  ASSERT_EQ(lastRanges.size(), 1);
  EXPECT_GT(lastRanges[0].endRow, 0);

  // Read back all data and verify row-by-row match with written batches
  // Note: When reading back, string_col1 and string_col2 will have identical
  // content since they were written from the same source vector
  verifyFileData(file, type, batches);

  // Verify position index chunk row counts
  verifyPositionIndex(*tablet);

  // Verify value index maps each key to correct row position
  verifyValueIndex(*tablet, readFile.get(), type, batches, {"key_col"});
}

TEST_P(VeloxWriterIndexTest, streamStatsNoDuplicates) {
  auto type = velox::ROW({
      {"key_col", velox::BIGINT()},
      {"string_col1", velox::VARCHAR()},
      {"string_col2", velox::VARCHAR()},
  });

  nimble::index::ClusterIndexConfig clusterIndexConfig =
      createIndexConfig({"key_col"});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  constexpr int kNumBatches = 5;
  constexpr int kBatchSize = 100;

  nimble::VeloxWriter writer(
      type,
      std::move(writeFile),
      *rootPool_,
      createWriterOptions(clusterIndexConfig, []() {
        return std::make_unique<nimble::LambdaFlushPolicy>(
            [](auto) { return true; }, [](auto) { return false; });
      }));

  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  std::vector<velox::RowVectorPtr> batches;
  int64_t keyVal = 0;
  for (int batch = 0; batch < kNumBatches; ++batch) {
    std::vector<int64_t> keyValues;
    keyValues.reserve(kBatchSize);
    for (int i = 0; i < kBatchSize; ++i) {
      keyValues.push_back(keyVal++);
    }

    std::vector<velox::VectorPtr> children;
    children.push_back(vectorMaker.flatVector<int64_t>(keyValues));
    children.push_back(vectorMaker.flatVector<std::string>(
        kBatchSize,
        [batch](auto row) { return fmt::format("left-{}-{}", batch, row); }));
    children.push_back(vectorMaker.flatVector<std::string>(
        kBatchSize,
        [batch](auto row) { return fmt::format("right-{}-{}", batch, row); }));

    auto batchVec = std::make_shared<velox::RowVector>(
        leafPool_.get(),
        type,
        nullptr, // no nulls at top level
        kBatchSize,
        std::move(children));
    batches.push_back(batchVec);
    writer.write(batchVec);
  }
  writer.close();

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto tablet = nimble::TabletReader::create(
      readFile, leafPool_.get(), makeTestTabletOptions(leafPool_.get()));
  const auto expectedStats = duplicateStreamStatsFromLayout(*tablet);
  EXPECT_EQ(writer.stats().duplicateStreamCount, 0);
  EXPECT_EQ(writer.stats().duplicateStreamBytes, 0);
  EXPECT_EQ(expectedStats.duplicateStreamCount, 0);
  EXPECT_EQ(expectedStats.duplicateStreamBytes, 0);

  verifyFileData(file, type, batches);
  verifyPositionIndex(*tablet);
  verifyValueIndex(*tablet, readFile.get(), type, batches, {"key_col"});
}

// Test that custom prefixRestartInterval in ClusterIndexConfig flows through
// the entire E2E pipeline and is correctly applied to the PrefixEncoding.
TEST_F(VeloxWriterTest, customPrefixRestartInterval) {
  auto type = velox::ROW({
      {"key_col", velox::VARCHAR()},
      {"data_col", velox::INTEGER()},
  });

  // Test with various restart interval values including custom and default
  const std::vector<std::optional<uint32_t>> testRestartIntervals = {
      std::nullopt, // Test default (16)
      1,
      4,
      32,
  };

  for (const auto& customRestartInterval : testRestartIntervals) {
    SCOPED_TRACE(
        fmt::format(
            "prefixRestartInterval={}",
            customRestartInterval.has_value()
                ? std::to_string(customRestartInterval.value())
                : "default"));

    nimble::EncodingLayout::Config encodingConfig;
    if (customRestartInterval.has_value()) {
      encodingConfig = nimble::EncodingLayout::Config{
          {{std::string(nimble::PrefixEncoding::kRestartIntervalConfigKey),
            std::to_string(customRestartInterval.value())}}};
    }

    nimble::index::ClusterIndexConfig clusterIndexConfig{
        .columns = {"key_col"},
        .enforceKeyOrder = true,
        .encodingLayout =
            nimble::EncodingLayout{
                nimble::EncodingType::Prefix,
                std::move(encodingConfig),
                nimble::CompressionType::Uncompressed},
    };

    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

    nimble::VeloxWriter writer(
        type,
        std::move(writeFile),
        *rootPool_,
        {.clusterIndexConfig = clusterIndexConfig});

    velox::test::VectorMaker vectorMaker{leafPool_.get()};

    // Create sorted key values
    auto batch = vectorMaker.rowVector(
        {"key_col", "data_col"},
        {vectorMaker.flatVector<std::string>(
             {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh"}),
         vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8})});

    writer.write(batch);
    writer.close();

    // Read back and verify the restart interval in the key encoding
    auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
    auto tablet = nimble::TabletReader::create(
        readFile, leafPool_.get(), makeTestTabletOptions(leafPool_.get()));

    const auto* index = tablet->clusterIndex();
    ASSERT_NE(index, nullptr) << "Index must exist";

    // Get key stream region from partition 0
    nimble::index::test::ClusterIndexTestHelper indexHelper(index);
    const auto keyStreamRegion = indexHelper.keyStreamRegion(0);

    // Load the key stream data
    velox::common::Region region{
        keyStreamRegion.offset, keyStreamRegion.length, "keyStream"};
    folly::IOBuf iobuf;
    readFile->preadv({&region, 1}, {&iobuf, 1});

    std::string buffer;
    buffer.resize(iobuf.computeChainDataLength());
    size_t offset = 0;
    for (auto range : iobuf) {
      std::memcpy(buffer.data() + offset, range.data(), range.size());
      offset += range.size();
    }

    // Decode the chunk to get raw encoding data
    nimble::index::test::SingleChunkDecoder chunkDecoder(*leafPool_, buffer);
    auto encodingData = chunkDecoder.decode();

    // Decode the key encoding
    std::vector<velox::BufferPtr> stringBuffers;
    auto keyEncoding = nimble::EncodingFactory().create(
        *leafPool_, encodingData, [&](uint32_t totalLength) {
          auto& buf = stringBuffers.emplace_back(
              velox::AlignedBuffer::allocate<char>(
                  totalLength, leafPool_.get()));
          return buf->asMutable<void>();
        });

    // Verify the restart interval through debugString()
    const std::string debug = keyEncoding->debugString(0);
    const uint32_t expectedInterval = customRestartInterval.value_or(
        nimble::PrefixEncoding::kDefaultRestartInterval);
    EXPECT_TRUE(
        debug.find("restart_interval=" + std::to_string(expectedInterval)) !=
        std::string::npos)
        << "Expected restart_interval=" << expectedInterval
        << " in debugString: " << debug;
  }
}

INSTANTIATE_TEST_SUITE_P(
    VeloxWriterIndexTestSuite,
    VeloxWriterIndexTest,
    ::testing::Values(
        IndexTestParams{false, nimble::EncodingType::Prefix, std::nullopt},
        IndexTestParams{false, nimble::EncodingType::Prefix, 1},
        IndexTestParams{false, nimble::EncodingType::Prefix, 1024},
        IndexTestParams{true, nimble::EncodingType::Prefix, std::nullopt},
        IndexTestParams{true, nimble::EncodingType::Prefix, 1},
        IndexTestParams{true, nimble::EncodingType::Prefix, 1024},
        IndexTestParams{false, nimble::EncodingType::Trivial, std::nullopt},
        IndexTestParams{true, nimble::EncodingType::Trivial, std::nullopt}),
    [](const ::testing::TestParamInfo<IndexTestParams>& info) {
      return info.param.toString();
    });

TEST_F(VeloxWriterTest, disableStatsCollection) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  // Build a schema covering major field writer types:
  // scalar, string, timestamp, array, map, flatmap, row.
  auto type = velox::ROW(
      {{"int_col", velox::INTEGER()},
       {"string_col", velox::VARCHAR()},
       {"ts_col", velox::TIMESTAMP()},
       {"array_col", velox::ARRAY(velox::BIGINT())},
       {"map_col", velox::MAP(velox::INTEGER(), velox::VARCHAR())},
       {"flatmap_col", velox::MAP(velox::VARCHAR(), velox::BIGINT())}});

  auto vector = vectorMaker.rowVector(
      {"int_col",
       "string_col",
       "ts_col",
       "array_col",
       "map_col",
       "flatmap_col"},
      {vectorMaker.flatVector<int32_t>({1, 2, 3}),
       vectorMaker.flatVector<velox::StringView>({"a", "bb", "ccc"}),
       vectorMaker.flatVector<velox::Timestamp>(
           {velox::Timestamp(1, 0),
            velox::Timestamp(2, 0),
            velox::Timestamp(3, 0)}),
       vectorMaker.arrayVector<int64_t>({{1, 2}, {3}, {4, 5, 6}}),
       vectorMaker.mapVector<int32_t, velox::StringView>(
           {{{1, "x"}}, {{2, "y"}, {3, "z"}}, {{4, "w"}}}),
       vectorMaker.mapVector<velox::StringView, int64_t>(
           {{{"k1", 10}}, {{"k2", 20}}, {{"k1", 30}, {"k3", 40}}})});

  // Write with stats collection disabled.
  std::string file;
  {
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriterOptions options;
    options.enableStatsCollection = false;
    options.flatMapColumns = {{"flatmap_col", {}}};
    nimble::VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, std::move(options));
    writer.write(vector);

    // Poll stats mid-write, before flush. columnStats must be empty.
    {
      auto midWriteStats = writer.stats();
      EXPECT_TRUE(midWriteStats.columnStats.empty());
    }

    writer.flush();

    // Poll stats after flush, before close.
    {
      auto postFlushStats = writer.stats();
      EXPECT_TRUE(postFlushStats.columnStats.empty());
      EXPECT_EQ(postFlushStats.stripeCount, 1);
    }

    // Write a second batch to exercise multi-stripe path.
    writer.write(vector);
    writer.close();

    // Poll stats after close.
    auto stats = writer.stats();
    EXPECT_TRUE(stats.columnStats.empty());
    EXPECT_EQ(stats.stripeCount, 2);
  }

  // Verify the file is readable and data round-trips.
  {
    velox::InMemoryReadFile readFile(file);
    nimble::VeloxReader reader(&readFile, *leafPool_);
    velox::VectorPtr result;
    uint64_t totalRows = 0;
    while (reader.next(100, result)) {
      totalRows += result->size();
    }
    EXPECT_EQ(totalRows, 6);
  }
}

TEST_F(VeloxWriterTest, disableStatsCollectionWithChunking) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto type = velox::ROW({{"col0", velox::INTEGER()}});
  auto vector = vectorMaker.rowVector(
      {"col0"}, {vectorMaker.flatVector<int32_t>({1, 2, 3})});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriterOptions options;
  options.enableStatsCollection = false;
  options.enableChunking = true;
  nimble::VeloxWriter writer(
      type, std::move(writeFile), *rootPool_, std::move(options));
  writer.write(vector);
  writer.close();

  auto stats = writer.stats();
  EXPECT_TRUE(stats.columnStats.empty());
  EXPECT_EQ(stats.stripeCount, 1);
  EXPECT_GT(stats.writtenBytes, 0);
}

namespace {

nimble::EncodingLayout deltaEncodingLayout() {
  return nimble::EncodingLayout{
      nimble::EncodingType::Delta,
      {},
      nimble::CompressionType::Uncompressed,
      {
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              {},
              nimble::CompressionType::Uncompressed},
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              {},
              nimble::CompressionType::Uncompressed},
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              {},
              nimble::CompressionType::Uncompressed},
      }};
}

nimble::EncodingLayoutTree singleScalarLayoutTree(
    nimble::EncodingLayout layout) {
  return nimble::EncodingLayoutTree{
      nimble::Kind::Row,
      {},
      "",
      {{nimble::Kind::Scalar, {{0, std::move(layout)}}, ""}}};
}

nimble::VeloxReadParams nonLegacyReadParams() {
  nimble::VeloxReadParams params;
  params.encodingFactory =
      [](velox::memory::MemoryPool& pool,
         std::string_view data,
         std::function<void*(uint32_t)> stringBufferFactory)
      -> std::unique_ptr<nimble::Encoding> {
    return nimble::EncodingFactory().create(
        pool, data, std::move(stringBufferFactory));
  };
  return params;
}

void verifyDeltaEncoding(
    const std::string& file,
    velox::memory::MemoryPool& pool,
    bool checkChildren = false) {
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = nimble::TabletReader::create(
      readFile, &pool, makeTestTabletOptions(&pool));
  auto section =
      tablet->loadOptionalSection(std::string(nimble::kSchemaSection));
  NIMBLE_CHECK(section.has_value(), "Schema not found.");
  auto schema =
      nimble::SchemaDeserializer::deserialize(section->content().data());
  auto& scalarNode = schema->asRow().childAt(0)->asScalar();

  for (auto i = 0; i < tablet->stripeCount(); ++i) {
    auto stripeIdentifier = tablet->stripeIdentifier(i);
    std::vector<uint32_t> identifiers{scalarNode.scalarDescriptor().offset()};
    auto streams = tablet->load(stripeIdentifier, identifiers);

    nimble::InMemoryChunkedStream chunkedStream{pool, std::move(streams[0])};
    ASSERT_TRUE(chunkedStream.hasNext());
    auto capture =
        nimble::EncodingLayoutCapture::capture(chunkedStream.nextChunk());
    EXPECT_EQ(nimble::EncodingType::Delta, capture.encodingType())
        << "Stripe " << i;
    if (checkChildren) {
      EXPECT_EQ(
          nimble::EncodingType::Trivial,
          capture.child(nimble::EncodingIdentifiers::Delta::Deltas)
              ->encodingType());
      EXPECT_EQ(
          nimble::EncodingType::Trivial,
          capture.child(nimble::EncodingIdentifiers::Delta::Restatements)
              ->encodingType());
      EXPECT_EQ(
          nimble::EncodingType::Trivial,
          capture.child(nimble::EncodingIdentifiers::Delta::IsRestatements)
              ->encodingType());
    }
  }
}

} // namespace

// Monotonically increasing int64 data. Verifies Delta encoding structure
// (including child encodings) and round-trip data correctness.
TEST_F(VeloxWriterTest, encodingLayoutDelta) {
  constexpr int32_t kRowCount = 100;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"col0"}, {vectorMaker.flatVector<int64_t>(kRowCount, [](auto row) {
        return 10 + row * 3;
      })});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = singleScalarLayoutTree(deltaEncodingLayout())});
  writer.write(vector);
  writer.close();

  for (auto useChainedBuffers : {false, true}) {
    auto readFile =
        std::make_shared<nimble::testing::InMemoryTrackableReadFile>(
            file, useChainedBuffers);
    auto tablet = nimble::TabletReader::create(
        readFile, leafPool_.get(), makeTestTabletOptions(leafPool_.get()));
    auto section =
        tablet->loadOptionalSection(std::string(nimble::kSchemaSection));
    NIMBLE_CHECK(section.has_value(), "Schema not found.");
    auto schema =
        nimble::SchemaDeserializer::deserialize(section->content().data());
    auto& scalarNode = schema->asRow().childAt(0)->asScalar();

    for (auto i = 0; i < tablet->stripeCount(); ++i) {
      auto stripeIdentifier = tablet->stripeIdentifier(i);
      std::vector<uint32_t> identifiers{scalarNode.scalarDescriptor().offset()};
      auto streams = tablet->load(stripeIdentifier, identifiers);

      nimble::InMemoryChunkedStream chunkedStream{
          *leafPool_, std::move(streams[0])};
      ASSERT_TRUE(chunkedStream.hasNext());
      auto capture =
          nimble::EncodingLayoutCapture::capture(chunkedStream.nextChunk());
      EXPECT_EQ(nimble::EncodingType::Delta, capture.encodingType());
      EXPECT_EQ(
          nimble::EncodingType::Trivial,
          capture.child(nimble::EncodingIdentifiers::Delta::Deltas)
              ->encodingType());
      EXPECT_EQ(
          nimble::EncodingType::Trivial,
          capture.child(nimble::EncodingIdentifiers::Delta::Restatements)
              ->encodingType());
      EXPECT_EQ(
          nimble::EncodingType::Trivial,
          capture.child(nimble::EncodingIdentifiers::Delta::IsRestatements)
              ->encodingType());
    }
  }

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(
      &readFile, *leafPool_, nullptr, nonLegacyReadParams());
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kRowCount, result));
  ASSERT_EQ(result->size(), kRowCount);
  for (auto i = 0; i < kRowCount; ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, i));
  }
}

// Periodic large jumps trigger restatements in delta encoding.
TEST_F(VeloxWriterTest, encodingLayoutDeltaWithRestatements) {
  constexpr int32_t kRowCount = 100;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"col0"}, {vectorMaker.flatVector<int64_t>(kRowCount, [](auto row) {
        int64_t segment = row / 10;
        int64_t offset = row % 10;
        return segment * 1000 + offset * 3;
      })});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = singleScalarLayoutTree(deltaEncodingLayout())});
  writer.write(vector);
  writer.close();

  verifyDeltaEncoding(file, *leafPool_);

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(
      &readFile, *leafPool_, nullptr, nonLegacyReadParams());
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kRowCount, result));
  ASSERT_EQ(result->size(), kRowCount);
  for (auto i = 0; i < kRowCount; ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, i));
  }
}

// Verifies Delta encoding works with int32_t values.
TEST_F(VeloxWriterTest, encodingLayoutDeltaInt32) {
  constexpr int32_t kRowCount = 200;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"col0"}, {vectorMaker.flatVector<int32_t>(kRowCount, [](auto row) {
        return 5 + row * 2;
      })});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = singleScalarLayoutTree(deltaEncodingLayout())});
  writer.write(vector);
  writer.close();

  verifyDeltaEncoding(file, *leafPool_, true);

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(
      &readFile, *leafPool_, nullptr, nonLegacyReadParams());
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kRowCount, result));
  ASSERT_EQ(result->size(), kRowCount);
  for (auto i = 0; i < kRowCount; ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, i));
  }
}

// Negative values with zero crossings exercise the signed overflow
// restatement protection in DeltaEncoding::computeDeltas.
TEST_F(VeloxWriterTest, encodingLayoutDeltaNegativeValues) {
  constexpr int32_t kRowCount = 200;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"col0"}, {vectorMaker.flatVector<int64_t>(kRowCount, [](auto row) {
        // Oscillate across zero with periodic jumps.
        int64_t segment = row / 50;
        int64_t offset = row % 50;
        return static_cast<int64_t>(-100 + offset * 3 + segment * 20);
      })});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = singleScalarLayoutTree(deltaEncodingLayout())});
  writer.write(vector);
  writer.close();

  verifyDeltaEncoding(file, *leafPool_);

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(
      &readFile, *leafPool_, nullptr, nonLegacyReadParams());
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kRowCount, result));
  ASSERT_EQ(result->size(), kRowCount);
  for (auto i = 0; i < kRowCount; ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, i));
  }
}

// Nullable column with Delta encoding nested inside a Nullable encoding.
TEST_F(VeloxWriterTest, encodingLayoutDeltaNullable) {
  constexpr int32_t kRowCount = 150;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  std::vector<std::optional<int64_t>> data(kRowCount);
  for (int32_t i = 0; i < kRowCount; ++i) {
    if (i % 7 == 0) {
      data[i] = std::nullopt;
    } else {
      data[i] = 10 + static_cast<int64_t>(i) * 3;
    }
  }

  auto vector =
      vectorMaker.rowVector({"col0"}, {vectorMaker.flatVectorNullable(data)});

  // The layout specifies the data encoding only. The writer automatically
  // applies Nullable wrapping when nulls are present.
  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = singleScalarLayoutTree(deltaEncodingLayout())});
  writer.write(vector);
  writer.close();

  // Nimble stores nulls as a separate stream, so the data stream should
  // contain only the non-null values encoded with Delta.
  verifyDeltaEncoding(file, *leafPool_, true);

  velox::InMemoryReadFile readFile2(file);
  nimble::VeloxReader reader(
      &readFile2, *leafPool_, nullptr, nonLegacyReadParams());
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kRowCount, result));
  ASSERT_EQ(result->size(), kRowCount);
  for (auto i = 0; i < kRowCount; ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, i));
  }
}

// Multiple stripes with a small flush threshold. Verifies Delta encoding
// is consistent across all stripes.
TEST_F(VeloxWriterTest, encodingLayoutDeltaMultiStripe) {
  constexpr int32_t kBatchSize = 500;
  constexpr int32_t kBatchCount = 4;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  std::vector<velox::RowVectorPtr> batches;
  for (int32_t b = 0; b < kBatchCount; ++b) {
    batches.push_back(vectorMaker.rowVector(
        {"col0"}, {vectorMaker.flatVector<int64_t>(kBatchSize, [b](auto row) {
          return static_cast<int64_t>(b) * 10000 + row * 3;
        })}));
  }

  auto layoutTree = singleScalarLayoutTree(deltaEncodingLayout());

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      batches[0]->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = std::move(layoutTree), .flushPolicyFactory = []() {
         return std::make_unique<nimble::StripeRawSizeFlushPolicy>(1024);
       }});

  for (const auto& batch : batches) {
    writer.write(batch);
  }
  writer.close();

  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = nimble::TabletReader::create(
      readFile, leafPool_.get(), makeTestTabletOptions(leafPool_.get()));
  ASSERT_GT(tablet->stripeCount(), 1);

  verifyDeltaEncoding(file, *leafPool_, true);

  // Read back all rows across stripes.
  velox::InMemoryReadFile readFile2(file);
  nimble::VeloxReader reader(
      &readFile2, *leafPool_, nullptr, nonLegacyReadParams());
  int32_t totalRows = 0;
  int32_t batchIdx = 0;
  int32_t batchOffset = 0;
  velox::VectorPtr result;
  while (reader.next(kBatchSize, result)) {
    for (int32_t i = 0; i < result->size(); ++i) {
      ASSERT_TRUE(result->equalValueAt(batches[batchIdx].get(), i, batchOffset))
          << "Mismatch at global row " << totalRows;
      ++batchOffset;
      ++totalRows;
      if (batchOffset == kBatchSize) {
        ++batchIdx;
        batchOffset = 0;
      }
    }
  }
  EXPECT_EQ(totalRows, kBatchSize * kBatchCount);
}

// Two columns: col0 uses Delta, col1 uses MainlyConstant. Verifies mixed
// encoding layouts in the same file work correctly.
TEST_F(VeloxWriterTest, encodingLayoutDeltaMultiColumn) {
  constexpr int32_t kRowCount = 100;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"col0", "col1"},
      {vectorMaker.flatVector<int64_t>(
           kRowCount, [](auto row) { return row * 5; }),
       vectorMaker.flatVector<int32_t>(
           kRowCount, [](auto row) { return row % 20 == 0 ? row : 42; })});

  nimble::EncodingLayoutTree layoutTree{
      nimble::Kind::Row,
      {},
      "",
      {
          {nimble::Kind::Scalar, {{0, deltaEncodingLayout()}}, ""},
          {nimble::Kind::Scalar,
           {{0,
             nimble::EncodingLayout{
                 nimble::EncodingType::MainlyConstant,
                 {},
                 nimble::CompressionType::Uncompressed,
                 {
                     nimble::EncodingLayout{
                         nimble::EncodingType::Trivial,
                         {},
                         nimble::CompressionType::Uncompressed},
                     nimble::EncodingLayout{
                         nimble::EncodingType::Trivial,
                         {},
                         nimble::CompressionType::Uncompressed},
                 }}}},
           ""},
      }};

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = std::move(layoutTree)});
  writer.write(vector);
  writer.close();

  // Verify both column encodings.
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = nimble::TabletReader::create(
      readFile, leafPool_.get(), makeTestTabletOptions(leafPool_.get()));
  auto section =
      tablet->loadOptionalSection(std::string(nimble::kSchemaSection));
  NIMBLE_CHECK(section.has_value(), "Schema not found.");
  auto schema =
      nimble::SchemaDeserializer::deserialize(section->content().data());
  auto& col0Node = schema->asRow().childAt(0)->asScalar();
  auto& col1Node = schema->asRow().childAt(1)->asScalar();

  for (auto i = 0; i < tablet->stripeCount(); ++i) {
    auto stripeIdentifier = tablet->stripeIdentifier(i);
    std::vector<uint32_t> identifiers{
        col0Node.scalarDescriptor().offset(),
        col1Node.scalarDescriptor().offset()};
    auto streams = tablet->load(stripeIdentifier, identifiers);

    {
      nimble::InMemoryChunkedStream chunkedStream{
          *leafPool_, std::move(streams[0])};
      ASSERT_TRUE(chunkedStream.hasNext());
      auto capture =
          nimble::EncodingLayoutCapture::capture(chunkedStream.nextChunk());
      EXPECT_EQ(nimble::EncodingType::Delta, capture.encodingType());
    }
    {
      nimble::InMemoryChunkedStream chunkedStream{
          *leafPool_, std::move(streams[1])};
      ASSERT_TRUE(chunkedStream.hasNext());
      auto capture =
          nimble::EncodingLayoutCapture::capture(chunkedStream.nextChunk());
      EXPECT_EQ(nimble::EncodingType::MainlyConstant, capture.encodingType());
    }
  }

  velox::InMemoryReadFile readFile2(file);
  nimble::VeloxReader reader(
      &readFile2, *leafPool_, nullptr, nonLegacyReadParams());
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kRowCount, result));
  ASSERT_EQ(result->size(), kRowCount);
  for (auto i = 0; i < kRowCount; ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, i));
  }
}

// Multiple write() calls produce a single stripe, verifying that Delta
// encoding handles data accumulated across batches.
TEST_F(VeloxWriterTest, encodingLayoutDeltaMultipleBatches) {
  constexpr int32_t kBatchSize = 50;
  constexpr int32_t kBatchCount = 5;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  std::vector<velox::RowVectorPtr> batches;
  for (int32_t b = 0; b < kBatchCount; ++b) {
    batches.push_back(vectorMaker.rowVector(
        {"col0"}, {vectorMaker.flatVector<int64_t>(kBatchSize, [b](auto row) {
          return static_cast<int64_t>(b * kBatchSize + row) * 7;
        })}));
  }

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      batches[0]->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = singleScalarLayoutTree(deltaEncodingLayout())});

  for (const auto& batch : batches) {
    writer.write(batch);
  }
  writer.close();

  verifyDeltaEncoding(file, *leafPool_, true);

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(
      &readFile, *leafPool_, nullptr, nonLegacyReadParams());
  velox::VectorPtr result;
  int32_t totalRows = 0;
  int32_t batchIdx = 0;
  int32_t batchOffset = 0;
  while (reader.next(kBatchSize, result)) {
    for (int32_t i = 0; i < result->size(); ++i) {
      ASSERT_TRUE(result->equalValueAt(batches[batchIdx].get(), i, batchOffset))
          << "Mismatch at global row " << totalRows;
      ++batchOffset;
      ++totalRows;
      if (batchOffset == kBatchSize) {
        ++batchIdx;
        batchOffset = 0;
      }
    }
  }
  EXPECT_EQ(totalRows, kBatchSize * kBatchCount);
}

// Writes delta-encoded data and reads it back using the default (legacy)
// encoding factory, verifying the legacy DeltaEncoding decoder works e2e.
TEST_F(VeloxWriterTest, encodingLayoutDeltaLegacyRead) {
  constexpr int32_t kRowCount = 100;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"col0"}, {vectorMaker.flatVector<int64_t>(kRowCount, [](auto row) {
        return 10 + row * 3;
      })});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = singleScalarLayoutTree(deltaEncodingLayout())});
  writer.write(vector);
  writer.close();

  verifyDeltaEncoding(file, *leafPool_, true);

  // Read using default VeloxReader (legacy encoding factory).
  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kRowCount, result));
  ASSERT_EQ(result->size(), kRowCount);
  for (auto i = 0; i < kRowCount; ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, i))
        << "Mismatch at row " << i;
  }
}

// Writes delta-encoded data with restatements (non-monotonic) and reads it
// back using the default (legacy) encoding factory.
TEST_F(VeloxWriterTest, encodingLayoutDeltaWithRestatementsLegacyRead) {
  constexpr int32_t kRowCount = 100;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"col0"}, {vectorMaker.flatVector<int64_t>(kRowCount, [](auto row) {
        int64_t segment = row / 10;
        int64_t offset = row % 10;
        return segment * 1000 + offset * 3;
      })});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = singleScalarLayoutTree(deltaEncodingLayout())});
  writer.write(vector);
  writer.close();

  verifyDeltaEncoding(file, *leafPool_);

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kRowCount, result));
  ASSERT_EQ(result->size(), kRowCount);
  for (auto i = 0; i < kRowCount; ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, i))
        << "Mismatch at row " << i;
  }
}

// Writes delta-encoded int32 data across multiple stripes and reads back
// using the default (legacy) encoding factory.
TEST_F(VeloxWriterTest, encodingLayoutDeltaMultiStripeLegacyRead) {
  constexpr int32_t kBatchSize = 500;
  constexpr int32_t kBatchCount = 4;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  std::vector<velox::RowVectorPtr> batches;
  for (int32_t b = 0; b < kBatchCount; ++b) {
    batches.push_back(vectorMaker.rowVector(
        {"col0"}, {vectorMaker.flatVector<int32_t>(kBatchSize, [b](auto row) {
          return static_cast<int32_t>(b) * 10000 + row * 2;
        })}));
  }

  auto layoutTree = singleScalarLayoutTree(deltaEncodingLayout());

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      batches[0]->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = std::move(layoutTree), .flushPolicyFactory = []() {
         return std::make_unique<nimble::StripeRawSizeFlushPolicy>(1024);
       }});

  for (const auto& batch : batches) {
    writer.write(batch);
  }
  writer.close();

  auto readFileTrackable =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = nimble::TabletReader::create(
      readFileTrackable,
      leafPool_.get(),
      makeTestTabletOptions(leafPool_.get()));
  ASSERT_GT(tablet->stripeCount(), 1);

  verifyDeltaEncoding(file, *leafPool_, true);

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);
  int32_t totalRows = 0;
  int32_t batchIdx = 0;
  int32_t batchOffset = 0;
  velox::VectorPtr result;
  while (reader.next(kBatchSize, result)) {
    for (int32_t i = 0; i < result->size(); ++i) {
      ASSERT_TRUE(result->equalValueAt(batches[batchIdx].get(), i, batchOffset))
          << "Mismatch at global row " << totalRows;
      ++batchOffset;
      ++totalRows;
      if (batchOffset == kBatchSize) {
        ++batchIdx;
        batchOffset = 0;
      }
    }
  }
  EXPECT_EQ(totalRows, kBatchSize * kBatchCount);
}

// Delta encoding with int16_t (smallint) values.
TEST_F(VeloxWriterTest, encodingLayoutDeltaInt16) {
  constexpr int32_t kRowCount = 300;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"col0"}, {vectorMaker.flatVector<int16_t>(kRowCount, [](auto row) {
        return static_cast<int16_t>(row * 3);
      })});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = singleScalarLayoutTree(deltaEncodingLayout())});
  writer.write(vector);
  writer.close();

  verifyDeltaEncoding(file, *leafPool_, true);

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(
      &readFile, *leafPool_, nullptr, nonLegacyReadParams());
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kRowCount, result));
  ASSERT_EQ(result->size(), kRowCount);
  for (auto i = 0; i < kRowCount; ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, i));
  }
}

// Delta encoding with seekToRow: write, seek to middle, read remainder.
TEST_F(VeloxWriterTest, encodingLayoutDeltaSeekToRow) {
  constexpr int32_t kRowCount = 200;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"col0"}, {vectorMaker.flatVector<int64_t>(kRowCount, [](auto row) {
        return 100 + row * 5;
      })});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = singleScalarLayoutTree(deltaEncodingLayout())});
  writer.write(vector);
  writer.close();

  verifyDeltaEncoding(file, *leafPool_);

  // Read using default (legacy) VeloxReader with seekToRow.
  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);

  constexpr int32_t kSeekRow = 100;
  reader.seekToRow(kSeekRow);
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kRowCount - kSeekRow, result));
  ASSERT_EQ(result->size(), kRowCount - kSeekRow);
  for (auto i = 0; i < result->size(); ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, kSeekRow + i))
        << "Mismatch at row " << i << " (global " << kSeekRow + i << ")";
  }
}

// Delta encoding with nullable data read using legacy reader.
TEST_F(VeloxWriterTest, encodingLayoutDeltaNullableLegacyRead) {
  constexpr int32_t kRowCount = 150;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  std::vector<std::optional<int64_t>> data(kRowCount);
  for (int32_t i = 0; i < kRowCount; ++i) {
    if (i % 7 == 0) {
      data[i] = std::nullopt;
    } else {
      data[i] = 10 + static_cast<int64_t>(i) * 3;
    }
  }

  auto vector =
      vectorMaker.rowVector({"col0"}, {vectorMaker.flatVectorNullable(data)});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = singleScalarLayoutTree(deltaEncodingLayout())});
  writer.write(vector);
  writer.close();

  verifyDeltaEncoding(file, *leafPool_, true);

  // Read using default VeloxReader (legacy encoding factory).
  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kRowCount, result));
  ASSERT_EQ(result->size(), kRowCount);
  for (auto i = 0; i < kRowCount; ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, i))
        << "Mismatch at row " << i;
  }
}

// Delta encoding: sawtooth pattern triggers frequent restatements.
TEST_F(VeloxWriterTest, encodingLayoutDeltaSawtooth) {
  constexpr int32_t kRowCount = 500;
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"col0"}, {vectorMaker.flatVector<int64_t>(kRowCount, [](auto row) {
        // Sawtooth: rises to 100 then resets every 20 rows.
        return static_cast<int64_t>((row % 20) * 5);
      })});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      vector->type(),
      std::move(writeFile),
      *rootPool_,
      {.encodingLayoutTree = singleScalarLayoutTree(deltaEncodingLayout())});
  writer.write(vector);
  writer.close();

  verifyDeltaEncoding(file, *leafPool_, true);

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(
      &readFile, *leafPool_, nullptr, nonLegacyReadParams());
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kRowCount, result));
  ASSERT_EQ(result->size(), kRowCount);
  for (auto i = 0; i < kRowCount; ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, i));
  }
}

TEST_F(VeloxWriterTest, flatmapColumnsKeysSchemaConsistency) {
  // Two writers with the same 10 predefined keys but different data arrival
  // order should produce identical schemas. 500 rows each, 5 batches.
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto type = velox::ROW({{"m", velox::MAP(velox::INTEGER(), velox::REAL())}});

  const std::vector<std::string> predefinedKeysList = {
      "20", "5", "13", "8", "17", "2", "11", "19", "7", "15"};
  const std::set<std::string> predefinedKeys(
      predefinedKeysList.begin(), predefinedKeysList.end());
  constexpr int32_t kNumRows = 500;
  constexpr int32_t kNumKeys = 10;
  constexpr int32_t kBatches = 5;
  constexpr int32_t kRowsPerBatch = kNumRows / kBatches;

  auto writeWithKeyOrder = [&](bool reverseKeys) -> std::string {
    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriterOptions options;
    options.flatMapColumns = {{"m", predefinedKeys}};
    nimble::VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, std::move(options));

    for (int32_t b = 0; b < kBatches; ++b) {
      auto mapVec = vectorMaker.mapVector<int32_t, float>(
          kRowsPerBatch,
          /* sizeAt */ [](auto row) { return (row % kNumKeys) + 1; },
          /* keyAt */
          [&](auto /*row*/, auto mapIndex) {
            int32_t keyIdx = reverseKeys ? (kNumKeys - 1 - mapIndex) % kNumKeys
                                         : mapIndex % kNumKeys;
            return folly::to<int32_t>(predefinedKeysList[keyIdx]);
          },
          /* valueAt */
          [](auto /*row*/, auto mapIndex) {
            return static_cast<float>(mapIndex);
          },
          /* isNullAt */ [](auto row) { return row % 50 == 0; });
      writer.write(vectorMaker.rowVector({"m"}, {mapVec}));
    }
    writer.close();
    return file;
  };

  auto file1 = writeWithKeyOrder(false);
  auto file2 = writeWithKeyOrder(true);

  nimble::VeloxReader reader1(
      std::make_shared<velox::InMemoryReadFile>(file1), *leafPool_);
  nimble::VeloxReader reader2(
      std::make_shared<velox::InMemoryReadFile>(file2), *leafPool_);

  const auto& flatMap1 = reader1.schema()->asRow().childAt(0)->asFlatMap();
  const auto& flatMap2 = reader2.schema()->asRow().childAt(0)->asFlatMap();

  ASSERT_EQ(flatMap1.childrenCount(), flatMap2.childrenCount());
  ASSERT_EQ(flatMap1.childrenCount(), kNumKeys);
  std::vector<std::string> sortedKeys(
      predefinedKeys.begin(), predefinedKeys.end());
  for (uint32_t i = 0; i < flatMap1.childrenCount(); ++i) {
    EXPECT_EQ(flatMap1.nameAt(i), flatMap2.nameAt(i));
    EXPECT_EQ(flatMap1.nameAt(i), sortedKeys[i]);
  }
}

TEST_F(VeloxWriterTest, flatmapColumnsKeysRoundtrip) {
  // Write 1000 rows with 8 predefined keys across 4 batches, verify roundtrip.
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto type =
      velox::ROW({{"m", velox::MAP(velox::INTEGER(), velox::BIGINT())}});

  const std::vector<std::string> predefinedKeysList = {
      "10", "3", "7", "15", "1", "20", "8", "12"};
  const std::set<std::string> predefinedKeys(
      predefinedKeysList.begin(), predefinedKeysList.end());
  constexpr int32_t kNumKeys = 8;
  constexpr int32_t kRowsPerBatch = 250;
  constexpr int32_t kBatches = 4;
  constexpr int32_t kTotalRows = kRowsPerBatch * kBatches;

  // Collect all written batches for verification.
  auto expected = velox::BaseVector::create(type, 0, leafPool_.get());

  std::string file;
  {
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriterOptions options;
    options.flatMapColumns = {{"m", predefinedKeys}};
    nimble::VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, std::move(options));

    for (int32_t b = 0; b < kBatches; ++b) {
      auto mapVec = vectorMaker.mapVector<int32_t, int64_t>(
          kRowsPerBatch,
          /* sizeAt */
          [](auto row) { return (row % kNumKeys) + 1; },
          /* keyAt */
          [&](auto /*row*/, auto mapIndex) {
            return folly::to<int32_t>(predefinedKeysList[mapIndex % kNumKeys]);
          },
          /* valueAt */
          [&](auto row, auto mapIndex) {
            return static_cast<int64_t>(
                (b * kRowsPerBatch + row) * 100 + mapIndex);
          },
          /* isNullAt */ [](auto row) { return row % 100 == 0; });
      auto batch = vectorMaker.rowVector({"m"}, {mapVec});
      writer.write(batch);
      expected->append(batch.get());
    }
    writer.close();
  }

  nimble::VeloxReader reader(
      std::make_shared<velox::InMemoryReadFile>(file), *leafPool_);
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kTotalRows, result));
  ASSERT_EQ(result->size(), kTotalRows);
  for (auto i = 0; i < kTotalRows; ++i) {
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i))
        << "Mismatch at row " << i;
  }
}

TEST_F(VeloxWriterTest, flatmapColumnsKeysVarchar) {
  // Test with 6 VARCHAR keys, 500 rows, 3 batches to verify StringView
  // lifetime correctness across multiple batches.
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto type =
      velox::ROW({{"m", velox::MAP(velox::VARCHAR(), velox::INTEGER())}});

  const std::vector<std::string> predefinedKeysList = {
      "alpha", "beta", "gamma", "delta", "epsilon", "zeta"};
  const std::set<std::string> predefinedKeys(
      predefinedKeysList.begin(), predefinedKeysList.end());
  constexpr int32_t kNumKeys = 6;
  constexpr int32_t kRowsPerBatch = 500;
  constexpr int32_t kBatches = 3;
  constexpr int32_t kTotalRows = kRowsPerBatch * kBatches;

  auto expected = velox::BaseVector::create(type, 0, leafPool_.get());

  std::string file;
  {
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriterOptions options;
    options.flatMapColumns = {{"m", predefinedKeys}};
    nimble::VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, std::move(options));

    for (int32_t b = 0; b < kBatches; ++b) {
      auto mapVec = vectorMaker.mapVector<velox::StringView, int32_t>(
          kRowsPerBatch,
          /* sizeAt */ [](auto row) { return (row % kNumKeys) + 1; },
          /* keyAt */
          [&](auto /*row*/, auto mapIndex) {
            return velox::StringView(predefinedKeysList[mapIndex % kNumKeys]);
          },
          /* valueAt */
          [&](auto /*row*/, auto mapIndex) {
            return static_cast<int32_t>(b * kRowsPerBatch + mapIndex);
          },
          /* isNullAt */ [](auto row) { return row % 80 == 0; });
      auto batch = vectorMaker.rowVector({"m"}, {mapVec});
      writer.write(batch);
      expected->append(batch.get());
    }
    writer.close();
  }

  nimble::VeloxReader reader(
      std::make_shared<velox::InMemoryReadFile>(file), *leafPool_);

  // Verify schema key order.
  const auto& flatMap = reader.schema()->asRow().childAt(0)->asFlatMap();
  ASSERT_EQ(flatMap.childrenCount(), kNumKeys);
  {
    std::vector<std::string> sortedKeys(
        predefinedKeys.begin(), predefinedKeys.end());
    for (int i = 0; i < kNumKeys; ++i) {
      EXPECT_EQ(flatMap.nameAt(i), sortedKeys[i]);
    }
  }

  // Verify data roundtrip.
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kTotalRows, result));
  ASSERT_EQ(result->size(), kTotalRows);
  for (auto i = 0; i < kTotalRows; ++i) {
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i))
        << "Mismatch at row " << i;
  }
}

TEST_F(VeloxWriterTest, flatmapColumnsKeysMultipleBatches) {
  // Write 10 batches of 200 rows each with 8 predefined keys. Each batch
  // uses a different subset of keys to exercise the pre-registration path.
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto type =
      velox::ROW({{"m", velox::MAP(velox::INTEGER(), velox::INTEGER())}});

  const std::vector<std::string> predefinedKeysList = {
      "1", "2", "3", "4", "5", "6", "7", "8"};
  const std::set<std::string> predefinedKeys(
      predefinedKeysList.begin(), predefinedKeysList.end());
  constexpr int32_t kNumKeys = 8;
  constexpr int32_t kRowsPerBatch = 200;
  constexpr int32_t kBatches = 10;
  constexpr int32_t kTotalRows = kRowsPerBatch * kBatches;

  auto expected = velox::BaseVector::create(type, 0, leafPool_.get());

  std::string file;
  {
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriterOptions options;
    options.flatMapColumns = {{"m", predefinedKeys}};
    nimble::VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, std::move(options));

    for (int32_t b = 0; b < kBatches; ++b) {
      // Each batch uses a sliding window of keys starting at offset b.
      auto mapVec = vectorMaker.mapVector<int32_t, int32_t>(
          kRowsPerBatch,
          /* sizeAt */
          [&](auto row) {
            // Vary map size: 1 to (b % kNumKeys + 1)
            return (row % (b % kNumKeys + 1)) + 1;
          },
          /* keyAt */
          [&](auto /*row*/, auto mapIndex) {
            return folly::to<int32_t>(
                predefinedKeysList[(b + mapIndex) % kNumKeys]);
          },
          /* valueAt */
          [&](auto row, auto mapIndex) {
            return static_cast<int32_t>(b * 1000 + row * 10 + mapIndex);
          },
          /* isNullAt */ [](auto row) { return row % 40 == 0; });
      auto batch = vectorMaker.rowVector({"m"}, {mapVec});
      writer.write(batch);
      expected->append(batch.get());
    }
    writer.close();
  }

  nimble::VeloxReader reader(
      std::make_shared<velox::InMemoryReadFile>(file), *leafPool_);

  // Schema should have all 8 keys in predefined (sorted) order.
  const auto& flatMap = reader.schema()->asRow().childAt(0)->asFlatMap();
  ASSERT_EQ(flatMap.childrenCount(), kNumKeys);
  {
    std::vector<std::string> sortedKeys(
        predefinedKeys.begin(), predefinedKeys.end());
    for (int i = 0; i < kNumKeys; ++i) {
      EXPECT_EQ(flatMap.nameAt(i), sortedKeys[i]);
    }
  }

  // Verify all rows roundtrip.
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kTotalRows, result));
  ASSERT_EQ(result->size(), kTotalRows);
  for (auto i = 0; i < kTotalRows; ++i) {
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i))
        << "Mismatch at row " << i;
  }
}

TEST_F(VeloxWriterTest, flatmapColumnsKeysEmptyData) {
  // Pre-register 10 keys, write 200 rows of empty maps across 4 batches.
  // Schema should still contain all predefined keys.
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto type = velox::ROW({{"m", velox::MAP(velox::INTEGER(), velox::REAL())}});

  const std::set<std::string> predefinedKeys = {
      "5", "3", "7", "11", "2", "9", "14", "1", "18", "6"};
  constexpr int32_t kNumKeys = 10;
  constexpr int32_t kRowsPerBatch = 50;
  constexpr int32_t kBatches = 4;

  std::string file;
  {
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriterOptions options;
    options.flatMapColumns = {{"m", predefinedKeys}};
    nimble::VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, std::move(options));

    for (int32_t b = 0; b < kBatches; ++b) {
      auto mapVec = vectorMaker.mapVector<int32_t, float>(
          kRowsPerBatch,
          /* sizeAt */ [](auto) { return 0; }, // all empty maps
          /* keyAt */ [](auto, auto) { return 0; },
          /* valueAt */ [](auto, auto) { return 0.0f; },
          /* isNullAt */ [](auto row) { return row % 10 == 0; });
      writer.write(vectorMaker.rowVector({"m"}, {mapVec}));
    }
    writer.close();
  }

  nimble::VeloxReader reader(
      std::make_shared<velox::InMemoryReadFile>(file), *leafPool_);
  const auto& flatMap = reader.schema()->asRow().childAt(0)->asFlatMap();
  ASSERT_EQ(flatMap.childrenCount(), kNumKeys);
  {
    std::vector<std::string> sortedKeys(
        predefinedKeys.begin(), predefinedKeys.end());
    for (int i = 0; i < kNumKeys; ++i) {
      EXPECT_EQ(flatMap.nameAt(i), sortedKeys[i]);
    }
  }
}

TEST_F(VeloxWriterTest, flatmapColumnsKeysUnknownKeyRejection) {
  // Pre-register 5 keys, write 100 rows. Last row includes an unknown key.
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto type =
      velox::ROW({{"m", velox::MAP(velox::INTEGER(), velox::INTEGER())}});

  const std::set<std::string> predefinedKeys = {"1", "2", "3", "4", "5"};
  constexpr int32_t kNumRows = 100;
  constexpr int32_t kNumKeys = 5;

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriterOptions options;
  options.flatMapColumns = {{"m", predefinedKeys}};
  nimble::VeloxWriter writer(
      type, std::move(writeFile), *rootPool_, std::move(options));

  // Include unknown key 99 in every row.
  auto input = vectorMaker.rowVector(
      {"m"},
      {vectorMaker.mapVector<int32_t, int32_t>(
          kNumRows,
          /* sizeAt */ [](auto) { return 2; },
          /* keyAt */
          [](auto row, auto mapIndex) {
            return mapIndex == 0 ? (row % kNumKeys) + 1 : 99;
          },
          /* valueAt */
          [](auto row, auto mapIndex) { return row * 10 + mapIndex; })});

  EXPECT_THROW(writer.write(input), nimble::NimbleUserError);
}

TEST_F(VeloxWriterTest, flatmapColumnsKeysRejectsRowIngestion) {
  // When flatMapColumns with predefined keys is configured, passing a ROW
  // vector (instead of MAP) should throw. Test with 100 rows.
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto type = velox::ROW({{"m", velox::MAP(velox::INTEGER(), velox::REAL())}});

  constexpr int32_t kNumRows = 100;

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriterOptions options;
  options.flatMapColumns = {{"m", {"1", "2", "3", "4", "5"}}};
  nimble::VeloxWriter writer(
      type, std::move(writeFile), *rootPool_, std::move(options));

  // Build a RowVector child to trigger ingestRow path.
  auto rowChild = vectorMaker.rowVector(
      {"a"}, {vectorMaker.flatVector<float>(kNumRows, [](auto row) {
        return static_cast<float>(row);
      })});
  auto input = std::make_shared<velox::RowVector>(
      leafPool_.get(),
      velox::ROW({{"m", rowChild->type()}}),
      nullptr,
      kNumRows,
      std::vector<velox::VectorPtr>{rowChild});

  EXPECT_THROW(writer.write(input), nimble::NimbleUserError);
}

TEST_F(VeloxWriterTest, flatmapColumnsKeysImplicitFlatMapColumn) {
  // Columns listed in flatMapColumns with predefined keys are implicitly
  // treated as flat map columns. Test with 8 keys and 500 rows across 3
  // batches.
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto type =
      velox::ROW({{"m", velox::MAP(velox::INTEGER(), velox::INTEGER())}});

  const std::vector<std::string> predefinedKeysList = {
      "10", "3", "7", "15", "1", "20", "8", "12"};
  const std::set<std::string> predefinedKeys(
      predefinedKeysList.begin(), predefinedKeysList.end());
  constexpr int32_t kNumKeys = 8;
  constexpr int32_t kRowsPerBatch = 500;
  constexpr int32_t kBatches = 3;
  constexpr int32_t kTotalRows = kRowsPerBatch * kBatches;

  auto expected = velox::BaseVector::create(type, 0, leafPool_.get());

  std::string file;
  {
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriterOptions options;
    // Only set flatMapColumns with predefined keys.
    options.flatMapColumns = {{"m", predefinedKeys}};
    nimble::VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, std::move(options));

    for (int32_t b = 0; b < kBatches; ++b) {
      auto mapVec = vectorMaker.mapVector<int32_t, int32_t>(
          kRowsPerBatch,
          /* sizeAt */ [](auto row) { return (row % kNumKeys) + 1; },
          /* keyAt */
          [&](auto /*row*/, auto mapIndex) {
            return folly::to<int32_t>(predefinedKeysList[mapIndex % kNumKeys]);
          },
          /* valueAt */
          [&](auto row, auto mapIndex) {
            return static_cast<int32_t>(b * 1000 + row * 10 + mapIndex);
          },
          /* isNullAt */ [](auto row) { return row % 50 == 0; });
      auto batch = vectorMaker.rowVector({"m"}, {mapVec});
      writer.write(batch);
      expected->append(batch.get());
    }
    writer.close();
  }

  // Verify it was written as a flatmap (schema has FlatMap kind).
  nimble::VeloxReader reader(
      std::make_shared<velox::InMemoryReadFile>(file), *leafPool_);
  const auto& child = reader.schema()->asRow().childAt(0);
  EXPECT_EQ(child->kind(), nimble::Kind::FlatMap);
  const auto& flatMap = child->asFlatMap();
  ASSERT_EQ(flatMap.childrenCount(), kNumKeys);
  {
    std::vector<std::string> sortedKeys(
        predefinedKeys.begin(), predefinedKeys.end());
    for (int i = 0; i < kNumKeys; ++i) {
      EXPECT_EQ(flatMap.nameAt(i), sortedKeys[i]);
    }
  }

  // Verify data roundtrip.
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(kTotalRows, result));
  ASSERT_EQ(result->size(), kTotalRows);
  for (auto i = 0; i < kTotalRows; ++i) {
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i))
        << "Mismatch at row " << i;
  }
}

struct ParallelEncodeParam {
  uint32_t maxEncodeParallelism;
  uint32_t minStreamsPerEncodeUnit;

  std::string debugString() const {
    return fmt::format(
        "maxParallel_{}_minStreams_{}",
        maxEncodeParallelism,
        minStreamsPerEncodeUnit);
  }
};

class ParallelEncodeWriterTest
    : public VeloxWriterTest,
      public ::testing::WithParamInterface<ParallelEncodeParam> {
 protected:
  nimble::VeloxWriterOptions parallelWriterOptions() {
    nimble::VeloxWriterOptions options;
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(4);
    options.encodingExecutor = folly::getKeepAliveToken(*executor_);
    options.maxEncodeParallelism = GetParam().maxEncodeParallelism;
    options.minStreamsPerEncodeUnit = GetParam().minStreamsPerEncodeUnit;
    return options;
  }

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
};

TEST_P(ParallelEncodeWriterTest, rowParallelEncode) {
  auto type = velox::ROW({
      {"a", velox::BIGINT()},
      {"b", velox::DOUBLE()},
      {"c", velox::INTEGER()},
      {"d", velox::BIGINT()},
      {"e", velox::REAL()},
      {"f", velox::BIGINT()},
      {"g", velox::INTEGER()},
      {"h", velox::DOUBLE()},
  });

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  velox::VectorFuzzer fuzzer(
      {.vectorSize = 1000, .nullRatio = 0.1}, leafPool_.get(), seed);

  auto writerOptions = parallelWriterOptions();

  std::string seqFile;
  std::string parFile;

  {
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&seqFile);
    nimble::VeloxWriter writer(type, std::move(writeFile), *rootPool_, {});
    for (int i = 0; i < 10; ++i) {
      writer.write(fuzzer.fuzzInputRow(type));
    }
    writer.close();
  }

  {
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&parFile);
    nimble::VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, writerOptions);
    fuzzer.reSeed(seed);
    for (int i = 0; i < 10; ++i) {
      writer.write(fuzzer.fuzzInputRow(type));
    }
    writer.close();
  }

  auto seqRead = std::make_shared<velox::InMemoryReadFile>(seqFile);
  auto parRead = std::make_shared<velox::InMemoryReadFile>(parFile);
  nimble::VeloxReader seqReader(seqRead.get(), *leafPool_);
  nimble::VeloxReader parReader(parRead.get(), *leafPool_);

  velox::VectorPtr seqResult;
  velox::VectorPtr parResult;
  while (seqReader.next(1000, seqResult)) {
    ASSERT_TRUE(parReader.next(1000, parResult));
    ASSERT_EQ(seqResult->size(), parResult->size());
    for (velox::vector_size_t row = 0; row < seqResult->size(); ++row) {
      ASSERT_TRUE(seqResult->equalValueAt(parResult.get(), row, row))
          << "Mismatch at row " << row;
    }
  }
}

TEST_P(ParallelEncodeWriterTest, flatMapParallelEncode) {
  auto type = velox::ROW({
      {"flatmap", velox::MAP(velox::INTEGER(), velox::BIGINT())},
      {"col", velox::BIGINT()},
  });

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  velox::VectorFuzzer fuzzer(
      {.vectorSize = 1000, .nullRatio = 0.1, .containerLength = 20},
      leafPool_.get(),
      seed);

  // The fuzzer emits random INTEGER keys, so the number of distinct flat-map
  // keys varies per seed and can exceed the default limit. This test compares
  // parallel vs sequential encoding, so lift the limit (0 = unlimited) to keep
  // the run independent of the seed's key cardinality.
  auto writerOptions = parallelWriterOptions();
  writerOptions.flatMapColumns = {{"flatmap", {}}};
  writerOptions.maxFlatMapKeys = 0;

  nimble::VeloxWriterOptions seqOptions;
  seqOptions.flatMapColumns = {{"flatmap", {}}};
  seqOptions.maxFlatMapKeys = 0;

  std::string seqFile;
  std::string parFile;

  {
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&seqFile);
    nimble::VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, seqOptions);
    for (int i = 0; i < 10; ++i) {
      writer.write(fuzzer.fuzzInputRow(type));
    }
    writer.close();
  }

  {
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&parFile);
    nimble::VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, writerOptions);
    fuzzer.reSeed(seed);
    for (int i = 0; i < 10; ++i) {
      writer.write(fuzzer.fuzzInputRow(type));
    }
    writer.close();
  }

  auto seqRead = std::make_shared<velox::InMemoryReadFile>(seqFile);
  auto parRead = std::make_shared<velox::InMemoryReadFile>(parFile);
  nimble::VeloxReader seqReader(seqRead.get(), *leafPool_);
  nimble::VeloxReader parReader(parRead.get(), *leafPool_);

  velox::VectorPtr seqResult;
  velox::VectorPtr parResult;
  while (seqReader.next(1000, seqResult)) {
    ASSERT_TRUE(parReader.next(1000, parResult));
    ASSERT_EQ(seqResult->size(), parResult->size());
    for (velox::vector_size_t row = 0; row < seqResult->size(); ++row) {
      ASSERT_TRUE(seqResult->equalValueAt(parResult.get(), row, row))
          << "Mismatch at row " << row;
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    ParallelEncodeWriterTestSuite,
    ParallelEncodeWriterTest,
    ::testing::Values(
        ParallelEncodeParam{2, 1},
        ParallelEncodeParam{4, 1},
        ParallelEncodeParam{8, 1},
        ParallelEncodeParam{4, 4},
        ParallelEncodeParam{100, 1}),
    [](const ::testing::TestParamInfo<ParallelEncodeParam>& info) {
      return info.param.debugString();
    });

DEBUG_ONLY_TEST_F(VeloxWriterTest, parallelEncodeRowTaskCount) {
  velox::common::testutil::TestValue::enable();

  auto type = velox::ROW({
      {"a", velox::BIGINT()},
      {"b", velox::DOUBLE()},
      {"c", velox::INTEGER()},
      {"d", velox::BIGINT()},
      {"e", velox::REAL()},
      {"f", velox::BIGINT()},
      {"g", velox::INTEGER()},
      {"h", velox::DOUBLE()},
  });

  velox::VectorFuzzer fuzzer(
      {.vectorSize = 100, .nullRatio = 0}, leafPool_.get());

  struct TestCase {
    uint32_t maxEncodeParallelism;
    uint32_t minStreamsPerEncodeUnit;
    uint32_t expectedTaskCount;
  };

  const std::vector<TestCase> testCases = {
      {2, 1, 2},
      {4, 1, 4},
      {8, 1, 8},
      {4, 4, 2},
      {8, 4, 2},
      {100, 1, 8},
  };

  folly::CPUThreadPoolExecutor executor(4);

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(
        fmt::format(
            "maxParallel={}, minStreams={}, expected={}",
            testCase.maxEncodeParallelism,
            testCase.minStreamsPerEncodeUnit,
            testCase.expectedTaskCount));

    nimble::VeloxWriterOptions writerOptions;
    writerOptions.encodingExecutor = folly::getKeepAliveToken(executor);
    writerOptions.maxEncodeParallelism = testCase.maxEncodeParallelism;
    writerOptions.minStreamsPerEncodeUnit = testCase.minStreamsPerEncodeUnit;

    uint32_t parallelWriteCount = 0;
    std::vector<uint32_t> observedTaskCounts;
    SCOPED_TESTVALUE_SET(
        "facebook::nimble::RowFieldWriter::co_write",
        std::function<void(const uint32_t*)>([&](const uint32_t* taskCount) {
          ++parallelWriteCount;
          observedTaskCounts.emplace_back(*taskCount);
        }));

    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, writerOptions);

    const size_t numBatches = 3;
    for (size_t i = 0; i < numBatches; ++i) {
      writer.write(fuzzer.fuzzInputRow(type));
    }
    writer.close();

    EXPECT_EQ(parallelWriteCount, numBatches);
    for (const auto taskCount : observedTaskCounts) {
      EXPECT_EQ(taskCount, testCase.expectedTaskCount);
    }

    auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
    nimble::VeloxReader reader(readFile.get(), *leafPool_);
    velox::VectorPtr result;
    ASSERT_TRUE(reader.next(numBatches * 100, result));
    EXPECT_EQ(result->size(), numBatches * 100);
  }
}

DEBUG_ONLY_TEST_F(VeloxWriterTest, parallelEncodeFlatMapTaskCount) {
  velox::common::testutil::TestValue::enable();

  auto type = velox::ROW({
      {"flatmap", velox::MAP(velox::INTEGER(), velox::BIGINT())},
      {"col", velox::BIGINT()},
  });

  velox::VectorFuzzer fuzzer(
      {.vectorSize = 100, .nullRatio = 0, .containerLength = 20},
      leafPool_.get());

  folly::CPUThreadPoolExecutor executor(4);

  nimble::VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns = {{"flatmap", {}}};
  writerOptions.encodingExecutor = folly::getKeepAliveToken(executor);
  writerOptions.maxEncodeParallelism = 4;
  writerOptions.minStreamsPerEncodeUnit = 1;

  uint32_t flatMapParallelCount = 0;
  SCOPED_TESTVALUE_SET(
      "facebook::nimble::FlatMapFieldWriter::co_writeMapValues",
      std::function<void(const uint32_t*)>([&](const uint32_t* taskCount) {
        ++flatMapParallelCount;
        EXPECT_GT(*taskCount, 1);
      }));

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      type, std::move(writeFile), *rootPool_, writerOptions);

  const size_t numBatches = 5;
  for (size_t i = 0; i < numBatches; ++i) {
    writer.write(fuzzer.fuzzInputRow(type));
  }
  writer.close();

  EXPECT_GT(flatMapParallelCount, 0);

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  nimble::VeloxReader reader(readFile.get(), *leafPool_);
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(numBatches * 100, result));
  EXPECT_EQ(result->size(), numBatches * 100);
}

} // namespace facebook
