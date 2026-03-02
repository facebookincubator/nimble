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
#include <folly/system/HardwareConcurrency.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/common/tests/NimbleFileWriter.h"
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingLayout.h"
#include "dwio/nimble/encodings/EncodingUtils.h"
#include "dwio/nimble/encodings/PrefixEncoding.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "dwio/nimble/index/tests/IndexTestUtils.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/FileLayout.h"
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

TEST_F(VeloxWriterTest, emptyFile) {
  auto type = velox::ROW({{"simple", velox::INTEGER()}});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(type, std::move(writeFile), *rootPool_, {});
  writer.close();

  // Verify FileLayout for empty file using FileLayout::create()
  velox::InMemoryReadFile readFile(file);
  auto layout = nimble::FileLayout::create(&readFile, leafPool_.get());
  EXPECT_EQ(layout.fileSize, file.size());
  EXPECT_EQ(layout.postscript.majorVersion(), nimble::kVersionMajor);
  EXPECT_EQ(layout.postscript.minorVersion(), nimble::kVersionMinor);
  EXPECT_GT(layout.footer.size(), 0);
  EXPECT_TRUE(layout.stripeGroups.empty());
  EXPECT_TRUE(layout.indexGroups.empty());
  EXPECT_TRUE(layout.stripesInfo.empty());

  nimble::VeloxReader reader(&readFile, *leafPool_);
  velox::VectorPtr result;
  ASSERT_FALSE(reader.next(1, result));
}

TEST_F(VeloxWriterTest, emptyFileWithIndexEnabled) {
  auto type = velox::ROW({
      {"key_col", velox::INTEGER()},
      {"value_col", velox::VARCHAR()},
  });

  nimble::IndexConfig indexConfig{
      .columns = {"key_col"},
      .sortOrders = {nimble::SortOrder{.ascending = true}},
      .enforceKeyOrder = true,
  };

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(
      type, std::move(writeFile), *rootPool_, {.indexConfig = indexConfig});
  writer.close();

  // Verify FileLayout for empty file with index enabled using
  // FileLayout::create()
  velox::InMemoryReadFile readFile(file);
  auto layout = nimble::FileLayout::create(&readFile, leafPool_.get());
  EXPECT_EQ(layout.fileSize, file.size());
  EXPECT_EQ(layout.stripesInfo.size(), 0);
  EXPECT_EQ(layout.stripeGroups.size(), 0);
  EXPECT_TRUE(layout.stripeGroups.empty());
  // Index groups should be empty for empty file (no stripes to index)
  EXPECT_TRUE(layout.indexGroups.empty());
  EXPECT_TRUE(layout.stripesInfo.empty());

  nimble::VeloxReader reader(&readFile, *leafPool_);
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
  velox::InMemoryReadFile readFile(file);
  auto layout = nimble::FileLayout::create(&readFile, leafPool_.get());
  EXPECT_EQ(layout.fileSize, file.size());
  EXPECT_EQ(layout.stripesInfo.size(), 1);
  EXPECT_EQ(layout.stripeGroups.size(), 1);
  EXPECT_EQ(layout.stripeGroups.size(), 1);
  // No index configured
  EXPECT_TRUE(layout.indexGroups.empty());
  // Stripes metadata should be valid (stripeGroups not empty)
  EXPECT_GT(layout.stripes.size(), 0);
  EXPECT_LT(layout.stripes.offset(), layout.footer.offset());
  // Per-stripe info
  EXPECT_EQ(layout.stripesInfo.size(), 1);
  EXPECT_EQ(layout.stripesInfo[0].stripeGroupIndex, 0);
  EXPECT_GT(layout.stripesInfo[0].size, 0);

  nimble::VeloxReader reader(&readFile, *leafPool_);
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
      {.flatMapColumns = {"flatmap"},
       .featureReordering =
           std::vector<std::tuple<size_t, std::vector<int64_t>>>{
               {0, {1, 2}}, {1, {3, 4}}}});
  writer.write(vector);
  writer.close();
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
        {.flatMapColumns = {"flatmap"}});
    EXPECT_THROW(writer.write(vec), nimble::NimbleInternalError);
    EXPECT_ANY_THROW(writer.close());
  }
  // Vector with a rotating duplicate key set. The more typical layout requiring
  // in map stream to represent.
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
        {.flatMapColumns = {"flatmap"}});
    EXPECT_THROW(writer.write(vec), nimble::NimbleInternalError);
    EXPECT_ANY_THROW(writer.close());
  }
}

namespace {
std::vector<velox::RowVectorPtr> generateBatches(
    const std::shared_ptr<const velox::RowType>& type,
    size_t batchCount,
    size_t size,
    uint32_t seed,
    velox::memory::MemoryPool& pool) {
  velox::VectorFuzzer fuzzer(
      {.vectorSize = size, .nullRatio = 0.1}, &pool, seed);
  std::vector<velox::RowVectorPtr> batches;
  batches.reserve(batchCount);
  for (size_t i = 0; i < batchCount; ++i) {
    batches.push_back(fuzzer.fuzzInputFlatRow(type));
  }
  return batches;
}

struct ChunkSizeResults {
  uint32_t stripeCount;
  uint32_t minChunkCount;
  uint32_t maxChunkCount;
};

ChunkSizeResults validateChunkSize(
    nimble::VeloxReader& reader,
    const uint64_t minStreamChunkRawSize,
    const uint64_t maxStreamChunkRawSize) {
  constexpr int kRowCountOffset = 2;
  constexpr double kMaxErrorRate = 0.2;
  const auto& tablet = reader.tabletReader();
  auto& pool = reader.memoryPool();

  std::unordered_set<uint32_t> stringStreamOffsets;
  nimble::SchemaReader::traverseSchema(
      reader.schema(),
      [&](uint32_t /*level*/,
          const nimble::Type& type,
          const nimble::SchemaReader::NodeInfo& /*info*/) {
        if (type.isScalar()) {
          auto scalarKind = type.asScalar().scalarDescriptor().scalarKind();
          if (scalarKind == nimble::ScalarKind::String ||
              scalarKind == nimble::ScalarKind::Binary) {
            stringStreamOffsets.insert(
                type.asScalar().scalarDescriptor().offset());
          }
        }
      });

  const uint32_t stripeCount = tablet.stripeCount();
  uint32_t maxChunkCount = 0;
  uint32_t minChunkCount = std::numeric_limits<uint32_t>::max();

  for (uint32_t stripeIndex = 0; stripeIndex < stripeCount; ++stripeIndex) {
    const auto stripeIdentifier = tablet.stripeIdentifier(stripeIndex);
    const auto streamCount = tablet.streamCount(stripeIdentifier);

    std::vector<uint32_t> streamIds(streamCount);
    std::iota(streamIds.begin(), streamIds.end(), 0);
    auto streamLoaders = tablet.load(stripeIdentifier, streamIds);

    for (uint32_t streamId = 0; streamId < streamLoaders.size(); ++streamId) {
      if (!streamLoaders[streamId]) {
        continue;
      }
      nimble::InMemoryChunkedStream chunkedStream{
          pool, std::move(streamLoaders[streamId])};
      uint32_t currentStreamChunkCount = 0;
      const bool isStringStream = stringStreamOffsets.contains(streamId);
      while (chunkedStream.hasNext()) {
        ++currentStreamChunkCount;
        const auto chunk = chunkedStream.nextChunk();
        const uint64_t chunkRawDataSize =
            nimble::test::TestUtils::getRawDataSize(pool, chunk);
        const uint32_t rowCount =
            *reinterpret_cast<const uint32_t*>(chunk.data() + kRowCountOffset);
        const double stringError =
            isStringStream ? rowCount * sizeof(std::string_view) : 0;

        // For string streams, a single row may exceed max chunk size if the
        // string itself is larger than the limit. This is acceptable since we
        // cannot split a single string value.
        if (!(isStringStream && rowCount == 1)) {
          const double maxError =
              kMaxErrorRate * maxStreamChunkRawSize + stringError;
          EXPECT_LE(chunkRawDataSize, maxStreamChunkRawSize + maxError)
              << "Stream " << streamId << " has a chunk with size "
              << chunkRawDataSize << " which is above max chunk size of "
              << maxStreamChunkRawSize;
        }

        // Validate min chunk size when not last chunk
        if (chunkedStream.hasNext() &&
            chunkRawDataSize < minStreamChunkRawSize) {
          const double minError =
              kMaxErrorRate * minStreamChunkRawSize + stringError;
          EXPECT_GE(chunkRawDataSize, minStreamChunkRawSize - minError)
              << "Stream " << streamId << " has a non-last chunk with size "
              << chunkRawDataSize << " which is below min chunk size of "
              << minStreamChunkRawSize;
        }
      }
      DWIO_ENSURE_GT(
          currentStreamChunkCount,
          0,
          "Non null streams should have at least one chunk");
      maxChunkCount = std::max(maxChunkCount, currentStreamChunkCount);
      minChunkCount = std::min(minChunkCount, currentStreamChunkCount);
    }
  }

  return ChunkSizeResults{
      .stripeCount = stripeCount,
      .minChunkCount = minChunkCount,
      .maxChunkCount = maxChunkCount,
  };
}
} // namespace

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

  // Each vector contains 99 strings with 36 characters each (36*99=3564) + 100
  // bytes for null vector + 99 string_views (99*16=1584) for a total of 5248
  // bytes, so writing 200 batches should exceed the flush theshold of 1MB
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
          .flatMapColumns = {"flatmap"},
          .encodingLayoutTree = std::move(expected),
          // Boosting acceptance ratio by 100x to make sure it is always
          // accepted (even if compressed size if bigger than uncompressed size)
          .compressionOptions =
              {.compressionAcceptRatio = 100, .internalMinCompressionSize = 0},
      });

  writer.write(vector);
  writer.close();

  for (auto useChainedBuffers : {false, true}) {
    nimble::testing::InMemoryTrackableReadFile readFile(
        file, useChainedBuffers);
    auto tablet = nimble::TabletReader::create(&readFile, leafPool_.get(), {});
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
          .flatMapColumns = {"map"},
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
  options.flatMapColumns = {"c0"};
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
    folly::F14FastSet<std::string> flatMapColumns = {}) {
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

  auto tablet = nimble::TabletReader::create(
      std::make_shared<velox::InMemoryReadFile>(file), leafPool.get(), {});
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
          // Chunks requested, and min chunk size is zero. However, first write
          // didn't have any data, so no chunk was written.
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
          // Chunks requested, but min size is too big, so expecting one merged
          // chunk.
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
                  //   std::vector<std::pair<int32_t, std::optional<int32_t>>>{
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
      /* flatmapColumns */ {"c1"});
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

        // Chunks requested, but min size is too big, so expecting single merged
        // chunk
        nimble::InMemoryChunkedStream chunked{
            *leafPool_, std::move(streamLoaders[1])};
        ASSERT_CHUNK_COUNT(1, chunked);
      },
      /* flatmapColumns */ {"c1"});
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
      /* flatmapColumns */ {"c1"});
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
      /* flatmapColumns */ {"c1"});
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
      /* flatmapColumns */ {"c1"});
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
      /* flatmapColumns */ {"c1"});
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

    auto readFilePtr = std::make_shared<velox::InMemoryReadFile>(file);
    nimble::TabletReader::Options readerOptions;
    readerOptions.preloadOptionalSections = {
        std::string(facebook::nimble::kStatsSection)};
    auto tablet = facebook::nimble::TabletReader::create(
        readFilePtr, leafPool_.get(), readerOptions);
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

    auto readFilePtr = std::make_shared<velox::InMemoryReadFile>(file);
    nimble::TabletReader::Options readerOptions;
    readerOptions.preloadOptionalSections = {
        std::string(facebook::nimble::kVectorizedStatsSection)};
    auto tablet = facebook::nimble::TabletReader::create(
        readFilePtr, leafPool_.get(), readerOptions);
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

TEST_F(VeloxWriterTest, fuzzComplex) {
  auto type = velox::ROW(
      {{"array", velox::ARRAY(velox::VARCHAR())},
       {"dict_array", velox::ARRAY(velox::REAL())},
       {"map", velox::MAP(velox::INTEGER(), velox::DOUBLE())},
       {"row",
        velox::ROW({
            {"a", velox::REAL()},
            {"b", velox::INTEGER()},
        })},
       {"row",
        velox::ROW(
            {{"nested_row",
              velox::ROW(
                  {{"nested_nested_row", velox::ROW({{"a", velox::INTEGER()}})},
                   {"b", velox::INTEGER()}})}})},
       {"map",
        velox::MAP(velox::INTEGER(), velox::ROW({{"a", velox::INTEGER()}}))},
       {"nested",
        velox::ARRAY(
            velox::ROW({
                {"a", velox::INTEGER()},
                {"b", velox::MAP(velox::REAL(), velox::VARBINARY())},
            }))},
       {"nested_map_array1",
        velox::MAP(velox::INTEGER(), velox::ARRAY(velox::REAL()))},
       {"nested_map_array2",
        velox::MAP(velox::INTEGER(), velox::ARRAY(velox::INTEGER()))},
       {"dict_map", velox::MAP(velox::INTEGER(), velox::INTEGER())}});

  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);
  const uint32_t seed = FLAGS_writer_tests_seed > 0 ? FLAGS_writer_tests_seed
                                                    : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};
  for (auto parallelismFactor : {0U, 1U, folly::available_concurrency()}) {
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor;
    nimble::VeloxWriterOptions writerOptions;
    writerOptions.enableChunking = true;
    writerOptions.disableSharedStringBuffers = true;
    writerOptions.flushPolicyFactory =
        []() -> std::unique_ptr<nimble::FlushPolicy> {
      return std::make_unique<nimble::ChunkFlushPolicy>(
          nimble::ChunkFlushPolicyConfig{
              .writerMemoryHighThresholdBytes = 200 << 10,
              .writerMemoryLowThresholdBytes = 100 << 10,
              .targetStripeSizeBytes = 100 << 10,
              .estimatedCompressionFactor = 1.7,
          });
    };

    LOG(INFO) << "Parallelism Factor: " << parallelismFactor;
    writerOptions.dictionaryArrayColumns.insert("nested_map_array1");
    writerOptions.dictionaryArrayColumns.insert("nested_map_array2");
    writerOptions.dictionaryArrayColumns.insert("dict_array");
    writerOptions.deduplicatedMapColumns.insert("dict_map");

    if (parallelismFactor > 0) {
      executor =
          std::make_shared<folly::CPUThreadPoolExecutor>(parallelismFactor);
      writerOptions.encodingExecutor = folly::getKeepAliveToken(*executor);
      writerOptions.writeExecutor = folly::getKeepAliveToken(*executor);
    }

    const auto iterations = 20;
    // provide sufficient buffer between min and max chunk size thresholds
    constexpr uint64_t chunkThresholdBuffer =
        sizeof(std::string_view) + sizeof(bool);
    for (auto i = 0; i < iterations; ++i) {
      writerOptions.minStreamChunkRawSize =
          std::uniform_int_distribution<uint64_t>(10, 4096)(rng);
      writerOptions.maxStreamChunkRawSize =
          std::uniform_int_distribution<uint64_t>(
              writerOptions.minStreamChunkRawSize + chunkThresholdBuffer,
              8192)(rng);
      const auto batchSize =
          std::uniform_int_distribution<uint32_t>(10, 400)(rng);
      const auto batchCount = 5;

      std::string file;
      auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
      nimble::VeloxWriter writer(
          type, std::move(writeFile), *leafPool_.get(), writerOptions);
      const auto batches = generateBatches(
          type,
          /*batchCount=*/batchCount,
          /*size=*/batchSize,
          /*seed=*/seed,
          *leafPool_);

      for (const auto& batch : batches) {
        writer.write(batch);
      }
      writer.close();

      velox::InMemoryReadFile readFile(file);
      nimble::VeloxReader reader(&readFile, *leafPool_);
      validateChunkSize(
          reader,
          writerOptions.minStreamChunkRawSize,
          writerOptions.maxStreamChunkRawSize);
    }
  }
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

  nimble::RawSizeContext context;
  nimble::OrderedRanges ranges;
  ranges.add(0, rowCount);
  const uint64_t stringColumnRawSize =
      nimble::getRawSizeFromVector(stringColumn, ranges, context) +
      sizeof(uint64_t) * rowCount;
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
  // the memoryPressureThreshold with the assumption of at least once max chunk
  // being produced for each stream in that batch.
  const uint64_t maxChunkSize = stringColumnRawSize / 2;
  uint64_t memoryPressureThreshold = totalRawSize - (kBatchSize * maxChunkSize);

  std::vector<bool> actualChunkingDecisions;
  nimble::VeloxWriterOptions writerOptions;
  writerOptions.chunkedStreamBatchSize = kBatchSize;
  writerOptions.enableChunking = true;
  writerOptions.disableSharedStringBuffers = true;
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

  nimble::IndexConfig createIndexConfig(
      const std::vector<std::string>& columns,
      bool enforceKeyOrder = true) {
    nimble::IndexConfig config{
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
      config.minChunkRawSize = 32; // 32B - small to force chunking
      config.maxChunkRawSize = 1 << 10; // 1KB - small to force chunking
    } else {
      config.minChunkRawSize = 1ULL << 30; // 1GB - large to prevent chunking
      config.maxChunkRawSize = 1ULL << 30; // 1GB - large to prevent chunking
    }
    return config;
  }

  nimble::VeloxWriterOptions createWriterOptions(
      const nimble::IndexConfig& indexConfig,
      const std::function<std::unique_ptr<nimble::FlushPolicy>()>&
          flushPolicyFactory = nullptr) {
    nimble::VeloxWriterOptions options{
        .indexConfig = std::move(indexConfig),
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
      const auto stripeId =
          tablet.stripeIdentifier(stripeIdx, /*loadIndex=*/true);
      ASSERT_NE(stripeId.indexGroup(), nullptr)
          << "Index group should be available for stripe " << stripeIdx;

      nimble::index::test::StripeIndexGroupTestHelper helper(
          stripeId.indexGroup().get());

      const uint32_t stripeOffsetInGroup = stripeIdx - helper.firstStripe();

      // Load all streams for this stripe
      const uint32_t streamCount = tablet.streamCount(stripeId);
      std::vector<uint32_t> streamIds(streamCount);
      std::iota(streamIds.begin(), streamIds.end(), 0);
      auto streamLoaders = tablet.load(stripeId, streamIds);

      for (uint32_t streamId = 0; streamId < streamLoaders.size(); ++streamId) {
        if (!streamLoaders[streamId]) {
          continue;
        }

        auto streamStats = helper.streamStats(streamId);

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
            auto encoding = nimble::EncodingFactory::decode(
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

          // Decode using SingleChunkDecoder (same pattern as verifyValueIndex)
          nimble::index::test::SingleChunkDecoder chunkDecoder(
              *leafPool_, chunkStreamData);
          auto encodingData = chunkDecoder.decode();
          std::vector<velox::BufferPtr> stringBuffers;
          auto encoding = nimble::EncodingFactory::decode(
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

  // Verifies that the value index correctly maps each key to its row position.
  // For each row in the input batches:
  // 1. Encodes the key using KeyEncoder
  // 2. Looks up the stripe via TabletIndex
  // 3. Gets chunk location within stripe via StripeIndexGroup::lookupChunk
  // 4. Loads the key stream chunk and decodes it
  // 5. Uses seekAtOrAfter to find the exact row within the chunk
  // 6. For duplicate keys, verifies the found row id matches the earliest row
  //    with the same key value
  void verifyValueIndex(
      const nimble::TabletReader& tablet,
      velox::ReadFile* file,
      const velox::RowTypePtr& type,
      const std::vector<velox::RowVectorPtr>& batches,
      const std::vector<std::string>& indexColumns) {
    const auto* index = tablet.index();
    ASSERT_NE(index, nullptr) << "Index must exist";

    // Pre-compute stripe start row offsets
    std::vector<uint64_t> stripeStartRows;
    stripeStartRows.reserve(tablet.stripeCount());
    uint64_t startRow = 0;
    for (uint32_t i = 0; i < tablet.stripeCount(); ++i) {
      stripeStartRows.push_back(startRow);
      startRow += tablet.stripeRowCount(i);
    }

    // Create sort orders for KeyEncoder (all ascending, nulls last)
    std::vector<velox::core::SortOrder> sortOrders(
        indexColumns.size(),
        velox::core::SortOrder(true, false)); // ascending, nulls last

    // Create KeyEncoder for encoding row keys
    auto keyEncoder = velox::serializer::KeyEncoder::create(
        indexColumns, type, sortOrders, leafPool_.get());

    // Pre-encode all keys and build a map from encoded key to earliest row id.
    // This handles duplicate keys where seekAtOrAfter returns the first
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
    // encoding
    std::unordered_map<uint64_t, std::unique_ptr<nimble::Encoding>>
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

        // Look up the stripe for this key
        auto stripeLocation = index->lookup(encodedKeyView);
        ASSERT_TRUE(stripeLocation.has_value())
            << "Key at row " << currentRowId << " should be found in index";

        uint32_t stripeIndex = stripeLocation->stripeIndex;
        ASSERT_LT(stripeIndex, tablet.stripeCount())
            << "Stripe index out of range";

        // Get stripe identifier with index group loaded
        const auto stripeId =
            tablet.stripeIdentifier(stripeIndex, /*loadIndex=*/true);
        ASSERT_NE(stripeId.indexGroup(), nullptr)
            << "Index group should be available for stripe " << stripeIndex;

        // Look up chunk by encoded key to get chunk location within stripe
        const auto chunkLocation =
            stripeId.indexGroup()->lookupChunk(stripeIndex, encodedKeyView);
        ASSERT_TRUE(chunkLocation.has_value())
            << "Key at row " << currentRowId << " should be found in stripe "
            << stripeIndex;

        // Get key stream region for this stripe
        const auto keyStreamRegion = stripeId.indexGroup()->keyStreamRegion(
            stripeIndex, tablet.stripeOffset(stripeIndex));

        // Cache key: chunk file offset (unique across all stripes)
        const uint64_t chunkFileOffset =
            keyStreamRegion.offset + chunkLocation->streamOffset;

        // Load and decode key chunk if not cached
        if (keyStreamCache.find(chunkFileOffset) == keyStreamCache.end()) {
          // Get chunk length from the index. The index stores chunk offsets
          // in key_stream_chunk_offsets, so we can compute the length by
          // looking at the next chunk offset (or stream end for the last
          // chunk). Use helper to get chunk length from StripeIndexGroup
          // internals.
          nimble::index::test::StripeIndexGroupTestHelper helper(
              stripeId.indexGroup().get());
          const uint32_t chunkLength = helper.keyChunkLength(
              stripeIndex, chunkLocation->streamOffset, keyStreamRegion.length);

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

          keyStreamCache[chunkFileOffset] = nimble::EncodingFactory::decode(
              *leafPool_, encodingData, [&](uint32_t totalLength) {
                auto& buf = stringBuffers.emplace_back(
                    velox::AlignedBuffer::allocate<char>(
                        totalLength, leafPool_.get()));
                return buf->asMutable<void>();
              });
        }

        auto* keyEncoding = keyStreamCache[chunkFileOffset].get();
        ASSERT_NE(keyEncoding, nullptr);

        // Reset encoding and seek to find the row within the chunk
        keyEncoding->reset();

        // Use seekAtOrAfter to find the exact row position within the remaining
        // rows
        auto seekResult = keyEncoding->seekAtOrAfter(&encodedKeyView);
        ASSERT_TRUE(seekResult.has_value())
            << "seekAtOrAfter should find key at row " << currentRowId;

        // Calculate the actual file row id
        // seekAtOrAfter returns the offset from current position where the key
        // was found
        uint64_t fileRowId = stripeStartRows[stripeIndex] +
            chunkLocation->rowOffset + seekResult.value();

        // For duplicate keys, seekAtOrAfter returns the first occurrence.
        // Verify that the found row id matches the earliest row with the same
        // key.
        uint64_t expectedFileRowId = keyToEarliestRowId.at(encodedKey);
        EXPECT_EQ(fileRowId, expectedFileRowId)
            << "Row " << currentRowId << " key lookup mismatch: "
            << "expected earliest row " << expectedFileRowId << ", got "
            << fileRowId << " (stripe " << stripeIndex << ", stripe start row "
            << stripeStartRows[stripeIndex] << ", chunk row offset "
            << chunkLocation->rowOffset << ", seek offset "
            << seekResult.value() << ")";

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

  nimble::IndexConfig indexConfig = createIndexConfig({"key_col"});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  constexpr int kNumBatches = 5;
  constexpr int kBatchSize = 100;
  nimble::VeloxWriter writer(
      type,
      std::move(writeFile),
      *rootPool_,
      createWriterOptions(indexConfig, []() {
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
  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);

  const auto& tablet = reader.tabletReader();

  // Verify each batch triggered exactly one stripe
  EXPECT_EQ(tablet.stripeCount(), kNumBatches)
      << "Each batch should trigger exactly one stripe";

  // Verify all stripes are in the same stripe group (index 0)
  // Default metadataFlushThreshold is large, so all stripes stay in one group
  for (uint32_t i = 0; i < tablet.stripeCount(); ++i) {
    auto stripeId = tablet.stripeIdentifier(i);
    EXPECT_EQ(stripeId.stripeGroup()->index(), 0)
        << "Stripe " << i << " should be in stripe group 0";
  }

  // Verify index section exists
  EXPECT_TRUE(tablet.hasOptionalSection(std::string(nimble::kIndexSection)));

  // Verify index is available
  const auto* index = tablet.index();
  ASSERT_NE(index, nullptr);

  // Verify index columns
  EXPECT_EQ(index->indexColumns().size(), 1);
  EXPECT_EQ(index->indexColumns()[0], "key_col");

  // Verify stripe keys are monotonically increasing
  for (uint32_t i = 1; i < index->numStripes(); ++i) {
    EXPECT_LE(index->stripeKey(i - 1), index->stripeKey(i))
        << "Stripe keys should be in non-descending order";
  }

  // Verify lookups work correctly
  // Keys should be found in the correct stripes
  EXPECT_TRUE(index->lookup(index->minKey()).has_value());
  EXPECT_TRUE(index->lookup(index->maxKey()).has_value());

  // Keys outside range should not be found
  EXPECT_FALSE(index->lookup("").has_value());

  // Read back all data and verify row-by-row match with written batches
  verifyFileData(file, type, batches, kBatchSize);

  // Verify position index chunk row counts
  verifyPositionIndex(tablet);

  // Verify value index maps each key to correct row position
  verifyValueIndex(tablet, &readFile, type, batches, {"key_col"});
}

TEST_P(VeloxWriterIndexTest, multipleGroups) {
  // Test writing a file with index and multiple stripe groups.
  // Uses small batches with flush-per-batch policy to create multiple stripes.
  auto type = defaultType();

  nimble::IndexConfig indexConfig = createIndexConfig({"key_col"});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  constexpr int kNumBatches = 5;
  constexpr int kBatchSize = 100;
  nimble::VeloxWriter writer(
      type,
      std::move(writeFile),
      *rootPool_,
      createWriterOptions(indexConfig, []() {
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
  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);

  const auto& tablet = reader.tabletReader();

  // Verify each batch triggered exactly one stripe
  EXPECT_EQ(tablet.stripeCount(), kNumBatches)
      << "Each batch should trigger exactly one stripe";

  // Note: Without controlling metadataFlushThreshold from VeloxWriterOptions,
  // all stripes end up in the same group (default threshold is 8MB).
  // This test verifies the default behavior where all stripes share group 0.
  for (uint32_t i = 0; i < tablet.stripeCount(); ++i) {
    auto stripeId = tablet.stripeIdentifier(i);
    EXPECT_EQ(stripeId.stripeGroup()->index(), 0)
        << "Stripe " << i << " should be in stripe group 0";
  }

  // Verify index section exists
  EXPECT_TRUE(tablet.hasOptionalSection(std::string(nimble::kIndexSection)));

  // Verify index is available
  const auto* index = tablet.index();
  ASSERT_NE(index, nullptr);

  // Verify index columns
  EXPECT_EQ(index->indexColumns().size(), 1);
  EXPECT_EQ(index->indexColumns()[0], "key_col");

  // Verify stripe keys are monotonically increasing
  for (uint32_t i = 1; i < index->numStripes(); ++i) {
    EXPECT_LE(index->stripeKey(i - 1), index->stripeKey(i))
        << "Stripe keys should be in non-descending order";
  }

  // Verify we can look up stripes by key
  // Get the first stripe's key and verify it maps to stripe 0
  auto firstLocation = index->lookup(index->minKey());
  ASSERT_TRUE(firstLocation.has_value());
  EXPECT_EQ(firstLocation->stripeIndex, 0);

  // Get the last stripe's key and verify it maps to the last stripe
  auto lastLocation = index->lookup(index->maxKey());
  ASSERT_TRUE(lastLocation.has_value());
  EXPECT_EQ(lastLocation->stripeIndex, index->numStripes() - 1);

  // Verify stripeIdentifier with loadIndex returns index group
  for (uint32_t i = 0; i < tablet.stripeCount(); ++i) {
    auto stripeId = tablet.stripeIdentifier(i, /*loadIndex=*/true);
    EXPECT_NE(stripeId.stripeGroup(), nullptr);
    EXPECT_NE(stripeId.indexGroup(), nullptr)
        << "Index group should be available for stripe " << i;
  }

  // Verify stripeIdentifier without loadIndex does not return index group
  {
    auto stripeId = tablet.stripeIdentifier(0, /*loadIndex=*/false);
    EXPECT_NE(stripeId.stripeGroup(), nullptr);
    EXPECT_EQ(stripeId.indexGroup(), nullptr);
  }

  // Read back all data and verify row-by-row match with written batches
  verifyFileData(file, type, batches);

  // Verify position index chunk row counts
  verifyPositionIndex(tablet);

  // Verify value index maps each key to correct row position
  verifyValueIndex(tablet, &readFile, type, batches, {"key_col"});
}

TEST_P(VeloxWriterIndexTest, multipleIndexColumns) {
  // Test index with multiple index columns (composite key).
  auto type = velox::ROW({
      {"key1", velox::BIGINT()},
      {"key2", velox::INTEGER()},
      {"data_col", velox::VARCHAR()},
  });

  nimble::IndexConfig indexConfig = createIndexConfig({"key1", "key2"});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(
      type, std::move(writeFile), *rootPool_, createWriterOptions(indexConfig));

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
  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);

  const auto& tablet = reader.tabletReader();
  const auto* index = tablet.index();
  ASSERT_NE(index, nullptr);

  // Verify both columns are indexed
  EXPECT_EQ(index->indexColumns().size(), 2);
  EXPECT_EQ(index->indexColumns()[0], "key1");
  EXPECT_EQ(index->indexColumns()[1], "key2");

  // Verify stripe keys exist
  EXPECT_FALSE(index->minKey().empty());
  EXPECT_FALSE(index->maxKey().empty());

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
  verifyPositionIndex(tablet);

  verifyValueIndex(tablet, &readFile, type, batches, {"key1", "key2"});
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

      nimble::IndexConfig indexConfig{
          .columns = {"key_col"},
          .enforceKeyOrder = enforceKeyOrder,
          .noDuplicateKey = enforceKeyOrder,
      };

      std::string file;
      auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

      nimble::VeloxWriter writer(
          type, std::move(writeFile), *rootPool_, {.indexConfig = indexConfig});

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
      // - DuplicateKeys: keys starting with same value as previous batch's last
      // key
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

  nimble::IndexConfig indexConfig = createIndexConfig({"key_col"});

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
      createWriterOptions(indexConfig, []() {
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
  velox::InMemoryReadFile readFile(file);

  // Verify FileLayout for non-empty file with index using FileLayout::create()
  {
    auto layout = nimble::FileLayout::create(&readFile, leafPool_.get());
    EXPECT_EQ(layout.fileSize, file.size());
    EXPECT_EQ(layout.stripesInfo.size(), kNumBatches);
    EXPECT_EQ(layout.stripeGroups.size(), 1);
    EXPECT_EQ(layout.stripeGroups.size(), 1);
    // With index enabled, should have index groups
    EXPECT_EQ(layout.indexGroups.size(), 1);
    // Stripes metadata should be valid
    EXPECT_GT(layout.stripes.size(), 0);
    EXPECT_LT(layout.stripes.offset(), layout.footer.offset());
    // Index group should be before stripes section
    EXPECT_LT(layout.indexGroups[0].offset(), layout.stripes.offset());
    // Per-stripe info
    EXPECT_EQ(layout.stripesInfo.size(), kNumBatches);
    for (size_t i = 0; i < layout.stripesInfo.size(); ++i) {
      EXPECT_EQ(layout.stripesInfo[i].stripeGroupIndex, 0);
      EXPECT_GT(layout.stripesInfo[i].size, 0);
    }
  }
  nimble::VeloxReader reader(&readFile, *leafPool_);

  const auto& tablet = reader.tabletReader();

  // Verify index exists
  const auto* index = tablet.index();
  ASSERT_NE(index, nullptr);
  EXPECT_EQ(index->indexColumns().size(), 1);
  EXPECT_EQ(index->indexColumns()[0], "key_col");

  // Verify stripe keys are monotonically increasing (even with duplicates)
  for (uint32_t i = 1; i < index->numStripes(); ++i) {
    EXPECT_LE(index->stripeKey(i - 1), index->stripeKey(i))
        << "Stripe keys should be in non-descending order";
  }

  // Verify we can look up stripes by key
  auto firstLocation = index->lookup(index->minKey());
  ASSERT_TRUE(firstLocation.has_value());
  EXPECT_EQ(firstLocation->stripeIndex, 0);

  auto lastLocation = index->lookup(index->maxKey());
  ASSERT_TRUE(lastLocation.has_value());
  EXPECT_EQ(lastLocation->stripeIndex, index->numStripes() - 1);

  // Read back all data and verify row-by-row match with written batches
  verifyFileData(file, type, batches);

  // Verify position index chunk row counts
  verifyPositionIndex(tablet);

  // Verify value index maps each key to correct row position
  // With duplicate keys, lookup should find the first occurrence
  verifyValueIndex(tablet, &readFile, type, batches, {"key_col"});
}

TEST_P(VeloxWriterIndexTest, chunking) {
  // Test index with chunking enabled
  auto type = defaultType();

  nimble::IndexConfig indexConfig = createIndexConfig({"key_col"});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  constexpr int kNumBatches = 50;
  constexpr int kBatchSize = 500;
  auto options = createWriterOptions(indexConfig, []() {
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
  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);

  const auto& tablet = reader.tabletReader();

  // Verify index exists and works
  const auto* index = tablet.index();
  ASSERT_NE(index, nullptr);
  EXPECT_EQ(index->indexColumns().size(), 1);
  EXPECT_EQ(index->indexColumns()[0], "key_col");

  // Verify stripe keys are in order
  for (uint32_t i = 1; i < index->numStripes(); ++i) {
    EXPECT_LE(index->stripeKey(i - 1), index->stripeKey(i));
  }

  // Verify index group can be loaded for each stripe
  for (uint32_t i = 0; i < tablet.stripeCount(); ++i) {
    auto stripeId = tablet.stripeIdentifier(i, /*loadIndex=*/true);
    ASSERT_NE(stripeId.indexGroup(), nullptr);
  }

  // Read back all data and verify row-by-row match with written batches
  verifyFileData(file, type, batches);

  // Verify position index chunk row counts
  verifyPositionIndex(tablet);

  // Verify value index maps each key to correct row position
  verifyValueIndex(tablet, &readFile, type, batches, {"key_col"});
}

TEST_P(VeloxWriterIndexTest, streamDeduplication) {
  // Test that streams with identical content are deduplicated by the tablet
  // writer. We create batches where string_col1 and string_col2 reference the
  // same underlying vector, which should result in identical stream content
  // that gets deduplicated.
  auto type = defaultType();

  nimble::IndexConfig indexConfig = createIndexConfig({"key_col"});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  constexpr int kNumBatches = 5;
  constexpr int kBatchSize = 100;
  constexpr uint32_t kSeed = 42;

  nimble::VeloxWriter writer(
      type,
      std::move(writeFile),
      *rootPool_,
      createWriterOptions(indexConfig, []() {
        // Flush after every batch to create multiple stripes
        return std::make_unique<nimble::LambdaFlushPolicy>(
            [](auto) { return true; }, [](auto) { return false; });
      }));

  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  // Configure fuzzer for generating non-string columns
  velox::VectorFuzzer::Options fuzzerOpts;
  fuzzerOpts.vectorSize = kBatchSize;
  fuzzerOpts.nullRatio = 0.1;
  fuzzerOpts.containerLength = 5;
  fuzzerOpts.stringLength = 20;
  fuzzerOpts.containerVariableLength = true;
  velox::VectorFuzzer fuzzer(fuzzerOpts, leafPool_.get(), kSeed);

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

    // Create the shared string vector that will be used for both string columns
    auto sharedStringVector = fuzzer.fuzz(velox::VARCHAR());

    // Build children: key column, fuzzed scalar columns, shared string columns,
    // then fuzzed complex columns
    std::vector<velox::VectorPtr> children;
    // Column 0: key_col
    children.push_back(vectorMaker.flatVector<int64_t>(keyValues));
    // Column 1: int_col
    children.push_back(fuzzer.fuzz(type->childAt(1)));
    // Column 2: double_col
    children.push_back(fuzzer.fuzz(type->childAt(2)));
    // Column 3: string_col1 - use the shared string vector
    children.push_back(sharedStringVector);
    // Column 4: string_col2 - use the SAME shared string vector
    children.push_back(sharedStringVector);
    // Column 5: bool_col
    children.push_back(fuzzer.fuzz(type->childAt(5)));
    // Columns 6-8: complex types
    for (size_t colIdx = 6; colIdx < type->size(); ++colIdx) {
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
  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(&readFile, *leafPool_);

  const auto& tablet = reader.tabletReader();

  // Verify index exists
  const auto* index = tablet.index();
  ASSERT_NE(index, nullptr);
  EXPECT_EQ(index->indexColumns().size(), 1);
  EXPECT_EQ(index->indexColumns()[0], "key_col");

  // Verify stripe keys are monotonically increasing
  for (uint32_t i = 1; i < index->numStripes(); ++i) {
    EXPECT_LE(index->stripeKey(i - 1), index->stripeKey(i))
        << "Stripe keys should be in non-descending order";
  }

  // Verify we can look up stripes by key
  auto firstLocation = index->lookup(index->minKey());
  ASSERT_TRUE(firstLocation.has_value());
  EXPECT_EQ(firstLocation->stripeIndex, 0);

  auto lastLocation = index->lookup(index->maxKey());
  ASSERT_TRUE(lastLocation.has_value());
  EXPECT_EQ(lastLocation->stripeIndex, index->numStripes() - 1);

  // Read back all data and verify row-by-row match with written batches
  // Note: When reading back, string_col1 and string_col2 will have identical
  // content since they were written from the same source vector
  verifyFileData(file, type, batches);

  // Verify position index chunk row counts
  verifyPositionIndex(tablet);

  // Verify value index maps each key to correct row position
  verifyValueIndex(tablet, &readFile, type, batches, {"key_col"});
}

// Test that custom prefixRestartInterval in IndexConfig flows through
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

    nimble::IndexConfig indexConfig{
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
        type, std::move(writeFile), *rootPool_, {.indexConfig = indexConfig});

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
    velox::InMemoryReadFile readFile(file);
    auto tablet = nimble::TabletReader::create(&readFile, leafPool_.get(), {});

    const auto* index = tablet->index();
    ASSERT_NE(index, nullptr) << "Index must exist";

    // Get first stripe's index group
    const auto stripeId = tablet->stripeIdentifier(0, /*loadIndex=*/true);
    ASSERT_NE(stripeId.indexGroup(), nullptr)
        << "Index group should be available";

    // Get key stream region
    const auto keyStreamRegion =
        stripeId.indexGroup()->keyStreamRegion(0, tablet->stripeOffset(0));

    // Load the key stream data
    velox::common::Region region{
        keyStreamRegion.offset, keyStreamRegion.length, "keyStream"};
    folly::IOBuf iobuf;
    readFile.preadv({&region, 1}, {&iobuf, 1});

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
    auto keyEncoding = nimble::EncodingFactory::decode(
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

} // namespace facebook
