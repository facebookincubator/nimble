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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingLayoutCapture.h"
#include "dwio/nimble/encodings/EncodingUtils.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/velox/ChunkedStream.h"
#include "dwio/nimble/velox/EncodingLayoutTree.h"
#include "dwio/nimble/velox/FlushPolicy.h"
#include "dwio/nimble/velox/SchemaSerialization.h"
#include "dwio/nimble/velox/StatsGenerated.h"
#include "dwio/nimble/velox/VeloxReader.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "folly/FileUtil.h"
#include "folly/Random.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook {

DEFINE_uint32(
    writer_tests_seed,
    0,
    "If provided, this seed will be used when executing tests. "
    "Otherwise, a random seed will be used.");

class VeloxWriterTests : public ::testing::Test {
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

TEST_F(VeloxWriterTests, EmptyFile) {
  auto type = velox::ROW({{"simple", velox::INTEGER()}});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(*rootPool_, type, std::move(writeFile), {});
  writer.close();

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(*leafPool_, &readFile);

  velox::VectorPtr result;
  ASSERT_FALSE(reader.next(1, result));
}

TEST_F(VeloxWriterTests, ExceptionOnClose) {
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
      *rootPool_,
      vector->type(),
      std::move(writeFile),
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

TEST_F(VeloxWriterTests, EmptyFileNoSchema) {
  const uint32_t batchSize = 10;
  auto type = velox::ROW({{"simple", velox::INTEGER()}});
  nimble::VeloxWriterOptions writerOptions;

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  nimble::VeloxWriter writer(
      *rootPool_, type, std::move(writeFile), std::move(writerOptions));
  writer.close();

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(*leafPool_, &readFile);

  velox::VectorPtr result;
  ASSERT_FALSE(reader.next(batchSize, result));
}

TEST_F(VeloxWriterTests, RootHasNulls) {
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
      *rootPool_, vector->type(), std::move(writeFile), {});
  writer.write(vector);
  writer.close();

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(*leafPool_, &readFile);

  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(batchSize, result));
  ASSERT_EQ(result->size(), batchSize);
  for (auto i = 0; i < batchSize; ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, i));
  }
}

TEST_F(
    VeloxWriterTests,
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
      *rootPool_,
      vector->type(),
      std::move(writeFile),
      {.flatMapColumns = {"flatmap"},
       .featureReordering =
           std::vector<std::tuple<size_t, std::vector<int64_t>>>{
               {0, {1, 2}}, {1, {3, 4}}}});
  writer.write(vector);
  writer.close();
}

TEST_F(VeloxWriterTests, DuplicateFlatmapKey) {
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
        *rootPool_,
        vec->type(),
        std::move(writeFile),
        {.flatMapColumns = {"flatmap"}});
    EXPECT_THROW(writer.write(vec), nimble::NimbleUserError);
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
        *rootPool_,
        vec->type(),
        std::move(writeFile),
        {.flatMapColumns = {"flatmap"}});
    EXPECT_THROW(writer.write(vec), nimble::NimbleUserError);
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
    const uint64_t minStreamChunkRawSize) {
  const auto& tablet = reader.tabletReader();
  auto& pool = reader.memoryPool();

  const uint32_t stripeCount = tablet.stripeCount();
  uint32_t maxChunkCount = 0;
  uint32_t minChunkCount = std::numeric_limits<uint32_t>::max();

  for (uint32_t stripeIndex = 0; stripeIndex < stripeCount; ++stripeIndex) {
    const auto stripeIdentifier = tablet.getStripeIdentifier(stripeIndex);
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
      while (chunkedStream.hasNext()) {
        ++currentStreamChunkCount;
        const auto chunk = chunkedStream.nextChunk();
        // Validate min chunk size when not last chunk
        if (chunkedStream.hasNext()) {
          const auto encoding = nimble::EncodingFactory::decode(pool, chunk);
          const auto rowCount = encoding->rowCount();
          const auto dataType = encoding->dataType();
          const uint64_t chunkRawDataSize =
              facebook::nimble::detail::dataTypeSize(dataType) * rowCount;
          EXPECT_GE(chunkRawDataSize, minStreamChunkRawSize)
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
    : public VeloxWriterTests,
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
      *rootPool_, type, std::move(writeFile), std::move(writerOptions));
  auto batches =
      generateBatches(type, GetParam().batchCount, 4000, 20221110, *leafPool_);

  for (const auto& batch : batches) {
    writer.write(batch);
  }
  writer.close();

  velox::InMemoryReadFile readFile(file);
  auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(type);
  nimble::VeloxReader reader(*leafPool_, &readFile, std::move(selector));

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

TEST_F(VeloxWriterTests, MemoryReclaimPath) {
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
      *writerPool, type, std::move(writeFile), std::move(writerOptions));
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

TEST_F(VeloxWriterTests, FlushHugeStrings) {
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
      *rootPool_,
      vector->type(),
      std::move(writeFile),
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
  nimble::VeloxReader reader(*leafPool_, &readFile, std::move(selector));

  EXPECT_EQ(3, reader.tabletReader().stripeCount());
}

TEST_F(VeloxWriterTests, EncodingLayout) {
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
                       nimble::CompressionType::Uncompressed,
                       {
                           nimble::EncodingLayout{
                               nimble::EncodingType::FixedBitWidth,
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
                            nimble::CompressionType::Uncompressed,
                            {
                                std::nullopt,
                                nimble::EncodingLayout{
                                    nimble::EncodingType::Trivial,
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
                               nimble::CompressionType::Uncompressed,
                               {
                                   nimble::EncodingLayout{
                                       nimble::EncodingType::Trivial,
                                       nimble::CompressionType::Uncompressed},
                                   nimble::EncodingLayout{
                                       nimble::EncodingType::FixedBitWidth,
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
      *rootPool_,
      vector->type(),
      std::move(writeFile),
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
    nimble::TabletReader tablet{*leafPool_, &readFile};
    auto section =
        tablet.loadOptionalSection(std::string(nimble::kSchemaSection));
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

    for (auto i = 0; i < tablet.stripeCount(); ++i) {
      auto stripeIdentifier = tablet.getStripeIdentifier(i);
      std::vector<uint32_t> identifiers{
          mapNode.lengthsDescriptor().offset(),
          mapValuesNode.scalarDescriptor().offset(),
          flatMapKey1Node.scalarDescriptor().offset(),
          flatMapKey2Node.scalarDescriptor().offset()};
      auto streams = tablet.load(stripeIdentifier, identifiers);
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

TEST_F(VeloxWriterTests, EncodingLayoutSchemaMismatch) {
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
                          nimble::CompressionType::Uncompressed,
                          {
                              nimble::EncodingLayout{
                                  nimble::EncodingType::FixedBitWidth,
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
        *rootPool_,
        vector->type(),
        std::move(writeFile),
        {
            .encodingLayoutTree = std::move(expected),
            .compressionOptions = {.compressionAcceptRatio = 100},
        });
    FAIL() << "Writer should fail on incompatible encoding layout node";
  } catch (const nimble::NimbleUserError& e) {
    EXPECT_NE(
        std::string(e.what()).find(
            "Incompatible encoding layout node. Expecting map node"),
        std::string::npos);
  }
}

TEST_F(VeloxWriterTests, EncodingLayoutSchemaEvolutionMapToFlatmap) {
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
                       nimble::CompressionType::Uncompressed,
                       {
                           nimble::EncodingLayout{
                               nimble::EncodingType::FixedBitWidth,
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
                            nimble::CompressionType::Uncompressed,
                            {
                                std::nullopt,
                                nimble::EncodingLayout{
                                    nimble::EncodingType::Trivial,
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
      *rootPool_,
      vector->type(),
      std::move(writeFile),
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

TEST_F(VeloxWriterTests, EncodingLayoutSchemaEvolutionFlamapToMap) {
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
                               nimble::CompressionType::Uncompressed,
                               {
                                   nimble::EncodingLayout{
                                       nimble::EncodingType::Trivial,
                                       nimble::CompressionType::Uncompressed},
                                   nimble::EncodingLayout{
                                       nimble::EncodingType::FixedBitWidth,
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
      *rootPool_,
      vector->type(),
      std::move(writeFile),
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

TEST_F(VeloxWriterTests, EncodingLayoutSchemaEvolutionExpandingRow) {
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
      *rootPool_,
      vector->type(),
      std::move(writeFile),
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

TEST_F(VeloxWriterTests, CombineMultipleLayersOfDictionaries) {
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
      *rootPool_,
      ROW({"c0"}, {MAP(VARCHAR(), ARRAY(BIGINT()))}),
      std::move(writeFile),
      std::move(options));
  writer.write(vector);
  writer.close();
  InMemoryReadFile readFile(file);
  nimble::VeloxReadParams params;
  params.readFlatMapFieldAsStruct = {"c0"};
  params.flatMapFeatureSelector["c0"].features = {"c0"};
  nimble::VeloxReader reader(*leafPool_, &readFile, nullptr, std::move(params));
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
      rootPool,
      type,
      std::move(writeFile),
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

  nimble::TabletReader tablet{
      *leafPool, std::make_unique<velox::InMemoryReadFile>(file)};
  verifier(tablet);

  nimble::VeloxReader reader(
      *leafPool, std::make_shared<velox::InMemoryReadFile>(file));
  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(expected->size(), result));
  ASSERT_EQ(expected->size(), result->size());
  for (auto i = 0; i < expected->size(); ++i) {
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i));
  }
  ASSERT_FALSE(reader.next(1, result));

  validateChunkSize(reader, minStreamChunkRawSize);
}

TEST_F(VeloxWriterTests, ChunkedStreamsRowAllNullsNoChunks) {
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
      {{vector, false}, {vector, false}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, ChunkedStreamsRowAllNullsWithChunksMinSizeBig) {
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
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, ChunkedStreamsRowAllNullsWithChunksMinSizeZero) {
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
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, ChunkedStreamsRowSomeNullsNoChunks) {
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
      {{nullsVector, false}, {nonNullsVector, false}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, ChunkedStreamsRowSomeNullsWithChunksMinSizeBig) {
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
      {{nullsVector, true}, {nonNullsVector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, ChunkedStreamsRowSomeNullsWithChunksMinSizeZero) {
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
      {{nullsVector, true}, {nonNullsVector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, ChunkedStreamsRowNoNullsNoChunks) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({1, 2, 3}),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 0,
      {{vector, false}, {vector, false}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, ChunkedStreamsRowNoNullsWithChunksMinSizeBig) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({1, 2, 3}),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 1024,
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, ChunkedStreamsRowNoNullsWithChunksMinSizeZero) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto vector = vectorMaker.rowVector(
      {"c1"},
      {
          vectorMaker.flatVector<int32_t>({1, 2, 3}),
      });

  testChunks(
      *rootPool_,
      /* minStreamChunkRawSize */ 0,
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, ChunkedStreamsChildAllNullsNoChunks) {
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
      {{vector, false}, {vector, false}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // When all rows are not null, the nulls stream is omitted.
        // When all values are null, the values stream is omitted.
        // Since these are the last two stream, they are optimized away.
        ASSERT_EQ(0, tablet.streamCount(stripeIdentifier));
      });
}

TEST_F(VeloxWriterTests, ChunkedStreamsChildAllNullsWithChunksMinSizeBig) {
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
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // When all rows are not null, the nulls stream is omitted.
        // When all values are null, the values stream is omitted.
        // Since these are the last two stream, they are optimized away.
        ASSERT_EQ(0, tablet.streamCount(stripeIdentifier));
      });
}

TEST_F(VeloxWriterTests, ChunkedStreamsChildAllNullsWithChunksMinSizeZero) {
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
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
        ASSERT_EQ(1, tablet.stripeCount());

        // When all rows are not null, the nulls stream is omitted.
        // When all values are null, the values stream is omitted.
        // Since these are the last two stream, they are optimized away.
        ASSERT_EQ(0, tablet.streamCount(stripeIdentifier));
      });
}

TEST_F(VeloxWriterTests, ChunkedStreamsFlatmapAllNullsNoChunks) {
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
      {{vector, false}, {vector, false}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, ChunkedStreamsFlatmapAllNullsWithChunksMinSizeBig) {
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
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, ChunkedStreamsFlatmapAllNullsWithChunksMinSizeZero) {
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
      {{vector, true}, {vector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, ChunkedStreamsFlatmapSomeNullsNoChunks) {
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
      {{nullsVector, false}, {nonNullsVector, false}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, ChunkedStreamsFlatmapSomeNullsWithChunksMinSizeBig) {
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
      {{nullsVector, true}, {nonNullsVector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, ChunkedStreamsFlatmapSomeNullsWithChunksMinSizeZero) {
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
      {{nullsVector, true}, {nonNullsVector, true}},
      [&](const auto& tablet) {
        auto stripeIdentifier = tablet.getStripeIdentifier(0);
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

TEST_F(VeloxWriterTests, RawSizeWritten) {
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

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      *rootPool_, vector->type(), std::move(writeFile), {});
  writer.write(vector);
  writer.close();

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(*leafPool_, &readFile);

  std::vector<std::string> preloadedOptionalSections = {
      std::string(facebook::nimble::kStatsSection)};
  auto tablet = std::make_shared<facebook::nimble::TabletReader>(
      *leafPool_, &readFile, preloadedOptionalSections);
  auto statsSection =
      tablet.get()->loadOptionalSection(preloadedOptionalSections[0]);
  ASSERT_TRUE(statsSection.has_value());

  auto rawSize = flatbuffers::GetRoot<facebook::nimble::serialization::Stats>(
                     statsSection->content().data())
                     ->raw_size();
  ASSERT_EQ(expectedRawSize, rawSize);
}

struct ChunkFlushPolicyTestCase {
  const size_t batchCount{20};
  const bool enableChunking{true};
  const uint64_t targetStripeSizeBytes{250 << 10};
  const uint64_t writerMemoryHighThresholdBytes{80 << 10};
  const uint64_t writerMemoryLowThresholdBytes{75 << 10};
  const double estimatedCompressionFactor{1.3};
  const uint32_t minStreamChunkRawSize{100};
  const uint32_t expectedStripeCount{0};
  const uint32_t expectedMaxChunkCount{0};
  const uint32_t expectedMinChunkCount{0};
  const uint32_t chunkedStreamBatchSize{2};
};

class ChunkFlushPolicyTest
    : public VeloxWriterTests,
      public ::testing::WithParamInterface<ChunkFlushPolicyTestCase> {};

TEST_P(ChunkFlushPolicyTest, ChunkFlushPolicyIntegration) {
  const auto type = velox::ROW(
      {{"BIGINT", velox::BIGINT()}, {"SMALLINT", velox::SMALLINT()}});
  nimble::VeloxWriterOptions writerOptions{
      .minStreamChunkRawSize = GetParam().minStreamChunkRawSize,
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
      *rootPool_, type, std::move(writeFile), std::move(writerOptions));
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
  nimble::VeloxReader reader(*leafPool_, &readFile);
  ChunkSizeResults result =
      validateChunkSize(reader, GetParam().minStreamChunkRawSize);

  EXPECT_EQ(GetParam().expectedStripeCount, result.stripeCount);
  EXPECT_EQ(GetParam().expectedMaxChunkCount, result.maxChunkCount);
  EXPECT_EQ(GetParam().expectedMinChunkCount, result.minChunkCount);
}

TEST_F(VeloxWriterTests, FuzzComplex) {
  auto type = velox::ROW(
      {{"array", velox::ARRAY(velox::REAL())},
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
                {"b", velox::MAP(velox::REAL(), velox::REAL())},
            }))},
       {"nested_map_array1",
        velox::MAP(velox::INTEGER(), velox::ARRAY(velox::REAL()))},
       {"nested_map_array2",
        velox::MAP(velox::INTEGER(), velox::ARRAY(velox::INTEGER()))},
       {"dict_map", velox::MAP(velox::INTEGER(), velox::INTEGER())}});
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);
  uint32_t seed = FLAGS_writer_tests_seed > 0 ? FLAGS_writer_tests_seed
                                              : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};
  for (auto parallelismFactor : {0U, 1U, std::thread::hardware_concurrency()}) {
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor;
    nimble::VeloxWriterOptions writerOptions;
    writerOptions.enableChunking = true;
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
    for (auto i = 0; i < iterations; ++i) {
      writerOptions.minStreamChunkRawSize =
          std::uniform_int_distribution<uint32_t>(10, 4096)(rng);
      const auto batchSize =
          std::uniform_int_distribution<uint32_t>(10, 400)(rng);
      const auto batchCount = 5;

      std::string file;
      auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
      nimble::VeloxWriter writer(
          *leafPool_.get(), type, std::move(writeFile), writerOptions);
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
      nimble::VeloxReader reader(*leafPool_, &readFile);
      validateChunkSize(reader, writerOptions.minStreamChunkRawSize);
    }
  }
}

TEST_F(VeloxWriterTests, BatchedChunkingRelievesMemoryPressure) {
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
      sizeof(std::string_view) * rowCount;
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

  // We will return true twice and false once
  const std::vector<bool> expectedChunkingDecisions{true, true, false};
  std::vector<bool> actualChunkingDecisions;

  // We will be chunking the large streams in the first two batches. 8 string
  // streams in total. We set the expected rawSize after chunking these two
  // batches as our memory threshold.
  const uint64_t memoryPressureThreshold =
      totalRawSize - (2 * kBatchSize * stringColumnRawSize);

  nimble::VeloxWriterOptions writerOptions;
  writerOptions.chunkedStreamBatchSize = kBatchSize;
  writerOptions.enableChunking = true;
  writerOptions.minStreamChunkRawSize = intColumnRawSize / 2;
  writerOptions.flushPolicyFactory =
      [&]() -> std::unique_ptr<nimble::FlushPolicy> {
    return std::make_unique<nimble::LambdaFlushPolicy>(
        /* shouldFlush */ [](const auto&) { return true; },
        /* shouldChunk */
        [&](const nimble::StripeProgress& stripeProgress) {
          const bool shouldChunk =
              stripeProgress.stripeRawSize > memoryPressureThreshold;
          actualChunkingDecisions.push_back(shouldChunk);
          return shouldChunk;
        });
  };

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      *rootPool_, rowVector->type(), std::move(writeFile), writerOptions);
  writer.write(rowVector);
  writer.close();

  EXPECT_THAT(
      actualChunkingDecisions,
      ::testing::ElementsAreArray(expectedChunkingDecisions));

  velox::InMemoryReadFile readFile(file);
  nimble::VeloxReader reader(*leafPool_, &readFile);
  validateChunkSize(reader, writerOptions.minStreamChunkRawSize);
}

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
            .expectedStripeCount = 7,
            .expectedMaxChunkCount = 2,
            .expectedMinChunkCount = 1,
            .chunkedStreamBatchSize = 2,
        },
        // High memory regression threshold and no compression
        // Produces file identical to RawStripeSizeFlushPolicy
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
            .expectedStripeCount = 4,
            .expectedMaxChunkCount = 1,
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
            .expectedStripeCount = 10,
            .expectedMaxChunkCount = 2,
            .expectedMinChunkCount = 2, // +1 chunk
            .chunkedStreamBatchSize = 2,
        },
        // High target stripe size bytes (with disabled memory pressure
        // optimization) produces fewer stripes. Single chunks.
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
            .expectedStripeCount = 1,
            .expectedMaxChunkCount = 1,
            .expectedMinChunkCount = 1,
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
            .expectedStripeCount = 7,
            .expectedMaxChunkCount = 2,
            .expectedMinChunkCount = 1,
            .chunkedStreamBatchSize = 10}));
} // namespace facebook
