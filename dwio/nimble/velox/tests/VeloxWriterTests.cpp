/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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
#include <zstd.h>

#include "common/strings/UUID.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/encodings/EncodingLayoutCapture.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/velox/ChunkedStream.h"
#include "dwio/nimble/velox/EncodingLayoutTree.h"
#include "dwio/nimble/velox/SchemaSerialization.h"
#include "dwio/nimble/velox/VeloxReader.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "folly/FileUtil.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/vector/VectorStream.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace ::facebook;

class VeloxWriterTests : public testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::SharedArbitrator::registerFactory();
    velox::memory::MemoryManager::testingSetInstance(
        {.arbitratorKind = "SHARED"});
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
      throw std::runtime_error("error/" + strings::generateUUID());
    }
    void flush() final {
      throw std::runtime_error("error/" + strings::generateUUID());
    }
    void close() final {
      throw std::runtime_error("error/" + strings::generateUUID());
    }
    uint64_t size() const final {
      throw std::runtime_error("error/" + strings::generateUUID());
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
            [&](auto&) { return nimble::FlushDecision::Stripe; });
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

TEST_F(VeloxWriterTests, FeatureReorderingNonFlatmapColumn) {
  const uint32_t batchSize = 10;

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

  try {
    writer.write(vector);
    writer.close();
    FAIL();
  } catch (const nimble::NimbleUserError& e) {
    EXPECT_EQ("USER", e.errorSource());
    EXPECT_EQ("INVALID_ARGUMENT", e.errorCode());
    EXPECT_EQ(
        "Column 'map' for feature ordering is not a flat map.",
        e.errorMessage());
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

  for (size_t i = 0; i < batchCount; ++i) {
    batches.push_back(fuzzer.fuzzInputFlatRow(type));
  }
  return batches;
}
} // namespace

struct RawStripeSizeFlushPolicyTestCase {
  const size_t batchCount;
  const uint32_t rawStripeSize;
  const uint32_t stripeCount;
};

class RawStripeSizeFlushPolicyTest
    : public VeloxWriterTests,
      public ::testing::WithParamInterface<RawStripeSizeFlushPolicyTestCase> {};

TEST_P(RawStripeSizeFlushPolicyTest, RawStripeSizeFlushPolicy) {
  auto type = velox::ROW({{"simple", velox::INTEGER()}});
  nimble::VeloxWriterOptions writerOptions{.flushPolicyFactory = []() {
    // Buffering 256MB data before encoding stripes.
    return std::make_unique<nimble::RawStripeSizeFlushPolicy>(
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
  explicit MockReclaimer() {}
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
      "root", 4L << 20, velox::exec::MemoryReclaimer::create());
  auto writerPool = rootPool->addAggregateChild(
      "writer", velox::exec::MemoryReclaimer::create());

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
    return std::make_unique<nimble::RawStripeSizeFlushPolicy>(1 * 1024 * 1024);
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
          .compressionOptions = {.compressionAcceptRatio = 100},
      });

  writer.write(vector);
  writer.close();

  for (auto useChaniedBuffers : {false, true}) {
    nimble::testing::InMemoryTrackableReadFile readFile(
        file, useChaniedBuffers);
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
      std::vector<uint32_t> identifiers{
          mapNode.lengthsDescriptor().offset(),
          mapValuesNode.scalarDescriptor().offset(),
          flatMapKey1Node.scalarDescriptor().offset(),
          flatMapKey2Node.scalarDescriptor().offset()};
      auto streams = tablet.load(i, identifiers);
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
              vectorMaker.flatVector<uint32_t>({1, 2, 3, 4, 5}),
              vectorMaker.flatVector<uint32_t>({1, 2, 3, 4, 5}),
          }),
          vectorMaker.rowVector({
              vectorMaker.flatVector<uint32_t>({1, 2, 3, 4, 5}),
              vectorMaker.flatVector<uint32_t>({1, 2, 3, 4, 5}),
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
    std::vector<std::tuple<velox::VectorPtr, nimble::FlushDecision>> vectors,
    std::function<void(const nimble::TabletReader&)> verifier,
    folly::F14FastSet<std::string> flatMapColumns = {}) {
  ASSERT_LT(0, vectors.size());
  auto& type = std::get<0>(vectors[0])->type();

  auto leafPool = rootPool.addLeafChild("chunk_leaf");
  auto expected = velox::BaseVector::create(type, 0, leafPool.get());

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  auto flushDecision = nimble::FlushDecision::None;
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
                    [&](auto&) { return flushDecision; });
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
      {{vector, nimble::FlushDecision::None},
       {vector, nimble::FlushDecision::None}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        // Logically, there should be two streams in the tablet.
        // However, when writing stripes, we do not write empty streams.
        // In this case, the integer column is empty, and therefore, omitted.
        ASSERT_EQ(1, tablet.streamCount(0));
        EXPECT_LT(0, tablet.streamSizes(0)[0]);

        auto streamLoaders = tablet.load(0, std::array<uint32_t, 1>{0});
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
      {{vector, nimble::FlushDecision::Chunk},
       {vector, nimble::FlushDecision::Chunk}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        // Logically, there should be two streams in the tablet.
        // However, when writing stripes, we do not write empty streams.
        // In this case, the integer column is empty, and therefore, omitted.
        ASSERT_EQ(1, tablet.streamCount(0));
        EXPECT_LT(0, tablet.streamSizes(0)[0]);

        auto streamLoaders = tablet.load(0, std::array<uint32_t, 1>{0});
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
      {{vector, nimble::FlushDecision::Chunk},
       {vector, nimble::FlushDecision::Chunk}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        // Logically, there should be two streams in the tablet.
        // However, when writing stripes, we do not write empty streams.
        // In this case, the integer column is empty, and therefore, omitted.
        ASSERT_EQ(1, tablet.streamCount(0));
        EXPECT_LT(0, tablet.streamSizes(0)[0]);

        auto streamLoaders = tablet.load(0, std::array<uint32_t, 1>{0});
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
      {{nullsVector, nimble::FlushDecision::None},
       {nonNullsVector, nimble::FlushDecision::None}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        // We have values in stream 2, so it is not optimized away.
        ASSERT_EQ(2, tablet.streamCount(0));
        EXPECT_LT(0, tablet.streamSizes(0)[0]);
        EXPECT_LT(0, tablet.streamSizes(0)[1]);

        auto streamLoaders = tablet.load(0, std::array<uint32_t, 2>{0, 1});
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
      {{nullsVector, nimble::FlushDecision::Chunk},
       {nonNullsVector, nimble::FlushDecision::Chunk}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        ASSERT_EQ(2, tablet.streamCount(0));
        EXPECT_LT(0, tablet.streamSizes(0)[0]);
        EXPECT_LT(0, tablet.streamSizes(0)[1]);

        auto streamLoaders = tablet.load(0, std::array<uint32_t, 2>{0, 1});
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
      {{nullsVector, nimble::FlushDecision::Chunk},
       {nonNullsVector, nimble::FlushDecision::Chunk}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        ASSERT_EQ(2, tablet.streamCount(0));
        EXPECT_LT(0, tablet.streamSizes(0)[0]);
        EXPECT_LT(0, tablet.streamSizes(0)[1]);

        auto streamLoaders = tablet.load(0, std::array<uint32_t, 2>{0, 1});
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
      {{vector, nimble::FlushDecision::None},
       {vector, nimble::FlushDecision::None}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        ASSERT_EQ(2, tablet.streamCount(0));

        // When there are no nulls, the nulls stream is omitted.
        EXPECT_EQ(0, tablet.streamSizes(0)[0]);
        EXPECT_LT(0, tablet.streamSizes(0)[1]);

        auto streamLoaders = tablet.load(0, std::array<uint32_t, 2>{0, 1});
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
      {{vector, nimble::FlushDecision::Chunk},
       {vector, nimble::FlushDecision::Chunk}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        ASSERT_EQ(2, tablet.streamCount(0));

        // When there are no nulls, the nulls stream is omitted.
        EXPECT_EQ(0, tablet.streamSizes(0)[0]);
        EXPECT_LT(0, tablet.streamSizes(0)[1]);

        auto streamLoaders = tablet.load(0, std::array<uint32_t, 2>{0, 1});
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
      {{vector, nimble::FlushDecision::Chunk},
       {vector, nimble::FlushDecision::Chunk}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        ASSERT_EQ(2, tablet.streamCount(0));

        // When there are no nulls, the nulls stream is omitted.
        EXPECT_EQ(0, tablet.streamSizes(0)[0]);
        EXPECT_LT(0, tablet.streamSizes(0)[1]);

        auto streamLoaders = tablet.load(0, std::array<uint32_t, 2>{0, 1});
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
      {{vector, nimble::FlushDecision::None},
       {vector, nimble::FlushDecision::None}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        // When all rows are not null, the nulls stream is omitted.
        // When all values are null, the values stream is omitted.
        // Since these are the last two stream, they are optimized away.
        ASSERT_EQ(0, tablet.streamCount(0));
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
      {{vector, nimble::FlushDecision::Chunk},
       {vector, nimble::FlushDecision::Chunk}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        // When all rows are not null, the nulls stream is omitted.
        // When all values are null, the values stream is omitted.
        // Since these are the last two stream, they are optimized away.
        ASSERT_EQ(0, tablet.streamCount(0));
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
      {{vector, nimble::FlushDecision::Chunk},
       {vector, nimble::FlushDecision::Chunk}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        // When all rows are not null, the nulls stream is omitted.
        // When all values are null, the values stream is omitted.
        // Since these are the last two stream, they are optimized away.
        ASSERT_EQ(0, tablet.streamCount(0));
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
      {{vector, nimble::FlushDecision::None},
       {vector, nimble::FlushDecision::None}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        // Expected streams:
        // 0: Row nulls stream (expected empty, as all values are not null)
        // 1: Flatmap nulls stream
        ASSERT_EQ(2, tablet.streamCount(0));
        EXPECT_EQ(0, tablet.streamSizes(0)[0]);
        EXPECT_LT(0, tablet.streamSizes(0)[1]);

        auto streamLoaders = tablet.load(0, std::array<uint32_t, 2>{0, 1});
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
      {{vector, nimble::FlushDecision::Chunk},
       {vector, nimble::FlushDecision::Chunk}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        // Expected streams:
        // 0: Row nulls stream (expected empty, as all values are not null)
        // 1: Flatmap nulls stream
        ASSERT_EQ(2, tablet.streamCount(0));
        EXPECT_EQ(0, tablet.streamSizes(0)[0]);
        EXPECT_LT(0, tablet.streamSizes(0)[1]);

        auto streamLoaders = tablet.load(0, std::array<uint32_t, 2>{0, 1});
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
      {{vector, nimble::FlushDecision::Chunk},
       {vector, nimble::FlushDecision::Chunk}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        // Expected streams:
        // 0: Row nulls stream (expected empty, as all values are not null)
        // 1: Flatmap nulls stream
        ASSERT_EQ(2, tablet.streamCount(0));
        EXPECT_EQ(0, tablet.streamSizes(0)[0]);
        EXPECT_LT(0, tablet.streamSizes(0)[1]);

        auto streamLoaders = tablet.load(0, std::array<uint32_t, 2>{0, 1});
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
      {{nullsVector, nimble::FlushDecision::None},
       {nonNullsVector, nimble::FlushDecision::None}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        // Expected streams:
        // 0: Row nulls stream (expected empty, as all values are not null)
        // 1: Flatmap nulls stream
        // 2: Scalar stream (flatmap value for key 5)
        // 3: Scalar stream (flatmap in-map for key 5)
        ASSERT_EQ(4, tablet.streamCount(0));
        EXPECT_EQ(0, tablet.streamSizes(0)[0]);
        EXPECT_LT(0, tablet.streamSizes(0)[1]);
        EXPECT_LT(0, tablet.streamSizes(0)[2]);
        EXPECT_LT(0, tablet.streamSizes(0)[3]);

        auto streamLoaders =
            tablet.load(0, std::array<uint32_t, 4>{0, 1, 2, 3});
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
      {{nullsVector, nimble::FlushDecision::Chunk},
       {nonNullsVector, nimble::FlushDecision::Chunk}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        // Expected streams:
        // 0: Row nulls stream (expected empty, as all values are not null)
        // 1: Flatmap nulls stream
        // 2: Scalar stream (flatmap value for key 5)
        // 3: Scalar stream (flatmap in-map for key 5)
        ASSERT_EQ(4, tablet.streamCount(0));
        EXPECT_EQ(0, tablet.streamSizes(0)[0]);
        EXPECT_LT(0, tablet.streamSizes(0)[1]);
        EXPECT_LT(0, tablet.streamSizes(0)[2]);
        EXPECT_LT(0, tablet.streamSizes(0)[3]);

        auto streamLoaders =
            tablet.load(0, std::array<uint32_t, 4>{0, 1, 2, 3});
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
      {{nullsVector, nimble::FlushDecision::Chunk},
       {nonNullsVector, nimble::FlushDecision::Chunk}},
      [&](const auto& tablet) {
        ASSERT_EQ(1, tablet.stripeCount());

        // Expected streams:
        // 0: Row nulls stream (expected empty, as all values are not null)
        // 1: Flatmap nulls stream
        // 2: Scalar stream (flatmap value for key 5)
        // 3: Scalar stream (flatmap in-map for key 5)
        ASSERT_EQ(4, tablet.streamCount(0));
        EXPECT_EQ(0, tablet.streamSizes(0)[0]);
        EXPECT_LT(0, tablet.streamSizes(0)[1]);
        EXPECT_LT(0, tablet.streamSizes(0)[2]);
        EXPECT_LT(0, tablet.streamSizes(0)[3]);

        auto streamLoaders =
            tablet.load(0, std::array<uint32_t, 4>{0, 1, 2, 3});
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

INSTANTIATE_TEST_CASE_P(
    RawStripeSizeFlushPolicyTestSuite,
    RawStripeSizeFlushPolicyTest,
    testing::Values(
        RawStripeSizeFlushPolicyTestCase{
            .batchCount = 50,
            .rawStripeSize = 256 << 10,
            .stripeCount = 4},
        RawStripeSizeFlushPolicyTestCase{
            .batchCount = 100,
            .rawStripeSize = 256 << 10,
            .stripeCount = 7},
        RawStripeSizeFlushPolicyTestCase{
            .batchCount = 100,
            .rawStripeSize = 256 << 11,
            .stripeCount = 4},
        RawStripeSizeFlushPolicyTestCase{
            .batchCount = 100,
            .rawStripeSize = 256 << 12,
            .stripeCount = 2},
        RawStripeSizeFlushPolicyTestCase{
            .batchCount = 100,
            .rawStripeSize = 256 << 20,
            .stripeCount = 1}));
