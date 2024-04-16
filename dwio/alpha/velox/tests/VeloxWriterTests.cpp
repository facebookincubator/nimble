// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <velox/exec/MemoryReclaimer.h>
#include <zstd.h>

#include "common/strings/UUID.h"
#include "dwio/alpha/common/EncodingPrimitives.h"
#include "dwio/alpha/common/tests/TestUtils.h"
#include "dwio/alpha/encodings/EncodingLayoutCapture.h"
#include "dwio/alpha/velox/ChunkedStream.h"
#include "dwio/alpha/velox/EncodingLayoutTree.h"
#include "dwio/alpha/velox/SchemaSerialization.h"
#include "dwio/alpha/velox/TabletSections.h"
#include "dwio/alpha/velox/VeloxReader.h"
#include "dwio/alpha/velox/VeloxWriter.h"
#include "thrift/lib/cpp2/protocol/DebugProtocol.h"
#include "velox/common/memory/SharedArbitrator.h"
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

  alpha::VeloxWriter writer(*rootPool_, type, std::move(writeFile), {});
  writer.close();

  velox::InMemoryReadFile readFile(file);
  alpha::VeloxReader reader(*leafPool_, &readFile);

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

  alpha::VeloxWriter writer(
      *rootPool_,
      vector->type(),
      std::move(writeFile),
      {.flushPolicyFactory = [&]() {
        return std::make_unique<alpha::LambdaFlushPolicy>(
            [&](auto&) { return alpha::FlushDecision::Stripe; });
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
  alpha::VeloxWriterOptions writerOptions;

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  alpha::VeloxWriter writer(
      *rootPool_, type, std::move(writeFile), std::move(writerOptions));
  writer.close();

  velox::InMemoryReadFile readFile(file);
  alpha::VeloxReader reader(*leafPool_, &readFile);

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
  alpha::VeloxWriter writer(
      *rootPool_, vector->type(), std::move(writeFile), {});
  writer.write(vector);
  writer.close();

  velox::InMemoryReadFile readFile(file);
  alpha::VeloxReader reader(*leafPool_, &readFile);

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

  alpha::VeloxWriter writer(
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
  } catch (const alpha::AlphaUserError& e) {
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
  alpha::VeloxWriterOptions writerOptions{.flushPolicyFactory = []() {
    // Buffering 256MB data before encoding stripes.
    return std::make_unique<alpha::RawStripeSizeFlushPolicy>(
        GetParam().rawStripeSize);
  }};

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  alpha::VeloxWriter writer(
      *rootPool_, type, std::move(writeFile), std::move(writerOptions));
  auto batches =
      generateBatches(type, GetParam().batchCount, 4000, 20221110, *leafPool_);

  for (const auto& batch : batches) {
    writer.write(batch);
  }
  writer.close();

  velox::InMemoryReadFile readFile(file);
  auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(type);
  alpha::VeloxReader reader(*leafPool_, &readFile, std::move(selector));

  EXPECT_EQ(GetParam().stripeCount, reader.getTabletView().stripeCount());
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

TEST_F(VeloxWriterTests, memoryReclaimPath) {
  auto rootPool = velox::memory::memoryManager()->addRootPool(
      "root", 4L << 20, velox::exec::MemoryReclaimer::create());
  auto writerPool = rootPool->addAggregateChild(
      "writer", velox::exec::MemoryReclaimer::create());

  auto type = velox::ROW(
      {{"simple_int", velox::INTEGER()}, {"simple_double", velox::DOUBLE()}});
  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  std::atomic_bool reclaimEntered = false;
  alpha::VeloxWriterOptions writerOptions{.reclaimerFactory = [&]() {
    auto reclaimer = std::make_unique<MockReclaimer>();
    reclaimer->setEnterArbitrationFunc([&]() { reclaimEntered = true; });
    return reclaimer;
  }};
  alpha::VeloxWriter writer(
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

TEST_F(VeloxWriterTests, EncodingLayout) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      {},
      "",
      {
          {alpha::Kind::Map,
           {
               {
                   0,
                   alpha::EncodingLayout{
                       alpha::EncodingType::Dictionary,
                       alpha::CompressionType::Uncompressed,
                       {
                           alpha::EncodingLayout{
                               alpha::EncodingType::FixedBitWidth,
                               alpha::CompressionType::Zstrong},
                           std::nullopt,
                       }},
               },
           },
           "",
           {
               // Map keys
               {alpha::Kind::Scalar, {}, ""},
               // Map Values
               {alpha::Kind::Scalar,
                {
                    {
                        0,
                        alpha::EncodingLayout{
                            alpha::EncodingType::MainlyConstant,
                            alpha::CompressionType::Uncompressed,
                            {
                                std::nullopt,
                                alpha::EncodingLayout{
                                    alpha::EncodingType::Trivial,
                                    alpha::CompressionType::Zstrong},
                            }},
                    },
                },
                ""},
           }},
          {alpha::Kind::FlatMap,
           {},
           "",
           {
               {
                   alpha::Kind::Scalar,
                   {
                       {
                           0,
                           alpha::EncodingLayout{
                               alpha::EncodingType::MainlyConstant,
                               alpha::CompressionType::Uncompressed,
                               {
                                   alpha::EncodingLayout{
                                       alpha::EncodingType::Trivial,
                                       alpha::CompressionType::Uncompressed},
                                   alpha::EncodingLayout{
                                       alpha::EncodingType::FixedBitWidth,
                                       alpha::CompressionType::Uncompressed},
                               }},
                       },
                   },
                   "1",
               },
               {
                   alpha::Kind::Scalar,
                   {
                       {
                           0,
                           alpha::EncodingLayout{
                               alpha::EncodingType::Constant,
                               alpha::CompressionType::Uncompressed,
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

  alpha::VeloxWriter writer(
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
    alpha::testing::InMemoryTrackableReadFile readFile(file, useChaniedBuffers);
    alpha::Tablet tablet{*leafPool_, &readFile};
    auto section =
        tablet.loadOptionalSection(std::string(alpha::kSchemaSection));
    ALPHA_CHECK(section.has_value(), "Schema not found.");
    auto schema =
        alpha::SchemaDeserializer::deserialize(section->content().data());
    auto& mapNode = schema->asRow().childAt(0)->asMap();
    auto& mapValuesNode = mapNode.values()->asScalar();
    auto& flatMapNode = schema->asRow().childAt(1)->asFlatMap();
    ASSERT_EQ(3, flatMapNode.childrenCount());

    auto findChild =
        [](const facebook::alpha::FlatMapType& map,
           std::string_view key) -> std::shared_ptr<const alpha::Type> {
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
        alpha::InMemoryChunkedStream chunkedStream{
            *leafPool_, std::move(streams[0])};
        ASSERT_TRUE(chunkedStream.hasNext());
        // Verify Map stream
        auto capture =
            alpha::EncodingLayoutCapture::capture(chunkedStream.nextChunk());
        EXPECT_EQ(alpha::EncodingType::Dictionary, capture.encodingType());
        EXPECT_EQ(
            alpha::EncodingType::FixedBitWidth,
            capture.child(alpha::EncodingIdentifiers::Dictionary::Alphabet)
                ->encodingType());
        EXPECT_EQ(
            alpha::CompressionType::Zstrong,
            capture.child(alpha::EncodingIdentifiers::Dictionary::Alphabet)
                ->compressionType());
      }

      {
        alpha::InMemoryChunkedStream chunkedStream{
            *leafPool_, std::move(streams[1])};
        ASSERT_TRUE(chunkedStream.hasNext());
        // Verify Map Values stream
        auto capture =
            alpha::EncodingLayoutCapture::capture(chunkedStream.nextChunk());
        EXPECT_EQ(alpha::EncodingType::MainlyConstant, capture.encodingType());
        EXPECT_EQ(
            alpha::EncodingType::Trivial,
            capture
                .child(alpha::EncodingIdentifiers::MainlyConstant::OtherValues)
                ->encodingType());
        EXPECT_EQ(
            alpha::CompressionType::Zstrong,
            capture
                .child(alpha::EncodingIdentifiers::MainlyConstant::OtherValues)
                ->compressionType());
      }

      {
        alpha::InMemoryChunkedStream chunkedStream{
            *leafPool_, std::move(streams[2])};
        ASSERT_TRUE(chunkedStream.hasNext());
        // Verify FlatMap Kay "1" stream
        auto capture =
            alpha::EncodingLayoutCapture::capture(chunkedStream.nextChunk());
        EXPECT_EQ(alpha::EncodingType::MainlyConstant, capture.encodingType());
        EXPECT_EQ(
            alpha::EncodingType::Trivial,
            capture.child(alpha::EncodingIdentifiers::MainlyConstant::IsCommon)
                ->encodingType());
        EXPECT_EQ(
            alpha::CompressionType::Uncompressed,
            capture.child(alpha::EncodingIdentifiers::MainlyConstant::IsCommon)
                ->compressionType());
        EXPECT_EQ(
            alpha::EncodingType::FixedBitWidth,
            capture
                .child(alpha::EncodingIdentifiers::MainlyConstant::OtherValues)
                ->encodingType());
        EXPECT_EQ(
            alpha::CompressionType::Uncompressed,
            capture
                .child(alpha::EncodingIdentifiers::MainlyConstant::OtherValues)
                ->compressionType());
      }

      {
        alpha::InMemoryChunkedStream chunkedStream{
            *leafPool_, std::move(streams[3])};
        ASSERT_TRUE(chunkedStream.hasNext());
        // Verify FlatMap Kay "2" stream
        auto capture =
            alpha::EncodingLayoutCapture::capture(chunkedStream.nextChunk());
        EXPECT_EQ(alpha::EncodingType::Constant, capture.encodingType());
      }
    }
  }
}

TEST_F(VeloxWriterTests, EncodingLayoutSchemaMismatch) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      {},
      "",
      {
          {
              alpha::Kind::Scalar,
              {
                  {
                      0,
                      alpha::EncodingLayout{
                          alpha::EncodingType::Dictionary,
                          alpha::CompressionType::Uncompressed,
                          {
                              alpha::EncodingLayout{
                                  alpha::EncodingType::FixedBitWidth,
                                  alpha::CompressionType::Zstrong},
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
    alpha::VeloxWriter writer(
        *rootPool_,
        vector->type(),
        std::move(writeFile),
        {
            .encodingLayoutTree = std::move(expected),
            .compressionOptions = {.compressionAcceptRatio = 100},
        });
    FAIL() << "Writer should fail on incompatible encoding layout node";
  } catch (const alpha::AlphaUserError& e) {
    EXPECT_NE(
        std::string(e.what()).find(
            "Incompatible encoding layout node. Expecting map node"),
        std::string::npos);
  }
}

TEST_F(VeloxWriterTests, EncodingLayoutSchemaEvolutionMapToFlatmap) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      {},
      "",
      {
          {alpha::Kind::Map,
           {
               {
                   0,
                   alpha::EncodingLayout{
                       alpha::EncodingType::Dictionary,
                       alpha::CompressionType::Uncompressed,
                       {
                           alpha::EncodingLayout{
                               alpha::EncodingType::FixedBitWidth,
                               alpha::CompressionType::Zstrong},
                           std::nullopt,
                       }},
               },
           },
           "",
           {
               // Map keys
               {alpha::Kind::Scalar, {}, ""},
               // Map Values
               {alpha::Kind::Scalar,
                {
                    {
                        0,
                        alpha::EncodingLayout{
                            alpha::EncodingType::MainlyConstant,
                            alpha::CompressionType::Uncompressed,
                            {
                                std::nullopt,
                                alpha::EncodingLayout{
                                    alpha::EncodingType::Trivial,
                                    alpha::CompressionType::Zstrong},
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

  alpha::VeloxWriter writer(
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
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      {},
      "",
      {
          {alpha::Kind::FlatMap,
           {},
           "",
           {
               {
                   alpha::Kind::Scalar,
                   {
                       {
                           0,
                           alpha::EncodingLayout{
                               alpha::EncodingType::MainlyConstant,
                               alpha::CompressionType::Uncompressed,
                               {
                                   alpha::EncodingLayout{
                                       alpha::EncodingType::Trivial,
                                       alpha::CompressionType::Uncompressed},
                                   alpha::EncodingLayout{
                                       alpha::EncodingType::FixedBitWidth,
                                       alpha::CompressionType::Uncompressed},
                               }},
                       },
                   },
                   "1",
               },
               {
                   alpha::Kind::Scalar,
                   {
                       {
                           0,
                           alpha::EncodingLayout{
                               alpha::EncodingType::Constant,
                               alpha::CompressionType::Uncompressed,
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

  alpha::VeloxWriter writer(
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
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      {},
      "",
      {
          {alpha::Kind::Row,
           {
               {
                   0,
                   alpha::EncodingLayout{
                       alpha::EncodingType::Trivial,
                       alpha::CompressionType::Uncompressed},
               },
           },
           "",
           {
               {
                   alpha::Kind::Scalar,
                   {
                       {
                           0,
                           alpha::EncodingLayout{
                               alpha::EncodingType::Trivial,
                               alpha::CompressionType::Uncompressed},
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

  alpha::VeloxWriter writer(
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
