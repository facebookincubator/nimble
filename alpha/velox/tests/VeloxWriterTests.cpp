// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <zstd.h>

#include "dwio/alpha/common/EncodingPrimitives.h"
#include "dwio/alpha/common/tests/TestUtils.h"
#include "dwio/alpha/encodings/EncodingLayoutCapture.h"
#include "dwio/alpha/velox/EncodingLayoutTree.h"
#include "dwio/alpha/velox/SchemaSerialization.h"
#include "dwio/alpha/velox/TabletSections.h"
#include "dwio/alpha/velox/VeloxReader.h"
#include "dwio/alpha/velox/VeloxWriter.h"
#include "thrift/lib/cpp2/protocol/DebugProtocol.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/vector/VectorStream.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace ::facebook;

namespace {
auto rootPool = velox::memory::deprecatedDefaultMemoryManager().addRootPool(
    "velox_writer_tests");
auto leafPool = rootPool -> addLeafChild("leaf");
} // namespace

TEST(VeloxWriterTests, EmptyFile) {
  const uint32_t batchSize = 10;
  auto type = velox::ROW({{"simple", velox::INTEGER()}});
  alpha::VeloxWriterOptions writerOptions;

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  alpha::VeloxWriter writer(
      *rootPool, type, std::move(writeFile), std::move(writerOptions));
  writer.close();

  velox::InMemoryReadFile readFile(file);
  auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(type);
  alpha::VeloxReader reader(*leafPool, &readFile, std::move(selector));

  velox::VectorPtr result;
  ASSERT_FALSE(reader.next(batchSize, result));
}

TEST(VeloxWriterTests, EmptyFileNoSchema) {
  const uint32_t batchSize = 10;
  auto type = velox::ROW({{"simple", velox::INTEGER()}});
  alpha::VeloxWriterOptions writerOptions;

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  alpha::VeloxWriter writer(
      *rootPool, type, std::move(writeFile), std::move(writerOptions));
  writer.close();

  velox::InMemoryReadFile readFile(file);
  alpha::VeloxReader reader(*leafPool, &readFile);

  velox::VectorPtr result;
  ASSERT_FALSE(reader.next(batchSize, result));
}

TEST(VeloxWriterTests, RootHasNulls) {
  auto batchSize = 5;
  velox::test::VectorMaker vectorMaker{leafPool.get()};
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
      *rootPool, vector->type(), std::move(writeFile), {});
  writer.write(vector);
  writer.close();

  velox::InMemoryReadFile readFile(file);
  alpha::VeloxReader reader(*leafPool, &readFile);

  velox::VectorPtr result;
  ASSERT_TRUE(reader.next(batchSize, result));
  ASSERT_EQ(result->size(), batchSize);
  for (auto i = 0; i < batchSize; ++i) {
    ASSERT_TRUE(result->equalValueAt(vector.get(), i, i));
  }
}

TEST(VeloxWriterTests, FeatureReorderingNonFlatmapColumn) {
  const uint32_t batchSize = 10;

  velox::test::VectorMaker vectorMaker{leafPool.get()};
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
      *rootPool,
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
std::vector<velox::VectorPtr> generateBatches(
    const std::shared_ptr<const velox::Type>& type,
    size_t batchCount,
    size_t size,
    uint32_t seed,
    velox::memory::MemoryPool& pool) {
  std::vector<velox::VectorPtr> batches;
  std::mt19937 gen{};
  gen.seed(seed);
  for (size_t i = 0; i < batchCount; ++i) {
    batches.push_back(
        velox::test::BatchMaker::createBatch(type, size, *leafPool, gen));
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
    : public ::testing::Test,
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
      *rootPool, type, std::move(writeFile), std::move(writerOptions));
  auto batches =
      generateBatches(type, GetParam().batchCount, 4000, 20221110, *leafPool);

  for (const auto& batch : batches) {
    writer.write(batch);
  }
  writer.close();

  velox::InMemoryReadFile readFile(file);
  auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(type);
  alpha::VeloxReader reader(*leafPool, &readFile, std::move(selector));

  EXPECT_EQ(GetParam().stripeCount, reader.getTabletView().stripeCount());
}

TEST(VeloxWriterTests, EncodingLayout) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      std::nullopt,
      "",
      {
          {alpha::Kind::Map,
           alpha::EncodingLayout{
               alpha::EncodingType::Dictionary,
               alpha::CompressionType::Uncompressed,
               {
                   alpha::EncodingLayout{
                       alpha::EncodingType::FixedBitWidth,
                       alpha::CompressionType::Zstrong},
                   std::nullopt,
               }},
           "",
           {
               // Map keys
               {alpha::Kind::Scalar, std::nullopt, ""},
               // Map Values
               {alpha::Kind::Scalar,
                alpha::EncodingLayout{
                    alpha::EncodingType::MainlyConstant,
                    alpha::CompressionType::Uncompressed,
                    {
                        std::nullopt,
                        alpha::EncodingLayout{
                            alpha::EncodingType::Trivial,
                            alpha::CompressionType::Zstrong},
                    }},
                ""},
           }},
          {alpha::Kind::FlatMap,
           std::nullopt,
           "",
           {
               {
                   alpha::Kind::Scalar,
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
                   "1",
               },
               {
                   alpha::Kind::Scalar,
                   alpha::EncodingLayout{
                       alpha::EncodingType::Constant,
                       alpha::CompressionType::Uncompressed,
                   },
                   "2",
               },
           }},
      }};

  velox::test::VectorMaker vectorMaker{leafPool.get()};
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
      *rootPool,
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

  alpha::testing::InMemoryTrackableReadFile readFile(file);
  alpha::Tablet tablet{*leafPool, &readFile};
  auto section = tablet.loadOptionalSection(std::string(alpha::kSchemaSection));
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
  auto& flatMapKey1Node = findChild(flatMapNode, "1")->asScalar();
  auto flatMapKey2Node = findChild(flatMapNode, "2")->asScalar();

  for (auto i = 0; i < tablet.stripeCount(); ++i) {
    std::vector<uint32_t> identifiers{
        mapNode.offset(),
        mapValuesNode.offset(),
        flatMapKey1Node.offset(),
        flatMapKey2Node.offset()};
    auto streams = tablet.load(i, identifiers);
    {
      // Verify Map stream
      auto capture =
          alpha::EncodingLayoutCapture::capture(streams[0]->nextChunk());
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
      // Verify Map Values stream
      auto capture =
          alpha::EncodingLayoutCapture::capture(streams[1]->nextChunk());
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
      // Verify FlatMap Kay "1" stream
      auto capture =
          alpha::EncodingLayoutCapture::capture(streams[2]->nextChunk());
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
      // Verify FlatMap Kay "2" stream
      auto capture =
          alpha::EncodingLayoutCapture::capture(streams[3]->nextChunk());
      EXPECT_EQ(alpha::EncodingType::Constant, capture.encodingType());
    }
  }
}

TEST(VeloxWriterTests, EncodingLayoutSchemaMismatch) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      std::nullopt,
      "",
      {
          {
              alpha::Kind::Scalar,
              alpha::EncodingLayout{
                  alpha::EncodingType::Dictionary,
                  alpha::CompressionType::Uncompressed,
                  {
                      alpha::EncodingLayout{
                          alpha::EncodingType::FixedBitWidth,
                          alpha::CompressionType::Zstrong},
                      std::nullopt,
                  }},
              "",
          },
      }};

  velox::test::VectorMaker vectorMaker{leafPool.get()};
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
        *rootPool,
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

TEST(VeloxWriterTests, EncodingLayoutSchemaEvolutionMapToFlatmap) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      std::nullopt,
      "",
      {
          {alpha::Kind::Map,
           alpha::EncodingLayout{
               alpha::EncodingType::Dictionary,
               alpha::CompressionType::Uncompressed,
               {
                   alpha::EncodingLayout{
                       alpha::EncodingType::FixedBitWidth,
                       alpha::CompressionType::Zstrong},
                   std::nullopt,
               }},
           "",
           {
               // Map keys
               {alpha::Kind::Scalar, std::nullopt, ""},
               // Map Values
               {alpha::Kind::Scalar,
                alpha::EncodingLayout{
                    alpha::EncodingType::MainlyConstant,
                    alpha::CompressionType::Uncompressed,
                    {
                        std::nullopt,
                        alpha::EncodingLayout{
                            alpha::EncodingType::Trivial,
                            alpha::CompressionType::Zstrong},
                    }},
                ""},
           }},
      }};

  velox::test::VectorMaker vectorMaker{leafPool.get()};
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
      *rootPool,
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

TEST(VeloxWriterTests, EncodingLayoutSchemaEvolutionFlamapToMap) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      std::nullopt,
      "",
      {
          {alpha::Kind::FlatMap,
           std::nullopt,
           "",
           {
               {
                   alpha::Kind::Scalar,
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
                   "1",
               },
               {
                   alpha::Kind::Scalar,
                   alpha::EncodingLayout{
                       alpha::EncodingType::Constant,
                       alpha::CompressionType::Uncompressed,
                   },
                   "2",
               },
           }},
      }};

  velox::test::VectorMaker vectorMaker{leafPool.get()};
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
      *rootPool,
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

TEST(VeloxWriterTests, EncodingLayoutSchemaEvolutionExpandingRow) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      std::nullopt,
      "",
      {
          {alpha::Kind::Row,
           alpha::EncodingLayout{
               alpha::EncodingType::Trivial,
               alpha::CompressionType::Uncompressed},
           "",
           {
               {
                   alpha::Kind::Scalar,
                   alpha::EncodingLayout{
                       alpha::EncodingType::Trivial,
                       alpha::CompressionType::Uncompressed},
                   "",
               },
           }},
      }};

  velox::test::VectorMaker vectorMaker{leafPool.get()};

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
      *rootPool,
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
