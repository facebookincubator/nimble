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

#include "dwio/nimble/velox/index/NimbleIndexProjector.h"

#include <gtest/gtest.h>

#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/serializer/Deserializer.h"
#include "dwio/nimble/serializer/DeserializerImpl.h"
#include "dwio/nimble/serializer/Serializer.h"
#include "dwio/nimble/serializer/SerializerImpl.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/tablet/tests/TabletTestUtils.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "dwio/nimble/velox/VeloxReader.h"
#include "dwio/nimble/velox/VeloxWriter.h"

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/memory/Memory.h"
#include "velox/serializers/KeyEncoder.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::nimble::test {

using namespace velox;
using namespace velox::dwio::common;
using Subfield = velox::common::Subfield;

namespace {

// Test-local mirrors of `rocks::readResultRowRange` and
// `rocks::readResultResumeKey`.
// The dwio/nimble layer cannot depend on rocks/nimble (the dependency runs
// the other way — rocks/nimble's NimbleTable depends on the projector), so
// these tests cannot include `rocks/nimble/NimbleTable.h`. Both helpers call
// the same underlying `serde::readTabletChunkHeader` as the production rocks::
// helpers, so behavior is in lockstep.
RowRange readEmbeddedRowRange(const folly::IOBuf& chunkSlice) {
  const char* pos = reinterpret_cast<const char*>(chunkSlice.data());
  const char* end = pos + chunkSlice.length();
  return serde::readTabletChunkHeader(pos, end).rowRange;
}

std::optional<std::string> readEmbeddedResumeKey(
    const folly::IOBuf& chunkSlice) {
  const char* pos = reinterpret_cast<const char*>(chunkSlice.data());
  const char* end = pos + chunkSlice.length();
  return serde::readTabletChunkHeader(pos, end).resumeKey;
}

// Coalesces the chunk slice IOBuf chain into a single contiguous buffer for
// passing to Deserializer. The Deserializer reads the per-slice header
// natively (consuming the row range and resume-key fields) but does not
// slice its output — it over-fetches by decoding the full stripe.
folly::IOBuf coalesceChunkSlice(const folly::IOBuf& chunkSlice) {
  return chunkSlice.cloneCoalescedAsValue();
}

} // namespace

struct TestParam {
  bool enableCache;
  bool pinMetadata;

  std::string debugString() const {
    return fmt::format(
        "enableCache={}, pinMetadata={}", enableCache, pinMetadata);
  }
};

class NimbleIndexProjectorTest : public ::testing::TestWithParam<TestParam> {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::initialize({});
  }

  void SetUp() override {
    rootPool_ =
        memory::memoryManager()->addRootPool("NimbleIndexProjectorTest");
    leafPool_ = rootPool_->addLeafChild("leaf");
    vectorMaker_ = std::make_unique<velox::test::VectorMaker>(leafPool_.get());
  }

  void TearDown() override {
    vectorMaker_.reset();
    leafPool_.reset();
    rootPool_.reset();
  }

  // Writes sorted data with an index on the key column.
  void writeData(
      const std::vector<RowVectorPtr>& batches,
      const std::vector<std::string>& indexColumns,
      const folly::F14FastMap<std::string, std::set<std::string>>&
          flatMapColumns = {},
      uint64_t stripeSize = 4 << 10 /* 4KB */) {
    sinkData_.clear();
    auto writeFile = std::make_unique<InMemoryWriteFile>(&sinkData_);

    VeloxWriterOptions options;
    options.enableChunking = true;
    options.flatMapColumns = flatMapColumns;
    ClusterIndexConfig clusterIndexConfig;
    clusterIndexConfig.columns = indexColumns;
    clusterIndexConfig.sortOrders = std::vector<SortOrder>(
        indexColumns.size(), SortOrder{.ascending = true});
    clusterIndexConfig.enforceKeyOrder = true;
    clusterIndexConfig.noDuplicateKey = true;
    options.clusterIndexConfig = std::move(clusterIndexConfig);

    options.flushPolicyFactory = [stripeSize]() {
      return std::make_unique<LambdaFlushPolicy>(
          /*shouldFlush*/
          [stripeSize](const StripeProgress& progress) {
            return progress.stripeRawSize >= stripeSize;
          });
    };
    auto rowType = asRowType(batches[0]->type());
    VeloxWriter writer(
        rowType, std::move(writeFile), *rootPool_, std::move(options));
    for (const auto& batch : batches) {
      writer.write(batch);
    }
    writer.close();
  }

 public:
  static std::vector<TestParam> getTestParams() {
    return {
        {false, false},
        {false, true},
        {true, false},
        {true, true},
    };
  }

 protected:
  velox::FileHandle makeFileHandle() {
    auto readFile =
        std::make_shared<InMemoryReadFile>(std::string_view(sinkData_));
    return velox::FileHandle{
        std::move(readFile),
        velox::StringIdLease(velox::fileIds(), "test_file"),
        velox::StringIdLease(velox::fileIds(), "test_group")};
  }

  struct ProjectorWithStats {
    std::unique_ptr<NimbleIndexProjector> projector;
    std::shared_ptr<velox::io::IoStatistics> ioStats;
  };

  std::unique_ptr<NimbleIndexProjector> createProjector(
      const std::vector<Subfield>& projectedSubfields) {
    return createProjectorWithStats(projectedSubfields).projector;
  }

  ProjectorWithStats createProjectorWithStats(
      const std::vector<Subfield>& projectedSubfields,
      velox::cache::AsyncDataCache* cache = nullptr,
      std::optional<bool> cacheData = std::nullopt,
      bool setDataIoStats = true,
      bool setMetadataIoStats = true,
      bool setIndexIoStats = true) {
    auto fileHandle = makeFileHandle();
    auto ioStats = std::make_shared<velox::io::IoStatistics>();
    dwio::common::ReaderOptions readerOptions(leafPool_.get());
    if (setDataIoStats) {
      readerOptions.setDataIoStats(ioStats);
    }
    if (setMetadataIoStats) {
      readerOptions.setMetadataIoStats(
          std::make_shared<velox::io::IoStatistics>());
    }
    if (setIndexIoStats) {
      readerOptions.setIndexIoStats(
          std::make_shared<velox::io::IoStatistics>());
    }
    readerOptions.setFileFormat(FileFormat::NIMBLE);
    readerOptions.setLoadClusterIndex(true);
    readerOptions.setCacheMetadata(GetParam().enableCache);
    readerOptions.setPinMetadata(GetParam().pinMetadata);
    if (cacheData.has_value()) {
      readerOptions.setCacheData(*cacheData);
    }
    return {
        NimbleIndexProjector::create(
            fileHandle, cache, projectedSubfields, readerOptions),
        ioStats};
  }

  // Creates encoded key bounds for a point lookup on a single int64 key.
  velox::serializer::EncodedKeyBounds makePointLookup(
      const RowTypePtr& keyType,
      const std::vector<std::string>& indexColumns,
      int64_t key) {
    if (keyEncoder_ == nullptr) {
      std::vector<velox::core::SortOrder> sortOrders(
          indexColumns.size(), velox::core::SortOrder{true, false});
      keyEncoder_ = velox::serializer::KeyEncoder::create(
          indexColumns, keyType, sortOrders, leafPool_.get());
    }

    auto keyVector = vectorMaker_->rowVector(
        indexColumns, {vectorMaker_->flatVector<int64_t>({key})});

    velox::serializer::IndexBounds bounds;
    bounds.indexColumns = indexColumns;
    bounds.set(
        velox::serializer::IndexBound{keyVector, true},
        velox::serializer::IndexBound{keyVector, true});

    auto encoded = keyEncoder_->encodeIndexBounds(bounds);
    EXPECT_EQ(encoded.size(), 1);
    return encoded[0];
  }

  // Creates encoded key bounds for a range scan on a single int64 key.
  // The range is [lowerKey, upperKey) (lower inclusive, upper exclusive).
  velox::serializer::EncodedKeyBounds makeRangeLookup(
      const RowTypePtr& keyType,
      const std::vector<std::string>& indexColumns,
      int64_t lowerKey,
      int64_t upperKey) {
    if (keyEncoder_ == nullptr) {
      std::vector<velox::core::SortOrder> sortOrders(
          indexColumns.size(), velox::core::SortOrder{true, false});
      keyEncoder_ = velox::serializer::KeyEncoder::create(
          indexColumns, keyType, sortOrders, leafPool_.get());
    }

    auto lowerVector = vectorMaker_->rowVector(
        indexColumns, {vectorMaker_->flatVector<int64_t>({lowerKey})});
    auto upperVector = vectorMaker_->rowVector(
        indexColumns, {vectorMaker_->flatVector<int64_t>({upperKey})});

    velox::serializer::IndexBounds bounds;
    bounds.indexColumns = indexColumns;
    bounds.set(
        velox::serializer::IndexBound{lowerVector, true},
        velox::serializer::IndexBound{upperVector, false});

    auto encoded = keyEncoder_->encodeIndexBounds(bounds);
    EXPECT_EQ(encoded.size(), 1);
    return encoded[0];
  }

  // Writes deterministic data with keys 0..numBatches*rowsPerBatch-1 across
  // multiple stripes (one stripe per batch).
  void writeResumeKeyTestData(int rowsPerBatch = 100, int numBatches = 10) {
    std::vector<RowVectorPtr> batches;
    for (int b = 0; b < numBatches; ++b) {
      std::vector<int64_t> keys(rowsPerBatch);
      std::vector<int32_t> values(rowsPerBatch);
      for (int i = 0; i < rowsPerBatch; ++i) {
        keys[i] = b * rowsPerBatch + i;
        values[i] = keys[i] * 10;
      }
      batches.emplace_back(vectorMaker_->rowVector(
          {"key", "value"},
          {vectorMaker_->flatVector<int64_t>(keys),
           vectorMaker_->flatVector<int32_t>(values)}));
    }
    // stripeSize=1 forces each batch into its own stripe.
    writeData(batches, {"key"}, {}, /*stripeSize=*/1);
  }

  const std::shared_ptr<io::IoStatistics> dataIoStats_{
      std::make_shared<io::IoStatistics>()};
  const std::shared_ptr<io::IoStatistics> metadataIoStats_{
      std::make_shared<io::IoStatistics>()};
  const std::shared_ptr<io::IoStatistics> indexIoStats_{
      std::make_shared<io::IoStatistics>()};

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::unique_ptr<velox::test::VectorMaker> vectorMaker_;
  std::string sinkData_;
  std::unique_ptr<velox::serializer::KeyEncoder> keyEncoder_;
};

TEST_P(NimbleIndexProjectorTest, basicColumnProjection) {
  // Schema: key (int64, sorted), col_a (int32), col_b (varchar)
  auto rowType =
      ROW({"key", "col_a", "col_b"}, {BIGINT(), INTEGER(), VARCHAR()});

  // Generate sorted data.
  const int numRows = 200;
  std::vector<int64_t> keys(numRows);
  std::vector<int32_t> colA(numRows);
  std::vector<std::string> colB(numRows);
  for (int i = 0; i < numRows; ++i) {
    keys[i] = i * 10; // 0, 10, 20, ...
    colA[i] = i * 100;
    colB[i] = fmt::format("value_{}", i);
  }

  std::vector<StringView> colBViews;
  colBViews.reserve(numRows);
  for (const auto& s : colB) {
    colBViews.emplace_back(s);
  }
  auto batch = vectorMaker_->rowVector(
      {"key", "col_a", "col_b"},
      {vectorMaker_->flatVector<int64_t>(keys),
       vectorMaker_->flatVector<int32_t>(colA),
       vectorMaker_->flatVector(colBViews)});

  writeData({batch}, {"key"});

  std::vector<Subfield> subfields;
  subfields.emplace_back("col_a");
  subfields.emplace_back("col_b");
  auto projector = createProjector(subfields);

  // Point lookup for key=50 (row index 5).
  auto pointBounds = makePointLookup(rowType, {"key"}, 50);
  NimbleIndexProjector::Request request;
  request.keyBounds = {pointBounds};
  auto result = projector->project(request, {});

  ASSERT_EQ(result.responses.size(), 1);
  const auto& response = result.responses[0];
  ASSERT_FALSE(response.chunks.empty());
  EXPECT_FALSE(response.resumeKey.has_value());

  const auto& chunkSlice = response.chunks[0];
  const auto rowRange = readEmbeddedRowRange(chunkSlice);
  ASSERT_GT(rowRange.numRows(), 0);

  // Coalesce the chunk slice IOBuf chain into a contiguous buffer that the
  // Deserializer can read end-to-end.
  auto coalesced = coalesceChunkSlice(chunkSlice);

  // Deserialize using nimble Deserializer with nimble schema.
  auto projectedVeloxType = ROW({"col_a", "col_b"}, {INTEGER(), VARCHAR()});
  auto projectedNimbleSchema = convertToNimbleType(*projectedVeloxType);
  DeserializerOptions deserOptions;
  deserOptions.hasHeader = true;
  Deserializer deserializer(
      projectedNimbleSchema, leafPool_.get(), deserOptions);

  VectorPtr deserialized;
  deserializer.deserialize(
      std::string_view(
          reinterpret_cast<const char*>(coalesced.data()), coalesced.length()),
      deserialized);
  ASSERT_NE(deserialized, nullptr);

  auto* rowResult = deserialized->as<RowVector>();
  ASSERT_NE(rowResult, nullptr);
  // Over-fetch: Deserializer returns the full stripe; the requested row
  // sits at position rowRange.startRow within the output.
  ASSERT_GE(rowResult->size(), rowRange.endRow);
  auto* colAResult = rowResult->childAt(0)->as<FlatVector<int32_t>>();
  ASSERT_NE(colAResult, nullptr);
  // key=50 -> colA=500
  EXPECT_EQ(colAResult->valueAt(rowRange.startRow), 500);
}

TEST_P(NimbleIndexProjectorTest, emptyResult) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  // Keys: 0, 10, 20, ..., 90
  const int numRows = 10;
  std::vector<int64_t> keys(numRows);
  std::vector<int32_t> values(numRows);
  for (int i = 0; i < numRows; ++i) {
    keys[i] = i * 10;
    values[i] = i;
  }

  auto batch = vectorMaker_->rowVector(
      {"key", "value"},
      {vectorMaker_->flatVector<int64_t>(keys),
       vectorMaker_->flatVector<int32_t>(values)});

  writeData({batch}, {"key"});

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  // Lookup key=1000, which doesn't exist (beyond max).
  auto bounds = makePointLookup(rowType, {"key"}, 1000);
  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds};
  auto result = projector->project(request, {});

  ASSERT_EQ(result.responses.size(), 1);
  EXPECT_TRUE(result.responses[0].chunks.empty());
  EXPECT_FALSE(result.responses[0].resumeKey.has_value());

  // All stats should be zero for a miss.
  EXPECT_EQ(projector->stats().numReadStripes, 0);
  EXPECT_EQ(projector->stats().numScannedRows, 0);
  EXPECT_EQ(projector->stats().numProjectedRows, 0);
  EXPECT_EQ(projector->stats().numReadRows, 0);
  EXPECT_EQ(projector->stats().rawBytesRead, 0);
  EXPECT_EQ(projector->stats().rawOverreadBytes, 0);
}

TEST_P(NimbleIndexProjectorTest, multipleRequests) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  const int numRows = 100;
  std::vector<int64_t> keys(numRows);
  std::vector<int32_t> values(numRows);
  for (int i = 0; i < numRows; ++i) {
    keys[i] = i;
    values[i] = i * 10;
  }

  auto batch = vectorMaker_->rowVector(
      {"key", "value"},
      {vectorMaker_->flatVector<int64_t>(keys),
       vectorMaker_->flatVector<int32_t>(values)});

  writeData({batch}, {"key"});

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  // Multiple point lookups.
  auto bounds0 = makePointLookup(rowType, {"key"}, 5);
  auto bounds1 = makePointLookup(rowType, {"key"}, 50);
  auto bounds2 = makePointLookup(rowType, {"key"}, 95);

  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds0, bounds1, bounds2};
  auto result = projector->project(request, {});

  ASSERT_EQ(result.responses.size(), 3);
  uint64_t totalBytes = 0;
  for (const auto& response : result.responses) {
    for (const auto& chunkSlice : response.chunks) {
      totalBytes += chunkSlice.computeChainDataLength();
    }
  }
  EXPECT_GT(totalBytes, 0);

  // All requests should have results (no misses).
  for (const auto& response : result.responses) {
    EXPECT_FALSE(response.chunks.empty());
    EXPECT_FALSE(response.resumeKey.has_value());
  }
}

TEST_P(NimbleIndexProjectorTest, overlappingRequestsShareBody) {
  // Two range scans that hit the same single stripe with different row ranges.
  // Verify that the per-request kTabletRaw chunk slices share the body+trailer
  // IOBuf bytes via cloneOne() and carry distinct per-slice header nodes.
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  const int numRows = 100;
  std::vector<int64_t> keys(numRows);
  std::vector<int32_t> values(numRows);
  for (int i = 0; i < numRows; ++i) {
    keys[i] = i;
    values[i] = i * 10;
  }
  auto batch = vectorMaker_->rowVector(
      {"key", "value"},
      {vectorMaker_->flatVector<int64_t>(keys),
       vectorMaker_->flatVector<int32_t>(values)});

  // Single stripe with all 100 rows (default 4KB stripe size easily fits).
  writeData({batch}, {"key"});

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  // Two overlapping range scans into the same stripe.
  auto bounds0 = makeRangeLookup(rowType, {"key"}, 10, 40); // 30 rows
  auto bounds1 = makeRangeLookup(rowType, {"key"}, 25, 70); // 45 rows

  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds0, bounds1};
  auto result = projector->project(request, {});

  ASSERT_EQ(result.responses.size(), 2);
  ASSERT_EQ(result.responses[0].chunks.size(), 1);
  ASSERT_EQ(result.responses[1].chunks.size(), 1);

  const auto& chunkSlice0 = result.responses[0].chunks[0];
  const auto& chunkSlice1 = result.responses[1].chunks[0];

  // Each chunk slice is a 2-node chain: per-slice header + shared body+trailer.
  ASSERT_TRUE(chunkSlice0.isChained());
  ASSERT_TRUE(chunkSlice1.isChained());

  // Header nodes (first node in each chunk slice) must be DISTINCT
  // allocations since they carry per-request row range bytes.
  EXPECT_NE(chunkSlice0.data(), chunkSlice1.data());

  // Body nodes (second node in each chunk slice) must point to the same
  // underlying memory because they are cloneOne() copies of the shared
  // per-stripe body+trailer.
  EXPECT_EQ(chunkSlice0.prev()->data(), chunkSlice1.prev()->data());
  EXPECT_EQ(chunkSlice0.prev()->length(), chunkSlice1.prev()->length());

  // Embedded row ranges match the (stripe-relative) request ranges.
  const auto range0 = readEmbeddedRowRange(chunkSlice0);
  const auto range1 = readEmbeddedRowRange(chunkSlice1);
  EXPECT_EQ(range0, RowRange(10, 40));
  EXPECT_EQ(range1, RowRange(25, 70));

  // Neither response is truncated, so neither slice carries a resume key.
  EXPECT_FALSE(readEmbeddedResumeKey(chunkSlice0).has_value());
  EXPECT_FALSE(readEmbeddedResumeKey(chunkSlice1).has_value());

  // The shared body dominates each chunk slice size — assert that
  // computeChainDataLength reflects header + body and that the body is
  // refcount-shared.
  EXPECT_GT(chunkSlice0.computeChainDataLength(), chunkSlice0.length());
  EXPECT_GT(chunkSlice0.prev()->length(), 0u);
}

// Round-trips a single-batch deserialize over a key range. The Deserializer
// over-fetches (decodes the full stripe rather than slicing to the row
// range), so output->size() is the stripe rowCount and we verify the
// in-range subset has the correct values. Used by the four positional range
// tests below (start / middle / end / full).
//
// Defined as a macro (rather than a function) so it can access the protected
// fixture members from inside the TEST_P body.
#define RUN_DESERIALIZER_RANGE_TEST(lowerKey_, upperKey_)                      \
  do {                                                                         \
    auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});               \
    const int numRows = 100;                                                   \
    std::vector<int64_t> keys(numRows);                                        \
    std::vector<int32_t> values(numRows);                                      \
    for (int i = 0; i < numRows; ++i) {                                        \
      keys[i] = i;                                                             \
      values[i] = i * 10;                                                      \
    }                                                                          \
    auto batch = vectorMaker_->rowVector(                                      \
        {"key", "value"},                                                      \
        {vectorMaker_->flatVector<int64_t>(keys),                              \
         vectorMaker_->flatVector<int32_t>(values)});                          \
    writeData({batch}, {"key"});                                               \
    std::vector<Subfield> subfields;                                           \
    subfields.emplace_back("value");                                           \
    auto projector = createProjector(subfields);                               \
    auto bounds = makeRangeLookup(rowType, {"key"}, (lowerKey_), (upperKey_)); \
    NimbleIndexProjector::Request request;                                     \
    request.keyBounds = {bounds};                                              \
    auto result = projector->project(request, {});                             \
    ASSERT_EQ(result.responses[0].chunks.size(), 1);                           \
    auto coalesced = result.responses[0].chunks[0].cloneCoalescedAsValue();    \
    auto bytes = std::string_view(                                             \
        reinterpret_cast<const char*>(coalesced.data()), coalesced.length());  \
    DeserializerOptions deserOptions;                                          \
    deserOptions.hasHeader = true;                                             \
    Deserializer deserializer(                                                 \
        projector->projectedNimbleType(), leafPool_.get(), deserOptions);      \
    std::vector<std::string_view> singleBatch{bytes};                          \
    VectorPtr output;                                                          \
    deserializer.deserialize(singleBatch, output);                             \
    ASSERT_EQ(output->size(), static_cast<uint32_t>(numRows));                 \
    auto* rowVec = output->as<velox::RowVector>();                             \
    auto* valueVec = rowVec->childAt(0)->as<velox::FlatVector<int32_t>>();     \
    /* Verify the in-range subset has expected values. */                      \
    for (uint32_t i = static_cast<uint32_t>(lowerKey_);                        \
         i < static_cast<uint32_t>(upperKey_);                                 \
         ++i) {                                                                \
      EXPECT_EQ(valueVec->valueAt(i), static_cast<int32_t>(i * 10))            \
          << "i=" << i;                                                        \
    }                                                                          \
  } while (0)

TEST_P(NimbleIndexProjectorTest, deserializerRangeAtStart) {
  // Range covers the first 30 rows of a 100-row stripe: [0, 30).
  RUN_DESERIALIZER_RANGE_TEST(0, 30);
}

TEST_P(NimbleIndexProjectorTest, deserializerRangeInMiddle) {
  // Range covers a 30-row window strictly inside the stripe: [30, 60).
  RUN_DESERIALIZER_RANGE_TEST(30, 60);
}

TEST_P(NimbleIndexProjectorTest, deserializerRangeAtEnd) {
  // Range covers the last 30 rows of a 100-row stripe: [70, 100).
  RUN_DESERIALIZER_RANGE_TEST(70, 100);
}

TEST_P(NimbleIndexProjectorTest, deserializerRangeFullStripe) {
  // Range spans the entire stripe: [0, 100). Validates round-trip on a
  // full-stripe row range (boundary case where the in-range subset equals
  // the over-fetched stripe).
  RUN_DESERIALIZER_RANGE_TEST(0, 100);
}

#undef RUN_DESERIALIZER_RANGE_TEST

TEST_P(NimbleIndexProjectorTest, deserializerMultiBatchWithRowRange) {
  // Each input batch's embedded row range is applied per-segment by the
  // Deserializer; the output is the concatenation of each batch's in-range
  // rows. Same chunk slice passed twice yields 2 × range.numRows() output
  // rows, with values[10..39] repeated.
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  const int numRows = 100;
  std::vector<int64_t> keys(numRows);
  std::vector<int32_t> values(numRows);
  for (int i = 0; i < numRows; ++i) {
    keys[i] = i;
    values[i] = i * 10;
  }
  auto batch = vectorMaker_->rowVector(
      {"key", "value"},
      {vectorMaker_->flatVector<int64_t>(keys),
       vectorMaker_->flatVector<int32_t>(values)});
  writeData({batch}, {"key"});

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  auto bounds = makeRangeLookup(rowType, {"key"}, 10, 40);
  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds};
  auto result = projector->project(request, {});
  ASSERT_EQ(result.responses[0].chunks.size(), 1);

  auto coalesced = result.responses[0].chunks[0].cloneCoalescedAsValue();
  auto bytes = std::string_view(
      reinterpret_cast<const char*>(coalesced.data()), coalesced.length());

  DeserializerOptions deserOptions;
  deserOptions.hasHeader = true;
  Deserializer deserializer(
      projector->projectedNimbleType(), leafPool_.get(), deserOptions);

  std::vector<std::string_view> multiBatch{bytes, bytes};
  VectorPtr output;
  deserializer.deserialize(multiBatch, output);

  // Over-fetch: each batch decodes its full 100 stripe rows; concatenation
  // gives 200 rows. The in-range subset (positions [10, 40) within each
  // batch) carries values[10..39].
  ASSERT_EQ(output->size(), 200u);
  auto* rowVec = output->as<velox::RowVector>();
  auto* valueVec = rowVec->childAt(0)->as<velox::FlatVector<int32_t>>();
  for (uint32_t i = 10; i < 40; ++i) {
    EXPECT_EQ(valueVec->valueAt(i), static_cast<int32_t>(i * 10))
        << "batch=0 i=" << i;
    EXPECT_EQ(valueVec->valueAt(100 + i), static_cast<int32_t>(i * 10))
        << "batch=1 i=" << i;
  }
}

TEST_P(NimbleIndexProjectorTest, deserializerMultiBatchMixedRanges) {
  // Multi-batch deserialize where each batch carries a different positional
  // row range: start [0, 20), middle [30, 50), end [80, 100), and full
  // [0, 100). Output is the concatenation of in-range rows from each batch,
  // preserving order.
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  const int numRows = 100;
  std::vector<int64_t> keys(numRows);
  std::vector<int32_t> values(numRows);
  for (int i = 0; i < numRows; ++i) {
    keys[i] = i;
    values[i] = i * 10;
  }
  auto batch = vectorMaker_->rowVector(
      {"key", "value"},
      {vectorMaker_->flatVector<int64_t>(keys),
       vectorMaker_->flatVector<int32_t>(values)});
  writeData({batch}, {"key"});

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  // One projector call with four independent range requests; each produces
  // its own chunk slice (one per request × stripe intersection).
  NimbleIndexProjector::Request request;
  request.keyBounds = {
      makeRangeLookup(rowType, {"key"}, 0, 20),
      makeRangeLookup(rowType, {"key"}, 30, 50),
      makeRangeLookup(rowType, {"key"}, 80, 100),
      makeRangeLookup(rowType, {"key"}, 0, 100),
  };
  auto result = projector->project(request, {});
  ASSERT_EQ(result.responses.size(), 4);

  // Coalesce each response's chunk slice into a contiguous buffer the
  // deserializer can read from. The owned IOBufs must outlive the
  // string_views below — keep them in a parallel vector.
  std::vector<folly::IOBuf> ownedSlices;
  ownedSlices.reserve(4);
  std::vector<std::string_view> batches;
  batches.reserve(4);
  for (auto& response : result.responses) {
    ASSERT_EQ(response.chunks.size(), 1u);
    ownedSlices.push_back(response.chunks[0].cloneCoalescedAsValue());
    batches.emplace_back(
        reinterpret_cast<const char*>(ownedSlices.back().data()),
        ownedSlices.back().length());
  }

  DeserializerOptions deserOptions;
  deserOptions.hasHeader = true;
  Deserializer deserializer(
      projector->projectedNimbleType(), leafPool_.get(), deserOptions);
  VectorPtr output;
  deserializer.deserialize(batches, output);

  // Over-fetch: each batch decodes its full 100 stripe rows; concatenation
  // gives 400 rows = 4 × 100. Verify each batch's in-range subset has the
  // expected values at its position within the cumulative output.
  ASSERT_EQ(output->size(), 400u);
  auto* rowVec = output->as<velox::RowVector>();
  auto* valueVec = rowVec->childAt(0)->as<velox::FlatVector<int32_t>>();
  // Batch 0: range [0, 20), batch starts at output offset 0.
  for (uint32_t i = 0; i < 20; ++i) {
    EXPECT_EQ(valueVec->valueAt(i), static_cast<int32_t>(i * 10))
        << "batch=0 i=" << i;
  }
  // Batch 1: range [30, 50), batch starts at output offset 100.
  for (uint32_t i = 30; i < 50; ++i) {
    EXPECT_EQ(valueVec->valueAt(100 + i), static_cast<int32_t>(i * 10))
        << "batch=1 i=" << i;
  }
  // Batch 2: range [80, 100), batch starts at output offset 200.
  for (uint32_t i = 80; i < 100; ++i) {
    EXPECT_EQ(valueVec->valueAt(200 + i), static_cast<int32_t>(i * 10))
        << "batch=2 i=" << i;
  }
  // Batch 3: range [0, 100) (full), batch starts at output offset 300.
  for (uint32_t i = 0; i < 100; ++i) {
    EXPECT_EQ(valueVec->valueAt(300 + i), static_cast<int32_t>(i * 10))
        << "batch=3 i=" << i;
  }
}

TEST_P(NimbleIndexProjectorTest, deserializerMultiBatchMixedVersions) {
  // Mix a kTabletRaw chunk slice (projector output, narrow row range) with
  // a kCompactRaw chunk (Serializer output, no row range) in one deserialize
  // call. Verifies the no-range branch of the per-batch loop: when
  // batchRanges[i] is nullopt, numRowsInBatch falls back to batchRows
  // (the full kCompactRaw batch).
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  const int numRows = 100;
  std::vector<int64_t> keys(numRows);
  std::vector<int32_t> values(numRows);
  for (int i = 0; i < numRows; ++i) {
    keys[i] = i;
    values[i] = i * 10;
  }
  auto batch = vectorMaker_->rowVector(
      {"key", "value"},
      {vectorMaker_->flatVector<int64_t>(keys),
       vectorMaker_->flatVector<int32_t>(values)});
  writeData({batch}, {"key"});

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  // kTabletRaw batch: range [10, 40) = 30 rows.
  auto bounds = makeRangeLookup(rowType, {"key"}, 10, 40);
  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds};
  auto result = projector->project(request, {});
  ASSERT_EQ(result.responses[0].chunks.size(), 1u);
  auto tabletRawOwned = result.responses[0].chunks[0].cloneCoalescedAsValue();

  // kCompactRaw batch: serialize a 5-row Row<INTEGER> vector matching the
  // projected schema. The Serializer is constructed against the same velox
  // type the Deserializer will use, so the streams encode in lockstep.
  auto projectedVeloxType =
      convertToVeloxType(*projector->projectedNimbleType());
  std::vector<int32_t> kCompactRawValues = {1000, 2000, 3000, 4000, 5000};
  auto kCompactRawRow = vectorMaker_->rowVector(
      {"value"}, {vectorMaker_->flatVector<int32_t>(kCompactRawValues)});
  SerializerOptions serOptions{
      .version = SerializationVersion::kCompactRaw,
  };
  Serializer ser{
      serOptions,
      projectedVeloxType,
      leafPool_.get(),
  };
  auto kCompactRawBytes = ser.serialize(
      kCompactRawRow,
      OrderedRanges::of(0, static_cast<uint32_t>(kCompactRawValues.size())));

  DeserializerOptions deserOptions;
  deserOptions.hasHeader = true;
  Deserializer deserializer(
      projector->projectedNimbleType(), leafPool_.get(), deserOptions);

  std::vector<std::string_view> batches{
      std::string_view(
          reinterpret_cast<const char*>(tabletRawOwned.data()),
          tabletRawOwned.length()),
      kCompactRawBytes,
  };
  VectorPtr output;
  deserializer.deserialize(batches, output);

  // Over-fetch: kTabletRaw batch decodes its full 100 stripe rows;
  // kCompactRaw batch contributes 5 rows. Concatenation = 105 rows. The
  // in-range subset of the kTabletRaw batch (positions [10, 40) within the
  // first 100) carries values[10..39]; the kCompactRaw batch's 5 rows follow.
  ASSERT_EQ(output->size(), 105u);
  auto* rowVec = output->as<velox::RowVector>();
  auto* valueVec = rowVec->childAt(0)->as<velox::FlatVector<int32_t>>();
  for (uint32_t i = 10; i < 40; ++i) {
    EXPECT_EQ(valueVec->valueAt(i), static_cast<int32_t>(i * 10))
        << "kTabletRaw i=" << i;
  }
  for (uint32_t i = 0; i < 5; ++i) {
    EXPECT_EQ(valueVec->valueAt(100 + i), kCompactRawValues[i])
        << "kCompactRaw i=" << i;
  }
}

TEST_P(NimbleIndexProjectorTest, stats) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  // Write 4 batches of 10 rows each. The flush policy flushes every batch,
  // producing exactly 4 stripes with deterministic row counts.
  const int numBatches = 4;
  const int rowsPerBatch = 10;
  const int numRows = numBatches * rowsPerBatch;
  std::vector<RowVectorPtr> batches;
  for (int b = 0; b < numBatches; ++b) {
    std::vector<int64_t> keys(rowsPerBatch);
    std::vector<int32_t> values(rowsPerBatch);
    for (int i = 0; i < rowsPerBatch; ++i) {
      keys[i] = b * rowsPerBatch + i;
      values[i] = keys[i] * 10;
    }
    batches.emplace_back(vectorMaker_->rowVector(
        {"key", "value"},
        {vectorMaker_->flatVector<int64_t>(keys),
         vectorMaker_->flatVector<int32_t>(values)}));
  }

  // Flush every batch to get exactly numBatches stripes.
  writeData(batches, {"key"}, {}, /*stripeSize=*/1);

  auto makeProjector = [&]() {
    std::vector<Subfield> subs;
    subs.emplace_back("value");
    return createProjector(subs);
  };

  // Empty request should throw.
  {
    auto proj = makeProjector();
    NimbleIndexProjector::Request request;
    NIMBLE_ASSERT_THROW(
        proj->project(request, {}), "keyBounds must not be empty");
  }

  // Single point lookup: key=15 is in stripe 1 (rows 10..19).
  {
    auto proj = makeProjector();
    auto bounds = makePointLookup(rowType, {"key"}, 15);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    proj->project(request, {});

    EXPECT_EQ(proj->stats().numReadStripes, 1);
    EXPECT_EQ(proj->stats().numScannedRows, rowsPerBatch);
    EXPECT_EQ(proj->stats().numProjectedRows, proj->stats().numScannedRows);
    EXPECT_EQ(proj->stats().numReadRows, 1);

    // Timings should be non-zero for a hit.
    EXPECT_GT(proj->stats().lookupTiming.count, 0);
    EXPECT_GT(proj->stats().lookupTiming.wallNanos, 0);
    EXPECT_GT(proj->stats().scanTiming.wallNanos, 0);
    EXPECT_GT(proj->stats().projectionTiming.wallNanos, 0);

    // IO stats should be non-zero for a hit.
    EXPECT_GT(proj->stats().rawBytesRead, 0);
    EXPECT_GE(proj->stats().rawOverreadBytes, 0);
  }

  // Two point lookups in first and last stripes.
  {
    auto proj = makeProjector();
    auto bounds0 = makePointLookup(rowType, {"key"}, 0);
    auto bounds1 = makePointLookup(rowType, {"key"}, numRows - 1);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds0, bounds1};
    proj->project(request, {});

    EXPECT_EQ(proj->stats().numReadStripes, 2);
    EXPECT_EQ(
        proj->stats().numScannedRows, static_cast<uint64_t>(2 * rowsPerBatch));
    EXPECT_EQ(proj->stats().numProjectedRows, proj->stats().numScannedRows);
    EXPECT_EQ(proj->stats().numReadRows, 2);

    // IO stats should reflect more data read than single lookup.
    EXPECT_GT(proj->stats().rawBytesRead, 0);
  }

  // Point lookup for a missing key (beyond max) should yield zero stats.
  {
    auto proj = makeProjector();
    auto bounds = makePointLookup(rowType, {"key"}, numRows + 1'000);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    proj->project(request, {});

    EXPECT_EQ(proj->stats().numReadStripes, 0);
    EXPECT_EQ(proj->stats().numScannedRows, 0);
    EXPECT_EQ(proj->stats().numProjectedRows, 0);
    EXPECT_EQ(proj->stats().numReadRows, 0);
  }
}

TEST_P(NimbleIndexProjectorTest, statsToString) {
  NimbleIndexProjector::Stats stats;
  stats.numReadStripes = 3;
  stats.numScannedRows = 1'000;
  stats.numProjectedRows = 1'000;
  stats.numReadRows = 42;
  stats.numReadBytes = 8'192;
  stats.rawBytesRead = 10'000;
  stats.rawOverreadBytes = 1'808;
  stats.numStorageReads = 5;
  stats.numCacheHits = 7;
  stats.cacheHitBytes = 4'096;
  EXPECT_EQ(
      stats.toString(),
      "Stats(numReadStripes=3, numScannedRows=1000, numProjectedRows=1000, numReadRows=42, "
      "numReadBytes=8.00KB, rawBytesRead=9.77KB, rawOverreadBytes=1.77KB, numStorageReads=5, "
      "numCacheHits=7, cacheHitBytes=4.00KB, "
      "lookupTiming=[count: 0, wallTime: 0ns, cpuTime: 0ns], "
      "scanTiming=[count: 0, wallTime: 0ns, cpuTime: 0ns], "
      "projectionTiming=[count: 0, wallTime: 0ns, cpuTime: 0ns])");
}

TEST_P(NimbleIndexProjectorTest, flatMapProjection) {
  // Schema: key (int64, sorted), features (MAP<VARCHAR, BIGINT> as FlatMap).
  // FlatMap keys in the file: "a", "b", "c", "d", "e".
  // Project keys in non-alphabetical order: ["d", "b", "e"] to verify that
  // projectedNimbleType() stream ordering matches the serialized data.
  auto rowType = ROW({"key", "features"}, {BIGINT(), MAP(VARCHAR(), BIGINT())});

  const int numRows = 200;
  const std::vector<std::string> mapKeys = {"a", "b", "c", "d", "e"};
  const auto numMapKeys = static_cast<vector_size_t>(mapKeys.size());

  std::vector<StringView> mapKeyViews;
  mapKeyViews.reserve(mapKeys.size());
  for (const auto& k : mapKeys) {
    mapKeyViews.emplace_back(k);
  }

  auto batch = vectorMaker_->rowVector(
      {"key", "features"},
      {vectorMaker_->flatVector<int64_t>(
           numRows, [](auto row) { return row * 10; }),
       vectorMaker_->mapVector<StringView, int64_t>(
           numRows,
           /*sizeAt*/ [&](auto /*row*/) { return numMapKeys; },
           /*keyAt*/
           [&](auto /*row*/, auto mapIndex) { return mapKeyViews[mapIndex]; },
           /*valueAt*/
           [](auto row, auto mapIndex) {
             return static_cast<int64_t>(row * 100 + mapIndex);
           })});

  writeData({batch}, {"key"}, {{"features", {}}});

  // Project keys in non-alphabetical order to exercise FlatMap sorting.
  std::vector<Subfield> subfields;
  subfields.emplace_back("features[\"d\"]");
  subfields.emplace_back("features[\"b\"]");
  subfields.emplace_back("features[\"e\"]");
  auto projector = createProjector(subfields);

  // Point lookup for key=50 (row index 5).
  auto pointBounds = makePointLookup(rowType, {"key"}, 50);
  NimbleIndexProjector::Request request;
  request.keyBounds = {pointBounds};
  auto result = projector->project(request, {});

  ASSERT_EQ(result.responses.size(), 1);
  const auto& response = result.responses[0];
  ASSERT_FALSE(response.chunks.empty());

  const auto& chunkSlice = response.chunks[0];
  const auto rowRange = readEmbeddedRowRange(chunkSlice);
  ASSERT_GT(rowRange.numRows(), 0);

  // Deserialize using the projector's projected nimble type which accounts
  // for FlatMap key sorting (keys sorted alphabetically: "b", "d", "e").
  auto coalesced = coalesceChunkSlice(chunkSlice);
  DeserializerOptions deserOptions;
  deserOptions.hasHeader = true;
  Deserializer deserializer(
      projector->projectedNimbleType(), leafPool_.get(), deserOptions);

  VectorPtr deserialized;
  deserializer.deserialize(
      std::string_view(
          reinterpret_cast<const char*>(coalesced.data()), coalesced.length()),
      deserialized);
  ASSERT_NE(deserialized, nullptr);

  auto* rowResult = deserialized->as<RowVector>();
  ASSERT_NE(rowResult, nullptr);
  ASSERT_EQ(rowResult->childrenSize(), 1);

  // FlatMap is deserialized as a MapVector. Extract keys and values at the
  // target row to verify correctness. The Deserializer over-fetches (decodes
  // the full stripe rather than slicing), so the requested row sits at
  // position rowRange.startRow within the output.
  auto* mapResult = rowResult->childAt(0)->as<MapVector>();
  ASSERT_NE(mapResult, nullptr);
  ASSERT_GE(mapResult->size(), rowRange.endRow);

  const vector_size_t targetRow = rowRange.startRow;

  auto* mapKeyResults = mapResult->mapKeys()->as<FlatVector<StringView>>();
  auto* mapValues = mapResult->mapValues()->as<FlatVector<int64_t>>();
  ASSERT_NE(mapKeyResults, nullptr);
  ASSERT_NE(mapValues, nullptr);

  // Collect key-value pairs at the target row.
  const auto mapOffset = mapResult->offsetAt(targetRow);
  const auto mapSize = mapResult->sizeAt(targetRow);

  // row=5: key=50, features[k] = 5*100 + index_of(k).
  std::map<std::string, int64_t> kvPairs;
  for (vector_size_t i = 0; i < mapSize; ++i) {
    kvPairs[std::string(mapKeyResults->valueAt(mapOffset + i))] =
        mapValues->valueAt(mapOffset + i);
  }

  // Verify projected keys: "b"(idx=1), "d"(idx=3), "e"(idx=4).
  ASSERT_EQ(kvPairs.size(), 3);
  EXPECT_EQ(kvPairs["b"], 501); // 5*100 + 1
  EXPECT_EQ(kvPairs["d"], 503); // 5*100 + 3
  EXPECT_EQ(kvPairs["e"], 504); // 5*100 + 4
}

TEST_P(NimbleIndexProjectorTest, flatMapProjectionWithNarrowRowRange) {
  // Schema: key (int64, sorted), features (MAP<VARCHAR, BIGINT> as FlatMap).
  // Range scan over keys [50, 100) — 5 rows starting at row index 5 (since
  // keys are row*10). The Deserializer over-fetches and returns the full
  // 200-row stripe; we verify the in-range subset at positions [5, 10).
  auto rowType = ROW({"key", "features"}, {BIGINT(), MAP(VARCHAR(), BIGINT())});

  const int numRows = 200;
  const std::vector<std::string> mapKeys = {"a", "b", "c"};
  const auto numMapKeys = static_cast<vector_size_t>(mapKeys.size());

  std::vector<StringView> mapKeyViews;
  mapKeyViews.reserve(mapKeys.size());
  for (const auto& k : mapKeys) {
    mapKeyViews.emplace_back(k);
  }

  auto batch = vectorMaker_->rowVector(
      {"key", "features"},
      {vectorMaker_->flatVector<int64_t>(
           numRows, [](auto row) { return row * 10; }),
       vectorMaker_->mapVector<StringView, int64_t>(
           numRows,
           /*sizeAt*/ [&](auto /*row*/) { return numMapKeys; },
           /*keyAt*/
           [&](auto /*row*/, auto mapIndex) { return mapKeyViews[mapIndex]; },
           /*valueAt*/
           [](auto row, auto mapIndex) {
             return static_cast<int64_t>(row * 100 + mapIndex);
           })});

  writeData({batch}, {"key"}, {{"features", {}}});

  std::vector<Subfield> subfields;
  subfields.emplace_back("features[\"a\"]");
  subfields.emplace_back("features[\"c\"]");
  auto projector = createProjector(subfields);

  // Range [50, 100) → rows 5..9 (5 rows).
  auto bounds = makeRangeLookup(rowType, {"key"}, 50, 100);
  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds};
  auto result = projector->project(request, {});
  ASSERT_EQ(result.responses.size(), 1);
  ASSERT_EQ(result.responses[0].chunks.size(), 1u);

  const auto& chunkSlice = result.responses[0].chunks[0];
  const auto rowRange = readEmbeddedRowRange(chunkSlice);
  ASSERT_EQ(rowRange.numRows(), 5u);

  auto coalesced = coalesceChunkSlice(chunkSlice);
  DeserializerOptions deserOptions;
  deserOptions.hasHeader = true;
  Deserializer deserializer(
      projector->projectedNimbleType(), leafPool_.get(), deserOptions);

  VectorPtr deserialized;
  deserializer.deserialize(
      std::string_view(
          reinterpret_cast<const char*>(coalesced.data()), coalesced.length()),
      deserialized);
  ASSERT_NE(deserialized, nullptr);
  // Over-fetch: full stripe is decoded.
  ASSERT_EQ(deserialized->size(), 200u);

  auto* rowResult = deserialized->as<RowVector>();
  ASSERT_NE(rowResult, nullptr);
  auto* mapResult = rowResult->childAt(0)->as<MapVector>();
  ASSERT_NE(mapResult, nullptr);
  ASSERT_EQ(mapResult->size(), 200u);

  auto* mapKeyResults = mapResult->mapKeys()->as<FlatVector<StringView>>();
  auto* mapValues = mapResult->mapValues()->as<FlatVector<int64_t>>();
  ASSERT_NE(mapKeyResults, nullptr);
  ASSERT_NE(mapValues, nullptr);

  // Verify the in-range subset (rows 5..9): both projected FlatMap keys
  // "a" (idx 0) and "c" (idx 2) are present with values row*100 + index.
  for (vector_size_t row = 5; row < 10; ++row) {
    const auto mapOffset = mapResult->offsetAt(row);
    const auto mapSize = mapResult->sizeAt(row);
    ASSERT_EQ(mapSize, 2) << "row=" << row;
    std::map<std::string, int64_t> kv;
    for (vector_size_t k = 0; k < mapSize; ++k) {
      kv[std::string(mapKeyResults->valueAt(mapOffset + k))] =
          mapValues->valueAt(mapOffset + k);
    }
    EXPECT_EQ(kv["a"], row * 100 + 0) << "row=" << row;
    EXPECT_EQ(kv["c"], row * 100 + 2) << "row=" << row;
  }
}

TEST_P(NimbleIndexProjectorTest, flatMapFullColumnProjection) {
  // Full FlatMap projection (no key subscripts) is not supported —
  // buildProjectedNimbleType requires explicit key selection for FlatMap.
  auto rowType = ROW({"key", "features"}, {BIGINT(), MAP(VARCHAR(), BIGINT())});

  const int numRows = 200;
  const std::vector<std::string> mapKeys = {"a", "b", "c", "d", "e"};
  const auto numMapKeys = static_cast<vector_size_t>(mapKeys.size());

  std::vector<StringView> mapKeyViews;
  mapKeyViews.reserve(mapKeys.size());
  for (const auto& k : mapKeys) {
    mapKeyViews.emplace_back(k);
  }

  auto batch = vectorMaker_->rowVector(
      {"key", "features"},
      {vectorMaker_->flatVector<int64_t>(
           numRows, [](auto row) { return row * 10; }),
       vectorMaker_->mapVector<StringView, int64_t>(
           numRows,
           /*sizeAt*/ [&](auto /*row*/) { return numMapKeys; },
           /*keyAt*/
           [&](auto /*row*/, auto mapIndex) { return mapKeyViews[mapIndex]; },
           /*valueAt*/
           [](auto row, auto mapIndex) {
             return static_cast<int64_t>(row * 100 + mapIndex);
           })});

  writeData({batch}, {"key"}, {{"features", {}}});

  // Project entire column (no key subscripts) — should fail.
  std::vector<Subfield> subfields;
  subfields.emplace_back("features");
  NIMBLE_ASSERT_THROW(
      createProjector(subfields),
      "Cannot project entire FlatMap column without key subscripts");
}

TEST_P(NimbleIndexProjectorTest, flatMapKeyProjectionSchemaComparison) {
  // Verify that buildProjectedNimbleType (nimble-type overload) produces
  // FlatMap with keys sorted alphabetically for key-level projection.
  auto rowType = ROW({"key", "features"}, {BIGINT(), MAP(VARCHAR(), BIGINT())});

  const int numRows = 200;
  const std::vector<std::string> mapKeys = {"a", "b", "c", "d", "e"};
  const auto numMapKeys = static_cast<vector_size_t>(mapKeys.size());

  std::vector<StringView> mapKeyViews;
  mapKeyViews.reserve(mapKeys.size());
  for (const auto& k : mapKeys) {
    mapKeyViews.emplace_back(k);
  }

  auto batch = vectorMaker_->rowVector(
      {"key", "features"},
      {vectorMaker_->flatVector<int64_t>(
           numRows, [](auto row) { return row * 10; }),
       vectorMaker_->mapVector<StringView, int64_t>(
           numRows,
           /*sizeAt*/ [&](auto /*row*/) { return numMapKeys; },
           /*keyAt*/
           [&](auto /*row*/, auto mapIndex) { return mapKeyViews[mapIndex]; },
           /*valueAt*/
           [](auto row, auto mapIndex) {
             return static_cast<int64_t>(row * 100 + mapIndex);
           })});

  writeData({batch}, {"key"}, {{"features", {}}});

  // Project keys in non-alphabetical order: ["d", "b"].
  std::vector<Subfield> subfields;
  subfields.emplace_back("features[\"d\"]");
  subfields.emplace_back("features[\"b\"]");
  auto projector = createProjector(subfields);

  // Projected schema should be FlatMap with keys sorted alphabetically.
  const auto& buildSchema = projector->projectedNimbleType();
  ASSERT_TRUE(buildSchema->isRow());
  ASSERT_EQ(buildSchema->asRow().childrenCount(), 1);

  const auto& buildFlatMap = buildSchema->asRow().childAt(0)->asFlatMap();
  ASSERT_EQ(buildFlatMap.childrenCount(), 2);

  // Keys should be sorted alphabetically: "b", "d".
  EXPECT_EQ(buildFlatMap.nameAt(0), "b");
  EXPECT_EQ(buildFlatMap.nameAt(1), "d");

  // Verify schema decodes the projected data correctly.
  auto pointBounds = makePointLookup(rowType, {"key"}, 50);
  NimbleIndexProjector::Request request;
  request.keyBounds = {pointBounds};
  auto result = projector->project(request, {});

  ASSERT_EQ(result.responses.size(), 1);
  ASSERT_FALSE(result.responses[0].chunks.empty());

  const auto& chunkSlice = result.responses[0].chunks[0];
  auto coalesced = coalesceChunkSlice(chunkSlice);
  auto data = std::string_view(
      reinterpret_cast<const char*>(coalesced.data()), coalesced.length());

  DeserializerOptions deserOptions;
  deserOptions.hasHeader = true;
  Deserializer deserializer(buildSchema, leafPool_.get(), deserOptions);
  VectorPtr output;
  deserializer.deserialize(data, output);
  ASSERT_NE(output, nullptr);
  auto* row = output->as<RowVector>();
  auto* map = row->childAt(0)->as<MapVector>();
  ASSERT_NE(map, nullptr);
  EXPECT_GE(map->size(), 1);
}

TEST_P(NimbleIndexProjectorTest, flatMapIntKeyProjection) {
  // Schema: key (int64, sorted), features (MAP<INT, BIGINT> as FlatMap).
  // Tests LongSubscript handling in resolveSubfield: "features[3]" produces
  // a LongSubscript which is converted to string key "3" for FlatMap lookup.
  auto rowType = ROW({"key", "features"}, {BIGINT(), MAP(INTEGER(), BIGINT())});

  const int numRows = 200;
  const int numMapKeys = 5;
  auto batch = vectorMaker_->rowVector(
      {"key", "features"},
      {vectorMaker_->flatVector<int64_t>(
           numRows, [](auto row) { return row * 10; }),
       vectorMaker_->mapVector<int32_t, int64_t>(
           numRows,
           /*sizeAt*/ [&](auto /*row*/) { return numMapKeys; },
           /*keyAt*/
           [](auto /*row*/, auto mapIndex) {
             return static_cast<int32_t>(mapIndex);
           },
           /*valueAt*/
           [](auto row, auto mapIndex) {
             return static_cast<int64_t>(row * 100 + mapIndex);
           })});

  writeData({batch}, {"key"}, {{"features", {}}});

  // Project integer keys in non-sorted order: [3, 1, 4].
  std::vector<Subfield> subfields;
  subfields.emplace_back("features[3]");
  subfields.emplace_back("features[1]");
  subfields.emplace_back("features[4]");
  auto projector = createProjector(subfields);

  // Point lookup for key=50 (row index 5).
  auto pointBounds = makePointLookup(rowType, {"key"}, 50);
  NimbleIndexProjector::Request request;
  request.keyBounds = {pointBounds};
  auto result = projector->project(request, {});

  ASSERT_EQ(result.responses.size(), 1);
  const auto& response = result.responses[0];
  ASSERT_FALSE(response.chunks.empty());

  const auto& chunkSlice = response.chunks[0];
  const auto rowRange = readEmbeddedRowRange(chunkSlice);
  ASSERT_GT(rowRange.numRows(), 0);

  // Deserialize using the projector's projected nimble type.
  auto coalesced = coalesceChunkSlice(chunkSlice);
  DeserializerOptions deserOptions;
  deserOptions.hasHeader = true;
  Deserializer deserializer(
      projector->projectedNimbleType(), leafPool_.get(), deserOptions);

  VectorPtr deserialized;
  deserializer.deserialize(
      std::string_view(
          reinterpret_cast<const char*>(coalesced.data()), coalesced.length()),
      deserialized);
  ASSERT_NE(deserialized, nullptr);

  auto* rowResult = deserialized->as<RowVector>();
  ASSERT_NE(rowResult, nullptr);
  ASSERT_EQ(rowResult->childrenSize(), 1);

  auto* mapResult = rowResult->childAt(0)->as<MapVector>();
  ASSERT_NE(mapResult, nullptr);
  // Over-fetch: full stripe is decoded.
  ASSERT_GE(mapResult->size(), rowRange.endRow);

  // Target row sits at position rowRange.startRow within the output.
  const vector_size_t targetRow = rowRange.startRow;

  auto* mapKeyResults = mapResult->mapKeys()->as<FlatVector<int32_t>>();
  auto* mapValues = mapResult->mapValues()->as<FlatVector<int64_t>>();
  ASSERT_NE(mapKeyResults, nullptr);
  ASSERT_NE(mapValues, nullptr);

  const auto mapOffset = mapResult->offsetAt(targetRow);
  const auto mapSize = mapResult->sizeAt(targetRow);

  // row=5: key=50, features[k] = 5*100 + k.
  std::map<int32_t, int64_t> kvPairs;
  for (vector_size_t i = 0; i < mapSize; ++i) {
    kvPairs[mapKeyResults->valueAt(mapOffset + i)] =
        mapValues->valueAt(mapOffset + i);
  }

  // Verify projected keys: 1, 3, 4.
  ASSERT_EQ(kvPairs.size(), 3);
  EXPECT_EQ(kvPairs[1], 501); // 5*100 + 1
  EXPECT_EQ(kvPairs[3], 503); // 5*100 + 3
  EXPECT_EQ(kvPairs[4], 504); // 5*100 + 4
}

TEST_P(NimbleIndexProjectorTest, flatMapMissingKeys) {
  // Verify that missing FlatMap keys are silently skipped.
  auto rowType = ROW({"key", "features"}, {BIGINT(), MAP(VARCHAR(), BIGINT())});

  const int numRows = 200;
  const std::vector<std::string> mapKeys = {"a", "b", "c"};
  const auto numMapKeys = static_cast<vector_size_t>(mapKeys.size());

  std::vector<StringView> mapKeyViews;
  mapKeyViews.reserve(mapKeys.size());
  for (const auto& k : mapKeys) {
    mapKeyViews.emplace_back(k);
  }

  auto batch = vectorMaker_->rowVector(
      {"key", "features"},
      {vectorMaker_->flatVector<int64_t>(
           numRows, [](auto row) { return row * 10; }),
       vectorMaker_->mapVector<StringView, int64_t>(
           numRows,
           /*sizeAt*/ [&](auto /*row*/) { return numMapKeys; },
           /*keyAt*/
           [&](auto /*row*/, auto mapIndex) { return mapKeyViews[mapIndex]; },
           /*valueAt*/
           [](auto row, auto mapIndex) {
             return static_cast<int64_t>(row * 100 + mapIndex);
           })});

  writeData({batch}, {"key"}, {{"features", {}}});

  // Project mix of existing ("a", "c") and missing ("x", "z") keys.
  {
    std::vector<Subfield> subfields;
    subfields.emplace_back("features[\"a\"]");
    subfields.emplace_back("features[\"x\"]");
    subfields.emplace_back("features[\"c\"]");
    subfields.emplace_back("features[\"z\"]");
    auto projector = createProjector(subfields);

    // Projected schema should only contain existing keys, sorted: "a", "c".
    const auto& schema = projector->projectedNimbleType();
    ASSERT_TRUE(schema->isRow());
    const auto& flatMap = schema->asRow().childAt(0)->asFlatMap();
    ASSERT_EQ(flatMap.childrenCount(), 2);
    EXPECT_EQ(flatMap.nameAt(0), "a");
    EXPECT_EQ(flatMap.nameAt(1), "c");

    // Verify data reads correctly.
    auto pointBounds = makePointLookup(rowType, {"key"}, 50);
    NimbleIndexProjector::Request request;
    request.keyBounds = {pointBounds};
    auto result = projector->project(request, {});
    ASSERT_EQ(result.responses.size(), 1);
    ASSERT_FALSE(result.responses[0].chunks.empty());
  }

  // All requested keys missing — should fail.
  {
    std::vector<Subfield> subfields;
    subfields.emplace_back("features[\"x\"]");
    subfields.emplace_back("features[\"y\"]");
    NIMBLE_ASSERT_THROW(
        createProjector(subfields),
        "Cannot project entire FlatMap column without key subscripts");
  }
}

TEST_P(NimbleIndexProjectorTest, featureReorderingStorageReads) {
  // Schema: key (int64, sorted), features (MAP<INT, BIGINT> as FlatMap).
  // Write with 10 FlatMap keys (0-9), project 3 keys {7, 3, 1}.
  // With feature reordering, projected streams are adjacent on disk and require
  // fewer storage reads when coalesce distance is small.
  auto rowType = ROW({"key", "features"}, {BIGINT(), MAP(INTEGER(), BIGINT())});

  const int numRows = 1'000;
  const int numMapKeys = 10;
  const std::vector<int64_t> projectedKeys = {7, 3, 1};

  auto makeBatch = [&]() {
    return vectorMaker_->rowVector(
        {"key", "features"},
        {vectorMaker_->flatVector<int64_t>(
             numRows, [](auto row) { return static_cast<int64_t>(row); }),
         vectorMaker_->mapVector<int32_t, int64_t>(
             numRows,
             /*sizeAt*/ [&](auto /*row*/) { return numMapKeys; },
             /*keyAt*/
             [](auto /*row*/, auto mapIndex) {
               return static_cast<int32_t>(mapIndex);
             },
             /*valueAt*/
             [](auto row, auto mapIndex) {
               return static_cast<int64_t>(row * 100 + mapIndex);
             })});
  };

  // Write data with optional feature reordering.
  auto writeFile = [&](bool enableReordering) {
    sinkData_.clear();
    auto file = std::make_unique<InMemoryWriteFile>(&sinkData_);

    VeloxWriterOptions options;
    options.enableChunking = true;
    options.flatMapColumns = {{"features", {}}};

    ClusterIndexConfig clusterIndexConfig;
    clusterIndexConfig.columns = {"key"};
    clusterIndexConfig.sortOrders = {SortOrder{.ascending = true}};
    clusterIndexConfig.enforceKeyOrder = true;
    clusterIndexConfig.noDuplicateKey = true;
    options.clusterIndexConfig = std::move(clusterIndexConfig);

    if (enableReordering) {
      // Ordinal 1 = "features" column (after "key").
      options.featureReordering =
          std::vector<std::tuple<size_t, std::vector<int64_t>>>{
              {1, projectedKeys}};
    }

    auto batch = makeBatch();
    VeloxWriter writer(
        batch->type(), std::move(file), *rootPool_, std::move(options));
    writer.write(batch);
    writer.close();
  };

  // Create projector with custom coalesce distance.
  auto makeProjector = [&](int32_t maxCoalesceDistance) {
    auto fileHandle = makeFileHandle();
    dwio::common::ReaderOptions readerOptions(leafPool_.get());
    readerOptions.setDataIoStats(dataIoStats_);
    readerOptions.setMetadataIoStats(metadataIoStats_);
    readerOptions.setIndexIoStats(indexIoStats_);
    readerOptions.setFileFormat(FileFormat::NIMBLE);
    readerOptions.setLoadClusterIndex(true);
    readerOptions.setMaxCoalesceDistance(maxCoalesceDistance);

    std::vector<Subfield> subfields;
    for (auto key : projectedKeys) {
      subfields.emplace_back(fmt::format("features[{}]", key));
    }
    return NimbleIndexProjector::create(
        fileHandle, /*cache=*/nullptr, subfields, readerOptions);
  };

  // Part 1: Verify stream adjacency on disk with feature reordering.
  {
    writeFile(/*enableReordering=*/true);
    auto readFile =
        std::make_shared<InMemoryReadFile>(std::string_view(sinkData_));
    auto tablet = TabletReader::create(
        readFile, leafPool_.get(), makeTestTabletOptions(leafPool_.get()));
    ASSERT_GE(tablet->stripeCount(), 1);

    auto stripeId = tablet->stripeIdentifier(0);
    auto offsets = tablet->streamOffsets(stripeId);
    auto sizes = tablet->streamSizes(stripeId);

    VeloxReader reader(readFile.get(), *leafPool_);
    const auto& flatMap = reader.schema()->asRow().childAt(1)->asFlatMap();

    std::unordered_map<std::string, uint32_t> keyToValueStreamId;
    for (size_t i = 0; i < flatMap.childrenCount(); ++i) {
      keyToValueStreamId[flatMap.nameAt(i)] =
          flatMap.childAt(i)->asScalar().scalarDescriptor().offset();
    }

    auto diskPosition = [&](int64_t key) -> uint32_t {
      return offsets[keyToValueStreamId.at(folly::to<std::string>(key))];
    };

    // Verify projected keys appear in reordering order on disk.
    for (size_t i = 1; i < projectedKeys.size(); ++i) {
      EXPECT_LT(
          diskPosition(projectedKeys[i - 1]), diskPosition(projectedKeys[i]))
          << "Key " << projectedKeys[i - 1] << " should appear before key "
          << projectedKeys[i] << " on disk";
    }

    // Verify projected keys' value streams are contiguous (adjacent on disk).
    for (size_t i = 1; i < projectedKeys.size(); ++i) {
      auto prevStreamId =
          keyToValueStreamId.at(folly::to<std::string>(projectedKeys[i - 1]));
      auto currStreamId =
          keyToValueStreamId.at(folly::to<std::string>(projectedKeys[i]));
      EXPECT_EQ(
          offsets[prevStreamId] + sizes[prevStreamId], offsets[currStreamId])
          << "Key " << projectedKeys[i - 1]
          << " value stream should be adjacent to key " << projectedKeys[i];
    }
  }

  // Part 2: Compare storage reads with and without feature reordering.
  struct ReorderParam {
    bool enableReordering;
    int32_t maxCoalesceDistance;
    std::string debugString() const {
      return fmt::format(
          "enableReordering={}, maxCoalesceDistance={}",
          enableReordering,
          maxCoalesceDistance);
    }
  };

  std::vector<ReorderParam> testSettings = {
      {false, 0},
      {true, 0},
      {false, 512 << 10},
      {true, 512 << 10},
  };

  // Map from debugString to numStorageReads.
  std::map<std::string, uint64_t> storageReads;

  for (const auto& param : testSettings) {
    SCOPED_TRACE(param.debugString());

    writeFile(param.enableReordering);
    auto projector = makeProjector(param.maxCoalesceDistance);

    // Point lookup for key in the middle.
    auto bounds = makePointLookup(rowType, {"key"}, numRows / 2);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    auto result = projector->project(request, {});

    ASSERT_EQ(result.responses.size(), 1);
    ASSERT_FALSE(result.responses[0].chunks.empty());

    storageReads[param.debugString()] = projector->stats().numStorageReads;
  }

  // Verify all configurations produced valid results with non-zero reads.
  for (const auto& [key, reads] : storageReads) {
    SCOPED_TRACE(key);
    EXPECT_GT(reads, 0) << "Expected non-zero storage reads";
  }
}

// Verifies that metadata is reused within the same reader on repeated
// project() calls. Loops over combinations of pinMetadata and cacheMetadata
// to confirm that all valid configurations avoid re-reading metadata.
TEST_P(NimbleIndexProjectorTest, metadataReuseWithSameReader) {
  if (!GetParam().enableCache) {
    GTEST_SKIP() << "Only applicable when cache is enabled";
  }

  struct TestCase {
    bool pinMetadata;
    bool cacheMetadata;
    bool expectRamHit;
    std::string debugString() const {
      return fmt::format(
          "pinMetadata={}, cacheMetadata={}, expectRamHit={}",
          pinMetadata,
          cacheMetadata,
          expectRamHit);
    }
  };

  const std::vector<TestCase> testCases = {
      {true, true, true},
      {true, false, false},
      {false, true, true},
  };

  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  const int numRows = 100;
  std::vector<int64_t> keys(numRows);
  std::vector<int32_t> values(numRows);
  for (int i = 0; i < numRows; ++i) {
    keys[i] = i;
    values[i] = i * 10;
  }

  auto batch = vectorMaker_->rowVector(
      {"key", "value"},
      {vectorMaker_->flatVector<int64_t>(keys),
       vectorMaker_->flatVector<int32_t>(values)});

  writeData({batch}, {"key"});

  // Compute metadata boundary: the start of the first stripe group metadata.
  auto innerFile =
      std::make_shared<InMemoryReadFile>(std::string_view(sinkData_));
  auto tablet = TabletReader::create(
      innerFile, leafPool_.get(), makeTestTabletOptions(leafPool_.get()));
  auto stripeGroupsMeta = tablet->stripeGroupsMetadata();
  ASSERT_FALSE(stripeGroupsMeta.empty());
  const auto metadataBoundary = stripeGroupsMeta[0].offset();
  tablet.reset();

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());

    auto trackingFile = std::make_shared<testing::TrackingReadFile>(innerFile);
    velox::FileHandle trackingHandle{trackingFile, {}, {}};

    dwio::common::ReaderOptions readerOptions(leafPool_.get());
    readerOptions.setDataIoStats(dataIoStats_);
    readerOptions.setMetadataIoStats(metadataIoStats_);
    readerOptions.setIndexIoStats(indexIoStats_);
    readerOptions.setFileFormat(FileFormat::NIMBLE);
    readerOptions.setLoadClusterIndex(true);
    readerOptions.setCacheMetadata(testCase.cacheMetadata);
    readerOptions.setPinMetadata(testCase.pinMetadata);

    std::vector<Subfield> subfields;
    subfields.emplace_back("value");
    auto projector = NimbleIndexProjector::create(
        trackingHandle, /*cache=*/nullptr, subfields, readerOptions);

    auto bounds = makePointLookup(rowType, {"key"}, 50);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};

    // First project: reads both data and metadata regions.
    projector->project(request, {});
    ASSERT_GT(trackingFile->maxReadOffset(), metadataBoundary)
        << "First project should read metadata beyond the boundary";

    // Second project on the same projector: metadata should be reused,
    // avoiding re-reads beyond the metadata boundary.
    trackingFile->resetMaxReadOffset();
    projector->project(request, {});
    EXPECT_LE(trackingFile->maxReadOffset(), metadataBoundary)
        << "Second project should not re-read metadata when "
        << testCase.debugString();
  }
}

TEST_P(NimbleIndexProjectorTest, resumeKeyOnTruncation) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  // 10 stripes x 100 rows = 1000 rows, keys 0..999.
  writeResumeKeyTestData();

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  // Range scan [0, 500) with maxRows=50. Since maxRows is a soft limit
  // at stripe boundaries and each stripe has 100 rows, the first stripe
  // (100 rows) exceeds the limit but is fully included.
  auto bounds = makeRangeLookup(rowType, {"key"}, 0, 500);
  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds};
  NimbleIndexProjector::Options options;
  options.maxRows = 50;
  auto result = projector->project(request, options);

  ASSERT_EQ(result.responses.size(), 1);
  const auto& response = result.responses[0];
  ASSERT_FALSE(response.chunks.empty());

  // Soft limit: first stripe (100 rows) included fully.
  uint64_t totalRows = 0;
  for (const auto& chunkSlice : response.chunks) {
    totalRows += readEmbeddedRowRange(chunkSlice).numRows();
  }
  EXPECT_EQ(totalRows, 100);

  // Resume key must be set since the request spans more stripes.
  ASSERT_TRUE(response.resumeKey.has_value());
  EXPECT_FALSE(response.resumeKey->empty());

  // The resume key is embedded only on the last slice; earlier slices carry
  // an empty resume key marker.
  const auto numSlices = response.chunks.size();
  for (size_t i = 0; i + 1 < numSlices; ++i) {
    EXPECT_FALSE(readEmbeddedResumeKey(response.chunks[i]).has_value())
        << "non-last slice " << i << " unexpectedly carries a resume key";
  }
  const auto embeddedResumeKey = readEmbeddedResumeKey(response.chunks.back());
  ASSERT_TRUE(embeddedResumeKey.has_value());
  EXPECT_EQ(*embeddedResumeKey, *response.resumeKey);
}

TEST_P(NimbleIndexProjectorTest, resumeKeyNotSetWithoutTruncation) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  writeResumeKeyTestData();

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");

  // Case 1: maxRows = 0 (unlimited).
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 50);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    auto result = projector->project(request, {});
    ASSERT_EQ(result.responses.size(), 1);
    EXPECT_FALSE(result.responses[0].resumeKey.has_value());
  }

  // Case 2: maxRows > total matching rows.
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 50);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxRows = 10000;
    auto result = projector->project(request, options);
    ASSERT_EQ(result.responses.size(), 1);
    EXPECT_FALSE(result.responses[0].resumeKey.has_value());
  }

  // Case 3: Point lookup (always returns <=1 row per key).
  {
    auto projector = createProjector(subfields);
    auto bounds = makePointLookup(rowType, {"key"}, 50);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxRows = 10;
    auto result = projector->project(request, options);
    ASSERT_EQ(result.responses.size(), 1);
    EXPECT_FALSE(result.responses[0].resumeKey.has_value());
  }
}

TEST_P(NimbleIndexProjectorTest, softLimitAtStripeBoundary) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  // 10 stripes x 100 rows.
  writeResumeKeyTestData(/*rowsPerBatch=*/100, /*numBatches=*/10);

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");

  // maxRows=99: below stripe size, but soft limit includes entire first stripe.
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 500);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxRows = 99;
    auto result = projector->project(request, options);

    ASSERT_EQ(result.responses.size(), 1);
    uint64_t totalRows = 0;
    for (const auto& chunkSlice : result.responses[0].chunks) {
      totalRows += readEmbeddedRowRange(chunkSlice).numRows();
    }
    EXPECT_EQ(totalRows, 100);
    EXPECT_TRUE(result.responses[0].resumeKey.has_value());
  }

  // maxRows=100: exactly one stripe, stops after it.
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 500);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxRows = 100;
    auto result = projector->project(request, options);

    ASSERT_EQ(result.responses.size(), 1);
    uint64_t totalRows = 0;
    for (const auto& chunkSlice : result.responses[0].chunks) {
      totalRows += readEmbeddedRowRange(chunkSlice).numRows();
    }
    EXPECT_EQ(totalRows, 100);
    EXPECT_TRUE(result.responses[0].resumeKey.has_value());
  }

  // maxRows=101: exceeds first stripe, processes second stripe too (soft
  // limit). Second stripe has 100 rows, total = 200.
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 500);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxRows = 101;
    auto result = projector->project(request, options);

    ASSERT_EQ(result.responses.size(), 1);
    uint64_t totalRows = 0;
    for (const auto& chunkSlice : result.responses[0].chunks) {
      totalRows += readEmbeddedRowRange(chunkSlice).numRows();
    }
    EXPECT_EQ(totalRows, 200);
    EXPECT_TRUE(result.responses[0].resumeKey.has_value());
  }
}

TEST_P(NimbleIndexProjectorTest, resumeKeyPagination) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  // 10 stripes x 100 rows = 1000 rows, keys 0..999.
  writeResumeKeyTestData();

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");

  const int64_t rangeStart = 0;
  const int64_t rangeEnd = 1000;
  // maxRows=150 with 100-row stripes: each page gets 2 stripes (200 rows)
  // because 1st stripe (100) is under limit, 2nd stripe (200 total) exceeds
  // the soft limit and stops.
  const uint64_t maxRows = 150;

  uint64_t totalRowsRead = 0;
  auto currentBounds = makeRangeLookup(rowType, {"key"}, rangeStart, rangeEnd);
  int pageCount = 0;

  while (true) {
    auto projector = createProjector(subfields);
    NimbleIndexProjector::Request request;
    request.keyBounds = {currentBounds};
    NimbleIndexProjector::Options options;
    options.maxRows = maxRows;
    auto result = projector->project(request, options);

    ASSERT_EQ(result.responses.size(), 1);
    const auto& response = result.responses[0];

    uint64_t pageRows = 0;
    for (const auto& chunkSlice : response.chunks) {
      pageRows += readEmbeddedRowRange(chunkSlice).numRows();
    }
    EXPECT_GT(pageRows, 0) << "Page " << pageCount << " returned no rows";
    totalRowsRead += pageRows;
    ++pageCount;

    if (!response.resumeKey.has_value()) {
      break;
    }

    currentBounds = velox::serializer::EncodedKeyBounds{
        .lowerKey = response.resumeKey.value(),
        .upperKey = currentBounds.upperKey};
    ASSERT_LT(pageCount, 100) << "Pagination loop exceeded safety limit";
  }

  // All rows should have been read across all pages.
  EXPECT_EQ(totalRowsRead, static_cast<uint64_t>(rangeEnd - rangeStart));
}

TEST_P(NimbleIndexProjectorTest, maxRowsTotalAcrossRequests) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  // 10 stripes x 100 rows = 1000 rows, keys 0..999.
  writeResumeKeyTestData();

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  // Two range scans: request 0 spans stripes 0-4, request 1 spans stripe 9.
  // maxRows=50 (soft limit): first stripe (100 rows) exceeds limit, stops.
  // Request 0 gets 100 rows from stripe 0, request 1 gets nothing (stripe 9
  // is never reached).
  auto bounds0 = makeRangeLookup(rowType, {"key"}, 0, 500);
  auto bounds1 = makeRangeLookup(rowType, {"key"}, 900, 1000);

  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds0, bounds1};
  NimbleIndexProjector::Options options;
  options.maxRows = 50;
  auto result = projector->project(request, options);

  ASSERT_EQ(result.responses.size(), 2);

  // Request 0: gets full first stripe (100 rows), has resume key.
  {
    const auto& response = result.responses[0];
    uint64_t totalRows = 0;
    for (const auto& chunkSlice : response.chunks) {
      totalRows += readEmbeddedRowRange(chunkSlice).numRows();
    }
    EXPECT_EQ(totalRows, 100);
    EXPECT_TRUE(response.resumeKey.has_value());
  }

  // Request 1: stripe 9 was never processed, resume key = original lower key.
  {
    const auto& response = result.responses[1];
    EXPECT_TRUE(response.chunks.empty());
    ASSERT_TRUE(response.resumeKey.has_value());
    EXPECT_EQ(response.resumeKey.value(), bounds1.lowerKey);
  }
}

TEST_P(NimbleIndexProjectorTest, noResumeKeyWhenRequestFullySatisfied) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  // 10 stripes x 100 rows = 1000 rows, keys 0..999.
  writeResumeKeyTestData();

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  // Request 0: [0, 100) — exactly one stripe, fully satisfied.
  // Request 1: [0, 500) — spans 5 stripes, will be truncated.
  // maxRows=100: first stripe covers both, request 0 ends within it.
  auto bounds0 = makeRangeLookup(rowType, {"key"}, 0, 100);
  auto bounds1 = makeRangeLookup(rowType, {"key"}, 0, 500);

  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds0, bounds1};
  NimbleIndexProjector::Options options;
  options.maxRows = 100;
  auto result = projector->project(request, options);

  ASSERT_EQ(result.responses.size(), 2);

  // Request 0: fully satisfied within stripe 0 — no resume key.
  {
    const auto& response = result.responses[0];
    ASSERT_FALSE(response.chunks.empty());
    EXPECT_FALSE(response.resumeKey.has_value());
  }

  // Request 1: spans beyond stripe 0, truncated — has resume key.
  {
    const auto& response = result.responses[1];
    ASSERT_FALSE(response.chunks.empty());
    EXPECT_TRUE(response.resumeKey.has_value());
  }
}

TEST_P(NimbleIndexProjectorTest, maxRowsExceedsTotal) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  // 10 stripes x 100 rows = 1000 rows.
  writeResumeKeyTestData();

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  // maxRows larger than total matching rows — no truncation.
  auto bounds = makeRangeLookup(rowType, {"key"}, 0, 300);
  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds};
  NimbleIndexProjector::Options options;
  options.maxRows = 10000;
  auto result = projector->project(request, options);

  ASSERT_EQ(result.responses.size(), 1);
  uint64_t totalRows = 0;
  for (const auto& chunkSlice : result.responses[0].chunks) {
    totalRows += readEmbeddedRowRange(chunkSlice).numRows();
  }
  EXPECT_EQ(totalRows, 300);
  EXPECT_FALSE(result.responses[0].resumeKey.has_value());
}

TEST_P(NimbleIndexProjectorTest, maxRowsZeroMeansUnlimited) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  writeResumeKeyTestData();

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  auto bounds = makeRangeLookup(rowType, {"key"}, 0, 1000);
  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds};
  NimbleIndexProjector::Options options;
  options.maxRows = 0;
  auto result = projector->project(request, options);

  ASSERT_EQ(result.responses.size(), 1);
  uint64_t totalRows = 0;
  for (const auto& chunkSlice : result.responses[0].chunks) {
    totalRows += readEmbeddedRowRange(chunkSlice).numRows();
  }
  EXPECT_EQ(totalRows, 1000);
  EXPECT_FALSE(result.responses[0].resumeKey.has_value());
}

TEST_P(NimbleIndexProjectorTest, maxRowsSingleRowStripes) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  // 10 stripes x 1 row each.
  writeResumeKeyTestData(/*rowsPerBatch=*/1, /*numBatches=*/10);

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  // maxRows=3: should process exactly 3 stripes (3 rows).
  auto bounds = makeRangeLookup(rowType, {"key"}, 0, 10);
  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds};
  NimbleIndexProjector::Options options;
  options.maxRows = 3;
  auto result = projector->project(request, options);

  ASSERT_EQ(result.responses.size(), 1);
  uint64_t totalRows = 0;
  for (const auto& chunkSlice : result.responses[0].chunks) {
    totalRows += readEmbeddedRowRange(chunkSlice).numRows();
  }
  EXPECT_EQ(totalRows, 3);
  EXPECT_TRUE(result.responses[0].resumeKey.has_value());
}

TEST_P(NimbleIndexProjectorTest, maxRowsPerRequestClipping) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  writeResumeKeyTestData();

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");

  // Stripe-aligned: 200 rows = exactly 2 stripes. No resume key.
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 500);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxRowsPerRequest = 200;
    auto result = projector->project(request, options);

    ASSERT_EQ(result.responses.size(), 1);
    EXPECT_FALSE(result.responses[0].resumeKey.has_value());
    EXPECT_EQ(projector->stats().numReadRows, 200);
  }

  // Mid-stripe hard cut: 150 rows = 1 full stripe (100) + 50 from stripe 2.
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 500);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxRowsPerRequest = 150;
    auto result = projector->project(request, options);

    ASSERT_EQ(result.responses.size(), 1);
    const auto& response = result.responses[0];
    EXPECT_FALSE(response.resumeKey.has_value());
    EXPECT_EQ(projector->stats().numReadRows, 150);
    ASSERT_EQ(response.chunks.size(), 2);
    EXPECT_EQ(readEmbeddedRowRange(response.chunks[0]).numRows(), 100);
    EXPECT_EQ(readEmbeddedRowRange(response.chunks[1]).numRows(), 50);
  }
}

TEST_P(NimbleIndexProjectorTest, maxRowsPerRequestIndependentPruning) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  writeResumeKeyTestData();

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  // Request 0: [0, 500) spans stripes 0-4.
  // Request 1: [200, 800) spans stripes 2-7.
  // maxRowsPerRequest=100: each gets exactly 100 rows, no resume key.
  auto bounds0 = makeRangeLookup(rowType, {"key"}, 0, 500);
  auto bounds1 = makeRangeLookup(rowType, {"key"}, 200, 800);

  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds0, bounds1};
  NimbleIndexProjector::Options options;
  options.maxRowsPerRequest = 100;
  auto result = projector->project(request, options);

  ASSERT_EQ(result.responses.size(), 2);

  {
    const auto& response = result.responses[0];
    uint64_t totalRows = 0;
    for (const auto& chunkSlice : response.chunks) {
      totalRows += readEmbeddedRowRange(chunkSlice).numRows();
    }
    EXPECT_EQ(totalRows, 100);
    EXPECT_FALSE(response.resumeKey.has_value());
  }

  {
    const auto& response = result.responses[1];
    uint64_t totalRows = 0;
    for (const auto& chunkSlice : response.chunks) {
      totalRows += readEmbeddedRowRange(chunkSlice).numRows();
    }
    EXPECT_EQ(totalRows, 100);
    EXPECT_FALSE(response.resumeKey.has_value());
  }
}

TEST_P(NimbleIndexProjectorTest, maxBytesTruncation) {
  writeResumeKeyTestData();
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");

  // First, read one stripe without limits to learn the per-stripe byte size.
  uint64_t bytesPerStripe;
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 100);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    auto result = projector->project(request, {});
    ASSERT_EQ(result.responses[0].chunks.size(), 1);
    bytesPerStripe = projector->stats().numReadBytes;
    ASSERT_GT(bytesPerStripe, 0);
  }

  // Range [0, 500) spans 5 stripes. Set byte limit to allow ~2 stripes.
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 500);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxBytes = bytesPerStripe * 2;

    auto result = projector->project(request, options);
    ASSERT_EQ(result.responses.size(), 1);
    const auto& response = result.responses[0];

    // Should be truncated with a resume key.
    ASSERT_TRUE(response.resumeKey.has_value());

    // Should have read exactly 2 stripes (byte budget consumed after 2).
    EXPECT_EQ(projector->stats().numReadStripes, 2);
    EXPECT_EQ(projector->stats().numReadRows, 200);
  }
}

TEST_P(NimbleIndexProjectorTest, maxBytesNoTruncation) {
  writeResumeKeyTestData();
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");

  // maxBytes=0 (unlimited) -> no truncation, no resume key.
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 100, 300);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    auto result = projector->project(request, {});
    ASSERT_EQ(result.responses.size(), 1);
    EXPECT_FALSE(result.responses[0].resumeKey.has_value());
  }

  // maxBytes larger than total data -> no truncation, no resume key.
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 100, 300);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxBytes = std::numeric_limits<uint64_t>::max();
    auto result = projector->project(request, options);
    ASSERT_EQ(result.responses.size(), 1);
    EXPECT_FALSE(result.responses[0].resumeKey.has_value());
  }
}

TEST_P(NimbleIndexProjectorTest, maxBytesPagination) {
  writeResumeKeyTestData();
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");

  // Learn per-stripe byte size.
  uint64_t bytesPerStripe;
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 100);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    auto result = projector->project(request, {});
    ASSERT_EQ(result.responses[0].chunks.size(), 1);
    bytesPerStripe = projector->stats().numReadBytes;
  }

  // Paginate through the full range using byte limits.
  auto paginate =
      [&](int64_t lowerKey, int64_t upperKey, uint64_t maxBytes) -> uint64_t {
    auto currentBounds = makeRangeLookup(rowType, {"key"}, lowerKey, upperKey);
    uint64_t totalReadRows = 0;
    int iterations = 0;
    const int maxIterations = 100;

    while (iterations < maxIterations) {
      ++iterations;
      auto projector = createProjector(subfields);
      NimbleIndexProjector::Request request;
      request.keyBounds = {currentBounds};
      NimbleIndexProjector::Options options;
      options.maxBytes = maxBytes;

      auto result = projector->project(request, options);
      EXPECT_EQ(result.responses.size(), 1);
      totalReadRows += projector->stats().numReadRows;

      const auto& response = result.responses[0];
      if (!response.resumeKey.has_value()) {
        break;
      }
      currentBounds = velox::serializer::EncodedKeyBounds{
          .lowerKey = response.resumeKey.value(),
          .upperKey = currentBounds.upperKey};
    }
    EXPECT_LT(iterations, maxIterations);
    return totalReadRows;
  };

  EXPECT_EQ(paginate(0, 1000, bytesPerStripe), 1000);
  EXPECT_EQ(paginate(0, 1000, bytesPerStripe * 2), 1000);
  EXPECT_EQ(paginate(0, 1000, bytesPerStripe * 3), 1000);
  EXPECT_EQ(paginate(0, 1000, bytesPerStripe * 100), 1000);
  EXPECT_EQ(paginate(150, 750, bytesPerStripe), 600);
  EXPECT_EQ(paginate(150, 750, bytesPerStripe * 2), 600);
}

TEST_P(NimbleIndexProjectorTest, maxBytesAndMaxRowsInteraction) {
  writeResumeKeyTestData();
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");

  // Learn per-stripe byte size.
  uint64_t bytesPerStripe;
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 100);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    auto result = projector->project(request, {});
    ASSERT_EQ(result.responses[0].chunks.size(), 1);
    bytesPerStripe = projector->stats().numReadBytes;
  }

  // Range [0, 500) = 5 stripes, 500 rows.
  auto bounds = makeRangeLookup(rowType, {"key"}, 0, 500);

  // Case 1: Global row limit is tighter (50 rows < 3 stripes of bytes).
  {
    auto projector = createProjector(subfields);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxRows = 50;
    options.maxBytes = bytesPerStripe * 3;

    auto result = projector->project(request, options);
    ASSERT_EQ(result.responses.size(), 1);
    ASSERT_TRUE(result.responses[0].resumeKey.has_value());
    EXPECT_EQ(projector->stats().numReadStripes, 1);
    EXPECT_EQ(projector->stats().numReadRows, 100);
  }

  // Case 2: Byte limit is tighter (1 stripe < 500 rows).
  {
    auto projector = createProjector(subfields);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxRows = 500;
    options.maxBytes = bytesPerStripe;

    auto result = projector->project(request, options);
    ASSERT_EQ(result.responses.size(), 1);
    ASSERT_TRUE(result.responses[0].resumeKey.has_value());
    EXPECT_EQ(projector->stats().numReadStripes, 1);
    EXPECT_EQ(projector->stats().numReadRows, 100);
  }
}

TEST_P(NimbleIndexProjectorTest, maxRowsPerRequestAndGlobalMaxRows) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  writeResumeKeyTestData();

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");

  // Per-request limit is tighter: 100 rows/request vs 500 global.
  // Request is fulfilled (100 rows), no resume key from maxRowsPerRequest.
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 500);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxRowsPerRequest = 100;
    options.maxRows = 500;

    auto result = projector->project(request, options);
    ASSERT_EQ(result.responses.size(), 1);
    EXPECT_FALSE(result.responses[0].resumeKey.has_value());
    EXPECT_EQ(projector->stats().numReadRows, 100);
  }

  // Global limit is tighter: 50 rows global vs 300 rows/request.
  // Global limit triggers resume key (stripe-boundary soft limit = 100 rows).
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 500);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxRowsPerRequest = 300;
    options.maxRows = 50;

    auto result = projector->project(request, options);
    ASSERT_EQ(result.responses.size(), 1);
    EXPECT_TRUE(result.responses[0].resumeKey.has_value());
    EXPECT_EQ(projector->stats().numReadStripes, 1);
    EXPECT_EQ(projector->stats().numReadRows, 100);
  }

  // Per-request hard limit cuts mid-stripe: 50 rows/request < 100-row stripe.
  // No resume key — request is fulfilled with 50 rows.
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 500);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxRowsPerRequest = 50;
    options.maxRows = 500;

    auto result = projector->project(request, options);
    ASSERT_EQ(result.responses.size(), 1);
    EXPECT_FALSE(result.responses[0].resumeKey.has_value());
    EXPECT_EQ(projector->stats().numReadRows, 50);
  }
}

TEST_P(
    NimbleIndexProjectorTest,
    maxRowsPerRequestMultiRequestMidStripeClipping) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  // 10 stripes x 100 rows.
  writeResumeKeyTestData();

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  // Request 0: [0, 300) spans stripes 0-2.
  // Request 1: [0, 300) same range.
  // maxRowsPerRequest=150: both get 100 (stripe 0) + 50 (clipped stripe 1).
  auto bounds0 = makeRangeLookup(rowType, {"key"}, 0, 300);
  auto bounds1 = makeRangeLookup(rowType, {"key"}, 0, 300);

  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds0, bounds1};
  NimbleIndexProjector::Options options;
  options.maxRowsPerRequest = 150;
  auto result = projector->project(request, options);

  ASSERT_EQ(result.responses.size(), 2);
  for (int i = 0; i < 2; ++i) {
    SCOPED_TRACE(fmt::format("request {}", i));
    const auto& response = result.responses[i];
    uint64_t totalRows = 0;
    for (const auto& chunkSlice : response.chunks) {
      totalRows += readEmbeddedRowRange(chunkSlice).numRows();
    }
    EXPECT_EQ(totalRows, 150);
    EXPECT_FALSE(response.resumeKey.has_value());
    ASSERT_EQ(response.chunks.size(), 2);
    EXPECT_EQ(readEmbeddedRowRange(response.chunks[0]).numRows(), 100);
    EXPECT_EQ(readEmbeddedRowRange(response.chunks[1]).numRows(), 50);
  }
}

TEST_P(NimbleIndexProjectorTest, maxRowsPerRequestSingleRow) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  // 10 stripes x 100 rows.
  writeResumeKeyTestData();

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  // maxRowsPerRequest=1: each request gets exactly 1 row, clipped from the
  // first row of the first matching stripe.
  auto bounds = makeRangeLookup(rowType, {"key"}, 0, 500);
  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds};
  NimbleIndexProjector::Options options;
  options.maxRowsPerRequest = 1;
  auto result = projector->project(request, options);

  ASSERT_EQ(result.responses.size(), 1);
  const auto& response = result.responses[0];
  EXPECT_FALSE(response.resumeKey.has_value());
  EXPECT_EQ(projector->stats().numReadRows, 1);
  ASSERT_EQ(response.chunks.size(), 1);
  EXPECT_EQ(readEmbeddedRowRange(response.chunks[0]).numRows(), 1);
}

TEST_P(NimbleIndexProjectorTest, maxRowsLargeScale) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  // 50 stripes x 200 rows = 10,000 rows.
  writeResumeKeyTestData(/*rowsPerBatch=*/200, /*numBatches=*/50);

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");

  // Paginate through all 10,000 rows with maxRows=500 (soft limit).
  // Each page gets at least 500 rows (soft limit includes the full stripe
  // that crosses the threshold, so ~600 rows per page with 200-row stripes).
  auto paginate = [&](uint64_t maxRows) -> uint64_t {
    auto currentBounds = makeRangeLookup(rowType, {"key"}, 0, 10000);
    uint64_t totalReadRows = 0;
    int pages = 0;
    while (pages < 100) {
      ++pages;
      auto projector = createProjector(subfields);
      NimbleIndexProjector::Request request;
      request.keyBounds = {currentBounds};
      NimbleIndexProjector::Options options;
      options.maxRows = maxRows;
      auto result = projector->project(request, options);
      EXPECT_EQ(result.responses.size(), 1);
      totalReadRows += projector->stats().numReadRows;
      if (!result.responses[0].resumeKey.has_value()) {
        break;
      }
      currentBounds = velox::serializer::EncodedKeyBounds{
          .lowerKey = result.responses[0].resumeKey.value(),
          .upperKey = currentBounds.upperKey};
    }
    EXPECT_LT(pages, 100);
    return totalReadRows;
  };

  EXPECT_EQ(paginate(500), 10000);
  EXPECT_EQ(paginate(1000), 10000);
  EXPECT_EQ(paginate(5000), 10000);
}

TEST_P(NimbleIndexProjectorTest, maxRowsMultiRequestMixedSatisfaction) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  // 10 stripes x 100 rows.
  writeResumeKeyTestData();

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  auto projector = createProjector(subfields);

  // Request 0: [0, 50) — within stripe 0, fully satisfied.
  // Request 1: [0, 100) — exactly stripe 0, fully satisfied.
  // Request 2: [0, 500) — spans 5 stripes, truncated by maxRows.
  // Request 3: [950, 1000) — stripe 9, never reached.
  // maxRows=100: processes stripe 0, then stops.
  auto bounds0 = makeRangeLookup(rowType, {"key"}, 0, 50);
  auto bounds1 = makeRangeLookup(rowType, {"key"}, 0, 100);
  auto bounds2 = makeRangeLookup(rowType, {"key"}, 0, 500);
  auto bounds3 = makeRangeLookup(rowType, {"key"}, 950, 1000);

  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds0, bounds1, bounds2, bounds3};
  NimbleIndexProjector::Options options;
  options.maxRows = 100;
  auto result = projector->project(request, options);

  ASSERT_EQ(result.responses.size(), 4);

  // Request 0: 50 rows, fully satisfied, no resume key.
  EXPECT_FALSE(result.responses[0].chunks.empty());
  EXPECT_FALSE(result.responses[0].resumeKey.has_value());

  // Request 1: 100 rows, fully satisfied (ends at stripe boundary), no resume.
  EXPECT_FALSE(result.responses[1].chunks.empty());
  EXPECT_FALSE(result.responses[1].resumeKey.has_value());

  // Request 2: 100 rows from stripe 0, has data in stripes 1-4, resume key.
  EXPECT_FALSE(result.responses[2].chunks.empty());
  EXPECT_TRUE(result.responses[2].resumeKey.has_value());

  // Request 3: stripe 9 never processed, resume key = original lower key.
  EXPECT_TRUE(result.responses[3].chunks.empty());
  ASSERT_TRUE(result.responses[3].resumeKey.has_value());
  EXPECT_EQ(result.responses[3].resumeKey.value(), bounds3.lowerKey);
}

TEST_P(NimbleIndexProjectorTest, maxBytesAndMaxRowsPerRequestInteraction) {
  writeResumeKeyTestData();
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");

  // Learn per-stripe byte size.
  uint64_t bytesPerStripe;
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 100);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    auto result = projector->project(request, {});
    ASSERT_EQ(result.responses[0].chunks.size(), 1);
    bytesPerStripe = projector->stats().numReadBytes;
  }

  // maxRowsPerRequest clips to 150 rows (mid-stripe), maxBytes allows 5
  // stripes. maxRowsPerRequest is tighter.
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 500);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxRowsPerRequest = 150;
    options.maxBytes = bytesPerStripe * 5;

    auto result = projector->project(request, options);
    ASSERT_EQ(result.responses.size(), 1);
    EXPECT_FALSE(result.responses[0].resumeKey.has_value());
    EXPECT_EQ(projector->stats().numReadRows, 150);
  }

  // maxBytes allows 1 stripe (100 rows), maxRowsPerRequest allows 300.
  // maxBytes is tighter — sets resume key.
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 500);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    NimbleIndexProjector::Options options;
    options.maxRowsPerRequest = 300;
    options.maxBytes = bytesPerStripe;

    auto result = projector->project(request, options);
    ASSERT_EQ(result.responses.size(), 1);
    EXPECT_TRUE(result.responses[0].resumeKey.has_value());
    EXPECT_EQ(projector->stats().numReadStripes, 1);
    EXPECT_EQ(projector->stats().numReadRows, 100);
  }
}

TEST_P(NimbleIndexProjectorTest, maxBytesLargeScale) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});
  // 50 stripes x 200 rows = 10,000 rows.
  writeResumeKeyTestData(/*rowsPerBatch=*/200, /*numBatches=*/50);

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");

  // Learn per-stripe byte size.
  uint64_t bytesPerStripe;
  {
    auto projector = createProjector(subfields);
    auto bounds = makeRangeLookup(rowType, {"key"}, 0, 200);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    auto result = projector->project(request, {});
    ASSERT_EQ(result.responses[0].chunks.size(), 1);
    bytesPerStripe = projector->stats().numReadBytes;
  }

  // Paginate all 10,000 rows with byte limit of ~3 stripes per page.
  auto currentBounds = makeRangeLookup(rowType, {"key"}, 0, 10000);
  uint64_t totalReadRows = 0;
  int pages = 0;
  while (pages < 100) {
    ++pages;
    auto projector = createProjector(subfields);
    NimbleIndexProjector::Request request;
    request.keyBounds = {currentBounds};
    NimbleIndexProjector::Options options;
    options.maxBytes = bytesPerStripe * 3;
    auto result = projector->project(request, options);
    EXPECT_EQ(result.responses.size(), 1);
    totalReadRows += projector->stats().numReadRows;
    if (!result.responses[0].resumeKey.has_value()) {
      break;
    }
    currentBounds = velox::serializer::EncodedKeyBounds{
        .lowerKey = result.responses[0].resumeKey.value(),
        .upperKey = currentBounds.upperKey};
  }
  EXPECT_LT(pages, 100);
  EXPECT_EQ(totalReadRows, 10000);
}

TEST_P(NimbleIndexProjectorTest, cacheDataReadStats) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  const int numRows = 100;
  std::vector<int64_t> keys(numRows);
  std::vector<int32_t> values(numRows);
  for (int i = 0; i < numRows; ++i) {
    keys[i] = i;
    values[i] = i * 10;
  }

  auto batch = vectorMaker_->rowVector(
      {"key", "value"},
      {vectorMaker_->flatVector<int64_t>(keys),
       vectorMaker_->flatVector<int32_t>(values)});

  writeData({batch}, {"key"});

  auto cache =
      cache::AsyncDataCache::create(memory::memoryManager()->allocator());

  struct TestCase {
    bool useCache;
    bool cacheData;
    bool expectCacheHitsOnSecondPass;
    std::string debugString() const {
      return fmt::format(
          "useCache={}, cacheData={}, expectCacheHitsOnSecondPass={}",
          useCache,
          cacheData,
          expectCacheHitsOnSecondPass);
    }
  };

  const std::vector<TestCase> testCases = {
      {false, false, false},
      {true, false, false},
      {true, true, true},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());

    std::vector<Subfield> subs;
    subs.emplace_back("value");
    auto* cachePtr = testCase.useCache ? cache.get() : nullptr;

    // First pass: populates the cache if cacheData is true.
    {
      auto [projector, ioStats] =
          createProjectorWithStats(subs, cachePtr, testCase.cacheData);
      auto bounds = makePointLookup(rowType, {"key"}, 50);
      NimbleIndexProjector::Request request;
      request.keyBounds.push_back(std::move(bounds));
      auto result = projector->project(request, {});
      ASSERT_EQ(result.responses.size(), 1);
      EXPECT_FALSE(result.responses[0].chunks.empty());

      const auto& stats = projector->stats();
      EXPECT_GT(stats.numStorageReads, 0);
      EXPECT_GT(stats.rawBytesRead, 0);
      EXPECT_GT(ioStats->read().count(), 0);
      EXPECT_GT(ioStats->rawBytesRead(), 0);
    }

    // Second pass: should get cache hits if cacheData was true.
    {
      auto [projector, ioStats] =
          createProjectorWithStats(subs, cachePtr, testCase.cacheData);
      auto bounds = makePointLookup(rowType, {"key"}, 50);
      NimbleIndexProjector::Request request;
      request.keyBounds.push_back(std::move(bounds));
      auto result = projector->project(request, {});
      ASSERT_EQ(result.responses.size(), 1);
      EXPECT_FALSE(result.responses[0].chunks.empty());

      const auto& stats = projector->stats();
      if (testCase.expectCacheHitsOnSecondPass) {
        EXPECT_GT(stats.numCacheHits, 0)
            << "Expected cache hits on second pass with cacheData=true";
        EXPECT_GT(stats.cacheHitBytes, 0);
        EXPECT_GT(ioStats->ramHit().count(), 0);
        EXPECT_GT(ioStats->ramHit().sum(), 0);
      } else {
        EXPECT_EQ(stats.numCacheHits, 0)
            << "Expected no cache hits with cacheData=false";
        EXPECT_EQ(stats.cacheHitBytes, 0);
        EXPECT_EQ(ioStats->ramHit().count(), 0);
        EXPECT_EQ(ioStats->ramHit().sum(), 0);
        EXPECT_GT(stats.numStorageReads, 0);
        EXPECT_GT(ioStats->read().count(), 0);
      }
    }
  }

  cache->shutdown();
}

TEST_P(NimbleIndexProjectorTest, requiresIoStats) {
  auto batch = vectorMaker_->rowVector(
      {"key", "value"},
      {vectorMaker_->flatVector<int64_t>({1, 2, 3}),
       vectorMaker_->flatVector<int32_t>({10, 20, 30})});
  writeData({batch}, {"key"});

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");

  struct TestCase {
    bool setDataIoStats;
    bool setMetadataIoStats;
    bool setIndexIoStats;
    std::string expectedMessage;
  };

  const std::vector<TestCase> testCases = {
      {false,
       true,
       true,
       "NimbleIndexProjector requires ReaderOptions::dataIoStats to be set"},
      {true,
       false,
       true,
       "NimbleIndexProjector requires ReaderOptions::metadataIoStats to be "
       "set"},
      {true,
       true,
       false,
       "NimbleIndexProjector requires ReaderOptions::indexIoStats to be set"},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.expectedMessage);
    NIMBLE_ASSERT_THROW(
        createProjectorWithStats(
            subfields,
            /*cache=*/nullptr,
            /*cacheData=*/std::nullopt,
            /*setDataIoStats=*/testCase.setDataIoStats,
            /*setMetadataIoStats=*/testCase.setMetadataIoStats,
            /*setIndexIoStats=*/testCase.setIndexIoStats),
        testCase.expectedMessage);
  }
}

TEST_P(NimbleIndexProjectorTest, invalidFileHandleWithCache) {
  auto rowType = ROW({"key", "value"}, {BIGINT(), INTEGER()});

  const int numRows = 10;
  auto batch = vectorMaker_->rowVector(
      {"key", "value"},
      {vectorMaker_->flatVector<int64_t>(numRows, [](auto i) { return i; }),
       vectorMaker_->flatVector<int32_t>(numRows, [](auto i) { return i; })});
  writeData({batch}, {"key"});

  auto cache =
      cache::AsyncDataCache::create(memory::memoryManager()->allocator());

  auto readFile =
      std::make_shared<InMemoryReadFile>(std::string_view(sinkData_));
  velox::FileHandle emptyHandle{readFile, {}, {}};
  dwio::common::ReaderOptions readerOptions(leafPool_.get());
  readerOptions.setDataIoStats(std::make_shared<velox::io::IoStatistics>());
  readerOptions.setMetadataIoStats(std::make_shared<velox::io::IoStatistics>());
  readerOptions.setIndexIoStats(std::make_shared<velox::io::IoStatistics>());
  readerOptions.setFileFormat(FileFormat::NIMBLE);
  readerOptions.setLoadClusterIndex(true);
  readerOptions.setCacheData(true);

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  NIMBLE_ASSERT_THROW(
      NimbleIndexProjector::create(
          emptyHandle, cache.get(), subfields, readerOptions),
      "FileHandle must have valid uuid and groupId when cache is provided");

  cache->shutdown();
}

INSTANTIATE_TEST_CASE_P(
    AllCacheParams,
    NimbleIndexProjectorTest,
    ::testing::ValuesIn(NimbleIndexProjectorTest::getTestParams()),
    [](const ::testing::TestParamInfo<TestParam>& info) {
      return fmt::format(
          "cache{}_pin{}",
          info.param.enableCache ? "On" : "Off",
          info.param.pinMetadata ? "On" : "Off");
    });

} // namespace facebook::nimble::test
