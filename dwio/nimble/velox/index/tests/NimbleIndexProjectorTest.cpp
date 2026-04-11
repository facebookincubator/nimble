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
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "dwio/nimble/velox/VeloxReader.h"
#include "dwio/nimble/velox/VeloxWriter.h"

#include "velox/common/memory/Memory.h"
#include "velox/serializers/KeyEncoder.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::nimble::test {

using namespace velox;
using namespace velox::dwio::common;
using Subfield = velox::common::Subfield;

struct TestParam {
  bool enableCache;
  bool pinFileMetadata;

  std::string debugString() const {
    return fmt::format(
        "enableCache={}, pinFileMetadata={}", enableCache, pinFileMetadata);
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
    IndexConfig indexConfig;
    indexConfig.columns = indexColumns;
    indexConfig.sortOrders = std::vector<SortOrder>(
        indexColumns.size(), SortOrder{.ascending = true});
    indexConfig.enforceKeyOrder = true;
    indexConfig.noDuplicateKey = true;
    options.indexConfig = std::move(indexConfig);

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
  NimbleIndexProjector createProjector(
      const std::vector<Subfield>& projectedSubfields) {
    auto readFile =
        std::make_shared<InMemoryReadFile>(std::string_view(sinkData_));
    dwio::common::ReaderOptions readerOptions(leafPool_.get());
    readerOptions.setFileFormat(FileFormat::NIMBLE);
    readerOptions.setFileMetadataCacheEnabled(GetParam().enableCache);
    readerOptions.setPinFileMetadata(GetParam().pinFileMetadata);
    return NimbleIndexProjector(
        std::move(readFile), projectedSubfields, readerOptions);
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
  auto result = projector.project(request, {});

  ASSERT_EQ(result.responses.size(), 1);
  const auto& response = result.responses[0];
  ASSERT_FALSE(response.slices.empty());
  EXPECT_FALSE(response.resumeKey.has_value());

  const auto& slice = response.slices[0];
  const auto& chunk = result.chunks[slice.chunkIndex];
  ASSERT_GT(chunk.numRows, 0);

  // Clone and coalesce chained IOBuf for deserialization.
  auto coalesced = chunk.data.cloneCoalescedAsValue();

  // Deserialize using nimble Deserializer with nimble schema.
  auto projectedVeloxType = ROW({"col_a", "col_b"}, {INTEGER(), VARCHAR()});
  auto projectedNimbleSchema = convertToNimbleType(*projectedVeloxType);
  DeserializerOptions deserOptions;
  deserOptions.hasHeader = true;
  deserOptions.useVarintRowCount = projector.useVarintRowCount();
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
  ASSERT_LT(slice.rows.startRow, rowResult->size());
  auto* colAResult = rowResult->childAt(0)->as<FlatVector<int32_t>>();
  ASSERT_NE(colAResult, nullptr);
  // key=50 -> colA=500
  EXPECT_EQ(colAResult->valueAt(slice.rows.startRow), 500);
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
  auto result = projector.project(request, {});

  ASSERT_EQ(result.responses.size(), 1);
  EXPECT_TRUE(result.responses[0].slices.empty());
  EXPECT_FALSE(result.responses[0].resumeKey.has_value());
  EXPECT_TRUE(result.chunks.empty());

  // All stats should be zero for a miss.
  EXPECT_EQ(projector.stats().numReadStripes, 0);
  EXPECT_EQ(projector.stats().numScannedRows, 0);
  EXPECT_EQ(projector.stats().numProjectedRows, 0);
  EXPECT_EQ(projector.stats().numReadRows, 0);
  EXPECT_EQ(projector.stats().rawBytesRead, 0);
  EXPECT_EQ(projector.stats().rawOverreadBytes, 0);
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
  auto result = projector.project(request, {});

  ASSERT_EQ(result.responses.size(), 3);
  uint64_t totalBytes = 0;
  for (const auto& chunk : result.chunks) {
    totalBytes += chunk.data.length();
  }
  EXPECT_GT(totalBytes, 0);

  // All requests should have results (no misses).
  for (const auto& response : result.responses) {
    EXPECT_FALSE(response.slices.empty());
    EXPECT_FALSE(response.resumeKey.has_value());
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

  // Empty request should yield zero stats.
  {
    auto proj = makeProjector();
    NimbleIndexProjector::Request request;
    proj.project(request, {});
    EXPECT_EQ(proj.stats().numReadStripes, 0);
    EXPECT_EQ(proj.stats().numScannedRows, 0);
    EXPECT_EQ(proj.stats().numProjectedRows, 0);
    EXPECT_EQ(proj.stats().numReadRows, 0);
    EXPECT_EQ(proj.stats().rawBytesRead, 0);
    EXPECT_EQ(proj.stats().rawOverreadBytes, 0);
  }

  // Single point lookup: key=15 is in stripe 1 (rows 10..19).
  {
    auto proj = makeProjector();
    auto bounds = makePointLookup(rowType, {"key"}, 15);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    proj.project(request, {});

    EXPECT_EQ(proj.stats().numReadStripes, 1);
    EXPECT_EQ(proj.stats().numScannedRows, rowsPerBatch);
    EXPECT_EQ(proj.stats().numProjectedRows, proj.stats().numScannedRows);
    EXPECT_EQ(proj.stats().numReadRows, 1);

    // Timings should be non-zero for a hit.
    EXPECT_GT(proj.stats().lookupTiming.count, 0);
    EXPECT_GT(proj.stats().lookupTiming.wallNanos, 0);
    EXPECT_GT(proj.stats().scanTiming.wallNanos, 0);
    EXPECT_GT(proj.stats().projectionTiming.wallNanos, 0);

    // IO stats should be non-zero for a hit.
    EXPECT_GT(proj.stats().rawBytesRead, 0);
    EXPECT_GE(proj.stats().rawOverreadBytes, 0);
  }

  // Two point lookups in first and last stripes.
  {
    auto proj = makeProjector();
    auto bounds0 = makePointLookup(rowType, {"key"}, 0);
    auto bounds1 = makePointLookup(rowType, {"key"}, numRows - 1);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds0, bounds1};
    proj.project(request, {});

    EXPECT_EQ(proj.stats().numReadStripes, 2);
    EXPECT_EQ(
        proj.stats().numScannedRows, static_cast<uint64_t>(2 * rowsPerBatch));
    EXPECT_EQ(proj.stats().numProjectedRows, proj.stats().numScannedRows);
    EXPECT_EQ(proj.stats().numReadRows, 2);

    // IO stats should reflect more data read than single lookup.
    EXPECT_GT(proj.stats().rawBytesRead, 0);
  }

  // Point lookup for a missing key (beyond max) should yield zero stats.
  {
    auto proj = makeProjector();
    auto bounds = makePointLookup(rowType, {"key"}, numRows + 1'000);
    NimbleIndexProjector::Request request;
    request.keyBounds = {bounds};
    proj.project(request, {});

    EXPECT_EQ(proj.stats().numReadStripes, 0);
    EXPECT_EQ(proj.stats().numScannedRows, 0);
    EXPECT_EQ(proj.stats().numProjectedRows, 0);
    EXPECT_EQ(proj.stats().numReadRows, 0);
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
  EXPECT_EQ(
      stats.toString(),
      "Stats(numReadStripes=3, numScannedRows=1000, numProjectedRows=1000, numReadRows=42, "
      "numReadBytes=8.00KB, rawBytesRead=9.77KB, rawOverreadBytes=1.77KB, numStorageReads=5, "
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
  auto result = projector.project(request, {});

  ASSERT_EQ(result.responses.size(), 1);
  const auto& response = result.responses[0];
  ASSERT_FALSE(response.slices.empty());

  const auto& slice = response.slices[0];
  const auto& chunk = result.chunks[slice.chunkIndex];
  ASSERT_GT(chunk.numRows, 0);

  // Deserialize using the projector's projected nimble type which accounts
  // for FlatMap key sorting (keys sorted alphabetically: "b", "d", "e").
  auto coalesced = chunk.data.cloneCoalescedAsValue();
  DeserializerOptions deserOptions;
  deserOptions.hasHeader = true;
  deserOptions.useVarintRowCount = projector.useVarintRowCount();
  Deserializer deserializer(
      projector.projectedNimbleType(), leafPool_.get(), deserOptions);

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
  // target row to verify correctness.
  auto* mapResult = rowResult->childAt(0)->as<MapVector>();
  ASSERT_NE(mapResult, nullptr);

  const auto targetRow = slice.rows.startRow;
  ASSERT_LT(targetRow, mapResult->size());

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
  const auto& buildSchema = projector.projectedNimbleType();
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
  auto result = projector.project(request, {});

  ASSERT_EQ(result.responses.size(), 1);
  ASSERT_FALSE(result.responses[0].slices.empty());

  const auto& slice = result.responses[0].slices[0];
  const auto& chunk = result.chunks[slice.chunkIndex];
  auto coalesced = chunk.data.cloneCoalescedAsValue();
  auto data = std::string_view(
      reinterpret_cast<const char*>(coalesced.data()), coalesced.length());

  DeserializerOptions deserOptions;
  deserOptions.hasHeader = true;
  deserOptions.useVarintRowCount = projector.useVarintRowCount();
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
  auto result = projector.project(request, {});

  ASSERT_EQ(result.responses.size(), 1);
  const auto& response = result.responses[0];
  ASSERT_FALSE(response.slices.empty());

  const auto& slice = response.slices[0];
  const auto& chunk = result.chunks[slice.chunkIndex];
  ASSERT_GT(chunk.numRows, 0);

  // Deserialize using the projector's projected nimble type.
  auto coalesced = chunk.data.cloneCoalescedAsValue();
  DeserializerOptions deserOptions;
  deserOptions.hasHeader = true;
  deserOptions.useVarintRowCount = projector.useVarintRowCount();
  Deserializer deserializer(
      projector.projectedNimbleType(), leafPool_.get(), deserOptions);

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

  const auto targetRow = slice.rows.startRow;
  ASSERT_LT(targetRow, mapResult->size());

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
    const auto& schema = projector.projectedNimbleType();
    ASSERT_TRUE(schema->isRow());
    const auto& flatMap = schema->asRow().childAt(0)->asFlatMap();
    ASSERT_EQ(flatMap.childrenCount(), 2);
    EXPECT_EQ(flatMap.nameAt(0), "a");
    EXPECT_EQ(flatMap.nameAt(1), "c");

    // Verify data reads correctly.
    auto pointBounds = makePointLookup(rowType, {"key"}, 50);
    NimbleIndexProjector::Request request;
    request.keyBounds = {pointBounds};
    auto result = projector.project(request, {});
    ASSERT_EQ(result.responses.size(), 1);
    ASSERT_FALSE(result.responses[0].slices.empty());
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

    IndexConfig indexConfig;
    indexConfig.columns = {"key"};
    indexConfig.sortOrders = {SortOrder{.ascending = true}};
    indexConfig.enforceKeyOrder = true;
    indexConfig.noDuplicateKey = true;
    options.indexConfig = std::move(indexConfig);

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
    auto readFile =
        std::make_shared<InMemoryReadFile>(std::string_view(sinkData_));
    dwio::common::ReaderOptions readerOptions(leafPool_.get());
    readerOptions.setFileFormat(FileFormat::NIMBLE);
    readerOptions.setMaxCoalesceDistance(maxCoalesceDistance);

    std::vector<Subfield> subfields;
    for (auto key : projectedKeys) {
      subfields.emplace_back(fmt::format("features[{}]", key));
    }
    return NimbleIndexProjector(std::move(readFile), subfields, readerOptions);
  };

  // Part 1: Verify stream adjacency on disk with feature reordering.
  {
    writeFile(/*enableReordering=*/true);
    auto readFile =
        std::make_shared<InMemoryReadFile>(std::string_view(sinkData_));
    auto tablet = TabletReader::create(readFile, leafPool_.get(), {});
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
    auto result = projector.project(request, {});

    ASSERT_EQ(result.responses.size(), 1);
    ASSERT_FALSE(result.responses[0].slices.empty());

    storageReads[param.debugString()] = projector.stats().numStorageReads;
  }

  // With zero coalesce distance, feature reordering should result in fewer
  // storage reads because projected streams are adjacent on disk.
  auto readsReorder0 = storageReads[ReorderParam{true, 0}.debugString()];
  auto readsNoReorder0 = storageReads[ReorderParam{false, 0}.debugString()];
  EXPECT_LT(readsReorder0, readsNoReorder0)
      << "Feature reordering should reduce storage reads with zero coalesce. "
      << "With reorder: " << readsReorder0 << ", without: " << readsNoReorder0;

  // With large coalesce distance, reordering should not increase reads.
  auto readsReorderLarge =
      storageReads[ReorderParam{true, 512 << 10}.debugString()];
  auto readsNoReorderLarge =
      storageReads[ReorderParam{false, 512 << 10}.debugString()];
  EXPECT_LE(readsReorderLarge, readsNoReorderLarge)
      << "With large coalesce, reordering should not increase reads. "
      << "With reorder: " << readsReorderLarge
      << ", without: " << readsNoReorderLarge;
}

// Verifies that pinFileMetadata prevents re-reading metadata on repeated
// project() calls within the same projector. Cross-reader caching via
// AsyncDataCache requires CachedBufferedInput support in
// NimbleIndexProjector (followup).
TEST_P(NimbleIndexProjectorTest, pinnedMetadataNoReread) {
  if (!GetParam().pinFileMetadata) {
    GTEST_SKIP() << "Only applicable when pinFileMetadata is enabled";
  }

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

  auto innerFile =
      std::make_shared<InMemoryReadFile>(std::string_view(sinkData_));
  auto trackingFile = std::make_shared<testing::TrackingReadFile>(innerFile);

  // Compute metadata boundary: the start of the first stripe group metadata.
  auto tablet = TabletReader::create(innerFile, leafPool_.get(), {});
  auto stripeGroupsMeta = tablet->stripeGroupsMetadata();
  ASSERT_FALSE(stripeGroupsMeta.empty());
  const auto metadataBoundary = stripeGroupsMeta[0].offset();
  tablet.reset();

  dwio::common::ReaderOptions readerOptions(leafPool_.get());
  readerOptions.setFileFormat(FileFormat::NIMBLE);
  readerOptions.setFileMetadataCacheEnabled(GetParam().enableCache);
  readerOptions.setPinFileMetadata(GetParam().pinFileMetadata);

  std::vector<Subfield> subfields;
  subfields.emplace_back("value");
  NimbleIndexProjector projector(trackingFile, subfields, readerOptions);

  auto bounds = makePointLookup(rowType, {"key"}, 50);
  NimbleIndexProjector::Request request;
  request.keyBounds = {bounds};

  // First project: reads both data and metadata regions.
  projector.project(request, {});
  ASSERT_GT(trackingFile->maxReadOffset(), metadataBoundary)
      << "First project should read metadata beyond the boundary";

  // Second project on the same projector: pinned metadata should avoid
  // re-reading beyond the metadata boundary.
  trackingFile->resetMaxReadOffset();
  projector.project(request, {});
  EXPECT_LE(trackingFile->maxReadOffset(), metadataBoundary)
      << "Second project should not re-read metadata when "
      << GetParam().debugString();
}

INSTANTIATE_TEST_CASE_P(
    AllCacheParams,
    NimbleIndexProjectorTest,
    ::testing::ValuesIn(NimbleIndexProjectorTest::getTestParams()),
    [](const ::testing::TestParamInfo<TestParam>& info) {
      return fmt::format(
          "cache{}_pin{}",
          info.param.enableCache ? "On" : "Off",
          info.param.pinFileMetadata ? "On" : "Off");
    });

} // namespace facebook::nimble::test
