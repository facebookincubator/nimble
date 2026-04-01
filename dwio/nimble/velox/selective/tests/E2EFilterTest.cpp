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

#include <utility>

#include "dwio/nimble/encodings/EncodingLayout.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/velox/ChunkedStream.h"
#include "dwio/nimble/velox/EncodingLayoutTree.h"
#include "dwio/nimble/velox/SchemaSerialization.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "dwio/nimble/velox/selective/SelectiveNimbleReader.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/ScanTracker.h"
#include "velox/common/memory/MallocAllocator.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/tests/utils/E2EFilterTestBase.h"

namespace facebook::nimble {
namespace {

using namespace facebook::velox;

using E2EFilterTestParams = std::tuple<bool, bool, bool, bool, bool, bool>;

struct E2EFilterTestParam {
  bool indexEnabled;
  bool skipConstantFlatMapInMapStreams;
  bool enableChunkIndex;
  bool stringBufferOptimized;
  bool pinFileMetadata;
  bool enableCache;

  explicit E2EFilterTestParam(const E2EFilterTestParams& t)
      : indexEnabled{std::get<0>(t)},
        skipConstantFlatMapInMapStreams{std::get<1>(t)},
        enableChunkIndex{std::get<2>(t)},
        stringBufferOptimized{std::get<3>(t)},
        pinFileMetadata{std::get<4>(t)},
        enableCache{std::get<5>(t)} {}

  std::string debugString() const {
    return fmt::format(
        "index_{}_skipConstantInMap_{}_chunkIndex_{}_stringBufferOptimized_{}_pinMetadata_{}_cache_{}",
        indexEnabled,
        skipConstantFlatMapInMapStreams,
        enableChunkIndex,
        stringBufferOptimized,
        pinFileMetadata,
        enableCache);
  }
};

class E2EFilterTest
    : public dwio::common::E2EFilterTestBase,
      public ::testing::WithParamInterface<E2EFilterTestParams> {
 protected:
  static void SetUpTestCase() {
    E2EFilterTestBase::SetUpTestCase();
    registerSelectiveNimbleReaderFactory();
  }

  void SetUp() override {
    E2EFilterTestBase::SetUp();
    batchSize_ = 2003;
    batchCount_ = 5;
    testRowGroupSkip_ = false;
    if (param().enableCache) {
      allocator_ = std::make_shared<memory::MallocAllocator>(
          memory::MemoryAllocator::Options{
              .capacity = 512 << 20, .reservationByteLimit = 0});
      cache_ = cache::AsyncDataCache::create(allocator_.get());
      cacheIoStatistics_ = std::make_shared<io::IoStatistics>();
      scanTracker_ = std::make_shared<cache::ScanTracker>(
          "testTracker", nullptr, 256 << 10);
      ioExecutor_ = std::make_unique<folly::CPUThreadPoolExecutor>(10);
    }
  }

  void TearDown() override {
    ioExecutor_.reset();
    if (cache_ != nullptr) {
      cache_->shutdown();
    }
    cache_.reset();
    allocator_.reset();
  }

  E2EFilterTestParam param() const {
    return E2EFilterTestParam{GetParam()};
  }

  virtual bool indexEnabled() const {
    return param().indexEnabled;
  }

  bool skipConstantFlatMapInMapStreams() const {
    return param().skipConstantFlatMapInMapStreams;
  }

  void setUpRowReaderOptions(
      dwio::common::RowReaderOptions& opts,
      const std::shared_ptr<dwio::common::ScanSpec>& spec) override {
    opts.setScanSpec(spec);
    opts.setTimestampPrecision(TimestampPrecision::kNanoseconds);
    opts.setIndexEnabled(indexEnabled());
    opts.setPassStringBuffersFromDecoder(param().stringBufferOptimized);
  }

  void testWithTypes(
      const std::string& columns,
      std::function<void()> customize,
      bool wrapInStruct,
      const std::vector<std::string>& filterable,
      int32_t numCombinations,
      bool withRecursiveNulls = true,
      std::optional<std::vector<std::pair<EncodingType, float>>>
          encodingFactors = std::nullopt) {
    // Select index columns if index is enabled.
    std::vector<std::string> indexColumns;
    if (indexEnabled()) {
      indexColumns = selectIndexColumns(columns, wrapInStruct);
    }

    // Set encoding factors for this test run.
    encodingFactors_ = encodingFactors;

    if (!withRecursiveNulls) {
      testScenario(
          columns,
          customize,
          wrapInStruct,
          filterable,
          numCombinations,
          /*withRecursiveNulls=*/false,
          indexColumns);
      encodingFactors_.reset();
      return;
    }

    testScenario(
        columns,
        customize,
        wrapInStruct,
        filterable,
        numCombinations,
        /*withRecursiveNulls=*/true,
        indexColumns);
    auto noNullCustomize = [&] {
      customize();
      makeNotNull();
    };
    testScenario(
        columns,
        noNullCustomize,
        wrapInStruct,
        filterable,
        numCombinations,
        /*withRecursiveNulls=*/true,
        indexColumns);

    // Clear encoding factors after test run.
    encodingFactors_.reset();
  }

  // Selects eligible index columns (integer types) from the schema.
  // Returns 1-2 randomly selected columns.
  std::vector<std::string> selectIndexColumns(
      const std::string& columns,
      bool wrapInStruct) {
    auto rowType =
        velox::test::DataSetBuilder::makeRowType(columns, wrapInStruct);
    std::vector<std::string> eligibleColumns;
    for (column_index_t i = 0; i < static_cast<column_index_t>(rowType->size());
         ++i) {
      const auto& childType = rowType->childAt(i);
      // Only top-level integer and varchar types are eligible for indexing.
      if (childType->kind() == TypeKind::TINYINT ||
          childType->kind() == TypeKind::SMALLINT ||
          childType->kind() == TypeKind::INTEGER ||
          childType->kind() == TypeKind::BIGINT ||
          childType->kind() == TypeKind::VARCHAR) {
        eligibleColumns.push_back(rowType->nameOf(i));
      }
    }

    if (eligibleColumns.empty()) {
      return {};
    }

    // Randomly select 1-2 columns.
    std::mt19937 gen(seed_);
    std::shuffle(eligibleColumns.begin(), eligibleColumns.end(), gen);
    const size_t numColumns =
        std::min(eligibleColumns.size(), static_cast<size_t>(1 + gen() % 2));
    eligibleColumns.resize(numColumns);
    return eligibleColumns;
  }

  void testRunLengthDictionaryWithTypes(
      const std::string& columns,
      std::function<void()> customize,
      bool wrapInStruct,
      const std::vector<std::string>& filterable,
      int32_t numCombinations,
      bool withRecursiveNulls,
      int64_t dictionaryGenSeed) {
    constexpr size_t kMaxRunLength = 10;
    if (withRecursiveNulls) {
      testRunLengthDictionaryScenario(
          columns,
          customize,
          wrapInStruct,
          filterable,
          numCombinations,
          kMaxRunLength,
          /*withRecursiveNulls=*/true,
          dictionaryGenSeed);
      auto noNullCustomize = [&] {
        customize();
        makeNotNull();
      };
      testRunLengthDictionaryScenario(
          columns,
          noNullCustomize,
          wrapInStruct,
          filterable,
          numCombinations,
          kMaxRunLength,
          /*withRecursiveNulls=*/true,
          dictionaryGenSeed);
    } else {
      testRunLengthDictionaryScenario(
          columns,
          customize,
          wrapInStruct,
          filterable,
          numCombinations,
          kMaxRunLength,
          /*withRecursiveNulls=*/false,
          dictionaryGenSeed);
    }
  }

  void writeToMemory(
      const TypePtr& type,
      const std::vector<RowVectorPtr>& batches,
      bool forRowGroupSkip,
      const std::vector<std::string>& indexColumns = {}) override {
    VELOX_CHECK(!forRowGroupSkip);
    // Clear cached IO data from previous writes to avoid stale cache entries.
    if (cache_ != nullptr) {
      cache_->clear();
    }
    rowType_ = asRowType(type);
    writeSchema_ = rowType_;
    VeloxWriterOptions options;
    options.skipConstantFlatMapInMapStreams = skipConstantFlatMapInMapStreams();
    options.enableChunking = true;
    options.enableChunkIndex = param().enableChunkIndex;

    // Force a specific encoding via layout tree, or use biased factors.
    if (forcedEncodingType_.has_value()) {
      auto encodingType = forcedEncodingType_.value();
      std::vector<EncodingLayoutTree> children;
      for (column_index_t col = 0;
           col < static_cast<column_index_t>(rowType_->size());
           ++col) {
        EncodingLayout layout{
            encodingType,
            {},
            CompressionType::Uncompressed,
            {EncodingLayout{
                 EncodingType::Trivial, {}, CompressionType::Uncompressed},
             EncodingLayout{
                 EncodingType::Trivial, {}, CompressionType::Uncompressed},
             EncodingLayout{
                 EncodingType::Trivial, {}, CompressionType::Uncompressed}}};
        children.push_back(
            EncodingLayoutTree{
                Kind::Scalar,
                {{0, std::move(layout)}},
                std::string(rowType_->nameOf(col))});
      }
      options.encodingLayoutTree.emplace(
          Kind::Row,
          std::unordered_map<
              EncodingLayoutTree::StreamIdentifier,
              EncodingLayout>{},
          "",
          std::move(children));
    } else if (encodingFactors_.has_value()) {
      // Use custom encoding factors if set.
      // Capture by value to avoid dangling reference.
      auto factors = encodingFactors_.value();
      options.encodingSelectionPolicyFactory = [factors](DataType dataType) {
        ManualEncodingSelectionPolicyFactory factory(factors);
        return factory.createPolicy(dataType);
      };
    }

    auto i = 0;
    options.flushPolicyFactory = [&] {
      return std::make_unique<LambdaFlushPolicy>(
          /*flushLambda=*/[&](const StripeProgress&) { return (i++ % 3 == 2); },
          /*chunkLambda=*/
          [&](const StripeProgress&) { return (i++ % 3 == 2); });
    };
    if (!flatMapColumns_.empty()) {
      setUpFlatMapColumns();
      options.flatMapColumns = flatMapColumns_;
    }
    if (!deduplicatedArrayColumns_.empty()) {
      options.dictionaryArrayColumns = deduplicatedArrayColumns_;
    }
    if (!deduplicatedMapColumns_.empty()) {
      options.deduplicatedMapColumns = deduplicatedMapColumns_;
    }
    // Configure index if index columns are specified.
    if (!indexColumns.empty()) {
      IndexConfig indexConfig;
      indexConfig.columns = indexColumns;
      indexConfig.enforceKeyOrder = true;
      options.indexConfig = std::move(indexConfig);
    }
    auto writeFile = std::make_unique<InMemoryWriteFile>(&sinkData_);
    VeloxWriter writer(
        writeSchema_, std::move(writeFile), *rootPool_, std::move(options));
    for (auto& batch : batches) {
      writer.write(batch);
    }
    writer.close();
  }

  std::unique_ptr<dwio::common::Reader> makeReader(
      const dwio::common::ReaderOptions& opts,
      std::unique_ptr<dwio::common::BufferedInput> input) final {
    auto readerOpts = opts;
    readerOpts.setPinFileMetadata(param().pinFileMetadata);
    if (param().enableCache) {
      auto readFile = std::make_shared<InMemoryReadFile>(sinkData_);
      auto& ids = fileIds();
      StringIdLease fileId(ids, "testFile");
      StringIdLease groupId(ids, "testGroup");
      io::ReaderOptions cacheReaderOpts(&readerOpts.memoryPool());
      input = std::make_unique<dwio::common::CachedBufferedInput>(
          std::move(readFile),
          dwio::common::MetricsLog::voidLog(),
          std::move(fileId),
          cache_.get(),
          scanTracker_,
          std::move(groupId),
          cacheIoStatistics_,
          nullptr,
          ioExecutor_.get(),
          cacheReaderOpts);
    }
    auto factory =
        dwio::common::getReaderFactory(dwio::common::FileFormat::NIMBLE);
    return factory->createReader(std::move(input), readerOpts);
  }

  folly::F14FastMap<std::string, std::set<std::string>> flatMapColumns_;
  folly::F14FastSet<std::string> deduplicatedArrayColumns_;
  folly::F14FastSet<std::string> deduplicatedMapColumns_;

  // Optional encoding factors for ManualEncodingSelectionPolicyFactory.
  // When set, writeToMemory will use these factors instead of the default
  // encoding selection policy. Clear after test to avoid affecting other tests.
  std::optional<std::vector<std::pair<EncodingType, float>>> encodingFactors_;

  // Optional forced encoding type. When set, writeToMemory builds an
  // EncodingLayoutTree that forces every scalar column to use this encoding.
  // Takes precedence over encodingFactors_.
  std::optional<EncodingType> forcedEncodingType_;

  // Cache infrastructure (only initialized when enableCache is true).
  std::shared_ptr<memory::MallocAllocator> allocator_;
  std::shared_ptr<cache::AsyncDataCache> cache_;
  std::shared_ptr<io::IoStatistics> cacheIoStatistics_;
  std::shared_ptr<cache::ScanTracker> scanTracker_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> ioExecutor_;

  // Generates encoding factors that strongly favor a specific encoding type.
  // Encodings with lower read factors are preferred (factor represents CPU
  // cost). The favored encoding gets a low factor (0.1), while others get high
  // factors (100.0). Only includes encodings in the pool; omitting encodings
  // from the pool prevents them from being selected. Trivial encoding is always
  // included as a fallback.
  static std::vector<std::pair<EncodingType, float>> biasedEncodingFactors(
      EncodingType favored) {
    std::vector<std::pair<EncodingType, float>> factors;

    // Trivial is always needed as a fallback when other encodings don't apply.
    // Use a high factor to discourage it unless necessary.
    factors.emplace_back(EncodingType::Trivial, 100.0);

    // Add the favored encoding with a low factor (strongly preferred).
    if (favored != EncodingType::Trivial) {
      factors.emplace_back(favored, 0.1);
    } else {
      // If Trivial is favored, just use it with low factor.
      factors[0].second = 0.1;
    }

    // For certain encodings, we need to include additional encodings in the
    // pool to handle edge cases (e.g., constant values).
    switch (favored) {
      case EncodingType::FixedBitWidth:
      case EncodingType::RLE:
      case EncodingType::Varint:
        // These may fall back to Constant for constant data.
        factors.emplace_back(EncodingType::Constant, 100.0);
        break;
      case EncodingType::MainlyConstant:
        // MainlyConstant needs Constant for the common value.
        factors.emplace_back(EncodingType::Constant, 100.0);
        break;
      case EncodingType::Delta:
        // Delta uses child encodings for deltas, restatements, and
        // isRestatements that may fall back to Constant.
        factors.emplace_back(EncodingType::Constant, 100.0);
        break;
      default:
        break;
    }

    return factors;
  }

  // Verifies that all scalar columns in the file written to sinkData_ use
  // the expected encoding type. Accounts for nullable wrapping.
  void verifyColumnEncodingsOnDisk(EncodingType expectedEncodingType) {
    auto readFile = std::make_shared<InMemoryReadFile>(sinkData_);
    auto& pool = *leafPool_;
    auto tablet = TabletReader::create(readFile, &pool, {});
    auto section = tablet->loadOptionalSection(std::string(kSchemaSection));
    ASSERT_TRUE(section.has_value());
    auto schema = SchemaDeserializer::deserialize(section->content().data());

    for (uint32_t col = 0; col < schema->asRow().childrenCount(); ++col) {
      auto& childNode = schema->asRow().childAt(col)->asScalar();
      for (auto i = 0; i < tablet->stripeCount(); ++i) {
        auto stripeIdentifier = tablet->stripeIdentifier(i);
        std::vector<uint32_t> identifiers{
            childNode.scalarDescriptor().offset()};
        auto streams = tablet->load(stripeIdentifier, identifiers);

        InMemoryChunkedStream chunkedStream{pool, std::move(streams[0])};
        if (!chunkedStream.hasNext()) {
          continue;
        }
        auto capture =
            EncodingLayoutCapture::capture(chunkedStream.nextChunk());
        // The top-level encoding may be Nullable (wrapping the data encoding)
        // or the data encoding directly.
        if (capture.encodingType() == EncodingType::Nullable) {
          ASSERT_TRUE(capture.child(1).has_value())
              << "Column " << col << " stripe " << i;
          EXPECT_EQ(expectedEncodingType, capture.child(1)->encodingType())
              << "Column " << col << " stripe " << i;
        } else {
          EXPECT_EQ(expectedEncodingType, capture.encodingType())
              << "Column " << col << " stripe " << i;
        }
      }
    }
  }

 private:
  void setUpFlatMapColumns() {
    auto rowTypeWithId = dwio::common::TypeWithId::create(rowType_);
    auto names = rowType_->names();
    auto types = rowType_->children();
    for (int i = 0; i < rowType_->size(); ++i) {
      if (!flatMapColumns_.contains(rowType_->nameOf(i))) {
        continue;
      }
      auto& type = rowType_->childAt(i);
      if (!type->isRow()) {
        continue;
      }
      types[i] = MAP(VARCHAR(), type->childAt(0));
    }
    writeSchema_ = ROW(std::move(names), std::move(types));
  }

  RowTypePtr writeSchema_;
};

TEST_P(E2EFilterTest, byte) {
  testWithTypes(
      "tiny_val:tinyint,"
      "bool_val:boolean,"
      "long_val:bigint,"
      "tiny_null:bigint",
      [&]() { makeAllNulls("tiny_null"); },
      true,
      {"tiny_val", "bool_val", "tiny_null"},
      20);
}

TEST_P(E2EFilterTest, integer) {
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "long_null:bigint",
      [&] { makeAllNulls("long_null"); },
      true,
      {"short_val", "int_val", "long_val"},
      20);
}

// Biased variant that forces FixedBitWidth encoding selection.
TEST_P(E2EFilterTest, integerBiasedFixedBitWidth) {
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&]() {
        makeIntDistribution<int16_t>(
            "short_val",
            /*min=*/10,
            /*max=*/1000,
            /*repeats=*/1,
            /*rareFrequency=*/0,
            /*rareMin=*/0,
            /*rareMax=*/0,
            /*keepNulls=*/false);
        makeIntDistribution<int32_t>(
            "int_val",
            /*min=*/100,
            /*max=*/100000,
            /*repeats=*/1,
            /*rareFrequency=*/0,
            /*rareMin=*/0,
            /*rareMax=*/0,
            /*keepNulls=*/false);
        makeIntDistribution<int64_t>(
            "long_val",
            /*min=*/1000,
            /*max=*/1000000,
            /*repeats=*/1,
            /*rareFrequency=*/0,
            /*rareMin=*/0,
            /*rareMax=*/0,
            /*keepNulls=*/false);
      },
      /*wrapInStruct=*/false,
      {"short_val", "int_val", "long_val"},
      /*numCombinations=*/20,
      /*withRecursiveNulls=*/true,
      biasedEncodingFactors(EncodingType::FixedBitWidth));
}

// Biased variant that forces Trivial encoding selection.
TEST_P(E2EFilterTest, integerBiasedTrivial) {
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&]() {
        makeIntDistribution<int16_t>(
            "short_val",
            /*min=*/10,
            /*max=*/1000,
            /*repeats=*/1,
            /*rareFrequency=*/0,
            /*rareMin=*/0,
            /*rareMax=*/0,
            /*keepNulls=*/false);
        makeIntDistribution<int32_t>(
            "int_val",
            /*min=*/100,
            /*max=*/100000,
            /*repeats=*/1,
            /*rareFrequency=*/0,
            /*rareMin=*/0,
            /*rareMax=*/0,
            /*keepNulls=*/false);
        makeIntDistribution<int64_t>(
            "long_val",
            /*min=*/1000,
            /*max=*/1000000,
            /*repeats=*/1,
            /*rareFrequency=*/0,
            /*rareMin=*/0,
            /*rareMax=*/0,
            /*keepNulls=*/false);
      },
      /*wrapInStruct=*/false,
      {"short_val", "int_val", "long_val"},
      /*numCombinations=*/20,
      /*withRecursiveNulls=*/true,
      biasedEncodingFactors(EncodingType::Trivial));
}

TEST_P(E2EFilterTest, integerLowCardinality) {
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&]() {
        makeIntDistribution<int64_t>(
            "long_val",
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -9999, // rareMin
            10000000000, // rareMax
            true); // keepNulls
        makeIntDistribution<int32_t>(
            "int_val",
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -9999, // rareMin
            100000000, // rareMax
            false); // keepNulls
        makeIntDistribution<int16_t>(
            "short_val",
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -999, // rareMin
            30000, // rareMax
            true); // keepNulls
      },
      true,
      {"short_val", "int_val", "long_val"},
      20);
}

TEST_P(E2EFilterTest, integerRle) {
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&] {
        makeIntRle<int16_t>("short_val");
        makeIntRle<int32_t>("int_val");
        makeIntRle<int64_t>("long_val");
        makeIntRle<int16_t>("struct_val.short_val");
        makeIntRle<int32_t>("struct_val.int_val");
        makeIntRle<int64_t>("struct_val.long_val");
      },
      true,
      {"short_val", "int_val", "long_val"},
      20);
}

// Biased variant that forces RLE encoding selection.
TEST_P(E2EFilterTest, integerRleBiased) {
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&]() {
        makeIntRle<int16_t>("short_val");
        makeIntRle<int32_t>("int_val");
        makeIntRle<int64_t>("long_val");
        makeIntRle<int16_t>("struct_val.short_val");
        makeIntRle<int32_t>("struct_val.int_val");
        makeIntRle<int64_t>("struct_val.long_val");
      },
      /*wrapInStruct=*/true,
      {"short_val", "int_val", "long_val"},
      /*numCombinations=*/20,
      /*withRecursiveNulls=*/true,
      biasedEncodingFactors(EncodingType::RLE));
}

TEST_P(E2EFilterTest, integerRleCustomSeed_4049269257) {
  if (common::testutil::useRandomSeed()) {
    return;
  }
  seed_ = 4049269257;
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&] {
        makeIntRle<int16_t>("short_val");
        makeIntRle<int32_t>("int_val");
        makeIntRle<int64_t>("long_val");
        makeIntRle<int16_t>("struct_val.short_val");
        makeIntRle<int32_t>("struct_val.int_val");
        makeIntRle<int64_t>("struct_val.long_val");
      },
      true,
      {"short_val", "int_val", "long_val"},
      20);
}

TEST_P(E2EFilterTest, integerRleCustomSeed_583694982) {
  if (common::testutil::useRandomSeed()) {
    return;
  }
  seed_ = 583694982;
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&] {
        makeIntRle<int16_t>("short_val");
        makeIntRle<int32_t>("int_val");
        makeIntRle<int64_t>("long_val");
        makeIntRle<int16_t>("struct_val.short_val");
        makeIntRle<int32_t>("struct_val.int_val");
        makeIntRle<int64_t>("struct_val.long_val");
      },
      true,
      {"short_val", "int_val", "long_val"},
      20);
}

TEST_P(E2EFilterTest, integerRleCustomSeed_2518626933) {
  if (common::testutil::useRandomSeed()) {
    return;
  }
  seed_ = 2518626933;
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&] {
        makeIntRle<int16_t>("short_val");
        makeIntRle<int32_t>("int_val");
        makeIntRle<int64_t>("long_val");
        makeIntRle<int16_t>("struct_val.short_val");
        makeIntRle<int32_t>("struct_val.int_val");
        makeIntRle<int64_t>("struct_val.long_val");
      },
      true,
      {"short_val", "int_val", "long_val"},
      20);
}

TEST_P(E2EFilterTest, mainlyConstant) {
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&] {
        makeIntMainlyConstant<int16_t>("short_val");
        makeIntMainlyConstant<int32_t>("int_val");
        makeIntMainlyConstant<int64_t>("long_val");
        makeIntMainlyConstant<int16_t>("struct_val.short_val");
        makeIntMainlyConstant<int32_t>("struct_val.int_val");
        makeIntMainlyConstant<int64_t>("struct_val.long_val");
      },
      true,
      {"short_val", "int_val", "long_val"},
      20);
}

// Biased variant that forces MainlyConstant encoding selection.
TEST_P(E2EFilterTest, mainlyConstantBiased) {
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&]() {
        makeIntMainlyConstant<int16_t>("short_val");
        makeIntMainlyConstant<int32_t>("int_val");
        makeIntMainlyConstant<int64_t>("long_val");
      },
      /*wrapInStruct=*/false,
      {"short_val", "int_val", "long_val"},
      /*numCombinations=*/20,
      /*withRecursiveNulls=*/true,
      biasedEncodingFactors(EncodingType::MainlyConstant));
}

// Forces Delta encoding via encoding layout tree and verifies it on disk.
TEST_P(E2EFilterTest, integerBiasedDelta) {
  forcedEncodingType_ = EncodingType::Delta;
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&]() {
        makeIntDistribution<int16_t>(
            "short_val",
            /*min=*/10,
            /*max=*/1000,
            /*repeats=*/1,
            /*rareFrequency=*/0,
            /*rareMin=*/0,
            /*rareMax=*/0,
            /*keepNulls=*/false);
        makeIntDistribution<int32_t>(
            "int_val",
            /*min=*/100,
            /*max=*/100000,
            /*repeats=*/1,
            /*rareFrequency=*/0,
            /*rareMin=*/0,
            /*rareMax=*/0,
            /*keepNulls=*/false);
        makeIntDistribution<int64_t>(
            "long_val",
            /*min=*/1000,
            /*max=*/1000000,
            /*repeats=*/1,
            /*rareFrequency=*/0,
            /*rareMin=*/0,
            /*rareMax=*/0,
            /*keepNulls=*/false);
      },
      /*wrapInStruct=*/false,
      {"short_val", "int_val", "long_val"},
      /*numCombinations=*/20,
      /*withRecursiveNulls=*/true);
  forcedEncodingType_.reset();
  // Verify the on-disk encoding is Delta (sinkData_ holds the last written
  // file from testWithTypes).
  verifyColumnEncodingsOnDisk(EncodingType::Delta);
}

TEST_P(E2EFilterTest, float) {
  testWithTypes(
      "float_val:float,"
      "double_val:double,"
      "long_val:bigint,"
      "float_null:float",
      [&]() { makeAllNulls("float_null"); },
      true,
      {"float_val", "double_val", "float_null"},
      20);
}

TEST_P(E2EFilterTest, string) {
  testWithTypes(
      "string_val:string,"
      "string_val_2:string",
      [&]() {
        makeStringUnique("string_val");
        makeStringUnique("string_val_2");
      },
      true,
      {"string_val", "string_val_2"},
      20);
}

TEST_P(E2EFilterTest, stringDictionary) {
  testWithTypes(
      "string_val:string,"
      "string_val_2:string",
      [&]() {
        makeStringDistribution("string_val", 100, true, false);
        makeStringDistribution("string_val_2", 170, false, true);
      },
      true,
      {"string_val", "string_val_2"},
      20);
}

TEST_P(E2EFilterTest, listAndMapNoRecursiveNulls) {
  int numCombinations = 20;
#if !defined(NDEBUG) || defined(TSAN_BUILD)
  // The test is running slow under dev/debug and TSAN build; reduce the number
  // of combinations to avoid timeout.
  numCombinations = 2;
#endif
  testWithTypes(
      "long_val:bigint,"
      "long_val_2:bigint,"
      "int_val:int,"
      "simple_array_val:array<int>,"
      "array_val:array<struct<array_member: array<int>, float_val:float,long_val:bigint, string_val:string>>"
      "map_val:map<bigint,struct<nested_map: map<int, int>>>",
      [&]() {},
      true,
      {"long_val",
       "long_val_2",
       "int_val",
       "simple_array_val",
       "array_val",
       "map_val"},
      numCombinations,
      /*withRecursiveNulls=*/false);
}

TEST_P(E2EFilterTest, listAndMap) {
  int numCombinations = 20;
#if !defined(NDEBUG) || defined(TSAN_BUILD)
  // The test is running slow under dev/debug and TSAN build; reduce the number
  // of combinations to avoid timeout.
  numCombinations = 2;
#endif
  testWithTypes(
      "long_val:bigint,"
      "long_val_2:bigint,"
      "int_val:int,"
      "simple_array_val:array<int>,"
      "array_val:array<struct<array_member: array<int>, float_val:float,long_val:bigint, string_val:string>>"
      "map_val:map<bigint,struct<nested_map: map<int, int>>>",
      [&]() {},
      true,
      {"long_val",
       "long_val_2",
       "int_val",
       "simple_array_val",
       "array_val",
       "map_val"},
      numCombinations,
      /*withRecursiveNulls=*/true);
}

TEST_P(E2EFilterTest, listAndMapSmallReads) {
  int numCombinations = 20;
#if !defined(NDEBUG) || defined(TSAN_BUILD)
  // The test is running slow under dev/debug and TSAN build; reduce the number
  // of combinations to avoid timeout.
  numCombinations = 2;
#endif
  setReadSize(50);

  testWithTypes(
      "long_val:bigint,"
      "long_val_2:bigint,"
      "int_val:int,"
      "simple_array_val:array<int>,"
      "array_val:array<struct<array_member: array<int>, float_val:float,long_val:bigint, string_val:string>>"
      "map_val:map<bigint,struct<nested_map: map<int, int>>>",
      [&]() {},
      true,
      {"long_val",
       "long_val_2",
       "int_val",
       "simple_array_val",
       "array_val",
       "map_val"},
      numCombinations,
      /*withRecursiveNulls=*/true);
}

TEST_P(E2EFilterTest, deduplicatedArrayNoRecursiveNulls) {
  int numCombinations = 20;
#if !defined(NDEBUG) || defined(TSAN_BUILD)
  // The test is running slow under dev/debug and TSAN build; reduce the number
  // of combinations to avoid timeout.
  numCombinations = 2;
#endif
  deduplicatedArrayColumns_ = {"array_val"};
  testWithTypes(
      "long_val:bigint,"
      "int_val:int,"
      "array_val:array<int>",
      [&]() {},
      true,
      {"long_val", "int_val", "array_val"},
      numCombinations,
      /*withRecursiveNulls=*/false);

  std::mt19937 gen{seed_};
  testRunLengthDictionaryWithTypes(
      "long_val:bigint,"
      "int_val:int,"
      "array_val:array<int>",
      [&]() {},
      true,
      {"long_val", "int_val", "array_val"},
      numCombinations,
      /*withRecursiveNulls=*/false,
      folly::Random::rand64(gen));
}

TEST_P(E2EFilterTest, deduplicatedArray) {
  int numCombinations = 20;
#if !defined(NDEBUG) || defined(TSAN_BUILD)
  // The test is running slow under dev/debug and TSAN build; reduce the number
  // of combinations to avoid timeout.
  numCombinations = 2;
#endif
  deduplicatedArrayColumns_ = {"array_val"};
  testWithTypes(
      "long_val:bigint,"
      "int_val:int,"
      "array_val:array<int>",
      [&]() {},
      true,
      {"long_val", "int_val", "array_val"},
      numCombinations,
      /*withRecursiveNulls=*/true);

  std::mt19937 gen{seed_};
  testRunLengthDictionaryWithTypes(
      "long_val:bigint,"
      "int_val:int,"
      "array_val:array<int>",
      [&]() {},
      true,
      {"long_val", "int_val", "array_val"},
      numCombinations,
      /*withRecursiveNulls=*/true,
      folly::Random::rand64(gen));
}

// Pruning is only generated in struct_val.array_val in the normal case if
// filter is present on top level array_val, but struct_val.array_val is not
// deduplicated, so pruning is untested in normal case.
TEST_P(E2EFilterTest, deduplicatedArraySubfieldPruning) {
  int numCombinations = 20;
#if !defined(NDEBUG) || defined(TSAN_BUILD)
  // The test is running slow under dev/debug and TSAN build; reduce the number
  // of combinations to avoid timeout.
  numCombinations = 2;
#endif
  deduplicatedArrayColumns_ = {"array_val"};
  testWithTypes(
      "long_val:bigint,"
      "array_val:array<int>",
      [&]() {},
      false,
      {"long_val"},
      numCombinations,
      /*withRecursiveNulls=*/true);
  std::mt19937 gen{seed_};
  testRunLengthDictionaryWithTypes(
      "long_val:bigint,"
      "array_val:array<int>",
      [&]() {},
      false,
      {"long_val"},
      numCombinations,
      /*withRecursiveNulls=*/true,
      folly::Random::rand64(gen));
}

TEST_P(E2EFilterTest, deduplicatedArraySmallReads) {
  int numCombinations = 20;
#if !defined(NDEBUG) || defined(TSAN_BUILD)
  // The test is running slow under dev/debug and TSAN build; reduce the number
  // of combinations to avoid timeout.
  numCombinations = 2;
#endif
  setReadSize(50);

  deduplicatedArrayColumns_ = {"array_val"};
  testWithTypes(
      "long_val:bigint,"
      "int_val:int,"
      "array_val:array<int>",
      [&]() {},
      true,
      {"long_val", "int_val", "array_val"},
      numCombinations,
      /*withRecursiveNulls=*/true);

  std::mt19937 gen{seed_};
  testRunLengthDictionaryWithTypes(
      "long_val:bigint,"
      "int_val:int,"
      "array_val:array<int>",
      [&]() {},
      true,
      {"long_val", "int_val", "array_val"},
      numCombinations,
      /*withRecursiveNulls=*/true,
      folly::Random::rand64(gen));
}

// Small skips that don't reach the next dictionary run.
TEST_P(E2EFilterTest, deduplicatedArrayCustomSeed_ConnectedSkips) {
  if (common::testutil::useRandomSeed()) {
    return;
  }
  seed_ = 80108952;

  int numCombinations = 20;
#if !defined(NDEBUG) || defined(TSAN_BUILD)
  // The test is running slow under dev/debug and TSAN build; reduce the number
  // of combinations to avoid timeout.
  numCombinations = 2;
#endif
  deduplicatedArrayColumns_ = {"array_val"};

  testWithTypes(
      "long_val:bigint,"
      "int_val:int,"
      "array_val:array<int>",
      [&]() {},
      true,
      {"long_val", "int_val", "array_val"},
      numCombinations,
      /*withRecursiveNulls=*/true);

  testRunLengthDictionaryWithTypes(
      "long_val:bigint,"
      "int_val:int,"
      "array_val:array<int>",
      [&]() {},
      true,
      {"long_val", "int_val", "array_val"},
      numCombinations,
      /*withRecursiveNulls=*/true,
      /*dictionaryGenSeed=*/1729375660);
}

// Small reads that don't reach the next dictionary run and uses the cached
// last run only.
TEST_P(E2EFilterTest, deduplicatedArrayCustomSeed_CachedRunOnly) {
  if (common::testutil::useRandomSeed()) {
    return;
  }
  seed_ = 41071487;

  int numCombinations = 20;
#if !defined(NDEBUG) || defined(TSAN_BUILD)
  // The test is running slow under dev/debug and TSAN build; reduce the number
  // of combinations to avoid timeout.
  numCombinations = 2;
#endif
  deduplicatedArrayColumns_ = {"array_val"};

  testWithTypes(
      "long_val:bigint,"
      "int_val:int,"
      "array_val:array<int>",
      [&]() {},
      true,
      {"long_val", "int_val", "array_val"},
      numCombinations,
      /*withRecursiveNulls=*/true);

  testRunLengthDictionaryWithTypes(
      "long_val:bigint,"
      "int_val:int,"
      "array_val:array<int>",
      [&]() {},
      true,
      {"long_val", "int_val", "array_val"},
      numCombinations,
      /*withRecursiveNulls=*/true,
      /*dictionaryGenSeed=*/1729403307);
}

TEST_P(E2EFilterTest, deduplicatedMapNoRecursiveNulls) {
  int numCombinations = 20;
#if !defined(NDEBUG) || defined(TSAN_BUILD)
  // The test is running slow under dev/debug and TSAN build; reduce the number
  // of combinations to avoid timeout.
  numCombinations = 2;
#endif
  deduplicatedMapColumns_ = {"map_val"};
  testWithTypes(
      "long_val:bigint,"
      "int_val:int,"
      "map_val:map<int, int>",
      [&]() {},
      true,
      {"long_val", "int_val", "map_val"},
      numCombinations,
      /*withRecursiveNulls=*/false);

  std::mt19937 gen{seed_};
  testRunLengthDictionaryWithTypes(
      "long_val:bigint,"
      "int_val:int,"
      "map_val:map<int, int>",
      [&]() {},
      true,
      {"long_val", "int_val", "map_val"},
      numCombinations,
      /*withRecursiveNulls=*/false,
      folly::Random::rand64(gen));
}

TEST_P(E2EFilterTest, deduplicatedMap) {
  int numCombinations = 20;
#if !defined(NDEBUG) || defined(TSAN_BUILD)
  // The test is running slow under dev/debug and TSAN build; reduce the number
  // of combinations to avoid timeout.
  numCombinations = 2;
#endif
  deduplicatedMapColumns_ = {"map_val"};
  testWithTypes(
      "long_val:bigint,"
      "int_val:int,"
      "map_val:map<int, int>",
      [&]() {},
      true,
      {"long_val", "int_val", "map_val"},
      numCombinations,
      /*withRecursiveNulls=*/true);

  std::mt19937 gen{seed_};
  testRunLengthDictionaryWithTypes(
      "long_val:bigint,"
      "int_val:int,"
      "map_val:map<int, int>",
      [&]() {},
      true,
      {"long_val", "int_val", "map_val"},
      numCombinations,
      /*withRecursiveNulls=*/true,
      folly::Random::rand64(gen));
}

// Pruning is only generated in struct_val.map_val in the normal case if filter
// is present on top level map_val, but struct_val.map_val is not deduplicated,
// so pruning is untested in normal case.
TEST_P(E2EFilterTest, deduplicatedMapSubfieldPruning) {
  int numCombinations = 20;
#if !defined(NDEBUG) || defined(TSAN_BUILD)
  // The test is running slow under dev/debug and TSAN build; reduce the number
  // of combinations to avoid timeout.
  numCombinations = 2;
#endif
  deduplicatedMapColumns_ = {"map_val"};
  testWithTypes(
      "long_val:bigint,"
      "map_val:map<int, int>",
      [&]() {},
      false,
      {"long_val"},
      numCombinations,
      /*withRecursiveNulls=*/true);
  std::mt19937 gen{seed_};
  testRunLengthDictionaryWithTypes(
      "long_val:bigint,"
      "map_val:map<int, int>",
      [&]() {},
      false,
      {"long_val"},
      numCombinations,
      /*withRecursiveNulls=*/true,
      folly::Random::rand64(gen));
}

TEST_P(E2EFilterTest, nullCompactRanges) {
  // Makes a dataset with nulls at the beginning. Tries different
  // filter combinations on progressively larger batches. tests for a
  // bug in null compaction where null bits past end of nulls buffer
  // were compacted while there actually were no nulls.
  readSizes_ = {10, 100, 1000, 2500, 2500, 2500};
  testWithTypes(
      "tiny_val:tinyint,"
      "bool_val:boolean,"
      "long_val:bigint,"
      "tiny_null:bigint",
      [&]() { makeNotNull(500); },
      true,
      {"tiny_val", "bool_val", "long_val", "tiny_null"},
      20);
}

TEST_P(E2EFilterTest, lazyStruct) {
  testWithTypes(
      "long_val:bigint,"
      "outer_struct: struct<nested1:bigint, "
      "inner_struct: struct<nested2: bigint>>",
      [&]() {},
      true,
      {"long_val"},
      10);
}

TEST_P(E2EFilterTest, filterStruct) {
#ifdef TSAN_BUILD
  // The test is running slow under TSAN; reduce the number of combinations to
  // avoid timeout.
  constexpr int kNumCombinations = 10;
#else
  constexpr int kNumCombinations = 40;
#endif
  // The data has a struct member with one second level struct
  // column. Both structs have a column that gets filtered 'nestedxxx'
  // and one that does not 'dataxxx'.
  testWithTypes(
      "long_val:bigint,"
      "outer_struct: struct<nested1:bigint, "
      "  data1: string, "
      "  inner_struct: struct<nested2: bigint, data2: smallint>>",
      [&]() {},
      true,
      {"long_val",
       "outer_struct.inner_struct",
       "outer_struct.nested1",
       "outer_struct.inner_struct.nested2"},
      kNumCombinations);
}

// Enable once the writer support struct as flat map.
TEST_P(E2EFilterTest, DISABLED_FlatMapAsStruct) {
  constexpr auto kColumns =
      "long_val:bigint,"
      "long_vals:struct<v1:bigint,v2:bigint,v3:bigint>,"
      "struct_vals:struct<nested1:struct<v1:bigint, v2:float>,nested2:struct<v1:bigint, v2:float>>";
  flatMapColumns_ = {{"long_vals", {}}, {"struct_vals", {}}};
  testWithTypes(kColumns, [] {}, false, {"long_val"}, 10);
}

TEST_P(E2EFilterTest, flatMapScalar) {
  constexpr auto kColumns =
      "long_val:bigint,"
      "long_vals:map<tinyint,bigint>,"
      "string_vals:map<string,string>";
  flatMapColumns_ = {{"long_vals", {}}, {"string_vals", {}}};
  auto customize = [this] {
    dataSetBuilder_->makeUniformMapKeys(common::Subfield("string_vals"));
    dataSetBuilder_->makeMapStringValues(common::Subfield("string_vals"));
  };
  int numCombinations = 5;
#if defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(__address_sanitizer__)
  numCombinations = 1;
#endif
#endif
  testWithTypes(
      kColumns, customize, false, {"long_val", "long_vals"}, numCombinations);
}

TEST_P(E2EFilterTest, flatMapComplexNoRecursiveNulls) {
  constexpr auto kColumns =
      "long_val:bigint,"
      "struct_vals:map<varchar,struct<v1:bigint, v2:float>>,"
      "array_vals:map<tinyint,array<int>>";
  flatMapColumns_ = {{"struct_vals", {}}, {"array_vals", {}}};
  auto customize = [this] {
    dataSetBuilder_->makeUniformMapKeys(common::Subfield("struct_vals"));
  };
  int numCombinations = 5;
#if defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(__address_sanitizer__)
  numCombinations = 1;
#endif
#endif
#if !defined(NDEBUG)
  numCombinations = 1;
#endif
  testWithTypes(
      kColumns,
      customize,
      false,
      {"long_val"},
      numCombinations,
      /*withRecursiveNulls=*/false);
}

TEST_P(E2EFilterTest, flatMapComplex) {
  constexpr auto kColumns =
      "long_val:bigint,"
      "struct_vals:map<varchar,struct<v1:bigint, v2:float>>,"
      "array_vals:map<tinyint,array<int>>";
  flatMapColumns_ = {{"struct_vals", {}}, {"array_vals", {}}};
  auto customize = [this] {
    dataSetBuilder_->makeUniformMapKeys(common::Subfield("struct_vals"));
  };
  int numCombinations = 5;
#if defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(__address_sanitizer__)
  numCombinations = 1;
#endif
#endif
#if !defined(NDEBUG)
  numCombinations = 1;
#endif
  testWithTypes(
      kColumns,
      customize,
      false,
      {"long_val"},
      numCombinations,
      /*withRecursiveNulls=*/true);
}

TEST_P(E2EFilterTest, mutationCornerCases) {
  testMutationCornerCases();
}

TEST_P(E2EFilterTest, timestamp) {
  testWithTypes(
      "timestamp_val:timestamp,"
      "long_val:bigint,"
      "timestamp_null:timestamp",
      [&]() { makeAllNulls("timestamp_null"); },
      true,
      {"timestamp_val", "long_val"},
      20);
}

TEST_P(E2EFilterTest, timestampNullFilter) {
  testWithTypes(
      "timestamp_val:timestamp,"
      "long_val:bigint,"
      "timestamp_null:timestamp",
      [&]() { makeAllNulls("timestamp_null"); },
      true,
      {"timestamp_null"},
      20);
}

TEST_P(E2EFilterTest, timestampNoNulls) {
  testWithTypes(
      "timestamp_val:timestamp,"
      "long_val:bigint",
      [&]() { makeNotNull(); },
      true,
      {"long_val", "timestamp_val"},
      20,
      /*withRecursiveNulls=*/false);
}

TEST_P(E2EFilterTest, timestampMultiple) {
  testWithTypes(
      "ts1:timestamp,"
      "ts2:timestamp,"
      "ts3:timestamp",
      [&]() { makeAllNulls("ts3"); },
      true,
      {"ts1", "ts2"},
      20);
}

TEST_P(E2EFilterTest, timestampArray) {
  testWithTypes(
      "long_val:bigint,"
      "timestamp_array:array<timestamp>",
      [&]() {},
      true,
      {"long_val", "timestamp_array"},
      20);
}

TEST_P(E2EFilterTest, timestampMap) {
  testWithTypes(
      "long_val:bigint,"
      "timestamp_map:map<int, timestamp>",
      [&]() {},
      true,
      {"long_val", "timestamp_map"},
      20);
}

TEST_P(E2EFilterTest, timestampInStruct) {
  testWithTypes(
      "long_val:bigint,"
      "struct_with_ts:struct<ts:timestamp, val:bigint>",
      [&]() {},
      true,
      {"long_val", "struct_with_ts"},
      20);
}

TEST_P(E2EFilterTest, timestampNestedArray) {
  testWithTypes(
      "long_val:bigint,"
      "nested_ts_array:array<array<timestamp>>",
      [&]() {},
      true,
      {"long_val"},
      20);
}

TEST_P(E2EFilterTest, timestampNestedStruct) {
  testWithTypes(
      "long_val:bigint,"
      "outer:struct<inner:struct<ts:timestamp>>",
      [&]() {},
      true,
      {"long_val", "outer"},
      20);
}

TEST_P(E2EFilterTest, timestampNestedMap) {
  testWithTypes(
      "long_val:bigint,"
      "nested_ts_map:map<int, map<int, timestamp>>",
      [&]() {},
      true,
      {"long_val"},
      20);
}

TEST_P(E2EFilterTest, timestampArrayInMap) {
  testWithTypes(
      "long_val:bigint,"
      "map_of_ts_array:map<int, array<timestamp>>",
      [&]() {},
      true,
      {"long_val"},
      20);
}

TEST_P(E2EFilterTest, timestampMapInArray) {
  testWithTypes(
      "long_val:bigint,"
      "array_of_ts_map:array<map<int, timestamp>>",
      [&]() {},
      true,
      {"long_val"},
      20);
}

TEST_P(E2EFilterTest, timestampStructInArray) {
  testWithTypes(
      "long_val:bigint,"
      "array_of_ts_struct:array<struct<ts:timestamp, val:bigint>>",
      [&]() {},
      true,
      {"long_val"},
      20);
}

TEST_P(E2EFilterTest, timestampArrayInStruct) {
  testWithTypes(
      "long_val:bigint,"
      "struct_with_ts_array:struct<ts_array:array<timestamp>, val:bigint>",
      [&]() {},
      true,
      {"long_val"},
      20);
}

INSTANTIATE_TEST_SUITE_P(
    E2EFilterTests,
    E2EFilterTest,
    testing::Combine(
        testing::Bool(),
        testing::Bool(),
        testing::Bool(),
        testing::Bool(),
        testing::Bool(),
        testing::Bool()),
    [](const ::testing::TestParamInfo<E2EFilterTestParams>& info) {
      return E2EFilterTestParam{info.param}.debugString();
    });

} // namespace

// Test that PrefixEncoding's readWithVisitor works correctly when reading
// sorted string data through the selective reader. This test derives from
// E2EFilterTest and overrides writeToMemory to explicitly specify
// PrefixEncoding for the string column.
class PrefixEncodingE2ETest : public E2EFilterTest {
 protected:
  bool indexEnabled() const override {
    return false;
  }

  void setUpRowReaderOptions(
      dwio::common::RowReaderOptions& opts,
      const std::shared_ptr<dwio::common::ScanSpec>& spec) override {
    opts.setScanSpec(spec);
    opts.setTimestampPrecision(TimestampPrecision::kNanoseconds);
    opts.setIndexEnabled(false);
    // PrefixEncoding requires string buffer optimization for correct
    // string_view lifecycle management in readWithVisitor.
    opts.setPassStringBuffersFromDecoder(true);
  }

  void writeToMemory(
      const TypePtr& type,
      const std::vector<RowVectorPtr>& batches,
      bool forRowGroupSkip,
      const std::vector<std::string>& /*indexColumns*/ = {}) override {
    VELOX_CHECK(!forRowGroupSkip);
    // Clear cached IO data from previous writes to avoid stale cache entries.
    if (cache_ != nullptr) {
      cache_->clear();
    }
    rowType_ = asRowType(type);

    // Create encoding layout tree with prefix encoding for string columns
    std::vector<EncodingLayoutTree> children;
    for (column_index_t i = 0;
         i < static_cast<column_index_t>(rowType_->size());
         ++i) {
      const auto& childType = rowType_->childAt(i);
      const auto& childName = rowType_->nameOf(i);

      if (childType->isVarchar() || childType->isVarbinary()) {
        // Use prefix encoding for string columns
        children.push_back(
            EncodingLayoutTree{
                Kind::Scalar,
                {{EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream,
                  EncodingLayout{
                      EncodingType::Prefix,
                      {},
                      CompressionType::Uncompressed}}},
                std::string(childName)});
      } else {
        // For all other types, let the writer decide
        children.push_back(
            EncodingLayoutTree{Kind::Scalar, {}, std::string(childName)});
      }
    }

    EncodingLayoutTree encodingLayoutTree{
        Kind::Row, {}, "", std::move(children)};

    auto writeFile = std::make_unique<InMemoryWriteFile>(&sinkData_);
    VeloxWriter writer(
        rowType_,
        std::move(writeFile),
        *rootPool_,
        {.encodingLayoutTree = std::move(encodingLayoutTree)});
    for (auto& batch : batches) {
      writer.write(batch);
    }
    writer.close();
  }
};

INSTANTIATE_TEST_SUITE_P(
    PrefixEncodingE2ETests,
    PrefixEncodingE2ETest,
    testing::Combine(
        testing::Bool(),
        testing::Bool(),
        testing::Bool(),
        testing::Bool(),
        testing::Bool(),
        testing::Bool()),
    [](const ::testing::TestParamInfo<E2EFilterTestParams>& info) {
      return E2EFilterTestParam{info.param}.debugString();
    });

TEST_P(PrefixEncodingE2ETest, withoutFilterOnString) {
  for (bool withRecursiveNulls : {false, true}) {
    SCOPED_TRACE(fmt::format("withRecursiveNulls={}", withRecursiveNulls));
    testWithTypes(
        "string_val:string,"
        "long_val:bigint",
        [&]() {
          // Make string values unique and sorted to benefit from prefix
          // encoding
          makeStringUnique("string_val");
        },
        /*wrapInStruct=*/false,
        /*filterable=*/{"long_val"},
        /*numCombinations=*/5,
        withRecursiveNulls);
  }
}

TEST_P(PrefixEncodingE2ETest, filterOnString) {
  // Test with filter on both string and integer columns.
  // This tests the skip functionality in readWithVisitor.
  for (bool withRecursiveNulls : {false, true}) {
    SCOPED_TRACE(fmt::format("withRecursiveNulls={}", withRecursiveNulls));
    testWithTypes(
        "string_val:string,"
        "string_val_2:string,"
        "long_val:bigint",
        [&]() {
          makeStringUnique("string_val");
          makeStringUnique("string_val_2");
        },
        /*wrapInStruct=*/false,
        {"string_val", "long_val"},
        /*numCombinations=*/5,
        withRecursiveNulls);
  }
}

} // namespace facebook::nimble
