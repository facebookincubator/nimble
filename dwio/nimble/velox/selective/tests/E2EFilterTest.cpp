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

#include "dwio/nimble/encodings/common/EncodingLayout.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/tablet/tests/TabletTestUtils.h"
#include "dwio/nimble/velox/ChunkedStream.h"
#include "dwio/nimble/velox/EncodingLayoutTree.h"
#include "dwio/nimble/velox/SchemaSerialization.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "dwio/nimble/velox/selective/SelectiveNimbleReader.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/ScanTracker.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/memory/MallocAllocator.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/tests/utils/E2EFilterTestBase.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::nimble {
namespace {

using namespace facebook::velox;

using E2EFilterTestParams =
    std::tuple<bool, bool, bool, bool, bool, bool, bool>;

struct E2EFilterTestParam {
  bool indexEnabled;
  bool skipConstantFlatMapInMapStreams;
  bool enableChunkIndex;
  bool stringDecoderZeroCopy;
  bool pinMetadata;
  bool enableCache;
  bool nimblePreserveDictionaryEncoding;

  explicit E2EFilterTestParam(const E2EFilterTestParams& t)
      : indexEnabled{std::get<0>(t)},
        skipConstantFlatMapInMapStreams{std::get<1>(t)},
        enableChunkIndex{std::get<2>(t)},
        stringDecoderZeroCopy{std::get<3>(t)},
        pinMetadata{std::get<4>(t)},
        enableCache{std::get<5>(t)},
        nimblePreserveDictionaryEncoding{std::get<6>(t)} {}

  std::string debugString() const {
    return fmt::format(
        "index_{}_skipConstantInMap_{}_chunkIndex_{}_stringDecoderZeroCopy_{}_pinMetadata_{}_cache_{}_nimblePreserveDict_{}",
        indexEnabled,
        skipConstantFlatMapInMapStreams,
        enableChunkIndex,
        stringDecoderZeroCopy,
        pinMetadata,
        enableCache,
        nimblePreserveDictionaryEncoding);
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
    dataIoStats_ = std::make_shared<io::IoStatistics>();
    metadataIoStats_ = std::make_shared<io::IoStatistics>();
    indexIoStats_ = std::make_shared<io::IoStatistics>();
    if (param().enableCache) {
      allocator_ = std::make_shared<memory::MallocAllocator>(
          memory::MemoryAllocator::Options{
              .capacity = 512 << 20, .reservationByteLimit = 0});
      cache_ = cache::AsyncDataCache::create(allocator_.get());
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
    opts.setStringDecoderZeroCopy(param().stringDecoderZeroCopy);
    opts.setNimblePreserveDictionaryEncoding(
        param().nimblePreserveDictionaryEncoding);
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
    // Ensure the chunk flush policy is honored for small test data by removing
    // the minimum chunk size threshold. Without this, small writes get merged
    // into a single chunk regardless of the flush policy.
    options.minStreamChunkRawSize = 0;

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
      ClusterIndexConfig clusterIndexConfig;
      clusterIndexConfig.columns = indexColumns;
      clusterIndexConfig.enforceKeyOrder = true;
      options.clusterIndexConfig = std::move(clusterIndexConfig);
    }
    auto writeFile = std::make_unique<InMemoryWriteFile>(&sinkData_);
    // Branch on whether we need forced dictionary encoding, since
    // EncodingLayoutTree is not move-assignable due to const members.
    if (useForcedDictionaryEncoding_) {
      options.encodingLayoutTree.emplace(
          buildForcedDictionaryEncodingLayoutTree());
    }
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
    readerOpts.setLoadClusterIndex(true);
    readerOpts.setPinMetadata(param().pinMetadata);
    readerOpts.setCacheMetadata(param().enableCache);
    readerOpts.setIndexIoStats(indexIoStats_);
    if (param().enableCache) {
      auto readFile = std::make_shared<InMemoryReadFile>(sinkData_);
      auto& ids = fileIds();
      StringIdLease fileId(
          ids, fmt::format("testFile_{}", reinterpret_cast<uintptr_t>(this)));
      StringIdLease groupId(ids, "testGroup");
      io::ReaderOptions cacheReaderOpts(&readerOpts.memoryPool());
      cacheReaderOpts.setDataIoStats(dataIoStats_);
      cacheReaderOpts.setMetadataIoStats(metadataIoStats_);
      input = std::make_unique<dwio::common::CachedBufferedInput>(
          std::move(readFile),
          dwio::common::MetricsLog::voidLog(),
          std::move(fileId),
          cache_.get(),
          scanTracker_,
          std::move(groupId),
          dataIoStats_,
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
  // When true, forces dictionary encoding for string columns via
  // EncodingLayoutTree. This is useful for testing nested encoding scenarios
  // where dictionary is not at the top level (e.g., Nullable -> Dictionary).
  bool useForcedDictionaryEncoding_{false};

  // Builds an encoding layout tree that forces dictionary encoding for string
  // columns. When data has nulls, the writer will wrap this with nullable
  // encoding, creating a Nullable -> Dictionary nesting.
  EncodingLayoutTree buildForcedDictionaryEncodingLayoutTree() {
    std::vector<EncodingLayoutTree> children;
    for (column_index_t i = 0;
         i < static_cast<column_index_t>(rowType_->size());
         ++i) {
      const auto& childType = rowType_->childAt(i);
      const auto& childName = rowType_->nameOf(i);

      if (childType->isVarchar() || childType->isVarbinary()) {
        // Dictionary has two sub-encodings: Alphabet (id=0) and Indices (id=1).
        // Specify nullopt for both to let the writer decide their encodings.
        // Without these children, the layout replay fails with
        // "Sub-encoding identifier out of range" when the writer creates
        // Dictionary's sub-encoding selection policies.
        children.push_back(
            EncodingLayoutTree{
                Kind::Scalar,
                {{EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream,
                  EncodingLayout{
                      EncodingType::Dictionary,
                      {},
                      // Required by EncodingLayout constructor; does not
                      // override the encoding selection policy's choice.
                      CompressionType::Uncompressed,
                      {
                          // Alphabet (id=0): let writer decide
                          std::nullopt,
                          // Indices (id=1): let writer decide
                          std::nullopt,
                      }}}},
                std::string(childName)});
      } else {
        children.push_back(
            EncodingLayoutTree{Kind::Scalar, {}, std::string(childName)});
      }
    }
    return EncodingLayoutTree{Kind::Row, {}, "", std::move(children)};
  }

  EncodingLayoutTree buildForcedMainlyConstantDictionaryEncodingLayoutTree() {
    std::vector<EncodingLayoutTree> children;
    for (column_index_t i = 0;
         i < static_cast<column_index_t>(rowType_->size());
         ++i) {
      const auto& childType = rowType_->childAt(i);
      const auto& childName = rowType_->nameOf(i);

      if (childType->isVarchar() || childType->isVarbinary()) {
        // CompressionType::Uncompressed is required by EncodingLayout
        // constructor; does not override the encoding selection policy.
        EncodingLayout dictLayout{
            EncodingType::Dictionary,
            {},
            CompressionType::Uncompressed,
            {std::nullopt, std::nullopt}};
        children.push_back(
            EncodingLayoutTree{
                Kind::Scalar,
                {{EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream,
                  EncodingLayout{
                      EncodingType::MainlyConstant,
                      {},
                      CompressionType::Uncompressed,
                      {std::nullopt, std::move(dictLayout)}}}},
                std::string(childName)});
      } else {
        children.push_back(
            EncodingLayoutTree{Kind::Scalar, {}, std::string(childName)});
      }
    }
    return EncodingLayoutTree{Kind::Row, {}, "", std::move(children)};
  }

  /// Builds a layout tree that forces string columns into RLE→Dictionary
  /// encoding to exercise the RLE dictionary vector path in E2E tests.
  EncodingLayoutTree buildForcedRleDictionaryEncodingLayoutTree() {
    std::vector<EncodingLayoutTree> children;
    for (column_index_t i = 0;
         i < static_cast<column_index_t>(rowType_->size());
         ++i) {
      const auto& childType = rowType_->childAt(i);
      const auto& childName = rowType_->nameOf(i);

      if (childType->isVarchar() || childType->isVarbinary()) {
        EncodingLayout dictLayout{
            EncodingType::Dictionary,
            {},
            CompressionType::Uncompressed,
            {std::nullopt, std::nullopt}};
        children.push_back(
            EncodingLayoutTree{
                Kind::Scalar,
                {{EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream,
                  EncodingLayout{
                      EncodingType::RLE,
                      {},
                      CompressionType::Uncompressed,
                      {std::nullopt, std::move(dictLayout)}}}},
                std::string(childName)});
      } else {
        children.push_back(
            EncodingLayoutTree{Kind::Scalar, {}, std::string(childName)});
      }
    }
    return EncodingLayoutTree{Kind::Row, {}, "", std::move(children)};
  }

  void verifyDictionaryVectorReturned(
      EncodingLayoutTree layoutTree,
      const std::vector<std::string>& stringData) {
    auto type = asRowType(rowType_);
    const auto kRowCount = static_cast<vector_size_t>(stringData.size());

    auto stringVector =
        velox::test::VectorMaker(leafPool_.get())
            .flatVector<velox::StringView>(kRowCount, [&](auto row) {
              return velox::StringView(stringData[row]);
            });
    auto longVector = velox::test::VectorMaker(leafPool_.get())
                          .flatVector<int64_t>(kRowCount, [](auto row) {
                            return static_cast<int64_t>(row);
                          });

    auto batch = std::make_shared<RowVector>(
        leafPool_.get(),
        type,
        nullptr,
        kRowCount,
        std::vector<VectorPtr>{stringVector, longVector});

    {
      auto writeFile = std::make_unique<InMemoryWriteFile>(&sinkData_);
      VeloxWriterOptions writerOptions;
      writerOptions.encodingLayoutTree.emplace(std::move(layoutTree));
      VeloxWriter writer(
          type, std::move(writeFile), *rootPool_, std::move(writerOptions));
      writer.write(batch);
      writer.close();
    }

    auto input = std::make_unique<velox::dwio::common::BufferedInput>(
        std::make_shared<velox::InMemoryReadFile>(sinkData_),
        *leafPool_,
        velox::dwio::common::MetricsLog::voidLog());

    auto dataIoStats = std::make_shared<velox::io::IoStatistics>();
    auto metadataIoStats = std::make_shared<velox::io::IoStatistics>();
    velox::dwio::common::ReaderOptions readerOpts(leafPool_.get());
    readerOpts.setDataIoStats(dataIoStats);
    readerOpts.setMetadataIoStats(metadataIoStats);
    auto reader = makeReader(readerOpts, std::move(input));
    auto scanSpec = std::make_shared<velox::common::ScanSpec>("root");
    scanSpec->addAllChildFields(*type);
    velox::dwio::common::RowReaderOptions rowReaderOptions;
    rowReaderOptions.setScanSpec(scanSpec);
    rowReaderOptions.setStringDecoderZeroCopy(param().stringDecoderZeroCopy);
    rowReaderOptions.setNimblePreserveDictionaryEncoding(
        param().stringDecoderZeroCopy);
    auto rowReader = reader->createRowReader(rowReaderOptions);

    auto result = BaseVector::create(type, 0, leafPool_.get());
    size_t totalRows = 0;
    size_t globalRow = 0;
    while (rowReader->next(1000, result)) {
      auto* rowResult = result->as<RowVector>();
      ASSERT_NE(rowResult, nullptr);
      totalRows += rowResult->size();

      auto stringChild = rowResult->childAt(0)->loadedVector();
      if (param().stringDecoderZeroCopy) {
        ASSERT_EQ(stringChild->encoding(), VectorEncoding::Simple::DICTIONARY)
            << "Expected DICTIONARY encoding for string column";
      }

      DecodedVector decoded(*stringChild);
      for (size_t i = 0; i < rowResult->size(); ++i) {
        ASSERT_EQ(decoded.valueAt<StringView>(i).str(), stringData[globalRow])
            << "Mismatch at row=" << globalRow;
        ++globalRow;
      }
    }

    EXPECT_EQ(totalRows, kRowCount);
  }

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
  std::shared_ptr<io::IoStatistics> dataIoStats_;
  std::shared_ptr<io::IoStatistics> metadataIoStats_;
  std::shared_ptr<io::IoStatistics> indexIoStats_;
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
      case EncodingType::SubIntSplit:
        // SubIntSplit may fall back to Constant for fully-constant data.
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
    auto tablet = TabletReader::create(
        readFile, &pool, test::makeTestTabletOptions(&pool));
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

// Biased variant that forces SubIntSplit encoding selection.
// SubIntSplit only applies to 32- and 64-bit types; smallint is excluded.
TEST_P(E2EFilterTest, integerBiasedSubIntSplit) {
  testWithTypes(
      "int_val:int,"
      "long_val:bigint",
      [&]() {
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
      /*wrapInStruct=*/true,
      {"int_val", "long_val"},
      /*numCombinations=*/20,
      /*withRecursiveNulls=*/true,
      biasedEncodingFactors(EncodingType::SubIntSplit));
}

// Tests data with bit-structured distributions where SubIntSplit naturally
// wins: values concentrated in the low-bit range of a wider type, so the
// upper bit sections collapse to Constant encoding.
TEST_P(E2EFilterTest, integerSubIntSplitBitStructured) {
  testWithTypes(
      "int_val:int,"
      "long_val:bigint",
      [&]() {
        makeIntDistribution<int32_t>(
            "int_val",
            /*min=*/0,
            /*max=*/65535, // only 16 low bits vary; upper 16 are zero
            /*repeats=*/1,
            /*rareFrequency=*/0,
            /*rareMin=*/0,
            /*rareMax=*/0,
            /*keepNulls=*/false);
        makeIntDistribution<int64_t>(
            "long_val",
            /*min=*/0,
            /*max=*/4294967295, // only 32 low bits vary; upper 32 are zero
            /*repeats=*/1,
            /*rareFrequency=*/0,
            /*rareMin=*/0,
            /*rareMax=*/0,
            /*keepNulls=*/false);
      },
      /*wrapInStruct=*/false,
      {"int_val", "long_val"},
      /*numCombinations=*/20,
      /*withRecursiveNulls=*/true);
}

// Biased variant that forces SubIntSplit encoding for float/double columns.
// SubIntSplit encodes float/double via their uint32/uint64 IEEE 754 bit
// patterns; quantized data produces sections of differing entropy.
TEST_P(E2EFilterTest, floatBiasedSubIntSplit) {
  testWithTypes(
      "float_val:float,"
      "double_val:double",
      [&]() {
        makeQuantizedFloat<float>("float_val", 100, true);
        makeQuantizedFloat<double>("double_val", 100, true);
      },
      /*wrapInStruct=*/false,
      {"float_val", "double_val"},
      /*numCombinations=*/20,
      /*withRecursiveNulls=*/true,
      biasedEncodingFactors(EncodingType::SubIntSplit));
}

#ifdef NIMBLE_ENABLE_EXPERIMENTAL_ENCODINGS
// Forces SubIntSplit via EncodingLayoutTree and verifies the on-disk encoding
// matches. Schema uses only 32/64-bit types (SubIntSplit requires
// sizeof(T)>=4).
TEST_P(E2EFilterTest, integerForcedSubIntSplit) {
  forcedEncodingType_ = EncodingType::SubIntSplit;
  testWithTypes(
      "int_val:int,"
      "long_val:bigint",
      [&]() {
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
      {"int_val", "long_val"},
      /*numCombinations=*/20,
      /*withRecursiveNulls=*/true);
  forcedEncodingType_.reset();
  verifyColumnEncodingsOnDisk(EncodingType::SubIntSplit);
}
#endif

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
    opts.setStringDecoderZeroCopy(true);
    opts.setNimblePreserveDictionaryEncoding(
        param().nimblePreserveDictionaryEncoding);
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

// Test for cross-stripe reading of string data with filters.
// This tests that dictionary column visitors work correctly when reading
// across multiple stripes, where the dictionary may change between stripes.
TEST_P(E2EFilterTest, stringDictionaryAcrossStripes) {
  // Use frequent stripe flushes to maximize cross-stripe reads.
  flushEveryNBatches_ = 1;
  for (bool withRecursiveNulls : {false, true}) {
    SCOPED_TRACE(fmt::format("withRecursiveNulls={}", withRecursiveNulls));
    testWithTypes(
        "string_val:string,"
        "string_val_2:string,"
        "long_val:bigint",
        [&]() {
          makeStringDistribution("string_val", 100, true, false);
          makeStringDistribution("string_val_2", 170, false, true);
        },
        /*wrapInStruct=*/false,
        {"string_val", "long_val"},
        /*numCombinations=*/5,
        withRecursiveNulls);
  }
}

TEST_P(E2EFilterTest, stringUniqueAcrossStripes) {
  // Use frequent stripe flushes to maximize cross-stripe reads.
  flushEveryNBatches_ = 1;
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

// Test for nested encodings where dictionary encoding is NOT at the top level.
// This tests scenarios like Nullable -> Dictionary, which may have different
// behavior than Dictionary at the top level.
TEST_P(E2EFilterTest, stringDictionaryNestedUnderNullable) {
  // Force dictionary encoding for string columns via encoding layout.
  useForcedDictionaryEncoding_ = true;
  for (bool withRecursiveNulls : {false, true}) {
    SCOPED_TRACE(fmt::format("withRecursiveNulls={}", withRecursiveNulls));
    testWithTypes(
        "string_val:string,"
        "string_val_2:string,"
        "long_val:bigint",
        [&]() {
          makeStringDistribution("string_val", 100, true, false);
          makeStringDistribution("string_val_2", 170, false, true);
        },
        /*wrapInStruct=*/false,
        {"string_val", "long_val"},
        /*numCombinations=*/5,
        withRecursiveNulls);
  }
}

TEST_P(E2EFilterTest, filterOnNestedDictionaryString) {
  // Force dictionary encoding for string columns via encoding layout.
  useForcedDictionaryEncoding_ = true;
  for (bool withRecursiveNulls : {false, true}) {
    SCOPED_TRACE(fmt::format("withRecursiveNulls={}", withRecursiveNulls));
    testWithTypes(
        "string_val:string,"
        "string_val_2:string,"
        "long_val:bigint",
        [&]() {
          makeStringDistribution("string_val", 50, true, true);
          makeStringDistribution("string_val_2", 80, true, false);
        },
        /*wrapInStruct=*/false,
        {"string_val", "string_val_2", "long_val"},
        /*numCombinations=*/5,
        withRecursiveNulls);
  }
}

// Test combining cross-stripe reading with nested dictionary encodings.
// This is the most challenging scenario for dictionary column visitors.
TEST_P(E2EFilterTest, stringDictionaryCrossStripeWithNestedEncoding) {
  // Use both frequent stripe flushes and forced dictionary encoding.
  flushEveryNBatches_ = 1;
  useForcedDictionaryEncoding_ = true;
  for (bool withRecursiveNulls : {false, true}) {
    SCOPED_TRACE(fmt::format("withRecursiveNulls={}", withRecursiveNulls));
    testWithTypes(
        "string_val:string,"
        "string_val_2:string,"
        "long_val:bigint",
        [&]() {
          makeStringDistribution("string_val", 100, true, false);
          makeStringDistribution("string_val_2", 170, false, true);
        },
        /*wrapInStruct=*/false,
        {"string_val", "long_val"},
        /*numCombinations=*/5,
        withRecursiveNulls);
  }
}

TEST_P(E2EFilterTest, stringFilterOnBothColumnsCrossStripeNested) {
  // Use both frequent stripe flushes and forced dictionary encoding.
  flushEveryNBatches_ = 1;
  useForcedDictionaryEncoding_ = true;
  for (bool withRecursiveNulls : {false, true}) {
    SCOPED_TRACE(fmt::format("withRecursiveNulls={}", withRecursiveNulls));
    testWithTypes(
        "string_val:string,"
        "string_val_2:string,"
        "long_val:bigint",
        [&]() {
          makeStringDistribution("string_val", 50, true, true);
          makeStringDistribution("string_val_2", 80, true, false);
        },
        /*wrapInStruct=*/false,
        {"string_val", "string_val_2", "long_val"},
        /*numCombinations=*/5,
        withRecursiveNulls);
  }
}

// Test for nested encoding where dictionary is not at the top level.
// This test creates a MainlyConstant → Dictionary nesting by:
// 1. Creating data where most values are a constant (triggers MainlyConstant)
// 2. Forcing the otherValues_ child to use Dictionary encoding via
// EncodingLayoutTree
//
// This scenario is challenging because isDictionaryCompatible() must handle
// deeper nesting like MainlyConstant→Dictionary, not just direct Dictionary
// or Nullable→Dictionary.
TEST_P(E2EFilterTest, nestedMainlyConstantWithDictionary) {
  // Create data where most values are a constant string.
  // ~90% of values will be the constant, ~10% will be "other" values.
  auto type = ROW({"string_val", "long_val"}, {VARCHAR(), BIGINT()});

  const size_t kRowCount = 1000;
  std::vector<std::string> stringVals;
  std::vector<int64_t> longVals;
  stringVals.reserve(kRowCount);
  longVals.reserve(kRowCount);

  const std::string kConstantValue = "MOSTLY_CONSTANT_VALUE";
  // For the "other" values, use low cardinality to encourage dictionary
  // encoding for the otherValues_ child of MainlyConstant.
  const std::vector<std::string> otherValues = {
      "OTHER_A", "OTHER_B", "OTHER_C", "OTHER_D", "OTHER_E"};

  for (size_t i = 0; i < kRowCount; ++i) {
    if (i % 10 == 0) {
      // ~10% of rows have "other" values (low cardinality for dictionary).
      stringVals.push_back(otherValues[i % otherValues.size()]);
    } else {
      // ~90% of rows have the constant value.
      stringVals.push_back(kConstantValue);
    }
    longVals.push_back(i);
  }

  auto stringVector =
      velox::test::VectorMaker(leafPool_.get())
          .flatVector<velox::StringView>(kRowCount, [&](auto row) {
            return velox::StringView(stringVals[row]);
          });
  auto longVector = velox::test::VectorMaker(leafPool_.get())
                        .flatVector<int64_t>(
                            kRowCount, [&](auto row) { return longVals[row]; });

  auto batch = std::make_shared<RowVector>(
      leafPool_.get(),
      type,
      nullptr,
      kRowCount,
      std::vector<VectorPtr>{stringVector, longVector});

  // Build EncodingLayoutTree that forces MainlyConstant with Dictionary child
  // for otherValues_.
  // The encoding hierarchy we want:
  //   Row
  //     -> Scalar (string_val): MainlyConstant
  //         -> OtherValues (child 1): Dictionary
  //             -> Alphabet (child 0): let writer decide
  //             -> Indices (child 1): let writer decide
  //     -> Scalar (long_val): default
  EncodingLayoutTree encodingLayoutTree{
      Kind::Row,
      {},
      "",
      {
          EncodingLayoutTree{
              Kind::Scalar,
              {{EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream,
                EncodingLayout{
                    EncodingType::MainlyConstant,
                    {},
                    CompressionType::Uncompressed,
                    {
                        // Child 0: IsCommon (let the writer decide)
                        std::nullopt,
                        // Child 1: OtherValues -> Dictionary
                        // Dictionary encoding needs children for alphabet and
                        // indices.
                        EncodingLayout{
                            EncodingType::Dictionary,
                            {},
                            CompressionType::Uncompressed,
                            {
                                // Alphabet (id=0): let writer decide
                                std::nullopt,
                                // Indices (id=1): let writer decide
                                std::nullopt,
                            }},
                    }}}},
              "string_val"},
          EncodingLayoutTree{Kind::Scalar, {}, "long_val"},
      }};

  // Write the data with the forced encoding layout.
  auto writeFile = std::make_unique<InMemoryWriteFile>(&sinkData_);
  VeloxWriterOptions writerOptions;
  writerOptions.encodingLayoutTree.emplace(std::move(encodingLayoutTree));
  VeloxWriter writer(type, std::move(writeFile), *rootPool_, writerOptions);
  writer.write(batch);
  writer.close();

  // Read back and verify the data is correct.
  auto input = std::make_unique<velox::dwio::common::BufferedInput>(
      std::make_shared<velox::InMemoryReadFile>(sinkData_),
      *leafPool_,
      velox::dwio::common::MetricsLog::voidLog());

  velox::dwio::common::ReaderOptions readerOpts(leafPool_.get());
  readerOpts.setDataIoStats(dataIoStats_);
  readerOpts.setMetadataIoStats(metadataIoStats_);
  auto reader = makeReader(readerOpts, std::move(input));
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("root");
  scanSpec->addAllChildFields(*type);
  velox::dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  // Read all rows and verify.
  auto result = BaseVector::create(type, 0, leafPool_.get());
  size_t totalRows = 0;
  while (rowReader->next(1000, result)) {
    auto* rowResult = result->as<RowVector>();
    ASSERT_NE(rowResult, nullptr);
    totalRows += rowResult->size();

    // Use DecodedVector to handle both flat and dictionary-encoded vectors.
    // The string column might be wrapped in a DictionaryVector when using
    // dictionary encoding path.
    velox::DecodedVector decodedString(*rowResult->childAt(0));
    velox::DecodedVector decodedLong(*rowResult->childAt(1));

    for (size_t i = 0; i < rowResult->size(); ++i) {
      auto rowIdx = decodedLong.valueAt<int64_t>(i);
      std::string expected;
      if (rowIdx % 10 == 0) {
        expected = otherValues[rowIdx % otherValues.size()];
      } else {
        expected = kConstantValue;
      }
      ASSERT_EQ(decodedString.valueAt<velox::StringView>(i).str(), expected)
          << "Mismatch at row=" << rowIdx;
    }
  }

  EXPECT_EQ(totalRows, kRowCount);
}

// Writes string data with a forced encoding layout, reads back, and verifies
// dictionary vector return and data correctness.
TEST_P(E2EFilterTest, dictionaryVectorReturned) {
  const vector_size_t kRowCount = 1000;
  std::mt19937 gen(42);
  const int numDistinct = 5 + gen() % 20;
  std::vector<std::string> alphabet(numDistinct);
  for (int i = 0; i < numDistinct; ++i) {
    auto len = 3 + gen() % 30;
    alphabet[i].resize(len);
    for (auto& c : alphabet[i]) {
      c = 'a' + gen() % 26;
    }
  }
  std::vector<std::string> stringData(kRowCount);
  for (vector_size_t i = 0; i < kRowCount; ++i) {
    stringData[i] = alphabet[gen() % numDistinct];
  }

  rowType_ = ROW({"string_val", "long_val"}, {VARCHAR(), BIGINT()});
  verifyDictionaryVectorReturned(
      buildForcedDictionaryEncodingLayoutTree(), stringData);
}

TEST_P(E2EFilterTest, mainlyConstantDictionaryVectorReturned) {
  const vector_size_t kRowCount = 1000;
  std::mt19937 gen(123);
  const int numOther = 3 + gen() % 10;
  std::vector<std::string> otherAlphabet(numOther);
  for (int i = 0; i < numOther; ++i) {
    auto len = 3 + gen() % 20;
    otherAlphabet[i].resize(len);
    for (auto& c : otherAlphabet[i]) {
      c = 'a' + gen() % 26;
    }
  }
  auto commonLen = 20 + gen() % 40;
  std::string commonValue(commonLen, 'x' + gen() % 3);

  std::vector<std::string> stringData(kRowCount);
  for (vector_size_t i = 0; i < kRowCount; ++i) {
    stringData[i] =
        (gen() % 10 == 0) ? otherAlphabet[gen() % numOther] : commonValue;
  }

  rowType_ = ROW({"string_val", "long_val"}, {VARCHAR(), BIGINT()});
  verifyDictionaryVectorReturned(
      buildForcedMainlyConstantDictionaryEncodingLayoutTree(), stringData);
}

// Fuzz test: exercises MC→Dict readIndicesWithVisitor with randomized data
// shapes. Runs 10000 iterations with varying row counts, alphabet sizes,
// common percentages, and null rates to catch subtle scattering/composition
// bugs in materializeIndices and readIndicesWithVisitor.
TEST_P(E2EFilterTest, fuzzMainlyConstantDictionaryVector) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "fuzzMainlyConstantDictionaryVector seed: " << seed;
  std::mt19937 rng(seed);

  const int kRowCount = 50 + rng() % 500;
  const int numOther = 1 + rng() % 8;
  const int commonPct = 50 + rng() % 45;
  const bool withNulls = rng() % 3 == 0;

  SCOPED_TRACE(
      fmt::format(
          "seed={} rows={} numOther={} commonPct={} withNulls={}",
          seed,
          kRowCount,
          numOther,
          commonPct,
          withNulls));

  std::vector<std::string> otherAlphabet(numOther);
  for (int j = 0; j < numOther; ++j) {
    auto len = 1 + rng() % 30;
    otherAlphabet[j].resize(len);
    for (auto& c : otherAlphabet[j]) {
      c = 'a' + rng() % 26;
    }
  }
  // Use a prefix that won't appear in otherAlphabet (which uses 'a'-'z').
  auto commonLen = 5 + rng() % 50;
  std::string commonValue(commonLen, '0' + rng() % 3);

  std::vector<std::string> stringData(kRowCount);
  for (int i = 0; i < kRowCount; ++i) {
    stringData[i] = (static_cast<int>(rng() % 100) < commonPct)
        ? commonValue
        : otherAlphabet[rng() % numOther];
  }
  // Ensure at least one non-common value at a non-null position so the Dict
  // child isn't empty. With nullEvery(7), position i is null when i % 7 == 0.
  {
    bool hasNonCommonAtNonNull = false;
    for (int i = 0; i < kRowCount; ++i) {
      if ((!withNulls || i % 7 != 0) && stringData[i] != commonValue) {
        hasNonCommonAtNonNull = true;
        break;
      }
    }
    if (!hasNonCommonAtNonNull) {
      // Place a non-common value at a position that won't be null.
      int pos = 1;
      while (withNulls && pos % 7 == 0) {
        ++pos;
      }
      stringData[pos] = otherAlphabet[0];
    }
  }

  rowType_ = ROW({"string_val", "long_val"}, {VARCHAR(), BIGINT()});

  auto type = asRowType(rowType_);
  auto stringVector =
      velox::test::VectorMaker(leafPool_.get())
          .flatVector<velox::StringView>(
              kRowCount,
              [&](auto row) { return velox::StringView(stringData[row]); },
              withNulls ? velox::test::VectorMaker::nullEvery(7) : nullptr);
  auto longVector =
      velox::test::VectorMaker(leafPool_.get())
          .flatVector<int64_t>(kRowCount, [](auto row) { return row; });

  auto batch = std::make_shared<RowVector>(
      leafPool_.get(),
      type,
      nullptr,
      kRowCount,
      std::vector<VectorPtr>{stringVector, longVector});

  sinkData_.clear();
  {
    auto writeFile = std::make_unique<InMemoryWriteFile>(&sinkData_);
    VeloxWriterOptions writerOptions;
    writerOptions.encodingLayoutTree.emplace(
        buildForcedMainlyConstantDictionaryEncodingLayoutTree());
    VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, std::move(writerOptions));
    writer.write(batch);
    writer.close();
  }

  auto input = std::make_unique<velox::dwio::common::BufferedInput>(
      std::make_shared<velox::InMemoryReadFile>(sinkData_),
      *leafPool_,
      velox::dwio::common::MetricsLog::voidLog());

  velox::dwio::common::ReaderOptions readerOpts(leafPool_.get());
  readerOpts.setDataIoStats(dataIoStats_);
  readerOpts.setMetadataIoStats(metadataIoStats_);
  auto reader = makeReader(readerOpts, std::move(input));
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("root");
  scanSpec->addAllChildFields(*type);
  velox::dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.setScanSpec(scanSpec);
  rowReaderOptions.setStringDecoderZeroCopy(param().stringDecoderZeroCopy);
  rowReaderOptions.setNimblePreserveDictionaryEncoding(
      param().stringDecoderZeroCopy);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  auto result = BaseVector::create(type, 0, leafPool_.get());
  size_t totalRows = 0;
  size_t globalRow = 0;
  while (rowReader->next(kRowCount, result)) {
    auto* rowResult = result->as<RowVector>();
    ASSERT_NE(rowResult, nullptr);
    totalRows += rowResult->size();

    auto stringChild = rowResult->childAt(0)->loadedVector();
    DecodedVector decoded(*stringChild);
    for (size_t i = 0; i < rowResult->size(); ++i) {
      if (withNulls && globalRow % 7 == 0) {
        ASSERT_TRUE(decoded.isNullAt(i))
            << "Expected null at row=" << globalRow;
      } else {
        ASSERT_FALSE(decoded.isNullAt(i))
            << "Unexpected null at row=" << globalRow;
        ASSERT_EQ(decoded.valueAt<StringView>(i).str(), stringData[globalRow])
            << "Mismatch at row=" << globalRow;
      }
      ++globalRow;
    }
  }
  EXPECT_EQ(totalRows, kRowCount);
}

TEST_P(E2EFilterTest, fuzzRleDictionaryVector) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "fuzzRleDictionaryVector seed: " << seed;
  std::mt19937 rng(seed);

  const int kRowCount = 50 + rng() % 500;
  const int numDistinct = 2 + rng() % 8;
  const int maxRunLength = 2 + rng() % 20;
  const bool withNulls = rng() % 3 == 0;

  SCOPED_TRACE(
      fmt::format(
          "seed={} rows={} numDistinct={} maxRunLength={} withNulls={}",
          seed,
          kRowCount,
          numDistinct,
          maxRunLength,
          withNulls));

  std::vector<std::string> alphabet(numDistinct);
  for (int j = 0; j < numDistinct; ++j) {
    auto len = 1 + rng() % 30;
    alphabet[j].resize(len);
    for (auto& c : alphabet[j]) {
      c = 'a' + rng() % 26;
    }
  }

  std::vector<std::string> stringData;
  stringData.reserve(kRowCount);
  while (static_cast<int>(stringData.size()) < kRowCount) {
    auto value = alphabet[rng() % numDistinct];
    auto runLen =
        std::min<int>(1 + rng() % maxRunLength, kRowCount - stringData.size());
    for (int j = 0; j < runLen; ++j) {
      stringData.push_back(value);
    }
  }

  rowType_ = ROW({"string_val", "long_val"}, {VARCHAR(), BIGINT()});

  auto type = asRowType(rowType_);
  auto stringVector =
      velox::test::VectorMaker(leafPool_.get())
          .flatVector<velox::StringView>(
              kRowCount,
              [&](auto row) { return velox::StringView(stringData[row]); },
              withNulls ? velox::test::VectorMaker::nullEvery(7) : nullptr);
  auto longVector =
      velox::test::VectorMaker(leafPool_.get())
          .flatVector<int64_t>(kRowCount, [](auto row) { return row; });

  auto batch = std::make_shared<RowVector>(
      leafPool_.get(),
      type,
      nullptr,
      kRowCount,
      std::vector<VectorPtr>{stringVector, longVector});

  sinkData_.clear();
  {
    auto writeFile = std::make_unique<InMemoryWriteFile>(&sinkData_);
    VeloxWriterOptions writerOptions;
    writerOptions.encodingLayoutTree.emplace(
        buildForcedRleDictionaryEncodingLayoutTree());
    VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, std::move(writerOptions));
    writer.write(batch);
    writer.close();
  }

  auto input = std::make_unique<velox::dwio::common::BufferedInput>(
      std::make_shared<velox::InMemoryReadFile>(sinkData_),
      *leafPool_,
      velox::dwio::common::MetricsLog::voidLog());

  velox::dwio::common::ReaderOptions readerOpts(leafPool_.get());
  readerOpts.setDataIoStats(dataIoStats_);
  readerOpts.setMetadataIoStats(metadataIoStats_);
  auto reader = makeReader(readerOpts, std::move(input));
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("root");
  scanSpec->addAllChildFields(*type);
  velox::dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.setScanSpec(scanSpec);
  rowReaderOptions.setStringDecoderZeroCopy(param().stringDecoderZeroCopy);
  rowReaderOptions.setNimblePreserveDictionaryEncoding(
      param().stringDecoderZeroCopy);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  auto result = BaseVector::create(type, 0, leafPool_.get());
  size_t totalRows = 0;
  size_t globalRow = 0;
  while (rowReader->next(kRowCount, result)) {
    auto* rowResult = result->as<RowVector>();
    ASSERT_NE(rowResult, nullptr);
    totalRows += rowResult->size();

    auto stringChild = rowResult->childAt(0)->loadedVector();
    DecodedVector decoded(*stringChild);
    for (size_t i = 0; i < rowResult->size(); ++i) {
      if (withNulls && globalRow % 7 == 0) {
        ASSERT_TRUE(decoded.isNullAt(i))
            << "Expected null at row=" << globalRow;
      } else {
        ASSERT_FALSE(decoded.isNullAt(i))
            << "Unexpected null at row=" << globalRow;
        ASSERT_EQ(decoded.valueAt<StringView>(i).str(), stringData[globalRow])
            << "Mismatch at row=" << globalRow;
      }
      ++globalRow;
    }
  }
  EXPECT_EQ(totalRows, kRowCount);
}

// Constant string column — all values identical. The encoding factory picks
// ConstantEncoding which is dictionary-enabled, so the reader should return
// a DictionaryVector with a 1-element alphabet.
TEST_P(E2EFilterTest, constantStringDictionaryVector) {
  constexpr int kRowCount = 500;
  const std::string constantValue = "always_the_same_value";

  rowType_ = ROW({"string_val", "long_val"}, {VARCHAR(), BIGINT()});
  auto type = asRowType(rowType_);
  auto stringVector =
      velox::test::VectorMaker(leafPool_.get())
          .flatVector<velox::StringView>(kRowCount, [&](auto /*row*/) {
            return velox::StringView(constantValue);
          });
  auto longVector =
      velox::test::VectorMaker(leafPool_.get())
          .flatVector<int64_t>(kRowCount, [](auto row) { return row; });

  auto batch = std::make_shared<RowVector>(
      leafPool_.get(),
      type,
      nullptr,
      kRowCount,
      std::vector<VectorPtr>{stringVector, longVector});

  sinkData_.clear();
  {
    auto writeFile = std::make_unique<InMemoryWriteFile>(&sinkData_);
    VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, VeloxWriterOptions{});
    writer.write(batch);
    writer.close();
  }

  auto input = std::make_unique<velox::dwio::common::BufferedInput>(
      std::make_shared<velox::InMemoryReadFile>(sinkData_),
      *leafPool_,
      velox::dwio::common::MetricsLog::voidLog());

  velox::dwio::common::ReaderOptions readerOpts(leafPool_.get());
  readerOpts.setDataIoStats(dataIoStats_);
  readerOpts.setMetadataIoStats(metadataIoStats_);
  auto reader = makeReader(readerOpts, std::move(input));
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("root");
  scanSpec->addAllChildFields(*type);
  velox::dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.setScanSpec(scanSpec);
  rowReaderOptions.setStringDecoderZeroCopy(param().stringDecoderZeroCopy);
  rowReaderOptions.setNimblePreserveDictionaryEncoding(
      param().nimblePreserveDictionaryEncoding);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  auto result = BaseVector::create(type, 0, leafPool_.get());
  size_t totalRows = 0;
  while (rowReader->next(kRowCount, result)) {
    auto* rowResult = result->as<RowVector>();
    ASSERT_NE(rowResult, nullptr);
    totalRows += rowResult->size();

    auto stringChild = rowResult->childAt(0)->loadedVector();
    DecodedVector decoded(*stringChild);
    for (size_t i = 0; i < rowResult->size(); ++i) {
      ASSERT_FALSE(decoded.isNullAt(i));
      ASSERT_EQ(decoded.valueAt<StringView>(i).str(), constantValue);
    }
  }
  EXPECT_EQ(totalRows, kRowCount);
}

// Constant string with nulls — exercises Nullable→Constant path.
TEST_P(E2EFilterTest, constantStringWithNullsDictionaryVector) {
  constexpr int kRowCount = 500;
  const std::string constantValue = "nullable_constant";

  rowType_ = ROW({"string_val", "long_val"}, {VARCHAR(), BIGINT()});
  auto type = asRowType(rowType_);
  auto stringVector =
      velox::test::VectorMaker(leafPool_.get())
          .flatVector<velox::StringView>(
              kRowCount,
              [&](auto /*row*/) { return velox::StringView(constantValue); },
              velox::test::VectorMaker::nullEvery(7));
  auto longVector =
      velox::test::VectorMaker(leafPool_.get())
          .flatVector<int64_t>(kRowCount, [](auto row) { return row; });

  auto batch = std::make_shared<RowVector>(
      leafPool_.get(),
      type,
      nullptr,
      kRowCount,
      std::vector<VectorPtr>{stringVector, longVector});

  sinkData_.clear();
  {
    auto writeFile = std::make_unique<InMemoryWriteFile>(&sinkData_);
    VeloxWriter writer(
        type, std::move(writeFile), *rootPool_, VeloxWriterOptions{});
    writer.write(batch);
    writer.close();
  }

  auto input = std::make_unique<velox::dwio::common::BufferedInput>(
      std::make_shared<velox::InMemoryReadFile>(sinkData_),
      *leafPool_,
      velox::dwio::common::MetricsLog::voidLog());

  velox::dwio::common::ReaderOptions readerOpts(leafPool_.get());
  readerOpts.setDataIoStats(dataIoStats_);
  readerOpts.setMetadataIoStats(metadataIoStats_);
  auto reader = makeReader(readerOpts, std::move(input));
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("root");
  scanSpec->addAllChildFields(*type);
  velox::dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.setScanSpec(scanSpec);
  rowReaderOptions.setStringDecoderZeroCopy(param().stringDecoderZeroCopy);
  rowReaderOptions.setNimblePreserveDictionaryEncoding(
      param().nimblePreserveDictionaryEncoding);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  auto result = BaseVector::create(type, 0, leafPool_.get());
  size_t totalRows = 0;
  size_t globalRow = 0;
  while (rowReader->next(kRowCount, result)) {
    auto* rowResult = result->as<RowVector>();
    ASSERT_NE(rowResult, nullptr);
    totalRows += rowResult->size();

    auto stringChild = rowResult->childAt(0)->loadedVector();
    DecodedVector decoded(*stringChild);
    for (size_t i = 0; i < rowResult->size(); ++i) {
      if (globalRow % 7 == 0) {
        ASSERT_TRUE(decoded.isNullAt(i))
            << "Expected null at row=" << globalRow;
      } else {
        ASSERT_FALSE(decoded.isNullAt(i));
        ASSERT_EQ(decoded.valueAt<StringView>(i).str(), constantValue);
      }
      ++globalRow;
    }
  }
  EXPECT_EQ(totalRows, kRowCount);
}

// Test for cross-chunk encoding changes where encoding type varies between
// chunks. This is a challenging scenario for dictionary column visitors because
// the first chunk may use dictionary encoding while subsequent chunks may use
// trivial encoding (or vice versa).
//
// The test creates data where:
// - Batch 0: Highly repetitive strings -> likely dictionary encoding
// - Batch 1: Unique strings -> likely trivial encoding
// - Batch 2: Highly repetitive strings -> likely dictionary encoding
// - Batch 3: Unique strings -> likely trivial encoding
// - ... (8 batches total)
//
// Each batch is 200 rows and flushed as a separate chunk. The reader requests
// 1000 rows per next() call, forcing each read to span ~5 chunks with
// different encodings.
TEST_P(E2EFilterTest, crossChunkEncodingChange) {
  // Use frequent chunk/stripe flushes to create multiple chunks with different
  // data characteristics.
  flushEveryNBatches_ = 1;

  // Create custom batches with alternating encoding characteristics.
  // Use small batches (200 rows) so that next(1000) spans multiple chunks.
  auto type =
      ROW({"string_val", "string_val_2", "long_val"},
          {VARCHAR(), VARCHAR(), BIGINT()});

  const size_t kRowsPerBatch = 200;
  const int kNumBatches = 8;
  std::vector<RowVectorPtr> batches;

  for (int batchIdx = 0; batchIdx < kNumBatches; ++batchIdx) {
    std::vector<std::string> stringVals;
    std::vector<std::string> stringVals2;
    std::vector<int64_t> longVals;

    stringVals.reserve(kRowsPerBatch);
    stringVals2.reserve(kRowsPerBatch);
    longVals.reserve(kRowsPerBatch);

    if (batchIdx % 2 == 0) {
      // Even batches: highly repetitive strings -> dictionary encoding.
      // Use very low cardinality to ensure dictionary encoding is chosen.
      for (size_t i = 0; i < kRowsPerBatch; ++i) {
        stringVals.push_back(fmt::format("DICT_VAL_{}", i % 5));
        stringVals2.push_back(fmt::format("DICT2_VAL_{}", i % 3));
        longVals.push_back(batchIdx * kRowsPerBatch + i);
      }
    } else {
      // Odd batches: unique strings -> trivial encoding.
      // Use unique values to prevent dictionary encoding.
      for (size_t i = 0; i < kRowsPerBatch; ++i) {
        stringVals.push_back(
            fmt::format("UNIQUE_VAL_BATCH{}_ROW{}_EXTRA_PADDING", batchIdx, i));
        stringVals2.push_back(
            fmt::format("UNIQUE2_VAL_BATCH{}_ROW{}_MORE_PADDING", batchIdx, i));
        longVals.push_back(batchIdx * kRowsPerBatch + i);
      }
    }

    auto stringVector =
        velox::test::VectorMaker(leafPool_.get())
            .flatVector<velox::StringView>(kRowsPerBatch, [&](auto row) {
              return velox::StringView(stringVals[row]);
            });
    auto stringVector2 =
        velox::test::VectorMaker(leafPool_.get())
            .flatVector<velox::StringView>(kRowsPerBatch, [&](auto row) {
              return velox::StringView(stringVals2[row]);
            });
    auto longVector = velox::test::VectorMaker(leafPool_.get())
                          .flatVector<int64_t>(kRowsPerBatch, [&](auto row) {
                            return longVals[row];
                          });

    batches.push_back(
        std::make_shared<RowVector>(
            leafPool_.get(),
            type,
            nullptr,
            kRowsPerBatch,
            std::vector<VectorPtr>{stringVector, stringVector2, longVector}));
  }

  // Write the data to memory.
  writeToMemory(type, batches, /*forRowGroupSkip=*/false);

  // Read back and verify the data is correct.
  auto input = std::make_unique<velox::dwio::common::BufferedInput>(
      std::make_shared<velox::InMemoryReadFile>(sinkData_),
      *leafPool_,
      velox::dwio::common::MetricsLog::voidLog());

  velox::dwio::common::ReaderOptions crossChunkReaderOpts(leafPool_.get());
  crossChunkReaderOpts.setDataIoStats(dataIoStats_);
  crossChunkReaderOpts.setMetadataIoStats(metadataIoStats_);
  auto reader = makeReader(crossChunkReaderOpts, std::move(input));
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("root");
  scanSpec->addAllChildFields(*type);
  velox::dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  // Read with batch size larger than chunk size, forcing cross-chunk reads.
  // Each chunk has 200 rows, so next(1000) should span ~5 chunks.
  auto result = BaseVector::create(type, 0, leafPool_.get());
  size_t totalRows = 0;
  while (rowReader->next(1000, result)) {
    auto* rowResult = result->as<RowVector>();
    ASSERT_NE(rowResult, nullptr);
    totalRows += rowResult->size();

    // Verify the string values are correct.
    // Use DecodedVector to handle both flat and dictionary-encoded vectors.
    velox::DecodedVector decodedString(*rowResult->childAt(0));
    velox::DecodedVector decodedString2(*rowResult->childAt(1));
    velox::DecodedVector decodedLong(*rowResult->childAt(2));

    for (size_t i = 0; i < rowResult->size(); ++i) {
      auto globalRow = decodedLong.valueAt<int64_t>(i);
      auto batchIdx = globalRow / kRowsPerBatch;
      auto rowInBatch = globalRow % kRowsPerBatch;

      std::string expectedStr;
      std::string expectedStr2;
      if (batchIdx % 2 == 0) {
        expectedStr = fmt::format("DICT_VAL_{}", rowInBatch % 5);
        expectedStr2 = fmt::format("DICT2_VAL_{}", rowInBatch % 3);
      } else {
        expectedStr = fmt::format(
            "UNIQUE_VAL_BATCH{}_ROW{}_EXTRA_PADDING", batchIdx, rowInBatch);
        expectedStr2 = fmt::format(
            "UNIQUE2_VAL_BATCH{}_ROW{}_MORE_PADDING", batchIdx, rowInBatch);
      }

      ASSERT_EQ(decodedString.valueAt<velox::StringView>(i).str(), expectedStr)
          << "Mismatch at globalRow=" << globalRow;
      ASSERT_EQ(
          decodedString2.valueAt<velox::StringView>(i).str(), expectedStr2)
          << "Mismatch at globalRow=" << globalRow;
    }
  }

  EXPECT_EQ(totalRows, kNumBatches * kRowsPerBatch);
}

// --- Column extraction pushdown tests ---
// These tests write data to Nimble format, configure ScanSpec with extraction
// transforms, and verify the reader applies them correctly.

class NimbleExtractionTest : public E2EFilterTest {
 protected:
  struct ReadResult {
    VectorPtr result;
    std::unique_ptr<dwio::common::Reader> reader;
    std::unique_ptr<dwio::common::RowReader> rowReader;
  };

  // Write data to Nimble in-memory and read with a configured ScanSpec.
  // configureColSpec is invoked with the column's ScanSpec to apply
  // extraction settings (setExtractionType, setExtractionFieldIndex,
  // setFilter, setConstantValue on children, etc.).
  ReadResult readWithTransform(
      const RowVectorPtr& data,
      const std::string& columnName,
      std::function<void(common::ScanSpec*)> configureColSpec) {
    // Write.
    rowType_ = asRowType(data->type());
    VeloxWriterOptions options;
    auto writeFile = std::make_unique<InMemoryWriteFile>(&sinkData_);
    VeloxWriter writer(
        rowType_, std::move(writeFile), *rootPool_, std::move(options));
    writer.write(data);
    writer.close();

    // Read.
    auto ioStats = std::make_shared<io::IoStatistics>();
    dwio::common::ReaderOptions readerOpts(leafPool_.get());
    readerOpts.setDataIoStats(ioStats);
    readerOpts.setMetadataIoStats(ioStats);
    auto input = std::make_unique<dwio::common::BufferedInput>(
        std::make_shared<InMemoryReadFile>(sinkData_), readerOpts.memoryPool());

    ReadResult rr;
    rr.reader = makeReader(readerOpts, std::move(input));

    auto spec = std::make_shared<common::ScanSpec>("<root>");
    spec->addAllChildFields(*rowType_);
    if (configureColSpec) {
      configureColSpec(spec->childByName(columnName));
    }

    dwio::common::RowReaderOptions rowReaderOpts;
    setUpRowReaderOptions(rowReaderOpts, spec);
    rr.rowReader = rr.reader->createRowReader(rowReaderOpts);

    rr.result = BaseVector::create(rowType_, 0, leafPool_.get());
    rr.rowReader->next(data->size(), rr.result);
    return rr;
  }
};

TEST_P(NimbleExtractionTest, mapKeysExtraction) {
  // Write MAP(VARCHAR, BIGINT), apply MapKeys transform -> ARRAY(VARCHAR).
  velox::test::VectorMaker vm(leafPool_.get());
  auto mapVector = vm.mapVector<StringView, int64_t>(
      {{{"a", 1}, {"b", 2}}, {{"c", 3}}, {{"d", 4}, {"e", 5}, {"f", 6}}});
  auto data = vm.rowVector({"col"}, {mapVector});

  // Reader handles MapKeys natively — no transform needed.
  auto rr = readWithTransform(data, "col", [](common::ScanSpec* colSpec) {
    colSpec->setExtractionType(common::ScanSpec::ExtractionType::kKeys);
  });

  auto* row = rr.result->as<RowVector>();
  ASSERT_EQ(row->size(), 3);
  auto* arr = row->childAt(0)->loadedVector()->as<ArrayVector>();
  ASSERT_EQ(arr->sizeAt(0), 2);
  ASSERT_EQ(arr->sizeAt(1), 1);
  ASSERT_EQ(arr->sizeAt(2), 3);
}

TEST_P(NimbleExtractionTest, sizeExtraction) {
  // Write MAP(VARCHAR, BIGINT), apply Size transform -> BIGINT.
  velox::test::VectorMaker vm(leafPool_.get());
  auto mapVector = vm.mapVector<StringView, int64_t>(
      {{{"a", 1}, {"b", 2}, {"c", 3}}, {{"d", 4}}});
  auto data = vm.rowVector({"col"}, {mapVector});

  // Reader handles Size natively — no transform needed.
  auto rr = readWithTransform(data, "col", [](common::ScanSpec* colSpec) {
    colSpec->setExtractionType(common::ScanSpec::ExtractionType::kSize);
  });

  auto* row = rr.result->as<RowVector>();
  ASSERT_EQ(row->size(), 2);
  auto* sizes = row->childAt(0)->loadedVector()->as<FlatVector<int64_t>>();
  ASSERT_EQ(sizes->valueAt(0), 3);
  ASSERT_EQ(sizes->valueAt(1), 1);
}

TEST_P(NimbleExtractionTest, mapValuesExtraction) {
  // Write MAP(VARCHAR, BIGINT), apply MapValues transform -> ARRAY(BIGINT).
  velox::test::VectorMaker vm(leafPool_.get());
  auto mapVector = vm.mapVector<StringView, int64_t>(
      {{{"a", 10}}, {{"b", 20}, {"c", 30}}, {{"d", 40}}});
  auto data = vm.rowVector({"col"}, {mapVector});

  // Reader handles MapValues natively — no transform needed.
  auto rr = readWithTransform(data, "col", [](common::ScanSpec* colSpec) {
    colSpec->setExtractionType(common::ScanSpec::ExtractionType::kValues);
  });

  auto* row = rr.result->as<RowVector>();
  ASSERT_EQ(row->size(), 3);
  auto* arr = row->childAt(0)->loadedVector()->as<ArrayVector>();
  ASSERT_EQ(arr->sizeAt(0), 1);
  ASSERT_EQ(arr->sizeAt(1), 2);
  ASSERT_EQ(arr->sizeAt(2), 1);
  auto* elements = arr->elements()->as<FlatVector<int64_t>>();
  ASSERT_EQ(elements->valueAt(0), 10);
  ASSERT_EQ(elements->valueAt(1), 20);
  ASSERT_EQ(elements->valueAt(2), 30);
  ASSERT_EQ(elements->valueAt(3), 40);
}

TEST_P(NimbleExtractionTest, mapKeyFilterExtraction) {
  // Write MAP(VARCHAR, BIGINT), apply MapKeyFilter for keys {"a","b"}.
  velox::test::VectorMaker vm(leafPool_.get());
  auto mapVector = vm.mapVector<StringView, int64_t>(
      {{{"a", 1}, {"b", 2}, {"c", 3}}, {{"a", 10}, {"d", 40}}});
  auto data = vm.rowVector({"col"}, {mapVector});

  // MapKeyFilter is implemented as an IN filter on the map keys ScanSpec
  // (type-preserving — MAP stays MAP).
  auto rr = readWithTransform(data, "col", [](common::ScanSpec* colSpec) {
    auto* keysSpec = colSpec->childByName(common::ScanSpec::kMapKeysFieldName);
    keysSpec->setFilter(
        std::make_unique<common::BytesValues>(
            std::vector<std::string>{"a", "b"}, /*nullAllowed=*/false));
  });

  auto* row = rr.result->as<RowVector>();
  ASSERT_EQ(row->size(), 2);
  auto* filteredMap = row->childAt(0)->loadedVector()->as<MapVector>();
  // Row 0: {"a":1, "b":2} kept, "c" filtered out.
  ASSERT_EQ(filteredMap->sizeAt(0), 2);
  // Row 1: {"a":10} kept, "d" filtered out.
  ASSERT_EQ(filteredMap->sizeAt(1), 1);
}

TEST_P(NimbleExtractionTest, structFieldExtraction) {
  // Write ROW(x: INT, y: VARCHAR), extract just field "x".
  velox::test::VectorMaker vm(leafPool_.get());
  auto structVector = vm.rowVector(
      {"x", "y"},
      {vm.flatVector<int32_t>({10, 20, 30}),
       vm.flatVector<StringView>({"aa", "bb", "cc"})});
  auto data = vm.rowVector({"col"}, {structVector});

  // kField on struct: extract field index 0 ("x"); mark "y" constant null.
  auto rr = readWithTransform(data, "col", [&](common::ScanSpec* colSpec) {
    colSpec->setExtractionType(common::ScanSpec::ExtractionType::kField);
    colSpec->setExtractionFieldIndex(0);
    colSpec->childByName("y")->setConstantValue(
        BaseVector::createNullConstant(VARCHAR(), 1, leafPool_.get()));
  });

  auto* row = rr.result->as<RowVector>();
  ASSERT_EQ(row->size(), 3);
  auto* xField = row->childAt(0)->loadedVector()->as<FlatVector<int32_t>>();
  ASSERT_EQ(xField->valueAt(0), 10);
  ASSERT_EQ(xField->valueAt(1), 20);
  ASSERT_EQ(xField->valueAt(2), 30);
}

TEST_P(NimbleExtractionTest, arraySizeExtraction) {
  // Write ARRAY(BIGINT), apply Size transform -> BIGINT.
  velox::test::VectorMaker vm(leafPool_.get());
  auto arrayVector = vm.arrayVector<int64_t>({{1, 2, 3}, {4}, {5, 6}});
  auto data = vm.rowVector({"col"}, {arrayVector});

  // Reader handles Size natively — no transform needed.
  auto rr = readWithTransform(data, "col", [](common::ScanSpec* colSpec) {
    colSpec->setExtractionType(common::ScanSpec::ExtractionType::kSize);
  });

  auto* row = rr.result->as<RowVector>();
  ASSERT_EQ(row->size(), 3);
  auto* sizes = row->childAt(0)->loadedVector()->as<FlatVector<int64_t>>();
  ASSERT_EQ(sizes->valueAt(0), 3);
  ASSERT_EQ(sizes->valueAt(1), 1);
  ASSERT_EQ(sizes->valueAt(2), 2);
}

TEST_P(NimbleExtractionTest, mapValuesStructFieldExtraction) {
  // Write MAP(VARCHAR, ROW(x: INT, y: INT)), apply
  // [MapValues, AE, StructField("x")] -> ARRAY(INT).
  velox::test::VectorMaker vm(leafPool_.get());
  auto keys = vm.flatVector<StringView>({"a", "b", "c"});
  auto structValues = vm.rowVector(
      {"x", "y"},
      {vm.flatVector<int32_t>({10, 20, 30}),
       vm.flatVector<int32_t>({100, 200, 300})});
  auto mapVector = vm.mapVector({0, 2}, keys, structValues);
  auto data = vm.rowVector({"col"}, {mapVector});

  // Reader handles MapValues via kValues and StructField("x") via kField
  // on the values struct — no remaining transform needed.
  auto rr = readWithTransform(data, "col", [&](common::ScanSpec* colSpec) {
    colSpec->setExtractionType(common::ScanSpec::ExtractionType::kValues);
    auto* valuesSpec =
        colSpec->childByName(common::ScanSpec::kMapValuesFieldName);
    valuesSpec->setExtractionType(common::ScanSpec::ExtractionType::kField);
    valuesSpec->setExtractionFieldIndex(0);
    valuesSpec->childByName("y")->setConstantValue(
        BaseVector::createNullConstant(INTEGER(), 1, leafPool_.get()));
  });

  auto* row = rr.result->as<RowVector>();
  ASSERT_EQ(row->size(), 2);
  auto* arr = row->childAt(0)->loadedVector()->as<ArrayVector>();
  ASSERT_EQ(arr->sizeAt(0), 2);
  ASSERT_EQ(arr->sizeAt(1), 1);
  auto* elements = arr->elements()->as<FlatVector<int32_t>>();
  ASSERT_EQ(elements->valueAt(0), 10);
  ASSERT_EQ(elements->valueAt(1), 20);
  ASSERT_EQ(elements->valueAt(2), 30);
}

INSTANTIATE_TEST_SUITE_P(
    NimbleExtractionTests,
    NimbleExtractionTest,
    ::testing::Values(
        E2EFilterTestParams{false, false, false, false, false, false, false},
        E2EFilterTestParams{false, false, false, false, false, true, false}));

} // namespace facebook::nimble
