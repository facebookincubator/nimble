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

// Tests that exercise readWithVisitor code paths by constructing real
// SelectiveColumnReader instances via buildColumnReader and calling read()
// directly.  This bypasses the RowReader::next() pipeline while still using
// the full encoding → ChunkedDecoder → readWithVisitor chain.
//
// Data patterns are chosen to exercise specific encoding types:
//   constant → ConstantEncoding
//   sequential/random → FixedBitWidthEncoding / TrivialEncoding
//   repeated runs → RLEEncoding
//   nullable wrappers → NullableEncoding

#include <gtest/gtest.h>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/tests/NimbleFileWriter.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingUtils.h"
#include "dwio/nimble/encodings/legacy/EncodingFactory.h"
#include "dwio/nimble/encodings/legacy/EncodingUtils.h"
#include "dwio/nimble/encodings/tests/EncodingLayoutTestHelper.h"
#include "dwio/nimble/velox/selective/ByteColumnReader.h"
#include "dwio/nimble/velox/selective/ColumnReader.h"
#include "dwio/nimble/velox/selective/IntegerColumnReader.h"
#include "dwio/nimble/velox/selective/NimbleData.h"
#include "dwio/nimble/velox/selective/ReaderBase.h"
#include "dwio/nimble/velox/selective/RowSizeTracker.h"
#include "dwio/nimble/velox/selective/StringColumnReader.h"
#include "velox/dwio/common/ColumnVisitors.h"
#include "velox/dwio/common/SelectiveColumnReader.h"
#include "velox/dwio/common/SelectiveStructColumnReader.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::nimble {
namespace {

using namespace facebook::velox;

// ---------------------------------------------------------------------------
// Test-only accessor: exposes protected prepareRead and readOffset_ so that
// tests can call readWithVisitor explicitly with a hand-built ColumnVisitor.
// No new data members — static_cast from the real IntegerColumnReader* is
// layout-safe.
// ---------------------------------------------------------------------------
class IntegerColumnReaderTestAccessor : public IntegerColumnReader {
 public:
  using IntegerColumnReader::IntegerColumnReader;

  template <typename T>
  void
  doPrepareRead(int64_t offset, const RowSet& rows, const uint64_t* nulls) {
    this->template prepareRead<T>(offset, rows, nulls);
  }

  void advanceReadOffset(const RowSet& rows) {
    readOffset_ += rows.back() + 1;
  }
};

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
// Parameter: true = non-legacy (getStringBuffersFromDecoder=true),
//            false = legacy (getStringBuffersFromDecoder=false).
class ReadWithVisitorTest : public ::testing::TestWithParam<bool>,
                            public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::initializeMemoryManager(memory::MemoryManager::Options{});
  }

  bool useNonLegacy() const {
    return GetParam();
  }

  // Holds the infrastructure needed to build column readers for a single file.
  struct FileContext {
    std::string fileData;
    std::shared_ptr<ReaderBase> readerBase;
    std::unique_ptr<StripeStreams> streams;
    dwio::common::ColumnReaderStatistics stats;
    std::unique_ptr<RowSizeTracker> rowSizeTracker;
  };

  // Write |input| to an in-memory nimble file and prepare the stripe for
  // reading.  Returns a heap-allocated context to avoid copy/move issues.
  std::unique_ptr<FileContext> makeFileContext(const RowVectorPtr& input) {
    auto ctx = std::make_unique<FileContext>();
    ctx->fileData = test::createNimbleFile(*rootPool_, input);

    auto readFile = std::make_shared<InMemoryReadFile>(ctx->fileData);
    dwio::common::ReaderOptions readerOpts(pool());
    ctx->readerBase = ReaderBase::create(
        std::make_unique<dwio::common::BufferedInput>(readFile, *pool()),
        readerOpts);

    ctx->streams = std::make_unique<StripeStreams>(ctx->readerBase);
    ctx->streams->setStripe(0);

    ctx->rowSizeTracker =
        std::make_unique<RowSizeTracker>(ctx->readerBase->fileSchemaWithId());

    return ctx;
  }

  // Build a struct column reader (root) for the given schema + scanSpec.
  // Uses the test parameter to select either the non-legacy or legacy
  // encoding factory and dispatch trait.
  std::unique_ptr<dwio::common::SelectiveColumnReader> buildReader(
      FileContext& ctx,
      const RowTypePtr& rowType,
      common::ScanSpec& scanSpec) {
    using Factory = std::function<std::unique_ptr<Encoding>(
        memory::MemoryPool&, std::string_view, std::function<void*(uint32_t)>)>;
    Factory factory = useNonLegacy()
        ? Factory(
              [](memory::MemoryPool& pool,
                 std::string_view data,
                 std::function<void*(uint32_t)> sbf)
                  -> std::unique_ptr<Encoding> {
                return EncodingFactory::decode(pool, data, std::move(sbf));
              })
        : Factory(
              [](memory::MemoryPool& pool,
                 std::string_view data,
                 std::function<void*(uint32_t)> sbf)
                  -> std::unique_ptr<Encoding> {
                return legacy::EncodingFactory::decode(
                    pool, data, std::move(sbf));
              });
    NimbleParams params(
        *pool(),
        ctx.stats,
        ctx.readerBase->nimbleSchema(),
        *ctx.streams,
        ctx.rowSizeTracker.get(),
        std::move(factory),
        /*getStringBuffersFromDecoder=*/useNonLegacy());

    auto reader = buildColumnReader(
        rowType,
        ctx.readerBase->fileSchemaWithId(),
        params,
        scanSpec,
        /*isRoot=*/true);
    reader->setIsTopLevel();
    ctx.streams->load();
    return reader;
  }

  // Decode an encoded buffer with the appropriate factory.
  std::unique_ptr<Encoding> decodeEncoding(
      std::string_view encoded,
      velox::memory::MemoryPool& memPool) {
    if (useNonLegacy()) {
      return EncodingFactory::decode(memPool, encoded, nullptr);
    }
    return legacy::EncodingFactory::decode(memPool, encoded, nullptr);
  }

  // Dispatch callReadWithVisitor to the appropriate family.
  template <typename V>
  void dispatchCallReadWithVisitor(
      Encoding& encoding,
      V& visitor,
      ReadWithVisitorParams& params) {
    if (useNonLegacy()) {
      nimble::callReadWithVisitor(encoding, visitor, params);
    } else {
      legacy::callReadWithVisitor(encoding, visitor, params);
    }
  }

  // Encode data with a custom layout, then decode with the appropriate factory.
  template <typename T>
  std::unique_ptr<Encoding> createFromCustomLayout(
      const EncodingLayout& layout,
      const std::vector<T>& data,
      velox::memory::MemoryPool& memPool,
      Buffer& buffer) {
    auto policy = std::make_unique<ReplayedEncodingSelectionPolicy<T>>(
        layout,
        CompressionOptions{},
        [](DataType type) -> std::unique_ptr<EncodingSelectionPolicyBase> {
          UNIQUE_PTR_FACTORY(type, TrivialNestedPolicy);
        });
    auto encoded = EncodingFactory::encode<T>(
        std::move(policy),
        std::span<const T>(data.data(), data.size()),
        buffer);
    return decodeEncoding(encoded, memPool);
  }

  // Build root reader, get its first child, call read(), return child.
  // Caller inspects outputRows / numValues / rawValues / hasNulls on it.
  dwio::common::SelectiveColumnReader* readColumn(
      dwio::common::SelectiveColumnReader* root,
      int numRows) {
    // RowSet: sequential [0 .. numRows-1]
    std::vector<vector_size_t> rowVec(numRows);
    std::iota(rowVec.begin(), rowVec.end(), 0);
    RowSet rows(rowVec.data(), rowVec.size());

    // Trigger read on the root struct reader which calls read() on children.
    root->read(0, rows, nullptr);

    auto* structReader =
        dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root);
    EXPECT_NE(structReader, nullptr);
    EXPECT_FALSE(structReader->children().empty());
    return structReader->children()[0];
  }

  // Helper: get typed values from a column reader after read().
  template <typename T>
  std::vector<T> getValues(dwio::common::SelectiveColumnReader* reader) {
    auto* raw = reinterpret_cast<const T*>(reader->rawValues());
    return std::vector<T>(raw, raw + reader->numValues());
  }
};

// ===========================================================================
// Test: Dense read, no filter, sequential int64 data (no nulls)
// Exercises TrivialEncoding / FixedBitWidthEncoding readWithVisitor with
// AlwaysTrue filter.
// ===========================================================================
TEST_P(ReadWithVisitorTest, denseNoFilterNoNulls) {
  constexpr int kRows = 200;
  auto input = makeRowVector(
      {makeFlatVector<int64_t>(kRows, [](auto i) { return i * 7; })});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  // AlwaysTrue filter forces read() instead of lazy-loading the child.
  // This exercises the same readWithVisitor<AlwaysTrue> code path.
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  EXPECT_EQ(child->numValues(), kRows);
  auto values = getValues<int64_t>(child);
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(values[i], i * 7) << "row " << i;
  }
}

// ===========================================================================
// Test: Dense read, no filter, nullable int64 (exercises NullableEncoding)
// ===========================================================================
TEST_P(ReadWithVisitorTest, denseNoFilterWithNulls) {
  constexpr int kRows = 200;
  auto input = makeRowVector(
      {makeFlatVector<int64_t>(kRows, folly::identity, nullEvery(7))});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  // Every row is "output" since there is no filter.
  EXPECT_EQ(child->numValues(), kRows);
  EXPECT_TRUE(child->hasNulls());
}

// ===========================================================================
// Test: BigintRange filter, sequential data — sparse visitor output
// ===========================================================================
TEST_P(ReadWithVisitorTest, sparseWithBigintRange) {
  constexpr int kRows = 500;
  auto input = makeRowVector({makeFlatVector<int64_t>(kRows, folly::identity)});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintRange>(100, 200, false));

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  // Exactly rows [100..200] pass the filter → 101 values.
  EXPECT_EQ(child->numValues(), 101);
  auto rows = child->outputRows();
  EXPECT_EQ(rows[0], 100);
  EXPECT_EQ(rows[rows.size() - 1], 200);
}

// ===========================================================================
// Test: BigintRange filter + nullable data — filter skips nulls
// ===========================================================================
TEST_P(ReadWithVisitorTest, rangeFilterWithNulls) {
  constexpr int kRows = 500;
  auto c0 = makeFlatVector<int64_t>(kRows, folly::identity, nullEvery(7));
  auto input = makeRowVector({c0});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintRange>(50, 300, false));

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  // Count expected: value in [50,300] AND not null.
  int expected = 0;
  for (int i = 0; i < kRows; ++i) {
    if (!c0->isNullAt(i) && c0->valueAt(i) >= 50 && c0->valueAt(i) <= 300) {
      ++expected;
    }
  }
  EXPECT_EQ(child->numValues(), expected);
}

// ===========================================================================
// Test: IsNotNull filter on nullable data
// ===========================================================================
TEST_P(ReadWithVisitorTest, isNotNullFilter) {
  constexpr int kRows = 300;
  auto input = makeRowVector(
      {makeFlatVector<int64_t>(kRows, folly::identity, nullEvery(5))});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(std::make_unique<common::IsNotNull>());

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  int expectedNonNull = 0;
  for (int i = 0; i < kRows; ++i) {
    expectedNonNull += (i % 5 != 0) ? 1 : 0;
  }
  EXPECT_EQ(child->numValues(), expectedNonNull);
  // All output values are non-null; hasNulls should be false (or true only
  // if the encoding delivers reader-nulls).
}

// ===========================================================================
// Test: Constant data → ConstantEncoding::readWithVisitor
// ===========================================================================
TEST_P(ReadWithVisitorTest, constantEncoding) {
  constexpr int kRows = 300;
  auto input =
      makeRowVector({makeFlatVector<int64_t>(kRows, [](auto) { return 42; })});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  EXPECT_EQ(child->numValues(), kRows);
  auto values = getValues<int64_t>(child);
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(values[i], 42);
  }
}

// ===========================================================================
// Test: Constant data + non-matching filter → zero output rows
// ===========================================================================
TEST_P(ReadWithVisitorTest, constantEncodingFilterMiss) {
  constexpr int kRows = 300;
  auto input =
      makeRowVector({makeFlatVector<int64_t>(kRows, [](auto) { return 42; })});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintRange>(100, 200, false));

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  EXPECT_EQ(child->numValues(), 0);
  EXPECT_TRUE(child->outputRows().empty());
}

// ===========================================================================
// Test: Boolean data with BoolValue filter (SparseBoolEncoding)
// ===========================================================================
TEST_P(ReadWithVisitorTest, boolWithFilter) {
  constexpr int kRows = 300;
  auto c0 = makeFlatVector<bool>(kRows, [](auto i) { return i % 3 != 0; });
  auto input = makeRowVector({c0});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BoolValue>(true, false));

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  int expectedTrue = 0;
  for (int i = 0; i < kRows; ++i) {
    expectedTrue += (i % 3 != 0) ? 1 : 0;
  }
  EXPECT_EQ(child->numValues(), expectedTrue);
}

// ===========================================================================
// Test: Boolean data with nulls + filter
// ===========================================================================
TEST_P(ReadWithVisitorTest, boolWithNullsAndFilter) {
  constexpr int kRows = 300;
  auto c0 = makeFlatVector<bool>(
      kRows, [](auto i) { return i % 3 != 0; }, nullEvery(7));
  auto input = makeRowVector({c0});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BoolValue>(true, false));

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  int expected = 0;
  for (int i = 0; i < kRows; ++i) {
    if (!c0->isNullAt(i) && c0->valueAt(i)) {
      ++expected;
    }
  }
  EXPECT_EQ(child->numValues(), expected);
}

// ===========================================================================
// Test: String data, no filter (TrivialEncoding<string_view>)
// ===========================================================================
TEST_P(ReadWithVisitorTest, stringNoFilter) {
  constexpr int kRows = 200;
  auto input = makeRowVector({makeFlatVector<StringView>(kRows, [](auto i) {
    return StringView::makeInline(fmt::format("s{}", i));
  })});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  EXPECT_EQ(child->numValues(), kRows);
}

// ===========================================================================
// Test: String data with nulls + IsNotNull filter
// ===========================================================================
TEST_P(ReadWithVisitorTest, stringWithNullsIsNotNull) {
  constexpr int kRows = 200;
  auto c0 = makeFlatVector<StringView>(
      kRows,
      [](auto i) { return StringView::makeInline(fmt::format("s{}", i)); },
      nullEvery(5));
  auto input = makeRowVector({c0});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(std::make_unique<common::IsNotNull>());

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  int expectedNonNull = 0;
  for (int i = 0; i < kRows; ++i) {
    expectedNonNull += (i % 5 != 0) ? 1 : 0;
  }
  EXPECT_EQ(child->numValues(), expectedNonNull);
}

// ===========================================================================
// Test: All-null data → NullableEncoding with all-null inner
// ===========================================================================
TEST_P(ReadWithVisitorTest, allNullData) {
  constexpr int kRows = 100;
  auto input = makeRowVector(
      {makeFlatVector<int64_t>(kRows, [](auto) { return 0; }, nullEvery(1))});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  // Use IsNotNull to verify null handling in readWithVisitor — all rows are
  // null so zero rows should pass.
  scanSpec->childByName("c0")->setFilter(std::make_unique<common::IsNotNull>());

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  EXPECT_EQ(child->numValues(), 0);
  EXPECT_TRUE(child->outputRows().empty());
}

// ===========================================================================
// Test: All-null data + IsNotNull filter → zero output
// ===========================================================================
TEST_P(ReadWithVisitorTest, allNullWithIsNotNull) {
  constexpr int kRows = 100;
  auto input = makeRowVector(
      {makeFlatVector<int64_t>(kRows, [](auto) { return 0; }, nullEvery(1))});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(std::make_unique<common::IsNotNull>());

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  EXPECT_EQ(child->numValues(), 0);
}

// ===========================================================================
// Test: Large sequential data + range filter (multi-chunk / stripe)
// ===========================================================================
TEST_P(ReadWithVisitorTest, largeDataRangeFilter) {
  constexpr int kRows = 10000;
  auto c0 = makeFlatVector<int64_t>(kRows, folly::identity, nullEvery(13));
  auto input = makeRowVector({c0});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintRange>(1000, 5000, false));

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  int expected = 0;
  for (int i = 0; i < kRows; ++i) {
    if (!c0->isNullAt(i) && c0->valueAt(i) >= 1000 && c0->valueAt(i) <= 5000) {
      ++expected;
    }
  }
  EXPECT_EQ(child->numValues(), expected);

  // Verify outputRows are sorted and in range.
  auto rows = child->outputRows();
  for (int i = 0; i < rows.size(); ++i) {
    EXPECT_GE(rows[i], 1000);
    EXPECT_LE(rows[i], 5000);
    if (i > 0) {
      EXPECT_GT(rows[i], rows[i - 1]);
    }
  }
}

// ===========================================================================
// Test: Multiple types (int32, double) — exercises different column readers
// ===========================================================================
TEST_P(ReadWithVisitorTest, multipleColumnTypesWithFilters) {
  constexpr int kRows = 300;
  auto c0 =
      makeFlatVector<int32_t>(kRows, [](auto i) { return i; }, nullEvery(11));
  auto c1 = makeFlatVector<double>(
      kRows, [](auto i) { return i * 0.5; }, nullEvery(7));
  auto input = makeRowVector({c0, c1});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  // Filter on int column — exercises int readWithVisitor with sparse output.
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintRange>(50, 150, false));

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  int expected = 0;
  for (int i = 0; i < kRows; ++i) {
    if (!c0->isNullAt(i) && c0->valueAt(i) >= 50 && c0->valueAt(i) <= 150) {
      ++expected;
    }
  }
  EXPECT_EQ(child->numValues(), expected);
  EXPECT_FALSE(child->outputRows().empty());
}

// ===========================================================================
// Test: Filter on one column of multi-column row — exercises sparse rows
// propagation to second column
// ===========================================================================
TEST_P(ReadWithVisitorTest, filterOnOneColumnInspectState) {
  constexpr int kRows = 300;
  auto c0 = makeFlatVector<int64_t>(kRows, folly::identity);
  auto input = makeRowVector({c0});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintRange>(50, 100, false));

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* child = readColumn(root.get(), kRows);

  // c0 filter passes rows [50..100] → 51 values.
  EXPECT_EQ(child->numValues(), 51);

  // Verify outputRows are exactly [50..100].
  auto rows = child->outputRows();
  EXPECT_EQ(rows.size(), 51);
  for (int i = 0; i < rows.size(); ++i) {
    EXPECT_EQ(rows[i], 50 + i);
  }

  // Verify values.
  auto values = getValues<int64_t>(child);
  for (int i = 0; i < values.size(); ++i) {
    EXPECT_EQ(values[i], 50 + i);
  }
}

// ===========================================================================
// EXPLICIT readWithVisitor TESTS
//
// These tests manually construct a ColumnVisitor with explicit template
// parameters and call readWithVisitor on the concrete column reader.
// This bypasses readCommon / processFilter entirely.
// ===========================================================================

// Explicit readWithVisitor: BigintRange filter, dense rows, ExtractToReader.
TEST_P(ReadWithVisitorTest, explicitReadWithVisitor_BigintRange_Dense) {
  constexpr int kRows = 500;
  auto input = makeRowVector({makeFlatVector<int64_t>(kRows, folly::identity)});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintRange>(100, 200, false));

  auto root = buildReader(*ctx, rowType, *scanSpec);

  // Extract the child IntegerColumnReader.
  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  ASSERT_NE(structReader, nullptr);
  auto* child = structReader->children()[0];

  // Cast to our test accessor to call prepareRead.
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(child));
  ASSERT_NE(reader, nullptr);

  // Build dense RowSet [0..kRows-1].
  std::vector<vector_size_t> rowVec(kRows);
  std::iota(rowVec.begin(), rowVec.end(), 0);
  RowSet rows(rowVec.data(), rowVec.size());

  // Step 1: prepareRead — sets up nulls, output buffers, values capacity.
  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  // Step 2: Construct ColumnVisitor with explicit template parameters.
  common::BigintRange filter(100, 200, false);
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = true;
  DecoderVisitor<
      int64_t,
      common::BigintRange,
      dwio::common::ExtractToReader,
      kIsDense>
      visitor(filter, reader, rows, extractValues);

  // Step 3: Explicit readWithVisitor call.
  reader->readWithVisitor(rows, std::move(visitor));
  reader->advanceReadOffset(rows);

  // Step 4: Inspect reader state.
  EXPECT_EQ(reader->numValues(), 101);
  auto outRows = reader->outputRows();
  EXPECT_EQ(outRows.size(), 101);
  EXPECT_EQ(outRows[0], 100);
  EXPECT_EQ(outRows[100], 200);

  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < 101; ++i) {
    EXPECT_EQ(values[i], 100 + i);
  }
}

// Explicit readWithVisitor: AlwaysTrue filter (no filtering), dense rows.
TEST_P(ReadWithVisitorTest, explicitReadWithVisitor_AlwaysTrue_Dense) {
  constexpr int kRows = 200;
  auto input = makeRowVector(
      {makeFlatVector<int64_t>(kRows, [](auto i) { return i * 3; })});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  std::vector<vector_size_t> rowVec(kRows);
  std::iota(rowVec.begin(), rowVec.end(), 0);
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  common::AlwaysTrue filter;
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = true;
  DecoderVisitor<
      int64_t,
      common::AlwaysTrue,
      dwio::common::ExtractToReader,
      kIsDense>
      visitor(filter, reader, rows, extractValues);

  reader->readWithVisitor(rows, std::move(visitor));
  reader->advanceReadOffset(rows);

  EXPECT_EQ(reader->numValues(), kRows);
  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(values[i], i * 3);
  }
}

// Explicit readWithVisitor: BigintRange filter, sparse rows (non-dense).
TEST_P(ReadWithVisitorTest, explicitReadWithVisitor_BigintRange_Sparse) {
  constexpr int kRows = 500;
  auto input = makeRowVector({makeFlatVector<int64_t>(kRows, folly::identity)});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintRange>(0, 499, false));

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  // Sparse RowSet: every other row → non-dense (rows.back() != rows.size()-1).
  std::vector<vector_size_t> rowVec;
  for (int i = 0; i < kRows; i += 2) {
    rowVec.push_back(i);
  }
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  common::BigintRange filter(0, 499, false);
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = false;
  DecoderVisitor<
      int64_t,
      common::BigintRange,
      dwio::common::ExtractToReader,
      kIsDense>
      visitor(filter, reader, rows, extractValues);

  reader->readWithVisitor(rows, std::move(visitor));
  reader->advanceReadOffset(rows);

  // All even-indexed rows pass the filter.
  EXPECT_EQ(reader->numValues(), static_cast<int>(rowVec.size()));
  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < values.size(); ++i) {
    EXPECT_EQ(values[i], i * 2);
  }
}

// Explicit readWithVisitor: IsNotNull filter on nullable data.
TEST_P(ReadWithVisitorTest, explicitReadWithVisitor_IsNotNull_Nullable) {
  constexpr int kRows = 300;
  auto input = makeRowVector(
      {makeFlatVector<int64_t>(kRows, folly::identity, nullEvery(5))});
  auto rowType = asRowType(input->type());

  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(std::make_unique<common::IsNotNull>());

  auto root = buildReader(*ctx, rowType, *scanSpec);
  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  std::vector<vector_size_t> rowVec(kRows);
  std::iota(rowVec.begin(), rowVec.end(), 0);
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  common::IsNotNull filter;
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = true;
  DecoderVisitor<
      int64_t,
      common::IsNotNull,
      dwio::common::ExtractToReader,
      kIsDense>
      visitor(filter, reader, rows, extractValues);

  reader->readWithVisitor(rows, std::move(visitor));
  reader->advanceReadOffset(rows);

  int expectedNonNull = 0;
  for (int i = 0; i < kRows; ++i) {
    expectedNonNull += (i % 5 != 0) ? 1 : 0;
  }
  EXPECT_EQ(reader->numValues(), expectedNonNull);
}

// ===========================================================================
// ENCODING-LEVEL readWithVisitor TESTS
//
// These tests create Encoding* instances directly with forced encoding types
// and call callReadWithVisitor(*encoding, visitor, params) on them, fully
// isolating the encoding under test from file I/O.  The nimble file is only
// used to set up the SelectiveColumnReader infrastructure needed by the
// ColumnVisitor.
// ===========================================================================

// Construct ReadWithVisitorParams for the non-null path, modeled after
// ChunkedDecoder::readWithVisitor.
template <typename V>
ReadWithVisitorParams makeReadWithVisitorParams(
    V& visitor,
    const RowSet& rows,
    velox::memory::MemoryPool* memPool) {
  ReadWithVisitorParams params{};
  params.prepareResultNulls = [&visitor, &rows] {
    visitor.reader().prepareNulls(rows, true, 8);
  };
  params.initReturnReaderNulls = [&visitor, &rows] {
    visitor.reader().initReturnReaderNulls(rows);
  };
  const auto numRows = visitor.numRows();
  params.makeReaderNulls = [memPool, numRows, &visitor] {
    auto& nulls = visitor.reader().nullsInReadRange();
    if (!nulls) {
      nulls = velox::AlignedBuffer::allocate<bool>(
          visitor.rowAt(numRows - 1) + 1, memPool, velox::bits::kNotNull);
    }
    return nulls->template asMutable<uint64_t>();
  };
  params.numScanned = 0;
  return params;
}

// ---------------------------------------------------------------------------
// 1. TrivialEncoding<int64_t> + BigintRange + dense
// ---------------------------------------------------------------------------
TEST_P(ReadWithVisitorTest, encodingLevel_Trivial_BigintRange_Dense) {
  constexpr int kRows = 500;

  // Sequential data [0..499].
  std::vector<int64_t> data(kRows);
  std::iota(data.begin(), data.end(), 0);

  // Write nimble file with the same data (for reader infrastructure).
  auto input = makeRowVector({makeFlatVector<int64_t>(kRows, folly::identity)});
  auto rowType = asRowType(input->type());
  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintRange>(100, 200, false));
  auto root = buildReader(*ctx, rowType, *scanSpec);

  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  std::vector<vector_size_t> rowVec(kRows);
  std::iota(rowVec.begin(), rowVec.end(), 0);
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  // Create the encoding independently with forced TrivialEncoding.
  Buffer buffer(*pool());
  auto encoding =
      createFromCustomLayout<int64_t>(TrivialEnc{}, data, *pool(), buffer);

  // Construct visitor and params, then call callReadWithVisitor directly.
  common::BigintRange filter(100, 200, false);
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = true;
  DecoderVisitor<
      int64_t,
      common::BigintRange,
      dwio::common::ExtractToReader,
      kIsDense>
      visitor(filter, reader, rows, extractValues);
  auto params = makeReadWithVisitorParams(visitor, rows, pool());

  dispatchCallReadWithVisitor(*encoding, visitor, params);

  EXPECT_EQ(reader->numValues(), 101);
  auto outRows = reader->outputRows();
  EXPECT_EQ(outRows[0], 100);
  EXPECT_EQ(outRows[100], 200);
  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < 101; ++i) {
    EXPECT_EQ(values[i], 100 + i);
  }
}

// ---------------------------------------------------------------------------
// 2. ConstantEncoding<int64_t> + AlwaysTrue + dense
// ---------------------------------------------------------------------------
TEST_P(ReadWithVisitorTest, encodingLevel_Constant_AlwaysTrue_Dense) {
  constexpr int kRows = 300;

  // All-same data.
  std::vector<int64_t> data(kRows, 42);

  auto input =
      makeRowVector({makeFlatVector<int64_t>(kRows, [](auto) { return 42; })});
  auto rowType = asRowType(input->type());
  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());
  auto root = buildReader(*ctx, rowType, *scanSpec);

  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  std::vector<vector_size_t> rowVec(kRows);
  std::iota(rowVec.begin(), rowVec.end(), 0);
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  Buffer buffer(*pool());
  auto encoding =
      createFromCustomLayout<int64_t>(ConstantEnc{}, data, *pool(), buffer);

  common::AlwaysTrue filter;
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = true;
  DecoderVisitor<
      int64_t,
      common::AlwaysTrue,
      dwio::common::ExtractToReader,
      kIsDense>
      visitor(filter, reader, rows, extractValues);
  auto params = makeReadWithVisitorParams(visitor, rows, pool());

  dispatchCallReadWithVisitor(*encoding, visitor, params);

  EXPECT_EQ(reader->numValues(), kRows);
  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(values[i], 42);
  }
}

// ---------------------------------------------------------------------------
// 3. FixedBitWidthEncoding<int64_t> + BigintRange + sparse
// ---------------------------------------------------------------------------
TEST_P(ReadWithVisitorTest, encodingLevel_FixedBitWidth_BigintRange_Sparse) {
  constexpr int kRows = 500;

  // Small-range data [0..15] that fits in fixed-bit-width encoding.
  std::vector<int64_t> data(kRows);
  for (int i = 0; i < kRows; ++i) {
    data[i] = i % 16;
  }

  auto input = makeRowVector(
      {makeFlatVector<int64_t>(kRows, [](auto i) { return i % 16; })});
  auto rowType = asRowType(input->type());
  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintRange>(5, 10, false));
  auto root = buildReader(*ctx, rowType, *scanSpec);

  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  // Sparse RowSet: every other row.
  std::vector<vector_size_t> rowVec;
  for (int i = 0; i < kRows; i += 2) {
    rowVec.push_back(i);
  }
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  Buffer buffer(*pool());
  auto encoding = createFromCustomLayout<int64_t>(
      FixedBitWidthEnc{}, data, *pool(), buffer);

  common::BigintRange filter(5, 10, false);
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = false;
  DecoderVisitor<
      int64_t,
      common::BigintRange,
      dwio::common::ExtractToReader,
      kIsDense>
      visitor(filter, reader, rows, extractValues);
  auto params = makeReadWithVisitorParams(visitor, rows, pool());

  dispatchCallReadWithVisitor(*encoding, visitor, params);

  // Count expected: even-indexed rows where data[i] in [5..10].
  int expected = 0;
  for (int i = 0; i < kRows; i += 2) {
    if (data[i] >= 5 && data[i] <= 10) {
      ++expected;
    }
  }
  EXPECT_EQ(reader->numValues(), expected);

  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < reader->numValues(); ++i) {
    EXPECT_GE(values[i], 5);
    EXPECT_LE(values[i], 10);
  }
}

// ---------------------------------------------------------------------------
// 4. RLEEncoding<int64_t> + AlwaysTrue + dense
// ---------------------------------------------------------------------------
TEST_P(ReadWithVisitorTest, encodingLevel_RLE_AlwaysTrue_Dense) {
  constexpr int kRows = 500;

  // Repeated-run data: 50 copies of each value [0..9].
  std::vector<int64_t> data(kRows);
  for (int i = 0; i < kRows; ++i) {
    data[i] = i / 50;
  }

  auto input = makeRowVector(
      {makeFlatVector<int64_t>(kRows, [](auto i) { return i / 50; })});
  auto rowType = asRowType(input->type());
  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());
  auto root = buildReader(*ctx, rowType, *scanSpec);

  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  std::vector<vector_size_t> rowVec(kRows);
  std::iota(rowVec.begin(), rowVec.end(), 0);
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  Buffer buffer(*pool());
  auto encoding =
      createFromCustomLayout<int64_t>(RLEEnc{}, data, *pool(), buffer);

  common::AlwaysTrue filter;
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = true;
  DecoderVisitor<
      int64_t,
      common::AlwaysTrue,
      dwio::common::ExtractToReader,
      kIsDense>
      visitor(filter, reader, rows, extractValues);
  auto params = makeReadWithVisitorParams(visitor, rows, pool());

  dispatchCallReadWithVisitor(*encoding, visitor, params);

  EXPECT_EQ(reader->numValues(), kRows);
  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(values[i], i / 50) << "row " << i;
  }
}

// ---------------------------------------------------------------------------
// 5. TrivialEncoding
// 5. TrivialEncoding<int64_t> + AlwaysTrue + dense + SLOW PATH
//    hasBulkPath=false forces readWithVisitorSlow regardless of CPU features.
// ---------------------------------------------------------------------------
TEST_P(ReadWithVisitorTest, encodingLevel_Trivial_AlwaysTrue_Dense_SlowPath) {
  constexpr int kRows = 500;

  std::vector<int64_t> data(kRows);
  std::iota(data.begin(), data.end(), 0);

  auto input = makeRowVector({makeFlatVector<int64_t>(kRows, folly::identity)});
  auto rowType = asRowType(input->type());
  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());
  auto root = buildReader(*ctx, rowType, *scanSpec);

  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  std::vector<vector_size_t> rowVec(kRows);
  std::iota(rowVec.begin(), rowVec.end(), 0);
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  Buffer buffer(*pool());
  auto encoding =
      createFromCustomLayout<int64_t>(TrivialEnc{}, data, *pool(), buffer);

  common::AlwaysTrue filter;
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = true;
  constexpr bool kHasBulkPath = false;
  velox::dwio::common::ColumnVisitor<
      int64_t,
      common::AlwaysTrue,
      dwio::common::ExtractToReader,
      kIsDense,
      kHasBulkPath>
      visitor(filter, reader, rows, extractValues);
  auto params = makeReadWithVisitorParams(visitor, rows, pool());

  dispatchCallReadWithVisitor(*encoding, visitor, params);

  EXPECT_EQ(reader->numValues(), kRows);
  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(values[i], i);
  }
}

// ---------------------------------------------------------------------------
// 6. RLEEncoding<int64_t> + AlwaysTrue + dense + SLOW PATH
// ---------------------------------------------------------------------------
TEST_P(ReadWithVisitorTest, encodingLevel_RLE_AlwaysTrue_Dense_SlowPath) {
  constexpr int kRows = 500;

  std::vector<int64_t> data(kRows);
  for (int i = 0; i < kRows; ++i) {
    data[i] = i / 50;
  }

  auto input = makeRowVector(
      {makeFlatVector<int64_t>(kRows, [](auto i) { return i / 50; })});
  auto rowType = asRowType(input->type());
  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());
  auto root = buildReader(*ctx, rowType, *scanSpec);

  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  std::vector<vector_size_t> rowVec(kRows);
  std::iota(rowVec.begin(), rowVec.end(), 0);
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  Buffer buffer(*pool());
  auto encoding =
      createFromCustomLayout<int64_t>(RLEEnc{}, data, *pool(), buffer);

  common::AlwaysTrue filter;
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = true;
  constexpr bool kHasBulkPath = false;
  velox::dwio::common::ColumnVisitor<
      int64_t,
      common::AlwaysTrue,
      dwio::common::ExtractToReader,
      kIsDense,
      kHasBulkPath>
      visitor(filter, reader, rows, extractValues);
  auto params = makeReadWithVisitorParams(visitor, rows, pool());

  dispatchCallReadWithVisitor(*encoding, visitor, params);

  EXPECT_EQ(reader->numValues(), kRows);
  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(values[i], i / 50) << "row " << i;
  }
}

// ---------------------------------------------------------------------------
// 7. MainlyConstantEncoding<int64_t> + AlwaysTrue + dense (FAST path)
// ---------------------------------------------------------------------------
TEST_P(ReadWithVisitorTest, encodingLevel_MainlyConstant_AlwaysTrue_Dense) {
  constexpr int kRows = 500;

  std::vector<int64_t> data(kRows, 42);
  data[100] = 7;
  data[200] = 13;
  data[300] = 19;
  data[400] = 23;
  data[450] = 29;

  auto input = makeRowVector({makeFlatVector<int64_t>(data)});
  auto rowType = asRowType(input->type());
  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());
  auto root = buildReader(*ctx, rowType, *scanSpec);

  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  std::vector<vector_size_t> rowVec(kRows);
  std::iota(rowVec.begin(), rowVec.end(), 0);
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  Buffer buffer(*pool());
  auto encoding = createFromCustomLayout<int64_t>(
      MainlyConstantEnc{}, data, *pool(), buffer);

  common::AlwaysTrue filter;
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = true;
  DecoderVisitor<
      int64_t,
      common::AlwaysTrue,
      dwio::common::ExtractToReader,
      kIsDense>
      visitor(filter, reader, rows, extractValues);
  auto params = makeReadWithVisitorParams(visitor, rows, pool());

  dispatchCallReadWithVisitor(*encoding, visitor, params);

  EXPECT_EQ(reader->numValues(), kRows);
  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(values[i], data[i]) << "row " << i;
  }
}

// ---------------------------------------------------------------------------
// 8. MainlyConstantEncoding
// 8. MainlyConstantEncoding<int64_t> + AlwaysTrue + dense + SLOW PATH
// ---------------------------------------------------------------------------
TEST_P(
    ReadWithVisitorTest,
    encodingLevel_MainlyConstant_AlwaysTrue_Dense_SlowPath) {
  constexpr int kRows = 500;

  std::vector<int64_t> data(kRows, 42);
  data[100] = 7;
  data[200] = 13;
  data[300] = 19;
  data[400] = 23;
  data[450] = 29;

  auto input = makeRowVector({makeFlatVector<int64_t>(data)});
  auto rowType = asRowType(input->type());
  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());
  auto root = buildReader(*ctx, rowType, *scanSpec);

  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  std::vector<vector_size_t> rowVec(kRows);
  std::iota(rowVec.begin(), rowVec.end(), 0);
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  Buffer buffer(*pool());
  auto encoding = createFromCustomLayout<int64_t>(
      MainlyConstantEnc{}, data, *pool(), buffer);

  common::AlwaysTrue filter;
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = true;
  constexpr bool kHasBulkPath = false;
  velox::dwio::common::ColumnVisitor<
      int64_t,
      common::AlwaysTrue,
      dwio::common::ExtractToReader,
      kIsDense,
      kHasBulkPath>
      visitor(filter, reader, rows, extractValues);
  auto params = makeReadWithVisitorParams(visitor, rows, pool());

  dispatchCallReadWithVisitor(*encoding, visitor, params);

  EXPECT_EQ(reader->numValues(), kRows);
  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(values[i], data[i]) << "row " << i;
  }
}

// ---------------------------------------------------------------------------
// 9. DictionaryEncoding
// 9. DictionaryEncoding<int64_t> + AlwaysTrue + dense
//    Dictionary always uses slow path (no bulkScan).
// ---------------------------------------------------------------------------
TEST_P(ReadWithVisitorTest, encodingLevel_Dictionary_AlwaysTrue_Dense) {
  constexpr int kRows = 500;

  std::vector<int64_t> data(kRows);
  for (int i = 0; i < kRows; ++i) {
    data[i] = (i % 5) * 10;
  }

  auto input = makeRowVector(
      {makeFlatVector<int64_t>(kRows, [](auto i) { return (i % 5) * 10; })});
  auto rowType = asRowType(input->type());
  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());
  auto root = buildReader(*ctx, rowType, *scanSpec);

  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  std::vector<vector_size_t> rowVec(kRows);
  std::iota(rowVec.begin(), rowVec.end(), 0);
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  Buffer buffer(*pool());
  auto encoding =
      createFromCustomLayout<int64_t>(DictionaryEnc{}, data, *pool(), buffer);

  common::AlwaysTrue filter;
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = true;
  DecoderVisitor<
      int64_t,
      common::AlwaysTrue,
      dwio::common::ExtractToReader,
      kIsDense>
      visitor(filter, reader, rows, extractValues);
  auto params = makeReadWithVisitorParams(visitor, rows, pool());

  dispatchCallReadWithVisitor(*encoding, visitor, params);

  EXPECT_EQ(reader->numValues(), kRows);
  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(values[i], (i % 5) * 10) << "row " << i;
  }
}

// ---------------------------------------------------------------------------
// 10. DictionaryEncoding<int64_t> + BigintRange + sparse
// ---------------------------------------------------------------------------
TEST_P(ReadWithVisitorTest, encodingLevel_Dictionary_BigintRange_Sparse) {
  constexpr int kRows = 500;

  std::vector<int64_t> data(kRows);
  for (int i = 0; i < kRows; ++i) {
    data[i] = (i % 5) * 10;
  }

  auto input = makeRowVector(
      {makeFlatVector<int64_t>(kRows, [](auto i) { return (i % 5) * 10; })});
  auto rowType = asRowType(input->type());
  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintRange>(15, 35, false));
  auto root = buildReader(*ctx, rowType, *scanSpec);

  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  std::vector<vector_size_t> rowVec;
  for (int i = 0; i < kRows; i += 2) {
    rowVec.push_back(i);
  }
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  Buffer buffer(*pool());
  auto encoding =
      createFromCustomLayout<int64_t>(DictionaryEnc{}, data, *pool(), buffer);

  common::BigintRange filter(15, 35, false);
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = false;
  DecoderVisitor<
      int64_t,
      common::BigintRange,
      dwio::common::ExtractToReader,
      kIsDense>
      visitor(filter, reader, rows, extractValues);
  auto params = makeReadWithVisitorParams(visitor, rows, pool());

  dispatchCallReadWithVisitor(*encoding, visitor, params);

  int expected = 0;
  for (int i = 0; i < kRows; i += 2) {
    if (data[i] >= 15 && data[i] <= 35) {
      ++expected;
    }
  }
  EXPECT_EQ(reader->numValues(), expected);

  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < reader->numValues(); ++i) {
    EXPECT_GE(values[i], 15);
    EXPECT_LE(values[i], 35);
  }
}

// ---------------------------------------------------------------------------
// 11. MainlyConstant<Dictionary> + AlwaysTrue + dense (FAST path, composite)
// ---------------------------------------------------------------------------
TEST_P(
    ReadWithVisitorTest,
    encodingLevel_MainlyConstant_Dictionary_AlwaysTrue_Dense) {
  constexpr int kRows = 500;

  // Mostly 42; non-common values cycle through {100, 101, 102}.
  std::vector<int64_t> data(kRows, 42);
  for (int i = 0; i < kRows; i += 10) {
    data[i] = 100 + (i / 10) % 3;
  }

  auto input = makeRowVector({makeFlatVector<int64_t>(data)});
  auto rowType = asRowType(input->type());
  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());
  auto root = buildReader(*ctx, rowType, *scanSpec);

  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  std::vector<vector_size_t> rowVec(kRows);
  std::iota(rowVec.begin(), rowVec.end(), 0);
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  Buffer buffer(*pool());
  auto encoding = createFromCustomLayout<int64_t>(
      MainlyConstantEnc{.otherValues = DictionaryEnc{}}, data, *pool(), buffer);

  common::AlwaysTrue filter;
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = true;
  DecoderVisitor<
      int64_t,
      common::AlwaysTrue,
      dwio::common::ExtractToReader,
      kIsDense>
      visitor(filter, reader, rows, extractValues);
  auto params = makeReadWithVisitorParams(visitor, rows, pool());

  dispatchCallReadWithVisitor(*encoding, visitor, params);

  EXPECT_EQ(reader->numValues(), kRows);
  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(values[i], data[i]) << "row " << i;
  }
}

// ---------------------------------------------------------------------------
// 12. MainlyConstant<Dictionary> + AlwaysTrue + dense + SLOW PATH (composite)
// ---------------------------------------------------------------------------
TEST_P(
    ReadWithVisitorTest,
    encodingLevel_MainlyConstant_Dictionary_AlwaysTrue_Dense_SlowPath) {
  constexpr int kRows = 500;

  std::vector<int64_t> data(kRows, 42);
  for (int i = 0; i < kRows; i += 10) {
    data[i] = 100 + (i / 10) % 3;
  }

  auto input = makeRowVector({makeFlatVector<int64_t>(data)});
  auto rowType = asRowType(input->type());
  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());
  auto root = buildReader(*ctx, rowType, *scanSpec);

  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  std::vector<vector_size_t> rowVec(kRows);
  std::iota(rowVec.begin(), rowVec.end(), 0);
  RowSet rows(rowVec.data(), rowVec.size());

  reader->doPrepareRead<int64_t>(0, rows, nullptr);

  Buffer buffer(*pool());
  auto encoding = createFromCustomLayout<int64_t>(
      MainlyConstantEnc{.otherValues = DictionaryEnc{}}, data, *pool(), buffer);

  common::AlwaysTrue filter;
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = true;
  constexpr bool kHasBulkPath = false;
  velox::dwio::common::ColumnVisitor<
      int64_t,
      common::AlwaysTrue,
      dwio::common::ExtractToReader,
      kIsDense,
      kHasBulkPath>
      visitor(filter, reader, rows, extractValues);
  auto params = makeReadWithVisitorParams(visitor, rows, pool());

  dispatchCallReadWithVisitor(*encoding, visitor, params);

  EXPECT_EQ(reader->numValues(), kRows);
  auto values = getValues<int64_t>(reader);
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(values[i], data[i]) << "row " << i;
  }
}

// ---------------------------------------------------------------------------
// 13. FixedBitWidthEncoding<int32_t> + AlwaysTrue + dense + FAST PATH
//     The FixedBitWidth fast path (bulkScan) requires:
//       - isFourByteIntegralType (int32_t/uint32_t only, NOT int64_t)
//       - No filter (!kHasFilter → AlwaysTrue)
//       - No hook (!kHasHook → ExtractToReader)
//       - kExtractToReader && (kSameType || kIsWidening)
//     This test uses int32_t data to satisfy all conditions.
// ---------------------------------------------------------------------------
TEST_P(
    ReadWithVisitorTest,
    encodingLevel_FixedBitWidth_AlwaysTrue_Dense_Int32_FastPath) {
  constexpr int kRows = 500;

  // Small-range int32 data that fits in fixed-bit-width encoding.
  std::vector<int32_t> data(kRows);
  for (int i = 0; i < kRows; ++i) {
    data[i] = i % 16;
  }

  // Write a nimble file with an int32 column (INTEGER type).
  auto input = makeRowVector({makeFlatVector<int32_t>(
      kRows, [](auto i) { return static_cast<int32_t>(i % 16); })});
  auto rowType = asRowType(input->type());
  auto ctx = makeFileContext(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*rowType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::AlwaysTrue>());
  auto root = buildReader(*ctx, rowType, *scanSpec);

  auto* structReader =
      dynamic_cast<dwio::common::SelectiveStructColumnReaderBase*>(root.get());
  auto* reader = static_cast<IntegerColumnReaderTestAccessor*>(
      dynamic_cast<IntegerColumnReader*>(structReader->children()[0]));
  ASSERT_NE(reader, nullptr);

  std::vector<vector_size_t> rowVec(kRows);
  std::iota(rowVec.begin(), rowVec.end(), 0);
  RowSet rows(rowVec.data(), rowVec.size());

  // prepareRead with int32_t — must match the column's physical type.
  reader->doPrepareRead<int32_t>(0, rows, nullptr);

  Buffer buffer(*pool());
  auto encoding = createFromCustomLayout<int32_t>(
      FixedBitWidthEnc{}, data, *pool(), buffer);

  // Visitor DataType = int32_t → dispatch matches
  // FixedBitWidthEncoding<int32_t>. AlwaysTrue + ExtractToReader +
  // hasBulkPath=true (default) → fast path.
  common::AlwaysTrue filter;
  dwio::common::ExtractToReader extractValues(reader);
  constexpr bool kIsDense = true;
  DecoderVisitor<
      int32_t,
      common::AlwaysTrue,
      dwio::common::ExtractToReader,
      kIsDense>
      visitor(filter, reader, rows, extractValues);
  auto params = makeReadWithVisitorParams(visitor, rows, pool());

  dispatchCallReadWithVisitor(*encoding, visitor, params);

  EXPECT_EQ(reader->numValues(), kRows);
  auto values = getValues<int32_t>(reader);
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(values[i], i % 16) << "row " << i;
  }
}

INSTANTIATE_TEST_SUITE_P(
    NonLegacyAndLegacy,
    ReadWithVisitorTest,
    ::testing::Values(true, false),
    [](const ::testing::TestParamInfo<bool>& info) {
      return info.param ? "NonLegacy" : "Legacy";
    });

} // namespace
} // namespace facebook::nimble
