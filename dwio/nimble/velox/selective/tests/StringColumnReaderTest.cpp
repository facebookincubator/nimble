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

#include "dwio/nimble/velox/selective/SelectiveNimbleReader.h"

#include "dwio/nimble/common/tests/NimbleFileWriter.h"
#include "dwio/nimble/velox/FlushPolicy.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace facebook::nimble {
namespace {

using namespace facebook::velox;

// Tests for StringColumnReader covering VARCHAR and VARBINARY edge cases.
class StringColumnReaderTest : public ::testing::Test,
                               public velox::test::VectorTestBase,
                               public ::testing::WithParamInterface<bool> {
 protected:
  static void SetUpTestCase() {
    memory::initializeMemoryManager(velox::memory::MemoryManager::Options{});
    common::testutil::TestValue::enable();
    registerSelectiveNimbleReaderFactory();
  }

  static void TearDownTestCase() {
    unregisterSelectiveNimbleReaderFactory();
  }

  memory::MemoryPool* rootPool() {
    return rootPool_.get();
  }

  struct Readers {
    std::unique_ptr<dwio::common::Reader> reader;
    std::unique_ptr<dwio::common::RowReader> rowReader;
  };

  Readers makeReaders(
      const RowVectorPtr& expected,
      const std::string& file,
      const std::shared_ptr<common::ScanSpec>& scanSpec,
      bool stringDecoderZeroCopy) {
    auto readFile = std::make_shared<InMemoryReadFile>(file);
    auto factory =
        dwio::common::getReaderFactory(dwio::common::FileFormat::NIMBLE);
    dwio::common::ReaderOptions options(
        pool(), dataIoStats_.get(), metadataIoStats_.get());
    options.setScanSpec(scanSpec);
    Readers readers;
    readers.reader = factory->createReader(
        std::make_unique<dwio::common::BufferedInput>(readFile, *pool()),
        options);
    auto type = asRowType(expected->type());
    dwio::common::RowReaderOptions rowOptions;
    rowOptions.setScanSpec(scanSpec);
    rowOptions.setRequestedType(type);
    rowOptions.setStringDecoderZeroCopy(stringDecoderZeroCopy);
    readers.rowReader = readers.reader->createRowReader(rowOptions);
    return readers;
  }

  Readers makeReaders(
      const RowVectorPtr& input,
      const std::shared_ptr<common::ScanSpec>& scanSpec,
      bool stringDecoderZeroCopy) {
    return makeReaders(
        input,
        test::createNimbleFile(*rootPool(), input),
        scanSpec,
        stringDecoderZeroCopy);
  }

  void validate(
      const RowVector& expected,
      dwio::common::RowReader& rowReader,
      int batchSize) {
    auto result = BaseVector::create(asRowType(expected.type()), 0, pool());
    int numScanned = 0;
    int offset = 0;
    while (numScanned < expected.size()) {
      numScanned += rowReader.next(batchSize, result);
      result->validate();
      for (int j = 0; j < result->size(); ++j) {
        ASSERT_TRUE(result->equalValueAt(&expected, j, offset + j))
            << "Mismatch at row " << (offset + j);
      }
      offset += result->size();
    }
    ASSERT_EQ(numScanned, expected.size());
    ASSERT_EQ(0, rowReader.next(1, result));
  }

  void validateWithFilter(
      const RowVector& input,
      dwio::common::RowReader& rowReader,
      int batchSize,
      const std::function<bool(int)>& filter) {
    auto result = BaseVector::create(asRowType(input.type()), 0, pool());
    int numScanned = 0;
    int i = 0;
    while (numScanned < input.size()) {
      numScanned += rowReader.next(batchSize, result);
      result->validate();
      for (int j = 0; j < result->size(); ++j) {
        for (;;) {
          ASSERT_LT(i, input.size());
          if (filter(i)) {
            break;
          }
          ++i;
        }
        ASSERT_TRUE(result->equalValueAt(&input, j, i))
            << "Mismatch at input row " << i;
        ++i;
      }
    }
    while (i < input.size()) {
      ASSERT_FALSE(filter(i));
      ++i;
    }
    ASSERT_EQ(numScanned, input.size());
    ASSERT_EQ(0, rowReader.next(1, result));
  }

  const std::shared_ptr<io::IoStatistics> dataIoStats_{
      std::make_shared<io::IoStatistics>()};
  const std::shared_ptr<io::IoStatistics> metadataIoStats_{
      std::make_shared<io::IoStatistics>()};
};

// Returns the encoding of a vector, loading through lazy wrappers if present.
VectorEncoding::Simple getEncoding(const VectorPtr& vector) {
  return BaseVector::loadedVectorShared(vector)->encoding();
}

// Validates correctness and checks the expected encoding per batch.
// expectedEncodings[i] is the expected encoding for the i-th non-empty batch.
// totalRows is the total number of rows in the file (used to drive the read
// loop). expected is the expected string column values (only qualifying rows).
// childIndex selects which child of the result RowVector to check.
void validateWithEncodingChecks(
    const BaseVector& expected,
    dwio::common::RowReader& rowReader,
    int batchSize,
    int totalRows,
    const std::vector<VectorEncoding::Simple>& expectedEncodings,
    const RowTypePtr& resultType,
    velox::memory::MemoryPool* pool,
    int childIndex = 0) {
  auto result = BaseVector::create(resultType, 0, pool);
  int numScanned = 0;
  int outputOffset = 0;
  int batchIndex = 0;
  while (numScanned < totalRows) {
    numScanned += rowReader.next(batchSize, result);
    result->validate();
    if (result->size() == 0) {
      continue;
    }
    auto* row = result->asUnchecked<RowVector>();
    auto stringChild = BaseVector::loadedVectorShared(row->childAt(childIndex));
    ASSERT_LT(batchIndex, expectedEncodings.size());
    EXPECT_EQ(stringChild->encoding(), expectedEncodings[batchIndex])
        << "Non-empty batch " << batchIndex << " at output offset "
        << outputOffset << " has unexpected encoding";
    for (int j = 0; j < result->size(); ++j) {
      ASSERT_TRUE(stringChild->equalValueAt(&expected, j, outputOffset + j))
          << "Mismatch at row " << (outputOffset + j);
    }
    outputOffset += result->size();
    ++batchIndex;
  }
  ASSERT_EQ(outputOffset, expected.size());
  ASSERT_EQ(0, rowReader.next(1, result));
}

// Strings shorter than StringView::kInlineSize (12 bytes).
TEST_P(StringColumnReaderTest, shortStrings) {
  const bool stringDecoderZeroCopy = GetParam();
  auto input = makeRowVector({
      makeFlatVector<std::string>(
          100, [](auto i) { return std::to_string(i); }),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec, stringDecoderZeroCopy);
  validate(*input, *readers.rowReader, 23);
}

// Strings longer than inline size.
TEST_P(StringColumnReaderTest, longStrings) {
  const bool stringDecoderZeroCopy = GetParam();
  const std::string prefix(20, 'x');
  auto input = makeRowVector({
      makeFlatVector<std::string>(
          50, [&](auto i) { return prefix + std::to_string(i); }),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec, stringDecoderZeroCopy);
  validate(*input, *readers.rowReader, 13);
}

// Alternating short and long strings.
TEST_P(StringColumnReaderTest, mixedLengthStrings) {
  const bool stringDecoderZeroCopy = GetParam();
  const std::string longPrefix(20, 'y');
  auto input = makeRowVector({
      makeFlatVector<std::string>(
          100,
          [&](auto i) {
            return i % 2 == 0 ? std::to_string(i)
                              : longPrefix + std::to_string(i);
          }),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec, stringDecoderZeroCopy);
  validate(*input, *readers.rowReader, 17);
}

// Strings with null values.
TEST_P(StringColumnReaderTest, stringsWithNulls) {
  const bool stringDecoderZeroCopy = GetParam();
  auto input = makeRowVector({
      makeFlatVector<std::string>(
          100, [](auto i) { return "str_" + std::to_string(i); }, nullEvery(5)),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec, stringDecoderZeroCopy);
  validate(*input, *readers.rowReader, 11);
}

// All-null string column.
TEST_P(StringColumnReaderTest, allNullStrings) {
  const bool stringDecoderZeroCopy = GetParam();
  auto input = makeRowVector({
      makeConstant<StringView>(std::nullopt, 50),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec, stringDecoderZeroCopy);
  auto result = BaseVector::create(input->type(), 0, pool());
  ASSERT_EQ(readers.rowReader->next(50, result), 50);
  auto* row = result->asUnchecked<RowVector>();
  for (int i = 0; i < 50; ++i) {
    ASSERT_TRUE(row->childAt(0)->isNullAt(i));
  }
}

// All empty "" strings.
TEST_P(StringColumnReaderTest, emptyStrings) {
  const bool stringDecoderZeroCopy = GetParam();
  auto input = makeRowVector({
      makeFlatVector<std::string>(50, [](auto) { return ""; }),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec, stringDecoderZeroCopy);
  validate(*input, *readers.rowReader, 50);
}

// VARBINARY type (shares reader with VARCHAR).
TEST_P(StringColumnReaderTest, varbinaryColumn) {
  const bool stringDecoderZeroCopy = GetParam();
  auto input = makeRowVector({
      makeFlatVector<std::string>(
          50,
          [](auto i) {
            std::string s(4, '\0');
            s[0] = static_cast<char>(i);
            s[1] = static_cast<char>(i + 1);
            s[2] = static_cast<char>(i + 2);
            s[3] = static_cast<char>(i + 3);
            return std::move(s);
          }),
  });
  // We write as VARCHAR but the byte content round-trips the same way.
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec, stringDecoderZeroCopy);
  validate(*input, *readers.rowReader, 50);
}

// BytesValues filter on string column.
TEST_P(StringColumnReaderTest, stringBytesFilter) {
  const bool stringDecoderZeroCopy = GetParam();
  auto c0 = makeFlatVector<std::string>(
      100, [](auto i) { return "val_" + std::to_string(i % 10); });
  auto input = makeRowVector({c0});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BytesValues>(
          std::vector<std::string>{"val_3", "val_7"}, false));
  auto readers = makeReaders(input, scanSpec, stringDecoderZeroCopy);
  validateWithFilter(*input, *readers.rowReader, 23, [](auto i) {
    return i % 10 == 3 || i % 10 == 7;
  });
}

// BytesRange filter on string column.
TEST_P(StringColumnReaderTest, stringBytesRangeFilter) {
  const bool stringDecoderZeroCopy = GetParam();
  auto c0 = makeFlatVector<std::string>(
      100, [](auto i) { return "val_" + std::to_string(i % 10); });
  auto input = makeRowVector({c0});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BytesRange>(
          "val_3", false, false, "val_5", false, false, false));
  auto readers = makeReaders(input, scanSpec, stringDecoderZeroCopy);
  validateWithFilter(*input, *readers.rowReader, 23, [](auto i) {
    auto val = "val_" + std::to_string(i % 10);
    return val >= "val_3" && val <= "val_5";
  });
}

// Small batch sizes to exercise skip positioning.
TEST_P(StringColumnReaderTest, stringSkipAndRead) {
  const bool stringDecoderZeroCopy = GetParam();
  const std::string longPrefix(15, 'z');
  auto input = makeRowVector({
      makeFlatVector<std::string>(
          100,
          [&](auto i) {
            return i % 3 == 0 ? std::to_string(i)
                              : longPrefix + std::to_string(i);
          }),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec, stringDecoderZeroCopy);
  validate(*input, *readers.rowReader, 7);
}

// Verifies that dictionary state is correctly invalidated when a decoder-level
// skip crosses a chunk boundary. Uses a two-column schema where a filter on c0
// rejects all rows in chunk 1, causing c1 (string, lazy-loaded) to remain
// unread during batch 1. When batch 2 loads c1 via ColumnLoader, seekTo
// triggers skip(500) across the chunk boundary.
TEST_P(StringColumnReaderTest, skipAcrossChunkBoundary) {
  const bool passStringBuffersFromDecoder = GetParam();
  if (!passStringBuffersFromDecoder) {
    return;
  }

  constexpr int kChunkRows = 500;

  // Chunk 1: c0 = [0..499], c1 = {"aaa","bbb","ccc"} repeating.
  auto chunk1 = makeRowVector({
      makeFlatVector<int64_t>(kChunkRows, [](auto i) { return i; }),
      makeFlatVector<std::string>(
          kChunkRows,
          [](auto i) {
            return std::vector<std::string>{"aaa", "bbb", "ccc"}[i % 3];
          }),
  });
  // Chunk 2: c0 = [500..999], c1 = {"xxx","yyy"} repeating.
  auto chunk2 = makeRowVector({
      makeFlatVector<int64_t>(
          kChunkRows, [](auto i) { return kChunkRows + i; }),
      makeFlatVector<std::string>(
          kChunkRows,
          [](auto i) { return std::vector<std::string>{"xxx", "yyy"}[i % 2]; }),
  });

  VeloxWriterOptions writerOptions;
  writerOptions.enableChunking = true;
  writerOptions.minStreamChunkRawSize = 0;
  writerOptions.flushPolicyFactory = [] {
    return std::make_unique<LambdaFlushPolicy>(
        /*flushLambda=*/[](const StripeProgress&) { return false; },
        /*chunkLambda=*/[](const StripeProgress&) { return true; });
  };
  auto file = test::createNimbleFile(
      *rootPool(), {chunk1, chunk2}, writerOptions, /*flushAfterWrite=*/false);

  // Filter on c0 rejects chunk 1 ([0,499]), accepts chunk 2 ([500,999]).
  auto readType = ROW({"c0", "c1"}, {BIGINT(), VARCHAR()});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*readType);
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintRange>(
          kChunkRows, 2 * kChunkRows - 1, /*nullAllowed=*/false));

  auto expectedStrings = makeFlatVector<std::string>(kChunkRows, [](auto i) {
    return std::vector<std::string>{"xxx", "yyy"}[i % 2];
  });

  auto readers =
      makeReaders(chunk1, file, scanSpec, passStringBuffersFromDecoder);

  // Batch 1 (rows 0-499): c0 filter rejects all → c1 lazy, not loaded.
  // Batch 2 (rows 500-999): c1 loaded via ColumnLoader → seekTo(500) →
  //   skip(500) across chunk boundary → flat fallback.
  using E = VectorEncoding::Simple;
  validateWithEncodingChecks(
      *expectedStrings,
      *readers.rowReader,
      kChunkRows,
      /*totalRows=*/2 * kChunkRows,
      {E::FLAT},
      readType,
      pool(),
      /*childIndex=*/1);
}

// Verifies skip and dictionary behavior when the filter creates a gap in the
// middle of the file. The filter accepts rows [0,249] and [600,900], creating
// a skip that crosses the chunk boundary (at row 500) mid-file.
//
// Batch layout (batch size 250, 1000 total rows, chunk boundary at 500):
//   Batch 0 (offset=0):   c0 accepts 0-249, c1 loaded at readOffset=0 → dict
//   Batch 1 (offset=250): c0 rejects all    → c1 lazy, not loaded
//   Batch 2 (offset=500): c0 accepts 600-749, c1 loaded, readOffset=250 →
//     seekTo(500), skip(250) crosses chunk boundary → flat
//   Batch 3 (offset=750): c0 accepts 750-900, readOffset=750 → dict
TEST_P(StringColumnReaderTest, skipAcrossChunkBoundaryMidFile) {
  const bool passStringBuffersFromDecoder = GetParam();
  if (!passStringBuffersFromDecoder) {
    return;
  }

  constexpr int kChunkRows = 500;
  constexpr int kBatchSize = 250;

  auto chunk1 = makeRowVector({
      makeFlatVector<int64_t>(kChunkRows, [](auto i) { return i; }),
      makeFlatVector<std::string>(
          kChunkRows,
          [](auto i) {
            return std::vector<std::string>{"aaa", "bbb", "ccc"}[i % 3];
          }),
  });
  auto chunk2 = makeRowVector({
      makeFlatVector<int64_t>(
          kChunkRows, [](auto i) { return kChunkRows + i; }),
      makeFlatVector<std::string>(
          kChunkRows,
          [](auto i) { return std::vector<std::string>{"xxx", "yyy"}[i % 2]; }),
  });

  VeloxWriterOptions writerOptions;
  writerOptions.enableChunking = true;
  writerOptions.minStreamChunkRawSize = 0;
  writerOptions.flushPolicyFactory = [] {
    return std::make_unique<LambdaFlushPolicy>(
        /*flushLambda=*/[](const StripeProgress&) { return false; },
        /*chunkLambda=*/[](const StripeProgress&) { return true; });
  };
  auto file = test::createNimbleFile(
      *rootPool(), {chunk1, chunk2}, writerOptions, /*flushAfterWrite=*/false);

  // Filter: accept [0, 249] ∪ [600, 900].
  auto readType = ROW({"c0", "c1"}, {BIGINT(), VARCHAR()});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*readType);
  std::vector<std::unique_ptr<common::BigintRange>> ranges;
  ranges.push_back(
      std::make_unique<common::BigintRange>(
          0, kBatchSize - 1, /*nullAllowed=*/false));
  ranges.push_back(
      std::make_unique<common::BigintRange>(600, 900, /*nullAllowed=*/false));
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintMultiRange>(
          std::move(ranges), /*nullAllowed=*/false));

  // Expected string output: rows 0-249 from chunk 1, rows 600-900 from chunk 2.
  // 250 + 150 + 151 = 551 total qualifying rows.
  auto expectedStrings = makeFlatVector<std::string>(551, [](auto i) {
    if (i < 250) {
      return std::vector<std::string>{"aaa", "bbb", "ccc"}[i % 3];
    }
    // Rows 600-900 in the file → offset 100-400 within chunk 2.
    int chunk2Offset = (i - 250) + 100;
    return std::vector<std::string>{"xxx", "yyy"}[chunk2Offset % 2];
  });

  auto readers =
      makeReaders(chunk1, file, scanSpec, passStringBuffersFromDecoder);

  using E = VectorEncoding::Simple;
  validateWithEncodingChecks(
      *expectedStrings,
      *readers.rowReader,
      kBatchSize,
      /*totalRows=*/2 * kChunkRows,
      {E::DICTIONARY, E::FLAT, E::DICTIONARY},
      readType,
      pool(),
      /*childIndex=*/1);
}

// Verifies that the dictionary path is entered when a skip stays within the
// same chunk. Uses a single large chunk so there is no chunk boundary. A filter
// on c0 rejects batch 0, causing c1 to be lazy. When batch 1 loads c1, seekTo
// skips 500 values — but all within the same 1000-value chunk. The tighter
// remainingValues gate (offset + numRequestedRows - readOffset_ <=
// remainingValues) allows the dict path here, unlike the conservative
// readOffset_ != offset gate which would bail.
//
// The filter also creates gaps (accepts only even rows) so the first dictionary
// batch exercises the visitor's gap-skipping logic.
TEST_P(StringColumnReaderTest, skipWithinChunkReturnsDictionary) {
  const bool passStringBuffersFromDecoder = GetParam();
  if (!passStringBuffersFromDecoder) {
    return;
  }

  constexpr int kChunkRows = 1000;
  constexpr int kBatchSize = 500;

  auto input = makeRowVector({
      makeFlatVector<int64_t>(kChunkRows, [](auto i) { return i; }),
      makeFlatVector<std::string>(
          kChunkRows,
          [](auto i) {
            return std::vector<std::string>{"aaa", "bbb", "ccc"}[i % 3];
          }),
  });

  VeloxWriterOptions writerOptions;
  writerOptions.enableChunking = true;
  writerOptions.minStreamChunkRawSize = 0;
  writerOptions.flushPolicyFactory = [] {
    return std::make_unique<LambdaFlushPolicy>(
        /*flushLambda=*/[](const StripeProgress&) { return false; },
        /*chunkLambda=*/[](const StripeProgress&) { return false; });
  };
  auto file = test::createNimbleFile(*rootPool(), input, writerOptions);

  // Filter: accept [600, 700] ∪ [800, 900]. Rejects all of batch 0 and
  // creates a gap (rows 701-799) in batch 1's qualifying rows.
  auto readType = ROW({"c0", "c1"}, {BIGINT(), VARCHAR()});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*readType);
  std::vector<std::unique_ptr<common::BigintRange>> ranges;
  ranges.push_back(
      std::make_unique<common::BigintRange>(600, 700, /*nullAllowed=*/false));
  ranges.push_back(
      std::make_unique<common::BigintRange>(800, 900, /*nullAllowed=*/false));
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintMultiRange>(
          std::move(ranges), /*nullAllowed=*/false));

  // 101 rows from [600,700] + 101 rows from [800,900] = 202 qualifying rows.
  auto expectedStrings = makeFlatVector<std::string>(202, [](auto i) {
    int row = i < 101 ? 600 + i : 800 + (i - 101);
    return std::vector<std::string>{"aaa", "bbb", "ccc"}[row % 3];
  });

  auto readers =
      makeReaders(input, file, scanSpec, passStringBuffersFromDecoder);

  // Batch 0 (offset=0): c0 rejects all → c1 lazy, readOffset stays 0.
  // Batch 1 (offset=500): c0 accepts [600,700] ∪ [800,900] → c1 loaded via
  //   ColumnLoader, readOffset=0, offset=500. Skip 500 within single
  //   1000-value chunk. remainingValues (1000) >= 500 + 901 - 0 = 1401? No...
  //   rows = outputRows relative to offset 500: [100..200, 300..400].
  //   numRequestedRows = 401. totalValuesNeeded = 500 + 401 = 901 <= 1000.
  //   Dict path with gapped rows.
  using E = VectorEncoding::Simple;
  validateWithEncodingChecks(
      *expectedStrings,
      *readers.rowReader,
      kBatchSize,
      /*totalRows=*/kChunkRows,
      {E::DICTIONARY},
      readType,
      pool(),
      /*childIndex=*/1);
}

// Verifies that the dictionary path recovers after a flat fallback mid-file.
// Two chunks of 400 rows, batch size 250, filter accepts [0,349] ∪ [600,799].
//
//   Batch 0 (offset=0):   c0 accepts [0,249]. c1 readOffset=0, no skip.
//     remainingValues=400 >= 250. DICT. readOffset → 250.
//   Batch 1 (offset=250): c0 accepts [250,349], outputRows=[0..99].
//     readOffset=250, no skip. remainingValues=150 >= 100. DICT.
//     readOffset → 350.
//   Batch 2 (offset=500): c0 accepts [600,749], outputRows=[100..249].
//     readOffset=350, skip=150. totalNeeded=150+250=400 > remaining=50.
//     Skip crosses chunk boundary → FLAT. readOffset → 750.
//   Batch 3 (offset=750): c0 accepts [750,799], outputRows=[0..49].
//     readOffset=750, no skip. remainingValues=50 >= 50. DICT.
TEST_P(StringColumnReaderTest, skipCrossesChunkButNextBatchRecoversDictionary) {
  const bool passStringBuffersFromDecoder = GetParam();
  if (!passStringBuffersFromDecoder) {
    return;
  }

  constexpr int kChunkRows = 400;
  constexpr int kBatchSize = 250;

  auto chunk1 = makeRowVector({
      makeFlatVector<int64_t>(kChunkRows, [](auto i) { return i; }),
      makeFlatVector<std::string>(
          kChunkRows,
          [](auto i) {
            return std::vector<std::string>{"aaa", "bbb", "ccc"}[i % 3];
          }),
  });
  auto chunk2 = makeRowVector({
      makeFlatVector<int64_t>(
          kChunkRows, [](auto i) { return kChunkRows + i; }),
      makeFlatVector<std::string>(
          kChunkRows,
          [](auto i) { return std::vector<std::string>{"xxx", "yyy"}[i % 2]; }),
  });

  VeloxWriterOptions writerOptions;
  writerOptions.enableChunking = true;
  writerOptions.minStreamChunkRawSize = 0;
  writerOptions.flushPolicyFactory = [] {
    return std::make_unique<LambdaFlushPolicy>(
        /*flushLambda=*/[](const StripeProgress&) { return false; },
        /*chunkLambda=*/[](const StripeProgress&) { return true; });
  };
  auto file = test::createNimbleFile(
      *rootPool(), {chunk1, chunk2}, writerOptions, /*flushAfterWrite=*/false);

  auto readType = ROW({"c0", "c1"}, {BIGINT(), VARCHAR()});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*readType);
  std::vector<std::unique_ptr<common::BigintRange>> ranges;
  ranges.push_back(
      std::make_unique<common::BigintRange>(0, 349, /*nullAllowed=*/false));
  ranges.push_back(
      std::make_unique<common::BigintRange>(
          600, 2 * kChunkRows - 1, /*nullAllowed=*/false));
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintMultiRange>(
          std::move(ranges), /*nullAllowed=*/false));

  // 350 rows from chunk 1 (rows 0-349) + 200 rows from chunk 2 (rows 600-799).
  auto expectedStrings = makeFlatVector<std::string>(550, [](auto i) {
    if (i < 350) {
      return std::vector<std::string>{"aaa", "bbb", "ccc"}[i % 3];
    }
    int chunk2Offset = (i - 350) + 200;
    return std::vector<std::string>{"xxx", "yyy"}[chunk2Offset % 2];
  });

  auto readers =
      makeReaders(chunk1, file, scanSpec, passStringBuffersFromDecoder);

  using E = VectorEncoding::Simple;
  validateWithEncodingChecks(
      *expectedStrings,
      *readers.rowReader,
      kBatchSize,
      /*totalRows=*/2 * kChunkRows,
      {E::DICTIONARY, E::DICTIONARY, E::FLAT, E::DICTIONARY},
      readType,
      pool(),
      /*childIndex=*/1);
}

// Verifies dictionary recovery after a chunk boundary crossing. The decoder
// fully consumes chunk 1 in batches 0-1, then batch 2 is lazy (c1 not
// loaded). When batch 3 loads c1, ensureLoaded() advances to chunk 2 (firing
// onChunkLoad to clear stale state), then ensureDictionaryState() rebuilds
// the alphabet from chunk 2. The skip stays within chunk 2.
TEST_P(StringColumnReaderTest, skipAcrossChunkBoundaryDictRecovery) {
  const bool passStringBuffersFromDecoder = GetParam();
  if (!passStringBuffersFromDecoder) {
    return;
  }

  constexpr int kChunkRows = 500;
  constexpr int kBatchSize = 250;

  auto chunk1 = makeRowVector({
      makeFlatVector<int64_t>(kChunkRows, [](auto i) { return i; }),
      makeFlatVector<std::string>(
          kChunkRows,
          [](auto i) {
            return std::vector<std::string>{"aaa", "bbb", "ccc"}[i % 3];
          }),
  });
  auto chunk2 = makeRowVector({
      makeFlatVector<int64_t>(
          kChunkRows, [](auto i) { return kChunkRows + i; }),
      makeFlatVector<std::string>(
          kChunkRows,
          [](auto i) { return std::vector<std::string>{"xxx", "yyy"}[i % 2]; }),
  });

  VeloxWriterOptions writerOptions;
  writerOptions.enableChunking = true;
  writerOptions.minStreamChunkRawSize = 0;
  writerOptions.flushPolicyFactory = [] {
    return std::make_unique<LambdaFlushPolicy>(
        /*flushLambda=*/[](const StripeProgress&) { return false; },
        /*chunkLambda=*/[](const StripeProgress&) { return true; });
  };
  auto file = test::createNimbleFile(
      *rootPool(), {chunk1, chunk2}, writerOptions, /*flushAfterWrite=*/false);

  // Filter: accept [0, 499] and [750, 999]. Rejects batch 2 (rows 500-749).
  auto readType = ROW({"c0", "c1"}, {BIGINT(), VARCHAR()});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*readType);
  std::vector<std::unique_ptr<common::BigintRange>> ranges;
  ranges.push_back(
      std::make_unique<common::BigintRange>(
          0, kChunkRows - 1, /*nullAllowed=*/false));
  ranges.push_back(
      std::make_unique<common::BigintRange>(
          750, 2 * kChunkRows - 1, /*nullAllowed=*/false));
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintMultiRange>(
          std::move(ranges), /*nullAllowed=*/false));

  // Expected: 500 rows from chunk 1 + 250 rows from chunk 2 (rows 750-999).
  auto expectedStrings = makeFlatVector<std::string>(750, [](auto i) {
    if (i < 500) {
      return std::vector<std::string>{"aaa", "bbb", "ccc"}[i % 3];
    }
    int chunk2Offset = (i - 500) + 250;
    return std::vector<std::string>{"xxx", "yyy"}[chunk2Offset % 2];
  });

  auto readers =
      makeReaders(chunk1, file, scanSpec, passStringBuffersFromDecoder);

  // Batch 0 (offset=0, rows 0-249): chunk 1, 500 remaining >= 250 → dict
  // Batch 1 (offset=250, rows 250-499): chunk 1, 250 remaining >= 250 → dict
  //   After this batch, decoder has consumed all 500 values in chunk 1.
  // Batch 2 (offset=500, rows 500-749): c0 rejects all → c1 lazy, not loaded.
  //   readOffset stays at 500, decoder remainingValues=0.
  // Batch 3 (offset=750, rows 750-999): c1 loaded via ColumnLoader.
  //   ensureLoaded() loads chunk 2 (remainingValues=500). readOffset=500,
  //   offset=750, skip=250 within chunk 2. 500 >= 250+250=500 → dict.
  using E = VectorEncoding::Simple;
  validateWithEncodingChecks(
      *expectedStrings,
      *readers.rowReader,
      kBatchSize,
      /*totalRows=*/2 * kChunkRows,
      {E::DICTIONARY, E::DICTIONARY, E::DICTIONARY},
      readType,
      pool(),
      /*childIndex=*/1);
}

// Verifies that the remainingValues gate
// forces a flat-path fallback mid-file. With 2 chunks of 300 rows and batch
// size 200:
//   Read 1 (rows 0-199): chunk 1 has 300 remaining >= 200, dict path taken,
//     alphabet built from chunk 1.
//   Read 2 (rows 200-399): chunk 1 has only 100 remaining < 200, falls to flat
//     path which must clear dictionary state.
//   Read 3 (rows 400-599): chunk 2 has enough rows, dict path re-entered.
//     Must rebuild alphabet from chunk 2, not reuse stale chunk 1 alphabet.
TEST_P(StringColumnReaderTest, readAcrossChunkBoundary) {
  const bool passStringBuffersFromDecoder = GetParam();
  if (!passStringBuffersFromDecoder) {
    return;
  }

  auto chunk1 = makeRowVector({makeFlatVector<std::string>(300, [](auto i) {
    return std::vector<std::string>{"aaa", "bbb"}[i % 2];
  })});
  auto chunk2 = makeRowVector({makeFlatVector<std::string>(300, [](auto i) {
    return std::vector<std::string>{"xxx", "yyy"}[i % 2];
  })});

  VeloxWriterOptions writerOptions;
  writerOptions.enableChunking = true;
  writerOptions.minStreamChunkRawSize = 0;
  writerOptions.flushPolicyFactory = [] {
    return std::make_unique<LambdaFlushPolicy>(
        /*flushLambda=*/[](const StripeProgress&) { return false; },
        /*chunkLambda=*/[](const StripeProgress&) { return true; });
  };
  auto file = test::createNimbleFile(
      *rootPool(), {chunk1, chunk2}, writerOptions, /*flushAfterWrite=*/false);

  auto expected = makeRowVector({makeFlatVector<std::string>(600, [](auto i) {
    if (i < 300) {
      return std::vector<std::string>{"aaa", "bbb"}[i % 2];
    }
    return std::vector<std::string>{"xxx", "yyy"}[(i - 300) % 2];
  })});

  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*expected->type());
  auto readers =
      makeReaders(expected, file, scanSpec, passStringBuffersFromDecoder);
  // 600 rows, batch_size=200, chunk boundary at 300:
  //   Batch 0 (0-199): 300 remaining >= 200 → dict
  //   Batch 1 (200-399): 100 remaining < 200 → flat (crosses boundary)
  //   Batch 2 (400-599): chunk 2, 300 remaining >= 200 → dict
  using E = VectorEncoding::Simple;
  validateWithEncodingChecks(
      *expected->childAt(0),
      *readers.rowReader,
      200,
      /*totalRows=*/600,
      {E::DICTIONARY, E::FLAT, E::DICTIONARY},
      asRowType(expected->type()),
      pool());
}

// Flatmap string column read via dictionary path.
// When a string column is inside a flatmap, rows where the key is absent have
// inMap=0 (null). The encoding only has data for in-map rows. The dictionary
// path must account for these nulls when reading indices. If kHasNulls is
// ignored in readDictionaryIndicesImpl, the encoding will try to read more
// values than it contains.
TEST_P(StringColumnReaderTest, flatMapStringDictionaryPath) {
  const bool stringDecoderZeroCopy = GetParam();
  if (!stringDecoderZeroCopy) {
    GTEST_SKIP() << "Dictionary path requires stringDecoderZeroCopy";
  }

  // Write map<int8_t, varchar> as a flatmap. Key 1 is present in all rows,
  // key 2 is present in only some rows (creating inMap nulls for key 2).
  constexpr int kRows = 100;
  std::vector<int8_t> allKeys;
  std::vector<std::string> allVals;
  std::vector<vector_size_t> mapOffsets;
  std::vector<vector_size_t> mapSizes;
  for (int i = 0; i < kRows; ++i) {
    mapOffsets.push_back(allKeys.size());
    allKeys.push_back(1);
    allVals.push_back("val_" + std::to_string(i % 5));
    if (i % 3 != 0) {
      allKeys.push_back(2);
      allVals.push_back("key2_" + std::to_string(i % 4));
    }
    mapSizes.push_back(allKeys.size() - mapOffsets.back());
  }
  auto keysFv = makeFlatVector<int8_t>(allKeys);
  auto valsFv = makeFlatVector<std::string>(allVals);
  auto offBuf = allocateOffsets(kRows, pool());
  auto sizBuf = allocateSizes(kRows, pool());
  auto* rawOff = offBuf->asMutable<vector_size_t>();
  auto* rawSiz = sizBuf->asMutable<vector_size_t>();
  for (int i = 0; i < kRows; ++i) {
    rawOff[i] = mapOffsets[i];
    rawSiz[i] = mapSizes[i];
  }
  auto input = makeRowVector({std::make_shared<MapVector>(
      pool(),
      MAP(TINYINT(), VARCHAR()),
      nullptr,
      kRows,
      offBuf,
      sizBuf,
      keysFv,
      valsFv)});

  VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns = {{"c0", {}}};
  auto file = test::createNimbleFile(*rootPool(), input, writerOptions);

  // Read back as struct with flatmap-as-struct projection.
  auto outType = ROW({"c0"}, {ROW({"1", "2"}, {VARCHAR(), VARCHAR()})});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*outType);
  scanSpec->childByName("c0")->setFlatMapAsStruct(true);

  auto readFile = std::make_shared<InMemoryReadFile>(file);
  auto factory =
      dwio::common::getReaderFactory(dwio::common::FileFormat::NIMBLE);
  dwio::common::ReaderOptions options(
      pool(), dataIoStats_.get(), metadataIoStats_.get());
  options.setScanSpec(scanSpec);
  auto reader = factory->createReader(
      std::make_unique<dwio::common::BufferedInput>(readFile, *pool()),
      options);
  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(scanSpec);
  rowOptions.setRequestedType(asRowType(input->type()));
  rowOptions.setStringDecoderZeroCopy(true);
  auto rowReader = reader->createRowReader(rowOptions);

  // Build expected output: key 1 always present, key 2 null when i%3==0.
  auto expected = makeRowVector({makeRowVector(
      {"1", "2"},
      {
          makeFlatVector<std::string>(
              kRows, [](auto i) { return "val_" + std::to_string(i % 5); }),
          makeFlatVector<std::string>(
              kRows,
              [](auto i) { return "key2_" + std::to_string(i % 4); },
              [](auto i) { return i % 3 == 0; }),
      })});

  auto result = BaseVector::create(outType, 0, pool());
  int numScanned = 0;
  int offset = 0;
  while (numScanned < kRows) {
    numScanned += rowReader->next(23, result);
    result->validate();
    for (int j = 0; j < result->size(); ++j) {
      ASSERT_TRUE(result->equalValueAt(expected.get(), j, offset + j))
          << "Mismatch at row " << (offset + j) << ": expected "
          << expected->toString(offset + j) << " got " << result->toString(j);
    }
    offset += result->size();
  }
  ASSERT_EQ(numScanned, kRows);
  ASSERT_EQ(0, rowReader->next(1, result));
}

// Verifies that dictionary state is invalidated via onChunkLoad when a flat
// path read triggers a chunk transition, and the next dict-path read must
// rebuild its alphabet from the new chunk.
//
// Verifies that dictionary state is invalidated via onChunkLoad when
// consecutive reads consume exactly aligned chunks.
//
// Layout: chunk 1 and chunk 2 both have exactly kBatchSize rows.
// With the ensureLoaded() fix (reload when remainingValues_==0):
//   Read 1: ensureLoaded() loads chunk 1, remainingValues==kBatchSize,
//     dict path entered, alphabet built from chunk 1 (2 entries).
//   Read 2: remainingValues==0, ensureLoaded() loads chunk 2 via
//     loadNextChunk() which fires onChunkLoad → clears dictionaryState_.
//     Dict path re-entered, alphabet rebuilt from chunk 2 (3 entries).
// Without onChunkLoad: stale 2-entry alphabet is reused for chunk 2's
// 3-entry dictionary, producing wrong values or index-out-of-bounds.
DEBUG_ONLY_TEST_P(
    StringColumnReaderTest,
    chunkTransitionInvalidatesDictionaryState) {
  const bool stringDecoderZeroCopy = GetParam();
  if (!stringDecoderZeroCopy) {
    GTEST_SKIP() << "Dictionary path requires stringDecoderZeroCopy";
  }

  constexpr int kBatchSize = 500;
  // Chunk 1: 3-entry alphabet {"aaa", "bbb", "ccc"}.
  auto chunk1 =
      makeRowVector({makeFlatVector<std::string>(kBatchSize, [](auto i) {
        return std::vector<std::string>{"aaa", "bbb", "ccc"}[i % 3];
      })});
  // Chunk 2: completely different 5-entry alphabet.
  auto chunk2 =
      makeRowVector({makeFlatVector<std::string>(kBatchSize, [](auto i) {
        return std::vector<std::string>{"pp", "qq", "rr", "ss", "tt"}[i % 5];
      })});

  // Write chunks manually so each write() produces exactly one chunk.
  // Set minStreamChunkRawSize=0 to prevent the chunker from merging small
  // writes into a single chunk.
  VeloxWriterOptions writerOptions;
  writerOptions.enableChunking = true;
  writerOptions.minStreamChunkRawSize = 0;
  writerOptions.flushPolicyFactory = [] {
    return std::make_unique<LambdaFlushPolicy>(
        /*flushLambda=*/[](const StripeProgress&) { return false; },
        /*chunkLambda=*/[](const StripeProgress&) { return true; });
  };
  std::string file;
  {
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    VeloxWriter writer(
        chunk1->type(),
        std::move(writeFile),
        *rootPool(),
        std::move(writerOptions));
    writer.write(chunk1);
    writer.write(chunk2);
    writer.close();
  }

  auto expected =
      makeRowVector({makeFlatVector<std::string>(kBatchSize * 2, [](auto i) {
        if (i < kBatchSize) {
          return std::vector<std::string>{"aaa", "bbb", "ccc"}[i % 3];
        }
        return std::vector<std::string>{
            "pp", "qq", "rr", "ss", "tt"}[(i - kBatchSize) % 5];
      })});

  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*expected->type());
  auto readers = makeReaders(expected, file, scanSpec, stringDecoderZeroCopy);

  int dictPathEntries = 0;
  SCOPED_TESTVALUE_SET(
      "facebook::nimble::StringColumnReader::readWithDictionary",
      std::function<void(const std::vector<std::string_view>*)>(
          [&](const std::vector<std::string_view>* alphabet) {
            std::set<std::string> entries;
            for (const auto& sv : *alphabet) {
              entries.insert(std::string(sv));
            }
            if (dictPathEntries == 0) {
              // Chunk 1 alphabet should contain chunk 1 values only.
              EXPECT_TRUE(entries.count("aaa"));
              EXPECT_FALSE(entries.count("pp"))
                  << "Chunk 1 alphabet contains stale chunk 2 values";
            } else {
              // Chunk 2 alphabet should contain chunk 2 values only.
              EXPECT_TRUE(entries.count("pp"));
              EXPECT_FALSE(entries.count("aaa"))
                  << "Chunk 2 alphabet contains stale chunk 1 values";
            }
            ++dictPathEntries;
          }));

  validateWithEncodingChecks(
      *expected->childAt(0),
      *readers.rowReader,
      kBatchSize,
      /*totalRows=*/2 * kBatchSize,
      {VectorEncoding::Simple::DICTIONARY, VectorEncoding::Simple::DICTIONARY},
      asRowType(expected->type()),
      pool());
  // Both reads should enter the dict path: one per chunk. The validate call
  // verifies data correctness and DictionaryVector output — if the
  // onChunkLoad callback didn't clear stale dictionary state, the second
  // read would use chunk 1's alphabet to decode chunk 2's indices.
  EXPECT_EQ(dictPathEntries, 2)
      << "Expected dict path to be entered for both chunks";
}

// Verifies correctness when reads cross stripe boundaries in a non-chunked
// layout. We always break reads at the stripe boundaries, so there are no chunk
// transitions, thus we can always read with dictionary encoding.
TEST_P(StringColumnReaderTest, readAcrossStripeBoundary) {
  const bool stringDecoderZeroCopy = GetParam();
  if (!stringDecoderZeroCopy) {
    GTEST_SKIP() << "Dictionary path requires stringDecoderZeroCopy";
  }

  constexpr int kStripeRows = 150;
  // Stripe 1: alphabet {"alpha", "bravo", "charlie"}.
  auto stripe1 =
      makeRowVector({makeFlatVector<std::string>(kStripeRows, [](auto i) {
        return std::vector<std::string>{"alpha", "bravo", "charlie"}[i % 3];
      })});
  // Stripe 2: different alphabet {"delta", "echo"}.
  auto stripe2 =
      makeRowVector({makeFlatVector<std::string>(kStripeRows, [](auto i) {
        return std::vector<std::string>{"delta", "echo"}[i % 2];
      })});

  // Each write call becomes a separate stripe. Remove MainlyConstant from
  // read factors to force Dictionary as top-level encoding.
  auto readFactors = ManualEncodingSelectionPolicyFactory::defaultReadFactors();
  std::erase_if(readFactors, [](const auto& pair) {
    return pair.first == EncodingType::MainlyConstant;
  });
  VeloxWriterOptions writerOptions;
  writerOptions.enableChunking = true;
  writerOptions.encodingSelectionPolicyFactory =
      [readFactors](DataType dataType) {
        return ManualEncodingSelectionPolicyFactory(readFactors)
            .createPolicy(dataType);
      };
  auto file = test::createNimbleFile(
      *rootPool(), {stripe1, stripe2}, writerOptions, /*flushAfterWrite=*/true);

  auto expected =
      makeRowVector({makeFlatVector<std::string>(kStripeRows * 2, [](auto i) {
        if (i < kStripeRows) {
          return std::vector<std::string>{"alpha", "bravo", "charlie"}[i % 3];
        }
        return std::vector<std::string>{"delta", "echo"}[(i - kStripeRows) % 2];
      })});

  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*expected->type());
  auto readers = makeReaders(expected, file, scanSpec, stringDecoderZeroCopy);
  // 300 rows, batch_size=100, stripe boundary at 150:
  //   Batch 0 (0-99): stripe 1, 150 remaining >= 100 → dict
  //   Batch 1 (100-149): stripe 1, 50 remaining >= 50 → dict
  //   Batch 2 (0-99): stripe 2 (fresh reader), 150 remaining >= 100 → dict
  //   Batch 3 (100-149): stripe 2, 50 remaining >= 50 → dict
  using E = VectorEncoding::Simple;
  validateWithEncodingChecks(
      *expected->childAt(0),
      *readers.rowReader,
      100,
      /*totalRows=*/2 * kStripeRows,
      {E::DICTIONARY, E::DICTIONARY, E::DICTIONARY, E::DICTIONARY},
      asRowType(expected->type()),
      pool());
}

// Verifies that the dictionary path is entered and the alphabet has the
// expected size for single-chunk reads. Uses TestValue to inspect internal
// reader state without exposing private members.
DEBUG_ONLY_TEST_P(StringColumnReaderTest, dictionaryPathAlphabetSize) {
  const bool stringDecoderZeroCopy = GetParam();
  if (!stringDecoderZeroCopy) {
    GTEST_SKIP() << "Dictionary path requires stringDecoderZeroCopy";
  }

  constexpr int kRows = 500;
  // 3-entry alphabet with high repetition to trigger Dictionary encoding.
  auto input = makeRowVector({makeFlatVector<std::string>(kRows, [](auto i) {
    return std::vector<std::string>{"alpha", "bravo", "charlie"}[i % 3];
  })});

  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec, stringDecoderZeroCopy);

  int dictPathEntries = 0;
  SCOPED_TESTVALUE_SET(
      "facebook::nimble::StringColumnReader::readWithDictionary",
      std::function<void(const std::vector<std::string_view>*)>(
          [&](const std::vector<std::string_view>* alphabet) {
            ++dictPathEntries;
            std::set<std::string> entries;
            for (const auto& sv : *alphabet) {
              entries.insert(std::string(sv));
            }
            EXPECT_TRUE(entries.count("alpha"));
            EXPECT_TRUE(entries.count("bravo"));
            EXPECT_TRUE(entries.count("charlie"));
          }));

  validateWithEncodingChecks(
      *input->childAt(0),
      *readers.rowReader,
      kRows,
      /*totalRows=*/kRows,
      {VectorEncoding::Simple::DICTIONARY},
      asRowType(input->type()),
      pool());
  EXPECT_EQ(dictPathEntries, 1);
}

INSTANTIATE_TEST_CASE_P(
    StringColumnReaderTestSuite,
    StringColumnReaderTest,
    testing::Values(false, true),
    [](const testing::TestParamInfo<bool>& info) {
      return info.param ? "passStringBuffers" : "noPassStringBuffers";
    });

} // namespace
} // namespace facebook::nimble
