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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/TypeUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace facebook::nimble {
namespace {

using namespace facebook::velox;

// Tests for ListColumnReader and MapColumnReader (variable-length containers).
class VariableLengthColumnReaderTest : public ::testing::Test,
                                       public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::initializeMemoryManager(velox::memory::MemoryManager::Options{});
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
      const std::shared_ptr<common::ScanSpec>& scanSpec) {
    auto readFile = std::make_shared<InMemoryReadFile>(file);
    auto factory =
        dwio::common::getReaderFactory(dwio::common::FileFormat::NIMBLE);
    dwio::common::ReaderOptions options(pool());
    options.setScanSpec(scanSpec);
    Readers readers;
    readers.reader = factory->createReader(
        std::make_unique<dwio::common::BufferedInput>(readFile, *pool()),
        options);
    auto type = asRowType(expected->type());
    dwio::common::RowReaderOptions rowOptions;
    rowOptions.setScanSpec(scanSpec);
    rowOptions.setRequestedType(type);
    readers.rowReader = readers.reader->createRowReader(rowOptions);
    return readers;
  }

  Readers makeReaders(
      const RowVectorPtr& input,
      const std::shared_ptr<common::ScanSpec>& scanSpec) {
    return makeReaders(
        input, test::createNimbleFile(*rootPool(), input), scanSpec);
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
            << "Mismatch at row " << (offset + j) << ": expected "
            << expected.toString(offset + j) << " got " << result->toString(j);
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
};

// ----- ListColumnReader tests -----

TEST_F(VariableLengthColumnReaderTest, listEmptyContainers) {
  // 10 rows of non-null empty arrays — verifies empty != null.
  std::vector<std::vector<std::optional<int64_t>>> data(10);
  auto input = makeRowVector({makeNullableArrayVector<int64_t>(data)});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  auto result = BaseVector::create(input->type(), 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, result), 10);
  result->validate();
  auto* row = result->asUnchecked<RowVector>();
  auto& c0 = row->childAt(0);
  for (int i = 0; i < 10; ++i) {
    ASSERT_FALSE(c0->isNullAt(i)) << "Row " << i << " should not be null";
  }
  // All arrays should have size 0.
  DecodedVector decoded(*c0);
  auto* base = decoded.base()->as<ArrayVector>();
  for (int i = 0; i < 10; ++i) {
    auto idx = decoded.index(i);
    ASSERT_EQ(base->sizeAt(idx), 0) << "Row " << i << " should be empty";
  }
}

TEST_F(VariableLengthColumnReaderTest, listMixedNullsAndEmpty) {
  // Interleaved null/empty/populated arrays.
  std::vector<std::optional<std::vector<std::optional<int64_t>>>> data = {
      std::nullopt, // null
      {{}}, // empty
      {{{1, 2, 3}}}, // populated
      std::nullopt, // null
      {{}}, // empty
      {{{4, 5}}}, // populated
      std::nullopt, // null
      {{{6}}}, // populated
      {{}}, // empty
  };
  auto input = makeRowVector({makeNullableArrayVector<int64_t>(data)});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 3);
}

TEST_F(VariableLengthColumnReaderTest, listSkipAndRead) {
  // 100 rows with a filter on a sibling column to force skipping rows.
  auto c0 = makeArrayVector<int64_t>(
      100, [](auto i) { return 1 + i % 4; }, [](auto j) { return j * 10; });
  auto c1 = makeFlatVector<int64_t>(100, folly::identity);
  auto input = makeRowVector({c0, c1});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  // Filter: keep only rows where c1 >= 50.
  scanSpec->childByName("c1")->setFilter(
      std::make_unique<common::BigintRange>(50, 99, false));
  auto readers = makeReaders(input, scanSpec);
  validateWithFilter(
      *input, *readers.rowReader, 13, [](auto i) { return i >= 50; });
}

TEST_F(VariableLengthColumnReaderTest, listSmallBatches) {
  // 50 rows read with batch size 3 — many batch boundary transitions.
  auto input = makeRowVector({
      makeArrayVector<int64_t>(
          50, [](auto i) { return 1 + i % 5; }, [](auto j) { return j; }),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 3);
}

TEST_F(VariableLengthColumnReaderTest, listLargeElements) {
  // 5 rows with 200 elements each — large offsets/sizes.
  auto input = makeRowVector({
      makeArrayVector<int64_t>(
          5, [](auto) { return 200; }, [](auto j) { return j; }),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 5);
}

TEST_F(VariableLengthColumnReaderTest, listNestedArrays) {
  // ARRAY(ARRAY(BIGINT)) — recursive ListColumnReader.
  auto inner = makeArrayVector<int64_t>(
      15, [](auto i) { return 1 + i % 3; }, [](auto j) { return j * 100; });
  // Wrap in outer array: 5 rows, each with 3 inner arrays.
  auto outer = makeArrayVector({0, 3, 6, 9, 12}, inner);
  auto input = makeRowVector({outer});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 3);
}

TEST_F(VariableLengthColumnReaderTest, listWithNullElements) {
  // Array elements nullable via nullEvery(3) — child null propagation.
  auto input = makeRowVector({
      makeArrayVector<int64_t>(
          30,
          [](auto i) { return 1 + i % 4; },
          [](auto j) { return j; },
          nullptr,
          nullEvery(3)),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 7);
}

TEST_F(VariableLengthColumnReaderTest, listEstimatedRowSize) {
  auto input = makeRowVector({
      makeArrayVector<int64_t>(
          20, [](auto i) { return 1 + i % 5; }, [](auto j) { return j; }),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  auto estimatedRowSize = readers.rowReader->estimatedRowSize();
  ASSERT_TRUE(estimatedRowSize.has_value());
  ASSERT_GT(*estimatedRowSize, 0);
}

// ----- MapColumnReader tests -----

TEST_F(VariableLengthColumnReaderTest, mapEmptyContainers) {
  // 10 rows of non-null empty maps — verifies empty != null.
  std::vector<
      std::optional<std::vector<std::pair<int64_t, std::optional<int64_t>>>>>
      data(10, {{}});
  auto input = makeRowVector({makeNullableMapVector<int64_t, int64_t>(data)});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  auto result = BaseVector::create(input->type(), 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, result), 10);
  result->validate();
  auto* row = result->asUnchecked<RowVector>();
  auto& c0 = row->childAt(0);
  for (int i = 0; i < 10; ++i) {
    ASSERT_FALSE(c0->isNullAt(i)) << "Row " << i << " should not be null";
  }
  DecodedVector decoded(*c0);
  auto* base = decoded.base()->as<MapVector>();
  for (int i = 0; i < 10; ++i) {
    auto idx = decoded.index(i);
    ASSERT_EQ(base->sizeAt(idx), 0) << "Row " << i << " should be empty";
  }
}

TEST_F(VariableLengthColumnReaderTest, mapMixedNullsAndEmpty) {
  // Interleaved null/empty/populated maps.
  std::vector<
      std::optional<std::vector<std::pair<int64_t, std::optional<int64_t>>>>>
      data = {
          std::nullopt,
          {{}},
          {{{{1, 10}, {2, 20}}}},
          std::nullopt,
          {{}},
          {{{{3, 30}}}},
          std::nullopt,
          {{{{4, 40}, {5, 50}, {6, 60}}}},
          {{}},
      };
  auto input = makeRowVector({makeNullableMapVector<int64_t, int64_t>(data)});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 3);
}

TEST_F(VariableLengthColumnReaderTest, mapSkipAndRead) {
  // 100 rows with filter to force skip, verify key/value alignment.
  auto c0 = makeMapVector<int64_t, int64_t>(
      100,
      [](auto i) { return 1 + i % 4; },
      [](auto j) { return j; },
      [](auto j) { return j * 100; });
  auto c1 = makeFlatVector<int64_t>(100, folly::identity);
  auto input = makeRowVector({c0, c1});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c1")->setFilter(
      std::make_unique<common::BigintRange>(50, 99, false));
  auto readers = makeReaders(input, scanSpec);
  validateWithFilter(
      *input, *readers.rowReader, 13, [](auto i) { return i >= 50; });
}

TEST_F(VariableLengthColumnReaderTest, mapSmallBatches) {
  // 50 rows read with batch size 3.
  auto input = makeRowVector({
      makeMapVector<int64_t, int64_t>(
          50,
          [](auto i) { return 1 + i % 5; },
          [](auto j) { return j; },
          [](auto j) { return j * 10; }),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 3);
}

TEST_F(VariableLengthColumnReaderTest, mapStringKeys) {
  // MAP(VARCHAR, BIGINT) — string key handling.
  auto input = makeRowVector({makeMapVector<std::string, int64_t>({
      {{"alpha", 1}, {"beta", 2}},
      {{"gamma", 3}},
      {{"delta", 4}, {"epsilon", 5}, {"zeta", 6}},
  })});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 3);
}

TEST_F(VariableLengthColumnReaderTest, mapStringKeysAndValues) {
  // MAP(VARCHAR, VARCHAR) — full string map.
  auto input = makeRowVector({makeMapVector<std::string, std::string>({
      {{"k1", "v1"}, {"k2", "v2"}},
      {{"k3", "v3"}},
      {{"k4", "v4"}, {"k5", "v5"}, {"k6", "v6"}},
      {},
      {{"k7", "v7"}},
  })});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 2);
}

TEST_F(VariableLengthColumnReaderTest, mapNestedValues) {
  // MAP(BIGINT, MAP(BIGINT, BIGINT)) — recursive MapColumnReader.
  auto innerMap = makeMapVector<int64_t, int64_t>(
      8,
      [](auto i) { return 1 + i % 2; },
      [](auto j) { return j; },
      [](auto j) { return j * 10; });
  // Wrap in outer map: 4 rows, each with 2 inner maps.
  auto outerKeys = makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8});
  auto outer = std::make_shared<MapVector>(
      pool(),
      MAP(BIGINT(), MAP(BIGINT(), BIGINT())),
      nullptr,
      4,
      makeIndices({0, 2, 4, 6}),
      makeIndices({2, 2, 2, 2}),
      outerKeys,
      innerMap);
  auto input = makeRowVector({outer});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 2);
}

TEST_F(VariableLengthColumnReaderTest, mapWithNullValues) {
  // Map values nullable — null value propagation.
  auto input = makeRowVector({
      makeMapVector<int64_t, int64_t>(
          30,
          [](auto i) { return 1 + i % 4; },
          [](auto j) { return j; },
          [](auto j) { return j * 10; },
          nullptr,
          nullEvery(3)),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 7);
}

TEST_F(VariableLengthColumnReaderTest, mapEstimatedRowSize) {
  auto input = makeRowVector({
      makeMapVector<int64_t, int64_t>(
          20,
          [](auto i) { return 1 + i % 5; },
          [](auto j) { return j; },
          [](auto j) { return j * 10; }),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  auto estimatedRowSize = readers.rowReader->estimatedRowSize();
  ASSERT_TRUE(estimatedRowSize.has_value());
  ASSERT_GT(*estimatedRowSize, 0);
}

TEST_F(VariableLengthColumnReaderTest, listAllNullsWithFilter) {
  // All-null array with IsNotNull filter — all rows filtered out.
  auto input = makeRowVector({
      BaseVector::createNullConstant(ARRAY(BIGINT()), 20, pool()),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(std::make_unique<common::IsNotNull>());
  auto readers = makeReaders(input, scanSpec);
  validateWithFilter(
      *input, *readers.rowReader, 10, [](auto) { return false; });
}

} // namespace
} // namespace facebook::nimble
