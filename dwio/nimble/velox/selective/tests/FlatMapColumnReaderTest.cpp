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

// Tests for FlatMapAsStructColumnReader, FlatMapAsMapColumnReader, and
// FlatMapColumnReader (native).
class FlatMapColumnReaderTest : public ::testing::Test,
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

  std::string writeFlatMapFile(
      const RowVectorPtr& input,
      const std::string& flatMapColumn = "c0") {
    VeloxWriterOptions writerOptions;
    writerOptions.flatMapColumns = {flatMapColumn};
    return test::createNimbleFile(*rootPool(), input, writerOptions);
  }

  // Standard makeReaders: uses expected's type as the requestedType.
  Readers makeReaders(
      const RowVectorPtr& expected,
      const std::string& file,
      const std::shared_ptr<common::ScanSpec>& scanSpec,
      bool preserveFlatMapsInMemory = false) {
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
    rowOptions.setPreserveFlatMapsInMemory(preserveFlatMapsInMemory);
    readers.rowReader = readers.reader->createRowReader(rowOptions);
    return readers;
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
};

// ----- FlatMapAsStruct tests -----
// Pattern: write input (MAP type) via writeFlatMapFile, create reader using
// input (MAP-typed) for requestedType, set setFlatMapAsStruct on scanSpec,
// then read into a batch created with outType (struct-typed).

TEST_F(FlatMapColumnReaderTest, asStructBasic) {
  auto input = makeRowVector({
      makeMapVector<int32_t, int64_t>({
          {{1, 10}, {2, 20}, {3, 30}},
          {{1, 40}, {3, 50}},
          {{2, 60}},
      }),
  });
  auto file = writeFlatMapFile(input);
  auto outType =
      ROW({"c0"}, {ROW({"1", "2", "3"}, {BIGINT(), BIGINT(), BIGINT()})});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*outType);
  scanSpec->childByName("c0")->setFlatMapAsStruct(true);
  auto readers = makeReaders(input, file, scanSpec);
  VectorPtr batch = BaseVector::create(outType, 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, batch), 3);
  auto expected = makeRowVector({makeRowVector(
      {"1", "2", "3"},
      {
          makeNullableFlatVector<int64_t>({10, 40, std::nullopt}),
          makeNullableFlatVector<int64_t>({20, std::nullopt, 60}),
          makeNullableFlatVector<int64_t>({30, 50, std::nullopt}),
      })});
  velox::test::assertEqualVectors(expected, batch);
}

TEST_F(FlatMapColumnReaderTest, asStructInt8Keys) {
  auto input = makeRowVector({
      makeMapVector<int8_t, int64_t>({
          {{1, 100}, {2, 200}},
          {{1, 300}},
      }),
  });
  auto file = writeFlatMapFile(input);
  auto outType = ROW({"c0"}, {ROW({"1", "2"}, {BIGINT(), BIGINT()})});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*outType);
  scanSpec->childByName("c0")->setFlatMapAsStruct(true);
  auto readers = makeReaders(input, file, scanSpec);
  VectorPtr batch = BaseVector::create(outType, 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, batch), 2);
  auto expected = makeRowVector({makeRowVector(
      {"1", "2"},
      {
          makeFlatVector<int64_t>({100, 300}),
          makeNullableFlatVector<int64_t>({200, std::nullopt}),
      })});
  velox::test::assertEqualVectors(expected, batch);
}

TEST_F(FlatMapColumnReaderTest, asStructInt16Keys) {
  auto input = makeRowVector({
      makeMapVector<int16_t, int64_t>({
          {{10, 100}, {20, 200}},
          {{10, 300}, {20, 400}},
      }),
  });
  auto file = writeFlatMapFile(input);
  auto outType = ROW({"c0"}, {ROW({"10", "20"}, {BIGINT(), BIGINT()})});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*outType);
  scanSpec->childByName("c0")->setFlatMapAsStruct(true);
  auto readers = makeReaders(input, file, scanSpec);
  VectorPtr batch = BaseVector::create(outType, 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, batch), 2);
  auto expected = makeRowVector({makeRowVector(
      {"10", "20"},
      {
          makeFlatVector<int64_t>({100, 300}),
          makeFlatVector<int64_t>({200, 400}),
      })});
  velox::test::assertEqualVectors(expected, batch);
}

TEST_F(FlatMapColumnReaderTest, asStructInt64Keys) {
  auto input = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {{100, 1}, {200, 2}},
          {{200, 3}},
      }),
  });
  auto file = writeFlatMapFile(input);
  auto outType = ROW({"c0"}, {ROW({"100", "200"}, {BIGINT(), BIGINT()})});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*outType);
  scanSpec->childByName("c0")->setFlatMapAsStruct(true);
  auto readers = makeReaders(input, file, scanSpec);
  VectorPtr batch = BaseVector::create(outType, 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, batch), 2);
  auto expected = makeRowVector({makeRowVector(
      {"100", "200"},
      {
          makeNullableFlatVector<int64_t>({1, std::nullopt}),
          makeFlatVector<int64_t>({2, 3}),
      })});
  velox::test::assertEqualVectors(expected, batch);
}

TEST_F(FlatMapColumnReaderTest, asStructStringKeys) {
  auto input = makeRowVector({
      makeMapVector<StringView, int64_t>({
          {{"a", 1}, {"b", 2}, {"c", 3}},
          {{"a", 4}, {"c", 5}},
          {{"b", 6}},
      }),
  });
  auto file = writeFlatMapFile(input);
  auto outType =
      ROW({"c0"}, {ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), BIGINT()})});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*outType);
  scanSpec->childByName("c0")->setFlatMapAsStruct(true);
  auto readers = makeReaders(input, file, scanSpec);
  VectorPtr batch = BaseVector::create(outType, 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, batch), 3);
  auto expected = makeRowVector({makeRowVector(
      {"a", "b", "c"},
      {
          makeNullableFlatVector<int64_t>({1, 4, std::nullopt}),
          makeNullableFlatVector<int64_t>({2, std::nullopt, 6}),
          makeNullableFlatVector<int64_t>({3, 5, std::nullopt}),
      })});
  velox::test::assertEqualVectors(expected, batch);
}

TEST_F(FlatMapColumnReaderTest, asStructMultiBatch) {
  // 50 rows, batch size 7 — multi-batch transitions.
  auto input = makeRowVector({
      makeMapVector<int32_t, int64_t>(
          50,
          [](auto i) { return 1 + i % 3; },
          [](auto j) { return static_cast<int32_t>(1 + j % 5); },
          [](auto j) { return static_cast<int64_t>(j * 10); }),
  });
  auto file = writeFlatMapFile(input);
  // Keys are 1-5.
  auto outType =
      ROW({"c0"},
          {ROW(
              {"1", "2", "3", "4", "5"},
              {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()})});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*outType);
  scanSpec->childByName("c0")->setFlatMapAsStruct(true);
  auto readers = makeReaders(input, file, scanSpec);
  auto result = BaseVector::create(outType, 0, pool());
  int totalRead = 0;
  while (totalRead < 50) {
    auto numRead = readers.rowReader->next(7, result);
    if (numRead == 0) {
      break;
    }
    result->validate();
    auto* row = result->asUnchecked<RowVector>();
    ASSERT_TRUE(row->childAt(0)->type()->isRow());
    totalRead += result->size();
  }
  ASSERT_EQ(totalRead, 50);
}

TEST_F(FlatMapColumnReaderTest, asStructSubsetOfKeys) {
  // Request only keys {"2", "4"} from {1, 2, 3, 4, 5} — subfield pruning.
  auto input = makeRowVector({
      makeMapVector<int32_t, int64_t>({
          {{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}},
          {{1, 60}, {2, 70}, {3, 80}, {4, 90}, {5, 100}},
      }),
  });
  auto file = writeFlatMapFile(input);
  auto outType = ROW({"c0"}, {ROW({"2", "4"}, {BIGINT(), BIGINT()})});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*outType);
  scanSpec->childByName("c0")->setFlatMapAsStruct(true);
  auto readers = makeReaders(input, file, scanSpec);
  VectorPtr batch = BaseVector::create(outType, 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, batch), 2);
  auto expected = makeRowVector({makeRowVector(
      {"2", "4"},
      {
          makeFlatVector<int64_t>({20, 70}),
          makeFlatVector<int64_t>({40, 90}),
      })});
  velox::test::assertEqualVectors(expected, batch);
}

TEST_F(FlatMapColumnReaderTest, asStructWithNulls) {
  // Some null map rows — null propagation to struct output.
  auto input = makeRowVector({
      makeNullableMapVector<int32_t, int64_t>({
          {{{{1, 10}, {2, 20}}}},
          std::nullopt,
          {{{{1, 30}}}},
          std::nullopt,
      }),
  });
  auto file = writeFlatMapFile(input);
  auto outType = ROW({"c0"}, {ROW({"1", "2"}, {BIGINT(), BIGINT()})});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*outType);
  scanSpec->childByName("c0")->setFlatMapAsStruct(true);
  auto readers = makeReaders(input, file, scanSpec);
  VectorPtr batch = BaseVector::create(outType, 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, batch), 4);
  batch->validate();
  auto* row = batch->asUnchecked<RowVector>();
  auto& c0 = row->childAt(0);
  // Rows 1 and 3 should be null structs.
  ASSERT_FALSE(c0->isNullAt(0));
  ASSERT_TRUE(c0->isNullAt(1));
  ASSERT_FALSE(c0->isNullAt(2));
  ASSERT_TRUE(c0->isNullAt(3));
}

TEST_F(FlatMapColumnReaderTest, asStructAllEmpty) {
  // All rows have empty maps — all struct fields should be null.
  auto input = makeRowVector({
      makeMapVector<int32_t, int64_t>({{}, {}, {}}),
  });
  auto file = writeFlatMapFile(input);
  auto outType = ROW({"c0"}, {ROW({"1"}, {BIGINT()})});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*outType);
  scanSpec->childByName("c0")->setFlatMapAsStruct(true);
  auto readers = makeReaders(input, file, scanSpec);
  VectorPtr batch = BaseVector::create(outType, 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, batch), 3);
  auto expected = makeRowVector({
      makeRowVector({"1"}, {makeNullConstant(TypeKind::BIGINT, 3)}),
  });
  velox::test::assertEqualVectors(expected, batch);
}

TEST_F(FlatMapColumnReaderTest, asStructComplexValues) {
  // MAP(INT, ARRAY(BIGINT)) as struct — complex value types.
  auto inner = makeArrayVector<int64_t>({{1, 2}, {3}, {4, 5, 6}});
  auto keys = makeFlatVector<int32_t>({1, 2, 1});
  auto mapVector = std::make_shared<MapVector>(
      pool(),
      MAP(INTEGER(), ARRAY(BIGINT())),
      nullptr,
      2,
      makeIndices({0, 2}),
      makeIndices({2, 1}),
      keys,
      inner);
  auto input = makeRowVector({mapVector});
  auto file = writeFlatMapFile(input);
  auto outType =
      ROW({"c0"}, {ROW({"1", "2"}, {ARRAY(BIGINT()), ARRAY(BIGINT())})});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*outType);
  scanSpec->childByName("c0")->setFlatMapAsStruct(true);
  auto readers = makeReaders(input, file, scanSpec);
  VectorPtr batch = BaseVector::create(outType, 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, batch), 2);
  batch->validate();
  auto* row = batch->asUnchecked<RowVector>();
  auto* structCol = row->childAt(0)->loadedVector()->asUnchecked<RowVector>();
  ASSERT_EQ(structCol->childrenSize(), 2);
}

// ----- FlatMapAsMap tests -----

TEST_F(FlatMapColumnReaderTest, asMapBasic) {
  auto input = makeRowVector({
      makeMapVector<int32_t, int64_t>({
          {{1, 10}, {2, 20}},
          {{1, 30}, {3, 40}},
          {{2, 50}},
      }),
  });
  auto file = writeFlatMapFile(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, file, scanSpec);
  validate(*input, *readers.rowReader, 10);
}

TEST_F(FlatMapColumnReaderTest, asMapMultiBatch) {
  // 30 rows, batch size 5.
  auto input = makeRowVector({
      makeMapVector<int32_t, int64_t>(
          30,
          [](auto i) { return 1 + i % 3; },
          [](auto j) { return static_cast<int32_t>(1 + j % 4); },
          [](auto j) { return static_cast<int64_t>(j * 10); }),
  });
  auto file = writeFlatMapFile(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, file, scanSpec);
  validate(*input, *readers.rowReader, 5);
}

TEST_F(FlatMapColumnReaderTest, asMapWithNulls) {
  auto input = makeRowVector({
      makeNullableMapVector<int32_t, int64_t>({
          {{{{1, 10}, {2, 20}}}},
          std::nullopt,
          {{{{3, 30}}}},
          std::nullopt,
          {{{{1, 40}, {2, 50}, {3, 60}}}},
      }),
  });
  auto file = writeFlatMapFile(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, file, scanSpec);
  validate(*input, *readers.rowReader, 3);
}

TEST_F(FlatMapColumnReaderTest, asMapKeyFilter) {
  auto input = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {{1, 100}, {5, 500}, {10, 1000}},
          {{2, 200}, {8, 800}},
          {{3, 300}, {7, 700}, {15, 1500}},
      }),
  });
  auto file = writeFlatMapFile(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")
      ->childByName(common::ScanSpec::kMapKeysFieldName)
      ->setFilter(std::make_unique<common::BigintRange>(5, 10, false));
  auto expected = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {{5, 500}, {10, 1000}},
          {{8, 800}},
          {{7, 700}},
      }),
  });
  auto readers = makeReaders(expected, file, scanSpec);
  validate(*expected, *readers.rowReader, 10);
}

TEST_F(FlatMapColumnReaderTest, asMapComplexValues) {
  // MAP(INT, ROW({BIGINT})) — struct-valued flat map as map.
  auto values =
      makeRowVector({"val"}, {makeFlatVector<int64_t>({100, 200, 300, 400})});
  auto keys = makeFlatVector<int32_t>({1, 2, 1, 2});
  auto mapVector = std::make_shared<MapVector>(
      pool(),
      MAP(INTEGER(), ROW({"val"}, {BIGINT()})),
      nullptr,
      2,
      makeIndices({0, 2}),
      makeIndices({2, 2}),
      keys,
      values);
  auto input = makeRowVector({mapVector});
  auto file = writeFlatMapFile(input);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, file, scanSpec);
  validate(*input, *readers.rowReader, 10);
}

// ----- Native FlatMapColumnReader tests (preserveFlatMapsInMemory) -----

TEST_F(FlatMapColumnReaderTest, nativeMultiBatch) {
  // 40 rows, batch size 7 — FlatMapVector multi-batch.
  std::vector<std::vector<std::pair<int32_t, std::optional<int64_t>>>> data;
  for (int i = 0; i < 40; ++i) {
    std::vector<std::pair<int32_t, std::optional<int64_t>>> row;
    for (int j = 0; j <= i % 3; ++j) {
      row.emplace_back(
          static_cast<int32_t>(1 + j), static_cast<int64_t>(i * 10 + j));
    }
    data.push_back(std::move(row));
  }
  auto flatMap = makeFlatMapVector<int32_t, int64_t>(data);
  auto input = makeRowVector({flatMap, flatMap->toMapVector()});
  VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns = {"c0"};
  auto file = test::createNimbleFile(*rootPool(), input, writerOptions);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers =
      makeReaders(input, file, scanSpec, /*preserveFlatMapsInMemory=*/true);
  // Cross-compare: c0 (flat map on disk) is read as FlatMapVector,
  // c1 (regular map on disk) is read as MapVector.
  auto expected = makeRowVector({flatMap->toMapVector(), flatMap});
  validate(*expected, *readers.rowReader, 7);
}

TEST_F(FlatMapColumnReaderTest, nativeWithNulls) {
  auto flatMap = makeNullableFlatMapVector<int32_t, int64_t>({
      {{{1, 10}, {2, 20}}},
      std::nullopt,
      {{{1, 30}}},
      std::nullopt,
      {{{2, 40}, {3, 50}}},
  });
  auto input = makeRowVector({flatMap, flatMap->toMapVector()});
  VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns = {"c0"};
  auto file = test::createNimbleFile(*rootPool(), input, writerOptions);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers =
      makeReaders(input, file, scanSpec, /*preserveFlatMapsInMemory=*/true);
  auto expected = makeRowVector({flatMap->toMapVector(), flatMap});
  validate(*expected, *readers.rowReader, 3);
}

} // namespace
} // namespace facebook::nimble
