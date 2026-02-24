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
      bool passStringBuffersFromDecoder) {
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
    rowOptions.setPassStringBuffersFromDecoder(passStringBuffersFromDecoder);
    readers.rowReader = readers.reader->createRowReader(rowOptions);
    return readers;
  }

  Readers makeReaders(
      const RowVectorPtr& input,
      const std::shared_ptr<common::ScanSpec>& scanSpec,
      bool passStringBuffersFromDecoder) {
    return makeReaders(
        input,
        test::createNimbleFile(*rootPool(), input),
        scanSpec,
        passStringBuffersFromDecoder);
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
};

// Strings shorter than StringView::kInlineSize (12 bytes).
TEST_P(StringColumnReaderTest, shortStrings) {
  const bool passStringBuffersFromDecoder = GetParam();
  auto input = makeRowVector({
      makeFlatVector<std::string>(
          100, [](auto i) { return std::to_string(i); }),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec, passStringBuffersFromDecoder);
  validate(*input, *readers.rowReader, 23);
}

// Strings longer than inline size.
TEST_P(StringColumnReaderTest, longStrings) {
  const bool passStringBuffersFromDecoder = GetParam();
  const std::string prefix(20, 'x');
  auto input = makeRowVector({
      makeFlatVector<std::string>(
          50, [&](auto i) { return prefix + std::to_string(i); }),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec, passStringBuffersFromDecoder);
  validate(*input, *readers.rowReader, 13);
}

// Alternating short and long strings.
TEST_P(StringColumnReaderTest, mixedLengthStrings) {
  const bool passStringBuffersFromDecoder = GetParam();
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
  auto readers = makeReaders(input, scanSpec, passStringBuffersFromDecoder);
  validate(*input, *readers.rowReader, 17);
}

// Strings with null values.
TEST_P(StringColumnReaderTest, stringsWithNulls) {
  const bool passStringBuffersFromDecoder = GetParam();
  auto input = makeRowVector({
      makeFlatVector<std::string>(
          100, [](auto i) { return "str_" + std::to_string(i); }, nullEvery(5)),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec, passStringBuffersFromDecoder);
  validate(*input, *readers.rowReader, 11);
}

// All-null string column.
TEST_P(StringColumnReaderTest, allNullStrings) {
  const bool passStringBuffersFromDecoder = GetParam();
  auto input = makeRowVector({
      makeConstant<StringView>(std::nullopt, 50),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec, passStringBuffersFromDecoder);
  auto result = BaseVector::create(input->type(), 0, pool());
  ASSERT_EQ(readers.rowReader->next(50, result), 50);
  auto* row = result->asUnchecked<RowVector>();
  for (int i = 0; i < 50; ++i) {
    ASSERT_TRUE(row->childAt(0)->isNullAt(i));
  }
}

// All empty "" strings.
TEST_P(StringColumnReaderTest, emptyStrings) {
  const bool passStringBuffersFromDecoder = GetParam();
  auto input = makeRowVector({
      makeFlatVector<std::string>(50, [](auto) { return ""; }),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec, passStringBuffersFromDecoder);
  validate(*input, *readers.rowReader, 50);
}

// VARBINARY type (shares reader with VARCHAR).
TEST_P(StringColumnReaderTest, varbinaryColumn) {
  const bool passStringBuffersFromDecoder = GetParam();
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
  auto readers = makeReaders(input, scanSpec, passStringBuffersFromDecoder);
  validate(*input, *readers.rowReader, 50);
}

// BytesValues filter on string column.
TEST_P(StringColumnReaderTest, stringBytesFilter) {
  const bool passStringBuffersFromDecoder = GetParam();
  auto c0 = makeFlatVector<std::string>(
      100, [](auto i) { return "val_" + std::to_string(i % 10); });
  auto input = makeRowVector({c0});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BytesValues>(
          std::vector<std::string>{"val_3", "val_7"}, false));
  auto readers = makeReaders(input, scanSpec, passStringBuffersFromDecoder);
  validateWithFilter(*input, *readers.rowReader, 23, [](auto i) {
    return i % 10 == 3 || i % 10 == 7;
  });
}

// BytesRange filter on string column.
TEST_P(StringColumnReaderTest, stringBytesRangeFilter) {
  const bool passStringBuffersFromDecoder = GetParam();
  auto c0 = makeFlatVector<std::string>(
      100, [](auto i) { return "val_" + std::to_string(i % 10); });
  auto input = makeRowVector({c0});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BytesRange>(
          "val_3", false, false, "val_5", false, false, false));
  auto readers = makeReaders(input, scanSpec, passStringBuffersFromDecoder);
  validateWithFilter(*input, *readers.rowReader, 23, [](auto i) {
    auto val = "val_" + std::to_string(i % 10);
    return val >= "val_3" && val <= "val_5";
  });
}

// Small batch sizes to exercise skip positioning.
TEST_P(StringColumnReaderTest, stringSkipAndRead) {
  const bool passStringBuffersFromDecoder = GetParam();
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
  auto readers = makeReaders(input, scanSpec, passStringBuffersFromDecoder);
  validate(*input, *readers.rowReader, 7);
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
