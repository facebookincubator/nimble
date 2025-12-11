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

enum FilterType { kNone, kKeep, kDrop };
auto format_as(FilterType filterType) {
  return fmt::underlying(filterType);
}

// This test suite covers the basic and mostly single batch test cases, as well
// as some corner cases that are hard to cover in randomized tests.  We rely on
// E2EFilterTest for more comprehensive tests with multi stripes and multi
// filters.
class SelectiveNimbleReaderTest : public ::testing::Test,
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
    EXPECT_EQ(readers.reader->numberOfRows(), expected->size());
    auto type = asRowType(expected->type());
    dwio::common::typeutils::checkTypeCompatibility(
        *readers.reader->rowType(), *type);
    dwio::common::RowReaderOptions rowOptions;
    rowOptions.setScanSpec(scanSpec);
    rowOptions.setRequestedType(type);
    rowOptions.setPreserveFlatMapsInMemory(preserveFlatMapsInMemory);
    readers.rowReader = readers.reader->createRowReader(rowOptions);
    return readers;
  }

  Readers makeReaders(
      const RowVectorPtr& input,
      const std::shared_ptr<common::ScanSpec>& scanSpec) {
    return makeReaders(
        input, test::createNimbleFile(*rootPool(), input), scanSpec);
  }

  template <typename F>
  void validate(
      const RowVector& input,
      dwio::common::RowReader& rowReader,
      int batchSize,
      F&& filter) {
    validate(input, rowReader, batchSize, -1, std::forward<F>(filter));
  }

  template <typename F>
  void validate(
      const RowVector& input,
      dwio::common::RowReader& rowReader,
      int batchSize,
      int dropColumn,
      F&& filter) {
    auto result =
        BaseVector::create(rowType(input.type(), dropColumn), 0, pool());
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
          VLOG(1) << i << ": " << input.toString(i);
          ++i;
        }
        VLOG(1) << i << ": " << input.toString(i) << " " << j << ": "
                << result->toString(j);
        if (dropColumn < 0) {
          ASSERT_TRUE(result->equalValueAt(&input, j, i));
        } else {
          auto* resultRow = result->asUnchecked<RowVector>();
          for (int k = 0, kk = 0; k < resultRow->childrenSize(); ++k) {
            if (k != dropColumn) {
              auto& expected = input.childAt(k);
              auto& actual = resultRow->childAt(kk++);
              ASSERT_TRUE(actual->equalValueAt(expected.get(), j, i));
            }
          }
        }
        ++i;
      }
    }
    while (i < input.size()) {
      VLOG(1) << i << ": " << input.toString(i);
      ASSERT_FALSE(filter(i));
      ++i;
    }
    ASSERT_EQ(numScanned, input.size());
    ASSERT_EQ(0, rowReader.next(1, result));
  }

  template <typename MakeData, typename ValidationFilter>
  void runTestCase(
      MakeData&& makeData,
      const common::Filter& filter,
      int batchSize,
      ValidationFilter&& validationFilter,
      std::function<VectorPtr(bool)> makeExpectedData = nullptr) {
    for (bool hasNulls : {false, true}) {
      auto data = makeData(hasNulls);
      auto input = makeRowVector({data, data});
      auto file = test::createNimbleFile(*rootPool(), input);
      RowVectorPtr expected;
      if (makeExpectedData) {
        auto expectedData = makeExpectedData(hasNulls);
        expected = makeRowVector({expectedData, expectedData});
      } else {
        expected = input;
      }
      for (auto filterType : {kNone, kKeep, kDrop}) {
        SCOPED_TRACE(
            fmt::format("hasNulls={} filterType={}", hasNulls, filterType));
        int dropColumn = -1;
        auto scanSpec = std::make_shared<common::ScanSpec>("root");
        scanSpec->addAllChildFields(*expected->type());
        if (filterType != kNone) {
          common::ScanSpec* c0 = scanSpec->childByName("c0");
          c0->setFilter(filter.clone());
          if (filterType == kDrop) {
            c0->setProjectOut(false);
            c0->setChannel(common::ScanSpec::kNoChannel);
            scanSpec->childByName("c1")->setChannel(0);
            dropColumn = 0;
          }
        }
        auto readers = makeReaders(expected, file, scanSpec);
        validate(
            *expected, *readers.rowReader, batchSize, dropColumn, [&](auto i) {
              return filterType == kNone || validationFilter(data, i);
            });
      }
    }
  }

  void checkArrayWithOffsets(
      const std::vector<std::optional<std::vector<std::optional<int64_t>>>>&
          data,
      const std::vector<bool>& filter,
      const std::vector<int>& readSizes,
      std::optional<int> maxArrayElementsCount = std::nullopt,
      bool filterAfterRead = false) {
    RowVectorPtr vector;
    if (filter.empty()) {
      vector = makeRowVector({makeNullableArrayVector<int64_t>(data)});
    } else if (filterAfterRead) {
      vector = makeRowVector({
          makeNullableArrayVector<int64_t>(data),
          makeRowVector(
              {makeConstant<int8_t>(0, data.size())},
              [&](auto i) { return !filter[i]; }),
      });
    } else {
      vector = makeRowVector({
          makeNullableArrayVector<int64_t>(data),
          makeFlatVector<bool>(filter),
      });
    }
    auto rowType = asRowType(vector->type());
    auto file = test::createNimbleFile(
        *rootPool(), vector, {.dictionaryArrayColumns = {"c0"}});
    auto scanSpec = std::make_shared<common::ScanSpec>("root");
    scanSpec->addAllChildFields(*rowType);
    if (!filter.empty()) {
      if (filterAfterRead) {
        scanSpec->childByName("c0")->setFilter(
            std::make_unique<common::IsNotNull>());
        scanSpec->childByName("c1")->setFilter(
            std::make_unique<common::IsNotNull>());
      } else {
        scanSpec->childByName("c1")->setFilter(
            std::make_unique<common::BoolValue>(true, false));
      }
    }
    if (maxArrayElementsCount.has_value()) {
      scanSpec->childByName("c0")->setMaxArrayElementsCount(
          *maxArrayElementsCount);
    }
    auto readers = makeReaders(vector, file, scanSpec);
    auto result = BaseVector::create(rowType, 0, pool());
    int totalScanned = 0;
    std::vector<bool> selected(data.size(), true);
    if (!filter.empty()) {
      selected = filter;
    }
    for (int readSize : readSizes) {
      ASSERT_EQ(readers.rowReader->next(readSize, result), readSize);
      auto begin = selected.begin() + totalScanned;
      ASSERT_EQ(result->size(), std::accumulate(begin, begin + readSize, 0));
      auto& c0 = result->loadedVector()->asChecked<RowVector>()->childAt(0);
      int resultIndex = 0;
      int wrappedIndex = -1;
      int offset = 0;
      int lastSize = 0;
      int lastExpected = -1;
      for (int i = 0; i < readSize; ++i) {
        if (!selected[totalScanned + i]) {
          continue;
        }
        auto& expected = data[totalScanned + i];
        if (!expected.has_value()) {
          ASSERT_TRUE(c0->isNullAt(resultIndex));
        } else {
          if (lastExpected < 0 || data[lastExpected] != expected) {
            ++wrappedIndex;
            lastExpected = totalScanned + i;
            offset += lastSize;
          }
          folly::Range<const std::optional<int64_t>*> expectedRange;
          if (maxArrayElementsCount.has_value()) {
            expectedRange = {
                expected->data(),
                std::min<size_t>(
                    expected->size(), maxArrayElementsCount.value())};
          } else {
            expectedRange = *expected;
          }
          ASSERT_FALSE(c0->isNullAt(resultIndex));
          ASSERT_EQ(c0->wrappedIndex(resultIndex), wrappedIndex);
          auto* alphabet = c0->wrappedVector()->asChecked<ArrayVector>();
          ASSERT_EQ(alphabet->offsetAt(wrappedIndex), offset);
          ASSERT_EQ(alphabet->sizeAt(wrappedIndex), expectedRange.size());
          lastSize = alphabet->sizeAt(wrappedIndex);
          auto* elements =
              alphabet->elements()->asChecked<FlatVector<int64_t>>();
          for (int j = 0; j < expectedRange.size(); ++j) {
            if (expectedRange[j].has_value()) {
              ASSERT_FALSE(elements->isNullAt(offset + j));
              ASSERT_EQ(elements->valueAt(offset + j), expectedRange[j]);
            } else {
              ASSERT_TRUE(elements->isNullAt(offset + j));
            }
          }
        }
        ++resultIndex;
      }
      totalScanned += readSize;
    }
    ASSERT_EQ(totalScanned, data.size());
    ASSERT_EQ(readers.rowReader->next(1, result), 0);
  }

  void checkSlidingWindowMap(
      const std::vector<std::optional<
          std::vector<std::pair<int64_t, std::optional<int64_t>>>>>& data,
      const std::vector<bool>& rowFilter,
      const common::Filter* keyFilter,
      const std::vector<int>& readSizes,
      bool filterAfterRead = false) {
    RowVectorPtr vector;
    if (rowFilter.empty()) {
      vector = makeRowVector({makeNullableMapVector<int64_t>(data)});
    } else if (filterAfterRead) {
      vector = makeRowVector({
          makeNullableMapVector<int64_t>(data),
          makeRowVector(
              {makeConstant<int8_t>(0, data.size())},
              [&](auto i) { return !rowFilter[i]; }),
      });
    } else {
      vector = makeRowVector({
          makeNullableMapVector<int64_t>(data),
          makeFlatVector<bool>(rowFilter),
      });
    }
    auto rowType = asRowType(vector->type());
    auto file = test::createNimbleFile(
        *rootPool(), vector, {.deduplicatedMapColumns = {"c0"}});
    auto scanSpec = std::make_shared<common::ScanSpec>("root");
    scanSpec->addAllChildFields(*rowType);
    if (!rowFilter.empty()) {
      if (filterAfterRead) {
        scanSpec->childByName("c0")->setFilter(
            std::make_unique<common::IsNotNull>());
        scanSpec->childByName("c1")->setFilter(
            std::make_unique<common::IsNotNull>());
      } else {
        scanSpec->childByName("c1")->setFilter(
            std::make_unique<common::BoolValue>(true, false));
      }
    }
    if (keyFilter) {
      scanSpec->childByName("c0")
          ->childByName(common::ScanSpec::kMapKeysFieldName)
          ->setFilter(keyFilter->clone());
    }
    auto readers = makeReaders(vector, file, scanSpec);
    auto result = BaseVector::create(rowType, 0, pool());
    int totalScanned = 0;
    std::vector<bool> selected(data.size(), true);
    if (!rowFilter.empty()) {
      selected = rowFilter;
    }
    for (int readSize : readSizes) {
      ASSERT_EQ(readers.rowReader->next(readSize, result), readSize);
      auto begin = selected.begin() + totalScanned;
      ASSERT_EQ(result->size(), std::accumulate(begin, begin + readSize, 0));
      auto& c0 = result->loadedVector()->asChecked<RowVector>()->childAt(0);
      int resultIndex = 0;
      int wrappedIndex = -1;
      int offset = 0;
      int lastSize = 0;
      int lastExpected = -1;
      for (int i = 0; i < readSize; ++i) {
        if (!selected[totalScanned + i]) {
          continue;
        }
        auto& expected = data[totalScanned + i];
        if (!expected.has_value()) {
          ASSERT_TRUE(c0->isNullAt(resultIndex));
        } else {
          if (lastExpected < 0 || data[lastExpected] != expected) {
            ++wrappedIndex;
            lastExpected = totalScanned + i;
            offset += lastSize;
          }
          std::vector<int> selectedKeys;
          if (keyFilter) {
            for (int j = 0; j < expected->size(); ++j) {
              if (keyFilter->testInt64((*expected)[j].first)) {
                selectedKeys.push_back(j);
              }
            }
          } else {
            selectedKeys.resize(expected->size());
            std::iota(selectedKeys.begin(), selectedKeys.end(), 0);
          }
          ASSERT_FALSE(c0->isNullAt(resultIndex));
          ASSERT_EQ(c0->wrappedIndex(resultIndex), wrappedIndex);
          auto* alphabet = c0->wrappedVector()->asChecked<MapVector>();
          ASSERT_EQ(alphabet->offsetAt(wrappedIndex), offset);
          ASSERT_EQ(alphabet->sizeAt(wrappedIndex), selectedKeys.size());
          lastSize = alphabet->sizeAt(wrappedIndex);
          auto* keys = alphabet->mapKeys()->asChecked<FlatVector<int64_t>>();
          auto* values =
              alphabet->mapValues()->asChecked<FlatVector<int64_t>>();
          for (int j = 0; j < selectedKeys.size(); ++j) {
            auto [expectedKey, expectedValue] = (*expected)[selectedKeys[j]];
            ASSERT_FALSE(keys->isNullAt(offset + j));
            ASSERT_EQ(keys->valueAt(offset + j), expectedKey);
            if (expectedValue.has_value()) {
              ASSERT_FALSE(values->isNullAt(offset + j));
              ASSERT_EQ(values->valueAt(offset + j), *expectedValue);
            } else {
              ASSERT_TRUE(values->isNullAt(offset + j));
            }
          }
        }
        ++resultIndex;
      }
      totalScanned += readSize;
    }
    ASSERT_EQ(totalScanned, data.size());
    ASSERT_EQ(readers.rowReader->next(1, result), 0);
  }

 private:
  static RowTypePtr rowType(const TypePtr& type, int dropColumn) {
    if (dropColumn < 0) {
      return asRowType(type);
    }
    auto& rowType = type->asRow();
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (int k = 0; k < rowType.size(); ++k) {
      if (k != dropColumn) {
        names.push_back(rowType.nameOf(k));
        types.push_back(rowType.childAt(k));
      }
    }
    return ROW(std::move(names), std::move(types));
  }
};

// This case covers
//   - Dense + No nulls
//   - Sparse + No nulls
//   - Sparse + Nulls
TEST_F(SelectiveNimbleReaderTest, basic) {
  auto c0 = makeFlatVector<int64_t>(1009, folly::identity);
  auto* rawC0 = c0->mutableRawValues();
  std::default_random_engine rng(42);
  std::shuffle(rawC0, rawC0 + 809, rng);
  setNulls(c0, nullEvery(11));
  auto input = makeRowVector({
      c0,
      makeFlatVector<int64_t>(1009, folly::identity, nullEvery(17)),
      makeRowVector({c0}, nullEvery(13)),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintRange>(0, 502, false));
  auto readers = makeReaders(input, scanSpec);
  auto estimatedRowSize = readers.rowReader->estimatedRowSize();
  ASSERT_TRUE(estimatedRowSize.has_value());
  ASSERT_EQ(*estimatedRowSize, 24);
  validate(*input, *readers.rowReader, 101, [&](auto i) {
    return !c0->isNullAt(i) && rawC0[i] <= 502;
  });
}

TEST_F(SelectiveNimbleReaderTest, denseWithNulls) {
  auto input = makeRowVector({
      makeRowVector(
          {makeFlatVector<int64_t>(103, folly::identity)}, nullEvery(11)),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  auto result = BaseVector::create(input->type(), 0, pool());
  validate(*input, *readers.rowReader, 7, [](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, denseMostlyNulls) {
  auto isNull = [](auto i) { return i != 53; };
  auto input = makeRowVector({
      makeRowVector({makeFlatVector<int64_t>(103, folly::identity)}, isNull),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 11, [](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, sparseMostlyNulls) {
  auto c0 = makeFlatVector<int64_t>(101, folly::identity);
  auto input = makeRowVector({
      c0,
      makeRowVector({c0}, [](auto i) { return i % 17 != 0; }),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BigintRange>(47, 100, true));
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 13, [&](auto i) {
    return c0->isNullAt(i) || i >= 47;
  });
}

TEST_F(SelectiveNimbleReaderTest, allNulls) {
  std::vector<vector_size_t> offsets(104);
  std::iota(offsets.begin(), offsets.end(), 0);
  auto input = makeRowVector({
      makeConstant<int64_t>(std::nullopt, 103),
      BaseVector::createNullConstant(ROW({"c0"}, {BIGINT()}), 103, pool()),
      BaseVector::createNullConstant(ARRAY(BIGINT()), 103, pool()),
      BaseVector::createNullConstant(MAP(BIGINT(), BIGINT()), 103, pool()),
      makeMapVector<int32_t, int32_t>(
          103,
          [](auto i) { return i % 2 == 0 ? 1 : 0; },
          [](auto) { return 1; },
          [](auto) { return 42; },
          {},
          [](auto) { return true; }),
      makeMapVector(
          offsets,
          BaseVector::createNullConstant(INTEGER(), 103, pool()),
          BaseVector::createNullConstant(INTEGER(), 103, pool())),
  });
  VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns = {"c4"};
  auto fileContent = test::createNimbleFile(*rootPool(), input, writerOptions);

  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, fileContent, scanSpec);
  validate(*input, *readers.rowReader, 11, [](auto) { return true; });

  scanSpec->resetCachedValues(false);
  scanSpec->childByName("c0")->setFilter(std::make_unique<common::IsNull>());
  readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 11, [](auto) { return true; });

  scanSpec->resetCachedValues(false);
  scanSpec->childByName("c0")->setFilter(std::make_unique<common::IsNotNull>());
  readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 11, [](auto) { return false; });
}

TEST_F(SelectiveNimbleReaderTest, multiChunkNulls) {
  auto chunk1 = makeRowVector({
      makeFlatVector<StringView>(
          2, [](auto) { return "foo"; }, [](auto i) { return i == 1; }),
      makeConstant<bool>(true, 2),
  });
  auto chunk2 = makeRowVector({
      makeFlatVector<StringView>(
          10,
          [](auto i) { return i == 0 ? "foo" : "bar"; },
          [](auto i) { return i >= 2; }),
      makeFlatVector<bool>(10, [](auto i) { return i != 7; }),
  });
  auto chunk3 = makeRowVector({
      makeConstant<StringView>(StringView("quux"), 10),
      makeConstant<bool>(true, 10),
  });
  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::MainlyConstant, 1.0},
      {EncodingType::Constant, 1.0},
  };
  ManualEncodingSelectionPolicyFactory encodingFactory(readFactors);
  VeloxWriterOptions options;
  options.encodingSelectionPolicyFactory = [&](DataType dataType) {
    return encodingFactory.createPolicy(dataType);
  };
  options.enableChunking = true;
  options.minStreamChunkRawSize = 0;
  options.flushPolicyFactory = [] {
    return std::make_unique<LambdaFlushPolicy>(
        /*flushLambda=*/[](const StripeProgress&) { return false; },
        /*chunkLambda=*/[](const StripeProgress&) { return true; });
  };
  auto file = test::createNimbleFile(
      *rootPool(), {chunk1, chunk2, chunk3}, options, false);
  auto& input = chunk1;
  input->append(chunk2.get());
  input->append(chunk3.get());
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c1")->setFilter(
      std::make_unique<common::BoolValue>(true, true));
  auto readers = makeReaders(input, file, scanSpec);
  validate(
      *input, *readers.rowReader, input->size(), [](auto i) { return i != 9; });
}

TEST_F(SelectiveNimbleReaderTest, filterIsNull) {
  auto c0 = makeFlatVector<int64_t>(11, folly::identity, nullEvery(2));
  auto input = makeRowVector({makeRowVector({c0}, nullEvery(3))});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->childByName("c0")->setFilter(
      std::make_unique<common::IsNull>());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 5, [&](auto i) {
    return i % 2 == 0 || i % 3 == 0;
  });
}

TEST_F(SelectiveNimbleReaderTest, multiChunkInt16RowSetOverBoundary) {
  auto chunk1 = makeRowVector({
      makeFlatVector<int16_t>(10, folly::identity),
  });
  auto chunk2 = makeRowVector({
      makeFlatVector<int16_t>(3, folly::identity),
  });
  std::vector<std::pair<EncodingType, float>> readFactors;
  ManualEncodingSelectionPolicyFactory encodingFactory(readFactors);
  VeloxWriterOptions options;
  options.encodingSelectionPolicyFactory = [&](DataType dataType) {
    return encodingFactory.createPolicy(dataType);
  };
  options.enableChunking = true;
  options.minStreamChunkRawSize = 0;
  options.flushPolicyFactory = [] {
    return std::make_unique<LambdaFlushPolicy>(
        /*flushLambda=*/[](const StripeProgress&) { return false; },
        /*chunkLambda=*/[](const StripeProgress&) { return true; });
  };
  auto file =
      test::createNimbleFile(*rootPool(), {chunk1, chunk2}, options, false);
  auto& input = chunk1;
  input->append(chunk2.get());
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(std::make_unique<common::IsNotNull>());
  auto readers = makeReaders(input, file, scanSpec);
  validate(
      *input, *readers.rowReader, input->size(), [](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, strings) {
  const std::string longPrefix(17, 'x');
  auto c0 = makeFlatVector<std::string>(
      13,
      [&](auto i) {
        return i % 2 == 0 ? std::to_string(i)
                          : fmt::format("{}{}", longPrefix, i);
      },
      nullEvery(5));
  auto input = makeRowVector({c0});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 3, [](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, bools) {
  auto input = makeRowVector({
      makeFlatVector<bool>(
          67, [](auto i) { return i % 3 != 0; }, nullEvery(7)),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(
      std::make_unique<common::BoolValue>(true, true));
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 13, [&](auto i) {
    return i % 7 == 0 || i % 3 != 0;
  });
}

TEST_F(SelectiveNimbleReaderTest, floats) {
  runTestCase(
      [&](bool hasNulls) {
        return makeFlatVector<double>(
            101,
            [](auto i) { return sin(i); },
            hasNulls ? nullEvery(11) : nullptr);
      },
      common::FloatingPointRange<double>(
          -INFINITY, true, false, 0.5, false, false, false),
      23,
      [&](auto& data, auto i) { return !data->isNullAt(i) && sin(i) <= 0.5; });
}

TEST_F(SelectiveNimbleReaderTest, rle) {
  runTestCase(
      [&](bool hasNulls) {
        return makeFlatVector<double>(
            101,
            [](auto i) { return sin(i / 13); },
            hasNulls ? nullEvery(17) : nullptr);
      },
      common::FloatingPointRange<double>(
          -INFINITY, true, false, 0.5, false, false, false),
      23,
      [&](auto& data, auto i) {
        return !data->isNullAt(i) && sin(i / 13) <= 0.5;
      });
}

TEST_F(SelectiveNimbleReaderTest, byteRle) {
  runTestCase(
      [&](bool hasNulls) {
        return makeFlatVector<int8_t>(
            101,
            [](auto i) { return i / 13; },
            hasNulls ? nullEvery(11) : nullptr);
      },
      common::BigintRange(4, 6, false),
      23,
      [&](auto& data, auto i) {
        return !data->isNullAt(i) && 4 <= i / 13 && i / 13 <= 6;
      });
}

TEST_F(SelectiveNimbleReaderTest, rleString) {
  auto c0 = makeFlatVector<std::string>(
      101, [](auto i) { return std::to_string(sin(i / 13)); }, nullEvery(17));
  auto input = makeRowVector({c0});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 23, [](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, rleBool) {
  auto c0 =
      makeFlatVector<bool>(67, [](auto i) { return i >= 31; }, nullEvery(17));
  auto input = makeRowVector({c0});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 23, [](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, mainlyConstant) {
  const std::string longPrefix(17, 'x');
  auto makeData = [&](bool hasNulls) {
    return makeFlatVector<std::string>(
        101,
        [&](auto i) {
          return i % 11 != 0 ? "common" : fmt::format("{}{}", longPrefix, i);
        },
        hasNulls ? nullEvery(17) : nullptr);
  };
  {
    SCOPED_TRACE("Keep common value");
    runTestCase(
        makeData,
        common::BytesValues(
            {"common", fmt::format("{}{}", longPrefix, 11)}, false),
        23,
        [&](auto& data, auto i) {
          return !data->isNullAt(i) && (i % 11 != 0 || i == 11);
        });
  }
  {
    SCOPED_TRACE("Drop common value");
    runTestCase(
        makeData,
        common::BytesValues({fmt::format("{}{}", longPrefix, 11)}, false),
        23,
        [&](auto& data, auto i) { return !data->isNullAt(i) && i == 11; });
  }
}

TEST_F(SelectiveNimbleReaderTest, dictionary) {
  const std::string alphabet[] = {"foo", "bar", "quux"};
  auto c0 = makeFlatVector<std::string>(
      1009, [&](auto i) { return alphabet[i % 3]; }, nullEvery(17));
  auto input = makeRowVector({c0});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 23, [](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, smallDictionaryValue) {
  auto c0 = makeFlatVector<int8_t>(257, folly::identity);
  auto input = makeRowVector({c0});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::MainlyConstant, 1.0},
      {EncodingType::Dictionary, 1.0},
  };
  ManualEncodingSelectionPolicyFactory encodingFactory(readFactors);
  VeloxWriterOptions options;
  options.encodingSelectionPolicyFactory = [&](DataType dataType) {
    return encodingFactory.createPolicy(dataType);
  };
  auto readers = makeReaders(
      input, test::createNimbleFile(*rootPool(), input, options), scanSpec);
  validate(*input, *readers.rowReader, 257, [](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, constant) {
  auto c0 = makeFlatVector<std::string>(
      101, [&](auto) { return "foo"; }, nullEvery(17));
  auto input = makeRowVector({c0});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  validate(*input, *readers.rowReader, 23, [](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, array) {
  runTestCase(
      [&](bool hasNulls) {
        return makeArrayVector<double>(
            13,
            [](auto i) { return 1 + i % 5; },
            [](auto j) { return sin(j); },
            hasNulls ? nullEvery(7) : nullptr);
      },
      common::IsNotNull(),
      5,
      [](auto& data, auto i) { return !data->isNullAt(i); });
}

TEST_F(SelectiveNimbleReaderTest, arrayPruning) {
  auto input = makeRowVector({
      makeArrayVector<int64_t>({{1, 2, 3, 4, 5}, {1, 2}}),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setMaxArrayElementsCount(3);
  auto readers = makeReaders(input, scanSpec);
  auto expected = makeRowVector({
      makeArrayVector<int64_t>({{1, 2, 3}, {1, 2}}),
  });
  validate(*expected, *readers.rowReader, 23, [](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, arrayElementFilter) {
  auto input = makeRowVector({
      makeArrayVector<int64_t>({{1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}}),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")
      ->childByName(common::ScanSpec::kArrayElementsFieldName)
      ->setFilter(std::make_unique<common::BigintRange>(4, 7, false));
  auto readers = makeReaders(input, scanSpec);
  auto expected = makeRowVector({makeArrayVector<int64_t>({
      {4, 5},
      {6, 7},
  })});
  validate(*expected, *readers.rowReader, 23, [](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, map) {
  runTestCase(
      [&](bool hasNulls) {
        return makeMapVector<int64_t, double>(
            13,
            [](auto i) { return 1 + i % 5; },
            [](auto j) { return j; },
            [](auto j) { return sin(j); },
            hasNulls ? nullEvery(7) : nullptr);
      },
      common::IsNotNull(),
      5,
      [](auto& data, auto i) { return !data->isNullAt(i); });
}

TEST_F(SelectiveNimbleReaderTest, mapKeyFilter) {
  auto input = makeRowVector({makeMapVector<int64_t, double>({
      {{1, 0.5}, {2, 1.0}, {3, 1.5}, {4, 2.0}, {5, 2.5}},
      {{6, 3.0}, {7, 3.5}, {20, 10.0}},
  })});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")
      ->childByName(common::ScanSpec::kMapKeysFieldName)
      ->setFilter(std::make_unique<common::BigintRange>(4, 10, false));
  auto readers = makeReaders(input, scanSpec);
  auto expected = makeRowVector({makeMapVector<int64_t, double>({
      {{4, 2.0}, {5, 2.5}},
      {{6, 3.0}, {7, 3.5}},
  })});
  validate(*expected, *readers.rowReader, 23, [](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, mapValueFilter) {
  auto input = makeRowVector({makeMapVector<int64_t, double>({
      {{1, 0.5}, {2, 1.0}, {3, 1.5}, {4, 2.0}, {5, 2.5}},
      {{6, 3.0}, {7, 3.5}, {20, 10.0}},
  })});
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")
      ->childByName(common::ScanSpec::kMapValuesFieldName)
      ->setFilter(
          std::make_unique<common::DoubleRange>(
              2.0, false, false, 5.0, false, true, false));
  auto readers = makeReaders(input, scanSpec);
  auto expected = makeRowVector({makeMapVector<int64_t, double>({
      {{4, 2.0}, {5, 2.5}},
      {{6, 3.0}, {7, 3.5}},
  })});
  validate(*expected, *readers.rowReader, 23, [](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, estimatedRowSize) {
  constexpr int kSize = 13;
  auto input = makeRowVector({
      makeFlatVector<bool>(
          kSize, [](auto i) { return i % 3 != 0; }, nullEvery(5)),
      makeMapVector<int8_t, std::string>(
          kSize,
          [](auto i) { return 1 + i % 5; },
          [](auto j) { return j % 128; },
          [](auto j) { return std::to_string(j); },
          nullEvery(7)),
      makeMapVector<int32_t, double>(
          kSize,
          [](auto i) { return 1 + i % 5; },
          [](auto j) { return j % 6; },
          [](auto j) { return sin(j); },
          nullEvery(7)),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns = {"c2"};
  auto fileContent = test::createNimbleFile(*rootPool(), input, writerOptions);
  auto readers = makeReaders(input, fileContent, scanSpec);
  auto estimatedRowSize = readers.rowReader->estimatedRowSize();
  ASSERT_TRUE(estimatedRowSize.has_value());
  ASSERT_EQ(*estimatedRowSize, 102);
  {
    SCOPED_TRACE("Read flatmap as struct");
    auto c2Type =
        ROW({"1", "2", "3", "6", "4", "5"},
            {DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});
    auto outputType =
        ROW({"c0", "c1", "c2"}, {BOOLEAN(), MAP(TINYINT(), VARCHAR()), c2Type});
    auto scanSpec = std::make_shared<common::ScanSpec>("root");
    scanSpec->addAllChildFields(*outputType);
    scanSpec->childByName("c2")->setFlatMapAsStruct(true);
    auto readFile = std::make_shared<InMemoryReadFile>(fileContent);
    auto factory =
        dwio::common::getReaderFactory(dwio::common::FileFormat::NIMBLE);
    dwio::common::ReaderOptions options(pool());
    options.setScanSpec(scanSpec);
    auto reader = factory->createReader(
        std::make_unique<dwio::common::BufferedInput>(readFile, *pool()),
        options);
    dwio::common::RowReaderOptions rowOptions;
    rowOptions.setScanSpec(scanSpec);
    auto rowReader = readers.reader->createRowReader(rowOptions);
    auto estimatedRowSize = rowReader->estimatedRowSize();
    ASSERT_TRUE(estimatedRowSize.has_value());
    ASSERT_EQ(*estimatedRowSize, 62);
    auto result = BaseVector::create(outputType, 0, pool());
    int numRows = 0;
    while (rowReader->next(11, result) > 0) {
      auto* rowResult = result->asUnchecked<RowVector>();
      ASSERT_TRUE(result->type()->childAt(2)->isRow());
      ASSERT_TRUE(rowResult->childAt(2)->type()->isRow());
      auto* c2 =
          rowResult->childAt(2)->loadedVector()->asUnchecked<RowVector>();
      ASSERT_EQ(c2->childrenSize(), 6);
      ASSERT_TRUE(c2->childAt(3)->isConstantEncoding());
      ASSERT_TRUE(c2->childAt(3)->isNullAt(0));
      numRows += result->size();
    }
    ASSERT_EQ(numRows, kSize);
  }
}

TEST_F(SelectiveNimbleReaderTest, estimatedRowSizeNullableString) {
  auto input = makeRowVector({
      makeNullableFlatVector<std::string>({{"foo"}, std::nullopt, {"bar"}}),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto readers = makeReaders(input, scanSpec);
  auto estimatedRowSize = readers.rowReader->estimatedRowSize();
  ASSERT_TRUE(estimatedRowSize.has_value());
  ASSERT_EQ(*estimatedRowSize, 6);
  validate(*input, *readers.rowReader, 3, [&](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, arrayWithOffsetsLastRunFilteredOut) {
  checkArrayWithOffsets({{{1}}, {{}}, {{}}}, {false, true, true}, {1, 1, 1});
}

TEST_F(SelectiveNimbleReaderTest, arrayWithOffsetsLastRunNotLoaded) {
  checkArrayWithOffsets(
      {{{1}}, std::nullopt, {{1}}}, {false, true, true}, {1, 1, 1});
}

TEST_F(SelectiveNimbleReaderTest, arrayWithOffsetsNoSeekBackward) {
  checkArrayWithOffsets({{{1}}, std::nullopt, {{1}}}, {}, {1, 1, 1});
}

TEST_F(SelectiveNimbleReaderTest, arrayWithOffsetsLastRunResize) {
  checkArrayWithOffsets({{{1}}, {{1}}, {{}}, {{}}}, {}, {1, 2, 1});
}

TEST_F(SelectiveNimbleReaderTest, arrayWithOffsetsCopyLastRunAfterSkip) {
  checkArrayWithOffsets(
      {{{1}}, {{1}}, {{2}}, std::nullopt, {{3}}},
      {true, false, false, true, true},
      {1, 2, 1, 1});
}

TEST_F(SelectiveNimbleReaderTest, arrayWithOffsetsSubfieldPruning) {
  checkArrayWithOffsets(
      {{{1, 2}}, {{1, 2}}, {{1}}, std::nullopt, {{1, 2, 3}}}, {}, {1, 1, 3}, 1);
}

TEST_F(SelectiveNimbleReaderTest, arrayWithOffsetsLastRunFilteredOutAfterRead) {
  checkArrayWithOffsets(
      {{{1}}, {{1}}}, {false, true}, {1, 1}, std::nullopt, true);
}

TEST_F(SelectiveNimbleReaderTest, arrayWithOffsetsReuseNullResult) {
  auto vector = makeRowVector({
      std::make_shared<MapVector>(
          pool(),
          MAP(BIGINT(), ARRAY(BIGINT())),
          nullptr,
          4,
          makeIndices({0, 2, 4, 6}),
          makeIndices({2, 2, 2, 2}),
          makeFlatVector<int64_t>({1, 2, 1, 2, 1, 2, 1, 2}),
          makeNullableArrayVector<int64_t>({
              {std::nullopt, std::nullopt},
              {std::optional(3)},
              {std::nullopt, std::nullopt},
              {std::optional(3)},
              {std::nullopt},
              {std::optional(3)},
              {std::nullopt},
              {std::optional(4)},
          })),
  });
  VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns = {"c0"};
  writerOptions.dictionaryArrayColumns = {"c0"};
  auto fileContent = test::createNimbleFile(*rootPool(), vector, writerOptions);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*vector->type());
  auto readers = makeReaders(vector, fileContent, scanSpec);
  validate(*vector, *readers.rowReader, 2, [](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, arrayWithOffsetsLastRowSetLifeCycle) {
  std::vector<std::optional<std::vector<std::optional<int64_t>>>> c0, c1, c2;
  // First batch, one row after filtering, and allocate outputRows_ with a
  // smaller size.
  for (int i = 0; i < 16; ++i) {
    c0.emplace_back(std::nullopt);
    c1.push_back({{1}});
    c2.push_back({{}});
  }
  c0.push_back({{}});
  c1.push_back({{1}});
  c2.push_back({{}});
  // Second batch, all filtered out by c2, but c1 reads some value without
  // calling getValues.
  c0.emplace_back(std::nullopt);
  // Add a null to force setComplexNulls to read last row set before batch 3.
  c1.emplace_back(std::nullopt);
  c2.push_back({{}});
  for (int i = 0; i < 15; ++i) {
    c0.emplace_back(std::nullopt);
    c1.push_back({{2}});
    c2.push_back({{}});
  }
  c0.push_back({{}});
  c1.push_back({{2}});
  c2.emplace_back(std::nullopt);
  // Third batch, nothing is filtered out, and force outputRows_ buffer
  // reallocation.
  for (int i = 0; i < 17; ++i) {
    c0.push_back({{}});
    c1.push_back({{2}});
    c2.push_back({{}});
  }
  auto vector = makeRowVector({
      makeNullableArrayVector<int64_t>(c0),
      makeRowVector(
          {"c1c0"},
          {makeNullableArrayVector<int64_t>(c1)},
          // Add some nulls so it is not lazy.
          [](auto i) { return i == 0; }),
      makeNullableArrayVector<int64_t>(c2),
  });
  VeloxWriterOptions writerOptions;
  writerOptions.dictionaryArrayColumns = {"c1"};
  auto fileContent = test::createNimbleFile(*rootPool(), vector, writerOptions);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*vector->type());
  scanSpec->childByName("c0")->setFilter(std::make_shared<common::IsNotNull>());
  scanSpec->childByName("c1")->setFilter(std::make_shared<common::IsNotNull>());
  scanSpec->childByName("c2")->setFilter(std::make_shared<common::IsNotNull>());
  scanSpec->disableStatsBasedFilterReorder();
  auto readers = makeReaders(vector, fileContent, scanSpec);
  validate(*vector, *readers.rowReader, 17, [](auto i) {
    return i == 16 || i >= 34;
  });
}

TEST_F(SelectiveNimbleReaderTest, slidingWindowMapSubfieldPruning) {
  common::BigintRange keyFilter(2, 2, false);
  checkSlidingWindowMap(
      {
          {{{1, {1}}, {2, {2}}}},
          {{{1, {1}}, {2, {2}}}},
          {{{2, {3}}}},
          {{{2, {3}}}},
          std::nullopt,
          {{{1, {4}}, {2, {5}}, {3, {6}}}},
      },
      {},
      &keyFilter,
      {1, 1, 2, 2});
}

TEST_F(SelectiveNimbleReaderTest, slidingWindowMapLengthDedup) {
  checkSlidingWindowMap(
      {
          {{{1, {2}}}},
          {{}},
          {{{3, {4}}}},
          {{}},
          {{{5, {6}}}},
      },
      {},
      nullptr,
      {3, 1, 1});
}

TEST_F(SelectiveNimbleReaderTest, slidingWindowMapLastRunFilteredOutAfterRead) {
  checkSlidingWindowMap(
      {{{{1, {2}}}}, {{{1, {2}}}}}, {false, true}, nullptr, {1, 1}, true);
}

TEST_F(SelectiveNimbleReaderTest, schemaEvolutionRealToDouble) {
  runTestCase(
      [&](bool hasNulls) {
        return makeFlatVector<float>(
            101,
            [](auto i) { return sin(i); },
            hasNulls ? nullEvery(11) : nullptr);
      },
      common::DoubleRange(-INFINITY, true, false, 0.5, false, false, false),
      23,
      [&](auto& data, auto i) { return !data->isNullAt(i) && sin(i) <= 0.5; },
      [&](bool hasNulls) {
        return makeFlatVector<double>(
            101,
            [](auto i) { return static_cast<float>(sin(i)); },
            hasNulls ? nullEvery(11) : nullptr);
      });
}

TEST_F(SelectiveNimbleReaderTest, schemaEvolutionBoolToTinyint) {
  runTestCase(
      [&](bool hasNulls) {
        return makeFlatVector<bool>(
            101,
            [](auto i) { return i % 3 == 0; },
            hasNulls ? nullEvery(11) : nullptr);
      },
      common::BigintRange(1, 100, false),
      23,
      [&](auto& data, auto i) { return !data->isNullAt(i) && i % 3 == 0; },
      [&](bool hasNulls) {
        return makeFlatVector<int8_t>(
            101,
            [](auto i) { return i % 3 == 0; },
            hasNulls ? nullEvery(11) : nullptr);
      });
}

TEST_F(SelectiveNimbleReaderTest, schemaEvolutionBoolToBigint) {
  runTestCase(
      [&](bool hasNulls) {
        return makeFlatVector<bool>(
            101,
            [](auto i) { return i % 3 == 0; },
            hasNulls ? nullEvery(11) : nullptr);
      },
      common::BigintRange(1, 100, false),
      23,
      [&](auto& data, auto i) { return !data->isNullAt(i) && i % 3 == 0; },
      [&](bool hasNulls) {
        return makeFlatVector<int64_t>(
            101,
            [](auto i) { return i % 3 == 0; },
            hasNulls ? nullEvery(11) : nullptr);
      });
}

TEST_F(SelectiveNimbleReaderTest, schemaEvolutionTinyintToBigint) {
  runTestCase(
      [&](bool hasNulls) {
        return makeFlatVector<int8_t>(
            101, [](auto i) { return i; }, hasNulls ? nullEvery(11) : nullptr);
      },
      common::BigintRange(20, 80, false),
      23,
      [&](auto& data, auto i) {
        return !data->isNullAt(i) && 20 <= i && i <= 80;
      },
      [&](bool hasNulls) {
        return makeFlatVector<int64_t>(
            101, [](auto i) { return i; }, hasNulls ? nullEvery(11) : nullptr);
      });
}

TEST_F(SelectiveNimbleReaderTest, schemaEvolutionIntegerToBigint) {
  runTestCase(
      [&](bool hasNulls) {
        return makeFlatVector<int32_t>(
            101, [](auto i) { return i; }, hasNulls ? nullEvery(11) : nullptr);
      },
      common::BigintRange(20, 80, false),
      23,
      [&](auto& data, auto i) {
        return !data->isNullAt(i) && 20 <= i && i <= 80;
      },
      [&](bool hasNulls) {
        return makeFlatVector<int64_t>(
            101, [](auto i) { return i; }, hasNulls ? nullEvery(11) : nullptr);
      });
}

TEST_F(SelectiveNimbleReaderTest, schemaEvolutionAddPrimitiveField) {
  runTestCase(
      [&](bool hasNulls) {
        return makeRowVector(
            {
                makeFlatVector<int32_t>(101, folly::identity),
            },
            hasNulls ? nullEvery(11) : nullptr);
      },
      common::IsNotNull(),
      23,
      [&](auto& data, auto i) { return !data->isNullAt(i); },
      [&](bool hasNulls) {
        return makeRowVector(
            {
                makeFlatVector<int32_t>(101, folly::identity),
                makeNullConstant(TypeKind::BIGINT, 101),
            },
            hasNulls ? nullEvery(11) : nullptr);
      });
}

TEST_F(SelectiveNimbleReaderTest, schemaEvolutionAddComplexField) {
  runTestCase(
      [&](bool hasNulls) {
        return makeRowVector(
            {
                makeFlatVector<int32_t>(101, folly::identity),
            },
            hasNulls ? nullEvery(11) : nullptr);
      },
      common::IsNotNull(),
      23,
      [&](auto& data, auto i) { return !data->isNullAt(i); },
      [&](bool hasNulls) {
        return makeRowVector(
            {
                makeFlatVector<int32_t>(101, folly::identity),
                BaseVector::createNullConstant(ARRAY(BIGINT()), 101, pool()),
            },
            hasNulls ? nullEvery(11) : nullptr);
      });
}

TEST_F(SelectiveNimbleReaderTest, schemaEvolutionAddNestedStruct) {
  runTestCase(
      [&](bool hasNulls) {
        return makeRowVector(
            {
                makeFlatVector<int32_t>(101, folly::identity),
                makeRowVector({
                    makeFlatVector<int32_t>(101, folly::identity),
                }),
            },
            hasNulls ? nullEvery(11) : nullptr);
      },
      common::IsNotNull(),
      23,
      [&](auto& data, auto i) { return !data->isNullAt(i); },
      [&](bool hasNulls) {
        return makeRowVector(
            {
                makeFlatVector<int32_t>(101, folly::identity),
                makeRowVector({
                    makeFlatVector<int32_t>(101, folly::identity),
                    makeNullConstant(TypeKind::REAL, 101),
                }),
                BaseVector::createNullConstant(
                    ROW({"c0"}, {REAL()}), 101, pool()),
            },
            hasNulls ? nullEvery(11) : nullptr);
      });
}

TEST_F(SelectiveNimbleReaderTest, schemaEvolutionFilterOnMissingSubfield) {
  // Toplevel missing fields are checked and the whole file is potentially
  // skipped in SplitReader::filterOnStats.  Maybe we should check subfields
  // there as well to avoid unnecessary IO.  For now let's make sure subfield is
  // handled in file reader.
  auto input = makeRowVector({
      makeFlatVector<int32_t>(101, folly::identity),
  });
  auto file = test::createNimbleFile(*rootPool(), input);
  auto expected = makeRowVector({
      makeFlatVector<int32_t>(101, folly::identity),
      BaseVector::createNullConstant(ROW({"c1c0"}, {BIGINT()}), 101, pool()),
  });
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*expected->type());
  auto* c1 = scanSpec->childByName("c1");
  c1->setConstantValue(
      BaseVector::createNullConstant(ROW({"c1c0"}, {BIGINT()}), 1, pool()));
  auto* c1c0 = c1->childByName("c1c0");
  c1c0->setFilter(std::make_unique<common::BigintRange>(0, 50, false));
  auto readers = makeReaders(expected, file, scanSpec);
  validate(*expected, *readers.rowReader, 101, [&](auto) { return false; });
  c1c0->setFilter(std::make_unique<common::BigintRange>(0, 50, true));
  readers = makeReaders(expected, file, scanSpec);
  validate(*expected, *readers.rowReader, 101, [&](auto) { return true; });
}

TEST_F(SelectiveNimbleReaderTest, nativeFlatMap) {
  // Roundtrip test that takes a flat map vector and writes it to a file as a
  // storage flat map. Then reads the storage flat map as a flat map vector, and
  // compares with the initial input.
  auto testRoundtrip = [&](const FlatMapVectorPtr& inputFlatMap) {
    auto input = makeRowVector({inputFlatMap, inputFlatMap->toMapVector()});

    VeloxWriterOptions writerOptions;
    writerOptions.flatMapColumns = {"c0"};
    auto fileContent =
        test::createNimbleFile(*rootPool(), input, writerOptions);
    auto scanSpec = std::make_shared<common::ScanSpec>("root");
    scanSpec->addAllChildFields(*input->type());
    auto readers = makeReaders(input, fileContent, scanSpec, true);

    // We invert the order on purpose to cross-compare MapVector with
    // FlatMapVectors; logically they are the same.
    auto expected = makeRowVector({inputFlatMap->toMapVector(), inputFlatMap});
    validate(*expected, *readers.rowReader, inputFlatMap->size(), [](auto) {
      return true;
    });
  };

  testRoundtrip(makeFlatMapVector<int32_t, int32_t>({}));
  testRoundtrip(makeFlatMapVector<int32_t, int32_t>({{}}));
  testRoundtrip(
      makeFlatMapVector<int16_t, float>({
          {{1, 300}},
          {{1, 400}},
          {{2, 20}},
          {{2, 30}},
          {{2, 40}},
          {{2, 50}},
          {{2, 60}},
      }));
  testRoundtrip(
      makeFlatMapVector<int16_t, float>({
          {},
          {{1, 1.9}, {2, 2.1}, {0, 3.12}},
          {{127, 0.12}},
      }));

  testRoundtrip(
      makeFlatMapVector<int16_t, float>({
          {},
          {{1, 1.9}, {2, 2.1}, {0, 3.12}},
          {{127, 0.12}},
      }));

  testRoundtrip(
      makeFlatMapVector<StringView, StringView>({
          {{"a", "a1"}},
          {{"b", "b1"}},
          {{"c", "c1"}},
          {{"d", "d1"}},
      }));

  testRoundtrip(
      makeNullableFlatMapVector<int32_t, int32_t>({
          {{{101, 1}, {102, 2}, {103, 3}}},
          {{{105, 0}, {106, 0}}},
          {std::nullopt},
          {{{101, 11}, {103, 13}, {105, std::nullopt}}},
          {{{101, 1}, {102, 2}, {103, 3}}},
      }));

  auto constructFlatMap = [&](VectorPtr keys) {
    return std::make_shared<FlatMapVector>(
        pool(),
        MAP(INTEGER(), INTEGER()),
        nullptr,
        3,
        keys,
        std::vector<VectorPtr>{
            makeFlatVector<int32_t>({1, 2, 3}),
            makeFlatVector<int32_t>({4, 5, 6}),
            makeFlatVector<int32_t>({7, 8, 9})},
        std::vector<BufferPtr>{nullptr, nullptr, nullptr});
  };

  // Dictionary wrapped keys
  {
    LOG(INFO) << "Dictionary wrapped keys";
    testRoundtrip(constructFlatMap(
        BaseVector::wrapInDictionary(
            nullptr,
            makeIndices({0, 1, 2}),
            3,
            makeFlatVector<int32_t>({4, 5, 6}))));

    testRoundtrip(constructFlatMap(
        BaseVector::wrapInDictionary(
            nullptr,
            makeIndices({2, 0, 1}),
            3,
            makeFlatVector<int32_t>({4, 5, 6}))));

    // Difference in cardinality
    testRoundtrip(constructFlatMap(
        BaseVector::wrapInDictionary(
            nullptr,
            makeIndices({1, 3, 5}),
            3,
            makeFlatVector<int32_t>({4, 5, 6, 7, 8, 9}))));

    VELOX_ASSERT_THROW(
        testRoundtrip(constructFlatMap(
            BaseVector::wrapInDictionary(
                nullptr,
                makeIndices({2, 1, 1}),
                3,
                makeFlatVector<int32_t>({4, 5, 6})))),
        "FlatMapVector keys are not distinct.");
  }

  // Constant wrapped keys
  {
    VELOX_ASSERT_THROW(
        testRoundtrip(constructFlatMap(
            BaseVector::wrapInConstant(
                3, 0, makeFlatVector<int32_t>({1, 2, 3})))),
        "FlatMapVector keys are not distinct.");
  }
}

TEST_F(SelectiveNimbleReaderTest, mapAsStruct) {
  auto input = makeRowVector({
      makeMapVector<int32_t, int64_t>({{{1, 4}, {2, 5}}, {{1, 6}, {3, 7}}}),
  });
  auto outType = ROW({"c0"}, {ROW({"3", "1"}, BIGINT())});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*outType);
  spec->childByName("c0")->setFlatMapAsStruct(true);
  auto readers = makeReaders(input, spec);
  VectorPtr batch = BaseVector::create(outType, 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, batch), 2);
  auto expected = makeRowVector({
      makeRowVector(
          {"3", "1"},
          {
              makeNullableFlatVector<int64_t>({std::nullopt, 7}),
              makeFlatVector<int64_t>({4, 6}),
          }),
  });
  velox::test::assertEqualVectors(expected, batch);
}

TEST_F(SelectiveNimbleReaderTest, mapAsStructFilterAfterRead) {
  auto input = makeRowVector({
      makeMapVector<int32_t, int64_t>({{{1, 4}, {2, 5}}, {}, {{1, 6}, {3, 7}}}),
      makeRowVector(
          {makeConstant<int64_t>(0, 3)}, [](auto i) { return i == 0; }),
  });
  auto outType =
      ROW({"c0", "c1"}, {ROW({"3", "1"}, BIGINT()), ROW({"c0"}, BIGINT())});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*outType);
  auto* c0Spec = spec->childByName("c0");
  c0Spec->setFlatMapAsStruct(true);
  c0Spec->setFilter(std::make_shared<common::IsNotNull>());
  spec->childByName("c1")->setFilter(std::make_shared<common::IsNotNull>());
  auto readers = makeReaders(input, spec);
  VectorPtr batch = BaseVector::create(outType, 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, batch), 3);
  auto expected = makeRowVector({
      makeRowVector(
          {"3", "1"},
          {
              makeNullableFlatVector<int64_t>({std::nullopt, 7}),
              makeNullableFlatVector<int64_t>({std::nullopt, 6}),
          }),
      makeRowVector({makeConstant<int64_t>(0, 2)}),
  });
  velox::test::assertEqualVectors(expected, batch);
}

TEST_F(SelectiveNimbleReaderTest, mapAsStructAllEmpty) {
  auto input = makeRowVector({makeMapVector<int32_t, int64_t>({{}, {}})});
  auto outType = ROW({"c0"}, {ROW({"1"}, BIGINT())});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*outType);
  spec->childByName("c0")->setFlatMapAsStruct(true);
  auto readers = makeReaders(input, spec);
  VectorPtr batch = BaseVector::create(outType, 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, batch), 2);
  auto expected = makeRowVector({
      makeRowVector({"1"}, {makeNullConstant(TypeKind::BIGINT, 2)}),
  });
  velox::test::assertEqualVectors(expected, batch);
}

TEST_F(SelectiveNimbleReaderTest, mapAsStructAllNulls) {
  auto input = makeRowVector(
      {makeNullableMapVector<int32_t, int64_t>({std::nullopt, std::nullopt})});
  auto outType = ROW({"c0"}, {ROW({"1"}, BIGINT())});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*outType);
  spec->childByName("c0")->setFlatMapAsStruct(true);
  auto readers = makeReaders(input, spec);
  VectorPtr batch = BaseVector::create(outType, 0, pool());
  ASSERT_EQ(readers.rowReader->next(10, batch), 2);
  auto expected = makeRowVector({
      BaseVector::createNullConstant(ROW({"1"}, BIGINT()), 2, pool()),
  });
  velox::test::assertEqualVectors(expected, batch);
}

} // namespace
} // namespace facebook::nimble
