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

#include "dwio/nimble/velox/VeloxWriter.h"
#include "dwio/nimble/velox/selective/SelectiveNimbleReader.h"
#include "velox/dwio/common/tests/utils/E2EFilterTestBase.h"

namespace facebook::nimble {
namespace {

using namespace facebook::velox;

class E2EFilterTest : public dwio::common::E2EFilterTestBase {
 protected:
  static void SetUpTestCase() {
    E2EFilterTestBase::SetUpTestCase();
    registerSelectiveNimbleReaderFactory();
  }

  void SetUp() final {
    E2EFilterTestBase::SetUp();
    batchSize_ = 2003;
    batchCount_ = 5;
    testRowGroupSkip_ = false;
  }

  void testWithTypes(
      const std::string& columns,
      std::function<void()> customize,
      bool wrapInStruct,
      const std::vector<std::string>& filterable,
      int32_t numCombinations,
      bool withRecursiveNulls = true) {
    if (withRecursiveNulls) {
      testScenario(
          columns,
          customize,
          wrapInStruct,
          filterable,
          numCombinations,
          /*withRecursiveNulls=*/true);
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
          /*withRecursiveNulls=*/true);
    } else {
      testScenario(
          columns,
          customize,
          wrapInStruct,
          filterable,
          numCombinations,
          /*withRecursiveNulls=*/false);
    }
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
      bool forRowGroupSkip) final {
    VELOX_CHECK(!forRowGroupSkip);
    rowType_ = asRowType(type);
    writeSchema_ = rowType_;
    VeloxWriterOptions options;
    options.enableChunking = true;
    options.flushPolicyFactory = [] {
      return std::make_unique<LambdaFlushPolicy>(
          [i = 0](const StripeProgress&) mutable {
            if (i++ % 3 == 2) {
              return FlushDecision::Stripe;
            }
            return FlushDecision::Chunk;
          });
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
    auto writeFile = std::make_unique<InMemoryWriteFile>(&sinkData_);
    VeloxWriter writer(
        *rootPool_, writeSchema_, std::move(writeFile), std::move(options));
    for (auto& batch : batches) {
      writer.write(batch);
    }
    writer.close();
  }

  std::unique_ptr<dwio::common::Reader> makeReader(
      const dwio::common::ReaderOptions& opts,
      std::unique_ptr<dwio::common::BufferedInput> input) final {
    auto factory =
        dwio::common::getReaderFactory(dwio::common::FileFormat::NIMBLE);
    return factory->createReader(std::move(input), opts);
  }

  folly::F14FastSet<std::string> flatMapColumns_;
  folly::F14FastSet<std::string> deduplicatedArrayColumns_;
  folly::F14FastSet<std::string> deduplicatedMapColumns_;

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

TEST_F(E2EFilterTest, byte) {
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

TEST_F(E2EFilterTest, integer) {
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

TEST_F(E2EFilterTest, integerLowCardinality) {
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

TEST_F(E2EFilterTest, integerRle) {
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

TEST_F(E2EFilterTest, integerRleCustomSeed_4049269257) {
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

TEST_F(E2EFilterTest, integerRleCustomSeed_583694982) {
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

TEST_F(E2EFilterTest, integerRleCustomSeed_2518626933) {
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

TEST_F(E2EFilterTest, mainlyConstant) {
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

TEST_F(E2EFilterTest, float) {
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

TEST_F(E2EFilterTest, string) {
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

TEST_F(E2EFilterTest, stringDictionary) {
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

TEST_F(E2EFilterTest, listAndMapNoRecursiveNulls) {
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

TEST_F(E2EFilterTest, listAndMap) {
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

TEST_F(E2EFilterTest, listAndMapSmallReads) {
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

TEST_F(E2EFilterTest, DeduplicatedArrayNoRecursiveNulls) {
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

TEST_F(E2EFilterTest, DeduplicatedArray) {
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
TEST_F(E2EFilterTest, DeduplicatedArraySubfieldPruning) {
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

TEST_F(E2EFilterTest, DeduplicatedArraySmallReads) {
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
TEST_F(E2EFilterTest, DeduplicatedArrayCustomSeed_ConnectedSkips) {
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
TEST_F(E2EFilterTest, DeduplicatedArrayCustomSeed_CachedRunOnly) {
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

TEST_F(E2EFilterTest, DeduplicatedMapNoRecursiveNulls) {
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

TEST_F(E2EFilterTest, DeduplicatedMap) {
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
TEST_F(E2EFilterTest, DeduplicatedMapSubfieldPruning) {
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

TEST_F(E2EFilterTest, nullCompactRanges) {
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

TEST_F(E2EFilterTest, lazyStruct) {
  testWithTypes(
      "long_val:bigint,"
      "outer_struct: struct<nested1:bigint, "
      "inner_struct: struct<nested2: bigint>>",
      [&]() {},
      true,
      {"long_val"},
      10);
}

TEST_F(E2EFilterTest, filterStruct) {
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
TEST_F(E2EFilterTest, DISABLED_flatMapAsStruct) {
  constexpr auto kColumns =
      "long_val:bigint,"
      "long_vals:struct<v1:bigint,v2:bigint,v3:bigint>,"
      "struct_vals:struct<nested1:struct<v1:bigint, v2:float>,nested2:struct<v1:bigint, v2:float>>";
  flatMapColumns_ = {"long_vals", "struct_vals"};
  testWithTypes(kColumns, [] {}, false, {"long_val"}, 10);
}

TEST_F(E2EFilterTest, flatMapScalar) {
  constexpr auto kColumns =
      "long_val:bigint,"
      "long_vals:map<tinyint,bigint>,"
      "string_vals:map<string,string>";
  flatMapColumns_ = {"long_vals", "string_vals"};
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

TEST_F(E2EFilterTest, flatMapComplexNoRecursiveNulls) {
  constexpr auto kColumns =
      "long_val:bigint,"
      "struct_vals:map<varchar,struct<v1:bigint, v2:float>>,"
      "array_vals:map<tinyint,array<int>>";
  flatMapColumns_ = {"struct_vals", "array_vals"};
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

TEST_F(E2EFilterTest, flatMapComplex) {
  constexpr auto kColumns =
      "long_val:bigint,"
      "struct_vals:map<varchar,struct<v1:bigint, v2:float>>,"
      "array_vals:map<tinyint,array<int>>";
  flatMapColumns_ = {"struct_vals", "array_vals"};
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

TEST_F(E2EFilterTest, mutationCornerCases) {
  testMutationCornerCases();
}

} // namespace
} // namespace facebook::nimble
