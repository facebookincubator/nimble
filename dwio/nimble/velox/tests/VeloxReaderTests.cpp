/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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
#include <gtest/gtest.h>
#include <cmath>
#include <optional>
#include <stdexcept>
#include <string_view>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/common/tests/NimbleFileWriter.h"
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "dwio/nimble/velox/VeloxReader.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "folly/FileUtil.h"
#include "folly/Random.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "velox/common/base/BitUtil.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/type/CppToType.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/NullsBuilder.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace ::facebook;

DEFINE_string(
    output_test_file_path,
    "",
    "If provided, files created during tests will be writtern to this path. "
    "Each test will overwrite the previous file, so this is mainly useful when a single test is executed.");

DEFINE_uint32(
    reader_tests_seed,
    0,
    "If provided, this seed will be used when executing tests. "
    "Otherwise, a random seed will be used.");

namespace {
struct VeloxMapGeneratorConfig {
  // A RowType containing a group of map feature column types.
  std::shared_ptr<const velox::RowType> featureTypes;

  velox::TypeKind keyType;

  std::string stringKeyPrefix = "test_";

  // Maximum number of key value pairs per row in a map column.
  uint32_t maxNumKVPerRow = 10;

  // If true, produces map columns containing different number of key value
  // pairs in each row. Otherwise, every row will have 'maxNumKVPerRow' key
  // value pairs.
  bool variantNumKV = true;

  unsigned long seed = FLAGS_reader_tests_seed > 0 ? FLAGS_reader_tests_seed
                                                   : folly::Random::rand32();
  bool hasNulls = true;

  // TODO: Put VeloxFuzzer::Options directly inside VeloxMapGeneratorConfig.
  // Length of generated string field.
  uint32_t stringLength = 20;

  // If true, generated string field will have variable length, maxing at
  // 'stringLength'.
  bool stringVariableLength = true;
};

// Generates a RowVector containing a set of feature MapVector with
// fixed set of keys in each row.
class VeloxMapGenerator {
 public:
  VeloxMapGenerator(
      velox::memory::MemoryPool* pool,
      VeloxMapGeneratorConfig config)
      : leafPool_{pool}, config_{config}, rng_(config_.seed), buffer_(*pool) {
    LOG(INFO) << "seed: " << config_.seed;
  }

  velox::VectorPtr generateBatch(velox::vector_size_t numRows) {
    auto offsets = velox::allocateOffsets(numRows, leafPool_);
    auto rawOffsets = offsets->template asMutable<velox::vector_size_t>();
    auto sizes = velox::allocateSizes(numRows, leafPool_);
    auto rawSizes = sizes->template asMutable<velox::vector_size_t>();
    velox::vector_size_t childSize = 0;
    for (auto i = 0; i < numRows; ++i) {
      rawOffsets[i] = childSize;
      auto numMapKV = config_.variantNumKV
          ? folly::Random::rand32(rng_) % (config_.maxNumKVPerRow + 1)
          : config_.maxNumKVPerRow;
      rawSizes[i] = numMapKV;
      childSize += numMapKV;
    }

    // create keys
    auto keys = generateKeys(numRows, childSize, rawSizes);
    auto offset = 0;

    // encode keys
    if (folly::Random::oneIn(2, rng_)) {
      offset = 0;
      auto indices = velox::AlignedBuffer::allocate<velox::vector_size_t>(
          childSize, leafPool_);
      auto rawIndices = indices->asMutable<velox::vector_size_t>();
      for (auto i = 0; i < numRows; ++i) {
        auto mapSize = rawSizes[i];
        for (auto j = 0; j < mapSize; ++j) {
          rawIndices[offset + j] = offset + mapSize - j - 1;
        }
        offset += mapSize;
      }
      keys = velox::BaseVector::wrapInDictionary(
          nullptr, indices, childSize, keys);
    }

    velox::VectorFuzzer fuzzer(
        {
            .vectorSize = static_cast<size_t>(childSize),
            .nullRatio = config_.hasNulls ? 0.1 : 0,
            .dictionaryHasNulls = config_.hasNulls,
            .stringLength = config_.stringLength,
            .stringVariableLength = config_.stringVariableLength,
            .containerLength = 5,
            .containerVariableLength = true,
        },
        leafPool_,
        config_.seed);

    // Generate a random null vector.
    velox::NullsBuilder builder{numRows, leafPool_};
    if (config_.hasNulls) {
      for (auto i = 0; i < numRows; ++i) {
        if (folly::Random::oneIn(10, rng_)) {
          builder.setNull(i);
        }
      }
    }
    auto nulls = builder.build();
    std::vector<velox::VectorPtr> children;
    for (auto& featureColumn : config_.featureTypes->children()) {
      velox::VectorPtr map = std::make_shared<velox::MapVector>(
          leafPool_,
          featureColumn,
          nulls,
          numRows,
          offsets,
          sizes,
          keys,
          fuzzer.fuzz(featureColumn->asMap().valueType()));
      // Encode map
      if (folly::Random::oneIn(2, rng_)) {
        map = fuzzer.fuzzDictionary(map);
      }
      children.push_back(map);
    }

    return std::make_shared<velox::RowVector>(
        leafPool_, config_.featureTypes, nullptr, numRows, std::move(children));
  }

  std::mt19937& rng() {
    return rng_;
  }

 private:
  std::shared_ptr<velox::BaseVector> generateKeys(
      velox::vector_size_t numRows,
      velox::vector_size_t childSize,
      velox::vector_size_t* rawSizes) {
    switch (config_.keyType) {
#define SCALAR_CASE(veloxKind, cppType)                                      \
  case velox::TypeKind::veloxKind: {                                         \
    auto keys =                                                              \
        velox::BaseVector::create(velox::veloxKind(), childSize, leafPool_); \
    auto rawKeyValues = keys->asFlatVector<cppType>()->mutableRawValues();   \
    auto offset = 0;                                                         \
    for (auto i = 0; i < numRows; ++i) {                                     \
      for (auto j = 0; j < rawSizes[i]; ++j) {                               \
        rawKeyValues[offset++] = folly::to<cppType>(j);                      \
      }                                                                      \
    }                                                                        \
    return keys;                                                             \
  }
      SCALAR_CASE(TINYINT, int8_t)
      SCALAR_CASE(SMALLINT, int16_t)
      SCALAR_CASE(INTEGER, int32_t)
      SCALAR_CASE(BIGINT, int64_t)

#undef SCALAR_CASE
      case velox::TypeKind::VARCHAR: {
        auto keys =
            velox::BaseVector::create(velox::VARCHAR(), childSize, leafPool_);
        auto flatVector = keys->asFlatVector<velox::StringView>();
        auto offset = 0;
        for (auto i = 0; i < numRows; ++i) {
          for (auto j = 0; j < rawSizes[i]; ++j) {
            auto key = config_.stringKeyPrefix + folly::to<std::string>(j);
            flatVector->set(
                offset++, {key.data(), static_cast<int32_t>(key.size())});
          }
        }
        return keys;
      }
      default:
        NIMBLE_NOT_SUPPORTED("Unsupported Key Type");
    }
  }

  velox::memory::MemoryPool* leafPool_;
  VeloxMapGeneratorConfig config_;
  std::mt19937 rng_;
  nimble::Buffer buffer_;
};

template <typename T>
void fillKeysVector(
    velox::VectorPtr& vector,
    velox::vector_size_t offset,
    T& key) {
  auto flatVectorMutable =
      static_cast<velox::FlatVector<T>&>(*vector).mutableRawValues();
  flatVectorMutable[offset] = key;
}

template <typename T>
std::string getStringKey(T key) {
  return folly::to<std::string>(key);
}

template <>
std::string getStringKey(velox::StringView key) {
  return std::string(key);
}

// utility function to convert an input Map velox::VectorPtr to outVector if
// isKeyPresent
template <typename T>
void filterFlatMap(
    const velox::VectorPtr& vector,
    velox::VectorPtr& outVector,
    std::function<bool(std::string& key)> isKeyPresent) {
  auto mapVector = vector->as<velox::MapVector>();
  auto offsets = mapVector->rawOffsets();
  auto sizes = mapVector->rawSizes();
  auto keysVector = mapVector->mapKeys()->asFlatVector<T>();
  auto valuesVector = mapVector->mapValues();

  if (outVector == nullptr) {
    outVector = velox::BaseVector::create(
        vector->type(), vector->size(), vector->pool());
  }
  auto resultVector = outVector->as<velox::MapVector>();
  auto newKeysVector = resultVector->mapKeys();
  velox::VectorPtr newValuesVector = velox::BaseVector::create(
      mapVector->mapValues()->type(), 0, mapVector->pool());
  auto* offsetsPtr = resultVector->mutableOffsets(vector->size())
                         ->asMutable<velox::vector_size_t>();
  auto* lengthsPtr = resultVector->mutableSizes(vector->size())
                         ->asMutable<velox::vector_size_t>();
  newKeysVector->resize(keysVector->size());
  newValuesVector->resize(valuesVector->size());
  resultVector->setNullCount(vector->size());

  velox::vector_size_t offset = 0;
  for (velox::vector_size_t index = 0; index < mapVector->size(); ++index) {
    offsetsPtr[index] = offset;
    if (!mapVector->isNullAt(index)) {
      resultVector->setNull(index, false);
      for (velox::vector_size_t i = offsets[index];
           i < offsets[index] + sizes[index];
           ++i) {
        auto keyValue = keysVector->valueAtFast(i);
        auto&& stringKeyValue = getStringKey(keyValue);
        if (isKeyPresent(stringKeyValue)) {
          fillKeysVector(newKeysVector, offset, keyValue);
          newValuesVector->copy(valuesVector.get(), offset, i, 1);
          ++offset;
        }
      }
    } else {
      resultVector->setNull(index, true);
    }
    lengthsPtr[index] = offset - offsetsPtr[index];
  }

  newKeysVector->resize(offset, false);
  newValuesVector->resize(offset, false);
  resultVector->setKeysAndValues(
      std::move(newKeysVector), std::move(newValuesVector));
}

// compare two map vector, where expected map will be converted a new vector
// based on isKeyPresent Functor
template <typename T>
void compareFlatMapAsFilteredMap(
    velox::VectorPtr expected,
    velox::VectorPtr actual,
    std::function<bool(std::string&)> isKeyPresent) {
  auto flat = velox::BaseVector::create(
      expected->type(), expected->size(), expected->pool());
  flat->copy(expected.get(), 0, 0, expected->size());
  auto expectedRow = flat->as<velox::RowVector>();
  auto actualRow = actual->as<velox::RowVector>();
  EXPECT_EQ(expectedRow->childrenSize(), actualRow->childrenSize());
  for (auto i = 0; i < expectedRow->childrenSize(); ++i) {
    velox::VectorPtr outVector;
    filterFlatMap<T>(expectedRow->childAt(i), outVector, isKeyPresent);
    for (int j = 0; j < outVector->size(); j++) {
      ASSERT_TRUE(outVector->equalValueAt(actualRow->childAt(i).get(), j, j))
          << "Content mismatch at index " << j
          << "\nReference: " << outVector->toString(j)
          << "\nResult: " << actualRow->childAt(i)->toString(j);
    }
  }
}

template <typename TData, typename TRequested>
void verifyUpcastedScalars(
    const velox::VectorPtr& expected,
    uint32_t& idxInExpected,
    const velox::VectorPtr& result,
    uint32_t readSize) {
  ASSERT_TRUE(expected->isScalar() && result->isScalar());
  auto flatExpected = expected->asFlatVector<TData>();
  auto flatResult = result->asFlatVector<TRequested>();
  for (uint32_t i = 0; i < result->size(); ++i) {
    EXPECT_EQ(expected->isNullAt(idxInExpected), result->isNullAt(i))
        << "Unexpected null status. index: " << i << ", readSize: " << readSize;
    if (!result->isNullAt(i)) {
      if constexpr (
          nimble::isIntegralType<TData>() || nimble::isBoolType<TData>()) {
        EXPECT_EQ(
            static_cast<TRequested>(flatExpected->valueAtFast(idxInExpected)),
            flatResult->valueAtFast(i))
            << "Unexpected value. index: " << i << ", readSize: " << readSize;
      } else {
        EXPECT_DOUBLE_EQ(
            static_cast<TRequested>(flatExpected->valueAtFast(idxInExpected)),
            flatResult->valueAtFast(i))
            << "Unexpected value. index: " << i << ", readSize: " << readSize;
      }
    }
    ++idxInExpected;
  }
}

size_t streamsReadCount(
    velox::memory::MemoryPool& pool,
    velox::ReadFile* readFile,
    const std::vector<nimble::testing::Chunk>& chunks) {
  // Assumed for the algorithm
  VELOX_CHECK_EQ(false, readFile->shouldCoalesce());
  nimble::TabletReader tablet(pool, readFile);
  VELOX_CHECK_GE(tablet.stripeCount(), 1);
  auto stripeIdentifier = tablet.getStripeIdentifier(0);
  auto offsets = tablet.streamOffsets(stripeIdentifier);
  std::unordered_set<uint32_t> streamOffsets;
  LOG(INFO) << "Number of streams: " << offsets.size();
  for (auto offset : offsets) {
    LOG(INFO) << "Stream offset: " << offset;
    streamOffsets.insert(offset);
  }
  size_t readCount = 0;
  auto fileSize = readFile->size();
  for (const auto [offset, size] : chunks) {
    // This is to prevent the case when the file is too small, then entire file
    // is read from 0 to the end. It can also happen that we don't read from 0
    // to the end, but just the last N bytes (a big block at the end). If that
    // read coincidently starts at the beginning of a stream, I may think that
    // I'm reading a stream. So I'm also guarding against it.
    if (streamOffsets.contains(offset) && (offset + size) != fileSize) {
      ++readCount;
    }
  }
  return readCount;
}

} // namespace

class VeloxReaderTests : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    rootPool_ = velox::memory::memoryManager()->addRootPool("default_root");
    leafPool_ = rootPool_->addLeafChild("default_leaf");
  }

  static bool vectorEquals(
      const velox::VectorPtr& expected,
      const velox::VectorPtr& actual,
      velox::vector_size_t index) {
    return expected->equalValueAt(actual.get(), index, index);
  }

  void verifyReadersEqual(
      std::unique_ptr<nimble::VeloxReader> lhs,
      std::unique_ptr<nimble::VeloxReader> rhs,
      int32_t expectedNumTotalArrays,
      int32_t expectedNumUniqueArrays) {
    velox::VectorPtr leftResult;
    velox::VectorPtr rightResult;
    // Read all arrays (if there are any)
    ASSERT_EQ(
        lhs->next(expectedNumTotalArrays, leftResult),
        expectedNumTotalArrays > 0);
    ASSERT_EQ(
        rhs->next(expectedNumTotalArrays, rightResult),
        expectedNumTotalArrays > 0);

    // If empty, leftResult & rightResult point to 0 page,
    // so we terminate early.
    if (expectedNumTotalArrays == 0) {
      return;
    }
    ASSERT_EQ(
        leftResult->wrappedVector()
            ->as<velox::RowVector>()
            ->childAt(0)
            ->loadedVector()
            ->wrappedVector()
            ->size(),
        expectedNumUniqueArrays);
    ASSERT_EQ(
        rightResult->wrappedVector()
            ->as<velox::RowVector>()
            ->childAt(0)
            ->loadedVector()
            ->wrappedVector()
            ->size(),
        expectedNumUniqueArrays);

    for (int i = 0; i < expectedNumTotalArrays; ++i) {
      ASSERT_TRUE(vectorEquals(leftResult, rightResult, i))
          << "Mismatch at index: " << i;
    }

    // Ensure no extra data floating in left/right
    ASSERT_FALSE(lhs->next(1, leftResult));
    ASSERT_FALSE(rhs->next(1, rightResult));
  }

  void testVeloxTypeFromNimbleSchema(
      velox::memory::MemoryPool& memoryPool,
      nimble::VeloxWriterOptions writerOptions,
      const velox::RowVectorPtr& vector) {
    const auto& veloxRowType =
        std::dynamic_pointer_cast<const velox::RowType>(vector->type());
    auto file =
        nimble::test::createNimbleFile(*rootPool_, vector, writerOptions);
    auto inMemFile = velox::InMemoryReadFile(file);

    nimble::VeloxReader veloxReader(
        memoryPool,
        &inMemFile,
        std::make_shared<velox::dwio::common::ColumnSelector>(veloxRowType));
    const auto& veloxTypeResult = convertToVeloxType(*veloxReader.schema());

    EXPECT_EQ(*veloxRowType, *veloxTypeResult)
        << "Expected: " << veloxRowType->toString()
        << ", actual: " << veloxTypeResult->toString();
  }

  template <typename T>
  void getFieldDefaultValue(nimble::Vector<T>& input, uint32_t index) {
    static_assert(
        T() == 0, "Default Constructor value is not zero initialized");
    input[index] = T();
  }

  template <>
  void getFieldDefaultValue(
      nimble::Vector<std::string>& input,
      uint32_t index) {
    input[index] = std::string();
  }

  template <typename T>
  void
  verifyDefaultValue(T valueToBeUpdatedWith, T defaultValue, int32_t size) {
    nimble::Vector<T> testData(leafPool_.get(), size);
    for (int i = 0; i < testData.size(); ++i) {
      getFieldDefaultValue<T>(testData, i);
      ASSERT_EQ(testData[i], defaultValue);
      testData[i] = valueToBeUpdatedWith;
      getFieldDefaultValue<T>(testData, i);
      ASSERT_EQ(testData[i], defaultValue);
    }
  }

  template <typename T>
  void testFlatMapNullValues() {
    auto type = velox::ROW(
        {{"fld", velox::MAP(velox::INTEGER(), velox::CppToType<T>::create())}});

    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

    facebook::nimble::VeloxWriterOptions writerOptions;
    writerOptions.flatMapColumns.insert("fld");

    nimble::VeloxWriter writer(
        *rootPool_, type, std::move(writeFile), std::move(writerOptions));

    facebook::velox::test::VectorMaker vectorMaker(leafPool_.get());
    auto values = vectorMaker.flatVectorNullable<T>(
        {std::nullopt, std::nullopt, std::nullopt});
    auto keys = vectorMaker.flatVector<int32_t>({1, 2, 3});
    auto vector = vectorMaker.rowVector(
        {"fld"}, {vectorMaker.mapVector({0, 1, 2}, keys, values)});

    writer.write(vector);
    writer.close();

    nimble::VeloxReadParams readParams;
    velox::InMemoryReadFile readFile(file);
    auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(type);
    nimble::VeloxReader reader(
        *leafPool_, &readFile, std::move(selector), readParams);

    velox::VectorPtr output;
    auto size = 3;
    reader.next(size, output);
    for (auto i = 0; i < size; ++i) {
      EXPECT_TRUE(vectorEquals(vector, output, i));
    }
  }

  template <typename T = int32_t>
  void writeAndVerify(
      std::mt19937& rng,
      velox::memory::MemoryPool& pool,
      const velox::RowTypePtr& type,
      std::function<velox::VectorPtr(const velox::RowTypePtr&)> generator,
      std::function<bool(
          const velox::VectorPtr&,
          const velox::VectorPtr&,
          velox::vector_size_t)> validator,
      size_t count,
      nimble::VeloxWriterOptions writerOptions = {},
      nimble::VeloxReadParams readParams = {},
      std::function<bool(std::string&)> isKeyPresent = nullptr,
      std::function<void(const velox::VectorPtr&)> comparator = nullptr,
      bool multiSkip = false,
      bool checkMemoryLeak = false) {
    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::FlushDecision decision;
    writerOptions.enableChunking = true;
    writerOptions.minStreamChunkRawSize = folly::Random::rand32(30, rng);
    writerOptions.flushPolicyFactory = [&]() {
      return std::make_unique<nimble::LambdaFlushPolicy>(
          [&](auto&) { return decision; });
    };

    std::vector<velox::VectorPtr> expected;
    nimble::VeloxWriter writer(
        *rootPool_, type, std::move(writeFile), std::move(writerOptions));
    bool perBatchFlush = folly::Random::oneIn(2, rng);
    for (auto i = 0; i < count; ++i) {
      auto vector = generator(type);
      int32_t rowIndex = 0;
      while (rowIndex < vector->size()) {
        decision = nimble::FlushDecision::None;
        auto batchSize = vector->size() - rowIndex;
        // Randomly produce chunks
        if (folly::Random::oneIn(2, rng)) {
          batchSize = folly::Random::rand32(0, batchSize, rng) + 1;
          decision = nimble::FlushDecision::Chunk;
        }
        if ((perBatchFlush || folly::Random::oneIn(5, rng)) &&
            (rowIndex + batchSize == vector->size())) {
          decision = nimble::FlushDecision::Stripe;
        }
        writer.write(vector->slice(rowIndex, batchSize));
        rowIndex += batchSize;
      }
      expected.push_back(vector);
    }
    writer.close();

    if (!FLAGS_output_test_file_path.empty()) {
      folly::writeFile(file, FLAGS_output_test_file_path.c_str());
    }

    velox::InMemoryReadFile readFile(file);
    auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(type);
    // new pool with to limit already used memory and with tracking enabled
    auto leakDetectPool =
        facebook::velox::memory::deprecatedDefaultMemoryManager().addRootPool(
            "memory_leak_detect");
    auto readerPool = leakDetectPool->addLeafChild("reader_pool");

    nimble::VeloxReader reader(
        *readerPool.get(), &readFile, selector, readParams);
    if (folly::Random::oneIn(2, rng)) {
      LOG(INFO) << "using executor";
      readParams.decodingExecutor =
          std::make_shared<folly::CPUThreadPoolExecutor>(1);
    }

    auto rootTypeFromSchema = convertToVeloxType(*reader.schema());
    EXPECT_EQ(*type, *rootTypeFromSchema)
        << "Expected: " << type->toString()
        << ", actual: " << rootTypeFromSchema->toString();

    velox::VectorPtr result;
    velox::vector_size_t numIncrements = 0, prevMemory = 0;
    for (auto i = 0; i < expected.size(); ++i) {
      auto& current = expected.at(i);
      ASSERT_TRUE(reader.next(current->size(), result));
      ASSERT_EQ(result->size(), current->size());
      if (comparator) {
        comparator(result);
      }
      if (isKeyPresent) {
        compareFlatMapAsFilteredMap<T>(current, result, isKeyPresent);
      } else {
        for (auto j = 0; j < result->size(); ++j) {
          ASSERT_TRUE(validator(current, result, j))
              << "Content mismatch at batch " << i << " at row " << j
              << "\nReference: " << current->toString(j)
              << "\nResult: " << result->toString(j);
        }
      }

      // validate skip
      if (i % 2 == 0) {
        nimble::VeloxReader reader1(pool, &readFile, selector, readParams);
        nimble::VeloxReader reader2(pool, &readFile, selector, readParams);
        auto rowCount = expected.at(0)->size();
        velox::vector_size_t remaining = rowCount;
        uint32_t skipCount = 0;
        do {
          auto toSkip = folly::Random::rand32(1, remaining, rng);
          velox::VectorPtr result1;
          velox::VectorPtr result2;
          reader1.next(toSkip, result1);
          reader2.skipRows(toSkip);
          remaining -= toSkip;

          if (remaining > 0) {
            auto toRead = folly::Random::rand32(1, remaining, rng);
            reader1.next(toRead, result1);
            reader2.next(toRead, result2);

            ASSERT_EQ(result1->size(), result2->size());

            for (auto j = 0; j < result1->size(); ++j) {
              ASSERT_TRUE(vectorEquals(result1, result2, j))
                  << "Content mismatch at index " << j
                  << " skipCount  = " << skipCount
                  << " remaining = " << remaining << " to read = " << toRead
                  << "\nReference: " << result1->toString(j)
                  << "\nResult: " << result2->toString(j);
            }

            remaining -= toRead;
          }
          skipCount += 1;
        } while (multiSkip && remaining > 0);
      }

      // validate memory usage
      if (readerPool->usedBytes() > prevMemory) {
        numIncrements++;
      }
      prevMemory = readerPool->usedBytes();
    }
    ASSERT_FALSE(reader.next(1, result));
    if (checkMemoryLeak) {
      EXPECT_LE(numIncrements, 3 * expected.size() / 4);
    }
  }

  std::unique_ptr<nimble::VeloxReader> getReaderForLifeCycleTest(
      const std::shared_ptr<const velox::RowType> schema,
      size_t batchSize,
      std::mt19937& rng,
      nimble::VeloxWriterOptions writerOptions = {},
      nimble::VeloxReadParams readParams = {}) {
    velox::VectorFuzzer fuzzer(
        {.vectorSize = batchSize, .nullRatio = 0.5},
        leafPool_.get(),
        folly::Random::rand32(rng));
    auto vector = fuzzer.fuzzInputFlatRow(schema);
    auto file =
        nimble::test::createNimbleFile(*rootPool_, vector, writerOptions);

    std::unique_ptr<velox::InMemoryReadFile> readFile =
        std::make_unique<velox::InMemoryReadFile>(file);

    std::shared_ptr<nimble::TabletReader> tablet =
        std::make_shared<nimble::TabletReader>(*leafPool_, std::move(readFile));
    auto selector =
        std::make_shared<velox::dwio::common::ColumnSelector>(schema);
    std::unique_ptr<nimble::VeloxReader> reader =
        std::make_unique<nimble::VeloxReader>(
            *leafPool_, tablet, std::move(selector), readParams);
    return reader;
  }

  std::unique_ptr<nimble::VeloxReader> getReaderForWrite(
      velox::memory::MemoryPool& pool,
      const velox::RowTypePtr& type,
      std::vector<std::function<velox::VectorPtr(const velox::RowTypePtr&)>>
          generators,
      size_t batchSize,
      bool multiSkip = false,
      bool checkMemoryLeak = false) {
    nimble::VeloxWriterOptions writerOptions = {};
    nimble::VeloxReadParams readParams = {};

    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    writerOptions.enableChunking = true;
    writerOptions.flushPolicyFactory = [&]() {
      return std::make_unique<nimble::LambdaFlushPolicy>(
          [&](auto&) { return nimble::FlushDecision::None; });
    };
    writerOptions.dictionaryArrayColumns.insert("dictionaryArray");

    nimble::VeloxWriter writer(
        *rootPool_, type, std::move(writeFile), std::move(writerOptions));

    for (int i = 0; i < generators.size(); ++i) {
      auto vectorGenerator = generators.at(i);
      auto vector = vectorGenerator(type);
      // In the future, we can take in batchSize as param and try every size
      writer.write(vector);
    }
    writer.close();

    auto readFile = std::make_unique<velox::InMemoryReadFile>(file);
    auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(type);

    return std::make_unique<nimble::VeloxReader>(
        *leafPool_, std::move(readFile), std::move(selector), readParams);
  }

  void verifySlidingWindowMap(
      int deduplicatedMapCount,
      const std::vector<int32_t>& keys,
      const std::vector<float>& values,
      const std::vector<int32_t>& offsets,
      const std::vector<int32_t>& nulls = {}) {
    auto type = velox::ROW({
        {"slidingWindowMap", velox::MAP(velox::INTEGER(), velox::REAL())},
    });

    auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);
    uint32_t seed = folly::Random::rand32();
    LOG(INFO) << "seed: " << seed;

    nimble::VeloxWriterOptions writerOptions;
    writerOptions.deduplicatedMapColumns.insert("slidingWindowMap");
    velox::test::VectorMaker vectorMaker{leafPool_.get()};

    auto iterations = 20;
    auto batches = 20;
    std::mt19937 rng{seed};
    bool checkMemoryLeak = true;

    /* Check the inner map */
    auto compare = [&](const velox::VectorPtr& vector) {
      ASSERT_EQ(
          vector->wrappedVector()
              ->as<velox::RowVector>()
              ->childAt(0)
              ->loadedVector()
              ->wrappedVector()
              ->size(),
          deduplicatedMapCount);
    };

    for (auto i = 0; i < iterations; ++i) {
      auto keysVector = vectorMaker.flatVector<int32_t>(keys);
      auto valuesVector = vectorMaker.flatVector<float>(values);

      writeAndVerify(
          rng,
          *leafPool_.get(),
          rowType,
          [&](auto& /*type*/) {
            return vectorMaker.rowVector(
                {"slidingWindowMap"},
                {
                    vectorMaker.mapVector(
                        offsets, keysVector, valuesVector, nulls),
                });
          },
          vectorEquals,
          batches,
          writerOptions,
          {},
          nullptr,
          compare,
          false,
          checkMemoryLeak);
    }
  }

  template <typename T>
  velox::VectorPtr createEncodedDictionaryVectorNullable(
      const std::vector<std::optional<velox::vector_size_t>>& offsets,
      const std::vector<std::vector<T>>& dictionaryValues) {
    velox::test::VectorMaker vectorMaker{leafPool_.get()};
    auto offsetBuffer = velox::AlignedBuffer::allocate<velox::vector_size_t>(
        offsets.size() /* numValues */, leafPool_.get(), 0 /* initValue */);
    auto nullBuffer = velox::AlignedBuffer::allocate<uint64_t>(
        velox::bits::nwords(offsets.size()), leafPool_.get());
    auto* offsetPtr = offsetBuffer->asMutable<velox::vector_size_t>();
    auto* nullPtr = nullBuffer->template asMutable<uint64_t>();
    bool hasNulls = false;
    for (int i = 0; i < offsets.size(); ++i) {
      if (offsets[i].has_value()) {
        velox::bits::setNull(nullPtr, i, false);
        offsetPtr[i] = offsets[i].value();
      } else {
        hasNulls = true;
        velox::bits::setNull(nullPtr, i, true);
      }
    }

    auto array = vectorMaker.arrayVector<T>(dictionaryValues);
    auto dictionaryArrayEncoded = velox::BaseVector::wrapInDictionary(
        hasNulls ? nullBuffer : nullptr, offsetBuffer, offsets.size(), array);

    return dictionaryArrayEncoded;
  }

  template <typename T>
  std::vector<std::function<velox::VectorPtr(const velox::RowTypePtr&)>>
  getDictionaryGenerator(
      velox::test::VectorMaker& vectorMaker,
      const std::vector<std::optional<velox::vector_size_t>>& offsets,
      const std::vector<std::vector<T>>& dictionaryValues) {
    auto dictionaryArrayGenerator = [&, offsets, dictionaryValues](
                                        auto& /*type*/) {
      return vectorMaker.rowVector(
          {"dictionaryArray"},
          {
              createEncodedDictionaryVectorNullable(offsets, dictionaryValues),
          });
    };
    return std::vector<
        std::function<velox::VectorPtr(const velox::RowTypePtr&)>>(
        {dictionaryArrayGenerator});
  }

  template <typename T>
  std::vector<std::function<velox::VectorPtr(const velox::RowTypePtr&)>>
  getArrayGenerator(
      velox::test::VectorMaker& vectorMaker,
      const std::vector<std::vector<T>>& arrayValues) {
    auto arrayVectorGenerator = [&, arrayValues](auto& /*type*/) {
      return vectorMaker.rowVector(
          {"dictionaryArray"},
          {
              vectorMaker.arrayVector<T>(arrayValues),
          });
    };
    return std::vector<
        std::function<velox::VectorPtr(const velox::RowTypePtr&)>>(
        {arrayVectorGenerator});
  }

  template <typename T>
  std::vector<std::function<velox::VectorPtr(const velox::RowTypePtr&)>>
  getArrayGeneratorNullable(
      velox::test::VectorMaker& vectorMaker,
      const std::vector<std::optional<std::vector<std::optional<T>>>>&
          arrayValues) {
    auto arrayVectorGenerator = [&](auto& /*type*/) {
      return vectorMaker.rowVector(
          {"dictionaryArray"},
          {
              vectorMaker.arrayVectorNullable<T>(arrayValues),
          });
    };
    return std::vector<
        std::function<velox::VectorPtr(const velox::RowTypePtr&)>>(
        {arrayVectorGenerator});
  }

  template <typename T>
  void verifyDictionaryEncodedPassthrough(
      const std::vector<std::optional<velox::vector_size_t>>& offsets,
      const std::vector<std::vector<T>>& dictionaryValues,
      const std::vector<std::vector<T>>& arrayValues) {
    auto type = velox::ROW({
        {"dictionaryArray", velox::ARRAY(velox::CppToType<T>::create())},
    });
    auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);

    velox::test::VectorMaker vectorMaker{leafPool_.get()};

    auto dictionaryGenerators =
        getDictionaryGenerator(vectorMaker, offsets, dictionaryValues);
    auto arrayGenerators = getArrayGenerator(vectorMaker, arrayValues);

    auto dictionaryReader =
        getReaderForWrite(*leafPool_, rowType, dictionaryGenerators, 1);
    auto arrayReader =
        getReaderForWrite(*leafPool_, rowType, arrayGenerators, 1);

    // if dictionaryValues is empty with null offsets,
    // our loaded wrapped vector will contain a single null
    auto expectedNumUniqueArrays =
        dictionaryValues.size() > 0 ? dictionaryValues.size() : 1;

    verifyReadersEqual(
        std::move(dictionaryReader),
        std::move(arrayReader),
        offsets.size(),
        expectedNumUniqueArrays);
  }

  template <typename T>
  void verifyDictionaryEncodedPassthroughNullable(
      const std::vector<std::optional<velox::vector_size_t>>& offsets,
      const std::vector<std::vector<T>>& dictionaryValues,
      const std::vector<std::optional<std::vector<std::optional<T>>>>&
          arrayValues) {
    auto type = velox::ROW({
        {"dictionaryArray", velox::ARRAY(velox::CppToType<T>::create())},
    });
    auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);

    velox::test::VectorMaker vectorMaker{leafPool_.get()};
    // Test duplicate in first index
    auto dictionaryGenerators =
        getDictionaryGenerator(vectorMaker, offsets, dictionaryValues);
    auto arrayGenerators = getArrayGeneratorNullable(vectorMaker, arrayValues);

    auto dictionaryReader =
        getReaderForWrite(*leafPool_, rowType, dictionaryGenerators, 1);
    auto arrayReader =
        getReaderForWrite(*leafPool_, rowType, arrayGenerators, 1);

    // if dictionaryValues is empty with null offsets,
    // our loaded wrapped vector will contain a single null
    auto expectedNumUniqueArrays =
        dictionaryValues.size() > 0 ? dictionaryValues.size() : 1;

    verifyReadersEqual(
        std::move(dictionaryReader),
        std::move(arrayReader),
        offsets.size(), /* expectedNumTotalArrays */
        expectedNumUniqueArrays);
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> leafPool_;
};

TEST_F(VeloxReaderTests, DontReadUnselectedColumnsFromFile) {
  auto type = velox::ROW({
      {"tinyint_val", velox::TINYINT()},
      {"smallint_val", velox::SMALLINT()},
      {"int_val", velox::INTEGER()},
      {"long_val", velox::BIGINT()},
      {"float_val", velox::REAL()},
      {"double_val", velox::DOUBLE()},
      {"string_val", velox::VARCHAR()},
      {"array_val", velox::ARRAY(velox::BIGINT())},
      {"map_val", velox::MAP(velox::INTEGER(), velox::BIGINT())},
  });

  size_t batchSize = 100;
  auto selectedColumnNames =
      std::vector<std::string>{"tinyint_val", "double_val"};

  velox::VectorFuzzer fuzzer({.vectorSize = batchSize}, leafPool_.get());
  auto vector = fuzzer.fuzzInputFlatRow(type);

  auto file = nimble::test::createNimbleFile(*rootPool_, vector);

  uint32_t readSize = 1;
  for (auto useChaniedBuffers : {false, true}) {
    nimble::testing::InMemoryTrackableReadFile readFile(
        file, useChaniedBuffers);
    // We want to check stream by stream if they are being read
    readFile.setShouldCoalesce(false);

    auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(
        std::dynamic_pointer_cast<const velox::RowType>(vector->type()),
        selectedColumnNames);
    nimble::VeloxReader reader(*leafPool_, &readFile, std::move(selector));

    velox::VectorPtr result;
    reader.next(readSize, result);

    auto chunks = readFile.chunks();

    for (auto [offset, size] : chunks) {
      LOG(INFO) << "Stream read: " << offset;
    }

    EXPECT_EQ(
        streamsReadCount(*leafPool_, &readFile, chunks),
        selectedColumnNames.size());
  }
}

TEST_F(VeloxReaderTests, DontReadUnprojectedFeaturesFromFile) {
  auto type = velox::ROW({
      {"float_features", velox::MAP(velox::INTEGER(), velox::REAL())},
  });
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);

  int batchSize = 500;
  uint32_t seed = FLAGS_reader_tests_seed > 0 ? FLAGS_reader_tests_seed
                                              : folly::Random::rand32();

  VeloxMapGeneratorConfig generatorConfig{
      .featureTypes = rowType,
      .keyType = velox::TypeKind::INTEGER,
      .maxNumKVPerRow = 10,
      .seed = seed,
      .hasNulls = false,
  };

  VeloxMapGenerator generator(leafPool_.get(), generatorConfig);
  auto vector = generator.generateBatch(batchSize);

  nimble::VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns.insert("float_features");

  auto file = nimble::test::createNimbleFile(
      *rootPool_, vector, std::move(writerOptions));

  for (auto useChaniedBuffers : {false, true}) {
    facebook::nimble::testing::InMemoryTrackableReadFile readFile(
        file, useChaniedBuffers);
    // We want to check stream by stream if they are being read
    readFile.setShouldCoalesce(false);

    auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(
        std::dynamic_pointer_cast<const velox::RowType>(vector->type()));

    nimble::VeloxReadParams params;
    params.readFlatMapFieldAsStruct.insert("float_features");
    auto& selectedFeatures =
        params.flatMapFeatureSelector["float_features"].features;
    std::mt19937 rng(seed);
    for (int i = 0; i < generatorConfig.maxNumKVPerRow; ++i) {
      if (folly::Random::oneIn(2, rng)) {
        selectedFeatures.push_back(folly::to<std::string>(i));
      }
    }
    // Features list can't be empty.
    if (selectedFeatures.empty()) {
      selectedFeatures = {folly::to<std::string>(
          folly::Random::rand32(generatorConfig.maxNumKVPerRow))};
    }

    LOG(INFO) << "Selected features (" << selectedFeatures.size()
              << ") :" << folly::join(", ", selectedFeatures);

    nimble::VeloxReader reader(
        *leafPool_, &readFile, std::move(selector), params);

    uint32_t readSize = 1000;
    velox::VectorPtr result;
    reader.next(readSize, result);

    auto selectedFeaturesSet = std::unordered_set<std::string>(
        selectedFeatures.cbegin(), selectedFeatures.cend());

    // We have those streams: Row, FlatMap, N*(Values + inMap)
    // Row: Empty stream. Not read.
    // FlatMap: Empty if !hasNulls
    // N: Number of features
    // Values: Empty if all rows are null (if inMap all false)
    // inMap: Non-empty
    //
    // Therefore the formula is: 0 + 0 + N*(Values*any(inMap) + inMap)
    ASSERT_FALSE(generatorConfig.hasNulls);
    int expectedNonEmptyStreamsCount = 0; // 0 if !hasNulls
    auto rowResult = result->as<velox::RowVector>();
    ASSERT_EQ(rowResult->childrenSize(), 1); // FlatMap
    auto flatMap = rowResult->childAt(0)->as<velox::RowVector>();

    for (int feature = 0; feature < flatMap->childrenSize(); ++feature) {
      // Each feature will have at least inMap stream
      ++expectedNonEmptyStreamsCount;
      if (selectedFeaturesSet.contains(
              flatMap->type()->asRow().nameOf(feature))) {
        auto columnResult = flatMap->childAt(feature);
        for (int row = 0; row < columnResult->size(); ++row) {
          // Values stream for this column will only exist if there's at least
          // one element inMap in this column (if not all rows are null at
          // either row level or element level)
          if (!flatMap->isNullAt(row) && !columnResult->isNullAt(row)) {
            ++expectedNonEmptyStreamsCount;
            // exit row iteration, we know that there's at least one element
            break;
          }
        }
      }
    }

    auto chunks = readFile.chunks();

    LOG(INFO) << "Total streams read: " << chunks.size();
    for (auto [offset, size] : chunks) {
      LOG(INFO) << "Stream read: " << offset;
    }

    EXPECT_EQ(
        streamsReadCount(*leafPool_, &readFile, chunks),
        expectedNonEmptyStreamsCount);
  }
}

TEST_F(VeloxReaderTests, ReadComplexData) {
  auto type = velox::ROW({
      {"tinyint_val", velox::TINYINT()},
      {"smallint_val", velox::SMALLINT()},
      {"int_val", velox::INTEGER()},
      {"long_val", velox::BIGINT()},
      {"float_val", velox::REAL()},
      {"double_val", velox::DOUBLE()},
      {"bool_val", velox::BOOLEAN()},
      {"string_val", velox::VARCHAR()},
      {"array_val", velox::ARRAY(velox::BIGINT())},
      {"map_val", velox::MAP(velox::INTEGER(), velox::BIGINT())},
      {"struct_val",
       velox::ROW({
           {"float_val", velox::REAL()},
           {"double_val", velox::DOUBLE()},
       })},
      {"nested_val",
       velox::MAP(
           velox::INTEGER(),
           velox::ROW({
               {"float_val", velox::REAL()},
               {"array_val",
                velox::ARRAY(velox::MAP(velox::INTEGER(), velox::BIGINT()))},
           }))},
  });

  auto typeUpcast = velox::ROW({
      {"tinyint_val", velox::SMALLINT()},
      {"smallint_val", velox::INTEGER()},
      {"int_val", velox::BIGINT()},
      {"long_val", velox::BIGINT()},
      {"float_val", velox::DOUBLE()},
      {"double_val", velox::DOUBLE()},
      {"bool_val", velox::INTEGER()},
      {"string_val", velox::VARCHAR()},
      {"array_val", velox::ARRAY(velox::BIGINT())},
      {"map_val", velox::MAP(velox::INTEGER(), velox::BIGINT())},
      {"struct_val",
       velox::ROW({
           {"float_val", velox::REAL()},
           {"double_val", velox::DOUBLE()},
       })},
      {"nested_val",
       velox::MAP(
           velox::INTEGER(),
           velox::ROW({
               {"float_val", velox::REAL()},
               {"array_val",
                velox::ARRAY(velox::MAP(velox::INTEGER(), velox::BIGINT()))},
           }))},
  });

  for (size_t batchSize : {5, 1234}) {
    velox::VectorFuzzer fuzzer({.vectorSize = batchSize}, leafPool_.get());
    auto vector = fuzzer.fuzzInputFlatRow(type);
    auto file = nimble::test::createNimbleFile(*rootPool_, vector);

    for (bool upcast : {false, true}) {
      for (uint32_t readSize : {1, 2, 5, 7, 20, 100, 555, 2000}) {
        velox::InMemoryReadFile readFile(file);
        auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(
            std::dynamic_pointer_cast<const velox::RowType>(
                upcast ? typeUpcast : vector->type()));
        nimble::VeloxReader reader(*leafPool_, &readFile, std::move(selector));

        velox::vector_size_t rowIndex = 0;
        std::vector<uint32_t> childRowIndices(
            vector->as<velox::RowVector>()->childrenSize(), 0);
        velox::VectorPtr result;
        while (reader.next(readSize, result)) {
          ASSERT_EQ(result->type()->kind(), velox::TypeKind::ROW);
          if (upcast) {
            verifyUpcastedScalars<int8_t, int16_t>(
                vector->as<velox::RowVector>()->childAt(0),
                childRowIndices[0],
                result->as<velox::RowVector>()->childAt(0),
                readSize);
            verifyUpcastedScalars<int16_t, int32_t>(
                vector->as<velox::RowVector>()->childAt(1),
                childRowIndices[1],
                result->as<velox::RowVector>()->childAt(1),
                readSize);
            verifyUpcastedScalars<int32_t, int64_t>(
                vector->as<velox::RowVector>()->childAt(2),
                childRowIndices[2],
                result->as<velox::RowVector>()->childAt(2),
                readSize);
            verifyUpcastedScalars<int64_t, int64_t>(
                vector->as<velox::RowVector>()->childAt(3),
                childRowIndices[3],
                result->as<velox::RowVector>()->childAt(3),
                readSize);
            verifyUpcastedScalars<float, double>(
                vector->as<velox::RowVector>()->childAt(4),
                childRowIndices[4],
                result->as<velox::RowVector>()->childAt(4),
                readSize);
            verifyUpcastedScalars<double, double>(
                vector->as<velox::RowVector>()->childAt(5),
                childRowIndices[5],
                result->as<velox::RowVector>()->childAt(5),
                readSize);
            verifyUpcastedScalars<bool, int32_t>(
                vector->as<velox::RowVector>()->childAt(6),
                childRowIndices[6],
                result->as<velox::RowVector>()->childAt(6),
                readSize);
          } else {
            for (velox::vector_size_t i = 0; i < result->size(); ++i) {
              ASSERT_TRUE(vector->equalValueAt(result.get(), rowIndex, i))
                  << "Content mismatch at index " << rowIndex
                  << "\nReference: " << vector->toString(rowIndex)
                  << "\nResult: " << result->toString(i);

              ++rowIndex;
            }
          }
        }
      }
    }
  }
}

TEST_F(VeloxReaderTests, Lifetime) {
  velox::StringView s{"012345678901234567890123456789"};
  std::vector<velox::StringView> strings{s, s, s, s, s};
  std::vector<std::vector<velox::StringView>> stringsOfStrings{
      strings, strings, strings, strings, strings};
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector = vectorMaker.rowVector(
      {vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5}),
       vectorMaker.flatVector(strings),
       vectorMaker.arrayVector<velox::StringView>(stringsOfStrings),
       vectorMaker.mapVector<int32_t, velox::StringView>(
           5,
           /*sizeAt*/ [](auto row) { return row; },
           /*keyAt*/ [](auto row) { return row; },
           /*valueAt*/
           [&s](auto /* row */) { return s; }),
       vectorMaker.rowVector(
           /* childNames */ {"a", "b"},
           /* children */
           {vectorMaker.flatVector<float>({1., 2., 3., 4., 5.}),
            vectorMaker.flatVector(strings)})});

  velox::VectorPtr result;
  {
    auto file = nimble::test::createNimbleFile(*rootPool_, vector);
    velox::InMemoryReadFile readFile(file);
    auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(
        std::dynamic_pointer_cast<const velox::RowType>(vector->type()));
    nimble::VeloxReader reader(*leafPool_, &readFile, std::move(selector));

    ASSERT_TRUE(reader.next(vector->size(), result));
    ASSERT_FALSE(reader.next(vector->size(), result));
  }

  // At this point, the reader is dropped, so the vector should be
  // self-contained and doesn't rely on the reader state.

  ASSERT_EQ(vector->size(), result->size());
  ASSERT_EQ(result->type()->kind(), velox::TypeKind::ROW);

  for (int32_t i = 0; i < result->size(); ++i) {
    ASSERT_TRUE(vector->equalValueAt(result.get(), i, i))
        << "Content mismatch at index " << i
        << "\nReference: " << vector->toString(i)
        << "\nResult: " << result->toString(i);
  }
}

TEST_F(VeloxReaderTests, AllValuesNulls) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector = vectorMaker.rowVector(
      {vectorMaker.flatVectorNullable<int32_t>(
           {std::nullopt, std::nullopt, std::nullopt}),
       vectorMaker.flatVectorNullable<double>(
           {std::nullopt, std::nullopt, std::nullopt}),
       velox::BaseVector::createNullConstant(
           velox::ROW({{"foo", velox::INTEGER()}}), 3, leafPool_.get()),
       velox::BaseVector::createNullConstant(
           velox::MAP(velox::INTEGER(), velox::BIGINT()), 3, leafPool_.get()),
       velox::BaseVector::createNullConstant(
           velox::ARRAY(velox::INTEGER()), 3, leafPool_.get())});

  auto projectedType = velox::ROW({
      {"c0", velox::INTEGER()},
      {"c1", velox::DOUBLE()},
      {"c2", velox::ROW({{"foo", velox::INTEGER()}})},
      {"c3", velox::MAP(velox::INTEGER(), velox::BIGINT())},
      {"c4", velox::ARRAY(velox::INTEGER())},
  });
  velox::VectorPtr result;
  {
    nimble::VeloxWriterOptions options;
    options.flatMapColumns.insert("c3");
    options.dictionaryArrayColumns.insert("c4");
    auto file = nimble::test::createNimbleFile(*rootPool_, vector, options);
    velox::InMemoryReadFile readFile(file);

    nimble::VeloxReadParams params;
    params.readFlatMapFieldAsStruct.insert("c3");
    params.flatMapFeatureSelector.insert({"c3", {{"1"}}});
    auto selector =
        std::make_shared<velox::dwio::common::ColumnSelector>(projectedType);
    nimble::VeloxReader reader(*leafPool_, &readFile, selector, params);

    ASSERT_TRUE(reader.next(vector->size(), result));
    ASSERT_FALSE(reader.next(vector->size(), result));
  }

  // At this point, the reader is dropped, so the vector should be
  // self-contained and doesn't rely on the reader state.

  ASSERT_EQ(vector->size(), result->size());
  auto& vectorType = result->type();
  ASSERT_EQ(vectorType->kind(), velox::TypeKind::ROW);
  ASSERT_EQ(vectorType->size(), projectedType->size());
  ASSERT_EQ(vectorType->childAt(3)->kind(), velox::TypeKind::ROW);
  ASSERT_EQ(vectorType->childAt(4)->kind(), velox::TypeKind::ARRAY);

  auto resultRow = result->as<velox::RowVector>();
  for (int32_t i = 0; i < result->size(); ++i) {
    for (auto j = 0; j < vectorType->size(); ++j) {
      ASSERT_TRUE(resultRow->childAt(j)->isNullAt(i));
    }
  }
}

TEST_F(VeloxReaderTests, StringBuffers) {
  // Creating a string column with 10 identical strings.
  // We will perform 2 reads of 5 rows each, and compare the string buffers
  // generated.
  // Note: all strings are long enough to force Velox to store them in string
  // buffers instead of inlining them.
  std::string s{"012345678901234567890123456789"};
  std::vector<std::string> column{s, s, s, s, s, s, s, s, s, s};
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector = vectorMaker.rowVector({vectorMaker.flatVector(column)});

  velox::VectorPtr result;
  auto file = nimble::test::createNimbleFile(*rootPool_, vector);
  velox::InMemoryReadFile readFile(file);
  auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(
      std::dynamic_pointer_cast<const velox::RowType>(vector->type()));
  nimble::VeloxReader reader(*leafPool_, &readFile, std::move(selector));

  ASSERT_TRUE(reader.next(5, result));

  ASSERT_EQ(5, result->size());
  ASSERT_EQ(result->type()->kind(), velox::TypeKind::ROW);
  auto rowVector = result->as<velox::RowVector>();
  ASSERT_EQ(1, rowVector->childrenSize());
  const auto& buffers1 = rowVector->childAt(0)
                             ->as<velox::FlatVector<velox::StringView>>()
                             ->stringBuffers();
  EXPECT_LE(
      1,
      rowVector->childAt(0)
          ->as<velox::FlatVector<velox::StringView>>()
          ->stringBuffers()
          .size());

  // Capture string buffer size after first batch read
  auto bufferSizeFirst = std::accumulate(
      buffers1.begin(), buffers1.end(), 0, [](int sum, const auto& buffer) {
        return sum + buffer->size();
      });

  ASSERT_TRUE(reader.next(5, result));
  rowVector = result->as<velox::RowVector>();
  ASSERT_EQ(1, rowVector->childrenSize());
  const auto& buffers2 = rowVector->childAt(0)
                             ->as<velox::FlatVector<velox::StringView>>()
                             ->stringBuffers();

  ASSERT_EQ(5, result->size());
  EXPECT_LE(
      1,
      rowVector->childAt(0)
          ->as<velox::FlatVector<velox::StringView>>()
          ->stringBuffers()
          .size());

  // Capture string buffer size after second batch read. Since both batched
  // contain exactly the same strings ,batch sizes should match.
  auto bufferSizeSecond = std::accumulate(
      buffers2.begin(), buffers2.end(), 0, [](int sum, const auto& buffer) {
        return sum + buffer->size();
      });

  EXPECT_EQ(bufferSizeFirst, bufferSizeSecond);
}

TEST_F(VeloxReaderTests, NullVectors) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  // In the following table, the first 5 rows contain nulls and the last 5
  // rows don't.
  auto vector = vectorMaker.rowVector(
      {vectorMaker.flatVectorNullable<int32_t>(
           {1, 2, std::nullopt, 4, 5, 6, 7, 8, 9, 10}),
       vectorMaker.flatVectorNullable<velox::StringView>(
           {"1", std::nullopt, "3", "4", "5", "6", "7", "8", "9", "10"}),
       vectorMaker.arrayVectorNullable<double>(
           {std::vector<std::optional<double>>{1.0, 2.2, std::nullopt},
            {},
            std::nullopt,
            std::vector<std::optional<double>>{1.1, 2.0},
            {},
            std::vector<std::optional<double>>{6.1},
            std::vector<std::optional<double>>{7.1},
            std::vector<std::optional<double>>{8.1},
            std::vector<std::optional<double>>{9.1},
            std::vector<std::optional<double>>{10.1}}),
       vectorMaker.mapVector<int32_t, int64_t>(
           10,
           /*sizeAt*/ [](auto row) { return row; },
           /*keyAt*/ [](auto row) { return row; },
           /*valueAt*/
           [](auto row) { return row; },
           /*isNullAt*/ [](auto row) { return row < 5 && row % 2 == 0; }),
       vectorMaker.rowVector(
           /* childNames */ {"a", "b"},
           /* children */
           {vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            vectorMaker.flatVector<double>(
                {1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.10})})});
  vector->childAt(4)->setNull(2, true); // Set null in row vector

  auto file = nimble::test::createNimbleFile(*rootPool_, vector);
  velox::InMemoryReadFile readFile(file);
  auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(
      std::dynamic_pointer_cast<const velox::RowType>(vector->type()));

  nimble::VeloxReader reader(*leafPool_, &readFile, std::move(selector));

  velox::VectorPtr result;

  // When reader is reading the first 5 rows, it should find null entries and
  // vectors should indicate that nulls exist.
  ASSERT_TRUE(reader.next(5, result));
  ASSERT_EQ(5, result->size());
  ASSERT_EQ(velox::TypeKind::ROW, result->type()->kind());

  auto rowVector = result->as<velox::RowVector>();

  ASSERT_EQ(5, rowVector->childrenSize());
  EXPECT_TRUE(rowVector->childAt(0)->mayHaveNulls());
  EXPECT_TRUE(rowVector->childAt(1)->mayHaveNulls());
  EXPECT_TRUE(rowVector->childAt(2)->mayHaveNulls());
  EXPECT_TRUE(rowVector->childAt(3)->mayHaveNulls());
  EXPECT_TRUE(rowVector->childAt(4)->mayHaveNulls());

  for (int32_t i = 0; i < result->size(); ++i) {
    ASSERT_TRUE(vector->equalValueAt(result.get(), i, i))
        << "Content mismatch at index " << i
        << "\nReference: " << vector->toString(i)
        << "\nResult: " << result->toString(i);
  }

  // When reader is reading the last 5 rows, it should identify that no null
  // exist and optimize vectors to efficiently indicate that.
  ASSERT_TRUE(reader.next(5, result));
  rowVector = result->as<velox::RowVector>();

  EXPECT_FALSE(rowVector->childAt(0)->mayHaveNulls());
  EXPECT_FALSE(rowVector->childAt(1)->mayHaveNulls());
  EXPECT_FALSE(rowVector->childAt(2)->mayHaveNulls());
  EXPECT_FALSE(rowVector->childAt(3)->mayHaveNulls());
  EXPECT_FALSE(rowVector->childAt(4)->mayHaveNulls());

  for (int32_t i = 0; i < result->size(); ++i) {
    ASSERT_TRUE(vector->equalValueAt(result.get(), i + 5, i))
        << "Content mismatch at index " << i + 5
        << "\nReference: " << vector->toString(i + 5)
        << "\nResult: " << result->toString(i);
  }

  ASSERT_FALSE(reader.next(1, result));
}

TEST_F(VeloxReaderTests, SlidingWindowMapSizeOne) {
  verifySlidingWindowMap(
      /* deduplicatedMapCount */ 1,
      /* keys */ {1, 2},
      /* values */ {0.1, 0.2},
      /* offsets */ {0});

  // Nullable cases
  verifySlidingWindowMap(
      /* deduplicatedMapCount */ 1,
      /* keys */ {1, 2},
      /* values */ {0.1, 0.2},
      /* offsets */ {0, 2},
      /* nulls */ {1});
}

TEST_F(VeloxReaderTests, SlidingWindowMapSizeTwo) {
  verifySlidingWindowMap(
      /* deduplicatedMapCount */ 1,
      /* keys */ {1, 2, 1, 2},
      /* values */ {0.1, 0.2, 0.1, 0.2},
      /* offsets */ {0, 2});

  // Nullable cases
  verifySlidingWindowMap(
      /* deduplicatedMapCount */ 1,
      /* keys */ {1, 2, 1, 2},
      /* values */ {0.1, 0.2, 0.1, 0.2},
      /* offsets */ {0, 2, 2},
      /* nulls */ {1});
}

TEST_F(VeloxReaderTests, SlidingWindowMapEmpty) {
  verifySlidingWindowMap(
      /* deduplicatedMapCount */ 1,
      /* keys */ {},
      /* values */ {},
      /* offsets */ {0});

  verifySlidingWindowMap(
      /* deduplicatedMapCount */ 1,
      /* keys */ {},
      /* values */ {},
      /* offsets */ {0, 0, 0});

  // Nullable cases
  verifySlidingWindowMap(
      /* deduplicatedMapCount */ 1,
      /* keys */ {},
      /* values */ {},
      /* offsets */ {0, 0, 0, 0},
      /* nulls */ {1});
}

TEST_F(VeloxReaderTests, SlidingWindowMapMixedEmptyLength) {
  verifySlidingWindowMap(
      /* deduplicatedMapCount */ 5,
      /* keys */ {1, 2, 2, 1, 1, 2, 4, 4},
      /* values */ {0.1, 0.2, 0.2, 0.1, 0.1, 0.3, 0.4, 0.4},
      /* offsets */ {0, 2, 4, 4, 4, 6, 6, 6, 6, 7});

  // Nullable cases
  verifySlidingWindowMap(
      /* deduplicatedMapCount */ 5,
      /* keys */ {1, 2, 2, 1, 1, 2, 4, 4},
      /* values */ {0.1, 0.2, 0.2, 0.1, 0.1, 0.3, 0.4, 0.4},
      /* offsets */ {0, 2, 4, 4, 4, 6, 6, 6, 6, 7, 7},
      /* nulls */ {9});
}

TEST_F(VeloxReaderTests, ArrayWithOffsetsCaching) {
  auto type = velox::ROW({
      {"dictionaryArray", velox::ARRAY(velox::INTEGER())},
  });
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  // Test cache hit on second write
  auto generator = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVector<int32_t>(
                {{1, 2, 3},
                 {1, 2, 3},
                 {1, 2, 3, 4, 5},
                 {1, 2, 3, 4, 5},
                 {1, 2, 3, 4, 5},
                 {1, 2, 3, 4, 5}}),
        });
  };
  auto halfGenerator = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVector<int32_t>(
                {{1, 2, 3}, {1, 2, 3}, {1, 2, 3, 4, 5}}),
        });
  };
  auto otherHalfGenerator = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVector<int32_t>(
                {{1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}}),
        });
  };

  auto leftGenerators =
      std::vector<std::function<velox::VectorPtr(const velox::RowTypePtr&)>>(
          {generator});
  auto rightGenerators =
      std::vector<std::function<velox::VectorPtr(const velox::RowTypePtr&)>>(
          {halfGenerator, otherHalfGenerator});
  auto expectedTotalArrays = 6;
  auto expectedUniqueArrays = 2;
  auto left = getReaderForWrite(*leafPool_, rowType, leftGenerators, 1);
  auto right = getReaderForWrite(*leafPool_, rowType, rightGenerators, 1);

  verifyReadersEqual(
      std::move(left),
      std::move(right),
      expectedTotalArrays,
      expectedUniqueArrays);

  // Test cache miss on second write
  // We can re-use the totalGenerator since the total output is unchanged
  auto halfGeneratorCacheMiss = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVector<int32_t>({{1, 2, 3}, {1, 2, 3}}),
        });
  };
  auto otherHalfGeneratorCacheMiss = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVector<int32_t>(
                {{1, 2, 3, 4, 5},
                 {1, 2, 3, 4, 5},
                 {1, 2, 3, 4, 5},
                 {1, 2, 3, 4, 5}}),
        });
  };

  rightGenerators =
      std::vector<std::function<velox::VectorPtr(const velox::RowTypePtr&)>>(
          {halfGeneratorCacheMiss, otherHalfGeneratorCacheMiss});
  expectedTotalArrays = 6;
  expectedUniqueArrays = 2;
  left = getReaderForWrite(*leafPool_, rowType, leftGenerators, 1);
  right = getReaderForWrite(*leafPool_, rowType, rightGenerators, 1);

  verifyReadersEqual(
      std::move(left),
      std::move(right),
      expectedTotalArrays,
      expectedUniqueArrays);

  // Check cached value against null on next write.
  auto fullGeneratorWithNull = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVectorNullable<int32_t>(
                {std::vector<std::optional<int32_t>>{1, 2, 3},
                 std::vector<std::optional<int32_t>>{1, 2, 3},
                 std::vector<std::optional<int32_t>>{1, 2, 3, 4, 5},
                 std::nullopt,
                 std::vector<std::optional<int32_t>>{1, 2, 3, 4, 5},
                 std::vector<std::optional<int32_t>>{1, 2, 3, 4, 5}}),
        });
  };
  auto halfGeneratorNoNull = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVectorNullable<int32_t>(
                {std::vector<std::optional<int32_t>>{1, 2, 3},
                 std::vector<std::optional<int32_t>>{1, 2, 3},
                 std::vector<std::optional<int32_t>>{1, 2, 3, 4, 5}}),
        });
  };
  auto otherHalfGeneratorWithNull = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVectorNullable<int32_t>(
                {std::nullopt,
                 std::vector<std::optional<int32_t>>{1, 2, 3, 4, 5},
                 std::vector<std::optional<int32_t>>{1, 2, 3, 4, 5}}),
        });
  };

  leftGenerators =
      std::vector<std::function<velox::VectorPtr(const velox::RowTypePtr&)>>(
          {fullGeneratorWithNull});
  rightGenerators =
      std::vector<std::function<velox::VectorPtr(const velox::RowTypePtr&)>>(
          {halfGeneratorNoNull, otherHalfGeneratorWithNull});

  expectedTotalArrays = 6; // null array included in count
  expectedUniqueArrays = 2; // {{1, 2, 3}, {1, 2, 3, 4, 5}}
  left = getReaderForWrite(*leafPool_, rowType, leftGenerators, 1);
  right = getReaderForWrite(*leafPool_, rowType, rightGenerators, 1);

  verifyReadersEqual(
      std::move(left),
      std::move(right),
      expectedTotalArrays,
      expectedUniqueArrays);

  // Check cache stores last valid value, and not null.
  // Re-use total generator from previous test case
  auto halfGeneratorWithNull = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVectorNullable<int32_t>(
                {std::vector<std::optional<int32_t>>{1, 2, 3},
                 std::vector<std::optional<int32_t>>{1, 2, 3},
                 std::vector<std::optional<int32_t>>{1, 2, 3, 4, 5},
                 std::nullopt}),
        });
  };
  auto otherHalfGeneratorNoNull = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVectorNullable<int32_t>(
                {std::vector<std::optional<int32_t>>{1, 2, 3, 4, 5},
                 std::vector<std::optional<int32_t>>{1, 2, 3, 4, 5}}),
        });
  };
  rightGenerators =
      std::vector<std::function<velox::VectorPtr(const velox::RowTypePtr&)>>(
          {halfGeneratorWithNull, otherHalfGeneratorNoNull});

  expectedTotalArrays = 6; // null array included in count
  expectedUniqueArrays = 2; // {{1, 2, 3}, {1, 2, 3, 4, 5}}
  left = getReaderForWrite(*leafPool_, rowType, leftGenerators, 1);
  right = getReaderForWrite(*leafPool_, rowType, rightGenerators, 1);

  verifyReadersEqual(
      std::move(left),
      std::move(right),
      expectedTotalArrays,
      expectedUniqueArrays);

  // Check cached value against empty vector
  auto fullGeneratorWithEmpty = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVector<int32_t>(
                {{1, 2, 3},
                 {1, 2, 3},
                 {1, 2, 3, 4, 5},
                 {},
                 {1, 2, 3, 4, 5},
                 {1, 2, 3, 4, 5}}),
        });
  };

  auto halfGeneratorNoEmpty = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVector<int32_t>(
                {{1, 2, 3}, {1, 2, 3}, {1, 2, 3, 4, 5}}),
        });
  };
  auto otherHalfGeneratorWithEmpty = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVector<int32_t>(
                {{}, {1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}}),
        });
  };

  leftGenerators =
      std::vector<std::function<velox::VectorPtr(const velox::RowTypePtr&)>>(
          {fullGeneratorWithEmpty});
  rightGenerators =
      std::vector<std::function<velox::VectorPtr(const velox::RowTypePtr&)>>(
          {halfGeneratorNoEmpty, otherHalfGeneratorWithEmpty});
  expectedTotalArrays = 6; // empty array included in count
  expectedUniqueArrays = 4; // {{1, 2, 3}, {1, 2, 3, 4, 5}, {}, {1, 2, 3, 4, 5}}
  left = getReaderForWrite(*leafPool_, rowType, leftGenerators, 1);
  right = getReaderForWrite(*leafPool_, rowType, rightGenerators, 1);

  verifyReadersEqual(
      std::move(left),
      std::move(right),
      expectedTotalArrays,
      expectedUniqueArrays);

  // Test empty vector stored in cache from first write
  // Re-use total generator from last test case
  auto halfGeneratorWithEmpty = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVector<int32_t>(
                {{1, 2, 3}, {1, 2, 3}, {1, 2, 3, 4, 5}, {}}),
        });
  };
  auto otherHalfGeneratorNoEmpty = [&](auto& /*type*/) {
    return vectorMaker.rowVector(
        {"dictionaryArray"},
        {
            vectorMaker.arrayVector<int32_t>(
                {{1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}}),
        });
  };

  leftGenerators =
      std::vector<std::function<velox::VectorPtr(const velox::RowTypePtr&)>>(
          {fullGeneratorWithEmpty});
  rightGenerators =
      std::vector<std::function<velox::VectorPtr(const velox::RowTypePtr&)>>(
          {halfGeneratorWithEmpty, otherHalfGeneratorNoEmpty});
  expectedTotalArrays = 6; // empty array included in count
  expectedUniqueArrays = 4; // {{1, 2, 3}, {1, 2, 3, 4, 5}, {}, {1, 2, 3, 4, 5}}
  left = getReaderForWrite(*leafPool_, rowType, leftGenerators, 1);
  right = getReaderForWrite(*leafPool_, rowType, rightGenerators, 1);

  verifyReadersEqual(
      std::move(left),
      std::move(right),
      expectedTotalArrays,
      expectedUniqueArrays);
}

TEST_F(VeloxReaderTests, DictionaryEncodedPassthrough) {
  // test duplicate in first index
  auto offsets = std::vector<std::optional<velox::vector_size_t>>{0, 0, 1, 2};
  auto dictionaryValues =
      std::vector<std::vector<int32_t>>{{10, 15, 20}, {30, 40, 50}, {3, 4, 5}};
  auto fullArrayVector = std::vector<std::vector<int32_t>>{
      {10, 15, 20}, {10, 15, 20}, {30, 40, 50}, {3, 4, 5}};
  verifyDictionaryEncodedPassthrough(
      offsets, dictionaryValues, fullArrayVector);

  // test duplicate in middle index
  offsets = std::vector<std::optional<velox::vector_size_t>>{0, 1, 1, 2};
  dictionaryValues =
      std::vector<std::vector<int32_t>>{{10, 15}, {30, 40, 50, 60}, {3, 4, 5}};
  fullArrayVector = std::vector<std::vector<int32_t>>{
      {10, 15}, {30, 40, 50, 60}, {30, 40, 50, 60}, {3, 4, 5}};
  verifyDictionaryEncodedPassthrough(
      offsets, dictionaryValues, fullArrayVector);

  // test duplicate in last index
  offsets = std::vector<std::optional<velox::vector_size_t>>{0, 1, 2, 2, 2};
  dictionaryValues = std::vector<std::vector<int32_t>>{
      {10, 15}, {30, 40, 50, 60}, {3, 4, 5, 6, 7}};
  fullArrayVector = std::vector<std::vector<int32_t>>{
      {10, 15},
      {30, 40, 50, 60},
      {3, 4, 5, 6, 7},
      {3, 4, 5, 6, 7},
      {3, 4, 5, 6, 7}};
  verifyDictionaryEncodedPassthrough(
      offsets, dictionaryValues, fullArrayVector);

  // test all duplicated
  offsets = std::vector<std::optional<velox::vector_size_t>>{0, 0, 0, 0, 0};
  dictionaryValues = std::vector<std::vector<int32_t>>{{10, 15, 20}};
  fullArrayVector = std::vector<std::vector<int32_t>>{
      {10, 15, 20}, {10, 15, 20}, {10, 15, 20}, {10, 15, 20}, {10, 15, 20}};
  verifyDictionaryEncodedPassthrough(
      offsets, dictionaryValues, fullArrayVector);

  // test none duplictaed
  offsets = std::vector<std::optional<velox::vector_size_t>>{0, 1, 2, 3, 4};
  dictionaryValues = std::vector<std::vector<int32_t>>{
      {10, 15, 20}, {11, 14, 21}, {12, 13, 22}, {0, 0, 0}, {100, 99, 98}};
  fullArrayVector = std::vector<std::vector<int32_t>>{
      {10, 15, 20}, {11, 14, 21}, {12, 13, 22}, {0, 0, 0}, {100, 99, 98}};
  verifyDictionaryEncodedPassthrough(
      offsets, dictionaryValues, fullArrayVector);

  // test all empty
  offsets = std::vector<std::optional<velox::vector_size_t>>{};
  dictionaryValues = std::vector<std::vector<int32_t>>{};
  fullArrayVector = std::vector<std::vector<int32_t>>{};
  verifyDictionaryEncodedPassthrough(
      offsets, dictionaryValues, fullArrayVector);

  // test null in first index
  offsets =
      std::vector<std::optional<velox::vector_size_t>>{std::nullopt, 0, 1, 2};
  dictionaryValues =
      std::vector<std::vector<int32_t>>{{10, 15, 20}, {30, 40, 50}, {3, 4, 5}};
  auto fullArrayVectorNullable =
      std::vector<std::optional<std::vector<std::optional<int32_t>>>>{
          std::nullopt,
          std::vector<std::optional<int32_t>>{10, 15, 20},
          std::vector<std::optional<int32_t>>{30, 40, 50},
          std::vector<std::optional<int32_t>>{3, 4, 5}};
  verifyDictionaryEncodedPassthroughNullable(
      offsets, dictionaryValues, fullArrayVectorNullable);

  // test duplicates split over null in middle
  offsets = std::vector<std::optional<velox::vector_size_t>>{
      std::nullopt, 0, std::nullopt, 0, 1};
  dictionaryValues =
      std::vector<std::vector<int32_t>>{{10, 15, 20}, {30, 40, 50, 60}};
  fullArrayVectorNullable =
      std::vector<std::optional<std::vector<std::optional<int32_t>>>>{
          std::nullopt,
          std::vector<std::optional<int32_t>>{10, 15, 20},
          std::nullopt,
          std::vector<std::optional<int32_t>>{10, 15, 20},
          std::vector<std::optional<int32_t>>{30, 40, 50, 60}};
  verifyDictionaryEncodedPassthroughNullable(
      offsets, dictionaryValues, fullArrayVectorNullable);

  // test duplicates split over multiple nulls in middle
  offsets = std::vector<std::optional<velox::vector_size_t>>{
      std::nullopt, 0, std::nullopt, std::nullopt, 0, 1};
  dictionaryValues =
      std::vector<std::vector<int32_t>>{{10, 15, 20}, {30, 40, 50, 60}};
  fullArrayVectorNullable =
      std::vector<std::optional<std::vector<std::optional<int32_t>>>>{
          std::nullopt,
          std::vector<std::optional<int32_t>>{10, 15, 20},
          std::nullopt,
          std::nullopt,
          std::vector<std::optional<int32_t>>{10, 15, 20},
          std::vector<std::optional<int32_t>>{30, 40, 50, 60}};
  verifyDictionaryEncodedPassthroughNullable(
      offsets, dictionaryValues, fullArrayVectorNullable);

  // test all null
  offsets = std::vector<std::optional<velox::vector_size_t>>{
      std::nullopt, std::nullopt, std::nullopt};
  dictionaryValues = std::vector<std::vector<int32_t>>{};
  fullArrayVectorNullable =
      std::vector<std::optional<std::vector<std::optional<int32_t>>>>{
          std::nullopt, std::nullopt, std::nullopt};
  verifyDictionaryEncodedPassthroughNullable(
      offsets, dictionaryValues, fullArrayVectorNullable);
}

TEST_F(VeloxReaderTests, FuzzSimple) {
  auto type = velox::ROW({
      {"bool_val", velox::BOOLEAN()},
      {"byte_val", velox::TINYINT()},
      {"short_val", velox::SMALLINT()},
      {"int_val", velox::INTEGER()},
      {"long_val", velox::BIGINT()},
      {"float_val", velox::REAL()},
      {"double_val", velox::DOUBLE()},
      {"string_val", velox::VARCHAR()},
      {"binary_val", velox::VARBINARY()},
      // {"ts_val", velox::TIMESTAMP()},
  });
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);
  uint32_t seed = FLAGS_reader_tests_seed > 0 ? FLAGS_reader_tests_seed
                                              : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;

  // Small batches creates more edge cases.
  size_t batchSize = 10;
  velox::VectorFuzzer noNulls(
      {
          .vectorSize = batchSize,
          .nullRatio = 0,
          .stringLength = 20,
          .stringVariableLength = true,
      },
      leafPool_.get(),
      seed);

  velox::VectorFuzzer hasNulls{
      {
          .vectorSize = batchSize,
          .nullRatio = 0.05,
          .stringLength = 10,
          .stringVariableLength = true,
      },
      leafPool_.get(),
      seed};

  auto iterations = 20;
  auto batches = 20;
  std::mt19937 rng{seed};
  for (auto parallelismFactor : {0U, 1U, std::thread::hardware_concurrency()}) {
    LOG(INFO) << "Parallelism Factor: " << parallelismFactor;
    nimble::VeloxWriterOptions writerOptions;
    if (parallelismFactor > 0) {
      writerOptions.encodingExecutor =
          std::make_shared<folly::CPUThreadPoolExecutor>(parallelismFactor);
    }

    for (auto i = 0; i < iterations; ++i) {
      writeAndVerify(
          rng,
          *leafPool_,
          rowType,
          [&](auto& type) { return noNulls.fuzzInputRow(type); },
          vectorEquals,
          batches,
          writerOptions);
      writeAndVerify(
          rng,
          *leafPool_,
          rowType,
          [&](auto& type) { return hasNulls.fuzzInputRow(type); },
          vectorEquals,
          batches,
          writerOptions);
    }
  }
}

TEST_F(VeloxReaderTests, FuzzComplex) {
  auto type = velox::ROW(
      {{"array", velox::ARRAY(velox::REAL())},
       {"dict_array", velox::ARRAY(velox::REAL())},
       {"map", velox::MAP(velox::INTEGER(), velox::DOUBLE())},
       {"row",
        velox::ROW({
            {"a", velox::REAL()},
            {"b", velox::INTEGER()},
        })},
       {"nested",
        velox::ARRAY(velox::ROW({
            {"a", velox::INTEGER()},
            {"b", velox::MAP(velox::REAL(), velox::REAL())},
        }))},
       {"nested_map_array1",
        velox::MAP(velox::INTEGER(), velox::ARRAY(velox::REAL()))},
       {"nested_map_array2",
        velox::MAP(velox::INTEGER(), velox::ARRAY(velox::INTEGER()))},
       {"dict_map", velox::MAP(velox::INTEGER(), velox::INTEGER())}});
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);
  uint32_t seed = FLAGS_reader_tests_seed > 0 ? FLAGS_reader_tests_seed
                                              : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;

  nimble::VeloxWriterOptions writerOptions;
  writerOptions.dictionaryArrayColumns.insert("nested_map_array1");
  writerOptions.dictionaryArrayColumns.insert("nested_map_array2");
  writerOptions.dictionaryArrayColumns.insert("dict_array");
  writerOptions.deduplicatedMapColumns.insert("dict_map");
  // Small batches creates more edge cases.
  size_t batchSize = 10;
  velox::VectorFuzzer noNulls(
      {
          .vectorSize = batchSize,
          .nullRatio = 0,
          .stringLength = 20,
          .stringVariableLength = true,
          .containerLength = 5,
          .containerVariableLength = true,
      },
      leafPool_.get(),
      seed);

  velox::VectorFuzzer hasNulls{
      {
          .vectorSize = batchSize,
          .nullRatio = 0.05,
          .stringLength = 10,
          .stringVariableLength = true,
          .containerLength = 5,
          .containerVariableLength = true,
      },
      leafPool_.get(),
      seed};

  auto iterations = 20;
  auto batches = 20;
  std::mt19937 rng{seed};

  for (auto parallelismFactor : {0U, 1U, std::thread::hardware_concurrency()}) {
    LOG(INFO) << "Parallelism Factor: " << parallelismFactor;
    writerOptions.encodingExecutor = parallelismFactor > 0
        ? std::make_shared<folly::CPUThreadPoolExecutor>(parallelismFactor)
        : nullptr;

    for (auto i = 0; i < iterations; ++i) {
      writeAndVerify(
          rng,
          *leafPool_.get(),
          rowType,
          [&](auto& type) { return noNulls.fuzzInputRow(type); },
          vectorEquals,
          batches,
          writerOptions);
      writeAndVerify(
          rng,
          *leafPool_,
          rowType,
          [&](auto& type) { return hasNulls.fuzzInputRow(type); },
          vectorEquals,
          batches,
          writerOptions);
    }
  }
}

TEST_F(VeloxReaderTests, ArrayWithOffsets) {
  auto type = velox::ROW({
      {"dictionaryArray", velox::ARRAY(velox::INTEGER())},
  });
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);
  uint32_t seed = FLAGS_reader_tests_seed > 0 ? FLAGS_reader_tests_seed
                                              : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;

  nimble::VeloxWriterOptions writerOptions;
  writerOptions.dictionaryArrayColumns.insert("dictionaryArray");
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto iterations = 20;
  auto batches = 20;
  std::mt19937 rng{seed};
  int expectedNumArrays = 0;
  bool checkMemoryLeak = true;

  auto compare = [&](const velox::VectorPtr& vector) {
    ASSERT_EQ(
        vector->wrappedVector()
            ->as<velox::RowVector>()
            ->childAt(0)
            ->loadedVector()
            ->wrappedVector()
            ->size(),
        expectedNumArrays);
  };
  for (auto i = 0; i < iterations; ++i) {
    expectedNumArrays = 1;
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVector<int32_t>({{1, 2}}),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVector<int32_t>({{1, 2}, {1, 2}}),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVector<int32_t>({{}}),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVector<int32_t>({{}, {}, {}}),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);

    expectedNumArrays = 3;
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVector<int32_t>(
                      {{1, 2}, {1, 2}, {2, 3}, {5, 6, 7}, {5, 6, 7}}),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVector<int32_t>(
                      {{1, 2}, {1, 2}, {2, 3}, {}, {}}),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVector<int32_t>(
                      {{}, {}, {2, 3}, {5, 6, 7}, {5, 6, 7}}),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVector<int32_t>(
                      {{1, 2}, {1, 2}, {}, {5, 6, 7}, {5, 6, 7}}),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);

    expectedNumArrays = 4;
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVector<int32_t>(
                      {{1, 3}, {1, 2}, {}, {5, 6, 7}, {5, 6, 7}}),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);

    expectedNumArrays = 5;
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  // The middle element is a 0 length element and not null
                  vectorMaker.arrayVector<int32_t>(
                      {{1, 3}, {1, 2}, {}, {1, 2}, {5, 6, 7}}),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);

    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  // The middle element is a 0 length element and not null
                  vectorMaker.arrayVector<int32_t>(
                      {{1, 3}, {1, 2}, {}, {1, 2}, {5, 6, 7}}),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);
  }
}

TEST_F(VeloxReaderTests, ArrayWithOffsetsNullable) {
  auto type = velox::ROW({
      {"dictionaryArray", velox::ARRAY(velox::INTEGER())},
  });
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);
  uint32_t seed = FLAGS_reader_tests_seed > 0 ? FLAGS_reader_tests_seed
                                              : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;

  nimble::VeloxWriterOptions writerOptions;
  writerOptions.dictionaryArrayColumns.insert("dictionaryArray");
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto iterations = 20;
  auto batches = 20;
  std::mt19937 rng{seed};
  int expectedNumArrays = 0;
  bool checkMemoryLeak = true;

  auto compare = [&](const velox::VectorPtr& vector) {
    ASSERT_EQ(
        vector->wrappedVector()
            ->as<velox::RowVector>()
            ->childAt(0)
            ->loadedVector()
            ->wrappedVector()
            ->size(),
        expectedNumArrays);
  };
  for (auto i = 0; i < iterations; ++i) {
    expectedNumArrays = 1;
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVectorNullable<int32_t>({{}, std::nullopt}),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);

    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVectorNullable<int32_t>({std::nullopt}),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);

    expectedNumArrays = 2;
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVectorNullable<int32_t>({
                      std::vector<std::optional<int32_t>>{1, 2, std::nullopt},
                      {},
                      std::vector<std::optional<int32_t>>{1, 2, std::nullopt},
                      std::nullopt,
                      std::vector<std::optional<int32_t>>{1, 2, std::nullopt},
                      std::vector<std::optional<int32_t>>{1, 2, std::nullopt},
                      std::vector<std::optional<int32_t>>{1, 2},
                  }),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);

    expectedNumArrays = 2;
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVectorNullable<int32_t>({
                      std::vector<std::optional<int32_t>>{1, 3},
                      std::vector<std::optional<int32_t>>{1, 2},
                      {},
                      std::vector<std::optional<int32_t>>{1, 2},
                      std::nullopt,
                      std::vector<std::optional<int32_t>>{1, 2},
                  }),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);

    expectedNumArrays = 1;
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVectorNullable<int32_t>({
                      std::vector<std::optional<int32_t>>{1, 2},
                      std::vector<std::optional<int32_t>>{1, 2},
                      {},
                      std::nullopt,
                      std::vector<std::optional<int32_t>>{1, 2},
                  }),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);

    expectedNumArrays = 1;
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVectorNullable<int32_t>({
                      std::vector<std::optional<int32_t>>{1, 2},
                      std::vector<std::optional<int32_t>>{1, 2},
                      std::vector<std::optional<int32_t>>{1, 2},
                      {},
                      std::nullopt,
                  }),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);

    expectedNumArrays = 1;
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVectorNullable<int32_t>({
                      {},
                      std::nullopt,
                      std::vector<std::optional<int32_t>>{1, 2},
                      std::vector<std::optional<int32_t>>{1, 2},
                      std::vector<std::optional<int32_t>>{1, 2},
                  }),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        false,
        checkMemoryLeak);
  }
}

TEST_F(VeloxReaderTests, ArrayWithOffsetsMultiskips) {
  auto type = velox::ROW({
      {"dictionaryArray", velox::ARRAY(velox::INTEGER())},
  });
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);
  uint32_t seed = FLAGS_reader_tests_seed > 0 ? FLAGS_reader_tests_seed
                                              : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;

  nimble::VeloxWriterOptions writerOptions;
  writerOptions.dictionaryArrayColumns.insert("dictionaryArray");
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  auto iterations = 50;
  auto batches = 20;
  std::mt19937 rng{seed};
  int expectedNumArrays = 0;
  bool checkMemoryLeak = true;

  auto compare = [&](const velox::VectorPtr& vector) {
    ASSERT_EQ(
        vector->wrappedVector()
            ->as<velox::RowVector>()
            ->childAt(0)
            ->loadedVector()
            ->wrappedVector()
            ->size(),
        expectedNumArrays);
  };

  auto strideVector = [&](const std::vector<std::vector<int32_t>>& vector) {
    std::vector<std::vector<int32_t>> stridedVector;

    for (auto const& vec : vector) {
      for (uint32_t idx = 0; idx < folly::Random::rand32(1, 5, rng); ++idx) {
        stridedVector.push_back(vec);
      }
    }
    return stridedVector;
  };

  for (auto i = 0; i < iterations; ++i) {
    expectedNumArrays = 6;
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  // The middle element is a 0 length element and not null
                  vectorMaker.arrayVector<int32_t>(strideVector(
                      {{1, 2}, {1, 2, 3}, {}, {1, 2, 3}, {}, {4, 5, 6, 7}})),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        true,
        checkMemoryLeak);

    expectedNumArrays = 3;
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector(
              {"dictionaryArray"},
              {
                  vectorMaker.arrayVectorNullable<int32_t>({
                      std::vector<std::optional<int32_t>>{1, 2},
                      std::vector<std::optional<int32_t>>{1, 2, 3},
                      std::nullopt,
                      std::vector<std::optional<int32_t>>{1, 2, 3},
                      std::nullopt,
                      std::vector<std::optional<int32_t>>{4, 5, 6, 7},
                  }),
              });
        },
        vectorEquals,
        batches,
        writerOptions,
        {},
        nullptr,
        compare,
        true,
        checkMemoryLeak);
  }
}

// convert map to struct
template <typename T = int32_t>
bool compareFlatMap(
    const velox::VectorPtr& expected,
    const velox::VectorPtr& actual,
    velox::vector_size_t index) {
  auto mapVector = expected->as<velox::MapVector>();
  auto offsets = mapVector->rawOffsets();
  auto sizes = mapVector->rawSizes();
  auto keysVector = mapVector->mapKeys()->asFlatVector<T>();
  auto valuesVector = mapVector->mapValues();

  auto structVector = actual->as<velox::RowVector>();
  folly::F14FastMap<std::string, velox::vector_size_t> columnOffsets(
      structVector->childrenSize());
  for (auto i = 0; i < structVector->childrenSize(); ++i) {
    columnOffsets[structVector->type()->asRow().nameOf(i)] = i;
  }

  std::unordered_set<std::string> keys;
  if (!mapVector->isNullAt(index)) {
    for (auto i = offsets[index]; i < offsets[index] + sizes[index]; ++i) {
      auto key = keysVector->valueAtFast(i);
      keys.insert(folly::to<std::string>(key));
      if (!valuesVector->equalValueAt(
              structVector->childAt(columnOffsets[folly::to<std::string>(key)])
                  .get(),
              i,
              index)) {
        return false;
      }
    }
  }
  // missing keys should be null
  for (const auto& columnOffset : columnOffsets) {
    if (keys.count(folly::to<std::string>(columnOffset.first)) == 0 &&
        !structVector->childAt(columnOffset.second)->isNullAt(index)) {
      return false;
    }
  }

  return true;
}

template <typename T = int32_t>
bool compareFlatMaps(
    const velox::VectorPtr& expected,
    const velox::VectorPtr& actual,
    velox::vector_size_t index) {
  auto flat = velox::BaseVector::create(
      expected->type(), expected->size(), expected->pool());
  flat->copy(expected.get(), 0, 0, expected->size());
  auto expectedRow = flat->as<velox::RowVector>();
  auto actualRow = actual->as<velox::RowVector>();
  EXPECT_EQ(expectedRow->childrenSize(), actualRow->childrenSize());
  for (auto i = 0; i < expectedRow->childrenSize(); ++i) {
    auto columnType = actualRow->type()->childAt(i);
    if (columnType->kind() != velox::TypeKind::ROW) {
      return false;
    }
    if (!compareFlatMap<T>(
            expectedRow->childAt(i), actualRow->childAt(i), index)) {
      return false;
    }
  }
  return true;
}

TEST_F(VeloxReaderTests, FlatMapNullValues) {
  testFlatMapNullValues<int8_t>();
  testFlatMapNullValues<int16_t>();
  testFlatMapNullValues<int32_t>();
  testFlatMapNullValues<int64_t>();
  testFlatMapNullValues<float>();
  testFlatMapNullValues<double>();
  testFlatMapNullValues<velox::StringView>();
}

TEST_F(VeloxReaderTests, SlidingWindowMapNestedInFlatMap) {
  auto type = velox::ROW({
      {"nested_map",
       velox::MAP(
           velox::INTEGER(), velox::MAP(velox::INTEGER(), velox::REAL()))},
  });

  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);
  uint32_t seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;

  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto innerMap = vectorMaker.mapVector(

      /*offsets=*/{0, 2, 4, 6, 7},
      /*keys=*/vectorMaker.flatVector<int32_t>({1, 2, 2, 1, 1, 2, 4, 4}),
      /*values=*/
      vectorMaker.flatVector<float>({0.1, 0.2, 0.2, 0.1, 0.1, 0.3, 0.4, 0.4}),
      /*nulls=*/{});

  auto outerMap = vectorMaker.mapVector(
      /*offsets=*/{0, 1, 2, 3, 4},
      /*keys=*/vectorMaker.flatVector<int32_t>({1, 1, 1, 1, 1}),
      /*values=*/{innerMap},
      /*null=s*/ {});

  nimble::VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns.insert("nested_map");
  nimble::VeloxReadParams params;
  params.readFlatMapFieldAsStruct.insert("nested_map");
  params.flatMapFeatureSelector.insert({"nested_map", {{"1"}}});
  writerOptions.deduplicatedMapColumns.insert("nested_map");

  int deduplicatedMapCount = 3;
  auto iterations = 10;
  auto batches = 20;
  std::mt19937 rng{seed};
  bool checkMemoryLeak = true;
  /* Check the inner map is a deduplicated map */
  auto compare = [&](const velox::VectorPtr& vector) {
    auto structVector =
        vector->as<velox::RowVector>()->childAt(0)->as<velox::RowVector>();
    ASSERT_EQ(
        structVector->childAt(0)->wrappedVector()->size(),
        deduplicatedMapCount);
  };
  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify(
        rng,
        *leafPool_.get(),
        rowType,
        [&](auto& /*type*/) {
          return vectorMaker.rowVector({"nested_map"}, {outerMap});
        },
        compareFlatMaps<int32_t>,
        batches,
        writerOptions,
        params,
        nullptr,
        compare,
        false,
        checkMemoryLeak);
  }
}

TEST_F(VeloxReaderTests, FlatMapToStruct) {
  auto floatFeatures = velox::MAP(velox::INTEGER(), velox::REAL());
  auto idListFeatures =
      velox::MAP(velox::INTEGER(), velox::ARRAY(velox::BIGINT()));
  auto idScoreListFeatures =
      velox::MAP(velox::INTEGER(), velox::MAP(velox::BIGINT(), velox::REAL()));
  auto rowColumn = velox::MAP(
      velox::INTEGER(),
      velox::ROW({{"a", velox::INTEGER()}, {"b", velox::REAL()}}));

  auto type = velox::ROW({
      {"float_features", floatFeatures},
      {"id_list_features", idListFeatures},
      {"id_score_list_features", idScoreListFeatures},
      {"row_column", rowColumn},
  });
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);

  VeloxMapGeneratorConfig generatorConfig{
      .featureTypes = rowType,
      .keyType = velox::TypeKind::INTEGER,
      .maxNumKVPerRow = 10};
  VeloxMapGenerator generator(leafPool_.get(), generatorConfig);

  nimble::VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns.insert("float_features");
  writerOptions.flatMapColumns.insert("id_list_features");
  writerOptions.flatMapColumns.insert("id_score_list_features");
  writerOptions.flatMapColumns.insert("row_column");

  nimble::VeloxReadParams params;
  params.readFlatMapFieldAsStruct.insert("float_features");
  params.readFlatMapFieldAsStruct.insert("id_list_features");
  params.readFlatMapFieldAsStruct.insert("id_score_list_features");
  params.readFlatMapFieldAsStruct.insert("row_column");
  for (auto i = 0; i < 10; ++i) {
    params.flatMapFeatureSelector["float_features"].features.push_back(
        folly::to<std::string>(i));
    params.flatMapFeatureSelector["id_list_features"].features.push_back(
        folly::to<std::string>(i));
    params.flatMapFeatureSelector["id_score_list_features"].features.push_back(
        folly::to<std::string>(i));
    params.flatMapFeatureSelector["row_column"].features.push_back(
        folly::to<std::string>(i));
  }

  auto iterations = 20;
  auto batches = 10;
  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify(
        generator.rng(),
        *leafPool_,
        rowType,
        [&](auto&) { return generator.generateBatch(10); },
        compareFlatMaps<int32_t>,
        batches,
        writerOptions,
        params);
  }
}

TEST_F(VeloxReaderTests, FlatMapToStructForComplexType) {
  auto rowColumn = velox::MAP(
      velox::INTEGER(),
      velox::ROW(
          {{"a", velox::INTEGER()},
           {"b", velox::MAP(velox::INTEGER(), velox::ARRAY(velox::REAL()))}}));

  auto type = velox::ROW({
      {"row_column", rowColumn},
  });
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);

  VeloxMapGeneratorConfig generatorConfig{
      .featureTypes = rowType,
      .keyType = velox::TypeKind::INTEGER,
      .maxNumKVPerRow = 10};
  VeloxMapGenerator generator(leafPool_.get(), generatorConfig);

  nimble::VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns.insert("row_column");

  nimble::VeloxReadParams params;
  params.readFlatMapFieldAsStruct.insert("row_column");
  for (auto i = 0; i < 10; ++i) {
    params.flatMapFeatureSelector["row_column"].features.push_back(
        folly::to<std::string>(i));
  }

  auto iterations = 20;
  auto batches = 10;
  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify(
        generator.rng(),
        *leafPool_,
        rowType,
        [&](auto&) { return generator.generateBatch(10); },
        compareFlatMaps<int32_t>,
        batches,
        writerOptions,
        params);
  }
}

TEST_F(VeloxReaderTests, StringKeyFlatMapAsStruct) {
  auto stringKeyFeatures = velox::MAP(velox::VARCHAR(), velox::REAL());
  auto type = velox::ROW({
      {"string_key_feature", stringKeyFeatures},
  });
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);

  nimble::VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns.insert("string_key_feature");

  VeloxMapGeneratorConfig generatorConfig{
      .featureTypes = rowType,
      .keyType = velox::TypeKind::VARCHAR,
      .stringKeyPrefix = "testKeyString_",
      .maxNumKVPerRow = 10,
  };
  VeloxMapGenerator generator(leafPool_.get(), generatorConfig);

  nimble::VeloxReadParams params;
  params.readFlatMapFieldAsStruct.emplace("string_key_feature");
  for (auto i = 0; i < 10; ++i) {
    params.flatMapFeatureSelector["string_key_feature"].features.push_back(
        "testKeyString_" + folly::to<std::string>(i));
  }

  auto iterations = 10;
  auto batches = 1;
  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify(
        generator.rng(),
        *leafPool_,
        rowType,
        [&](auto&) { return generator.generateBatch(10); },
        compareFlatMaps<velox::StringView>,
        batches,
        writerOptions,
        params);
  }

  iterations = 20;
  batches = 10;
  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify(
        generator.rng(),
        *leafPool_,
        rowType,
        [&](auto&) { return generator.generateBatch(10); },
        compareFlatMaps<velox::StringView>,
        batches,
        writerOptions,
        params);
  }
}

TEST_F(VeloxReaderTests, FlatMapAsMapEncoding) {
  auto floatFeatures = velox::MAP(velox::INTEGER(), velox::REAL());
  auto idListFeatures =
      velox::MAP(velox::INTEGER(), velox::ARRAY(velox::BIGINT()));
  auto idScoreListFeatures =
      velox::MAP(velox::INTEGER(), velox::MAP(velox::BIGINT(), velox::REAL()));
  auto deduplicatedIdScoreListFeatures =
      velox::MAP(velox::INTEGER(), velox::MAP(velox::BIGINT(), velox::REAL()));
  auto deduplicatedIdListFeatures =
      velox::MAP(velox::INTEGER(), velox::ARRAY(velox::BIGINT()));
  auto type = velox::ROW({
      {"float_features", floatFeatures},
      {"id_list_features", idListFeatures},
      {"id_score_list_features", idScoreListFeatures},
      {"deduplicated_id_score_list_features", deduplicatedIdScoreListFeatures},
      {"deduplicated_id_list_features", deduplicatedIdListFeatures},
  });
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);
  VeloxMapGeneratorConfig generatorConfig{
      .featureTypes = rowType,
      .keyType = velox::TypeKind::INTEGER,
  };
  VeloxMapGenerator generator(leafPool_.get(), generatorConfig);

  nimble::VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns.emplace("float_features");
  writerOptions.flatMapColumns.emplace("id_list_features");
  writerOptions.flatMapColumns.emplace("id_score_list_features");
  writerOptions.flatMapColumns.emplace("deduplicated_id_score_list_features");
  writerOptions.flatMapColumns.emplace("deduplicated_id_list_features");
  writerOptions.deduplicatedMapColumns.emplace(
      "deduplicated_id_score_list_features");
  writerOptions.dictionaryArrayColumns.emplace("deduplicated_id_list_features");
  // Verify the flatmap read without feature selection they are read as
  // MapEncoding
  nimble::VeloxReadParams params;
  auto iterations = 10;
  auto batches = 10;
  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify(
        generator.rng(),
        *leafPool_,
        rowType,
        [&](auto&) { return generator.generateBatch(10); },
        vectorEquals,
        batches,
        writerOptions,
        params);
  }

  for (auto i = 0; i < 10; ++i) {
    params.flatMapFeatureSelector["float_features"].features.push_back(
        folly::to<std::string>(i));
    params.flatMapFeatureSelector["id_list_features"].features.push_back(
        folly::to<std::string>(i));
    params.flatMapFeatureSelector["id_score_list_features"].features.push_back(
        folly::to<std::string>(i));
    params.flatMapFeatureSelector["deduplicated_id_score_list_features"]
        .features.push_back(folly::to<std::string>(i));
    params.flatMapFeatureSelector["deduplicated_id_list_features"]
        .features.push_back(folly::to<std::string>(i));
  }

  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify(
        generator.rng(),
        *leafPool_,
        rowType,
        [&](auto&) { return generator.generateBatch(10); },
        vectorEquals,
        batches,
        writerOptions,
        params);
  }

  {
    // Selecting only odd values column from flat map
    params.flatMapFeatureSelector.clear();
    for (auto i = 0; i < 10; ++i) {
      if (i % 2 == 1) {
        params.flatMapFeatureSelector["float_features"].features.push_back(
            folly::to<std::string>(i));
        params.flatMapFeatureSelector["id_list_features"].features.push_back(
            folly::to<std::string>(i));
        params.flatMapFeatureSelector["id_score_list_features"]
            .features.push_back(folly::to<std::string>(i));
        params.flatMapFeatureSelector["deduplicated_id_score_list_features"]
            .features.push_back(folly::to<std::string>(i));
        params.flatMapFeatureSelector["deduplicated_id_list_features"]
            .features.push_back(folly::to<std::string>(i));
      }
    }

    std::unordered_set<std::string> floatFeaturesLookup{
        params.flatMapFeatureSelector["float_features"].features.begin(),
        params.flatMapFeatureSelector["float_features"].features.end()};
    std::unordered_set<std::string> idListFeaturesLookup{
        params.flatMapFeatureSelector["id_list_features"].features.begin(),
        params.flatMapFeatureSelector["id_list_features"].features.end()};
    std::unordered_set<std::string> idScoreListFeaturesLookup{
        params.flatMapFeatureSelector["id_score_list_features"]
            .features.begin(),
        params.flatMapFeatureSelector["id_score_list_features"].features.end()};
    std::unordered_set<std::string> deduplicatedIdScoreListFeaturesLookp{
        params.flatMapFeatureSelector["deduplicated_id_score_list_features"]
            .features.begin(),
        params.flatMapFeatureSelector["deduplicated_id_score_list_features"]
            .features.end()};
    std::unordered_set<std::string> deduplicatedIdListFeaturesLookp{
        params.flatMapFeatureSelector["deduplicated_id_list_features"]
            .features.begin(),
        params.flatMapFeatureSelector["deduplicated_id_list_features"]
            .features.end()};
    auto isKeyPresent = [&](std::string& key) {
      return floatFeaturesLookup.find(key) != floatFeaturesLookup.end() ||
          idListFeaturesLookup.find(key) != idListFeaturesLookup.end() ||
          idScoreListFeaturesLookup.find(key) !=
          idScoreListFeaturesLookup.end() ||
          deduplicatedIdScoreListFeaturesLookp.find(key) !=
          deduplicatedIdScoreListFeaturesLookp.end() ||
          deduplicatedIdListFeaturesLookp.find(key) !=
          deduplicatedIdListFeaturesLookp.end();
    };
    for (auto i = 0; i < iterations; ++i) {
      writeAndVerify(
          generator.rng(),
          *leafPool_,
          rowType,
          [&](auto&) { return generator.generateBatch(10); },
          vectorEquals,
          batches,
          writerOptions,
          params,
          isKeyPresent);
    }
  }

  {
    // Exclude odd values column from flat map
    params.flatMapFeatureSelector.clear();
    std::unordered_set<std::string> floatFeaturesLookup;
    std::unordered_set<std::string> idListFeaturesLookup;
    std::unordered_set<std::string> idScoreListFeaturesLookup;
    std::unordered_set<std::string> deduplicatedIdScoreListFeaturesLookp;
    std::unordered_set<std::string> deduplicatedIdListFeaturesLookp;

    params.flatMapFeatureSelector["float_features"].mode =
        nimble::SelectionMode::Exclude;
    params.flatMapFeatureSelector["id_list_features"].mode =
        nimble::SelectionMode::Exclude;
    params.flatMapFeatureSelector["id_score_list_features"].mode =
        nimble::SelectionMode::Exclude;
    params.flatMapFeatureSelector["deduplicated_id_score_list_features"].mode =
        nimble::SelectionMode::Exclude;
    params.flatMapFeatureSelector["deduplicated_id_list_features"].mode =
        nimble::SelectionMode::Exclude;
    for (auto i = 0; i < 10; ++i) {
      std::string iStr = folly::to<std::string>(i);
      if (i % 2 == 1) {
        params.flatMapFeatureSelector["float_features"].features.push_back(
            iStr);
        params.flatMapFeatureSelector["id_list_features"].features.push_back(
            iStr);
        params.flatMapFeatureSelector["id_score_list_features"]
            .features.push_back(iStr);
        params.flatMapFeatureSelector["deduplicated_id_score_list_features"]
            .features.push_back(iStr);
        params.flatMapFeatureSelector["deduplicated_id_list_features"]
            .features.push_back(iStr);
      } else {
        floatFeaturesLookup.insert(iStr);
        idListFeaturesLookup.insert(iStr);
        idScoreListFeaturesLookup.insert(iStr);
        deduplicatedIdScoreListFeaturesLookp.insert(iStr);
        deduplicatedIdListFeaturesLookp.insert(iStr);
      }
    }

    auto isKeyPresent = [&](std::string& key) {
      return floatFeaturesLookup.find(key) != floatFeaturesLookup.end() ||
          idListFeaturesLookup.find(key) != idListFeaturesLookup.end() ||
          idScoreListFeaturesLookup.find(key) !=
          idScoreListFeaturesLookup.end() ||
          deduplicatedIdScoreListFeaturesLookp.find(key) !=
          deduplicatedIdScoreListFeaturesLookp.end() ||
          deduplicatedIdListFeaturesLookp.find(key) !=
          deduplicatedIdListFeaturesLookp.end();
    };
    for (auto i = 0; i < iterations; ++i) {
      writeAndVerify(
          generator.rng(),
          *leafPool_,
          rowType,
          [&](auto&) { return generator.generateBatch(10); },
          vectorEquals,
          batches,
          writerOptions,
          params,
          isKeyPresent);
    }
  }
}

TEST_F(VeloxReaderTests, StringKeyFlatMapAsMapEncoding) {
  auto stringKeyFeatures = velox::MAP(velox::VARCHAR(), velox::REAL());
  auto type = velox::ROW({
      {"string_key_feature", stringKeyFeatures},
  });
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type);

  VeloxMapGeneratorConfig generatorConfig{
      .featureTypes = rowType,
      .keyType = velox::TypeKind::VARCHAR,
      .stringKeyPrefix = "testKeyString_",
  };
  VeloxMapGenerator generator(leafPool_.get(), generatorConfig);

  nimble::VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns.insert("string_key_feature");

  nimble::VeloxReadParams params;
  // Selecting only keys with even index to it
  for (auto i = 0; i < 10; ++i) {
    if (i % 2 == 0) {
      params.flatMapFeatureSelector["string_key_feature"].features.push_back(
          "testKeyString_" + folly::to<std::string>(i));
    }
  }

  std::unordered_set<std::string> stringKeyFeature{
      params.flatMapFeatureSelector["string_key_feature"].features.begin(),
      params.flatMapFeatureSelector["string_key_feature"].features.end()};

  auto isKeyPresent = [&](std::string& key) {
    return stringKeyFeature.find(key) != stringKeyFeature.end();
  };

  auto iterations = 10;

  // Keeping the batchCount 1 produces the case where flatmap readers nulls
  // column is empty, as decodedmap produce the mayHavenulls as false
  auto batches = 1;
  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify<velox::StringView>(
        generator.rng(),
        *leafPool_,
        rowType,
        [&](auto&) { return generator.generateBatch(10); },
        nullptr, /* for key present use a fix function */
        batches,
        writerOptions,
        params,
        isKeyPresent);
  }

  iterations = 20;
  batches = 10;
  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify<velox::StringView>(
        generator.rng(),
        *leafPool_,
        rowType,
        [&](auto&) { return generator.generateBatch(10); },
        nullptr, /* for key present use a fix function */
        batches,
        writerOptions,
        params,
        isKeyPresent);
  }
}

class TestNimbleReaderFactory {
 public:
  TestNimbleReaderFactory(
      velox::memory::MemoryPool& leafPool,
      velox::memory::MemoryPool& rootPool,
      std::vector<velox::VectorPtr> vectors,
      const nimble::VeloxWriterOptions& writerOptions = {})
      : memoryPool_(leafPool) {
    file_ = std::make_unique<velox::InMemoryReadFile>(
        nimble::test::createNimbleFile(rootPool, vectors, writerOptions));
    type_ = std::dynamic_pointer_cast<const velox::RowType>(vectors[0]->type());
  }

  nimble::VeloxReader createReader(nimble::VeloxReadParams params = {}) {
    auto selector =
        std::make_shared<velox::dwio::common::ColumnSelector>(type_);
    return nimble::VeloxReader(
        this->memoryPool_, file_.get(), std::move(selector), params);
  }

  nimble::TabletReader createTablet() {
    return nimble::TabletReader(memoryPool_, file_.get());
  }

 private:
  std::unique_ptr<velox::InMemoryReadFile> file_;
  std::shared_ptr<const velox::RowType> type_;
  velox::memory::MemoryPool& memoryPool_;
};

std::vector<velox::VectorPtr> createSkipSeekVectors(
    velox::memory::MemoryPool& pool,
    const std::vector<int>& rowsPerStripe) {
  uint32_t seed = FLAGS_reader_tests_seed > 0 ? FLAGS_reader_tests_seed
                                              : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};

  std::vector<velox::VectorPtr> vectors(rowsPerStripe.size());
  velox::test::VectorMaker vectorMaker{&pool};

  for (auto i = 0; i < rowsPerStripe.size(); ++i) {
    std::string s;
    vectors[i] = vectorMaker.rowVector(
        {"a", "b", "dictionaryArray"},
        {vectorMaker.flatVector<int32_t>(
             rowsPerStripe[i],
             /* valueAt */
             [&rng](auto /* row */) { return folly::Random::rand32(rng); },
             /* isNullAt */ [](auto row) { return row % 2 == 1; }),
         vectorMaker.flatVector<velox::StringView>(
             rowsPerStripe[i],
             /* valueAt */
             [&s, &rng](auto /* row */) {
               s = "arf_" + folly::to<std::string>(folly::Random::rand32(rng));
               return velox::StringView(s.data(), s.size());
             },
             /* isNullAt */ [](auto row) { return row % 2 == 1; }),
         vectorMaker.arrayVector<int32_t>(
             rowsPerStripe[i],
             /* sizeAt */
             [](auto /* row */) { return 1; },
             /* valueAt */
             [](auto row) {
               /* duplicated values to check cache usage */
               return row / 4;
             })});
  }

  return vectors;
}

void readAndVerifyContent(
    nimble::VeloxReader& reader,
    std::vector<velox::VectorPtr> expectedVectors,
    uint32_t rowsToRead,
    uint32_t expectedNumberOfRows,
    uint32_t expectedStripe,
    velox::vector_size_t expectedRowInStripe) {
  velox::VectorPtr result;
  EXPECT_TRUE(reader.next(rowsToRead, result));
  ASSERT_EQ(result->type()->kind(), velox::TypeKind::ROW);
  velox::RowVector* rowVec = result->as<velox::RowVector>();
  ASSERT_EQ(rowVec->childAt(0)->type()->kind(), velox::TypeKind::INTEGER);
  ASSERT_EQ(rowVec->childAt(1)->type()->kind(), velox::TypeKind::VARCHAR);
  ASSERT_EQ(rowVec->childAt(2)->type()->kind(), velox::TypeKind::ARRAY);
  const int curRows = result->size();
  ASSERT_EQ(curRows, expectedNumberOfRows);
  ASSERT_LT(expectedStripe, expectedVectors.size());
  auto& expected = expectedVectors[expectedStripe];

  for (velox::vector_size_t i = 0; i < curRows; ++i) {
    if (!expected->equalValueAt(result.get(), i + expectedRowInStripe, i)) {
      ASSERT_TRUE(
          expected->equalValueAt(result.get(), i + expectedRowInStripe, i))
          << "Content mismatch at index " << i
          << "\nReference: " << expected->toString(i + expectedRowInStripe)
          << "\nResult: " << result->toString(i);
    }
  }
}

TEST_F(VeloxReaderTests, ReaderSeekTest) {
  // Generate an Nimble file with 3 stripes and 10 rows each
  auto vectors = createSkipSeekVectors(*leafPool_, {10, 10, 10});
  nimble::VeloxWriterOptions writerOptions;
  writerOptions.dictionaryArrayColumns.insert("dictionaryArray");

  TestNimbleReaderFactory readerFactory(
      *leafPool_, *rootPool_, vectors, writerOptions);
  auto reader = readerFactory.createReader();
  auto rowNumber = reader.getRowNumber();
  EXPECT_EQ(0, rowNumber);

  auto rowResult = reader.skipRows(0);
  EXPECT_EQ(0, rowResult);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(0, rowNumber);
  rowResult = reader.seekToRow(0);
  EXPECT_EQ(0, rowResult);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(0, rowNumber);

  // [Stripe# 0, Current Pos: 0] seek to 1 position
  rowResult = reader.seekToRow(1);
  EXPECT_EQ(rowResult, 1);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 1);
  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 1,
      /* expectedNumberOfRows */ 1,
      /* expectedStripe */ 0,
      /* expectedRowInStripe */ 1);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(2, rowNumber);

  // [Stripe# 0, Current Pos: 2] seek to 5 position
  rowResult = reader.seekToRow(5);
  rowNumber = reader.getRowNumber();
  // [Stripe# 0, Current Pos: 5] seeks start from rowIdx 0
  EXPECT_EQ(rowResult, 5);
  EXPECT_EQ(rowNumber, 5);
  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 6,
      /* expectedNumberOfRows */ 5,
      /* expectedStripe */ 0,
      /* expectedRowInStripe */ 5);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 10);

  // [Stripe# 0, Current Pos: 10] seek to 10 position
  rowResult = reader.seekToRow(10);
  rowNumber = reader.getRowNumber();
  // [Stripe# 1, Current Pos: 0]
  EXPECT_EQ(rowResult, 10);
  EXPECT_EQ(rowNumber, 10);
  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 10,
      /* expectedNumberOfRows */ 10,
      /* expectedStripe */ 1,
      /* expectedRowInStripe */ 0);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(20, rowNumber);

  // [Stripe# 2, Current Pos: 0]
  rowResult = reader.seekToRow(29);
  rowNumber = reader.getRowNumber();
  // [Stripe# 2, Current Pos: 9]
  EXPECT_EQ(rowResult, 29);
  EXPECT_EQ(rowNumber, 29);
  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 2,
      /* expectedNumberOfRows */ 1,
      /* expectedStripe */ 2,
      /* expectedRowInStripe */ 9);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(30, rowNumber);

  // seek past
  {
    rowResult = reader.seekToRow(32);
    // Seeks with rows >= totalRows in Nimble file, seeks to lastRow
    EXPECT_EQ(rowResult, 30);
    rowNumber = reader.getRowNumber();
    EXPECT_EQ(rowNumber, 30);

    velox::VectorPtr result;
    EXPECT_FALSE(reader.next(1, result));
    rowNumber = reader.getRowNumber();
    EXPECT_EQ(rowNumber, 30);
  }
}

TEST_F(VeloxReaderTests, ReaderSkipTest) {
  // Generate an Nimble file with 3 stripes and 10 rows each
  auto vectors = createSkipSeekVectors(*leafPool_, {10, 10, 10});
  nimble::VeloxWriterOptions writerOptions;
  writerOptions.dictionaryArrayColumns.insert("dictionaryArray");

  TestNimbleReaderFactory readerFactory(
      *leafPool_, *rootPool_, vectors, writerOptions);
  auto reader = readerFactory.createReader();
  auto rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 0);

  // Current position in Comments below represent the position in stripe
  // [Stripe# 0, Current Pos: 0], After skip [Stripe# 0, Current Pos: 1]
  auto rowResult = reader.skipRows(1);
  EXPECT_EQ(rowResult, 1);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 1);
  // readAndVerifyContent() moves the rowPosition in reader
  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 1,
      /* expectedNumberOfRows */ 1,
      /* expectedStripe */ 0,
      /* expectedRowInStripe */ 1);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 2);

  // [Stripe# 0, Current Pos: 2], After skip [Stripe# 0, Current Pos: 7]
  rowResult = reader.skipRows(5);
  EXPECT_EQ(rowResult, 5);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 7);

  // reader don't read across stripe so expectedRow is 3
  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 4,
      /* expectedNumberOfRows */ 3,
      /* expectedStripe */ 0,
      /* expectedRowInStripe */ 7);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 10);

  // [Stripe# 1, Current Pos: 0], After skip [Stripe# 2, Current Pos: 0]
  rowResult = reader.skipRows(10);
  EXPECT_EQ(rowResult, 10);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 20);
  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 1,
      /* expectedNumberOfRows */ 1,
      /* expectedStripe */ 2,
      /* expectedRowInStripe */ 0);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 21);

  // [Stripe# 2, Current Pos: 1], After skip [Stripe# 2, Current Pos: 9]
  rowResult = reader.skipRows(8);
  EXPECT_EQ(rowResult, 8);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 29);
  // reader don't read across stripe so expectedRow is 1
  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 2,
      /* expectedNumberOfRows */ 1,
      /* expectedStripe */ 2,
      /* expectedRowInStripe */ 9);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 30);

  {
    // [Stripe# 3, Current Pos: 0], Reached EOF
    rowResult = reader.skipRows(5);
    EXPECT_EQ(rowResult, 0);
    rowNumber = reader.getRowNumber();
    EXPECT_EQ(rowNumber, 30);

    velox::VectorPtr result;
    EXPECT_FALSE(reader.next(1, result));
    rowNumber = reader.getRowNumber();
    EXPECT_EQ(rowNumber, 30);
  }

  // Try to seek to start and test skip
  rowResult = reader.seekToRow(0);
  EXPECT_EQ(0, rowResult);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(0, rowNumber);

  // [Stripe# 0, Current Pos: 0], After skip [Stripe# 1, Current Pos: 2]
  rowResult = reader.skipRows(12);
  EXPECT_EQ(rowResult, 12);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 12);
  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 10,
      /* expectedNumberOfRows */ 8,
      /* expectedStripe */ 1,
      /* expectedRowInStripe */ 2);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 20);

  // Test continuous skip calls and then readandVerify
  reader.seekToRow(0);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 0);

  // [Stripe# 0, Current Pos: 0], After skip [Stripe# 1, Current Pos: 0]
  for (int i = 0; i < 10; ++i) {
    rowResult = reader.skipRows(1);
    EXPECT_EQ(rowResult, 1);
    EXPECT_EQ(reader.getRowNumber(), ++rowNumber);
  }

  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 1,
      /* expectedNumberOfRows */ 1,
      /* expectedStripe */ 1,
      /* expectedRowInStripe */ 0);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 11);

  // Continuous skip calls across stripe
  // [Stripe# 1, Current Pos: 1], After skip [Stripe# 2, Current Pos: 9]
  for (int i = 0; i < 6; ++i) {
    rowResult = reader.skipRows(3);
    rowNumber += 3;
    EXPECT_EQ(reader.getRowNumber(), rowNumber);
  }
  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 2,
      /* expectedNumberOfRows */ 1,
      /* expectedStripe */ 2,
      /* expectedRowInStripe */ 9);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 30);
  {
    // Current position: EOF
    velox::VectorPtr result;
    EXPECT_FALSE(reader.next(1, result));
    rowNumber = reader.getRowNumber();
    EXPECT_EQ(rowNumber, 30);
  }

  // Read the data(This also move the reader state) follow by skips and verify
  reader.seekToRow(0);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 0);
  for (int i = 0; i < 11; ++i) {
    readAndVerifyContent(
        reader,
        vectors,
        /* rowsToRead */ 1,
        /* expectedNumberOfRows */ 1,
        /* expectedStripe */ (i / 10),
        /* expectedRowInStripe */ (i % 10));
    EXPECT_EQ(reader.getRowNumber(), ++rowNumber);
  }
  // [Stripe# 1, Current Pos: 1], After skip [Stripe# 1, Current Pos: 6]
  rowResult = reader.skipRows(5);
  EXPECT_EQ(rowResult, 5);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 16);
  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 5,
      /* expectedNumberOfRows */ 4,
      /* expectedStripe */ 1,
      /* expectedRowInStripe */ 6);
  rowNumber = reader.getRowNumber();
  EXPECT_EQ(rowNumber, 20);

  {
    // verify the skip to more rows then file have
    reader.seekToRow(0);
    rowNumber = reader.getRowNumber();
    EXPECT_EQ(rowNumber, 0);
    // [Stripe# 0, Current Pos: 0], After skip EOF
    rowResult = reader.skipRows(32);
    EXPECT_EQ(30, rowResult);
    rowNumber = reader.getRowNumber();
    EXPECT_EQ(rowNumber, 30);
    velox::VectorPtr result;
    EXPECT_FALSE(reader.next(1, result));

    reader.seekToRow(0);
    rowNumber = reader.getRowNumber();
    EXPECT_EQ(rowNumber, 0);
    // [Stripe# 0, Current Pos: 0], After skip [Stripe# 2, Current Pos: 2]
    rowResult = reader.skipRows(22);
    EXPECT_EQ(22, rowResult);
    rowNumber = reader.getRowNumber();
    EXPECT_EQ(rowNumber, 22);
    readAndVerifyContent(
        reader,
        vectors,
        /* rowsToRead */ 9,
        /* expectedNumberOfRows */ 8,
        /* expectedStripe */ 2,
        /* expectedRowInStripe */ 2);
    rowNumber = reader.getRowNumber();
    EXPECT_EQ(rowNumber, 30);

    EXPECT_FALSE(reader.next(1, result));
    rowNumber = reader.getRowNumber();
    EXPECT_EQ(rowNumber, 30);
  }
}

TEST_F(VeloxReaderTests, ReaderSkipSingleStripeTest) {
  // Generate an Nimble file with 1 stripe and 12 rows
  auto vectors = createSkipSeekVectors(*leafPool_, {12});
  nimble::VeloxWriterOptions writerOptions;
  writerOptions.dictionaryArrayColumns.insert("dictionaryArray");

  TestNimbleReaderFactory readerFactory(
      *leafPool_, *rootPool_, vectors, writerOptions);
  auto reader = readerFactory.createReader();

  // Current position in Comments below represent the position in stripe
  // [Stripe# 0, Current Pos: 0], After skip [Stripe# 0, Current Pos: 1]
  auto rowResult = reader.skipRows(1);
  EXPECT_EQ(rowResult, 1);
  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 12,
      /* expectedNumberOfRows */ 11,
      /* expectedStripe */ 0,
      /* expectedRowInStripe */ 1);

  // Current pos : EOF, try to read skip past it
  {
    rowResult = reader.skipRows(13);
    EXPECT_EQ(rowResult, 0);
    rowResult = reader.skipRows(1);
    EXPECT_EQ(rowResult, 0);
    velox::VectorPtr result;
    EXPECT_FALSE(reader.next(1, result));
  }

  // Seek to position 2 and then skip 11 rows to reach EOF
  rowResult = reader.seekToRow(2);
  EXPECT_EQ(rowResult, 2);
  rowResult = reader.skipRows(11);
  EXPECT_EQ(rowResult, 10);
  {
    velox::VectorPtr result;
    EXPECT_FALSE(reader.next(1, result));
  }

  // Seek to 0 and skip 13 rows
  rowResult = reader.seekToRow(0);
  EXPECT_EQ(rowResult, 0);
  rowResult = reader.skipRows(13);
  EXPECT_EQ(rowResult, 12);
  {
    velox::VectorPtr result;
    EXPECT_FALSE(reader.next(1, result));
  }
}

TEST_F(VeloxReaderTests, ReaderSeekSingleStripeTest) {
  // Generate an Nimble file with 1 stripes and 11 rows
  auto vectors = createSkipSeekVectors(*leafPool_, {11});
  nimble::VeloxWriterOptions writerOptions;
  writerOptions.dictionaryArrayColumns.insert("dictionaryArray");

  TestNimbleReaderFactory readerFactory(
      *leafPool_, *rootPool_, vectors, writerOptions);
  auto reader = readerFactory.createReader();

  // Current position in Comments below represent the position in stripe
  // [Stripe# 0, Current Pos: 0], After skip [Stripe# 0, Current Pos: 5]
  auto rowResult = reader.seekToRow(5);
  EXPECT_EQ(rowResult, 5);
  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 12,
      /* expectedNumberOfRows */ 6,
      /* expectedStripe */ 0,
      /* expectedRowInStripe */ 5);

  // Current pos : EOF, try to read skip past it
  {
    rowResult = reader.seekToRow(15);
    EXPECT_EQ(rowResult, 11);
    velox::VectorPtr result;
    EXPECT_FALSE(reader.next(1, result));
    rowResult = reader.seekToRow(10000);
    EXPECT_EQ(rowResult, 11);
    EXPECT_FALSE(reader.next(1, result));
  }
}

TEST_F(VeloxReaderTests, ReaderSkipUnevenStripesTest) {
  // Generate an Nimble file with 4 stripes
  auto vectors = createSkipSeekVectors(*leafPool_, {12, 15, 25, 18});
  nimble::VeloxWriterOptions writerOptions;
  writerOptions.dictionaryArrayColumns.insert("dictionaryArray");

  TestNimbleReaderFactory readerFactory(
      *leafPool_, *rootPool_, vectors, writerOptions);
  auto reader = readerFactory.createReader();

  // Current position in Comments below represent the position in stripe
  // [Stripe# 0, Current Pos: 0], After skip [Stripe# 2, Current Pos: 8]
  auto rowResult = reader.skipRows(35);
  EXPECT_EQ(rowResult, 35);
  readAndVerifyContent(
      reader,
      vectors,
      /* rowsToRead */ 12,
      /* expectedNumberOfRows */ 12,
      /* expectedStripe */ 2,
      /* expectedRowInStripe */ 8);

  // [Stripe# 2, Current Pos: 20], After skip EOF
  {
    rowResult = reader.skipRows(25);
    EXPECT_EQ(rowResult, 23);
    velox::VectorPtr result;
    EXPECT_FALSE(reader.next(1, result));
  }
}

// this test is created to keep an eye on the default value for T() for
// primitive type. Recently it came to our notice that T() does zero
// initialize the value for optimized builds. T() we have used a bit in the
// code to zero out the result. This is a dummy test to fail fast if it is not
// zero initialized for primitive types
TEST_F(VeloxReaderTests, TestPrimitiveFieldDefaultValue) {
  verifyDefaultValue<velox::vector_size_t>(2, 0, 10);
  verifyDefaultValue<int8_t>(2, 0, 30);
  verifyDefaultValue<uint8_t>(2, 0, 30);
  verifyDefaultValue<int16_t>(2, 0, 30);
  verifyDefaultValue<uint16_t>(2, 0, 30);
  verifyDefaultValue<int64_t>(2, 0, 30);
  verifyDefaultValue<uint64_t>(2, 0, 30);
  verifyDefaultValue<uint32_t>(2, 0.0, 30);
  verifyDefaultValue<float>(2.1, 0.0, 30);
  verifyDefaultValue<bool>(true, false, 30);
  verifyDefaultValue<double>(3.2, 0.0, 30);
  verifyDefaultValue<std::string>("test", "", 30);
}

struct RangeTestParams {
  uint64_t rangeStart;
  uint64_t rangeEnd;
  uint32_t firstRow;

  // Tuple arguments: rowsToRead, expectedNumberOfRows, expectedStripe,
  // expectedRowInStripe, expectedRowNumber
  std::vector<std::tuple<uint32_t, uint32_t, uint32_t, uint32_t, uint32_t>>
      expectedReads;

  // Tuple arguments: seekToRow, expectedSeekResult
  std::vector<std::tuple<uint32_t, uint32_t>> expectedSeeks;

  // Tuple arguments: skipRows, expectedSkipResult, expectedRowNumber
  std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> expectedSkips;
};

TEST_F(VeloxReaderTests, RangeReads) {
  // Generate an Nimble file with 4 stripes
  auto vectors = createSkipSeekVectors(*leafPool_, {10, 15, 25, 9});
  nimble::VeloxWriterOptions writerOptions;
  writerOptions.dictionaryArrayColumns.insert("dictionaryArray");

  TestNimbleReaderFactory readerFactory(
      *leafPool_, *rootPool_, vectors, writerOptions);

  auto test = [&readerFactory, &vectors](RangeTestParams params) {
    auto reader = readerFactory.createReader(nimble::VeloxReadParams{
        .fileRangeStartOffset = params.rangeStart,
        .fileRangeEndOffset = params.rangeEnd});
    EXPECT_EQ(reader.getRowNumber(), params.firstRow);

    for (const auto& expectedRead : params.expectedReads) {
      readAndVerifyContent(
          reader,
          vectors,
          /* rowsToRead */ std::get<0>(expectedRead),
          /* expectedNumberOfRows */ std::get<1>(expectedRead),
          /* expectedStripe */ std::get<2>(expectedRead),
          /* expectedRowInStripe */ std::get<3>(expectedRead));
      EXPECT_EQ(reader.getRowNumber(), std::get<4>(expectedRead));
    }

    {
      velox::VectorPtr result;
      EXPECT_FALSE(reader.next(1, result));
    }

    for (const auto& expectedSeek : params.expectedSeeks) {
      EXPECT_EQ(
          std::get<1>(expectedSeek),
          reader.seekToRow(std::get<0>(expectedSeek)));
      EXPECT_EQ(reader.getRowNumber(), std::get<1>(expectedSeek));
    }

    reader.seekToRow(0);
    EXPECT_EQ(reader.getRowNumber(), params.firstRow);
    for (const auto& expectedSkip : params.expectedSkips) {
      EXPECT_EQ(
          std::get<1>(expectedSkip),
          reader.skipRows(std::get<0>(expectedSkip)));
      EXPECT_EQ(reader.getRowNumber(), std::get<2>(expectedSkip));
    }

    reader.seekToRow(0);
    EXPECT_EQ(reader.getRowNumber(), params.firstRow);
    for (const auto& expectedRead : params.expectedReads) {
      readAndVerifyContent(
          reader,
          vectors,
          /* rowsToRead */ std::get<0>(expectedRead),
          /* expectedNumberOfRows */ std::get<1>(expectedRead),
          /* expectedStripe */ std::get<2>(expectedRead),
          /* expectedRowInStripe */ std::get<3>(expectedRead));
      EXPECT_EQ(reader.getRowNumber(), std::get<4>(expectedRead));
    }

    {
      velox::VectorPtr result;
      EXPECT_FALSE(reader.next(1, result));
    }
  };

  // Try to read all data in the file. Since we cover the entire file (end is
  // bigger than the file size), we expect to be able to read all lines.
  LOG(INFO) << "--> Range covers the entire file";
  LOG(INFO) << "File:     |--s0--|--s1--|--s2--|--s3--|";
  LOG(INFO) << "Range:    |---------------------------------|";
  LOG(INFO) << "Expected: |--s0--|--s1--|--s2--|--s3--|";
  test({
      .rangeStart = 0,
      .rangeEnd = 100'000'000,
      .firstRow = 0,

      // Reads stop at stripe boundaries, so we need to invoke several reads
      // to read the entire file.
      .expectedReads =
          {{30, 10, 0, 0, 10},
           {30, 15, 1, 0, 25},
           {30, 25, 2, 0, 50},
           {30, 9, 3, 0, 59}},

      // Seeks should be allowed to anywhere in this file (rows 0 to 59)
      .expectedSeeks =
          {{0, 0},
           {5, 5},
           {10, 10},
           {15, 15},
           {25, 25},
           {30, 30},
           {45, 45},
           {50, 50},
           {55, 55},
           {59, 59},
           {60, 59}},

      // Skips should cover the entire file (59 rows)
      .expectedSkips =
          {{0, 0, 0}, {10, 10, 10}, {20, 20, 30}, {30, 29, 59}, {1, 0, 59}},
  });

  // Test a range covering only the first stripe.
  // Using range starting at 0 guarantees we cover the first stripe.
  // Since first stripe is much greater than 1 byte, using range ending at 1,
  // guarantees we don't cover any other stripe other than the fisrt stripe.
  LOG(INFO) << "--> Range covers beginning of first stripe";
  LOG(INFO) << "File:     |--s0--|--s1--|--s2--|--s3--|";
  LOG(INFO) << "Range:    |---|";
  LOG(INFO) << "Expected: |--s0--|";
  test({
      .rangeStart = 0,
      .rangeEnd = 1,
      .firstRow = 0,

      // Reads should only find rows in stripe 0.
      .expectedReads = {{5, 5, 0, 0, 5}, {10, 5, 0, 5, 10}},

      // Seeks should be allowed to access rows in  first stripe only (rows 0
      // to 10)
      .expectedSeeks =
          {{0, 0}, {5, 5}, {10, 10}, {15, 10}, {30, 10}, {59, 10}, {60, 10}},

      // Skips should cover first stripe only (59 rows)
      .expectedSkips = {{0, 0, 0}, {5, 5, 5}, {10, 5, 10}, {1, 0, 10}},
  });

  auto tablet = readerFactory.createTablet();

  // Test a range starting somewhere in the first stripe (but not at zero
  // offset) to exactly the end of the first stripe. This should be resolved
  // to zero stripes.
  LOG(INFO) << "--> Range covers end of stripe 0";
  LOG(INFO) << "File:     |--s0--|--s1--|--s2--|--s3--|";
  LOG(INFO) << "Range:       |---|";
  LOG(INFO) << "Expected: <empty>>";
  test({
      .rangeStart = 1,
      .rangeEnd = tablet.stripeOffset(1),
      .firstRow = 0,

      // No read should succeed, as we have zero stripes to read from
      .expectedReads = {},

      // All seeks should be ignored
      .expectedSeeks =
          {{0, 0}, {5, 0}, {10, 0}, {15, 0}, {30, 0}, {59, 0}, {60, 0}},

      // All skips should be ignored
      .expectedSkips = {{0, 0, 0}, {5, 0, 0}, {59, 0, 0}},
  });

  // Test a range starting somewhere in stripe 0 (but not at zero) to somwhere
  // in stripe 1. This should resolve to only stripe 1.
  LOG(INFO) << "--> Range covers beginning of stripe 1";
  LOG(INFO) << "File:     |--s0--|--s1--|--s2--|--s3--|";
  LOG(INFO) << "Range:       |------|";
  LOG(INFO) << "Expected:        |--s1--|";

  test({
      .rangeStart = 1,
      .rangeEnd = tablet.stripeOffset(1) + 1,
      .firstRow = 10,

      // Reads should all resolve to stripe 1
      .expectedReads = {{5, 5, 1, 0, 15}, {20, 10, 1, 5, 25}},

      // Seeks should succeed if they are in range [10, 25). Otherwise, they
      // should return the edges of stripe 1.
      .expectedSeeks =
          {{0, 10},
           {5, 10},
           {10, 10},
           {15, 15},
           {25, 25},
           {26, 25},
           {59, 25},
           {60, 25}},

      // Skips should allow skipping only 15 rows (number of rows in stripe 1)
      .expectedSkips = {{0, 0, 10}, {5, 5, 15}, {11, 10, 25}, {1, 0, 25}},
  });

  // Test a range starting exactly on stripe 1 to somwhere
  // in stripe 2. This should resolve to only stripe 1.
  LOG(INFO) << "--> Range starts at beginning of stripe 2";
  LOG(INFO) << "File:     |--s0--|--s1--|--s2--|--s3--|";
  LOG(INFO) << "Range:           |---|";
  LOG(INFO) << "Expected:        |--s1--|";
  test({
      .rangeStart = tablet.stripeOffset(1),
      .rangeEnd = tablet.stripeOffset(1) + 1,
      .firstRow = 10,

      // Reads should all resolve to stripe 1
      .expectedReads = {{5, 5, 1, 0, 15}, {20, 10, 1, 5, 25}},

      // Seeks should succeed if they are in range [10, 25). Otherwise, they
      // should return the edges of stripe 1.
      .expectedSeeks =
          {{0, 10},
           {5, 10},
           {10, 10},
           {15, 15},
           {25, 25},
           {26, 25},
           {59, 25},
           {60, 25}},

      // Skips should allow skipping only 15 rows (number of rows in stripe 1)
      .expectedSkips = {{0, 0, 10}, {5, 5, 15}, {11, 10, 25}, {1, 0, 25}},
  });

  // Test a range spanning multiple stripes. We'll start somewhere in stripe 0
  // and end somewhere in stripe 2. This should resolve to stripes 1 and 2.
  LOG(INFO) << "--> Range spans stripes (0, 1 ,2)";
  LOG(INFO) << "File:     |--s0--|--s1--|--s2--|--s3--|";
  LOG(INFO) << "Range:        |------------|";
  LOG(INFO) << "Expected:        |--s1--|--s2--|";
  test({
      .rangeStart = tablet.stripeOffset(1) - 1,
      .rangeEnd = tablet.stripeOffset(2) + 1,
      .firstRow = 10,

      // Reads should all resolve to stripes 1 and 2 (rows [15 to 50)).
      // Reads stop at stripe boundaries, so we need to invoke several reads
      // to continue to the next stripe.
      .expectedReads =
          {{5, 5, 1, 0, 15},
           {20, 10, 1, 5, 25},
           {20, 20, 2, 0, 45},
           {20, 5, 2, 20, 50}},

      // Seeks should succeed if they are in range [10, 50). Otherwise, they
      // should return the edges of stripe 1 and 2.
      .expectedSeeks =
          {{0, 10},
           {5, 10},
           {10, 10},
           {15, 15},
           {25, 25},
           {26, 26},
           {49, 49},
           {50, 50},
           {59, 50},
           {60, 50}},

      // Skips should allow skipping only 40 rows (number of rows in stripes 1
      // and 2)
      .expectedSkips =
          {{0, 0, 10},
           {5, 5, 15},
           {11, 11, 26},
           {23, 23, 49},
           {2, 1, 50},
           {1, 0, 50}},
  });

  // Test a range spanning multiple stripes. We'll start at the beginning of
  // stripe 1 and end somewhere in stripe 3. This should resolve to stripes 1,
  // 2 and 3.
  LOG(INFO) << "--> Range spans stripes (1 ,2, 3)";
  LOG(INFO) << "File:     |--s0--|--s1--|--s2--|--s3--|";
  LOG(INFO) << "Range:           |----------------|";
  LOG(INFO) << "Expected:        |--s1--|--s2--|--s3--|";
  test({
      .rangeStart = tablet.stripeOffset(1),
      .rangeEnd = tablet.stripeOffset(3) + 1,
      .firstRow = 10,

      // Reads should all resolve to stripes 1, 2 and 3 (rows [15 to 59)).
      // Reads stop at stripe boundaries, so we need to invoke several reads
      // to continue to the next stripe.
      .expectedReads =
          {{5, 5, 1, 0, 15},
           {20, 10, 1, 5, 25},
           {20, 20, 2, 0, 45},
           {20, 5, 2, 20, 50},
           {20, 9, 3, 0, 59}},

      // Seeks should succeed if they are in range [10, 59). Otherwise, they
      // should return the edges of stripe 1 and 3.
      .expectedSeeks =
          {{0, 10},
           {5, 10},
           {10, 10},
           {15, 15},
           {25, 25},
           {26, 26},
           {49, 49},
           {50, 50},
           {59, 59},
           {60, 59}},

      // Skips should allow skipping only 49 rows (number of rows in stripes 1
      // to 3)
      .expectedSkips =
          {{0, 0, 10},
           {5, 5, 15},
           {11, 11, 26},
           {32, 32, 58},
           {2, 1, 59},
           {1, 0, 59}},
  });

  // Test last stripe.
  LOG(INFO) << "--> Range covers stripe 3";
  LOG(INFO) << "File:     |--s0--|--s1--|--s2--|--s3--|";
  LOG(INFO) << "Range:                         |----------|";
  LOG(INFO) << "Expected:                      |--s3--|";
  test({
      .rangeStart = tablet.stripeOffset(3),
      .rangeEnd = 100'000'000,
      .firstRow = 50,

      // Reads should all resolve to stripe 3 (rows 50 to 59).
      .expectedReads = {{5, 5, 3, 0, 55}, {5, 4, 3, 5, 59}},

      // Seeks should succeed if they are in range [50, 59). Otherwise, they
      // should return the edges of stripe 3.
      .expectedSeeks =
          {{0, 50},
           {10, 50},
           {15, 50},
           {26, 50},
           {49, 50},
           {50, 50},
           {59, 59},
           {60, 59}},

      // Skips should allow skipping only 9 rows (number of rows in stripe 3)
      .expectedSkips = {{0, 0, 50}, {5, 5, 55}, {5, 4, 59}, {1, 0, 59}},
  });
}

TEST_F(VeloxReaderTests, TestScalarFieldLifeCycle) {
  auto testScalarFieldLifeCycle =
      [&](const std::shared_ptr<const velox::RowType> schema,
          int32_t batchSize,
          std::mt19937& rng) {
        velox::VectorPtr result;
        auto reader = getReaderForLifeCycleTest(schema, 4 * batchSize, rng);
        EXPECT_TRUE(reader->next(batchSize, result));
        // Hold the reference to values Buffers
        auto child = result->as<velox::RowVector>()->childAt(0);
        velox::BaseVector* rowPtr = result.get();
        velox::Buffer* rawNulls = child->nulls().get();
        velox::BufferPtr values = child->values();
        // Reset the child so that it can be reused
        child.reset();
        EXPECT_TRUE(reader->next(batchSize, result));
        child = result->as<velox::RowVector>()->childAt(0);
        if (rawNulls && child->nulls().get()) {
          EXPECT_EQ(rawNulls, child->nulls().get());
        }
        EXPECT_NE(values.get(), child->values().get());
        EXPECT_EQ(rowPtr, result.get());

        // Hold the reference to NULL buffer
        velox::BufferPtr nulls = child->nulls();
        velox::Buffer* rawValues = child->values().get();
        child.reset();
        EXPECT_TRUE(reader->next(batchSize, result));
        child = result->as<velox::RowVector>()->childAt(0);
        if (nulls.get() && child->nulls().get()) {
          EXPECT_NE(nulls.get(), child->nulls().get());
        }
        EXPECT_EQ(rawValues, child->values().get());
        EXPECT_EQ(rowPtr, result.get());

        rawNulls = nulls.get();
        // Hold reference to child ScalarVector and it should use another
        // ScalarVector along with childBuffers
        EXPECT_TRUE(reader->next(batchSize, result));
        auto child1 = result->as<velox::RowVector>()->childAt(0);
        EXPECT_NE(child, child1);
        EXPECT_EQ(rowPtr, result.get());
        // after VectorPtr is reset its buffer also reset
        if (rawNulls && child1->nulls().get()) {
          EXPECT_NE(rawNulls, child1->nulls().get());
        }
        EXPECT_NE(rawValues, child1->values().get());
      };

  uint32_t seed = FLAGS_reader_tests_seed > 0 ? FLAGS_reader_tests_seed
                                              : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};
  std::vector<std::shared_ptr<const velox::RowType>> types = {
      velox::ROW({{"tinyInt", velox::TINYINT()}}),
      velox::ROW({{"smallInt", velox::SMALLINT()}}),
      velox::ROW({{"int", velox::INTEGER()}}),
      velox::ROW({{"bigInt", velox::BIGINT()}}),
      velox::ROW({{"Real", velox::REAL()}}),
      velox::ROW({{"Double", velox::DOUBLE()}}),
      velox::ROW({{"VARCHAR", velox::VARCHAR()}}),
  };
  for (auto& type : types) {
    LOG(INFO) << "Field Type: " << type->nameOf(0);
    for (int i = 0; i < 10; ++i) {
      testScalarFieldLifeCycle(type, 10, rng);
    }
  }
}

TEST_F(VeloxReaderTests, TestArrayFieldLifeCycle) {
  uint32_t seed = FLAGS_reader_tests_seed > 0 ? FLAGS_reader_tests_seed
                                              : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};
  auto type = velox::ROW({{"arr_val", velox::ARRAY(velox::BIGINT())}});
  auto testArrayFieldLifeCycle =
      [&](const std::shared_ptr<const velox::RowType> type,
          int32_t batchSize,
          std::mt19937& rng) {
        velox::VectorPtr result;
        auto reader = getReaderForLifeCycleTest(type, 4 * batchSize, rng);
        EXPECT_TRUE(reader->next(batchSize, result));
        // Hold the reference to internal Buffers and element doesn't change
        auto child = std::dynamic_pointer_cast<velox::ArrayVector>(
            result->as<velox::RowVector>()->childAt(0));
        velox::BaseVector* childPtr = child.get();
        velox::BaseVector* rowPtr = result.get();
        velox::Buffer* rawNulls = child->nulls().get();
        velox::Buffer* rawSizes = child->sizes().get();
        velox::BufferPtr offsets = child->offsets();
        auto elementsPtr = child->elements().get();
        child.reset();
        EXPECT_TRUE(reader->next(batchSize, result));
        child = std::dynamic_pointer_cast<velox::ArrayVector>(
            result->as<velox::RowVector>()->childAt(0));

        if (rawNulls && child->nulls().get()) {
          EXPECT_EQ(rawNulls, child->nulls().get());
        }
        EXPECT_EQ(rawSizes, child->sizes().get());
        EXPECT_NE(offsets, child->offsets());
        EXPECT_EQ(elementsPtr, child->elements().get());
        EXPECT_EQ(rowPtr, result.get());

        // Hold the reference to Elements vector, other buffer should be
        // reused
        auto elements = child->elements();
        velox::Buffer* rawOffsets = child->offsets().get();
        childPtr = child.get();
        child.reset();
        EXPECT_TRUE(reader->next(batchSize, result));
        child = std::dynamic_pointer_cast<velox::ArrayVector>(
            result->as<velox::RowVector>()->childAt(0));
        if (rawNulls && child->nulls().get()) {
          EXPECT_EQ(rawNulls, child->nulls().get());
        }
        EXPECT_EQ(rawSizes, child->sizes().get());
        EXPECT_EQ(rawOffsets, child->offsets().get());
        EXPECT_NE(elements, child->elements());
        EXPECT_EQ(childPtr, child.get());
        EXPECT_EQ(rowPtr, result.get());

        // Don't release the Child Array vector to row vector, all the buffers
        // in array should not be resused.
        elementsPtr = child->elements().get();
        EXPECT_TRUE(reader->next(batchSize, result));
        auto child1 = std::dynamic_pointer_cast<velox::ArrayVector>(
            result->as<velox::RowVector>()->childAt(0));
        if (rawNulls && child1->nulls().get()) {
          EXPECT_NE(rawNulls, child1->nulls().get());
        }
        EXPECT_NE(rawSizes, child1->sizes().get());
        EXPECT_NE(rawOffsets, child1->offsets().get());
        EXPECT_NE(elementsPtr, child1->elements().get());
        EXPECT_NE(childPtr, child1.get());
        EXPECT_EQ(rowPtr, result.get());
      };
  for (int i = 0; i < 10; ++i) {
    testArrayFieldLifeCycle(type, 10, rng);
  }
}

TEST_F(VeloxReaderTests, TestMapFieldLifeCycle) {
  auto testMapFieldLifeCycle =
      [&](const std::shared_ptr<const velox::RowType> type,
          int32_t batchSize,
          std::mt19937& rng) {
        velox::VectorPtr result;
        auto reader = getReaderForLifeCycleTest(type, 5 * batchSize, rng);
        EXPECT_TRUE(reader->next(batchSize, result));
        // Hold the reference to internal Buffers and element doesn't change
        auto child = std::dynamic_pointer_cast<velox::MapVector>(
            result->as<velox::RowVector>()->childAt(0));
        velox::BaseVector* childPtr = child.get();
        velox::BaseVector* rowPtr = result.get();
        velox::Buffer* rawNulls = child->nulls().get();
        velox::BufferPtr sizes = child->sizes();
        velox::Buffer* rawOffsets = child->offsets().get();
        auto keysPtr = child->mapKeys().get();
        auto valuesPtr = child->mapValues().get();
        child.reset();
        EXPECT_TRUE(reader->next(batchSize, result));
        child = std::dynamic_pointer_cast<velox::MapVector>(
            result->as<velox::RowVector>()->childAt(0));

        if (rawNulls && child->nulls().get()) {
          EXPECT_EQ(rawNulls, child->nulls().get());
        }
        EXPECT_NE(sizes, child->sizes());
        EXPECT_EQ(rawOffsets, child->offsets().get());
        EXPECT_EQ(keysPtr, child->mapKeys().get());
        EXPECT_EQ(valuesPtr, child->mapValues().get());
        EXPECT_EQ(rowPtr, result.get());

        // Hold the reference to keys vector, other buffer should be reused
        auto mapKeys = child->mapKeys();
        velox::Buffer* rawSizes = child->sizes().get();
        childPtr = child.get();
        child.reset();
        EXPECT_TRUE(reader->next(batchSize, result));
        child = std::dynamic_pointer_cast<velox::MapVector>(
            result->as<velox::RowVector>()->childAt(0));
        if (rawNulls && child->nulls().get()) {
          EXPECT_EQ(rawNulls, child->nulls().get());
        }
        EXPECT_EQ(rawSizes, child->sizes().get());
        EXPECT_EQ(rawOffsets, child->offsets().get());
        EXPECT_NE(mapKeys, child->mapKeys());
        EXPECT_EQ(valuesPtr, child->mapValues().get());
        EXPECT_EQ(childPtr, child.get());
        EXPECT_EQ(rowPtr, result.get());

        // Hold the reference to values vector, other buffer should be reused
        keysPtr = child->mapKeys().get();
        auto mapValues = child->mapValues();
        childPtr = child.get();
        child.reset();
        EXPECT_TRUE(reader->next(batchSize, result));
        child = std::dynamic_pointer_cast<velox::MapVector>(
            result->as<velox::RowVector>()->childAt(0));
        if (rawNulls && child->nulls().get()) {
          EXPECT_EQ(rawNulls, child->nulls().get());
        }
        EXPECT_EQ(rawSizes, child->sizes().get());
        EXPECT_EQ(rawOffsets, child->offsets().get());
        EXPECT_EQ(keysPtr, child->mapKeys().get());
        EXPECT_NE(mapValues, child->mapValues());
        EXPECT_EQ(childPtr, child.get());
        EXPECT_EQ(rowPtr, result.get());

        // Don't release the Child map vector to row vector, all the buffers
        // in array should not be resused.
        valuesPtr = child->mapValues().get();
        EXPECT_TRUE(reader->next(batchSize, result));
        auto child1 = std::dynamic_pointer_cast<velox::MapVector>(
            result->as<velox::RowVector>()->childAt(0));
        if (rawNulls && child1->nulls().get()) {
          EXPECT_NE(rawNulls, child1->nulls().get());
        }
        EXPECT_NE(rawSizes, child1->sizes().get());
        EXPECT_NE(rawOffsets, child1->offsets().get());
        EXPECT_NE(keysPtr, child1->mapKeys().get());
        EXPECT_NE(valuesPtr, child1->mapValues().get());
        EXPECT_NE(childPtr, child1.get());
        EXPECT_EQ(rowPtr, result.get());
      };
  uint32_t seed = FLAGS_reader_tests_seed > 0 ? FLAGS_reader_tests_seed
                                              : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};
  auto type =
      velox::ROW({{"map_val", velox::MAP(velox::INTEGER(), velox::REAL())}});
  for (int i = 0; i < 10; ++i) {
    if (i == 4) {
      LOG(INFO) << i;
    }
    testMapFieldLifeCycle(type, 10, rng);
    testMapFieldLifeCycle(type, 10, rng);
  }
}

TEST_F(VeloxReaderTests, TestFlatMapAsMapFieldLifeCycle) {
  auto testFlatMapFieldLifeCycle =
      [&](const std::shared_ptr<const velox::RowType> type,
          int32_t batchSize,
          std::mt19937& rng) {
        velox::VectorPtr result;
        nimble::VeloxWriterOptions writeOptions;
        writeOptions.flatMapColumns.insert("flat_map");
        auto reader =
            getReaderForLifeCycleTest(type, 5 * batchSize, rng, writeOptions);
        EXPECT_TRUE(reader->next(batchSize, result));
        // Hold the reference to internal Buffers and element doesn't change
        auto child = std::dynamic_pointer_cast<velox::MapVector>(
            result->as<velox::RowVector>()->childAt(0));
        velox::BaseVector* childPtr = child.get();
        velox::BaseVector* rowPtr = result.get();
        velox::Buffer* rawNulls = child->nulls().get();
        velox::BufferPtr sizes = child->sizes();
        velox::Buffer* rawOffsets = child->offsets().get();
        auto keysPtr = child->mapKeys().get();
        child.reset();
        EXPECT_TRUE(reader->next(batchSize, result));
        child = std::dynamic_pointer_cast<velox::MapVector>(
            result->as<velox::RowVector>()->childAt(0));

        if (rawNulls && child->nulls().get()) {
          EXPECT_EQ(rawNulls, child->nulls().get());
        }
        EXPECT_NE(sizes, child->sizes());
        EXPECT_EQ(rawOffsets, child->offsets().get());
        EXPECT_EQ(keysPtr, child->mapKeys().get());
        EXPECT_EQ(rowPtr, result.get());

        // Hold the reference to keys vector, other buffer should be reused
        auto mapKeys = child->mapKeys();
        velox::Buffer* rawSizes = child->sizes().get();
        childPtr = child.get();
        child.reset();
        EXPECT_TRUE(reader->next(batchSize, result));
        child = std::dynamic_pointer_cast<velox::MapVector>(
            result->as<velox::RowVector>()->childAt(0));
        if (rawNulls && child->nulls().get()) {
          EXPECT_EQ(rawNulls, child->nulls().get());
        }
        EXPECT_EQ(rawSizes, child->sizes().get());
        EXPECT_EQ(rawOffsets, child->offsets().get());
        EXPECT_NE(mapKeys, child->mapKeys());
        EXPECT_EQ(childPtr, child.get());
        EXPECT_EQ(rowPtr, result.get());

        // Don't release the Child map vector to row vector, all the buffers
        // in array should not be resused.
        EXPECT_TRUE(reader->next(batchSize, result));
        auto child1 = std::dynamic_pointer_cast<velox::MapVector>(
            result->as<velox::RowVector>()->childAt(0));
        if (rawNulls && child1->nulls().get()) {
          EXPECT_NE(rawNulls, child1->nulls().get());
        }
        EXPECT_NE(rawSizes, child1->sizes().get());
        EXPECT_NE(rawOffsets, child1->offsets().get());
        EXPECT_NE(keysPtr, child1->mapKeys().get());
        EXPECT_NE(childPtr, child1.get());
        EXPECT_EQ(rowPtr, result.get());
      };
  uint32_t seed = FLAGS_reader_tests_seed > 0 ? FLAGS_reader_tests_seed
                                              : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};
  auto type =
      velox::ROW({{"flat_map", velox::MAP(velox::INTEGER(), velox::REAL())}});
  for (int i = 0; i < 10; ++i) {
    testFlatMapFieldLifeCycle(type, 10, rng);
    testFlatMapFieldLifeCycle(type, 10, rng);
  }
}

TEST_F(VeloxReaderTests, TestRowFieldLifeCycle) {
  auto testRowFieldLifeCycle =
      [&](const std::shared_ptr<const velox::RowType> type,
          int32_t batchSize,
          std::mt19937& rng) {
        velox::VectorPtr result;
        auto reader = getReaderForLifeCycleTest(type, 5 * batchSize, rng);
        EXPECT_TRUE(reader->next(batchSize, result));
        // Hold the reference to internal Buffers and element doesn't change
        auto child = std::dynamic_pointer_cast<velox::RowVector>(
            result->as<velox::RowVector>()->childAt(0));
        velox::BaseVector* childPtr = child.get();
        velox::BaseVector* rowPtr = result.get();
        velox::BufferPtr nulls = child->nulls();
        velox::BaseVector* childPtrAtIdx0 = child->childAt(0).get();
        velox::BaseVector* childPtrAtIdx1 = child->childAt(1).get();
        child.reset();
        EXPECT_TRUE(reader->next(batchSize, result));
        child = std::dynamic_pointer_cast<velox::RowVector>(
            result->as<velox::RowVector>()->childAt(0));
        if (nulls && child->nulls()) {
          EXPECT_NE(nulls, child->nulls());
        }
        EXPECT_EQ(childPtrAtIdx0, child->childAt(0).get());
        EXPECT_EQ(childPtrAtIdx1, child->childAt(1).get());
        EXPECT_EQ(rowPtr, result.get());

        // Hold the reference to one of child vector, sibling should not
        // change
        auto childAtIdx0 = child->childAt(0);
        velox::Buffer* rawNulls = child->nulls().get();
        childPtr = child.get();
        child.reset();
        EXPECT_TRUE(reader->next(batchSize, result));
        child = std::dynamic_pointer_cast<velox::RowVector>(
            result->as<velox::RowVector>()->childAt(0));
        if (rawNulls && child->nulls().get()) {
          EXPECT_EQ(rawNulls, child->nulls().get());
        }
        EXPECT_NE(childAtIdx0, child->childAt(0));
        EXPECT_EQ(childPtrAtIdx1, child->childAt(1).get());
        EXPECT_EQ(childPtr, child.get());
        EXPECT_EQ(rowPtr, result.get());

        // Don't release the Child-row vector to row vector, all the buffers
        // in array should not be resused.
        EXPECT_TRUE(reader->next(batchSize, result));
        auto child1 = std::dynamic_pointer_cast<velox::RowVector>(
            result->as<velox::RowVector>()->childAt(0));
        if (rawNulls && child1->nulls().get()) {
          EXPECT_NE(rawNulls, child1->nulls().get());
        }
        EXPECT_NE(child->childAt(0), child1->childAt(0));
        EXPECT_NE(child->childAt(1), child1->childAt(1));
        EXPECT_NE(childPtr, child1.get());
        EXPECT_EQ(rowPtr, result.get());
      };

  auto type = velox::ROW(
      {{"row_val",
        velox::ROW(
            {{"a", velox::INTEGER()}, {"b", velox::ARRAY(velox::BIGINT())}})}});
  uint32_t seed = FLAGS_reader_tests_seed > 0 ? FLAGS_reader_tests_seed
                                              : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};
  for (int i = 0; i < 20; ++i) {
    testRowFieldLifeCycle(type, 10, rng);
  }
}

TEST_F(VeloxReaderTests, VeloxTypeFromNimbleSchema) {
  auto type = velox::ROW({
      {"tinyint_val", velox::TINYINT()},
      {"smallint_val", velox::SMALLINT()},
      {"int_val", velox::INTEGER()},
      {"long_val", velox::BIGINT()},
      {"float_val", velox::REAL()},
      {"double_val", velox::DOUBLE()},
      {"binary_val", velox::VARBINARY()},
      {"string_val", velox::VARCHAR()},
      {"array_val", velox::ARRAY(velox::BIGINT())},
      {"map_val", velox::MAP(velox::INTEGER(), velox::BIGINT())},
      {"struct_val",
       velox::ROW({
           {"float_val", velox::REAL()},
           {"double_val", velox::DOUBLE()},
       })},
      {"nested_map_row_val",
       velox::MAP(
           velox::INTEGER(),
           velox::ROW({
               {"float_val", velox::REAL()},
               {"array_val",
                velox::ARRAY(velox::MAP(velox::INTEGER(), velox::BIGINT()))},
           }))},
      {"dictionary_array_val", velox::ARRAY(velox::BIGINT())},
  });

  velox::VectorFuzzer fuzzer({.vectorSize = 100}, leafPool_.get());
  auto vector = fuzzer.fuzzInputFlatRow(type);

  nimble::VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns.insert("nested_map_row_val");
  writerOptions.dictionaryArrayColumns.insert("dictionary_array_val");
  testVeloxTypeFromNimbleSchema(*leafPool_, writerOptions, vector);
}

TEST_F(VeloxReaderTests, VeloxTypeFromNimbleSchemaEmptyFlatMap) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  uint32_t numRows = 5;
  auto vector = vectorMaker.rowVector(
      {"col_0", "col_1"},
      {
          vectorMaker.flatVector<int32_t>(
              numRows,
              [](velox::vector_size_t row) { return 1000 + row; },
              [](velox::vector_size_t row) { return row == 1; }),
          vectorMaker.mapVector<velox::StringView, int32_t>(
              numRows,
              /*sizeAt*/
              [](velox::vector_size_t /* mapRow */) { return 0; },
              /*keyAt*/
              [](velox::vector_size_t /* mapRow */,
                 velox::vector_size_t /* row */) { return ""; },
              /*valueAt*/
              [](velox::vector_size_t /* mapRow */,
                 velox::vector_size_t /* row */) { return 0; },
              /*isNullAt*/
              [](velox::vector_size_t /* mapRow */) { return true; }),
      });
  nimble::VeloxWriterOptions writerOptions;
  writerOptions.flatMapColumns.insert("col_1");
  testVeloxTypeFromNimbleSchema(*leafPool_, writerOptions, vector);
}

TEST_F(VeloxReaderTests, MissingMetadata) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector =
      vectorMaker.rowVector({vectorMaker.flatVector<int32_t>({1, 2, 3})});

  nimble::VeloxWriterOptions options;
  auto file = nimble::test::createNimbleFile(*rootPool_, vector, options);
  for (auto useChaniedBuffers : {false, true}) {
    nimble::testing::InMemoryTrackableReadFile readFile(
        file, useChaniedBuffers);

    nimble::VeloxReader reader(*leafPool_, &readFile);
    {
      readFile.resetChunks();
      const auto& metadata = reader.metadata();
      // Default metadata injects at least one entry
      ASSERT_LE(1, metadata.size());
      EXPECT_EQ(1, readFile.chunks().size());
    }

    {
      // Metadata is loaded lazily, so reading again just to be sure all is
      // well.
      readFile.resetChunks();
      const auto& metadata = reader.metadata();
      ASSERT_LE(1, metadata.size());
      EXPECT_EQ(0, readFile.chunks().size());
    }
  }
}

TEST_F(VeloxReaderTests, WithMetadata) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector =
      vectorMaker.rowVector({vectorMaker.flatVector<int32_t>({1, 2, 3})});

  nimble::VeloxWriterOptions options{
      .metadata = {{"key 1", "value 1"}, {"key 2", "value 2"}},
  };
  auto file = nimble::test::createNimbleFile(*rootPool_, vector, options);
  for (auto useChaniedBuffers : {false, true}) {
    nimble::testing::InMemoryTrackableReadFile readFile(
        file, useChaniedBuffers);

    nimble::VeloxReader reader(*leafPool_, &readFile);

    {
      readFile.resetChunks();
      auto metadata = reader.metadata();
      ASSERT_EQ(2, metadata.size());
      ASSERT_TRUE(metadata.contains("key 1"));
      ASSERT_TRUE(metadata.contains("key 2"));
      ASSERT_EQ("value 1", metadata["key 1"]);
      ASSERT_EQ("value 2", metadata["key 2"]);

      EXPECT_EQ(1, readFile.chunks().size());
    }

    {
      // Metadata is loaded lazily, so reading again just to be sure all is
      // well.
      readFile.resetChunks();
      auto metadata = reader.metadata();
      ASSERT_EQ(2, metadata.size());
      ASSERT_TRUE(metadata.contains("key 1"));
      ASSERT_TRUE(metadata.contains("key 2"));
      ASSERT_EQ("value 1", metadata["key 1"]);
      ASSERT_EQ("value 2", metadata["key 2"]);

      EXPECT_EQ(0, readFile.chunks().size());
    }
  }
}

TEST_F(VeloxReaderTests, InaccurateSchemaWithSelection) {
  // Some compute engines (e.g. Presto) sometimes don't have the full schema
  // to pass into the reader (if column projection is used). The reader needs
  // the schema in order to correctly construct the output vector. However,
  // for unprojected columns, the reader just need to put a placeholder null
  // column (so ordinals will work as expected), and the actual column type
  // doesn't matter. In this case, we expect the compute engine to construct a
  // column selector, with dummy nodes in the schema for the unprojected
  // columns. This test verifies that the reader handles this correctly.
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto vector = vectorMaker.rowVector(
      {"int1", "int2", "string", "double", "row1", "row2", "int3", "int4"},
      {vectorMaker.flatVector<int32_t>({11, 12, 13, 14, 15}),
       vectorMaker.flatVector<int32_t>({21, 22, 23, 24, 25}),
       vectorMaker.flatVector({"s1", "s2", "s3", "s4", "s5"}),
       vectorMaker.flatVector<double>({1.1, 2.2, 3.3, 4.4, 5.5}),
       vectorMaker.rowVector(
           /* childNames */ {"a1", "b1"},
           /* children */
           {vectorMaker.flatVector<int32_t>({111, 112, 113, 114, 115}),
            vectorMaker.flatVector({"s111", "s112", "s113", "s114", "s115"})}),
       vectorMaker.rowVector(
           /* childNames */ {"a2", "b2"},
           /* children */
           {vectorMaker.flatVector<int32_t>({211, 212, 213, 214, 215}),
            vectorMaker.flatVector({"s211", "s212", "s213", "s214", "s215"})}),
       vectorMaker.flatVector<int32_t>({31, 32, 33, 34, 35}),
       vectorMaker.flatVector<int32_t>({41, 42, 43, 44, 45})});

  velox::VectorPtr result;
  {
    auto file = nimble::test::createNimbleFile(*rootPool_, vector);
    velox::InMemoryReadFile readFile(file);
    auto inaccurateType = velox::ROW({
        {"c1", velox::VARCHAR()},
        {"c2", velox::INTEGER()},
        {"c3", velox::VARCHAR()},
        {"c4", velox::VARCHAR()},
        {"c5", velox::VARCHAR()},
        {"c6", velox::ROW({velox::INTEGER(), velox::VARCHAR()})},
        {"c7", velox::INTEGER()},
        // We didn't add the last column on purpose, to test that the reader
        // can handle smaller schemas.
    });

    std::unordered_set<uint64_t> projected{1, 2, 5, 6};
    auto selector = std::make_shared<velox::dwio::common::ColumnSelector>(
        inaccurateType,
        std::vector<uint64_t>{projected.begin(), projected.end()});
    nimble::VeloxReader reader(*leafPool_, &readFile, std::move(selector));

    ASSERT_TRUE(reader.next(vector->size(), result));
    const auto& rowResult = result->as<velox::RowVector>();
    ASSERT_EQ(inaccurateType->size(), rowResult->childrenSize());
    for (auto i = 0; i < rowResult->childrenSize(); ++i) {
      const auto& child = rowResult->childAt(i);
      if (projected.count(i) == 0) {
        ASSERT_EQ(rowResult->childAt(i), nullptr);
      } else {
        ASSERT_EQ(5, child->size());
        for (auto j = 0; j < child->size(); ++j) {
          ASSERT_FALSE(child->isNullAt(j));
          ASSERT_TRUE(child->equalValueAt(vector->childAt(i).get(), j, j));
        }
      }
    }
    ASSERT_FALSE(reader.next(vector->size(), result));
  }
}

TEST_F(VeloxReaderTests, ChunkStreamsWithNulls) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};
  std::vector<facebook::velox::VectorPtr> vectors{
      vectorMaker.rowVector({
          vectorMaker.flatVectorNullable<int32_t>(
              {std::nullopt, std::nullopt, std::nullopt}),
          vectorMaker.flatVectorNullable<int32_t>(
              {2, std::nullopt, std::nullopt}),
          vectorMaker.flatVectorNullable<int32_t>(
              {std::nullopt, std::nullopt, std::nullopt}),
      }),
      vectorMaker.rowVector({
          vectorMaker.flatVectorNullable<int32_t>(
              {std::nullopt, std::nullopt, std::nullopt}),
          vectorMaker.flatVectorNullable<int32_t>(
              {std::nullopt, std::nullopt, std::nullopt}),
          vectorMaker.flatVectorNullable<int32_t>(
              {std::nullopt, std::nullopt, std::nullopt}),
      }),
      vectorMaker.rowVector({
          vectorMaker.flatVectorNullable<int32_t>(
              {std::nullopt, std::nullopt, std::nullopt}),
          vectorMaker.flatVectorNullable<int32_t>(
              {2, std::nullopt, std::nullopt}),
          vectorMaker.flatVectorNullable<int32_t>(
              {std::nullopt, 3, std::nullopt}),
      })};

  for (auto enableChunking : {false, true}) {
    nimble::VeloxWriterOptions options{
        .flushPolicyFactory =
            [&]() {
              return std::make_unique<nimble::LambdaFlushPolicy>(
                  [&](auto&) { return nimble::FlushDecision::Chunk; });
            },
        .enableChunking = enableChunking};
    auto file = nimble::test::createNimbleFile(
        *rootPool_, vectors, options, /* flushAfterWrite */ false);
    velox::InMemoryReadFile readFile(file);
    nimble::VeloxReader reader(*leafPool_, &readFile, /* selector */ nullptr);

    velox::VectorPtr result;
    for (const auto& expected : vectors) {
      ASSERT_TRUE(reader.next(expected->size(), result));
      ASSERT_EQ(expected->size(), result->size());
      for (auto i = 0; i < expected->size(); ++i) {
        LOG(INFO) << expected->toString(i);
        ASSERT_TRUE(expected->equalValueAt(result.get(), i, i))
            << "Content mismatch at index " << i
            << "\nReference: " << expected->toString(i)
            << "\nResult: " << result->toString(i);
      }
    }
  }
}

TEST_F(VeloxReaderTests, EstimatedRowSizeSimple) {
  const uint32_t rowCount = 100;
  const double kMaxErrorRate = 0.2;
  auto testPrimitive = [&, rootPool = rootPool_, leafPool = leafPool_](
                           velox::TypePtr type, uint64_t typeSize) {
    for (bool hasNull : {true, false}) {
      velox::VectorFuzzer::Options fuzzerOpts_;
      fuzzerOpts_.vectorSize = rowCount;
      if (hasNull) {
        fuzzerOpts_.nullRatio = 0.2;
      }
      velox::VectorFuzzer fuzzer(fuzzerOpts_, leafPool.get());
      auto vector = fuzzer.fuzzInputFlatRow(velox::ROW({{"simple_col", type}}));
      auto file = nimble::test::createNimbleFile(*rootPool, vector);
      velox::InMemoryReadFile readFile(file);
      nimble::VeloxReader reader(*leafPool, &readFile, nullptr);
      velox::VectorPtr result;
      ASSERT_EQ(
          nimble::VeloxReader::kConservativeEstimatedRowSize,
          reader.estimatedRowSize());
      reader.next(1, result);
      if (type->isFixedWidth()) {
        ASSERT_EQ(typeSize, reader.estimatedRowSize());
      } else {
        const auto expectedRowSize =
            (fuzzerOpts_.stringLength + 16) * (1 - fuzzerOpts_.nullRatio);
        int64_t estimateDifference =
            reader.estimatedRowSize() - expectedRowSize;
        ASSERT_LE(
            std::abs(estimateDifference) * 1.0 / expectedRowSize,
            kMaxErrorRate);
      }
    }
  };

  testPrimitive(velox::BOOLEAN(), 0);
  testPrimitive(
      velox::TINYINT(),
      sizeof(velox::TypeTraits<velox::TypeKind::TINYINT>::NativeType));
  testPrimitive(
      velox::SMALLINT(),
      sizeof(velox::TypeTraits<velox::TypeKind::SMALLINT>::NativeType));
  testPrimitive(
      velox::INTEGER(),
      sizeof(velox::TypeTraits<velox::TypeKind::INTEGER>::NativeType));
  testPrimitive(
      velox::BIGINT(),
      sizeof(velox::TypeTraits<velox::TypeKind::BIGINT>::NativeType));
  testPrimitive(
      velox::REAL(),
      sizeof(velox::TypeTraits<velox::TypeKind::REAL>::NativeType));
  testPrimitive(
      velox::DOUBLE(),
      sizeof(velox::TypeTraits<velox::TypeKind::DOUBLE>::NativeType));
  // Nimble writes TIMESTAMP as BIGINT.
  testPrimitive(
      velox::TIMESTAMP(),
      sizeof(velox::TypeTraits<velox::TypeKind::BIGINT>::NativeType));
  // HUGEINT is currently not supported. Remove the throw test when it is.
  ASSERT_THROW(
      testPrimitive(
          velox::HUGEINT(),
          sizeof(velox::TypeTraits<velox::TypeKind::HUGEINT>::NativeType)),
      nimble::NimbleUserError);
  testPrimitive(velox::VARCHAR(), 0);
}

TEST_F(VeloxReaderTests, EstimatedRowSizeComplex) {
  // For string cases and with null cases, it is really hard to get an even
  // close estimation as velox always over provision memory for vectors. Loose
  // or very loose error rate is applied to the test verification as there is no
  // other way of binding the estimate verification with velox implementations.
  const double kMaxErrorRate = 0.2;
  const double kMaxErrorRateLoose = 0.4;
  const double kMaxErrorRateVeryLoose = 0.6;

  auto testMultiValue = [&](uint32_t numRows,
                            uint32_t numElements,
                            bool isArray) {
    velox::VectorFuzzer::Options fuzzerOpts;
    fuzzerOpts.vectorSize = numRows;
    fuzzerOpts.containerLength = numElements;
    fuzzerOpts.complexElementsMaxSize =
        fuzzerOpts.vectorSize * fuzzerOpts.containerLength;
    fuzzerOpts.containerHasNulls = false;
    fuzzerOpts.containerVariableLength = false;
    fuzzerOpts.stringLength = velox::StringView::kInlineSize;
    fuzzerOpts.stringVariableLength = false;
    std::vector<velox::TypePtr> testElementTypes{
        velox::BIGINT(), velox::VARCHAR()};
    for (auto type : testElementTypes) {
      auto dbgGuard = folly::makeGuard([&] {
        LOG(INFO) << "Testing " << (isArray ? "array" : "map") << " with "
                  << type->toString() << " numRows " << numRows
                  << " numElements " << numElements;
      });
      velox::VectorFuzzer fuzzer(fuzzerOpts, leafPool_.get());
      velox::VectorPtr vector;
      if (isArray) {
        vector = fuzzer.fuzzInputFlatRow(
            velox::ROW({{"flat_array_col", velox::ARRAY(type)}}));
      } else {
        vector = fuzzer.fuzzInputFlatRow(
            velox::ROW({{"flat_map_col", velox::MAP(type, type)}}));
      }

      auto file = nimble::test::createNimbleFile(*rootPool_, vector);

      velox::InMemoryReadFile readFile(file);
      nimble::VeloxReader reader(*leafPool_, &readFile, nullptr);
      velox::VectorPtr result;
      ASSERT_EQ(
          nimble::VeloxReader::kConservativeEstimatedRowSize,
          reader.estimatedRowSize());
      // Read 1 less row so that it does not reach stripe boundary.
      reader.next(numRows - 1, result);
      int64_t estimateBasedOnRetainedSize = result->retainedSize() / numRows;
      int64_t estimateDifference =
          reader.estimatedRowSize() - estimateBasedOnRetainedSize;
      auto maxErrorRate = type->kindEquals(velox::VARCHAR())
          ? kMaxErrorRateLoose
          : kMaxErrorRate;
      ASSERT_LE(
          std::abs(estimateDifference) * 1.0 / estimateBasedOnRetainedSize,
          maxErrorRate);
      dbgGuard.dismiss();
    }
  };

  // Test regular map
  testMultiValue(300, 100, false);
  // Test regular array
  testMultiValue(300, 100, true);

  auto testFlatMap = [&](uint32_t rowCount,
                         uint32_t numFeatures,
                         bool readFlatMapFieldAsStruct,
                         bool hasNulls,
                         double maxErrorRateFixed,
                         double maxErrorRateString) {
    std::vector<velox::TypePtr> testElementTypes{
        velox::BIGINT(), velox::VARCHAR()};
    for (auto& type : testElementTypes) {
      std::stringstream dbgStr;
      dbgStr << "Testing "
             << (readFlatMapFieldAsStruct ? "FlatMapAsStruct" : "MergedFlatMap")
             << " with " << type->toString() << " numRows " << rowCount
             << " numFeatures " << numFeatures << " hasNulls "
             << (hasNulls ? "true" : "false");
      auto dbgGuard = folly::makeGuard([&] { LOG(INFO) << dbgStr.str(); });
      VeloxMapGeneratorConfig generatorConfig{
          .featureTypes =
              velox::ROW({{"flat_map_col", velox::MAP(type, type)}}),
          .keyType = type->kind(),
          .maxNumKVPerRow = numFeatures,
          .variantNumKV = false,
          .seed = 1387939242,
          .hasNulls = hasNulls,
          // Use inline size for better control size estimate by utilizing
          // BaseVector::retainedSize()
          .stringLength = velox::StringView::kInlineSize,
          .stringVariableLength = false,
      };
      VeloxMapGenerator generator(leafPool_.get(), generatorConfig);
      auto vector = generator.generateBatch(rowCount);

      nimble::VeloxWriterOptions writerOptions;
      writerOptions.flatMapColumns.insert("flat_map_col");

      // TODO: Remove the customized policy after estimation is supported for
      // all encoding types.
      writerOptions.encodingSelectionPolicyFactory =
          [encodingFactory = nimble::ManualEncodingSelectionPolicyFactory{{
               {nimble::EncodingType::Constant, 1.0},
               {nimble::EncodingType::Trivial, 0.7},
               {nimble::EncodingType::FixedBitWidth, 0.9},
               {nimble::EncodingType::MainlyConstant, 1000.0},
               {nimble::EncodingType::SparseBool, 1.0},
               {nimble::EncodingType::Dictionary, 1000.0},
               {nimble::EncodingType::RLE, 1000.0},
               {nimble::EncodingType::Varint, 1000.0},
           }}](nimble::DataType dataType)
          -> std::unique_ptr<nimble::EncodingSelectionPolicyBase> {
        return encodingFactory.createPolicy(dataType);
      };
      auto file =
          nimble::test::createNimbleFile(*rootPool_, vector, writerOptions);

      nimble::VeloxReadParams readParams;
      if (readFlatMapFieldAsStruct) {
        readParams.readFlatMapFieldAsStruct.insert("flat_map_col");
        for (auto i = 0; i < numFeatures; ++i) {
          if (type->kind() == velox::TypeKind::BIGINT) {
            readParams.flatMapFeatureSelector["flat_map_col"]
                .features.push_back(folly::to<std::string>(i));
          } else {
            ASSERT_EQ(type->kind(), velox::TypeKind::VARCHAR);
            readParams.flatMapFeatureSelector["flat_map_col"]
                .features.push_back(
                    fmt::format("{}{}", generatorConfig.stringKeyPrefix, i));
          }
        }
      }

      velox::InMemoryReadFile readFile(file);
      nimble::VeloxReader reader(*leafPool_, &readFile, nullptr, readParams);
      velox::VectorPtr result;
      ASSERT_EQ(
          nimble::VeloxReader::kConservativeEstimatedRowSize,
          reader.estimatedRowSize());
      // Read 1 less row so that it does not reach stripe boundary.
      reader.next(rowCount - 1, result);
      int64_t estimateBasedOnRetainedSize = result->retainedSize() / rowCount;
      int64_t estimatedRowSize = reader.estimatedRowSize();
      dbgStr << " actual " << estimatedRowSize << " expected "
             << estimateBasedOnRetainedSize;
      int64_t estimateDifference =
          estimatedRowSize - estimateBasedOnRetainedSize;
      const auto maxErrorRate = type->kindEquals(velox::VARCHAR())
          ? maxErrorRateString
          : maxErrorRateFixed;
      ASSERT_LE(
          std::abs(estimateDifference) * 1.0 / estimateBasedOnRetainedSize,
          maxErrorRate);
      dbgGuard.dismiss();
    }
  };

  // Testing StructFlatMapFieldReader
  testFlatMap(
      /* rowCount= */ 1000,
      /* numFeatures= */ 100,
      /* readFlatMapFieldAsStruct= */ true,
      /* hasNulls= */ false,
      /* maxErrorRateFixed= */ kMaxErrorRate,
      /* maxErrorRateString= */ kMaxErrorRateLoose);
  testFlatMap(
      /* rowCount= */ 1000,
      /* numFeatures= */ 100,
      /* readFlatMapFieldAsStruct= */ true,
      /* hasNulls= */ true,
      /* maxErrorRateFixed= */ kMaxErrorRate,
      /* maxErrorRateString= */ kMaxErrorRateVeryLoose);
  testFlatMap(
      /* rowCount= */ 50,
      /* numFeatures= */ 1000,
      /* readFlatMapFieldAsStruct= */ true,
      /* hasNulls= */ false,
      /* maxErrorRateFixed= */ kMaxErrorRate,
      /* maxErrorRateString= */ kMaxErrorRateLoose);
  testFlatMap(
      /* rowCount= */ 50,
      /* numFeatures= */ 1000,
      /* readFlatMapFieldAsStruct= */ true,
      /* hasNulls= */ true,
      /* maxErrorRateFixed= */ kMaxErrorRateLoose,
      /* maxErrorRateString= */ kMaxErrorRateVeryLoose);

  // Testing MergedFlatMapFieldReader
  testFlatMap(
      /* rowCount= */ 500,
      /* numFeatures= */ 100,
      /* readFlatMapFieldAsStruct= */ false,
      /* hasNulls= */ true,
      /* maxErrorRateFixed= */ kMaxErrorRate,
      /* maxErrorRateString= */ kMaxErrorRateLoose);
  testFlatMap(
      /* rowCount= */ 500,
      /* numFeatures= */ 100,
      /* readFlatMapFieldAsStruct= */ false,
      /* hasNulls= */ false,
      /* maxErrorRateFixed= */ kMaxErrorRateLoose,
      /* maxErrorRateString= */ kMaxErrorRateLoose);
  testFlatMap(
      /* rowCount= */ 50,
      /* numFeatures= */ 1000,
      /* readFlatMapFieldAsStruct= */ false,
      /* hasNulls= */ true,
      /* maxErrorRateFixed= */ kMaxErrorRate,
      /* maxErrorRateString= */ kMaxErrorRateLoose);
  testFlatMap(
      /* rowCount= */ 50,
      /* numFeatures= */ 1000,
      /* readFlatMapFieldAsStruct= */ false,
      /* hasNulls= */ false,
      /* maxErrorRateFixed= */ kMaxErrorRate,
      /* maxErrorRateString= */ kMaxErrorRateLoose);
}

TEST_F(VeloxReaderTests, EstimatedRowSizeMix) {
  const double kMaxErrorRate = 0.2;
  const double kMaxErrorRateLoose = 0.4;

  auto testMix = [&](uint32_t numRows,
                     uint32_t numElements,
                     std::function<velox::TypePtr(velox::TypePtr)>&& typeFunc,
                     bool hasNulls,
                     double maxErrorRateFixed,
                     double maxErrorRateString) {
    velox::VectorFuzzer::Options fuzzerOpts;
    fuzzerOpts.vectorSize = numRows;
    fuzzerOpts.nullRatio = hasNulls ? 0.1 : 0;
    fuzzerOpts.containerLength = numElements;
    fuzzerOpts.containerHasNulls = hasNulls;
    fuzzerOpts.complexElementsMaxSize = fuzzerOpts.vectorSize *
        (fuzzerOpts.containerLength * fuzzerOpts.containerLength);
    fuzzerOpts.containerVariableLength = false;

    std::vector<velox::TypePtr> testElementTypes{
        /*velox::BIGINT() ,*/ velox::VARCHAR()};
    for (auto& elementType : testElementTypes) {
      velox::VectorFuzzer fuzzer(fuzzerOpts, leafPool_.get());
      velox::VectorPtr vector;
      vector = fuzzer.fuzzInputFlatRow(
          velox::ROW({{"flat_mix_col", typeFunc(elementType)}}));

      nimble::VeloxWriterOptions writerOptions;
      // TODO: Remove the customized policy after estimation is supported
      // for all encoding types.
      writerOptions.encodingSelectionPolicyFactory =
          [encodingFactory = nimble::ManualEncodingSelectionPolicyFactory{{
               {nimble::EncodingType::Constant, 1.0},
               {nimble::EncodingType::Trivial, 0.7},
               {nimble::EncodingType::FixedBitWidth, 0.9},
               {nimble::EncodingType::MainlyConstant, 1000.0},
               {nimble::EncodingType::SparseBool, 1.0},
               {nimble::EncodingType::Dictionary, 1000.0},
               {nimble::EncodingType::RLE, 1000.0},
               {nimble::EncodingType::Varint, 1000.0},
           }}](nimble::DataType dataType)
          -> std::unique_ptr<nimble::EncodingSelectionPolicyBase> {
        return encodingFactory.createPolicy(dataType);
      };
      auto file =
          nimble::test::createNimbleFile(*rootPool_, vector, writerOptions);

      velox::InMemoryReadFile readFile(file);
      nimble::VeloxReader reader(*leafPool_, &readFile, nullptr);
      velox::VectorPtr result;
      ASSERT_EQ(
          nimble::VeloxReader::kConservativeEstimatedRowSize,
          reader.estimatedRowSize());
      reader.next(numRows - 1, result);
      int64_t estimateBasedOnRetainedSize = result->retainedSize() / numRows;
      int64_t estimateDifference =
          reader.estimatedRowSize() - estimateBasedOnRetainedSize;
      const auto maxErrorRate = elementType->kindEquals(velox::VARCHAR())
          ? maxErrorRateString
          : maxErrorRateFixed;
      ASSERT_LE(
          std::abs(estimateDifference) * 1.0 / estimateBasedOnRetainedSize,
          maxErrorRate);
    }
  };

  // ARRAY(MAP(T, T))
  testMix(
      /* numRows= */
      50,
      /* numElements= */ 100,
      /* typeFunc= */
      [](auto elementType) {
        return velox::ARRAY(velox::MAP(elementType, elementType));
      },
      /* hasNulls */ false,
      /* maxErrorRateFixed= */ kMaxErrorRate,
      /* maxErrorRateString= */ kMaxErrorRateLoose);
  testMix(
      /* numRows= */
      50,
      /* numElements= */ 100,
      /* typeFunc= */
      [](auto elementType) {
        return velox::ARRAY(velox::MAP(elementType, elementType));
      },
      /* hasNulls */ true,
      /* maxErrorRateFixed= */ kMaxErrorRateLoose,
      /* maxErrorRateString= */ kMaxErrorRateLoose);

  // MAP(ARRAY(T), ARRAY(T))
  testMix(
      /* numRows= */
      50,
      /* numElements= */ 100,
      /* typeFunc= */
      [](auto elementType) {
        return velox::MAP(velox::ARRAY(elementType), velox::ARRAY(elementType));
      },
      /* hasNulls */ false,
      /* maxErrorRateFixed= */ kMaxErrorRate,
      /* maxErrorRateString= */ kMaxErrorRateLoose);
  testMix(
      /* numRows= */
      50,
      /* numElements= */ 100,
      /* typeFunc= */
      [](auto elementType) {
        return velox::MAP(velox::ARRAY(elementType), velox::ARRAY(elementType));
      },
      /* hasNulls */ true,
      /* maxErrorRateFixed= */ kMaxErrorRate,
      /* maxErrorRateString= */ kMaxErrorRateLoose);
}

TEST_F(VeloxReaderTests, ReadNonExistingFlatMapFeature) {
  auto testFlatMap = [&](uint32_t rowCount,
                         uint32_t numFeatures,
                         bool readFlatMapFieldAsStruct,
                         bool hasNulls) {
    velox::TypePtr type = velox::BIGINT();
    std::stringstream dbgStr;
    dbgStr << "Testing "
           << (readFlatMapFieldAsStruct ? "FlatMapAsStruct" : "MergedFlatMap")
           << " with " << type->toString() << " numRows " << rowCount
           << " numFeatures " << numFeatures << " hasNulls "
           << (hasNulls ? "true" : "false");
    auto dbgGuard = folly::makeGuard([&] { LOG(INFO) << dbgStr.str(); });
    VeloxMapGeneratorConfig generatorConfig{
        .featureTypes = velox::ROW({{"flat_map_col", velox::MAP(type, type)}}),
        .keyType = type->kind(),
        .maxNumKVPerRow = numFeatures,
        .variantNumKV = false,
        .seed = 1387939242,
        .hasNulls = hasNulls,
        // Use inline size for better control size estimate by utilizing
        // BaseVector::retainedSize()
        .stringLength = velox::StringView::kInlineSize,
        .stringVariableLength = false,
    };
    VeloxMapGenerator generator(leafPool_.get(), generatorConfig);
    auto vector = generator.generateBatch(rowCount);

    nimble::VeloxWriterOptions writerOptions;
    writerOptions.flatMapColumns.insert("flat_map_col");

    // TODO: Remove the customized policy after estimation is supported for
    // all encoding types.
    writerOptions.encodingSelectionPolicyFactory =
        [encodingFactory = nimble::ManualEncodingSelectionPolicyFactory{{
             {nimble::EncodingType::Constant, 1.0},
             {nimble::EncodingType::Trivial, 0.7},
             {nimble::EncodingType::FixedBitWidth, 0.9},
             {nimble::EncodingType::MainlyConstant, 1000.0},
             {nimble::EncodingType::SparseBool, 1.0},
             {nimble::EncodingType::Dictionary, 1000.0},
             {nimble::EncodingType::RLE, 1000.0},
             {nimble::EncodingType::Varint, 1000.0},
         }}](nimble::DataType dataType)
        -> std::unique_ptr<nimble::EncodingSelectionPolicyBase> {
      return encodingFactory.createPolicy(dataType);
    };
    auto file =
        nimble::test::createNimbleFile(*rootPool_, vector, writerOptions);

    nimble::VeloxReadParams readParams;
    if (readFlatMapFieldAsStruct) {
      readParams.readFlatMapFieldAsStruct.insert("flat_map_col");
    }
    // Insert non-existing feature id.
    readParams.flatMapFeatureSelector["flat_map_col"].features.push_back(
        folly::to<std::string>(-1));

    velox::InMemoryReadFile readFile(file);
    nimble::VeloxReader reader(*leafPool_, &readFile, nullptr, readParams);
    velox::VectorPtr result;
    ASSERT_EQ(
        nimble::VeloxReader::kConservativeEstimatedRowSize,
        reader.estimatedRowSize());
    const uint64_t numReadRows = rowCount / 2;
    reader.next(numReadRows, result);
    if (readFlatMapFieldAsStruct) {
      ASSERT_EQ(
          result->as<velox::RowVector>()
              ->childAt(0)
              ->as<velox::RowVector>()
              ->childAt(0)
              ->getNullCount()
              .value(),
          numReadRows);
    } else {
      ASSERT_EQ(
          result->as<velox::RowVector>()
              ->childAt(0)
              ->as<velox::MapVector>()
              ->mapKeys()
              ->size(),
          0);
      ASSERT_EQ(
          result->as<velox::RowVector>()
              ->childAt(0)
              ->as<velox::MapVector>()
              ->mapValues()
              ->size(),
          0);
    }
    reader.estimatedRowSize();
    reader.next(numReadRows, result);
    if (readFlatMapFieldAsStruct) {
      ASSERT_EQ(
          result->as<velox::RowVector>()
              ->childAt(0)
              ->as<velox::RowVector>()
              ->childAt(0)
              ->getNullCount()
              .value(),
          numReadRows);
    } else {
      ASSERT_EQ(
          result->as<velox::RowVector>()
              ->childAt(0)
              ->as<velox::MapVector>()
              ->mapKeys()
              ->size(),
          0);
      ASSERT_EQ(
          result->as<velox::RowVector>()
              ->childAt(0)
              ->as<velox::MapVector>()
              ->mapValues()
              ->size(),
          0);
    }
    dbgGuard.dismiss();
  };

  testFlatMap(
      /* rowCount= */ 50,
      /* numFeatures= */ 10,
      /* readFlatMapFieldAsStruct= */ false,
      /* hasNulls= */ false);
  testFlatMap(
      /* rowCount= */ 50,
      /* numFeatures= */ 10,
      /* readFlatMapFieldAsStruct= */ true,
      /* hasNulls= */ false);
  testFlatMap(
      /* rowCount= */ 50,
      /* numFeatures= */ 10,
      /* readFlatMapFieldAsStruct= */ false,
      /* hasNulls= */ true);
  testFlatMap(
      /* rowCount= */ 50,
      /* numFeatures= */ 10,
      /* readFlatMapFieldAsStruct= */ true,
      /* hasNulls= */ true);
}
