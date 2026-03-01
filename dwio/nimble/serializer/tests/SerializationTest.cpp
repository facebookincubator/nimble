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
#include <folly/Random.h>
#include <gtest/gtest.h>

#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/serializer/Deserializer.h"
#include "dwio/nimble/serializer/Serializer.h"
#include "dwio/nimble/velox/EncodingLayoutTree.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/NullsBuilder.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook;
using namespace facebook::nimble;

// Test parameters for parameterized tests.
// For encoding modes (kDenseEncoded, kSparseEncoded), we test both with and
// without compression to exercise the compressionOptions path in
// ReplayedEncodingSelectionPolicy.
struct TestParams {
  std::optional<SerializationVersion> version;
  // Compression options for encoding modes. Only used when encodingLayoutTree
  // is specified and version is kDenseEncoded or kSparseEncoded.
  CompressionOptions compressionOptions{};
  // Whether compression is enabled (for test naming).
  bool compressionEnabled{false};
};

class SerializationTest : public ::testing::TestWithParam<TestParams> {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance(
        velox::memory::MemoryManager::Options{});
  }

  void SetUp() override {
    rootPool_ = velox::memory::memoryManager()->addRootPool("default_root");
    pool_ = velox::memory::memoryManager()->addLeafPool("default_leaf");
  }

  std::optional<SerializationVersion> version() const {
    return GetParam().version;
  }

  const CompressionOptions& compressionOptions() const {
    return GetParam().compressionOptions;
  }

  bool compressionEnabled() const {
    return GetParam().compressionEnabled;
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;

  static bool vectorEquals(
      const velox::VectorPtr& expected,
      const velox::VectorPtr& actual,
      velox::vector_size_t index) {
    return expected->equalValueAt(actual.get(), index, index);
  }

  template <typename T = int32_t>
  void writeAndVerify(
      folly::detail::DefaultGenerator& rng,
      velox::memory::MemoryPool* pool,
      const velox::TypePtr& type,
      std::function<velox::VectorPtr(const velox::TypePtr&)> generator,
      std::function<bool(
          const velox::VectorPtr&,
          const velox::VectorPtr&,
          velox::vector_size_t)> validator,
      size_t count) {
    SerializerOptions options{
        .compressionType = CompressionType::Zstd,
        .compressionThreshold = 32,
        .compressionLevel = 3,
        .version = version(),
    };
    Serializer serializer{options, type, pool};
    Deserializer deserializer{
        SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes()),
        pool,
        DeserializerOptions{
            .version = version(),
        }};

    velox::VectorPtr output;
    velox::VectorPtr expected;
    std::vector<std::string> serialized;
    for (auto i = 0; i < count; ++i) {
      auto input = generator(type);
      serialized.emplace_back(
          serializer.serialize(input, OrderedRanges::of(0, input->size())));
      if (!expected) {
        expected = input;
      } else {
        auto oldSize = expected->size();
        expected->resize(oldSize + input->size());
        expected->copy(input.get(), oldSize, 0, input->size());
      }
      if (i < count - 1 && folly::Random::oneIn(3, rng)) {
        velox::BaseVector::ensureWritable(
            velox::SelectivityVector::empty(),
            expected->type(),
            expected->pool(),
            expected);
        continue;
      }
      std::vector<std::string_view> serializedSVs;
      serializedSVs.reserve(serialized.size());
      for (auto& s : serialized) {
        serializedSVs.push_back(s);
      }
      deserializer.deserialize(serializedSVs, output);

      ASSERT_EQ(output->size(), expected->size());
      for (auto j = 0; j < expected->size(); ++j) {
        ASSERT_TRUE(validator(output, expected, j))
            << "Content mismatch at index " << j
            << "\nReference: " << expected->toString(j)
            << "\nResult: " << output->toString(j);
      }
      expected.reset();
      serialized.clear();
    }
  }
};

TEST_P(SerializationTest, fuzzSimple) {
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
  auto seed = folly::Random::rand32();
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
      pool_.get(),
      seed);

  folly::detail::DefaultGenerator rng{seed};
  auto iterations = 20;
  auto batches = 20;
  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify(
        rng,
        pool_.get(),
        type,
        [&](auto& type) {
          return noNulls.fuzzInputRow(
              std::dynamic_pointer_cast<const velox::RowType>(type));
        },
        vectorEquals,
        batches);
  }
}

namespace {

std::string formatName(const ::testing::TestParamInfo<TestParams>& info) {
  std::string name;
  if (!info.param.version.has_value()) {
    name = "DenseFormat";
  } else {
    switch (*info.param.version) {
      case SerializationVersion::kDense:
        name = "DenseFormat";
        break;
      case SerializationVersion::kSparse:
        name = "SparseFormat";
        break;
      case SerializationVersion::kDenseEncoded:
        name = "DenseEncodedFormat";
        break;
      case SerializationVersion::kSparseEncoded:
        name = "SparseEncodedFormat";
        break;
    }
  }
  // Add compression suffix for encoding modes.
  if (info.param.compressionEnabled) {
    name += "_Compressed";
  }
  return name;
}

} // namespace

TEST_P(SerializationTest, flatMapEncodingFuzz) {
  // Test flat map encoding with fuzzer-generated data.
  // Iterates over different schema variations:
  // - Scalar value types
  // - Nested FlatMap value types (Map<K, Map<K2, V>>)
  // - Mix of scalar and nested

  auto seed = folly::Random::rand32();
  LOG(INFO) << "flatMapEncodingFuzz seed: " << seed;

  // Schema 1: Scalar value types only.
  auto scalarType = velox::ROW({
      {"id", velox::BIGINT()},
      {"int_features", velox::MAP(velox::INTEGER(), velox::REAL())},
      {"string_features", velox::MAP(velox::VARCHAR(), velox::DOUBLE())},
  });

  // Schema 2: Nested FlatMap value types (Map<K, Map<K2, V>>).
  auto nestedType = velox::ROW({
      {"id", velox::BIGINT()},
      {"nested_features",
       velox::MAP(
           velox::INTEGER(), velox::MAP(velox::INTEGER(), velox::REAL()))},
  });

  // Schema 3: Mix of scalar and nested FlatMap value types.
  auto mixedType = velox::ROW({
      {"id", velox::BIGINT()},
      {"scalar_features", velox::MAP(velox::INTEGER(), velox::DOUBLE())},
      {"nested_features",
       velox::MAP(
           velox::VARCHAR(), velox::MAP(velox::INTEGER(), velox::REAL()))},
  });

  // Schema 4: Map of Array (Map<K, Array<V>>).
  auto mapOfArrayType = velox::ROW({
      {"id", velox::BIGINT()},
      {"array_features",
       velox::MAP(velox::INTEGER(), velox::ARRAY(velox::BIGINT()))},
  });

  // Schema 5: Map of Array of Array (Map<K, Array<Array<V>>>).
  auto mapOfNestedArrayType = velox::ROW({
      {"id", velox::BIGINT()},
      {"nested_array_features",
       velox::MAP(
           velox::INTEGER(), velox::ARRAY(velox::ARRAY(velox::BIGINT())))},
  });

  struct TestCase {
    std::string name;
    velox::TypePtr type;
    folly::F14FastSet<std::string> flatMapColumns;
  };

  std::vector<TestCase> testCases = {
      {"scalar", scalarType, {"int_features", "string_features"}},
      {"nested", nestedType, {"nested_features"}},
      {"mixed", mixedType, {"scalar_features", "nested_features"}},
      {"mapOfArray", mapOfArrayType, {"array_features"}},
      {"mapOfNestedArray", mapOfNestedArrayType, {"nested_array_features"}},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);

    size_t batchSize = 50;
    velox::VectorFuzzer fuzzer(
        {
            .vectorSize = batchSize,
            .nullRatio = 0,
            // Use fixed non-zero string length to avoid empty string keys.
            .stringLength = 8,
            .stringVariableLength = false,
            .containerLength = 5,
            .containerVariableLength = true,
            // Remove duplicate keys from maps.
            .normalizeMapKeys = true,
        },
        pool_.get(),
        seed);

    const SerializerOptions options{
        .compressionType = CompressionType::Zstd,
        .compressionThreshold = 32,
        .compressionLevel = 3,
        .version = version(),
        .flatMapColumns = testCase.flatMapColumns,
    };
    Serializer serializer{options, testCase.type, pool_.get()};

    // Generate and serialize multiple batches.
    auto iterations = 20;
    std::vector<velox::VectorPtr> inputs;
    std::vector<std::string> serializedData;
    inputs.reserve(iterations);
    serializedData.reserve(iterations);

    for (auto i = 0; i < iterations; ++i) {
      auto input = fuzzer.fuzzInputRow(
          std::dynamic_pointer_cast<const velox::RowType>(testCase.type));
      inputs.push_back(input);
      serializedData.emplace_back(
          serializer.serialize(input, OrderedRanges::of(0, input->size())));
    }

    // Create deserializer with final schema containing all discovered keys.
    Deserializer deserializer{
        SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes()),
        pool_.get(),
        DeserializerOptions{
            .version = version(),
        }};

    // Deserialize and verify each batch.
    velox::VectorPtr output;
    for (auto i = 0; i < iterations; ++i) {
      deserializer.deserialize(serializedData[i], output);

      ASSERT_EQ(output->size(), inputs[i]->size());
      for (velox::vector_size_t j = 0; j < inputs[i]->size(); ++j) {
        ASSERT_TRUE(vectorEquals(output, inputs[i], j))
            << "Content mismatch at index " << j
            << "\nReference: " << inputs[i]->toString(j)
            << "\nResult: " << output->toString(j);
      }
    }
  }
}

TEST_P(SerializationTest, fuzzComplex) {
  auto type = velox::ROW({
      {"array", velox::ARRAY(velox::REAL())},
      {"map", velox::MAP(velox::INTEGER(), velox::DOUBLE())},
      {"row",
       velox::ROW({
           {"a", velox::REAL()},
           {"b", velox::INTEGER()},
       })},
      {"nested",
       velox::ARRAY(
           velox::ROW({
               {"a", velox::INTEGER()},
               {"b", velox::MAP(velox::REAL(), velox::REAL())},
           }))},
  });
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;

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
      pool_.get(),
      seed);

  folly::detail::DefaultGenerator rng{seed};
  auto iterations = 20;
  auto batches = 20;
  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify(
        rng,
        pool_.get(),
        type,
        [&](auto& type) {
          return noNulls.fuzzInputRow(
              std::dynamic_pointer_cast<const velox::RowType>(type));
        },
        vectorEquals,
        batches);
  }
}

TEST_P(SerializationTest, rootNotRow) {
  const auto type = velox::MAP(velox::INTEGER(), velox::ARRAY(velox::DOUBLE()));
  const auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;

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
      pool_.get(),
      seed);

  folly::detail::DefaultGenerator rng{seed};
  const auto iterations = 20;
  const auto batches = 20;
  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify(
        rng,
        pool_.get(),
        type,
        [&](auto& type) { return noNulls.fuzz(type); },
        vectorEquals,
        batches);
  }
}

TEST_P(SerializationTest, flatMapEncoding) {
  // Test flat map encoding with fixed keys.
  auto type = velox::ROW({
      {"id", velox::BIGINT()},
      {"float_features", velox::MAP(velox::INTEGER(), velox::REAL())},
      {"string_features", velox::MAP(velox::VARCHAR(), velox::DOUBLE())},
  });

  auto seed = folly::Random::rand32();
  LOG(INFO) << "flatMapEncoding seed: " << seed;

  // Fixed keys for FlatMap columns - every row has the same keys.
  const std::vector<int32_t> intKeys = {1, 2, 3, 4, 5};
  const std::vector<std::string> stringKeys = {"key_a", "key_b", "key_c"};

  size_t batchSize = 100;
  folly::Random::DefaultGenerator rng(seed);

  // Generate input with fixed keys.
  auto generateInput = [&]() -> velox::VectorPtr {
    // Generate id column
    auto ids =
        velox::BaseVector::create(velox::BIGINT(), batchSize, pool_.get());
    const auto batchSizeInt = static_cast<velox::vector_size_t>(batchSize);
    const auto intKeysCount = static_cast<velox::vector_size_t>(intKeys.size());
    const auto stringKeysCount =
        static_cast<velox::vector_size_t>(stringKeys.size());

    for (velox::vector_size_t i = 0; i < batchSizeInt; ++i) {
      ids->asFlatVector<int64_t>()->set(i, folly::Random::rand64(rng));
    }

    // Generate float_features with fixed integer keys
    std::vector<velox::VectorPtr> intMapKeys;
    std::vector<velox::VectorPtr> intMapValues;
    std::vector<velox::vector_size_t> intMapOffsets;
    intMapOffsets.push_back(0);

    auto intKeysFlat = velox::BaseVector::create(
        velox::INTEGER(), batchSizeInt * intKeysCount, pool_.get());
    auto intValuesFlat = velox::BaseVector::create(
        velox::REAL(), batchSizeInt * intKeysCount, pool_.get());
    velox::vector_size_t intOffset = 0;
    for (velox::vector_size_t i = 0; i < batchSizeInt; ++i) {
      for (auto key : intKeys) {
        intKeysFlat->asFlatVector<int32_t>()->set(intOffset, key);
        intValuesFlat->asFlatVector<float>()->set(
            intOffset, folly::Random::randDouble01(rng));
        ++intOffset;
      }
      intMapOffsets.push_back(intOffset);
    }

    auto intMapVector = std::make_shared<velox::MapVector>(
        pool_.get(),
        velox::MAP(velox::INTEGER(), velox::REAL()),
        nullptr,
        batchSizeInt,
        velox::allocateOffsets(batchSizeInt, pool_.get()),
        velox::allocateSizes(batchSizeInt, pool_.get()),
        intKeysFlat,
        intValuesFlat);

    auto* intRawOffsets = intMapVector->mutableOffsets(batchSizeInt)
                              ->asMutable<velox::vector_size_t>();
    auto* intRawSizes = intMapVector->mutableSizes(batchSizeInt)
                            ->asMutable<velox::vector_size_t>();
    for (velox::vector_size_t i = 0; i < batchSizeInt; ++i) {
      intRawOffsets[i] = intMapOffsets[i];
      intRawSizes[i] = intKeysCount;
    }

    // Generate string_features with fixed string keys
    auto strKeysFlat = velox::BaseVector::create(
        velox::VARCHAR(), batchSizeInt * stringKeysCount, pool_.get());
    auto strValuesFlat = velox::BaseVector::create(
        velox::DOUBLE(), batchSizeInt * stringKeysCount, pool_.get());
    velox::vector_size_t strOffset = 0;
    for (velox::vector_size_t i = 0; i < batchSizeInt; ++i) {
      for (const auto& key : stringKeys) {
        strKeysFlat->asFlatVector<velox::StringView>()->set(
            strOffset, velox::StringView(key));
        strValuesFlat->asFlatVector<double>()->set(
            strOffset, folly::Random::randDouble01(rng));
        ++strOffset;
      }
    }

    auto strMapVector = std::make_shared<velox::MapVector>(
        pool_.get(),
        velox::MAP(velox::VARCHAR(), velox::DOUBLE()),
        nullptr,
        batchSizeInt,
        velox::allocateOffsets(batchSizeInt, pool_.get()),
        velox::allocateSizes(batchSizeInt, pool_.get()),
        strKeysFlat,
        strValuesFlat);

    auto* strRawOffsets = strMapVector->mutableOffsets(batchSizeInt)
                              ->asMutable<velox::vector_size_t>();
    auto* strRawSizes = strMapVector->mutableSizes(batchSizeInt)
                            ->asMutable<velox::vector_size_t>();
    for (velox::vector_size_t i = 0; i < batchSizeInt; ++i) {
      strRawOffsets[i] = i * stringKeysCount;
      strRawSizes[i] = stringKeysCount;
    }

    // Create row vector
    return std::make_shared<velox::RowVector>(
        pool_.get(),
        type,
        nullptr,
        batchSizeInt,
        std::vector<velox::VectorPtr>{ids, intMapVector, strMapVector});
  };

  const SerializerOptions options{
      .compressionType = CompressionType::Zstd,
      .compressionThreshold = 32,
      .compressionLevel = 3,
      .version = version(),
      .flatMapColumns = {"float_features", "string_features"},
  };
  Serializer serializer{options, type, pool_.get()};

  // Serialize multiple batches with the same fixed keys.
  auto iterations = 10;
  std::vector<velox::VectorPtr> inputs;
  std::vector<std::string> serializedData;
  inputs.reserve(iterations);
  serializedData.reserve(iterations);

  for (auto i = 0; i < iterations; ++i) {
    auto input = generateInput();
    inputs.push_back(input);
    serializedData.emplace_back(
        serializer.serialize(input, OrderedRanges::of(0, input->size())));
  }

  // Create deserializer with final schema.
  Deserializer deserializer{
      SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes()),
      pool_.get(),
      DeserializerOptions{
          .version = version(),
      }};

  // Deserialize and verify each batch.
  velox::VectorPtr output;
  for (auto i = 0; i < iterations; ++i) {
    deserializer.deserialize(serializedData[i], output);

    ASSERT_EQ(output->size(), inputs[i]->size());
    for (velox::vector_size_t j = 0; j < inputs[i]->size(); ++j) {
      ASSERT_TRUE(vectorEquals(output, inputs[i], j))
          << "Content mismatch at index " << j
          << "\nReference: " << inputs[i]->toString(j)
          << "\nResult: " << output->toString(j);
    }
  }
}

TEST_P(SerializationTest, flatMapEncodingWithVaryingKeys) {
  // Test flat map encoding where each row has a varying number of keys.
  // This tests the realistic scenario where different rows have different
  // subsets of the possible FlatMap keys.
  auto type = velox::ROW({
      {"id", velox::BIGINT()},
      {"features", velox::MAP(velox::INTEGER(), velox::DOUBLE())},
  });

  auto seed = folly::Random::rand32();
  LOG(INFO) << "flatMapEncodingWithVaryingKeys seed: " << seed;

  // All possible keys - each row will have a subset of these.
  const std::vector<int32_t> allKeys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  size_t batchSize = 50;
  folly::Random::DefaultGenerator rng(seed);

  // Generate input where each row has a varying subset of keys.
  auto generateInput = [&]() -> velox::VectorPtr {
    const auto batchSizeInt = static_cast<velox::vector_size_t>(batchSize);
    const auto allKeysCount = static_cast<velox::vector_size_t>(allKeys.size());

    // Generate id column
    auto ids =
        velox::BaseVector::create(velox::BIGINT(), batchSizeInt, pool_.get());
    for (velox::vector_size_t i = 0; i < batchSizeInt; ++i) {
      ids->asFlatVector<int64_t>()->set(i, folly::Random::rand64(rng));
    }

    // For each row, randomly select a subset of keys (1 to allKeys.size()).
    // To ensure determinism and reproducibility, each row gets keys based on
    // its index.
    std::vector<velox::vector_size_t> mapOffsets;
    std::vector<int32_t> keysData;
    std::vector<double> valuesData;

    mapOffsets.push_back(0);
    for (velox::vector_size_t row = 0; row < batchSizeInt; ++row) {
      // Each row gets a different number of keys (1 to allKeys.size()).
      // Use a deterministic pattern: row 0 gets 1 key, row 1 gets 2 keys, etc.
      velox::vector_size_t numKeysForRow = (row % allKeysCount) + 1;

      for (velox::vector_size_t k = 0; k < numKeysForRow; ++k) {
        keysData.push_back(allKeys[k]);
        valuesData.push_back(folly::Random::randDouble01(rng));
      }
      mapOffsets.push_back(static_cast<velox::vector_size_t>(keysData.size()));
    }

    // Create keys vector
    const auto keysCount = static_cast<velox::vector_size_t>(keysData.size());
    auto keysFlat =
        velox::BaseVector::create(velox::INTEGER(), keysCount, pool_.get());
    for (velox::vector_size_t i = 0; i < keysCount; ++i) {
      keysFlat->asFlatVector<int32_t>()->set(i, keysData[i]);
    }

    // Create values vector
    const auto valuesCount =
        static_cast<velox::vector_size_t>(valuesData.size());
    auto valuesFlat =
        velox::BaseVector::create(velox::DOUBLE(), valuesCount, pool_.get());
    for (velox::vector_size_t i = 0; i < valuesCount; ++i) {
      valuesFlat->asFlatVector<double>()->set(i, valuesData[i]);
    }

    // Create map vector
    auto mapVector = std::make_shared<velox::MapVector>(
        pool_.get(),
        velox::MAP(velox::INTEGER(), velox::DOUBLE()),
        nullptr,
        batchSizeInt,
        velox::allocateOffsets(batchSizeInt, pool_.get()),
        velox::allocateSizes(batchSizeInt, pool_.get()),
        keysFlat,
        valuesFlat);

    auto* rawOffsets = mapVector->mutableOffsets(batchSizeInt)
                           ->asMutable<velox::vector_size_t>();
    auto* rawSizes = mapVector->mutableSizes(batchSizeInt)
                         ->asMutable<velox::vector_size_t>();
    for (velox::vector_size_t i = 0; i < batchSizeInt; ++i) {
      rawOffsets[i] = mapOffsets[i];
      rawSizes[i] = mapOffsets[i + 1] - mapOffsets[i];
    }

    // Create row vector
    return std::make_shared<velox::RowVector>(
        pool_.get(),
        type,
        nullptr,
        batchSizeInt,
        std::vector<velox::VectorPtr>{ids, mapVector});
  };

  const SerializerOptions options{
      .compressionType = CompressionType::Zstd,
      .compressionThreshold = 32,
      .compressionLevel = 3,
      .version = version(),
      .flatMapColumns = {"features"},
  };
  Serializer serializer{options, type, pool_.get()};

  // Serialize multiple batches.
  auto iterations = 10;
  std::vector<velox::VectorPtr> inputs;
  std::vector<std::string> serializedData;
  inputs.reserve(iterations);
  serializedData.reserve(iterations);

  for (auto i = 0; i < iterations; ++i) {
    auto input = generateInput();
    inputs.push_back(input);
    serializedData.emplace_back(
        serializer.serialize(input, OrderedRanges::of(0, input->size())));
  }

  // Create deserializer with final schema containing all discovered keys.
  Deserializer deserializer{
      SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes()),
      pool_.get(),
      DeserializerOptions{
          .version = version(),
      }};

  // Deserialize and verify each batch.
  velox::VectorPtr output;
  for (auto i = 0; i < iterations; ++i) {
    deserializer.deserialize(serializedData[i], output);

    ASSERT_EQ(output->size(), inputs[i]->size());
    for (velox::vector_size_t j = 0; j < inputs[i]->size(); ++j) {
      ASSERT_TRUE(vectorEquals(output, inputs[i], j))
          << "Content mismatch at index " << j
          << "\nReference: " << inputs[i]->toString(j)
          << "\nResult: " << output->toString(j);
    }
  }
}

TEST_P(SerializationTest, flatMapEncodingWithNestedTypes) {
  // Test flat map encoding with nested value types using fixed keys.
  auto type = velox::ROW({
      {"id", velox::BIGINT()},
      {"nested_features",
       velox::MAP(velox::INTEGER(), velox::ARRAY(velox::REAL()))},
      {"struct_features",
       velox::MAP(
           velox::VARCHAR(),
           velox::ROW({{"a", velox::INTEGER()}, {"b", velox::DOUBLE()}}))},
  });

  auto seed = folly::Random::rand32();
  LOG(INFO) << "flatMapEncodingWithNestedTypes seed: " << seed;

  // Fixed keys for FlatMap columns - every row has the same keys.
  const std::vector<int32_t> intKeys = {10, 20, 30};
  const std::vector<std::string> stringKeys = {"struct_a", "struct_b"};

  size_t batchSize = 50;
  folly::Random::DefaultGenerator rng(seed);

  // Generate input with fixed keys.
  auto generateInput = [&]() -> velox::VectorPtr {
    const auto batchSizeInt = static_cast<velox::vector_size_t>(batchSize);
    const auto intKeysCount = static_cast<velox::vector_size_t>(intKeys.size());
    const auto stringKeysCount =
        static_cast<velox::vector_size_t>(stringKeys.size());
    const velox::vector_size_t arraySize = 3;

    // Generate id column
    auto ids =
        velox::BaseVector::create(velox::BIGINT(), batchSizeInt, pool_.get());
    for (velox::vector_size_t i = 0; i < batchSizeInt; ++i) {
      ids->asFlatVector<int64_t>()->set(i, folly::Random::rand64(rng));
    }

    // Generate nested_features: MAP(INTEGER, ARRAY(REAL)) with fixed keys
    const auto totalArrayElements = batchSizeInt * intKeysCount * arraySize;
    const auto totalMapEntries = batchSizeInt * intKeysCount;

    auto intKeysFlat = velox::BaseVector::create(
        velox::INTEGER(), totalMapEntries, pool_.get());
    auto arrayElements = velox::BaseVector::create(
        velox::REAL(), totalArrayElements, pool_.get());

    // Fill array elements with random values
    for (velox::vector_size_t i = 0; i < totalArrayElements; ++i) {
      arrayElements->asFlatVector<float>()->set(
          i, folly::Random::randDouble01(rng));
    }

    // Create the nested array vector
    auto arrayVector = std::make_shared<velox::ArrayVector>(
        pool_.get(),
        velox::ARRAY(velox::REAL()),
        nullptr,
        totalMapEntries,
        velox::allocateOffsets(totalMapEntries, pool_.get()),
        velox::allocateSizes(totalMapEntries, pool_.get()),
        arrayElements);

    auto* arrayOffsets = arrayVector->mutableOffsets(totalMapEntries)
                             ->asMutable<velox::vector_size_t>();
    auto* arraySizes = arrayVector->mutableSizes(totalMapEntries)
                           ->asMutable<velox::vector_size_t>();
    for (velox::vector_size_t i = 0; i < totalMapEntries; ++i) {
      arrayOffsets[i] = i * arraySize;
      arraySizes[i] = arraySize;
    }

    // Fill integer keys
    velox::vector_size_t keyIdx = 0;
    for (velox::vector_size_t i = 0; i < batchSizeInt; ++i) {
      for (auto key : intKeys) {
        intKeysFlat->asFlatVector<int32_t>()->set(keyIdx++, key);
      }
    }

    auto intMapVector = std::make_shared<velox::MapVector>(
        pool_.get(),
        velox::MAP(velox::INTEGER(), velox::ARRAY(velox::REAL())),
        nullptr,
        batchSizeInt,
        velox::allocateOffsets(batchSizeInt, pool_.get()),
        velox::allocateSizes(batchSizeInt, pool_.get()),
        intKeysFlat,
        arrayVector);

    auto* intMapOffsets = intMapVector->mutableOffsets(batchSizeInt)
                              ->asMutable<velox::vector_size_t>();
    auto* intMapSizes = intMapVector->mutableSizes(batchSizeInt)
                            ->asMutable<velox::vector_size_t>();
    for (velox::vector_size_t i = 0; i < batchSizeInt; ++i) {
      intMapOffsets[i] = i * intKeysCount;
      intMapSizes[i] = intKeysCount;
    }

    // Generate struct_features: MAP(VARCHAR, ROW({a: INT, b: DOUBLE}))
    const auto totalStrMapEntries = batchSizeInt * stringKeysCount;
    auto strKeysFlat = velox::BaseVector::create(
        velox::VARCHAR(), totalStrMapEntries, pool_.get());
    auto structFieldA = velox::BaseVector::create(
        velox::INTEGER(), totalStrMapEntries, pool_.get());
    auto structFieldB = velox::BaseVector::create(
        velox::DOUBLE(), totalStrMapEntries, pool_.get());

    velox::vector_size_t strIdx = 0;
    for (velox::vector_size_t i = 0; i < batchSizeInt; ++i) {
      for (const auto& key : stringKeys) {
        strKeysFlat->asFlatVector<velox::StringView>()->set(
            strIdx, velox::StringView(key));
        structFieldA->asFlatVector<int32_t>()->set(
            strIdx, folly::Random::rand32(rng));
        structFieldB->asFlatVector<double>()->set(
            strIdx, folly::Random::randDouble01(rng));
        ++strIdx;
      }
    }

    auto structVector = std::make_shared<velox::RowVector>(
        pool_.get(),
        velox::ROW({{"a", velox::INTEGER()}, {"b", velox::DOUBLE()}}),
        nullptr,
        totalStrMapEntries,
        std::vector<velox::VectorPtr>{structFieldA, structFieldB});

    auto strMapVector = std::make_shared<velox::MapVector>(
        pool_.get(),
        velox::MAP(
            velox::VARCHAR(),
            velox::ROW({{"a", velox::INTEGER()}, {"b", velox::DOUBLE()}})),
        nullptr,
        batchSizeInt,
        velox::allocateOffsets(batchSizeInt, pool_.get()),
        velox::allocateSizes(batchSizeInt, pool_.get()),
        strKeysFlat,
        structVector);

    auto* strMapOffsets = strMapVector->mutableOffsets(batchSizeInt)
                              ->asMutable<velox::vector_size_t>();
    auto* strMapSizes = strMapVector->mutableSizes(batchSizeInt)
                            ->asMutable<velox::vector_size_t>();
    for (velox::vector_size_t i = 0; i < batchSizeInt; ++i) {
      strMapOffsets[i] = i * stringKeysCount;
      strMapSizes[i] = stringKeysCount;
    }

    // Create row vector
    return std::make_shared<velox::RowVector>(
        pool_.get(),
        type,
        nullptr,
        batchSizeInt,
        std::vector<velox::VectorPtr>{ids, intMapVector, strMapVector});
  };

  SerializerOptions options{
      .compressionType = CompressionType::Uncompressed,
      .version = version(),
      .flatMapColumns = {"nested_features", "struct_features"},
  };
  Serializer serializer{options, type, pool_.get()};

  // Serialize multiple batches with the same fixed keys.
  auto iterations = 10;
  std::vector<velox::VectorPtr> inputs;
  std::vector<std::string> serializedData;
  inputs.reserve(iterations);
  serializedData.reserve(iterations);

  for (auto i = 0; i < iterations; ++i) {
    auto input = generateInput();
    inputs.push_back(input);
    serializedData.emplace_back(
        serializer.serialize(input, OrderedRanges::of(0, input->size())));
  }

  // Create deserializer with final schema.
  Deserializer deserializer{
      SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes()),
      pool_.get(),
      DeserializerOptions{
          .version = version(),
      }};

  // Deserialize and verify each batch.
  velox::VectorPtr output;
  for (auto i = 0; i < iterations; ++i) {
    deserializer.deserialize(serializedData[i], output);

    ASSERT_EQ(output->size(), inputs[i]->size());
    for (velox::vector_size_t j = 0; j < inputs[i]->size(); ++j) {
      ASSERT_TRUE(vectorEquals(output, inputs[i], j))
          << "Content mismatch at index " << j
          << "\nReference: " << inputs[i]->toString(j)
          << "\nResult: " << output->toString(j);
    }
  }
}

TEST_P(SerializationTest, nestedFlatMapWithVaryingInnerKeys) {
  // Test nested FlatMap where inner FlatMap has varying keys across batches.
  // This tests the scenario where:
  // - Batch 1: outer key "a" has inner keys [1, 2]
  // - Batch 2: outer key "a" has inner keys [2, 3] (key 1 missing, key 3 new)
  // The deserialization should correctly handle the missing inner keys.
  auto type = velox::ROW({
      {"id", velox::BIGINT()},
      {"nested_map",
       velox::MAP(
           velox::VARCHAR(), velox::MAP(velox::INTEGER(), velox::DOUBLE()))},
  });

  auto seed = folly::Random::rand32();
  LOG(INFO) << "nestedFlatMapWithVaryingInnerKeys seed: " << seed;

  folly::Random::DefaultGenerator rng(seed);
  const velox::vector_size_t batchSize = 10;

  // Helper to create a batch with specific outer and inner keys.
  auto generateBatch =
      [&](const std::vector<std::string>& outerKeys,
          const std::vector<std::vector<int32_t>>& innerKeysPerOuterKey)
      -> velox::VectorPtr {
    VELOX_CHECK_EQ(outerKeys.size(), innerKeysPerOuterKey.size());

    // Generate id column
    auto ids =
        velox::BaseVector::create(velox::BIGINT(), batchSize, pool_.get());
    for (velox::vector_size_t i = 0; i < batchSize; ++i) {
      ids->asFlatVector<int64_t>()->set(i, folly::Random::rand64(rng));
    }

    // Calculate total sizes
    velox::vector_size_t totalOuterEntries = 0;
    velox::vector_size_t totalInnerEntries = 0;
    for (size_t k = 0; k < outerKeys.size(); ++k) {
      totalOuterEntries += batchSize; // Each row has each outer key
      totalInnerEntries += batchSize *
          static_cast<velox::vector_size_t>(innerKeysPerOuterKey[k].size());
    }

    // Create inner map keys and values
    auto innerKeys = velox::BaseVector::create(
        velox::INTEGER(), totalInnerEntries, pool_.get());
    auto innerValues = velox::BaseVector::create(
        velox::DOUBLE(), totalInnerEntries, pool_.get());

    // Create inner map vector
    auto innerMap = std::make_shared<velox::MapVector>(
        pool_.get(),
        velox::MAP(velox::INTEGER(), velox::DOUBLE()),
        nullptr,
        totalOuterEntries,
        velox::allocateOffsets(totalOuterEntries, pool_.get()),
        velox::allocateSizes(totalOuterEntries, pool_.get()),
        innerKeys,
        innerValues);

    auto* innerMapOffsets = innerMap->mutableOffsets(totalOuterEntries)
                                ->asMutable<velox::vector_size_t>();
    auto* innerMapSizes = innerMap->mutableSizes(totalOuterEntries)
                              ->asMutable<velox::vector_size_t>();

    // Create outer map keys
    auto outerKeysVec = velox::BaseVector::create(
        velox::VARCHAR(), totalOuterEntries, pool_.get());

    // Fill data: for each row, add all outer keys with their inner maps
    velox::vector_size_t outerIdx = 0;
    velox::vector_size_t innerIdx = 0;
    for (velox::vector_size_t row = 0; row < batchSize; ++row) {
      for (size_t k = 0; k < outerKeys.size(); ++k) {
        outerKeysVec->asFlatVector<velox::StringView>()->set(
            outerIdx, velox::StringView(outerKeys[k]));

        innerMapOffsets[outerIdx] = innerIdx;
        const auto& innerKeysForThis = innerKeysPerOuterKey[k];
        innerMapSizes[outerIdx] =
            static_cast<velox::vector_size_t>(innerKeysForThis.size());

        for (auto innerKey : innerKeysForThis) {
          innerKeys->asFlatVector<int32_t>()->set(innerIdx, innerKey);
          innerValues->asFlatVector<double>()->set(
              innerIdx, folly::Random::randDouble01(rng));
          ++innerIdx;
        }
        ++outerIdx;
      }
    }

    // Create outer map vector
    auto outerMap = std::make_shared<velox::MapVector>(
        pool_.get(),
        velox::MAP(
            velox::VARCHAR(), velox::MAP(velox::INTEGER(), velox::DOUBLE())),
        nullptr,
        batchSize,
        velox::allocateOffsets(batchSize, pool_.get()),
        velox::allocateSizes(batchSize, pool_.get()),
        outerKeysVec,
        innerMap);

    auto* outerMapOffsets =
        outerMap->mutableOffsets(batchSize)->asMutable<velox::vector_size_t>();
    auto* outerMapSizes =
        outerMap->mutableSizes(batchSize)->asMutable<velox::vector_size_t>();

    const auto outerKeysCount =
        static_cast<velox::vector_size_t>(outerKeys.size());
    for (velox::vector_size_t i = 0; i < batchSize; ++i) {
      outerMapOffsets[i] = i * outerKeysCount;
      outerMapSizes[i] = outerKeysCount;
    }

    return std::make_shared<velox::RowVector>(
        pool_.get(),
        type,
        nullptr,
        batchSize,
        std::vector<velox::VectorPtr>{ids, outerMap});
  };

  SerializerOptions options{
      .compressionType = CompressionType::Zstd,
      .compressionThreshold = 32,
      .compressionLevel = 3,
      .version = version(),
      .flatMapColumns = {"nested_map"},
  };
  Serializer serializer{options, type, pool_.get()};

  // Create batches with varying inner keys:
  // Batch 1: outer key "a" has inner keys [1, 2], outer key "b" has [10]
  // Batch 2: outer key "a" has inner keys [2, 3], outer key "b" has [20]
  // (inner key 1 missing in batch 2, inner key 3 new in batch 2)
  std::vector<velox::VectorPtr> inputs;
  std::vector<std::string> serializedData;

  // Batch 1
  auto batch1 = generateBatch({"a", "b"}, {{1, 2}, {10}});
  inputs.push_back(batch1);
  serializedData.emplace_back(
      serializer.serialize(batch1, OrderedRanges::of(0, batch1->size())));

  // Batch 2 - different inner keys
  auto batch2 = generateBatch({"a", "b"}, {{2, 3}, {20}});
  inputs.push_back(batch2);
  serializedData.emplace_back(
      serializer.serialize(batch2, OrderedRanges::of(0, batch2->size())));

  // Batch 3 - outer key "c" is new, "a" has different inner keys again
  auto batch3 = generateBatch({"a", "c"}, {{1, 3}, {100}});
  inputs.push_back(batch3);
  serializedData.emplace_back(
      serializer.serialize(batch3, OrderedRanges::of(0, batch3->size())));

  // Create deserializer with final schema containing all discovered keys.
  Deserializer deserializer{
      SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes()),
      pool_.get(),
      DeserializerOptions{
          .version = version(),
      }};

  // Test 1: Deserialize each batch individually - should work.
  velox::VectorPtr output;
  for (size_t i = 0; i < inputs.size(); ++i) {
    deserializer.deserialize(serializedData[i], output);
    ASSERT_EQ(output->size(), inputs[i]->size())
        << "Size mismatch for batch " << i;
    for (velox::vector_size_t j = 0; j < inputs[i]->size(); ++j) {
      ASSERT_TRUE(vectorEquals(output, inputs[i], j))
          << "Content mismatch at batch " << i << " index " << j
          << "\nReference: " << inputs[i]->toString(j)
          << "\nResult: " << output->toString(j);
    }
  }

  // Test 2: Deserialize multiple batches together - this is the key test.
  // This tests gap detection for nested FlatMaps with varying inner keys.
  std::vector<std::string_view> allBatches;
  allBatches.reserve(serializedData.size());
  for (const auto& s : serializedData) {
    allBatches.push_back(s);
  }

  deserializer.deserialize(allBatches, output);

  // Build expected output by concatenating inputs.
  velox::VectorPtr expected = inputs[0];
  for (size_t i = 1; i < inputs.size(); ++i) {
    auto oldSize = expected->size();
    expected->resize(oldSize + inputs[i]->size());
    expected->copy(inputs[i].get(), oldSize, 0, inputs[i]->size());
  }

  ASSERT_EQ(output->size(), expected->size()) << "Multi-batch size mismatch";

  for (velox::vector_size_t j = 0; j < expected->size(); ++j) {
    ASSERT_TRUE(vectorEquals(output, expected, j))
        << "Multi-batch content mismatch at index " << j
        << "\nReference: " << expected->toString(j)
        << "\nResult: " << output->toString(j);
  }
}

TEST_P(SerializationTest, nullsNotSupported) {
  // Test that the serializer throws when input has nulls.
  // The serializer currently does not support null values.

  auto seed = folly::Random::rand32();
  LOG(INFO) << "nullsNotSupported seed: " << seed;

  // Test 1: Top-level row with null.
  {
    auto type =
        velox::ROW({{"id", velox::BIGINT()}, {"value", velox::DOUBLE()}});
    auto row = velox::BaseVector::create(type, 10, pool_.get());
    row->setNull(5, true); // Set one row as null

    SerializerOptions options{.version = version()};
    Serializer serializer{options, type, pool_.get()};

    NIMBLE_ASSERT_THROW(
        serializer.serialize(row, OrderedRanges::of(0, row->size())),
        "nulls not supported");
  }

  // Test 2: Nested column with null.
  {
    auto type =
        velox::ROW({{"id", velox::BIGINT()}, {"value", velox::DOUBLE()}});
    auto ids = velox::BaseVector::create(velox::BIGINT(), 10, pool_.get());
    auto values = velox::BaseVector::create(velox::DOUBLE(), 10, pool_.get());
    for (velox::vector_size_t i = 0; i < 10; ++i) {
      ids->asFlatVector<int64_t>()->set(i, i);
      values->asFlatVector<double>()->set(i, i * 1.5);
    }
    values->setNull(3, true); // Set one value as null

    auto row = std::make_shared<velox::RowVector>(
        pool_.get(),
        type,
        nullptr,
        10,
        std::vector<velox::VectorPtr>{ids, values});

    SerializerOptions options{.version = version()};
    Serializer serializer{options, type, pool_.get()};

    NIMBLE_ASSERT_THROW(
        serializer.serialize(row, OrderedRanges::of(0, row->size())),
        "nulls not supported");
  }

  // Test 3: Map with null key/value.
  {
    auto type = velox::ROW({
        {"id", velox::BIGINT()},
        {"map_col", velox::MAP(velox::INTEGER(), velox::DOUBLE())},
    });

    const velox::vector_size_t numRows = 5;
    const velox::vector_size_t entriesPerRow = 3;
    const velox::vector_size_t totalEntries = numRows * entriesPerRow;

    auto ids = velox::BaseVector::create(velox::BIGINT(), numRows, pool_.get());
    auto mapKeys =
        velox::BaseVector::create(velox::INTEGER(), totalEntries, pool_.get());
    auto mapValues =
        velox::BaseVector::create(velox::DOUBLE(), totalEntries, pool_.get());

    for (velox::vector_size_t i = 0; i < numRows; ++i) {
      ids->asFlatVector<int64_t>()->set(i, i);
    }
    for (velox::vector_size_t i = 0; i < totalEntries; ++i) {
      mapKeys->asFlatVector<int32_t>()->set(i, i);
      mapValues->asFlatVector<double>()->set(i, i * 0.5);
    }
    mapValues->setNull(5, true); // Set one map value as null

    auto mapVector = std::make_shared<velox::MapVector>(
        pool_.get(),
        velox::MAP(velox::INTEGER(), velox::DOUBLE()),
        nullptr,
        numRows,
        velox::allocateOffsets(numRows, pool_.get()),
        velox::allocateSizes(numRows, pool_.get()),
        mapKeys,
        mapValues);

    auto* rawOffsets =
        mapVector->mutableOffsets(numRows)->asMutable<velox::vector_size_t>();
    auto* rawSizes =
        mapVector->mutableSizes(numRows)->asMutable<velox::vector_size_t>();
    for (velox::vector_size_t i = 0; i < numRows; ++i) {
      rawOffsets[i] = i * entriesPerRow;
      rawSizes[i] = entriesPerRow;
    }

    auto row = std::make_shared<velox::RowVector>(
        pool_.get(),
        type,
        nullptr,
        numRows,
        std::vector<velox::VectorPtr>{ids, mapVector});

    SerializerOptions options{.version = version()};
    Serializer serializer{options, type, pool_.get()};

    NIMBLE_ASSERT_THROW(
        serializer.serialize(row, OrderedRanges::of(0, row->size())),
        "nulls not supported");
  }

  // Test 4: FlatMap with null value (only for sparse formats).
  if (SerializerOptions{.version = version()}.sparseFormat()) {
    auto type = velox::ROW({
        {"id", velox::BIGINT()},
        {"features", velox::MAP(velox::INTEGER(), velox::DOUBLE())},
    });

    const velox::vector_size_t numRows = 5;
    const velox::vector_size_t entriesPerRow = 2;
    const velox::vector_size_t totalEntries = numRows * entriesPerRow;

    auto ids = velox::BaseVector::create(velox::BIGINT(), numRows, pool_.get());
    auto mapKeys =
        velox::BaseVector::create(velox::INTEGER(), totalEntries, pool_.get());
    auto mapValues =
        velox::BaseVector::create(velox::DOUBLE(), totalEntries, pool_.get());

    for (velox::vector_size_t i = 0; i < numRows; ++i) {
      ids->asFlatVector<int64_t>()->set(i, i);
    }
    for (velox::vector_size_t i = 0; i < totalEntries; ++i) {
      mapKeys->asFlatVector<int32_t>()->set(i, i % entriesPerRow);
      mapValues->asFlatVector<double>()->set(i, i * 0.5);
    }
    mapValues->setNull(3, true); // Set one FlatMap value as null

    auto mapVector = std::make_shared<velox::MapVector>(
        pool_.get(),
        velox::MAP(velox::INTEGER(), velox::DOUBLE()),
        nullptr,
        numRows,
        velox::allocateOffsets(numRows, pool_.get()),
        velox::allocateSizes(numRows, pool_.get()),
        mapKeys,
        mapValues);

    auto* rawOffsets =
        mapVector->mutableOffsets(numRows)->asMutable<velox::vector_size_t>();
    auto* rawSizes =
        mapVector->mutableSizes(numRows)->asMutable<velox::vector_size_t>();
    for (velox::vector_size_t i = 0; i < numRows; ++i) {
      rawOffsets[i] = i * entriesPerRow;
      rawSizes[i] = entriesPerRow;
    }

    auto row = std::make_shared<velox::RowVector>(
        pool_.get(),
        type,
        nullptr,
        numRows,
        std::vector<velox::VectorPtr>{ids, mapVector});

    SerializerOptions options{
        .version = version(),
        .flatMapColumns = {"features"},
    };
    Serializer serializer{options, type, pool_.get()};

    NIMBLE_ASSERT_THROW(
        serializer.serialize(row, OrderedRanges::of(0, row->size())),
        "nulls not supported");
  }
}

TEST_P(SerializationTest, flatMapSparseKeysScatterBitmap) {
  // Test that FlatMap values are placed at correct positions when some rows
  // don't have certain keys. This exercises the scatterBitmap code path in
  // deserialization - values must be scattered to correct positions based on
  // the inMap bitmap rather than written contiguously.
  auto type = velox::ROW({
      {"id", velox::BIGINT()},
      {"features", velox::MAP(velox::INTEGER(), velox::DOUBLE())},
  });

  // Create a batch where:
  // - Row 0: has key 1 only
  // - Row 1: has keys 1 and 2
  // - Row 2: has key 1 only
  // - Row 3: has keys 1 and 2
  // - Row 4: has key 1 only
  //
  // For key 2, inMap = [false, true, false, true, false]
  // Values for key 2: [v1, v2] (only 2 values in stream)
  // Expected output positions: row 1 gets v1, row 3 gets v2
  // Without scatterBitmap: row 0 gets v1, row 1 gets v2 (WRONG!)

  const velox::vector_size_t numRows = 5;

  auto ids = velox::BaseVector::create(velox::BIGINT(), numRows, pool_.get());
  for (velox::vector_size_t i = 0; i < numRows; ++i) {
    ids->asFlatVector<int64_t>()->set(i, i * 100);
  }

  // Build map data: rows 0,2,4 have 1 key; rows 1,3 have 2 keys.
  // Total entries: 3*1 + 2*2 = 7
  std::vector<int32_t> keysData;
  std::vector<double> valuesData;
  std::vector<velox::vector_size_t> offsets;
  offsets.push_back(0);

  // Row 0: key 1 only
  keysData.push_back(1);
  valuesData.push_back(10.0);
  offsets.push_back(keysData.size());

  // Row 1: keys 1 and 2
  keysData.push_back(1);
  valuesData.push_back(11.0);
  keysData.push_back(2);
  valuesData.push_back(21.0); // This should end up at row 1 position for key 2
  offsets.push_back(keysData.size());

  // Row 2: key 1 only
  keysData.push_back(1);
  valuesData.push_back(12.0);
  offsets.push_back(keysData.size());

  // Row 3: keys 1 and 2
  keysData.push_back(1);
  valuesData.push_back(13.0);
  keysData.push_back(2);
  valuesData.push_back(23.0); // This should end up at row 3 position for key 2
  offsets.push_back(keysData.size());

  // Row 4: key 1 only
  keysData.push_back(1);
  valuesData.push_back(14.0);
  offsets.push_back(keysData.size());

  const auto totalEntries = static_cast<velox::vector_size_t>(keysData.size());

  auto keysFlat =
      velox::BaseVector::create(velox::INTEGER(), totalEntries, pool_.get());
  auto valuesFlat =
      velox::BaseVector::create(velox::DOUBLE(), totalEntries, pool_.get());

  for (velox::vector_size_t i = 0; i < totalEntries; ++i) {
    keysFlat->asFlatVector<int32_t>()->set(i, keysData[i]);
    valuesFlat->asFlatVector<double>()->set(i, valuesData[i]);
  }

  auto mapVector = std::make_shared<velox::MapVector>(
      pool_.get(),
      velox::MAP(velox::INTEGER(), velox::DOUBLE()),
      nullptr,
      numRows,
      velox::allocateOffsets(numRows, pool_.get()),
      velox::allocateSizes(numRows, pool_.get()),
      keysFlat,
      valuesFlat);

  auto* rawOffsets =
      mapVector->mutableOffsets(numRows)->asMutable<velox::vector_size_t>();
  auto* rawSizes =
      mapVector->mutableSizes(numRows)->asMutable<velox::vector_size_t>();
  for (velox::vector_size_t i = 0; i < numRows; ++i) {
    rawOffsets[i] = offsets[i];
    rawSizes[i] = offsets[i + 1] - offsets[i];
  }

  auto input = std::make_shared<velox::RowVector>(
      pool_.get(),
      type,
      nullptr,
      numRows,
      std::vector<velox::VectorPtr>{ids, mapVector});

  // Serialize with FlatMap encoding
  const SerializerOptions options{
      .compressionType = CompressionType::Zstd,
      .compressionThreshold = 32,
      .compressionLevel = 3,
      .version = version(),
      .flatMapColumns = {"features"},
  };
  Serializer serializer{options, type, pool_.get()};
  auto serialized =
      serializer.serialize(input, OrderedRanges::of(0, input->size()));

  // Deserialize
  Deserializer deserializer{
      SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes()),
      pool_.get(),
      DeserializerOptions{.version = version()}};

  velox::VectorPtr output;
  deserializer.deserialize(serialized, output);

  // Verify output matches input
  ASSERT_EQ(output->size(), input->size());
  for (velox::vector_size_t i = 0; i < numRows; ++i) {
    ASSERT_TRUE(vectorEquals(output, input, i))
        << "Content mismatch at row " << i
        << "\nExpected: " << input->toString(i)
        << "\nActual: " << output->toString(i)
        << "\nThis may indicate scatterBitmap is not being handled correctly.";
  }
}

TEST_P(SerializationTest, versionMismatch) {
  // Test that deserializing with wrong version fails with a clear error.
  auto type = velox::ROW({
      {"int_val", velox::INTEGER()},
      {"string_val", velox::VARCHAR()},
  });

  velox::VectorFuzzer fuzzer(
      {
          .vectorSize = 10,
          .nullRatio = 0,
          .stringLength = 10,
      },
      pool_.get());

  auto input = fuzzer.fuzzInputRow(
      std::dynamic_pointer_cast<const velox::RowType>(type));

  // Serialize with dense format (no version header).
  SerializerOptions serializerOptions{};
  Serializer serializer{serializerOptions, type, pool_.get()};
  auto serialized =
      serializer.serialize(input, OrderedRanges::of(0, input->size()));

  // Try to deserialize with sparse format (expects version header).
  // The first byte is rowCount (not version), so it will be read as version.
  // If rowCount > 1 (kSparse), it will fail the version check.
  Deserializer deserializer{
      SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes()),
      pool_.get(),
      DeserializerOptions{.version = SerializationVersion::kSparse}};

  velox::VectorPtr output;
  // The version check should fail because the first byte (part of rowCount)
  // is likely > 1, which exceeds the maximum supported version (kSparse = 1).
  NIMBLE_ASSERT_THROW(
      deserializer.deserialize(serialized, output), "Unsupported version");
}

// Test encoding layout tree for non-FlatMap types.
TEST_P(SerializationTest, encodingLayoutTree) {
  // ROW type with scalar, string, and nested Map(Array(Array(Int))) children.
  auto type = velox::ROW({
      {"int_val", velox::INTEGER()},
      {"double_val", velox::DOUBLE()},
      {"string_val", velox::VARCHAR()},
      {"nested_map",
       velox::MAP(
           velox::VARCHAR(), velox::ARRAY(velox::ARRAY(velox::INTEGER())))},
  });

  // Create encoding layout tree with specific encodings.
  EncodingLayoutTree layoutTree{
      Kind::Row,
      {{EncodingLayoutTree::StreamIdentifiers::Row::NullsStream,
        EncodingLayout{
            EncodingType::SparseBool, {}, CompressionType::Uncompressed}}},
      "",
      {
          // int_val child
          {Kind::Scalar,
           {{EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream,
             EncodingLayout{
                 EncodingType::FixedBitWidth,
                 {},
                 CompressionType::Uncompressed}}},
           ""},
          // double_val child
          {Kind::Scalar,
           {{EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream,
             EncodingLayout{
                 EncodingType::Trivial, {}, CompressionType::Uncompressed}}},
           ""},
          // string_val child
          // Trivial encoding for strings needs a child encoding for lengths.
          {Kind::Scalar,
           {{EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream,
             EncodingLayout{
                 EncodingType::Trivial,
                 {},
                 CompressionType::Uncompressed,
                 {EncodingLayout{
                     EncodingType::Trivial,
                     {},
                     CompressionType::Uncompressed}}}}},
           ""},
          // nested_map: MAP(VARCHAR, ARRAY(ARRAY(INTEGER)))
          {Kind::Map,
           {{EncodingLayoutTree::StreamIdentifiers::Map::LengthsStream,
             EncodingLayout{
                 EncodingType::FixedBitWidth,
                 {},
                 CompressionType::Uncompressed}}},
           "",
           {
               // Key child (VARCHAR)
               // Trivial encoding for strings needs a child encoding for
               // lengths.
               {Kind::Scalar,
                {{EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream,
                  EncodingLayout{
                      EncodingType::Trivial,
                      {},
                      CompressionType::Uncompressed,
                      {EncodingLayout{
                          EncodingType::Trivial,
                          {},
                          CompressionType::Uncompressed}}}}},
                ""},
               // Value child: ARRAY(ARRAY(INTEGER))
               {Kind::Array,
                {{EncodingLayoutTree::StreamIdentifiers::Array::LengthsStream,
                  EncodingLayout{
                      EncodingType::FixedBitWidth,
                      {},
                      CompressionType::Uncompressed}}},
                "",
                {
                    // Inner ARRAY(INTEGER)
                    {Kind::Array,
                     {{EncodingLayoutTree::StreamIdentifiers::Array::
                           LengthsStream,
                       EncodingLayout{
                           EncodingType::FixedBitWidth,
                           {},
                           CompressionType::Uncompressed}}},
                     "",
                     {
                         // INTEGER element
                         {Kind::Scalar,
                          {{EncodingLayoutTree::StreamIdentifiers::Scalar::
                                ScalarStream,
                            EncodingLayout{
                                EncodingType::FixedBitWidth,
                                {},
                                CompressionType::Uncompressed}}},
                          ""},
                     }},
                }},
           }},
      }};

  SerializerOptions options{
      .version = version(),
      .compressionOptions = compressionOptions(),
      .encodingLayoutTree = layoutTree,
  };

  Serializer serializer{options, type, pool_.get()};

  // Generate test data.
  const velox::vector_size_t numRows = 5;

  // Pre-generate string values to avoid dangling StringViews.
  std::vector<std::string> stringData;
  for (velox::vector_size_t i = 0; i < numRows; ++i) {
    stringData.push_back("str" + std::to_string(i));
  }

  auto intVals =
      velox::BaseVector::create(velox::INTEGER(), numRows, pool_.get());
  auto doubleVals =
      velox::BaseVector::create(velox::DOUBLE(), numRows, pool_.get());
  auto stringVals =
      velox::BaseVector::create(velox::VARCHAR(), numRows, pool_.get());
  for (velox::vector_size_t i = 0; i < numRows; ++i) {
    intVals->asFlatVector<int32_t>()->set(i, i * 100);
    doubleVals->asFlatVector<double>()->set(i, i * 1.5);
    stringVals->asFlatVector<velox::StringView>()->set(
        i, velox::StringView(stringData[i]));
  }

  // Build nested_map: MAP(VARCHAR, ARRAY(ARRAY(INTEGER)))
  // Each row has 2 map entries, each entry has outer array of size 2,
  // each inner array has 3 integers.
  const velox::vector_size_t mapEntriesPerRow = 2;
  const velox::vector_size_t outerArraySize = 2;
  const velox::vector_size_t innerArraySize = 3;
  const velox::vector_size_t totalMapEntries = numRows * mapEntriesPerRow;
  const velox::vector_size_t totalOuterArrays =
      totalMapEntries * outerArraySize;
  const velox::vector_size_t totalInnerInts = totalOuterArrays * innerArraySize;

  // Build innermost integers
  auto innerInts =
      velox::BaseVector::create(velox::INTEGER(), totalInnerInts, pool_.get());
  for (velox::vector_size_t i = 0; i < totalInnerInts; ++i) {
    innerInts->asFlatVector<int32_t>()->set(i, i);
  }

  // Build inner arrays (ARRAY(INTEGER))
  auto innerArrayOffsets =
      velox::allocateOffsets(totalOuterArrays, pool_.get());
  auto innerArraySizes = velox::allocateSizes(totalOuterArrays, pool_.get());
  auto* innerOffsets = innerArrayOffsets->asMutable<velox::vector_size_t>();
  auto* innerSizes = innerArraySizes->asMutable<velox::vector_size_t>();
  for (velox::vector_size_t i = 0; i < totalOuterArrays; ++i) {
    innerOffsets[i] = i * innerArraySize;
    innerSizes[i] = innerArraySize;
  }
  auto innerArrayVector = std::make_shared<velox::ArrayVector>(
      pool_.get(),
      velox::ARRAY(velox::INTEGER()),
      nullptr,
      totalOuterArrays,
      innerArrayOffsets,
      innerArraySizes,
      innerInts);

  // Build outer arrays (ARRAY(ARRAY(INTEGER)))
  auto outerArrayOffsets = velox::allocateOffsets(totalMapEntries, pool_.get());
  auto outerArraySizes = velox::allocateSizes(totalMapEntries, pool_.get());
  auto* outerOffsets = outerArrayOffsets->asMutable<velox::vector_size_t>();
  auto* outerSizes = outerArraySizes->asMutable<velox::vector_size_t>();
  for (velox::vector_size_t i = 0; i < totalMapEntries; ++i) {
    outerOffsets[i] = i * outerArraySize;
    outerSizes[i] = outerArraySize;
  }
  auto outerArrayVector = std::make_shared<velox::ArrayVector>(
      pool_.get(),
      velox::ARRAY(velox::ARRAY(velox::INTEGER())),
      nullptr,
      totalMapEntries,
      outerArrayOffsets,
      outerArraySizes,
      innerArrayVector);

  // Build map keys (VARCHAR)
  std::vector<std::string> mapKeyData;
  for (velox::vector_size_t i = 0; i < totalMapEntries; ++i) {
    mapKeyData.push_back("key" + std::to_string(i));
  }
  auto mapKeys =
      velox::BaseVector::create(velox::VARCHAR(), totalMapEntries, pool_.get());
  for (velox::vector_size_t i = 0; i < totalMapEntries; ++i) {
    mapKeys->asFlatVector<velox::StringView>()->set(
        i, velox::StringView(mapKeyData[i]));
  }

  // Build map vector
  auto mapOffsets = velox::allocateOffsets(numRows, pool_.get());
  auto mapSizes = velox::allocateSizes(numRows, pool_.get());
  auto* rawMapOffsets = mapOffsets->asMutable<velox::vector_size_t>();
  auto* rawMapSizes = mapSizes->asMutable<velox::vector_size_t>();
  for (velox::vector_size_t i = 0; i < numRows; ++i) {
    rawMapOffsets[i] = i * mapEntriesPerRow;
    rawMapSizes[i] = mapEntriesPerRow;
  }
  auto mapVector = std::make_shared<velox::MapVector>(
      pool_.get(),
      velox::MAP(
          velox::VARCHAR(), velox::ARRAY(velox::ARRAY(velox::INTEGER()))),
      nullptr,
      numRows,
      mapOffsets,
      mapSizes,
      mapKeys,
      outerArrayVector);

  auto input = std::make_shared<velox::RowVector>(
      pool_.get(),
      type,
      nullptr,
      numRows,
      std::vector<velox::VectorPtr>{
          intVals, doubleVals, stringVals, mapVector});

  // Serialize.
  auto serialized =
      serializer.serialize(input, OrderedRanges::of(0, input->size()));

  // Deserialize and verify.
  DeserializerOptions deserializerOptions{.version = version()};
  Deserializer deserializer{
      SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes()),
      pool_.get(),
      deserializerOptions};

  velox::VectorPtr output;
  deserializer.deserialize(serialized, output);

  ASSERT_EQ(output->size(), input->size());
  for (velox::vector_size_t i = 0; i < input->size(); ++i) {
    ASSERT_TRUE(vectorEquals(output, input, i))
        << "Content mismatch at index " << i;
  }
}

// Test encoding layout tree for FlatMap with dynamic key discovery and nested
// types.
TEST_P(SerializationTest, encodingLayoutTreeFlatMap) {
  // Use nested types to test:
  // - features: MAP(INTEGER, ARRAY(DOUBLE)) - FlatMap with nested array values
  // - tags: MAP(VARCHAR, DOUBLE) - FlatMap with string keys and scalar values
  // - metadata: MAP(INTEGER, VARCHAR) - Regular Map (not FlatMap)
  const auto type = velox::ROW({
      {"id", velox::BIGINT()},
      {"features", velox::MAP(velox::INTEGER(), velox::ARRAY(velox::DOUBLE()))},
      {"tags", velox::MAP(velox::VARCHAR(), velox::DOUBLE())},
      {"metadata", velox::MAP(velox::INTEGER(), velox::VARCHAR())},
  });

  // Create encoding layout tree with FlatMap encodings.
  // The FlatMap children are keyed by the key value as string.
  EncodingLayoutTree layoutTree{
      Kind::Row,
      {{EncodingLayoutTree::StreamIdentifiers::Row::NullsStream,
        EncodingLayout{
            EncodingType::SparseBool, {}, CompressionType::Uncompressed}}},
      "",
      {
          // id child (scalar)
          {Kind::Scalar,
           {{EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream,
             EncodingLayout{
                 EncodingType::FixedBitWidth,
                 {},
                 CompressionType::Uncompressed}}},
           ""},
          // features child (FlatMap with ARRAY(DOUBLE) values)
          {Kind::FlatMap,
           {{EncodingLayoutTree::StreamIdentifiers::FlatMap::NullsStream,
             EncodingLayout{
                 EncodingType::SparseBool, {}, CompressionType::Uncompressed}}},
           "",
           {
               // Key "1" - ARRAY(DOUBLE) value encoding
               {Kind::Array,
                {{EncodingLayoutTree::StreamIdentifiers::Array::LengthsStream,
                  EncodingLayout{
                      EncodingType::FixedBitWidth,
                      {},
                      CompressionType::Uncompressed}}},
                "1",
                {
                    // Array element (DOUBLE)
                    {Kind::Scalar,
                     {{EncodingLayoutTree::StreamIdentifiers::Scalar::
                           ScalarStream,
                       EncodingLayout{
                           EncodingType::Trivial,
                           {},
                           CompressionType::Uncompressed}}},
                     ""},
                }},
               // Key "2" - ARRAY(DOUBLE) value encoding
               {Kind::Array,
                {{EncodingLayoutTree::StreamIdentifiers::Array::LengthsStream,
                  EncodingLayout{
                      EncodingType::FixedBitWidth,
                      {},
                      CompressionType::Uncompressed}}},
                "2",
                {
                    // Array element (DOUBLE)
                    {Kind::Scalar,
                     {{EncodingLayoutTree::StreamIdentifiers::Scalar::
                           ScalarStream,
                       EncodingLayout{
                           EncodingType::FixedBitWidth,
                           {},
                           CompressionType::Uncompressed}}},
                     ""},
                }},
           }},
          // tags child (FlatMap with VARCHAR keys and DOUBLE values)
          {Kind::FlatMap,
           {{EncodingLayoutTree::StreamIdentifiers::FlatMap::NullsStream,
             EncodingLayout{
                 EncodingType::SparseBool, {}, CompressionType::Uncompressed}}},
           "",
           {
               // Key "tag_a" - DOUBLE value encoding
               {Kind::Scalar,
                {{EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream,
                  EncodingLayout{
                      EncodingType::Trivial,
                      {},
                      CompressionType::Uncompressed}}},
                "tag_a"},
               // Key "tag_b" - DOUBLE value encoding
               {Kind::Scalar,
                {{EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream,
                  EncodingLayout{
                      EncodingType::FixedBitWidth,
                      {},
                      CompressionType::Uncompressed}}},
                "tag_b"},
           }},
          // metadata child (regular Map, not FlatMap)
          {Kind::Map,
           {{EncodingLayoutTree::StreamIdentifiers::Map::LengthsStream,
             EncodingLayout{
                 EncodingType::FixedBitWidth,
                 {},
                 CompressionType::Uncompressed}}},
           "",
           {
               // Key child (INTEGER)
               {Kind::Scalar,
                {{EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream,
                  EncodingLayout{
                      EncodingType::FixedBitWidth,
                      {},
                      CompressionType::Uncompressed}}},
                ""},
               // Value child (VARCHAR)
               // Trivial encoding for strings needs a child encoding for
               // lengths.
               {Kind::Scalar,
                {{EncodingLayoutTree::StreamIdentifiers::Scalar::ScalarStream,
                  EncodingLayout{
                      EncodingType::Trivial,
                      {},
                      CompressionType::Uncompressed,
                      {EncodingLayout{
                          EncodingType::Trivial,
                          {},
                          CompressionType::Uncompressed}}}}},
                ""},
           }},
      }};

  SerializerOptions options{
      .version = version(),
      .compressionOptions = compressionOptions(),
      .flatMapColumns = {"features", "tags"},
      .encodingLayoutTree = layoutTree,
  };

  Serializer serializer{options, type, pool_.get()};

  // Generate test data with FlatMap keys 1 and 2, each with ARRAY(DOUBLE)
  // values.
  const velox::vector_size_t numRows = 5;

  auto ids = velox::BaseVector::create(velox::BIGINT(), numRows, pool_.get());
  for (velox::vector_size_t i = 0; i < numRows; ++i) {
    ids->asFlatVector<int64_t>()->set(i, i * 100);
  }

  // Build map: each row has keys 1 and 2, each with an array of 2 doubles.
  // Map structure: key -> ARRAY(DOUBLE)
  const velox::vector_size_t entriesPerRow = 2; // keys 1 and 2
  const velox::vector_size_t elementsPerArray = 2;
  const velox::vector_size_t totalMapEntries = numRows * entriesPerRow;
  const velox::vector_size_t totalArrayElements =
      totalMapEntries * elementsPerArray;

  // Build map keys
  auto mapKeys =
      velox::BaseVector::create(velox::INTEGER(), totalMapEntries, pool_.get());
  for (velox::vector_size_t i = 0; i < numRows; ++i) {
    mapKeys->asFlatVector<int32_t>()->set(i * 2, 1); // key 1
    mapKeys->asFlatVector<int32_t>()->set(i * 2 + 1, 2); // key 2
  }

  // Build array elements (doubles)
  auto arrayElements = velox::BaseVector::create(
      velox::DOUBLE(), totalArrayElements, pool_.get());
  for (velox::vector_size_t i = 0; i < totalArrayElements; ++i) {
    arrayElements->asFlatVector<double>()->set(i, i * 1.5);
  }

  // Build array offsets and sizes (each array has elementsPerArray elements)
  auto arrayOffsetsBuffer =
      velox::allocateOffsets(totalMapEntries, pool_.get());
  auto arraySizesBuffer = velox::allocateSizes(totalMapEntries, pool_.get());
  auto* arrayOffsets = arrayOffsetsBuffer->asMutable<velox::vector_size_t>();
  auto* arraySizes = arraySizesBuffer->asMutable<velox::vector_size_t>();
  for (velox::vector_size_t i = 0; i < totalMapEntries; ++i) {
    arrayOffsets[i] = i * elementsPerArray;
    arraySizes[i] = elementsPerArray;
  }

  // Build array vector for map values
  auto mapValues = std::make_shared<velox::ArrayVector>(
      pool_.get(),
      velox::ARRAY(velox::DOUBLE()),
      nullptr,
      totalMapEntries,
      arrayOffsetsBuffer,
      arraySizesBuffer,
      arrayElements);

  // Build map offsets and sizes (each row has entriesPerRow map entries)
  auto mapOffsetsBuffer = velox::allocateOffsets(numRows, pool_.get());
  auto mapSizesBuffer = velox::allocateSizes(numRows, pool_.get());
  auto* mapOffsets = mapOffsetsBuffer->asMutable<velox::vector_size_t>();
  auto* mapSizes = mapSizesBuffer->asMutable<velox::vector_size_t>();
  for (velox::vector_size_t i = 0; i < numRows; ++i) {
    mapOffsets[i] = i * entriesPerRow;
    mapSizes[i] = entriesPerRow;
  }

  auto featuresMap = std::make_shared<velox::MapVector>(
      pool_.get(),
      velox::MAP(velox::INTEGER(), velox::ARRAY(velox::DOUBLE())),
      nullptr,
      numRows,
      mapOffsetsBuffer,
      mapSizesBuffer,
      mapKeys,
      mapValues);

  // Build tags FlatMap: MAP(VARCHAR, DOUBLE) with keys "tag_a" and "tag_b"
  std::vector<std::string> tagKeyStrings;
  tagKeyStrings.reserve(totalMapEntries);
  for (velox::vector_size_t i = 0; i < numRows; ++i) {
    tagKeyStrings.push_back("tag_a");
    tagKeyStrings.push_back("tag_b");
  }
  auto tagKeys =
      velox::BaseVector::create(velox::VARCHAR(), totalMapEntries, pool_.get());
  for (size_t i = 0; i < tagKeyStrings.size(); ++i) {
    tagKeys->asFlatVector<velox::StringView>()->set(
        i, velox::StringView(tagKeyStrings[i]));
  }
  auto tagValues =
      velox::BaseVector::create(velox::DOUBLE(), totalMapEntries, pool_.get());
  for (velox::vector_size_t i = 0; i < totalMapEntries; ++i) {
    tagValues->asFlatVector<double>()->set(i, i * 2.5);
  }
  auto tagOffsetsBuffer = velox::allocateOffsets(numRows, pool_.get());
  auto tagSizesBuffer = velox::allocateSizes(numRows, pool_.get());
  auto* tagOffsets = tagOffsetsBuffer->asMutable<velox::vector_size_t>();
  auto* tagSizes = tagSizesBuffer->asMutable<velox::vector_size_t>();
  for (velox::vector_size_t i = 0; i < numRows; ++i) {
    tagOffsets[i] = i * entriesPerRow;
    tagSizes[i] = entriesPerRow;
  }
  auto tagsMap = std::make_shared<velox::MapVector>(
      pool_.get(),
      velox::MAP(velox::VARCHAR(), velox::DOUBLE()),
      nullptr,
      numRows,
      tagOffsetsBuffer,
      tagSizesBuffer,
      tagKeys,
      tagValues);

  // Build metadata regular Map: MAP(INTEGER, VARCHAR)
  std::vector<std::string> metaValueStrings;
  metaValueStrings.reserve(totalMapEntries);
  for (velox::vector_size_t i = 0; i < totalMapEntries; ++i) {
    metaValueStrings.push_back("meta_" + std::to_string(i));
  }
  auto metaKeys =
      velox::BaseVector::create(velox::INTEGER(), totalMapEntries, pool_.get());
  auto metaValues =
      velox::BaseVector::create(velox::VARCHAR(), totalMapEntries, pool_.get());
  for (velox::vector_size_t i = 0; i < totalMapEntries; ++i) {
    metaKeys->asFlatVector<int32_t>()->set(i, i);
    metaValues->asFlatVector<velox::StringView>()->set(
        i, velox::StringView(metaValueStrings[i]));
  }
  auto metaOffsetsBuffer = velox::allocateOffsets(numRows, pool_.get());
  auto metaSizesBuffer = velox::allocateSizes(numRows, pool_.get());
  auto* metaOffsets = metaOffsetsBuffer->asMutable<velox::vector_size_t>();
  auto* metaSizes = metaSizesBuffer->asMutable<velox::vector_size_t>();
  for (velox::vector_size_t i = 0; i < numRows; ++i) {
    metaOffsets[i] = i * entriesPerRow;
    metaSizes[i] = entriesPerRow;
  }
  auto metadataMap = std::make_shared<velox::MapVector>(
      pool_.get(),
      velox::MAP(velox::INTEGER(), velox::VARCHAR()),
      nullptr,
      numRows,
      metaOffsetsBuffer,
      metaSizesBuffer,
      metaKeys,
      metaValues);

  auto input = std::make_shared<velox::RowVector>(
      pool_.get(),
      type,
      nullptr,
      numRows,
      std::vector<velox::VectorPtr>{ids, featuresMap, tagsMap, metadataMap});

  // Serialize.
  auto serialized =
      serializer.serialize(input, OrderedRanges::of(0, input->size()));

  // Deserialize and verify.
  DeserializerOptions deserializerOptions{.version = version()};
  Deserializer deserializer{
      SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes()),
      pool_.get(),
      deserializerOptions};

  velox::VectorPtr output;
  deserializer.deserialize(serialized, output);

  ASSERT_EQ(output->size(), input->size());
  for (velox::vector_size_t i = 0; i < input->size(); ++i) {
    ASSERT_TRUE(vectorEquals(output, input, i))
        << "Content mismatch at index " << i;
  }
}

// Test that wrong encoding layout tree kind throws an error.
TEST_P(SerializationTest, encodingLayoutTreeWrongKind) {
  // Test: Pass Row layout for a Scalar column.
  auto type = velox::ROW({{"int_val", velox::INTEGER()}});

  // Create encoding layout tree with wrong kind (Row instead of Scalar).
  EncodingLayoutTree layoutTree{
      Kind::Row,
      {{EncodingLayoutTree::StreamIdentifiers::Row::NullsStream,
        EncodingLayout{
            EncodingType::SparseBool, {}, CompressionType::Uncompressed}}},
      "",
      {
          // int_val child - WRONG: using Row layout instead of Scalar
          {Kind::Row,
           {{EncodingLayoutTree::StreamIdentifiers::Row::NullsStream,
             EncodingLayout{
                 EncodingType::SparseBool, {}, CompressionType::Uncompressed}}},
           ""},
      }};

  SerializerOptions options{
      .version = version(),
      .encodingLayoutTree = layoutTree,
  };

  NIMBLE_ASSERT_THROW(
      Serializer(options, type, pool_.get()),
      "Incompatible encoding layout node. Expecting scalar node.");
}

// Test that FlatMap vs Map mismatch throws an error.
TEST_P(SerializationTest, encodingLayoutTreeFlatMapForMap) {
  // Test: Pass FlatMap layout for a Map column.
  auto type =
      velox::ROW({{"map_val", velox::MAP(velox::INTEGER(), velox::DOUBLE())}});

  // Create encoding layout tree with FlatMap kind for a Map column.
  EncodingLayoutTree layoutTree{
      Kind::Row,
      {{EncodingLayoutTree::StreamIdentifiers::Row::NullsStream,
        EncodingLayout{
            EncodingType::SparseBool, {}, CompressionType::Uncompressed}}},
      "",
      {
          // map_val child - WRONG: using FlatMap layout for Map column
          {Kind::FlatMap,
           {{EncodingLayoutTree::StreamIdentifiers::FlatMap::NullsStream,
             EncodingLayout{
                 EncodingType::SparseBool, {}, CompressionType::Uncompressed}}},
           ""},
      }};

  SerializerOptions options{
      .version = version(),
      .encodingLayoutTree = layoutTree,
  };

  NIMBLE_ASSERT_THROW(
      Serializer(options, type, pool_.get()),
      "Incompatible encoding layout node. Expecting map node.");
}

// Test that Map vs FlatMap mismatch throws an error.
TEST_P(SerializationTest, encodingLayoutTreeMapForFlatMap) {
  // Test: Pass Map layout for a FlatMap column.
  const auto type =
      velox::ROW({{"map_val", velox::MAP(velox::INTEGER(), velox::DOUBLE())}});

  // Create encoding layout tree with Map kind for a FlatMap column.
  EncodingLayoutTree layoutTree{
      Kind::Row,
      {{EncodingLayoutTree::StreamIdentifiers::Row::NullsStream,
        EncodingLayout{
            EncodingType::SparseBool, {}, CompressionType::Uncompressed}}},
      "",
      {
          // map_val child - WRONG: using Map layout for FlatMap column
          {Kind::Map,
           {{EncodingLayoutTree::StreamIdentifiers::Map::LengthsStream,
             EncodingLayout{
                 EncodingType::FixedBitWidth,
                 {},
                 CompressionType::Uncompressed}}},
           ""},
      }};

  SerializerOptions options{
      .version = version(),
      .flatMapColumns = {"map_val"},
      .encodingLayoutTree = layoutTree,
  };

  NIMBLE_ASSERT_THROW(
      Serializer(options, type, pool_.get()),
      "Incompatible encoding layout node. Expecting flatmap node.");
}

INSTANTIATE_TEST_SUITE_P(
    AllFormats,
    SerializationTest,
    ::testing::Values(
        // Non-encoding modes (compression options don't apply).
        TestParams{.version = std::nullopt}, // Dense format
        TestParams{.version = SerializationVersion::kSparse},
        // Encoding modes without compression.
        TestParams{.version = SerializationVersion::kDenseEncoded},
        TestParams{.version = SerializationVersion::kSparseEncoded},
        // Encoding modes with compression enabled (for encodingLayoutTree
        // tests).
        TestParams{
            .version = SerializationVersion::kDenseEncoded,
            .compressionOptions =
                {.compressionAcceptRatio = 1.0f, .zstdMinCompressionSize = 0},
            .compressionEnabled = true},
        TestParams{
            .version = SerializationVersion::kSparseEncoded,
            .compressionOptions =
                {.compressionAcceptRatio = 1.0f, .zstdMinCompressionSize = 0},
            .compressionEnabled = true}),
    formatName);
