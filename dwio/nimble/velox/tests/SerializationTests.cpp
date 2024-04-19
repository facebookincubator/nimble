// Copyright 2004-present Facebook. All Rights Reserved.

#include <gtest/gtest.h>

#include "dwio/nimble/velox/Deserializer.h"
#include "dwio/nimble/velox/Serializer.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/NullsBuilder.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace facebook;
using namespace facebook::nimble;

class SerializationTests : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    rootPool_ = velox::memory::memoryManager()->addRootPool("default_root");
    pool_ = velox::memory::memoryManager()->addLeafPool("default_leaf");
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
      velox::memory::MemoryPool& pool,
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
    };
    Serializer serializer{options, *rootPool_, type};
    Deserializer deserializer{
        pool,
        SchemaReader::getSchema(serializer.schemaBuilder().getSchemaNodes())};

    velox::VectorPtr output;
    for (auto i = 0; i < count; ++i) {
      auto input = generator(type);
      auto serialized =
          serializer.serialize(input, OrderedRanges::of(0, input->size()));
      deserializer.deserialize(serialized, output);

      ASSERT_EQ(output->size(), input->size());
      for (auto j = 0; j < input->size(); ++j) {
        ASSERT_TRUE(validator(output, input, j))
            << "Content mismatch at index " << j
            << "\nReference: " << input->toString(j)
            << "\nResult: " << output->toString(j);
      }
    }
  }
};

TEST_F(SerializationTests, FuzzSimple) {
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

  auto iterations = 20;
  auto batches = 20;
  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify(
        *pool_,
        type,
        [&](auto& type) {
          return noNulls.fuzzInputRow(
              std::dynamic_pointer_cast<const velox::RowType>(type));
        },
        vectorEquals,
        batches);
  }
}

TEST_F(SerializationTests, FuzzComplex) {
  auto type = velox::ROW({
      {"array", velox::ARRAY(velox::REAL())},
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

  auto iterations = 20;
  auto batches = 20;
  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify(
        *pool_.get(),
        type,
        [&](auto& type) {
          return noNulls.fuzzInputRow(
              std::dynamic_pointer_cast<const velox::RowType>(type));
        },
        vectorEquals,
        batches);
  }
}

TEST_F(SerializationTests, RootNotRow) {
  auto type = velox::MAP(velox::INTEGER(), velox::ARRAY(velox::DOUBLE()));
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

  auto iterations = 20;
  auto batches = 20;
  for (auto i = 0; i < iterations; ++i) {
    writeAndVerify(
        *pool_.get(),
        type,
        [&](auto& type) { return noNulls.fuzz(type); },
        vectorEquals,
        batches);
  }
}
