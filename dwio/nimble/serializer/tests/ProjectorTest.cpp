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

#include <gtest/gtest.h>

#include <optional>
#include <utility>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/serializer/Deserializer.h"
#include "dwio/nimble/serializer/Projector.h"
#include "dwio/nimble/serializer/Serializer.h"
#include "dwio/nimble/velox/SchemaBuilder.h"
#include "velox/type/Subfield.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

namespace facebook::nimble::serde {

// Test parameter: input version (nullopt = no version header) and output
// version.
struct FormatParam {
  std::optional<SerializationVersion> inputVersion;
  SerializationVersion outputVersion;

  // For test naming.
  std::string name() const {
    std::string inputName;
    if (!inputVersion.has_value()) {
      inputName = "NoVersion";
    } else {
      switch (inputVersion.value()) {
        case SerializationVersion::kDense:
          inputName = "Dense";
          break;
        case SerializationVersion::kDenseEncoded:
          inputName = "DenseEncoded";
          break;
        case SerializationVersion::kSparse:
          inputName = "Sparse";
          break;
        case SerializationVersion::kSparseEncoded:
          inputName = "SparseEncoded";
          break;
      }
    }

    std::string outputName;
    switch (outputVersion) {
      case SerializationVersion::kDense:
        outputName = "Dense";
        break;
      case SerializationVersion::kDenseEncoded:
        outputName = "DenseEncoded";
        break;
      case SerializationVersion::kSparse:
        outputName = "Sparse";
        break;
      case SerializationVersion::kSparseEncoded:
        outputName = "SparseEncoded";
        break;
    }

    return inputName + "_to_" + outputName;
  }
};

// Check if input/output format combination is compatible.
// Projector copies raw bytes, so encoding type must match:
// - kDense/kSparse use legacy raw encoding
// - kDenseEncoded/kSparseEncoded use nimble encoding
// - No-version (legacy) uses raw encoding
bool isCompatibleFormat(
    std::optional<SerializationVersion> input,
    SerializationVersion output) {
  // No-version (legacy) is raw encoding, compatible with kDense/kSparse.
  if (!input.has_value()) {
    return output == SerializationVersion::kDense ||
        output == SerializationVersion::kSparse;
  }

  const bool inputEncoded =
      (input.value() == SerializationVersion::kDenseEncoded ||
       input.value() == SerializationVersion::kSparseEncoded);
  const bool outputEncoded =
      (output == SerializationVersion::kDenseEncoded ||
       output == SerializationVersion::kSparseEncoded);

  return inputEncoded == outputEncoded;
}

// Generate compatible format combinations.
std::vector<FormatParam> allFormatCombinations() {
  std::vector<FormatParam> params;

  // Input versions: 4 with version header + 1 without (legacy dense).
  std::vector<std::optional<SerializationVersion>> inputVersions = {
      SerializationVersion::kDense,
      SerializationVersion::kDenseEncoded,
      SerializationVersion::kSparse,
      SerializationVersion::kSparseEncoded,
      std::nullopt, // No version header (legacy dense format).
  };

  // Output versions: always has version header.
  std::vector<SerializationVersion> outputVersions = {
      SerializationVersion::kDense,
      SerializationVersion::kDenseEncoded,
      SerializationVersion::kSparse,
      SerializationVersion::kSparseEncoded,
  };

  for (const auto& input : inputVersions) {
    for (const auto& output : outputVersions) {
      if (isCompatibleFormat(input, output)) {
        params.push_back({input, output});
      }
    }
  }

  return params;
}

// Base test fixture with helper methods.
class ProjectorTestBase : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    rootPool_ = memory::memoryManager()->addRootPool("projector_test_root");
    pool_ = memory::memoryManager()->addLeafPool("projector_test_leaf");
  }

  // Serialize a vector.
  std::string serialize(
      const VectorPtr& vec,
      const TypePtr& type,
      SerializerOptions options = SerializerOptions{}) {
    Serializer serializer{std::move(options), type, pool_.get()};
    auto sv = serializer.serialize(vec, OrderedRanges::of(0, vec->size()));
    return std::string(sv);
  }

  // Serialize a vector and return both data and schema.
  // Use this for FlatMap tests where keys are discovered during serialization.
  std::pair<std::string, std::shared_ptr<const nimble::Type>>
  serializeWithSchema(
      const VectorPtr& vec,
      const TypePtr& type,
      SerializerOptions options = SerializerOptions{}) {
    Serializer serializer{std::move(options), type, pool_.get()};
    auto sv = serializer.serialize(vec, OrderedRanges::of(0, vec->size()));
    auto schema =
        SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes());
    return {std::string(sv), schema};
  }

  // Get the nimble schema from a serializer (before serialization).
  // Note: For FlatMap, use serializeWithSchema() to get schema after
  // serialization when keys are discovered.
  std::shared_ptr<const nimble::Type> getNimbleSchema(
      const TypePtr& type,
      SerializerOptions options = SerializerOptions{}) {
    Serializer serializer{std::move(options), type, pool_.get()};
    return SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes());
  }

  // Deserialize a projected buffer with the projected schema.
  VectorPtr deserialize(
      std::string_view data,
      std::shared_ptr<const nimble::Type> nimbleSchema,
      DeserializerOptions options = DeserializerOptions{}) {
    Deserializer deserializer{nimbleSchema, pool_.get(), options};
    VectorPtr output;
    deserializer.deserialize({data}, output);
    return output;
  }

  // Create a simple row vector with scalar columns.
  RowVectorPtr makeSimpleRowVector(
      const std::vector<std::string>& names,
      const std::vector<VectorPtr>& children) {
    return std::make_shared<RowVector>(
        pool_.get(),
        ROW(std::vector<std::string>(names), extractTypes(children)),
        nullptr,
        children[0]->size(),
        children);
  }

  // Extract types from vectors.
  std::vector<TypePtr> extractTypes(const std::vector<VectorPtr>& vectors) {
    std::vector<TypePtr> types;
    types.reserve(vectors.size());
    for (const auto& v : vectors) {
      types.push_back(v->type());
    }
    return types;
  }

  // Create a flat vector of integers.
  template <typename T>
  FlatVectorPtr<T> makeIntVector(const std::vector<T>& values) {
    auto vector = BaseVector::create<FlatVector<T>>(
        CppToType<T>::create(), values.size(), pool_.get());
    for (size_t i = 0; i < values.size(); ++i) {
      vector->set(i, values[i]);
    }
    return vector;
  }

  // Create a flat vector of strings.
  FlatVectorPtr<StringView> makeStringVector(
      const std::vector<std::string>& values) {
    auto vector = BaseVector::create<FlatVector<StringView>>(
        VARCHAR(), values.size(), pool_.get());
    for (size_t i = 0; i < values.size(); ++i) {
      vector->set(i, StringView(values[i]));
    }
    return vector;
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

// Helper to create a vector of subfields.
std::vector<common::Subfield> makeSubfields(
    std::initializer_list<const char*> paths) {
  std::vector<common::Subfield> subfields;
  subfields.reserve(paths.size());
  for (const auto* path : paths) {
    subfields.emplace_back(path);
  }
  return subfields;
}

// Parameterized test fixture for format combinations.
class ProjectorFormatTest : public ProjectorTestBase,
                            public ::testing::WithParamInterface<FormatParam> {
 protected:
  // Get serializer options for input format.
  SerializerOptions inputSerializerOptions() const {
    const auto& param = GetParam();
    if (!param.inputVersion.has_value()) {
      // No version header - use default (legacy dense).
      return SerializerOptions{};
    }
    return SerializerOptions{.version = param.inputVersion.value()};
  }

  // Get projector options.
  Projector::Options projectorOptions() const {
    const auto& param = GetParam();
    return Projector::Options{
        .inputHasVersionHeader = param.inputVersion.has_value(),
        .projectVersion = param.outputVersion,
    };
  }

  // Get deserializer options for output format.
  DeserializerOptions outputDeserializerOptions() const {
    const auto& param = GetParam();
    return {.version = param.outputVersion};
  }
};

// Test projecting a single column from a multi-column row.
TEST_P(ProjectorFormatTest, projectSingleColumn) {
  auto type = ROW({
      {"a", INTEGER()},
      {"b", BIGINT()},
      {"c", VARCHAR()},
  });

  auto vec = makeSimpleRowVector(
      {"a", "b", "c"},
      {
          makeIntVector<int32_t>({1, 2, 3}),
          makeIntVector<int64_t>({100, 200, 300}),
          makeStringVector({"x", "y", "z"}),
      });

  auto serialized = serialize(vec, type, inputSerializerOptions());
  auto inputSchema = getNimbleSchema(type, inputSerializerOptions());

  // Project only column "b".
  auto subfields = makeSubfields({"b"});
  Projector projector(inputSchema, subfields, projectorOptions());
  auto outputSchema = projector.projectedSchema();

  // Verify output schema has only one column.
  ASSERT_TRUE(outputSchema->isRow());
  ASSERT_EQ(outputSchema->asRow().childrenCount(), 1);
  ASSERT_EQ(outputSchema->asRow().nameAt(0), "b");

  // Helper to verify result.
  auto verifyResult = [&](std::string_view projected) {
    auto result =
        deserialize(projected, outputSchema, outputDeserializerOptions());
    ASSERT_EQ(result->size(), 3);

    auto resultRow = result->as<RowVector>();
    auto bCol = resultRow->childAt(0)->as<FlatVector<int64_t>>();
    EXPECT_EQ(bCol->valueAt(0), 100);
    EXPECT_EQ(bCol->valueAt(1), 200);
    EXPECT_EQ(bCol->valueAt(2), 300);
  };

  // Test single projection.
  auto projected = projector.project(serialized);
  verifyResult(projected);

  // Test batch projection.
  std::vector<std::string_view> inputs = {serialized, serialized};
  auto batchResults = projector.project(inputs);
  ASSERT_EQ(batchResults.size(), 2);
  verifyResult(batchResults[0]);
  verifyResult(batchResults[1]);
}

// Test projecting multiple columns.
TEST_P(ProjectorFormatTest, projectMultipleColumns) {
  auto type = ROW({
      {"a", INTEGER()},
      {"b", BIGINT()},
      {"c", VARCHAR()},
      {"d", DOUBLE()},
  });

  auto vec = makeSimpleRowVector(
      {"a", "b", "c", "d"},
      {
          makeIntVector<int32_t>({1, 2}),
          makeIntVector<int64_t>({100, 200}),
          makeStringVector({"x", "y"}),
          BaseVector::create<FlatVector<double>>(DOUBLE(), 2, pool_.get()),
      });
  vec->childAt(3)->as<FlatVector<double>>()->set(0, 1.5);
  vec->childAt(3)->as<FlatVector<double>>()->set(1, 2.5);

  auto serialized = serialize(vec, type, inputSerializerOptions());
  auto inputSchema = getNimbleSchema(type, inputSerializerOptions());

  // Project columns "a" and "c".
  auto subfields = makeSubfields({"a", "c"});
  Projector projector(inputSchema, subfields, projectorOptions());
  auto outputSchema = projector.projectedSchema();

  ASSERT_EQ(outputSchema->asRow().childrenCount(), 2);
  ASSERT_EQ(outputSchema->asRow().nameAt(0), "a");
  ASSERT_EQ(outputSchema->asRow().nameAt(1), "c");

  // Helper to verify result.
  auto verifyResult = [&](std::string_view projected) {
    auto result =
        deserialize(projected, outputSchema, outputDeserializerOptions());
    ASSERT_EQ(result->size(), 2);

    auto resultRow = result->as<RowVector>();
    auto aCol = resultRow->childAt(0)->as<FlatVector<int32_t>>();
    EXPECT_EQ(aCol->valueAt(0), 1);
    EXPECT_EQ(aCol->valueAt(1), 2);

    auto cCol = resultRow->childAt(1)->as<FlatVector<StringView>>();
    EXPECT_EQ(cCol->valueAt(0).str(), "x");
    EXPECT_EQ(cCol->valueAt(1).str(), "y");
  };

  // Test single and batch projection.
  verifyResult(projector.project(serialized));

  auto batchResults = projector.project({serialized, serialized});
  ASSERT_EQ(batchResults.size(), 2);
  verifyResult(batchResults[0]);
  verifyResult(batchResults[1]);
}

// Test projecting nested struct fields.
TEST_P(ProjectorFormatTest, projectNestedField) {
  auto type = ROW({
      {"outer",
       ROW({
           {"inner1", INTEGER()},
           {"inner2", VARCHAR()},
       })},
      {"other", BIGINT()},
  });

  auto innerVec = makeSimpleRowVector(
      {"inner1", "inner2"},
      {
          makeIntVector<int32_t>({10, 20}),
          makeStringVector({"a", "b"}),
      });

  auto vec = makeSimpleRowVector(
      {"outer", "other"},
      {
          innerVec,
          makeIntVector<int64_t>({100, 200}),
      });

  auto serialized = serialize(vec, type, inputSerializerOptions());
  auto inputSchema = getNimbleSchema(type, inputSerializerOptions());

  // Project only "outer.inner1".
  auto subfields = makeSubfields({"outer.inner1"});
  Projector projector(inputSchema, subfields, projectorOptions());
  auto outputSchema = projector.projectedSchema();

  // Output schema: ROW { outer: ROW { inner1: INTEGER } }
  ASSERT_TRUE(outputSchema->isRow());
  ASSERT_EQ(outputSchema->asRow().childrenCount(), 1);
  ASSERT_EQ(outputSchema->asRow().nameAt(0), "outer");

  auto outerType = outputSchema->asRow().childAt(0);
  ASSERT_TRUE(outerType->isRow());
  ASSERT_EQ(outerType->asRow().childrenCount(), 1);
  ASSERT_EQ(outerType->asRow().nameAt(0), "inner1");

  // Helper to verify result.
  auto verifyResult = [&](std::string_view projected) {
    auto result =
        deserialize(projected, outputSchema, outputDeserializerOptions());
    ASSERT_EQ(result->size(), 2);

    auto resultRow = result->as<RowVector>();
    auto outerRow = resultRow->childAt(0)->as<RowVector>();
    auto inner1Col = outerRow->childAt(0)->as<FlatVector<int32_t>>();
    EXPECT_EQ(inner1Col->valueAt(0), 10);
    EXPECT_EQ(inner1Col->valueAt(1), 20);
  };

  // Test single and batch projection.
  verifyResult(projector.project(serialized));

  auto batchResults = projector.project({serialized, serialized});
  ASSERT_EQ(batchResults.size(), 2);
  verifyResult(batchResults[0]);
  verifyResult(batchResults[1]);
}

// Test projecting with array type (entire array column).
TEST_P(ProjectorFormatTest, projectArrayColumn) {
  auto type = ROW({
      {"arr", ARRAY(INTEGER())},
      {"other", BIGINT()},
  });

  auto arrElements = makeIntVector<int32_t>({1, 2, 3, 4, 5, 6});
  auto arrOffsets = allocateOffsets(2, pool_.get());
  auto arrSizes = allocateSizes(2, pool_.get());
  auto rawOffsets = arrOffsets->asMutable<vector_size_t>();
  auto rawSizes = arrSizes->asMutable<vector_size_t>();
  rawOffsets[0] = 0;
  rawSizes[0] = 3; // [1, 2, 3]
  rawOffsets[1] = 3;
  rawSizes[1] = 3; // [4, 5, 6]

  auto arrVec = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(INTEGER()),
      nullptr,
      2,
      arrOffsets,
      arrSizes,
      arrElements);

  auto vec = makeSimpleRowVector(
      {"arr", "other"},
      {
          arrVec,
          makeIntVector<int64_t>({100, 200}),
      });

  auto serialized = serialize(vec, type, inputSerializerOptions());
  auto inputSchema = getNimbleSchema(type, inputSerializerOptions());

  // Project only "arr".
  auto subfields = makeSubfields({"arr"});
  Projector projector(inputSchema, subfields, projectorOptions());
  auto outputSchema = projector.projectedSchema();

  ASSERT_EQ(outputSchema->asRow().childrenCount(), 1);
  ASSERT_EQ(outputSchema->asRow().nameAt(0), "arr");

  // Helper to verify result.
  auto verifyResult = [&](std::string_view projected) {
    auto result =
        deserialize(projected, outputSchema, outputDeserializerOptions());
    ASSERT_EQ(result->size(), 2);

    auto resultRow = result->as<RowVector>();
    auto arrResult = resultRow->childAt(0)->as<ArrayVector>();
    EXPECT_EQ(arrResult->sizeAt(0), 3);
    EXPECT_EQ(arrResult->sizeAt(1), 3);
  };

  // Test single and batch projection.
  verifyResult(projector.project(serialized));

  auto batchResults = projector.project({serialized, serialized});
  ASSERT_EQ(batchResults.size(), 2);
  verifyResult(batchResults[0]);
  verifyResult(batchResults[1]);
}

// Test empty input (0 rows).
TEST_P(ProjectorFormatTest, emptyInput) {
  auto type = ROW({
      {"a", INTEGER()},
      {"b", BIGINT()},
  });

  auto vec = makeSimpleRowVector(
      {"a", "b"},
      {
          makeIntVector<int32_t>({}),
          makeIntVector<int64_t>({}),
      });

  auto serialized = serialize(vec, type, inputSerializerOptions());
  auto inputSchema = getNimbleSchema(type, inputSerializerOptions());

  auto subfields = makeSubfields({"a"});
  Projector projector(inputSchema, subfields, projectorOptions());
  auto outputSchema = projector.projectedSchema();

  // Helper to verify result.
  auto verifyResult = [&](std::string_view projected) {
    auto result =
        deserialize(projected, outputSchema, outputDeserializerOptions());
    EXPECT_EQ(result->size(), 0);
  };

  // Test single and batch projection.
  verifyResult(projector.project(serialized));

  auto batchResults = projector.project({serialized, serialized});
  ASSERT_EQ(batchResults.size(), 2);
  verifyResult(batchResults[0]);
  verifyResult(batchResults[1]);
}

INSTANTIATE_TEST_SUITE_P(
    AllFormats,
    ProjectorFormatTest,
    ::testing::ValuesIn(allFormatCombinations()),
    [](const ::testing::TestParamInfo<FormatParam>& info) {
      return info.param.name();
    });

// Non-parameterized tests for special cases.
class ProjectorTest : public ProjectorTestBase {};

// Test that incompatible format combinations are rejected at projection time.
TEST_F(ProjectorTest, incompatibleFormatsRejected) {
  auto type = ROW({
      {"a", INTEGER()},
      {"b", BIGINT()},
  });

  auto vec = makeSimpleRowVector(
      {"a", "b"},
      {
          makeIntVector<int32_t>({1, 2}),
          makeIntVector<int64_t>({100, 200}),
      });

  auto subfields = makeSubfields({"a"});

  // Test encoded input -> raw output (incompatible).
  {
    SerializerOptions serOpts{.version = SerializationVersion::kDenseEncoded};
    auto serialized = serialize(vec, type, serOpts);
    auto inputSchema = getNimbleSchema(type, serOpts);

    Projector projector(
        inputSchema,
        subfields,
        {.inputHasVersionHeader = true,
         .projectVersion = SerializationVersion::kDense});

    NIMBLE_ASSERT_THROW(
        projector.project(serialized), "Incompatible input/output formats");
  }

  {
    SerializerOptions serOpts{.version = SerializationVersion::kSparseEncoded};
    auto serialized = serialize(vec, type, serOpts);
    auto inputSchema = getNimbleSchema(type, serOpts);

    Projector projector(
        inputSchema,
        subfields,
        {.inputHasVersionHeader = true,
         .projectVersion = SerializationVersion::kSparse});

    NIMBLE_ASSERT_THROW(
        projector.project(serialized), "Incompatible input/output formats");
  }

  // Test raw input -> encoded output (incompatible).
  {
    SerializerOptions serOpts{.version = SerializationVersion::kDense};
    auto serialized = serialize(vec, type, serOpts);
    auto inputSchema = getNimbleSchema(type, serOpts);

    Projector projector(
        inputSchema,
        subfields,
        {.inputHasVersionHeader = true,
         .projectVersion = SerializationVersion::kDenseEncoded});

    NIMBLE_ASSERT_THROW(
        projector.project(serialized), "Incompatible input/output formats");
  }

  {
    SerializerOptions serOpts{.version = SerializationVersion::kSparse};
    auto serialized = serialize(vec, type, serOpts);
    auto inputSchema = getNimbleSchema(type, serOpts);

    Projector projector(
        inputSchema,
        subfields,
        {.inputHasVersionHeader = true,
         .projectVersion = SerializationVersion::kSparseEncoded});

    NIMBLE_ASSERT_THROW(
        projector.project(serialized), "Incompatible input/output formats");
  }

  // Test no-version (legacy raw) -> encoded output (incompatible).
  {
    // Legacy format has no version header.
    SerializerOptions serOpts{};
    auto serialized = serialize(vec, type, serOpts);
    auto inputSchema = getNimbleSchema(type, serOpts);

    Projector projector(
        inputSchema,
        subfields,
        {.inputHasVersionHeader = false,
         .projectVersion = SerializationVersion::kDenseEncoded});

    NIMBLE_ASSERT_THROW(
        projector.project(serialized), "Incompatible input/output formats");
  }
}

// Test that empty projection is invalid.
TEST_F(ProjectorTest, emptyProjectionInvalid) {
  auto type = ROW({
      {"a", INTEGER()},
      {"b", BIGINT()},
  });

  SerializerOptions serOpts{.version = SerializationVersion::kDenseEncoded};
  auto inputSchema = getNimbleSchema(type, serOpts);

  // Empty subfields should throw.
  std::vector<common::Subfield> emptySubfields;
  NIMBLE_ASSERT_THROW(
      Projector(
          inputSchema,
          emptySubfields,
          {.inputHasVersionHeader = true,
           .projectVersion = SerializationVersion::kDenseEncoded}),
      "Must project at least one subfield");
}

// Test full projection fast path - same format, pass through.
TEST_F(ProjectorTest, fullProjectionPassThrough) {
  auto type = ROW({
      {"a", INTEGER()},
      {"b", BIGINT()},
      {"c", VARCHAR()},
  });

  auto vec = makeSimpleRowVector(
      {"a", "b", "c"},
      {
          makeIntVector<int32_t>({1, 2, 3}),
          makeIntVector<int64_t>({100, 200, 300}),
          makeStringVector({"x", "y", "z"}),
      });

  // Test all 4 versions - pass through when input and output match.
  for (auto version :
       {SerializationVersion::kDense,
        SerializationVersion::kDenseEncoded,
        SerializationVersion::kSparse,
        SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));

    SerializerOptions serOpts{.version = version};
    auto serialized = serialize(vec, type, serOpts);
    auto inputSchema = getNimbleSchema(type, serOpts);

    // Full projection - all columns selected, same format.
    auto subfields = makeSubfields({"a", "b", "c"});
    Projector projector(
        inputSchema,
        subfields,
        {.inputHasVersionHeader = true, .projectVersion = version});

    auto projected = projector.project(serialized);

    // Fast path: output should be identical to input (pass through).
    EXPECT_EQ(projected, serialized);

    // Verify can still deserialize correctly.
    auto outputSchema = projector.projectedSchema();
    auto result = deserialize(projected, outputSchema, {.version = version});
    ASSERT_EQ(result->size(), 3);

    auto resultRow = result->as<RowVector>();
    EXPECT_EQ(resultRow->childAt(0)->as<FlatVector<int32_t>>()->valueAt(0), 1);
    EXPECT_EQ(
        resultRow->childAt(1)->as<FlatVector<int64_t>>()->valueAt(0), 100);
  }
}

// Test full projection with format conversion (no fast path).
TEST_F(ProjectorTest, fullProjectionFormatConversion) {
  auto type = ROW({
      {"a", INTEGER()},
      {"b", BIGINT()},
  });

  auto vec = makeSimpleRowVector(
      {"a", "b"},
      {
          makeIntVector<int32_t>({1, 2}),
          makeIntVector<int64_t>({100, 200}),
      });

  // Input: dense encoded format.
  SerializerOptions serOpts{.version = SerializationVersion::kDenseEncoded};
  auto serialized = serialize(vec, type, serOpts);
  auto inputSchema = getNimbleSchema(type, serOpts);

  // Full projection but converting to sparse format.
  auto subfields = makeSubfields({"a", "b"});
  Projector projector(
      inputSchema,
      subfields,
      {.inputHasVersionHeader = true,
       .projectVersion = SerializationVersion::kSparseEncoded});

  auto projected = projector.project(serialized);

  // Not a fast path - output differs from input (different format).
  EXPECT_NE(projected, serialized);

  // But still deserializes correctly.
  auto outputSchema = projector.projectedSchema();
  auto result = deserialize(
      projected,
      outputSchema,
      {.version = SerializationVersion::kSparseEncoded});
  ASSERT_EQ(result->size(), 2);

  auto resultRow = result->as<RowVector>();
  EXPECT_EQ(resultRow->childAt(0)->as<FlatVector<int32_t>>()->valueAt(0), 1);
  EXPECT_EQ(resultRow->childAt(1)->as<FlatVector<int64_t>>()->valueAt(0), 100);
}

// Test that unsupported operations throw.
TEST_F(ProjectorTest, unsupportedArraySubscript) {
  auto type = ROW({{"arr", ARRAY(INTEGER())}});

  SerializerOptions serOpts{.version = SerializationVersion::kDenseEncoded};
  auto inputSchema = getNimbleSchema(type, serOpts);

  // Array subscripts are not supported.
  auto subfields = makeSubfields({"arr[0]"});

  NIMBLE_ASSERT_THROW(
      Projector(
          inputSchema,
          subfields,
          {.inputHasVersionHeader = true,
           .projectVersion = SerializationVersion::kDenseEncoded}),
      "Unsupported subfield kind");
}

// Test that regular map key projection throws.
TEST_F(ProjectorTest, unsupportedMapKeyProjection) {
  auto type = ROW({{"m", MAP(VARCHAR(), INTEGER())}});

  SerializerOptions serOpts{.version = SerializationVersion::kDenseEncoded};
  auto inputSchema = getNimbleSchema(type, serOpts);

  // Regular map subscripts are not supported (would need re-encoding).
  auto subfields = makeSubfields({"m[\"key\"]"});

  NIMBLE_ASSERT_THROW(
      Projector(
          inputSchema,
          subfields,
          {.inputHasVersionHeader = true,
           .projectVersion = SerializationVersion::kDenseEncoded}),
      "String subscript");
}

// Test FlatMap serialization/deserialization without projection.
TEST_F(ProjectorTest, flatMapSerializeDeserializeNoProjction) {
  auto type = ROW({
      {"id", BIGINT()},
      {"features", MAP(INTEGER(), DOUBLE())},
  });

  // Create test data with keys 1, 2, 3.
  const vector_size_t numRows = 2;
  auto ids = makeIntVector<int64_t>({100, 200});

  const int entriesPerRow = 3;
  const int totalEntries = numRows * entriesPerRow;

  auto mapOffsets = allocateOffsets(numRows, pool_.get());
  auto mapSizes = allocateSizes(numRows, pool_.get());
  auto rawOffsets = mapOffsets->asMutable<vector_size_t>();
  auto rawSizes = mapSizes->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawOffsets[i] = i * entriesPerRow;
    rawSizes[i] = entriesPerRow;
  }

  auto mapKeys = BaseVector::create<FlatVector<int32_t>>(
      INTEGER(), totalEntries, pool_.get());
  auto mapValues = BaseVector::create<FlatVector<double>>(
      DOUBLE(), totalEntries, pool_.get());

  for (int i = 0; i < totalEntries; ++i) {
    mapKeys->set(i, (i % entriesPerRow) + 1); // Keys: 1, 2, 3
    mapValues->set(i, i * 1.5);
  }

  auto mapVector = std::make_shared<MapVector>(
      pool_.get(),
      MAP(INTEGER(), DOUBLE()),
      nullptr,
      numRows,
      mapOffsets,
      mapSizes,
      mapKeys,
      mapValues);

  auto vec = std::make_shared<RowVector>(
      pool_.get(),
      type,
      nullptr,
      numRows,
      std::vector<VectorPtr>{ids, mapVector});

  // Serialize with FlatMap encoding.
  SerializerOptions serOpts{
      .version = SerializationVersion::kDenseEncoded,
      .flatMapColumns = {"features"},
  };
  auto [serialized, inputSchema] = serializeWithSchema(vec, type, serOpts);

  // Deserialize directly (no projection).
  auto result = deserialize(
      serialized,
      inputSchema,
      {.version = SerializationVersion::kDenseEncoded});

  ASSERT_EQ(result->size(), 2);
  auto resultRow = result->as<RowVector>();
  auto featuresMap = resultRow->childAt(1)->as<MapVector>();

  // Each row should have 3 entries.
  for (vector_size_t i = 0; i < numRows; ++i) {
    EXPECT_EQ(featuresMap->sizeAt(i), 3);
  }
}

// Test projecting entire FlatMap column (not individual keys).
TEST_F(ProjectorTest, projectEntireFlatMapColumn) {
  auto type = ROW({
      {"id", BIGINT()},
      {"features", MAP(INTEGER(), DOUBLE())},
  });

  // Create test data with keys 1, 2, 3.
  const vector_size_t numRows = 2;
  auto ids = makeIntVector<int64_t>({100, 200});

  const int entriesPerRow = 3;
  const int totalEntries = numRows * entriesPerRow;

  auto mapOffsets = allocateOffsets(numRows, pool_.get());
  auto mapSizes = allocateSizes(numRows, pool_.get());
  auto rawOffsets = mapOffsets->asMutable<vector_size_t>();
  auto rawSizes = mapSizes->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawOffsets[i] = i * entriesPerRow;
    rawSizes[i] = entriesPerRow;
  }

  auto mapKeys = BaseVector::create<FlatVector<int32_t>>(
      INTEGER(), totalEntries, pool_.get());
  auto mapValues = BaseVector::create<FlatVector<double>>(
      DOUBLE(), totalEntries, pool_.get());

  for (int i = 0; i < totalEntries; ++i) {
    mapKeys->set(i, (i % entriesPerRow) + 1); // Keys: 1, 2, 3
    mapValues->set(i, i * 1.5);
  }

  auto mapVector = std::make_shared<MapVector>(
      pool_.get(),
      MAP(INTEGER(), DOUBLE()),
      nullptr,
      numRows,
      mapOffsets,
      mapSizes,
      mapKeys,
      mapValues);

  auto vec = std::make_shared<RowVector>(
      pool_.get(),
      type,
      nullptr,
      numRows,
      std::vector<VectorPtr>{ids, mapVector});

  // Serialize with FlatMap encoding.
  SerializerOptions serOpts{
      .version = SerializationVersion::kDenseEncoded,
      .flatMapColumns = {"features"},
  };
  auto [serialized, inputSchema] = serializeWithSchema(vec, type, serOpts);

  // Project entire "features" column (not individual keys).
  auto subfields = makeSubfields({"features"});
  Projector projector(
      inputSchema,
      subfields,
      {.inputHasVersionHeader = true,
       .projectVersion = SerializationVersion::kDenseEncoded});

  auto outputSchema = projector.projectedSchema();
  auto projected = projector.project(serialized);

  // Deserialize and verify.
  auto result = deserialize(
      projected,
      outputSchema,
      {.version = SerializationVersion::kDenseEncoded});

  ASSERT_EQ(result->size(), 2);
  auto resultRow = result->as<RowVector>();
  auto featuresMap = resultRow->childAt(0)->as<MapVector>();

  // Each row should have 3 entries (all keys).
  for (vector_size_t i = 0; i < numRows; ++i) {
    EXPECT_EQ(featuresMap->sizeAt(i), 3);
  }
}

// Test FlatMap full projection (all keys) works.
TEST_F(ProjectorTest, projectFlatMapAllKeys) {
  auto type = ROW({
      {"id", BIGINT()},
      {"features", MAP(INTEGER(), DOUBLE())},
  });

  // Create test data with keys 1, 2, 3.
  const vector_size_t numRows = 2;
  auto ids = makeIntVector<int64_t>({100, 200});

  const int entriesPerRow = 3;
  const int totalEntries = numRows * entriesPerRow;

  auto mapOffsets = allocateOffsets(numRows, pool_.get());
  auto mapSizes = allocateSizes(numRows, pool_.get());
  auto rawOffsets = mapOffsets->asMutable<vector_size_t>();
  auto rawSizes = mapSizes->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawOffsets[i] = i * entriesPerRow;
    rawSizes[i] = entriesPerRow;
  }

  auto mapKeys = BaseVector::create<FlatVector<int32_t>>(
      INTEGER(), totalEntries, pool_.get());
  auto mapValues = BaseVector::create<FlatVector<double>>(
      DOUBLE(), totalEntries, pool_.get());

  for (int i = 0; i < totalEntries; ++i) {
    mapKeys->set(i, (i % entriesPerRow) + 1); // Keys: 1, 2, 3
    mapValues->set(i, i * 1.5);
  }

  auto mapVector = std::make_shared<MapVector>(
      pool_.get(),
      MAP(INTEGER(), DOUBLE()),
      nullptr,
      numRows,
      mapOffsets,
      mapSizes,
      mapKeys,
      mapValues);

  auto vec = std::make_shared<RowVector>(
      pool_.get(),
      type,
      nullptr,
      numRows,
      std::vector<VectorPtr>{ids, mapVector});

  // Serialize with FlatMap encoding.
  SerializerOptions serOpts{
      .version = SerializationVersion::kDenseEncoded,
      .flatMapColumns = {"features"},
  };
  auto [serialized, inputSchema] = serializeWithSchema(vec, type, serOpts);

  // Project ALL keys from FlatMap.
  auto subfields =
      makeSubfields({"features[\"1\"]", "features[\"2\"]", "features[\"3\"]"});
  Projector projector(
      inputSchema,
      subfields,
      {.inputHasVersionHeader = true,
       .projectVersion = SerializationVersion::kDenseEncoded});

  auto outputSchema = projector.projectedSchema();
  auto projected = projector.project(serialized);

  // Deserialize and verify.
  auto result = deserialize(
      projected,
      outputSchema,
      {.version = SerializationVersion::kDenseEncoded});

  ASSERT_EQ(result->size(), 2);
  auto resultRow = result->as<RowVector>();
  auto featuresMap = resultRow->childAt(0)->as<MapVector>();

  // Each row should have 3 entries (all keys).
  for (vector_size_t i = 0; i < numRows; ++i) {
    EXPECT_EQ(featuresMap->sizeAt(i), 3);
  }
}

// Test FlatMap stream indices are correct.
TEST_F(ProjectorTest, flatMapStreamIndices) {
  auto type = ROW({
      {"id", BIGINT()},
      {"features", MAP(INTEGER(), DOUBLE())},
  });

  // Create test data with keys 1, 2, 3.
  const vector_size_t numRows = 2;
  auto ids = makeIntVector<int64_t>({100, 200});

  const int entriesPerRow = 3;
  const int totalEntries = numRows * entriesPerRow;

  auto mapOffsets = allocateOffsets(numRows, pool_.get());
  auto mapSizes = allocateSizes(numRows, pool_.get());
  auto rawOffsets = mapOffsets->asMutable<vector_size_t>();
  auto rawSizes = mapSizes->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawOffsets[i] = i * entriesPerRow;
    rawSizes[i] = entriesPerRow;
  }

  auto mapKeys = BaseVector::create<FlatVector<int32_t>>(
      INTEGER(), totalEntries, pool_.get());
  auto mapValues = BaseVector::create<FlatVector<double>>(
      DOUBLE(), totalEntries, pool_.get());

  for (int i = 0; i < totalEntries; ++i) {
    mapKeys->set(i, (i % entriesPerRow) + 1); // Keys: 1, 2, 3
    mapValues->set(i, i * 1.5);
  }

  auto mapVector = std::make_shared<MapVector>(
      pool_.get(),
      MAP(INTEGER(), DOUBLE()),
      nullptr,
      numRows,
      mapOffsets,
      mapSizes,
      mapKeys,
      mapValues);

  auto vec = std::make_shared<RowVector>(
      pool_.get(),
      type,
      nullptr,
      numRows,
      std::vector<VectorPtr>{ids, mapVector});

  // Serialize with FlatMap encoding.
  SerializerOptions serOpts{
      .version = SerializationVersion::kDenseEncoded,
      .flatMapColumns = {"features"},
  };
  auto [serialized, inputSchema] = serializeWithSchema(vec, type, serOpts);

  // Verify input schema structure.
  // ROW(nulls=0) -> id(scalar=1) -> features(FlatMap, nulls=2)
  //   -> key "1": value=3, inMap=4
  //   -> key "2": value=5, inMap=6
  //   -> key "3": value=7, inMap=8
  ASSERT_TRUE(inputSchema->isRow());
  const auto& row = inputSchema->asRow();
  ASSERT_EQ(row.childrenCount(), 2);
  ASSERT_TRUE(row.childAt(1)->isFlatMap());
  const auto& flatMap = row.childAt(1)->asFlatMap();
  ASSERT_EQ(flatMap.childrenCount(), 3); // 3 keys discovered

  // Print stream offsets for debugging.
  LOG(INFO) << "Row nulls offset: " << row.nullsDescriptor().offset();
  LOG(INFO) << "id offset: "
            << row.childAt(0)->asScalar().scalarDescriptor().offset();
  LOG(INFO) << "FlatMap nulls offset: " << flatMap.nullsDescriptor().offset();
  for (size_t i = 0; i < flatMap.childrenCount(); ++i) {
    LOG(INFO) << "Key '" << flatMap.nameAt(i)
              << "' inMap offset: " << flatMap.inMapDescriptorAt(i).offset()
              << ", value offset: "
              << flatMap.childAt(i)->asScalar().scalarDescriptor().offset();
  }

  // Project only key "2".
  auto subfields = makeSubfields({"features[\"2\"]"});
  Projector projector(
      inputSchema,
      subfields,
      {.inputHasVersionHeader = true,
       .projectVersion = SerializationVersion::kDenseEncoded});

  const auto& indices = projector.testingInputStreamIndices();
  LOG(INFO) << "Projected stream indices: ";
  for (uint32_t idx : indices) {
    LOG(INFO) << "  " << idx;
  }

  // Expected: Row nulls, FlatMap nulls, value for "2", inMap for "2"
  // Should be 4 streams total.
  ASSERT_EQ(indices.size(), 4);

  // Verify output schema has correct stream ordering.
  // FlatMap allocates value streams BEFORE inMap streams for each key.
  // This ordering must be preserved in the projected schema.
  auto outputSchema = projector.projectedSchema();
  ASSERT_TRUE(outputSchema->isRow());
  const auto& outputRow = outputSchema->asRow();
  ASSERT_EQ(outputRow.childrenCount(), 1); // Only "features" projected
  ASSERT_TRUE(outputRow.childAt(0)->isFlatMap());
  const auto& outputFlatMap = outputRow.childAt(0)->asFlatMap();
  ASSERT_EQ(outputFlatMap.childrenCount(), 1); // Only key "2" projected

  // Output schema offsets should be: Row nulls=0, FlatMap nulls=1, value=2,
  // inMap=3
  EXPECT_EQ(outputRow.nullsDescriptor().offset(), 0);
  EXPECT_EQ(outputFlatMap.nullsDescriptor().offset(), 1);
  // Value stream must come BEFORE inMap stream (this is the key ordering
  // check).
  EXPECT_EQ(
      outputFlatMap.childAt(0)->asScalar().scalarDescriptor().offset(), 2);
  EXPECT_EQ(outputFlatMap.inMapDescriptorAt(0).offset(), 3);
}

// Test projecting FlatMap with single key.
TEST_F(ProjectorTest, projectFlatMapSingleKey) {
  auto type = ROW({
      {"id", BIGINT()},
      {"features", MAP(INTEGER(), DOUBLE())},
  });

  // Create test data with keys 1, 2, 3.
  const vector_size_t numRows = 3;
  auto ids = makeIntVector<int64_t>({100, 200, 300});

  // Build map: each row has 3 entries with keys 1, 2, 3.
  const int entriesPerRow = 3;
  const int totalEntries = numRows * entriesPerRow;

  auto mapOffsets = allocateOffsets(numRows, pool_.get());
  auto mapSizes = allocateSizes(numRows, pool_.get());
  auto rawOffsets = mapOffsets->asMutable<vector_size_t>();
  auto rawSizes = mapSizes->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawOffsets[i] = i * entriesPerRow;
    rawSizes[i] = entriesPerRow;
  }

  auto mapKeys = BaseVector::create<FlatVector<int32_t>>(
      INTEGER(), totalEntries, pool_.get());
  auto mapValues = BaseVector::create<FlatVector<double>>(
      DOUBLE(), totalEntries, pool_.get());

  for (int i = 0; i < totalEntries; ++i) {
    mapKeys->set(i, (i % entriesPerRow) + 1); // Keys: 1, 2, 3
    mapValues->set(i, i * 1.5);
  }

  auto mapVector = std::make_shared<MapVector>(
      pool_.get(),
      MAP(INTEGER(), DOUBLE()),
      nullptr,
      numRows,
      mapOffsets,
      mapSizes,
      mapKeys,
      mapValues);

  auto vec = std::make_shared<RowVector>(
      pool_.get(),
      type,
      nullptr,
      numRows,
      std::vector<VectorPtr>{ids, mapVector});

  // Serialize with FlatMap encoding.
  // Use serializeWithSchema to get schema AFTER serialization (when FlatMap
  // keys are discovered).
  SerializerOptions serOpts{
      .version = SerializationVersion::kDenseEncoded,
      .flatMapColumns = {"features"},
  };
  auto [serialized, inputSchema] = serializeWithSchema(vec, type, serOpts);

  // Project only key "2" from FlatMap.
  auto subfields = makeSubfields({"features[\"2\"]"});
  Projector projector(
      inputSchema,
      subfields,
      {.inputHasVersionHeader = true,
       .projectVersion = SerializationVersion::kDenseEncoded});

  auto outputSchema = projector.projectedSchema();

  // Verify output schema: ROW { features: FlatMap with only key "2" }
  ASSERT_TRUE(outputSchema->isRow());
  ASSERT_EQ(outputSchema->asRow().childrenCount(), 1);
  ASSERT_EQ(outputSchema->asRow().nameAt(0), "features");
  ASSERT_TRUE(outputSchema->asRow().childAt(0)->isFlatMap());
  ASSERT_EQ(outputSchema->asRow().childAt(0)->asFlatMap().childrenCount(), 1);
  ASSERT_EQ(outputSchema->asRow().childAt(0)->asFlatMap().nameAt(0), "2");

  // Project and deserialize.
  auto projected = projector.project(serialized);
  auto result = deserialize(
      projected,
      outputSchema,
      {.version = SerializationVersion::kDenseEncoded});

  ASSERT_EQ(result->size(), 3);
  auto resultRow = result->as<RowVector>();
  auto featuresMap = resultRow->childAt(0)->as<MapVector>();

  // Each row should have 1 entry (key 2).
  for (vector_size_t i = 0; i < numRows; ++i) {
    EXPECT_EQ(featuresMap->sizeAt(i), 1);
  }
}

// Test projecting multiple FlatMap keys.
TEST_F(ProjectorTest, projectFlatMapMultipleKeys) {
  auto type = ROW({
      {"id", BIGINT()},
      {"features", MAP(VARCHAR(), INTEGER())},
  });

  // Create test data with string keys "a", "b", "c".
  const vector_size_t numRows = 2;
  auto ids = makeIntVector<int64_t>({100, 200});

  const int entriesPerRow = 3;
  const int totalEntries = numRows * entriesPerRow;

  auto mapOffsets = allocateOffsets(numRows, pool_.get());
  auto mapSizes = allocateSizes(numRows, pool_.get());
  auto rawOffsets = mapOffsets->asMutable<vector_size_t>();
  auto rawSizes = mapSizes->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawOffsets[i] = i * entriesPerRow;
    rawSizes[i] = entriesPerRow;
  }

  auto mapKeys = BaseVector::create<FlatVector<StringView>>(
      VARCHAR(), totalEntries, pool_.get());
  auto mapValues = BaseVector::create<FlatVector<int32_t>>(
      INTEGER(), totalEntries, pool_.get());

  std::vector<std::string> keyNames = {"a", "b", "c"};
  for (int i = 0; i < totalEntries; ++i) {
    mapKeys->set(i, StringView(keyNames[i % entriesPerRow]));
    mapValues->set(i, i * 10);
  }

  auto mapVector = std::make_shared<MapVector>(
      pool_.get(),
      MAP(VARCHAR(), INTEGER()),
      nullptr,
      numRows,
      mapOffsets,
      mapSizes,
      mapKeys,
      mapValues);

  auto vec = std::make_shared<RowVector>(
      pool_.get(),
      type,
      nullptr,
      numRows,
      std::vector<VectorPtr>{ids, mapVector});

  // Serialize with FlatMap encoding.
  // Use serializeWithSchema to get schema AFTER serialization.
  SerializerOptions serOpts{
      .version = SerializationVersion::kDenseEncoded,
      .flatMapColumns = {"features"},
  };
  auto [serialized, inputSchema] = serializeWithSchema(vec, type, serOpts);

  // Project keys "a" and "c" from FlatMap (skip "b").
  auto subfields = makeSubfields({"features[\"a\"]", "features[\"c\"]"});
  Projector projector(
      inputSchema,
      subfields,
      {.inputHasVersionHeader = true,
       .projectVersion = SerializationVersion::kDenseEncoded});

  auto outputSchema = projector.projectedSchema();

  // Verify output schema has 2 keys.
  ASSERT_TRUE(outputSchema->asRow().childAt(0)->isFlatMap());
  ASSERT_EQ(outputSchema->asRow().childAt(0)->asFlatMap().childrenCount(), 2);
  ASSERT_EQ(outputSchema->asRow().childAt(0)->asFlatMap().nameAt(0), "a");
  ASSERT_EQ(outputSchema->asRow().childAt(0)->asFlatMap().nameAt(1), "c");

  // Project and deserialize.
  auto projected = projector.project(serialized);
  auto result = deserialize(
      projected,
      outputSchema,
      {.version = SerializationVersion::kDenseEncoded});

  ASSERT_EQ(result->size(), 2);
  auto resultRow = result->as<RowVector>();
  auto featuresMap = resultRow->childAt(0)->as<MapVector>();

  // Each row should have 2 entries (keys a and c).
  for (vector_size_t i = 0; i < numRows; ++i) {
    EXPECT_EQ(featuresMap->sizeAt(i), 2);
  }
}

// Test projecting FlatMap with non-existent key throws.
TEST_F(ProjectorTest, projectFlatMapNonExistentKey) {
  auto type = ROW({
      {"id", BIGINT()},
      {"features", MAP(INTEGER(), DOUBLE())},
  });

  // Create test data with keys 1, 2, 3.
  const vector_size_t numRows = 2;
  auto ids = makeIntVector<int64_t>({100, 200});

  const int entriesPerRow = 3;
  const int totalEntries = numRows * entriesPerRow;

  auto mapOffsets = allocateOffsets(numRows, pool_.get());
  auto mapSizes = allocateSizes(numRows, pool_.get());
  auto rawOffsets = mapOffsets->asMutable<vector_size_t>();
  auto rawSizes = mapSizes->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawOffsets[i] = i * entriesPerRow;
    rawSizes[i] = entriesPerRow;
  }

  auto mapKeys = BaseVector::create<FlatVector<int32_t>>(
      INTEGER(), totalEntries, pool_.get());
  auto mapValues = BaseVector::create<FlatVector<double>>(
      DOUBLE(), totalEntries, pool_.get());

  for (int i = 0; i < totalEntries; ++i) {
    mapKeys->set(i, (i % entriesPerRow) + 1); // Keys: 1, 2, 3
    mapValues->set(i, i * 1.5);
  }

  auto mapVector = std::make_shared<MapVector>(
      pool_.get(),
      MAP(INTEGER(), DOUBLE()),
      nullptr,
      numRows,
      mapOffsets,
      mapSizes,
      mapKeys,
      mapValues);

  auto vec = std::make_shared<RowVector>(
      pool_.get(),
      type,
      nullptr,
      numRows,
      std::vector<VectorPtr>{ids, mapVector});

  // Serialize with FlatMap encoding to discover keys 1, 2, 3.
  SerializerOptions serOpts{
      .version = SerializationVersion::kDenseEncoded,
      .flatMapColumns = {"features"},
  };
  auto [serialized, inputSchema] = serializeWithSchema(vec, type, serOpts);
  (void)serialized; // Not used, just needed to discover keys.

  // Try to project a key that doesn't exist in schema.
  auto subfields = makeSubfields({"features[\"999\"]"});

  NIMBLE_ASSERT_THROW(
      Projector(
          inputSchema,
          subfields,
          {.inputHasVersionHeader = true,
           .projectVersion = SerializationVersion::kDenseEncoded}),
      "Key '999' not found in FlatMapType");
}

// Test stream indices are correct.
TEST_F(ProjectorTest, streamIndicesCorrect) {
  auto type = ROW({
      {"a", INTEGER()}, // Stream 1
      {"b", BIGINT()}, // Stream 2
      {"c", VARCHAR()}, // Stream 3
  });
  // Stream 0 is root row nulls.

  SerializerOptions serOpts{.version = SerializationVersion::kDenseEncoded};
  auto inputSchema = getNimbleSchema(type, serOpts);

  // Project "b" only - should include streams 0 (root nulls) and 2 (b data).
  auto subfields = makeSubfields({"b"});
  Projector projector(
      inputSchema,
      subfields,
      {.inputHasVersionHeader = true,
       .projectVersion = SerializationVersion::kDenseEncoded});

  const auto& indices = projector.testingInputStreamIndices();
  ASSERT_EQ(indices.size(), 2);
  EXPECT_EQ(indices[0], 0); // Root row nulls
  EXPECT_EQ(indices[1], 2); // Column b
}

// Test auto name mapping via projectType for schema evolution (column renames).
TEST_F(ProjectorTest, projectWithUpdatedRowType) {
  // Input schema uses old column names.
  auto inputType = ROW({
      {"old_id", INTEGER()},
      {"old_name", VARCHAR()},
      {"unchanged", BIGINT()},
  });

  // Project type uses new column names (current table schema).
  auto projectType = ROW({
      {"new_id", INTEGER()},
      {"new_name", VARCHAR()},
      {"unchanged", BIGINT()},
  });

  const int numRows = 5;
  std::vector<int32_t> idVals;
  std::vector<std::string> nameVals;
  std::vector<int64_t> valVals;
  for (int i = 0; i < numRows; ++i) {
    idVals.emplace_back(i * 10);
    nameVals.emplace_back("name" + std::to_string(i));
    valVals.emplace_back(i * 100);
  }
  auto ids = makeIntVector(idVals);
  auto names = makeStringVector(nameVals);
  auto values = makeIntVector(valVals);

  auto vec = std::make_shared<RowVector>(
      pool_.get(),
      inputType,
      nullptr,
      numRows,
      std::vector<VectorPtr>{ids, names, values});

  // Serialize with old column names.
  SerializerOptions serOpts{.version = SerializationVersion::kDenseEncoded};
  auto [serialized, inputSchema] = serializeWithSchema(vec, inputType, serOpts);

  // Query uses new column names from projectType.
  auto subfields = makeSubfields({"new_id", "unchanged"});

  // Without projectType, projection should fail.
  NIMBLE_ASSERT_THROW(
      Projector(
          inputSchema,
          subfields,
          {.inputHasVersionHeader = true,
           .projectVersion = SerializationVersion::kDenseEncoded}),
      "Field 'new_id' not found in RowType");

  // With projectType, name mapping is auto-computed and projection succeeds.
  Projector::Options opts{
      .inputHasVersionHeader = true,
      .projectVersion = SerializationVersion::kDenseEncoded,
      .projectType = projectType,
  };
  Projector projector(inputSchema, subfields, opts);

  auto projected = projector.project(serialized);
  ASSERT_FALSE(projected.empty());

  // Verify projected schema uses new names from projectType.
  auto projectedSchema = projector.projectedSchema();
  ASSERT_TRUE(projectedSchema->isRow());
  const auto& projectedRow = projectedSchema->asRow();
  ASSERT_EQ(projectedRow.childrenCount(), 2);
  EXPECT_EQ(projectedRow.nameAt(0), "new_id");
  EXPECT_EQ(projectedRow.nameAt(1), "unchanged");

  // Verify stream indices include the mapped column.
  const auto& indices = projector.testingInputStreamIndices();
  // Should have: root nulls (0), old_id (1), unchanged (3).
  // old_name (2) is skipped.
  ASSERT_EQ(indices.size(), 3);
  EXPECT_EQ(indices[0], 0); // Root row nulls
  EXPECT_EQ(indices[1], 1); // old_id (maps to new_id)
  EXPECT_EQ(indices[2], 3); // unchanged
}

// Test auto name mapping with nested ROW inside FlatMap value.
TEST_F(ProjectorTest, projectWithUpdatedNestedRowType) {
  // Input schema has old nested field names.
  auto inputType = ROW({
      {"id", INTEGER()},
      {"features", MAP(VARCHAR(), ROW({{"old_value", INTEGER()}}))},
  });

  // Project type has new nested field names.
  auto projectType = ROW({
      {"id", INTEGER()},
      {"features", MAP(VARCHAR(), ROW({{"new_value", INTEGER()}}))},
  });

  const int numRows = 3;
  std::vector<int32_t> idVals;
  for (int i = 0; i < numRows; ++i) {
    idVals.emplace_back(i);
  }
  auto ids = makeIntVector(idVals);

  // Create map with nested row values.
  auto offsets = allocateOffsets(numRows, pool_.get());
  auto sizes = allocateSizes(numRows, pool_.get());
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  auto* rawSizes = sizes->asMutable<vector_size_t>();

  std::vector<std::string> keys;
  std::vector<int32_t> vals;
  int offset = 0;
  for (int i = 0; i < numRows; ++i) {
    rawOffsets[i] = offset;
    rawSizes[i] = 2;
    keys.emplace_back("key_a");
    vals.emplace_back(i * 10);
    keys.emplace_back("key_b");
    vals.emplace_back(i * 20);
    offset += 2;
  }

  auto keysVec = makeStringVector(keys);
  auto valsVec = makeIntVector(vals);
  auto nestedRowType = ROW({{"old_value", INTEGER()}});
  auto nestedRows = std::make_shared<RowVector>(
      pool_.get(),
      nestedRowType,
      nullptr,
      static_cast<vector_size_t>(vals.size()),
      std::vector<VectorPtr>{valsVec});
  auto mapVector = std::make_shared<MapVector>(
      pool_.get(),
      MAP(VARCHAR(), nestedRowType),
      nullptr,
      numRows,
      offsets,
      sizes,
      keysVec,
      nestedRows);

  auto vec = std::make_shared<RowVector>(
      pool_.get(),
      inputType,
      nullptr,
      numRows,
      std::vector<VectorPtr>{ids, mapVector});

  // Serialize with FlatMap to discover keys.
  SerializerOptions serOpts{
      .version = SerializationVersion::kDenseEncoded,
      .flatMapColumns = {"features"},
  };
  auto [serialized, inputSchema] = serializeWithSchema(vec, inputType, serOpts);

  // Query uses key from input (FlatMap keys are not renamed via projectType).
  auto subfields = makeSubfields({"features[\"key_a\"]"});

  // Without projectType, nested row keeps old field names.
  {
    Projector projectorNoType(
        inputSchema,
        subfields,
        {.inputHasVersionHeader = true,
         .projectVersion = SerializationVersion::kDenseEncoded});
    const auto& nestedRow = projectorNoType.projectedSchema()
                                ->asRow()
                                .childAt(0)
                                ->asFlatMap()
                                .childAt(0)
                                ->asRow();
    EXPECT_EQ(nestedRow.nameAt(0), "old_value");
  }

  // With projectType, nested row field is renamed.
  Projector::Options opts{
      .inputHasVersionHeader = true,
      .projectVersion = SerializationVersion::kDenseEncoded,
      .projectType = projectType,
  };
  Projector projector(inputSchema, subfields, opts);

  auto projected = projector.project(serialized);
  ASSERT_FALSE(projected.empty());

  // Verify projected FlatMap has key_a with nested row using new field name.
  auto projectedSchema = projector.projectedSchema();
  ASSERT_TRUE(projectedSchema->isRow());
  const auto& projectedRow = projectedSchema->asRow();
  ASSERT_EQ(projectedRow.childrenCount(), 1);
  EXPECT_EQ(projectedRow.nameAt(0), "features");
  ASSERT_TRUE(projectedRow.childAt(0)->isFlatMap());
  const auto& projectedFlatMap = projectedRow.childAt(0)->asFlatMap();
  ASSERT_EQ(projectedFlatMap.childrenCount(), 1);
  EXPECT_EQ(projectedFlatMap.nameAt(0), "key_a");

  // Check nested row uses new field name from projectType.
  ASSERT_TRUE(projectedFlatMap.childAt(0)->isRow());
  const auto& nestedProjectedRow = projectedFlatMap.childAt(0)->asRow();
  ASSERT_EQ(nestedProjectedRow.childrenCount(), 1);
  EXPECT_EQ(nestedProjectedRow.nameAt(0), "new_value");
}

TEST_F(ProjectorTest, projectNestedFieldUnderFlatMapValue) {
  // Input schema has old nested field names.
  auto inputType = ROW({
      {"id", INTEGER()},
      {"features", MAP(VARCHAR(), ROW({{"old_value", INTEGER()}}))},
  });

  // Project type has new nested field names.
  auto projectType = ROW({
      {"id", INTEGER()},
      {"features", MAP(VARCHAR(), ROW({{"new_value", INTEGER()}}))},
  });

  const int numRows = 3;
  std::vector<int32_t> idVals = {0, 1, 2};
  auto ids = makeIntVector(idVals);

  // Create map with nested row values.
  auto offsets = allocateOffsets(numRows, pool_.get());
  auto sizes = allocateSizes(numRows, pool_.get());
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  auto* rawSizes = sizes->asMutable<vector_size_t>();

  std::vector<std::string> keys;
  std::vector<int32_t> vals;
  int offset = 0;
  for (int i = 0; i < numRows; ++i) {
    rawOffsets[i] = offset;
    rawSizes[i] = 2;
    keys.emplace_back("key_a");
    vals.emplace_back(i * 10);
    keys.emplace_back("key_b");
    vals.emplace_back(i * 20);
    offset += 2;
  }

  auto keysVec = makeStringVector(keys);
  auto valsVec = makeIntVector(vals);
  auto nestedRowType = ROW({{"old_value", INTEGER()}});
  auto nestedRows = std::make_shared<RowVector>(
      pool_.get(),
      nestedRowType,
      nullptr,
      static_cast<vector_size_t>(vals.size()),
      std::vector<VectorPtr>{valsVec});
  auto mapVector = std::make_shared<MapVector>(
      pool_.get(),
      MAP(VARCHAR(), nestedRowType),
      nullptr,
      numRows,
      offsets,
      sizes,
      keysVec,
      nestedRows);

  auto vec = std::make_shared<RowVector>(
      pool_.get(),
      inputType,
      nullptr,
      numRows,
      std::vector<VectorPtr>{ids, mapVector});

  SerializerOptions serOpts{
      .version = SerializationVersion::kDenseEncoded,
      .flatMapColumns = {"features"},
  };
  auto [serialized, inputSchema] = serializeWithSchema(vec, inputType, serOpts);

  // Project a nested field under FlatMap value using new name.
  auto subfields = makeSubfields({"features[\"key_a\"].new_value"});

  // Without projectType, should fail because 'new_value' doesn't exist.
  NIMBLE_ASSERT_THROW(
      Projector(
          inputSchema,
          subfields,
          {.inputHasVersionHeader = true,
           .projectVersion = SerializationVersion::kDenseEncoded}),
      "Field 'new_value' not found in RowType");

  // With projectType, nested field is renamed and projection succeeds.
  Projector projector(
      inputSchema,
      subfields,
      {.inputHasVersionHeader = true,
       .projectVersion = SerializationVersion::kDenseEncoded,
       .projectType = projectType});

  auto projected = projector.project(serialized);
  ASSERT_FALSE(projected.empty());

  // Verify projected schema: features -> key_a -> row with new_value.
  auto projectedSchema = projector.projectedSchema();
  const auto& projectedFlatMap =
      projectedSchema->asRow().childAt(0)->asFlatMap();
  ASSERT_EQ(projectedFlatMap.childrenCount(), 1);
  EXPECT_EQ(projectedFlatMap.nameAt(0), "key_a");
  const auto& nestedProjectedRow = projectedFlatMap.childAt(0)->asRow();
  ASSERT_EQ(nestedProjectedRow.childrenCount(), 1);
  EXPECT_EQ(nestedProjectedRow.nameAt(0), "new_value");
}

} // namespace facebook::nimble::serde
