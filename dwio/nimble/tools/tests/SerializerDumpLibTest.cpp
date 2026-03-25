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

#include "dwio/nimble/serializer/Serializer.h"
#include "dwio/nimble/tools/SerializerDumpLib.h"
#include "dwio/nimble/velox/SchemaReader.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/FlatVector.h"

using namespace facebook;
using namespace facebook::nimble;

class SerializerDumpLibTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = velox::memory::memoryManager()->addRootPool("test");
    leafPool_ = pool_->addLeafChild("test_leaf");
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::shared_ptr<velox::memory::MemoryPool> leafPool_;
};

TEST_F(SerializerDumpLibTest, BasicStats) {
  // Create a simple Row<int32_t, int64_t> schema and serialize some data.
  auto type = velox::ROW({"a", "b"}, {velox::INTEGER(), velox::BIGINT()});

  SerializerOptions options{
      .version = SerializationVersion::kCompact,
  };
  Serializer serializer{options, type, leafPool_.get()};

  // Create a vector with 10 rows.
  auto intVector =
      velox::BaseVector::create(velox::INTEGER(), 10, leafPool_.get());
  auto* flatInt = intVector->asFlatVector<int32_t>();
  for (int i = 0; i < 10; ++i) {
    flatInt->set(i, i * 100);
  }

  auto bigintVector =
      velox::BaseVector::create(velox::BIGINT(), 10, leafPool_.get());
  auto* flatBigint = bigintVector->asFlatVector<int64_t>();
  for (int i = 0; i < 10; ++i) {
    flatBigint->set(i, i * 1000L);
  }

  auto rowVector = std::make_shared<velox::RowVector>(
      leafPool_.get(),
      type,
      nullptr,
      10,
      std::vector<velox::VectorPtr>{intVector, bigintVector});

  std::string serialized(
      serializer.serialize(rowVector, OrderedRanges::of(0, rowVector->size())));

  // Get the schema for stream labeling.
  auto schema =
      SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes());

  // Create dump and add the serialized buffer.
  tools::SerializationDump dump(schema, leafPool_.get());
  dump.addSerialization(serialized);

  const auto& stats = dump.stats();
  EXPECT_EQ(stats.rowCount, 10);
  EXPECT_GT(stats.streams.size(), 0);
  EXPECT_GT(stats.encodedSize, 0);
  EXPECT_GT(stats.rawSize, 0);

  // Verify toString produces non-empty output.
  auto str = dump.toString();
  EXPECT_FALSE(str.empty());
  EXPECT_NE(str.find("Nimble encoding dump"), std::string::npos);
}

TEST_F(SerializerDumpLibTest, MultipleSerializations) {
  auto type = velox::ROW({"x"}, {velox::INTEGER()});

  SerializerOptions options{
      .version = SerializationVersion::kCompact,
  };
  Serializer serializer{options, type, leafPool_.get()};

  // Serialize two batches.
  auto makeVector = [&](int count, int offset) {
    auto vec =
        velox::BaseVector::create(velox::INTEGER(), count, leafPool_.get());
    auto* flat = vec->asFlatVector<int32_t>();
    for (int i = 0; i < count; ++i) {
      flat->set(i, offset + i);
    }
    return std::make_shared<velox::RowVector>(
        leafPool_.get(),
        type,
        nullptr,
        count,
        std::vector<velox::VectorPtr>{vec});
  };

  auto batch1 = makeVector(5, 0);
  auto batch2 = makeVector(7, 100);
  // Copy serialized data since serialize() returns a view to an internal
  // buffer that gets invalidated by the next serialize() call.
  std::string serialized1(
      serializer.serialize(batch1, OrderedRanges::of(0, batch1->size())));
  std::string serialized2(
      serializer.serialize(batch2, OrderedRanges::of(0, batch2->size())));

  auto schema =
      SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes());

  tools::SerializationDump dump(schema, leafPool_.get());
  dump.addSerializations({serialized1, serialized2});

  const auto& stats = dump.stats();
  EXPECT_EQ(stats.rowCount, 12);
}

TEST_F(SerializerDumpLibTest, Reset) {
  auto type = velox::ROW({"v"}, {velox::INTEGER()});

  SerializerOptions options{
      .version = SerializationVersion::kCompact,
  };
  Serializer serializer{options, type, leafPool_.get()};

  auto vec = velox::BaseVector::create(velox::INTEGER(), 3, leafPool_.get());
  auto* flat = vec->asFlatVector<int32_t>();
  for (int i = 0; i < 3; ++i) {
    flat->set(i, i);
  }
  auto rowVector = std::make_shared<velox::RowVector>(
      leafPool_.get(), type, nullptr, 3, std::vector<velox::VectorPtr>{vec});

  std::string serialized(
      serializer.serialize(rowVector, OrderedRanges::of(0, rowVector->size())));

  auto schema =
      SchemaReader::getSchema(serializer.schemaBuilder().schemaNodes());

  tools::SerializationDump dump(schema, leafPool_.get());
  dump.addSerialization(serialized);
  EXPECT_EQ(dump.stats().rowCount, 3);

  dump.reset();
  EXPECT_EQ(dump.stats().rowCount, 0);
  EXPECT_TRUE(dump.stats().streams.empty());
}
