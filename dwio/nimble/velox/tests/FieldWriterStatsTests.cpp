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

#include "dwio/nimble/velox/VeloxWriter.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace facebook;
using namespace facebook::nimble;

namespace facebook::nimble {
class FieldWriterStatsTests : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::SharedArbitrator::registerFactory();
    velox::memory::MemoryManagerOptions options;
    options.arbitratorKind = "SHARED";
    velox::memory::MemoryManager::testingSetInstance(options);
  }

  void SetUp() override {
    rootPool_ = velox::memory::memoryManager()->addRootPool("default_root");
    leafPool_ = rootPool_->addLeafChild("default_leaf");
  }

  const std::unordered_map<offset_size, FieldWriterContext::ColumnStats>&
  getStats(const velox::VectorPtr& vector) {
    std::string file;
    auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
    nimble::VeloxWriter writer(
        *rootPool_, vector->type(), std::move(writeFile), {});
    writer.write(vector);
    writer.close();
    return writer.getColumnStats();
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> leafPool_;
};
} // namespace facebook::nimble

TEST_F(FieldWriterStatsTests, SimpleFieldWriterStats) {
  velox::test::VectorMaker vectorMaker{leafPool_.get()};

  constexpr uint64_t expectedRawSize = sizeof(int32_t) * 20;
  auto vector = vectorMaker.rowVector(
      {"row1", "row2"},
      {
          vectorMaker.rowVector({
              vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5}),
              vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5}),
          }),
          vectorMaker.rowVector({
              vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5}),
              vectorMaker.flatVector<int32_t>({1, 2, 3, 4, 5}),
          }),
      });
  LOG(INFO) << vector->toString();
  LOG(INFO) << "vector: " << vector->toString(0, 100);

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriter writer(
      *rootPool_, vector->type(), std::move(writeFile), {});
  writer.write(vector);
  writer.close();
  auto stats = writer.getColumnStats();

  EXPECT_EQ(expectedRawSize, sizeof(int32_t) * 20);
}
