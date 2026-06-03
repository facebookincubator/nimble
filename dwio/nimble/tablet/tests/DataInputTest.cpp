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
#include "dwio/nimble/tablet/DataInput.h"

#include <gtest/gtest.h>
#include <random>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/tests/GTestUtils.h"

#include "folly/executors/CPUThreadPoolExecutor.h"
#include "velox/common/file/File.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TestValue.h"

namespace facebook::nimble {
namespace {

struct OptionParams {
  uint64_t alignment;
  int32_t maxCoalesceDistance;
  int64_t maxCoalesceBytes;
  int32_t minIoGroupsPerTask;

  std::string toString() const {
    return fmt::format(
        "align{}_dist{}_bytes{}_batch{}",
        alignment,
        maxCoalesceDistance,
        maxCoalesceBytes,
        minIoGroupsPerTask);
  }
};

class DataInputTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    velox::memory::MemoryManager::testingSetInstance({});
    velox::common::testutil::TestValue::enable();
  }

  void SetUp() override {
    pool_ = velox::memory::memoryManager()->addLeafPool("DataInputTest");
    executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(2);
    ioStats_ = std::make_shared<velox::io::IoStatistics>();
  }

  std::unique_ptr<velox::InMemoryReadFile> createFile(
      const std::string& content) {
    return std::make_unique<velox::InMemoryReadFile>(content);
  }

  DirectDataInput::Options makeOptions(
      uint64_t alignment = 1,
      int32_t maxCoalesceDistance = 1'000'000,
      int64_t maxCoalesceBytes = 128 << 20) {
    DirectDataInput::Options options;
    options.pool = pool_.get();
    options.executor = executor_.get();
    options.ioStats = ioStats_;
    options.alignment = alignment;
    options.maxCoalesceDistance = maxCoalesceDistance;
    options.maxCoalesceBytes = maxCoalesceBytes;
    return options;
  }

  DirectDataInput::Options makeOptions(const OptionParams& params) {
    DirectDataInput::Options options;
    options.pool = pool_.get();
    options.executor = executor_.get();
    options.ioStats = ioStats_;
    options.alignment = params.alignment;
    options.maxCoalesceDistance = params.maxCoalesceDistance;
    options.maxCoalesceBytes = params.maxCoalesceBytes;
    options.minIoGroupsPerTask = params.minIoGroupsPerTask;
    return options;
  }

  static std::string_view refData(const DataInput::BufferRef& ref) {
    return {ref.data, ref.length};
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<velox::io::IoStatistics> ioStats_;
};

// Parameterized test fixture for running functional tests across
// different option combinations.
class DataInputParamTest : public DataInputTest,
                           public ::testing::WithParamInterface<OptionParams> {
};

TEST_P(DataInputParamTest, singleGroup) {
  // Use alignment-friendly size (multiple of 4096).
  std::string data(102'400, '\0');
  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  auto file = createFile(data);

  // Each scenario is a list of {offset, length} regions in one group.
  using Regions = std::vector<std::pair<uint64_t, uint64_t>>;
  std::vector<Regions> scenarios = {
      {{100, 200}},
      {{5'000, 500}, {1'000, 300}, {3'000, 200}},
      {{300, 50}, {100, 50}, {200, 50}},
      {{0, 100}, {50'000, 100}, {98'000, 100}},
      {{0, 1}},
      {{0, 4'096}},
  };

  for (size_t scenario = 0; scenario < scenarios.size(); ++scenario) {
    const auto& regions = scenarios[scenario];
    std::string desc;
    for (const auto& [offset, length] : regions) {
      desc += fmt::format("[{},{}] ", offset, length);
    }
    SCOPED_TRACE(fmt::format("scenario {}: {}", scenario, desc));
    DirectDataInput input(file.get(), makeOptions(GetParam()));
    input.reserve(regions.size());
    input.startGroup();
    std::vector<uint32_t> indices;
    for (const auto& [offset, length] : regions) {
      indices.push_back(input.enqueue({offset, length}));
    }
    auto handle = input.load();
    for (size_t i = 0; i < regions.size(); ++i) {
      EXPECT_EQ(
          refData(input.bufferRef(indices[i])),
          data.substr(regions[i].first, regions[i].second));
    }
  }
}

TEST_P(DataInputParamTest, multipleGroups) {
  std::string data(102'400, '\0');
  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  auto file = createFile(data);

  // Each scenario is a list of groups, each group is a list of regions.
  using Groups = std::vector<std::vector<std::pair<uint64_t, uint64_t>>>;
  std::vector<Groups> scenarios = {
      {{{100, 200}}, {{500, 100}}},
      {{{500, 100}, {100, 200}, {800, 50}},
       {{50'500, 300}, {50'000, 400}, {60'000, 100}}},
      {{{100, 50}}, {{200, 50}}, {{300, 50}}},
      {{{500, 50}, {100, 50}}, {{150, 50}, {600, 50}}},
      {{{0, 100}}, {{98'000, 100}}},
  };

  for (size_t scenario = 0; scenario < scenarios.size(); ++scenario) {
    const auto& groups = scenarios[scenario];
    std::string desc;
    for (size_t g = 0; g < groups.size(); ++g) {
      desc += fmt::format("G{}: ", g);
      for (const auto& [offset, length] : groups[g]) {
        desc += fmt::format("[{},{}] ", offset, length);
      }
    }
    SCOPED_TRACE(fmt::format("scenario {}: {}", scenario, desc));
    DirectDataInput input(file.get(), makeOptions(GetParam()));
    uint32_t total = 0;
    for (const auto& g : groups) {
      total += g.size();
    }
    input.reserve(total);

    std::vector<std::pair<uint32_t, std::pair<uint64_t, uint64_t>>> indices;
    for (const auto& group : groups) {
      input.startGroup();
      for (const auto& [offset, length] : group) {
        indices.emplace_back(
            input.enqueue({offset, length}), std::make_pair(offset, length));
      }
    }
    auto handle = input.load();
    for (const auto& [idx, region] : indices) {
      EXPECT_EQ(
          refData(input.bufferRef(idx)),
          data.substr(region.first, region.second));
    }
  }
}

TEST_P(DataInputParamTest, clearAndReuse) {
  std::string data(100'000, '\0');
  for (int i = 0; i < 100'000; ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  auto file = createFile(data);

  DirectDataInput input(file.get(), makeOptions(GetParam()));

  for (int cycle = 0; cycle < 3; ++cycle) {
    SCOPED_TRACE(fmt::format("cycle {}", cycle));
    input.reserve(2);
    input.startGroup();
    const auto idx0 =
        input.enqueue({static_cast<uint64_t>(cycle * 1'000), 100});
    input.startGroup();
    const auto idx1 =
        input.enqueue({static_cast<uint64_t>(50'000 + cycle * 1'000), 100});

    auto handle = input.load();
    EXPECT_EQ(refData(input.bufferRef(idx0)), data.substr(cycle * 1'000, 100));
    EXPECT_EQ(
        refData(input.bufferRef(idx1)),
        data.substr(50'000 + cycle * 1'000, 100));
    input.clear();
  }
}

TEST_P(DataInputParamTest, fuzz) {
  const size_t fileSize = 102'400;
  std::string data(fileSize, '\0');
  for (size_t i = 0; i < fileSize; ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  auto file = createFile(data);

  std::mt19937 rng(42);
  const int numIterations = 10;

  for (int iter = 0; iter < numIterations; ++iter) {
    SCOPED_TRACE(fmt::format("iteration {}", iter));
    DirectDataInput input(file.get(), makeOptions(GetParam()));

    const int numGroups = 1 + rng() % 5;
    uint32_t totalRegions = 0;
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> groups(numGroups);
    for (int group = 0; group < numGroups; ++group) {
      const int numRegions = 1 + rng() % 10;
      for (int region = 0; region < numRegions; ++region) {
        const auto maxOffset = fileSize - 4'096;
        const auto offset = rng() % maxOffset;
        const auto maxLen = std::min<uint64_t>(4'096, fileSize - offset);
        const auto length = 1 + rng() % maxLen;
        groups[group].emplace_back(offset, length);
      }
      totalRegions += groups[group].size();
    }

    input.reserve(totalRegions);
    std::vector<std::pair<uint32_t, std::pair<uint64_t, uint64_t>>> indices;
    for (const auto& group : groups) {
      input.startGroup();
      for (const auto& [offset, length] : group) {
        indices.emplace_back(
            input.enqueue({offset, length}), std::make_pair(offset, length));
      }
    }

    auto handle = input.load();

    for (const auto& [idx, region] : indices) {
      EXPECT_EQ(
          refData(input.bufferRef(idx)),
          data.substr(region.first, region.second));
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    DataInputOptions,
    DataInputParamTest,
    ::testing::Values(
        OptionParams{1, 100, 128 << 20, 1},
        OptionParams{1, 100, 128 << 20, 4},
        OptionParams{1, 100, 128 << 20, 32},
        OptionParams{1, 1'000'000, 128 << 20, 1},
        OptionParams{1, 100, 1'024, 1},
        OptionParams{1, 100, 512, 4},
        OptionParams{4'096, 100, 128 << 20, 1},
        OptionParams{4'096, 100, 128 << 20, 4},
        OptionParams{4'096, 1'000'000, 128 << 20, 1}),
    [](const auto& info) { return info.param.toString(); });

// --- Non-parameterized tests ---

TEST_F(DataInputTest, alignment) {
  std::string data(16'384, '\0');
  for (int i = 0; i < 16'384; ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  auto file = createFile(data);

  struct TestParam {
    uint64_t alignment;
    uint64_t offset;
    uint64_t length;
    uint64_t expectedOverread;

    std::string debugString() const {
      return fmt::format(
          "alignment {} offset {} length {} expectedOverread {}",
          alignment,
          offset,
          length,
          expectedOverread);
    }
  };

  auto alignDown = [](uint64_t value, uint64_t align) {
    return value & ~(align - 1);
  };
  auto alignUp = [](uint64_t value, uint64_t align) {
    return (value + align - 1) & ~(align - 1);
  };
  auto expectedOverread = [&](uint64_t align,
                              uint64_t offset,
                              uint64_t length) {
    return alignUp(offset + length, align) - alignDown(offset, align) - length;
  };

  std::vector<TestParam> testSettings = {
      // offset 4100 → alignDown=4096, alignUp(4300)=8192 → read=4096,
      // overread=8192-4096-200=3896
      {4'096, 4'100, 200, expectedOverread(4'096, 4'100, 200)},

      // offset already aligned → alignDown=4096, alignUp(4296)=8192 →
      // overread=8192-4096-200=3896
      {4'096, 4'096, 200, expectedOverread(4'096, 4'096, 200)},

      // offset 0, length 100 → alignDown=0, alignUp(100)=4096 →
      // overread=3996
      {4'096, 0, 100, expectedOverread(4'096, 0, 100)},

      // alignment=1 → no padding, overread=0.
      {1, 4'100, 200, 0},

      // alignment=512, offset 600 → alignDown=512, alignUp(800)=1024 →
      // overread=1024-512-200=312
      {512, 600, 200, expectedOverread(512, 600, 200)},
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto stats = std::make_shared<velox::io::IoStatistics>();
    auto options = makeOptions(testData.alignment);
    options.ioStats = stats;

    DirectDataInput input(file.get(), options);
    input.reserve(1);
    input.startGroup();
    const auto idx = input.enqueue({testData.offset, testData.length});
    auto handle = input.load();

    EXPECT_EQ(input.bufferRef(idx).length, testData.length);
    EXPECT_EQ(
        refData(input.bufferRef(idx)),
        data.substr(testData.offset, testData.length));
    EXPECT_EQ(stats->rawBytesRead(), testData.length);
    EXPECT_EQ(stats->rawOverreadBytes(), testData.expectedOverread);
    EXPECT_EQ(stats->read().count(), 1);
  }
}

TEST_F(DataInputTest, intraGroupCoalescing) {
  std::string data(10'000, '\0');
  for (int i = 0; i < 10'000; ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  auto file = createFile(data);

  struct TestParam {
    std::vector<std::pair<uint64_t, uint64_t>> regions;
    int32_t maxCoalesceDistance;
    uint64_t expectedReadCount;

    std::string debugString() const {
      return fmt::format(
          "regions={}, dist={}, expectedReads={}",
          regions.size(),
          maxCoalesceDistance,
          expectedReadCount);
    }
  };

  std::vector<TestParam> testSettings = {
      // Two nearby + one far → 2 reads.
      {.regions = {{100, 50}, {200, 50}, {5'000, 50}},
       .maxCoalesceDistance = 100,
       .expectedReadCount = 2},

      // All within coalesce distance → 1 read.
      {.regions = {{100, 50}, {200, 50}, {300, 50}},
       .maxCoalesceDistance = 100,
       .expectedReadCount = 1},

      // All far apart → 3 reads.
      {.regions = {{100, 50}, {5'000, 50}, {9'000, 50}},
       .maxCoalesceDistance = 10,
       .expectedReadCount = 3},

      // Unsorted input, nearby after sort → 1 read.
      {.regions = {{300, 50}, {100, 50}, {200, 50}},
       .maxCoalesceDistance = 100,
       .expectedReadCount = 1},

      // Single region → 1 read.
      {.regions = {{100, 50}},
       .maxCoalesceDistance = 100,
       .expectedReadCount = 1},
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto stats = std::make_shared<velox::io::IoStatistics>();
    auto options = makeOptions(/*alignment=*/1, testData.maxCoalesceDistance);
    options.ioStats = stats;
    DirectDataInput input(file.get(), options);

    input.reserve(testData.regions.size());
    input.startGroup();
    std::vector<std::pair<uint32_t, std::pair<uint64_t, uint64_t>>> indices;
    for (const auto& [offset, length] : testData.regions) {
      const auto idx = input.enqueue({offset, length});
      indices.emplace_back(idx, std::make_pair(offset, length));
    }

    auto handle = input.load();

    for (const auto& [idx, region] : indices) {
      EXPECT_EQ(
          refData(input.bufferRef(idx)),
          data.substr(region.first, region.second));
    }
    EXPECT_EQ(stats->read().count(), testData.expectedReadCount);
  }
}

TEST_F(DataInputTest, crossGroupCoalescing) {
  std::string data(10'000, '\0');
  for (int i = 0; i < 10'000; ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  auto file = createFile(data);

  struct TestParam {
    // Regions per group: {offset, length}.
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> groups;
    int32_t maxCoalesceDistance;
    uint64_t expectedReadCount;

    std::string debugString() const {
      return fmt::format(
          "groups={}, dist={}, expectedReads={}",
          groups.size(),
          maxCoalesceDistance,
          expectedReadCount);
    }
  };

  std::vector<TestParam> testSettings = {
      // Groups in file order, nearby offsets → coalesced into 1 read.
      {.groups = {{{100, 50}}, {{200, 50}}},
       .maxCoalesceDistance = 1'000,
       .expectedReadCount = 1},

      // Groups in file order but far apart → 2 reads.
      {.groups = {{{100, 50}}, {{5'000, 50}}},
       .maxCoalesceDistance = 100,
       .expectedReadCount = 2},

      // Groups with interleaved offsets: after intra-group sort,
      // concatenation is [100, 500, 150, 600] — not globally sorted.
      // 500→150 goes backward so coalescing breaks → 2+ reads.
      {.groups = {{{500, 50}, {100, 50}}, {{150, 50}, {600, 50}}},
       .maxCoalesceDistance = 1'000,
       .expectedReadCount = 2},

      // Three groups, all nearby → 1 read.
      {.groups = {{{100, 50}}, {{200, 50}}, {{300, 50}}},
       .maxCoalesceDistance = 1'000,
       .expectedReadCount = 1},

      // Three groups, first two nearby, third far → 2 reads.
      {.groups = {{{100, 50}}, {{200, 50}}, {{5'000, 50}}},
       .maxCoalesceDistance = 100,
       .expectedReadCount = 2},
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto stats = std::make_shared<velox::io::IoStatistics>();
    auto options = makeOptions(/*alignment=*/1, testData.maxCoalesceDistance);
    options.ioStats = stats;
    DirectDataInput input(file.get(), options);

    std::vector<std::pair<uint32_t, std::pair<uint64_t, uint64_t>>> indices;
    uint32_t totalRegions = 0;
    for (const auto& group : testData.groups) {
      totalRegions += group.size();
    }
    input.reserve(totalRegions);

    for (const auto& group : testData.groups) {
      input.startGroup();
      for (const auto& [offset, length] : group) {
        const auto idx = input.enqueue({offset, length});
        indices.emplace_back(idx, std::make_pair(offset, length));
      }
    }

    auto handle = input.load();

    for (const auto& [idx, region] : indices) {
      EXPECT_EQ(
          refData(input.bufferRef(idx)),
          data.substr(region.first, region.second));
    }
    EXPECT_EQ(stats->read().count(), testData.expectedReadCount);
  }
}

TEST_F(DataInputTest, clearAndReuse) {
  std::string data(1'000, 'A');
  auto file = createFile(data);

  DirectDataInput input(file.get(), makeOptions());

  input.reserve(1);
  input.startGroup();
  const auto idx0 = input.enqueue({0, 100});
  auto loadedData = input.load();
  EXPECT_EQ(refData(input.bufferRef(idx0)), data.substr(0, 100));

  input.clear();
  input.reserve(1);
  input.startGroup();
  const auto idx1 = input.enqueue({500, 100});
  loadedData = input.load();
  EXPECT_EQ(refData(input.bufferRef(idx1)), data.substr(500, 100));
}

TEST_F(DataInputTest, bufferRefLifetimeAfterClear) {
  std::string data(1'000, '\0');
  for (int i = 0; i < 1'000; ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  auto file = createFile(data);

  DirectDataInput input(file.get(), makeOptions());
  input.reserve(1);
  input.startGroup();
  const auto idx = input.enqueue({100, 200});
  auto loadedData = input.load();

  // Copy the BufferRef before clear — loadedData handle keeps memory alive.
  const auto savedRef = input.bufferRef(idx);
  const auto expected = data.substr(100, 200);

  input.clear();

  EXPECT_EQ(refData(savedRef), expected);
}

TEST_F(DataInputTest, emptyLoad) {
  const std::string data(100, 'X');
  auto file = createFile(data);

  DirectDataInput input(file.get(), makeOptions());

  // Load without startGroup() should fail.
  NIMBLE_ASSERT_THROW(input.load(), "");

  // Load with empty group (no enqueue) should fail.
  input.startGroup();
  NIMBLE_ASSERT_THROW(input.load(), "No regions enqueued");
}

TEST_F(DataInputTest, emptyGroup) {
  std::string data(1'000, '\0');
  for (int i = 0; i < 1'000; ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  auto file = createFile(data);

  DirectDataInput input(file.get(), makeOptions());
  input.reserve(1);

  // Enqueue without startGroup() should fail.
  NIMBLE_ASSERT_THROW(input.enqueue({100, 50}), "");

  // Empty group followed by startGroup() should fail.
  input.startGroup();
  NIMBLE_ASSERT_THROW(input.startGroup(), "Previous group is empty");

  // Enqueue into the first group, then start a new group.
  const auto idx = input.enqueue({100, 50});
  input.startGroup();
  const auto idx2 = input.enqueue({500, 50});
  auto loadedData = input.load();

  EXPECT_EQ(refData(input.bufferRef(idx)), data.substr(100, 50));
  EXPECT_EQ(refData(input.bufferRef(idx2)), data.substr(500, 50));
}

TEST_F(DataInputTest, handleOutlivesDataInput) {
  std::string data(1'000, '\0');
  for (int i = 0; i < 1'000; ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  auto file = createFile(data);

  const auto statsBefore = pool_->stats();

  DataInput::Handle handle;
  DataInput::BufferRef savedRef;
  const auto expected = data.substr(200, 100);
  {
    DirectDataInput input(file.get(), makeOptions());
    input.reserve(1);
    input.startGroup();
    const auto idx = input.enqueue({200, 100});
    handle = input.load();
    savedRef = input.bufferRef(idx);
    EXPECT_EQ(refData(savedRef), expected);

    const auto statsAfterLoad = pool_->stats();
    EXPECT_EQ(statsAfterLoad.numAllocs, statsBefore.numAllocs + 1);
  }

  // DirectDataInput is destroyed, but handle keeps the buffer alive.
  const auto statsAfterDestroy = pool_->stats();
  EXPECT_EQ(statsAfterDestroy.numFrees, statsBefore.numFrees);
  EXPECT_EQ(std::string_view(savedRef.data, savedRef.length), expected);

  handle.reset();

  // After releasing the handle, memory is freed.
  const auto statsAfterRelease = pool_->stats();
  EXPECT_EQ(statsAfterRelease.numFrees, statsBefore.numFrees + 1);
}

TEST_F(DataInputTest, singleRequest) {
  std::string data(4'096, '\0');
  for (int i = 0; i < 4'096; ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  auto file = createFile(data);

  struct TestParam {
    uint64_t offset;
    uint64_t length;
    std::string debugString() const {
      return fmt::format("offset {} length {}", offset, length);
    }
  };

  std::vector<TestParam> testSettings = {
      {3, 1},
      {0, 1},
      {0, 4'096},
      {100, 200},
      {4'000, 96},
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    DirectDataInput input(file.get(), makeOptions());
    input.reserve(1);
    input.startGroup();
    const auto idx = input.enqueue({testData.offset, testData.length});
    auto handle = input.load();
    EXPECT_EQ(
        refData(input.bufferRef(idx)),
        data.substr(testData.offset, testData.length));
  }
}

TEST_F(DataInputTest, noCoalesceDistantRequests) {
  std::string data(100'000, '\0');
  for (int i = 0; i < 100'000; ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  auto file = createFile(data);

  DirectDataInput input(
      file.get(), makeOptions(/*alignment=*/1, /*maxCoalesceDistance=*/10));
  input.reserve(2);
  input.startGroup();
  input.enqueue({100, 50});
  input.enqueue({50'000, 50});

  auto handle = input.load();

  // With 10-byte coalesce distance, two distant regions should not merge.
  EXPECT_EQ(ioStats_->read().count(), 2);
  EXPECT_EQ(ioStats_->rawBytesRead(), 100);
  EXPECT_LT(ioStats_->rawOverreadBytes(), 100);
}

TEST_F(DataInputTest, maxCoalesceDistanceCap) {
  // Verify that maxCoalesceDistance is capped at kMaxCoalesceDistance.
  // Two regions beyond kMaxCoalesceDistance apart should not be coalesced.
  std::string data(1'000'000, '\0');
  for (int i = 0; i < 1'000'000; ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  auto file = createFile(data);

  auto stats = std::make_shared<velox::io::IoStatistics>();
  DirectDataInput::Options options;
  options.pool = pool_.get();
  options.executor = executor_.get();
  options.ioStats = stats;
  options.maxCoalesceDistance = 1'000'000;

  DirectDataInput input(file.get(), options);
  input.reserve(2);
  input.startGroup();
  const auto idx0 = input.enqueue({0, 10});
  const auto idx1 = input.enqueue({500'000, 10});
  auto handle = input.load();

  EXPECT_EQ(refData(input.bufferRef(idx0)), data.substr(0, 10));
  EXPECT_EQ(refData(input.bufferRef(idx1)), data.substr(500'000, 10));
  EXPECT_EQ(stats->rawBytesRead(), 20);
  // Gap of 500K exceeds kMaxCoalesceDistance (256K), so two separate IO groups.
  EXPECT_EQ(stats->read().count(), 2);
  EXPECT_LT(stats->rawOverreadBytes(), 1'000);
}

TEST_F(DataInputTest, invalidOptions) {
  std::string data(100, 'X');
  auto file = createFile(data);

  auto validOptions = makeOptions();

  auto nullPool = validOptions;
  nullPool.pool = nullptr;
  NIMBLE_ASSERT_THROW(DirectDataInput(file.get(), nullPool), "");

  auto nullExecutor = validOptions;
  nullExecutor.executor = nullptr;
  NIMBLE_ASSERT_THROW(DirectDataInput(file.get(), nullExecutor), "");

  auto nullIoStats = validOptions;
  nullIoStats.ioStats = nullptr;
  NIMBLE_ASSERT_THROW(DirectDataInput(file.get(), nullIoStats), "");

  NIMBLE_ASSERT_THROW(DirectDataInput(nullptr, validOptions), "");

  auto badAlignment = validOptions;
  badAlignment.alignment = 3;
  NIMBLE_ASSERT_THROW(DirectDataInput(file.get(), badAlignment), "power of 2");
}

DEBUG_ONLY_TEST_F(DataInputTest, readError) {
  std::string data(10'000, '\0');
  for (int i = 0; i < 10'000; ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  auto file = createFile(data);

  DirectDataInput input(
      file.get(), makeOptions(/*alignment=*/1, /*maxCoalesceDistance=*/10));
  input.reserve(2);
  input.startGroup();
  input.enqueue({100, 50});
  input.startGroup();
  input.enqueue({5'000, 50});

  std::atomic_bool injected{false};
  SCOPED_TESTVALUE_SET(
      "facebook::nimble::DirectDataInput::executeIoGroups",
      std::function<void(DirectDataInput*)>([&](DirectDataInput*) {
        if (!injected.exchange(true)) {
          NIMBLE_FAIL("Injected pread failure");
        }
      }));

  NIMBLE_ASSERT_THROW(input.load(), "Injected pread failure");
}

} // namespace
} // namespace facebook::nimble
