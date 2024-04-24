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
#include <glog/logging.h>
#include "common/files/FileUtil.h"
#include "common/init/light.h"
#include "dwio/api/DwioConfig.h"
#include "dwio/common/filesystem/FileSystem.h"
#include "dwio/nimble/velox/VeloxReader.h"
#include "dwio/utils/FeatureFlatteningUtils.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/experimental/FunctionScheduler.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"

DEFINE_string(file, "", "Input file to read");
DEFINE_bool(
    memory_mode,
    false,
    "Should file be loaded and served from memory?");
DEFINE_bool(
    shared_memory_pool,
    false,
    "Should we use single memory pool for all readers?");
DEFINE_uint32(concurrency, 16, "Number of reader to use in parallel");
DEFINE_uint32(read_count, 500, "Total number of file reads to perform");
DEFINE_uint32(batch_size, 32, "Batch size to use when reading from file");
DEFINE_uint32(feature_shuffle_seed, 140185, "Seed used to shuffle features");
DEFINE_uint32(
    feature_percentage,
    20,
    "Percentage of features to use (out of 100)");

using namespace ::facebook;

namespace {
class Reader {
 public:
  virtual ~Reader() = default;

  virtual bool next(uint32_t count, velox::VectorPtr& vector) = 0;
};

class ReaderFactory {
 public:
  explicit ReaderFactory(std::shared_ptr<velox::ReadFile> readFile)
      : readFile_{std::move(readFile)} {
    NIMBLE_ASSERT(readFile_->size() > 2, "Invalid read file size.");
    uint16_t magicNumber;
    readFile_->pread(readFile_->size() - 2, 2, &magicNumber);
    format_ = (magicNumber == 0xA1FA) ? velox::dwio::common::FileFormat::NIMBLE
                                      : velox::dwio::common::FileFormat::DWRF;
    auto columnFeatures =
        dwio::utils::feature_flattening::extractFeatureNames(readFile_);
    features_.reserve(columnFeatures.size());
    for (const auto& pair : columnFeatures) {
      std::vector<int32_t> features{pair.second.cbegin(), pair.second.cend()};
      std::sort(features.begin(), features.end());
      std::mt19937 gen;
      gen.seed(FLAGS_feature_shuffle_seed);
      std::shuffle(features.begin(), features.end(), gen);
      features.resize(features.size() * FLAGS_feature_percentage / 100);
      LOG(INFO) << "Loading " << features.size() << " features for column "
                << pair.first;
      features_.insert({pair.first, std::move(features)});
    }
  }

  std::unique_ptr<Reader> create(velox::memory::MemoryPool& memoryPool);

 private:
  std::shared_ptr<velox::ReadFile> readFile_;
  velox::dwio::common::FileFormat format_;
  folly::F14FastMap<std::string, std::vector<int32_t>> features_;
};

class NimbleReader : public Reader {
 public:
  NimbleReader(
      velox::memory::MemoryPool& memoryPool,
      std::shared_ptr<velox::ReadFile> readFile,
      const folly::F14FastMap<std::string, std::vector<int32_t>>& features) {
    nimble::VeloxReadParams params;
    for (const auto& feature : features) {
      if (feature.second.size() == 0) {
        continue;
      }

      params.readFlatMapFieldAsStruct.insert(feature.first);
      auto& target = params.flatMapFeatureSelector[feature.first];

      std::transform(
          feature.second.begin(),
          feature.second.end(),
          std::inserter(target.features, target.features.end()),
          [](auto f) { return folly::to<std::string>(f); });
    }
    reader_ = std::make_unique<nimble::VeloxReader>(
        memoryPool, readFile, /* selector */ nullptr, std::move(params));
  }

  bool next(uint32_t count, velox::VectorPtr& vector) override {
    return reader_->next(count, vector);
  }

 private:
  std::shared_ptr<velox::ReadFile> file_;
  std::unique_ptr<nimble::VeloxReader> reader_;
};

class DwrfReader : public Reader {
 public:
  DwrfReader(
      velox::memory::MemoryPool& memoryPool,
      std::shared_ptr<velox::ReadFile> readFile,
      const folly::F14FastMap<std::string, std::vector<int32_t>>& features) {
    velox::dwio::common::ReaderOptions readerOptions{&memoryPool};
    reader_ = std::make_unique<velox::dwrf::DwrfReader>(
        readerOptions,
        std::make_unique<velox::dwio::common::BufferedInput>(
            std::move(readFile), readerOptions.getMemoryPool()));

    auto& schema = reader_->rowType();
    velox::dwio::common::RowReaderOptions rowReaderOptions;
    std::vector<std::string> projection;
    std::vector<std::string> mapAsStruct;
    for (auto& col : schema->names()) {
      if (features.count(col) == 0 || features.at(col).size() == 0) {
        projection.push_back(col);
      } else {
        std::string joinedFeatures;
        folly::join(",", features.at(col), joinedFeatures);
        projection.push_back(fmt::format("{}#[{}]", col, joinedFeatures));
        mapAsStruct.push_back(fmt::format("{}#[{}]", col, joinedFeatures));
      }
    }
    auto cs = std::make_shared<velox::dwio::common::ColumnSelector>(
        schema, projection);
    rowReaderOptions.select(cs);
    rowReaderOptions.setFlatmapNodeIdsAsStruct(
        facebook::dwio::api::getNodeIdToSelectedKeysMap(*cs, mapAsStruct));
    rowReaderOptions.setReturnFlatVector(true);

    rowReader_ = reader_->createRowReader(rowReaderOptions);
  }

  bool next(uint32_t count, velox::VectorPtr& vector) override {
    return rowReader_->next(count, vector);
  }

 private:
  std::unique_ptr<velox::dwrf::DwrfReader> reader_;
  std::unique_ptr<velox::dwio::common::RowReader> rowReader_;
};

std::unique_ptr<Reader> ReaderFactory::create(
    velox::memory::MemoryPool& memoryPool) {
  if (format_ == velox::dwio::common::FileFormat::NIMBLE) {
    return std::make_unique<NimbleReader>(memoryPool, readFile_, features_);
  } else {
    return std::make_unique<DwrfReader>(memoryPool, readFile_, features_);
  }
}
} // namespace

int main(int argc, char* argv[]) {
  auto init = facebook::init::InitFacebookLight{&argc, &argv};

  std::shared_ptr<velox::ReadFile> readFile;
  if (FLAGS_memory_mode) {
    std::string content;
    files::FileUtil::readFileToString(FLAGS_file, &content);
    readFile = std::make_shared<velox::InMemoryReadFile>(content);
  } else {
    readFile = std::make_shared<velox::LocalReadFile>(FLAGS_file);
  }

  folly::FunctionScheduler scheduler;
  std::atomic<uint64_t> rowCount = 0;
  std::atomic<uint64_t> completedCount = 0;
  std::atomic<uint64_t> inflightCount = 0;
  scheduler.addFunction(
      [&]() {
        LOG(INFO) << "Rows per second: " << rowCount
                  << ", Completed Reader Count: " << completedCount
                  << ", In Flight Readers: " << inflightCount;
        rowCount = 0;
      },
      std::chrono::seconds(1),
      "Counts");

  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      FLAGS_concurrency,
      std::make_unique<folly::LifoSemMPMCQueue<
          folly::CPUThreadPoolExecutor::CPUTask,
          folly::QueueBehaviorIfFull::BLOCK>>(FLAGS_concurrency),
      std::make_shared<folly::NamedThreadFactory>("reader"));

  std::shared_ptr<velox::memory::MemoryPool> sharedMemoryPool =
      velox::memory::deprecatedAddDefaultLeafMemoryPool();

  ReaderFactory readerFactory{readFile};

  scheduler.start();

  for (auto i = 0; i < FLAGS_read_count; ++i) {
    executor->add([&]() {
      std::shared_ptr<velox::memory::MemoryPool> localMemoryPool = nullptr;
      if (FLAGS_shared_memory_pool) {
        localMemoryPool = sharedMemoryPool;
      } else {
        localMemoryPool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
      }

      auto reader = readerFactory.create(*localMemoryPool);
      ++inflightCount;
      velox::VectorPtr result;
      while (reader->next(FLAGS_batch_size, result)) {
        rowCount += result->size();
      }

      --inflightCount;
      ++completedCount;
    });
  }

  executor->join();

  return 0;
}
