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

#include "dwio/nimble/velox/selective/ChunkedDecoder.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/dwio/common/DirectBufferedInput.h"

#include <folly/executors/IOThreadPoolExecutor.h>
#include <gtest/gtest.h>

namespace facebook::nimble {

using namespace facebook::velox;

class ChunkedDecoderTestHelper {
 public:
  explicit ChunkedDecoderTestHelper(ChunkedDecoder* decoder)
      : decoder_(decoder) {
    NIMBLE_CHECK_NOT_NULL(decoder_);
  }

  bool ensureInput(int size) {
    return decoder_->ensureInput(size);
  }

  std::string_view inputData() const {
    return std::string_view(decoder_->inputData_, decoder_->inputSize_);
  }

  void advanceInputData(int size) {
    decoder_->inputData_ += size;
    decoder_->inputSize_ -= size;
  }

 private:
  ChunkedDecoder* const decoder_;
};

namespace {

class TestingSeekableInputStream : public dwio::common::SeekableInputStream {
 public:
  explicit TestingSeekableInputStream(std::vector<std::string> data)
      : data_(std::move(data)) {}

  bool Next(const void** data, int32_t* size) override {
    if (index_ > 0) {
      std::fill(data_[index_ - 1].begin(), data_[index_ - 1].end(), 0xFF);
    }
    if (index_ >= data_.size()) {
      NIMBLE_CHECK_EQ(index_, data_.size());
      return false;
    }
    *data = data_[index_].data();
    *size = data_[index_].size();
    ++index_;
    return true;
  }

  void BackUp(int32_t /*count*/) override {
    NIMBLE_UNREACHABLE();
  }

  bool SkipInt64(int64_t /*count*/) override {
    NIMBLE_UNREACHABLE();
  }

  google::protobuf::int64 ByteCount() const override {
    NIMBLE_UNREACHABLE();
  }

  void seekToPosition(dwio::common::PositionProvider& /*position*/) override {
    NIMBLE_UNREACHABLE();
  }

  std::string getName() const override {
    return "TestingSeekableInputStream";
  }

  size_t positionSize() const override {
    NIMBLE_UNREACHABLE();
  }

 private:
  std::vector<std::string> data_;
  int index_ = 0;
};

class ChunkedDecoderTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    leafPool_ = memory::MemoryManager::getInstance()->addLeafPool();
  }

  MemoryPool& pool() {
    return *leafPool_;
  }

 private:
  std::shared_ptr<MemoryPool> leafPool_;
};

TEST_F(ChunkedDecoderTest, bufferedInput) {
  auto fileIoStats = std::make_shared<velox::io::IoStatistics>();
  constexpr size_t kFileSize = 1 << 12;
  constexpr size_t kLoadQuantum = 1 << 4;

  // Set up a file that has different content every load quantum for
  // the direct input stream.
  std::string fileContent{};
  size_t filePos = 0;
  size_t segmentLength;
  for (size_t segmentIdx = 0; filePos < kFileSize; ++segmentIdx) {
    segmentLength = std::min(kLoadQuantum, kFileSize - filePos);
    fileContent.append(segmentLength, 'a' + segmentIdx);
    filePos += segmentLength;
  }

  auto file = std::make_shared<InMemoryReadFile>(fileContent);
  dwio::common::ReaderOptions readerOpts{&pool()};
  readerOpts.setLoadQuantum(kLoadQuantum);
  auto executor = std::make_unique<folly::IOThreadPoolExecutor>(10, 10);
  auto input = std::make_unique<dwio::common::DirectBufferedInput>(
      file,
      dwio::common::MetricsLog::voidLog(),
      StringIdLease{},
      nullptr,
      StringIdLease{},
      fileIoStats,
      std::make_shared<filesystems::File::IoStats>(),
      executor.get(),
      readerOpts);

  auto chunkedDecoder = std::make_unique<nimble::ChunkedDecoder>(
      input->read(0, kFileSize, velox::dwio::common::LogType::TEST),
      false,
      pool());
  ChunkedDecoderTestHelper helper(chunkedDecoder.get());
  helper.ensureInput(kFileSize);
  ASSERT_EQ(helper.inputData(), fileContent);
}

TEST_F(ChunkedDecoderTest, ensureInput) {
  std::vector<std::string> data = {"a", "b", "c", "d", "e", "fg", "h"};
  ChunkedDecoder decoder(
      std::make_unique<TestingSeekableInputStream>(std::move(data)),
      false,
      pool());
  ChunkedDecoderTestHelper helper(&decoder);
  auto checkNext = [&](const std::string& expected) {
    helper.ensureInput(expected.size());
    ASSERT_GE(helper.inputData().size(), expected.size());
    ASSERT_EQ(helper.inputData().substr(0, expected.size()), expected);
    helper.advanceInputData(expected.size());
  };
  checkNext("ab");
  checkNext("c");
  checkNext("de");
  checkNext("f");
  checkNext("gh");
  ASSERT_TRUE(helper.inputData().empty());
  ASSERT_FALSE(helper.ensureInput(1));
}

} // namespace

} // namespace facebook::nimble
