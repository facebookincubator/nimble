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
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "velox/common/memory/Memory.h"

using namespace facebook;

class TrivialEncodingTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    rootPool_ =
        velox::memory::memoryManager()->addRootPool("TrivialEncodingTest");
    pool_ = rootPool_->addLeafChild("TrivialEncodingTestLeaf");
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  nimble::Vector<std::string_view> toVector(
      std::initializer_list<std::string_view> values) {
    nimble::Vector<std::string_view> result{pool_.get()};
    result.insert(result.end(), values.begin(), values.end());
    return result;
  }

  std::unique_ptr<nimble::Encoding> createEncoding(
      const nimble::Vector<std::string_view>& values,
      const nimble::Encoding::Options& options = {}) {
    stringBuffers_.clear();
    return nimble::test::Encoder<nimble::TrivialEncoding<std::string_view>>::
        createEncoding(
            *buffer_,
            values,
            [&](uint32_t totalLength) {
              auto& buffer = stringBuffers_.emplace_back(
                  velox::AlignedBuffer::allocate<char>(
                      totalLength, pool_.get()));
              return buffer->asMutable<void>();
            },
            nimble::CompressionType::Uncompressed,
            options);
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
  std::vector<velox::BufferPtr> stringBuffers_;
};
