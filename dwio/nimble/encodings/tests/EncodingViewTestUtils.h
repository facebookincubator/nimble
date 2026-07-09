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
#pragma once

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "dwio/nimble/encodings/views/EncodingViewFactory.h"
#include "fmt/core.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble::test {

class EncodingViewTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  template <typename T>
  nimble::Vector<T> makeVector(std::initializer_list<T> values) {
    nimble::Vector<T> out{pool_.get()};
    out.insert(out.end(), values.begin(), values.end());
    return out;
  }

  template <typename Encoding>
  void expectReads(
      const nimble::Vector<typename Encoding::cppDataType>& values,
      const std::vector<uint32_t>& positions,
      nimble::Encoding::Options baseOptions = {}) {
    using T = typename Encoding::cppDataType;
    for (const auto useVarint : {false, true}) {
      SCOPED_TRACE(fmt::format("useVarint={}", useVarint));
      auto options = baseOptions;
      options.useVarintRowCount = useVarint;
      auto serialized = nimble::test::Encoder<Encoding>::encode(
          *buffer_, values, nimble::CompressionType::Uncompressed, options);
      auto view = nimble::createEncodingView(serialized, pool_.get(), options);
      ASSERT_NE(view, nullptr);
      for (const auto position : positions) {
        SCOPED_TRACE(fmt::format("position={}", position));
        T value;
        view->readAt(position, &value);
        EXPECT_EQ(value, values[position]);
      }
    }
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
};

} // namespace facebook::nimble::test
