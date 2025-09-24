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
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Constants.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"

#include <vector>

using namespace facebook;
using namespace facebook::nimble::test;

namespace facebook::nimble::test {

class TestCompressionPolicy : public nimble::CompressionPolicy {
 public:
  explicit TestCompressionPolicy(
      nimble::CompressionType compressionType,
      uint64_t minCompressionSize) {
    EXPECT_TRUE(
        compressionType == nimble::CompressionType::Zstd ||
        compressionType == nimble::CompressionType::MetaInternal);

    compressionInfo_ = {
        .compressionType = compressionType,
        .minCompressionSize = minCompressionSize};

    compressionInfo_.parameters.zstd.compressionLevel = 3;
    compressionInfo_.parameters.metaInternal.compressionLevel = 4;
    compressionInfo_.parameters.metaInternal.decompressionLevel = 2;
    compressionInfo_.parameters.metaInternal.useManagedCompression = 1; // On
  }

  nimble::CompressionInformation compression() const override {
    return compressionInfo_;
  }

  virtual bool shouldAccept(
      nimble::CompressionType /* compressionType */,
      uint64_t /* uncompressedSize */,
      uint64_t /* compressedSize */) const override {
    return true;
  }

 private:
  nimble::CompressionInformation compressionInfo_;
};

template <typename T>
void assertMinCompressibleSizeMetaInternal(
    const nimble::CompressionType compressionType,
    const uint32_t expectedMinCompressibleBytes) {
  const auto pool =
      facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  bool hitUncompressedBetter = false;
  bool hitCompressedBetter = false;
  const uint32_t itemSize = std::is_same<T, std::string>::value ? 1 : sizeof(T);

  for (uint32_t i = 1; i < 100; ++i) {
    const uint32_t uncompressedSize = itemSize * i;

    // assumption is that the data with all bytes being equal is the best case
    // for compression
    std::vector<char> data(uncompressedSize);

    TestCompressionPolicy compressionPolicy{
        compressionType, expectedMinCompressibleBytes};
    nimble::CompressionEncoder<T> compressionEncoder{
        *pool,
        compressionPolicy,
        nimble::TypeTraits<T>::dataType,
        {data.data(), uncompressedSize}};

    // ZStd compressor returns uncompressed data if the input is too small,
    // MetInternal returns compressed data even if the input is too small.
    const bool expectCompressedBetter =
        uncompressedSize >= expectedMinCompressibleBytes &&
        compressionEncoder.compressionType() == compressionType;
    const auto compressedSize = compressionEncoder.getSize();
    EXPECT_TRUE(
        compressionEncoder.compressionType() == compressionType ||
        compressionEncoder.compressionType() == CompressionType::Uncompressed);

    if (expectCompressedBetter) {
      EXPECT_GT(uncompressedSize, compressedSize);
      hitCompressedBetter = true;
    } else {
      EXPECT_LE(uncompressedSize, compressedSize);
      hitUncompressedBetter = true;
    }
  }

  EXPECT_TRUE(hitUncompressedBetter);
  EXPECT_TRUE(hitCompressedBetter);
}
} // namespace facebook::nimble::test

template <typename C>
class CompressionTests : public ::testing::Test {};

#define TYPES int8_t, int16_t, int32_t, int64_t, double, float, std::string
using TestTypes = ::testing::Types<TYPES>;

TYPED_TEST_CASE(CompressionTests, TestTypes);

TYPED_TEST(CompressionTests, MinCompressibleSizeMetaInternal) {
  using T = TypeParam;
  assertMinCompressibleSizeMetaInternal<T>(
      nimble::CompressionType::MetaInternal,
      nimble::kMetaInternalMinCompressionSize + 20);
}

TYPED_TEST(CompressionTests, MinCompressibleSizeZstd) {
  using T = TypeParam;
  assertMinCompressibleSizeMetaInternal<T>(
      nimble::CompressionType::Zstd, nimble::kZstdMinCompressionSize);
}

TEST(CompressionTests, VerifyDefaultMinCompressionSize) {
  nimble::CompressionOptions compressionOptions{};
  EXPECT_EQ(
      compressionOptions.internalMinCompressionSize,
      nimble::kMetaInternalMinCompressionSize);
  EXPECT_EQ(
      compressionOptions.zstdMinCompressionSize,
      nimble::kZstdMinCompressionSize);
}

TEST(CompressionTests, MinCompresssionSizeIsApplied) {
  const auto pool =
      facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  const auto compressionType = nimble::CompressionType::MetaInternal;
  const uint32_t uncompressedSize = 100;
  char* data = new char[uncompressedSize];
  std::memset(data, 0, uncompressedSize);

  {
    // make minCompressionSize slightly smaller than data to apply compression
    TestCompressionPolicy policy{compressionType, uncompressedSize - 1};
    nimble::CompressionEncoder<std::string> encoder{
        *pool, policy, nimble::DataType::String, {data, uncompressedSize}};
    EXPECT_EQ(encoder.compressionType(), compressionType);
    EXPECT_GT(uncompressedSize, encoder.getSize());
  }

  {
    // make minCompressionSize same as the data size to apply compression
    TestCompressionPolicy policy{compressionType, uncompressedSize};
    nimble::CompressionEncoder<std::string> encoder{
        *pool, policy, nimble::DataType::String, {data, uncompressedSize}};
    EXPECT_EQ(encoder.compressionType(), compressionType);
    EXPECT_GT(uncompressedSize, encoder.getSize());
  }

  {
    // make minCompressionSize slightly larger than data to skip compression
    TestCompressionPolicy policy{compressionType, uncompressedSize + 1};
    nimble::CompressionEncoder<std::string> encoder{
        *pool, policy, nimble::DataType::String, {data, uncompressedSize}};
    EXPECT_EQ(encoder.compressionType(), nimble::CompressionType::Uncompressed);
  }
}
