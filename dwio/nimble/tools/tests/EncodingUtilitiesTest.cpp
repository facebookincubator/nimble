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

#include <sstream>
#include <vector>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "dwio/nimble/tools/EncodingUtilities.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble::tools::test {

using namespace facebook::nimble;

class EncodingUtilitiesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  }

  // Build a Trivial-encoded stream for uint32_t data (uncompressed).
  // Layout:
  //   byte 0: EncodingType::Trivial (0)
  //   byte 1: DataType::Uint32 (6)
  //   bytes 2-5: rowCount (uint32_t)
  //   byte 6: CompressionType::Uncompressed (0)
  //   bytes 7+: raw data (rowCount * sizeof(uint32_t))
  std::string buildTrivialUint32Stream(const std::vector<uint32_t>& values) {
    uint32_t rowCount = static_cast<uint32_t>(values.size());
    uint32_t dataSize = rowCount * sizeof(uint32_t);
    uint32_t totalSize = 6 + 1 + dataSize; // prefix + compressionType + data
    std::string buf(totalSize, '\0');
    char* pos = buf.data();
    encoding::writeChar(
        static_cast<char>(EncodingType::Trivial), pos); // byte 0
    encoding::writeChar(static_cast<char>(DataType::Uint32), pos); // byte 1
    encoding::writeUint32(rowCount, pos); // bytes 2-5
    encoding::writeChar(
        static_cast<char>(CompressionType::Uncompressed), pos); // byte 6
    for (auto v : values) {
      encoding::writeUint32(v, pos);
    }
    return buf;
  }

  // Build a Constant-encoded stream for uint32_t data.
  // Layout:
  //   byte 0: EncodingType::Constant (9)
  //   byte 1: DataType::Uint32 (6)
  //   bytes 2-5: rowCount (uint32_t)
  //   bytes 6-9: the constant value (uint32_t)
  std::string buildConstantUint32Stream(uint32_t value, uint32_t rowCount) {
    uint32_t totalSize = 6 + sizeof(uint32_t); // prefix + value
    std::string buf(totalSize, '\0');
    char* pos = buf.data();
    encoding::writeChar(
        static_cast<char>(EncodingType::Constant), pos); // byte 0
    encoding::writeChar(static_cast<char>(DataType::Uint32), pos); // byte 1
    encoding::writeUint32(rowCount, pos); // bytes 2-5
    encoding::writeUint32(value, pos); // bytes 6-9
    return buf;
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

// --- operator<< for EncodingPropertyType ---

TEST_F(EncodingUtilitiesTest, EncodingPropertyTypeStreamOperator) {
  {
    std::ostringstream ss;
    ss << EncodingPropertyType::Compression;
    EXPECT_EQ(ss.str(), "Compression");
  }
  {
    std::ostringstream ss;
    ss << EncodingPropertyType::EncodedSize;
    EXPECT_EQ(ss.str(), "Unknown");
  }
}

// --- getEncodingLabel ---

TEST_F(EncodingUtilitiesTest, GetEncodingLabelTrivial) {
  auto stream = buildTrivialUint32Stream({10, 20, 30});
  auto label = getEncodingLabel(stream);
  // Should contain "Trivial" and "Uint32"
  EXPECT_NE(label.find("Trivial"), std::string::npos);
  EXPECT_NE(label.find("Uint32"), std::string::npos);
  // Should contain "Uncompressed" since we set that compression type
  EXPECT_NE(label.find("Uncompressed"), std::string::npos);
}

TEST_F(EncodingUtilitiesTest, GetEncodingLabelConstant) {
  auto stream = buildConstantUint32Stream(42, 100);
  auto label = getEncodingLabel(stream);
  EXPECT_NE(label.find("Constant"), std::string::npos);
  EXPECT_NE(label.find("Uint32"), std::string::npos);
}

// --- traverseEncodings ---

TEST_F(EncodingUtilitiesTest, TraverseEncodingsTrivialVisitor) {
  auto stream = buildTrivialUint32Stream({1, 2, 3});
  int visitCount = 0;
  EncodingType visitedType = EncodingType::Constant; // init to something else

  traverseEncodings(
      stream,
      [&](EncodingType encodingType,
          DataType dataType,
          uint32_t level,
          uint32_t index,
          const std::string& /* nestedEncodingName */,
          std::unordered_map<EncodingPropertyType, EncodingProperty> properties)
          -> bool {
        ++visitCount;
        visitedType = encodingType;
        EXPECT_EQ(dataType, DataType::Uint32);
        EXPECT_EQ(level, 0u);
        EXPECT_EQ(index, 0u);
        // Should have EncodedSize property
        EXPECT_NE(
            properties.find(EncodingPropertyType::EncodedSize),
            properties.end());
        // Should have Compression property for Trivial
        EXPECT_NE(
            properties.find(EncodingPropertyType::Compression),
            properties.end());
        return true;
      });

  EXPECT_EQ(visitCount, 1);
  EXPECT_EQ(visitedType, EncodingType::Trivial);
}

TEST_F(EncodingUtilitiesTest, TraverseEncodingsConstantVisitor) {
  auto stream = buildConstantUint32Stream(99, 50);
  int visitCount = 0;

  traverseEncodings(
      stream,
      [&](EncodingType encodingType,
          DataType dataType,
          uint32_t level,
          uint32_t /* index */,
          const std::string& /* nestedEncodingName */,
          std::unordered_map<EncodingPropertyType, EncodingProperty> properties)
          -> bool {
        ++visitCount;
        EXPECT_EQ(encodingType, EncodingType::Constant);
        EXPECT_EQ(dataType, DataType::Uint32);
        EXPECT_EQ(level, 0u);
        // Constant encoding should have EncodedSize but no Compression
        EXPECT_NE(
            properties.find(EncodingPropertyType::EncodedSize),
            properties.end());
        EXPECT_EQ(
            properties.find(EncodingPropertyType::Compression),
            properties.end());
        return true;
      });

  EXPECT_EQ(visitCount, 1);
}

TEST_F(EncodingUtilitiesTest, TraverseEncodingsEarlyStop) {
  auto stream = buildTrivialUint32Stream({1, 2, 3});
  int visitCount = 0;

  traverseEncodings(
      stream,
      [&](EncodingType /* encodingType */,
          DataType /* dataType */,
          uint32_t /* level */,
          uint32_t /* index */,
          const std::string& /* nestedEncodingName */,
          const std::unordered_map<EncodingPropertyType, EncodingProperty>&
          /* properties */) -> bool {
        ++visitCount;
        return false; // stop traversal
      });

  EXPECT_EQ(visitCount, 1);
}

TEST_F(EncodingUtilitiesTest, TraverseEncodingsEncodedSizeValue) {
  auto stream = buildTrivialUint32Stream({10, 20});
  std::string encodedSize;

  traverseEncodings(
      stream,
      [&](EncodingType /* encodingType */,
          DataType /* dataType */,
          uint32_t /* level */,
          uint32_t /* index */,
          const std::string& /* nestedEncodingName */,
          std::unordered_map<EncodingPropertyType, EncodingProperty> properties)
          -> bool {
        auto it = properties.find(EncodingPropertyType::EncodedSize);
        if (it != properties.end()) {
          encodedSize = it->second.value;
        }
        return true;
      });

  // Stream size = 6 (prefix) + 1 (compression) + 2*4 (data) = 15
  EXPECT_EQ(encodedSize, std::to_string(stream.size()));
}

// --- Test with real encoder (using test utilities) ---

TEST_F(EncodingUtilitiesTest, GetEncodingLabelWithRealTrivialEncoding) {
  nimble::Buffer buffer(*pool_);
  nimble::Vector<uint32_t> values(pool_.get());
  for (uint32_t i = 0; i < 10; ++i) {
    values.push_back(i * 100);
  }

  auto encoded =
      nimble::test::Encoder<nimble::TrivialEncoding<uint32_t>>::encode(
          buffer, values);
  auto label = getEncodingLabel(encoded);
  EXPECT_NE(label.find("Trivial"), std::string::npos);
  EXPECT_NE(label.find("Uint32"), std::string::npos);
}

TEST_F(EncodingUtilitiesTest, GetEncodingLabelWithRealConstantEncoding) {
  nimble::Buffer buffer(*pool_);
  nimble::Vector<uint32_t> values(pool_.get());
  for (uint32_t i = 0; i < 5; ++i) {
    values.push_back(42);
  }

  auto encoded =
      nimble::test::Encoder<nimble::ConstantEncoding<uint32_t>>::encode(
          buffer, values);
  auto label = getEncodingLabel(encoded);
  EXPECT_NE(label.find("Constant"), std::string::npos);
  EXPECT_NE(label.find("Uint32"), std::string::npos);
}

} // namespace facebook::nimble::tools::test
