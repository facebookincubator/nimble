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
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
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

  // Build a PFOR-encoded stream wrapping the given exception positions / values
  // sub-streams. baseBitWidth is 0 so there is no trailing bitpacked residual
  // region (traverseEncodings does not read it).
  // Layout:
  //   bytes 0-5: prefix (EncodingType::PFOR, DataType::Uint32, rowCount)
  //   bytes 6-9: baseline (uint32)
  //   byte 10: baseBitWidth (0)
  //   bytes 11-14: numExceptions (uint32)
  //   then the positions sub-stream and the values sub-stream, each preceded by
  //   a 4-byte size.
  std::string buildPforUint32Stream(
      uint32_t numExceptions,
      const std::string& positions,
      const std::string& values) {
    const size_t totalSize = 6 + sizeof(uint32_t) /* baseline */ +
        1 /* baseBitWidth */ + sizeof(uint32_t) /* numExceptions */ +
        sizeof(uint32_t) + positions.size() + sizeof(uint32_t) + values.size();
    std::string buf(totalSize, '\0');
    char* pos = buf.data();
    encoding::writeChar(static_cast<char>(EncodingType::PFOR), pos);
    encoding::writeChar(static_cast<char>(DataType::Uint32), pos);
    encoding::writeUint32(/* rowCount */ 100, pos);
    encoding::writeUint32(/* baseline */ 0, pos);
    encoding::writeChar(/* baseBitWidth */ 0, pos);
    encoding::writeUint32(numExceptions, pos);
    encoding::writeUint32(static_cast<uint32_t>(positions.size()), pos);
    encoding::writeBytes(positions, pos);
    encoding::writeUint32(static_cast<uint32_t>(values.size()), pos);
    encoding::writeBytes(values, pos);
    return buf;
  }

  // Build a BlockBitPacking-encoded stream wrapping the given per-block
  // metadata sub-streams. The packed data region is omitted since
  // traverseEncodings does not read it. Layout:
  //   bytes 0-5: prefix (EncodingType::BlockBitPacking, DataType::Uint32,
  //              rowCount)
  //   byte 6: compressionType (Uncompressed)
  //   bytes 7-8: blockSize (uint16)
  //   bytes 9-10: numBlocks (uint16)
  //   then the baselines / bitWidths / offsets sub-streams, each preceded by a
  //   4-byte size.
  std::string buildBlockBitPackingUint32Stream(
      const std::string& baselines,
      const std::string& bitWidths,
      const std::string& offsets) {
    const size_t totalSize = 6 + 1 /* compressionType */ + 2 /* blockSize */ +
        2 /* numBlocks */ + sizeof(uint32_t) + baselines.size() +
        sizeof(uint32_t) + bitWidths.size() + sizeof(uint32_t) + offsets.size();
    std::string buf(totalSize, '\0');
    char* pos = buf.data();
    encoding::writeChar(static_cast<char>(EncodingType::BlockBitPacking), pos);
    encoding::writeChar(static_cast<char>(DataType::Uint32), pos);
    encoding::writeUint32(/* rowCount */ 100, pos);
    encoding::writeChar(static_cast<char>(CompressionType::Uncompressed), pos);
    encoding::write<uint16_t>(/* blockSize */ 64, pos);
    encoding::write<uint16_t>(/* numBlocks */ 2, pos);
    encoding::writeUint32(static_cast<uint32_t>(baselines.size()), pos);
    encoding::writeBytes(baselines, pos);
    encoding::writeUint32(static_cast<uint32_t>(bitWidths.size()), pos);
    encoding::writeBytes(bitWidths, pos);
    encoding::writeUint32(static_cast<uint32_t>(offsets.size()), pos);
    encoding::writeBytes(offsets, pos);
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

// --- PFOR nested exception sub-streams ---

TEST_F(EncodingUtilitiesTest, TraverseEncodingsPforChildren) {
  auto positions = buildTrivialUint32Stream({0, 5});
  auto values = buildTrivialUint32Stream({100, 200});
  auto stream = buildPforUint32Stream(/* numExceptions */ 2, positions, values);

  EncodingType rootType = EncodingType::Constant; // init to something else
  std::vector<std::pair<std::string, uint32_t>> nestedVisits; // name, level
  traverseEncodings(
      stream,
      [&](EncodingType encodingType,
          DataType /* dataType */,
          uint32_t level,
          uint32_t /* index */,
          const std::string& nestedEncodingName,
          const std::unordered_map<EncodingPropertyType, EncodingProperty>&
          /* properties */) -> bool {
        if (level == 0) {
          rootType = encodingType;
        } else {
          nestedVisits.emplace_back(nestedEncodingName, level);
        }
        return true;
      });

  EXPECT_EQ(rootType, EncodingType::PFOR);
  const std::vector<std::pair<std::string, uint32_t>> expected{
      {"ExceptionPositions", 1u}, {"ExceptionValues", 1u}};
  EXPECT_EQ(nestedVisits, expected);
}

TEST_F(EncodingUtilitiesTest, TraverseEncodingsPforNoExceptions) {
  // With zero exceptions both sub-streams are empty and must be skipped rather
  // than traversed (which would read past the end of the stream).
  auto stream = buildPforUint32Stream(
      /* numExceptions */ 0, /* positions */ "", /* values */ "");

  int nestedVisitCount = 0;
  traverseEncodings(
      stream,
      [&](EncodingType /* encodingType */,
          DataType /* dataType */,
          uint32_t level,
          uint32_t /* index */,
          const std::string& /* nestedEncodingName */,
          const std::unordered_map<EncodingPropertyType, EncodingProperty>&
          /* properties */) -> bool {
        if (level > 0) {
          ++nestedVisitCount;
        }
        return true;
      });

  EXPECT_EQ(nestedVisitCount, 0);
}

TEST_F(EncodingUtilitiesTest, GetEncodingLabelPfor) {
  // The rendered label surfaces the picked sub-encodings for the exception
  // side-channels, e.g. PFOR<Uint32>[ExceptionPositions:Trivial<...>,
  // ExceptionValues:Trivial<...>].
  auto positions = buildTrivialUint32Stream({0, 5});
  auto values = buildTrivialUint32Stream({100, 200});
  auto stream = buildPforUint32Stream(/* numExceptions */ 2, positions, values);

  auto label = getEncodingLabel(stream);
  EXPECT_NE(label.find("PFOR"), std::string::npos);
  EXPECT_NE(label.find("ExceptionPositions:Trivial"), std::string::npos);
  EXPECT_NE(label.find("ExceptionValues:Trivial"), std::string::npos);
}

// --- BlockBitPacking nested metadata sub-streams ---

TEST_F(EncodingUtilitiesTest, TraverseEncodingsBlockBitPackingChildren) {
  auto baselines = buildTrivialUint32Stream({0, 64});
  auto bitWidths = buildTrivialUint32Stream({4, 5});
  auto offsets = buildTrivialUint32Stream({0, 32});
  auto stream = buildBlockBitPackingUint32Stream(baselines, bitWidths, offsets);

  EncodingType rootType = EncodingType::Constant; // init to something else
  std::vector<std::string> nestedNames;
  traverseEncodings(
      stream,
      [&](EncodingType encodingType,
          DataType /* dataType */,
          uint32_t level,
          uint32_t /* index */,
          const std::string& nestedEncodingName,
          const std::unordered_map<EncodingPropertyType, EncodingProperty>&
          /* properties */) -> bool {
        if (level == 0) {
          rootType = encodingType;
        } else {
          nestedNames.push_back(nestedEncodingName);
        }
        return true;
      });

  EXPECT_EQ(rootType, EncodingType::BlockBitPacking);
  const std::vector<std::string> expected{
      "Baselines", "BitWidths", "DataOffsets"};
  EXPECT_EQ(nestedNames, expected);
}

TEST_F(EncodingUtilitiesTest, GetEncodingLabelBlockBitPacking) {
  // The rendered label surfaces the picked sub-encodings for the per-block
  // metadata, e.g. BlockBitPacking<Uint32>[Baselines:Trivial<...>,
  // BitWidths:Trivial<...>,DataOffsets:Trivial<...>].
  auto baselines = buildTrivialUint32Stream({0, 64});
  auto bitWidths = buildTrivialUint32Stream({4, 5});
  auto offsets = buildTrivialUint32Stream({0, 32});
  auto stream = buildBlockBitPackingUint32Stream(baselines, bitWidths, offsets);

  auto label = getEncodingLabel(stream);
  EXPECT_NE(label.find("BlockBitPacking"), std::string::npos);
  EXPECT_NE(label.find("Baselines:Trivial"), std::string::npos);
  EXPECT_NE(label.find("BitWidths:Trivial"), std::string::npos);
  EXPECT_NE(label.find("DataOffsets:Trivial"), std::string::npos);
}

} // namespace facebook::nimble::tools::test
