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

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/serializer/SerializationHeader.h"

using namespace facebook::nimble;
using namespace facebook::nimble::serde;

namespace {

std::string nullBarrierName(bool requiresNullBarrier) {
  return requiresNullBarrier ? "requiresNullBarrier" : "noNullBarrier";
}

void setNullBarrierRequiredFlag(
    std::string& buffer,
    size_t flagsOffset,
    bool requiresNullBarrier) {
  NIMBLE_CHECK_LT(flagsOffset, buffer.size(), "Invalid flags byte offset");
  buffer[flagsOffset] = static_cast<char>(
      requiresNullBarrier ? SerializationHeader::kNullBarrierRequiredFlag : 0);
}

std::string describeVersion(std::optional<SerializationVersion> version) {
  return version.has_value() ? toString(*version) : std::string{"nullopt"};
}

struct SerializationHeaderRoundTripParam {
  std::string name;
  std::optional<SerializationVersion> version;
  SerializationVersion expectedVersion{SerializationVersion::kLegacy};
  uint32_t rowCount{};
  bool requiresNullBarrier{};
  bool hasHeader{};

  std::string toString() const {
    return "version=" + describeVersion(version) +
        " rowCount=" + std::to_string(rowCount) +
        " requiresNullBarrier=" + nullBarrierName(requiresNullBarrier) +
        " hasHeader=" + (hasHeader ? "true" : "false");
  }
};

struct SerializationHeaderSizeParam {
  std::string name;
  SerializationVersion version{SerializationVersion::kLegacy};
  uint32_t rowCount{};
  bool requiresNullBarrier{};

  std::string toString() const {
    return "version=" + facebook::nimble::toString(version) +
        " rowCount=" + std::to_string(rowCount) +
        " requiresNullBarrier=" + nullBarrierName(requiresNullBarrier);
  }
};

struct ReadNullBarrierFlagParam {
  std::string name;
  std::optional<SerializationVersion> version;
  uint8_t flagsByte{};
  bool expectedRequiresNullBarrier{};
  size_t expectedAdvance{};

  std::string toString() const {
    return "version=" + describeVersion(version) +
        " flagsByte=" + std::to_string(flagsByte) +
        " expectedRequiresNullBarrier=" +
        nullBarrierName(expectedRequiresNullBarrier) +
        " expectedAdvance=" + std::to_string(expectedAdvance);
  }
};

struct ReadNullBarrierFlagVersionParam {
  std::string name;
  SerializationVersion version;
  uint8_t flagsByte{};
  bool expectedRequiresNullBarrier{};
  size_t expectedAdvance{};

  std::string toString() const {
    return "version=" + facebook::nimble::toString(version) +
        " flagsByte=" + std::to_string(flagsByte) +
        " expectedRequiresNullBarrier=" +
        nullBarrierName(expectedRequiresNullBarrier) +
        " expectedAdvance=" + std::to_string(expectedAdvance);
  }
};

struct TabletChunkHeaderRoundTripParam {
  std::string name;
  uint32_t rowCount{};
  RowRange rowRange;
  std::optional<std::string> resumeKey;
  bool requiresNullBarrier{};

  std::string toString() const {
    return "rowCount=" + std::to_string(rowCount) + " rowRange=[" +
        std::to_string(rowRange.startRow) + "," +
        std::to_string(rowRange.endRow) +
        "] requiresNullBarrier=" + nullBarrierName(requiresNullBarrier) +
        " resumeKey=" + (resumeKey.has_value() ? "present" : "absent");
  }
};

struct InvalidTabletRowRangeParam {
  std::string name;
  uint32_t rowCount{};
  RowRange rowRange;
  bool requiresNullBarrier{};

  std::string toString() const {
    return "rowCount=" + std::to_string(rowCount) + " rowRange=[" +
        std::to_string(rowRange.startRow) + "," +
        std::to_string(rowRange.endRow) +
        "] requiresNullBarrier=" + nullBarrierName(requiresNullBarrier);
  }
};

std::vector<SerializationHeaderSizeParam> serializationHeaderSizeParams() {
  struct VersionCase {
    SerializationVersion version;
    bool requiresNullBarrier{};
  };

  const VersionCase versionCases[]{
      {
          .version = SerializationVersion::kSerialization,
          .requiresNullBarrier = false,
      },
      {
          .version = SerializationVersion::kSerialization,
          .requiresNullBarrier = true,
      },
      {
          .version = SerializationVersion::kProjection,
          .requiresNullBarrier = false,
      },
      {
          .version = SerializationVersion::kProjection,
          .requiresNullBarrier = true,
      },
  };

  std::vector<SerializationHeaderSizeParam> params;
  for (const auto& versionCase : versionCases) {
    for (uint32_t rowCount : {0u, 1u, 127u, 128u, 16383u, 1000000u}) {
      params.push_back({
          .name = toString(versionCase.version) + "_" +
              nullBarrierName(versionCase.requiresNullBarrier) + "_rows" +
              std::to_string(rowCount),
          .version = versionCase.version,
          .rowCount = rowCount,
          .requiresNullBarrier = versionCase.requiresNullBarrier,
      });
    }
  }
  return params;
}

std::vector<TabletChunkHeaderRoundTripParam>
tabletChunkHeaderRoundTripParams() {
  struct BaseCase {
    std::string name;
    uint32_t rowCount{};
    RowRange rowRange;
    std::optional<std::string> resumeKey;
  };

  const BaseCase baseCases[]{
      {
          .name = "noResumeKey",
          .rowCount = 100,
          .rowRange = RowRange{0, 100},
      },
      {
          .name = "narrowRowRange",
          .rowCount = 100,
          .rowRange = RowRange{10, 40},
      },
      {
          .name = "withResumeKey",
          .rowCount = 100,
          .rowRange = RowRange{0, 100},
          .resumeKey = std::string{"my-resume-key"},
      },
      {
          .name = "narrowRangeAndResumeKey",
          .rowCount = 4096,
          .rowRange = RowRange{500, 1000},
          .resumeKey = std::string{"\x00\x01\x02", 3},
      },
      {
          .name = "emptyResumeKey",
          .rowCount = 1,
          .rowRange = RowRange{0, 1},
          .resumeKey = std::string{},
      },
      {
          .name = "maxUint32RowCount",
          .rowCount = std::numeric_limits<uint32_t>::max(),
          .rowRange = RowRange{0, std::numeric_limits<uint32_t>::max()},
      },
      {
          .name = "exactBoundaryEndRowEqualsRowCount",
          .rowCount = 100,
          .rowRange = RowRange{99, 100},
      },
  };

  std::vector<TabletChunkHeaderRoundTripParam> params;
  for (const auto& baseCase : baseCases) {
    for (const bool requiresNullBarrier : {false, true}) {
      params.push_back({
          .name = baseCase.name + "_" + nullBarrierName(requiresNullBarrier),
          .rowCount = baseCase.rowCount,
          .rowRange = baseCase.rowRange,
          .resumeKey = baseCase.resumeKey,
          .requiresNullBarrier = requiresNullBarrier,
      });
    }
  }
  return params;
}

std::vector<InvalidTabletRowRangeParam> invalidTabletRowRangeParams() {
  struct BaseCase {
    std::string name;
    uint32_t rowCount{};
    RowRange rowRange;
  };

  const BaseCase baseCases[]{
      {
          .name = "rowRangeExceedingRowCount",
          .rowCount = 5,
          .rowRange = RowRange{0, 100},
      },
      {
          .name = "invertedRowRange",
          .rowCount = 10,
          .rowRange = RowRange{5, 3},
      },
  };

  std::vector<InvalidTabletRowRangeParam> params;
  for (const auto& baseCase : baseCases) {
    for (const bool requiresNullBarrier : {false, true}) {
      params.push_back({
          .name = baseCase.name + "_" + nullBarrierName(requiresNullBarrier),
          .rowCount = baseCase.rowCount,
          .rowRange = baseCase.rowRange,
          .requiresNullBarrier = requiresNullBarrier,
      });
    }
  }
  return params;
}

TabletChunkHeader roundTripTabletChunkHeader(const TabletChunkHeader& in) {
  auto buf = createTabletChunkHeader(in);
  const char* pos = reinterpret_cast<const char*>(buf.data());
  const char* end = pos + buf.length();
  auto out = readTabletChunkHeader(pos, end);
  EXPECT_EQ(pos, end) << "Unexpected trailing bytes after header";
  return out;
}

} // namespace

// ---- SerializationHeader tests ----

class SerializationHeaderRoundTripTest
    : public ::testing::TestWithParam<SerializationHeaderRoundTripParam> {};

TEST_P(SerializationHeaderRoundTripTest, writeReadRoundtrip) {
  const auto& testCase = GetParam();
  SCOPED_TRACE(testCase.toString());

  std::string buffer;
  if (testCase.version.has_value() && usesHeaderFlags(*testCase.version)) {
    const auto flagsOffset =
        writeSerializationHeader(buffer, *testCase.version, testCase.rowCount);
    EXPECT_EQ(
        flagsOffset, sizeof(uint8_t) + varint::varintSize(testCase.rowCount));
    setNullBarrierRequiredFlag(
        buffer, flagsOffset, testCase.requiresNullBarrier);
  } else {
    EXPECT_FALSE(testCase.requiresNullBarrier);
    writeLegacySerializationHeader(buffer, testCase.version, testCase.rowCount);
  }

  const char* pos = buffer.data();
  const auto header =
      readSerializationHeader(pos, pos + buffer.size(), testCase.hasHeader);
  EXPECT_EQ(header.version, testCase.expectedVersion);
  EXPECT_EQ(header.rowCount, testCase.rowCount);
  EXPECT_EQ(header.requiresNullBarrier, testCase.requiresNullBarrier);
  EXPECT_FALSE(header.rowRange.has_value());
}

INSTANTIATE_TEST_SUITE_P(
    SerializationHeaderTest,
    SerializationHeaderRoundTripTest,
    ::testing::Values(
        SerializationHeaderRoundTripParam{
            .name = "noHeader",
            .version = std::nullopt,
            .expectedVersion = SerializationVersion::kLegacy,
            .rowCount = 100,
            .requiresNullBarrier = false,
            .hasHeader = false,
        },
        SerializationHeaderRoundTripParam{
            .name = "legacy",
            .version = std::optional{SerializationVersion::kLegacy},
            .expectedVersion = SerializationVersion::kLegacy,
            .rowCount = 42,
            .requiresNullBarrier = false,
            .hasHeader = true,
        },
        SerializationHeaderRoundTripParam{
            .name = "legacyCompact",
            .version = std::optional{SerializationVersion::kLegacyCompact},
            .expectedVersion = SerializationVersion::kLegacyCompact,
            .rowCount = 1000,
            .requiresNullBarrier = false,
            .hasHeader = true,
        },
        SerializationHeaderRoundTripParam{
            .name = "legacySerialization",
            .version =
                std::optional{SerializationVersion::kLegacySerialization},
            .expectedVersion = SerializationVersion::kLegacySerialization,
            .rowCount = 1000,
            .requiresNullBarrier = false,
            .hasHeader = true,
        },
        SerializationHeaderRoundTripParam{
            .name = "serializationNoNullBarrier",
            .version = std::optional{SerializationVersion::kSerialization},
            .expectedVersion = SerializationVersion::kSerialization,
            .rowCount = 1000,
            .requiresNullBarrier = false,
            .hasHeader = true,
        },
        SerializationHeaderRoundTripParam{
            .name = "serializationRequiresNullBarrier",
            .version = std::optional{SerializationVersion::kSerialization},
            .expectedVersion = SerializationVersion::kSerialization,
            .rowCount = 1000,
            .requiresNullBarrier = true,
            .hasHeader = true,
        },
        SerializationHeaderRoundTripParam{
            .name = "projectionNoNullBarrier",
            .version = std::optional{SerializationVersion::kProjection},
            .expectedVersion = SerializationVersion::kProjection,
            .rowCount = 1000,
            .requiresNullBarrier = false,
            .hasHeader = true,
        },
        SerializationHeaderRoundTripParam{
            .name = "projectionRequiresNullBarrier",
            .version = std::optional{SerializationVersion::kProjection},
            .expectedVersion = SerializationVersion::kProjection,
            .rowCount = 1000,
            .requiresNullBarrier = true,
            .hasHeader = true,
        }),
    [](const ::testing::TestParamInfo<SerializationHeaderRoundTripParam>&
           info) { return info.param.name; });

TEST(SerializationHeaderTest, readRejectsUnsupportedVersion) {
  std::string buffer;
  buffer.push_back(static_cast<char>(99));
  const char* pos = buffer.data();
  NIMBLE_ASSERT_THROW(
      readSerializationHeader(pos, pos + buffer.size(), true),
      "Unsupported version");
}

TEST(SerializationHeaderTest, writeNullableHeaderRejectsNonNullableVersions) {
  for (const auto version :
       {SerializationVersion::kLegacy,
        SerializationVersion::kLegacyCompact,
        SerializationVersion::kTablet,
        SerializationVersion::kLegacySerialization}) {
    SCOPED_TRACE(toString(version));
    std::string buffer;
    NIMBLE_ASSERT_THROW(
        writeSerializationHeader(buffer, version, 100),
        "Serialization header writes require kSerialization or kProjection");
  }
}

TEST(SerializationHeaderTest, writeLegacyHeaderRejectsFlaggedVersions) {
  for (const auto version :
       {SerializationVersion::kSerialization,
        SerializationVersion::kProjection}) {
    SCOPED_TRACE(toString(version));
    std::string buffer;
    NIMBLE_ASSERT_THROW(
        writeLegacySerializationHeader(buffer, std::optional{version}, 100),
        "Serialization headers without flags cannot write versions with header flags");
  }
}

TEST(SerializationHeaderTest, writeLegacyHeaderRejectsTabletVersion) {
  std::string buffer;
  NIMBLE_ASSERT_THROW(
      writeLegacySerializationHeader(
          buffer, std::optional{SerializationVersion::kTablet}, 100),
      "kTablet headers must use createTabletChunkHeader()");
}

TEST(
    SerializationHeaderTest,
    writeLegacySerializationHeaderEncodesLegacyFormats) {
  enum class RowCountEncoding {
    kFixed32,
    kVarint32,
  };

  struct TestCase {
    std::string name;
    std::optional<SerializationVersion> version;
    SerializationVersion expectedVersion{SerializationVersion::kLegacy};
    uint32_t rowCount{};
    bool hasHeader{};
    RowCountEncoding rowCountEncoding{RowCountEncoding::kFixed32};
  };

  const TestCase testCases[]{
      {
          .name = "noHeader",
          .version = std::nullopt,
          .expectedVersion = SerializationVersion::kLegacy,
          .rowCount = 0x12345678,
          .hasHeader = false,
          .rowCountEncoding = RowCountEncoding::kFixed32,
      },
      {
          .name = "legacy",
          .version = std::optional{SerializationVersion::kLegacy},
          .expectedVersion = SerializationVersion::kLegacy,
          .rowCount = 42,
          .hasHeader = true,
          .rowCountEncoding = RowCountEncoding::kFixed32,
      },
      {
          .name = "legacyCompact",
          .version = std::optional{SerializationVersion::kLegacyCompact},
          .expectedVersion = SerializationVersion::kLegacyCompact,
          .rowCount = 1000,
          .hasHeader = true,
          .rowCountEncoding = RowCountEncoding::kVarint32,
      },
      {
          .name = "legacySerialization",
          .version = std::optional{SerializationVersion::kLegacySerialization},
          .expectedVersion = SerializationVersion::kLegacySerialization,
          .rowCount = 128,
          .hasHeader = true,
          .rowCountEncoding = RowCountEncoding::kVarint32,
      },
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);

    std::string buffer;
    writeLegacySerializationHeader(buffer, testCase.version, testCase.rowCount);

    const size_t versionSize = testCase.hasHeader ? sizeof(uint8_t) : 0;
    if (testCase.hasHeader) {
      ASSERT_TRUE(testCase.version.has_value());
      ASSERT_GE(buffer.size(), versionSize);
      EXPECT_EQ(
          static_cast<uint8_t>(buffer[0]),
          static_cast<uint8_t>(*testCase.version));
    }

    const char* rowCountStart = buffer.data() + versionSize;
    switch (testCase.rowCountEncoding) {
      case RowCountEncoding::kFixed32: {
        ASSERT_EQ(buffer.size(), versionSize + sizeof(uint32_t));
        const auto* data = reinterpret_cast<const uint8_t*>(rowCountStart);
        EXPECT_EQ(data[0], static_cast<uint8_t>(testCase.rowCount));
        EXPECT_EQ(data[1], static_cast<uint8_t>(testCase.rowCount >> 8));
        EXPECT_EQ(data[2], static_cast<uint8_t>(testCase.rowCount >> 16));
        EXPECT_EQ(data[3], static_cast<uint8_t>(testCase.rowCount >> 24));
        break;
      }
      case RowCountEncoding::kVarint32: {
        ASSERT_GE(buffer.size(), versionSize + 1);
        const char* pos = rowCountStart;
        EXPECT_EQ(varint::readVarint32(&pos), testCase.rowCount);
        EXPECT_EQ(pos, buffer.data() + buffer.size());
        break;
      }
    }

    const char* pos = buffer.data();
    const auto header = readSerializationHeader(
        pos, pos + buffer.size(), /*hasHeader=*/testCase.hasHeader);
    EXPECT_EQ(header.version, testCase.expectedVersion);
    EXPECT_EQ(header.rowCount, testCase.rowCount);
    EXPECT_FALSE(header.requiresNullBarrier);
    EXPECT_EQ(pos, buffer.data() + buffer.size());
  }
}

TEST(SerializationHeaderTest, estimateRejectsReadOnlyVersions) {
  for (const auto version :
       {SerializationVersion::kLegacy,
        SerializationVersion::kLegacyCompact,
        SerializationVersion::kTablet,
        SerializationVersion::kLegacySerialization}) {
    SCOPED_TRACE(toString(version));
    NIMBLE_ASSERT_THROW(
        estimateSerializationHeaderSize(version, 100),
        "Serialization header writes require kSerialization or kProjection");
  }
}

TEST(SerializationHeaderTest, setNullBarrierRequiredFlagRejectsInvalidOffset) {
  std::string buffer;
  const auto flagsOffset = writeSerializationHeader(
      buffer, SerializationVersion::kSerialization, /*rowCount=*/100);
  ASSERT_LT(flagsOffset, buffer.size());

  NIMBLE_ASSERT_THROW(
      setNullBarrierRequiredFlag(buffer, buffer.size(), true),
      "Invalid flags byte offset");
}

TEST(SerializationHeaderTest, writesFlagsAfterRowCount) {
  std::string buffer;
  const auto flagsOffset = writeSerializationHeader(
      buffer, SerializationVersion::kSerialization, /*rowCount=*/128);
  setNullBarrierRequiredFlag(buffer, flagsOffset, /*requiresNullBarrier=*/true);

  ASSERT_EQ(buffer.size(), size_t{4});
  EXPECT_EQ(
      static_cast<uint8_t>(buffer[0]),
      static_cast<uint8_t>(SerializationVersion::kSerialization));
  EXPECT_EQ(static_cast<uint8_t>(buffer[1]), 0x80);
  EXPECT_EQ(static_cast<uint8_t>(buffer[2]), 0x01);
  EXPECT_EQ(
      static_cast<uint8_t>(buffer[3]),
      SerializationHeader::kNullBarrierRequiredFlag);
  EXPECT_EQ(flagsOffset, size_t{3});
}

class SerializationHeaderSizeTest
    : public ::testing::TestWithParam<SerializationHeaderSizeParam> {};

TEST_P(SerializationHeaderSizeTest, estimateSizeMatchesActual) {
  const auto& testCase = GetParam();
  SCOPED_TRACE(testCase.toString());

  std::string buffer;
  const auto flagsOffset =
      writeSerializationHeader(buffer, testCase.version, testCase.rowCount);
  setNullBarrierRequiredFlag(buffer, flagsOffset, testCase.requiresNullBarrier);
  EXPECT_EQ(
      estimateSerializationHeaderSize(testCase.version, testCase.rowCount),
      buffer.size());
}

INSTANTIATE_TEST_SUITE_P(
    SerializationHeaderTest,
    SerializationHeaderSizeTest,
    ::testing::ValuesIn(serializationHeaderSizeParams()),
    [](const ::testing::TestParamInfo<SerializationHeaderSizeParam>& info) {
      return info.param.name;
    });

class ReadNullBarrierFlagTest
    : public ::testing::TestWithParam<ReadNullBarrierFlagParam> {};

TEST_P(ReadNullBarrierFlagTest, readPointer) {
  const auto& testCase = GetParam();
  SCOPED_TRACE(testCase.toString());
  std::string flagged;
  flagged.push_back(static_cast<char>(testCase.flagsByte));

  const char* pos = flagged.data();
  EXPECT_EQ(
      readRequiresNullBarrierFlag(pos, testCase.version),
      testCase.expectedRequiresNullBarrier);
  EXPECT_EQ(pos, flagged.data() + testCase.expectedAdvance);
}

TEST_P(ReadNullBarrierFlagTest, readCursor) {
  const auto& testCase = GetParam();
  SCOPED_TRACE(testCase.toString());
  constexpr uint8_t kSentinel{0x7f};
  std::string flagged;
  flagged.push_back(static_cast<char>(testCase.flagsByte));
  flagged.push_back(static_cast<char>(kSentinel));

  auto buf = folly::IOBuf::wrapBufferAsValue(flagged.data(), flagged.size());
  folly::io::Cursor cursor(&buf);
  EXPECT_EQ(
      readRequiresNullBarrierFlag(cursor, testCase.version),
      testCase.expectedRequiresNullBarrier);
  EXPECT_EQ(
      cursor.read<uint8_t>(),
      testCase.expectedAdvance == 0 ? testCase.flagsByte : kSentinel);
}

TEST(SerializationHeaderTest, readRequiresNullBarrierFlagVersionOverloads) {
  const ReadNullBarrierFlagVersionParam testCases[]{
      {
          .name = "serializationRequiresNullBarrier",
          .version = SerializationVersion::kSerialization,
          .flagsByte = SerializationHeader::kNullBarrierRequiredFlag,
          .expectedRequiresNullBarrier = true,
          .expectedAdvance = 1,
      },
      {
          .name = "projectionNoNullBarrier",
          .version = SerializationVersion::kProjection,
          .flagsByte = 0,
          .expectedRequiresNullBarrier = false,
          .expectedAdvance = 1,
      },
      {
          .name = "legacySerializationIgnoresFlagByte",
          .version = SerializationVersion::kLegacySerialization,
          .flagsByte = SerializationHeader::kNullBarrierRequiredFlag,
          .expectedRequiresNullBarrier = false,
          .expectedAdvance = 0,
      },
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.toString());
    std::string flagged;
    flagged.push_back(static_cast<char>(testCase.flagsByte));

    const char* pos = flagged.data();
    EXPECT_EQ(
        readRequiresNullBarrierFlag(pos, testCase.version),
        testCase.expectedRequiresNullBarrier);
    EXPECT_EQ(pos, flagged.data() + testCase.expectedAdvance);

    constexpr uint8_t kSentinel{0x7f};
    flagged.push_back(static_cast<char>(kSentinel));
    auto buf = folly::IOBuf::wrapBufferAsValue(flagged.data(), flagged.size());
    folly::io::Cursor cursor(&buf);
    EXPECT_EQ(
        readRequiresNullBarrierFlag(cursor, testCase.version),
        testCase.expectedRequiresNullBarrier);
    EXPECT_EQ(
        cursor.read<uint8_t>(),
        testCase.expectedAdvance == 0 ? testCase.flagsByte : kSentinel);
  }
}

INSTANTIATE_TEST_SUITE_P(
    SerializationHeaderTest,
    ReadNullBarrierFlagTest,
    ::testing::Values(
        ReadNullBarrierFlagParam{
            .name = "serializationNoNullBarrier",
            .version = std::optional{SerializationVersion::kSerialization},
            .flagsByte = 0,
            .expectedRequiresNullBarrier = false,
            .expectedAdvance = 1,
        },
        ReadNullBarrierFlagParam{
            .name = "serializationRequiresNullBarrier",
            .version = std::optional{SerializationVersion::kSerialization},
            .flagsByte = SerializationHeader::kNullBarrierRequiredFlag,
            .expectedRequiresNullBarrier = true,
            .expectedAdvance = 1,
        },
        ReadNullBarrierFlagParam{
            .name = "serializationReservedBitsIgnored",
            .version = std::optional{SerializationVersion::kSerialization},
            .flagsByte = 0x02,
            .expectedRequiresNullBarrier = false,
            .expectedAdvance = 1,
        },
        ReadNullBarrierFlagParam{
            .name = "serializationReservedBitsWithNullBarrier",
            .version = std::optional{SerializationVersion::kSerialization},
            .flagsByte = 0x03,
            .expectedRequiresNullBarrier = true,
            .expectedAdvance = 1,
        },
        ReadNullBarrierFlagParam{
            .name = "projectionNoNullBarrier",
            .version = std::optional{SerializationVersion::kProjection},
            .flagsByte = 0,
            .expectedRequiresNullBarrier = false,
            .expectedAdvance = 1,
        },
        ReadNullBarrierFlagParam{
            .name = "projectionRequiresNullBarrier",
            .version = std::optional{SerializationVersion::kProjection},
            .flagsByte = SerializationHeader::kNullBarrierRequiredFlag,
            .expectedRequiresNullBarrier = true,
            .expectedAdvance = 1,
        },
        ReadNullBarrierFlagParam{
            .name = "legacyIgnoresFlagByte",
            .version = std::optional{SerializationVersion::kLegacy},
            .flagsByte = SerializationHeader::kNullBarrierRequiredFlag,
            .expectedRequiresNullBarrier = false,
            .expectedAdvance = 0,
        },
        ReadNullBarrierFlagParam{
            .name = "legacyCompactIgnoresFlagByte",
            .version = std::optional{SerializationVersion::kLegacyCompact},
            .flagsByte = SerializationHeader::kNullBarrierRequiredFlag,
            .expectedRequiresNullBarrier = false,
            .expectedAdvance = 0,
        },
        ReadNullBarrierFlagParam{
            .name = "legacySerializationIgnoresFlagByte",
            .version =
                std::optional{SerializationVersion::kLegacySerialization},
            .flagsByte = SerializationHeader::kNullBarrierRequiredFlag,
            .expectedRequiresNullBarrier = false,
            .expectedAdvance = 0,
        },
        ReadNullBarrierFlagParam{
            .name = "noHeaderIgnoresFlagByte",
            .version = std::nullopt,
            .flagsByte = SerializationHeader::kNullBarrierRequiredFlag,
            .expectedRequiresNullBarrier = false,
            .expectedAdvance = 0,
        }),
    [](const ::testing::TestParamInfo<ReadNullBarrierFlagParam>& info) {
      return info.param.name;
    });

// ---- TabletChunkHeader tests ----

class TabletChunkHeaderRoundTripTest
    : public ::testing::TestWithParam<TabletChunkHeaderRoundTripParam> {};

TEST_P(TabletChunkHeaderRoundTripTest, readTabletChunkHeaderRoundtrip) {
  const auto& testCase = GetParam();
  SCOPED_TRACE(testCase.toString());

  const auto out = roundTripTabletChunkHeader({
      .rowCount = testCase.rowCount,
      .requiresNullBarrier = testCase.requiresNullBarrier,
      .rowRange = testCase.rowRange,
      .resumeKey = testCase.resumeKey,
  });
  EXPECT_EQ(out.rowCount, testCase.rowCount);
  EXPECT_EQ(out.requiresNullBarrier, testCase.requiresNullBarrier);
  EXPECT_EQ(out.rowRange, testCase.rowRange);
  EXPECT_EQ(out.resumeKey, testCase.resumeKey);
}

TEST_P(TabletChunkHeaderRoundTripTest, readSerializationHeaderRoundtrip) {
  const auto& testCase = GetParam();
  SCOPED_TRACE(testCase.toString());

  auto buf = createTabletChunkHeader({
      .rowCount = testCase.rowCount,
      .requiresNullBarrier = testCase.requiresNullBarrier,
      .rowRange = testCase.rowRange,
      .resumeKey = testCase.resumeKey,
  });
  const char* pos = reinterpret_cast<const char*>(buf.data());
  const char* end = pos + buf.length();
  const auto header = readSerializationHeader(pos, end, true);

  EXPECT_EQ(header.version, SerializationVersion::kTablet);
  EXPECT_EQ(header.rowCount, testCase.rowCount);
  EXPECT_EQ(header.requiresNullBarrier, testCase.requiresNullBarrier);
  ASSERT_TRUE(header.rowRange.has_value());
  EXPECT_EQ(*header.rowRange, testCase.rowRange);
}

INSTANTIATE_TEST_SUITE_P(
    TabletChunkHeaderTest,
    TabletChunkHeaderRoundTripTest,
    ::testing::ValuesIn(tabletChunkHeaderRoundTripParams()),
    [](const ::testing::TestParamInfo<TabletChunkHeaderRoundTripParam>& info) {
      return info.param.name;
    });

TEST(TabletChunkHeaderTest, writesFlagsAfterRowCount) {
  const auto buf = createTabletChunkHeader({
      .rowCount = 128,
      .requiresNullBarrier = true,
      .rowRange = RowRange{0, 128},
  });
  ASSERT_GE(buf.length(), size_t{4});
  const auto* data = reinterpret_cast<const uint8_t*>(buf.data());
  EXPECT_EQ(data[0], static_cast<uint8_t>(SerializationVersion::kTablet));
  EXPECT_EQ(data[1], 0x80);
  EXPECT_EQ(data[2], 0x01);
  EXPECT_EQ(data[3], SerializationHeader::kNullBarrierRequiredFlag);
}

TEST(TabletChunkHeaderTest, rejectsUnknownVersion) {
  std::string buf;
  buf.push_back(static_cast<char>(1));
  buf.push_back(0x01);
  const char* pos = buf.data();
  EXPECT_THROW(
      readTabletChunkHeader(pos, pos + buf.size()), NimbleInternalError);
}

TEST(TabletChunkHeaderTest, truncatedVersionByte) {
  std::string buf;
  const char* pos = buf.data();
  EXPECT_THROW(readTabletChunkHeader(pos, pos), NimbleInternalError);
}

class InvalidTabletRowRangeTest
    : public ::testing::TestWithParam<InvalidTabletRowRangeParam> {};

TEST_P(InvalidTabletRowRangeTest, rejectsInvalidRowRange) {
  const auto& testCase = GetParam();
  SCOPED_TRACE(testCase.toString());

  auto buf = createTabletChunkHeader({
      .rowCount = testCase.rowCount,
      .requiresNullBarrier = testCase.requiresNullBarrier,
      .rowRange = testCase.rowRange,
  });
  const char* pos = reinterpret_cast<const char*>(buf.data());
  EXPECT_THROW(
      readTabletChunkHeader(pos, pos + buf.length()), NimbleInternalError);
}

INSTANTIATE_TEST_SUITE_P(
    TabletChunkHeaderTest,
    InvalidTabletRowRangeTest,
    ::testing::ValuesIn(invalidTabletRowRangeParams()),
    [](const ::testing::TestParamInfo<InvalidTabletRowRangeParam>& info) {
      return info.param.name;
    });
