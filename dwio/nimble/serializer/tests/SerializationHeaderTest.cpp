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

#include <limits>

#include <gtest/gtest.h>

#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/serializer/SerializationHeader.h"

using namespace facebook::nimble;
using namespace facebook::nimble::serde;

// ---- SerializationHeader tests ----

TEST(SerializationHeaderTest, writeReadRoundtripLegacy) {
  std::string buffer;
  writeSerializationHeader(buffer, SerializationVersion::kLegacy, 42);

  const char* pos = buffer.data();
  auto header = readSerializationHeader(pos, pos + buffer.size(), true);
  EXPECT_EQ(header.version, SerializationVersion::kLegacy);
  EXPECT_EQ(header.rowCount, 42);
  EXPECT_FALSE(header.rowRange.has_value());
}

TEST(SerializationHeaderTest, writeReadRoundtripCompactRaw) {
  std::string buffer;
  writeSerializationHeader(buffer, SerializationVersion::kCompactRaw, 1000);

  const char* pos = buffer.data();
  auto header = readSerializationHeader(pos, pos + buffer.size(), true);
  EXPECT_EQ(header.version, SerializationVersion::kCompactRaw);
  EXPECT_EQ(header.rowCount, 1000);
  EXPECT_FALSE(header.rowRange.has_value());
}

TEST(SerializationHeaderTest, writeReadRoundtripNoHeader) {
  std::string buffer;
  writeSerializationHeader(buffer, std::nullopt, 100);

  const char* pos = buffer.data();
  auto header = readSerializationHeader(pos, pos + buffer.size(), false);
  EXPECT_EQ(header.version, SerializationVersion::kLegacy);
  EXPECT_EQ(header.rowCount, 100);
  EXPECT_FALSE(header.rowRange.has_value());
}

TEST(SerializationHeaderTest, readTabletHeader) {
  auto iobuf =
      createTabletChunkHeader({.rowCount = 500, .rowRange = RowRange{10, 200}});
  const char* pos = reinterpret_cast<const char*>(iobuf.data());
  const char* end = pos + iobuf.length();

  auto header = readSerializationHeader(pos, end, true);
  EXPECT_EQ(header.version, SerializationVersion::kTablet);
  EXPECT_EQ(header.rowCount, 500);
  ASSERT_TRUE(header.rowRange.has_value());
  EXPECT_EQ(header.rowRange->startRow, 10);
  EXPECT_EQ(header.rowRange->endRow, 200);
}

TEST(SerializationHeaderTest, readRejectsUnsupportedVersion) {
  std::string buffer;
  buffer.push_back(static_cast<char>(99));
  const char* pos = buffer.data();
  NIMBLE_ASSERT_THROW(
      readSerializationHeader(pos, pos + buffer.size(), true),
      "Unsupported version");
}

TEST(SerializationHeaderTest, writeRejectsTablet) {
  std::string buffer;
  NIMBLE_ASSERT_THROW(
      writeSerializationHeader(buffer, SerializationVersion::kTablet, 100),
      "kTablet headers must use createTabletChunkHeader()");
}

TEST(SerializationHeaderTest, estimateRejectsTablet) {
  NIMBLE_ASSERT_THROW(
      estimateSerializationHeaderSize(SerializationVersion::kTablet, 100),
      "kTablet headers must use createTabletChunkHeader()");
}

TEST(SerializationHeaderTest, estimateSizeMatchesActual) {
  for (auto version :
       {std::optional<SerializationVersion>{std::nullopt},
        std::optional{SerializationVersion::kLegacy},
        std::optional{SerializationVersion::kCompactRaw}}) {
    for (uint32_t rowCount : {0u, 1u, 127u, 128u, 16383u, 1000000u}) {
      std::string buffer;
      writeSerializationHeader(buffer, version, rowCount);
      EXPECT_EQ(
          estimateSerializationHeaderSize(version, rowCount), buffer.size())
          << "version="
          << (version.has_value() ? toString(*version) : "nullopt")
          << " rowCount=" << rowCount;
    }
  }
}

// ---- TabletChunkHeader tests ----

namespace {

TabletChunkHeader roundTripTabletChunkHeader(const TabletChunkHeader& in) {
  auto buf = createTabletChunkHeader(in);
  const char* pos = reinterpret_cast<const char*>(buf.data());
  const char* end = pos + buf.length();
  auto out = readTabletChunkHeader(pos, end);
  EXPECT_EQ(pos, end) << "Unexpected trailing bytes after header";
  return out;
}

} // namespace

TEST(TabletChunkHeaderTest, noResumeKey) {
  TabletChunkHeader in{.rowCount = 100, .rowRange = RowRange{0, 100}};
  const auto out = roundTripTabletChunkHeader(in);
  EXPECT_EQ(out.rowCount, 100u);
  EXPECT_EQ(out.rowRange, RowRange(0, 100));
  EXPECT_FALSE(out.resumeKey.has_value());
}

TEST(TabletChunkHeaderTest, narrowRowRange) {
  TabletChunkHeader in{.rowCount = 100, .rowRange = RowRange{10, 40}};
  const auto out = roundTripTabletChunkHeader(in);
  EXPECT_EQ(out.rowCount, 100u);
  EXPECT_EQ(out.rowRange.startRow, 10u);
  EXPECT_EQ(out.rowRange.endRow, 40u);
  EXPECT_FALSE(out.resumeKey.has_value());
}

TEST(TabletChunkHeaderTest, withResumeKey) {
  TabletChunkHeader in{
      .rowCount = 100,
      .rowRange = RowRange{0, 100},
      .resumeKey = std::string{"my-resume-key"}};
  const auto out = roundTripTabletChunkHeader(in);
  EXPECT_EQ(out.rowCount, 100u);
  EXPECT_EQ(out.rowRange, RowRange(0, 100));
  ASSERT_TRUE(out.resumeKey.has_value());
  EXPECT_EQ(*out.resumeKey, "my-resume-key");
}

TEST(TabletChunkHeaderTest, narrowRangeAndResumeKey) {
  TabletChunkHeader in{
      .rowCount = 4096,
      .rowRange = RowRange{500, 1000},
      .resumeKey = std::string{"\x00\x01\x02", 3}};
  const auto out = roundTripTabletChunkHeader(in);
  EXPECT_EQ(out.rowCount, 4096u);
  EXPECT_EQ(out.rowRange, RowRange(500, 1000));
  ASSERT_TRUE(out.resumeKey.has_value());
  EXPECT_EQ(*out.resumeKey, (std::string{"\x00\x01\x02", 3}));
}

TEST(TabletChunkHeaderTest, emptyResumeKey) {
  TabletChunkHeader in{
      .rowCount = 1, .rowRange = RowRange{0, 1}, .resumeKey = std::string{}};
  const auto out = roundTripTabletChunkHeader(in);
  EXPECT_EQ(out.rowCount, 1u);
  ASSERT_TRUE(out.resumeKey.has_value());
  EXPECT_TRUE(out.resumeKey->empty());
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

TEST(TabletChunkHeaderTest, roundTripsMaxUint32RowCount) {
  TabletChunkHeader in{
      .rowCount = std::numeric_limits<uint32_t>::max(),
      .rowRange = RowRange{0, std::numeric_limits<uint32_t>::max()},
  };
  const auto out = roundTripTabletChunkHeader(in);
  EXPECT_EQ(out.rowCount, std::numeric_limits<uint32_t>::max());
  EXPECT_EQ(out.rowRange.startRow, 0u);
  EXPECT_EQ(out.rowRange.endRow, std::numeric_limits<uint32_t>::max());
}

TEST(TabletChunkHeaderTest, exactBoundaryEndRowEqualsRowCount) {
  TabletChunkHeader in{.rowCount = 100, .rowRange = RowRange{99, 100}};
  const auto out = roundTripTabletChunkHeader(in);
  EXPECT_EQ(out.rowCount, 100u);
  EXPECT_EQ(out.rowRange, RowRange(99, 100));
}

TEST(TabletChunkHeaderTest, rejectsRowRangeExceedingRowCount) {
  std::string buf;
  buf.push_back(static_cast<char>(SerializationVersion::kTablet));
  buf.push_back(0x05); // rowCount = 5
  buf.push_back(0x00); // startRow = 0
  buf.push_back(0x64); // endRow = 100 (> rowCount)
  buf.push_back(0x00); // resumeKeyLength = 0
  const char* pos = buf.data();
  EXPECT_THROW(
      readTabletChunkHeader(pos, pos + buf.size()), NimbleInternalError);
}

TEST(TabletChunkHeaderTest, rejectsInvertedRowRange) {
  std::string buf;
  buf.push_back(static_cast<char>(SerializationVersion::kTablet));
  buf.push_back(0x0a); // rowCount = 10
  buf.push_back(0x05); // startRow = 5
  buf.push_back(0x03); // endRow = 3 (< startRow)
  buf.push_back(0x00); // resumeKeyLength = 0
  const char* pos = buf.data();
  EXPECT_THROW(
      readTabletChunkHeader(pos, pos + buf.size()), NimbleInternalError);
}
