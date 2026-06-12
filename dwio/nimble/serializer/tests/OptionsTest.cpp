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

#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/serializer/Options.h"

using namespace facebook::nimble;

TEST(OptionsTest, serializationVersionEnumValues) {
  // Verify enum underlying values match expected wire format versions.
  EXPECT_EQ(static_cast<uint8_t>(SerializationVersion::kLegacy), 0);
  EXPECT_EQ(static_cast<uint8_t>(SerializationVersion::kLegacyCompact), 2);
  EXPECT_EQ(static_cast<uint8_t>(SerializationVersion::kSerialization), 3);
  EXPECT_EQ(static_cast<uint8_t>(SerializationVersion::kProjection), 4);
  EXPECT_EQ(static_cast<uint8_t>(SerializationVersion::kTablet), 5);
}

TEST(OptionsTest, toStringVersion) {
  EXPECT_EQ(toString(SerializationVersion::kLegacy), "kLegacy");
  EXPECT_EQ(toString(SerializationVersion::kLegacyCompact), "kLegacyCompact");
  EXPECT_EQ(toString(SerializationVersion::kTablet), "kTablet");
}

TEST(OptionsTest, streamOperator) {
  std::ostringstream os;
  os << SerializationVersion::kTablet;
  EXPECT_EQ(os.str(), "kTablet");
}

TEST(OptionsTest, fmtFormatter) {
  EXPECT_EQ(
      fmt::format("{}", SerializationVersion::kLegacyCompact),
      "kLegacyCompact");
  EXPECT_EQ(fmt::format("{}", SerializationVersion::kTablet), "kTablet");
}

TEST(OptionsTest, serializerOptionsDefaults) {
  SerializerOptions options{};

  // Verify default values.
  EXPECT_EQ(options.compressionType, CompressionType::Uncompressed);
  EXPECT_EQ(options.compressionThreshold, 0);
  EXPECT_EQ(options.compressionLevel, 0);
  EXPECT_FALSE(options.version.has_value());
  EXPECT_TRUE(options.flatMapColumns.empty());

  // Verify helper methods with defaults (nullopt = legacy format).
  EXPECT_FALSE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kLegacy);
  EXPECT_FALSE(options.enableEncoding());

  // Verify encoding layout tree and compression options defaults.
  EXPECT_FALSE(options.encodingLayoutTree.has_value());
  EXPECT_FALSE(options.compressionOptions.has_value());

  // Verify default encoding selection policy factory creates a valid policy.
  auto policy = options.encodingSelectionPolicyFactory(DataType::Int32);
  EXPECT_NE(policy, nullptr);
}

TEST(OptionsTest, serializerOptionsWithLegacyVersion) {
  // Explicit kLegacy still means version header is present.
  SerializerOptions options{.version = SerializationVersion::kLegacy};

  EXPECT_TRUE(options.version.has_value());
  EXPECT_EQ(*options.version, SerializationVersion::kLegacy);
  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kLegacy);
  EXPECT_FALSE(options.enableEncoding());
}

TEST(OptionsTest, serializerOptionsWithFlatMapColumns) {
  SerializerOptions options{
      .compressionType = CompressionType::Zstd,
      .compressionLevel = 3,
      .version = SerializationVersion::kLegacyCompact,
      .flatMapColumns = {{"col1", {}}, {"col2", {}}},
  };

  EXPECT_EQ(options.compressionType, CompressionType::Zstd);
  EXPECT_EQ(options.compressionLevel, 3);
  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(options.flatMapColumns.size(), 2);
  EXPECT_TRUE(options.flatMapColumns.contains("col1"));
  EXPECT_TRUE(options.flatMapColumns.contains("col2"));
}

TEST(OptionsTest, deserializerOptionsDefaults) {
  DeserializerOptions options{};

  EXPECT_FALSE(options.hasHeader);
  EXPECT_EQ(options.decodeExecutor, nullptr);
  EXPECT_EQ(options.maxDecodeParallelism, 0u);
}

TEST(OptionsTest, deserializerOptionsWithVersion) {
  DeserializerOptions options{.hasHeader = true};

  EXPECT_TRUE(options.hasHeader);
}

TEST(OptionsTest, serializerOptionsWithCompactRawVersion) {
  SerializerOptions options{.version = SerializationVersion::kLegacyCompact};

  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(
      options.serializationVersion(), SerializationVersion::kLegacyCompact);
  EXPECT_TRUE(options.enableEncoding());
}

TEST(OptionsTest, nonLegacyFormat) {
  EXPECT_FALSE(nonLegacyFormat(SerializationVersion::kLegacy));
  EXPECT_TRUE(nonLegacyFormat(SerializationVersion::kLegacyCompact));
  EXPECT_TRUE(nonLegacyFormat(SerializationVersion::kTablet));
}

TEST(OptionsTest, isCompactFormat) {
  EXPECT_FALSE(isCompactFormat(SerializationVersion::kLegacy));
  EXPECT_TRUE(isCompactFormat(SerializationVersion::kLegacyCompact));
  EXPECT_FALSE(isCompactFormat(SerializationVersion::kTablet));
}

TEST(OptionsTest, isCompactFormatOptional) {
  EXPECT_FALSE(isCompactFormat(std::nullopt));
  EXPECT_FALSE(isCompactFormat(std::optional{SerializationVersion::kLegacy}));
  EXPECT_TRUE(
      isCompactFormat(std::optional{SerializationVersion::kLegacyCompact}));
  EXPECT_FALSE(isCompactFormat(std::optional{SerializationVersion::kTablet}));
}

TEST(OptionsTest, isTabletVersion) {
  EXPECT_FALSE(isTabletVersion(SerializationVersion::kLegacy));
  EXPECT_FALSE(isTabletVersion(SerializationVersion::kLegacyCompact));
  EXPECT_TRUE(isTabletVersion(SerializationVersion::kTablet));
}

TEST(OptionsTest, isTabletVersionOptional) {
  EXPECT_FALSE(isTabletVersion(std::nullopt));
  EXPECT_FALSE(isTabletVersion(std::optional{SerializationVersion::kLegacy}));
  EXPECT_FALSE(
      isTabletVersion(std::optional{SerializationVersion::kLegacyCompact}));
  EXPECT_TRUE(isTabletVersion(std::optional{SerializationVersion::kTablet}));
}

TEST(OptionsTest, usesVarintRowCount) {
  EXPECT_FALSE(usesVarintRowCount(SerializationVersion::kLegacy));
  EXPECT_TRUE(usesVarintRowCount(SerializationVersion::kLegacyCompact));
  EXPECT_TRUE(usesVarintRowCount(SerializationVersion::kTablet));
}

TEST(OptionsTest, usesVarintRowCountOptional) {
  EXPECT_FALSE(usesVarintRowCount(std::nullopt));
  EXPECT_FALSE(
      usesVarintRowCount(std::optional{SerializationVersion::kLegacy}));
  EXPECT_TRUE(
      usesVarintRowCount(std::optional{SerializationVersion::kLegacyCompact}));
  EXPECT_TRUE(usesVarintRowCount(std::optional{SerializationVersion::kTablet}));
}

TEST(OptionsTest, getTrailerEncodingTypeBasic) {
  EXPECT_EQ(
      getTrailerEncodingType(EncodingType::Trivial), EncodingType::Trivial);
  EXPECT_EQ(getTrailerEncodingType(EncodingType::Varint), EncodingType::Varint);
  EXPECT_EQ(getTrailerEncodingType(EncodingType::Delta), EncodingType::Delta);
  EXPECT_EQ(
      getTrailerEncodingType(EncodingType::FixedBitWidth),
      EncodingType::FixedBitWidth);
}

TEST(OptionsTest, getTrailerEncodingTypeError) {
  NIMBLE_ASSERT_THROW(
      getTrailerEncodingType(EncodingType::RLE),
      "Unsupported EncodingType for stream sizes trailer");
  NIMBLE_ASSERT_THROW(
      getTrailerEncodingType(EncodingType::Dictionary),
      "Unsupported EncodingType for stream sizes trailer");
  NIMBLE_ASSERT_THROW(
      getTrailerEncodingType(EncodingType::MainlyConstant),
      "Unsupported EncodingType for stream sizes trailer");
}
