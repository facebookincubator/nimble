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
  EXPECT_EQ(static_cast<uint8_t>(SerializationVersion::kCompact), 1);
  EXPECT_EQ(static_cast<uint8_t>(SerializationVersion::kCompactRaw), 2);
  EXPECT_EQ(static_cast<uint8_t>(SerializationVersion::kTabletRaw), 3);
}

TEST(OptionsTest, toStringVersion) {
  EXPECT_EQ(toString(SerializationVersion::kLegacy), "kLegacy");
  EXPECT_EQ(toString(SerializationVersion::kCompact), "kCompact");
  EXPECT_EQ(toString(SerializationVersion::kCompactRaw), "kCompactRaw");
  EXPECT_EQ(toString(SerializationVersion::kTabletRaw), "kTabletRaw");
}

TEST(OptionsTest, streamOperator) {
  std::ostringstream os;
  os << SerializationVersion::kTabletRaw;
  EXPECT_EQ(os.str(), "kTabletRaw");
}

TEST(OptionsTest, fmtFormatter) {
  EXPECT_EQ(
      fmt::format("{}", SerializationVersion::kCompactRaw), "kCompactRaw");
  EXPECT_EQ(fmt::format("{}", SerializationVersion::kTabletRaw), "kTabletRaw");
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

TEST(OptionsTest, serializerOptionsWithDenseVersion) {
  SerializerOptions options{.version = SerializationVersion::kCompact};

  EXPECT_TRUE(options.version.has_value());
  EXPECT_EQ(*options.version, SerializationVersion::kCompact);
  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kCompact);
  EXPECT_TRUE(options.enableEncoding());
}

TEST(OptionsTest, serializerOptionsWithFlatMapColumns) {
  SerializerOptions options{
      .compressionType = CompressionType::Zstd,
      .compressionLevel = 3,
      .version = SerializationVersion::kCompact,
      .flatMapColumns = {"col1", "col2"},
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

  // Verify default values.
  EXPECT_FALSE(options.version.has_value());

  // Verify helper methods with defaults (nullopt = legacy format).
  EXPECT_FALSE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kLegacy);
  EXPECT_FALSE(options.enableEncoding());
}

TEST(OptionsTest, deserializerOptionsWithLegacyVersion) {
  // Explicit kLegacy still means version header is expected.
  DeserializerOptions options{.version = SerializationVersion::kLegacy};

  EXPECT_TRUE(options.version.has_value());
  EXPECT_EQ(*options.version, SerializationVersion::kLegacy);
  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kLegacy);
  EXPECT_FALSE(options.enableEncoding());
}

TEST(OptionsTest, deserializerOptionsWithDenseVersion) {
  DeserializerOptions options{.version = SerializationVersion::kCompact};

  EXPECT_TRUE(options.version.has_value());
  EXPECT_EQ(*options.version, SerializationVersion::kCompact);
  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kCompact);
  EXPECT_TRUE(options.enableEncoding());
}

TEST(OptionsTest, serializerOptionsWithCompactRawVersion) {
  SerializerOptions options{.version = SerializationVersion::kCompactRaw};

  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kCompactRaw);
  EXPECT_TRUE(options.enableEncoding());
}

TEST(OptionsTest, deserializerOptionsWithCompactRawVersion) {
  DeserializerOptions options{.version = SerializationVersion::kCompactRaw};

  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kCompactRaw);
  EXPECT_TRUE(options.enableEncoding());
}

TEST(OptionsTest, deserializerOptionsWithTabletRawVersion) {
  DeserializerOptions options{.version = SerializationVersion::kTabletRaw};

  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kTabletRaw);
  // kTabletRaw enables encoding (streams use nimble encoding, just with
  // chunk headers).
  EXPECT_TRUE(options.enableEncoding());
}

TEST(OptionsTest, isCompactFormat) {
  EXPECT_FALSE(isCompactFormat(SerializationVersion::kLegacy));
  EXPECT_TRUE(isCompactFormat(SerializationVersion::kCompact));
  EXPECT_TRUE(isCompactFormat(SerializationVersion::kCompactRaw));
  EXPECT_FALSE(isCompactFormat(SerializationVersion::kTabletRaw));
}

TEST(OptionsTest, isCompactFormatOptional) {
  EXPECT_FALSE(isCompactFormat(std::nullopt));
  EXPECT_FALSE(isCompactFormat(std::optional{SerializationVersion::kLegacy}));
  EXPECT_TRUE(isCompactFormat(std::optional{SerializationVersion::kCompact}));
  EXPECT_TRUE(
      isCompactFormat(std::optional{SerializationVersion::kCompactRaw}));
  EXPECT_FALSE(
      isCompactFormat(std::optional{SerializationVersion::kTabletRaw}));
}

TEST(OptionsTest, isTabletRawFormat) {
  EXPECT_FALSE(isTabletRawFormat(SerializationVersion::kLegacy));
  EXPECT_FALSE(isTabletRawFormat(SerializationVersion::kCompact));
  EXPECT_FALSE(isTabletRawFormat(SerializationVersion::kCompactRaw));
  EXPECT_TRUE(isTabletRawFormat(SerializationVersion::kTabletRaw));
}

TEST(OptionsTest, isTabletRawFormatOptional) {
  EXPECT_FALSE(isTabletRawFormat(std::nullopt));
  EXPECT_FALSE(isTabletRawFormat(std::optional{SerializationVersion::kLegacy}));
  EXPECT_FALSE(
      isTabletRawFormat(std::optional{SerializationVersion::kCompact}));
  EXPECT_FALSE(
      isTabletRawFormat(std::optional{SerializationVersion::kCompactRaw}));
  EXPECT_TRUE(
      isTabletRawFormat(std::optional{SerializationVersion::kTabletRaw}));
}

TEST(OptionsTest, usesVarintRowCount) {
  EXPECT_FALSE(usesVarintRowCount(SerializationVersion::kLegacy));
  EXPECT_TRUE(usesVarintRowCount(SerializationVersion::kCompact));
  EXPECT_TRUE(usesVarintRowCount(SerializationVersion::kCompactRaw));
  EXPECT_TRUE(usesVarintRowCount(SerializationVersion::kTabletRaw));
}

TEST(OptionsTest, usesVarintRowCountOptional) {
  EXPECT_FALSE(usesVarintRowCount(std::nullopt));
  EXPECT_FALSE(
      usesVarintRowCount(std::optional{SerializationVersion::kLegacy}));
  EXPECT_TRUE(
      usesVarintRowCount(std::optional{SerializationVersion::kCompact}));
  EXPECT_TRUE(
      usesVarintRowCount(std::optional{SerializationVersion::kCompactRaw}));
  EXPECT_TRUE(
      usesVarintRowCount(std::optional{SerializationVersion::kTabletRaw}));
}

TEST(OptionsTest, getRawEncodingTypeBasic) {
  EXPECT_EQ(getRawEncodingType(EncodingType::Trivial), EncodingType::Trivial);
  EXPECT_EQ(getRawEncodingType(EncodingType::Varint), EncodingType::Varint);
}

TEST(OptionsTest, getRawEncodingTypeError) {
  NIMBLE_ASSERT_THROW(
      getRawEncodingType(EncodingType::RLE),
      "Unsupported EncodingType for kCompactRaw");
  NIMBLE_ASSERT_THROW(
      getRawEncodingType(EncodingType::Dictionary),
      "Unsupported EncodingType for kCompactRaw");
  NIMBLE_ASSERT_THROW(
      getRawEncodingType(EncodingType::FixedBitWidth),
      "Unsupported EncodingType for kCompactRaw");
}
