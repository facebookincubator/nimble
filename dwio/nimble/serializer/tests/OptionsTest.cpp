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

#include "dwio/nimble/serializer/Options.h"

using namespace facebook::nimble;

TEST(OptionsTest, serializationVersionEnumValues) {
  // Verify enum underlying values match expected wire format versions.
  EXPECT_EQ(static_cast<uint8_t>(SerializationVersion::kDense), 0);
  EXPECT_EQ(static_cast<uint8_t>(SerializationVersion::kSparse), 1);
  EXPECT_EQ(static_cast<uint8_t>(SerializationVersion::kDenseEncoded), 2);
  EXPECT_EQ(static_cast<uint8_t>(SerializationVersion::kSparseEncoded), 3);
}

TEST(OptionsTest, serializerOptionsDefaults) {
  SerializerOptions options{};

  // Verify default values.
  EXPECT_EQ(options.compressionType, CompressionType::Uncompressed);
  EXPECT_EQ(options.compressionThreshold, 0);
  EXPECT_EQ(options.compressionLevel, 0);
  EXPECT_FALSE(options.version.has_value());
  EXPECT_TRUE(options.flatMapColumns.empty());

  // Verify helper methods with defaults (nullopt = dense format).
  EXPECT_FALSE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kDense);
  EXPECT_FALSE(options.enableEncoding());
  EXPECT_TRUE(options.denseFormat());
  EXPECT_FALSE(options.sparseFormat());

  // Verify default encoding selection policy factory creates a valid policy.
  auto policy = options.encodingSelectionPolicyFactory(DataType::Int32);
  EXPECT_NE(policy, nullptr);
}

TEST(OptionsTest, serializerOptionsWithDenseVersion) {
  // Explicit kDense still means version header is present.
  SerializerOptions options{.version = SerializationVersion::kDense};

  EXPECT_TRUE(options.version.has_value());
  EXPECT_EQ(*options.version, SerializationVersion::kDense);
  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kDense);
  EXPECT_FALSE(options.enableEncoding());
  EXPECT_TRUE(options.denseFormat());
  EXPECT_FALSE(options.sparseFormat());
}

TEST(OptionsTest, serializerOptionsWithSparseVersion) {
  SerializerOptions options{.version = SerializationVersion::kSparse};

  EXPECT_TRUE(options.version.has_value());
  EXPECT_EQ(*options.version, SerializationVersion::kSparse);
  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kSparse);
  EXPECT_FALSE(options.enableEncoding());
  EXPECT_FALSE(options.denseFormat());
  EXPECT_TRUE(options.sparseFormat());
}

TEST(OptionsTest, serializerOptionsWithDenseEncodedVersion) {
  SerializerOptions options{.version = SerializationVersion::kDenseEncoded};

  EXPECT_TRUE(options.version.has_value());
  EXPECT_EQ(*options.version, SerializationVersion::kDenseEncoded);
  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(
      options.serializationVersion(), SerializationVersion::kDenseEncoded);
  EXPECT_TRUE(options.enableEncoding());
  EXPECT_TRUE(options.denseFormat());
  EXPECT_FALSE(options.sparseFormat());
}

TEST(OptionsTest, serializerOptionsWithSparseEncodedVersion) {
  SerializerOptions options{.version = SerializationVersion::kSparseEncoded};

  EXPECT_TRUE(options.version.has_value());
  EXPECT_EQ(*options.version, SerializationVersion::kSparseEncoded);
  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(
      options.serializationVersion(), SerializationVersion::kSparseEncoded);
  EXPECT_TRUE(options.enableEncoding());
  EXPECT_FALSE(options.denseFormat());
  EXPECT_TRUE(options.sparseFormat());
}

TEST(OptionsTest, serializerOptionsWithFlatMapColumns) {
  SerializerOptions options{
      .compressionType = CompressionType::Zstd,
      .compressionLevel = 3,
      .version = SerializationVersion::kSparse,
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

  // Verify helper methods with defaults (nullopt = dense format).
  EXPECT_FALSE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kDense);
  EXPECT_FALSE(options.enableEncoding());
  EXPECT_TRUE(options.denseFormat());
  EXPECT_FALSE(options.sparseFormat());
}

TEST(OptionsTest, deserializerOptionsWithDenseVersion) {
  // Explicit kDense still means version header is expected.
  DeserializerOptions options{.version = SerializationVersion::kDense};

  EXPECT_TRUE(options.version.has_value());
  EXPECT_EQ(*options.version, SerializationVersion::kDense);
  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kDense);
  EXPECT_FALSE(options.enableEncoding());
  EXPECT_TRUE(options.denseFormat());
  EXPECT_FALSE(options.sparseFormat());
}

TEST(OptionsTest, deserializerOptionsWithSparseVersion) {
  DeserializerOptions options{.version = SerializationVersion::kSparse};

  EXPECT_TRUE(options.version.has_value());
  EXPECT_EQ(*options.version, SerializationVersion::kSparse);
  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(options.serializationVersion(), SerializationVersion::kSparse);
  EXPECT_FALSE(options.enableEncoding());
  EXPECT_FALSE(options.denseFormat());
  EXPECT_TRUE(options.sparseFormat());
}

TEST(OptionsTest, deserializerOptionsWithDenseEncodedVersion) {
  DeserializerOptions options{.version = SerializationVersion::kDenseEncoded};

  EXPECT_TRUE(options.version.has_value());
  EXPECT_EQ(*options.version, SerializationVersion::kDenseEncoded);
  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(
      options.serializationVersion(), SerializationVersion::kDenseEncoded);
  EXPECT_TRUE(options.enableEncoding());
  EXPECT_TRUE(options.denseFormat());
  EXPECT_FALSE(options.sparseFormat());
}

TEST(OptionsTest, deserializerOptionsWithSparseEncodedVersion) {
  DeserializerOptions options{.version = SerializationVersion::kSparseEncoded};

  EXPECT_TRUE(options.version.has_value());
  EXPECT_EQ(*options.version, SerializationVersion::kSparseEncoded);
  EXPECT_TRUE(options.hasVersionHeader());
  EXPECT_EQ(
      options.serializationVersion(), SerializationVersion::kSparseEncoded);
  EXPECT_TRUE(options.enableEncoding());
  EXPECT_FALSE(options.denseFormat());
  EXPECT_TRUE(options.sparseFormat());
}
