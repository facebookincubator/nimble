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
#include "dwio/nimble/encodings/EncodingSelection.h"

using namespace facebook::nimble;

class MetaInternalCompressionKeyTest : public ::testing::Test {
 protected:
  void roundTripTest(const MetaInternalCompressionKey& key) {
    // Test round-trip serialization
    std::string serialized = key.toString();
    MetaInternalCompressionKey deserialized =
        MetaInternalCompressionKey::fromString(serialized);

    EXPECT_EQ(key.ns, deserialized.ns);
    EXPECT_EQ(key.tableName, deserialized.tableName);
    EXPECT_EQ(key.columnName, deserialized.columnName);
  }
};

TEST_F(MetaInternalCompressionKeyTest, BasicRoundTrip) {
  MetaInternalCompressionKey key{
      .ns = "namespace", .tableName = "table", .columnName = "column"};

  roundTripTest(key);
}

TEST_F(MetaInternalCompressionKeyTest, EmptyFields) {
  MetaInternalCompressionKey key{.ns = "", .tableName = "", .columnName = ""};

  roundTripTest(key);
}

TEST_F(MetaInternalCompressionKeyTest, SingleFieldEmpty) {
  // Test with one field empty at a time
  MetaInternalCompressionKey key1{
      .ns = "", .tableName = "table", .columnName = "column"};
  roundTripTest(key1);

  MetaInternalCompressionKey key2{
      .ns = "namespace", .tableName = "", .columnName = "column"};
  roundTripTest(key2);

  MetaInternalCompressionKey key3{
      .ns = "namespace", .tableName = "table", .columnName = ""};
  roundTripTest(key3);
}

TEST_F(MetaInternalCompressionKeyTest, ColonCharacters) {
  // Test with colon characters in various fields
  MetaInternalCompressionKey key1{
      .ns = "name:space", .tableName = "table", .columnName = "column"};
  roundTripTest(key1);

  MetaInternalCompressionKey key2{
      .ns = "namespace", .tableName = "ta:ble", .columnName = "column"};
  roundTripTest(key2);

  MetaInternalCompressionKey key3{
      .ns = "namespace", .tableName = "table", .columnName = "col:umn"};
  roundTripTest(key3);

  // Test with multiple colons in each field
  MetaInternalCompressionKey key4{
      .ns = "name::space:with:many:colons",
      .tableName = "ta::ble::name",
      .columnName = "col::umn::name"};
  roundTripTest(key4);
}

TEST_F(MetaInternalCompressionKeyTest, SpecialCharacters) {
  // Test with various special characters that JSON should handle properly
  MetaInternalCompressionKey key{
      .ns = "namespace\"with'quotes",
      .tableName = "table\nwith\nnewlines",
      .columnName = "column\\with\\backslashes"};
  roundTripTest(key);
}

TEST_F(MetaInternalCompressionKeyTest, MixedSpecialCharacters) {
  // Test with a mix of challenging characters
  MetaInternalCompressionKey key{
      .ns = "ns:with\"quotes'and\nnewlines\tand\\\\:backslashes",
      .tableName = "table:with:many:colons:and\"quotes",
      .columnName = "column\nwith\r\nmixed\ttabs:and\\:colons"};
  roundTripTest(key);
}

TEST_F(MetaInternalCompressionKeyTest, InvalidJsonInput) {
  // Test error handling for invalid JSON
  EXPECT_THROW(
      MetaInternalCompressionKey::fromString("not json"), std::runtime_error);

  EXPECT_THROW(
      MetaInternalCompressionKey::fromString("{\"invalid\": \"json\"}"),
      std::runtime_error);

  EXPECT_THROW(
      MetaInternalCompressionKey::fromString(
          "{\"ns\": \"value\"}"), // missing fields
      std::runtime_error);
}

TEST_F(MetaInternalCompressionKeyTest, PartialJsonInput) {
  // Test with JSON missing required fields
  EXPECT_THROW(
      MetaInternalCompressionKey::fromString(
          "{\"ns\": \"namespace\", \"tableName\": \"table\"}"), // missing
                                                                // columnName
      std::runtime_error);

  EXPECT_THROW(
      MetaInternalCompressionKey::fromString(
          "{\"ns\": \"namespace\", \"columnName\": \"column\"}"), // missing
                                                                  // tableName
      std::runtime_error);

  EXPECT_THROW(
      MetaInternalCompressionKey::fromString(
          "{\"tableName\": \"table\", \"columnName\": \"column\"}"), // missing
                                                                     // ns
      std::runtime_error);
}

TEST_F(MetaInternalCompressionKeyTest, ExtraFieldsInJson) {
  // Test that extra fields in JSON are ignored
  std::string jsonWithExtra = R"({
    "ns": "namespace",
    "tableName": "table", 
    "columnName": "column",
    "extraField": "ignored"
  })";

  MetaInternalCompressionKey key =
      MetaInternalCompressionKey::fromString(jsonWithExtra);
  EXPECT_EQ(key.ns, "namespace");
  EXPECT_EQ(key.tableName, "table");
  EXPECT_EQ(key.columnName, "column");
}

TEST_F(MetaInternalCompressionKeyTest, NullValuesInJson) {
  // Test error handling for null values in JSON
  EXPECT_THROW(
      MetaInternalCompressionKey::fromString(
          R"({"ns": null, "tableName": "table", "columnName": "column"})"),
      std::runtime_error);
}
