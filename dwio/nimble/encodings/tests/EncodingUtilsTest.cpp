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
#include "dwio/nimble/encodings/EncodingUtils.h"
#include <gtest/gtest.h>

using namespace facebook::nimble;

TEST(EncodingUtilsTest, DataTypeSizeOneByte) {
  EXPECT_EQ(1, detail::dataTypeSize(DataType::Int8));
  EXPECT_EQ(1, detail::dataTypeSize(DataType::Uint8));
  EXPECT_EQ(1, detail::dataTypeSize(DataType::Bool));
}

TEST(EncodingUtilsTest, DataTypeSizeTwoBytes) {
  EXPECT_EQ(2, detail::dataTypeSize(DataType::Int16));
  EXPECT_EQ(2, detail::dataTypeSize(DataType::Uint16));
}

TEST(EncodingUtilsTest, DataTypeSizeFourBytes) {
  EXPECT_EQ(4, detail::dataTypeSize(DataType::Int32));
  EXPECT_EQ(4, detail::dataTypeSize(DataType::Uint32));
  EXPECT_EQ(4, detail::dataTypeSize(DataType::Float));
}

TEST(EncodingUtilsTest, DataTypeSizeEightBytes) {
  EXPECT_EQ(8, detail::dataTypeSize(DataType::Int64));
  EXPECT_EQ(8, detail::dataTypeSize(DataType::Uint64));
  EXPECT_EQ(8, detail::dataTypeSize(DataType::Double));
}

TEST(EncodingUtilsTest, DataTypeSizeUnsupported) {
  EXPECT_THROW(detail::dataTypeSize(DataType::String), NimbleUserError);
  EXPECT_THROW(detail::dataTypeSize(DataType::Undefined), NimbleUserError);
}
