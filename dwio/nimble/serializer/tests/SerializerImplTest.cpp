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

#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/serializer/SerializerImpl.h"

using namespace facebook::nimble;
using namespace facebook::nimble::serde;

class WriteHeaderTest : public ::testing::Test {
 protected:
  // Helper to read header from buffer and verify contents.
  void verifyHeader(
      const std::string& buffer,
      SerializationVersion expectedVersion,
      uint32_t expectedRowCount,
      const std::vector<uint32_t>& expectedOffsets) {
    const char* pos = buffer.data();

    // Read version byte.
    auto actualVersion = static_cast<SerializationVersion>(*pos++);
    EXPECT_EQ(actualVersion, expectedVersion);

    // Read row count. Encoded versions use varint.
    const bool useVarint =
        (expectedVersion == SerializationVersion::kDenseEncoded ||
         expectedVersion == SerializationVersion::kSparseEncoded);
    uint32_t actualRowCount;
    if (useVarint) {
      actualRowCount = varint::readVarint32(&pos);
    } else {
      actualRowCount = encoding::readUint32(pos);
    }
    EXPECT_EQ(actualRowCount, expectedRowCount);

    // For sparse formats, read stream count and offsets.
    const bool sparseFormat =
        (expectedVersion == SerializationVersion::kSparse ||
         expectedVersion == SerializationVersion::kSparseEncoded);
    if (sparseFormat) {
      uint32_t streamCount = encoding::readUint32(pos);
      EXPECT_EQ(streamCount, expectedOffsets.size());

      for (size_t i = 0; i < expectedOffsets.size(); ++i) {
        uint32_t offset = encoding::readUint32(pos);
        EXPECT_EQ(offset, expectedOffsets[i]);
      }
    }

    // Verify we consumed exactly the expected amount.
    size_t rowCountSize =
        useVarint ? varint::varintSize(expectedRowCount) : sizeof(uint32_t);
    size_t expectedSize = 1 + rowCountSize; // version + rowCount
    if (sparseFormat) {
      expectedSize +=
          sizeof(uint32_t) + expectedOffsets.size() * sizeof(uint32_t);
    }
    EXPECT_EQ(buffer.size(), expectedSize);
  }
};

TEST_F(WriteHeaderTest, denseFormats) {
  for (auto version :
       {SerializationVersion::kDense, SerializationVersion::kDenseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::string buffer;
    serde::detail::writeHeader(buffer, version, 100, {});
    verifyHeader(buffer, version, 100, {});
  }
}

TEST_F(WriteHeaderTest, sparseFormatsWithOffsets) {
  std::vector<uint32_t> offsets = {0, 1, 2};
  for (auto version :
       {SerializationVersion::kSparse, SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::string buffer;
    serde::detail::writeHeader(buffer, version, 200, offsets);
    verifyHeader(buffer, version, 200, offsets);
  }
}

TEST_F(WriteHeaderTest, sparseFormatsEmptyOffsets) {
  for (auto version :
       {SerializationVersion::kSparse, SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::string buffer;
    serde::detail::writeHeader(buffer, version, 10, {});
    verifyHeader(buffer, version, 10, {});
  }
}

TEST_F(WriteHeaderTest, sparseFormatsManyOffsets) {
  std::vector<uint32_t> offsets;
  for (uint32_t i = 0; i < 100; ++i) {
    offsets.push_back(i * 2); // Non-sequential offsets.
  }
  for (auto version :
       {SerializationVersion::kSparse, SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::string buffer;
    serde::detail::writeHeader(buffer, version, 1000, offsets);
    verifyHeader(buffer, version, 1000, offsets);
  }
}

TEST_F(WriteHeaderTest, zeroRowCount) {
  for (auto version :
       {SerializationVersion::kDense,
        SerializationVersion::kDenseEncoded,
        SerializationVersion::kSparse,
        SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::string buffer;
    serde::detail::writeHeader(buffer, version, 0, {});
    verifyHeader(buffer, version, 0, {});
  }
}

TEST_F(WriteHeaderTest, maxRowCount) {
  for (auto version :
       {SerializationVersion::kDense,
        SerializationVersion::kDenseEncoded,
        SerializationVersion::kSparse,
        SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::string buffer;
    serde::detail::writeHeader(
        buffer, version, std::numeric_limits<uint32_t>::max(), {});
    verifyHeader(buffer, version, std::numeric_limits<uint32_t>::max(), {});
  }
}

TEST_F(WriteHeaderTest, noVersionHeader) {
  std::string buffer;
  serde::detail::writeHeader(buffer, std::nullopt, 100, {});

  // No version byte - only row count.
  EXPECT_EQ(buffer.size(), sizeof(uint32_t));

  const char* pos = buffer.data();
  uint32_t rowCount = encoding::readUint32(pos);
  EXPECT_EQ(rowCount, 100);
}

class WriteStreamTest : public ::testing::Test {
 protected:
  void verifyStream(const std::string& buffer, std::string_view expectedData) {
    const char* pos = buffer.data();

    // Read size.
    uint32_t actualSize = encoding::readUint32(pos);
    EXPECT_EQ(actualSize, expectedData.size());

    // Read data.
    std::string_view actualData(pos, actualSize);
    EXPECT_EQ(actualData, expectedData);

    // Verify total buffer size.
    EXPECT_EQ(buffer.size(), sizeof(uint32_t) + expectedData.size());
  }
};

TEST_F(WriteStreamTest, emptyStream) {
  std::string buffer;
  serde::detail::writeStream(buffer, "");

  verifyStream(buffer, "");
}

TEST_F(WriteStreamTest, singleByteStream) {
  std::string buffer;
  serde::detail::writeStream(buffer, "X");

  verifyStream(buffer, "X");
}

TEST_F(WriteStreamTest, multiByteStream) {
  std::string buffer;
  std::string data = "Hello, World!";
  serde::detail::writeStream(buffer, data);

  verifyStream(buffer, data);
}

TEST_F(WriteStreamTest, binaryStream) {
  std::string buffer;
  std::string data;
  for (int i = 0; i < 256; ++i) {
    data.push_back(static_cast<char>(i));
  }
  serde::detail::writeStream(buffer, data);

  verifyStream(buffer, data);
}

TEST_F(WriteStreamTest, multipleStreams) {
  std::string buffer;
  std::string data1 = "first";
  std::string data2 = "second";
  std::string data3 = "third";

  serde::detail::writeStream(buffer, data1);
  serde::detail::writeStream(buffer, data2);
  serde::detail::writeStream(buffer, data3);

  // Verify all three streams were written.
  const char* pos = buffer.data();

  uint32_t size1 = encoding::readUint32(pos);
  EXPECT_EQ(size1, data1.size());
  EXPECT_EQ(std::string_view(pos, size1), data1);
  pos += size1;

  uint32_t size2 = encoding::readUint32(pos);
  EXPECT_EQ(size2, data2.size());
  EXPECT_EQ(std::string_view(pos, size2), data2);
  pos += size2;

  uint32_t size3 = encoding::readUint32(pos);
  EXPECT_EQ(size3, data3.size());
  EXPECT_EQ(std::string_view(pos, size3), data3);
}

// Tests for readStream

TEST(ReadStreamTest, emptyStream) {
  std::string buffer;
  serde::detail::writeStream(buffer, "");

  const char* pos = buffer.data();
  auto result = serde::detail::readStream(pos);

  EXPECT_TRUE(result.empty());
  EXPECT_EQ(pos, buffer.data() + buffer.size());
}

TEST(ReadStreamTest, singleByteStream) {
  std::string buffer;
  serde::detail::writeStream(buffer, "X");

  const char* pos = buffer.data();
  auto result = serde::detail::readStream(pos);

  EXPECT_EQ(result, "X");
  EXPECT_EQ(pos, buffer.data() + buffer.size());
}

TEST(ReadStreamTest, multiByteStream) {
  std::string buffer;
  std::string data = "Hello, World!";
  serde::detail::writeStream(buffer, data);

  const char* pos = buffer.data();
  auto result = serde::detail::readStream(pos);

  EXPECT_EQ(result, data);
  EXPECT_EQ(pos, buffer.data() + buffer.size());
}

TEST(ReadStreamTest, multipleStreams) {
  std::string buffer;
  serde::detail::writeStream(buffer, "first");
  serde::detail::writeStream(buffer, "second");
  serde::detail::writeStream(buffer, "third");

  const char* pos = buffer.data();

  auto result1 = serde::detail::readStream(pos);
  EXPECT_EQ(result1, "first");

  auto result2 = serde::detail::readStream(pos);
  EXPECT_EQ(result2, "second");

  auto result3 = serde::detail::readStream(pos);
  EXPECT_EQ(result3, "third");

  EXPECT_EQ(pos, buffer.data() + buffer.size());
}

// Tests for parseStreams

class ParseStreamsTest : public ::testing::Test {
 protected:
  // Helper to build a dense format buffer.
  std::string buildDenseBuffer(const std::vector<std::string>& streams) {
    std::string buffer;
    for (const auto& stream : streams) {
      serde::detail::writeStream(buffer, stream);
    }
    return buffer;
  }

  // Helper to build a sparse format buffer.
  std::string buildSparseBuffer(
      const std::vector<std::pair<uint32_t, std::string>>& offsetsAndData) {
    std::string buffer;

    // Write stream count.
    char countBuf[sizeof(uint32_t)];
    char* countPtr = countBuf;
    encoding::writeUint32(offsetsAndData.size(), countPtr);
    buffer.append(countBuf, sizeof(uint32_t));

    // Write offsets.
    for (const auto& [offset, _] : offsetsAndData) {
      char offsetBuf[sizeof(uint32_t)];
      char* offsetPtr = offsetBuf;
      encoding::writeUint32(offset, offsetPtr);
      buffer.append(offsetBuf, sizeof(uint32_t));
    }

    // Write stream data.
    for (const auto& [_, data] : offsetsAndData) {
      serde::detail::writeStream(buffer, data);
    }

    return buffer;
  }
};

TEST_F(ParseStreamsTest, denseFormatEmpty) {
  std::string buffer;

  for (auto version :
       {SerializationVersion::kDense, SerializationVersion::kDenseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    auto streams = serde::detail::parseStreams(
        buffer.data(), buffer.data() + buffer.size(), version);
    EXPECT_TRUE(streams.empty());
  }
}

TEST_F(ParseStreamsTest, denseFormatSingleStream) {
  auto buffer = buildDenseBuffer({"hello"});

  for (auto version :
       {SerializationVersion::kDense, SerializationVersion::kDenseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    auto streams = serde::detail::parseStreams(
        buffer.data(), buffer.data() + buffer.size(), version);

    ASSERT_EQ(streams.size(), 1);
    EXPECT_EQ(streams[0], "hello");
  }
}

TEST_F(ParseStreamsTest, denseFormatMultipleStreams) {
  auto buffer = buildDenseBuffer({"first", "second", "third"});

  for (auto version :
       {SerializationVersion::kDense, SerializationVersion::kDenseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    auto streams = serde::detail::parseStreams(
        buffer.data(), buffer.data() + buffer.size(), version);

    ASSERT_EQ(streams.size(), 3);
    EXPECT_EQ(streams[0], "first");
    EXPECT_EQ(streams[1], "second");
    EXPECT_EQ(streams[2], "third");
  }
}

TEST_F(ParseStreamsTest, sparseFormatEmpty) {
  auto buffer = buildSparseBuffer({});

  for (auto version :
       {SerializationVersion::kSparse, SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    auto streams = serde::detail::parseStreams(
        buffer.data(), buffer.data() + buffer.size(), version);
    EXPECT_TRUE(streams.empty());
  }
}

TEST_F(ParseStreamsTest, sparseFormatSequential) {
  auto buffer = buildSparseBuffer({{0, "zero"}, {1, "one"}, {2, "two"}});

  for (auto version :
       {SerializationVersion::kSparse, SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    auto streams = serde::detail::parseStreams(
        buffer.data(), buffer.data() + buffer.size(), version);

    ASSERT_EQ(streams.size(), 3);
    EXPECT_EQ(streams[0], "zero");
    EXPECT_EQ(streams[1], "one");
    EXPECT_EQ(streams[2], "two");
  }
}

TEST_F(ParseStreamsTest, sparseFormatWithGaps) {
  auto buffer = buildSparseBuffer({{0, "zero"}, {2, "two"}, {5, "five"}});

  for (auto version :
       {SerializationVersion::kSparse, SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    auto streams = serde::detail::parseStreams(
        buffer.data(), buffer.data() + buffer.size(), version);

    ASSERT_EQ(streams.size(), 6);
    EXPECT_EQ(streams[0], "zero");
    EXPECT_TRUE(streams[1].empty()); // gap
    EXPECT_EQ(streams[2], "two");
    EXPECT_TRUE(streams[3].empty()); // gap
    EXPECT_TRUE(streams[4].empty()); // gap
    EXPECT_EQ(streams[5], "five");
  }
}

TEST_F(ParseStreamsTest, sparseFormatOutOfOrder) {
  // Offsets don't have to be in order.
  auto buffer = buildSparseBuffer({{2, "two"}, {0, "zero"}, {1, "one"}});

  for (auto version :
       {SerializationVersion::kSparse, SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    auto streams = serde::detail::parseStreams(
        buffer.data(), buffer.data() + buffer.size(), version);

    ASSERT_EQ(streams.size(), 3);
    EXPECT_EQ(streams[0], "zero");
    EXPECT_EQ(streams[1], "one");
    EXPECT_EQ(streams[2], "two");
  }
}

// Tests for projectStreams

class ProjectStreamsTest : public ParseStreamsTest {};

TEST_F(ProjectStreamsTest, denseSelectAll) {
  auto buffer = buildDenseBuffer({"aaa", "bbb", "ccc"});

  for (auto version :
       {SerializationVersion::kDense, SerializationVersion::kDenseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::vector<uint32_t> selected = {0, 1, 2};
    auto streams = serde::detail::projectStreams(
        buffer.data(), buffer.data() + buffer.size(), version, selected);

    ASSERT_EQ(streams.size(), 3);
    EXPECT_EQ(streams[0].index, 0);
    EXPECT_EQ(streams[0].data, "aaa");
    EXPECT_EQ(streams[1].index, 1);
    EXPECT_EQ(streams[1].data, "bbb");
    EXPECT_EQ(streams[2].index, 2);
    EXPECT_EQ(streams[2].data, "ccc");
  }
}

TEST_F(ProjectStreamsTest, denseSelectSubset) {
  auto buffer = buildDenseBuffer({"aaa", "bbb", "ccc", "ddd", "eee"});

  for (auto version :
       {SerializationVersion::kDense, SerializationVersion::kDenseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::vector<uint32_t> selected = {1, 3};
    auto streams = serde::detail::projectStreams(
        buffer.data(), buffer.data() + buffer.size(), version, selected);

    ASSERT_EQ(streams.size(), 2);
    EXPECT_EQ(streams[0].index, 0);
    EXPECT_EQ(streams[0].data, "bbb");
    EXPECT_EQ(streams[1].index, 1);
    EXPECT_EQ(streams[1].data, "ddd");
  }
}

TEST_F(ProjectStreamsTest, denseSelectFirst) {
  auto buffer = buildDenseBuffer({"aaa", "bbb", "ccc"});

  for (auto version :
       {SerializationVersion::kDense, SerializationVersion::kDenseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::vector<uint32_t> selected = {0};
    auto streams = serde::detail::projectStreams(
        buffer.data(), buffer.data() + buffer.size(), version, selected);

    ASSERT_EQ(streams.size(), 1);
    EXPECT_EQ(streams[0].index, 0);
    EXPECT_EQ(streams[0].data, "aaa");
  }
}

TEST_F(ProjectStreamsTest, denseSelectLast) {
  auto buffer = buildDenseBuffer({"aaa", "bbb", "ccc"});

  for (auto version :
       {SerializationVersion::kDense, SerializationVersion::kDenseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::vector<uint32_t> selected = {2};
    auto streams = serde::detail::projectStreams(
        buffer.data(), buffer.data() + buffer.size(), version, selected);

    ASSERT_EQ(streams.size(), 1);
    EXPECT_EQ(streams[0].index, 0);
    EXPECT_EQ(streams[0].data, "ccc");
  }
}

TEST_F(ProjectStreamsTest, denseSkipsEmptyStreams) {
  auto buffer = buildDenseBuffer({"aaa", "", "ccc"});

  for (auto version :
       {SerializationVersion::kDense, SerializationVersion::kDenseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::vector<uint32_t> selected = {0, 1, 2};
    auto streams = serde::detail::projectStreams(
        buffer.data(), buffer.data() + buffer.size(), version, selected);

    // Empty stream at index 1 is skipped.
    ASSERT_EQ(streams.size(), 2);
    EXPECT_EQ(streams[0].index, 0);
    EXPECT_EQ(streams[0].data, "aaa");
    EXPECT_EQ(streams[1].index, 2);
    EXPECT_EQ(streams[1].data, "ccc");
  }
}

TEST_F(ProjectStreamsTest, denseAllEmpty) {
  auto buffer = buildDenseBuffer({"", "", ""});

  for (auto version :
       {SerializationVersion::kDense, SerializationVersion::kDenseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::vector<uint32_t> selected = {0, 1, 2};
    auto streams = serde::detail::projectStreams(
        buffer.data(), buffer.data() + buffer.size(), version, selected);
    EXPECT_TRUE(streams.empty());
  }
}

TEST_F(ProjectStreamsTest, sparseSelectAll) {
  auto buffer = buildSparseBuffer({{0, "aaa"}, {1, "bbb"}, {2, "ccc"}});

  for (auto version :
       {SerializationVersion::kSparse, SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::vector<uint32_t> selected = {0, 1, 2};
    auto streams = serde::detail::projectStreams(
        buffer.data(), buffer.data() + buffer.size(), version, selected);

    ASSERT_EQ(streams.size(), 3);
    EXPECT_EQ(streams[0].index, 0);
    EXPECT_EQ(streams[0].data, "aaa");
    EXPECT_EQ(streams[1].index, 1);
    EXPECT_EQ(streams[1].data, "bbb");
    EXPECT_EQ(streams[2].index, 2);
    EXPECT_EQ(streams[2].data, "ccc");
  }
}

TEST_F(ProjectStreamsTest, sparseSelectSubset) {
  auto buffer = buildSparseBuffer(
      {{0, "aaa"}, {1, "bbb"}, {2, "ccc"}, {3, "ddd"}, {4, "eee"}});

  for (auto version :
       {SerializationVersion::kSparse, SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::vector<uint32_t> selected = {1, 3};
    auto streams = serde::detail::projectStreams(
        buffer.data(), buffer.data() + buffer.size(), version, selected);

    ASSERT_EQ(streams.size(), 2);
    EXPECT_EQ(streams[0].index, 0);
    EXPECT_EQ(streams[0].data, "bbb");
    EXPECT_EQ(streams[1].index, 1);
    EXPECT_EQ(streams[1].data, "ddd");
  }
}

TEST_F(ProjectStreamsTest, sparseWithGaps) {
  // Input has gaps (offsets 0, 2, 5). Select 0 and 5.
  auto buffer = buildSparseBuffer({{0, "zero"}, {2, "two"}, {5, "five"}});

  for (auto version :
       {SerializationVersion::kSparse, SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::vector<uint32_t> selected = {0, 5};
    auto streams = serde::detail::projectStreams(
        buffer.data(), buffer.data() + buffer.size(), version, selected);

    ASSERT_EQ(streams.size(), 2);
    EXPECT_EQ(streams[0].index, 0);
    EXPECT_EQ(streams[0].data, "zero");
    EXPECT_EQ(streams[1].index, 1);
    EXPECT_EQ(streams[1].data, "five");
  }
}

TEST_F(ProjectStreamsTest, sparseSelectedNotInInput) {
  // Input has offsets {0, 2}. Select {0, 1, 2} — offset 1 is missing.
  auto buffer = buildSparseBuffer({{0, "zero"}, {2, "two"}});

  for (auto version :
       {SerializationVersion::kSparse, SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::vector<uint32_t> selected = {0, 1, 2};
    auto streams = serde::detail::projectStreams(
        buffer.data(), buffer.data() + buffer.size(), version, selected);

    // Offset 1 not in input, so only 2 non-empty streams returned.
    ASSERT_EQ(streams.size(), 2);
    EXPECT_EQ(streams[0].index, 0);
    EXPECT_EQ(streams[0].data, "zero");
    EXPECT_EQ(streams[1].index, 2);
    EXPECT_EQ(streams[1].data, "two");
  }
}

TEST_F(ProjectStreamsTest, sparseEmpty) {
  auto buffer = buildSparseBuffer({});

  for (auto version :
       {SerializationVersion::kSparse, SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::vector<uint32_t> selected = {0, 1};
    auto streams = serde::detail::projectStreams(
        buffer.data(), buffer.data() + buffer.size(), version, selected);
    EXPECT_TRUE(streams.empty());
  }
}

TEST_F(ProjectStreamsTest, sparseOutOfOrder) {
  // Sparse offsets written out of order: {2, 0, 1}.
  auto buffer = buildSparseBuffer({{2, "two"}, {0, "zero"}, {1, "one"}});

  for (auto version :
       {SerializationVersion::kSparse, SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::vector<uint32_t> selected = {0, 2};
    auto streams = serde::detail::projectStreams(
        buffer.data(), buffer.data() + buffer.size(), version, selected);

    // Results sorted by output offset.
    ASSERT_EQ(streams.size(), 2);
    EXPECT_EQ(streams[0].index, 0);
    EXPECT_EQ(streams[0].data, "zero");
    EXPECT_EQ(streams[1].index, 1);
    EXPECT_EQ(streams[1].data, "two");
  }
}

TEST_F(ProjectStreamsTest, sparseSkipsEmptyStreams) {
  auto buffer = buildSparseBuffer({{0, "aaa"}, {1, ""}, {2, "ccc"}});

  for (auto version :
       {SerializationVersion::kSparse, SerializationVersion::kSparseEncoded}) {
    SCOPED_TRACE(static_cast<int>(version));
    std::vector<uint32_t> selected = {0, 1, 2};
    auto streams = serde::detail::projectStreams(
        buffer.data(), buffer.data() + buffer.size(), version, selected);

    // Empty stream at offset 1 is skipped.
    ASSERT_EQ(streams.size(), 2);
    EXPECT_EQ(streams[0].index, 0);
    EXPECT_EQ(streams[0].data, "aaa");
    EXPECT_EQ(streams[1].index, 2);
    EXPECT_EQ(streams[1].data, "ccc");
  }
}
