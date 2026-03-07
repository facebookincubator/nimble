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
#include "velox/common/memory/Memory.h"

using namespace facebook::nimble;
using namespace facebook::nimble::serde;
using namespace facebook::velox;

class WriteHeaderTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("write_header_test");
  }

  // Helper to build a complete kCompact buffer: header + data + trailer.
  std::string buildDenseHeaderTrailer(
      uint32_t rowCount,
      const std::vector<uint32_t>& sizes,
      std::optional<EncodingType> encodingType = std::nullopt) {
    std::string buffer;
    serde::detail::writeHeader(
        buffer, SerializationVersion::kCompact, rowCount);
    serde::detail::writeTrailer(sizes, encodingType, pool_.get(), buffer);
    return buffer;
  }

  // Helper to verify header by writing and parsing back using roundtrip.
  // For kCompact format, verifies the dense sizes array from trailer.
  void verifyHeader(
      const std::string& buffer,
      SerializationVersion expectedVersion,
      uint32_t expectedRowCount,
      const std::vector<uint32_t>& expectedSizes = {}) {
    const char* pos = buffer.data();
    const char* end = buffer.data() + buffer.size();

    // Read version byte.
    auto actualVersion = static_cast<SerializationVersion>(*pos++);
    EXPECT_EQ(actualVersion, expectedVersion);

    // Read row count. kCompact uses varint, kLegacy uses u32.
    const bool isDense = (expectedVersion == SerializationVersion::kCompact);
    uint32_t actualRowCount;
    if (isDense) {
      actualRowCount = varint::readVarint32(&pos);
    } else {
      actualRowCount = encoding::readUint32(pos);
    }
    EXPECT_EQ(actualRowCount, expectedRowCount);

    // For kCompact format, read sizesSize from last 4 bytes of trailer.
    if (isDense) {
      const uint32_t sizesSize = serde::detail::readStreamSizesEncodedSize(end);
      auto actualSizes = serde::detail::decodeStreamSizes(
          {end - sizeof(uint32_t) - sizesSize, sizesSize}, pool_.get());
      EXPECT_EQ(actualSizes, expectedSizes);
    }
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(WriteHeaderTest, legacyFormat) {
  std::string buffer;
  serde::detail::writeHeader(buffer, SerializationVersion::kLegacy, 100);
  verifyHeader(buffer, SerializationVersion::kLegacy, 100, {});
}

TEST_F(WriteHeaderTest, denseFormatWithSizes) {
  // Dense sizes array: sizes[0]=10, sizes[1]=20, sizes[2]=30.
  std::vector<uint32_t> sizes = {10, 20, 30};
  auto buffer = buildDenseHeaderTrailer(200, sizes);
  verifyHeader(buffer, SerializationVersion::kCompact, 200, sizes);
}

TEST_F(WriteHeaderTest, denseFormatEmptySizes) {
  auto buffer = buildDenseHeaderTrailer(10, {});
  verifyHeader(buffer, SerializationVersion::kCompact, 10, {});
}

TEST_F(WriteHeaderTest, denseFormatManySizes) {
  // Dense sizes array with 200 entries (100 non-zero interleaved with zeros).
  std::vector<uint32_t> sizes(200, 0);
  for (uint32_t i = 0; i < 100; ++i) {
    sizes[i * 2] = i + 1;
  }
  auto buffer = buildDenseHeaderTrailer(1000, sizes);
  verifyHeader(buffer, SerializationVersion::kCompact, 1000, sizes);
}

TEST_F(WriteHeaderTest, zeroRowCount) {
  {
    SCOPED_TRACE("kLegacy");
    std::string buffer;
    serde::detail::writeHeader(buffer, SerializationVersion::kLegacy, 0);
    verifyHeader(buffer, SerializationVersion::kLegacy, 0, {});
  }
  {
    SCOPED_TRACE("kCompact");
    auto buffer = buildDenseHeaderTrailer(0, {});
    verifyHeader(buffer, SerializationVersion::kCompact, 0, {});
  }
}

TEST_F(WriteHeaderTest, maxRowCount) {
  {
    SCOPED_TRACE("kLegacy");
    std::string buffer;
    serde::detail::writeHeader(
        buffer,
        SerializationVersion::kLegacy,
        std::numeric_limits<uint32_t>::max());
    verifyHeader(
        buffer,
        SerializationVersion::kLegacy,
        std::numeric_limits<uint32_t>::max(),
        {});
  }
  {
    SCOPED_TRACE("kCompact");
    auto buffer =
        buildDenseHeaderTrailer(std::numeric_limits<uint32_t>::max(), {});
    verifyHeader(
        buffer,
        SerializationVersion::kCompact,
        std::numeric_limits<uint32_t>::max(),
        {});
  }
}

TEST_F(WriteHeaderTest, noVersionHeader) {
  std::string buffer;
  serde::detail::writeHeader(buffer, std::nullopt, 100);

  // No version byte - only row count.
  EXPECT_EQ(buffer.size(), sizeof(uint32_t));

  const char* pos = buffer.data();
  uint32_t rowCount = encoding::readUint32(pos);
  EXPECT_EQ(rowCount, 100);
}

class WriteStreamTest : public ::testing::Test {
 protected:
  void verifyStream(
      const std::string& buffer,
      std::string_view expectedData,
      bool useVarint) {
    const char* pos = buffer.data();

    // Read size.
    uint32_t actualSize;
    if (useVarint) {
      actualSize = varint::readVarint32(&pos);
    } else {
      actualSize = encoding::readUint32(pos);
    }
    EXPECT_EQ(actualSize, expectedData.size());

    // Read data.
    std::string_view actualData(pos, actualSize);
    EXPECT_EQ(actualData, expectedData);
  }
};

TEST_F(WriteStreamTest, emptyStream) {
  {
    SCOPED_TRACE("u32");
    std::string buffer;
    serde::detail::writeStream<false>("", buffer);
    verifyStream(buffer, "", false);
  }
  {
    SCOPED_TRACE("varint");
    std::string buffer;
    serde::detail::writeStream<true>("", buffer);
    verifyStream(buffer, "", true);
  }
}

TEST_F(WriteStreamTest, singleByteStream) {
  {
    SCOPED_TRACE("u32");
    std::string buffer;
    serde::detail::writeStream<false>("X", buffer);
    verifyStream(buffer, "X", false);
  }
  {
    SCOPED_TRACE("varint");
    std::string buffer;
    serde::detail::writeStream<true>("X", buffer);
    verifyStream(buffer, "X", true);
  }
}

TEST_F(WriteStreamTest, multiByteStream) {
  std::string data = "Hello, World!";
  {
    SCOPED_TRACE("u32");
    std::string buffer;
    serde::detail::writeStream<false>(data, buffer);
    verifyStream(buffer, data, false);
  }
  {
    SCOPED_TRACE("varint");
    std::string buffer;
    serde::detail::writeStream<true>(data, buffer);
    verifyStream(buffer, data, true);
  }
}

TEST_F(WriteStreamTest, binaryStream) {
  std::string data;
  for (int i = 0; i < 256; ++i) {
    data.push_back(static_cast<char>(i));
  }
  {
    SCOPED_TRACE("u32");
    std::string buffer;
    serde::detail::writeStream<false>(data, buffer);
    verifyStream(buffer, data, false);
  }
  {
    SCOPED_TRACE("varint");
    std::string buffer;
    serde::detail::writeStream<true>(data, buffer);
    verifyStream(buffer, data, true);
  }
}

TEST_F(WriteStreamTest, multipleStreams) {
  std::string data1 = "first";
  std::string data2 = "second";
  std::string data3 = "third";

  {
    SCOPED_TRACE("u32");
    std::string buffer;
    serde::detail::writeStream<false>(data1, buffer);
    serde::detail::writeStream<false>(data2, buffer);
    serde::detail::writeStream<false>(data3, buffer);

    const char* pos = buffer.data();
    EXPECT_EQ(serde::detail::readStream<false>(pos), data1);
    EXPECT_EQ(serde::detail::readStream<false>(pos), data2);
    EXPECT_EQ(serde::detail::readStream<false>(pos), data3);
    EXPECT_EQ(pos, buffer.data() + buffer.size());
  }
  {
    SCOPED_TRACE("varint");
    std::string buffer;
    serde::detail::writeStream<true>(data1, buffer);
    serde::detail::writeStream<true>(data2, buffer);
    serde::detail::writeStream<true>(data3, buffer);

    const char* pos = buffer.data();
    EXPECT_EQ(serde::detail::readStream<true>(pos), data1);
    EXPECT_EQ(serde::detail::readStream<true>(pos), data2);
    EXPECT_EQ(serde::detail::readStream<true>(pos), data3);
    EXPECT_EQ(pos, buffer.data() + buffer.size());
  }
}

// Tests for readStream

TEST(ReadStreamTest, roundTrip) {
  {
    SCOPED_TRACE("u32");
    std::string buffer;
    serde::detail::writeStream<false>("", buffer);
    serde::detail::writeStream<false>("X", buffer);
    serde::detail::writeStream<false>("Hello, World!", buffer);

    const char* pos = buffer.data();
    EXPECT_TRUE(serde::detail::readStream<false>(pos).empty());
    EXPECT_EQ(serde::detail::readStream<false>(pos), "X");
    EXPECT_EQ(serde::detail::readStream<false>(pos), "Hello, World!");
    EXPECT_EQ(pos, buffer.data() + buffer.size());
  }
  {
    SCOPED_TRACE("varint");
    std::string buffer;
    serde::detail::writeStream<true>("", buffer);
    serde::detail::writeStream<true>("X", buffer);
    serde::detail::writeStream<true>("Hello, World!", buffer);

    const char* pos = buffer.data();
    EXPECT_TRUE(serde::detail::readStream<true>(pos).empty());
    EXPECT_EQ(serde::detail::readStream<true>(pos), "X");
    EXPECT_EQ(serde::detail::readStream<true>(pos), "Hello, World!");
    EXPECT_EQ(pos, buffer.data() + buffer.size());
  }
}

// Tests for parseStreams

class ParseStreamsTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("parse_streams_test");
  }

  // Helper to build a kLegacy format buffer with u32 sizes.
  std::string buildLegacyBuffer(const std::vector<std::string>& streams) {
    std::string buffer;
    for (const auto& stream : streams) {
      serde::detail::writeStream<false>(stream, buffer);
    }
    return buffer;
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(ParseStreamsTest, legacyFormatEmpty) {
  std::string buffer;
  auto streams = serde::detail::parseStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kLegacy,
      pool_.get());
  EXPECT_TRUE(streams.empty());
}

TEST_F(ParseStreamsTest, legacyFormatSingleStream) {
  auto buffer = buildLegacyBuffer({"hello"});
  auto streams = serde::detail::parseStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kLegacy,
      pool_.get());
  ASSERT_EQ(streams.size(), 1);
  EXPECT_EQ(streams[0], "hello");
}

TEST_F(ParseStreamsTest, legacyFormatMultipleStreams) {
  auto buffer = buildLegacyBuffer({"first", "second", "third"});
  auto streams = serde::detail::parseStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kLegacy,
      pool_.get());
  ASSERT_EQ(streams.size(), 3);
  EXPECT_EQ(streams[0], "first");
  EXPECT_EQ(streams[1], "second");
  EXPECT_EQ(streams[2], "third");
}

// kCompact format tests use writeHeader/parseStreams roundtrip since
// the size encoding is now opaque (nimble-encoded).

TEST_F(ParseStreamsTest, denseFormatEmpty) {
  // Write header + trailer with empty sizes array, then parse.
  std::string buffer;
  serde::detail::writeHeader(buffer, SerializationVersion::kCompact, 10);
  serde::detail::writeTrailer({}, std::nullopt, pool_.get(), buffer);

  auto streams = serde::detail::parseStreams(
      // Skip version byte and row count varint.
      buffer.data() + 1 + varint::varintSize(10),
      buffer.data() + buffer.size(),
      SerializationVersion::kCompact,
      pool_.get());
  EXPECT_TRUE(streams.empty());
}

TEST_F(ParseStreamsTest, denseFormatSequential) {
  // Dense sizes array: sizes[0]=4, sizes[1]=3, sizes[2]=3.
  std::vector<std::string> data = {"zero", "one", "two"};
  std::vector<uint32_t> sizes;
  for (const auto& d : data) {
    sizes.push_back(d.size());
  }

  std::string buffer;
  serde::detail::writeHeader(buffer, SerializationVersion::kCompact, 100);
  for (const auto& d : data) {
    buffer.append(d);
  }
  serde::detail::writeTrailer(sizes, std::nullopt, pool_.get(), buffer);

  // Skip version byte + varint row count.
  const char* pos = buffer.data() + 1;
  varint::readVarint32(&pos);

  auto streams = serde::detail::parseStreams(
      pos,
      buffer.data() + buffer.size(),
      SerializationVersion::kCompact,
      pool_.get());

  ASSERT_EQ(streams.size(), 3);
  EXPECT_EQ(streams[0], "zero");
  EXPECT_EQ(streams[1], "one");
  EXPECT_EQ(streams[2], "two");
}

TEST_F(ParseStreamsTest, denseFormatWithGaps) {
  // Dense sizes array with gaps (zeros at indices 1, 3, 4).
  // sizes = {4, 0, 3, 0, 0, 4}
  std::vector<uint32_t> sizes = {4, 0, 3, 0, 0, 4};

  std::string buffer;
  serde::detail::writeHeader(buffer, SerializationVersion::kCompact, 100);
  buffer.append("zero");
  buffer.append("two");
  buffer.append("five");
  serde::detail::writeTrailer(sizes, std::nullopt, pool_.get(), buffer);

  const char* pos = buffer.data() + 1;
  varint::readVarint32(&pos);

  auto streams = serde::detail::parseStreams(
      pos,
      buffer.data() + buffer.size(),
      SerializationVersion::kCompact,
      pool_.get());

  ASSERT_EQ(streams.size(), 6);
  EXPECT_EQ(streams[0], "zero");
  EXPECT_TRUE(streams[1].empty()); // gap
  EXPECT_EQ(streams[2], "two");
  EXPECT_TRUE(streams[3].empty()); // gap
  EXPECT_TRUE(streams[4].empty()); // gap
  EXPECT_EQ(streams[5], "five");
}

TEST_F(ParseStreamsTest, denseFormatOnlyLastStream) {
  // Dense sizes array with only the last stream present.
  // sizes = {0, 0, 0, 0, 5}
  std::vector<uint32_t> sizes = {0, 0, 0, 0, 5};

  std::string buffer;
  serde::detail::writeHeader(buffer, SerializationVersion::kCompact, 100);
  buffer.append("hello");
  serde::detail::writeTrailer(sizes, std::nullopt, pool_.get(), buffer);

  const char* pos = buffer.data() + 1;
  varint::readVarint32(&pos);

  auto streams = serde::detail::parseStreams(
      pos,
      buffer.data() + buffer.size(),
      SerializationVersion::kCompact,
      pool_.get());

  ASSERT_EQ(streams.size(), 5);
  EXPECT_TRUE(streams[0].empty());
  EXPECT_TRUE(streams[1].empty());
  EXPECT_TRUE(streams[2].empty());
  EXPECT_TRUE(streams[3].empty());
  EXPECT_EQ(streams[4], "hello");
}

// Tests for encodeTyped / decodeStreamSizes roundtrip.

class EncodeDecodeTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("encode_decode_test");
  }

  // Helper to encode a uint32_t array using nimble encoding.
  // Returns the raw encoded buffer (no size prefix).
  std::string encodeAndWrite(const std::vector<uint32_t>& values) {
    facebook::nimble::Buffer encodingBuffer{*pool_};
    ManualEncodingSelectionPolicyFactory factory;
    auto encoded = serde::detail::encodeTyped<uint32_t>(
        values, encodingBuffer, [&factory](DataType dataType) {
          return factory.createPolicy(dataType);
        });
    return std::string(encoded.data(), encoded.size());
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(EncodeDecodeTest, decodeStreamSizesRoundtrip) {
  struct TestParam {
    std::vector<uint32_t> values;
    std::string debugString() const {
      return fmt::format("values.size() {}", values.size());
    }
  };
  std::vector<TestParam> testSettings = {
      {{}},
      {{0}},
      {{0, 1, 2}},
      {{5, 0, 3, 1, 4, 2}},
      {{100, 200, 300, 400, 500}},
  };
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto buffer = encodeAndWrite(testData.values);
    auto decoded = serde::detail::decodeStreamSizes(buffer, pool_.get());
    EXPECT_EQ(decoded, testData.values);
  }
}

TEST_F(EncodeDecodeTest, decodeStreamSizesLargeValues) {
  std::vector<uint32_t> values;
  for (uint32_t i = 0; i < 1000; ++i) {
    values.push_back(i * 3);
  }

  auto buffer = encodeAndWrite(values);
  auto decoded = serde::detail::decodeStreamSizes(buffer, pool_.get());
  EXPECT_EQ(decoded, values);
}

TEST_F(EncodeDecodeTest, streamSizesEncodingType) {
  std::vector<uint32_t> sizes = {2, 2, 2};

  for (auto encodingType :
       {EncodingType::Trivial,
        EncodingType::FixedBitWidth,
        EncodingType::Varint}) {
    SCOPED_TRACE(toString(encodingType));

    std::string buffer;
    serde::detail::writeHeader(buffer, SerializationVersion::kCompact, 100);
    buffer.append("s0");
    buffer.append("s1");
    buffer.append("s2");
    serde::detail::writeTrailer(sizes, encodingType, pool_.get(), buffer);

    const char* pos = buffer.data() + 1;
    varint::readVarint32(&pos);
    auto streams = serde::detail::parseStreams(
        pos,
        buffer.data() + buffer.size(),
        SerializationVersion::kCompact,
        pool_.get());

    ASSERT_EQ(streams.size(), 3);
    EXPECT_EQ(streams[0], "s0");
    EXPECT_EQ(streams[1], "s1");
    EXPECT_EQ(streams[2], "s2");
  }
}

TEST_F(EncodeDecodeTest, streamSizesEncodingTypeDefault) {
  std::vector<uint32_t> sizes = {1, 1, 1};

  std::string buffer;
  serde::detail::writeHeader(buffer, SerializationVersion::kCompact, 100);
  buffer.append("a");
  buffer.append("b");
  buffer.append("c");
  serde::detail::writeTrailer(sizes, std::nullopt, pool_.get(), buffer);

  const char* pos = buffer.data() + 1;
  varint::readVarint32(&pos);
  auto streams = serde::detail::parseStreams(
      pos,
      buffer.data() + buffer.size(),
      SerializationVersion::kCompact,
      pool_.get());

  ASSERT_EQ(streams.size(), 3);
  EXPECT_EQ(streams[0], "a");
  EXPECT_EQ(streams[1], "b");
  EXPECT_EQ(streams[2], "c");
}

// Tests for projectStreams

class ProjectStreamsTest : public ParseStreamsTest {
 protected:
  // Helper to build a kCompact buffer using writeHeader + raw data.
  // Takes (streamIndex, data) pairs and builds a dense sizes array.
  // Returns buffer starting after version byte + row count (streams position).
  std::string buildDenseBuffer(
      const std::vector<std::pair<uint32_t, std::string>>& indexAndData) {
    // Build dense sizes array.
    uint32_t maxIndex = 0;
    for (const auto& [index, _] : indexAndData) {
      maxIndex = std::max(maxIndex, index);
    }
    std::vector<uint32_t> sizes(indexAndData.empty() ? 0 : maxIndex + 1, 0);
    for (const auto& [index, data] : indexAndData) {
      sizes[index] = data.size();
    }

    std::string buffer;
    serde::detail::writeHeader(buffer, SerializationVersion::kCompact, 100);

    // Append raw stream data in index order (only non-zero sizes).
    // Sort by index to ensure correct order.
    auto sorted = indexAndData;
    std::sort(sorted.begin(), sorted.end());
    for (const auto& [_, data] : sorted) {
      buffer.append(data);
    }

    // Write trailer with encoded sizes.
    serde::detail::writeTrailer(sizes, std::nullopt, pool_.get(), buffer);

    // Skip version byte + varint row count to get to streams position.
    const char* pos = buffer.data() + 1;
    varint::readVarint32(&pos);
    auto offset = pos - buffer.data();
    return buffer.substr(offset);
  }
};

TEST_F(ProjectStreamsTest, legacySelectAll) {
  auto buffer = buildLegacyBuffer({"aaa", "bbb", "ccc"});
  std::vector<uint32_t> selected = {0, 1, 2};
  auto streams = serde::detail::projectStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kLegacy,
      selected,
      pool_.get());

  ASSERT_EQ(streams.size(), 3);
  EXPECT_EQ(streams[0].index, 0);
  EXPECT_EQ(streams[0].data, "aaa");
  EXPECT_EQ(streams[1].index, 1);
  EXPECT_EQ(streams[1].data, "bbb");
  EXPECT_EQ(streams[2].index, 2);
  EXPECT_EQ(streams[2].data, "ccc");
}

TEST_F(ProjectStreamsTest, legacySelectSubset) {
  auto buffer = buildLegacyBuffer({"aaa", "bbb", "ccc", "ddd", "eee"});
  std::vector<uint32_t> selected = {1, 3};
  auto streams = serde::detail::projectStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kLegacy,
      selected,
      pool_.get());

  ASSERT_EQ(streams.size(), 2);
  EXPECT_EQ(streams[0].index, 0);
  EXPECT_EQ(streams[0].data, "bbb");
  EXPECT_EQ(streams[1].index, 1);
  EXPECT_EQ(streams[1].data, "ddd");
}

TEST_F(ProjectStreamsTest, legacySelectFirst) {
  auto buffer = buildLegacyBuffer({"aaa", "bbb", "ccc"});
  std::vector<uint32_t> selected = {0};
  auto streams = serde::detail::projectStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kLegacy,
      selected,
      pool_.get());

  ASSERT_EQ(streams.size(), 1);
  EXPECT_EQ(streams[0].index, 0);
  EXPECT_EQ(streams[0].data, "aaa");
}

TEST_F(ProjectStreamsTest, legacySelectLast) {
  auto buffer = buildLegacyBuffer({"aaa", "bbb", "ccc"});
  std::vector<uint32_t> selected = {2};
  auto streams = serde::detail::projectStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kLegacy,
      selected,
      pool_.get());

  ASSERT_EQ(streams.size(), 1);
  EXPECT_EQ(streams[0].index, 0);
  EXPECT_EQ(streams[0].data, "ccc");
}

TEST_F(ProjectStreamsTest, legacySkipsEmptyStreams) {
  auto buffer = buildLegacyBuffer({"aaa", "", "ccc"});
  std::vector<uint32_t> selected = {0, 1, 2};
  auto streams = serde::detail::projectStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kLegacy,
      selected,
      pool_.get());

  // Empty stream at index 1 is skipped.
  ASSERT_EQ(streams.size(), 2);
  EXPECT_EQ(streams[0].index, 0);
  EXPECT_EQ(streams[0].data, "aaa");
  EXPECT_EQ(streams[1].index, 2);
  EXPECT_EQ(streams[1].data, "ccc");
}

TEST_F(ProjectStreamsTest, legacyAllEmpty) {
  auto buffer = buildLegacyBuffer({"", "", ""});
  std::vector<uint32_t> selected = {0, 1, 2};
  auto streams = serde::detail::projectStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kLegacy,
      selected,
      pool_.get());
  EXPECT_TRUE(streams.empty());
}

TEST_F(ProjectStreamsTest, denseSelectAll) {
  auto buffer = buildDenseBuffer({{0, "aaa"}, {1, "bbb"}, {2, "ccc"}});

  std::vector<uint32_t> selected = {0, 1, 2};
  auto streams = serde::detail::projectStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kCompact,
      selected,
      pool_.get());

  ASSERT_EQ(streams.size(), 3);
  EXPECT_EQ(streams[0].index, 0);
  EXPECT_EQ(streams[0].data, "aaa");
  EXPECT_EQ(streams[1].index, 1);
  EXPECT_EQ(streams[1].data, "bbb");
  EXPECT_EQ(streams[2].index, 2);
  EXPECT_EQ(streams[2].data, "ccc");
}

TEST_F(ProjectStreamsTest, denseSelectSubset) {
  auto buffer = buildDenseBuffer(
      {{0, "aaa"}, {1, "bbb"}, {2, "ccc"}, {3, "ddd"}, {4, "eee"}});

  std::vector<uint32_t> selected = {1, 3};
  auto streams = serde::detail::projectStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kCompact,
      selected,
      pool_.get());

  ASSERT_EQ(streams.size(), 2);
  EXPECT_EQ(streams[0].index, 0);
  EXPECT_EQ(streams[0].data, "bbb");
  EXPECT_EQ(streams[1].index, 1);
  EXPECT_EQ(streams[1].data, "ddd");
}

TEST_F(ProjectStreamsTest, denseWithGaps) {
  // Input has gaps (offsets 0, 2, 5). Select 0 and 5.
  auto buffer = buildDenseBuffer({{0, "zero"}, {2, "two"}, {5, "five"}});

  std::vector<uint32_t> selected = {0, 5};
  auto streams = serde::detail::projectStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kCompact,
      selected,
      pool_.get());

  ASSERT_EQ(streams.size(), 2);
  EXPECT_EQ(streams[0].index, 0);
  EXPECT_EQ(streams[0].data, "zero");
  EXPECT_EQ(streams[1].index, 1);
  EXPECT_EQ(streams[1].data, "five");
}

TEST_F(ProjectStreamsTest, denseSelectedNotInInput) {
  // Input has offsets {0, 2}. Select {0, 1, 2} — offset 1 is missing.
  auto buffer = buildDenseBuffer({{0, "zero"}, {2, "two"}});

  std::vector<uint32_t> selected = {0, 1, 2};
  auto streams = serde::detail::projectStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kCompact,
      selected,
      pool_.get());

  // Offset 1 not in input, so only 2 non-empty streams returned.
  ASSERT_EQ(streams.size(), 2);
  EXPECT_EQ(streams[0].index, 0);
  EXPECT_EQ(streams[0].data, "zero");
  EXPECT_EQ(streams[1].index, 2);
  EXPECT_EQ(streams[1].data, "two");
}

TEST_F(ProjectStreamsTest, denseEmpty) {
  auto buffer = buildDenseBuffer({});

  std::vector<uint32_t> selected = {0, 1};
  auto streams = serde::detail::projectStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kCompact,
      selected,
      pool_.get());
  EXPECT_TRUE(streams.empty());
}

TEST_F(ProjectStreamsTest, denseSelectFirstAndLast) {
  auto buffer = buildDenseBuffer({{0, "zero"}, {1, "one"}, {2, "two"}});

  std::vector<uint32_t> selected = {0, 2};
  auto streams = serde::detail::projectStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kCompact,
      selected,
      pool_.get());

  ASSERT_EQ(streams.size(), 2);
  EXPECT_EQ(streams[0].index, 0);
  EXPECT_EQ(streams[0].data, "zero");
  EXPECT_EQ(streams[1].index, 1);
  EXPECT_EQ(streams[1].data, "two");
}

TEST_F(ProjectStreamsTest, denseSkipsEmptyStreams) {
  auto buffer = buildDenseBuffer({{0, "aaa"}, {1, ""}, {2, "ccc"}});

  std::vector<uint32_t> selected = {0, 1, 2};
  auto streams = serde::detail::projectStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kCompact,
      selected,
      pool_.get());

  // Empty stream at offset 1 is skipped.
  ASSERT_EQ(streams.size(), 2);
  EXPECT_EQ(streams[0].index, 0);
  EXPECT_EQ(streams[0].data, "aaa");
  EXPECT_EQ(streams[1].index, 2);
  EXPECT_EQ(streams[1].data, "ccc");
}
