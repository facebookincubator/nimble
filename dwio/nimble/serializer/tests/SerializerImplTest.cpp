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
#include <zstd.h>

#include "dwio/nimble/common/ChunkHeader.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingLayout.h"
#include "dwio/nimble/serializer/DeserializerImpl.h"
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
  std::string buildCompactHeaderTrailer(
      uint32_t rowCount,
      const std::vector<uint32_t>& sizes,
      EncodingType encodingType = EncodingType::Trivial) {
    std::string buffer;
    serde::detail::writeHeader(
        buffer, SerializationVersion::kCompact, rowCount);
    facebook::nimble::Buffer encodingBuffer{*pool_};
    serde::detail::writeCompactTrailer(
        sizes, encodingType, encodingBuffer, buffer);
    return buffer;
  }

  // Helper to build a complete kCompactRaw buffer: header + raw trailer.
  std::string buildCompactRawHeaderTrailer(
      uint32_t rowCount,
      const std::vector<uint32_t>& sizes,
      EncodingType encodingType = EncodingType::Trivial) {
    std::string buffer;
    serde::detail::writeHeader(
        buffer, SerializationVersion::kCompactRaw, rowCount);
    serde::detail::writeRawTrailer(sizes, encodingType, buffer);
    return buffer;
  }

  // Helper to verify header by writing and parsing back using roundtrip.
  // For kCompact format, verifies the dense offsets array from trailer.
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
    const bool isCompact = isCompactFormat(expectedVersion);
    uint32_t actualRowCount;
    if (isCompact) {
      actualRowCount = varint::readVarint32(&pos);
    } else {
      actualRowCount = encoding::readUint32(pos);
    }
    EXPECT_EQ(actualRowCount, expectedRowCount);

    // For kCompact/kCompactRaw, verify stream sizes from trailer.
    if (isCompactFormat(expectedVersion)) {
      auto actualSizes =
          serde::detail::readStreamSizes(end, expectedVersion, pool_.get());
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

TEST_F(WriteHeaderTest, compactFormatWithSizes) {
  std::vector<uint32_t> sizes = {10, 20, 30};
  auto buffer = buildCompactHeaderTrailer(200, sizes);
  verifyHeader(buffer, SerializationVersion::kCompact, 200, sizes);
}

TEST_F(WriteHeaderTest, compactFormatEmptySizes) {
  auto buffer = buildCompactHeaderTrailer(10, {});
  verifyHeader(buffer, SerializationVersion::kCompact, 10, {});
}

TEST_F(WriteHeaderTest, compactFormatManySizes) {
  // Sizes array with 200 entries (100 non-zero interleaved with zeros).
  std::vector<uint32_t> sizes(200, 0);
  for (uint32_t i = 0; i < 100; ++i) {
    sizes[i * 2] = i + 1;
  }
  auto buffer = buildCompactHeaderTrailer(1000, sizes);
  verifyHeader(buffer, SerializationVersion::kCompact, 1000, sizes);
}

TEST_F(WriteHeaderTest, compactRawFormatTrivial) {
  std::vector<uint32_t> sizes = {10, 20, 30};
  auto buffer = buildCompactRawHeaderTrailer(200, sizes, EncodingType::Trivial);
  verifyHeader(buffer, SerializationVersion::kCompactRaw, 200, sizes);
}

TEST_F(WriteHeaderTest, compactRawFormatVarint) {
  std::vector<uint32_t> sizes = {10, 20, 30};
  auto buffer = buildCompactRawHeaderTrailer(200, sizes, EncodingType::Varint);
  verifyHeader(buffer, SerializationVersion::kCompactRaw, 200, sizes);
}

TEST_F(WriteHeaderTest, compactRawFormatDelta) {
  std::vector<uint32_t> sizes = {10, 20, 30};
  auto buffer = buildCompactRawHeaderTrailer(200, sizes, EncodingType::Delta);
  verifyHeader(buffer, SerializationVersion::kCompactRaw, 200, sizes);
}

TEST_F(WriteHeaderTest, compactRawFormatDeltaSimilarSizes) {
  // Stream sizes that are very similar benefit most from delta encoding.
  std::vector<uint32_t> sizes = {1'000, 1'002, 998, 1'001, 999, 1'003};
  auto buffer = buildCompactRawHeaderTrailer(500, sizes, EncodingType::Delta);
  verifyHeader(buffer, SerializationVersion::kCompactRaw, 500, sizes);
}

TEST_F(WriteHeaderTest, compactRawFormatDeltaWithZeros) {
  std::vector<uint32_t> sizes = {100, 0, 0, 50, 0, 200};
  auto buffer = buildCompactRawHeaderTrailer(300, sizes, EncodingType::Delta);
  verifyHeader(buffer, SerializationVersion::kCompactRaw, 300, sizes);
}

TEST_F(WriteHeaderTest, compactRawFormatDeltaSingleElement) {
  std::vector<uint32_t> sizes = {42};
  auto buffer = buildCompactRawHeaderTrailer(100, sizes, EncodingType::Delta);
  verifyHeader(buffer, SerializationVersion::kCompactRaw, 100, sizes);
}

TEST_F(WriteHeaderTest, compactRawFormatEmptySizes) {
  auto buffer = buildCompactRawHeaderTrailer(10, {});
  verifyHeader(buffer, SerializationVersion::kCompactRaw, 10, {});
}

TEST_F(WriteHeaderTest, compactRawFormatManySizes) {
  std::vector<uint32_t> sizes(200, 0);
  for (uint32_t i = 0; i < 100; ++i) {
    sizes[i * 2] = i + 1;
  }
  auto buffer = buildCompactRawHeaderTrailer(1000, sizes);
  verifyHeader(buffer, SerializationVersion::kCompactRaw, 1000, sizes);
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
    auto buffer = buildCompactHeaderTrailer(0, {});
    verifyHeader(buffer, SerializationVersion::kCompact, 0, {});
  }
  {
    SCOPED_TRACE("kCompactRaw");
    auto buffer = buildCompactRawHeaderTrailer(0, {});
    verifyHeader(buffer, SerializationVersion::kCompactRaw, 0, {});
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
        buildCompactHeaderTrailer(std::numeric_limits<uint32_t>::max(), {});
    verifyHeader(
        buffer,
        SerializationVersion::kCompact,
        std::numeric_limits<uint32_t>::max(),
        {});
  }
  {
    SCOPED_TRACE("kCompactRaw");
    auto buffer =
        buildCompactRawHeaderTrailer(std::numeric_limits<uint32_t>::max(), {});
    verifyHeader(
        buffer,
        SerializationVersion::kCompactRaw,
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

// Tests for estimateHeaderSize

TEST_F(WriteHeaderTest, estimateHeaderSizeCompact) {
  struct TestParam {
    uint32_t rowCount;
    std::string debugString() const {
      return fmt::format("rowCount {}", rowCount);
    }
  };
  std::vector<TestParam> testSettings = {
      {0},
      {1},
      {127},
      {128},
      {16383},
      {16384},
      {1000000},
      {std::numeric_limits<uint32_t>::max()}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    std::string buffer;
    serde::detail::writeHeader(
        buffer, SerializationVersion::kCompact, testData.rowCount);
    EXPECT_EQ(
        serde::detail::estimateHeaderSize(
            SerializationVersion::kCompact, testData.rowCount),
        buffer.size());
  }
}

TEST_F(WriteHeaderTest, estimateHeaderSizeLegacy) {
  struct TestParam {
    uint32_t rowCount;
    std::string debugString() const {
      return fmt::format("rowCount {}", rowCount);
    }
  };
  std::vector<TestParam> testSettings = {
      {0}, {1}, {1000}, {std::numeric_limits<uint32_t>::max()}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    std::string buffer;
    serde::detail::writeHeader(
        buffer, SerializationVersion::kLegacy, testData.rowCount);
    EXPECT_EQ(
        serde::detail::estimateHeaderSize(
            SerializationVersion::kLegacy, testData.rowCount),
        buffer.size());
  }
}

TEST_F(WriteHeaderTest, estimateHeaderSizeCompactRaw) {
  struct TestParam {
    uint32_t rowCount;
    std::string debugString() const {
      return fmt::format("rowCount {}", rowCount);
    }
  };
  std::vector<TestParam> testSettings = {
      {0},
      {1},
      {127},
      {128},
      {16383},
      {16384},
      {1000000},
      {std::numeric_limits<uint32_t>::max()}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    std::string buffer;
    serde::detail::writeHeader(
        buffer, SerializationVersion::kCompactRaw, testData.rowCount);
    EXPECT_EQ(
        serde::detail::estimateHeaderSize(
            SerializationVersion::kCompactRaw, testData.rowCount),
        buffer.size());
  }
}

TEST_F(WriteHeaderTest, estimateHeaderSizeNoVersion) {
  std::string buffer;
  serde::detail::writeHeader(buffer, std::nullopt, 100);
  EXPECT_EQ(
      serde::detail::estimateHeaderSize(std::nullopt, 100), buffer.size());
}

// Tests for estimateCompactTrailerSize

TEST_F(WriteHeaderTest, estimateCompactTrailerSize) {
  struct TestParam {
    std::vector<uint32_t> sizes;
    std::string debugString() const {
      return fmt::format("numStreams {}", sizes.size());
    }
  };
  std::vector<TestParam> testSettings = {
      {{}},
      {{10}},
      {{10, 20, 30}},
      {{0, 0, 0, 0, 5}},
      {std::vector<uint32_t>(100, 42)},
      {std::vector<uint32_t>(500, 0)}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    std::string buffer;
    facebook::nimble::Buffer encodingBuffer{*pool_};
    serde::detail::writeCompactTrailer(
        testData.sizes, EncodingType::Trivial, encodingBuffer, buffer);
    EXPECT_GE(
        serde::detail::estimateCompactTrailerSize(testData.sizes.size()),
        buffer.size())
        << "Estimate must be >= actual trailer size";
  }
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
  facebook::nimble::Buffer encodingBuffer{*pool_};
  serde::detail::writeCompactTrailer(
      {}, EncodingType::Trivial, encodingBuffer, buffer);

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
  facebook::nimble::Buffer encodingBuffer{*pool_};
  serde::detail::writeCompactTrailer(
      sizes, EncodingType::Trivial, encodingBuffer, buffer);

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
  facebook::nimble::Buffer encodingBuffer{*pool_};
  serde::detail::writeCompactTrailer(
      sizes, EncodingType::Trivial, encodingBuffer, buffer);

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
  facebook::nimble::Buffer encodingBuffer{*pool_};
  serde::detail::writeCompactTrailer(
      sizes, EncodingType::Trivial, encodingBuffer, buffer);

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

// Tests for readStreamSizes roundtrip (kCompact).

class EncodeDecodeTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("encode_decode_test");
  }

  // Helper to build a kCompact trailer (encoded sizes + trailer size u32).
  std::string buildCompactTrailer(const std::vector<uint32_t>& values) {
    std::string buffer;
    facebook::nimble::Buffer encodingBuffer{*pool_};
    serde::detail::writeCompactTrailer(
        values, EncodingType::Trivial, encodingBuffer, buffer);
    return buffer;
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(EncodeDecodeTest, readStreamSizesRoundtrip) {
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

    auto buffer = buildCompactTrailer(testData.values);
    auto decoded = serde::detail::readStreamSizes(
        buffer.data() + buffer.size(),
        SerializationVersion::kCompact,
        pool_.get());
    EXPECT_EQ(decoded, testData.values);
  }
}

TEST_F(EncodeDecodeTest, readStreamSizesLargeValues) {
  std::vector<uint32_t> values;
  for (uint32_t i = 0; i < 1000; ++i) {
    values.emplace_back(i * 3);
  }

  auto buffer = buildCompactTrailer(values);
  auto decoded = serde::detail::readStreamSizes(
      buffer.data() + buffer.size(),
      SerializationVersion::kCompact,
      pool_.get());
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
    facebook::nimble::Buffer encodingBuffer{*pool_};
    serde::detail::writeCompactTrailer(
        sizes, encodingType, encodingBuffer, buffer);

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
  facebook::nimble::Buffer encodingBuffer{*pool_};
  serde::detail::writeCompactTrailer(
      sizes, EncodingType::Trivial, encodingBuffer, buffer);

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

TEST_F(EncodeDecodeTest, streamSizesEncodingTypeCompactRaw) {
  std::vector<uint32_t> sizes = {10, 20, 30};

  for (auto encodingType :
       {EncodingType::Trivial, EncodingType::Varint, EncodingType::Delta}) {
    SCOPED_TRACE(toString(encodingType));

    std::string buffer;
    serde::detail::writeHeader(buffer, SerializationVersion::kCompactRaw, 100);
    buffer.append(std::string(10, 'a'));
    buffer.append(std::string(20, 'b'));
    buffer.append(std::string(30, 'c'));
    serde::detail::writeRawTrailer(sizes, encodingType, buffer);

    const char* pos = buffer.data() + 1;
    varint::readVarint32(&pos);
    auto streams = serde::detail::parseStreams(
        pos,
        buffer.data() + buffer.size(),
        SerializationVersion::kCompactRaw,
        pool_.get());

    ASSERT_EQ(streams.size(), 3);
    EXPECT_EQ(streams[0], std::string(10, 'a'));
    EXPECT_EQ(streams[1], std::string(20, 'b'));
    EXPECT_EQ(streams[2], std::string(30, 'c'));
  }
}

// Tests for projectStreams

namespace {

struct ProjectedStream {
  uint32_t index;
  std::string_view data;

  ProjectedStream(uint32_t idx, std::string_view d) : index(idx), data(d) {}

  bool operator<(const ProjectedStream& other) const {
    return index < other.index;
  }
};

std::vector<ProjectedStream> projectStreams(
    const char* pos,
    const char* end,
    SerializationVersion version,
    const std::vector<uint32_t>& selectedStreamIndices,
    facebook::velox::memory::MemoryPool* pool) {
  NIMBLE_CHECK_NOT_NULL(pool);
  std::vector<ProjectedStream> streams;
  streams.reserve(selectedStreamIndices.size());

  if (isCompactFormat(version)) {
    auto streamSizes = serde::detail::readStreamSizes(end, version, pool);
    const char* dataStart = pos;

    uint32_t dataOffset = 0;
    uint32_t nextSelectedStreamIdx = 0;
    const uint32_t numStreams = streamSizes.size();
    for (uint32_t i = 0;
         i < numStreams && nextSelectedStreamIdx < selectedStreamIndices.size();
         ++i) {
      if (i == selectedStreamIndices[nextSelectedStreamIdx]) {
        if (streamSizes[i] > 0) {
          streams.emplace_back(
              ProjectedStream{
                  nextSelectedStreamIdx,
                  {dataStart + dataOffset, streamSizes[i]}});
        }
        ++nextSelectedStreamIdx;
      }
      dataOffset += streamSizes[i];
    }
  } else {
    NIMBLE_CHECK_EQ(
        version,
        SerializationVersion::kLegacy,
        "unexpected version {}",
        version);
    for (uint32_t streamIdx = 0, nextProjected = 0;
         pos < end && nextProjected < selectedStreamIndices.size();
         ++streamIdx) {
      if (streamIdx == selectedStreamIndices[nextProjected]) {
        auto data = serde::detail::readStream<false>(pos);
        if (!data.empty()) {
          streams.emplace_back(nextProjected, data);
        }
        ++nextProjected;
      } else {
        serde::detail::skipStream<false>(pos);
      }
    }
  }

  return streams;
}

} // namespace

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
    facebook::nimble::Buffer encodingBuffer{*pool_};
    serde::detail::writeCompactTrailer(
        sizes, EncodingType::Trivial, encodingBuffer, buffer);

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
  auto streams = projectStreams(
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
  auto streams = projectStreams(
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
  auto streams = projectStreams(
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
  auto streams = projectStreams(
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
  auto streams = projectStreams(
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
  auto streams = projectStreams(
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
  auto streams = projectStreams(
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
  auto streams = projectStreams(
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
  auto streams = projectStreams(
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
  auto streams = projectStreams(
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
  auto streams = projectStreams(
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
  auto streams = projectStreams(
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

// Verifies that writeCompactTrailer does not compress stream sizes,
// even when the default encoding selection policy enables compression.
TEST_F(EncodeDecodeTest, compactTrailerNoCompression) {
  // Generate enough stream sizes that compression would normally be applied.
  std::vector<uint32_t> sizes(500);
  for (uint32_t i = 0; i < sizes.size(); ++i) {
    sizes[i] = i * 7 + 1;
  }

  std::string buffer;
  facebook::nimble::Buffer encodingBuffer{*pool_};
  serde::detail::writeCompactTrailer(
      sizes, EncodingType::Trivial, encodingBuffer, buffer);

  // Read the trailer: last 4 bytes are trailerSize, preceding bytes are
  // the encoded stream sizes.
  const auto* end = buffer.data() + buffer.size();
  const uint32_t trailerSize = serde::detail::readTrailerSize(end);
  const auto* trailerStart = end - sizeof(uint32_t) - trailerSize;

  // The encoded stream sizes use nimble encoding. The encoding prefix is:
  //   EncodingType (1B) + DataType (1B) + rowCount (varint)
  // For TrivialEncoding, the next byte after the prefix is the compression
  // type. Verify it is Uncompressed regardless of encoding type.
  auto encoding = EncodingFactory(Encoding::Options{.useVarintRowCount = true})
                      .create(*pool_, {trailerStart, trailerSize}, nullptr);
  EXPECT_EQ(encoding->rowCount(), sizes.size());

  // Verify roundtrip produces correct values (ensuring the encoding works).
  std::vector<uint32_t> decoded(sizes.size());
  encoding->materialize(sizes.size(), decoded.data());
  EXPECT_EQ(decoded, sizes);

  // Verify the compression byte in the raw encoding is Uncompressed.
  // After the encoding prefix (EncodingType + DataType + varint rowCount),
  // TrivialEncoding stores a 1-byte compression type.
  const auto compressionByte =
      static_cast<CompressionType>(trailerStart[encoding->dataOffset()]);
  EXPECT_EQ(compressionByte, CompressionType::Uncompressed);
}

TEST_F(ProjectStreamsTest, denseSkipsEmptyStreams) {
  auto buffer = buildDenseBuffer({{0, "aaa"}, {1, ""}, {2, "ccc"}});

  std::vector<uint32_t> selected = {0, 1, 2};
  auto streams = projectStreams(
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

// Tests for compressionOptions interaction with encoding layout tree.

class EncodeTypedCompressionTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("encode_typed_compression");
  }

  // Helper to verify leaf compression types in captured encoding layout.
  void verifyLeafCompression(
      const EncodingLayout& layout,
      CompressionType expectedType) {
    if (layout.encodingType() == EncodingType::Trivial ||
        layout.encodingType() == EncodingType::FixedBitWidth) {
      EXPECT_EQ(layout.compressionType(), expectedType);
    }
    for (uint32_t i = 0; i < layout.childrenCount(); ++i) {
      auto child = layout.child(i);
      if (child.has_value()) {
        verifyLeafCompression(child.value(), expectedType);
      }
    }
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

// Verify that compressionOptions controls compression when encoding layout tree
// is provided to encodeTyped.
TEST_F(EncodeTypedCompressionTest, withEncodingLayout) {
  facebook::nimble::Buffer buffer(*pool_);

  ManualEncodingSelectionPolicyFactory factory{
      ManualEncodingSelectionPolicyFactory::defaultReadFactors(),
      /*compressionOptions=*/std::nullopt};
  auto policyFactory = [&factory](DataType dataType) {
    return factory.createPolicy(dataType);
  };

  std::vector<uint32_t> data(100, 42);
  EncodingLayout layout{
      EncodingType::Trivial, {}, CompressionType::Uncompressed};

  struct TestParam {
    std::optional<CompressionOptions> compressionOptions;
    std::string debugString() const {
      return fmt::format("compress {}", compressionOptions.has_value());
    }
  };
  std::vector<TestParam> testSettings = {
      {std::nullopt},
      {CompressionOptions{}},
  };
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    buffer.reset();

    auto encoded = serde::detail::encodeTyped<uint32_t>(
        std::span<const uint32_t>(data),
        buffer,
        policyFactory,
        &layout,
        testData.compressionOptions);

    // Decode and verify round-trip.
    auto encoding =
        EncodingFactory(Encoding::Options{.useVarintRowCount = true})
            .create(*pool_, encoded, [&](uint32_t totalLength) -> void* {
              return pool_->allocate(totalLength);
            });
    EXPECT_EQ(encoding->encodingType(), EncodingType::Trivial);
    EXPECT_EQ(encoding->rowCount(), data.size());

    std::vector<uint32_t> decoded(data.size());
    encoding->materialize(data.size(), decoded.data());
    EXPECT_EQ(decoded, data);

    // Verify compression type in the encoded output.
    auto capturedLayout = EncodingLayoutCapture::capture(encoded);
    if (!testData.compressionOptions.has_value()) {
      verifyLeafCompression(capturedLayout, CompressionType::Uncompressed);
    }
  }
}

// Verify that compressionOptions has no effect when no encoding layout is
// provided (manual policy path). The policy factory's own compression settings
// are used instead.
TEST_F(EncodeTypedCompressionTest, withoutEncodingLayout) {
  facebook::nimble::Buffer buffer(*pool_);

  // Factory with compression disabled.
  ManualEncodingSelectionPolicyFactory noCompressFactory{
      ManualEncodingSelectionPolicyFactory::defaultReadFactors(),
      /*compressionOptions=*/std::nullopt};
  auto noCompressPolicyFactory = [&noCompressFactory](DataType dataType) {
    return noCompressFactory.createPolicy(dataType);
  };

  std::vector<uint32_t> data(100, 42);

  // Even though compressionOptions is set, it should be ignored because
  // no encoding layout is provided — the factory's own settings are used.
  buffer.reset();
  auto encoded = serde::detail::encodeTyped<uint32_t>(
      std::span<const uint32_t>(data),
      buffer,
      noCompressPolicyFactory,
      /*encodingLayout=*/nullptr,
      /*compressionOptions=*/CompressionOptions{});

  auto capturedLayout = EncodingLayoutCapture::capture(encoded);
  verifyLeafCompression(capturedLayout, CompressionType::Uncompressed);

  // Verify round-trip.
  auto encoding =
      EncodingFactory(Encoding::Options{.useVarintRowCount = true})
          .create(*pool_, encoded, [&](uint32_t totalLength) -> void* {
            return pool_->allocate(totalLength);
          });
  std::vector<uint32_t> decoded(data.size());
  encoding->materialize(data.size(), decoded.data());
  EXPECT_EQ(decoded, data);
}

// Verify that the default SerializerOptions.encodingSelectionPolicyFactory
// creates policies with compression disabled.
TEST_F(EncodeTypedCompressionTest, defaultFactoryNoCompression) {
  SerializerOptions options{};

  auto policy = options.encodingSelectionPolicyFactory(DataType::Uint32);
  auto* typed = dynamic_cast<EncodingSelectionPolicy<uint32_t>*>(policy.get());
  ASSERT_NE(typed, nullptr);

  std::vector<uint32_t> data(1000, 42);
  auto result = typed->select(data, Statistics<uint32_t>::create(data));
  auto compressionPolicy = result.compressionPolicyFactory();
  EXPECT_EQ(
      compressionPolicy->compression().compressionType,
      CompressionType::Uncompressed);
}

// Tests for kTabletRaw chunk header stripping in StreamDataReader.
// kTabletRaw streams contain chunk headers:
//   [chunkSize:u32][compressionType:1B][encoded_data...]
// StreamDataReader must strip these headers and decompress as needed.

class TabletRawChunkStripTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("tablet_raw_chunk_test");
  }

  // Builds a single uncompressed chunk: [length:u32][Uncompressed:1B][data].
  static std::string buildUncompressedChunk(std::string_view data) {
    std::string chunk(kChunkHeaderSize + data.size(), '\0');
    auto* pos = chunk.data();
    writeChunkHeader(
        static_cast<uint32_t>(data.size()), CompressionType::Uncompressed, pos);
    std::memcpy(pos, data.data(), data.size());
    return chunk;
  }

  // Builds a single zstd-compressed chunk: [length:u32][Zstd:1B][compressed].
  static std::string buildCompressedChunk(std::string_view data) {
    const auto maxCompressedSize = ZSTD_compressBound(data.size());
    std::string compressed(maxCompressedSize, '\0');
    const auto compressedSize = ZSTD_compress(
        compressed.data(), maxCompressedSize, data.data(), data.size(), 1);
    NIMBLE_CHECK(!ZSTD_isError(compressedSize));
    compressed.resize(compressedSize);

    std::string chunk(kChunkHeaderSize + compressedSize, '\0');
    auto* pos = chunk.data();
    writeChunkHeader(
        static_cast<uint32_t>(compressedSize), CompressionType::Zstd, pos);
    std::memcpy(pos, compressed.data(), compressedSize);
    return chunk;
  }

  // Concatenates multiple chunks into a single stream.
  static std::string concatChunks(const std::vector<std::string>& chunks) {
    std::string result;
    for (const auto& chunk : chunks) {
      result.append(chunk);
    }
    return result;
  }

  // Assembles a kTabletRaw buffer from streams and returns the data collected
  // by iterateStreams.
  // Each entry in 'streams' is the raw stream bytes (with chunk headers).
  std::vector<std::pair<uint32_t, std::string>> deserializeTabletRaw(
      uint32_t rowCount,
      const std::vector<std::pair<uint32_t, std::string>>& streams) {
    // Build dense sizes array.
    uint32_t maxOffset = 0;
    for (const auto& [offset, _] : streams) {
      maxOffset = std::max(maxOffset, offset);
    }
    std::vector<uint32_t> sizes(streams.empty() ? 0 : maxOffset + 1, 0);
    std::string streamData;
    for (const auto& [offset, data] : streams) {
      sizes[offset] = static_cast<uint32_t>(data.size());
      streamData.append(data);
    }

    // Assemble: [header][stream data][trailer].
    std::string buffer;
    serde::detail::writeHeader(
        buffer, SerializationVersion::kTabletRaw, rowCount);
    buffer.append(streamData);
    serde::detail::writeRawTrailer(sizes, EncodingType::Trivial, buffer);

    // Parse via StreamDataReader.
    DeserializerOptions options{.hasHeader = true};
    StreamDataReader reader(pool_.get(), options);
    auto actualRows = reader.initialize(std::string_view(buffer));
    EXPECT_EQ(actualRows, rowCount);

    std::vector<std::pair<uint32_t, std::string>> result;
    reader.iterateStreams([&](uint32_t offset, std::string_view data) {
      result.emplace_back(offset, std::string(data));
    });
    return result;
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(TabletRawChunkStripTest, singleChunkUncompressed) {
  std::string payload = "hello world test data";
  auto stream = buildUncompressedChunk(payload);
  auto result = deserializeTabletRaw(10, {{0, stream}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, payload);
}

TEST_F(TabletRawChunkStripTest, singleChunkCompressed) {
  std::string payload = "compressed data that should be zstd encoded";
  auto stream = buildCompressedChunk(payload);
  auto result = deserializeTabletRaw(10, {{0, stream}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, payload);
}

TEST_F(TabletRawChunkStripTest, multipleChunksAllUncompressed) {
  std::string part1 = "first chunk data";
  std::string part2 = "second chunk data";
  std::string part3 = "third chunk data";
  auto stream = concatChunks(
      {buildUncompressedChunk(part1),
       buildUncompressedChunk(part2),
       buildUncompressedChunk(part3)});
  auto result = deserializeTabletRaw(10, {{0, stream}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, part1 + part2 + part3);
}

TEST_F(TabletRawChunkStripTest, multipleChunksAllCompressed) {
  std::string part1 = "first chunk of compressed data here";
  std::string part2 = "second chunk of compressed data here";
  auto stream =
      concatChunks({buildCompressedChunk(part1), buildCompressedChunk(part2)});
  auto result = deserializeTabletRaw(10, {{0, stream}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, part1 + part2);
}

TEST_F(TabletRawChunkStripTest, multipleChunksMixed) {
  std::string part1 = "uncompressed chunk one";
  std::string part2 = "compressed chunk two with some data";
  std::string part3 = "another uncompressed chunk three";
  auto stream = concatChunks(
      {buildUncompressedChunk(part1),
       buildCompressedChunk(part2),
       buildUncompressedChunk(part3)});
  auto result = deserializeTabletRaw(10, {{0, stream}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, part1 + part2 + part3);
}

TEST_F(TabletRawChunkStripTest, multipleStreams) {
  std::string payload1 = "stream zero payload";
  std::string payload2 = "stream two payload data";
  auto stream0 = buildUncompressedChunk(payload1);
  auto stream2 = buildCompressedChunk(payload2);
  // Stream offsets 0 and 2 (gap at 1).
  auto result = deserializeTabletRaw(10, {{0, stream0}, {2, stream2}});

  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, payload1);
  EXPECT_EQ(result[1].first, 2);
  EXPECT_EQ(result[1].second, payload2);
}

TEST_F(TabletRawChunkStripTest, multipleStreamsMultipleChunks) {
  // Stream 0: two uncompressed chunks.
  std::string s0p1 = "stream0 part1";
  std::string s0p2 = "stream0 part2";
  auto stream0 = concatChunks(
      {buildUncompressedChunk(s0p1), buildUncompressedChunk(s0p2)});

  // Stream 1: one compressed chunk.
  std::string s1p1 = "stream1 compressed single chunk";
  auto stream1 = buildCompressedChunk(s1p1);

  // Stream 2: mixed chunks.
  std::string s2p1 = "stream2 uncompressed";
  std::string s2p2 = "stream2 compressed part";
  auto stream2 =
      concatChunks({buildUncompressedChunk(s2p1), buildCompressedChunk(s2p2)});

  auto result =
      deserializeTabletRaw(10, {{0, stream0}, {1, stream1}, {2, stream2}});

  ASSERT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, s0p1 + s0p2);
  EXPECT_EQ(result[1].first, 1);
  EXPECT_EQ(result[1].second, s1p1);
  EXPECT_EQ(result[2].first, 2);
  EXPECT_EQ(result[2].second, s2p1 + s2p2);
}

TEST_F(TabletRawChunkStripTest, emptyPayloadChunk) {
  // Single uncompressed chunk with empty payload.
  // The chunk header is still present (5 bytes), so iterateStreams calls the
  // callback with the stripped (empty) data.
  auto stream = buildUncompressedChunk("");
  auto result = deserializeTabletRaw(10, {{0, stream}});
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_TRUE(result[0].second.empty());
}

TEST_F(TabletRawChunkStripTest, largePayload) {
  // Large payload to exercise buffer growth.
  std::string payload(100'000, 'x');
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<char>('a' + (i % 26));
  }

  // Test single compressed chunk with large data.
  auto stream = buildCompressedChunk(payload);
  auto result = deserializeTabletRaw(10, {{0, stream}});
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].second, payload);
}

TEST_F(TabletRawChunkStripTest, largePayloadMultipleChunks) {
  // Split large payload across multiple chunks of different types.
  std::string payload(50'000, '\0');
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<char>(i % 256);
  }

  std::string part1 = payload.substr(0, 20'000);
  std::string part2 = payload.substr(20'000, 15'000);
  std::string part3 = payload.substr(35'000);

  auto stream = concatChunks(
      {buildUncompressedChunk(part1),
       buildCompressedChunk(part2),
       buildCompressedChunk(part3)});
  auto result = deserializeTabletRaw(10, {{0, stream}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].second, payload);
}

// Tests for ZSTD_DCtx reuse in StreamData and StreamDataReader (D99456594).
// Inherits from TabletRawChunkStripTest for shared helpers
// (buildCompressedChunk, pool_, etc.).

class ZstdDCtxReuseTest : public TabletRawChunkStripTest {
 protected:
  void SetUp() override {
    TabletRawChunkStripTest::SetUp();
    dctx_.reset(ZSTD_createDCtx());
  }

  // Build legacy-format compressed stream data for a non-string scalar:
  // [CompressionType::Zstd (1B)][ZSTD-compressed payload]
  static std::string buildLegacyCompressedData(std::string_view payload) {
    std::string result;
    result.push_back(static_cast<char>(CompressionType::Zstd));
    const auto maxSize = ZSTD_compressBound(payload.size());
    const auto offset = result.size();
    result.resize(offset + maxSize);
    const auto compressedSize = ZSTD_compress(
        result.data() + offset, maxSize, payload.data(), payload.size(), 1);
    NIMBLE_CHECK(!ZSTD_isError(compressedSize));
    result.resize(offset + compressedSize);
    return result;
  }

  // Like TabletRawChunkStripTest::deserializeTabletRaw, but passes
  // the fixture's shared dctx to StreamDataReader.
  std::vector<std::pair<uint32_t, std::string>> deserializeTabletRawWithDCtx(
      uint32_t rowCount,
      const std::vector<std::pair<uint32_t, std::string>>& streams) {
    uint32_t maxOffset = 0;
    for (const auto& [offset, _] : streams) {
      maxOffset = std::max(maxOffset, offset);
    }
    std::vector<uint32_t> sizes(streams.empty() ? 0 : maxOffset + 1, 0);
    std::string streamData;
    for (const auto& [offset, data] : streams) {
      sizes[offset] = static_cast<uint32_t>(data.size());
      streamData.append(data);
    }

    std::string buffer;
    serde::detail::writeHeader(
        buffer, SerializationVersion::kTabletRaw, rowCount);
    buffer.append(streamData);
    serde::detail::writeRawTrailer(sizes, EncodingType::Trivial, buffer);

    DeserializerOptions options{.hasHeader = true};
    StreamDataReader reader(pool_.get(), options, dctx_.get());
    auto actualRows = reader.initialize(std::string_view(buffer));
    EXPECT_EQ(actualRows, rowCount);

    std::vector<std::pair<uint32_t, std::string>> result;
    reader.iterateStreams([&](uint32_t offset, std::string_view data) {
      result.emplace_back(offset, std::string(data));
    });
    return result;
  }

  struct DCtxDeleter {
    void operator()(ZSTD_DCtx* ctx) const {
      ZSTD_freeDCtx(ctx);
    }
  };

  std::unique_ptr<ZSTD_DCtx, DCtxDeleter> dctx_;
};

TEST_F(ZstdDCtxReuseTest, streamDataLegacyZstdWithDCtx) {
  const std::vector<int32_t> expected = {10, 20, 30, 40};
  std::string_view payload(
      reinterpret_cast<const char*>(expected.data()),
      expected.size() * sizeof(int32_t));
  auto compressed = buildLegacyCompressedData(payload);

  serde::StreamData sd(
      ScalarKind::Int32,
      SerializationVersion::kLegacy,
      compressed,
      pool_.get(),
      dctx_.get());

  std::vector<int32_t> output(expected.size());
  sd.copyTo(
      reinterpret_cast<char*>(output.data()), output.size() * sizeof(int32_t));
  EXPECT_EQ(output, expected);
}

TEST_F(ZstdDCtxReuseTest, streamDataDCtxReusedAcrossReset) {
  const std::vector<int32_t> values1 = {1, 2, 3};
  std::string_view payload1(
      reinterpret_cast<const char*>(values1.data()),
      values1.size() * sizeof(int32_t));
  auto compressed1 = buildLegacyCompressedData(payload1);

  const std::vector<int32_t> values2 = {100, 200};
  std::string_view payload2(
      reinterpret_cast<const char*>(values2.data()),
      values2.size() * sizeof(int32_t));
  auto compressed2 = buildLegacyCompressedData(payload2);

  // First decompression with dctx.
  serde::StreamData sd(
      ScalarKind::Int32,
      SerializationVersion::kLegacy,
      compressed1,
      pool_.get(),
      dctx_.get());

  std::vector<int32_t> output1(values1.size());
  sd.copyTo(
      reinterpret_cast<char*>(output1.data()),
      output1.size() * sizeof(int32_t));
  EXPECT_EQ(output1, values1);

  // Reset with new data, same dctx.
  sd.reset(compressed2, SerializationVersion::kLegacy, dctx_.get());

  std::vector<int32_t> output2(values2.size());
  sd.copyTo(
      reinterpret_cast<char*>(output2.data()),
      output2.size() * sizeof(int32_t));
  EXPECT_EQ(output2, values2);
}

TEST_F(ZstdDCtxReuseTest, streamDataReaderCompressedChunkWithDCtx) {
  std::string payload = "compressed data that should be zstd encoded for test";
  auto chunk = buildCompressedChunk(payload);
  auto result = deserializeTabletRawWithDCtx(10, {{0, chunk}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, payload);
}

TEST_F(ZstdDCtxReuseTest, streamDataReaderDCtxReusedAcrossStreams) {
  std::string payload1 = "first compressed stream payload data";
  std::string payload2 = "second compressed stream payload data";
  auto chunk1 = buildCompressedChunk(payload1);
  auto chunk2 = buildCompressedChunk(payload2);

  auto result = deserializeTabletRawWithDCtx(10, {{0, chunk1}, {1, chunk2}});

  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].second, payload1);
  EXPECT_EQ(result[1].second, payload2);
}
