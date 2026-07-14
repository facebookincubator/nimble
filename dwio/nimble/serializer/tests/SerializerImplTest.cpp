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
#include <random>
#include <string_view>

#include <gtest/gtest.h>
#include <lz4.h>
#include <zstd.h>

#include "dwio/nimble/common/ChunkHeader.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingLayout.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/serializer/DeserializerImpl.h"
#include "dwio/nimble/serializer/SerializerImpl.h"
#include "velox/common/memory/Memory.h"

using namespace facebook::nimble;
using namespace facebook::nimble::serde;
using namespace facebook::velox;

namespace {

// Test-only helper: scatter the sparse (stream ids, sizes) trailer into a dense
// vector sized to max(stream ids) + 1, matching the legacy dense round-trip
// view the tests below were written against.
std::vector<uint32_t> readTrailerStreamMetadataDenseForTest(const char* end) {
  auto [streamIds, sizes] = serde::detail::readTrailerStreamMetadata(end);
  if (streamIds.empty()) {
    return {};
  }
  std::vector<uint32_t> dense(streamIds.back() + 1, 0);
  for (size_t i = 0; i < streamIds.size(); ++i) {
    dense[streamIds[i]] = sizes[i];
  }
  return dense;
}

// Writes the kTablet trailer for a sequentially-laid-out body: every present
// slot is its own unique stream, so its stream size index is the running unique
// index and uniqueStreamSizes equals the present sizes in order. Test
// scaffolding that assembles kTablet chunks must emit the same trailer layout.
void writeTabletTrailer(
    const std::vector<uint32_t>& sizes,
    std::string& buffer) {
  std::vector<uint32_t> streamIds;
  std::vector<uint32_t> streamSizeIndices;
  std::vector<uint32_t> uniqueStreamSizes;
  for (size_t i = 0; i < sizes.size(); ++i) {
    const auto streamSize = sizes[i];
    if (streamSize == 0) {
      continue;
    }
    streamIds.emplace_back(static_cast<uint32_t>(i));
    streamSizeIndices.emplace_back(
        static_cast<uint32_t>(uniqueStreamSizes.size()));
    uniqueStreamSizes.emplace_back(streamSize);
  }
  serde::detail::writeTrailer(
      streamIds,
      streamSizeIndices,
      uniqueStreamSizes,
      EncodingType::Trivial,
      EncodingType::Trivial,
      EncodingType::Trivial,
      buffer);
}

void skipTrailerSectionPayloadForTest(
    EncodingType encodingType,
    const char*& payload) {
  switch (encodingType) {
    case EncodingType::Trivial: {
      const auto count = varint::readVarint32(&payload);
      payload += count * sizeof(uint32_t);
      return;
    }
    case EncodingType::Varint: {
      const auto count = varint::readVarint32(&payload);
      for (uint32_t i = 0; i < count; ++i) {
        varint::readVarint32(&payload);
      }
      return;
    }
    case EncodingType::Delta: {
      const auto count = varint::readVarint32(&payload);
      for (uint32_t i = 0; i < count; ++i) {
        varint::readVarint32(&payload);
      }
      return;
    }
    case EncodingType::FixedBitWidth: {
      const auto bitWidth = static_cast<uint8_t>(*payload++);
      const auto count = varint::readVarint32(&payload);
      if (bitWidth > 0 && count > 0) {
        payload += FixedBitArray::bufferSize(count, bitWidth);
      }
      return;
    }
    default:
      FAIL() << "Unsupported trailer encoding type: " << encodingType;
  }
}

void expectTabletTrailerEncodingTypes(
    const std::string& buffer,
    EncodingType streamIdsEncodingType,
    EncodingType sizeIndicesEncodingType,
    EncodingType uniqueSizesEncodingType) {
  const char* const end = buffer.data() + buffer.size();
  const auto trailerSize = serde::detail::readTrailerSize(end);
  const char* payload = end - sizeof(uint32_t) - trailerSize;
  const char* const trailerEnd = end - sizeof(uint32_t);

  const auto verifySection = [&](EncodingType expectedEncodingType) {
    ASSERT_LT(payload, trailerEnd);
    const auto actualEncodingType =
        static_cast<EncodingType>(static_cast<uint8_t>(*payload++));
    EXPECT_EQ(actualEncodingType, expectedEncodingType);
    skipTrailerSectionPayloadForTest(actualEncodingType, payload);
  };

  verifySection(streamIdsEncodingType);
  verifySection(sizeIndicesEncodingType);
  verifySection(uniqueSizesEncodingType);
  EXPECT_EQ(payload, trailerEnd);
}

} // namespace

class WriteHeaderTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("write_header_test");
  }

  // Helper to build a complete kLegacyCompact buffer: header + raw trailer.
  // Uses the same encoding for both the indices and sizes axes of the
  // two-array trailer.
  std::string buildCompactRawHeaderTrailer(
      uint32_t rowCount,
      const std::vector<uint32_t>& sizes,
      EncodingType encodingType = EncodingType::Trivial) {
    std::string buffer;
    serde::writeSerializationHeader(
        buffer, SerializationVersion::kSerialization, rowCount);
    serde::detail::writeTrailer(sizes, encodingType, encodingType, buffer);
    return buffer;
  }

  // Helper to verify header by writing and parsing back using roundtrip.
  // For kLegacyCompact format, verifies the dense offsets array from trailer.
  // The dense reconstruction may be shorter than expectedSizes when trailing
  // entries are zero — pad with zeros to expectedSizes.size() before
  // comparing so the test's dense data model still works against the
  // sparse-on-wire trailer.
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

    // Read row count. Non-legacy formats use varint; kLegacy uses u32.
    uint32_t actualRowCount;
    if (usesVarintRowCount(expectedVersion)) {
      actualRowCount = varint::readVarint32(&pos);
    } else {
      actualRowCount = encoding::readUint32(pos);
    }
    EXPECT_EQ(actualRowCount, expectedRowCount);

    // Skip the flags byte for versions that carry one (no nulls expected here).
    if (usesHeaderFlags(expectedVersion)) {
      EXPECT_EQ(static_cast<uint8_t>(*pos++), 0)
          << "unexpected null-barrier flag";
    }

    // Sequential stream layouts keep stream sizes in the trailer.
    if (nonLegacyFormat(expectedVersion) && !isTabletVersion(expectedVersion)) {
      auto actualSizes = readTrailerStreamMetadataDenseForTest(end);
      if (actualSizes.size() < expectedSizes.size()) {
        actualSizes.resize(expectedSizes.size(), 0);
      }
      EXPECT_EQ(actualSizes, expectedSizes);
    }
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(WriteHeaderTest, compactRawFormatTrivial) {
  std::vector<uint32_t> sizes = {10, 20, 30};
  auto buffer = buildCompactRawHeaderTrailer(200, sizes, EncodingType::Trivial);
  verifyHeader(buffer, SerializationVersion::kSerialization, 200, sizes);
}

TEST_F(WriteHeaderTest, compactRawFormatVarint) {
  std::vector<uint32_t> sizes = {10, 20, 30};
  auto buffer = buildCompactRawHeaderTrailer(200, sizes, EncodingType::Varint);
  verifyHeader(buffer, SerializationVersion::kSerialization, 200, sizes);
}

TEST_F(WriteHeaderTest, compactRawFormatDelta) {
  std::vector<uint32_t> sizes = {10, 20, 30};
  auto buffer = buildCompactRawHeaderTrailer(200, sizes, EncodingType::Delta);
  verifyHeader(buffer, SerializationVersion::kSerialization, 200, sizes);
}

TEST_F(WriteHeaderTest, compactRawFormatDeltaSimilarSizes) {
  // Stream sizes that are very similar benefit most from delta encoding.
  std::vector<uint32_t> sizes = {1'000, 1'002, 998, 1'001, 999, 1'003};
  auto buffer = buildCompactRawHeaderTrailer(500, sizes, EncodingType::Delta);
  verifyHeader(buffer, SerializationVersion::kSerialization, 500, sizes);
}

TEST_F(WriteHeaderTest, compactRawFormatDeltaWithZeros) {
  std::vector<uint32_t> sizes = {100, 0, 0, 50, 0, 200};
  auto buffer = buildCompactRawHeaderTrailer(300, sizes, EncodingType::Delta);
  verifyHeader(buffer, SerializationVersion::kSerialization, 300, sizes);
}

TEST_F(WriteHeaderTest, compactRawFormatDeltaSingleElement) {
  std::vector<uint32_t> sizes = {42};
  auto buffer = buildCompactRawHeaderTrailer(100, sizes, EncodingType::Delta);
  verifyHeader(buffer, SerializationVersion::kSerialization, 100, sizes);
}

TEST_F(WriteHeaderTest, compactRawFormatEmptySizes) {
  auto buffer = buildCompactRawHeaderTrailer(10, {});
  verifyHeader(buffer, SerializationVersion::kSerialization, 10, {});
}

TEST_F(WriteHeaderTest, compactRawFormatManySizes) {
  std::vector<uint32_t> sizes(200, 0);
  for (uint32_t i = 0; i < 100; ++i) {
    sizes[i * 2] = i + 1;
  }
  auto buffer = buildCompactRawHeaderTrailer(1000, sizes);
  verifyHeader(buffer, SerializationVersion::kSerialization, 1000, sizes);
}

TEST_F(WriteHeaderTest, zeroRowCount) {
  auto buffer = buildCompactRawHeaderTrailer(0, {});
  verifyHeader(buffer, SerializationVersion::kSerialization, 0, {});
}

TEST_F(WriteHeaderTest, maxRowCount) {
  auto buffer =
      buildCompactRawHeaderTrailer(std::numeric_limits<uint32_t>::max(), {});
  verifyHeader(
      buffer,
      SerializationVersion::kSerialization,
      std::numeric_limits<uint32_t>::max(),
      {});
}

// Tests for estimateHeaderSize

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
    serde::writeSerializationHeader(
        buffer, SerializationVersion::kSerialization, testData.rowCount);
    EXPECT_EQ(
        serde::estimateSerializationHeaderSize(
            SerializationVersion::kSerialization, testData.rowCount),
        buffer.size());
  }
}

// Tests for estimateTrailerSize

TEST_F(WriteHeaderTest, estimateTrailerSize) {
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
    serde::detail::writeTrailer(
        testData.sizes, EncodingType::Trivial, EncodingType::Trivial, buffer);
    EXPECT_GE(
        serde::detail::estimateTrailerSize(
            testData.sizes.size(),
            EncodingType::Trivial,
            EncodingType::Trivial),
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

// kSerialization format tests use writeHeader/parseStreams roundtrip since
// the size encoding is now opaque (nimble-encoded).

TEST_F(ParseStreamsTest, denseFormatEmpty) {
  // Write header + trailer with empty sizes array, then parse.
  std::string buffer;
  serde::writeSerializationHeader(
      buffer, SerializationVersion::kSerialization, 10);
  serde::detail::writeTrailer(
      {}, EncodingType::Trivial, EncodingType::Trivial, buffer);

  // Skip the header to reach the streams.
  const char* streamsPos = buffer.data();
  serde::readSerializationHeader(
      streamsPos, buffer.data() + buffer.size(), /*hasHeader=*/true);
  auto streams = serde::detail::parseStreams(
      streamsPos,
      buffer.data() + buffer.size(),
      SerializationVersion::kSerialization,
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
  serde::writeSerializationHeader(
      buffer, SerializationVersion::kSerialization, 100);
  for (const auto& d : data) {
    buffer.append(d);
  }
  serde::detail::writeTrailer(
      sizes, EncodingType::Trivial, EncodingType::Trivial, buffer);

  // Skip the header.
  const char* pos = buffer.data();
  serde::readSerializationHeader(
      pos, buffer.data() + buffer.size(), /*hasHeader=*/true);

  auto streams = serde::detail::parseStreams(
      pos,
      buffer.data() + buffer.size(),
      SerializationVersion::kSerialization,
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
  serde::writeSerializationHeader(
      buffer, SerializationVersion::kSerialization, 100);
  buffer.append("zero");
  buffer.append("two");
  buffer.append("five");
  serde::detail::writeTrailer(
      sizes, EncodingType::Trivial, EncodingType::Trivial, buffer);

  const char* pos = buffer.data();
  serde::readSerializationHeader(
      pos, buffer.data() + buffer.size(), /*hasHeader=*/true);

  auto streams = serde::detail::parseStreams(
      pos,
      buffer.data() + buffer.size(),
      SerializationVersion::kSerialization,
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
  serde::writeSerializationHeader(
      buffer, SerializationVersion::kSerialization, 100);
  buffer.append("hello");
  serde::detail::writeTrailer(
      sizes, EncodingType::Trivial, EncodingType::Trivial, buffer);

  const char* pos = buffer.data();
  serde::readSerializationHeader(
      pos, buffer.data() + buffer.size(), /*hasHeader=*/true);

  auto streams = serde::detail::parseStreams(
      pos,
      buffer.data() + buffer.size(),
      SerializationVersion::kSerialization,
      pool_.get());

  ASSERT_EQ(streams.size(), 5);
  EXPECT_TRUE(streams[0].empty());
  EXPECT_TRUE(streams[1].empty());
  EXPECT_TRUE(streams[2].empty());
  EXPECT_TRUE(streams[3].empty());
  EXPECT_EQ(streams[4], "hello");
}

// Tests for readTrailerStreamMetadata roundtrip (kLegacyCompact).

class EncodeDecodeTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("encode_decode_test");
  }

  // Helper to build a kLegacyCompact trailer (two-array sparse) using the same
  // encoding on both axes.
  std::string buildCompactTrailer(const std::vector<uint32_t>& values) {
    return buildCompactTrailer(
        values, EncodingType::Trivial, EncodingType::Trivial);
  }

  std::string buildCompactTrailer(
      const std::vector<uint32_t>& values,
      EncodingType indicesEnc,
      EncodingType sizesEnc) {
    std::string buffer;
    serde::detail::writeTrailer(values, indicesEnc, sizesEnc, buffer);
    return buffer;
  }

  // Roundtrip-compare the dense view, padding the readback with trailing
  // zeros to match the expected dense length (the sparse-on-wire trailer
  // does not preserve trailing zeros structurally — only via the schema's
  // streamCount which lives outside the trailer).
  void expectDenseRoundtripEq(
      const std::vector<uint32_t>& expected,
      const std::string& buffer) {
    auto decoded =
        readTrailerStreamMetadataDenseForTest(buffer.data() + buffer.size());
    if (decoded.size() < expected.size()) {
      decoded.resize(expected.size(), 0);
    }
    EXPECT_EQ(decoded, expected);
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(EncodeDecodeTest, readTrailerStreamMetadataRoundtrip) {
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
    expectDenseRoundtripEq(testData.values, buffer);
  }
}

// Round-trips the kTablet trailer layout (streamIds, sizeIndices, uniqueSizes).
// Duplicate slots carry the stream size index of the stream they alias, so the
// reader rebuilds the same (streamIds, offsets, sizes) triple without storing
// offsets or duplicated sizes.
TEST_F(EncodeDecodeTest, tabletTrailerRoundtrip) {
  // Slot 2 aliases slot 0 (size index 0) and slot 4 aliases slot 1
  // (size index 1). Unique streams in body order are slots 0, 1, 5.
  const std::vector<uint32_t> streamIds = {0, 1, 2, 4, 5};
  const std::vector<uint32_t> streamSizeIndices = {0, 1, 0, 1, 2};
  const std::vector<uint32_t> uniqueStreamSizes = {100, 50, 80};
  constexpr auto kStreamIdsEncoding = EncodingType::Varint;
  constexpr auto kSizeIndicesEncoding = EncodingType::Trivial;
  constexpr auto kUniqueSizesEncoding = EncodingType::FixedBitWidth;

  std::string buffer;
  serde::detail::writeTrailer(
      streamIds,
      streamSizeIndices,
      uniqueStreamSizes,
      kStreamIdsEncoding,
      kSizeIndicesEncoding,
      kUniqueSizesEncoding,
      buffer);
  expectTabletTrailerEncodingTypes(
      buffer, kStreamIdsEncoding, kSizeIndicesEncoding, kUniqueSizesEncoding);
  EXPECT_LE(
      buffer.size(),
      serde::detail::estimateTrailerSize(
          streamIds.size(),
          uniqueStreamSizes.size(),
          kStreamIdsEncoding,
          kSizeIndicesEncoding,
          kUniqueSizesEncoding));

  std::vector<uint32_t> decodedStreamIds;
  std::vector<uint32_t> offsets;
  std::vector<uint32_t> sizes;
  std::vector<uint32_t> uniqueStreamSizesScratch;
  serde::detail::readTrailerStreamMetadata(
      buffer.data() + buffer.size(),
      decodedStreamIds,
      offsets,
      sizes,
      uniqueStreamSizesScratch);

  const std::vector<uint32_t> expectedStreamIds = {0, 1, 2, 4, 5};
  const std::vector<uint32_t> expectedOffsets = {0, 100, 0, 100, 150};
  const std::vector<uint32_t> expectedSizes = {100, 50, 100, 50, 80};
  EXPECT_EQ(decodedStreamIds, expectedStreamIds);
  EXPECT_EQ(offsets, expectedOffsets);
  EXPECT_EQ(sizes, expectedSizes);
}

// Verifies that every kTablet trailer section encoding combination round-trips
// to the same decoded (streamIds, offsets, sizes) metadata.
TEST_F(EncodeDecodeTest, tabletTrailerEncodingCombinations) {
  const std::vector<EncodingType> encodings = {
      EncodingType::Trivial,
      EncodingType::Varint,
      EncodingType::Delta,
      EncodingType::FixedBitWidth,
  };
  struct TestCase {
    std::string_view label;
    std::vector<uint32_t> streamIds;
    std::vector<uint32_t> streamSizeIndices;
    std::vector<uint32_t> uniqueStreamSizes;
    std::vector<uint32_t> expectedOffsets;
    std::vector<uint32_t> expectedSizes;
  };
  const std::vector<TestCase> testCases = {
      {
          "empty",
          {},
          {},
          {},
          {},
          {},
      },
      {
          "no-duplicates",
          {0, 3, 7},
          {0, 1, 2},
          {10, 20, 30},
          {0, 10, 30},
          {10, 20, 30},
      },
      {
          "duplicates",
          {0, 1, 2, 4, 5},
          {0, 1, 0, 1, 2},
          {100, 50, 80},
          {0, 100, 0, 100, 150},
          {100, 50, 100, 50, 80},
      },
  };

  for (const auto streamIdsEnc : encodings) {
    for (const auto sizeIndicesEnc : encodings) {
      for (const auto uniqueSizesEnc : encodings) {
        for (const auto& testCase : testCases) {
          SCOPED_TRACE(
              fmt::format(
                  "streamIdsEnc={} sizeIndicesEnc={} uniqueSizesEnc={} case={}",
                  toString(streamIdsEnc),
                  toString(sizeIndicesEnc),
                  toString(uniqueSizesEnc),
                  testCase.label));
          std::string buffer;
          serde::detail::writeTrailer(
              testCase.streamIds,
              testCase.streamSizeIndices,
              testCase.uniqueStreamSizes,
              streamIdsEnc,
              sizeIndicesEnc,
              uniqueSizesEnc,
              buffer);
          expectTabletTrailerEncodingTypes(
              buffer, streamIdsEnc, sizeIndicesEnc, uniqueSizesEnc);

          const auto estimate = serde::detail::estimateTrailerSize(
              testCase.streamIds.size(),
              testCase.uniqueStreamSizes.size(),
              streamIdsEnc,
              sizeIndicesEnc,
              uniqueSizesEnc);
          EXPECT_LE(buffer.size(), estimate);

          std::vector<uint32_t> decodedStreamIds;
          std::vector<uint32_t> offsets;
          std::vector<uint32_t> sizes;
          std::vector<uint32_t> uniqueStreamSizesScratch;
          serde::detail::readTrailerStreamMetadata(
              buffer.data() + buffer.size(),
              decodedStreamIds,
              offsets,
              sizes,
              uniqueStreamSizesScratch);

          EXPECT_EQ(decodedStreamIds, testCase.streamIds);
          EXPECT_EQ(offsets, testCase.expectedOffsets);
          EXPECT_EQ(sizes, testCase.expectedSizes);
        }
      }
    }
  }
}

// Verifies that every (indicesEncoding, sizesEncoding) combination from the
// supported encoding set round-trips correctly across representative shapes:
// empty, dense, sparse (mostly-zero), and all-nonzero.
TEST_F(EncodeDecodeTest, readTrailerStreamMetadataAxisEncodingCombinations) {
  const std::vector<EncodingType> encodings = {
      EncodingType::Trivial,
      EncodingType::Varint,
      EncodingType::Delta,
      EncodingType::FixedBitWidth,
  };
  std::vector<std::pair<std::string, std::vector<uint32_t>>> namedCases = {
      {"empty", {}},
      {"single-nonzero", {42}},
      {"all-zero", {0, 0, 0, 0}},
      {"mostly-zero-scattered", {0, 0, 11, 0, 0, 22, 0, 0, 33, 0}},
      {"all-nonzero-small", {1, 2, 3, 4, 5}},
      {"all-nonzero-similar", {1000, 1002, 998, 1001, 999, 1003}},
      {"uint32-max-scattered",
       {0, std::numeric_limits<uint32_t>::max(), 0, 12345}},
  };
  {
    std::vector<uint32_t> v1000(1000, 0);
    v1000[50] = 11;
    v1000[123] = 22;
    v1000[456] = 33;
    v1000[789] = 44;
    v1000[999] = 55;
    namedCases.emplace_back("sparse-1000-slots", std::move(v1000));
  }

  for (const auto indicesEnc : encodings) {
    for (const auto sizesEnc : encodings) {
      for (const auto& [label, values] : namedCases) {
        SCOPED_TRACE(
            fmt::format(
                "indicesEnc={} sizesEnc={} case={}",
                toString(indicesEnc),
                toString(sizesEnc),
                label));
        auto buffer = buildCompactTrailer(values, indicesEnc, sizesEnc);
        expectDenseRoundtripEq(values, buffer);
        const auto estimate = serde::detail::estimateTrailerSize(
            values.size(), indicesEnc, sizesEnc);
        EXPECT_LE(buffer.size(), estimate)
            << "Estimate must be >= actual trailer size";
      }
    }
  }
}

// Randomized roundtrip over all (indicesEnc, sizesEnc) combinations.
// Length drawn uniformly from [0, kMaxStreamCount]; per-entry value drawn
// uniformly across uint32_t with a random nonzero density per iteration.
// Seed is fixed so the test is fully deterministic.
TEST_F(EncodeDecodeTest, readTrailerStreamMetadataRandomRoundtrip) {
  constexpr uint32_t kSeed = 0xC0FFEE;
  std::mt19937 rng(kSeed);
  constexpr uint32_t kMaxStreamCount = 1000;
  constexpr int kIterationsPerCombo = 5;
  std::uniform_int_distribution<uint32_t> lengthDist(0, kMaxStreamCount);
  std::uniform_int_distribution<uint32_t> valueDist(
      0, std::numeric_limits<uint32_t>::max());
  std::uniform_int_distribution<int> densityDist(0, 100);
  const std::vector<EncodingType> encodings = {
      EncodingType::Trivial,
      EncodingType::Varint,
      EncodingType::Delta,
      EncodingType::FixedBitWidth,
  };
  for (const auto indicesEnc : encodings) {
    for (const auto sizesEnc : encodings) {
      for (int iter = 0; iter < kIterationsPerCombo; ++iter) {
        const uint32_t streamCount = lengthDist(rng);
        const int nonzeroPercent = densityDist(rng);
        SCOPED_TRACE(
            fmt::format(
                "indicesEnc={} sizesEnc={} iter={} streamCount={} nonzero%={}",
                toString(indicesEnc),
                toString(sizesEnc),
                iter,
                streamCount,
                nonzeroPercent));
        std::vector<uint32_t> sizes;
        sizes.reserve(streamCount);
        std::uniform_int_distribution<int> coinDist(0, 99);
        for (uint32_t i = 0; i < streamCount; ++i) {
          sizes.push_back(coinDist(rng) < nonzeroPercent ? valueDist(rng) : 0);
        }
        auto buffer = buildCompactTrailer(sizes, indicesEnc, sizesEnc);
        expectDenseRoundtripEq(sizes, buffer);
      }
    }
  }
}

TEST_F(EncodeDecodeTest, readTrailerStreamMetadataLargeValues) {
  std::vector<uint32_t> values;
  values.reserve(1000);
  for (uint32_t i = 0; i < 1000; ++i) {
    values.emplace_back(i * 3);
  }

  auto buffer = buildCompactTrailer(values);
  expectDenseRoundtripEq(values, buffer);
}

// Decoder must reject a trailer whose indices and sizes axes report
// different counts — the two-array layout requires identical lengths.
TEST_F(EncodeDecodeTest, readTrailerStreamMetadataAxisCountMismatch) {
  // Build a buffer by writing two independent axes with differing counts.
  // Indices axis: 3 values via Trivial encoding; sizes axis: 2 values via
  // Trivial encoding. Then append the trailer-size suffix manually.
  std::string buffer;
  const auto trailerStart = buffer.size();
  std::vector<uint32_t> indices{0u, 1u, 2u};
  std::vector<uint32_t> sizes{10u, 20u};
  // Indices axis.
  buffer.push_back(static_cast<char>(EncodingType::Trivial));
  serde::detail::writeTrivialSection(indices, buffer);
  // Sizes axis with mismatching count.
  buffer.push_back(static_cast<char>(EncodingType::Trivial));
  serde::detail::writeTrivialSection(sizes, buffer);
  const uint32_t trailerSize =
      static_cast<uint32_t>(buffer.size() - trailerStart);
  const auto sizeOffset = buffer.size();
  buffer.resize(sizeOffset + sizeof(uint32_t));
  char* sizePos = buffer.data() + sizeOffset;
  encoding::writeUint32(trailerSize, sizePos);

  EXPECT_THROW(
      serde::detail::readTrailerStreamMetadata(buffer.data() + buffer.size()),
      facebook::nimble::NimbleInternalError);
}

TEST_F(EncodeDecodeTest, streamSizesEncodingType) {
  std::vector<uint32_t> sizes = {2, 2, 2};

  for (auto encodingType :
       {EncodingType::Trivial,
        EncodingType::FixedBitWidth,
        EncodingType::Varint}) {
    SCOPED_TRACE(toString(encodingType));

    std::string buffer;
    serde::writeSerializationHeader(
        buffer, SerializationVersion::kSerialization, 100);
    buffer.append("s0");
    buffer.append("s1");
    buffer.append("s2");
    serde::detail::writeTrailer(sizes, encodingType, encodingType, buffer);

    const char* pos = buffer.data();
    serde::readSerializationHeader(
        pos, buffer.data() + buffer.size(), /*hasHeader=*/true);
    auto streams = serde::detail::parseStreams(
        pos,
        buffer.data() + buffer.size(),
        SerializationVersion::kSerialization,
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
  serde::writeSerializationHeader(
      buffer, SerializationVersion::kSerialization, 100);
  buffer.append("a");
  buffer.append("b");
  buffer.append("c");
  serde::detail::writeTrailer(
      sizes, EncodingType::Trivial, EncodingType::Trivial, buffer);

  const char* pos = buffer.data();
  serde::readSerializationHeader(
      pos, buffer.data() + buffer.size(), /*hasHeader=*/true);
  auto streams = serde::detail::parseStreams(
      pos,
      buffer.data() + buffer.size(),
      SerializationVersion::kSerialization,
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
    serde::writeSerializationHeader(
        buffer, SerializationVersion::kSerialization, 100);
    buffer.append(std::string(10, 'a'));
    buffer.append(std::string(20, 'b'));
    buffer.append(std::string(30, 'c'));
    serde::detail::writeTrailer(sizes, encodingType, encodingType, buffer);

    const char* pos = buffer.data();
    serde::readSerializationHeader(
        pos, buffer.data() + buffer.size(), /*hasHeader=*/true);
    auto streams = serde::detail::parseStreams(
        pos,
        buffer.data() + buffer.size(),
        SerializationVersion::kSerialization,
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

  if (nonLegacyFormat(version) && !isTabletVersion(version)) {
    auto streamSizes = readTrailerStreamMetadataDenseForTest(end);
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
  // Helper to build a kLegacyCompact buffer using writeHeader + raw data.
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
    serde::writeSerializationHeader(
        buffer, SerializationVersion::kSerialization, 100);

    // Append raw stream data in index order (only non-zero sizes).
    // Sort by index to ensure correct order.
    auto sorted = indexAndData;
    std::sort(sorted.begin(), sorted.end());
    for (const auto& [_, data] : sorted) {
      buffer.append(data);
    }

    // Write trailer with encoded sizes.
    serde::detail::writeTrailer(
        sizes, EncodingType::Trivial, EncodingType::Trivial, buffer);

    // Skip the header (version + flags + varint row count) to reach streams.
    const char* pos = buffer.data();
    serde::readSerializationHeader(
        pos, buffer.data() + buffer.size(), /*hasHeader=*/true);
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
      SerializationVersion::kSerialization,
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
      SerializationVersion::kSerialization,
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
      SerializationVersion::kSerialization,
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
      SerializationVersion::kSerialization,
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
      SerializationVersion::kSerialization,
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
      SerializationVersion::kSerialization,
      selected,
      pool_.get());

  ASSERT_EQ(streams.size(), 2);
  EXPECT_EQ(streams[0].index, 0);
  EXPECT_EQ(streams[0].data, "zero");
  EXPECT_EQ(streams[1].index, 1);
  EXPECT_EQ(streams[1].data, "two");
}

// Verifies that writeTrailer roundtrips correctly.
TEST_F(EncodeDecodeTest, rawTrailerRoundtrip) {
  std::vector<uint32_t> sizes(500);
  for (uint32_t i = 0; i < sizes.size(); ++i) {
    sizes[i] = i * 7 + 1;
  }

  std::string buffer;
  serde::detail::writeTrailer(
      sizes, EncodingType::Trivial, EncodingType::Trivial, buffer);

  const auto* end = buffer.data() + buffer.size();
  auto decoded = readTrailerStreamMetadataDenseForTest(end);
  EXPECT_EQ(decoded, sizes);
}

TEST_F(ProjectStreamsTest, denseSkipsEmptyStreams) {
  auto buffer = buildDenseBuffer({{0, "aaa"}, {1, ""}, {2, "ccc"}});

  std::vector<uint32_t> selected = {0, 1, 2};
  auto streams = projectStreams(
      buffer.data(),
      buffer.data() + buffer.size(),
      SerializationVersion::kSerialization,
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
      ManualEncodingSelectionPolicyFactory::defaultEncodingReadFactors(),
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
        Encoding::Options{.useVarintRowCount = true},
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
      ManualEncodingSelectionPolicyFactory::defaultEncodingReadFactors(),
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
      Encoding::Options{.useVarintRowCount = true},
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

// Verify that the default SerializerOptions.encodingSelectionPolicyCreator
// creates policies with compression disabled.
TEST_F(EncodeTypedCompressionTest, defaultFactoryNoCompression) {
  SerializerOptions options{};

  auto policy = options.encodingSelectionPolicyCreator(DataType::Uint32);
  auto* typed = dynamic_cast<EncodingSelectionPolicy<uint32_t>*>(policy.get());
  ASSERT_NE(typed, nullptr);

  std::vector<uint32_t> data(1000, 42);
  auto result = typed->select(
      data, Statistics<uint32_t>::create(data), Encoding::Options{});
  auto compressionPolicy = result.compressionPolicyFactory();
  EXPECT_EQ(
      compressionPolicy->config().compressionType,
      CompressionType::Uncompressed);
}

// Tests for kTablet chunk header stripping in StreamDataParser.
// kTablet streams contain chunk headers:
//   [chunkSize:u32][compressionType:1B][encoded_data...]
// StreamDataParser must strip these headers and decompress as needed.

class TabletChunkStripTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("tablet_chunk_test");
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

  // Assembles a kTablet buffer from streams and returns the data collected
  // by iterateStreams.
  // Each entry in 'streams' is the raw stream bytes (with chunk headers).
  std::string buildTabletBuffer(
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
    auto headerIOBuf = serde::createTabletChunkHeader(
        {.rowCount = rowCount, .rowRange = RowRange{0, rowCount}});
    std::string buffer(
        reinterpret_cast<const char*>(headerIOBuf.data()),
        headerIOBuf.length());
    buffer.append(streamData);
    writeTabletTrailer(sizes, buffer);
    return buffer;
  }

  std::vector<std::pair<uint32_t, std::string>> deserializeTablet(
      uint32_t rowCount,
      const std::vector<std::pair<uint32_t, std::string>>& streams) {
    const auto buffer = buildTabletBuffer(rowCount, streams);

    // Parse via StreamDataParser.
    DeserializerOptions options{.hasHeader = true};
    StreamDataParser reader(pool_.get(), options);
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

TEST_F(TabletChunkStripTest, singleChunkUncompressed) {
  std::string payload = "hello world test data";
  auto stream = buildUncompressedChunk(payload);
  auto result = deserializeTablet(10, {{0, stream}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, payload);
}

TEST_F(TabletChunkStripTest, singleChunkCompressed) {
  std::string payload = "compressed data that should be zstd encoded";
  auto stream = buildCompressedChunk(payload);
  auto result = deserializeTablet(10, {{0, stream}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, payload);
}

TEST_F(TabletChunkStripTest, multipleChunksAllUncompressed) {
  std::string part1 = "first chunk data";
  std::string part2 = "second chunk data";
  std::string part3 = "third chunk data";
  auto stream = concatChunks(
      {buildUncompressedChunk(part1),
       buildUncompressedChunk(part2),
       buildUncompressedChunk(part3)});
  auto result = deserializeTablet(10, {{0, stream}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, part1 + part2 + part3);
}

TEST_F(TabletChunkStripTest, multipleChunksAllCompressed) {
  std::string part1 = "first chunk of compressed data here";
  std::string part2 = "second chunk of compressed data here";
  auto stream =
      concatChunks({buildCompressedChunk(part1), buildCompressedChunk(part2)});
  auto result = deserializeTablet(10, {{0, stream}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, part1 + part2);
}

TEST_F(TabletChunkStripTest, multipleChunksMixed) {
  std::string part1 = "uncompressed chunk one";
  std::string part2 = "compressed chunk two with some data";
  std::string part3 = "another uncompressed chunk three";
  auto stream = concatChunks(
      {buildUncompressedChunk(part1),
       buildCompressedChunk(part2),
       buildUncompressedChunk(part3)});
  auto result = deserializeTablet(10, {{0, stream}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, part1 + part2 + part3);
}

TEST_F(TabletChunkStripTest, multipleStreams) {
  std::string payload1 = "stream zero payload";
  std::string payload2 = "stream two payload data";
  auto stream0 = buildUncompressedChunk(payload1);
  auto stream2 = buildCompressedChunk(payload2);
  // Stream offsets 0 and 2 (gap at 1).
  auto result = deserializeTablet(10, {{0, stream0}, {2, stream2}});

  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, payload1);
  EXPECT_EQ(result[1].first, 2);
  EXPECT_EQ(result[1].second, payload2);
}

TEST_F(TabletChunkStripTest, streamViewsRemainStable) {
  struct StreamViewCase {
    std::string name;
    std::vector<std::pair<uint32_t, std::string>> firstStreams;
    std::vector<std::pair<uint32_t, std::string>> secondStreams;
    std::vector<std::pair<uint32_t, std::string>> expectedAfterFirstBatch;
    std::vector<std::pair<uint32_t, std::string>> expectedAfterSecondBatch;
  };

  const auto payload = [](char value) { return std::string(4096, value); };
  const auto materialize =
      [](const std::vector<std::pair<uint32_t, std::string_view>>& views) {
        std::vector<std::pair<uint32_t, std::string>> result;
        result.reserve(views.size());
        for (const auto& [offset, data] : views) {
          result.emplace_back(offset, std::string{data});
        }
        return result;
      };

  const auto uncompressed0 = payload('a');
  const auto uncompressed1 = payload('b');
  const auto uncompressed2 = payload('c');
  const auto compressed0 = payload('d');
  const auto compressed1 = payload('e');
  const auto compressed2 = payload('f');
  const auto mixed0 = payload('g');
  const auto mixed1 = payload('h');
  const auto mixed2 = payload('i');
  const auto mixed3 = payload('j');
  const auto mixed4 = payload('k');
  const auto mixed5 = payload('l');

  const std::vector<StreamViewCase> testCases{
      {
          .name = "uncompressed",
          .firstStreams =
              {{0, buildUncompressedChunk(uncompressed0)},
               {1, buildUncompressedChunk(uncompressed1)}},
          .secondStreams = {{0, buildUncompressedChunk(uncompressed2)}},
          .expectedAfterFirstBatch = {{0, uncompressed0}, {1, uncompressed1}},
          .expectedAfterSecondBatch =
              {{0, uncompressed0}, {1, uncompressed1}, {0, uncompressed2}},
      },
      {
          .name = "compressed",
          .firstStreams =
              {{0, buildCompressedChunk(compressed0)},
               {1, buildCompressedChunk(compressed1)}},
          .secondStreams = {{0, buildCompressedChunk(compressed2)}},
          .expectedAfterFirstBatch = {{0, compressed0}, {1, compressed1}},
          .expectedAfterSecondBatch =
              {{0, compressed0}, {1, compressed1}, {0, compressed2}},
      },
      {
          .name = "mixed",
          .firstStreams =
              {{0,
                concatChunks(
                    {buildUncompressedChunk(mixed0),
                     buildCompressedChunk(mixed1)})},
               {1,
                concatChunks(
                    {buildCompressedChunk(mixed2),
                     buildUncompressedChunk(mixed3)})}},
          .secondStreams =
              {{0,
                concatChunks(
                    {buildUncompressedChunk(mixed4),
                     buildCompressedChunk(mixed5)})}},
          .expectedAfterFirstBatch =
              {{0, mixed0 + mixed1}, {1, mixed2 + mixed3}},
          .expectedAfterSecondBatch =
              {{0, mixed0 + mixed1},
               {1, mixed2 + mixed3},
               {0, mixed4 + mixed5}},
      },
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);

    const auto firstBatch = buildTabletBuffer(10, testCase.firstStreams);
    const auto secondBatch = buildTabletBuffer(20, testCase.secondStreams);

    DeserializerOptions options{.hasHeader = true};
    StreamDataParser reader(pool_.get(), options);

    std::vector<std::pair<uint32_t, std::string_view>> views;
    EXPECT_EQ(reader.initialize(firstBatch), 10);
    reader.iterateStreams([&](uint32_t offset, std::string_view data) {
      views.emplace_back(offset, data);
    });
    EXPECT_EQ(materialize(views), testCase.expectedAfterFirstBatch);

    EXPECT_EQ(reader.initialize(secondBatch), 20);
    reader.iterateStreams([&](uint32_t offset, std::string_view data) {
      views.emplace_back(offset, data);
    });
    EXPECT_EQ(materialize(views), testCase.expectedAfterSecondBatch);

    reader.reset();
  }
}

TEST_F(TabletChunkStripTest, multipleStreamsMultipleChunks) {
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
      deserializeTablet(10, {{0, stream0}, {1, stream1}, {2, stream2}});

  ASSERT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, s0p1 + s0p2);
  EXPECT_EQ(result[1].first, 1);
  EXPECT_EQ(result[1].second, s1p1);
  EXPECT_EQ(result[2].first, 2);
  EXPECT_EQ(result[2].second, s2p1 + s2p2);
}

TEST_F(TabletChunkStripTest, emptyPayloadChunk) {
  for (const auto& [name, stream] : {
           std::pair{"uncompressed", buildUncompressedChunk("")},
           std::pair{"compressed", buildCompressedChunk("")},
       }) {
    SCOPED_TRACE(name);
    NIMBLE_ASSERT_THROW(
        deserializeTablet(10, {{0, stream}}),
        "Chunked stream must have a non-empty payload");
  }
}

TEST_F(TabletChunkStripTest, largePayload) {
  // Large payload to exercise buffer growth.
  std::string payload(100'000, 'x');
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<char>('a' + (i % 26));
  }

  // Test single compressed chunk with large data.
  auto stream = buildCompressedChunk(payload);
  auto result = deserializeTablet(10, {{0, stream}});
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].second, payload);
}

TEST_F(TabletChunkStripTest, largePayloadMultipleChunks) {
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
  auto result = deserializeTablet(10, {{0, stream}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].second, payload);
}

// Tests for ZSTD_DCtx reuse in StreamData and StreamDataParser.
// Inherits from TabletChunkStripTest for shared helpers
// (buildCompressedChunk, pool_, etc.).

class ZstdDCtxReuseTest : public TabletChunkStripTest {
 protected:
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

  // Like TabletChunkStripTest::deserializeTablet, but passes
  // the fixture's shared dctx to StreamDataParser.
  std::vector<std::pair<uint32_t, std::string>> deserializeTabletWithDCtx(
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

    auto headerIOBuf = serde::createTabletChunkHeader(
        {.rowCount = rowCount, .rowRange = RowRange{0, rowCount}});
    std::string buffer(
        reinterpret_cast<const char*>(headerIOBuf.data()),
        headerIOBuf.length());
    buffer.append(streamData);
    writeTabletTrailer(sizes, buffer);

    DeserializerOptions options{.hasHeader = true};
    StreamDataParser reader(pool_.get(), options);
    auto actualRows = reader.initialize(std::string_view(buffer));
    EXPECT_EQ(actualRows, rowCount);

    std::vector<std::pair<uint32_t, std::string>> result;
    reader.iterateStreams([&](uint32_t offset, std::string_view data) {
      result.emplace_back(offset, std::string(data));
    });
    return result;
  }

  BufferPtr decompressionBuffer_;
};

TEST_F(ZstdDCtxReuseTest, streamDataLegacyZstdWithDCtx) {
  const std::vector<int32_t> expected = {10, 20, 30, 40};
  std::string_view payload(
      reinterpret_cast<const char*>(expected.data()),
      expected.size() * sizeof(int32_t));
  auto compressed = buildLegacyCompressedData(payload);

  std::vector<BufferPtr> stringBuffers;
  serde::StreamData sd(
      ScalarKind::Int32,
      compressed,
      stringBuffers,
      pool_.get(),
      serde::StreamData::Options{
          .version = SerializationVersion::kLegacy,
          .decompressionBuffer = &decompressionBuffer_});

  std::vector<int32_t> output(expected.size());
  sd.copyTo(
      reinterpret_cast<char*>(output.data()), output.size() * sizeof(int32_t));
  EXPECT_EQ(output, expected);
}

TEST_F(ZstdDCtxReuseTest, streamDataLegacyDecodeFails) {
  const std::vector<int32_t> expected = {10, 20, 30, 40};
  std::string_view payload(
      reinterpret_cast<const char*>(expected.data()),
      expected.size() * sizeof(int32_t));
  auto compressed = buildLegacyCompressedData(payload);

  std::vector<BufferPtr> stringBuffers;
  serde::StreamData sd(
      ScalarKind::Int32,
      compressed,
      stringBuffers,
      pool_.get(),
      serde::StreamData::Options{
          .version = SerializationVersion::kLegacy,
          .decompressionBuffer = &decompressionBuffer_});

  std::vector<int32_t> output(expected.size());
  NIMBLE_ASSERT_THROW(
      sd.decode(
          output.data(),
          /*offset=*/0,
          static_cast<uint32_t>(output.size()),
          sizeof(int32_t)),
      "Legacy StreamData must be decoded through copyTo() or decodeStrings()");
}

TEST_F(ZstdDCtxReuseTest, streamDataZeroCountDecodeIsNoOp) {
  const std::vector<int32_t> expected = {10, 20, 30, 40};
  std::string_view payload(
      reinterpret_cast<const char*>(expected.data()),
      expected.size() * sizeof(int32_t));
  auto compressed = buildLegacyCompressedData(payload);

  std::vector<BufferPtr> stringBuffers;
  serde::StreamData sd(
      ScalarKind::Int32,
      compressed,
      stringBuffers,
      pool_.get(),
      serde::StreamData::Options{
          .version = SerializationVersion::kLegacy,
          .decompressionBuffer = &decompressionBuffer_});

  // A zero-count decode must be a no-op even for a stream with no encoding
  // (legacy/empty stream), rather than throwing.
  const auto result =
      sd.decode(/*output=*/nullptr, /*offset=*/0, /*count=*/0, sizeof(int32_t));
  EXPECT_EQ(result.numOutputRows, 0);
  EXPECT_EQ(result.nonNullOutputRows, 0);
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

  std::vector<BufferPtr> stringBuffers;
  serde::StreamData sd(
      ScalarKind::Int32,
      compressed1,
      stringBuffers,
      pool_.get(),
      serde::StreamData::Options{
          .version = SerializationVersion::kLegacy,
          .decompressionBuffer = &decompressionBuffer_});

  std::vector<int32_t> output1(values1.size());
  sd.copyTo(
      reinterpret_cast<char*>(output1.data()),
      output1.size() * sizeof(int32_t));
  EXPECT_EQ(output1, values1);

  // Recreate with new data; decompression buffer storage is external and
  // persists across StreamData lifetimes.
  serde::StreamData sd2(
      ScalarKind::Int32,
      compressed2,
      stringBuffers,
      pool_.get(),
      serde::StreamData::Options{
          .version = SerializationVersion::kLegacy,
          .decompressionBuffer = &decompressionBuffer_});

  std::vector<int32_t> output2(values2.size());
  sd2.copyTo(
      reinterpret_cast<char*>(output2.data()),
      output2.size() * sizeof(int32_t));
  EXPECT_EQ(output2, values2);
}

TEST_F(ZstdDCtxReuseTest, streamDataParserCompressedChunkWithDCtx) {
  std::string payload = "compressed data that should be zstd encoded for test";
  auto chunk = buildCompressedChunk(payload);
  auto result = deserializeTabletWithDCtx(10, {{0, chunk}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, payload);
}

TEST_F(ZstdDCtxReuseTest, streamDataParserDCtxReusedAcrossStreams) {
  std::string payload1 = "first compressed stream payload data";
  std::string payload2 = "second compressed stream payload data";
  auto chunk1 = buildCompressedChunk(payload1);
  auto chunk2 = buildCompressedChunk(payload2);

  auto result = deserializeTabletWithDCtx(10, {{0, chunk1}, {1, chunk2}});

  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].second, payload1);
  EXPECT_EQ(result[1].second, payload2);
}

class LZ4RoundtripTest : public TabletChunkStripTest {
 protected:
  static std::string buildLegacyLZ4CompressedData(std::string_view payload) {
    std::string result;
    result.push_back(static_cast<char>(CompressionType::Lz4));

    const auto origSize = static_cast<uint32_t>(payload.size());
    result.resize(result.size() + sizeof(uint32_t));
    auto* sizePos = result.data() + 1;
    encoding::writeUint32(origSize, sizePos);

    const auto maxSize = LZ4_compressBound(static_cast<int>(payload.size()));
    const auto offset = result.size();
    result.resize(offset + maxSize);
    const auto compressedSize = LZ4_compress_default(
        payload.data(),
        result.data() + offset,
        static_cast<int>(payload.size()),
        maxSize);
    NIMBLE_CHECK_GT(compressedSize, 0, "LZ4 compression failed");
    result.resize(offset + compressedSize);
    return result;
  }

  static std::string buildLZ4CompressedChunk(std::string_view data) {
    const auto maxCompressedSize =
        LZ4_compressBound(static_cast<int>(data.size()));
    std::string compressed(maxCompressedSize, '\0');
    const auto compressedSize = LZ4_compress_default(
        data.data(),
        compressed.data(),
        static_cast<int>(data.size()),
        maxCompressedSize);
    NIMBLE_CHECK_GT(compressedSize, 0, "LZ4 compression failed");
    compressed.resize(compressedSize);

    // Chunk payload: [origSize:u32][lz4_data...]
    const auto chunkPayloadSize = sizeof(uint32_t) + compressedSize;
    std::string chunk(kChunkHeaderSize + chunkPayloadSize, '\0');
    auto* pos = chunk.data();
    writeChunkHeader(
        static_cast<uint32_t>(chunkPayloadSize), CompressionType::Lz4, pos);
    encoding::writeUint32(static_cast<uint32_t>(data.size()), pos);
    std::memcpy(pos, compressed.data(), compressedSize);
    return chunk;
  }

  BufferPtr decompressionBuffer_;
};

TEST_F(LZ4RoundtripTest, streamDataLegacyLZ4) {
  const std::vector<int32_t> expected = {10, 20, 30, 40, 50};
  std::string_view payload(
      reinterpret_cast<const char*>(expected.data()),
      expected.size() * sizeof(int32_t));
  auto compressed = buildLegacyLZ4CompressedData(payload);

  std::vector<BufferPtr> stringBuffers;
  serde::StreamData sd(
      ScalarKind::Int32,
      compressed,
      stringBuffers,
      pool_.get(),
      serde::StreamData::Options{
          .version = SerializationVersion::kLegacy,
          .decompressionBuffer = &decompressionBuffer_});

  std::vector<int32_t> output(expected.size());
  sd.copyTo(
      reinterpret_cast<char*>(output.data()),
      static_cast<uint32_t>(output.size() * sizeof(int32_t)));
  EXPECT_EQ(output, expected);
}

TEST_F(LZ4RoundtripTest, streamDataLZ4ReusedAcrossReset) {
  const std::vector<int32_t> values1 = {1, 2, 3};
  std::string_view payload1(
      reinterpret_cast<const char*>(values1.data()),
      values1.size() * sizeof(int32_t));
  auto compressed1 = buildLegacyLZ4CompressedData(payload1);

  const std::vector<int32_t> values2 = {100, 200};
  std::string_view payload2(
      reinterpret_cast<const char*>(values2.data()),
      values2.size() * sizeof(int32_t));
  auto compressed2 = buildLegacyLZ4CompressedData(payload2);

  std::vector<BufferPtr> stringBuffers;
  serde::StreamData sd(
      ScalarKind::Int32,
      compressed1,
      stringBuffers,
      pool_.get(),
      serde::StreamData::Options{
          .version = SerializationVersion::kLegacy,
          .decompressionBuffer = &decompressionBuffer_});

  std::vector<int32_t> output1(values1.size());
  sd.copyTo(
      reinterpret_cast<char*>(output1.data()),
      static_cast<uint32_t>(output1.size() * sizeof(int32_t)));
  EXPECT_EQ(output1, values1);

  serde::StreamData sd2(
      ScalarKind::Int32,
      compressed2,
      stringBuffers,
      pool_.get(),
      serde::StreamData::Options{
          .version = SerializationVersion::kLegacy,
          .decompressionBuffer = &decompressionBuffer_});

  std::vector<int32_t> output2(values2.size());
  sd2.copyTo(
      reinterpret_cast<char*>(output2.data()),
      static_cast<uint32_t>(output2.size() * sizeof(int32_t)));
  EXPECT_EQ(output2, values2);
}

TEST_F(LZ4RoundtripTest, streamDataParserLZ4CompressedChunk) {
  std::string payload = "compressed data that should be lz4 encoded for test";
  auto chunk = buildLZ4CompressedChunk(payload);
  auto result = deserializeTablet(10, {{0, chunk}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, payload);
}

TEST_F(LZ4RoundtripTest, streamDataParserLZ4ReusedAcrossStreams) {
  std::string payload1 = "first compressed stream payload data";
  std::string payload2 = "second compressed stream payload data";
  auto chunk1 = buildLZ4CompressedChunk(payload1);
  auto chunk2 = buildLZ4CompressedChunk(payload2);

  auto result = deserializeTablet(10, {{0, chunk1}, {1, chunk2}});

  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].second, payload1);
  EXPECT_EQ(result[1].second, payload2);
}

TEST_F(LZ4RoundtripTest, streamDataParserLZ4MultipleChunks) {
  std::string part1 = "first chunk of lz4 compressed data here";
  std::string part2 = "second chunk of lz4 compressed data here";
  auto stream = concatChunks(
      {buildLZ4CompressedChunk(part1), buildLZ4CompressedChunk(part2)});
  auto result = deserializeTablet(10, {{0, stream}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, part1 + part2);
}

TEST_F(LZ4RoundtripTest, streamDataParserLZ4MixedWithUncompressed) {
  std::string part1 = "uncompressed chunk one";
  std::string part2 = "lz4 compressed chunk two with some data";
  std::string part3 = "another uncompressed chunk three";
  auto stream = concatChunks(
      {buildUncompressedChunk(part1),
       buildLZ4CompressedChunk(part2),
       buildUncompressedChunk(part3)});
  auto result = deserializeTablet(10, {{0, stream}});

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, 0);
  EXPECT_EQ(result[0].second, part1 + part2 + part3);
}

TEST_F(LZ4RoundtripTest, encodeDecodeLZ4Roundtrip) {
  const std::vector<int32_t> expected(256);
  std::string_view input(
      reinterpret_cast<const char*>(expected.data()),
      expected.size() * sizeof(int32_t));

  SerializerOptions options;
  options.compressionType = CompressionType::Lz4;
  options.compressionThreshold = 1;

  std::string buffer(input.size() + 2 * sizeof(uint32_t) + 1, '\0');
  const auto encodedSize = serde::detail::encode(options, input, buffer.data());

  // encode() outputs [size:u32][compressionType:i8][data...]; StreamData
  // expects the stream without the size prefix.
  std::string_view streamData(
      buffer.data() + sizeof(uint32_t), encodedSize - sizeof(uint32_t));

  std::vector<BufferPtr> stringBuffers;
  serde::StreamData sd(
      ScalarKind::Int32,
      streamData,
      stringBuffers,
      pool_.get(),
      serde::StreamData::Options{
          .version = SerializationVersion::kLegacy,
          .decompressionBuffer = &decompressionBuffer_});

  std::vector<int32_t> output(expected.size());
  sd.copyTo(
      reinterpret_cast<char*>(output.data()),
      static_cast<uint32_t>(output.size() * sizeof(int32_t)));
  EXPECT_EQ(output, expected);
}

TEST_F(LZ4RoundtripTest, encodeLZ4BelowThresholdStaysUncompressed) {
  const std::vector<int32_t> expected = {1, 2, 3};
  std::string_view input(
      reinterpret_cast<const char*>(expected.data()),
      expected.size() * sizeof(int32_t));

  SerializerOptions options;
  options.compressionType = CompressionType::Lz4;
  options.compressionThreshold = 1'000'000;

  std::string buffer(input.size() + 2 * sizeof(uint32_t) + 1, '\0');
  const auto encodedSize = serde::detail::encode(options, input, buffer.data());

  std::string_view streamData(
      buffer.data() + sizeof(uint32_t), encodedSize - sizeof(uint32_t));

  std::vector<BufferPtr> stringBuffers;
  serde::StreamData sd(
      ScalarKind::Int32,
      streamData,
      stringBuffers,
      pool_.get(),
      serde::StreamData::Options{
          .version = SerializationVersion::kLegacy,
          .decompressionBuffer = &decompressionBuffer_});

  std::vector<int32_t> output(expected.size());
  sd.copyTo(
      reinterpret_cast<char*>(output.data()),
      static_cast<uint32_t>(output.size() * sizeof(int32_t)));
  EXPECT_EQ(output, expected);
}

TEST_F(LZ4RoundtripTest, encodeDecodeLZ4HighCompression) {
  const std::vector<int32_t> expected(256);
  std::string_view input(
      reinterpret_cast<const char*>(expected.data()),
      expected.size() * sizeof(int32_t));

  SerializerOptions options;
  options.compressionType = CompressionType::Lz4;
  options.compressionThreshold = 1;
  options.compressionLevel = 9;

  std::string buffer(input.size() + 2 * sizeof(uint32_t) + 1, '\0');
  const auto encodedSize = serde::detail::encode(options, input, buffer.data());

  std::string_view streamData(
      buffer.data() + sizeof(uint32_t), encodedSize - sizeof(uint32_t));

  std::vector<BufferPtr> stringBuffers;
  serde::StreamData sd(
      ScalarKind::Int32,
      streamData,
      stringBuffers,
      pool_.get(),
      serde::StreamData::Options{
          .version = SerializationVersion::kLegacy,
          .decompressionBuffer = &decompressionBuffer_});

  std::vector<int32_t> output(expected.size());
  sd.copyTo(
      reinterpret_cast<char*>(output.data()),
      static_cast<uint32_t>(output.size() * sizeof(int32_t)));
  EXPECT_EQ(output, expected);
}

// Exercises the StreamDataParser::initialize path (delegates to the shared
// readTabletChunkFieldsAfterVersion helper) on forged kTablet input.
// Uses the WriteHeaderTest fixture purely for its initialized MemoryPool.
class StreamDataParserTabletTest : public WriteHeaderTest {
 protected:
  void expectInitializeThrows(const std::string& buf) {
    DeserializerOptions options;
    options.hasHeader = true;
    StreamDataParser reader{pool_.get(), options};
    EXPECT_THROW(
        reader.initialize(std::string_view(buf.data(), buf.size())),
        NimbleInternalError);
  }
};

TEST_F(StreamDataParserTabletTest, rejectsRowRangeExceedingRowCount) {
  std::string buf;
  buf.push_back(static_cast<char>(SerializationVersion::kTablet));
  buf.push_back(0x05); // rowCount = 5
  buf.push_back(0x00); // startRow = 0
  buf.push_back(0x64); // endRow = 100 (> rowCount)
  buf.push_back(0x00); // resumeKeyLength = 0
  expectInitializeThrows(buf);
}

TEST_F(StreamDataParserTabletTest, rejectsInvertedRowRange) {
  std::string buf;
  buf.push_back(static_cast<char>(SerializationVersion::kTablet));
  buf.push_back(0x0a); // rowCount = 10
  buf.push_back(0x05); // startRow = 5
  buf.push_back(0x03); // endRow = 3
  buf.push_back(0x00); // resumeKeyLength = 0
  expectInitializeThrows(buf);
}

TEST_F(StreamDataParserTabletTest, rejectsTruncatedResumeKey) {
  // Valid version + rowCount + row range + resumeKeyLength=4 (declares a
  // 3-byte key), but only 1 byte of resume key follows.
  std::string buf;
  buf.push_back(static_cast<char>(SerializationVersion::kTablet));
  buf.push_back(0x0a); // rowCount = 10
  buf.push_back(0x00); // startRow = 0
  buf.push_back(0x05); // endRow = 5
  buf.push_back(0x04); // resumeKeyLength = 4 → declares 3 key bytes
  buf.push_back('a'); // only 1 key byte present
  expectInitializeThrows(buf);
}
