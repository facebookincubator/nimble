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
#include <fmt/ranges.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/encodings/PrefixEncoding.h"
#include "dwio/nimble/index/IndexConfig.h"
#include "dwio/nimble/index/SortOrder.h"
#include "dwio/nimble/velox/IndexWriter.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/serializers/KeyEncoder.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::nimble::index::test {
namespace {

class IndexWriterTest : public testing::Test,
                        public velox::test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

 protected:
  static constexpr std::string_view kCol0{"col0"};
  static constexpr std::string_view kCol1{"col1"};
  static constexpr std::string_view kCol2{"col2"};

  void SetUp() override {
    buffer_ = std::make_unique<Buffer>(*pool_);
    type_ = velox::ROW(
        {{std::string(kCol0), velox::VARCHAR()},
         {std::string(kCol1), velox::INTEGER()},
         {std::string(kCol2), velox::VARCHAR()}});
  }

  void TearDown() override {
    buffer_ = nullptr;
  }

  std::unique_ptr<velox::serializer::KeyEncoder> createKeyEncoder() const {
    const auto keyType = velox::ROW({{std::string(kCol1), velox::INTEGER()}});
    return velox::serializer::KeyEncoder::create(
        {std::string(kCol1)},
        keyType,
        {velox::core::SortOrder{true, false}},
        pool_.get());
  }

  static IndexConfig indexConfig(
      EncodingType encodingType = EncodingType::Prefix) {
    EncodingLayout layout = encodingType == EncodingType::Prefix
        ? EncodingLayout{EncodingType::Prefix, {}, CompressionType::Zstd}
        : EncodingLayout{
              EncodingType::Trivial,
              {},
              CompressionType::Zstd,
              {EncodingLayout{
                  EncodingType::Trivial, {}, CompressionType::Uncompressed}}};
    return IndexConfig{
        .columns = {std::string(kCol1)}, .encodingLayout = layout};
  }

  std::unique_ptr<Buffer> buffer_;
  velox::RowTypePtr type_;
};

TEST_F(IndexWriterTest, createWithNoConfig) {
  auto type = velox::ROW({{"col0", velox::INTEGER()}});
  auto writer = IndexWriter::create(std::nullopt, type, pool_.get());
  EXPECT_EQ(writer, nullptr);
}

TEST_F(IndexWriterTest, createWithValidConfig) {
  auto type = velox::ROW({{"col0", velox::INTEGER()}});
  for (auto compressionType :
       {CompressionType::Uncompressed,
        CompressionType::Zstd,
        CompressionType::MetaInternal}) {
    for (auto encodingType : {EncodingType::Prefix, EncodingType::Trivial}) {
      SCOPED_TRACE(
          fmt::format(
              "compressionType: {}, encodingType: {}",
              compressionType,
              toString(encodingType)));
      EncodingLayout layout = encodingType == EncodingType::Prefix
          ? EncodingLayout{EncodingType::Prefix, {}, compressionType}
          : EncodingLayout{
                EncodingType::Trivial,
                {},
                compressionType,
                {EncodingLayout{
                    EncodingType::Trivial, {}, CompressionType::Uncompressed}}};
      IndexConfig config{.columns = {"col0"}, .encodingLayout = layout};
      auto writer = IndexWriter::create(config, type_, pool_.get());
      EXPECT_NE(writer, nullptr);
    }
  }
}

TEST_F(IndexWriterTest, createWithInvalidConfig) {
  auto type = velox::ROW({{"col0", velox::INTEGER()}});

  {
    SCOPED_TRACE("non-existent column");
    IndexConfig config{
        .columns = {"non_existent_col"},
        .encodingLayout = EncodingLayout{
            EncodingType::Trivial, {}, CompressionType::Uncompressed}};
    VELOX_ASSERT_USER_THROW(
        IndexWriter::create(config, type, pool_.get()), "Field not found");
  }

  {
    SCOPED_TRACE("non-supported encoding");
    IndexConfig config{
        .columns = {"col0"},
        .encodingLayout = EncodingLayout{
            EncodingType::FixedBitWidth, {}, CompressionType::Uncompressed}};
    NIMBLE_ASSERT_THROW(
        IndexWriter::create(config, type, pool_.get()),
        "Key stream encoding only supports Prefix or Trivial encoding");
  }
}

TEST_F(IndexWriterTest, rejectNullKeys) {
  // Test that null values in key columns are rejected
  {
    SCOPED_TRACE("null in key column throws");
    IndexConfig config{
        .columns = {std::string(kCol1)},
        .encodingLayout =
            EncodingLayout{EncodingType::Prefix, {}, CompressionType::Zstd}};
    auto writer = IndexWriter::create(config, type_, pool_.get());

    auto batch = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"a", "b", "c"}),
         makeNullableFlatVector<int32_t>({1, std::nullopt, 3}),
         makeFlatVector<velox::StringView>({"d", "e", "f"})});

    NIMBLE_ASSERT_USER_THROW(
        writer->write(batch, *buffer_),
        "Null value not allowed in key column at index 1: found 1 null(s)");
  }

  // Test multiple nulls in key column
  {
    SCOPED_TRACE("multiple nulls in key column");
    IndexConfig config{
        .columns = {std::string(kCol1)},
        .encodingLayout =
            EncodingLayout{EncodingType::Prefix, {}, CompressionType::Zstd}};
    auto writer = IndexWriter::create(config, type_, pool_.get());

    auto batch = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"a", "b", "c", "d", "e"}),
         makeNullableFlatVector<int32_t>(
             {std::nullopt, 2, std::nullopt, 4, std::nullopt}),
         makeFlatVector<velox::StringView>({"f", "g", "h", "i", "j"})});

    NIMBLE_ASSERT_USER_THROW(
        writer->write(batch, *buffer_),
        "Null value not allowed in key column at index 1: found 3 null(s)");
  }

  // Test no nulls passes validation
  {
    SCOPED_TRACE("no nulls in key column passes");
    IndexConfig config{
        .columns = {std::string(kCol1)},
        .encodingLayout =
            EncodingLayout{EncodingType::Prefix, {}, CompressionType::Zstd}};
    auto writer = IndexWriter::create(config, type_, pool_.get());

    auto batch = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"a", "b", "c"}),
         makeFlatVector<int32_t>({1, 2, 3}),
         makeFlatVector<velox::StringView>({"d", "e", "f"})});

    EXPECT_NO_THROW(writer->write(batch, *buffer_));
  }
}

TEST_F(IndexWriterTest, multiColumnWithDifferentSortOrders) {
  const auto type = velox::ROW({
      {"col0", velox::INTEGER()},
      {"col1", velox::VARCHAR()},
      {"col2", velox::BIGINT()},
  });

  {
    SCOPED_TRACE("valid: empty sort orders uses default");
    IndexConfig config{
        .columns = {"col0", "col1"},
        .sortOrders = {},
        .encodingLayout =
            EncodingLayout{EncodingType::Prefix, {}, CompressionType::Zstd}};
    auto writer = IndexWriter::create(config, type, pool_.get());
    EXPECT_NE(writer, nullptr);
  }

  {
    SCOPED_TRACE("invalid: sort orders size mismatch (fewer)");
    IndexConfig config{
        .columns = {"col0", "col1", "col2"},
        .sortOrders =
            {SortOrder{.ascending = true}, SortOrder{.ascending = false}},
        .encodingLayout =
            EncodingLayout{EncodingType::Prefix, {}, CompressionType::Zstd}};
    NIMBLE_ASSERT_THROW(
        IndexWriter::create(config, type, pool_.get()),
        "sortOrders size (2) must be empty or match columns size (3)");
  }

  {
    SCOPED_TRACE("invalid: sort orders size mismatch (more)");
    IndexConfig config{
        .columns = {"col0", "col1"},
        .sortOrders =
            {SortOrder{.ascending = true},
             SortOrder{.ascending = false},
             SortOrder{.ascending = true}},
        .encodingLayout =
            EncodingLayout{EncodingType::Prefix, {}, CompressionType::Zstd}};
    NIMBLE_ASSERT_THROW(
        IndexWriter::create(config, type, pool_.get()),
        "sortOrders size (3) must be empty or match columns size (2)");
  }

  // Data test: write data with multiple columns and different sort orders,
  // then verify the encoded keys match what KeyEncoder produces.
  {
    SCOPED_TRACE("data test: write and verify encoded keys");
    const std::vector<SortOrder> sortOrders = {
        SortOrder{.ascending = true},
        SortOrder{.ascending = false},
        SortOrder{.ascending = true}};
    const std::vector<velox::core::SortOrder> veloxSortOrders = {
        velox::core::kAscNullsLast,
        velox::core::kDescNullsLast,
        velox::core::kAscNullsLast};

    IndexConfig config{
        .columns = {"col0", "col1", "col2"},
        .sortOrders = sortOrders,
        .encodingLayout =
            EncodingLayout{EncodingType::Prefix, {}, CompressionType::Zstd}};

    auto writer = IndexWriter::create(config, type, pool_.get());
    ASSERT_NE(writer, nullptr);

    // Create input data with 3 rows.
    auto batch = makeRowVector(
        {"col0", "col1", "col2"},
        {makeFlatVector<int32_t>({1, 2, 3}),
         makeFlatVector<velox::StringView>({"a", "b", "c"}),
         makeFlatVector<int64_t>({100, 200, 300})});

    Buffer buffer(*pool_);
    writer->write(batch, buffer);
    EXPECT_TRUE(writer->hasKeys());

    writer->encodeStream(buffer);
    auto stream = writer->finishStripe(buffer);

    ASSERT_TRUE(stream.has_value());
    ASSERT_EQ(stream->chunks.size(), 1);
    EXPECT_EQ(stream->chunks[0].rowCount, 3);

    // Create a KeyEncoder with the same configuration to verify keys.
    auto keyEncoder = velox::serializer::KeyEncoder::create(
        {"col0", "col1", "col2"}, type, veloxSortOrders, pool_.get());

    // Encode first row to get firstKey.
    auto firstRow = makeRowVector(
        {"col0", "col1", "col2"},
        {makeFlatVector<int32_t>({1}),
         makeFlatVector<velox::StringView>({"a"}),
         makeFlatVector<int64_t>({100})});
    Vector<std::string_view> firstEncodedKey(pool_.get());
    Buffer keyBuffer(*pool_);
    keyEncoder->encode(firstRow, firstEncodedKey, [&keyBuffer](size_t size) {
      return keyBuffer.reserve(size);
    });
    EXPECT_EQ(stream->chunks[0].firstKey, firstEncodedKey[0]);

    // Encode last row to get lastKey.
    auto lastRow = makeRowVector(
        {"col0", "col1", "col2"},
        {makeFlatVector<int32_t>({3}),
         makeFlatVector<velox::StringView>({"c"}),
         makeFlatVector<int64_t>({300})});
    Vector<std::string_view> lastEncodedKey(pool_.get());
    keyEncoder->encode(lastRow, lastEncodedKey, [&keyBuffer](size_t size) {
      return keyBuffer.reserve(size);
    });
    EXPECT_EQ(stream->chunks[0].lastKey, lastEncodedKey[0]);
  }

  // Data test: verify that different sort orders produce different encoded
  // keys for the same input data.
  {
    SCOPED_TRACE("data test: different sort orders produce different keys");
    const auto singleColType = velox::ROW({{"col0", velox::INTEGER()}});

    // Create input data.
    auto batch = makeRowVector({"col0"}, {makeFlatVector<int32_t>({1, 2, 3})});

    Buffer buffer1(*pool_);
    Buffer buffer2(*pool_);

    // Write with ascending order.
    IndexConfig configAsc{
        .columns = {"col0"},
        .sortOrders = {SortOrder{.ascending = true}},
        .encodingLayout =
            EncodingLayout{EncodingType::Prefix, {}, CompressionType::Zstd}};
    auto writerAsc = IndexWriter::create(configAsc, singleColType, pool_.get());
    writerAsc->write(batch, buffer1);
    writerAsc->encodeStream(buffer1);
    auto streamAsc = writerAsc->finishStripe(buffer1);

    // Write with descending order.
    IndexConfig configDesc{
        .columns = {"col0"},
        .sortOrders = {SortOrder{.ascending = false}},
        .encodingLayout =
            EncodingLayout{EncodingType::Prefix, {}, CompressionType::Zstd}};
    auto writerDesc =
        IndexWriter::create(configDesc, singleColType, pool_.get());
    writerDesc->write(batch, buffer2);
    writerDesc->encodeStream(buffer2);
    auto streamDesc = writerDesc->finishStripe(buffer2);

    ASSERT_TRUE(streamAsc.has_value());
    ASSERT_TRUE(streamDesc.has_value());

    // The encoded keys should be different because of different sort orders.
    EXPECT_NE(streamAsc->chunks[0].firstKey, streamDesc->chunks[0].firstKey);
    EXPECT_NE(streamAsc->chunks[0].lastKey, streamDesc->chunks[0].lastKey);
  }
}

TEST_F(IndexWriterTest, emptyWriteFinishStripe) {
  IndexConfig config{
      .columns = {std::string(kCol1)},
      .encodingLayout = EncodingLayout{
          EncodingType::Trivial,
          {},
          CompressionType::Uncompressed,
          {EncodingLayout{
              EncodingType::Trivial, {}, CompressionType::Uncompressed}}}};
  auto writer = IndexWriter::create(config, type_, pool_.get());
  ASSERT_NE(writer, nullptr);

  EXPECT_FALSE(writer->hasKeys());
  auto stream = writer->finishStripe(*buffer_);
  EXPECT_FALSE(stream.has_value());
}

TEST_F(IndexWriterTest, writeEmptyVectorOnly) {
  IndexConfig config{
      .columns = {std::string(kCol1)},
      .encodingLayout = EncodingLayout{
          EncodingType::Trivial,
          {},
          CompressionType::Uncompressed,
          {EncodingLayout{
              EncodingType::Trivial, {}, CompressionType::Uncompressed}}}};
  auto writer = IndexWriter::create(config, type_, pool_.get());
  ASSERT_NE(writer, nullptr);

  // Write a single empty vector.
  auto emptyBatch = makeRowVector(
      {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
      {makeFlatVector<velox::StringView>({}),
       makeFlatVector<int32_t>({}),
       makeFlatVector<velox::StringView>({})});

  writer->write(emptyBatch, *buffer_);
  EXPECT_FALSE(writer->hasKeys());

  writer->encodeStream(*buffer_);
  auto stream = writer->finishStripe(*buffer_);
  EXPECT_FALSE(stream.has_value());
}

// Test writing empty vectors at different positions: beginning, middle, and
// end.
TEST_F(IndexWriterTest, writeEmptyVectorAtDifferentPositions) {
  IndexConfig config{
      .columns = {std::string(kCol1)},
      .encodingLayout = EncodingLayout{
          EncodingType::Trivial,
          {},
          CompressionType::Uncompressed,
          {EncodingLayout{
              EncodingType::Trivial, {}, CompressionType::Uncompressed}}}};
  auto writer = IndexWriter::create(config, type_, pool_.get());
  ASSERT_NE(writer, nullptr);

  auto emptyBatch = makeRowVector(
      {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
      {makeFlatVector<velox::StringView>({}),
       makeFlatVector<int32_t>({}),
       makeFlatVector<velox::StringView>({})});

  // Write empty vector at the beginning.
  writer->write(emptyBatch, *buffer_);
  EXPECT_FALSE(writer->hasKeys());

  // Write first non-empty vector.
  auto batch1 = makeRowVector(
      {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
      {makeFlatVector<velox::StringView>({"a", "b"}),
       makeFlatVector<int32_t>({1, 2}),
       makeFlatVector<velox::StringView>({"c", "d"})});
  writer->write(batch1, *buffer_);
  EXPECT_TRUE(writer->hasKeys());

  // Write empty vector in the middle.
  writer->write(emptyBatch, *buffer_);
  EXPECT_TRUE(writer->hasKeys());

  // Write second non-empty vector.
  auto batch2 = makeRowVector(
      {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
      {makeFlatVector<velox::StringView>({"e", "f", "g"}),
       makeFlatVector<int32_t>({3, 4, 5}),
       makeFlatVector<velox::StringView>({"h", "i", "j"})});
  writer->write(batch2, *buffer_);
  EXPECT_TRUE(writer->hasKeys());

  // Write empty vector at the end.
  writer->write(emptyBatch, *buffer_);
  EXPECT_TRUE(writer->hasKeys());

  writer->encodeStream(*buffer_);
  auto stream = writer->finishStripe(*buffer_);
  ASSERT_TRUE(stream.has_value());
  ASSERT_EQ(stream->chunks.size(), 1);
  // Total rows: 2 (batch1) + 3 (batch2) = 5
  EXPECT_EQ(stream->chunks[0].rowCount, 5);
}

TEST_F(IndexWriterTest, enforceKeyOrderInvalidWithinBatch) {
  for (bool enforceKeyOrder : {true, false}) {
    SCOPED_TRACE(fmt::format("enforceKeyOrder: {}", enforceKeyOrder));
    IndexConfig config{
        .columns = {std::string(kCol1)},
        .enforceKeyOrder = enforceKeyOrder,
        .noDuplicateKey = true,
        .encodingLayout = EncodingLayout{
            EncodingType::Trivial,
            {},
            CompressionType::Uncompressed,
            {EncodingLayout{
                EncodingType::Trivial, {}, CompressionType::Uncompressed}}}};
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    auto batch = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"a", "b", "c", "d", "e"}),
         makeFlatVector<int32_t>({5, 4, 3, 2, 1}),
         makeFlatVector<velox::StringView>({"f", "g", "h", "i", "j"})});

    if (enforceKeyOrder) {
      NIMBLE_ASSERT_USER_THROW(
          writer->write(batch, *buffer_),
          "Encoded keys must be in strictly ascending order (duplicates are not allowed)");
    } else {
      EXPECT_NO_THROW(writer->write(batch, *buffer_));
      writer->encodeStream(*buffer_);
      auto stream = writer->finishStripe(*buffer_);
      ASSERT_TRUE(stream.has_value());
      ASSERT_EQ(stream->chunks.size(), 1);
      EXPECT_EQ(stream->chunks[0].rowCount, 5);
    }
  }
}

TEST_F(IndexWriterTest, enforceKeyOrderInvalidAcrossBatches) {
  for (bool enforceKeyOrder : {true, false}) {
    SCOPED_TRACE(fmt::format("enforceKeyOrder: {}", enforceKeyOrder));
    IndexConfig config{
        .columns = {std::string(kCol1)},
        .enforceKeyOrder = enforceKeyOrder,
        .noDuplicateKey = true,
        .encodingLayout = EncodingLayout{
            EncodingType::Trivial,
            {},
            CompressionType::Uncompressed,
            {EncodingLayout{
                EncodingType::Trivial, {}, CompressionType::Uncompressed}}}};
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    auto batch1 = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"a", "b", "c"}),
         makeFlatVector<int32_t>({1, 2, 3}),
         makeFlatVector<velox::StringView>({"d", "e", "f"})});
    auto batch2 = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"g", "h", "i"}),
         makeFlatVector<int32_t>({2, 3, 4}),
         makeFlatVector<velox::StringView>({"j", "k", "l"})});

    writer->write(batch1, *buffer_);
    if (enforceKeyOrder) {
      NIMBLE_ASSERT_USER_THROW(
          writer->write(batch2, *buffer_),
          "Encoded keys must be in strictly ascending order (duplicates are not allowed)");
    } else {
      EXPECT_NO_THROW(writer->write(batch2, *buffer_));
      writer->encodeStream(*buffer_);
      auto stream = writer->finishStripe(*buffer_);
      ASSERT_TRUE(stream.has_value());
      EXPECT_EQ(stream->chunks.size(), 1);
      EXPECT_EQ(stream->chunks[0].rowCount, 6);
    }
  }
}

TEST_F(IndexWriterTest, enforceKeyOrderDuplicateKeys) {
  // When enforceKeyOrder=true and noDuplicateKey=true, duplicate keys should
  // be rejected.
  IndexConfig config{
      .columns = {std::string(kCol1)},
      .enforceKeyOrder = true,
      .noDuplicateKey = true,
      .encodingLayout = EncodingLayout{
          EncodingType::Trivial,
          {},
          CompressionType::Uncompressed,
          {EncodingLayout{
              EncodingType::Trivial, {}, CompressionType::Uncompressed}}}};
  auto writer = IndexWriter::create(config, type_, pool_.get());
  ASSERT_NE(writer, nullptr);

  // Batch with duplicate keys (1, 1, 2, 3, 3)
  auto batch = makeRowVector(
      {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
      {makeFlatVector<velox::StringView>({"a", "b", "c", "d", "e"}),
       makeFlatVector<int32_t>({1, 1, 2, 3, 3}),
       makeFlatVector<velox::StringView>({"f", "g", "h", "i", "j"})});

  NIMBLE_ASSERT_USER_THROW(
      writer->write(batch, *buffer_),
      "Encoded keys must be in strictly ascending order (duplicates are not allowed)");
}

TEST_F(IndexWriterTest, noDuplicateKey) {
  // Test 1: When only noDuplicateKey=true (enforceKeyOrder=false), no checking
  // is performed - duplicates and out-of-order keys are allowed
  {
    IndexConfig config{
        .columns = {std::string(kCol1)},
        .noDuplicateKey = true,
        .encodingLayout = EncodingLayout{
            EncodingType::Trivial,
            {},
            CompressionType::Uncompressed,
            {EncodingLayout{
                EncodingType::Trivial, {}, CompressionType::Uncompressed}}}};
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    // Batch with duplicate keys (1, 1, 2, 3, 3) - should succeed since
    // enforceKeyOrder=false
    auto batch = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"a", "b", "c", "d", "e"}),
         makeFlatVector<int32_t>({1, 1, 2, 3, 3}),
         makeFlatVector<velox::StringView>({"f", "g", "h", "i", "j"})});

    EXPECT_NO_THROW(writer->write(batch, *buffer_));
    writer->encodeStream(*buffer_);
    auto stream = writer->finishStripe(*buffer_);
    ASSERT_TRUE(stream.has_value());
    ASSERT_EQ(stream->chunks.size(), 1);
    EXPECT_EQ(stream->chunks[0].rowCount, 5);
  }

  // Test 2: When only noDuplicateKey=true, out-of-order keys are also allowed
  {
    IndexConfig config{
        .columns = {std::string(kCol1)},
        .noDuplicateKey = true,
        .encodingLayout = EncodingLayout{
            EncodingType::Trivial,
            {},
            CompressionType::Uncompressed,
            {EncodingLayout{
                EncodingType::Trivial, {}, CompressionType::Uncompressed}}}};
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    // Batch with out-of-order keys (5, 4, 3, 2, 1) - should succeed since
    // enforceKeyOrder=false
    auto batch = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"a", "b", "c", "d", "e"}),
         makeFlatVector<int32_t>({5, 4, 3, 2, 1}),
         makeFlatVector<velox::StringView>({"f", "g", "h", "i", "j"})});

    EXPECT_NO_THROW(writer->write(batch, *buffer_));
    writer->encodeStream(*buffer_);
    auto stream = writer->finishStripe(*buffer_);
    ASSERT_TRUE(stream.has_value());
    ASSERT_EQ(stream->chunks.size(), 1);
    EXPECT_EQ(stream->chunks[0].rowCount, 5);
  }

  // Test 3: When enforceKeyOrder=true and noDuplicateKey=true, duplicate keys
  // are rejected
  {
    IndexConfig config{
        .columns = {std::string(kCol1)},
        .enforceKeyOrder = true,
        .noDuplicateKey = true,
        .encodingLayout = EncodingLayout{
            EncodingType::Trivial,
            {},
            CompressionType::Uncompressed,
            {EncodingLayout{
                EncodingType::Trivial, {}, CompressionType::Uncompressed}}}};
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    // Batch with duplicate keys (1, 1, 2, 3, 3) - should fail
    auto batch = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"a", "b", "c", "d", "e"}),
         makeFlatVector<int32_t>({1, 1, 2, 3, 3}),
         makeFlatVector<velox::StringView>({"f", "g", "h", "i", "j"})});

    NIMBLE_ASSERT_USER_THROW(
        writer->write(batch, *buffer_),
        "Encoded keys must be in strictly ascending order (duplicates are not allowed)");
  }

  // Test 4: When enforceKeyOrder=true and noDuplicateKey=true, duplicates
  // across batches are rejected
  {
    IndexConfig config{
        .columns = {std::string(kCol1)},
        .enforceKeyOrder = true,
        .noDuplicateKey = true,
        .encodingLayout = EncodingLayout{
            EncodingType::Trivial,
            {},
            CompressionType::Uncompressed,
            {EncodingLayout{
                EncodingType::Trivial, {}, CompressionType::Uncompressed}}}};
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    auto batch1 = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"a", "b", "c"}),
         makeFlatVector<int32_t>({1, 2, 3}),
         makeFlatVector<velox::StringView>({"d", "e", "f"})});

    // Second batch starts with same key as last key of first batch
    auto batch2 = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"g", "h", "i"}),
         makeFlatVector<int32_t>({3, 4, 5}),
         makeFlatVector<velox::StringView>({"j", "k", "l"})});

    writer->write(batch1, *buffer_);
    NIMBLE_ASSERT_USER_THROW(
        writer->write(batch2, *buffer_),
        "Encoded keys must be in strictly ascending order (duplicates are not allowed)");
  }

  // Test 5: When enforceKeyOrder=true and noDuplicateKey=true, strictly
  // ascending keys succeed
  {
    IndexConfig config{
        .columns = {std::string(kCol1)},
        .enforceKeyOrder = true,
        .noDuplicateKey = true,
        .encodingLayout = EncodingLayout{
            EncodingType::Trivial,
            {},
            CompressionType::Uncompressed,
            {EncodingLayout{
                EncodingType::Trivial, {}, CompressionType::Uncompressed}}}};
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    auto batch1 = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"a", "b", "c"}),
         makeFlatVector<int32_t>({1, 2, 3}),
         makeFlatVector<velox::StringView>({"d", "e", "f"})});

    auto batch2 = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"g", "h", "i"}),
         makeFlatVector<int32_t>({4, 5, 6}),
         makeFlatVector<velox::StringView>({"j", "k", "l"})});

    EXPECT_NO_THROW(writer->write(batch1, *buffer_));
    EXPECT_NO_THROW(writer->write(batch2, *buffer_));
    writer->encodeStream(*buffer_);
    auto stream = writer->finishStripe(*buffer_);
    ASSERT_TRUE(stream.has_value());
    ASSERT_EQ(stream->chunks.size(), 1);
    EXPECT_EQ(stream->chunks[0].rowCount, 6);
  }

  // Test 6: When enforceKeyOrder=true but noDuplicateKey=false, duplicates are
  // allowed
  {
    IndexConfig config{
        .columns = {std::string(kCol1)},
        .enforceKeyOrder = true,
        .noDuplicateKey = false,
        .encodingLayout = EncodingLayout{
            EncodingType::Trivial,
            {},
            CompressionType::Uncompressed,
            {EncodingLayout{
                EncodingType::Trivial, {}, CompressionType::Uncompressed}}}};
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    // Batch with duplicate keys (1, 1, 2, 3, 3) - should succeed
    auto batch = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"a", "b", "c", "d", "e"}),
         makeFlatVector<int32_t>({1, 1, 2, 3, 3}),
         makeFlatVector<velox::StringView>({"f", "g", "h", "i", "j"})});

    EXPECT_NO_THROW(writer->write(batch, *buffer_));
    writer->encodeStream(*buffer_);
    auto stream = writer->finishStripe(*buffer_);
    ASSERT_TRUE(stream.has_value());
    ASSERT_EQ(stream->chunks.size(), 1);
    EXPECT_EQ(stream->chunks[0].rowCount, 5);
  }

  // Test 7: When enforceKeyOrder=true but noDuplicateKey=false, out-of-order
  // keys are still rejected
  {
    IndexConfig config{
        .columns = {std::string(kCol1)},
        .enforceKeyOrder = true,
        .noDuplicateKey = false,
        .encodingLayout = EncodingLayout{
            EncodingType::Trivial,
            {},
            CompressionType::Uncompressed,
            {EncodingLayout{
                EncodingType::Trivial, {}, CompressionType::Uncompressed}}}};
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    // Batch with out-of-order keys (5, 4, 3, 2, 1) - should fail
    auto batch = makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {makeFlatVector<velox::StringView>({"a", "b", "c", "d", "e"}),
         makeFlatVector<int32_t>({5, 4, 3, 2, 1}),
         makeFlatVector<velox::StringView>({"f", "g", "h", "i", "j"})});

    NIMBLE_ASSERT_USER_THROW(
        writer->write(batch, *buffer_),
        "Encoded keys must be in ascending order");
  }
}

struct IndexWriterTestParam {
  EncodingType encodingType;
  SortOrder sortOrder;
  std::optional<uint32_t> prefixRestartInterval;

  std::string toString() const {
    const std::string encodingName =
        encodingType == EncodingType::Prefix ? "Prefix" : "Trivial";
    const std::string sortName = sortOrder.ascending ? "Asc" : "Desc";
    std::string result = encodingName + "_" + sortName;
    if (prefixRestartInterval.has_value()) {
      result += "_restart" + std::to_string(prefixRestartInterval.value());
    }
    return result;
  }
};

class IndexWriterDataTest
    : public IndexWriterTest,
      public ::testing::WithParamInterface<IndexWriterTestParam> {
 protected:
  void SetUp() override {
    IndexWriterTest::SetUp();
    velox::VectorFuzzer::Options opts;
    opts.nullRatio = 0;
    fuzzer_ = std::make_unique<velox::VectorFuzzer>(opts, pool_.get());
  }

  void TearDown() override {
    fuzzer_ = nullptr;
    IndexWriterTest::TearDown();
  }

  EncodingType encodingType() const {
    return GetParam().encodingType;
  }

  SortOrder sortOrder() const {
    return GetParam().sortOrder;
  }

  std::unique_ptr<velox::serializer::KeyEncoder> createKeyEncoderWithSortOrder()
      const {
    const auto keyType = velox::ROW({{std::string(kCol1), velox::INTEGER()}});
    return velox::serializer::KeyEncoder::create(
        {std::string(kCol1)},
        keyType,
        {sortOrder().toVeloxSortOrder()},
        pool_.get());
  }

  IndexConfig indexConfigWithSortOrder(
      EncodingType encodingType = EncodingType::Prefix) const {
    EncodingLayout layout{
        EncodingType::Trivial, {}, CompressionType::Uncompressed};
    if (encodingType == EncodingType::Prefix) {
      EncodingLayout::Config config;
      if (GetParam().prefixRestartInterval.has_value()) {
        config = EncodingLayout::Config{
            {{std::string(PrefixEncoding::kRestartIntervalConfigKey),
              std::to_string(GetParam().prefixRestartInterval.value())}}};
      }
      layout = EncodingLayout{
          EncodingType::Prefix, std::move(config), CompressionType::Zstd};
    } else {
      layout = EncodingLayout{
          EncodingType::Trivial,
          {},
          CompressionType::Zstd,
          {EncodingLayout{
              EncodingType::Trivial, {}, CompressionType::Uncompressed}}};
    }
    return IndexConfig{
        .columns = {std::string(kCol1)},
        .sortOrders = {sortOrder()},
        .encodingLayout = layout};
  }

  // Creates a row vector with the given index column values and
  // fuzzer-generated non-index columns.
  velox::RowVectorPtr makeInput(const std::vector<int32_t>& indexValues) {
    auto opts = fuzzer_->getOptions();
    opts.vectorSize = indexValues.size();
    fuzzer_->setOptions(opts);
    auto fuzzed = fuzzer_->fuzzInputFlatRow(type_);
    return makeRowVector(
        {std::string(kCol0), std::string(kCol1), std::string(kCol2)},
        {fuzzed->childAt(0),
         makeFlatVector<int32_t>(indexValues),
         fuzzed->childAt(2)});
  }

  // Creates a row vector containing only the key column for KeyEncoder.
  velox::RowVectorPtr makeKeyInput(int32_t keyValue) {
    return makeRowVector(
        {std::string(kCol1)}, {makeFlatVector<int32_t>({keyValue})});
  }

  std::unique_ptr<velox::VectorFuzzer> fuzzer_;
};

TEST_P(IndexWriterDataTest, writeAndFinishStripeNonChunked) {
  auto config = indexConfigWithSortOrder(encodingType());
  auto keyEncoder = createKeyEncoderWithSortOrder();

  struct {
    std::vector<std::vector<int32_t>> batches;

    std::string debugString() const {
      std::vector<std::string> batchStrs;
      batchStrs.reserve(batches.size());
      for (const auto& batch : batches) {
        batchStrs.push_back(fmt::format("[{}]", fmt::join(batch, ", ")));
      }
      return fmt::format("batches: [{}]", fmt::join(batchStrs, ", "));
    }

    size_t totalRows() const {
      size_t total = 0;
      for (const auto& batch : batches) {
        total += batch.size();
      }
      return total;
    }
  } testCases[] = {
      // Single batch, single row
      {{{1}}},
      // Single batch, multiple rows
      {{{1, 2, 3, 4, 5}}},
      // Two batches
      {{{1, 2, 3}, {4, 5, 6}}},
      // Three batches
      {{{1, 2}, {3, 4}, {5, 6}}},
      // Multiple batches with varying sizes
      {{{1}, {2, 3}, {4, 5, 6}, {7, 8, 9, 10}}},
      // Large single batch
      {{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}},
      // Many small batches
      {{{1}, {2}, {3}, {4}, {5}}},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    for (const auto& batchData : testCase.batches) {
      writer->write(makeInput(batchData), *buffer_);
    }
    EXPECT_TRUE(writer->hasKeys());

    writer->encodeStream(*buffer_);
    auto stream = writer->finishStripe(*buffer_);

    ASSERT_TRUE(stream.has_value());
    ASSERT_EQ(stream->chunks.size(), 1);
    EXPECT_EQ(stream->chunks[0].rowCount, testCase.totalRows());

    Vector<std::string_view> encodedKeys(pool_.get());
    Buffer keyBuffer(*pool_);
    // Verify firstKey matches the first value of the first batch.
    keyEncoder->encode(
        makeKeyInput(testCase.batches.front().front()),
        encodedKeys,
        [&keyBuffer](size_t size) { return keyBuffer.reserve(size); });
    EXPECT_EQ(stream->chunks[0].firstKey, encodedKeys[0]);

    // Verify lastKey matches the last value of the last batch.
    encodedKeys.clear();
    keyEncoder->encode(
        makeKeyInput(testCase.batches.back().back()),
        encodedKeys,
        [&keyBuffer](size_t size) { return keyBuffer.reserve(size); });
    EXPECT_EQ(stream->chunks[0].lastKey, encodedKeys[0]);
  }
}

TEST_P(IndexWriterDataTest, writeAndFinishStripeChunked) {
  auto config = indexConfig(encodingType());
  // Use small chunk sizes to force multiple chunks.
  config.minChunkRawSize = 1;
  config.maxChunkRawSize = 50;
  auto keyEncoder = createKeyEncoder();

  struct {
    std::vector<std::vector<int32_t>> batches;

    std::string debugString() const {
      std::vector<std::string> batchStrs;
      batchStrs.reserve(batches.size());
      for (const auto& batch : batches) {
        batchStrs.push_back(fmt::format("[{}]", fmt::join(batch, ", ")));
      }
      return fmt::format("batches: [{}]", fmt::join(batchStrs, ", "));
    }

    // Returns all index values flattened in order.
    std::vector<int32_t> allValues() const {
      std::vector<int32_t> values;
      for (const auto& batch : batches) {
        values.insert(values.end(), batch.begin(), batch.end());
      }
      return values;
    }
  } testCases[] = {
      // Single batch with multiple rows to force chunking.
      {{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}},
      // Multiple batches.
      {{{1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}}},
      // Many small batches.
      {{{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}}},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    auto writer = IndexWriter::create(config, type_, pool_.get());
    ASSERT_NE(writer, nullptr);

    for (const auto& batchData : testCase.batches) {
      writer->write(makeInput(batchData), *buffer_);
    }
    EXPECT_TRUE(writer->hasKeys());

    while (writer->encodeChunk(false, false, *buffer_)) {
    }

    while (writer->encodeChunk(false, true, *buffer_)) {
    }

    auto stream = writer->finishStripe(*buffer_);
    ASSERT_TRUE(stream.has_value());
    ASSERT_GE(stream->chunks.size(), 1);

    // Encode all keys upfront for verification.
    const auto allValues = testCase.allValues();
    Vector<std::string_view> allEncodedKeys(pool_.get());
    Buffer keyBuffer(*pool_);
    keyEncoder->encode(
        makeRowVector(
            {std::string(kCol1)}, {makeFlatVector<int32_t>(allValues)}),
        allEncodedKeys,
        [&keyBuffer](size_t size) { return keyBuffer.reserve(size); });

    // Verify chunk keys based on row offsets.
    uint64_t rowOffset = 0;
    for (size_t i = 0; i < stream->chunks.size(); ++i) {
      SCOPED_TRACE(fmt::format("chunk {}", i));
      const auto& chunk = stream->chunks[i];

      // Verify firstKey matches the value at chunk start offset.
      EXPECT_EQ(chunk.firstKey, allEncodedKeys[rowOffset]);

      // Verify lastKey matches the value at chunk end offset.
      rowOffset += chunk.rowCount;
      ASSERT_EQ(chunk.lastKey, allEncodedKeys[rowOffset - 1]);
    }
    // Verify all rows are accounted for.
    EXPECT_EQ(rowOffset, allValues.size());
  }
}

TEST_P(IndexWriterDataTest, indexChunkSizeControl) {
  struct TestCase {
    // Number of input rows.
    size_t numInputRows;
    uint64_t minChunkRawSize;
    uint64_t maxChunkRawSize;
    // Whether to use ensureFullChunks in encodeChunk
    // calls.dwio/nimble/tools/fb/feature_reaper/NimbleRewrite.cpp
    bool ensureFullChunks;
    // Expected total chunk count after all encoding.
    size_t expectedChunks;

    std::string debugString() const {
      return fmt::format(
          "numInputRows: {}, minChunkRawSize: {}, maxChunkRawSize: {}, ensureFullChunks: {}",
          numInputRows,
          minChunkRawSize,
          maxChunkRawSize,
          ensureFullChunks);
    }
  } testCases[] = {
      // ===== Input smaller than thresholds =====
      // Single row input - always produces single chunk.
      {
          .numInputRows = 1,
          .minChunkRawSize = 1 << 20,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = false,
          .expectedChunks = 1,
      },
      // Small input with large thresholds - single chunk.
      {
          .numInputRows = 10,
          .minChunkRawSize = 1 << 20,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = false,
          .expectedChunks = 1,
      },
      // Medium input with large thresholds - single chunk.
      {
          .numInputRows = 100,
          .minChunkRawSize = 1 << 20,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = false,
          .expectedChunks = 1,
      },
      // Large input with very large thresholds - single chunk.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 1 << 20,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = false,
          .expectedChunks = 1,
      },
      // Large input with ensureFullChunks=true and large thresholds.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 1 << 20,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = true,
          .expectedChunks = 1,
      },
      // Single row with minimum possible min threshold.
      {
          .numInputRows = 1,
          .minChunkRawSize = 1,
          .maxChunkRawSize = 1 << 20,
          .ensureFullChunks = false,
          .expectedChunks = 1,
      },
      // ensureFullChunks with single row.
      {
          .numInputRows = 1,
          .minChunkRawSize = 1 << 20,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = true,
          .expectedChunks = 1,
      },
      // Medium input with ensureFullChunks - single chunk.
      {
          .numInputRows = 100,
          .minChunkRawSize = 1 << 20,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = true,
          .expectedChunks = 1,
      },
      // ===== Input larger than max threshold - forces multiple chunks =====

      // Large input with tiny thresholds - many chunks.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 1,
          .maxChunkRawSize = 100,
          .ensureFullChunks = false,
          .expectedChunks = 250,
      },
      // Large input with tiny thresholds and ensureFullChunks.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 1,
          .maxChunkRawSize = 100,
          .ensureFullChunks = true,
          .expectedChunks = 250,
      },
      // Large input with small max threshold.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 1,
          .maxChunkRawSize = 20,
          .ensureFullChunks = false,
          .expectedChunks = 1'000,
      },
      // Large input with small max threshold and ensureFullChunks.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 1,
          .maxChunkRawSize = 20,
          .ensureFullChunks = true,
          .expectedChunks = 1'000,
      },
      // Large input with medium thresholds.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 500,
          .ensureFullChunks = false,
          .expectedChunks = 44,
      },
      // Large input with medium thresholds and ensureFullChunks.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 500,
          .ensureFullChunks = true,
          .expectedChunks = 44,
      },
      // Very large input with small thresholds.
      {
          .numInputRows = 10'000,
          .minChunkRawSize = 50,
          .maxChunkRawSize = 200,
          .ensureFullChunks = false,
          .expectedChunks = 1'112,
      },
      // Very large input with small thresholds and ensureFullChunks.
      {
          .numInputRows = 10'000,
          .minChunkRawSize = 50,
          .maxChunkRawSize = 200,
          .ensureFullChunks = true,
          .expectedChunks = 1'112,
      },

      // ===== Input larger than min but smaller than max threshold =====

      // Input between min and max - may or may not produce chunks.
      {
          .numInputRows = 100,
          .minChunkRawSize = 50,
          .maxChunkRawSize = 10'000,
          .ensureFullChunks = false,
          .expectedChunks = 1,
      },
      // Input between min and max with ensureFullChunks.
      {
          .numInputRows = 100,
          .minChunkRawSize = 50,
          .maxChunkRawSize = 10'000,
          .ensureFullChunks = true,
          .expectedChunks = 1,
      },
      // Medium input with tight min/max range.
      {
          .numInputRows = 500,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 200,
          .ensureFullChunks = false,
          .expectedChunks = 56,
      },
      // Medium input with tight min/max range and ensureFullChunks.
      {
          .numInputRows = 500,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 200,
          .ensureFullChunks = true,
          .expectedChunks = 56,
      },
      // ===== Edge cases with equal min and max =====

      // Min equals max - fixed chunk size.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 100,
          .ensureFullChunks = false,
          .expectedChunks = 250,
      },
      // Min equals max with ensureFullChunks.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 100,
          .ensureFullChunks = true,
          .expectedChunks = 250,
      },
      // Tiny equal thresholds.
      {
          .numInputRows = 100,
          .minChunkRawSize = 1,
          .maxChunkRawSize = 20,
          .ensureFullChunks = false,
          .expectedChunks = 100,
      },

      // ===== Different input sizes with same thresholds =====

      // Varying input sizes with fixed medium thresholds.
      {
          .numInputRows = 50,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 1'000,
          .ensureFullChunks = false,
          .expectedChunks = 2,
      },
      {
          .numInputRows = 200,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 1'000,
          .ensureFullChunks = false,
          .expectedChunks = 5,
      },
      {
          .numInputRows = 500,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 1'000,
          .ensureFullChunks = false,
          .expectedChunks = 11,
      },
      {
          .numInputRows = 2'000,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 1'000,
          .ensureFullChunks = false,
          .expectedChunks = 43,
      },
      {
          .numInputRows = 5'000,
          .minChunkRawSize = 100,
          .maxChunkRawSize = 1'000,
          .ensureFullChunks = false,
          .expectedChunks = 107,
      },

      // ===== Zero min threshold =====

      // Zero min with small max.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 0,
          .maxChunkRawSize = 100,
          .ensureFullChunks = false,
          .expectedChunks = 250,
      },
      // Zero min with small max and ensureFullChunks.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 0,
          .maxChunkRawSize = 100,
          .ensureFullChunks = true,
          .expectedChunks = 250,
      },
      // Zero min with large max.
      {
          .numInputRows = 1'000,
          .minChunkRawSize = 0,
          .maxChunkRawSize = 10 << 20,
          .ensureFullChunks = false,
          .expectedChunks = 1,
      },
  };

  auto keyEncoder = createKeyEncoder();

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());

    // Generate input.
    std::vector<int32_t> input;
    input.reserve(testCase.numInputRows);
    for (size_t i = 0; i < testCase.numInputRows; ++i) {
      input.push_back(static_cast<int32_t>(i));
    }

    auto config = indexConfig(encodingType());
    config.minChunkRawSize = testCase.minChunkRawSize;
    config.maxChunkRawSize = testCase.maxChunkRawSize;

    auto writer = IndexWriter::create(config, type_, pool_.get());

    writer->write(makeInput(input), *buffer_);
    ASSERT_TRUE(writer->hasKeys());

    // First encode with lastChunk=false to use minChunkSize constraint.
    writer->encodeChunk(
        testCase.ensureFullChunks, /*lastChunk=*/false, *buffer_);

    // Then flush remaining with lastChunk=true.
    writer->encodeChunk(
        /*ensureFullChunks=*/false, /*lastChunk=*/true, *buffer_);

    auto stream = writer->finishStripe(*buffer_);
    ASSERT_TRUE(stream.has_value());
    ASSERT_EQ(stream->chunks.size(), testCase.expectedChunks);

    // Encode all keys upfront for verification.
    Vector<std::string_view> allEncodedKeys(pool_.get());
    Buffer keyBuffer(*pool_);
    keyEncoder->encode(
        makeRowVector({std::string(kCol1)}, {makeFlatVector<int32_t>(input)}),
        allEncodedKeys,
        [&keyBuffer](size_t size) { return keyBuffer.reserve(size); });

    // Verify chunk keys based on row offsets.
    uint64_t rowOffset = 0;
    for (size_t i = 0; i < stream->chunks.size(); ++i) {
      SCOPED_TRACE(fmt::format("chunk {}", i));
      const auto& chunk = stream->chunks[i];
      ASSERT_FALSE(chunk.content.empty());

      // Verify firstKey matches the value at chunk start offset.
      ASSERT_EQ(chunk.firstKey, allEncodedKeys[rowOffset]);

      // Verify lastKey matches the value at chunk end offset.
      ASSERT_EQ(chunk.lastKey, allEncodedKeys[rowOffset + chunk.rowCount - 1]);

      rowOffset += chunk.rowCount;
    }
    // Verify all rows are accounted for.
    ASSERT_EQ(rowOffset, input.size());
  }
}

TEST_P(IndexWriterDataTest, multiColumnIndex) {
  auto type = velox::ROW({
      {"col0", velox::INTEGER()},
      {"col1", velox::VARCHAR()},
  });
  EncodingLayout layout = encodingType() == EncodingType::Prefix
      ? EncodingLayout{EncodingType::Prefix, {}, CompressionType::Zstd}
      : EncodingLayout{
            EncodingType::Trivial,
            {},
            CompressionType::Zstd,
            {EncodingLayout{
                EncodingType::Trivial, {}, CompressionType::Uncompressed}}};
  IndexConfig config{.columns = {"col0", "col1"}, .encodingLayout = layout};
  auto writer = IndexWriter::create(config, type, pool_.get());

  auto batch = makeRowVector(
      {"col0", "col1"},
      {makeFlatVector<int32_t>({1, 2, 3}),
       makeFlatVector<velox::StringView>({"a", "b", "c"})});

  writer->write(batch, *buffer_);
  writer->encodeStream(*buffer_);
  auto stream = writer->finishStripe(*buffer_);

  ASSERT_TRUE(stream.has_value());
  EXPECT_EQ(stream->chunks.size(), 1);
  EXPECT_EQ(stream->chunks[0].rowCount, 3);
}

TEST_P(IndexWriterDataTest, close) {
  auto config = indexConfig(encodingType());
  auto writer = IndexWriter::create(config, type_, pool_.get());
  ASSERT_NE(writer, nullptr);

  writer->write(makeInput({1, 2, 3}), *buffer_);
  EXPECT_TRUE(writer->hasKeys());

  writer->close();
  EXPECT_FALSE(writer->hasKeys());
}

// Dedicated test to verify encodeChunk loop processing:
// - The while loop correctly iterates through all chunks from StreamChunker
// - Each chunk has correct firstKey/lastKey boundaries
// - chunker->compact() properly advances the stream (no data loss/duplication)
// - The method returns true when chunks are written, false when empty
//
// We set very small min/max chunk sizes so that all keys are encoded in a
// single encodeChunk call (with lastChunk=true), which exercises the internal
// while loop multiple times within that one call.
TEST_P(IndexWriterDataTest, encodeChunkLoopProcessing) {
  auto keyEncoder = createKeyEncoder();

  struct TestCase {
    std::vector<int32_t> input;
    // Expected minimum number of chunks (depends on encoding overhead).
    size_t expectedMinChunks;

    std::string debugString() const {
      return fmt::format(
          "input size: {}, expectedMinChunks: {}",
          input.size(),
          expectedMinChunks);
    }
  } testCases[] = {
      // Single row - single chunk.
      {.input = {1}, .expectedMinChunks = 1},
      // Small input - multiple chunks due to tiny max size.
      // Each key is ~9 bytes encoded, max 20 bytes = ~2 keys per chunk.
      {.input = {1, 2, 3, 4, 5}, .expectedMinChunks = 3},
      // Larger input - forces many loop iterations.
      {.input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, .expectedMinChunks = 5},
      // Even larger input.
      {.input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
       .expectedMinChunks = 8},
      // 30 keys - many loop iterations.
      {.input = {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
                 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30},
       .expectedMinChunks = 15},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());

    auto config = indexConfig(encodingType());
    // Use tiny chunk sizes to force multiple chunks per encodeChunk call.
    // This exercises the internal while loop multiple times.
    config.minChunkRawSize = 1;
    config.maxChunkRawSize = 20;

    auto writer = IndexWriter::create(config, type_, pool_.get());
    writer->write(makeInput(testCase.input), *buffer_);

    // Single encodeChunk call with lastChunk=true should process all keys
    // through the internal while loop. No need for additional calls.
    EXPECT_TRUE(writer->encodeChunk(false, true, *buffer_));

    // Subsequent call should return false since all keys are encoded.
    EXPECT_FALSE(writer->encodeChunk(false, true, *buffer_));

    auto stream = writer->finishStripe(*buffer_);
    ASSERT_TRUE(stream.has_value());

    // Verify we got at least the minimum expected chunks.
    ASSERT_GE(stream->chunks.size(), testCase.expectedMinChunks);

    // Encode all expected keys for verification.
    Vector<std::string_view> allEncodedKeys(pool_.get());
    Buffer keyBuffer(*pool_);
    keyEncoder->encode(
        makeRowVector(
            {std::string(kCol1)}, {makeFlatVector<int32_t>(testCase.input)}),
        allEncodedKeys,
        [&keyBuffer](size_t size) { return keyBuffer.reserve(size); });

    // Verify each chunk has correct firstKey/lastKey from loop processing.
    // This validates that compact() properly advances the stream.
    uint64_t rowOffset = 0;
    for (size_t i = 0; i < stream->chunks.size(); ++i) {
      SCOPED_TRACE(fmt::format("chunk {}", i));
      const auto& chunk = stream->chunks[i];

      // Verify firstKey matches key at chunk start.
      EXPECT_EQ(chunk.firstKey, allEncodedKeys[rowOffset]);
      // Verify lastKey matches key at chunk end.
      EXPECT_EQ(chunk.lastKey, allEncodedKeys[rowOffset + chunk.rowCount - 1]);

      rowOffset += chunk.rowCount;
    }

    // Verify no data lost or duplicated - all rows accounted for.
    EXPECT_EQ(rowOffset, testCase.input.size());
  }
}

// Test that encodeChunk returns false when there are no keys to encode.
TEST_P(IndexWriterDataTest, encodeChunkEmptyStream) {
  auto config = indexConfig(encodingType());
  auto writer = IndexWriter::create(config, type_, pool_.get());

  // encodeChunk on empty stream should return false.
  EXPECT_FALSE(writer->encodeChunk(false, false, *buffer_));
  EXPECT_FALSE(writer->encodeChunk(false, true, *buffer_));
  EXPECT_FALSE(writer->encodeChunk(true, false, *buffer_));
  EXPECT_FALSE(writer->encodeChunk(true, true, *buffer_));

  auto stream = writer->finishStripe(*buffer_);
  EXPECT_FALSE(stream.has_value());
}

INSTANTIATE_TEST_SUITE_P(
    IndexWriterDataTestSuite,
    IndexWriterDataTest,
    ::testing::Values(
        IndexWriterTestParam{
            EncodingType::Prefix,
            SortOrder{.ascending = true}},
        IndexWriterTestParam{
            EncodingType::Prefix,
            SortOrder{.ascending = false}},
        IndexWriterTestParam{
            EncodingType::Trivial,
            SortOrder{.ascending = true}},
        IndexWriterTestParam{
            EncodingType::Trivial,
            SortOrder{.ascending = false}},
        IndexWriterTestParam{
            EncodingType::Prefix,
            SortOrder{.ascending = false},
            1},
        IndexWriterTestParam{
            EncodingType::Prefix,
            SortOrder{.ascending = false},
            1024}),
    [](const ::testing::TestParamInfo<IndexWriterTestParam>& info) {
      return info.param.toString();
    });

} // namespace
} // namespace facebook::nimble::index::test
