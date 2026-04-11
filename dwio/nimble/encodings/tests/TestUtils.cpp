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

#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/encodings/EncodingUtils.h"

namespace facebook::nimble::test {

static constexpr int kCompressionTypeSize = 1;

// Read the row count from an encoding prefix at the given position.
// Advances pos past the row count field.
static uint32_t readRowCount(const char*& pos, bool useVarintRowCount) {
  if (useVarintRowCount) {
    return varint::readVarint32(&pos);
  } else {
    return encoding::readUint32(pos);
  }
}

// Skip the encoding prefix (encoding type + data type + row count) starting
// at pos. Returns the row count read.
static uint32_t skipPrefix(const char*& pos, bool useVarintRowCount) {
  pos += 2; // encoding type + data type
  return readRowCount(pos, useVarintRowCount);
}

uint64_t TestUtils::getRawDataSize(
    velox::memory::MemoryPool& memoryPool,
    std::string_view encodingStr,
    bool useVarintRowCount) {
  const Encoding::Options options{.useVarintRowCount = useVarintRowCount};
  std::vector<velox::BufferPtr> newStringBuffers;
  const auto stringBufferFactory = [&](uint32_t totalLength) {
    auto& buffer = newStringBuffers.emplace_back(
        velox::AlignedBuffer::allocate<char>(totalLength, &memoryPool));
    return buffer->asMutable<void>();
  };
  const EncodingFactory factory{options};
  auto encoding = factory.create(memoryPool, encodingStr, stringBufferFactory);
  EncodingType encodingType = encoding->encodingType();
  DataType dataType = encoding->dataType();
  uint32_t rowCount = encoding->rowCount();

  if (encodingType == EncodingType::Sentinel) {
    NIMBLE_UNSUPPORTED("Sentinel encoding is not supported");
  }

  if (encodingType == EncodingType::Nullable) {
    // Skip the outer prefix to find the inner data.
    auto pos = encodingStr.data();
    skipPrefix(pos, useVarintRowCount);
    auto nonNullsSize = encoding::readUint32(pos);
    // Sum of the nulls count and size of the non-null child encoding.
    return getRawDataSize(memoryPool, {pos, nonNullsSize}, useVarintRowCount) +
        rowCount;
  } else {
    if (dataType != DataType::String) {
      auto typeSize = nimble::detail::dataTypeSize(dataType);
      auto result = typeSize * rowCount;
      return result;
    } else {
      // Skip the prefix to get to the encoding-specific data.
      auto pos = encodingStr.data();
      skipPrefix(pos, useVarintRowCount);
      uint64_t result = 0;

      switch (encodingType) {
        case EncodingType::Trivial: {
          pos += kCompressionTypeSize;
          auto lengthsSize = encoding::readUint32(pos);
          auto lengths = factory.create(
              memoryPool, {pos, lengthsSize}, stringBufferFactory);
          std::vector<uint32_t> buffer(rowCount);
          lengths->materialize(rowCount, buffer.data());
          result += std::accumulate(buffer.begin(), buffer.end(), 0u);
          break;
        }

        case EncodingType::Constant: {
          auto valueSize = encoding::readUint32(pos);
          result += rowCount * valueSize;
          break;
        }

        case EncodingType::MainlyConstant: {
          auto isCommonSize = encoding::readUint32(pos);
          pos += isCommonSize;
          auto otherValuesSize = encoding::readUint32(pos);
          auto otherValuesOffset = pos;
          // Read the sub-encoding's row count by parsing its prefix.
          const char* subPos = pos;
          subPos += 2; // encoding type + data type
          auto otherValuesCount = readRowCount(subPos, useVarintRowCount);
          pos += otherValuesSize;
          auto constantValueSize = encoding::readUint32(pos);
          result += (rowCount - otherValuesCount) * constantValueSize;
          result += getRawDataSize(
              memoryPool,
              {otherValuesOffset, otherValuesSize},
              useVarintRowCount);
          break;
        }

        case EncodingType::Dictionary: {
          auto alphabetSize = encoding::readUint32(pos);
          // Read the sub-encoding's row count.
          const char* subPos = pos;
          subPos += 2; // encoding type + data type
          auto alphabetCount = readRowCount(subPos, useVarintRowCount);
          auto alphabet = factory.create(
              memoryPool, {pos, alphabetSize}, stringBufferFactory);
          std::vector<std::string_view> alphabetBuffer(alphabetCount);
          alphabet->materialize(alphabetCount, alphabetBuffer.data());

          pos += alphabetSize;
          auto indicesSize = encodingStr.length() - (pos - encodingStr.data());
          auto indices = factory.create(
              memoryPool, {pos, indicesSize}, stringBufferFactory);
          std::vector<uint32_t> indicesBuffer(rowCount);
          indices->materialize(rowCount, indicesBuffer.data());
          for (uint32_t i = 0; i < rowCount; ++i) {
            result += alphabetBuffer[indicesBuffer[i]].size();
          }
          break;
        }

        case EncodingType::RLE: {
          auto runLengthsSize = encoding::readUint32(pos);
          // Read the sub-encoding's row count.
          const char* subPos = pos;
          subPos += 2; // encoding type + data type
          auto runLengthsCount = readRowCount(subPos, useVarintRowCount);
          auto runLengths = factory.create(
              memoryPool, {pos, runLengthsSize}, stringBufferFactory);
          std::vector<uint32_t> runLengthsBuffer(runLengthsCount);
          runLengths->materialize(runLengthsCount, runLengthsBuffer.data());

          pos += runLengthsSize;
          auto runValuesSize =
              encodingStr.length() - (pos - encodingStr.data());
          auto runValues = factory.create(
              memoryPool, {pos, runValuesSize}, stringBufferFactory);
          std::vector<std::string_view> runValuesBuffer(runLengthsCount);
          runValues->materialize(runLengthsCount, runValuesBuffer.data());

          for (uint32_t i = 0; i < runLengthsCount; ++i) {
            result += runLengthsBuffer[i] * runValuesBuffer[i].size();
          }
          break;
        }
        default:
          NIMBLE_UNSUPPORTED("Encoding type does not support strings.");
      }
      return result;
    }
  }
}
} // namespace facebook::nimble::test
