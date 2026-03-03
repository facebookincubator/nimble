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
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Varint.h"

namespace facebook::nimble {

Encoding::Encoding(
    velox::memory::MemoryPool& pool,
    std::string_view data,
    const Options& options)
    : pool_{&pool},
      data_{data},
      encodingType_{data_[kEncodingTypeOffset]},
      options_{options},
      dataType_{readDataType(data)},
      rowCount_{readRowCount(data, options_.useVarintRowCount)},
      prefixSize_{readPrefixSize(data, options_.useVarintRowCount)} {}

/* static */ DataType Encoding::readDataType(std::string_view data) {
  return static_cast<DataType>(data[kDataTypeOffset]);
}

/* static */ uint32_t Encoding::readRowCount(
    std::string_view data,
    bool useVarint) {
  if (useVarint) {
    const char* pos = data.data() + kRowCountOffset;
    return varint::readVarint32(&pos);
  }
  return *reinterpret_cast<const uint32_t*>(data.data() + kRowCountOffset);
}

/* static */ uint32_t Encoding::readPrefixSize(
    std::string_view data,
    bool useVarint) {
  if (useVarint) {
    const char* pos = data.data() + kRowCountOffset;
    varint::readVarint32(&pos);
    return pos - data.data();
  }
  return kPrefixSize;
}

/* static */ void Encoding::copyIOBuf(char* pos, const folly::IOBuf& buf) {
  [[maybe_unused]] size_t length = buf.computeChainDataLength();
  for (auto data : buf) {
    std::copy(data.cbegin(), data.cend(), pos);
    pos += data.size();
    length -= data.size();
  }
  NIMBLE_DCHECK_EQ(length, 0, "IOBuf chain length corruption");
}

void Encoding::serializePrefix(
    EncodingType encodingType,
    DataType dataType,
    uint32_t rowCount,
    bool useVarint,
    char*& pos) {
  encoding::writeChar(static_cast<char>(encodingType), pos);
  encoding::writeChar(static_cast<char>(dataType), pos);
  if (useVarint) {
    varint::writeVarint(rowCount, &pos);
  } else {
    encoding::writeUint32(rowCount, pos);
  }
}

/* static */ uint32_t Encoding::serializePrefixSize(
    uint32_t rowCount,
    bool useVarint) {
  if (!useVarint) {
    return kPrefixSize;
  }
  return kRowCountOffset + varint::varintSize(rowCount);
}

std::string Encoding::debugString(int offset) const {
  return fmt::format(
      "{}{}<{}> rowCount={}",
      std::string(offset, ' '),
      toString(encodingType()),
      toString(dataType()),
      rowCount());
}

} // namespace facebook::nimble
