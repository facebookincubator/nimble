// Copyright 2004-present Facebook. All Rights Reserved.

#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"

namespace facebook::nimble {

EncodingType Encoding::encodingType() const {
  return static_cast<EncodingType>(data_[kEncodingTypeOffset]);
}

DataType Encoding::dataType() const {
  return static_cast<DataType>(data_[kDataTypeOffset]);
}

uint32_t Encoding::rowCount() const {
  return *reinterpret_cast<const uint32_t*>(data_.data() + kRowCountOffset);
}

/* static */ void Encoding::copyIOBuf(char* pos, const folly::IOBuf& buf) {
  [[maybe_unused]] size_t length = buf.computeChainDataLength();
  for (auto data : buf) {
    std::copy(data.cbegin(), data.cend(), pos);
    pos += data.size();
    length -= data.size();
  }
  NIMBLE_DASSERT(length == 0, "IOBuf chain length corruption");
}

void Encoding::serializePrefix(
    EncodingType encodingType,
    DataType dataType,
    uint32_t rowCount,
    char*& pos) {
  encoding::writeChar(static_cast<char>(encodingType), pos);
  encoding::writeChar(static_cast<char>(dataType), pos);
  encoding::writeUint32(rowCount, pos);
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
