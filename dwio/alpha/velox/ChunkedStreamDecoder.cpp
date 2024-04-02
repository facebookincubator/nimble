// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/ChunkedStreamDecoder.h"
#include "dwio/alpha/encodings/EncodingFactoryNew.h"

namespace facebook::alpha {

uint32_t ChunkedStreamDecoder::next(
    uint32_t count,
    void* output,
    std::function<void*()> nulls,
    const bits::Bitmap* scatterBitmap) {
  if (count == 0) {
    if (nulls && scatterBitmap) {
      auto nullsPtr = nulls();
      // @lint-ignore CLANGTIDY facebook-hte-BadMemset
      memset(nullsPtr, 0, bits::bytesRequired(scatterBitmap->size()));
    }
    return 0;
  }

  LoggingScope scope{logger_};

  uint32_t nonNullCount = 0;
  bool hasNulls = false;
  uint32_t offset = 0;
  void* nullsPtr = nullptr;
  std::function<void*()> initNulls = [&]() {
    if (!nullsPtr) {
      nullsPtr = nulls();
    }
    return nullsPtr;
  };

  while (count > 0) {
    ensureLoaded();

    auto rowsToRead = std::min(count, remaining_);
    uint32_t chunkNonNullCount = 0;
    uint32_t endOffset = 0;

    if (!nulls || !scatterBitmap) {
      ALPHA_CHECK(!scatterBitmap, "unexpected scatter bitmap");
      chunkNonNullCount = encoding_->materializeNullable(
          rowsToRead, output, initNulls, nullptr, offset);
      endOffset = offset + rowsToRead;
    } else {
      endOffset = bits::findSetBit(
          static_cast<const char*>(scatterBitmap->bits()),
          offset,
          scatterBitmap->size(),
          rowsToRead + 1);
      bits::Bitmap localBitmap{scatterBitmap->bits(), endOffset};
      chunkNonNullCount = encoding_->materializeNullable(
          rowsToRead, output, initNulls, &localBitmap, offset);
    }

    auto chunkHasNulls = chunkNonNullCount != (endOffset - offset);
    if (chunkHasNulls && !hasNulls) {
      // back fill the nulls bitmap to all non-nulls
      bits::BitmapBuilder builder{nullsPtr, offset};
      builder.set(0, offset);
    }
    hasNulls = hasNulls || chunkHasNulls;
    if (hasNulls && !chunkHasNulls) {
      // fill nulls bitmap to reflect that all values are non-null
      bits::BitmapBuilder builder{nullsPtr, endOffset};
      builder.set(offset, endOffset);
    }

    offset = endOffset;
    remaining_ -= rowsToRead;
    count -= rowsToRead;
    nonNullCount += chunkNonNullCount;
  }

  return nonNullCount;
}

void ChunkedStreamDecoder::skip(uint32_t count) {
  while (count > 0) {
    ensureLoaded();
    auto toSkip = std::min(count, remaining_);
    encoding_->skip(toSkip);
    count -= toSkip;
    remaining_ -= toSkip;
  }
}

void ChunkedStreamDecoder::reset() {
  stream_->reset();
  remaining_ = 0;
}

void ChunkedStreamDecoder::ensureLoaded() {
  if (UNLIKELY(remaining_ == 0)) {
    encoding_ = EncodingFactory::decode(pool_, stream_->nextChunk());
    remaining_ = encoding_->rowCount();
    ALPHA_ASSERT(remaining_ > 0, "Empty chunk");
  }
}

} // namespace facebook::alpha
