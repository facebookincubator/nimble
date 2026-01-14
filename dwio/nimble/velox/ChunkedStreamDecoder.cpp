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
#include "dwio/nimble/velox/ChunkedStreamDecoder.h"

#include "velox/buffer/Buffer.h"

namespace facebook::nimble {
uint32_t ChunkedStreamDecoder::next(
    uint32_t count,
    void* output,
    std::vector<velox::BufferPtr>& stringBuffers,
    std::function<void*()> nulls,
    const bits::Bitmap* scatterBitmap) {
  NIMBLE_DCHECK(stringBuffers.empty());

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
      NIMBLE_CHECK(!scatterBitmap, "unexpected scatter bitmap");
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

    // Note: we can over queue the current string buffers by exactly
    // once, but that doesn't change the life cycle of the buffers for now.
    // Keeping this pattern for simplicity.
    // For non-string types, currentStringBuffers_ will be empty
    stringBuffers.reserve(currentStringBuffers_.size());
    for (const auto& buf : currentStringBuffers_) {
      stringBuffers.push_back(buf);
    }
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
    currentStringBuffers_.clear();
    encoding_ = encodingFactory_(
        pool_, stream_->nextChunk(), [&](uint32_t totalLength) {
          auto& buffer = currentStringBuffers_.emplace_back(
              velox::AlignedBuffer::allocate<char>(totalLength, &pool_));
          return buffer->asMutable<void>();
        });
    remaining_ = encoding_->rowCount();
    NIMBLE_CHECK_GT(remaining_, 0, "Empty chunk");
  }
}

} // namespace facebook::nimble
