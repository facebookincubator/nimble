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

#pragma once

#include "dwio/nimble/common/ChunkHeader.h"
#include "dwio/nimble/encodings/DictionaryEncoding.h"
#include "dwio/nimble/encodings/MainlyConstantEncoding.h"
#include "dwio/nimble/encodings/NullableEncoding.h"
#include "dwio/nimble/encodings/common/EncodingUtils.h"
#include "dwio/nimble/encodings/legacy/EncodingTrait.h"
#include "dwio/nimble/index/ChunkIndexGroup.h"
#include "dwio/nimble/velox/selective/NimbleData.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::nimble {

class ChunkedDecoderTestHelper;

class ChunkedDecoder {
 public:
  ChunkedDecoder(
      std::unique_ptr<velox::dwio::common::SeekableInputStream> input,
      std::shared_ptr<index::StreamIndex> streamIndex,
      bool decodeValuesWithNulls,
      const EncodingFactory* encodingFactory,
      velox::memory::MemoryPool* pool,
      bool stringDecoderZeroCopy = false)
      : input_{std::move(input)},
        pool_{pool},
        decodeValuesWithNulls_{decodeValuesWithNulls},
        encodingFactory_(encodingFactory),
        stringDecoderZeroCopy_{stringDecoderZeroCopy},
        streamIndex_{std::move(streamIndex)},
        streamRowCount_{
            streamIndex_ ? std::optional<uint32_t>(streamIndex_->rowCount())
                         : std::nullopt} {
    NIMBLE_CHECK_NOT_NULL(input_);
    NIMBLE_CHECK_NOT_NULL(encodingFactory_);
  }

  /// Registers a callback invoked whenever loadNextChunk() loads a new chunk.
  /// Used by StringColumnReader to invalidate dictionary state that is tied
  /// to the current chunk's encoding and string buffers.
  void setOnChunkLoad(std::function<void()> callback) {
    onChunkLoad_ = std::move(callback);
  }

  /// Skip non null values.
  void skip(int64_t numValues);

  /// Skip non null values using index-based acceleration.
  void skipWithIndex(int64_t numValues);

  /// Skip non null values using sequential skipping.
  void skipWithoutIndex(int64_t numValues);

  /// Decode nulls or in-map buffer values.
  void nextBools(uint64_t* data, int64_t count, const uint64_t* incomingNulls);

  /// Decode index type values, which usually needed to be read fully.
  void nextIndices(int32_t* data, int64_t count, const uint64_t* incomingNulls);

  const velox::BufferPtr& getPreloadedValues() {
    NIMBLE_CHECK(
        decodeValuesWithNulls_,
        "Only Array and Map types preload offset values with nulls");
    return nullableValues_;
  }

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor) {
    const velox::RowSet rows(visitor.rows(), visitor.numRows());
    NIMBLE_DCHECK(std::is_sorted(rows.begin(), rows.end()));
    const auto numRows = visitor.numRows();
    ReadWithVisitorParams params{};
    bool resultNullsPrepared{false};
    // Allocate one extra byte (8 bits) for nulls to handle multi-chunk
    // reading, where each chunk we need to align the result nulls to byte
    // boundary then shift.
    params.prepareResultNulls = [&resultNullsPrepared, &visitor, &rows] {
      if (FOLLY_UNLIKELY(!resultNullsPrepared)) {
        visitor.reader().prepareNulls(
            rows, /*mayResize=*/true, /*extraBits=*/8);
        resultNullsPrepared = true;
      }
    };
    params.initReturnReaderNulls = [&visitor, &rows] {
      visitor.reader().initReturnReaderNulls(rows);
    };
    bool readerNullsMade{false};
    if (auto& nulls = visitor.reader().nullsInReadRange()) {
      // For flat map child columns, NimbleData::readNulls() may alias inMap_
      // and nullsInReadRange_ to the same buffer (when the child has no
      // separate nulls stream). Since makeReaderNulls() returns a mutable
      // pointer that NullableEncoding will overwrite with value-level nulls,
      // we must copy-on-first-write to avoid corrupting the in-map bitmap.
      if (static_cast<const NimbleData&>(visitor.reader().formatData())
              .inMap() == nulls->template as<uint64_t>()) {
        params.makeReaderNulls =
            [&readerNullsMade, &nulls, pool = pool_]() mutable {
              if (FOLLY_UNLIKELY(!readerNullsMade)) {
                nulls = velox::AlignedBuffer::copy(pool, nulls);
                readerNullsMade = true;
              }
              return nulls->template asMutable<uint64_t>();
            };
      } else {
        params.makeReaderNulls = [&readerNullsMade, &nulls] {
          readerNullsMade = true;
          return nulls->template asMutable<uint64_t>();
        };
      }
      dispatchReadWithVisitorImpl<true>(
          visitor, nulls->template as<uint64_t>(), params);
    } else {
      params.makeReaderNulls = [&readerNullsMade, this, numRows, &visitor] {
        if (FOLLY_UNLIKELY(!readerNullsMade)) {
          auto& nulls = visitor.reader().nullsInReadRange();
          if (!nulls) {
            nulls = velox::AlignedBuffer::allocate<bool>(
                visitor.rowAt(numRows - 1) + 1,
                pool_,
                /*value=*/velox::bits::kNotNull);
          }
          readerNullsMade = true;
        }
        return visitor.reader()
            .nullsInReadRange()
            ->template asMutable<uint64_t>();
      };
      dispatchReadWithVisitorImpl<false>(visitor, /*nulls=*/nullptr, params);
    }
  }

  /// Decode values from a nullable encoding.
  /// Template on data type T to support int64_t, uint32_t, uint16_t, etc.
  /// Populates nulls buffer and data buffer.
  template <typename T>
  void decodeNullable(
      uint64_t* nulls,
      T* data,
      int64_t count,
      const uint64_t* incomingNulls) {
    const int64_t totalNumValues = !incomingNulls
        ? count
        : velox::bits::countNonNulls(incomingNulls, 0, count);

    if (incomingNulls != nullptr) {
      if (totalNumValues == 0) {
        if (nulls) {
          velox::bits::fillBits(nulls, 0, count, velox::bits::kNull);
        }
        return;
      }

      velox::bits::Bitmap scatterBitmap{
          incomingNulls, static_cast<uint32_t>(count)};
      // These are subranges in the scatter bit map for each materialize
      // call.
      uint32_t offset{0};
      uint32_t endOffset{0};
      for (int64_t i = 0; i < totalNumValues;) {
        if (FOLLY_UNLIKELY(remainingValues_ == 0)) {
          loadNextChunk();
        }

        const auto numValues = std::min(totalNumValues - i, remainingValues_);
        endOffset = velox::bits::findSetBit(
            static_cast<const char*>(scatterBitmap.bits()),
            offset,
            scatterBitmap.size(),
            numValues + 1);
        velox::bits::Bitmap localBitmap{scatterBitmap.bits(), endOffset};
        auto nonNullCount = encoding_->materializeNullable(
            numValues, data, [&]() { return nulls; }, &localBitmap, offset);
        if (nulls && nonNullCount == endOffset - offset) {
          velox::bits::fillBits(
              nulls, offset, endOffset, velox::bits::kNotNull);
        }
        advancePosition(numValues);
        i += numValues;
        offset = endOffset;
      }
    } else {
      for (int64_t i = 0; i < totalNumValues;) {
        if (FOLLY_UNLIKELY(remainingValues_ == 0)) {
          loadNextChunk();
        }

        const auto numValues = std::min(totalNumValues - i, remainingValues_);
        const auto nonNullCount = encoding_->materializeNullable(
            numValues, data, [&]() { return nulls; }, nullptr, i);
        if (nulls && nonNullCount == numValues) {
          velox::bits::fillBits(nulls, i, i + numValues, velox::bits::kNotNull);
        }
        advancePosition(numValues);
        i += numValues;
      }
    }
  }

  /// A temporary solution to estimate row size before column statistics are
  /// implemented.  This is based on the first chunk so is only accurate when
  /// there is a single chunk in the stripe, or all the first chunks of all
  /// streams are aligned at the same row count (unlikely).
  ///
  /// TODO: Remove after column statistics are added.
  std::optional<size_t> estimateRowCount() const;

  /// A temporary solution to estimate row size, similar to estimateRowCount().
  ///
  /// TODO: Remove after column statistics are added.
  std::optional<size_t> estimateStringDataSize() const;

  /// Ensures a chunk with remaining values is loaded. Loads the first chunk
  /// if none has been loaded yet, or advances to the next chunk if the current
  /// one is fully consumed. Called before inspecting the current encoding
  /// (e.g., currentEncoding, dictionaryConvertible) so that the caller sees
  /// the encoding and remainingValues of the chunk that will actually be read.
  void ensureLoaded(bool preserveDictionaryEncoding = false) {
    if (encoding_ != nullptr && remainingValues_ != 0) {
      return;
    }

    if (hasMoreChunks()) {
      loadNextChunk(preserveDictionaryEncoding);
    }
  }

  /// Returns the current encoding, or nullptr if no chunk has been loaded yet.
  const Encoding* currentEncoding() const {
    return encoding_.get();
  }

  /// Returns true if the current chunk's encoding supports dictionary index
  /// reading. Delegates to Encoding::dictionaryEnabled() which handles Dict,
  /// Nullable→Dict, and MC→Dict via virtual dispatch.
  /// Caller must call ensureLoaded() first.
  bool dictionaryConvertible() const;

  int64_t remainingValues() const {
    return remainingValues_;
  }

  /// Reads dictionary indices from the current chunk's encoding.
  /// Non-legacy encodings only: requires stringDecoderZeroCopy_ == true.
  template <typename DictionaryVisitor>
  void readDictionaryIndices(DictionaryVisitor& visitor) {
    // Dictionary indices reading requires the non-legacy encoding path.
    // callReadIndicesWithVisitor static_casts to DictionaryEncoding /
    // NullableEncoding / MainlyConstantEncoding which only exist in the
    // non-legacy path (stringDecoderZeroCopy_ == true).
    NIMBLE_CHECK(
        stringDecoderZeroCopy_,
        "Dictionary indices reading requires non-legacy encoding path");
    const velox::RowSet rows(visitor.rows(), visitor.numRows());
    NIMBLE_DCHECK(std::is_sorted(rows.begin(), rows.end()));
    ReadWithVisitorParams params{};
    bool resultNullsPrepared{false};
    // Allocate one extra byte (8 bits) for nulls to handle multi-chunk
    // reading, where each chunk we need to align the result nulls to byte
    // boundary then shift.
    params.prepareResultNulls = [&resultNullsPrepared, &visitor, &rows] {
      if (FOLLY_UNLIKELY(!resultNullsPrepared)) {
        visitor.reader().prepareNulls(
            rows, /*mayResize=*/true, /*extraBits=*/8);
        resultNullsPrepared = true;
      }
    };
    params.initReturnReaderNulls = [&visitor, &rows] {
      visitor.reader().initReturnReaderNulls(rows);
    };
    bool readerNullsMade{false};
    if (auto& nulls = visitor.reader().nullsInReadRange()) {
      // For flat map child columns, NimbleData::readNulls() may alias inMap_
      // and nullsInReadRange_ to the same buffer (when the child has no
      // separate nulls stream). Since makeReaderNulls() returns a mutable
      // pointer that NullableEncoding will overwrite with value-level nulls,
      // we must copy-on-first-write to avoid corrupting the in-map bitmap.
      if (static_cast<const NimbleData&>(visitor.reader().formatData())
              .inMap() == nulls->template as<uint64_t>()) {
        params.makeReaderNulls =
            [&readerNullsMade, &nulls, pool = pool_]() mutable {
              if (FOLLY_UNLIKELY(!readerNullsMade)) {
                nulls = velox::AlignedBuffer::copy(pool, nulls);
                readerNullsMade = true;
              }
              return nulls->template asMutable<uint64_t>();
            };
      } else {
        params.makeReaderNulls = [&readerNullsMade, &nulls] {
          readerNullsMade = true;
          return nulls->template asMutable<uint64_t>();
        };
      }
      readDictionaryIndicesImpl<true>(
          visitor, nulls->template as<uint64_t>(), params);
    } else {
      params.makeReaderNulls = [&readerNullsMade, this, &visitor] {
        if (FOLLY_UNLIKELY(!readerNullsMade)) {
          auto& nulls = visitor.reader().nullsInReadRange();
          if (!nulls) {
            nulls = velox::AlignedBuffer::allocate<bool>(
                visitor.rowAt(visitor.numRows() - 1) + 1,
                pool_,
                /*value=*/velox::bits::kNotNull);
          }
          readerNullsMade = true;
        }
        return visitor.reader()
            .nullsInReadRange()
            ->template asMutable<uint64_t>();
      };
      readDictionaryIndicesImpl<false>(visitor, /*nulls=*/nullptr, params);
    }
  }

 private:
  // For regular cases, kHasNulls is false because NullableEncoding handles
  // the nulls from the outside. But when dealing with flatmap types, inMap
  // streams are not part of the data stream but interact with the value
  // streams just like nulls. Not using this parameter causes a bug for
  // flatmap data shapes.
  template <bool kHasNulls, typename V>
  void readDictionaryIndicesImpl(
      V& visitor,
      const uint64_t* nulls,
      ReadWithVisitorParams& params) {
    constexpr bool kExtractToReader = std::
        is_same_v<typename V::Extract, velox::dwio::common::ExtractToReader>;
    NIMBLE_CHECK_GT(remainingValues_, 0);
    params.numScanned = 0;
    const auto numRows = visitor.numRows();
    velox::vector_size_t numNulls{0};
    velox::vector_size_t numNonNulls{0};
    const auto endRowIndex = computeNextRowIndex<kHasNulls>(
        visitor, nulls, params, numRows, numNulls, numNonNulls);
    if (visitor.rowIndex() < endRowIndex) {
      visitor.setRows(velox::RowSet(visitor.rows(), endRowIndex));
      if (numNonNulls > 0) {
        callReadIndicesWithVisitor(*encoding_, visitor, params);
      } else if (!visitor.allowNulls()) {
        visitor.setRowIndex(endRowIndex);
      } else {
        bool emitNullsRowByRow = true;
        if constexpr (kExtractToReader) {
          params.prepareResultNulls();
          if (visitor.reader().returnReaderNulls()) {
            auto numValues = endRowIndex - visitor.rowIndex();
            visitor.setRowIndex(endRowIndex);
            visitor.addNumValues(numValues);
            emitNullsRowByRow = false;
          }
        }
        if (emitNullsRowByRow) {
          bool atEnd = false;
          while (!atEnd) {
            visitor.processNull(atEnd);
          }
        }
      }
    }
    advancePosition(numNonNulls);
    params.numScanned += numNulls + numNonNulls;
    if (stringDecoderZeroCopy_) {
      visitor.reader().setStringBuffers(
          {currentStringBuffers_.begin(), currentStringBuffers_.end()});
    }
  }

  template <bool kHasMask>
  bool testNulls(
      const uint64_t* nulls,
      int32_t i,
      uint64_t mask,
      velox::vector_size_t& numNulls,
      velox::vector_size_t& numNonNulls,
      velox::vector_size_t& outIndex) {
    auto word = nulls[i];
    if constexpr (kHasMask) {
      word &= mask;
    }
    auto c = __builtin_popcountll(word);
    if (FOLLY_UNLIKELY(c > 0 && remainingValues_ == 0)) {
      loadNextChunk();
    }
    if (c == 0 || numNonNulls + c < remainingValues_) {
      numNulls += (kHasMask ? __builtin_popcountll(mask) : 64) - c;
      numNonNulls += c;
      return true;
    }
    for (int j = 0; j < 64; ++j) {
      if constexpr (kHasMask) {
        if (!(mask & (1ull << j))) {
          continue;
        }
      }
      if (word & (1ull << j)) {
        if (++numNonNulls == remainingValues_) {
          outIndex = i * 64 + j;
          return false;
        }
      } else {
        ++numNulls;
      }
    }
    NIMBLE_UNREACHABLE();
  }

  velox::vector_size_t lastNonNullIndex(
      const uint64_t* nulls,
      velox::vector_size_t begin,
      velox::vector_size_t end,
      velox::vector_size_t& numNulls,
      velox::vector_size_t& numNonNulls) {
    velox::vector_size_t index;
    const bool allConsumed = velox::bits::testWords(
        begin,
        end,
        [&](int32_t i, uint64_t mask) {
          return testNulls<true>(nulls, i, mask, numNulls, numNonNulls, index);
        },
        [&](int32_t i) {
          return testNulls<false>(nulls, i, 0, numNulls, numNonNulls, index);
        });
    return allConsumed ? end - 1 : index;
  }

  template <bool kHasNulls, typename V>
  velox::vector_size_t computeNextRowIndex(
      const V& visitor,
      const uint64_t* nulls,
      const ReadWithVisitorParams& params,
      velox::vector_size_t numRows,
      velox::vector_size_t& numNulls,
      velox::vector_size_t& numNonNulls) {
    numNulls = 0;
    numNonNulls = 0;
    if constexpr (V::dense) {
      NIMBLE_DCHECK_EQ(visitor.currentRow(), params.numScanned);
      if constexpr (!kHasNulls) {
        numNonNulls =
            std::min<int64_t>(numRows - visitor.rowIndex(), remainingValues_);
        return visitor.rowIndex() + numNonNulls;
      }
      return 1 +
          lastNonNullIndex(
                 nulls, params.numScanned, numRows, numNulls, numNonNulls);
    }
    auto end = visitor.rowAt(numRows - 1) + 1;
    velox::vector_size_t chunkEnd;
    if constexpr (kHasNulls) {
      chunkEnd = 1 +
          lastNonNullIndex(
                     nulls, params.numScanned, end, numNulls, numNonNulls);
    } else {
      chunkEnd = params.numScanned + remainingValues_;
      numNonNulls =
          std::min<int64_t>(end - params.numScanned, remainingValues_);
    }

    return std::lower_bound(
               visitor.rows() + visitor.rowIndex(),
               visitor.rows() + numRows,
               chunkEnd) -
        visitor.rows();
  }

  /// Selects the encoding trait based on stringDecoderZeroCopy_ and
  /// forwards to the fully-monomorphic readWithVisitorImpl.
  template <bool kHasNulls, typename V>
  void dispatchReadWithVisitorImpl(
      V& visitor,
      const uint64_t* nulls,
      ReadWithVisitorParams& params) {
    if (stringDecoderZeroCopy_) {
      readWithVisitorImpl<kHasNulls, DefaultEncodingTrait>(
          visitor, nulls, params);
    } else {
      readWithVisitorImpl<kHasNulls, legacy::LegacyEncodingTrait>(
          visitor, nulls, params);
    }
  }

  template <bool kHasNulls, typename EncodingTrait, typename V>
  void readWithVisitorImpl(
      V& visitor,
      const uint64_t* nulls,
      ReadWithVisitorParams& params) {
    constexpr bool kExtractToReader = std::
        is_same_v<typename V::Extract, velox::dwio::common::ExtractToReader>;
    const auto numRows = visitor.numRows();

    std::vector<velox::BufferPtr> stringBuffers;
    while (visitor.rowIndex() < numRows) {
      if constexpr (!kHasNulls) {
        if (FOLLY_UNLIKELY(remainingValues_ == 0)) {
          loadNextChunk();
        }
      }
      velox::vector_size_t numNulls;
      velox::vector_size_t numNonNulls;
      const auto endRowIndex = computeNextRowIndex<kHasNulls>(
          visitor, nulls, params, numRows, numNulls, numNonNulls);
      VLOG(1) << visitor.reader().fileType().fullName() << ":"
              << " rowIndex=" << visitor.rowIndex()
              << " endRowIndex=" << endRowIndex << " numNulls=" << numNulls
              << " numNonNulls=" << numNonNulls
              << " remainingValues_=" << remainingValues_;
      if (visitor.rowIndex() < endRowIndex) {
        visitor.setRows(velox::RowSet(visitor.rows(), endRowIndex));
        if (numNonNulls > 0) {
          EncodingTrait::callReadWithVisitor(*encoding_, visitor, params);
        } else if (!visitor.allowNulls()) {
          visitor.setRowIndex(endRowIndex);
        } else {
          bool emitNullsRowByRow = true;
          if constexpr (kExtractToReader) {
            params.prepareResultNulls();
            if (visitor.reader().returnReaderNulls()) {
              auto numValues = endRowIndex - visitor.rowIndex();
              visitor.setRowIndex(endRowIndex);
              visitor.addNumValues(numValues);
              emitNullsRowByRow = false;
            }
          }
          if (emitNullsRowByRow) {
            bool atEnd = false;
            while (!atEnd) {
              visitor.processNull(atEnd);
            }
          }
        }
        NIMBLE_CHECK_EQ(visitor.rowIndex(), endRowIndex);
      }
      advancePosition(numNonNulls);
      params.numScanned += numNulls + numNonNulls;

      // Note: we can over queue the current string buffers by exactly
      // once, but that doesn't change the life cycle of the buffers for now.
      // Keeping this pattern for simplicity.
      // For non-string types, currentStringBuffers_ will be empty
      for (const auto& buf : currentStringBuffers_) {
        stringBuffers.push_back(buf);
      }
    }

    if (visitor.reader().formatData().stringDecoderZeroCopy()) {
      visitor.reader().setStringBuffers(std::move(stringBuffers));
    }
  }

  // Returns true if the input stream has at least one more chunk header
  // available, indicating that loadNextChunk() can be called.
  bool hasMoreChunks() {
    return ensureInput(kChunkHeaderSize);
  }

  // Ensures that the input buffer has at least 'size' bytes available for
  // reading. If the current buffer doesn't have enough data, this method will
  // read more data from the underlying input stream until the requirement is
  // met or the stream is exhausted.
  // Returns true if the input buffer has at least 'size' bytes available,
  // false if the end of stream is reached before meeting the requirement.
  bool ensureInput(int size);

  // TODO: remove with row size estimate hacks.
  bool ensureInputIncremental_hack(int size, const char*& pos);

  void invokeOnChunkLoad() {
    if (onChunkLoad_) {
      onChunkLoad_();
    }
  }

  void loadNextChunk(bool preserveDictionaryEncoding = false);

  void prepareInputBuffer(int32_t size);

  // Returns true if inputData_ points into inputBuffer_.
  bool fromInputBuffer() const {
    if (!inputBuffer_) {
      return false;
    }
    const char* bufStart = inputBuffer_->as<char>();
    return inputData_ >= bufStart &&
        inputData_ < bufStart + inputBuffer_->capacity();
  }

  // Seek to a specific chunk by offset.
  // Positions the decoder at the beginning of the chunk at the given offset.
  // The caller is responsible for updating rowPosition_ after this call.
  // @param offset The byte offset of the chunk in the stream
  void seekToChunk(uint32_t offset);

  // Advances the position by the given number of values.
  // Updates both remainingValues_ and currentRow_.
  inline void advancePosition(int64_t numValues) {
    remainingValues_ -= numValues;
    NIMBLE_CHECK_GE(remainingValues_, 0);
    rowPosition_ += numValues;
    NIMBLE_CHECK_LE(
        rowPosition_,
        streamRowCount_.value_or(std::numeric_limits<uint32_t>::max()));
  }

  const std::unique_ptr<velox::dwio::common::SeekableInputStream> input_;
  velox::memory::MemoryPool* const pool_;
  // When true, decode nullable values (for array/map length streams that
  // encode nulls alongside values). When false, decode values without nulls
  // (standard case for scalar types).
  const bool decodeValuesWithNulls_;
  const EncodingFactory* const encodingFactory_;
  const bool stringDecoderZeroCopy_{false};
  // Optional stream index for accelerating skip operations
  const std::shared_ptr<index::StreamIndex> streamIndex_;
  // Total row count in the stream, set from stream index if available.
  const std::optional<uint32_t> streamRowCount_;

  // Pointer to the current position in the input buffer.
  // Points to the next byte to be read from the stream.
  const char* inputData_{nullptr};
  // Number of bytes remaining in the input buffer starting from inputData_.
  int64_t inputSize_{0};
  velox::BufferPtr inputBuffer_;
  velox::BufferPtr decompressed_;
  velox::BufferPtr nullableValues_;
  std::vector<velox::BufferPtr> currentStringBuffers_;

  std::unique_ptr<Encoding> encoding_;
  int64_t remainingValues_{0};
  mutable std::optional<size_t> rowCountEstimate_{std::nullopt};
  mutable std::optional<size_t> stringDataSizeEstimate_{std::nullopt};

  // Tracks the current row position in the stream.
  uint32_t rowPosition_{0};

  // Callback invoked on chunk transitions. Used to clear cached state
  // (e.g., alphabet, dictionaryValues) in the owning column reader.
  std::function<void()> onChunkLoad_;

  friend class ChunkedDecoderTestHelper;
};

} // namespace facebook::nimble
