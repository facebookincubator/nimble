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

#include "dwio/nimble/encodings/EncodingUtils.h"
#include "dwio/nimble/index/StripeIndexGroup.h"
#include "dwio/nimble/velox/selective/NimbleData.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::nimble {

class ChunkedDecoderTestHelper;

class ChunkedDecoder {
 public:
  ChunkedDecoder(
      std::unique_ptr<velox::dwio::common::SeekableInputStream> input,
      bool decodeValuesWithNulls,
      std::shared_ptr<index::StreamIndex> streamIndex,
      velox::memory::MemoryPool* pool,
      std::function<std::unique_ptr<Encoding>(
          velox::memory::MemoryPool&,
          std::string_view,
          std::function<void*(uint32_t)>)> encodingFactory =
          [](velox::memory::MemoryPool& pool,
             std::string_view data,
             std::function<void*(uint32_t)> stringBufferFactory)
          -> std::unique_ptr<Encoding> {
        return EncodingFactory::decode(pool, data, stringBufferFactory);
      })
      : input_{std::move(input)},
        pool_{pool},
        decodeValuesWithNulls_{decodeValuesWithNulls},
        encodingFactory_{std::move(encodingFactory)},
        streamIndex_{std::move(streamIndex)},
        streamRowCount_{
            streamIndex_ ? std::optional<uint32_t>(streamIndex_->rowCount())
                         : std::nullopt} {
    NIMBLE_CHECK_NOT_NULL(input_);
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
    params.prepareResultNulls = [&] {
      if (FOLLY_UNLIKELY(!resultNullsPrepared)) {
        // Allocate one extra byte for nulls to handle multi-chunk reading,
        // where each chunk we need to align the result nulls to byte boundary
        // then shift.
        visitor.reader().prepareNulls(rows, true, 8);
        resultNullsPrepared = true;
      }
    };
    params.initReturnReaderNulls = [&] {
      visitor.reader().initReturnReaderNulls(rows);
    };
    if (auto& nulls = visitor.reader().nullsInReadRange()) {
      if (static_cast<const NimbleData&>(visitor.reader().formatData())
              .inMap() == nulls->template as<uint64_t>()) {
        params.makeReaderNulls = [&, copied = false]() mutable {
          if (FOLLY_UNLIKELY(!copied)) {
            // Make a copy to avoid value nulls overwriting in-map buffer.
            nulls = velox::AlignedBuffer::copy(pool_, nulls);
            copied = true;
          }
          return nulls->template asMutable<uint64_t>();
        };
      } else {
        params.makeReaderNulls = [&nulls] {
          return nulls->template asMutable<uint64_t>();
        };
      }
      readWithVisitorImpl<true>(
          visitor, nulls->template as<uint64_t>(), params);
    } else {
      params.makeReaderNulls = [this, numRows, &visitor] {
        auto& nulls = visitor.reader().nullsInReadRange();
        if (FOLLY_UNLIKELY(!nulls)) {
          nulls = velox::AlignedBuffer::allocate<bool>(
              visitor.rowAt(numRows - 1) + 1, pool_, velox::bits::kNotNull);
        }
        return nulls->template asMutable<uint64_t>();
      };
      readWithVisitorImpl<false>(visitor, nullptr, params);
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

  // A temporary solution to estimate row size before column statistics are
  // implemented.  This is based on the first chunk so is only accurate when
  // there is a single chunk in the stripe, or all the first chunks of all
  // streams are aligned at the same row count (unlikely).
  //
  // TODO: Remove after column statistics are added.
  std::optional<size_t> estimateRowCount() const;

  // A temporary solution to estimate row size, similar to estimateRowCount().
  //
  // TODO: Remove after column statistics are added.
  std::optional<size_t> estimateStringDataSize() const;

 private:
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

  template <bool kHasNulls, typename V>
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
      const auto nextRowIndex = computeNextRowIndex<kHasNulls>(
          visitor, nulls, params, numRows, numNulls, numNonNulls);
      VLOG(1) << visitor.reader().fileType().fullName() << ":"
              << " rowIndex=" << visitor.rowIndex()
              << " nextRowIndex=" << nextRowIndex << " numNulls=" << numNulls
              << " numNonNulls=" << numNonNulls
              << " remainingValues_=" << remainingValues_;
      if (visitor.rowIndex() < nextRowIndex) {
        visitor.setRows(velox::RowSet(visitor.rows(), nextRowIndex));
        if (numNonNulls > 0) {
          callReadWithVisitor(*encoding_, visitor, params);
        } else if (!visitor.allowNulls()) {
          visitor.setRowIndex(nextRowIndex);
        } else {
          if constexpr (kExtractToReader) {
            params.prepareResultNulls();
            if (visitor.reader().returnReaderNulls()) {
              auto numValues = nextRowIndex - visitor.rowIndex();
              visitor.setRowIndex(nextRowIndex);
              visitor.addNumValues(numValues);
              goto updateRemainingValues;
            }
          }
          bool atEnd = false;
          while (!atEnd) {
            visitor.processNull(atEnd);
          }
        }
      updateRemainingValues:
        NIMBLE_DCHECK_EQ(visitor.rowIndex(), nextRowIndex);
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

    if (visitor.reader().formatData().getStringBuffersFromDecoder()) {
      visitor.reader().setStringBuffers(std::move(stringBuffers));
    }
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

  void loadNextChunk();

  void prepareInputBuffer(int32_t size);

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
  const std::function<std::unique_ptr<Encoding>(
      velox::memory::MemoryPool&,
      std::string_view,
      std::function<void*(uint32_t)>)>
      encodingFactory_;
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

  friend class ChunkedDecoderTestHelper;
};

} // namespace facebook::nimble
