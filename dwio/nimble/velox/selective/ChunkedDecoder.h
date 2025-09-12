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
#include "dwio/nimble/velox/selective/NimbleData.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::nimble {

class ChunkedDecoderTestHelper;

class ChunkedDecoder {
 public:
  ChunkedDecoder(
      std::unique_ptr<velox::dwio::common::SeekableInputStream> input,
      velox::memory::MemoryPool& memoryPool,
      bool decodeValuesWithNulls)
      : input_{std::move(input)},
        memoryPool_{memoryPool},
        decodeValuesWithNulls_{decodeValuesWithNulls} {
    VELOX_CHECK_NOT_NULL(input_);
  }

  /// Skip non null values.
  void skip(int64_t numValues);

  /// Decode nulls or in-map buffer values.
  void nextBools(uint64_t* data, int64_t count, const uint64_t* incomingNulls);

  /// Decode index type values, which usually needed to be read fully.
  void nextIndices(int32_t* data, int64_t count, const uint64_t* incomingNulls);

  const velox::BufferPtr& getPreloadedValues() {
    VELOX_CHECK(
        decodeValuesWithNulls_,
        "Only Array and Map types preload offset values with nulls");
    return nullableValues_;
  }

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor) {
    const velox::RowSet rows(visitor.rows(), visitor.numRows());
    VELOX_DCHECK(std::is_sorted(rows.begin(), rows.end()));
    const auto numRows = visitor.numRows();
    ReadWithVisitorParams params{};
    bool resultNullsPrepared = false;
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
            nulls = velox::AlignedBuffer::copy(&memoryPool_, nulls);
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
              visitor.rowAt(numRows - 1) + 1,
              &memoryPool_,
              velox::bits::kNotNull);
        }
        return nulls->template asMutable<uint64_t>();
      };
      readWithVisitorImpl<false>(visitor, nullptr, params);
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
      VELOX_CHECK(loadNextChunk());
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
    VELOX_UNREACHABLE();
  }

  velox::vector_size_t lastNonNullIndex(
      const uint64_t* nulls,
      velox::vector_size_t begin,
      velox::vector_size_t end,
      velox::vector_size_t& numNulls,
      velox::vector_size_t& numNonNulls) {
    velox::vector_size_t index;
    bool allConsumed = velox::bits::testWords(
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
    numNulls = numNonNulls = 0;
    if constexpr (V::dense) {
      VELOX_DCHECK_EQ(visitor.currentRow(), params.numScanned);
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
    auto i = visitor.rowIndex();
    while (i < numRows && visitor.rowAt(i) < chunkEnd) {
      ++i;
    }
    return i;
  }

  template <bool kHasNulls, typename V>
  void readWithVisitorImpl(
      V& visitor,
      const uint64_t* nulls,
      ReadWithVisitorParams& params) {
    constexpr bool kExtractToReader = std::
        is_same_v<typename V::Extract, velox::dwio::common::ExtractToReader>;
    const auto numRows = visitor.numRows();
    while (visitor.rowIndex() < numRows) {
      if constexpr (!kHasNulls) {
        if (FOLLY_UNLIKELY(remainingValues_ == 0)) {
          VELOX_CHECK(loadNextChunk());
        }
      }
      velox::vector_size_t numNulls, numNonNulls;
      auto nextRowIndex = computeNextRowIndex<kHasNulls>(
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
        VELOX_DCHECK_EQ(visitor.rowIndex(), nextRowIndex);
      }
      remainingValues_ -= numNonNulls;
      params.numScanned += numNulls + numNonNulls;
    }
  }

  bool ensureInput(int size);

  // TODO: remove with row size estimate hacks.
  bool ensureInputIncremental_hack(int size, const char*& pos);

  bool loadNextChunk();

  void prepareInputBuffer(int size);

  // Used only for full scanning length data streams.
  void decodeNullable(
      uint64_t* nulls,
      uint32_t* data,
      int64_t count,
      const uint64_t* incomingNulls);

  std::unique_ptr<velox::dwio::common::SeekableInputStream> const input_;
  velox::memory::MemoryPool& memoryPool_;
  const bool decodeValuesWithNulls_;

  const char* inputData_ = nullptr;
  int64_t inputSize_ = 0;
  velox::BufferPtr inputBuffer_;
  velox::BufferPtr decompressed_;
  velox::BufferPtr nullableValues_;

  std::unique_ptr<Encoding> encoding_;
  int64_t remainingValues_ = 0;
  mutable std::optional<size_t> rowCountEstimate_{std::nullopt};
  mutable std::optional<size_t> stringDataSizeEstimate_{std::nullopt};

  friend class ChunkedDecoderTestHelper;
};

} // namespace facebook::nimble
