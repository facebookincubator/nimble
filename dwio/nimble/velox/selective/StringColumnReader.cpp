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

#include "dwio/nimble/velox/selective/StringColumnReader.h"

#include <algorithm>
#include "dwio/nimble/encodings/legacy/EncodingUtils.h"
#include "dwio/nimble/velox/selective/NimbleData.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/SelectiveColumnReaderInternal.h"
#include "velox/vector/DictionaryVector.h"

namespace facebook::nimble {

using namespace facebook::velox;

uint64_t StringColumnReader::skip(uint64_t numValues) {
  numValues = SelectiveColumnReader::skip(numValues);
  // Clear cached dictionary state when skip crosses a chunk boundary,
  // since the new chunk may have a different alphabet.
  decoder_.skip(numValues, [this] {
    clearDictionaryState();
    return true;
  });
  return numValues;
}

void StringColumnReader::ensureDictionaryState() {
  if (hasDictionaryState()) {
    return;
  }
  dictionaryState_.alphabet = buildEncodingDictionaryAlphabet<std::string_view>(
      decoder_.currentEncoding());
}

void StringColumnReader::updateDictionaryIndices(
    vector_size_t alphabetSize,
    vector_size_t valueOffset) {
  if (alphabetSize > 0) {
    auto* values = reinterpret_cast<int32_t*>(rawValues_);
    for (auto i = valueOffset; i < numValues_; ++i) {
      values[i] += alphabetSize;
    }
  }
}

bool StringColumnReader::tryExtendDictionaryAtChunkBoundary(
    int32_t& alphabetOffset,
    vector_size_t& valueOffset) {
  // Offset the indices written by the previous chunk so they reference the
  // merged alphabet instead of the per-chunk alphabet. numValues_ is
  // incremented by the visitor (via process() / processNull()) as
  // readDictionaryIndices reads each row. At this point, numValues_ reflects
  // all rows read so far across chunks, and [valueOffset, numValues_) are
  // the indices from the just-completed chunk that need offsetting.
  updateDictionaryIndices(alphabetOffset, valueOffset);
  valueOffset = numValues_;

  // Check if the new chunk's encoding supports dictionary index reading.
  // If not, return false to signal the caller to abandon the dict path.
  if (!decoder_.dictionaryConvertible()) {
    return false;
  }

  // Extend the merged alphabet with the new chunk's dictionary entries.
  // alphabetOffset tracks the cumulative size so that the next chunk's
  // indices can be offset correctly.
  alphabetOffset = static_cast<int32_t>(dictionaryState_.alphabet.size());
  auto chunkAlphabet = buildEncodingDictionaryAlphabet<std::string_view>(
      decoder_.currentEncoding());
  dictionaryState_.alphabet.insert(
      dictionaryState_.alphabet.end(),
      chunkAlphabet.begin(),
      chunkAlphabet.end());
  dictionaryState_.alphabetVector.reset();
  crossChunkRead_ = true;
  return true;
}

bool StringColumnReader::readWithDictionary(
    int64_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  // Dictionary path requires: no filter pushdown, no value hook, non-legacy
  // encoding path (zero-copy), session property enabled, and
  // dictionary-convertible encoding.
  if (scanSpec_->hasFilter() || scanSpec_->valueHook() ||
      !formatData().stringDecoderZeroCopy() ||
      !static_cast<const NimbleData&>(formatData())
           .nimblePreserveDictionaryEncoding()) {
    clearDictionaryState();
    return false;
  }

  // When the previous batch's readDictionaryIndices consumed the last row of
  // a chunk (remainingValues_ == 0), ensureLoaded loads the next chunk. The
  // callback clears stale dictionary state so ensureDictionaryState rebuilds
  // from the new encoding.
  decoder_.ensureLoaded(
      /*preserveDictionaryEncoding=*/true, [this] {
        clearDictionaryState();
        return true;
      });
  if (!decoder_.dictionaryConvertible()) {
    clearDictionaryState();
    return false;
  }

  prepareRead<int32_t>(offset, rows, incomingNulls);
  ensureDictionaryState();
  velox::common::testutil::TestValue::adjust(
      "facebook::nimble::StringColumnReader::readWithDictionary",
      &dictionaryState_.alphabet);
  const auto endReadRow = rows.back() + 1;
  // Set to true when the dictionary read is abandoned at a non-dict chunk;
  // false when the whole range is read in dictionary mode. On abandon,
  // readDictionaryIndices has already advanced readOffset_ to the decoder's
  // parked position (the chunk boundary), where the flat fallback resumes.
  bool abandonDictionary = false;

  // Read dictionary indices, potentially across multiple chunks. At each
  // chunk boundary, onChunkBoundary checks if the new chunk is also
  // dictionary-convertible:
  //   - Returns true: the new chunk's alphabet is appended to the merged
  //     alphabet and reading continues in dict mode.
  //   - Returns false: the new chunk is not dict-compatible, so
  //     readDictionaryIndices stops and the reactive flat fallback below
  //     takes over.
  dwio::common::StringColumnReadWithVisitorHelper<
      /*kEncodingHasNulls=*/true,
      /*kDictionary=*/false>(*this, rows)([&](auto visitor) {
    auto dictVisitor = visitor.toStringDictionaryColumnVisitor();

    int32_t alphabetOffset = 0;
    velox::vector_size_t valueOffset = numValues_;

    // At each chunk boundary, try to extend the merged alphabet with the
    // new chunk's dictionary. Returns true if the new chunk is
    // dict-compatible (alphabet extended, continue reading). Returns false
    // if the new chunk is not dict-compatible (stop, fall back to flat).
    auto onChunkBoundary = [&]() -> bool {
      return tryExtendDictionaryAtChunkBoundary(alphabetOffset, valueOffset);
    };

    // readDictionaryIndices returns false when the onChunkBoundary callback
    // returns false (new chunk is not dict-compatible), meaning the
    // dictionary path must be abandoned for the remaining rows.
    abandonDictionary =
        !decoder_.readDictionaryIndices(dictVisitor, onChunkBoundary);

    // Offset the final chunk's indices into the merged alphabet.
    updateDictionaryIndices(alphabetOffset, valueOffset);
  });

  if (!abandonDictionary) {
    readOffset_ += endReadRow;
    return true;
  }

  // Partial dict read: a non-dict chunk was encountered mid-read. Convert
  // the already-read dict indices to flat StringViews. readOffset_ already
  // points at the chunk boundary (set by readDictionaryIndices), so read()
  // resumes the flat read there and slices the remaining rows past it.
  abandonDictionaryEncoding(endReadRow);
  return false;
}

void StringColumnReader::read(
    int64_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  if (readWithDictionary(offset, rows, incomingNulls)) {
    return;
  }

  const auto endReadRow = rows.back() + 1;
  // readWithDictionary returns false for two reasons:
  // 1. Preconditions not met (e.g., zeroCopy disabled) — readOffset_
  //    unchanged, numReadRows == 0. Full flat read via prepareRead below.
  // 2. Dict abandoned mid-read — readOffset_ advanced to the chunk boundary,
  //    numReadRows > 0. Flat encoding fallback for the remaining rows only.
  // Both the return value (false) and numReadRows > 0 are needed to
  // distinguish abandoned dict (case 2) from dict path not taken (case 1).
  const auto numReadRows = readOffset_ > offset
      ? static_cast<velox::vector_size_t>(readOffset_ - offset)
      : 0;
  NIMBLE_CHECK_LE(numReadRows, endReadRow);

  // Build the row set for the flat read.
  // - Fresh read (numReadRows == 0): use the original rows directly.
  // - Continuation after an abandoned dict read: numReadRows is the chunk
  //   boundary where the decoder is parked. The remaining rows are the
  //   suffix of the (sorted) original rows at positions >= the boundary — a
  //   contiguous subrange we reference in place (no copy). The flat read
  //   resumes the decode at numReadRows and skips to the first such row.
  velox::RowSet readRowSet;
  if (numReadRows == 0) {
    prepareRead<std::string_view>(offset, rows, incomingNulls);
    readRowSet = rows;
  } else {
    const auto* remainingRowStart =
        std::lower_bound(rows.data(), rows.data() + rows.size(), numReadRows);
    readRowSet = velox::RowSet(
        remainingRowStart,
        static_cast<size_t>(rows.data() + rows.size() - remainingRowStart));
  }

  dwio::common::StringColumnReadWithVisitorHelper<
      /*kEncodingHasNulls=*/false,
      /*kDictionary=*/false>(*this, readRowSet)([&](auto visitor) {
    if (numReadRows == 0) {
      decoder_.readWithVisitor(visitor);
    } else {
      // Append string buffers from the flat read to the existing ones
      // (which include the dict portion's string buffer from
      // abandonDictionaryEncoding).
      decoder_.readWithVisitor(
          visitor,
          /*readOffset=*/numReadRows,
          [this](std::vector<velox::BufferPtr>&& buffers) {
            for (auto& buffer : buffers) {
              stringBuffers_.push_back(std::move(buffer));
            }
          });
    }
  });

  readOffset_ = offset + endReadRow;
}

void StringColumnReader::ensureAlphabetVector() {
  if (dictionaryState_.alphabetVector != nullptr) {
    return;
  }
  const auto alphabetSize =
      static_cast<vector_size_t>(dictionaryState_.alphabet.size());
  auto values = AlignedBuffer::allocate<StringView>(alphabetSize, pool_);
  auto* rawValues = values->asMutable<StringView>();
  for (vector_size_t i = 0; i < alphabetSize; ++i) {
    rawValues[i] = StringView(
        dictionaryState_.alphabet[i].data(),
        dictionaryState_.alphabet[i].size());
  }
  auto buffers = stringBuffers_;
  dictionaryState_.alphabetVector = std::make_shared<FlatVector<StringView>>(
      pool_,
      fileType_->type(),
      BufferPtr(nullptr),
      alphabetSize,
      std::move(values),
      std::move(buffers));
}

void StringColumnReader::abandonDictionaryEncoding(vector_size_t endReadRow) {
  NIMBLE_CHECK(!dictionaryState_.alphabet.empty());

  // Resolve dictionary indices to flat StringViews. The underlying string
  // data lives in the encoding's string buffers, which the reader already
  // holds via stringBuffers_ (set by readDictionaryIndices). No string
  // byte copying is needed. Null positions contain uninitialized indices
  // and are skipped — they get StringView() (empty) by the allocate default.
  const auto* indices = reinterpret_cast<const int32_t*>(rawValues_);
  auto flatValues =
      AlignedBuffer::allocate<StringView>(endReadRow, pool_, StringView());
  auto* rawFlatValues = flatValues->asMutable<StringView>();
  // The dict indices in rawValues_ are at output positions [0, numValues_).
  // resultNulls() is the single source of truth for the output-indexed null
  // bitmap: it selects nullsInReadRange_ for dense reads and resultNulls_ for
  // sparse, and returns an empty buffer when there are no nulls. Use it
  // directly rather than re-deriving the selection here.
  const auto& resultNullsBuffer = resultNulls();
  const auto* nulls =
      resultNullsBuffer ? resultNullsBuffer->as<uint64_t>() : nullptr;

  for (vector_size_t i = 0; i < numValues_; ++i) {
    if (nulls != nullptr && velox::bits::isBitNull(nulls, i)) {
      continue;
    }
    const auto idx = indices[i];
    NIMBLE_DCHECK_LT(idx, dictionaryState_.alphabet.size());
    const auto& sv = dictionaryState_.alphabet[idx];
    rawFlatValues[i] = StringView(sv.data(), sv.size());
  }

  values_ = std::move(flatValues);
  rawValues_ = values_->asMutable<char>();

  // prepareRead<int32_t> set valueSize_=4 for dict indices. After
  // converting to StringView values, update valueSize_ so that the
  // flat encoding fallback's addNull/addValue writes at correct byte offsets.
  valueSize_ = sizeof(StringView);
  clearDictionaryState();
  crossChunkRead_ = false;
}

void StringColumnReader::getValues(const RowSet& rows, VectorPtr* result) {
  if (!hasDictionaryState()) {
    rawStringBuffer_ = nullptr;
    rawStringSize_ = 0;
    rawStringUsed_ = 0;
    getFlatValues<StringView, StringView>(rows, result, fileType_->type());
    return;
  }

  compactScalarValues<int32_t, int32_t>(rows, /*isFinal=*/false);
  ensureAlphabetVector();

  *result = std::make_shared<DictionaryVector<StringView>>(
      pool_,
      resultNulls(),
      numValues_,
      dictionaryState_.alphabetVector,
      values_);
  numValues_ = 0;
  // If this batch crossed chunk boundaries, clear the merged alphabet
  // to free memory from previous chunks that are no longer needed for
  // the next read range. Single-chunk batches keep the cached alphabet
  // for reuse. The DictionaryVector holds a shared_ptr to
  // alphabetVector, so the data survives after clearing.
  if (crossChunkRead_) {
    clearDictionaryState();
    crossChunkRead_ = false;
  }
}

bool StringColumnReader::estimateMaterializedSize(
    size_t& byteSize,
    size_t& rowCount) const {
  auto decoderRowCount = decoder_.estimateRowCount();
  if (!decoderRowCount.has_value()) {
    return false;
  }
  auto decoderDataSize = decoder_.estimateStringDataSize();
  if (!decoderDataSize.has_value()) {
    return false;
  }
  rowCount = *decoderRowCount;
  auto rowSize = *decoderDataSize / rowCount;
  rowSize += (rowSize > velox::StringView::kInlineSize) ? 16 : 4;
  byteSize = rowSize * rowCount;
  return true;
}

} // namespace facebook::nimble
