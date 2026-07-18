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

#include <folly/ScopeGuard.h>
#include <algorithm>
#include "dwio/nimble/encodings/legacy/EncodingUtils.h"
#include "dwio/nimble/velox/selective/NimbleData.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/ColumnVisitors.h"
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

void StringColumnReader::ensureFilterCache() {
  using FilterResult = velox::dwio::common::FilterResult;
  const auto alphabetSize = dictionaryState_.alphabet.size();
  const auto oldSize = dictionaryState_.filterCache.size();
  // The filter cache size and the merged-alphabet (dictionary) size increase
  // monotonically: the alphabet only grows by appending within a read and the
  // cache extends to match; on a chunk boundary both are cleared and rebuilt
  // together (the cache is never reused across a different chunk), and the
  // reader is rebuilt per stripe. So oldSize never exceeds alphabetSize — it is
  // either equal (same alphabet reused) or smaller (alphabet grew).
  NIMBLE_CHECK_LE(oldSize, alphabetSize);
  if (oldSize >= alphabetSize) {
    return;
  }
  dictionaryState_.filterCache.resize(alphabetSize);
  velox::simd::memset(
      dictionaryState_.filterCache.data() + oldSize,
      static_cast<uint8_t>(FilterResult::kUnknown),
      static_cast<int32_t>(alphabetSize - oldSize));
}

namespace {

// Compacts the non-null (index, row) pairs out of a materialized run. Writes
// the dictionary index and file row of each non-null position into
// outIndices/outRows (caller-sized to at least numValues) and returns the
// non-null count. A null 'nulls' bitmap means no nulls, so every entry is
// copied. Pure: touches no reader member state.
int32_t extractNonNullEntries(
    const int32_t* indices,
    const velox::RowSet& rows,
    const uint64_t* nulls,
    velox::vector_size_t numValues,
    int32_t* outIndices,
    int32_t* outRows) {
  int32_t count = 0;
  for (velox::vector_size_t i = 0; i < numValues; ++i) {
    if (nulls != nullptr && velox::bits::isBitNull(nulls, i)) {
      continue;
    }
    outIndices[count] = indices[i];
    outRows[count] = rows[i];
    ++count;
  }
  return count;
}

// Filters a run of materialized dictionary indices through the merged alphabet
// and the per-index filterCache. Writes the passing indices to outputIndices
// and their rows to outputRows (both caller-sized, compacted to the survivors),
// records each newly resolved verdict into filterCache (kSuccess/kFailure), and
// returns the number of passing rows.
int32_t filterByCache(
    const int32_t* inputIndices,
    const int32_t* inputRows,
    int32_t numInput,
    const std::vector<std::string_view>& alphabet,
    const velox::common::Filter& filter,
    uint8_t* filterCache,
    int32_t* outputIndices,
    int32_t* outputRows) {
  // Delegate to the shared SIMD dictionary-filter kernel. Nimble's merged
  // alphabet is single-tier, so the per-index test resolves the dictionary
  // entry directly and applies the byte filter. The Nimble path always emits
  // both indices and rows, hence kFilterOnly=false.
  return velox::dwio::common::filterDictionaryRunSimd</*kFilterOnly=*/false>(
      inputIndices,
      numInput,
      inputRows,
      filterCache,
      outputRows,
      outputIndices,
      /*numValues=*/0,
      [&](int32_t dictIndex) {
        const auto& sv = alphabet[dictIndex];
        return filter.testBytes(sv.data(), static_cast<int32_t>(sv.size()));
      });
}

} // namespace

void StringColumnReader::ensureWritableResultNulls(velox::vector_size_t size) {
  if (!returnReaderNulls_) {
    // Non-dense reads already hold an output-indexed resultNulls_ allocated by
    // prepareRead/prepareNulls; nothing to do.
    return;
  }
  // Dense reads with nulls park the null flags in nullsInReadRange_ and return
  // them directly (returnReaderNulls_). The dict-filter path pre-compacts
  // values, so it must instead write compacted output nulls into resultNulls_.
  // Mirror compactScalarValues' allocation and switch the fast path off.
  if (!(resultNulls_ && resultNulls_->unique() &&
        resultNulls_->capacity() >=
            velox::bits::nbytes(size) + velox::simd::kPadding)) {
    resultNulls_ = velox::AlignedBuffer::allocate<bool>(
        size + velox::simd::kPadding * 8, pool_);
    rawResultNulls_ = resultNulls_->asMutable<uint64_t>();
  }
  returnReaderNulls_ = false;
}

void StringColumnReader::filterDictionaryIndices(
    const RowSet& rows,
    const velox::common::Filter* filter) {
  NIMBLE_CHECK_NOT_NULL(filter);
  ensureFilterCache();

  const auto readCount = numValues_;
  constexpr int32_t kSimdPadding = xsimd::batch<int32_t>::size;
  std::vector<int32_t> nonNullIndices(readCount + kSimdPadding, 0);
  std::vector<int32_t> nonNullRows(readCount + kSimdPadding, 0);
  // The dict indices in rawValues_ are output-indexed, so the null check must
  // use the output-indexed bitmap. resultNulls() is the single source of truth
  // for it (nullsInReadRange_ for dense reads, resultNulls_ for non-dense). Use
  // it instead of rawNullsInReadRange(), which is file-indexed and wrong for
  // non-dense reads (nested-under-nullable, cross-stripe, sibling-filter).
  const auto& resultNullsBuffer = resultNulls();
  const auto* nulls =
      resultNullsBuffer ? resultNullsBuffer->as<uint64_t>() : nullptr;

  const auto numNonNull = extractNonNullEntries(
      reinterpret_cast<const int32_t*>(rawValues_),
      rows,
      nulls,
      readCount,
      nonNullIndices.data(),
      nonNullRows.data());

  if (nulls == nullptr) {
    // No nulls in range: filter the dictionary indices in place. There is no
    // null bitmap to realign.
    auto* outputRows = mutableOutputRows(numNonNull);
    numValues_ = filterByCache(
        nonNullIndices.data(),
        nonNullRows.data(),
        numNonNull,
        dictionaryState_.alphabet,
        *filter,
        dictionaryState_.filterCache.data(),
        reinterpret_cast<int32_t*>(rawValues_),
        outputRows);
    outputRows_.resize(numValues_);
    return;
  }

  // Nulls are present. The dict-filter path pre-compacts rawValues_/outputRows_
  // to the passing rows, which makes the framework's compactScalarValues a
  // no-op (rows.size() == numValues_) and skips its null move. So realign the
  // result null bitmap to the compacted output layout here; otherwise
  // getValues() would hand DictionaryVector a read-range-indexed bitmap against
  // compacted indices and misplace nulls.
  if (!filter->testNull()) {
    // The filter rejects nulls, so every null row is dropped and the
    // SIMD-filtered non-null passing rows are the entire output. Filter in
    // place, then mark the compacted output all-non-null.
    auto* outputRows = mutableOutputRows(numNonNull);
    numValues_ = filterByCache(
        nonNullIndices.data(),
        nonNullRows.data(),
        numNonNull,
        dictionaryState_.alphabet,
        *filter,
        dictionaryState_.filterCache.data(),
        reinterpret_cast<int32_t*>(rawValues_),
        outputRows);
    outputRows_.resize(numValues_);
    ensureWritableResultNulls(readCount);
    velox::bits::fillBits(
        rawResultNulls_, 0, numValues_, velox::bits::kNotNull);
    return;
  }

  // The filter accepts nulls (e.g. IS NULL): retain every null row and merge
  // it, in ascending row order, with the SIMD-filtered non-null passing rows.
  // The non-null rows are filtered into temporaries because the merge writes
  // the interleaved result back into rawValues_/outputRows_ in row order.
  std::vector<int32_t> passIndices(numNonNull + kSimdPadding, 0);
  std::vector<int32_t> passRows(numNonNull + kSimdPadding, 0);
  const auto numPass = filterByCache(
      nonNullIndices.data(),
      nonNullRows.data(),
      numNonNull,
      dictionaryState_.alphabet,
      *filter,
      dictionaryState_.filterCache.data(),
      passIndices.data(),
      passRows.data());

  ensureWritableResultNulls(readCount);
  auto* indices = reinterpret_cast<int32_t*>(rawValues_);
  auto* outputRows = mutableOutputRows(readCount);
  numValues_ = processNullAndPassingRows(
      nulls,
      rows,
      readCount,
      passIndices.data(),
      passRows.data(),
      numPass,
      indices,
      outputRows);
  outputRows_.resize(numValues_);
}

velox::vector_size_t StringColumnReader::processNullAndPassingRows(
    const uint64_t* nulls,
    const velox::RowSet& rows,
    velox::vector_size_t readCount,
    const int32_t* passIndices,
    const int32_t* passRows,
    int32_t numPass,
    int32_t* indices,
    int32_t* outputRows) {
  velox::vector_size_t count = 0;
  int32_t passCursor = 0;
  for (velox::vector_size_t i = 0; i < readCount; ++i) {
    if (velox::bits::isBitNull(nulls, i)) {
      // Null row: passes because the filter accepts nulls. The index is
      // unused for a null position; write 0 as a safe placeholder.
      velox::bits::setNull(rawResultNulls_, count, /*isNull=*/true);
      indices[count] = 0;
      outputRows[count] = rows[i];
      ++count;
    } else if (passCursor < numPass && passRows[passCursor] == rows[i]) {
      // Non-null row that passed the byte filter.
      velox::bits::setNull(rawResultNulls_, count, /*isNull=*/false);
      indices[count] = passIndices[passCursor];
      outputRows[count] = rows[i];
      ++count;
      ++passCursor;
    }
  }
  return count;
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
  // Dictionary path requires: no value hook, non-legacy encoding path
  // (zero-copy), session property enabled, and dictionary-convertible
  // encoding. Filters are supported via post-materialization filtering
  // on the bulk-read dictionary indices.
  // TODO: Value hooks (aggregation pushdown) could be supported by
  // resolving alphabet[index] and calling hook->addValue() post-read,
  // similar to filterDictionaryIndices. Neither Nimble nor DWRF
  // currently support filter+hook combined for dict string columns.
  if (scanSpec_->valueHook() || !formatData().stringDecoderZeroCopy() ||
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
      /*preserveStringDictionaryEncoding=*/true, [this] {
        clearDictionaryState();
        return true;
      });
  if (!decoder_.dictionaryConvertible()) {
    clearDictionaryState();
    return false;
  }

  // A cross-chunk merged alphabet is per-read: getValues() clears it (and snaps
  // its backing into the output DictionaryVector) once the read is consumed.
  // But the parent struct reader skips this column's getValues() when it
  // filters out all rows in a batch, leaving crossChunkRead_ set and a stale
  // merged alphabet whose backing buffers the next read's setStringBuffers()
  // will free. Clear it before reuse so ensureDictionaryState() rebuilds a
  // fresh, valid alphabet. Single-chunk reads keep their cached alphabet
  // (crossChunkRead_ stays false).
  if (crossChunkRead_) {
    clearDictionaryState();
    crossChunkRead_ = false;
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
  //
  // Save and suppress the filter during index reading so the bulk
  // materializeIndices path runs (not approach C's per-row process path).
  // The SIMD filterDictionaryIndices applies the filter post-hoc.
  std::unique_ptr<velox::common::Filter> savedFilter;
  if (scanSpec_->hasFilter()) {
    savedFilter = scanSpec_->filter()->clone();
    scanSpec_->setFilter(nullptr);
  }
  // If the read below throws, reinstall the suppressed filter so scanSpec_ is
  // not left permanently filterless, which would silently drop the pushed-down
  // filter on subsequent reads.
  auto filterGuard = folly::makeGuard([&] {
    if (savedFilter != nullptr) {
      scanSpec_->setFilter(std::move(savedFilter));
    }
  });

  // kEncodingHasNulls must be false: Nimble materializes nulls lazily during
  // decode and does not pre-populate nullsInReadRange_.
  dwio::common::StringColumnReadWithVisitorHelper<
      /*kEncodingHasNulls=*/false,
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
  // Reinstate the filter before the post-hoc SIMD filtering below needs it; the
  // guard then no-ops since savedFilter has been moved out.
  filterGuard.dismiss();
  if (savedFilter != nullptr) {
    scanSpec_->setFilter(std::move(savedFilter));
  }

  // Reconcile the reader's null state after the bulk dictionary read, for
  // DENSE reads only. A dense read records its nulls in nullsInReadRange_
  // (fresh per read, output==file), but the bulk index path (unlike the
  // per-row processNull path) does not set anyNulls_/returnReaderNulls_;
  // without this, resultNulls() reports no nulls and
  // filterDictionaryIndices/getValues read the uninitialized index slots at
  // null positions. Set returnReaderNulls_ directly rather than via
  // resolveReturnNullBuffer(), which forces it false for filtered reads.
  //
  // Non-dense reads are intentionally NOT reconciled here: they set anyNulls_
  // via their own per-row processNull path, and their resultNulls_ must not
  // be inspected — it is populated lazily (only when a null is hit), so a
  // no-null non-dense read leaves it stale from a prior read and would
  // falsely report nulls, marking non-null rows null.
  //
  // Contract this couples to (keep in sync with SelectiveColumnReader): a
  // dense read is exactly rows == [0, rows.size()), the same test the
  // framework's useBulkPath()/resolveReturnNullBuffer() apply; and for a
  // dense bulk read with nulls, resultNulls() must return nullsInReadRange_
  // (returnReaderNulls_ == true). resolveReturnNullBuffer() would establish
  // that, but it forces the flag false whenever a filter is present, so we
  // set it directly here. Revisit if the framework changes how
  // returnReaderNulls_ is derived for dense reads.
  //
  // Applied regardless of abandonDictionary: the dict portion's indices
  // must be filtered before either getValues() (normal path) or
  // abandonDictionaryEncoding() (abandon path).
  const bool isDense = !rows.empty() && rows.back() == rows.size() - 1;
  if (!anyNulls_ && numValues_ > 0 && isDense &&
      nullsInReadRange() != nullptr &&
      !velox::bits::isAllSet(
          nullsInReadRange()->as<uint64_t>(),
          0,
          numValues_,
          velox::bits::kNotNull)) {
    anyNulls_ = true;
    returnReaderNulls_ = true;
  }
  if (scanSpec_->hasFilter()) {
    filterDictionaryIndices(rows, scanSpec_->filter());
  }

  if (!abandonDictionary) {
    readOffset_ += endReadRow;
    return true;
  }

  // Partial dict read: a non-dict chunk was encountered mid-read. Convert
  // the already-filtered dict indices to flat StringViews. readOffset_ already
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

  // Always read against the complete original rows so the visitor — and hence
  // null prep — sees the full output range. On a continuation after an
  // abandoned dict read, numReadRows is the chunk boundary where the decoder is
  // parked; advance the visitor to the first row at/after it so the flat read
  // resumes there (the dict portion was already produced) instead of re-reading
  // the prefix.
  velox::vector_size_t startRowIndex = 0;
  if (numReadRows == 0) {
    prepareRead<std::string_view>(offset, rows, incomingNulls);
  } else {
    startRowIndex = static_cast<velox::vector_size_t>(
        std::lower_bound(rows.data(), rows.data() + rows.size(), numReadRows) -
        rows.data());
  }

  dwio::common::StringColumnReadWithVisitorHelper<
      /*kEncodingHasNulls=*/false,
      /*kDictionary=*/false>(*this, rows)([&](auto visitor) {
    if (numReadRows == 0) {
      decoder_.readWithVisitor(visitor);
    } else {
      // Resume the flat read at the abandoned chunk boundary: skip the rows the
      // dict portion already produced, then append the new string buffers to
      // the existing ones (which include the dict portion's buffer from
      // abandonDictionaryEncoding).
      visitor.setRowIndex(startRowIndex);
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
