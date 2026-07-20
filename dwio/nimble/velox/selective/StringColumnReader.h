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

#include <vector>
#include "dwio/nimble/velox/selective/ChunkedDecoder.h"
#include "folly/container/F14Map.h"
#include "velox/dwio/common/SelectiveColumnReader.h"

namespace facebook::nimble {

class StringColumnReader : public velox::dwio::common::SelectiveColumnReader {
 public:
  StringColumnReader(
      const velox::TypePtr& requestedType,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& fileType,
      NimbleParams& params,
      velox::common::ScanSpec& scanSpec)
      : SelectiveColumnReader(requestedType, fileType, params, scanSpec),
        decoder_(formatData().as<NimbleData>().makeScalarDecoder()) {}

  uint64_t skip(uint64_t numValues) override;

  void read(
      int64_t offset,
      const velox::RowSet& rows,
      const uint64_t* incomingNulls) override;

  void getValues(const velox::RowSet& rows, velox::VectorPtr* result) override;

  bool estimateMaterializedSize(size_t& byteSize, size_t& rowCount)
      const override;

 protected:
  ChunkedDecoder decoder_;

 private:
  bool readsNullsOnly() const final {
    return false;
  }

  // Whether the dictionary encoding is preservable for this read: only without
  // a value hook, on the zero-copy (non-legacy) encoding path, and with
  // preserve-dictionary enabled. Gates read()'s dispatch into
  // readWithDictionary.
  bool dictionaryPreservable();

  // Merged alphabet accumulated across chunks for multi-chunk dictionary
  // reads. Each chunk's alphabet is appended, and indices are offset to
  // reference this merged alphabet. Cleared when skip crosses a chunk
  // boundary (via the skip callback) to invalidate stale state.
  struct DictionaryState {
    std::vector<std::string_view> alphabet;
    // Maps alphabet entries to their index in alphabet for deduplication
    // when compacting dictionaries across chunks.
    folly::F14FastMap<std::string_view, int32_t> alphabetIndex;
    velox::VectorPtr alphabetVector;
    velox::raw_vector<uint8_t> filterCache;

    void clear() {
      alphabet.clear();
      alphabetIndex.clear();
      alphabetVector.reset();
      filterCache.clear();
    }
  };

  // Materializes the alphabet FlatVector from dictionaryState_.alphabet
  // for use in DictionaryVector output.
  void ensureAlphabetVector();

  // Populates dictionaryState_ from the current chunk's encoding if not
  // already set.
  void ensureDictionaryState();

  void clearDictionaryState() {
    dictionaryState_.clear();
  }

  // Whether cross-chunk dictionary alphabets should be deduplicated
  // (compacted), per the nimbleCompactDictionaryAcrossChunks read option.
  bool compactDictionaryAcrossChunks() const {
    return static_cast<const NimbleData&>(formatData())
        .nimbleCompactDictionaryAcrossChunks();
  }

  // Back-fills alphabetIndex from the merged alphabet for any entries not yet
  // indexed (the first chunk's alphabet, built by ensureDictionaryState).
  // Called lazily on the first chunk boundary so single-chunk reads never
  // build the dedup map. Idempotent: a no-op once the map is in sync.
  void ensureAlphabetIndex();

  // Shifts the dictionary indices in rawValues_ at [valueOffset, numValues_) by
  // alphabetOffset, the compaction-off remap: each chunk is appended
  // contiguously, so a local index maps to alphabetOffset + localIndex. A
  // vectorizable scalar add applied to every position (null slots included,
  // harmless since their indices are never read downstream). alphabetOffset ==
  // 0 is the identity, i.e. the first chunk.
  void updateDictionaryIndices(
      int32_t alphabetOffset,
      velox::vector_size_t valueOffset);

  // Translates the dictionary indices in rawValues_ at [valueOffset,
  // numValues_) through remap, the compaction-on remap: deduplication makes the
  // mapping non-uniform, so each local index is looked up in the table. An
  // empty remap is the identity (the first chunk, whose local indices already
  // reference the merged alphabet's leading entries). Null positions hold
  // uninitialized indices; the lookup is guarded against out-of-range garbage.
  void updateCompactedDictionaryIndices(
      const std::vector<int32_t>& remap,
      velox::vector_size_t valueOffset);

  // Attempts to extend the merged dictionary alphabet when a chunk boundary
  // is crossed during multi-chunk dictionary index reading. First remaps the
  // indices written since valueOffset (to reference the merged alphabet), then
  // checks whether the new chunk is dictionary-convertible.
  //
  // Returns true if the new chunk's alphabet was appended and reading
  // should continue. Returns false if the new chunk is not
  // dictionary-convertible, signaling the caller to fall back to flat
  // decoding for the remaining rows.
  //
  // Carries the previous chunk's remap in whichever of the two forms the
  // compaction mode uses (see updateDictionaryIndices /
  // updateCompactedDictionaryIndices), and rebuilds it for the newly appended
  // chunk.
  //
  // @param alphabetOffset Running merged-alphabet offset for the compaction-off
  //   path. Updated to the pre-append merged size for the new chunk.
  // @param pendingRemap Local->merged table for the compaction-on path. Rebuilt
  //   (deduplicated) for the new chunk.
  // @param valueOffset Index into rawValues_ marking where the current
  //   chunk's indices start. Updated to numValues_ after remapping.
  bool tryExtendDictionaryAtChunkBoundary(
      int32_t& alphabetOffset,
      std::vector<int32_t>& pendingRemap,
      velox::vector_size_t& valueOffset);

  // Grows dictionaryState_.filterCache to one byte per merged-alphabet entry,
  // initializing newly added bytes to FilterResult::kUnknown. Extend-only: it
  // never shrinks, relying on DictionaryState::clear() to drop the cache
  // together with the alphabet so no stale verdict outlives its entry.
  void ensureFilterCache();

  // Switches off the dense returnReaderNulls_ fast path and ensures
  // resultNulls_ is a writable, output-indexed buffer of at least `size` bits.
  // The dict-filter path pre-compacts values, so compactScalarValues' null move
  // is skipped; this provides a buffer to write the compacted output nulls
  // into.
  void ensureWritableResultNulls(velox::vector_size_t size);

  // Applies the pushed-down filter to the just-materialized dictionary indices,
  // post-hoc on the merged alphabet (the bulk index read suppressed the
  // filter). Compacts in place to the passing rows and rewrites the reader's
  // output state:
  //   - rawValues_: overwritten with the passing dictionary indices, compacted.
  //   - outputRows_: set to the passing file rows, resized to the pass count.
  //   - numValues_: set to the number of passing rows.
  // When nulls are present it also realigns the result-null bitmap to the
  // compacted output layout via ensureWritableResultNulls() (which allocates
  // resultNulls_ and clears returnReaderNulls_), because pre-compaction makes
  // the framework's compactScalarValues null move a no-op. Reads the
  // output-indexed null bitmap from resultNulls() and consults
  // dictionaryState_.alphabet and filterCache (lazily filled via
  // ensureFilterCache()).
  void filterDictionaryIndices(
      const velox::RowSet& rows,
      const velox::common::Filter* filter);

  // Merges, in ascending row order, the null rows (which pass because the
  // filter accepts nulls) with the byte-filter-passing non-null rows held in
  // passIndices/passRows. Writes the merged dictionary indices into 'indices',
  // the merged file rows into 'outputRows', and the per-output null bits into
  // rawResultNulls_ (null positions marked null, index 0 as a placeholder).
  // Returns the merged output count (the new numValues_).
  velox::vector_size_t processNullAndPassingRows(
      const uint64_t* nulls,
      const velox::RowSet& rows,
      velox::vector_size_t readCount,
      const int32_t* passIndices,
      const int32_t* passRows,
      int32_t numPass,
      int32_t* indices,
      int32_t* outputRows);

  // Converts the dict indices in rawValues_ to flat StringView values by
  // resolving each index against the merged alphabet. The StringViews point
  // into the encoding's string buffers already held by stringBuffers_. Null
  // positions (uninitialized indices) are skipped. Called during reactive
  // fallback when a non-dictionary chunk is encountered mid-read.
  //
  // @param endReadRow End row of the read range (for output buffer sizing).
  void abandonDictionaryEncoding(velox::vector_size_t endReadRow);

  bool hasDictionaryState() const {
    return !dictionaryState_.alphabet.empty();
  }

  // Attempts to read all rows using the dictionary index path.
  //
  // Returns true if all rows were consumed via dictionary encoding.
  //
  // Returns false if the dict path could not handle the entire batch.
  // readOffset_ indicates how many rows were consumed:
  //   - readOffset_ == offset: dict path not taken at all. The caller
  //     should do a full flat read starting with prepareRead.
  //   - readOffset_ > offset: partial dict read stopped at a non-dict
  //     chunk. The dict rows are expanded to flat StringViews via
  //     abandonDictionaryEncoding, nulls are set up, and the decoder is
  //     at the continuation point. The caller reads the remaining rows
  //     as flat without calling prepareRead.
  bool readWithDictionary(
      int64_t offset,
      const velox::RowSet& rows,
      const uint64_t* incomingNulls);

  DictionaryState dictionaryState_;

  // Set when the current batch's read crossed chunk boundaries.
  // Consumed by getValues to clear the merged alphabet after the
  // DictionaryVector is constructed.
  bool crossChunkRead_{false};
};

} // namespace facebook::nimble
