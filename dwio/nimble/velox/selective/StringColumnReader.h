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

  // Merged alphabet accumulated across chunks for multi-chunk dictionary
  // reads. Each chunk's alphabet is appended, and indices are offset to
  // reference this merged alphabet. Cleared when skip crosses a chunk
  // boundary (via the skip callback) to invalidate stale state.
  struct DictionaryState {
    std::vector<std::string_view> alphabet;
    velox::VectorPtr alphabetVector;

    void clear() {
      alphabet.clear();
      alphabetVector.reset();
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

  // Attempts to extend the merged dictionary alphabet when a chunk boundary
  // is crossed during multi-chunk dictionary index reading. Offsets the
  // indices written since valueOffset by alphabetOffset (to reference the
  // merged alphabet), then checks whether the new chunk is
  // dictionary-convertible.
  //
  // Returns true if the new chunk's alphabet was appended and reading
  // should continue. Returns false if the new chunk is not
  // dictionary-convertible, signaling the caller to fall back to flat
  // decoding for the remaining rows.
  //
  // @param alphabetOffset Running offset into the merged alphabet. Updated
  //   to the new alphabet size when a chunk is appended. Tracked to produce
  //   the correct indices in the merged alphabets.
  // @param valueOffset Index into rawValues_ marking where the current
  //   chunk's indices start. Updated to numValues_ after offsetting.
  // Offsets dictionary indices in rawValues_ by alphabetSize, starting
  // from position 'valueOffset' up to numValues_. Used to remap per-chunk
  // indices into the merged alphabet's index space.
  void updateDictionaryIndices(
      velox::vector_size_t alphabetSize,
      velox::vector_size_t valueOffset);

  bool tryExtendDictionaryAtChunkBoundary(
      int32_t& alphabetOffset,
      velox::vector_size_t& valueOffset);

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
