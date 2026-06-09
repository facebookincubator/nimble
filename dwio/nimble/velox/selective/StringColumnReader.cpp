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

#include "dwio/nimble/encodings/legacy/EncodingUtils.h"
#include "dwio/nimble/velox/selective/NimbleData.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/SelectiveColumnReaderInternal.h"
#include "velox/vector/DictionaryVector.h"

namespace facebook::nimble {

using namespace facebook::velox;
using velox::dwio::common::DictionaryValues;
using velox::dwio::common::FilterResult;

uint64_t StringColumnReader::skip(uint64_t numValues) {
  numValues = SelectiveColumnReader::skip(numValues);
  decoder_.skip(numValues);
  return numValues;
}

void StringColumnReader::clearDictionaryState() {
  dictionaryState_.clear();
  scanState_.dictionary.clear();
  scanState_.updateRawState();
}

// TODO: Derive alphabetVector from scanState_.dictionary.values to eliminate
// redundant alphabet storage.
void StringColumnReader::ensureDictionaryState() {
  if (hasDictionaryState()) {
    return;
  }
  // Invalidate dictionary state when a new chunk is loaded, since the new
  // chunk may have a different encoding or dictionary alphabet.
  decoder_.setOnChunkLoad([this] { clearDictionaryState(); });
  dictionaryState_.alphabet = buildEncodingDictionaryAlphabet<std::string_view>(
      decoder_.currentEncoding());

  if (DictionaryValues::hasFilter(scanSpec_->filter())) {
    const auto& alphabet = dictionaryState_.alphabet;
    const auto alphabetSize = static_cast<int32_t>(alphabet.size());
    int64_t totalBytes = 0;
    for (const auto& entry : alphabet) {
      totalBytes += entry.size();
    }
    scanState_.dictionary.numValues = alphabetSize;
    scanState_.dictionary.strings =
        AlignedBuffer::allocate<char>(totalBytes, pool_);
    scanState_.dictionary.values =
        AlignedBuffer::allocate<StringView>(alphabetSize, pool_);
    auto* rawStrings = scanState_.dictionary.strings->asMutable<char>();
    auto* rawValues = scanState_.dictionary.values->asMutable<StringView>();
    int64_t offset = 0;
    for (int32_t i = 0; i < alphabetSize; ++i) {
      auto len = alphabet[i].size();
      std::memcpy(rawStrings + offset, alphabet[i].data(), len);
      rawValues[i] = StringView(rawStrings + offset, len);
      offset += len;
    }
    scanState_.filterCache.resize(alphabetSize);
    simd::memset(
        scanState_.filterCache.data(), FilterResult::kUnknown, alphabetSize);
    scanState_.updateRawState();
  }
}

bool StringColumnReader::readWithDictionary(
    int64_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  // Dictionary path requires: no value hook, non-legacy encoding path
  // (zero-copy), session property enabled, single-chunk read, and
  // dictionary-convertible encoding.
  if (scanSpec_->valueHook() || !formatData().stringDecoderZeroCopy() ||
      !static_cast<const NimbleData&>(formatData())
           .nimblePreserveDictionaryEncoding()) {
    return false;
  }
  decoder_.ensureLoaded(/*preserveDictionaryEncoding=*/true);
  const auto numRequestedRows = rows.back() + 1;
  // The decoder must have enough remaining values in the current chunk to
  // cover both the skip (from readOffset_ to offset, executed by prepareRead
  // → seekTo → skip) and the read range (rows 0 through rows.back()). If
  // this total exceeds remainingValues, the skip or read would cross a chunk
  // boundary, invalidating the dictionary state built below.
  const auto numValuesNeeded = offset - readOffset_ + numRequestedRows;
  if (decoder_.remainingValues() < numValuesNeeded ||
      !decoder_.dictionaryConvertible()) {
    return false;
  }
  // NOTE: ensureDictionaryState must move back after readDictionaryIndices
  // when nested encodings (MC, RLE) are supported, since the dictionary
  // alphabet may not be available until the encoding is actually read.
  ensureDictionaryState();
  velox::common::testutil::TestValue::adjust(
      "facebook::nimble::StringColumnReader::readWithDictionary",
      &dictionaryState_.alphabet);
  prepareRead<int32_t>(offset, rows, incomingNulls);
  dwio::common::StringColumnReadWithVisitorHelper<
      /*kEncodingHasNulls=*/false,
      /*kDictionary=*/true>(*this, rows)([&](auto visitor) {
    auto dictVisitor = visitor.toStringDictionaryColumnVisitor();
    decoder_.readDictionaryIndices(dictVisitor);
  });
  readOffset_ += numRequestedRows;
  return true;
}

void StringColumnReader::read(
    int64_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  if (readWithDictionary(offset, rows, incomingNulls)) {
    return;
  }
  clearDictionaryState();
  prepareRead<std::string_view>(offset, rows, incomingNulls);
  dwio::common::StringColumnReadWithVisitorHelper<
      /*kEncodingHasNulls=*/false,
      /*kDictionary=*/false>(
      *this, rows)([&](auto visitor) { decoder_.readWithVisitor(visitor); });
  readOffset_ += rows.back() + 1;
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

void StringColumnReader::getValues(const RowSet& rows, VectorPtr* result) {
  if (hasDictionaryState()) {
    compactScalarValues<int32_t, int32_t>(rows, /*isFinal=*/false);
    ensureAlphabetVector();

    *result = std::make_shared<DictionaryVector<StringView>>(
        pool_,
        resultNulls(),
        numValues_,
        dictionaryState_.alphabetVector,
        values_);
  } else {
    rawStringBuffer_ = nullptr;
    rawStringSize_ = 0;
    rawStringUsed_ = 0;
    getFlatValues<StringView, StringView>(rows, result, fileType_->type());
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
