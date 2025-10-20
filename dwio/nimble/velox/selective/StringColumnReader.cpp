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

#include "velox/dwio/common/SelectiveColumnReaderInternal.h"

namespace facebook::nimble {

using namespace facebook::velox;

uint64_t StringColumnReader::skip(uint64_t numValues) {
  numValues = SelectiveColumnReader::skip(numValues);
  decoder_.skip(numValues);
  return numValues;
}

void StringColumnReader::read(
    int64_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  prepareRead<std::string_view>(offset, rows, incomingNulls);
  dwio::common::StringColumnReadWithVisitorHelper<false, false>(
      *this, rows)([&](auto visitor) { decoder_.readWithVisitor(visitor); });
  readOffset_ += rows.back() + 1;
}

void StringColumnReader::getValues(const RowSet& rows, VectorPtr* result) {
  rawStringBuffer_ = nullptr;
  rawStringSize_ = 0;
  rawStringUsed_ = 0;
  getFlatValues<StringView, StringView>(rows, result, fileType_->type());
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
