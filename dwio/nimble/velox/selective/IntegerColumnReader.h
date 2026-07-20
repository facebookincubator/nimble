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

#include "dwio/nimble/velox/selective/ChunkedDecoder.h"
#include "velox/dwio/common/SelectiveIntegerColumnReader.h"

namespace facebook::nimble {

class IntegerColumnReader
    : public velox::dwio::common::SelectiveIntegerColumnReader {
 public:
  IntegerColumnReader(
      const velox::TypePtr& requestedType,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& fileType,
      NimbleParams& params,
      velox::common::ScanSpec& scanSpec)
      : SelectiveIntegerColumnReader(requestedType, params, scanSpec, fileType),
        decoder_(formatData().as<NimbleData>().makeScalarDecoder()) {
    // Enable value-based chunk pruning when this column has a deterministic
    // integer filter (no-op without chunk stats). Transform / delta-update
    // columns are excluded: their chunk min/max is over the stored
    // (pre-transform, pre-update) values, so pruning could drop a matching row.
    const auto* filter = scanSpec.filter();
    if (filter != nullptr && filter->isDeterministic() &&
        !scanSpec.hasTransform() && !scanSpec.deltaUpdate()) {
      decoder_.setChunkPruneScanSpec(&scanSpec, /*isFloatingPoint=*/false);
    }
  }

  uint64_t skip(uint64_t numValues) override;

  void read(
      int64_t offset,
      const velox::RowSet& rows,
      const uint64_t* incomingNulls) override;

  template <typename DecoderVisitor>
  void readWithVisitor(const velox::RowSet& /*rows*/, DecoderVisitor visitor) {
    decoder_.readWithVisitor(visitor);
  }

  bool estimateMaterializedSize(size_t& byteSize, size_t& rowCount)
      const override;

 private:
  bool readsNullsOnly() const final {
    return false;
  }

  ChunkedDecoder decoder_;
};

} // namespace facebook::nimble

namespace facebook::velox::dwio::common {

#define INTEGER_COLUMN_READER_PROCESS_FILTER(IsDense, ExtractValues) \
  template void SelectiveIntegerColumnReader::processFilter<         \
      nimble::IntegerColumnReader,                                   \
      IsDense,                                                       \
      false /* kEncodingHasNulls*/,                                  \
      ExtractValues>(                                                \
      const velox::common::Filter* filter,                           \
      ExtractValues extractValues,                                   \
      const RowSet& rows)

// External template instantiations to speed up build time by parallelizing.
// `SelectiveIntegerColumnReader::processFilter` is expensive to compile because
// it and the functions it calls are template heavy.
extern INTEGER_COLUMN_READER_PROCESS_FILTER(true, ExtractToReader);
extern INTEGER_COLUMN_READER_PROCESS_FILTER(false, ExtractToReader);
extern INTEGER_COLUMN_READER_PROCESS_FILTER(true, DropValues);
extern INTEGER_COLUMN_READER_PROCESS_FILTER(false, DropValues);

} // namespace facebook::velox::dwio::common
