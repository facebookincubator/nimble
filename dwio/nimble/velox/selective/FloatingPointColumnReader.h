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
#include "velox/dwio/common/SelectiveFloatingPointColumnReader.h"

namespace facebook::nimble {

template <typename TData, typename TRequested>
class FloatingPointColumnReader
    : public velox::dwio::common::
          SelectiveFloatingPointColumnReader<TData, TRequested> {
  using Base = velox::dwio::common::
      SelectiveFloatingPointColumnReader<TData, TRequested>;

 public:
  FloatingPointColumnReader(
      const velox::TypePtr& requestedType,
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& fileType,
      NimbleParams& params,
      velox::common::ScanSpec& scanSpec)
      : Base(requestedType, fileType, params, scanSpec),
        decoder_(
            this->formatData().template as<NimbleData>().makeScalarDecoder()) {}

  uint64_t skip(uint64_t numValues) final {
    numValues = Base::skip(numValues);
    decoder_.skip(numValues);
    return numValues;
  }

  void read(
      int64_t offset,
      const velox::RowSet& rows,
      const uint64_t* incomingNulls) final {
    this->template readCommon<
        FloatingPointColumnReader<TData, TRequested>,
        false>(offset, rows, incomingNulls);
    this->readOffset_ += rows.back() + 1;
  }

  template <typename DecoderVisitor>
  void readWithVisitor(const velox::RowSet& /*rows*/, DecoderVisitor visitor) {
    decoder_.readWithVisitor(visitor);
  }

  bool estimateMaterializedSize(size_t& byteSize, size_t& rowCount)
      const final {
    auto decoderRowCount = decoder_.estimateRowCount();
    if (!decoderRowCount.has_value()) {
      return false;
    }
    rowCount = *decoderRowCount;
    byteSize = sizeof(TRequested) * rowCount;
    return true;
  }

 private:
  bool readsNullsOnly() const final {
    return false;
  }

  ChunkedDecoder decoder_;
};

} // namespace facebook::nimble
