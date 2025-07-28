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

 private:
  bool readsNullsOnly() const final {
    return false;
  }

  ChunkedDecoder decoder_;
};

} // namespace facebook::nimble
