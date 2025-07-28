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

#include "dwio/nimble/velox/selective/StructColumnReader.h"

#include "dwio/nimble/velox/selective/ChunkedDecoder.h"
#include "dwio/nimble/velox/selective/ColumnReader.h"

namespace facebook::nimble {

using namespace facebook::velox;

void StructColumnReaderBase::seekTo(int64_t offset, bool /*readsNullsOnly*/) {
  if (offset == readOffset_) {
    return;
  }
  VELOX_CHECK_LT(readOffset_, offset);
  if (numParentNulls_) {
    VELOX_CHECK_LE(
        parentNullsRecordedTo_,
        offset,
        "Must not seek to before parentNullsRecordedTo_");
  }
  auto distance = offset - readOffset_ - numParentNulls_;
  auto numNonNulls = formatData_->skipNulls(distance);
  for (auto& child : children_) {
    if (child) {
      child->addSkippedParentNulls(
          readOffset_, offset, numParentNulls_ + distance - numNonNulls);
    }
  }
  numParentNulls_ = 0;
  parentNullsRecordedTo_ = 0;
  readOffset_ = offset;
}

StructColumnReader::StructColumnReader(
    const velox::TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    NimbleParams& params,
    common::ScanSpec& scanSpec,
    bool isRoot)
    : StructColumnReaderBase(
          requestedType,
          fileType,
          params,
          scanSpec,
          isRoot) {
  const auto& rowType = params.nimbleType()->asRow();
  const auto& fileRowType = fileType_->type()->asRow();
  for (auto* childSpec : scanSpec.stableChildren()) {
    if (childSpec->isConstant() || isChildMissing(*childSpec)) {
      childSpec->setSubscript(kConstantChildSpecSubscript);
      continue;
    }
    if (!childSpec->readFromFile()) {
      continue;
    }
    auto i = fileRowType.getChildIdx(childSpec->fieldName());
    auto& childFileType = fileType_->childAt(i);
    auto& childRequestedType =
        requestedType_->asRow().findChild(childSpec->fieldName());
    auto childParams = params.makeChildParams(rowType.childAt(i));
    addChild(buildColumnReader(
        childRequestedType, childFileType, childParams, *childSpec, false));
    childSpec->setSubscript(children_.size() - 1);
  }
}

bool StructColumnReader::estimateMaterializedSize(
    size_t& byteSize,
    size_t& rowCount) const {
  auto* nulls = formatData().as<NimbleData>().nullsDecoder();
  if (nulls) {
    auto nullsRowCount = nulls->estimateRowCount();
    if (!nullsRowCount.has_value()) {
      return false;
    }
    rowCount = *nullsRowCount;
  } else {
    rowCount = 0;
  }
  size_t rowSize = 0;
  for (auto& child : children_) {
    size_t childByteSize, childRowCount;
    if (!child->estimateMaterializedSize(childByteSize, childRowCount)) {
      return false;
    }
    if (!nulls) {
      rowCount = childRowCount;
    }
    if (childRowCount > 0) {
      rowSize += childByteSize / childRowCount;
    }
  }
  byteSize = rowSize * rowCount;
  if (nulls) {
    byteSize += rowCount / 8;
  }
  return true;
}

} // namespace facebook::nimble
