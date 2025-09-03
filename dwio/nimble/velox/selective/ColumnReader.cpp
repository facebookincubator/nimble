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

#include "dwio/nimble/velox/selective/ColumnReader.h"
#include <memory>

#include "dwio/nimble/velox/selective/ByteColumnReader.h"
#include "dwio/nimble/velox/selective/FlatMapColumnReader.h"
#include "dwio/nimble/velox/selective/FloatingPointColumnReader.h"
#include "dwio/nimble/velox/selective/IntegerColumnReader.h"
#include "dwio/nimble/velox/selective/NullColumnReader.h"
#include "dwio/nimble/velox/selective/StringColumnReader.h"
#include "dwio/nimble/velox/selective/StructColumnReader.h"
#include "dwio/nimble/velox/selective/VariableLengthColumnReader.h"
#include "velox/dwio/common/TypeUtils.h"

namespace facebook::nimble {

using namespace facebook::velox;

std::unique_ptr<dwio::common::SelectiveColumnReader> buildColumnReader(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    NimbleParams& params,
    common::ScanSpec& scanSpec,
    bool isRoot) {
  if (isRoot) {
    VELOX_CHECK(fileType->type()->isRow());
  }
  dwio::common::typeutils::checkTypeCompatibility(
      *fileType->type(), *requestedType);
  auto& nimbleType = params.nimbleType();
  if (nimbleType->isScalar() &&
      !params.streams().hasStream(
          nimbleType->asScalar().scalarDescriptor().offset())) {
    return std::make_unique<NullColumnReader>(
        requestedType, fileType, params, scanSpec);
  }
  switch (fileType->type()->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
      return std::make_unique<ByteColumnReader>(
          requestedType, fileType, params, scanSpec);
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
      VELOX_CHECK(!fileType->type()->isDecimal());
      return std::make_unique<IntegerColumnReader>(
          requestedType, fileType, params, scanSpec);
    case TypeKind::REAL:
      if (requestedType->kind() == TypeKind::REAL) {
        return std::make_unique<FloatingPointColumnReader<float, float>>(
            requestedType, fileType, params, scanSpec);
      } else {
        VELOX_CHECK_EQ(requestedType->kind(), TypeKind::DOUBLE);
        return std::make_unique<FloatingPointColumnReader<float, double>>(
            requestedType, fileType, params, scanSpec);
      }
    case TypeKind::DOUBLE:
      return std::make_unique<FloatingPointColumnReader<double, double>>(
          requestedType, fileType, params, scanSpec);
    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      return std::make_unique<StringColumnReader>(
          requestedType, fileType, params, scanSpec);
    case TypeKind::ROW:
      return std::make_unique<StructColumnReader>(
          requestedType, fileType, params, scanSpec, isRoot);
    case TypeKind::ARRAY: {
      if (nimbleType->isArrayWithOffsets()) {
        if (!params.streams().hasStream(nimbleType->asArrayWithOffsets()
                                            .offsetsDescriptor()
                                            .offset())) {
          return std::make_unique<NullColumnReader>(
              requestedType, fileType, params, scanSpec);
        }
        return std::make_unique<DeduplicatedArrayColumnReader>(
            requestedType, fileType, params, scanSpec);
      } else {
        VELOX_CHECK(nimbleType->isArray());
        if (!params.streams().hasStream(
                nimbleType->asArray().lengthsDescriptor().offset())) {
          return std::make_unique<NullColumnReader>(
              requestedType, fileType, params, scanSpec);
        }
        return std::make_unique<ListColumnReader>(
            requestedType, fileType, params, scanSpec);
      }
    }
    case TypeKind::MAP:
      if (nimbleType->isFlatMap()) {
        return createFlatMapColumnReader(
            requestedType, fileType, params, scanSpec);
      } else if (nimbleType->isSlidingWindowMap()) {
        if (!params.streams().hasStream(nimbleType->asSlidingWindowMap()
                                            .offsetsDescriptor()
                                            .offset())) {
          return std::make_unique<NullColumnReader>(
              requestedType, fileType, params, scanSpec);
        }
        return std::make_unique<DeduplicatedMapColumnReader>(
            requestedType, fileType, params, scanSpec);
      } else {
        VELOX_CHECK(nimbleType->isMap());
        if (!params.streams().hasStream(
                nimbleType->asMap().lengthsDescriptor().offset())) {
          return std::make_unique<NullColumnReader>(
              requestedType, fileType, params, scanSpec);
        }
        return std::make_unique<MapColumnReader>(
            requestedType, fileType, params, scanSpec);
      }
    default:
      VELOX_UNSUPPORTED(
          "Unsupported type: {}", mapTypeKindToName(fileType->type()->kind()));
  }
}

} // namespace facebook::nimble
