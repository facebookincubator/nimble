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
#include "dwio/nimble/velox/RawSizeUtils.h"

namespace facebook::nimble {

// Returns uint64_t bytes of raw data in the vector.
uint64_t getRawSizeFromVector(const velox::VectorPtr& vector) {
  VELOX_CHECK_NOT_NULL(vector, "vector is null");
  const auto& typeKind = vector->typeKind();
  switch (typeKind) {
    case velox::TypeKind::BOOLEAN: {
      return getRawSizeFromFixedWidthVector<bool>(vector);
    }
    case velox::TypeKind::TINYINT: {
      return getRawSizeFromFixedWidthVector<int8_t>(vector);
    }
    case velox::TypeKind::SMALLINT: {
      return getRawSizeFromFixedWidthVector<int16_t>(vector);
    }
    case velox::TypeKind::INTEGER: {
      return getRawSizeFromFixedWidthVector<int32_t>(vector);
    }
    case velox::TypeKind::BIGINT: {
      return getRawSizeFromFixedWidthVector<int64_t>(vector);
    }
    case velox::TypeKind::REAL: {
      return getRawSizeFromFixedWidthVector<float>(vector);
    }
    case velox::TypeKind::DOUBLE: {
      return getRawSizeFromFixedWidthVector<double>(vector);
    }
    case velox::TypeKind::VARCHAR:
    case velox::TypeKind::VARBINARY: {
      return getRawSizeFromStringVector<velox::StringView>(vector);
    }
    default: {
      VELOX_FAIL("Unsupported type: {}", typeKind);
    }
  }
}

} // namespace facebook::nimble
