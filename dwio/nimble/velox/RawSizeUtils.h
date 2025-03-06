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

#include "velox/vector/BaseVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::nimble {

constexpr uint64_t NULL_SIZE = 1;

template <typename T>
uint64_t getRawSizeFromFixedWidthVector(const velox::VectorPtr& vector) {
  VELOX_DCHECK(
      (std::disjunction_v<
          std::is_same<bool, T>,
          std::is_same<int8_t, T>,
          std::is_same<int16_t, T>,
          std::is_same<int32_t, T>,
          std::is_same<int64_t, T>,
          std::is_same<float, T>,
          std::is_same<double, T>>),
      "Wrong vector type. Expected bool | int8_t | int16_t | int32_t | int64_t | float | double.");

  const auto& encoding = vector->encoding();
  switch (encoding) {
    case velox::VectorEncoding::Simple::FLAT: {
      auto* flatVector = vector->asFlatVector<T>();
      VELOX_CHECK_NOT_NULL(flatVector, "vector is null");

      const auto nullCount = velox::BaseVector::countNulls(
          flatVector->nulls(), flatVector->size());

      // Non null count * size in bytes + null count * null size
      return ((flatVector->size() - nullCount) *
              flatVector->type()->cppSizeInBytes()) +
          (nullCount * NULL_SIZE);
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      auto* constVector = vector->as<velox::ConstantVector<T>>();
      VELOX_CHECK_NOT_NULL(constVector, "vector is null");

      return constVector->mayHaveNulls()
          ? NULL_SIZE * constVector->size()
          : constVector->size() * constVector->type()->cppSizeInBytes();
    }
    default: {
      VELOX_FAIL("Unsupported encoding: {}", encoding);
    }
  }
}

template <typename T>
uint64_t getRawSizeFromStringVector(const velox::VectorPtr& vector) {
  VELOX_DCHECK(
      (std::is_same_v<velox::StringView, T>),
      "Wrong vector type. Expected StringView.");

  const auto& encoding = vector->encoding();
  switch (encoding) {
    case velox::VectorEncoding::Simple::FLAT: {
      auto* flatVector = vector->as<velox::FlatVector<T>>();
      VELOX_CHECK_NOT_NULL(flatVector, "vector is null");

      const auto nullCount = velox::BaseVector::countNulls(
          flatVector->nulls(), flatVector->size());

      const velox::StringView* stringValues = flatVector->rawValues();
      uint64_t rawSize = std::accumulate(
          stringValues,
          stringValues + flatVector->size(),
          uint64_t(0),
          [](uint64_t sum, const velox::StringView& str) {
            return sum + str.size();
          });

      return rawSize + (nullCount * NULL_SIZE);
    }
    case velox::VectorEncoding::Simple::CONSTANT: {
      auto* constVector = vector->as<velox::ConstantVector<T>>();
      VELOX_CHECK_NOT_NULL(constVector, "vector is null");

      return constVector->mayHaveNulls()
          ? NULL_SIZE * constVector->size()
          : constVector->value().size() * constVector->size();
    }
    default: {
      VELOX_FAIL("Unsupported encoding: {}", encoding);
    }
  }
}

uint64_t getRawSizeFromVector(const velox::VectorPtr& vector);

} // namespace facebook::nimble
