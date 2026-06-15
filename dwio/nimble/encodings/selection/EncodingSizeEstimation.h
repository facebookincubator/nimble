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

#include <glog/logging.h>
#include <algorithm>
#include <cmath>
#include <optional>
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#ifdef NIMBLE_ENABLE_EXPERIMENTAL_ENCODINGS
#include "dwio/nimble/encodings/BlockBitPackingEncoding.h"
#endif
#include "dwio/nimble/encodings/ConstantEncoding.h"
#include "dwio/nimble/encodings/DictionaryEncoding.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/MainlyConstantEncoding.h"
#include "dwio/nimble/encodings/PFOREncoding.h"
#include "dwio/nimble/encodings/RLEEncoding.h"
#include "dwio/nimble/encodings/SimdForBitpackEncoding.h"
#include "dwio/nimble/encodings/SparseBoolEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/VarintEncoding.h"

namespace facebook::nimble {
namespace detail {

// This class is meant to quickly estimate the size of encoded data using a
// given encoding type. It does a lot of assumptions, and it is not meant to be
// 100% accurate.
template <typename T, bool FixedByteWidth>
struct EncodingSizeEstimation {
  using physicalType = typename TypeTraits<T>::physicalType;

  // TODO: Add EncodingType::ALP size estimation case once the actual ALP
  // algorithm is implemented.
  static std::optional<uint64_t> estimateNumericSize(
      const EncodingType encodingType,
      const uint64_t entryCount,
      const Statistics<physicalType>& statistics) {
    switch (encodingType) {
      case EncodingType::Constant: {
        return ConstantEncoding<physicalType>::estimateSize(statistics);
      }
      case EncodingType::MainlyConstant: {
        return MainlyConstantEncoding<physicalType>::estimateSize(
            entryCount, statistics, FixedByteWidth);
      }
      case EncodingType::Trivial: {
        return TrivialEncoding<physicalType>::estimateSize(entryCount);
      }
      case EncodingType::FixedBitWidth: {
        return FixedBitWidthEncoding<physicalType>::estimateSize(
            entryCount, statistics, FixedByteWidth);
      }
      case EncodingType::Dictionary: {
        return DictionaryEncoding<physicalType>::estimateSize(
            entryCount, statistics, FixedByteWidth);
      }
      case EncodingType::RLE: {
        return RLEEncoding<physicalType>::estimateSize(
            entryCount, statistics, FixedByteWidth);
      }
      case EncodingType::Varint: {
        // Note: the condition below actually support floating point numbers as
        // well, as we use physicalType, which, for floating point numbers, is
        // an integer.
        if constexpr (isIntegralType<physicalType>() && sizeof(T) >= 4) {
          return VarintEncoding<physicalType>::estimateSize(statistics);
        } else {
          return std::nullopt;
        }
      }
      case EncodingType::PFOR: {
        if constexpr (isIntegralType<physicalType>()) {
          return PFOREncoding<physicalType>::estimateSize(
              entryCount, statistics);
        } else {
          return std::nullopt;
        }
      }
      case EncodingType::SimdForBitpack: {
        if constexpr (isIntegralType<physicalType>()) {
          return SimdForBitpackEncoding<physicalType>::estimateSize(
              entryCount, statistics.min(), statistics.max());
        } else {
          return std::nullopt;
        }
      }
#ifdef NIMBLE_ENABLE_EXPERIMENTAL_ENCODINGS
      case EncodingType::BlockBitPacking: {
        if constexpr (isIntegralType<physicalType>()) {
          return BlockBitPackingEncoding<physicalType>::estimateSize(
              entryCount, statistics);
        } else {
          return std::nullopt;
        }
      }
#endif
      // SubIntSplit integration (re-enabled for
      // NIMBLE_ENABLE_EXPERIMENTAL_ENCODINGS; was commented out by #636):
#ifdef NIMBLE_ENABLE_EXPERIMENTAL_ENCODINGS
      case EncodingType::SubIntSplit: {
        if constexpr (
            isNumericType<physicalType>() &&
            (sizeof(physicalType) == 4 || sizeof(physicalType) == 8)) {
          constexpr uint64_t kTypeWidthBits =
              static_cast<uint64_t>(sizeof(physicalType)) * 8u;
          const uint64_t rangeBits =
              velox::bits::bitsRequired(statistics.max() - statistics.min());
          if (rangeBits > (kTypeWidthBits * 3) / 4) {
            return std::nullopt;
          }
          const auto fbwEst = estimateNumericSize(
              EncodingType::FixedBitWidth, entryCount, statistics);
          if (!fbwEst.has_value()) {
            return std::nullopt;
          }
          constexpr uint64_t kOverheadBytes = 6u + 2u + 4u * 6u + 4u * 8u;
          const uint64_t estimate =
              static_cast<uint64_t>(
                  static_cast<double>(fbwEst.value()) * 0.90) +
              kOverheadBytes;
          return estimate;
        } else {
          return std::nullopt;
        }
      }
#endif
      default: {
        return std::nullopt;
      }
    }
  }

  static std::optional<uint64_t> estimateBoolSize(
      const EncodingType encodingType,
      const size_t entryCount,
      const Statistics<physicalType>& statistics) {
    switch (encodingType) {
      case EncodingType::Constant: {
        return ConstantEncoding<bool>::estimateSize(statistics);
      }
      case EncodingType::SparseBool: {
        return SparseBoolEncoding::estimateSize(
            entryCount, statistics, FixedByteWidth);
      }
      case EncodingType::Trivial: {
        return TrivialEncoding<bool>::estimateSize(entryCount);
      }
      case EncodingType::RLE: {
        return RLEEncoding<bool>::estimateSize(
            entryCount, statistics, FixedByteWidth);
      }
      default: {
        return std::nullopt;
      }
    }
  }

  static std::optional<uint64_t> estimateStringSize(
      const EncodingType encodingType,
      const size_t entryCount,
      const Statistics<std::string_view>& statistics) {
    switch (encodingType) {
      case EncodingType::Constant: {
        return ConstantEncoding<std::string_view>::estimateSize(statistics);
      }
      case EncodingType::MainlyConstant: {
        return MainlyConstantEncoding<std::string_view>::estimateSize(
            entryCount, statistics, FixedByteWidth);
      }
      case EncodingType::Trivial: {
        return TrivialEncoding<std::string_view>::estimateSize(
            entryCount, statistics, FixedByteWidth);
      }
      case EncodingType::Dictionary: {
        return DictionaryEncoding<std::string_view>::estimateSize(
            entryCount, statistics, FixedByteWidth);
      }
      case EncodingType::RLE: {
        return RLEEncoding<std::string_view>::estimateSize(
            entryCount, statistics, FixedByteWidth);
      }
      default: {
        return std::nullopt;
      }
    }
  }

  static std::optional<uint64_t> estimateSize(
      const EncodingType encodingType,
      const size_t entryCount,
      const Statistics<physicalType>& statistics) {
    if constexpr (isNumericType<physicalType>()) {
      return estimateNumericSize(encodingType, entryCount, statistics);
    } else if constexpr (isBoolType<physicalType>()) {
      return estimateBoolSize(encodingType, entryCount, statistics);
    } else if constexpr (isStringType<physicalType>()) {
      return estimateStringSize(encodingType, entryCount, statistics);
    }

    NIMBLE_UNREACHABLE(
        "Unable to estimate size for type {}.", folly::demangle(typeid(T)));
  }
};
} // namespace detail
} // namespace facebook::nimble
