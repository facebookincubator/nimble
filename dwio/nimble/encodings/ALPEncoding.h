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

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <vector>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/common/EncodingType.h"
#include "dwio/nimble/encodings/selection/EncodingIdentifier.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"
#include "velox/common/encode/Coding.h"

/// ALP (Adaptive Lossless floating-Point) encoding for float/double.
///
///
/// Data layout after the standard Encoding prefix:
///   1 byte:  exponent (uint8)
///   1 byte:  factor   (uint8)
///   4 bytes: exceptionCount (uint32)
///   4 bytes: encodedValuesSize (uint32, size of nested encoding)
///   N bytes: nested encoding of ZigZag-coded signed encoded values
///   exceptionCount * 4 bytes: exception positions (uint32 each)
///   exceptionCount * sizeof(physicalType) bytes: exception values

namespace facebook::nimble {

namespace detail::alp {

template <typename FloatType>
inline FloatType toLogical(
    typename TypeTraits<FloatType>::physicalType physicalValue) {
  return EncodingPhysicalType<FloatType>::asEncodingLogicalType(physicalValue);
}

template <typename FloatType>
inline typename TypeTraits<FloatType>::physicalType toPhysical(
    FloatType logicalValue) {
  return EncodingPhysicalType<FloatType>::asEncodingPhysicalType(logicalValue);
}

} // namespace detail::alp

template <typename T>
class ALPEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
  static_assert(
      isFloatingPointType<T>(),
      "ALPEncoding only supports float and double types.");

 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  ALPEncoding(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      std::function<void*(uint32_t)> /* stringBufferFactory */,
      const Encoding::Options& options = {})
      : TypedEncoding<T, physicalType>(pool, data, options), pos_(0) {
    const char* pos = data.data() + this->dataOffset();

    exponent_ = encoding::read<uint8_t>(pos);
    factor_ = encoding::read<uint8_t>(pos);
    exceptionCount_ = encoding::readUint32(pos);
    const uint32_t encodedValuesSize = encoding::readUint32(pos);

    const EncodingFactory encodingFactory(options);
    auto noStringBufferFactory = [](uint32_t) -> void* { return nullptr; };
    encodedValuesEncoding_ = encodingFactory.create(
        *this->pool_,
        std::string_view(pos, encodedValuesSize),
        noStringBufferFactory);
    pos += encodedValuesSize;

    exceptionPositions_ = pos;
    pos += exceptionCount_ * sizeof(uint32_t);
    exceptionValues_ = pos;

    encodedBuffer_.resize(this->rowCount());
    encodedValuesEncoding_->materialize(
        this->rowCount(), encodedBuffer_.data());
  }

  void reset() final {
    pos_ = 0;
  }

  void skip(uint32_t rowCount) final {
    pos_ += rowCount;
  }

  void materialize(uint32_t rowCount, void* buffer) final {
    auto* output = static_cast<physicalType*>(buffer);
    for (uint32_t i = 0; i < rowCount; ++i) {
      const auto encoded = velox::ZigZag::decode(encodedBuffer_[pos_ + i]);
      output[i] = detail::alp::toPhysical<cppDataType>(
          decodeValue(encoded, exponent_, factor_));
    }

    patchExceptions(pos_, rowCount, output);
    pos_ += rowCount;
  }

  void materializeBoolsAsBits(
      uint32_t /*rowCount*/,
      uint64_t* /*buffer*/,
      int /*begin*/) final {
    NIMBLE_UNREACHABLE("ALP encoding does not support bool type.");
  }

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params) {
    auto skipFn = [&](auto toSkip) { pos_ += toSkip; };
    auto decodeFn = [&] {
      physicalType value = detail::alp::toPhysical<cppDataType>(decodeValue(
          velox::ZigZag::decode(encodedBuffer_[pos_]), exponent_, factor_));
      if (const auto* exceptionValue = findException(pos_)) {
        value = *exceptionValue;
      }
      ++pos_;
      return value;
    };
    detail::readWithVisitorSlow(visitor, params, skipFn, decodeFn);
  }

  std::string debugString(int offset) const final {
    return fmt::format(
        "{}{}<{}> rowCount={} exponent={} factor={} exceptions={}",
        std::string(offset, ' '),
        this->encodingType(),
        this->dataType(),
        this->rowCount(),
        exponent_,
        factor_,
        exceptionCount_);
  }

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {}) {
    const bool useVarint = options.useVarintRowCount;
    if (values.empty()) {
      NIMBLE_INCOMPATIBLE_ENCODING("ALP encoding cannot encode empty data.");
    }

    const uint32_t rowCount = values.size();
    auto* pool = &buffer.getMemoryPool();

    Vector<cppDataType> logicalValues(pool, rowCount);
    for (uint32_t i = 0; i < rowCount; ++i) {
      logicalValues[i] = detail::alp::toLogical<cppDataType>(values[i]);
    }

    const auto [exponent, factor] = findBestExponentFactor(
        std::span<const cppDataType>{
            logicalValues.data(), logicalValues.size()});

    Vector<uint64_t> encodedValues(pool, rowCount);
    Vector<uint32_t> exceptionPositions(pool);
    Vector<physicalType> exceptionValues(pool);

    for (uint32_t i = 0; i < rowCount; ++i) {
      if (!canRepresentExactly(logicalValues[i], values[i], exponent, factor)) {
        encodedValues[i] = 0;
        exceptionPositions.push_back(i);
        exceptionValues.push_back(values[i]);
        continue;
      }

      const auto encoded =
          encodeValue(static_cast<double>(logicalValues[i]), exponent, factor);
      encodedValues[i] = velox::ZigZag::encode(encoded);
    }

    const uint32_t exceptionCount = exceptionPositions.size();

    Buffer tempBuffer{*pool};
    std::string_view serializedEncoded =
        selection.template encodeNested<uint64_t>(
            EncodingIdentifiers::ALP::EncodedValues,
            {encodedValues},
            tempBuffer,
            options);

    const uint32_t exceptionPositionsSize = exceptionCount * sizeof(uint32_t);
    const uint32_t exceptionValuesSize = exceptionCount * sizeof(physicalType);
    const uint32_t encodingSize =
        Encoding::serializePrefixSize(rowCount, useVarint) + kAlpPrefixSize +
        serializedEncoded.size() + exceptionPositionsSize + exceptionValuesSize;

    char* reserved = buffer.reserve(encodingSize);
    char* pos = reserved;
    Encoding::serializePrefix(
        EncodingType::ALP, TypeTraits<T>::dataType, rowCount, useVarint, pos);

    encoding::write<uint8_t>(exponent, pos);
    encoding::write<uint8_t>(factor, pos);
    encoding::writeUint32(exceptionCount, pos);
    encoding::writeUint32(serializedEncoded.size(), pos);
    encoding::writeBytes(serializedEncoded, pos);

    if (exceptionCount > 0) {
      std::memcpy(pos, exceptionPositions.data(), exceptionPositionsSize);
      pos += exceptionPositionsSize;
      std::memcpy(pos, exceptionValues.data(), exceptionValuesSize);
      pos += exceptionValuesSize;
    }

    NIMBLE_CHECK_EQ(encodingSize, pos - reserved, "Encoding size mismatch.");
    return {reserved, encodingSize};
  }

  static std::optional<uint64_t> estimateSize(
      std::span<const physicalType> values,
      const Encoding::Options& options = {}) {
    if (values.empty()) {
      return std::nullopt;
    }

    const uint64_t rowCount = values.size();
    const uint32_t sampleSize =
        std::min(static_cast<uint32_t>(rowCount), kSampleSize);

    std::vector<cppDataType> logicalValues;
    logicalValues.reserve(sampleSize);
    std::vector<physicalType> sampledValues;
    sampledValues.reserve(sampleSize);
    for (uint32_t i = 0; i < sampleSize; ++i) {
      const auto value = values[sampledValueIndex(i, rowCount, sampleSize)];
      logicalValues.push_back(detail::alp::toLogical<cppDataType>(value));
      sampledValues.push_back(value);
    }

    const auto [exponent, factor] = findBestExponentFactor(
        std::span<const cppDataType>{
            logicalValues.data(), logicalValues.size()});

    std::vector<uint64_t> encodedValues;
    encodedValues.reserve(sampleSize);
    uint64_t sampleExceptionCount{0};
    for (auto i = 0; i < sampleSize; ++i) {
      if (!canRepresentExactly(
              logicalValues[i], sampledValues[i], exponent, factor)) {
        encodedValues.push_back(0);
        ++sampleExceptionCount;
        continue;
      }

      const auto encoded =
          encodeValue(static_cast<double>(logicalValues[i]), exponent, factor);
      encodedValues.push_back(velox::ZigZag::encode(encoded));
    }

    const auto encodedStats = Statistics<uint64_t>::create(
        std::span<const uint64_t>{encodedValues.data(), encodedValues.size()});
    // Match existing nested-stream estimators: use FixedBitWidth as a simple
    // representative estimate instead of recursively modeling nested selection.
    const uint64_t nestedEncodedValuesSize =
        FixedBitWidthEncoding<uint64_t>::estimateSize(
            rowCount, encodedStats, options);
    const uint64_t exceptionCount =
        (sampleExceptionCount * rowCount + sampleSize - 1) / sampleSize;
    const uint64_t exceptionPositionsSize = exceptionCount * sizeof(uint32_t);
    const uint64_t exceptionValuesSize = exceptionCount * sizeof(physicalType);
    return Encoding::serializePrefixSize(
               static_cast<uint32_t>(rowCount), options.useVarintRowCount) +
        kAlpPrefixSize + nestedEncodedValuesSize + exceptionPositionsSize +
        exceptionValuesSize;
  }

 private:
  // Pre-computed powers of 10 for double precision.
  static constexpr std::array<double, 24> kPow10Double{
      1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,  1e10, 1e11,
      1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23};
  // Largest exponent and factor values backed by kPow10Double.
  static constexpr int kMaxExponent{23};
  static constexpr int kMaxFactor{23};
  // ALP prefix: exponent(1) + factor(1) + exceptionCount(4) +
  // encodedValuesSize(4).
  static constexpr int kAlpPrefixSize{10};
  // Sample up to this many values to find the best (exponent, factor) pair.
  static constexpr uint32_t kSampleSize{1024};

  // Maps a dense sample ordinal to an evenly spaced input row.
  static uint64_t sampledValueIndex(
      uint32_t sampleIndex,
      uint64_t rowCount,
      uint32_t sampleSize) {
    return sampleIndex * rowCount / sampleSize;
  }

  // Checks whether the selected ALP transform can encode the value without an
  // exception.
  static bool canRepresentExactly(
      cppDataType value,
      physicalType physicalValue,
      int exponent,
      int factor) {
    const double exponentMultiplier = kPow10Double[exponent];
    const double factorMultiplier = kPow10Double[factor];
    const double scaled = static_cast<double>(value) * exponentMultiplier;
    if (!std::isfinite(scaled)) {
      return false;
    }
    if (scaled < static_cast<double>(std::numeric_limits<int64_t>::min()) ||
        scaled > static_cast<double>(std::numeric_limits<int64_t>::max())) {
      return false;
    }
    const int64_t factored =
        static_cast<int64_t>(std::llround(scaled / factorMultiplier));
    const double restored =
        static_cast<double>(factored) * factorMultiplier / exponentMultiplier;

    return detail::alp::toPhysical<cppDataType>(
               static_cast<cppDataType>(restored)) == physicalValue;
  }

  // Selects the sampled (exponent, factor) pair that preserves the most values.
  static std::pair<uint8_t, uint8_t> findBestExponentFactor(
      std::span<const cppDataType> values) {
    const uint32_t sampleSize =
        std::min(static_cast<uint32_t>(values.size()), kSampleSize);

    uint8_t bestExponent = 0;
    uint8_t bestFactor = 0;
    uint32_t bestRepresentableCount = 0;

    for (int e = 0; e <= kMaxExponent; ++e) {
      uint32_t countNoFactor = 0;
      for (uint32_t i = 0; i < sampleSize; ++i) {
        if (canRepresentExactly(
                values[i],
                detail::alp::toPhysical<cppDataType>(values[i]),
                e,
                /*factor=*/0)) {
          ++countNoFactor;
        }
      }
      if (countNoFactor > bestRepresentableCount) {
        bestRepresentableCount = countNoFactor;
        bestExponent = static_cast<uint8_t>(e);
        bestFactor = 0;
      }
      if (bestRepresentableCount == sampleSize) {
        break;
      }

      for (int f = 1; f <= std::min(e, kMaxFactor); ++f) {
        uint32_t countWithFactor = 0;
        for (uint32_t i = 0; i < sampleSize; ++i) {
          if (canRepresentExactly(
                  values[i],
                  detail::alp::toPhysical<cppDataType>(values[i]),
                  e,
                  f)) {
            ++countWithFactor;
          }
        }
        if (countWithFactor > bestRepresentableCount) {
          bestRepresentableCount = countWithFactor;
          bestExponent = static_cast<uint8_t>(e);
          bestFactor = static_cast<uint8_t>(f);
        }
        if (bestRepresentableCount == sampleSize) {
          break;
        }
      }
      if (bestRepresentableCount == sampleSize) {
        break;
      }
    }
    return {bestExponent, bestFactor};
  }

  // Converts a floating-point value to the integer stored by ALP.
  static int64_t encodeValue(double value, int exponent, int factor) {
    const double scaled = value * kPow10Double[exponent];
    return static_cast<int64_t>(std::llround(scaled / kPow10Double[factor]));
  }

  // Reconstructs a floating-point value from an ALP integer.
  static cppDataType decodeValue(int64_t encoded, int exponent, int factor) {
    return static_cast<cppDataType>(
        static_cast<double>(encoded) * kPow10Double[factor] /
        kPow10Double[exponent]);
  }

  void
  patchExceptions(uint32_t startRow, uint32_t rowCount, physicalType* output) {
    const auto* exPos = reinterpret_cast<const uint32_t*>(exceptionPositions_);
    const auto* exVals =
        reinterpret_cast<const physicalType*>(exceptionValues_);
    const auto* exBegin = exPos;
    const auto* exEnd = exPos + exceptionCount_;
    const auto* first = std::lower_bound(exBegin, exEnd, startRow);
    const auto* last = std::lower_bound(first, exEnd, startRow + rowCount);
    for (auto* it = first; it != last; ++it) {
      const uint32_t absPos = *it;
      output[absPos - startRow] = exVals[it - exBegin];
    }
  }

  const physicalType* findException(uint32_t row) const {
    const auto* exPos = reinterpret_cast<const uint32_t*>(exceptionPositions_);
    const auto* exVals =
        reinterpret_cast<const physicalType*>(exceptionValues_);
    const auto* exBegin = exPos;
    const auto* exEnd = exPos + exceptionCount_;
    const auto* it = std::lower_bound(exBegin, exEnd, row);
    if (it == exEnd || *it != row) {
      return nullptr;
    }
    return exVals + (it - exBegin);
  }

  uint8_t exponent_;
  uint8_t factor_;
  uint32_t exceptionCount_;
  std::unique_ptr<Encoding> encodedValuesEncoding_;
  const char* exceptionPositions_;
  const char* exceptionValues_;
  std::vector<uint64_t> encodedBuffer_;
  uint32_t pos_;
};

} // namespace facebook::nimble
