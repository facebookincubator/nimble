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
#include "dwio/nimble/common/Varint.h"
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
///   3 bytes: ALP control word:
///     bits 0..4: exponent
///     bits 5..9: factor
///     bits 10..15: reserved
///     bit 16: has exceptions
///     bits 17..23: reserved
///   1-5 bytes: exceptionCount (varint, present only when has exceptions)
///   1-5 bytes: encodedValuesSize (varint uint32, size of nested encoding)
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

struct Header {
  // Parameters used to transform floating-point values into integers.
  uint8_t exponent{0};
  uint8_t factor{0};

  // Whether the optional exception count and exception streams are present.
  bool hasExceptions{false};
};

inline Header readHeader(const char*& pos) {
  const auto control = static_cast<uint32_t>(
      static_cast<uint8_t>(pos[0]) | (static_cast<uint8_t>(pos[1]) << 8) |
      (static_cast<uint8_t>(pos[2]) << 16));
  pos += 3;
  return Header{
      .exponent = static_cast<uint8_t>(control & 0x1f),
      .factor = static_cast<uint8_t>((control >> 5) & 0x1f),
      .hasExceptions = (control & (1u << 16)) != 0};
}

inline void writeHeader(const Header& header, char*& pos) {
  NIMBLE_DCHECK_LE(header.exponent, 31);
  NIMBLE_DCHECK_LE(header.factor, 31);
  uint32_t control = header.exponent | (header.factor << 5);
  if (header.hasExceptions) {
    control |= 1u << 16;
  }
  pos[0] = static_cast<char>(control & 0xff);
  pos[1] = static_cast<char>((control >> 8) & 0xff);
  pos[2] = static_cast<char>((control >> 16) & 0xff);
  pos += 3;
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

    const auto header = detail::alp::readHeader(pos);
    exponent_ = header.exponent;
    factor_ = header.factor;
    exceptionCount_ = header.hasExceptions ? varint::readVarint32(&pos) : 0;
    const uint32_t encodedValuesSize = varint::readVarint32(&pos);

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

    ScopedEncodingBuffer scopedBuffer{
        &buffer.getMemoryPool(), options.encodingBufferPool};
    std::string_view serializedEncoded =
        selection.template encodeNested<uint64_t>(
            EncodingIdentifiers::ALP::EncodedValues,
            {encodedValues},
            scopedBuffer.get(),
            options);

    const uint32_t exceptionPositionsSize = exceptionCount * sizeof(uint32_t);
    const uint32_t exceptionValuesSize = exceptionCount * sizeof(physicalType);
    const uint32_t metadataSize = kHeaderSize +
        (exceptionCount > 0 ? varint::varintSize(exceptionCount) : 0) +
        varint::varintSize(serializedEncoded.size());
    const uint32_t encodingSize =
        Encoding::serializePrefixSize(rowCount, useVarint) + metadataSize +
        serializedEncoded.size() + exceptionPositionsSize + exceptionValuesSize;

    char* reserved = buffer.reserve(encodingSize);
    char* pos = reserved;
    Encoding::serializePrefix(
        EncodingType::ALP, TypeTraits<T>::dataType, rowCount, useVarint, pos);

    detail::alp::writeHeader(
        detail::alp::Header{
            .exponent = exponent,
            .factor = factor,
            .hasExceptions = exceptionCount > 0},
        pos);
    if (exceptionCount > 0) {
      varint::writeVarint(exceptionCount, &pos);
    }
    varint::writeVarint(serializedEncoded.size(), &pos);
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
    const uint32_t sampleSize = estimateSampleSize(rowCount);

    std::vector<physicalType> sampledValues;
    sampledValues.reserve(sampleSize);
    // Select evenly spaced input positions without accumulating rounding error.
    for (uint32_t i = 0; i < sampleSize; ++i) {
      const auto inputIndex = sampledValueIndex(i, rowCount, sampleSize);
      sampledValues.push_back(values[inputIndex]);
    }

    return estimateSizeFromSample(rowCount, sampledValues, options);
  }

  static std::optional<uint64_t> estimateSizeFromSample(
      uint64_t rowCount,
      std::span<const physicalType> sampledValues,
      const Encoding::Options& options = {}) {
    NIMBLE_CHECK_GT(rowCount, 0, "ALP estimation requires non-empty input.");
    NIMBLE_CHECK(
        !sampledValues.empty(), "ALP estimation requires a non-empty sample.");
    NIMBLE_CHECK_LE(
        sampledValues.size(),
        rowCount,
        "ALP sample size cannot exceed the input row count.");

    const uint64_t sampleSize = sampledValues.size();

    std::vector<cppDataType> logicalValues;
    logicalValues.reserve(sampleSize);
    for (const auto value : sampledValues) {
      logicalValues.push_back(detail::alp::toLogical<cppDataType>(value));
    }

    const auto [exponent, factor] = findBestExponentFactor(
        std::span<const cppDataType>{
            logicalValues.data(), logicalValues.size()});

    std::vector<uint64_t> encodedValues;
    encodedValues.reserve(sampleSize);
    uint64_t sampleExceptionCount{0};
    for (auto i = 0; i < sampleSize; ++i) {
      if (!canRepresentExactly(
              logicalValues[i],
              sampledValues[static_cast<size_t>(i)],
              exponent,
              factor)) {
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
    // Model the inexpensive scalar candidates without recursively estimating
    // complex nested encodings.
    const uint64_t nestedEncodedValuesSize = std::min(
        FixedBitWidthEncoding<uint64_t>::estimateSize(
            rowCount, encodedStats, options),
        TrivialEncoding<uint64_t>::estimateSize(rowCount));
    const uint64_t exceptionCount =
        (sampleExceptionCount * rowCount + sampleSize - 1) / sampleSize;
    const uint64_t exceptionPositionsSize = exceptionCount * sizeof(uint32_t);
    const uint64_t exceptionValuesSize = exceptionCount * sizeof(physicalType);
    const uint64_t metadataSize = kHeaderSize +
        (exceptionCount > 0 ? varint::varintSize(exceptionCount) : 0) +
        varint::varintSize(nestedEncodedValuesSize);
    return Encoding::serializePrefixSize(
               static_cast<uint32_t>(rowCount), options.useVarintRowCount) +
        metadataSize + nestedEncodedValuesSize + exceptionPositionsSize +
        exceptionValuesSize;
  }

  static uint32_t estimateSampleSize(uint64_t rowCount) {
    return std::min(static_cast<uint32_t>(rowCount), kSampleSize);
  }

  /// Maps a dense sample ordinal to an evenly spaced input row.
  static uint64_t sampledValueIndex(
      uint32_t sampleIndex,
      uint64_t rowCount,
      uint32_t sampleSize) {
    return sampleIndex * rowCount / sampleSize;
  }

  // Pre-computed powers of 10 for double precision.
  static constexpr std::array<double, 24> kPow10Double{
      1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,  1e10, 1e11,
      1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23};

 private:
  // Largest exponent and factor values backed by kPow10Double.
  static constexpr int kMaxExponent{23};
  static constexpr int kMaxFactor{23};
  // ALP-specific control word following the standard Encoding prefix.
  static constexpr uint32_t kHeaderSize{3};
  // Sample up to this many values to find the best (exponent, factor) pair.
  static constexpr uint32_t kSampleSize{1024};

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
    const auto* exVals =
        reinterpret_cast<const physicalType*>(exceptionValues_);
    const auto* exBegin =
        reinterpret_cast<const uint32_t*>(exceptionPositions_);
    const auto* exEnd = exBegin + exceptionCount_;
    const auto* first = std::lower_bound(exBegin, exEnd, startRow);
    const auto* last = std::lower_bound(first, exEnd, startRow + rowCount);
    for (auto* it = first; it != last; ++it) {
      const uint32_t absPos = *it;
      output[absPos - startRow] = exVals[it - exBegin];
    }
  }

  const physicalType* findException(uint32_t row) const {
    const auto* exVals =
        reinterpret_cast<const physicalType*>(exceptionValues_);
    const auto* exBegin =
        reinterpret_cast<const uint32_t*>(exceptionPositions_);
    const auto* exEnd = exBegin + exceptionCount_;
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
