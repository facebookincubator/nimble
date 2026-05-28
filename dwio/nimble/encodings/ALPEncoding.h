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

#include <cmath>
#include <cstring>
#include <limits>
#include <span>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/common/EncodingType.h"
#include "dwio/nimble/encodings/selection/EncodingIdentifier.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"

/// ALP (Adaptive Lossless floating-Point) encoding for float/double.
///
///
/// Data layout after the standard Encoding prefix:
///   1 byte:  exponent (uint8)
///   1 byte:  factor   (uint8)
///   4 bytes: exceptionCount (uint32)
///   4 bytes: encodedValuesSize (uint32, size of nested encoding)
///   N bytes: nested encoding of int64 encoded values
///   exceptionCount * 4 bytes: exception positions (uint32 each)
///   exceptionCount * sizeof(physicalType) bytes: exception values

namespace facebook::nimble {

namespace detail::alp {

/// Pre-computed powers of 10 for double precision.
inline constexpr double kPow10Double[] = {
    1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,  1e10, 1e11,
    1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23,
};
inline constexpr int kMaxExponent = 23;
inline constexpr int kMaxFactor = 23;

/// ALP prefix: exponent(1) + factor(1) + exceptionCount(4) +
/// encodedValuesSize(4).
inline constexpr int kAlpPrefixSize = 10;

/// Sample up to this many values to find the best (exponent, factor) pair.
inline constexpr uint32_t kSampleSize = 1024;

/// Checks whether a double value round-trips losslessly through int64
/// encoding with the given (exponent, factor) pair.
template <typename FloatType>
inline bool roundTrips(FloatType value, int exponent, int factor) {
  double scaled = static_cast<double>(value) * kPow10Double[exponent];
  if (scaled < static_cast<double>(std::numeric_limits<int64_t>::min()) ||
      scaled > static_cast<double>(std::numeric_limits<int64_t>::max())) {
    return false;
  }
  int64_t encoded = static_cast<int64_t>(std::llround(scaled));

  double restored = static_cast<double>(encoded) / kPow10Double[exponent];
  if (factor > 0) {
    int64_t factored =
        static_cast<int64_t>(std::llround(scaled / kPow10Double[factor]));
    double fromFactored = static_cast<double>(factored) * kPow10Double[factor] /
        kPow10Double[exponent];
    restored = fromFactored;
  }

  return static_cast<FloatType>(restored) == value;
}

/// Finds the best (exponent, factor) pair that maximizes round-trip success
/// rate on the given sample.
template <typename FloatType>
inline std::pair<uint8_t, uint8_t> findBestExponentFactor(
    std::span<const FloatType> values) {
  uint32_t sampleSize =
      std::min(static_cast<uint32_t>(values.size()), kSampleSize);

  uint8_t bestE = 0;
  uint8_t bestF = 0;
  uint32_t bestCount = 0;

  for (int e = 0; e <= kMaxExponent; ++e) {
    uint32_t countNoFactor = 0;
    for (uint32_t i = 0; i < sampleSize; ++i) {
      if (roundTrips(values[i], e, /*factor=*/0)) {
        ++countNoFactor;
      }
    }
    if (countNoFactor > bestCount) {
      bestCount = countNoFactor;
      bestE = static_cast<uint8_t>(e);
      bestF = 0;
    }
    if (bestCount == sampleSize) {
      break;
    }

    for (int f = 1; f <= std::min(e, kMaxFactor); ++f) {
      uint32_t countWithFactor = 0;
      for (uint32_t i = 0; i < sampleSize; ++i) {
        if (roundTrips(values[i], e, f)) {
          ++countWithFactor;
        }
      }
      if (countWithFactor > bestCount) {
        bestCount = countWithFactor;
        bestE = static_cast<uint8_t>(e);
        bestF = static_cast<uint8_t>(f);
      }
      if (bestCount == sampleSize) {
        break;
      }
    }
    if (bestCount == sampleSize) {
      break;
    }
  }
  return {bestE, bestF};
}

/// Encodes a single value with the given (exponent, factor) pair.
inline int64_t encodeValue(double value, int exponent, int factor) {
  double scaled = value * kPow10Double[exponent];
  if (factor > 0) {
    return static_cast<int64_t>(std::llround(scaled / kPow10Double[factor]));
  }
  return static_cast<int64_t>(std::llround(scaled));
}

/// Decodes a single encoded integer back to a floating-point value.
template <typename FloatType>
inline FloatType decodeValue(int64_t encoded, int exponent, int factor) {
  if (factor > 0) {
    return static_cast<FloatType>(
        static_cast<double>(encoded) * kPow10Double[factor] /
        kPow10Double[exponent]);
  }
  return static_cast<FloatType>(
      static_cast<double>(encoded) / kPow10Double[exponent]);
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
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      std::function<void*(uint32_t)> /* stringBufferFactory */,
      const Encoding::Options& options = {})
      : TypedEncoding<T, physicalType>(memoryPool, data, options), pos_(0) {
    const char* pos = data.data() + this->dataOffset();

    exponent_ = encoding::read<uint8_t>(pos);
    factor_ = encoding::read<uint8_t>(pos);
    exceptionCount_ = encoding::readUint32(pos);
    const uint32_t encodedValuesSize = encoding::readUint32(pos);

    const EncodingFactory encodingFactory(options);
    auto nullStringBufferFactory = [](uint32_t) -> void* { return nullptr; };
    encodedValuesEncoding_ = encodingFactory.create(
        *this->pool_,
        std::string_view(pos, encodedValuesSize),
        nullStringBufferFactory);
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
      output[i] = detail::alp::decodeValue<physicalType>(
          static_cast<int64_t>(encodedBuffer_[pos_ + i]), exponent_, factor_);
    }

    patchExceptions(output, pos_, rowCount);
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
      physicalType value = detail::alp::decodeValue<physicalType>(
          static_cast<int64_t>(encodedBuffer_[pos_]), exponent_, factor_);
      auto exPos = reinterpret_cast<const uint32_t*>(exceptionPositions_);
      auto exVals = reinterpret_cast<const physicalType*>(exceptionValues_);
      for (uint32_t e = 0; e < exceptionCount_; ++e) {
        if (exPos[e] == pos_) {
          value = exVals[e];
          break;
        }
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
        toString(this->encodingType()),
        toString(this->dataType()),
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

    auto [exponent, factor] =
        detail::alp::findBestExponentFactor<physicalType>(values);

    Vector<uint64_t> encodedValues(&buffer.getMemoryPool(), rowCount);
    Vector<uint32_t> exceptionPositions(&buffer.getMemoryPool());
    Vector<physicalType> exceptionValues(&buffer.getMemoryPool());

    for (uint32_t i = 0; i < rowCount; ++i) {
      int64_t encoded = detail::alp::encodeValue(
          static_cast<double>(values[i]), exponent, factor);
      encodedValues[i] = static_cast<uint64_t>(encoded);

      physicalType restored =
          detail::alp::decodeValue<physicalType>(encoded, exponent, factor);
      if (restored != values[i]) {
        exceptionPositions.push_back(i);
        exceptionValues.push_back(values[i]);
      }
    }

    const uint32_t exceptionCount = exceptionPositions.size();

    Buffer tempBuffer{buffer.getMemoryPool()};
    std::string_view serializedEncoded =
        selection.template encodeNested<uint64_t>(
            EncodingIdentifiers::ALP::EncodedValues,
            {encodedValues},
            tempBuffer,
            options);

    const uint32_t exceptionPositionsSize = exceptionCount * sizeof(uint32_t);
    const uint32_t exceptionValuesSize = exceptionCount * sizeof(physicalType);
    const uint32_t encodingSize =
        Encoding::serializePrefixSize(rowCount, useVarint) +
        detail::alp::kAlpPrefixSize + serializedEncoded.size() +
        exceptionPositionsSize + exceptionValuesSize;

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

    NIMBLE_DCHECK_EQ(encodingSize, pos - reserved, "Encoding size mismatch.");
    return {reserved, encodingSize};
  }

 private:
  uint8_t exponent_;
  uint8_t factor_;
  uint32_t exceptionCount_;
  std::unique_ptr<Encoding> encodedValuesEncoding_;
  const char* exceptionPositions_;
  const char* exceptionValues_;
  std::vector<uint64_t> encodedBuffer_;
  uint32_t pos_;

  void
  patchExceptions(physicalType* output, uint32_t startRow, uint32_t rowCount) {
    auto exPos = reinterpret_cast<const uint32_t*>(exceptionPositions_);
    auto exVals = reinterpret_cast<const physicalType*>(exceptionValues_);
    for (uint32_t e = 0; e < exceptionCount_; ++e) {
      uint32_t absPos = exPos[e];
      if (absPos >= startRow && absPos < startRow + rowCount) {
        output[absPos - startRow] = exVals[e];
      }
    }
  }
};

} // namespace facebook::nimble
