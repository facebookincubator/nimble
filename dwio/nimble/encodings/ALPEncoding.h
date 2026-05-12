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
#include <span>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingIdentifier.h"
#include "dwio/nimble/encodings/EncodingSelection.h"

// Adaptive Lossless floating-Point (ALP) encoding for float and double streams.
//
// Many real-world floating-point columns (prices, sensor readings, coordinates)
// have limited decimal precision. ALP exploits this by multiplying each value
// by 10^exponent, rounding to an integer, and storing the integers with a
// compact sub-encoding (e.g. FixedBitWidth or Delta). On decode, each integer
// is divided back by 10^exponent to recover the original float.
//
// Values that do not roundtrip exactly (non-finite values, or values whose
// decimal precision exceeds the chosen exponent) are stored separately as
// "exceptions" — their raw bit patterns are preserved in a side stream.
// The encoding is rejected if exceptions exceed 5% of values.
//
// As an example, consider the data:
//
//   1.23  4.56  7.89  INF  0.01
//
// With exponent=2 (factor=100):
//   Encoded integers:  123  456  789  0    1
//   Exception mask:    F    F    F    T    F
//   Exception values:               INF
//
// The encoded integers are stored via a nested sub-encoding. The exception
// mask is stored as a bool sub-encoding. The exception values are stored as
// raw physicalType values in a third sub-encoding.
//
// Three sub-streams are produced:
//   [0] EncodedValues:    integer-encoded values (uint64_t)
//   [1] ExceptionIndices: per-row bool mask (true = exception)
//   [2] ExceptionValues:  raw float/double bits for exceptions
//
// When exceptionCount == 0, streams [1] and [2] are omitted entirely, and
// decode takes a fast path that avoids the exception-patching loop.
//
// References:
//   Afroozeh et al., "ALP: Adaptive Lossless floating-Point Compression"
//   (SIGMOD 2023).

namespace facebook::nimble {

/// Internal helpers for ALP exponent search and value encoding.
namespace alp {

/// Reciprocal of the maximum allowed exception fraction. A value of 20 means
/// at most 1/20 = 5% of rows may be exceptions before the encoding is rejected.
constexpr uint64_t kMaxExceptionRate = 20;

// Maximum exponent to try when searching for the best encoding factor.
// For float: 7 decimal digits of precision.
// For double: 15 decimal digits of precision.
template <typename T>
constexpr int maxExponent() {
  if constexpr (std::is_same_v<T, float>) {
    return 7;
  } else {
    return 15;
  }
}

/// Integer type used to store encoded values. Both float and double use int64_t
/// to avoid overflow when multiplying by large powers of 10.
template <typename T>
using EncodedIntType = int64_t;

/// Precomputed powers of 10 for exponents 0..15.
inline const double kPowersOf10[] = {
    1e0,
    1e1,
    1e2,
    1e3,
    1e4,
    1e5,
    1e6,
    1e7,
    1e8,
    1e9,
    1e10,
    1e11,
    1e12,
    1e13,
    1e14,
    1e15};

/// Try encoding a single value with a given exponent. Multiplies by
/// 10^exponent, rounds to integer, then divides back to verify a bit-exact
/// roundtrip. Returns true if the value survives the roundtrip; on success,
/// writes the integer result to `encoded`.
template <typename T>
bool tryEncode(T value, int exponent, EncodedIntType<T>& encoded) {
  double factor = kPowersOf10[exponent];
  double scaled = static_cast<double>(value) * factor;

  // Check for overflow.
  if (scaled > static_cast<double>(std::numeric_limits<int64_t>::max()) ||
      scaled < static_cast<double>(std::numeric_limits<int64_t>::min())) {
    return false;
  }

  encoded = static_cast<int64_t>(std::llround(scaled));
  T decoded = static_cast<T>(static_cast<double>(encoded) / factor);
  return decoded == value;
}

/// Sweep exponents 0..maxExponent<T>() and return the one that produces the
/// fewest exceptions. Returns {bestExponent, exceptionCount}. Exits early if
/// an exponent with zero exceptions is found.
template <typename T>
std::pair<int, uint32_t> findBestExponent(std::span<const T> values) {
  int bestExponent = 0;
  uint32_t bestExceptions = values.size();

  for (int e = 0; e <= maxExponent<T>(); ++e) {
    uint32_t exceptions = 0;
    EncodedIntType<T> dummy;
    for (const auto& v : values) {
      if (!std::isfinite(v) || !tryEncode(v, e, dummy)) {
        ++exceptions;
      }
    }
    if (exceptions < bestExceptions) {
      bestExceptions = exceptions;
      bestExponent = e;
      if (exceptions == 0) {
        break;
      }
    }
  }
  return {bestExponent, bestExceptions};
}

} // namespace alp

/// Serialized data layout:
///   Encoding prefix (EncodingType::ALP, DataType, RowCount)
///   1 byte:  exponent (power-of-10 multiplier index)
///   4 bytes: exception count
///   4 bytes: encoded values sub-encoding size (X)
///   4 bytes: exception indices sub-encoding size (Y)
///   X bytes: encoded integer values (nested sub-encoding)
///   Y bytes: exception indices as bools (omitted when 0 exceptions)
///   Z bytes: exception values as raw floats (omitted when 0 exceptions)
template <typename T>
class ALPEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
  static_assert(
      isFloatingPointType<T>(),
      "ALPEncoding only supports float and double types.");

 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  /// Construct from serialized data. Parses the header fields and recursively
  /// creates sub-encodings for the encoded values, exception indices, and
  /// exception values streams.
  ALPEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory,
      const Encoding::Options& options = {});

  /// Reset all sub-encodings to the beginning of the stream.
  void reset() final;

  /// Skip rowCount values, advancing both the encoded-values stream and the
  /// exception streams by the appropriate amounts.
  void skip(uint32_t rowCount) final;

  /// Decode rowCount values into buffer. Materializes encoded integers from
  /// the sub-encoding, divides by 10^exponent, then patches in any exceptions.
  void materialize(uint32_t rowCount, void* buffer) final;

  /// Row-at-a-time decode for the selective reader path.
  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  /// Return a human-readable description of this encoding tree.
  std::string debugString(int offset) const final;

  /// Encode float/double values into the ALP serialized format. Finds the
  /// best exponent via exhaustive search, encodes values as integers, and
  /// stores exceptions separately. Throws NIMBLE_INCOMPATIBLE_ENCODING if
  /// the exception rate exceeds 1/kMaxExceptionRate.
  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {});

 private:
  /// Power-of-10 index chosen during encoding (0..15).
  int exponent_;

  /// Total number of exception values in the stream.
  uint32_t exceptionCount_;

  /// Sub-encoding holding the integer-encoded values (uint64_t stream).
  std::unique_ptr<Encoding> encodedValues_;

  /// Sub-encoding holding a per-row bool mask (true = exception).
  std::unique_ptr<Encoding> exceptionIndices_;

  /// Sub-encoding holding the raw bit patterns of exception values.
  std::unique_ptr<Encoding> exceptionValues_;

  /// Temporary buffer for materialized encoded integers.
  Vector<uint64_t> encodedBuffer_;

  /// Temporary buffer for materialized exception mask.
  Vector<bool> indicesBuffer_;

  /// Temporary buffer for materialized exception values.
  Vector<physicalType> exceptionsBuffer_;
};

//
// End of public API. Implementation follows.
//

template <typename T>
ALPEncoding<T>::ALPEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    std::function<void*(uint32_t)> stringBufferFactory,
    const Encoding::Options& options)
    : TypedEncoding<T, physicalType>(memoryPool, data, options),
      encodedBuffer_(&memoryPool),
      indicesBuffer_(&memoryPool),
      exceptionsBuffer_(&memoryPool) {
  const EncodingFactory factory{options};
  auto pos = data.data() + this->dataOffset();

  exponent_ = static_cast<uint8_t>(encoding::readChar(pos));
  exceptionCount_ = encoding::readUint32(pos);
  const uint32_t encodedValuesSize = encoding::readUint32(pos);
  const uint32_t exceptionIndicesSize = encoding::readUint32(pos);

  encodedValues_ =
      factory.create(memoryPool, {pos, encodedValuesSize}, stringBufferFactory);
  pos += encodedValuesSize;

  if (exceptionCount_ > 0) {
    exceptionIndices_ = factory.create(
        memoryPool, {pos, exceptionIndicesSize}, stringBufferFactory);
    pos += exceptionIndicesSize;

    exceptionValues_ = factory.create(
        memoryPool,
        {pos, static_cast<size_t>(data.end() - pos)},
        std::move(stringBufferFactory));
  }
}

template <typename T>
void ALPEncoding<T>::reset() {
  encodedValues_->reset();
  if (exceptionCount_ > 0) {
    exceptionIndices_->reset();
    exceptionValues_->reset();
  }
}

template <typename T>
void ALPEncoding<T>::skip(uint32_t rowCount) {
  if (rowCount == 0) {
    return;
  }

  encodedValues_->skip(rowCount);

  if (exceptionCount_ > 0) {
    indicesBuffer_.resize(rowCount);
    exceptionIndices_->materialize(rowCount, indicesBuffer_.data());
    uint32_t exceptionsToSkip = 0;
    for (uint32_t i = 0; i < rowCount; ++i) {
      if (indicesBuffer_[i]) {
        ++exceptionsToSkip;
      }
    }
    if (exceptionsToSkip > 0) {
      exceptionValues_->skip(exceptionsToSkip);
    }
  }
}

template <typename T>
void ALPEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  T* output = static_cast<T*>(buffer);
  double factor = alp::kPowersOf10[exponent_];

  // Read all encoded integer values (stored as uint64_t, interpreted as
  // int64_t).
  encodedBuffer_.resize(rowCount);
  encodedValues_->materialize(rowCount, encodedBuffer_.data());

  if (exceptionCount_ == 0) {
    // Fast path: no exceptions.
    for (uint32_t i = 0; i < rowCount; ++i) {
      int64_t enc = static_cast<int64_t>(encodedBuffer_[i]);
      output[i] = static_cast<T>(static_cast<double>(enc) / factor);
    }
    return;
  }

  // Read exception indices.
  indicesBuffer_.resize(rowCount);
  exceptionIndices_->materialize(rowCount, indicesBuffer_.data());

  // Count and read exception values (stored as physicalType, reinterpreted as
  // T).
  uint32_t numExceptions = 0;
  for (uint32_t i = 0; i < rowCount; ++i) {
    if (indicesBuffer_[i]) {
      ++numExceptions;
    }
  }
  exceptionsBuffer_.resize(numExceptions);
  if (numExceptions > 0) {
    exceptionValues_->materialize(numExceptions, exceptionsBuffer_.data());
  }

  // Decode: use integer-decoded values, patching in exceptions.
  uint32_t exceptionIdx = 0;
  for (uint32_t i = 0; i < rowCount; ++i) {
    if (indicesBuffer_[i]) {
      // Reinterpret physicalType back to T.
      T val;
      std::memcpy(&val, &exceptionsBuffer_[exceptionIdx++], sizeof(T));
      output[i] = val;
    } else {
      int64_t enc = static_cast<int64_t>(encodedBuffer_[i]);
      output[i] = static_cast<T>(static_cast<double>(enc) / factor);
    }
  }
}

template <typename T>
std::string_view ALPEncoding<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer,
    const Encoding::Options& options) {
  const bool useVarint = options.useVarintRowCount;
  const uint32_t rowCount = static_cast<uint32_t>(values.size());

  if (values.empty()) {
    NIMBLE_INCOMPATIBLE_ENCODING("ALPEncoding can't be used with 0 rows.");
  }

  // Reinterpret physical values back to float/double for ALP analysis.
  auto floatValues = std::span<const T>(
      reinterpret_cast<const T*>(values.data()), values.size());

  // Find the best exponent.
  auto [exponent, exceptionCount] = alp::findBestExponent(floatValues);

  // Reject if too many exceptions (> 1/kMaxExceptionRate of values).
  if (exceptionCount > rowCount / alp::kMaxExceptionRate) {
    NIMBLE_INCOMPATIBLE_ENCODING(
        "ALP encoding has too many exceptions ({}/{}).",
        exceptionCount,
        rowCount);
  }

  // Encode all values as integers, collecting exceptions.
  Vector<int64_t> encoded{&buffer.getMemoryPool()};
  encoded.resize(rowCount);
  Vector<bool> isException{&buffer.getMemoryPool()};
  isException.resize(rowCount);
  Vector<T> exceptionVals{&buffer.getMemoryPool()};
  exceptionVals.reserve(exceptionCount);

  for (uint32_t i = 0; i < rowCount; ++i) {
    T v = floatValues[i];
    alp::EncodedIntType<T> enc = 0;
    if (std::isfinite(v) && alp::tryEncode(v, exponent, enc)) {
      encoded[i] = enc;
      isException[i] = false;
    } else {
      encoded[i] = 0; // Placeholder; exception value stored separately.
      isException[i] = true;
      exceptionVals.push_back(v);
    }
  }

  // Encode the three sub-streams.
  // Note: encodeNested<NestedT> requires NestedT to be its own physical type.
  // So we use uint64_t for encoded integers (int64_t's physicalType) and
  // physicalType for exception values (float's physicalType = uint32_t, etc.).
  Buffer tempBuffer{buffer.getMemoryPool()};

  const std::string_view serializedEncoded =
      selection.template encodeNested<uint64_t>(
          EncodingIdentifiers::ALP::EncodedValues,
          std::span<const uint64_t>(
              reinterpret_cast<const uint64_t*>(encoded.data()),
              encoded.size()),
          tempBuffer,
          options);

  std::string_view serializedIndices;
  std::string_view serializedExceptions;

  if (exceptionCount > 0) {
    serializedIndices = selection.template encodeNested<bool>(
        EncodingIdentifiers::ALP::ExceptionIndices,
        isException,
        tempBuffer,
        options);

    serializedExceptions = selection.template encodeNested<physicalType>(
        EncodingIdentifiers::ALP::ExceptionValues,
        std::span<const physicalType>(
            reinterpret_cast<const physicalType*>(exceptionVals.data()),
            exceptionVals.size()),
        tempBuffer,
        options);
  }

  // Layout: prefix + exponent(1) + exceptionCount(4) + encodedSize(4) +
  //         indicesSize(4) + encoded + indices + exceptions
  const uint32_t encodingSize =
      Encoding::serializePrefixSize(rowCount, useVarint) + 1 + // exponent
      4 + // exception count
      4 + // encoded values size
      4 + // exception indices size
      static_cast<uint32_t>(serializedEncoded.size()) +
      static_cast<uint32_t>(serializedIndices.size()) +
      static_cast<uint32_t>(serializedExceptions.size());

  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::ALP, TypeTraits<T>::dataType, rowCount, useVarint, pos);
  encoding::writeChar(static_cast<char>(exponent), pos);
  encoding::writeUint32(exceptionCount, pos);
  encoding::writeUint32(static_cast<uint32_t>(serializedEncoded.size()), pos);
  encoding::writeUint32(static_cast<uint32_t>(serializedIndices.size()), pos);
  encoding::writeBytes(serializedEncoded, pos);
  if (exceptionCount > 0) {
    encoding::writeBytes(serializedIndices, pos);
    encoding::writeBytes(serializedExceptions, pos);
  }

  NIMBLE_DCHECK_EQ(pos - reserved, encodingSize, "Encoding size mismatch.");
  return {reserved, encodingSize};
}

template <typename T>
template <typename DecoderVisitor>
void ALPEncoding<T>::readWithVisitor(
    DecoderVisitor& visitor,
    ReadWithVisitorParams& params) {
  detail::readWithVisitorSlow(
      visitor,
      params,
      [&](auto toSkip) { skip(toSkip); },
      [&] {
        double factor = alp::kPowersOf10[exponent_];

        uint64_t encRaw;
        encodedValues_->materialize(1, &encRaw);

        if (exceptionCount_ > 0) {
          bool isExc;
          exceptionIndices_->materialize(1, &isExc);
          if (isExc) {
            physicalType rawVal;
            exceptionValues_->materialize(1, &rawVal);
            T val;
            std::memcpy(&val, &rawVal, sizeof(T));
            return val;
          }
        }

        int64_t enc = static_cast<int64_t>(encRaw);
        return static_cast<T>(static_cast<double>(enc) / factor);
      });
}

template <typename T>
std::string ALPEncoding<T>::debugString(int offset) const {
  std::string log = Encoding::debugString(offset);
  log += fmt::format(
      "\n{}exponent: {}, exceptions: {}",
      std::string(offset + 2, ' '),
      exponent_,
      exceptionCount_);
  log += fmt::format(
      "\n{}encodedValues child:\n{}",
      std::string(offset + 2, ' '),
      encodedValues_->debugString(offset + 4));
  if (exceptionCount_ > 0) {
    log += fmt::format(
        "\n{}exceptionIndices child:\n{}",
        std::string(offset + 2, ' '),
        exceptionIndices_->debugString(offset + 4));
    log += fmt::format(
        "\n{}exceptionValues child:\n{}",
        std::string(offset + 2, ' '),
        exceptionValues_->debugString(offset + 4));
  }
  return log;
}

} // namespace facebook::nimble
