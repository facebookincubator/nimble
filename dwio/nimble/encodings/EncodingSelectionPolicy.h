/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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
#include <optional>
#include "dwio/nimble/common/Bits.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/encodings/EncodingIdentifier.h"
#include "dwio/nimble/encodings/EncodingLayout.h"
#include "dwio/nimble/encodings/EncodingSelection.h"

namespace facebook::nimble {

using EncodingSelectionPolicyFactory =
    std::function<std::unique_ptr<EncodingSelectionPolicyBase>(DataType)>;

// The following enables encoding selection debug messages. By default, these
// logs are turned off (with zero overhead). In tests (or in debug sessions), we
// enable these logs.
#define RED "\033[31m"
#define GREEN "\033[32m"
#define YELLOW "\033[33m"
#define BLUE "\033[34m"
#define PURPLE "\033[35m"
#define CYAN "\033[36m"
#define RESET_COLOR "\033[0m"

#ifndef NIMBLE_ENCODING_SELECTION_DEBUG_MAX_ITEMS
#define NIMBLE_ENCODING_SELECTION_DEBUG_MAX_ITEMS 50
#endif

#ifdef NIMBLE_ENCODING_SELECTION_DEBUG
#define NIMBLE_SELECTION_LOG(stream) LOG(INFO) << stream << RESET_COLOR
#else
#define NIMBLE_SELECTION_LOG(stream)
#endif

#define COMMA ,
#define UNIQUE_PTR_FACTORY_EXTRA(data_type, class, extra_types, ...)      \
  switch (data_type) {                                                    \
    case facebook::nimble::DataType::Uint8: {                             \
      return std::make_unique<class<uint8_t extra_types>>(__VA_ARGS__);   \
    }                                                                     \
    case facebook::nimble::DataType::Int8: {                              \
      return std::make_unique<class<int8_t extra_types>>(__VA_ARGS__);    \
    }                                                                     \
    case facebook::nimble::DataType::Uint16: {                            \
      return std::make_unique<class<uint16_t extra_types>>(__VA_ARGS__);  \
    }                                                                     \
    case facebook::nimble::DataType::Int16: {                             \
      return std::make_unique<class<int16_t extra_types>>(__VA_ARGS__);   \
    }                                                                     \
    case facebook::nimble::DataType::Uint32: {                            \
      return std::make_unique<class<uint32_t extra_types>>(__VA_ARGS__);  \
    }                                                                     \
    case facebook::nimble::DataType::Int32: {                             \
      return std::make_unique<class<int32_t extra_types>>(__VA_ARGS__);   \
    }                                                                     \
    case facebook::nimble::DataType::Uint64: {                            \
      return std::make_unique<class<uint64_t extra_types>>(__VA_ARGS__);  \
    }                                                                     \
    case facebook::nimble::DataType::Int64: {                             \
      return std::make_unique<class<int64_t extra_types>>(__VA_ARGS__);   \
    }                                                                     \
    case facebook::nimble::DataType::Float: {                             \
      return std::make_unique<class<float extra_types>>(__VA_ARGS__);     \
    }                                                                     \
    case facebook::nimble::DataType::Double: {                            \
      return std::make_unique<class<double extra_types>>(__VA_ARGS__);    \
    }                                                                     \
    case facebook::nimble::DataType::Bool: {                              \
      return std::make_unique<class<bool extra_types>>(__VA_ARGS__);      \
    }                                                                     \
    case facebook::nimble::DataType::String: {                            \
      return std::make_unique<class<std::string_view extra_types>>(       \
          __VA_ARGS__);                                                   \
    }                                                                     \
    default: {                                                            \
      NIMBLE_UNREACHABLE(                                                 \
          fmt::format("Unsupported data type {}.", toString(data_type))); \
    }                                                                     \
  }

#define UNIQUE_PTR_FACTORY(data_type, class, ...) \
  UNIQUE_PTR_FACTORY_EXTRA(data_type, class, , __VA_ARGS__)

struct CompressionOptions {
  float compressionAcceptRatio = 0.98f;
  uint32_t zstdCompressionLevel = 3;
  uint32_t internalCompressionLevel = 4;
  uint32_t internalDecompressionLevel = 2;
  bool useVariableBitWidthCompressor = false;
};

// This is the manual encoding selection implementation.
// The manual selection is using a manually crafted model to choose the most
// appropriate encoding based on the provided statistics.
template <typename T, bool FixedByteWidth = true>
class ManualEncodingSelectionPolicy : public EncodingSelectionPolicy<T> {
  using physicalType = typename TypeTraits<T>::physicalType;

 public:
  ManualEncodingSelectionPolicy(
      std::vector<std::pair<EncodingType, float>> readFactors,
      CompressionOptions compressionOptions,
      std::optional<NestedEncodingIdentifier> identifier)
      : readFactors_{std::move(readFactors)},
        compressionOptions_{std::move(compressionOptions)},
        identifier_{identifier} {}

  std::unique_ptr<EncodingSelectionPolicyBase> createImpl(
      EncodingType encodingType,
      NestedEncodingIdentifier identifier,
      DataType type) override {
    // In each sub-level of the encoding selection, we exclude the encodings
    // selected in parent levels. Although this is not required (as hopefully,
    // the model will not pick a nested encoding of the same type as the
    // parent), it provides an additional safty net, making sure the encoding
    // selection will eventually converge, and also slightly speeds up nested
    // encoding selection.
    // TODO: validate the assumptions here compared to brute forcing, to see if
    // the same encoding is selected multiple times in the tree (for example,
    // should we allow trivial string lengths to be encoded using trivial
    // encoding?)
    std::vector<std::pair<EncodingType, float>> filteredReadFactors;
    filteredReadFactors.reserve(readFactors_.size() - 1);
    for (const auto& pair : readFactors_) {
      if (pair.first != encodingType) {
        filteredReadFactors.push_back(pair);
      }
    }
    UNIQUE_PTR_FACTORY_EXTRA(
        type,
        ManualEncodingSelectionPolicy,
        COMMA FixedByteWidth,
        std::move(filteredReadFactors),
        compressionOptions_,
        identifier);
  }

  EncodingSelectionResult select(
      std::span<const physicalType> values,
      const Statistics<physicalType>& statistics) override {
    if (values.empty()) {
      return {
          .encodingType = EncodingType::Trivial,
      };
    }

    float minCost = std::numeric_limits<float>::max();
    EncodingType selectedEncoding = EncodingType::Trivial;
    // Iterate on all candidate encodings, and pick the encoding with the
    // minimal cost.
    for (const auto& pair : readFactors_) {
      auto encodingType = pair.first;
      auto size = estimateSize(encodingType, values.size(), statistics);
      if (!size.has_value()) {
        NIMBLE_SELECTION_LOG(
            PURPLE << encodingType << " encoding is incompatible.");
        continue;
      }

      // We use read factor weights to raise/lower the favorability of each
      // encoding.
      auto readFactor = pair.second;
      auto cost = size.value() * readFactor;
      NIMBLE_SELECTION_LOG(
          YELLOW << "Encoding: " << encodingType << ", Size: " << size.value()
                 << ", Factor: " << readFactor << ", Cost: " << cost);
      if (cost < minCost) {
        minCost = cost;
        selectedEncoding = encodingType;
      }
    }

    // Currently, we always attempt to compress leaf data streams. The logic
    // behind this is that encoding selection optimizes the in-memory layout of
    // data, while compression provides extra saving for persistent storage (and
    // bandwidth).
    class AlwaysCompressPolicy : public CompressionPolicy {
     public:
      explicit AlwaysCompressPolicy(CompressionOptions compressionOptions)
          : compressionOptions_{std::move(compressionOptions)} {}

      CompressionInformation compression() const override {
#ifndef DISABLE_META_INTERNAL_COMPRESSOR
        CompressionInformation information{
            .compressionType = CompressionType::MetaInternal};
        information.parameters.metaInternal.compressionLevel =
            compressionOptions_.internalCompressionLevel;
        information.parameters.metaInternal.decompressionLevel =
            compressionOptions_.internalDecompressionLevel;
        information.parameters.metaInternal.useVariableBitWidthCompressor =
            compressionOptions_.useVariableBitWidthCompressor;
        return information;
#else
        CompressionInformation information{
            .compressionType = CompressionType::Zstd};
        information.parameters.zstd.compressionLevel =
            compressionOptions_.zstdCompressionLevel;
        return information;
#endif
      }

      virtual bool shouldAccept(
          CompressionType compressionType,
          uint64_t uncompressedSize,
          uint64_t compressedSize) const override {
        if (uncompressedSize * compressionOptions_.compressionAcceptRatio <
            compressedSize) {
          NIMBLE_SELECTION_LOG(
              BLUE << compressionType
                   << " compression rejected. Original size: "
                   << uncompressedSize
                   << ", Compressed size: " << compressedSize);
          return false;
        }

        NIMBLE_SELECTION_LOG(
            CYAN << compressionType
                 << " compression accepted. Original size: " << uncompressedSize
                 << ", Compressed size: " << compressedSize);
        return true;
      }

     private:
      const CompressionOptions compressionOptions_;
    };

    NIMBLE_SELECTION_LOG(
        "Selected Encoding"
        << (identifier_.has_value()
                ? folly::to<std::string>(
                      " [NestedEncodingIdentifier: ", identifier_.value(), "]")
                : "")
        << ": " << GREEN << selectedEncoding << RESET_COLOR
        << ", Sampled Data: "
        << folly::join(
               ",",
               std::span<const physicalType>{
                   values.data(),
                   std::min(
                       size_t(NIMBLE_ENCODING_SELECTION_DEBUG_MAX_ITEMS),
                       values.size())})
        << (values.size() > size_t(NIMBLE_ENCODING_SELECTION_DEBUG_MAX_ITEMS)
                ? "..."
                : ""));
    return {
        .encodingType = selectedEncoding,
        .compressionPolicyFactory = [compressionOptions =
                                         compressionOptions_]() {
          return std::make_unique<AlwaysCompressPolicy>(compressionOptions);
        }};
  }

  EncodingSelectionResult selectNullable(
      std::span<const physicalType> /* values */,
      std::span<const bool> /* nulls */,
      const Statistics<physicalType>& /* statistics */) override {
    return {
        .encodingType = EncodingType::Nullable,
    };
  }

  const std::vector<std::pair<EncodingType, float>>& readFactors() const {
    return readFactors_;
  }

  float compressionAcceptRatio() const {
    return compressionOptions_.compressionAcceptRatio;
  }

 private:
  static inline uint32_t
  bitPackedBytes(uint64_t minValue, uint64_t maxValue, uint32_t count) {
    if constexpr (FixedByteWidth) {
      // NOTE: We round up the bits required to the next byte boundary.
      // This is to match a (temporary) hack we added to FixedBitWidthEncoding,
      // to mitigare compression issue on non-rounded bit widths. See
      // dwio/nimble/encodings/FixedBitWidthEncoding.h for full details.
      return bits::bytesRequired(
          ((bits::bitsRequired(maxValue - minValue) + 7) & ~7) * count);
    } else {
      return bits::bytesRequired(
          bits::bitsRequired(maxValue - minValue) * count);
    }
  }

  static std::optional<uint64_t> estimateNumericSize(
      EncodingType encodingType,
      uint64_t entryCount,
      const Statistics<physicalType>& statistics) {
    switch (encodingType) {
      case EncodingType::Constant: {
        return statistics.uniqueCounts().size() == 1
            ? std::optional<uint64_t>{getEncodingOverhead<
                  EncodingType::Constant,
                  physicalType>()}
            : std::nullopt;
      }
      case EncodingType::MainlyConstant: {
        // Assumptions:
        // We store one entry for the common value.
        // Number of uncommon values is total item count minus the max unique
        // count (the common value count).
        // For each uncommon value we store the its value. We assume they will
        // be stored bit-packed.
        // We also store a bitmap for all rows (is-common bitmap). This bitmap
        // will most likely be stored as SparseBool. Therefore, for each
        // uncommon value there will be an index. These indices will most likely
        // be stored bit-packed, with bit width of max(rowCount).

        // Find most common item count
        const auto maxUniqueCount = std::max_element(
            statistics.uniqueCounts().cbegin(),
            statistics.uniqueCounts().cend(),
            [](const auto& a, const auto& b) { return a.second < b.second; });
        // Deduce uncommon values count
        const auto uncommonCount = entryCount - maxUniqueCount->second;
        // Assuming the uncommon values will be stored bit packed.
        const auto uncommonValueSize =
            bitPackedBytes(statistics.min(), statistics.max(), uncommonCount);
        // Uncommon (sparse bool) bitmap will have index per uncommon value,
        // stored bit packed.
        const auto uncommonIndicesSize =
            bitPackedBytes(0, entryCount, uncommonCount);
        uint32_t overhead =
            getEncodingOverhead<EncodingType::MainlyConstant, physicalType>() +
            // Overhead for storing uncommon values
            getEncodingOverhead<EncodingType::FixedBitWidth, physicalType>() +
            // Overhead for storing uncommon bitmap
            getEncodingOverhead<EncodingType::SparseBool, bool>() +
            getEncodingOverhead<EncodingType::FixedBitWidth, uint32_t>();
        return overhead + sizeof(physicalType) + uncommonValueSize +
            uncommonIndicesSize;
      }
      case EncodingType::Trivial: {
        return getEncodingOverhead<EncodingType::Trivial, physicalType>() +
            (entryCount * sizeof(physicalType));
      }
      case EncodingType::FixedBitWidth: {
        return getEncodingOverhead<
                   EncodingType::FixedBitWidth,
                   physicalType>() +
            bitPackedBytes(statistics.min(), statistics.max(), entryCount);
      }
      case EncodingType::Dictionary: {
        // Assumptions:
        // Alphabet stored trivially.
        // Indices are stored bit-packed, with bit width needed to store max
        // dictionary size (which is the unique value count).
        const uint64_t indicesSize =
            bitPackedBytes(0, statistics.uniqueCounts().size(), entryCount);
        const uint64_t alphabetSize =
            statistics.uniqueCounts().size() * sizeof(physicalType);
        uint32_t overhead =
            getEncodingOverhead<EncodingType::Dictionary, physicalType>() +
            // Alphabet overhead
            getEncodingOverhead<EncodingType::Trivial, physicalType>() +
            // Indices overhead
            getEncodingOverhead<EncodingType::FixedBitWidth, uint32_t>();
        return overhead + alphabetSize + indicesSize;
      }
      case EncodingType::RLE: {
        // Assumptions:
        // Run values are stored bit-packed (with bit width needed to store max
        // value). Run lengths are stored using bit-packing (with bit width
        // needed to store max repetition count).

        const auto runValuesSize = bitPackedBytes(
            statistics.min(),
            statistics.max(),
            statistics.consecutiveRepeatCount());
        const auto runLengthsSize = bitPackedBytes(
            statistics.minRepeat(),
            statistics.maxRepeat(),
            statistics.consecutiveRepeatCount());
        uint32_t overhead =
            getEncodingOverhead<EncodingType::RLE, physicalType>() +
            // Overhead of run values
            getEncodingOverhead<EncodingType::FixedBitWidth, physicalType>() +
            // Overhead of run lengths
            getEncodingOverhead<EncodingType::FixedBitWidth, uint32_t>();
        return overhead + runValuesSize + runLengthsSize;
      }
      case EncodingType::Varint: {
        // Note: the condition below actually support floating point numbers as
        // well, as we use physicalType, which, for floating point numbers, is
        // an integer.
        if constexpr (isIntegralType<physicalType>() && sizeof(T) >= 4) {
          // First (7 bit) bucket produces 1 byte number. Second bucket produce
          // 2 byte number and so forth.
          size_t i = 0;
          const uint64_t dataSize = std::accumulate(
              statistics.bucketCounts().cbegin(),
              statistics.bucketCounts().cend(),
              0,
              [&i](const uint64_t sum, const uint64_t bucketSize) {
                return sum + (bucketSize * (++i));
              });
          return getEncodingOverhead<EncodingType::Varint, physicalType>() +
              dataSize;
        } else {
          return std::nullopt;
        }
      }
      default: {
        return std::nullopt;
      }
    }
  }

  static std::optional<uint64_t> estimateBoolSize(
      EncodingType encodingType,
      size_t entryCount,
      const Statistics<physicalType>& statistics) {
    switch (encodingType) {
      case EncodingType::Constant: {
        return statistics.uniqueCounts().size() == 1
            ? std::optional<uint64_t>{getEncodingOverhead<
                  EncodingType::Constant,
                  physicalType>()}
            : std::nullopt;
      }
      case EncodingType::SparseBool: {
        // Assumptions:
        // Uncommon indices are stored bit-packed (with bit width capable of
        // representing max entry count).

        const auto exceptionCount = std::min(
            statistics.uniqueCounts().at(true),
            statistics.uniqueCounts().at(false));
        uint32_t overhead =
            getEncodingOverhead<EncodingType::SparseBool, physicalType>() +
            // Overhead for storing exception indices
            getEncodingOverhead<EncodingType::FixedBitWidth, uint32_t>();
        return overhead + sizeof(bool) +
            bitPackedBytes(0, entryCount, exceptionCount);
      }
      case EncodingType::Trivial: {
        return getEncodingOverhead<EncodingType::Trivial, physicalType>() +
            bitPackedBytes(0, 1, entryCount);
      }
      case EncodingType::RLE: {
        // Assumptions:
        // Run lengths are stored using bit-packing (with bit width
        // needed to store max repetition count).

        const auto runLengthsSize = bitPackedBytes(
            statistics.minRepeat(),
            statistics.maxRepeat(),
            statistics.consecutiveRepeatCount());
        uint32_t overhead =
            getEncodingOverhead<EncodingType::RLE, physicalType>() +
            // Overhead of run lengths
            getEncodingOverhead<EncodingType::FixedBitWidth, uint32_t>();
        return overhead + sizeof(bool) + runLengthsSize;
      }
      default: {
        return std::nullopt;
      }
    }
  }

  static std::optional<uint64_t> estimateStringSize(
      EncodingType encodingType,
      size_t entryCount,
      const Statistics<std::string_view>& statistics) {
    const uint32_t maxStringSize = statistics.max().size();
    switch (encodingType) {
      case EncodingType::Constant: {
        return statistics.uniqueCounts().size() == 1
            ? std::optional<uint64_t>{getEncodingOverhead<
                  EncodingType::Constant,
                  physicalType>(maxStringSize)}
            : std::nullopt;
      }
      case EncodingType::MainlyConstant: {
        // Assumptions:
        // We store one entry for the common value.
        // For each uncommon value we store the its value.
        // Uncommon values will be stored trivially, or if there are
        // repetitions, we assume they will be nested as a dictionary. Either
        // way, it means each (unique) string value will be stored exactly once.
        // (Note: for strings we store the blob size and the lengths, assuming
        // bit-packed).
        // We also store a bitmap for all rows (is-common bitmap). This bitmap
        // will most likely be stored as SparseBool. Therefore, for each
        // uncommon value there will be an index. These indices will most likely
        // be stored bit-packed, with bit width of max(rowCount).

        // Find the most common item count
        const auto maxUniqueCount = std::max_element(
            statistics.uniqueCounts().cbegin(),
            statistics.uniqueCounts().cend(),
            [](const auto& a, const auto& b) { return a.second < b.second; });
        // Get the total blob size for all (unique) strings
        const uint64_t alphabetByteSize = std::accumulate(
            statistics.uniqueCounts().cbegin(),
            statistics.uniqueCounts().cend(),
            0,
            [](const uint32_t sum, const auto& unique) {
              return sum + unique.first.size();
            });
        // Deduce uncommon values count
        const auto uncommonCount = entryCount - maxUniqueCount->second;
        // Remove common string from blob, and add the (bit-packed) string
        // lengths to the size.
        // Note: we remove the common string, as it is later added back when
        // calculating the header overhead.
        const uint64_t alphabetSize = alphabetByteSize -
            maxUniqueCount->first.size() +
            bitPackedBytes(statistics.min().size(),
                           statistics.max().size(),
                           statistics.uniqueCounts().size());
        // Uncommon values (sparse bool) bitmap will have index per value,
        // stored bit packed.
        const auto uncommonIndicesSize =
            bitPackedBytes(0, entryCount, uncommonCount);
        uint32_t overhead =
            getEncodingOverhead<EncodingType::MainlyConstant, physicalType>(
                maxUniqueCount->first.size()) +
            // Overhead for storing uncommon values
            getEncodingOverhead<EncodingType::Trivial, physicalType>(
                maxStringSize) +
            // Overhead for storing uncommon bitmap
            getEncodingOverhead<EncodingType::SparseBool, bool>();

        return overhead + alphabetSize + uncommonIndicesSize;
      }
      case EncodingType::Trivial: {
        // We assume string lengths will be stored bit packed.
        return getEncodingOverhead<EncodingType::Trivial, physicalType>(
                   maxStringSize) +
            statistics.totalStringsLength() +
            bitPackedBytes(
                   statistics.min().size(),
                   statistics.max().size(),
                   entryCount);
      }
      case EncodingType::Dictionary: {
        // Assumptions:
        // Alphabet stored trivially.
        // Indices are stored bit-packed, with bit width needed to store max
        // dictionary size (which is the unique value count).
        const uint64_t indicesSize =
            bitPackedBytes(0, statistics.uniqueCounts().size(), entryCount);
        // Get the total blob size for all (unique) strings
        const uint64_t alphabetByteSize = std::accumulate(
            statistics.uniqueCounts().cbegin(),
            statistics.uniqueCounts().cend(),
            0,
            [](const uint32_t sum, const auto& unique) {
              return sum + unique.first.size();
            });
        // Add (bit-packed) string lengths
        const uint64_t alphabetSize = alphabetByteSize +
            bitPackedBytes(statistics.min().size(),
                           statistics.max().size(),
                           statistics.uniqueCounts().size());
        uint32_t overhead =
            getEncodingOverhead<EncodingType::Dictionary, physicalType>(
                maxStringSize) +
            // Alphabet overhead
            getEncodingOverhead<EncodingType::Trivial, physicalType>(
                maxStringSize) +
            // Indices overhead
            getEncodingOverhead<EncodingType::FixedBitWidth, uint32_t>();
        return overhead + alphabetSize + indicesSize;
      }
      case EncodingType::RLE: {
        // Assumptions:
        // Run values are stored using dictionary (and inside, trivial +
        // bit-packing). Run lengths are stored using bit-packing (with bit
        // width needed to store max repetition count).

        uint64_t runValuesSize =
            // (unique) strings blob size
            std::accumulate(
                statistics.uniqueCounts().cbegin(),
                statistics.uniqueCounts().cend(),
                0,
                [](const uint32_t sum, const auto& unique) {
                  return sum + unique.first.size();
                }) +
            // (bit-packed) string lengths
            bitPackedBytes(
                statistics.min().size(),
                statistics.max().size(),
                statistics.consecutiveRepeatCount()) +
            // dictionary indices
            bitPackedBytes(
                0,
                statistics.uniqueCounts().size(),
                statistics.consecutiveRepeatCount());
        const auto runLengthsSize = bitPackedBytes(
            statistics.minRepeat(),
            statistics.maxRepeat(),
            statistics.consecutiveRepeatCount());
        uint32_t overhead =
            getEncodingOverhead<EncodingType::RLE, physicalType>() +
            // Overhead of run values
            getEncodingOverhead<EncodingType::Dictionary, physicalType>() +
            getEncodingOverhead<EncodingType::Trivial, physicalType>() +
            getEncodingOverhead<EncodingType::FixedBitWidth, uint32_t>() +
            // Overhead of run lengths
            getEncodingOverhead<EncodingType::FixedBitWidth, uint32_t>();
        return overhead + runValuesSize + runLengthsSize;
      }
      default: {
        return std::nullopt;
      }
    }
  }

  static std::optional<uint64_t> estimateSize(
      EncodingType encodingType,
      size_t entryCount,
      const Statistics<physicalType>& statistics) {
    if constexpr (isNumericType<physicalType>()) {
      return estimateNumericSize(encodingType, entryCount, statistics);
    } else if constexpr (isBoolType<physicalType>()) {
      return estimateBoolSize(encodingType, entryCount, statistics);
    } else if constexpr (isStringType<physicalType>()) {
      return estimateStringSize(encodingType, entryCount, statistics);
    }

    NIMBLE_UNREACHABLE(fmt::format(
        "Unable to estimate size for type {}.", folly::demangle(typeid(T))));
  }

  template <EncodingType encodingType, typename dataType>
  static uint32_t getEncodingOverhead(uint32_t size = sizeof(dataType)) {
    constexpr uint32_t commonPrefixSize = 6;
    if constexpr (encodingType == EncodingType::Trivial) {
      if constexpr (isStringType<dataType>()) {
        return commonPrefixSize +
            // Trivial strings also have nested encoding for lengths.
            getEncodingOverhead<EncodingType::FixedBitWidth, uint32_t>() +
            5; // CompressionType (1 byte), BlobOffset (4 bytes)
      } else {
        return commonPrefixSize + 1; // CompressionType (1 byte)
      }
    } else if constexpr (encodingType == EncodingType::Constant) {
      if constexpr (isStringType<dataType>()) {
        return commonPrefixSize + size +
            sizeof(uint32_t); // CommonValue (string bytes and string length)
      } else {
        return commonPrefixSize + sizeof(dataType); // CompressionType (1 byte)
      }
    } else if constexpr (encodingType == EncodingType::FixedBitWidth) {
      return commonPrefixSize + 2 +
          sizeof(dataType); // CompressionType (1 byte), Baseline (size of
                            // dataType), Bit Width (1 byte)
    } else if constexpr (encodingType == EncodingType::MainlyConstant) {
      if constexpr (isStringType<dataType>()) {
        return commonPrefixSize + (2 * sizeof(uint32_t)) + size +
            sizeof(uint32_t); // IsCommon size (4 bytes), OtherValues size (4
                              // bytes), Common value (string bytes and string
                              // length)
      } else {
        return commonPrefixSize + (2 * sizeof(uint32_t)) +
            sizeof(dataType); // IsCommon size (4 bytes), OtherValues size (4
                              // bytes), Common value (size of dataType)
      }
    } else if constexpr (encodingType == EncodingType::SparseBool) {
      return commonPrefixSize + 1; // IsSet flag (1 byte)
    } else if constexpr (encodingType == EncodingType::Dictionary) {
      return commonPrefixSize + sizeof(uint32_t); // Alphabet size (4 byte)
    } else if constexpr (encodingType == EncodingType::RLE) {
      return commonPrefixSize + sizeof(uint32_t); // Run Lengths size (4 byte)
    } else if constexpr (encodingType == EncodingType::Varint) {
      return commonPrefixSize + sizeof(dataType); // Baseline (size of dataType)
    } else if constexpr (encodingType == EncodingType::Nullable) {
      return commonPrefixSize + sizeof(uint32_t); // Data size (4 bytes)
    }
  }

  // These read factors are used to raise or lower the chances of an encoding to
  // be picked. Right now, these represent mostly the cpu cost to decode the
  // values. Trivial is being boosed as we factor in the added benefit of
  // applying compression.
  // See ManualEncodingSelectionPolicyFactory::defaultReadFactors for the
  // default.
  const std::vector<std::pair<EncodingType, float>> readFactors_;
  const CompressionOptions compressionOptions_;
  const std::optional<NestedEncodingIdentifier> identifier_;
};

// This model is trained offline and parameters are updated here.
// The parameters are relatively robust and do not need to be updated
// unless we add / remove an encoding type.
template <typename T>
struct EncodingPredictionModel {
  using physicalType = typename TypeTraits<T>::physicalType;
  explicit EncodingPredictionModel()
      : maxRepeatParam(1.52), minRepeatParam(1.13), uniqueParam(2.589) {}
  float maxRepeatParam;
  float minRepeatParam;
  float uniqueParam;
  float predict(const Statistics<physicalType>& statistics) {
    // TODO: Utilize more features within statistics for prediction.
    auto maxRepeat = statistics.maxRepeat();
    auto minRepeat = statistics.minRepeat();
    auto unique = statistics.uniqueCounts().size();
    return maxRepeatParam * maxRepeat + minRepeatParam * minRepeat +
        uniqueParam * unique;
  }
};

class ManualEncodingSelectionPolicyFactory {
 public:
  ManualEncodingSelectionPolicyFactory(
      std::vector<std::pair<EncodingType, float>> readFactors =
          defaultReadFactors(),
      CompressionOptions compressionOptions = {})
      : readFactors_{std::move(readFactors)},
        compressionOptions_{std::move(compressionOptions)} {}

  std::unique_ptr<EncodingSelectionPolicyBase> createPolicy(
      DataType dataType) const {
    UNIQUE_PTR_FACTORY(
        dataType,
        ManualEncodingSelectionPolicy,
        readFactors_,
        compressionOptions_,
        std::nullopt);
  }

  static std::vector<EncodingType> possibleEncodings() {
    return {
        EncodingType::Constant,
        EncodingType::Trivial,
        EncodingType::FixedBitWidth,
        EncodingType::MainlyConstant,
        EncodingType::SparseBool,
        EncodingType::Dictionary,
        EncodingType::RLE,
        EncodingType::Varint,
    };
  }

  static std::vector<std::pair<EncodingType, float>> defaultReadFactors() {
    return {
        {EncodingType::Constant, 1.0},
        {EncodingType::Trivial, 0.7},
        {EncodingType::FixedBitWidth, 0.9},
        {EncodingType::MainlyConstant, 1.0},
        {EncodingType::SparseBool, 1.0},
        {EncodingType::Dictionary, 1.0},
        {EncodingType::RLE, 1.0},
        {EncodingType::Varint, 1.0},
    };
  }

  static std::vector<std::pair<nimble::EncodingType, float>> parseReadFactors(
      const std::string& val) {
    std::vector<std::pair<nimble::EncodingType, float>> readFactors;
    std::vector<std::string> parts;
    folly::split(';', folly::trimWhitespace(val), parts);
    readFactors.reserve(parts.size());

    std::vector<nimble::EncodingType> possibleEncodings =
        nimble::ManualEncodingSelectionPolicyFactory::possibleEncodings();
    std::vector<std::string> possibleEncodingStrings;
    possibleEncodingStrings.reserve(possibleEncodings.size());
    std::transform(
        possibleEncodings.cbegin(),
        possibleEncodings.cend(),
        std::back_inserter(possibleEncodingStrings),
        [](const auto& encoding) { return toString(encoding); });
    for (const auto& part : parts) {
      std::vector<std::string> kv;
      folly::split('=', part, kv);
      NIMBLE_CHECK(
          kv.size() == 2,
          fmt::format(
              "Invalid read factor format. "
              "Expected format is <EncodingType>=<facor>;<EncodingType>=<facor>. "
              "Unable to parse '{}'.",
              part));
      auto value = folly::tryTo<float>(kv[1]);
      NIMBLE_CHECK(
          value.hasValue(),
          fmt::format(
              "Unable to parse read factor value '{}' in '{}'. Expected valid float value.",
              kv[1],
              part));
      bool found = false;
      auto key = folly::trimWhitespace(kv[0]);
      for (auto i = 0; i < possibleEncodings.size(); ++i) {
        auto encoding = possibleEncodings[i];
        // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
        if (key == possibleEncodingStrings[i]) {
          found = true;
          readFactors.emplace_back(encoding, value.value());
          break;
        }
      }
      NIMBLE_CHECK(
          found,
          fmt::format(
              "Unknown or unexpected read factor encoding '{}'. Allowed values: {}",
              key,
              folly::join(",", possibleEncodingStrings)));
    }
    return readFactors;
  }

 private:
  const std::vector<std::pair<EncodingType, float>> readFactors_;
  const CompressionOptions compressionOptions_;
};

// This is a learned encoding selection implementation.
template <typename T>
class LearnedEncodingSelectionPolicy : public EncodingSelectionPolicy<T> {
  using physicalType = typename TypeTraits<T>::physicalType;

 public:
  LearnedEncodingSelectionPolicy(
      std::vector<EncodingType> encodingChoices,
      std::optional<NestedEncodingIdentifier> identifier,
      EncodingPredictionModel<T> encodingModel = EncodingPredictionModel<T>())
      : encodingChoices_{std::move(encodingChoices)},
        identifier_{identifier},
        mlModel_{encodingModel} {}

  LearnedEncodingSelectionPolicy()
      : LearnedEncodingSelectionPolicy{
            possibleEncodingChoices(),
            std::nullopt,
            EncodingPredictionModel<T>()} {}

  EncodingSelectionResult select(
      std::span<const physicalType> values,
      const Statistics<physicalType>& statistics) override {
    if (values.empty()) {
      return {
          .encodingType = EncodingType::Trivial,
      };
    }

    float prediction = mlModel_.predict(statistics);
    if (prediction > 0.1) {
      return {
          .encodingType = EncodingType::Trivial,
      };
    }
    // TODO: Implement a multi-class Encoding model so that we can predict not
    // only a trivial encoding but also other encodings.

    return {
        .encodingType = EncodingType::Trivial,
    };
  }

  EncodingSelectionResult selectNullable(
      std::span<const physicalType> /* values */,
      std::span<const bool> /* nulls */,
      const Statistics<physicalType>& /* statistics */) override {
    return {
        .encodingType = EncodingType::Nullable,
    };
  }

  std::unique_ptr<EncodingSelectionPolicyBase> createImpl(
      EncodingType encodingType,
      NestedEncodingIdentifier identifier,
      DataType type) override {
    // In each sub-level of the encoding selection, we exclude the encodings
    // selected in parent levels. Although this is not required (as hopefully,
    // the model will not pick a nested encoding of the same type as the
    // parent), it provides an additional safty net, making sure the encoding
    // selection will eventually converge, and also slightly speeds up nested
    // encoding selection.
    // TODO: validate the assumptions here compared to brute forcing, to see if
    // the same encoding is selected multiple times in the tree (for example,
    // should we allow trivial string lengths to be encoded using trivial
    // encoding?)
    std::vector<EncodingType> filteredReadFactors;
    filteredReadFactors.reserve(encodingChoices_.size() - 1);
    for (const auto& encodingType_ : encodingChoices_) {
      if (encodingType_ != encodingType) {
        filteredReadFactors.push_back(encodingType_);
      }
    }
    UNIQUE_PTR_FACTORY(
        type,
        LearnedEncodingSelectionPolicy,
        std::move(filteredReadFactors),
        identifier);
  }

 private:
  static std::vector<EncodingType> possibleEncodingChoices() {
    return {
        EncodingType::Constant,
        EncodingType::Trivial,
        EncodingType::FixedBitWidth,
        EncodingType::MainlyConstant,
        EncodingType::SparseBool,
        EncodingType::Dictionary,
        EncodingType::RLE,
        EncodingType::Varint,
    };
  }

  std::vector<EncodingType> encodingChoices_;
  std::optional<NestedEncodingIdentifier> identifier_;
  EncodingPredictionModel<T> mlModel_;
};

class ReplayedCompressionPolicy : public nimble::CompressionPolicy {
 public:
  explicit ReplayedCompressionPolicy(
      nimble::CompressionType compressionType,
      CompressionOptions compressionOptions)
      : compressionType_{compressionType},
        compressionOptions_{std::move(compressionOptions)} {}

  nimble::CompressionInformation compression() const override {
    if (compressionType_ == nimble::CompressionType::Uncompressed) {
      return {.compressionType = nimble::CompressionType::Uncompressed};
    }

    if (compressionType_ == nimble::CompressionType::Zstd) {
      nimble::CompressionInformation information{
          .compressionType = nimble::CompressionType::Zstd};
      information.parameters.zstd.compressionLevel =
          compressionOptions_.zstdCompressionLevel;
      return information;
    }

    nimble::CompressionInformation information{
        .compressionType = nimble::CompressionType::MetaInternal};
    information.parameters.metaInternal.compressionLevel =
        compressionOptions_.internalCompressionLevel;
    information.parameters.metaInternal.decompressionLevel =
        compressionOptions_.internalDecompressionLevel;
    information.parameters.metaInternal.useVariableBitWidthCompressor =
        compressionOptions_.useVariableBitWidthCompressor;
    return information;
  }

  virtual bool shouldAccept(
      nimble::CompressionType /* compressionType */,
      uint64_t uncompressedSize,
      uint64_t compressedSize) const override {
    if (uncompressedSize * compressionOptions_.compressionAcceptRatio <
        compressedSize) {
      NIMBLE_SELECTION_LOG(
          BLUE << compressionType_ << " compression rejected. Original size: "
               << uncompressedSize << ", Compressed size: " << compressedSize);
      return false;
    }

    NIMBLE_SELECTION_LOG(
        CYAN << compressionType_ << " compression accepted. Original size: "
             << uncompressedSize << ", Compressed size: " << compressedSize);
    return true;
  }

 private:
  const nimble::CompressionType compressionType_;
  const CompressionOptions compressionOptions_;
};

template <typename TInner>
class ReplayedEncodingSelectionPolicy;

template <typename TInner>
class ReplayedEncodingSelectionPolicy
    : public nimble::EncodingSelectionPolicy<TInner> {
  using physicalType = typename nimble::TypeTraits<TInner>::physicalType;

 public:
  ReplayedEncodingSelectionPolicy(
      EncodingLayout encodingLayout,
      CompressionOptions compressionOptions,
      const EncodingSelectionPolicyFactory& encodingSelectionPolicyFactory)
      : encodingLayout_{encodingLayout},
        compressionOptions_{std::move(compressionOptions)},
        encodingSelectionPolicyFactory_{encodingSelectionPolicyFactory} {}

  nimble::EncodingSelectionResult select(
      std::span<const physicalType> /* values */,
      const nimble::Statistics<physicalType>& /* statistics */) override {
    NIMBLE_SELECTION_LOG(
        CYAN << "Replaying encoding " << encodingLayout_.encodingType());
    return {
        .encodingType = encodingLayout_.encodingType(),
        .compressionPolicyFactory = [this]() {
          return std::make_unique<ReplayedCompressionPolicy>(
              encodingLayout_.compressionType(), compressionOptions_);
        }};
  }

  EncodingSelectionResult selectNullable(
      std::span<const physicalType> /* values */,
      std::span<const bool> /* nulls */,
      const Statistics<physicalType>& /* statistics */) override {
    NIMBLE_SELECTION_LOG(
        CYAN << "Replaying nullable encoding "
             << encodingLayout_.encodingType());
    encodingLayout_ = EncodingLayout{
        EncodingType::Nullable,
        CompressionType::Uncompressed,
        {
            /* Data */ std::move(encodingLayout_),
            /* Nulls */ std::nullopt,
        }};
    return {
        .encodingType = EncodingType::Nullable,
    };
  }

  std::unique_ptr<nimble::EncodingSelectionPolicyBase> createImpl(
      nimble::EncodingType /* encodingType */,
      nimble::NestedEncodingIdentifier identifier,
      nimble::DataType type) override {
    NIMBLE_ASSERT(
        identifier < encodingLayout_.childrenCount(),
        "Sub-encoding identifier out of range.");
    auto child = encodingLayout_.child(identifier);

    if (child.has_value()) {
      UNIQUE_PTR_FACTORY(
          type,
          ReplayedEncodingSelectionPolicy,
          child.value(),
          compressionOptions_,
          encodingSelectionPolicyFactory_);
    } else {
      return encodingSelectionPolicyFactory_(type);
    }
  }

 private:
  EncodingLayout encodingLayout_;
  const CompressionOptions compressionOptions_;
  const EncodingSelectionPolicyFactory& encodingSelectionPolicyFactory_;
};

} // namespace facebook::nimble
