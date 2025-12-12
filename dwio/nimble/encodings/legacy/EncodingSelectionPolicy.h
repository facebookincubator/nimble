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
#include <optional>
#include <utility>
#include "dwio/nimble/common/Constants.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/encodings/EncodingIdentifier.h"
#include "dwio/nimble/encodings/EncodingLayout.h"
#include "dwio/nimble/encodings/EncodingSelection.h"
#include "dwio/nimble/encodings/legacy/EncodingSizeEstimation.h"

namespace facebook::nimble::legacy {

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
#define UNIQUE_PTR_FACTORY_EXTRA(data_type, class, extra_types, ...)        \
  switch (data_type) {                                                      \
    case facebook::nimble::DataType::Uint8: {                               \
      return std::make_unique<class<uint8_t extra_types>>(__VA_ARGS__);     \
    }                                                                       \
    case facebook::nimble::DataType::Int8: {                                \
      return std::make_unique<class<int8_t extra_types>>(__VA_ARGS__);      \
    }                                                                       \
    case facebook::nimble::DataType::Uint16: {                              \
      return std::make_unique<class<uint16_t extra_types>>(__VA_ARGS__);    \
    }                                                                       \
    case facebook::nimble::DataType::Int16: {                               \
      return std::make_unique<class<int16_t extra_types>>(__VA_ARGS__);     \
    }                                                                       \
    case facebook::nimble::DataType::Uint32: {                              \
      return std::make_unique<class<uint32_t extra_types>>(__VA_ARGS__);    \
    }                                                                       \
    case facebook::nimble::DataType::Int32: {                               \
      return std::make_unique<class<int32_t extra_types>>(__VA_ARGS__);     \
    }                                                                       \
    case facebook::nimble::DataType::Uint64: {                              \
      return std::make_unique<class<uint64_t extra_types>>(__VA_ARGS__);    \
    }                                                                       \
    case facebook::nimble::DataType::Int64: {                               \
      return std::make_unique<class<int64_t extra_types>>(__VA_ARGS__);     \
    }                                                                       \
    case facebook::nimble::DataType::Float: {                               \
      return std::make_unique<class<float extra_types>>(__VA_ARGS__);       \
    }                                                                       \
    case facebook::nimble::DataType::Double: {                              \
      return std::make_unique<class<double extra_types>>(__VA_ARGS__);      \
    }                                                                       \
    case facebook::nimble::DataType::Bool: {                                \
      return std::make_unique<class<bool extra_types>>(__VA_ARGS__);        \
    }                                                                       \
    case facebook::nimble::DataType::String: {                              \
      return std::make_unique<class<std::string_view extra_types>>(         \
          __VA_ARGS__);                                                     \
    }                                                                       \
    default: {                                                              \
      NIMBLE_UNREACHABLE("Unsupported data type {}.", toString(data_type)); \
    }                                                                       \
  }

#define UNIQUE_PTR_FACTORY(data_type, class, ...) \
  UNIQUE_PTR_FACTORY_EXTRA(data_type, class, , __VA_ARGS__)

struct CompressionOptions {
  float compressionAcceptRatio = 0.98f;
  uint64_t zstdMinCompressionSize = kZstdMinCompressionSize;
  uint32_t zstdCompressionLevel = 3;
  uint64_t internalMinCompressionSize = kMetaInternalMinCompressionSize;
  uint32_t internalCompressionLevel = 4;
  uint32_t internalDecompressionLevel = 2;
  bool useVariableBitWidthCompressor = false;
  MetaInternalCompressionKey metaInternalCompressionKey;
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
      auto size =
          nimble::legacy::detail::EncodingSizeEstimation<T, FixedByteWidth>::
              estimateSize(encodingType, values.size(), statistics);
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
            .compressionType = CompressionType::MetaInternal,
            .minCompressionSize =
                compressionOptions_.internalMinCompressionSize};
        information.parameters.metaInternal.compressionLevel =
            compressionOptions_.internalCompressionLevel;
        information.parameters.metaInternal.decompressionLevel =
            compressionOptions_.internalDecompressionLevel;
        information.parameters.metaInternal.useVariableBitWidthCompressor =
            compressionOptions_.useVariableBitWidthCompressor;
        information.parameters.metaInternal.compressionKey =
            compressionOptions_.metaInternalCompressionKey;
        return information;
#else
        CompressionInformation information{
            .compressionType = CompressionType::Zstd,
            .minCompressionSize = compressionOptions_.zstdMinCompressionSize};
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
    auto unique = statistics.uniqueCounts().value().size();
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
        ManualEncodingSelectionPolicyFactory::possibleEncodings();
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
      NIMBLE_CHECK_EQ(
          kv.size(),
          2,
          "Invalid read factor format. "
          "Expected format is <EncodingType>=<facor>;<EncodingType>=<facor>. "
          "Unable to parse '{}'.",
          part);
      auto value = folly::tryTo<float>(kv[1]);
      NIMBLE_CHECK(
          value.hasValue(),
          "Unable to parse read factor value '{}' in '{}'. Expected valid float value.",
          kv[1],
          part);
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
          "Unknown or unexpected read factor encoding '{}'. Allowed values: {}",
          key,
          folly::join(",", possibleEncodingStrings));
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

class ReplayedCompressionPolicy : public CompressionPolicy {
 public:
  explicit ReplayedCompressionPolicy(
      nimble::CompressionType compressionType,
      CompressionOptions compressionOptions)
      : compressionType_{compressionType},
        compressionOptions_{std::move(compressionOptions)} {}

  CompressionInformation compression() const override {
    if (compressionType_ == nimble::CompressionType::Uncompressed) {
      return {.compressionType = nimble::CompressionType::Uncompressed};
    }

    if (compressionType_ == nimble::CompressionType::Zstd) {
      CompressionInformation information{
          .compressionType = nimble::CompressionType::Zstd,
          .minCompressionSize = compressionOptions_.zstdMinCompressionSize};
      information.parameters.zstd.compressionLevel =
          compressionOptions_.zstdCompressionLevel;
      return information;
    }

    CompressionInformation information{
        .compressionType = nimble::CompressionType::MetaInternal,
        .minCompressionSize = compressionOptions_.internalMinCompressionSize};
    information.parameters.metaInternal.compressionLevel =
        compressionOptions_.internalCompressionLevel;
    information.parameters.metaInternal.decompressionLevel =
        compressionOptions_.internalDecompressionLevel;
    information.parameters.metaInternal.useVariableBitWidthCompressor =
        compressionOptions_.useVariableBitWidthCompressor;
    information.parameters.metaInternal.compressionKey =
        compressionOptions_.metaInternalCompressionKey;
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
class ReplayedEncodingSelectionPolicy : public EncodingSelectionPolicy<TInner> {
  using physicalType = typename nimble::TypeTraits<TInner>::physicalType;

 public:
  ReplayedEncodingSelectionPolicy(
      EncodingLayout encodingLayout,
      CompressionOptions compressionOptions,
      const EncodingSelectionPolicyFactory& encodingSelectionPolicyFactory)
      : encodingLayout_{std::move(encodingLayout)},
        compressionOptions_{std::move(compressionOptions)},
        encodingSelectionPolicyFactory_{encodingSelectionPolicyFactory} {}

  EncodingSelectionResult select(
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

  std::unique_ptr<EncodingSelectionPolicyBase> createImpl(
      nimble::EncodingType /* encodingType */,
      nimble::NestedEncodingIdentifier identifier,
      nimble::DataType type) override {
    NIMBLE_CHECK_LT(
        identifier,
        encodingLayout_.childrenCount(),
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

} // namespace facebook::nimble::legacy
