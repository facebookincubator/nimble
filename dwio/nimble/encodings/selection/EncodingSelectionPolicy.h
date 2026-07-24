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

#include <folly/hash/Hash.h>
#include <glog/logging.h>
#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include "dwio/nimble/common/Constants.h"
#include "dwio/nimble/encodings/common/EncodingLayout.h"
#include "dwio/nimble/encodings/common/EncodingType.h"
#include "dwio/nimble/encodings/selection/EncodingIdentifier.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"
#include "dwio/nimble/encodings/selection/EncodingSizeEstimation.h"
#include "velox/common/base/SuccinctPrinter.h"

namespace facebook::nimble {

using EncodingSelectionPolicyCreator =
    std::function<std::unique_ptr<EncodingSelectionPolicyBase>(DataType)>;

// The following enables encoding selection debug messages. By default, these
// logs are turned off (with zero overhead). In tests (or in debug sessions), we
// enable these logs.
#ifndef NIMBLE_ENCODING_SELECTION_DEBUG_MAX_ITEMS
#define NIMBLE_ENCODING_SELECTION_DEBUG_MAX_ITEMS 50
#endif

#ifdef NIMBLE_ENCODING_SELECTION_DEBUG
#define NIMBLE_SELECTION_LOG(stream) LOG(INFO) << stream
#else
#define NIMBLE_SELECTION_LOG(stream)
#endif

#define COMMA ,
#define UNIQUE_PTR_FACTORY_EXTRA(data_type, class, extra_types, ...)     \
  switch (data_type) {                                                   \
    case facebook::nimble::DataType::Uint8: {                            \
      return std::make_unique<class<uint8_t extra_types>>(__VA_ARGS__);  \
    }                                                                    \
    case facebook::nimble::DataType::Int8: {                             \
      return std::make_unique<class<int8_t extra_types>>(__VA_ARGS__);   \
    }                                                                    \
    case facebook::nimble::DataType::Uint16: {                           \
      return std::make_unique<class<uint16_t extra_types>>(__VA_ARGS__); \
    }                                                                    \
    case facebook::nimble::DataType::Int16: {                            \
      return std::make_unique<class<int16_t extra_types>>(__VA_ARGS__);  \
    }                                                                    \
    case facebook::nimble::DataType::Uint32: {                           \
      return std::make_unique<class<uint32_t extra_types>>(__VA_ARGS__); \
    }                                                                    \
    case facebook::nimble::DataType::Int32: {                            \
      return std::make_unique<class<int32_t extra_types>>(__VA_ARGS__);  \
    }                                                                    \
    case facebook::nimble::DataType::Uint64: {                           \
      return std::make_unique<class<uint64_t extra_types>>(__VA_ARGS__); \
    }                                                                    \
    case facebook::nimble::DataType::Int64: {                            \
      return std::make_unique<class<int64_t extra_types>>(__VA_ARGS__);  \
    }                                                                    \
    case facebook::nimble::DataType::Float: {                            \
      return std::make_unique<class<float extra_types>>(__VA_ARGS__);    \
    }                                                                    \
    case facebook::nimble::DataType::Double: {                           \
      return std::make_unique<class<double extra_types>>(__VA_ARGS__);   \
    }                                                                    \
    case facebook::nimble::DataType::Bool: {                             \
      return std::make_unique<class<bool extra_types>>(__VA_ARGS__);     \
    }                                                                    \
    case facebook::nimble::DataType::String: {                           \
      return std::make_unique<class<std::string_view extra_types>>(      \
          __VA_ARGS__);                                                  \
    }                                                                    \
    case facebook::nimble::DataType::Undefined:                          \
      break;                                                             \
  }                                                                      \
  NIMBLE_UNREACHABLE("Unsupported data type {}.", toString(data_type))

#define UNIQUE_PTR_FACTORY(data_type, class, ...) \
  UNIQUE_PTR_FACTORY_EXTRA(data_type, class, , __VA_ARGS__)

/// Manual encoding selection implementation.
/// Uses a manually crafted model to choose the most appropriate encoding based
/// on the provided statistics.
template <typename T>
class ManualEncodingSelectionPolicy : public EncodingSelectionPolicy<T> {
 public:
  using physicalType = typename TypeTraits<T>::physicalType;

  ManualEncodingSelectionPolicy(
      std::vector<std::pair<EncodingType, float>> encodingReadFactors,
      std::optional<CompressionOptions> compressionOptions,
      std::optional<NestedEncodingIdentifier> identifier)
      : candidateEncodingReadFactors_{std::move(encodingReadFactors)},
        compressionOptions_{std::move(compressionOptions)},
        identifier_{identifier} {}

  EncodingSelectionResult select(
      std::span<const physicalType> values,
      const Statistics<physicalType>& statistics,
      const Encoding::Options& options) override {
    if (values.empty()) {
      return {
          .encodingType = EncodingType::Trivial,
      };
    }

    auto candidateEncodingReadFactors = candidateEncodingReadFactors_;
    // TODO: Remove this opt-in once ALP is production-ready for default
    // selection.
    if constexpr (isFloatingPointType<T>()) {
      if (options.allowNestedAlpSelection &&
          (identifier_ == EncodingIdentifiers::Dictionary::Alphabet ||
           identifier_ == EncodingIdentifiers::MainlyConstant::OtherValues ||
           identifier_ == EncodingIdentifiers::RunLength::RunValues) &&
          std::none_of(
              candidateEncodingReadFactors.begin(),
              candidateEncodingReadFactors.end(),
              [](const auto& entry) {
                return entry.first == EncodingType::ALP;
              })) {
        candidateEncodingReadFactors.emplace_back(EncodingType::ALP, 1.0);
      }
    }

    // Fast path: when there are no candidate encodings, fall back to Trivial.
    if (candidateEncodingReadFactors.empty()) {
      return {
          .encodingType = EncodingType::Trivial,
      };
    }

    float minCost = std::numeric_limits<float>::max();
    EncodingType selectedEncoding = EncodingType::Trivial;
    // Iterate on all candidate encodings, and pick the encoding with the
    // minimal cost.
    for (const auto& entry : candidateEncodingReadFactors) {
      const auto encodingType = entry.first;
      const auto estimatedSize =
          detail::EncodingSizeEstimation<T>::estimateSize(
              encodingType, values, statistics, options);
      if (!estimatedSize.has_value()) {
        NIMBLE_SELECTION_LOG(encodingType << " encoding is incompatible.");
        continue;
      }

      // We use read factor weights to raise/lower the favorability of each
      // encoding.
      const auto readFactor = entry.second;
      const auto cost = estimatedSize.value() * readFactor;
      NIMBLE_SELECTION_LOG(
          "Encoding: " << encodingType << ", Size: "
                       << velox::succinctBytes(estimatedSize.value())
                       << ", Factor: " << readFactor << ", Cost: " << cost);
      if (cost < minCost) {
        minCost = cost;
        selectedEncoding = encodingType;
      }
    }

    NIMBLE_SELECTION_LOG(
        "Selected Encoding"
        << (identifier_.has_value()
                ? folly::to<std::string>(
                      " [NestedEncodingIdentifier: ", identifier_.value(), "]")
                : "")
        << ": " << selectedEncoding << ", Sampled Data: "
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
    if (!compressionOptions_.has_value()) {
      return {.encodingType = selectedEncoding};
    }
    // Encoding selection optimizes the in-memory layout. Compression is still
    // attempted for leaf data streams to reduce persistent storage size.
    return {
        .encodingType = selectedEncoding,
        .compressionPolicyFactory = [compressionOptions =
                                         compressionOptions_.value(),
                                     selectedEncoding]() {
          return std::make_unique<ConfiguredCompressionPolicy>(
              compressionOptions, selectedEncoding);
        }};
  }

  EncodingSelectionResult selectNullable(
      std::span<const physicalType> /* values */,
      std::span<const bool> /* nulls */,
      const Statistics<physicalType>& /* statistics */,
      const Encoding::Options& /* options */) override {
    return {
        .encodingType = EncodingType::Nullable,
    };
  }

  const std::vector<std::pair<EncodingType, float>>&
  candidateEncodingReadFactors() const {
    return candidateEncodingReadFactors_;
  }

 protected:
  std::unique_ptr<EncodingSelectionPolicyBase> createImpl(
      EncodingType parentEncodingType,
      NestedEncodingIdentifier nestedEncodingIdentifier,
      DataType nestedDataType) override {
    // In each sub-level of the encoding selection, we exclude the encodings
    // selected in parent levels. Although this is not required (as hopefully,
    // the model will not pick a nested encoding of the same type as the
    // parent), it provides an additional safety net, making sure the encoding
    // selection will eventually converge, and also slightly speeds up nested
    // encoding selection.
    // TODO: validate the assumptions here compared to brute forcing, to see if
    // the same encoding is selected multiple times in the tree (for example,
    // should we allow trivial string lengths to be encoded using trivial
    // encoding?)
    std::vector<std::pair<EncodingType, float>> nestedEncodingReadFactors;
    nestedEncodingReadFactors.reserve(candidateEncodingReadFactors_.size());
    for (const auto& entry : candidateEncodingReadFactors_) {
      if (entry.first != parentEncodingType) {
        nestedEncodingReadFactors.emplace_back(entry);
      }
    }
    UNIQUE_PTR_FACTORY(
        nestedDataType,
        ManualEncodingSelectionPolicy,
        std::move(nestedEncodingReadFactors),
        compressionOptions_,
        nestedEncodingIdentifier);
  }

 private:
  // Candidate encodings and their read-cost factors. Encoding selection uses
  // estimatedSize * readFactor as the cost, so a lower factor makes an
  // encoding more likely to be picked. Right now, these represent mostly the
  // CPU cost to decode values. Trivial is boosted with a lower factor because
  // it also benefits from applying compression.
  // See ManualEncodingSelectionPolicyFactory::defaultEncodingReadFactors for
  // the default.
  const std::vector<std::pair<EncodingType, float>>
      candidateEncodingReadFactors_;
  // When nullopt, compression is disabled for this policy and all nested
  // policies created from it.
  const std::optional<CompressionOptions> compressionOptions_;
  const std::optional<NestedEncodingIdentifier> identifier_;
};

class ManualEncodingSelectionPolicyFactory {
 public:
  /// TODO: Add ALP once it is production-ready for default manual selection.
  static std::vector<std::pair<EncodingType, float>>
  defaultEncodingReadFactors();

  /// Parses semicolon-delimited runtime read-factor config.
  /// Allows explicit opt-in to parseable encodings that are not part of
  /// defaultEncodingReadFactors().
  static std::vector<std::pair<nimble::EncodingType, float>>
  parseEncodingReadFactors(const std::string& readFactorsConfig);

  ManualEncodingSelectionPolicyFactory(
      std::vector<std::pair<EncodingType, float>> encodingReadFactors =
          defaultEncodingReadFactors(),
      std::optional<CompressionOptions> compressionOptions =
          CompressionOptions{});

  std::unique_ptr<EncodingSelectionPolicyBase> createPolicy(
      DataType dataType) const;

  /// All encodings the string-config parsers accept (production +
  /// experimental). Also used to resolve encoding names to EncodingType.
  static std::vector<EncodingType> possibleEncodings();

 private:
  const std::vector<std::pair<EncodingType, float>> encodingReadFactors_;
  const std::optional<CompressionOptions> compressionOptions_;
};

/// Learned encoding selection implementation.
template <typename T>
class LearnedEncodingSelectionPolicy : public EncodingSelectionPolicy<T> {
  using physicalType = typename TypeTraits<T>::physicalType;

 public:
  /// Model trained offline for learned encoding selection.
  /// Parameters are relatively robust and do not need updates unless encodings
  /// are added or removed.
  struct EncodingPredictionModel {
    explicit EncodingPredictionModel()
        : maxRepeatParam(1.52), minRepeatParam(1.13), uniqueParam(2.589) {}

    float maxRepeatParam;
    float minRepeatParam;
    float uniqueParam;

    float predict(const Statistics<physicalType>& statistics) {
      // TODO: Utilize more features within statistics for prediction.
      const auto maxRepeat = statistics.maxRepeat();
      const auto minRepeat = statistics.minRepeat();
      const auto unique = statistics.uniqueCounts().value().size();
      return maxRepeatParam * maxRepeat + minRepeatParam * minRepeat +
          uniqueParam * unique;
    }
  };

  LearnedEncodingSelectionPolicy(
      std::vector<EncodingType> encodingChoices,
      std::optional<NestedEncodingIdentifier> identifier,
      EncodingPredictionModel encodingModel = EncodingPredictionModel())
      : encodingChoices_{std::move(encodingChoices)},
        identifier_{identifier},
        mlModel_{encodingModel} {}

  LearnedEncodingSelectionPolicy()
      : LearnedEncodingSelectionPolicy{
            possibleEncodingChoices(),
            std::nullopt,
            EncodingPredictionModel()} {}

  EncodingSelectionResult select(
      std::span<const physicalType> values,
      const Statistics<physicalType>& statistics,
      const Encoding::Options& options) override;

  EncodingSelectionResult selectNullable(
      std::span<const physicalType> /* values */,
      std::span<const bool> /* nulls */,
      const Statistics<physicalType>& /* statistics */,
      const Encoding::Options& /* options */) override {
    return {
        .encodingType = EncodingType::Nullable,
    };
  }

  std::unique_ptr<EncodingSelectionPolicyBase> createImpl(
      EncodingType parentEncodingType,
      NestedEncodingIdentifier nestedEncodingIdentifier,
      DataType nestedDataType) override {
    // In each sub-level of the encoding selection, we exclude the encodings
    // selected in parent levels. Although this is not required (as hopefully,
    // the model will not pick a nested encoding of the same type as the
    // parent), it provides an additional safety net, making sure the encoding
    // selection will eventually converge, and also slightly speeds up nested
    // encoding selection.
    //
    // TODO: validate the assumptions here compared to brute forcing, to see if
    // the same encoding is selected multiple times in the tree (for example,
    // should we allow trivial string lengths to be encoded using trivial
    // encoding?)
    std::vector<EncodingType> nestedEncodingChoices;
    nestedEncodingChoices.reserve(encodingChoices_.size());
    for (const auto& encodingType_ : encodingChoices_) {
      if (encodingType_ != parentEncodingType) {
        nestedEncodingChoices.push_back(encodingType_);
      }
    }
    UNIQUE_PTR_FACTORY(
        nestedDataType,
        LearnedEncodingSelectionPolicy,
        std::move(nestedEncodingChoices),
        nestedEncodingIdentifier);
  }

 private:
  // TODO: Add ALP once it is production-ready for learned selection.
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
  EncodingPredictionModel mlModel_;
};

template <typename T>
EncodingSelectionResult LearnedEncodingSelectionPolicy<T>::select(
    std::span<const typename LearnedEncodingSelectionPolicy<T>::physicalType>
        values,
    const Statistics<typename LearnedEncodingSelectionPolicy<T>::physicalType>&
        statistics,
    const Encoding::Options& /* options */) {
  if (values.empty()) {
    return {
        .encodingType = EncodingType::Trivial,
    };
  }

  const auto prediction = mlModel_.predict(statistics);
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

/// Randomized encoding selection for fuzz/stress testing. For each stream it
/// picks uniformly at random among the encodings that are compatible with the
/// data. Compatibility is defined exactly as encoding-size estimability:
/// EncodingSizeEstimation returns nullopt for any encoding that cannot encode
/// the given physical type or data statistics (e.g. Dictionary/FixedBitWidth/
/// Varint on bool, Varint on non-integers, Constant on non-constant data), so
/// reusing that signal keeps the random pick compatible without a
/// hand-maintained rule table, and the writer's one-shot IncompatibleEncoding
/// fallback is never relied upon.
template <typename T>
class RandomEncodingSelectionPolicy : public EncodingSelectionPolicy<T> {
  using physicalType = typename TypeTraits<T>::physicalType;

 public:
  RandomEncodingSelectionPolicy(
      std::vector<EncodingType> encodingChoices,
      uint64_t seed,
      std::optional<CompressionOptions> compressionOptions =
          CompressionOptions{})
      : encodingChoices_{std::move(encodingChoices)},
        seed_{seed},
        compressionOptions_{std::move(compressionOptions)} {}

  EncodingSelectionResult select(
      std::span<const physicalType> values,
      const Statistics<physicalType>& statistics,
      const Encoding::Options& options) override {
    // Empty streams (e.g. the values of an all-null column, or an empty nested
    // stream) can only be encoded trivially; several encodings hard-fail on
    // zero rows. Matches Manual/Learned selection.
    if (values.empty()) {
      return {
          .encodingType = EncodingType::Trivial,
      };
    }

    // Keep only encodings that can estimate a size for this data. An absent
    // estimate means the encoding is incompatible with the physical type or the
    // data's statistics, so excluding it here is what keeps the random pick
    // compatible.
    std::vector<EncodingType> compatibleEncodings;
    compatibleEncodings.reserve(encodingChoices_.size());
    for (const auto& encodingType : encodingChoices_) {
      if (detail::EncodingSizeEstimation<T>::estimateSize(
              encodingType, values, statistics, options)
              .has_value()) {
        compatibleEncodings.push_back(encodingType);
      }
    }

    if (compatibleEncodings.empty()) {
      return {
          .encodingType = EncodingType::Trivial,
      };
    }

    // Seed a fresh generator from this policy's derived seed so the single pick
    // is deterministic and independent of encode thread order.
    std::mt19937_64 generator{seed_};
    std::uniform_int_distribution<size_t> distribution(
        0, compatibleEncodings.size() - 1);
    const auto selectedEncoding = compatibleEncodings[distribution(generator)];

    if (!compressionOptions_.has_value()) {
      return {.encodingType = selectedEncoding};
    }
    // Encoding selection optimizes the in-memory layout. Compression is still
    // attempted for leaf data streams to reduce persistent storage size.
    return {
        .encodingType = selectedEncoding,
        .compressionPolicyFactory = [compressionOptions =
                                         compressionOptions_.value(),
                                     selectedEncoding]() {
          return std::make_unique<ConfiguredCompressionPolicy>(
              compressionOptions, selectedEncoding);
        }};
  }

  EncodingSelectionResult selectNullable(
      std::span<const physicalType> /* values */,
      std::span<const bool> /* nulls */,
      const Statistics<physicalType>& /* statistics */,
      const Encoding::Options& /* options */) override {
    return {
        .encodingType = EncodingType::Nullable,
    };
  }

 protected:
  std::unique_ptr<EncodingSelectionPolicyBase> createImpl(
      EncodingType parentEncodingType,
      NestedEncodingIdentifier nestedEncodingIdentifier,
      DataType nestedDataType) override {
    // Exclude the parent encoding from nested choices so the recursive
    // selection always converges (mirrors Manual/Learned).
    std::vector<EncodingType> nestedEncodingChoices;
    nestedEncodingChoices.reserve(encodingChoices_.size());
    for (const auto& encodingType : encodingChoices_) {
      if (encodingType != parentEncodingType) {
        nestedEncodingChoices.push_back(encodingType);
      }
    }
    // Fold this policy's seed with the parent encoding and the child slot so
    // every node in the tree gets a distinct seed that depends only on its
    // structural path, never on encode timing. This keeps the whole random
    // layout reproducible from the single base seed even when streams are
    // encoded concurrently.
    const uint64_t nestedSeed = folly::hash::hash_combine(
        seed_,
        static_cast<std::underlying_type_t<EncodingType>>(parentEncodingType),
        nestedEncodingIdentifier);
    UNIQUE_PTR_FACTORY(
        nestedDataType,
        RandomEncodingSelectionPolicy,
        std::move(nestedEncodingChoices),
        nestedSeed,
        compressionOptions_);
  }

 private:
  std::vector<EncodingType> encodingChoices_;
  uint64_t seed_;
  std::optional<CompressionOptions> compressionOptions_;
};

/// Produces RandomEncodingSelectionPolicy instances seeded deterministically
/// from a single base seed, so an entire file's random encoding tree is
/// reproducible from that seed. Intended for fuzz/stress testing only.
class RandomEncodingSelectionPolicyFactory {
 public:
  /// The candidate encodings the random policy draws from (the production
  /// Learned/Manual default set). EncodingSizeEstimation filters this per
  /// stream down to the encodings compatible with the actual data.
  static std::vector<EncodingType> defaultEncodingChoices();

  /// Parses a "seed:<n>[,encodings:<E1>;<E2>;...]" config string into a factory
  /// (mirrors FlushPolicyFactoryFactory). 'seed' is required; 'encodings' is an
  /// optional ';'-separated subset that defaults to defaultEncodingChoices().
  /// Returns nullopt for an empty string; throws NimbleUserError on malformed
  /// input. Intended for test-only config wiring.
  static std::optional<RandomEncodingSelectionPolicyFactory> create(
      std::string_view configStr,
      std::optional<CompressionOptions> compressionOptions =
          CompressionOptions{});

  explicit RandomEncodingSelectionPolicyFactory(
      uint64_t seed,
      std::vector<EncodingType> encodingChoices = defaultEncodingChoices(),
      std::optional<CompressionOptions> compressionOptions =
          CompressionOptions{});

  std::unique_ptr<EncodingSelectionPolicyBase> createPolicy(
      DataType dataType) const;

 private:
  uint64_t seed_;
  std::vector<EncodingType> encodingChoices_;
  std::optional<CompressionOptions> compressionOptions_;
};

template <typename T>
class ReplayedEncodingSelectionPolicy
    : public nimble::EncodingSelectionPolicy<T> {
 public:
  using physicalType = typename nimble::TypeTraits<T>::physicalType;

  ReplayedEncodingSelectionPolicy(
      EncodingLayout encodingLayout,
      std::optional<CompressionOptions> compressionOptions,
      const EncodingSelectionPolicyCreator& encodingSelectionPolicyCreator)
      : compressionOptions_{std::move(compressionOptions)},
        encodingSelectionPolicyCreator_{encodingSelectionPolicyCreator},
        encodingLayout_{std::move(encodingLayout)} {}

  nimble::EncodingSelectionResult select(
      std::span<const physicalType> /* values */,
      const nimble::Statistics<physicalType>& /* statistics */,
      const Encoding::Options& /* options */) override {
    if (!compressionOptions_.has_value()) {
      return {
          .encodingType = encodingLayout_.encodingType(),
          .encodingConfig = encodingLayout_.config(),
      };
    }
    return {
        .encodingType = encodingLayout_.encodingType(),
        .encodingConfig = encodingLayout_.config(),
        .compressionPolicyFactory = [this]() {
          return std::make_unique<ReplayedCompressionPolicy>(
              encodingLayout_.compressionType(), compressionOptions_.value());
        }};
  }

  EncodingSelectionResult selectNullable(
      std::span<const physicalType> /* values */,
      std::span<const bool> /* nulls */,
      const Statistics<physicalType>& /* statistics */,
      const Encoding::Options& /* options */) override {
    // NullableEncoding asks createImpl() for nullable data and nulls children.
    // The replay policy is initialized with the data layout, so synthesize the
    // nullable parent shape here.
    encodingLayout_ = EncodingLayout{
        EncodingType::Nullable,
        {},
        CompressionType::Uncompressed,
        {
            /*Data=*/std::move(encodingLayout_),
            /*Nulls=*/std::nullopt,
        }};
    return {
        .encodingType = EncodingType::Nullable,
    };
  }

 protected:
  std::unique_ptr<nimble::EncodingSelectionPolicyBase> createImpl(
      nimble::EncodingType /* parentEncodingType */,
      nimble::NestedEncodingIdentifier nestedEncodingIdentifier,
      nimble::DataType nestedDataType) override {
    NIMBLE_CHECK_LT(
        nestedEncodingIdentifier,
        encodingLayout_.childrenCount(),
        "Sub-encoding identifier out of range.");
    auto child = encodingLayout_.child(nestedEncodingIdentifier);

    if (child.has_value()) {
      UNIQUE_PTR_FACTORY(
          nestedDataType,
          ReplayedEncodingSelectionPolicy,
          child.value(),
          compressionOptions_,
          encodingSelectionPolicyCreator_);
    } else {
      return encodingSelectionPolicyCreator_(nestedDataType);
    }
  }

 private:
  const std::optional<CompressionOptions> compressionOptions_;
  const EncodingSelectionPolicyCreator encodingSelectionPolicyCreator_;
  EncodingLayout encodingLayout_;
};

#undef COMMA

} // namespace facebook::nimble
