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
#include "dwio/nimble/encodings/selection/EncodingSelectionPolicy.h"

namespace facebook::nimble {

/* static */ std::vector<std::pair<EncodingType, float>>
ManualEncodingSelectionPolicyFactory::defaultEncodingReadFactors() {
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

/* static */ std::vector<std::pair<nimble::EncodingType, float>>
ManualEncodingSelectionPolicyFactory::parseEncodingReadFactors(
    const std::string& readFactorsConfig) {
  std::vector<std::pair<nimble::EncodingType, float>> encodingReadFactors;
  std::vector<std::string> parts;
  folly::split(';', folly::trimWhitespace(readFactorsConfig), parts);
  encodingReadFactors.reserve(parts.size());

  const auto possibleEncodings =
      ManualEncodingSelectionPolicyFactory::possibleEncodings();
  std::vector<std::string> possibleEncodingStrs;
  possibleEncodingStrs.reserve(possibleEncodings.size());
  std::transform(
      possibleEncodings.cbegin(),
      possibleEncodings.cend(),
      std::back_inserter(possibleEncodingStrs),
      [](const auto& encoding) { return toString(encoding); });
  for (const auto& part : parts) {
    std::vector<std::string> kv;
    folly::split('=', part, kv);
    NIMBLE_CHECK_EQ(
        kv.size(),
        2,
        "Invalid read factor format. "
        "Expected format is <EncodingType>=<factor>;<EncodingType>=<factor>. "
        "Unable to parse '{}'.",
        part);
    const auto value = folly::tryTo<float>(kv[1]);
    NIMBLE_CHECK(
        value.hasValue(),
        "Unable to parse read factor value '{}' in '{}'. Expected valid float value.",
        kv[1],
        part);
    bool found = false;
    const auto key = folly::trimWhitespace(kv[0]);
    for (auto i = 0; i < possibleEncodings.size(); ++i) {
      auto encoding = possibleEncodings[i];
      // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
      if (key == possibleEncodingStrs[i]) {
        found = true;
        encodingReadFactors.emplace_back(encoding, value.value());
        break;
      }
    }
    NIMBLE_CHECK(
        found,
        "Unknown or unexpected read factor encoding '{}'. Allowed values: {}",
        key,
        folly::join(",", possibleEncodingStrs));
  }
  return encodingReadFactors;
}

ManualEncodingSelectionPolicyFactory::ManualEncodingSelectionPolicyFactory(
    std::vector<std::pair<EncodingType, float>> encodingReadFactors,
    std::optional<CompressionOptions> compressionOptions)
    : encodingReadFactors_{std::move(encodingReadFactors)},
      compressionOptions_{std::move(compressionOptions)} {}

std::unique_ptr<EncodingSelectionPolicyBase>
ManualEncodingSelectionPolicyFactory::createPolicy(DataType dataType) const {
  UNIQUE_PTR_FACTORY(
      dataType,
      ManualEncodingSelectionPolicy,
      encodingReadFactors_,
      compressionOptions_,
      std::nullopt);
}

/* static */ std::vector<EncodingType>
ManualEncodingSelectionPolicyFactory::possibleEncodings() {
  return {
      EncodingType::Constant,
      EncodingType::Trivial,
      EncodingType::FixedBitWidth,
      EncodingType::MainlyConstant,
      EncodingType::SparseBool,
      EncodingType::Dictionary,
      EncodingType::RLE,
      EncodingType::Varint,
      // EXPERIMENTAL: The following encodings are not production-ready. Do not
      // enable for production tables without consulting the Nimble team
      // (oncall: dwios).
      EncodingType::ALP,
      EncodingType::PFOR,
      EncodingType::SimdForBitpack,
      EncodingType::SubIntSplit,
      EncodingType::BlockBitPacking,
      EncodingType::DeltaBlock,
      EncodingType::Fsst,
      EncodingType::Huffman,
  };
}

namespace {

// Trims leading/trailing whitespace, returning a view into the input.
std::string_view trimToken(std::string_view token) {
  const auto trimmed = folly::trimWhitespace(folly::StringPiece(token));
  return std::string_view(trimmed.data(), trimmed.size());
}

// Resolves a ';'-separated list of encoding names to EncodingTypes, matching
// against the full parseable set. Surfaces a NimbleUserError on an empty list
// or an unknown name.
std::vector<EncodingType> parseEncodingChoices(std::string_view value) {
  const auto candidates =
      ManualEncodingSelectionPolicyFactory::possibleEncodings();
  std::vector<EncodingType> choices;
  std::vector<std::string_view> names;
  folly::split(';', value, names);
  for (const auto rawName : names) {
    const auto name = trimToken(rawName);
    NIMBLE_USER_CHECK(
        !name.empty(),
        "Empty encoding name in nimble.encoding_selection_config.");
    bool found = false;
    for (const auto candidate : candidates) {
      if (name == toString(candidate)) {
        NIMBLE_USER_CHECK(
            std::find(choices.begin(), choices.end(), candidate) ==
                choices.end(),
            "Duplicate encoding '{}' in nimble.encoding_selection_config.",
            name);
        choices.push_back(candidate);
        found = true;
        break;
      }
    }
    NIMBLE_USER_CHECK(
        found,
        "Unknown encoding '{}' in nimble.encoding_selection_config.",
        name);
  }
  NIMBLE_USER_CHECK(
      !choices.empty(),
      "nimble.encoding_selection_config 'encodings' must list at least one encoding.");
  return choices;
}
} // namespace

/* static */ std::vector<EncodingType>
RandomEncodingSelectionPolicyFactory::defaultEncodingChoices() {
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

/* static */ std::optional<RandomEncodingSelectionPolicyFactory>
RandomEncodingSelectionPolicyFactory::create(
    std::string_view configStr,
    std::optional<CompressionOptions> compressionOptions) {
  if (configStr.empty()) {
    return std::nullopt;
  }
  std::optional<uint64_t> seed;
  std::vector<EncodingType> encodingChoices = defaultEncodingChoices();
  std::vector<std::string_view> rawEntries;
  folly::split(',', configStr, rawEntries);
  for (const auto rawEntry : rawEntries) {
    const auto colonPos = rawEntry.find(':');
    NIMBLE_USER_CHECK(
        colonPos != std::string_view::npos,
        "Malformed nimble.encoding_selection_config entry '{}'; want key:value.",
        rawEntry);
    const auto key = trimToken(rawEntry.substr(0, colonPos));
    const auto value = trimToken(rawEntry.substr(colonPos + 1));
    if (key == "seed") {
      const auto parsed = folly::tryTo<uint64_t>(folly::StringPiece(value));
      NIMBLE_USER_CHECK(
          parsed.hasValue(),
          "Invalid seed '{}' in nimble.encoding_selection_config.",
          value);
      seed = parsed.value();
    } else if (key == "encodings") {
      encodingChoices = parseEncodingChoices(value);
    } else {
      NIMBLE_USER_FAIL(
          "Unknown nimble.encoding_selection_config key '{}'.", key);
    }
  }
  NIMBLE_USER_CHECK(
      seed.has_value(), "nimble.encoding_selection_config requires a 'seed'.");
  return RandomEncodingSelectionPolicyFactory{
      seed.value(), std::move(encodingChoices), std::move(compressionOptions)};
}

RandomEncodingSelectionPolicyFactory::RandomEncodingSelectionPolicyFactory(
    uint64_t seed,
    std::vector<EncodingType> encodingChoices,
    std::optional<CompressionOptions> compressionOptions)
    : seed_{seed},
      encodingChoices_{std::move(encodingChoices)},
      compressionOptions_{std::move(compressionOptions)} {}

std::unique_ptr<EncodingSelectionPolicyBase>
RandomEncodingSelectionPolicyFactory::createPolicy(DataType dataType) const {
  // Derive the root policy's seed from the base seed and the column's data type
  // so top-level columns of different types diverge; createImpl then folds in
  // each nested slot. Deterministic and independent of encode thread order.
  const uint64_t rootSeed = folly::hash::hash_combine(
      seed_, static_cast<std::underlying_type_t<DataType>>(dataType));
  UNIQUE_PTR_FACTORY(
      dataType,
      RandomEncodingSelectionPolicy,
      encodingChoices_,
      rootSeed,
      compressionOptions_);
}
} // namespace facebook::nimble
