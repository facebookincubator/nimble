// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <folly/json/dynamic.h>
#include <folly/json/json.h>

#include "dwio/alpha/encodings/EncodingSelectionPolicy.h"
#include "dwio/alpha/velox/Config.h"

DEFINE_string(
    alpha_selection_read_factors,
    "Constant=1.0;Trivial=0.5;FixedBitWidth=0.9;MainlyConstant=1.0;SparseBool=1.0;Dictionary=1.0;RLE=1.0;Varint=1.0",
    "Encoding selection read factors, in the format: "
    "<EncodingName>=<FactorFloatValue>;<EncodingName>=<FactorFloatValue>;...");

DEFINE_double(
    alpha_selection_compression_accept_ratio,
    0.97,
    "Encoding selection compression accept ratio.");

DEFINE_bool(
    alpha_zstrong_enable_variable_bit_width_compressor,
    false,
    "Enable zstrong variable bit width compressor at write time. Transparent at read time.");

DEFINE_string(
    alpha_writer_input_buffer_default_growth_config,
    "{\"32\":4.0,\"512\":1.414,\"4096\":1.189}",
    "Default growth config for writer input buffers, each entry in the format of {range_start,growth_factor}");

namespace facebook::alpha {
namespace {
template <typename T>
std::vector<T> parseVector(const std::string& str) {
  std::vector<T> result;
  if (!str.empty()) {
    std::vector<folly::StringPiece> pieces;
    folly::split(',', str, pieces, true);
    for (auto& p : pieces) {
      const auto& trimmedCol = folly::trimWhitespace(p);
      if (!trimmedCol.empty()) {
        result.push_back(folly::to<T>(trimmedCol));
      }
    }
  }
  return result;
}

std::map<uint64_t, float> parseGrowthConfigMap(const std::string& str) {
  std::map<uint64_t, float> ret;
  ALPHA_CHECK(!str.empty(), "Can't supply an empty growth config.");
  folly::dynamic json = folly::parseJson(str);
  for (const auto& pair : json.items()) {
    auto [_, inserted] = ret.emplace(
        folly::to<uint64_t>(pair.first.asString()), pair.second.asDouble());
    ALPHA_CHECK(
        inserted, fmt::format("Duplicate key: {}.", pair.first.asString()));
  }
  return ret;
}
} // namespace

/* static */ Config::Entry<bool> Config::FLATTEN_MAP("orc.flatten.map", false);

/* static */ Config::Entry<const std::vector<uint32_t>> Config::MAP_FLAT_COLS(
    "orc.map.flat.cols",
    {},
    [](const std::vector<uint32_t>& val) { return folly::join(",", val); },
    [](const std::string& /* key */, const std::string& val) {
      return parseVector<uint32_t>(val);
    });

/* static */ Config::Entry<const std::vector<uint32_t>>
    Config::BATCH_REUSE_COLS(
        "alpha.dictionaryarray.cols",
        {},
        [](const std::vector<uint32_t>& val) { return folly::join(",", val); },
        [](const std::string& /* key */, const std::string& val) {
          return parseVector<uint32_t>(val);
        });

/* static */ Config::Entry<uint64_t> Config::RAW_STRIPE_SIZE(
    "alpha.raw.stripe.size",
    256L * 1024L * 1024L);

/* static */ Config::Entry<const std::vector<std::pair<EncodingType, float>>>
    Config::MANUAL_ENCODING_SELECTION_READ_FACTORS(
        "alpha.encodingselection.read.factors",
        ManualEncodingSelectionPolicyFactory::parseReadFactors(
            FLAGS_alpha_selection_read_factors),
        [](const std::vector<std::pair<EncodingType, float>>& val) {
          std::vector<std::string> encodingFactorStrings;
          std::transform(
              val.cbegin(),
              val.cend(),
              std::back_inserter(encodingFactorStrings),
              [](const auto& readFactor) {
                return fmt::format(
                    "{}={}", toString(readFactor.first), readFactor.second);
              });
          return folly::join(";", encodingFactorStrings);
        },
        [](const std::string& /* key */, const std::string& val) {
          return ManualEncodingSelectionPolicyFactory::parseReadFactors(val);
        });

/* static */ Config::Entry<float>
    Config::ENCODING_SELECTION_COMPRESSION_ACCEPT_RATIO(
        "alpha.encodingselection.compression.accept.ratio",
        FLAGS_alpha_selection_compression_accept_ratio);

/* static */ Config::Entry<uint32_t> Config::ZSTRONG_COMPRESSION_LEVEL(
    "alpha.zstrong.compression.level",
    4);

/* static */ Config::Entry<uint32_t> Config::ZSTRONG_DECOMPRESSION_LEVEL(
    "alpha.zstrong.decompression.level",
    2);

/* static */ Config::Entry<bool>
    Config::ENABLE_ZSTRONG_VARIABLE_BITWIDTH_COMPRESSOR(
        "alpha.zstrong.enable.variable.bit.width.compressor",
        FLAGS_alpha_zstrong_enable_variable_bit_width_compressor);

/* static */ Config::Entry<const std::map<uint64_t, float>>
    Config::INPUT_BUFFER_DEFAULT_GROWTH_CONFIGS(
        "alpha.writer.input.buffer.default.growth.configs",
        parseGrowthConfigMap(
            FLAGS_alpha_writer_input_buffer_default_growth_config),
        [](const std::map<uint64_t, float>& val) {
          folly::dynamic obj = folly::dynamic::object;
          for (const auto& [rangeStart, growthFactor] : val) {
            obj[folly::to<std::string>(rangeStart)] = growthFactor;
          }
          return folly::toJson(obj);
        },
        [](const std::string& /* key */, const std::string& val) {
          return parseGrowthConfigMap(val);
        });
} // namespace facebook::alpha
