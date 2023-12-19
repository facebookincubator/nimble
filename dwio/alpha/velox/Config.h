// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/common/Types.h"
#include "velox/common/config/Config.h"

namespace facebook::alpha {

class Config : public velox::common::ConfigBase<Config> {
 public:
  template <typename T>
  using Entry = velox::common::ConfigBase<Config>::Entry<T>;

  static Entry<bool> FLATTEN_MAP;
  static Entry<const std::vector<uint32_t>> MAP_FLAT_COLS;
  static Entry<const std::vector<uint32_t>> BATCH_REUSE_COLS;
  static Entry<uint64_t> RAW_STRIPE_SIZE;
  static Entry<const std::vector<std::pair<EncodingType, float>>>
      MANUAL_ENCODING_SELECTION_READ_FACTORS;
  static Entry<float> ENCODING_SELECTION_COMPRESSION_ACCEPT_RATIO;
  static Entry<uint32_t> ZSTRONG_COMPRESSION_LEVEL;
  static Entry<uint32_t> ZSTRONG_DECOMPRESSION_LEVEL;
  static Entry<bool> ENABLE_ZSTRONG_VARIABLE_BITWIDTH_COMPRESSOR;
  static Entry<const std::map<uint64_t, float>>
      INPUT_BUFFER_DEFAULT_GROWTH_CONFIGS;

  static std::shared_ptr<Config> fromMap(
      const std::map<std::string, std::string>& map) {
    auto ret = std::make_shared<Config>();
    ret->configs_.insert(map.cbegin(), map.cend());
    return ret;
  }
};

} // namespace facebook::alpha
