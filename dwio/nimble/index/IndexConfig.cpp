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
#include "dwio/nimble/index/IndexConfig.h"

#include <exception>

#include <folly/json.h>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/index/IndexSerialization.h"
#include "velox/common/config/Config.h"

namespace facebook::nimble::index {
namespace {

using ConfigBase = velox::config::ConfigBase;

constexpr std::string_view kColumns{"columns"};
constexpr std::string_view kSortOrders{"sortOrders"};
constexpr std::string_view kEnforceKeyOrder{"enforceKeyOrder"};
constexpr std::string_view kNoDuplicateKey{"noDuplicateKey"};
constexpr std::string_view kEncodingLayout{"encodingLayout"};
constexpr std::string_view kMaxRowsPerKeyChunk{"maxRowsPerKeyChunk"};
constexpr std::string_view kKeyChunkCompressionType{"keyChunkCompressionType"};
constexpr std::string_view kLoadFactor{"loadFactor"};
constexpr std::string_view kBloomFilterBitsPerKey{"bloomFilter.bitsPerKey"};
constexpr std::string_view kMaxPartitionSizeBytes{"maxPartitionSizeBytes"};

folly::dynamic parseJson(const std::string& key, const std::string& value) {
  try {
    return folly::parseJson(value);
  } catch (const std::exception& error) {
    NIMBLE_USER_FAIL(
        "Invalid JSON for index configuration '{}': {}", key, error.what());
  }
}

std::vector<std::string> parseStrings(
    const std::string& key,
    const std::string& value) {
  auto json = parseJson(key, value);
  NIMBLE_USER_CHECK(
      json.isArray(), "Index configuration must be a JSON array: {}", key);
  std::vector<std::string> result;
  result.reserve(json.size());
  for (const auto& element : json) {
    NIMBLE_USER_CHECK(
        element.isString(),
        "Index configuration array must contain strings: {}",
        key);
    result.emplace_back(element.asString());
  }
  return result;
}

std::string serializeStrings(const std::vector<std::string>& values) {
  return folly::toJson(folly::dynamic::array(values.begin(), values.end()));
}

std::vector<SortOrder> parseSortOrders(
    const std::string& key,
    const std::string& value) {
  auto json = parseJson(key, value);
  NIMBLE_USER_CHECK(
      json.isArray(), "Index configuration must be a JSON array: {}", key);
  std::vector<SortOrder> result;
  result.reserve(json.size());
  for (const auto& element : json) {
    NIMBLE_USER_CHECK(
        element.isBool(),
        "Index sort orders must be represented as booleans: {}",
        key);
    result.emplace_back(SortOrder{element.asBool()});
  }
  return result;
}

std::string serializeSortOrders(const std::vector<SortOrder>& sortOrders) {
  auto json = folly::dynamic::array();
  for (const auto& sortOrder : sortOrders) {
    json.push_back(sortOrder.ascending);
  }
  return folly::toJson(json);
}

EncodingType parseEncodingType(
    const std::string& key,
    const std::string& value) {
  auto type = encodingTypeFromString(value);
  NIMBLE_USER_CHECK(
      type.has_value(), "Unsupported encoding type for {}: {}", key, value);
  return type.value();
}

CompressionType parseCompressionType(
    const std::string& key,
    const std::string& value) {
  auto type = compressionTypeFromString(value);
  NIMBLE_USER_CHECK(
      type.has_value(),
      "Unsupported index compression type for {}: {}",
      key,
      value);
  return type.value();
}

const ConfigBase::Entry<const std::vector<std::string>> columnsEntry{
    std::string{kColumns},
    {},
    serializeStrings,
    parseStrings};
const ConfigBase::Entry<const std::vector<SortOrder>> sortOrdersEntry{
    std::string{kSortOrders},
    {},
    serializeSortOrders,
    parseSortOrders};
const ConfigBase::Entry<bool> enforceKeyOrderEntry{
    std::string{kEnforceKeyOrder},
    false};
const ConfigBase::Entry<bool> noDuplicateKeyEntry{
    std::string{kNoDuplicateKey},
    false};
const ConfigBase::Entry<uint64_t> clusterMaxRowsPerKeyChunkEntry{
    std::string{kMaxRowsPerKeyChunk},
    10'000};
const ConfigBase::Entry<CompressionType> keyChunkCompressionTypeEntry{
    std::string{kKeyChunkCompressionType},
    CompressionType::Uncompressed,
    [](CompressionType type) { return toString(type); },
    parseCompressionType};
const ConfigBase::Entry<uint64_t> sortedMaxRowsPerKeyChunkEntry{
    std::string{kMaxRowsPerKeyChunk},
    0};
const ConfigBase::Entry<float> loadFactorEntry{std::string{kLoadFactor}, 0.7f};
const ConfigBase::Entry<float> bloomFilterBitsPerKeyEntry{
    std::string{kBloomFilterBitsPerKey},
    10.0f};
const ConfigBase::Entry<uint64_t> maxPartitionSizeBytesEntry{
    std::string{kMaxPartitionSizeBytes},
    0};

const ConfigBase& options(const IndexConfig& config) {
  NIMBLE_USER_CHECK_NOT_NULL(
      config.options,
      "Index configuration options cannot be null: {}",
      config.name);
  return *config.options;
}

folly::dynamic encodingLayoutToJson(const EncodingLayout& layout) {
  folly::dynamic config = folly::dynamic::object();
  for (const auto& [key, value] : layout.config().values()) {
    config[key] = value;
  }

  auto children = folly::dynamic::array();
  for (NestedEncodingIdentifier i = 0; i < layout.childrenCount(); ++i) {
    const auto& child = layout.child(i);
    children.push_back(
        child.has_value() ? encodingLayoutToJson(child.value())
                          : folly::dynamic(nullptr));
  }

  return folly::dynamic::object("type", toString(layout.encodingType()))(
      "compression", toString(layout.compressionType()))(
      "config", std::move(config))("children", std::move(children));
}

const folly::dynamic& requiredField(
    const folly::dynamic& object,
    const char* field,
    const std::string& key) {
  NIMBLE_USER_CHECK(
      object.isObject() && object.get_ptr(field) != nullptr,
      "Index configuration '{}' must contain field '{}'",
      key,
      field);
  return object[field];
}

EncodingLayout encodingLayoutFromJson(
    const folly::dynamic& json,
    const std::string& key) {
  const auto& type = requiredField(json, "type", key);
  const auto& compression = requiredField(json, "compression", key);
  const auto& config = requiredField(json, "config", key);
  const auto& childrenJson = requiredField(json, "children", key);
  NIMBLE_USER_CHECK(
      type.isString(),
      "Encoding type in index configuration '{}' must be a string",
      key);
  NIMBLE_USER_CHECK(
      compression.isString(),
      "Compression type in index configuration '{}' must be a string",
      key);
  NIMBLE_USER_CHECK(
      config.isObject(),
      "Encoding config in index configuration '{}' must be an object",
      key);
  NIMBLE_USER_CHECK(
      childrenJson.isArray(),
      "Encoding children in index configuration '{}' must be an array",
      key);

  std::unordered_map<std::string, std::string> configValues;
  for (const auto& [configKey, value] : config.items()) {
    NIMBLE_USER_CHECK(
        configKey.isString() && value.isString(),
        "Encoding config in index configuration '{}' must contain string values",
        key);
    configValues.emplace(configKey.asString(), value.asString());
  }

  std::vector<std::optional<const EncodingLayout>> children;
  children.reserve(childrenJson.size());
  for (const auto& child : childrenJson) {
    children.emplace_back(
        child.isNull() ? std::nullopt
                       : std::optional<const EncodingLayout>{
                             encodingLayoutFromJson(child, key)});
  }

  return EncodingLayout{
      parseEncodingType(key, type.asString()),
      EncodingLayout::Config{std::move(configValues)},
      parseCompressionType(key, compression.asString()),
      std::move(children)};
}

std::string serializeEncodingLayout(const EncodingLayout& layout) {
  folly::json::serialization_opts options;
  options.sort_keys = true;
  return folly::json::serialize(encodingLayoutToJson(layout), options);
}

EncodingLayout parseEncodingLayout(
    const std::string& key,
    const std::string& value) {
  return encodingLayoutFromJson(parseJson(key, value), key);
}

const ConfigBase::Entry<const EncodingLayout> encodingLayoutEntry{
    std::string{kEncodingLayout},
    EncodingLayout{EncodingType::Prefix, {}, CompressionType::Uncompressed},
    serializeEncodingLayout,
    parseEncodingLayout};

std::unordered_map<std::string, std::string> makeKeyStreamOptions(
    const std::vector<std::string>& columns,
    const EncodingLayout& layout) {
  std::unordered_map<std::string, std::string> options{
      {columnsEntry.key, columnsEntry.toStr(columns)},
      {encodingLayoutEntry.key, encodingLayoutEntry.toStr(layout)},
  };
  return options;
}

std::shared_ptr<const ConfigBase> makeOptions(
    std::unordered_map<std::string, std::string> options) {
  return std::make_shared<const ConfigBase>(std::move(options));
}

} // namespace

IndexFamily toIndexFamily(serialization::IndexFamily family) {
  switch (family) {
    case serialization::IndexFamily_Cluster:
      return IndexFamily::Cluster;
    case serialization::IndexFamily_Dense:
      return IndexFamily::Dense;
  }
  NIMBLE_UNREACHABLE(
      "Unsupported serialized index family: {}", static_cast<int>(family));
}

serialization::IndexFamily toIndexFamily(IndexFamily family) {
  switch (family) {
    case IndexFamily::Cluster:
      return serialization::IndexFamily_Cluster;
    case IndexFamily::Dense:
      return serialization::IndexFamily_Dense;
  }
  NIMBLE_UNREACHABLE("Unsupported index family: {}", static_cast<int>(family));
}

IndexConfig toIndexConfig(ClusterIndexConfig config) {
  auto values = makeKeyStreamOptions(config.columns, config.encodingLayout);
  values.emplace(sortOrdersEntry.key, sortOrdersEntry.toStr(config.sortOrders));
  values.emplace(
      enforceKeyOrderEntry.key,
      enforceKeyOrderEntry.toStr(config.enforceKeyOrder));
  values.emplace(
      noDuplicateKeyEntry.key,
      noDuplicateKeyEntry.toStr(config.noDuplicateKey));
  values.emplace(
      clusterMaxRowsPerKeyChunkEntry.key,
      clusterMaxRowsPerKeyChunkEntry.toStr(config.maxRowsPerKeyChunk));
  values.emplace(
      keyChunkCompressionTypeEntry.key,
      keyChunkCompressionTypeEntry.toStr(config.keyChunkCompressionType));
  return IndexConfig{
      IndexFamily::Cluster,
      std::move(config.indexName),
      makeOptions(std::move(values))};
}

IndexConfig toIndexConfig(HashIndexConfig config) {
  std::unordered_map<std::string, std::string> values{
      {columnsEntry.key, columnsEntry.toStr(config.columns)},
      {loadFactorEntry.key, loadFactorEntry.toStr(config.loadFactor)},
      {maxPartitionSizeBytesEntry.key,
       maxPartitionSizeBytesEntry.toStr(config.maxPartitionSizeBytes)},
  };
  if (config.bloomFilter.has_value()) {
    values.emplace(
        bloomFilterBitsPerKeyEntry.key,
        bloomFilterBitsPerKeyEntry.toStr(config.bloomFilter->bitsPerKey));
  }
  return IndexConfig{
      IndexFamily::Dense,
      std::move(config.indexName),
      makeOptions(std::move(values))};
}

IndexConfig toIndexConfig(SortedIndexConfig config) {
  auto values = makeKeyStreamOptions(config.columns, config.encodingLayout);
  values.emplace(
      sortedMaxRowsPerKeyChunkEntry.key,
      sortedMaxRowsPerKeyChunkEntry.toStr(config.maxRowsPerKeyChunk));
  return IndexConfig{
      IndexFamily::Dense,
      std::move(config.indexName),
      makeOptions(std::move(values))};
}

ClusterIndexConfig toClusterIndexConfig(const IndexConfig& config) {
  const auto& values = options(config);
  return ClusterIndexConfig{
      .indexName = config.name,
      .columns = values.get(columnsEntry),
      .sortOrders = values.get(sortOrdersEntry),
      .enforceKeyOrder = values.get(enforceKeyOrderEntry),
      .noDuplicateKey = values.get(noDuplicateKeyEntry),
      .encodingLayout = values.get(encodingLayoutEntry),
      .maxRowsPerKeyChunk = values.get(clusterMaxRowsPerKeyChunkEntry),
      .keyChunkCompressionType = values.get(keyChunkCompressionTypeEntry),
  };
}

HashIndexConfig toHashIndexConfig(const IndexConfig& config) {
  const auto& values = options(config);
  return HashIndexConfig{
      .indexName = config.name,
      .columns = values.get(columnsEntry),
      .loadFactor = values.get(loadFactorEntry),
      .bloomFilter = values.valueExists(bloomFilterBitsPerKeyEntry.key)
          ? std::optional<BloomFilterConfig>{BloomFilterConfig{
                values.get(bloomFilterBitsPerKeyEntry)}}
          : std::nullopt,
      .maxPartitionSizeBytes = values.get(maxPartitionSizeBytesEntry),
  };
}

SortedIndexConfig toSortedIndexConfig(const IndexConfig& config) {
  const auto& values = options(config);
  return SortedIndexConfig{
      .indexName = config.name,
      .columns = values.get(columnsEntry),
      .encodingLayout = values.get(encodingLayoutEntry),
      .maxRowsPerKeyChunk = values.get(sortedMaxRowsPerKeyChunkEntry),
  };
}

} // namespace facebook::nimble::index
