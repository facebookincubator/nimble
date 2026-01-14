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
#include "dwio/nimble/velox/stats/ColumnStatistics.h"

#include "dwio/nimble/common/Exceptions.h"

namespace facebook::nimble {

ColumnStatistics::ColumnStatistics(
    uint64_t valueCount,
    uint64_t nullCount,
    uint64_t logicalSize,
    uint64_t physicalSize)
    : valueCount_{valueCount},
      nullCount_{nullCount},
      logicalSize_{logicalSize},
      physicalSize_{physicalSize} {}

uint64_t ColumnStatistics::getValueCount() const {
  return valueCount_;
}

uint64_t ColumnStatistics::getNullCount() const {
  return nullCount_;
}

uint64_t ColumnStatistics::getLogicalSize() const {
  return logicalSize_;
}

uint64_t ColumnStatistics::getPhysicalSize() const {
  return physicalSize_;
}

StatType ColumnStatistics::getType() const {
  return StatType::DEFAULT;
}

StringStatistics::StringStatistics(
    uint64_t valueCount,
    uint64_t nullCount,
    uint64_t logicalSize,
    uint64_t physicalSize,
    std::optional<std::string> min,
    std::optional<std::string> max)
    : ColumnStatistics(valueCount, nullCount, logicalSize, physicalSize),
      min_{std::move(min)},
      max_{std::move(max)} {}

std::optional<std::string> StringStatistics::getMin() const {
  return min_;
}

std::optional<std::string> StringStatistics::getMax() const {
  return max_;
}

StatType StringStatistics::getType() const {
  return StatType::STRING;
}

IntegralStatistics::IntegralStatistics(
    uint64_t valueCount,
    uint64_t nullCount,
    uint64_t logicalSize,
    uint64_t physicalSize,
    std::optional<int64_t> min,
    std::optional<int64_t> max)
    : ColumnStatistics(valueCount, nullCount, logicalSize, physicalSize),
      min_{std::move(min)},
      max_{std::move(max)} {}

std::optional<int64_t> IntegralStatistics::getMin() const {
  return min_;
}

std::optional<int64_t> IntegralStatistics::getMax() const {
  return max_;
}

StatType IntegralStatistics::getType() const {
  return StatType::INTEGRAL;
}

FloatingPointStatistics::FloatingPointStatistics(
    uint64_t valueCount,
    uint64_t nullCount,
    uint64_t logicalSize,
    uint64_t physicalSize,
    std::optional<double> min,
    std::optional<double> max)
    : ColumnStatistics(valueCount, nullCount, logicalSize, physicalSize),
      min_{std::move(min)},
      max_{std::move(max)} {}

std::optional<double> FloatingPointStatistics::getMin() const {
  return min_;
}

std::optional<double> FloatingPointStatistics::getMax() const {
  return max_;
}

StatType FloatingPointStatistics::getType() const {
  return StatType::FLOATING_POINT;
}

DeduplicatedColumnStatistics::DeduplicatedColumnStatistics(
    std::unique_ptr<ColumnStatistics> baseStatistics,
    uint64_t dedupedCount,
    uint64_t dedupedLogicalSize)
    : baseStatistics_{std::move(baseStatistics)},
      dedupedCount_{dedupedCount},
      dedupedLogicalSize_{dedupedLogicalSize} {}

const ColumnStatistics& DeduplicatedColumnStatistics::getBaseStatistics()
    const {
  return *baseStatistics_;
}

uint64_t DeduplicatedColumnStatistics::getDedupedCount() const {
  return dedupedCount_;
}

uint64_t DeduplicatedColumnStatistics::getDedupedLogicalSize() const {
  return dedupedLogicalSize_;
}

StatType DeduplicatedColumnStatistics::getType() const {
  return StatType::DEDUPLICATED;
}

void StatisticsCollector::addValues(std::span<bool> values) {
  auto nullCount = 0;
  for (bool value : values) {
    nullCount += !value;
    addLogicalSize(!value);
  }
  addCounts(values.size(), nullCount);
}

void StatisticsCollector::addCounts(uint64_t valueCount, uint64_t nullCount) {
  valueCount_ += valueCount;
  nullCount_ += nullCount;
}

void StatisticsCollector::addLogicalSize(uint64_t logicalSize) {
  logicalSize_ += logicalSize;
}

void StatisticsCollector::addPhysicalSize(uint64_t physicalSize) {
  physicalSize_ += physicalSize;
}

void StatisticsCollector::merge(const StatisticsCollector& other) {
  valueCount_ += other.valueCount_;
  nullCount_ += other.nullCount_;
  logicalSize_ += other.logicalSize_;
  physicalSize_ += other.physicalSize_;
}

template <typename T>
void IntegralStatisticsCollector::addValues(std::span<T> values) {
  static_assert(std::is_integral_v<T>);
  addLogicalSize(values.size() * sizeof(T));

  if (UNLIKELY(StatisticsCollector::getValueCount() == 0 && !values.empty())) {
    min_ = static_cast<int64_t>(values.front());
    max_ = static_cast<int64_t>(values.front());
  }

  for (const auto& value : values) {
    auto v = static_cast<int64_t>(value);
    min_ = min_ > v ? std::make_optional(v) : min_;
    max_ = max_ < v ? std::make_optional(v) : max_;
  }
}

void IntegralStatisticsCollector::merge(const StatisticsCollector& other) {
  NIMBLE_CHECK(
      other.getType() == StatType::INTEGRAL ||
          other.getType() == StatType::DEFAULT,
      "Merging stats with mismatched types.");
  StatisticsCollector::merge(other);
  if (other.getType() == StatType::INTEGRAL) {
    const auto& otherIntegralStats =
        dynamic_cast<const IntegralStatisticsCollector&>(other);
    if (otherIntegralStats.min_.has_value()) {
      if (!min_.has_value() || *otherIntegralStats.min_ < *min_) {
        min_ = *otherIntegralStats.min_;
      }
    }
    if (otherIntegralStats.max_.has_value()) {
      if (!max_.has_value() || *otherIntegralStats.max_ > *max_) {
        max_ = *otherIntegralStats.max_;
      }
    }
  }
}

template <typename T>
void FloatingPointStatisticsCollector::addValues(std::span<T> values) {
  static_assert(std::is_floating_point_v<T>);
  addLogicalSize(values.size() * sizeof(T));

  if (UNLIKELY(StatisticsCollector::getValueCount() == 0 && !values.empty())) {
    min_ = static_cast<double>(values.front());
    max_ = static_cast<double>(values.front());
  }

  for (const auto& value : values) {
    auto v = static_cast<double>(value);
    min_ = min_ > v ? std::make_optional(v) : min_;
    max_ = max_ < v ? std::make_optional(v) : max_;
  }
}

void FloatingPointStatisticsCollector::merge(const StatisticsCollector& other) {
  NIMBLE_CHECK(
      other.getType() == StatType::FLOATING_POINT ||
          other.getType() == StatType::DEFAULT,
      "Merging stats with mismatched types.");
  StatisticsCollector::merge(other);
  if (other.getType() == StatType::FLOATING_POINT) {
    const auto& otherFloatStats =
        dynamic_cast<const FloatingPointStatisticsCollector&>(other);
    if (otherFloatStats.min_.has_value()) {
      if (!min_.has_value() || *otherFloatStats.min_ < *min_) {
        min_ = *otherFloatStats.min_;
      }
    }
    if (otherFloatStats.max_.has_value()) {
      if (!max_.has_value() || *otherFloatStats.max_ > *max_) {
        max_ = *otherFloatStats.max_;
      }
    }
  }
}

void StringStatisticsCollector::addValues(std::span<std::string_view> values) {
  if (UNLIKELY(StatisticsCollector::getValueCount() == 0 && !values.empty())) {
    min_ = values.front();
    max_ = values.front();
  }

  for (const auto& value : values) {
    addLogicalSize(value.size());
    min_ = min_ > value ? std::make_optional(value) : min_;
    max_ = max_ < value ? std::make_optional(value) : max_;
  }
}

void StringStatisticsCollector::merge(const StatisticsCollector& other) {
  NIMBLE_CHECK(
      other.getType() == StatType::STRING ||
          other.getType() == StatType::DEFAULT,
      "Merging stats with mismatched types.");
  StatisticsCollector::merge(other);
  if (other.getType() == StatType::STRING) {
    const auto& otherStringStats =
        dynamic_cast<const StringStatisticsCollector&>(other);
    if (otherStringStats.min_.has_value()) {
      if (!min_.has_value() || *otherStringStats.min_ < *min_) {
        min_ = *otherStringStats.min_;
      }
    }
    if (otherStringStats.max_.has_value()) {
      if (!max_.has_value() || *otherStringStats.max_ > *max_) {
        max_ = *otherStringStats.max_;
      }
    }
  }
}

DeduplicatedStatisticsCollector::DeduplicatedStatisticsCollector(
    std::unique_ptr<StatisticsCollector> baseCollector)
    : baseCollector_{std::move(baseCollector)} {}

StatisticsCollector*
DeduplicatedStatisticsCollector::getBaseStatisticsCollector() const {
  return baseCollector_.get();
}

void DeduplicatedStatisticsCollector::recordDeduplicatedStats(
    uint64_t count,
    uint64_t deduplicatedSize) {
  dedupedCount_ += count;
  dedupedLogicalSize_ += deduplicatedSize;
}

void DeduplicatedStatisticsCollector::addCounts(
    uint64_t valueCount,
    uint64_t nullCount) {
  baseCollector_->addCounts(valueCount, nullCount);
}

void DeduplicatedStatisticsCollector::addLogicalSize(uint64_t logicalSize) {
  baseCollector_->addLogicalSize(logicalSize);
}

void DeduplicatedStatisticsCollector::addPhysicalSize(uint64_t physicalSize) {
  baseCollector_->addPhysicalSize(physicalSize);
}

void DeduplicatedStatisticsCollector::merge(const StatisticsCollector& other) {
  NIMBLE_CHECK(
      other.getType() == StatType::DEDUPLICATED,
      "Merging stats with mismatched types.");
  const auto& otherDeduplicatedStats =
      dynamic_cast<const DeduplicatedStatisticsCollector&>(other);
  dedupedCount_ += otherDeduplicatedStats.dedupedCount_;
  dedupedLogicalSize_ += otherDeduplicatedStats.dedupedLogicalSize_;
  baseCollector_->merge(*otherDeduplicatedStats.baseCollector_);
}

/* static */ std::unique_ptr<DeduplicatedStatisticsCollector>
DeduplicatedStatisticsCollector::wrap(
    std::unique_ptr<StatisticsCollector> baseCollector) {
  return std::make_unique<DeduplicatedStatisticsCollector>(
      std::move(baseCollector));
}

// Creates the column statistics collector for the given type (non-recursive).
// Deduplicated statistics collectors are always wrapped retroactively.
/* static */ std::unique_ptr<StatisticsCollector> StatisticsCollector::create(
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& type) {
  switch (type->type()->kind()) {
    case velox::TypeKind::TINYINT:
    case velox::TypeKind::SMALLINT:
    case velox::TypeKind::INTEGER:
    case velox::TypeKind::BIGINT: {
      return std::make_unique<IntegralStatisticsCollector>();
    }
    case velox::TypeKind::REAL:
    case velox::TypeKind::DOUBLE: {
      return std::make_unique<FloatingPointStatisticsCollector>();
    }
    case velox::TypeKind::VARCHAR:
    case velox::TypeKind::VARBINARY: {
      return std::make_unique<StringStatisticsCollector>();
    }
    case velox::TypeKind::BOOLEAN:
    case velox::TypeKind::TIMESTAMP:
    case velox::TypeKind::ROW:
    case velox::TypeKind::ARRAY:
    case velox::TypeKind::MAP: {
      return std::make_unique<StatisticsCollector>();
    }
    default:
      NIMBLE_UNSUPPORTED(
          fmt::format("Unsupported schema kind: {}.", type->type()->kind()));
  }
}

// Explicit template instantiations for IntegralStatisticsCollector::addValues
template void IntegralStatisticsCollector::addValues<int8_t>(std::span<int8_t>);
template void IntegralStatisticsCollector::addValues<int16_t>(
    std::span<int16_t>);
template void IntegralStatisticsCollector::addValues<int32_t>(
    std::span<int32_t>);
template void IntegralStatisticsCollector::addValues<int64_t>(
    std::span<int64_t>);

// Explicit template instantiations for
// FloatingPointStatisticsCollector::addValues
template void FloatingPointStatisticsCollector::addValues<float>(
    std::span<float>);
template void FloatingPointStatisticsCollector::addValues<double>(
    std::span<double>);

} // namespace facebook::nimble
