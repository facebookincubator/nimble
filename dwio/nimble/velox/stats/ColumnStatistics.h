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

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>

#include "velox/dwio/common/TypeWithId.h"

namespace facebook::nimble {

enum class StatType {
  DEFAULT,
  INTEGRAL,
  FLOATING_POINT,
  STRING,
  DEDUPLICATED,
};

// In memory column statistics representation. The typical
// usage pattern is for each field writer to hold their own
// pointer to the specific type of column statistics collector,
// and build the statistics during flush time, based on subrange
// of rows.
// The statistics objects can then either be serialized to an
// optional metadata section, or converted to other in memory
// representations for the ingestion engine. The conversion call
// site requires access to the file schema.
class ColumnStatistics {
 public:
  ColumnStatistics() = default;
  virtual ~ColumnStatistics() = default;
  ColumnStatistics(
      uint64_t valueCount,
      uint64_t nullCount,
      uint64_t logicalSize,
      uint64_t physicalSize);

  uint64_t getValueCount() const;
  uint64_t getNullCount() const;
  uint64_t getLogicalSize() const;
  uint64_t getPhysicalSize() const;

  virtual StatType getType() const;
  template <typename T>
  T* as() {
    static_assert(std::is_base_of_v<ColumnStatistics, T>);
    return dynamic_cast<T*>(this);
  }

 protected:
  uint64_t valueCount_{0};
  uint64_t nullCount_{0};
  uint64_t logicalSize_{0};
  uint64_t physicalSize_{0};
};

class StringStatistics : public ColumnStatistics {
 public:
  StringStatistics() = default;
  StringStatistics(
      uint64_t valueCount,
      uint64_t nullCount,
      uint64_t logicalSize,
      uint64_t physicalSize,
      std::optional<std::string> min,
      std::optional<std::string> max);

  std::optional<std::string> getMin() const;
  std::optional<std::string> getMax() const;

  StatType getType() const override;

 protected:
  std::optional<std::string> min_;
  std::optional<std::string> max_;
};

// TODO: Add a flag to support uint64_t range.
class IntegralStatistics : public virtual ColumnStatistics {
 public:
  IntegralStatistics() = default;
  IntegralStatistics(
      uint64_t valueCount,
      uint64_t nullCount,
      uint64_t logicalSize,
      uint64_t physicalSize,
      std::optional<int64_t> min,
      std::optional<int64_t> max);

  std::optional<int64_t> getMin() const;
  std::optional<int64_t> getMax() const;

  StatType getType() const override;

 protected:
  std::optional<int64_t> min_;
  std::optional<int64_t> max_;
};

class FloatingPointStatistics : public virtual ColumnStatistics {
 public:
  FloatingPointStatistics() = default;
  FloatingPointStatistics(
      uint64_t valueCount,
      uint64_t nullCount,
      uint64_t logicalSize,
      uint64_t physicalSize,
      std::optional<double> min,
      std::optional<double> max);

  std::optional<double> getMin() const;
  std::optional<double> getMax() const;

  StatType getType() const override;

 protected:
  std::optional<double> min_;
  std::optional<double> max_;
};

// TODO: A prettier alternative would be to implement it with template, column
// statistics as the policy or base class.
// Alternatively, make deduplicated stats an optional field in column stats.
class DeduplicatedColumnStatistics : public virtual ColumnStatistics {
 public:
  DeduplicatedColumnStatistics() = default;
  DeduplicatedColumnStatistics(
      std::unique_ptr<ColumnStatistics> baseStatistics,
      uint64_t dedupedCount,
      uint64_t dedupedLogicalSize);

  const ColumnStatistics& getBaseStatistics() const;
  uint64_t getDedupedCount() const;
  uint64_t getDedupedLogicalSize() const;

  StatType getType() const override;

 protected:
  std::unique_ptr<ColumnStatistics> baseStatistics_;
  uint64_t dedupedCount_;
  uint64_t dedupedLogicalSize_;
};

// The statistics collectors are ways to abstract away different
// patterns to modify/update the stats. The current stats collector
// design is around each field writer making bulk updates at flush
// time (stripe or chunk). This design avoids dependencies of writes
// for nested columns, and avoids disrupting ingestion parallelism.
// When we implement more granular stats or index, we can then serialize
// or fork the current stats snap shot.
// We populate local stats and rely on a separate util to roll up
// when all local stats are complete.
class StatisticsCollector : public ColumnStatistics {
 public:
  // We do not create stats collectors recursively. Instead, we traverse
  // the schema tree and build the stats collector for each node. We can
  // then lay out the stats collectors in a vector in schema traversal order.
  static std::unique_ptr<StatisticsCollector> create(
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& type);

  StatisticsCollector() = default;
  virtual ~StatisticsCollector() = default;

  template <typename T>
  T* as() {
    static_assert(std::is_base_of_v<StatisticsCollector, T>);
    return dynamic_cast<T*>(this);
  }

  void addValues(std::span<bool> values);
  virtual void addCounts(uint64_t valueCount, uint64_t nullCount);
  // TODO: if we want to go extra, we can have a complex/aggregate type
  // and limit the availability of this method.
  virtual void addLogicalSize(uint64_t logicalSize);
  virtual void addPhysicalSize(uint64_t physicalSize);

  virtual void merge(const StatisticsCollector& other);
};

class IntegralStatisticsCollector : public StatisticsCollector,
                                    public IntegralStatistics {
 public:
  IntegralStatisticsCollector() = default;

  template <typename T>
  void addValues(std::span<T> values);

  virtual void merge(const StatisticsCollector& other) override;

  // Override to resolve diamond inheritance ambiguity - use the INTEGRAL
  // type from IntegralStatistics
  StatType getType() const override {
    return IntegralStatistics::getType();
  }
};

class FloatingPointStatisticsCollector : public StatisticsCollector,
                                         public FloatingPointStatistics {
 public:
  FloatingPointStatisticsCollector() = default;

  template <typename T>
  void addValues(std::span<T> values);

  virtual void merge(const StatisticsCollector& other) override;

  // Override to resolve diamond inheritance ambiguity - use the FLOATING_POINT
  // type from FloatingPointStatistics
  StatType getType() const override {
    return FloatingPointStatistics::getType();
  }
};

class StringStatisticsCollector : public virtual StatisticsCollector,
                                  public StringStatistics {
 public:
  StringStatisticsCollector() = default;

  void addValues(std::span<std::string_view> values);
  virtual void merge(const StatisticsCollector& other) override;

  // Override to resolve diamond inheritance ambiguity - use the STRING
  // type from StringStatistics
  StatType getType() const override {
    return StringStatistics::getType();
  }
};

// TODO: All ancestors of the deduplicated column will need the deduplicated
// stats. Cleanest is adding another finalization step to
// retroactively wrap.
class DeduplicatedStatisticsCollector : public DeduplicatedColumnStatistics,
                                        public virtual StatisticsCollector {
 public:
  static std::unique_ptr<DeduplicatedStatisticsCollector> wrap(
      std::unique_ptr<StatisticsCollector> baseCollector);
  explicit DeduplicatedStatisticsCollector(
      std::unique_ptr<StatisticsCollector> baseCollector);

  void addCounts(uint64_t valueCount, uint64_t nullCount) override;
  void addLogicalSize(uint64_t logicalSize) override;
  void addPhysicalSize(uint64_t physicalSize) override;

  StatisticsCollector* getBaseStatisticsCollector() const;
  void recordDeduplicatedStats(uint64_t count, uint64_t deduplicatedSize);

  virtual void merge(const StatisticsCollector& other) override;

  // Override to resolve diamond inheritance ambiguity - use the DEDUPLICATED
  // type from DeduplicatedColumnStatistics
  StatType getType() const override {
    return DeduplicatedColumnStatistics::getType();
  }

 private:
  std::unique_ptr<StatisticsCollector> baseCollector_;
};
} // namespace facebook::nimble
