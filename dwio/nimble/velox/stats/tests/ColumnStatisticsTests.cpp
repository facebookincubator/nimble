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
#include <gtest/gtest.h>

#include "dwio/nimble/velox/stats/ColumnStatistics.h"

using namespace facebook;
using namespace facebook::nimble;

class ColumnStatisticsTests : public ::testing::Test {};

// Base ColumnStatistics Tests

TEST_F(ColumnStatisticsTests, DefaultStatistics) {
  // Default constructor.
  {
    ColumnStatistics stat;
    EXPECT_EQ(stat.getValueCount(), 0);
    EXPECT_EQ(stat.getNullCount(), 0);
    EXPECT_EQ(stat.getLogicalSize(), 0);
    EXPECT_EQ(stat.getPhysicalSize(), 0);
    EXPECT_EQ(stat.getType(), StatType::DEFAULT);
  }

  // Some random typical values.
  {
    ColumnStatistics stat(100, 10, 1000, 500);
    EXPECT_EQ(stat.getValueCount(), 100);
    EXPECT_EQ(stat.getNullCount(), 10);
    EXPECT_EQ(stat.getLogicalSize(), 1000);
    EXPECT_EQ(stat.getPhysicalSize(), 500);
    EXPECT_EQ(stat.getType(), StatType::DEFAULT);
  }

  // Explicit zeroes.
  {
    ColumnStatistics stat(0, 0, 0, 0);
    EXPECT_EQ(stat.getValueCount(), 0);
    EXPECT_EQ(stat.getNullCount(), 0);
    EXPECT_EQ(stat.getLogicalSize(), 0);
    EXPECT_EQ(stat.getPhysicalSize(), 0);
  }
  // Numeric limits
  {
    uint64_t maxVal = std::numeric_limits<uint64_t>::max();
    ColumnStatistics stat(maxVal, maxVal, maxVal, maxVal);
    EXPECT_EQ(stat.getValueCount(), maxVal);
    EXPECT_EQ(stat.getNullCount(), maxVal);
    EXPECT_EQ(stat.getLogicalSize(), maxVal);
    EXPECT_EQ(stat.getPhysicalSize(), maxVal);
  }
}

TEST_F(ColumnStatisticsTests, IntegralStatistics) {
  {
    IntegralStatistics stat;
    EXPECT_EQ(stat.getValueCount(), 0);
    EXPECT_EQ(stat.getNullCount(), 0);
    EXPECT_EQ(stat.getLogicalSize(), 0);
    EXPECT_EQ(stat.getPhysicalSize(), 0);
    EXPECT_EQ(stat.getType(), StatType::INTEGRAL);
    EXPECT_EQ(stat.getMin(), std::nullopt);
    EXPECT_EQ(stat.getMax(), std::nullopt);
  }

  {
    IntegralStatistics stat(100, 10, 1000, 500, -50, 150);
    EXPECT_EQ(stat.getValueCount(), 100);
    EXPECT_EQ(stat.getNullCount(), 10);
    EXPECT_EQ(stat.getLogicalSize(), 1000);
    EXPECT_EQ(stat.getPhysicalSize(), 500);
    EXPECT_EQ(stat.getMin(), -50);
    EXPECT_EQ(stat.getMax(), 150);
    EXPECT_EQ(stat.getType(), StatType::INTEGRAL);
  }

  {
    IntegralStatistics stat(100, 10, 1000, 500, std::nullopt, 150);
    EXPECT_EQ(stat.getValueCount(), 100);
    EXPECT_EQ(stat.getNullCount(), 10);
    EXPECT_EQ(stat.getLogicalSize(), 1000);
    EXPECT_EQ(stat.getPhysicalSize(), 500);
    EXPECT_EQ(stat.getMin(), std::nullopt);
    EXPECT_EQ(stat.getMax(), 150);
  }

  {
    int64_t minVal = std::numeric_limits<int64_t>::min();
    int64_t maxVal = std::numeric_limits<int64_t>::max();
    IntegralStatistics stat(100, 10, 1000, 500, minVal, maxVal);
    EXPECT_EQ(stat.getMin(), minVal);
    EXPECT_EQ(stat.getMax(), maxVal);
  }
}

TEST_F(ColumnStatisticsTests, FloatingPointStatistics) {
  {
    FloatingPointStatistics stat;
    EXPECT_EQ(stat.getValueCount(), 0);
    EXPECT_EQ(stat.getNullCount(), 0);
    EXPECT_EQ(stat.getLogicalSize(), 0);
    EXPECT_EQ(stat.getPhysicalSize(), 0);
    EXPECT_EQ(stat.getType(), StatType::FLOATING_POINT);
    EXPECT_EQ(stat.getMin(), std::nullopt);
    EXPECT_EQ(stat.getMax(), std::nullopt);
  }

  {
    FloatingPointStatistics stat(100, 10, 1000, 500, -1.5, 2.5);
    EXPECT_EQ(stat.getValueCount(), 100);
    EXPECT_EQ(stat.getNullCount(), 10);
    EXPECT_EQ(stat.getLogicalSize(), 1000);
    EXPECT_EQ(stat.getPhysicalSize(), 500);
    EXPECT_EQ(stat.getMin(), -1.5);
    EXPECT_EQ(stat.getMax(), 2.5);
    EXPECT_EQ(stat.getType(), StatType::FLOATING_POINT);
  }

  {
    FloatingPointStatistics stat(100, 10, 1000, 500, std::nullopt, 2.5);
    EXPECT_EQ(stat.getValueCount(), 100);
    EXPECT_EQ(stat.getNullCount(), 10);
    EXPECT_EQ(stat.getLogicalSize(), 1000);
    EXPECT_EQ(stat.getPhysicalSize(), 500);
    EXPECT_EQ(stat.getMin(), std::nullopt);
    EXPECT_EQ(stat.getMax(), 2.5);
  }

  {
    double minVal = std::numeric_limits<double>::lowest();
    double maxVal = std::numeric_limits<double>::max();
    FloatingPointStatistics stat(100, 10, 1000, 500, minVal, maxVal);
    EXPECT_EQ(stat.getMin(), minVal);
    EXPECT_EQ(stat.getMax(), maxVal);
  }
}

// StringStatistics Tests

TEST_F(ColumnStatisticsTests, StringStatisticsDefaultConstructor) {
  {
    StringStatistics stat;
    EXPECT_EQ(stat.getValueCount(), 0);
    EXPECT_EQ(stat.getNullCount(), 0);
    EXPECT_EQ(stat.getLogicalSize(), 0);
    EXPECT_EQ(stat.getPhysicalSize(), 0);
    EXPECT_EQ(stat.getType(), StatType::STRING);
    EXPECT_EQ(stat.getMin(), std::nullopt);
    EXPECT_EQ(stat.getMax(), std::nullopt);
  }

  {
    StringStatistics stat(100, 10, 1000, 500, "aaa", "zzz");
    EXPECT_EQ(stat.getValueCount(), 100);
    EXPECT_EQ(stat.getNullCount(), 10);
    EXPECT_EQ(stat.getLogicalSize(), 1000);
    EXPECT_EQ(stat.getPhysicalSize(), 500);
    EXPECT_EQ(stat.getMin(), "aaa");
    EXPECT_EQ(stat.getMax(), "zzz");
    EXPECT_EQ(stat.getType(), StatType::STRING);
  }

  {
    StringStatistics stat(100, 10, 1000, 500, std::nullopt, std::nullopt);
    EXPECT_EQ(stat.getMin(), std::nullopt);
    EXPECT_EQ(stat.getMax(), std::nullopt);
  }

  {
    StringStatistics stat(100, 10, 1000, 500, "", "");
    EXPECT_EQ(stat.getMin(), "");
    EXPECT_EQ(stat.getMax(), "");
  }

  // Long strings
  {
    std::string longStr(1000, 'x');
    StringStatistics stat(100, 10, 1000, 500, longStr, longStr);
    EXPECT_EQ(stat.getMin(), longStr);
    EXPECT_EQ(stat.getMax(), longStr);
  }

  // Unicode strings
  {
    StringStatistics stat(100, 10, 1000, 500, "αβγ", "日本語");
    EXPECT_EQ(stat.getMin(), "αβγ");
    EXPECT_EQ(stat.getMax(), "日本語");
  }
}

TEST_F(ColumnStatisticsTests, DeduplicatedColumnStatistics) {
  {
    DeduplicatedColumnStatistics stat;
    EXPECT_EQ(stat.getType(), StatType::DEDUPLICATED);
  }

  {
    IntegralStatistics baseStat(100, 10, 1000, 500, -50, 150);
    DeduplicatedColumnStatistics stat(&baseStat, 80, 800);

    EXPECT_EQ(stat.getDedupedCount(), 80);
    EXPECT_EQ(stat.getDedupedLogicalSize(), 800);
    EXPECT_EQ(stat.getType(), StatType::DEDUPLICATED);

    const auto* base = stat.getBaseStatistics();
    EXPECT_EQ(base->getValueCount(), 100);
    EXPECT_EQ(base->getNullCount(), 10);
  }
}

class StatisticsCollectorTests : public ::testing::Test {};

// Base StatisticsCollector Tests

TEST_F(StatisticsCollectorTests, Ctor) {
  StatisticsCollector collector;
  EXPECT_EQ(collector.getValueCount(), 0);
  EXPECT_EQ(collector.getNullCount(), 0);
  EXPECT_EQ(collector.getLogicalSize(), 0);
  EXPECT_EQ(collector.getPhysicalSize(), 0);
  EXPECT_EQ(collector.getType(), StatType::DEFAULT);
}

TEST_F(StatisticsCollectorTests, AddCounts) {
  StatisticsCollector collector;
  collector.addCounts(100, 10);
  EXPECT_EQ(collector.getValueCount(), 100);
  EXPECT_EQ(collector.getNullCount(), 10);

  collector.addCounts(50, 5);
  EXPECT_EQ(collector.getValueCount(), 150);
  EXPECT_EQ(collector.getNullCount(), 15);
}

TEST_F(StatisticsCollectorTests, AddLogicalSize) {
  StatisticsCollector collector;
  collector.addLogicalSize(1000);
  EXPECT_EQ(collector.getLogicalSize(), 1000);

  collector.addLogicalSize(500);
  EXPECT_EQ(collector.getLogicalSize(), 1500);
}

TEST_F(StatisticsCollectorTests, AddPhysicalSize) {
  StatisticsCollector collector;
  collector.addPhysicalSize(500);
  EXPECT_EQ(collector.getPhysicalSize(), 500);

  collector.addPhysicalSize(250);
  EXPECT_EQ(collector.getPhysicalSize(), 750);
}

TEST_F(StatisticsCollectorTests, AddBoolValues) {
  {
    StatisticsCollector collector;
    bool values[] = {true, true, true, true};
    collector.addValues(std::span<bool>(values, 4));

    EXPECT_EQ(collector.getLogicalSize(), 4);
  }

  {
    StatisticsCollector collector;
    bool values[] = {false, false, false};
    collector.addValues(std::span<bool>(values, 3));

    EXPECT_EQ(collector.getLogicalSize(), 3);
  }

  {
    StatisticsCollector collector;
    bool values[] = {false, true, false};
    collector.addValues(std::span<bool>(values, 3));

    EXPECT_EQ(collector.getLogicalSize(), 3);
  }
}

TEST_F(StatisticsCollectorTests, MergeDefaultStatisticsCollectors) {
  StatisticsCollector collector1;
  collector1.addCounts(100, 10);
  collector1.addLogicalSize(1000);
  collector1.addPhysicalSize(500);

  StatisticsCollector collector2;
  collector2.addCounts(200, 20);
  collector2.addLogicalSize(2000);
  collector2.addPhysicalSize(1000);

  collector1.merge(collector2);

  EXPECT_EQ(collector1.getValueCount(), 300);
  EXPECT_EQ(collector1.getNullCount(), 30);
  EXPECT_EQ(collector1.getLogicalSize(), 3000);
  EXPECT_EQ(collector1.getPhysicalSize(), 1500);
}

TEST_F(StatisticsCollectorTests, IntegralStatisticsCollectorCtor) {
  IntegralStatisticsCollector collector;
  auto* stats = collector.getStatsView()->as<IntegralStatistics>();
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->getMin(), std::nullopt);
  EXPECT_EQ(stats->getMax(), std::nullopt);
  // Access getType() via dynamic_cast
  auto* sbPtr = dynamic_cast<StatisticsCollector*>(&collector);
  ASSERT_NE(sbPtr, nullptr);
  EXPECT_EQ(sbPtr->getType(), StatType::INTEGRAL);
}

TEST_F(StatisticsCollectorTests, IntegralStatisticsCollectorMerge) {
  IntegralStatisticsCollector collector1;
  dynamic_cast<StatisticsCollector&>(collector1).addCounts(100, 10);
  dynamic_cast<StatisticsCollector&>(collector1).addLogicalSize(1000);

  IntegralStatisticsCollector collector2;
  dynamic_cast<StatisticsCollector&>(collector2).addCounts(200, 20);
  dynamic_cast<StatisticsCollector&>(collector2).addLogicalSize(2000);

  collector1.merge(collector2);

  auto* sbPtr = dynamic_cast<StatisticsCollector*>(&collector1);
  ASSERT_NE(sbPtr, nullptr);
  EXPECT_EQ(sbPtr->getValueCount(), 300);
  EXPECT_EQ(sbPtr->getNullCount(), 30);
  EXPECT_EQ(sbPtr->getLogicalSize(), 3000);
}

TEST_F(StatisticsCollectorTests, IntegralStatisticsCollectorMergeWithDefault) {
  IntegralStatisticsCollector collector1;
  dynamic_cast<StatisticsCollector&>(collector1).addCounts(100, 10);

  StatisticsCollector collector2;
  collector2.addCounts(50, 5);

  collector1.merge(collector2);

  auto* sbPtr = dynamic_cast<StatisticsCollector*>(&collector1);
  ASSERT_NE(sbPtr, nullptr);
  EXPECT_EQ(sbPtr->getValueCount(), 150);
  EXPECT_EQ(sbPtr->getNullCount(), 15);
}

TEST_F(StatisticsCollectorTests, FloatingPointStatisticsCollectorCtor) {
  FloatingPointStatisticsCollector collector;
  auto* stats = collector.getStatsView()->as<FloatingPointStatistics>();
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->getMin(), std::nullopt);
  EXPECT_EQ(stats->getMax(), std::nullopt);
  // Access getType() via dynamic_cast
  auto* sbPtr = dynamic_cast<StatisticsCollector*>(&collector);
  ASSERT_NE(sbPtr, nullptr);
  EXPECT_EQ(sbPtr->getType(), StatType::FLOATING_POINT);
}

TEST_F(StatisticsCollectorTests, FloatingPointStatisticsCollectorMerge) {
  FloatingPointStatisticsCollector collector1;
  dynamic_cast<StatisticsCollector&>(collector1).addCounts(100, 10);
  dynamic_cast<StatisticsCollector&>(collector1).addLogicalSize(1000);

  FloatingPointStatisticsCollector collector2;
  dynamic_cast<StatisticsCollector&>(collector2).addCounts(200, 20);
  dynamic_cast<StatisticsCollector&>(collector2).addLogicalSize(2000);

  collector1.merge(collector2);

  auto* sbPtr = dynamic_cast<StatisticsCollector*>(&collector1);
  ASSERT_NE(sbPtr, nullptr);
  EXPECT_EQ(sbPtr->getValueCount(), 300);
  EXPECT_EQ(sbPtr->getNullCount(), 30);
  EXPECT_EQ(sbPtr->getLogicalSize(), 3000);
}

TEST_F(
    StatisticsCollectorTests,
    FloatingPointStatisticsCollectorMergeWithDefault) {
  FloatingPointStatisticsCollector collector1;
  dynamic_cast<StatisticsCollector&>(collector1).addCounts(100, 10);

  StatisticsCollector collector2;
  collector2.addCounts(50, 5);

  collector1.merge(collector2);

  auto* sbPtr = dynamic_cast<StatisticsCollector*>(&collector1);
  ASSERT_NE(sbPtr, nullptr);
  EXPECT_EQ(sbPtr->getValueCount(), 150);
  EXPECT_EQ(sbPtr->getNullCount(), 15);
}

// StringStatisticsCollector Tests

TEST_F(StatisticsCollectorTests, StringStatisticsCollectorCtor) {
  StringStatisticsCollector collector;
  auto* stats = collector.getStatsView()->as<StringStatistics>();
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->getMin(), std::nullopt);
  EXPECT_EQ(stats->getMax(), std::nullopt);
  // Access getType() via dynamic_cast
  auto* sbPtr = dynamic_cast<StatisticsCollector*>(&collector);
  ASSERT_NE(sbPtr, nullptr);
  EXPECT_EQ(sbPtr->getType(), StatType::STRING);
}

TEST_F(StatisticsCollectorTests, AddStringValues) {
  {
    StringStatisticsCollector collector;
    std::vector<std::string_view> values = {"hello", "world", "test"};
    collector.addValues(std::span<std::string_view>(values));

    auto* sbPtr = dynamic_cast<StatisticsCollector*>(&collector);
    ASSERT_NE(sbPtr, nullptr);
    EXPECT_EQ(sbPtr->getLogicalSize(), 5 + 5 + 4);
  }

  {
    StringStatisticsCollector collector;
    std::vector<std::string_view> values = {"", "", ""};
    collector.addValues(std::span<std::string_view>(values));

    auto* sbPtr = dynamic_cast<StatisticsCollector*>(&collector);
    ASSERT_NE(sbPtr, nullptr);
    EXPECT_EQ(sbPtr->getLogicalSize(), 0);
  }
}

TEST_F(StatisticsCollectorTests, StringStatisticsCollectorMerge) {
  StringStatisticsCollector collector1;
  dynamic_cast<StatisticsCollector&>(collector1).addCounts(100, 10);
  dynamic_cast<StatisticsCollector&>(collector1).addLogicalSize(1000);

  StringStatisticsCollector collector2;
  dynamic_cast<StatisticsCollector&>(collector2).addCounts(200, 20);
  dynamic_cast<StatisticsCollector&>(collector2).addLogicalSize(2000);

  collector1.merge(collector2);

  auto* sbPtr = dynamic_cast<StatisticsCollector*>(&collector1);
  ASSERT_NE(sbPtr, nullptr);
  EXPECT_EQ(sbPtr->getValueCount(), 300);
  EXPECT_EQ(sbPtr->getNullCount(), 30);
  EXPECT_EQ(sbPtr->getLogicalSize(), 3000);
}

TEST_F(StatisticsCollectorTests, StringStatisticsCollectorMergeWithDefault) {
  StringStatisticsCollector collector1;
  dynamic_cast<StatisticsCollector&>(collector1).addCounts(100, 10);

  StatisticsCollector collector2;
  collector2.addCounts(50, 5);

  collector1.merge(collector2);

  auto* sbPtr = dynamic_cast<StatisticsCollector*>(&collector1);
  ASSERT_NE(sbPtr, nullptr);
  EXPECT_EQ(sbPtr->getValueCount(), 150);
  EXPECT_EQ(sbPtr->getNullCount(), 15);
}

TEST_F(StatisticsCollectorTests, WrapWithDeduplicatedStatisticsCollector) {
  auto baseCollector = std::make_unique<IntegralStatisticsCollector>();
  auto dedupCollector =
      DeduplicatedStatisticsCollector::wrap(std::move(baseCollector));

  EXPECT_NE(dedupCollector, nullptr);
  EXPECT_EQ(dedupCollector->getType(), StatType::DEDUPLICATED);
}

TEST_F(StatisticsCollectorTests, DeduplicatedStatisticsCollectorBaseCollector) {
  auto baseCollector = std::make_unique<IntegralStatisticsCollector>();
  auto* basePtr = baseCollector.get();
  DeduplicatedStatisticsCollector dedupCollector(std::move(baseCollector));

  EXPECT_EQ(dedupCollector.getBaseCollector(), basePtr);
  dedupCollector.addCounts(100, 10);
  dedupCollector.addLogicalSize(1000);
  dedupCollector.addPhysicalSize(500);

  auto* sbPtr = dynamic_cast<StatisticsCollector*>(basePtr);
  ASSERT_NE(sbPtr, nullptr);
  EXPECT_EQ(sbPtr->getValueCount(), 100);
  EXPECT_EQ(sbPtr->getNullCount(), 10);
  EXPECT_EQ(sbPtr->getLogicalSize(), 1000);
  EXPECT_EQ(sbPtr->getPhysicalSize(), 500);
}

TEST_F(StatisticsCollectorTests, RecordDedupedStats) {
  auto baseCollector = std::make_unique<IntegralStatisticsCollector>();
  DeduplicatedStatisticsCollector dedupCollector(std::move(baseCollector));

  dedupCollector.recordDeduplicatedStats(80, 800);
  auto* dedupStats =
      dedupCollector.getStatsView()->as<DeduplicatedColumnStatistics>();
  ASSERT_NE(dedupStats, nullptr);
  EXPECT_EQ(dedupStats->getDedupedCount(), 80);
  EXPECT_EQ(dedupStats->getDedupedLogicalSize(), 800);

  dedupCollector.recordDeduplicatedStats(20, 200);
  EXPECT_EQ(dedupStats->getDedupedCount(), 100);
  EXPECT_EQ(dedupStats->getDedupedLogicalSize(), 1000);
}

TEST_F(StatisticsCollectorTests, DeduplicatedStatisticsCollectorMerge) {
  auto baseCollector1 = std::make_unique<IntegralStatisticsCollector>();
  DeduplicatedStatisticsCollector dedupCollector1(std::move(baseCollector1));
  dedupCollector1.addCounts(100, 10);
  dedupCollector1.recordDeduplicatedStats(80, 800);

  auto baseCollector2 = std::make_unique<IntegralStatisticsCollector>();
  DeduplicatedStatisticsCollector dedupCollector2(std::move(baseCollector2));
  dedupCollector2.addCounts(200, 20);
  dedupCollector2.recordDeduplicatedStats(160, 1600);

  dedupCollector1.merge(dedupCollector2);

  auto* dedupStats =
      dedupCollector1.getStatsView()->as<DeduplicatedColumnStatistics>();
  ASSERT_NE(dedupStats, nullptr);
  EXPECT_EQ(dedupStats->getDedupedCount(), 240);
  EXPECT_EQ(dedupStats->getDedupedLogicalSize(), 2400);
  EXPECT_EQ(dedupCollector1.getBaseCollector()->getValueCount(), 300);
  EXPECT_EQ(dedupCollector1.getBaseCollector()->getNullCount(), 30);
}
