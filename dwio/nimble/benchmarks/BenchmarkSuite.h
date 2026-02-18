/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

// BenchmarkSuite — runs each benchmark in its own BenchmarkingState so
// start/end hooks can fire per-benchmark.  Uses a standalone
// folly::detail::BenchmarkingState per benchmark, enabling std::function
// callbacks that fire exactly once per benchmark function (start and end).

#pragma once

#include <chrono>
#include <cmath>
#include <functional>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/FileUtil.h>
#include <folly/String.h>
#include <folly/dynamic.h>
#include <folly/json/json.h>
#include <glog/logging.h>

namespace facebook::nimble {

class BenchmarkSuite {
 public:
  using Clock = std::chrono::high_resolution_clock;
  using StartHook = std::function<void(const std::string& name)>;
  using EndHook = std::function<
      void(const std::string& name, const folly::detail::BenchmarkResult&)>;
  using CustomMetrics = std::unordered_map<std::string, double>;

  void setOnStart(StartHook hook) {
    onStart_ = std::move(hook);
  }
  void setOnEnd(EndHook hook) {
    onEnd_ = std::move(hook);
  }

  void setTitle(std::string title) {
    title_ = std::move(title);
  }

  void setJsonOutputPath(std::string path) {
    jsonOutputPath_ = std::move(path);
  }

  void setJsonBaselinePath(std::string path) {
    jsonBaselinePath_ = std::move(path);
  }

  void addBenchmark(std::string name, std::function<void(unsigned)> fn) {
    entries_.push_back(
        Entry{std::move(name), std::move(fn), nullptr, /*sep=*/false});
  }

  void addBenchmark(
      std::string name,
      std::function<CustomMetrics(unsigned)> fn) {
    auto metrics = std::make_shared<CustomMetrics>();
    auto metricsPtr = metrics;
    entries_.push_back(
        Entry{
            std::move(name),
            [fn = std::move(fn), metricsPtr](unsigned iters) {
              *metricsPtr = fn(iters);
            },
            std::move(metrics),
            /*sep=*/false});
  }

  void addSeparator() {
    entries_.push_back(Entry{"", nullptr, nullptr, /*sep=*/true});
  }

  void run() {
    // Load baseline results from a previous run, if provided.
    std::unordered_map<std::string, BaselineEntry> baseline;
    if (!jsonBaselinePath_.empty()) {
      baseline = loadBaselineResults(jsonBaselinePath_);
    }

    std::vector<folly::detail::BenchmarkResult> allResults;

    // Print table header.
    printHeader();

    bool pendingSeparator = false;

    for (size_t idx = 0; idx < entries_.size(); ++idx) {
      auto& entry = entries_[idx];

      if (entry.isSeparator) {
        pendingSeparator = true;
        continue;
      }

      // Create a fresh BenchmarkingState for this benchmark.
      folly::detail::BenchmarkingState<Clock> state;

      // Register baseline benchmarks (required by the measurement engine).
      // Names must match the global constants in folly/Benchmark.cpp.
      auto baselineName = std::string("fbFollyGlobalBenchmarkBaseline");
      auto suspenderBaselineName =
          std::string("fbFollyGlobalBenchmarkSuspenderBaseline");

      state.addBenchmark(
          "BenchmarkSuite", std::move(baselineName), [](unsigned) {
      // Minimal baseline — matches folly's global baseline.
#ifdef _MSC_VER
            _ReadWriteBarrier();
#else
            asm volatile("");
#endif
            return 1u;
          });
      state.addBenchmark(
          "BenchmarkSuite", std::move(suspenderBaselineName), [](unsigned) {
            folly::BenchmarkSuspender sus;
            return 1u;
          });

      // Register the actual benchmark.
      auto fn = entry.fn; // copy the function for the lambda capture
      state.addBenchmark(
          "BenchmarkSuite", entry.name, [fn](unsigned iters) -> unsigned {
            fn(iters);
            return iters;
          });

      // Fire start hook.
      if (onStart_) {
        onStart_(entry.name);
      }

      // Run measurement.
      auto results = state.runBenchmarksWithResults();

      // Find the result for our benchmark (skip baselines).
      folly::detail::BenchmarkResult bmResult;
      bmResult.name = entry.name;
      bmResult.file = "BenchmarkSuite";
      bmResult.timeInNs = 0;
      for (auto& r : results) {
        if (r.name == entry.name) {
          bmResult = r;
          break;
        }
      }
      // Populate custom metrics from the benchmark function's return value.
      // The metrics map is written by the last invocation of the benchmark
      // function (with the final calibrated iteration count).
      if (entry.metrics && !entry.metrics->empty()) {
        for (const auto& [key, value] : *entry.metrics) {
          bmResult.counters[key] =
              folly::UserMetric{value, folly::UserMetric::Type::CUSTOM};
        }
      }
      // Print separator if one was pending before this benchmark.
      if (pendingSeparator) {
        printSeparator('-');
        pendingSeparator = false;
      } else if (!allResults.empty()) {
        printSeparator('.');
      }

      // Print this result immediately.
      printResult(bmResult);

      // Fire end hook.
      if (onEnd_) {
        onEnd_(entry.name, bmResult);
      }

      allResults.push_back(bmResult);
    }

    // Print table footer.
    printSeparator('=');

    // Write JSON output if a path was provided.
    if (!jsonOutputPath_.empty()) {
      writeJsonResults(allResults, jsonOutputPath_);
    }

    // Print comparison table if baseline was loaded.
    if (!baseline.empty()) {
      printf("\n");
      printComparisonTable(allResults, baseline);
    }
  }

 private:
  struct Entry {
    std::string name;
    std::function<void(unsigned)> fn;
    std::shared_ptr<CustomMetrics> metrics;
    bool isSeparator;
  };

  // Human-readable formatting helpers matching folly's output.
  struct ScaleInfo {
    double boundary;
    const char* suffix;
  };

  static std::string
  humanReadable(double n, unsigned int decimals, const ScaleInfo* scales) {
    if (std::isinf(n) || std::isnan(n)) {
      return folly::to<std::string>(n);
    }
    const double absValue = fabs(n);
    if (absValue == 0) {
      return folly::stringPrintf("%.*f", decimals, 0.0);
    }
    const ScaleInfo* scale = scales;
    while (absValue < scale[0].boundary && scale[1].suffix != nullptr) {
      ++scale;
    }
    const double scaledValue = n / scale->boundary;
    return fmt::format("{:.{}f}{}", scaledValue, decimals, scale->suffix);
  }

  static std::string readableTime(double n, unsigned int decimals) {
    static const ScaleInfo kTimeSuffixes[] = {
        {365.25 * 24 * 3600, "years"},
        {24 * 3600, "days"},
        {3600, "hr"},
        {60, "min"},
        {1, "s"},
        {1E-3, "ms"},
        {1E-6, "us"},
        {1E-9, "ns"},
        {1E-12, "ps"},
        {1E-15, "fs"},
        {0, nullptr},
    };
    return humanReadable(n, decimals, kTimeSuffixes);
  }

  static std::string metricReadable(double n, unsigned int decimals) {
    static const ScaleInfo kMetricSuffixes[] = {
        {1E24, "Y"},
        {1E21, "Z"},
        {1E18, "X"},
        {1E15, "P"},
        {1E12, "T"},
        {1E9, "G"},
        {1E6, "M"},
        {1E3, "K"},
        {1, ""},
        {1E-3, "m"},
        {1E-6, "u"},
        {1E-9, "n"},
        {1E-12, "p"},
        {1E-15, "f"},
        {1E-18, "a"},
        {1E-21, "z"},
        {1E-24, "y"},
        {0, nullptr},
    };
    return humanReadable(n, decimals, kMetricSuffixes);
  }

  static constexpr unsigned int kColumns = 76;
  static constexpr std::string_view kHeader = "time/iter   iters/s";

  // ANSI escape helpers.
  static constexpr const char* kBold = "\033[1m";
  static constexpr const char* kBoldBlue = "\033[1;34m";
  static constexpr const char* kBoldCyan = "\033[1;36m";
  static constexpr const char* kBoldGreen = "\033[1;32m";
  static constexpr const char* kBoldRed = "\033[1;31m";
  static constexpr const char* kBoldYellow = "\033[1;33m";
  static constexpr const char* kReset = "\033[0m";

  static void printSeparator(char pad) {
    printf("%s%s%s\n", kBoldBlue, std::string(kColumns, pad).c_str(), kReset);
  }

  void printHeader() const {
    printSeparator('=');
    static const std::string kDefaultTitle = "BenchmarkSuite";
    const std::string& title = title_.empty() ? kDefaultTitle : title_;
    size_t padLen = (kColumns > title.size() + kHeader.size())
        ? (kColumns - title.size() - kHeader.size())
        : 1;
    printf(
        "%s%s%*s%s%.*s%s\n",
        kBold,
        title.c_str(),
        static_cast<int>(padLen),
        "",
        kBoldYellow,
        static_cast<int>(kHeader.size()),
        kHeader.data(),
        kReset);
    printSeparator('=');
  }

  static void printResult(const folly::detail::BenchmarkResult& r) {
    const double nsPerIter = r.timeInNs;
    const double secPerIter = nsPerIter / 1E9;
    const double itersPerSec = (secPerIter == 0)
        ? std::numeric_limits<double>::infinity()
        : (1.0 / secPerIter);

    std::string name = r.name;
    size_t nameWidth = kColumns - kHeader.size();
    name.resize(nameWidth, ' ');

    printf(
        "%s%*s%s%s%9.9s  %s%8.8s%s",
        kBoldGreen,
        static_cast<int>(name.size()),
        name.c_str(),
        kReset,
        kBoldCyan,
        readableTime(secPerIter, 2).c_str(),
        kBoldYellow,
        metricReadable(itersPerSec, 2).c_str(),
        kReset);

    for (const auto& [metricName, metric] : r.counters) {
      double value = std::visit(
          [](auto v) -> double { return static_cast<double>(v); },
          metric.value);
      printf(
          "  %s%s%s:%s%.4g%s",
          kBoldGreen,
          metricName.c_str(),
          kReset,
          kBoldCyan,
          value,
          kReset);
    }
    printf("\n");
  }

  static void writeJsonResults(
      const std::vector<folly::detail::BenchmarkResult>& results,
      const std::string& outputPath) {
    folly::dynamic d;
    folly::benchmarkResultsToDynamic(results, d);
    auto jsonStr = folly::toPrettyJson(d);
    if (folly::writeFile(jsonStr, outputPath.c_str())) {
      LOG(INFO) << "JSON results written to " << outputPath;
    } else {
      LOG(ERROR) << "Failed to write JSON results to " << outputPath;
    }
  }

  struct BaselineEntry {
    double timeInNs{};
    std::unordered_map<std::string, double> customMetrics;
  };

  static std::unordered_map<std::string, BaselineEntry> loadBaselineResults(
      const std::string& path) {
    std::unordered_map<std::string, BaselineEntry> baseline;
    std::string contents;
    if (!folly::readFile(path.c_str(), contents)) {
      LOG(WARNING) << "Could not read baseline file: " << path;
      return baseline;
    }
    try {
      auto d = folly::parseJson(contents);
      for (const auto& datum : d) {
        BaselineEntry entry;
        entry.timeInNs = datum[2].asDouble();
        // Parse custom metrics from the optional 4th element (UserCounters).
        if (datum.size() > 3 && datum[3].isObject()) {
          for (const auto& [key, val] : datum[3].items()) {
            if (val.isObject() && val.count("value")) {
              entry.customMetrics[key.asString()] = val["value"].asDouble();
            }
          }
        }
        baseline[datum[1].asString()] = std::move(entry);
      }
    } catch (const std::exception& e) {
      LOG(WARNING) << "Could not parse baseline file: " << e.what();
    }
    return baseline;
  }

  // Print a comparison table showing delta between current and baseline
  // results. Columns: benchmark name, baseline time/iter, current time/iter,
  //          absolute delta, relative percentage change.
  // Custom metrics (if any) are shown as indented sub-rows.
  // Color coding: green = improvement (faster), red = regression (slower),
  //               yellow = within 1% (noise).
  void printComparisonTable(
      const std::vector<folly::detail::BenchmarkResult>& results,
      const std::unordered_map<std::string, BaselineEntry>& baseline) const {
    // Layout: name(34) + sp(1) + baseline(9) + sp(2) + current(9) +
    //         sp(2) + delta(10) + sp(2) + pct(7) = 76
    static constexpr unsigned int kNameWidth = kColumns - 42;

    // Header.
    printSeparator('=');
    {
      std::string title = "Comparison vs baseline";
      title.resize(kNameWidth, ' ');
      printf(
          "%s%s%s %s%9s  %9s  %10s  %7s%s\n",
          kBold,
          title.c_str(),
          kReset,
          kBoldYellow,
          "baseline",
          "current",
          "delta",
          "pct",
          kReset);
    }
    printSeparator('=');

    bool first = true;
    for (const auto& r : results) {
      std::string name = r.name;
      name.resize(kNameWidth, ' ');

      double currNs = r.timeInNs;
      double currSec = currNs / 1E9;

      auto it = baseline.find(r.name);
      if (it == baseline.end()) {
        // New benchmark — no baseline to compare against.
        if (!first) {
          printSeparator('.');
        }
        printf(
            "%s%s%s %s%9s%s  %s%9s%s  %10s  %7s\n",
            kBoldGreen,
            name.c_str(),
            kReset,
            kBoldYellow,
            "--",
            kReset,
            kBoldCyan,
            readableTime(currSec, 2).c_str(),
            kReset,
            "--",
            "(new)");
        first = false;
        continue;
      }

      double baseNs = it->second.timeInNs;
      double baseSec = baseNs / 1E9;
      double deltaNs = currNs - baseNs;
      double pct = (baseNs > 0) ? (deltaNs / baseNs * 100.0) : 0.0;

      // Negative delta = faster = improvement (green).
      // Positive delta = slower = regression (red).
      const char* deltaColor = kBoldYellow;
      if (pct < -1.0) {
        deltaColor = kBoldGreen;
      } else if (pct > 1.0) {
        deltaColor = kBoldRed;
      }

      // Format delta with explicit sign.
      double absDeltaSec = std::fabs(deltaNs) / 1E9;
      std::string deltaStr;
      if (deltaNs < 0) {
        deltaStr = "-" + readableTime(absDeltaSec, 2);
      } else {
        deltaStr = "+" + readableTime(absDeltaSec, 2);
      }

      auto pctStr = folly::stringPrintf("%+.2f%%", pct);

      if (!first) {
        printSeparator('.');
      }
      printf(
          "%s%s%s %s%9s%s  %s%9s%s  %s%10s  %7s%s\n",
          kBoldGreen,
          name.c_str(),
          kReset,
          kBoldCyan,
          readableTime(baseSec, 2).c_str(),
          kReset,
          kBoldCyan,
          readableTime(currSec, 2).c_str(),
          kReset,
          deltaColor,
          deltaStr.c_str(),
          pctStr.c_str(),
          kReset);

      // Print custom metric comparison sub-rows (indented).
      const auto& baseMetrics = it->second.customMetrics;
      for (const auto& [metricName, metric] : r.counters) {
        double currVal = std::visit(
            [](auto v) -> double { return static_cast<double>(v); },
            metric.value);
        std::string mname = "  " + metricName;
        mname.resize(kNameWidth, ' ');

        auto baseIt = baseMetrics.find(metricName);
        if (baseIt == baseMetrics.end()) {
          printf(
              "%s%s%s %s%9s%s  %s%9.4g%s  %10s  %7s\n",
              kBoldGreen,
              mname.c_str(),
              kReset,
              kBoldYellow,
              "--",
              kReset,
              kBoldCyan,
              currVal,
              kReset,
              "--",
              "(new)");
          continue;
        }

        double baseVal = baseIt->second;
        double deltaVal = currVal - baseVal;
        double mPct = (baseVal != 0) ? (deltaVal / baseVal * 100.0) : 0.0;

        const char* mColor = kBoldYellow;
        if (mPct < -1.0) {
          mColor = kBoldGreen;
        } else if (mPct > 1.0) {
          mColor = kBoldRed;
        }

        double absDelta = std::fabs(deltaVal);
        auto mDeltaStr =
            folly::stringPrintf("%s%.4g", deltaVal < 0 ? "-" : "+", absDelta);
        auto mPctStr = folly::stringPrintf("%+.2f%%", mPct);

        printf(
            "%s%s%s %s%9.4g%s  %s%9.4g%s  %s%10s  %7s%s\n",
            kBoldGreen,
            mname.c_str(),
            kReset,
            kBoldCyan,
            baseVal,
            kReset,
            kBoldCyan,
            currVal,
            kReset,
            mColor,
            mDeltaStr.c_str(),
            mPctStr.c_str(),
            kReset);
      }

      first = false;
    }

    printSeparator('=');
  }

  std::vector<Entry> entries_;
  StartHook onStart_;
  EndHook onEnd_;
  std::string title_;
  std::string jsonOutputPath_;
  std::string jsonBaselinePath_;
};

} // namespace facebook::nimble
