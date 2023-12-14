// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/encodings/Statistics.h"

#include <limits>
#include <memory>
#include <type_traits>

#include "dwio/alpha/common/Types.h"

namespace facebook::alpha {

namespace {

template <typename T>
using MapType = typename UniqueValueCounts<T>::MapType;

template <typename T>
struct StatisticsInternal {
  uint64_t consecutiveRepeatCount = 0;
  uint64_t minRepeat = 0;
  uint64_t maxRepeat = 0;
  uint64_t totalStringsLength = 0;
  uint64_t totalStringsRepeatLength = 0;
  T min = T();
  T max = T();
  std::vector<uint64_t> bucketCounts;
  MapType<T> uniqueCounts;
};

template <typename T>
StatisticsInternal<T> createIntegral(std::span<const T> data) {
  // Integer types need the following statistics:
  // 1. uniqueCounts
  // 2. min/max
  // 3. consecutiveRepeatCount/minRepeat/maxRepeat
  // 4. bucketCounts

  using UnsignedT = typename std::make_unsigned<T>::type;

  T currentValue = data[0];
  uint64_t currentRepeat = 0;
  uint64_t consecutiveRepeatCount = 0;
  uint64_t minRepeat = std::numeric_limits<uint64_t>::max();
  uint64_t maxRepeat = 0;

  // Distinct values count is using a robin_map. See benchmarks in
  // dwio/alpha/encodings/tests:map_benchmark for why this map is used.
  // Note: There is no science behind the reservation size. Just rying to
  // minimize internal allocations...
  MapType<T> uniqueCounts;
  uniqueCounts.reserve(data.size() / 3);

  // Bucket counts are calculated in two phases. In phase one, we iterate on all
  // entries, and (efficiently) count the occurences based on the MSB (most
  // significant bit) of the entry. In phase two, we merge the results of phase
  // one, for each conscutive 7 bits.
  // See benchmarks in
  // dwio/alpha/encodings/tests:bucket_benchmark for why this method is used.
  std::array<uint64_t, std::numeric_limits<UnsignedT>::digits + 1> bitCounts{};

  const auto [min, max] = std::minmax_element(data.begin(), data.end());
  for (auto i = 0; i < data.size(); ++i) {
    auto value = data[i];
    ++uniqueCounts[value];
    ++(bitCounts
           [std::numeric_limits<UnsignedT>::digits -
            std::countl_zero(static_cast<UnsignedT>(
                static_cast<UnsignedT>(value) -
                static_cast<UnsignedT>(*min)))]);

    if (value == currentValue) {
      ++currentRepeat;
    } else {
      if (currentRepeat > maxRepeat) {
        maxRepeat = currentRepeat;
      }

      if (currentRepeat < minRepeat) {
        minRepeat = currentRepeat;
      }

      currentRepeat = 1;
      currentValue = value;
      ++consecutiveRepeatCount;
    }
  }

  if (currentRepeat > maxRepeat) {
    maxRepeat = currentRepeat;
  }

  if (currentRepeat < minRepeat) {
    minRepeat = currentRepeat;
  }

  ++consecutiveRepeatCount;

  // Bucket count aggregation phase - Aggregating individial MSB counts into
  // groups of 7 bits.
  std::vector<uint64_t> bucketCounts(sizeof(T) * 8 / 7 + 1, 0);
  uint8_t start = 0;
  uint8_t end = 8;
  uint8_t iteration = 0;
  while (start < bitCounts.size()) {
    for (auto i = start; i < end; ++i) {
      bucketCounts[iteration] += bitCounts[i];
    }
    ++iteration;
    start = end;
    end += 7;
    if (bitCounts.size() < end) {
      end = bitCounts.size();
    }
  }

  return {
      .consecutiveRepeatCount = consecutiveRepeatCount,
      .minRepeat = minRepeat,
      .maxRepeat = maxRepeat,
      .min = *min,
      .max = *max,
      .bucketCounts = std::move(bucketCounts),
      .uniqueCounts = std::move(uniqueCounts)};
}

template <typename T>
StatisticsInternal<T> createFloatingPoint(std::span<const T> data) {
  // Floating point types need the following statistics:
  // 1. uniqueCounts
  // 2. min/max
  // 3. consecutiveRepeatCount/minRepeat/maxRepeat
  // Note: This list is the same as integer types, but without bucket stats.

  T currentValue = data[0];
  uint64_t currentRepeat = 0;
  uint64_t consecutiveRepeatCount = 0;
  uint64_t minRepeat = std::numeric_limits<uint64_t>::max();
  uint64_t maxRepeat = 0;

  // Note: There is no science behind the reservation size. Just rying to
  // minimize internal allocations...
  MapType<T> uniqueCounts;
  uniqueCounts.reserve(data.size() / 3);

  const auto [min, max] = std::minmax_element(data.begin(), data.end());

  for (auto i = 0; i < data.size(); ++i) {
    auto value = data[i];
    ++uniqueCounts[value];

    if (value == currentValue) {
      ++currentRepeat;
    } else {
      if (currentRepeat > maxRepeat) {
        maxRepeat = currentRepeat;
      }

      if (currentRepeat < minRepeat) {
        minRepeat = currentRepeat;
      }

      currentRepeat = 1;
      currentValue = value;
      ++consecutiveRepeatCount;
    }
  }

  if (currentRepeat > maxRepeat) {
    maxRepeat = currentRepeat;
  }

  if (currentRepeat < minRepeat) {
    minRepeat = currentRepeat;
  }

  ++consecutiveRepeatCount;

  return {
      .consecutiveRepeatCount = consecutiveRepeatCount,
      .minRepeat = minRepeat,
      .maxRepeat = maxRepeat,
      .min = *min,
      .max = *max,
      .uniqueCounts = std::move(uniqueCounts)};
}

template <typename T>
StatisticsInternal<T> createBoolean(std::span<const T> data) {
  // Boolean types need the following statistics:
  // 1. unique (true/false) counts
  // 2. consecutiveRepeatCount/minRepeat/maxRepeat
  T currentValue = data[0];
  uint64_t currentRepeat = 0;
  uint64_t consecutiveRepeatCount = 0;
  uint64_t minRepeat = std::numeric_limits<uint64_t>::max();
  uint64_t maxRepeat = 0;
  std::array<uint64_t, 2> counts{};

  for (auto i = 0; i < data.size(); ++i) {
    auto value = data[i];
    ++counts[static_cast<uint8_t>(value)];

    if (value == currentValue) {
      ++currentRepeat;
    } else {
      if (currentRepeat > maxRepeat) {
        maxRepeat = currentRepeat;
      }

      if (currentRepeat < minRepeat) {
        minRepeat = currentRepeat;
      }

      currentRepeat = 1;
      currentValue = value;
      ++consecutiveRepeatCount;
    }
  }

  if (currentRepeat > maxRepeat) {
    maxRepeat = currentRepeat;
  }

  if (currentRepeat < minRepeat) {
    minRepeat = currentRepeat;
  }

  ++consecutiveRepeatCount;

  MapType<T> uniqueCounts;
  uniqueCounts.reserve(2);
  if (counts[0] > 0) {
    uniqueCounts[false] = counts[0];
  }
  if (counts[1] > 0) {
    uniqueCounts[true] = counts[1];
  }

  return {
      .consecutiveRepeatCount = consecutiveRepeatCount,
      .minRepeat = minRepeat,
      .maxRepeat = maxRepeat,
      .uniqueCounts = std::move(uniqueCounts)};
}

template <typename T>
StatisticsInternal<std::string_view> createString(std::span<const T> data) {
  // String types need the following statistics:
  // 1. uniqueCounts
  // 2. totalStringLength
  // 3. consecutiveRepeatCount/minRepeat/maxRepeat
  // 4. min/max (these will hold the shortest and logest strings)

  std::string_view currentValue = data[0];
  uint64_t currentRepeat = 0;
  uint64_t consecutiveRepeatCount = 0;
  uint64_t minRepeat = std::numeric_limits<uint64_t>::max();
  uint64_t maxRepeat = 0;
  uint64_t totalLength = 0;
  uint64_t totalRepeatLength = data[0].size();
  std::string_view minString = data[0];
  std::string_view maxString = data[0];

  // Note: There is no science behind the reservation size. Just rying to
  // minimize internal allocations...
  MapType<std::string_view> uniqueCounts;
  uniqueCounts.reserve(data.size() / 3);

  for (auto i = 0; i < data.size(); ++i) {
    const auto& value = data[i];
    ++uniqueCounts[value];
    totalLength += value.size();

    if (value.size() > maxString.size()) {
      maxString = value;
    }
    if (value.size() < minString.size()) {
      minString = value;
    }

    if (value == currentValue) {
      ++currentRepeat;
    } else {
      totalRepeatLength += value.size();
      if (currentRepeat > maxRepeat) {
        maxRepeat = currentRepeat;
      }

      if (currentRepeat < minRepeat) {
        minRepeat = currentRepeat;
      }

      currentRepeat = 1;
      currentValue = value;
      ++consecutiveRepeatCount;
    }
  }

  if (currentRepeat > maxRepeat) {
    maxRepeat = currentRepeat;
  }

  if (currentRepeat < minRepeat) {
    minRepeat = currentRepeat;
  }

  ++consecutiveRepeatCount;

  return {
      .totalStringsLength = totalLength,
      .totalStringsRepeatLength = totalRepeatLength,
      .min = minString,
      .max = maxString,
      .consecutiveRepeatCount = consecutiveRepeatCount,
      .minRepeat = minRepeat,
      .maxRepeat = maxRepeat,
      .uniqueCounts = std::move(uniqueCounts)};
}

} // namespace

template <typename T>
template <typename InputType>
Statistics<T> Statistics<T>::create(std::span<const InputType> data) {
  Statistics<T> statistics;
  if (data.size() == 0) {
    return statistics;
  }

  StatisticsInternal<T> result;
  if constexpr (alpha::isBoolType<T>()) {
    result = createBoolean<T>(data);
  } else if constexpr (alpha::isIntegralType<T>()) {
    result = createIntegral<T>(data);
  } else if constexpr (alpha::isFloatingPointType<T>()) {
    result = createFloatingPoint<T>(data);
  } else if constexpr (alpha::isStringType<T>()) {
    result = createString<InputType>(data);
  }

  statistics.consecutiveRepeatCount_ = result.consecutiveRepeatCount;
  statistics.minRepeat_ = result.minRepeat;
  statistics.maxRepeat_ = result.maxRepeat;
  statistics.totalStringsLength_ = result.totalStringsLength;
  statistics.totalStringsRepeatLength_ = result.totalStringsRepeatLength;
  statistics.min_ = result.min;
  statistics.max_ = result.max;
  statistics.bucketCounts_ = std::move(result.bucketCounts);
  statistics.uniqueCounts_ =
      UniqueValueCounts<T>{std::move(result.uniqueCounts)};

  return statistics;
}

template Statistics<int8_t> Statistics<int8_t>::create(
    std::span<const int8_t> data);
template Statistics<uint8_t> Statistics<uint8_t>::create(
    std::span<const uint8_t> data);
template Statistics<int16_t> Statistics<int16_t>::create(
    std::span<const int16_t> data);
template Statistics<uint16_t> Statistics<uint16_t>::create(
    std::span<const uint16_t> data);
template Statistics<int32_t> Statistics<int32_t>::create(
    std::span<const int32_t> data);
template Statistics<uint32_t> Statistics<uint32_t>::create(
    std::span<const uint32_t> data);
template Statistics<int64_t> Statistics<int64_t>::create(
    std::span<const int64_t> data);
template Statistics<uint64_t> Statistics<uint64_t>::create(
    std::span<const uint64_t> data);
template Statistics<float> Statistics<float>::create(
    std::span<const float> data);
template Statistics<double> Statistics<double>::create(
    std::span<const double> data);
template Statistics<bool> Statistics<bool>::create(std::span<const bool> data);
template Statistics<std::string_view> Statistics<std::string_view>::create(
    std::span<const std::string_view> data);
template Statistics<std::string_view> Statistics<std::string_view>::create<
    std::string>(std::span<const std::string> data);

} // namespace facebook::alpha
