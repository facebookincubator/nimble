// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/encodings/LazyStatistics.h"
#include "dwio/alpha/common/Types.h"

#include <complex>
#include <limits>
#include <memory>
#include <type_traits>

namespace facebook::alpha {

namespace {

template <typename T, typename InputType>
using MapType = typename UniqueValueCounts<T, InputType>::MapType;

} // namespace

template <typename T, typename InputType>
void LazyStatistics<T, InputType>::populateRepeats() {
  uint64_t consecutiveRepeatCount = 0;
  uint64_t minRepeat = std::numeric_limits<uint64_t>::max();
  uint64_t maxRepeat = 0;

  uint64_t totalRepeatLength = 0; // only needed for strings
  if constexpr (alpha::isStringType<T>()) {
    totalRepeatLength = data_[0].size();
  }

  T currentValue = data_[0];
  uint64_t currentRepeat = 0;

  for (auto i = 0; i < data_.size(); ++i) {
    auto value = data_[i];

    if (value == currentValue) {
      ++currentRepeat;
    } else {
      if constexpr (alpha::isStringType<T>()) {
        totalRepeatLength += value.size();
      }
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
  minRepeat_ = minRepeat;
  maxRepeat_ = maxRepeat;
  consecutiveRepeatCount_ = consecutiveRepeatCount;
  totalStringsRepeatLength_ = totalRepeatLength;
}

template <typename T, typename InputType>
void LazyStatistics<T, InputType>::populateMinMax() {
  if constexpr (alpha::isNumericType<InputType>()) {
    const auto [min, max] = std::minmax_element(data_.begin(), data_.end());
    min_ = *min;
    max_ = *max;
  } else if constexpr (alpha::isStringType<InputType>()) {
    std::string_view minString = data_[0];
    std::string_view maxString = data_[0];
    for (int i = 0; i < data_.size(); ++i) {
      const auto& value = data_[i];
      if (value.size() > maxString.size()) {
        maxString = value;
      }
      if (value.size() < minString.size()) {
        minString = value;
      }
    }
    min_ = minString;
    max_ = maxString;
  }
}

template <typename T, typename InputType>
void LazyStatistics<T, InputType>::populateUniques() {
  MapType<T, InputType> uniqueCounts;
  if constexpr (alpha::isBoolType<T>()) {
    std::array<uint64_t, 2> counts{};
    for (int i = 0; i < data_.size(); ++i) {
      ++counts[static_cast<uint8_t>(data_[i])];
    }
    uniqueCounts.reserve(2);
    if (counts[0] > 0) {
      uniqueCounts[false] = counts[0];
    }
    if (counts[1] > 0) {
      uniqueCounts[true] = counts[1];
    }
  } else {
    // Note: There is no science behind the reservation size. Just trying to
    // minimize internal allocations...
    uniqueCounts.reserve(data_.size() / 3);
    for (auto i = 0; i < data_.size(); ++i) {
      ++uniqueCounts[data_[i]];
    }
  }
  uniqueCounts_.emplace(std::move(uniqueCounts));
}

template <typename T, typename InputType>
void LazyStatistics<T, InputType>::populateBucketCounts() {
  using UnsignedT = typename std::make_unsigned<T>::type;
  // Bucket counts are calculated in two phases. In phase one, we iterate on all
  // entries, and (efficiently) count the occurences based on the MSB (most
  // significant bit) of the entry. In phase two, we merge the results of phase
  // one, for each conscutive 7 bits.
  // See benchmarks in
  // dwio/alpha/encodings/tests:bucket_benchmark for why this method is used.
  std::array<uint64_t, std::numeric_limits<UnsignedT>::digits + 1> bitCounts{};
  for (auto i = 0; i < data_.size(); ++i) {
    auto value = data_[i];
    ++(bitCounts
           [std::numeric_limits<UnsignedT>::digits -
            std::countl_zero(static_cast<UnsignedT>(
                static_cast<UnsignedT>(value) -
                static_cast<UnsignedT>(min())))]);
  }

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

  bucketCounts_ = std::move(bucketCounts);
}

template <typename T, typename InputType>
void LazyStatistics<T, InputType>::populateStringLength() {
  uint64_t total = 0;
  for (int i = 0; i < data_.size(); ++i) {
    total += data_[i].size();
  }
  totalStringsLength_ = total;
}

template <typename T, typename InputType>
LazyStatistics<T, InputType> LazyStatistics<T, InputType>::create(
    std::span<const InputType> data) {
  LazyStatistics<T, InputType> statistics;
  if (data.size() == 0) {
    statistics.consecutiveRepeatCount_ = 0;
    statistics.minRepeat_ = 0;
    statistics.maxRepeat_ = 0;
    statistics.totalStringsLength_ = 0;
    statistics.totalStringsRepeatLength_ = 0;
    statistics.min_ = T();
    statistics.max_ = T();

    statistics.bucketCounts_ = {};
    statistics.uniqueCounts_ = {};
    return statistics;
  }

  statistics.data_ = data;
  return statistics;
}

template LazyStatistics<int8_t> LazyStatistics<int8_t>::create(
    std::span<const int8_t> data);
template LazyStatistics<uint8_t> LazyStatistics<uint8_t>::create(
    std::span<const uint8_t> data);
template LazyStatistics<int16_t> LazyStatistics<int16_t>::create(
    std::span<const int16_t> data);
template LazyStatistics<uint16_t> LazyStatistics<uint16_t>::create(
    std::span<const uint16_t> data);
template LazyStatistics<int32_t> LazyStatistics<int32_t>::create(
    std::span<const int32_t> data);
template LazyStatistics<uint32_t> LazyStatistics<uint32_t>::create(
    std::span<const uint32_t> data);
template LazyStatistics<int64_t> LazyStatistics<int64_t>::create(
    std::span<const int64_t> data);
template LazyStatistics<uint64_t> LazyStatistics<uint64_t>::create(
    std::span<const uint64_t> data);
template LazyStatistics<float> LazyStatistics<float>::create(
    std::span<const float> data);
template LazyStatistics<double> LazyStatistics<double>::create(
    std::span<const double> data);
template LazyStatistics<bool> LazyStatistics<bool>::create(
    std::span<const bool> data);
template LazyStatistics<std::string_view> LazyStatistics<
    std::string_view>::create(std::span<const std::string_view> data);
template LazyStatistics<std::string_view, std::string>
LazyStatistics<std::string_view, std::string>::create(
    std::span<const std::string> data);

// populateRepeats works on all types
template void LazyStatistics<int8_t>::populateRepeats();
template void LazyStatistics<uint8_t>::populateRepeats();
template void LazyStatistics<int16_t>::populateRepeats();
template void LazyStatistics<uint16_t>::populateRepeats();
template void LazyStatistics<int32_t>::populateRepeats();
template void LazyStatistics<uint32_t>::populateRepeats();
template void LazyStatistics<int64_t>::populateRepeats();
template void LazyStatistics<uint64_t>::populateRepeats();
template void LazyStatistics<float>::populateRepeats();
template void LazyStatistics<double>::populateRepeats();
template void LazyStatistics<bool>::populateRepeats();
template void LazyStatistics<std::string_view>::populateRepeats();
template void LazyStatistics<std::string_view, std::string>::populateRepeats();

// populateUniques works on all types
template void LazyStatistics<int8_t>::populateUniques();
template void LazyStatistics<uint8_t>::populateUniques();
template void LazyStatistics<int16_t>::populateUniques();
template void LazyStatistics<uint16_t>::populateUniques();
template void LazyStatistics<int32_t>::populateUniques();
template void LazyStatistics<uint32_t>::populateUniques();
template void LazyStatistics<int64_t>::populateUniques();
template void LazyStatistics<uint64_t>::populateUniques();
template void LazyStatistics<float>::populateUniques();
template void LazyStatistics<double>::populateUniques();
template void LazyStatistics<bool>::populateUniques();
template void LazyStatistics<std::string_view>::populateUniques();
template void LazyStatistics<std::string_view, std::string>::populateUniques();

// populateMinMax works on numeric types only
template void LazyStatistics<int8_t>::populateMinMax();
template void LazyStatistics<uint8_t>::populateMinMax();
template void LazyStatistics<int16_t>::populateMinMax();
template void LazyStatistics<uint16_t>::populateMinMax();
template void LazyStatistics<int32_t>::populateMinMax();
template void LazyStatistics<uint32_t>::populateMinMax();
template void LazyStatistics<int64_t>::populateMinMax();
template void LazyStatistics<uint64_t>::populateMinMax();
template void LazyStatistics<float>::populateMinMax();
template void LazyStatistics<double>::populateMinMax();
template void LazyStatistics<std::string_view>::populateMinMax();
template void LazyStatistics<std::string_view, std::string>::populateMinMax();

// populateBucketCounts works on integral types only
template void LazyStatistics<int8_t>::populateBucketCounts();
template void LazyStatistics<uint8_t>::populateBucketCounts();
template void LazyStatistics<int16_t>::populateBucketCounts();
template void LazyStatistics<uint16_t>::populateBucketCounts();
template void LazyStatistics<int32_t>::populateBucketCounts();
template void LazyStatistics<uint32_t>::populateBucketCounts();
template void LazyStatistics<int64_t>::populateBucketCounts();
template void LazyStatistics<uint64_t>::populateBucketCounts();

// String functions
template void LazyStatistics<std::string_view>::populateStringLength();
template void
LazyStatistics<std::string_view, std::string>::populateStringLength();

} // namespace facebook::alpha
