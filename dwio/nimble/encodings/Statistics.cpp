// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/nimble/encodings/Statistics.h"
#include "dwio/nimble/common/Types.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <type_traits>

namespace facebook::nimble {

namespace {

template <typename T, typename InputType>
using MapType = typename UniqueValueCounts<T, InputType>::MapType;

} // namespace

template <typename T, typename InputType>
void Statistics<T, InputType>::populateRepeats() const {
  uint64_t consecutiveRepeatCount = 0;
  uint64_t minRepeat = std::numeric_limits<uint64_t>::max();
  uint64_t maxRepeat = 0;

  uint64_t totalRepeatLength = 0; // only needed for strings
  if constexpr (nimble::isStringType<T>()) {
    totalRepeatLength = data_[0].size();
  }

  T currentValue = data_[0];
  uint64_t currentRepeat = 0;

  for (auto i = 0; i < data_.size(); ++i) {
    const auto& value = data_[i];

    if (value == currentValue) {
      ++currentRepeat;
    } else {
      if constexpr (nimble::isStringType<T>()) {
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
void Statistics<T, InputType>::populateMinMax() const {
  if constexpr (nimble::isNumericType<InputType>()) {
    const auto [min, max] = std::minmax_element(data_.begin(), data_.end());
    min_ = *min;
    max_ = *max;
  } else if constexpr (nimble::isStringType<InputType>()) {
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
void Statistics<T, InputType>::populateUniques() const {
  MapType<T, InputType> uniqueCounts;
  if constexpr (nimble::isBoolType<T>()) {
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
void Statistics<T, InputType>::populateBucketCounts() const {
  using UnsignedT = typename std::make_unsigned<T>::type;
  // Bucket counts are calculated in two phases. In phase one, we iterate on all
  // entries, and (efficiently) count the occurences based on the MSB (most
  // significant bit) of the entry. In phase two, we merge the results of phase
  // one, for each conscutive 7 bits.
  // See benchmarks in
  // dwio/nimble/encodings/tests:bucket_benchmark for why this method is used.
  std::array<uint64_t, std::numeric_limits<UnsignedT>::digits + 1> bitCounts{};
  for (auto i = 0; i < data_.size(); ++i) {
    ++(bitCounts
           [std::numeric_limits<UnsignedT>::digits -
            std::countl_zero(static_cast<UnsignedT>(
                static_cast<UnsignedT>(data_[i]) -
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
void Statistics<T, InputType>::populateStringLength() const {
  uint64_t total = 0;
  for (int i = 0; i < data_.size(); ++i) {
    total += data_[i].size();
  }
  totalStringsLength_ = total;
}

template <typename T, typename InputType>
Statistics<T, InputType> Statistics<T, InputType>::create(
    std::span<const InputType> data) {
  Statistics<T, InputType> statistics;
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
template Statistics<std::string_view, std::string>
Statistics<std::string_view, std::string>::create(
    std::span<const std::string> data);

// populateRepeats works on all types
template void Statistics<int8_t>::populateRepeats() const;
template void Statistics<uint8_t>::populateRepeats() const;
template void Statistics<int16_t>::populateRepeats() const;
template void Statistics<uint16_t>::populateRepeats() const;
template void Statistics<int32_t>::populateRepeats() const;
template void Statistics<uint32_t>::populateRepeats() const;
template void Statistics<int64_t>::populateRepeats() const;
template void Statistics<uint64_t>::populateRepeats() const;
template void Statistics<float>::populateRepeats() const;
template void Statistics<double>::populateRepeats() const;
template void Statistics<bool>::populateRepeats() const;
template void Statistics<std::string_view>::populateRepeats() const;
template void Statistics<std::string_view, std::string>::populateRepeats()
    const;

// populateUniques works on all types
template void Statistics<int8_t>::populateUniques() const;
template void Statistics<uint8_t>::populateUniques() const;
template void Statistics<int16_t>::populateUniques() const;
template void Statistics<uint16_t>::populateUniques() const;
template void Statistics<int32_t>::populateUniques() const;
template void Statistics<uint32_t>::populateUniques() const;
template void Statistics<int64_t>::populateUniques() const;
template void Statistics<uint64_t>::populateUniques() const;
template void Statistics<float>::populateUniques() const;
template void Statistics<double>::populateUniques() const;
template void Statistics<bool>::populateUniques() const;
template void Statistics<std::string_view>::populateUniques() const;
template void Statistics<std::string_view, std::string>::populateUniques()
    const;

// populateMinMax works on numeric types only
template void Statistics<int8_t>::populateMinMax() const;
template void Statistics<uint8_t>::populateMinMax() const;
template void Statistics<int16_t>::populateMinMax() const;
template void Statistics<uint16_t>::populateMinMax() const;
template void Statistics<int32_t>::populateMinMax() const;
template void Statistics<uint32_t>::populateMinMax() const;
template void Statistics<int64_t>::populateMinMax() const;
template void Statistics<uint64_t>::populateMinMax() const;
template void Statistics<float>::populateMinMax() const;
template void Statistics<double>::populateMinMax() const;
template void Statistics<std::string_view>::populateMinMax() const;
template void Statistics<std::string_view, std::string>::populateMinMax() const;

// populateBucketCounts works on integral types only
template void Statistics<int8_t>::populateBucketCounts() const;
template void Statistics<uint8_t>::populateBucketCounts() const;
template void Statistics<int16_t>::populateBucketCounts() const;
template void Statistics<uint16_t>::populateBucketCounts() const;
template void Statistics<int32_t>::populateBucketCounts() const;
template void Statistics<uint32_t>::populateBucketCounts() const;
template void Statistics<int64_t>::populateBucketCounts() const;
template void Statistics<uint64_t>::populateBucketCounts() const;

// String functions
template void Statistics<std::string_view>::populateStringLength() const;
template void Statistics<std::string_view, std::string>::populateStringLength()
    const;

} // namespace facebook::nimble
