// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <type_traits>

namespace facebook::alpha {

template <typename T, typename = void>
class AlphaCompare {
 public:
  static bool equals(const T& a, const T& b);
};

template <typename T, typename V>
inline bool AlphaCompare<T, V>::equals(const T& a, const T& b) {
  return a == b;
}

template <typename T>
class AlphaCompare<
    T,
    std::enable_if_t<std::is_same_v<T, double> || std::is_same_v<T, float>>> {
 public:
  // double or float
  using FloatingType =
      typename std::conditional<std::is_same_v<T, double>, double, float>::type;
  // 64bit integer or 32 bit integer
  using IntegralType = typename std::
      conditional<std::is_same_v<T, double>, int64_t, int32_t>::type;

  static_assert(sizeof(FloatingType) == sizeof(IntegralType));

  static bool equals(const T& a, const T& b);

  // This will be convenient when for debug, logging.
  static IntegralType asInteger(const T& a);
};

template <typename T>
inline bool AlphaCompare<
    T,
    std::enable_if_t<std::is_same_v<T, double> || std::is_same_v<T, float>>>::
    equals(const T& a, const T& b) {
  // For floating point types, we do bit-wise comparison, for other types,
  // just use the original ==.
  // TODO: handle NaN.
  return *(reinterpret_cast<const IntegralType*>(&(a))) ==
      *(reinterpret_cast<const IntegralType*>(&(b)));
}

template <typename T>
inline typename AlphaCompare<
    T,
    std::enable_if_t<
        std::is_same_v<T, double> || std::is_same_v<T, float>>>::IntegralType
AlphaCompare<
    T,
    std::enable_if_t<std::is_same_v<T, double> || std::is_same_v<T, float>>>::
    asInteger(const T& a) {
  return *(reinterpret_cast<const IntegralType*>(&(a)));
}

template <typename T>
struct AlphaComparator {
  constexpr bool operator()(const T& a, const T& b) const {
    return AlphaCompare<T>::equals(a, b);
  }
};

} // namespace facebook::alpha
