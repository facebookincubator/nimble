// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/SchemaTypes.h"

#include "dwio/alpha/common/Exceptions.h"

#include <fmt/format.h>

namespace facebook::alpha {
std::string toString(ScalarKind kind) {
  switch (kind) {
#define CASE(KIND)         \
  case ScalarKind::KIND: { \
    return #KIND;          \
  }
    CASE(Int8);
    CASE(UInt8);
    CASE(Int16);
    CASE(UInt16);
    CASE(Int32);
    CASE(UInt32);
    CASE(Int64);
    CASE(UInt64);
    CASE(Float);
    CASE(Double);
    CASE(Bool);
    CASE(String);
    CASE(Binary);
    CASE(Undefined);
#undef CASE
  }
  ALPHA_UNREACHABLE(fmt::format("Unknown: {}.", kind));
}

std::string toString(Kind kind) {
  switch (kind) {
#define CASE(KIND)   \
  case Kind::KIND: { \
    return #KIND;    \
  }
    CASE(Scalar);
    CASE(Row);
    CASE(Array);
    CASE(ArrayWithOffsets);
    CASE(Map);
    CASE(FlatMap);
#undef CASE
  }
  ALPHA_UNREACHABLE(fmt::format("Unknown: {}.", kind));
}

} // namespace facebook::alpha
