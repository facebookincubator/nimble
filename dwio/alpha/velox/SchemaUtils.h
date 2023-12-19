// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/velox/SchemaReader.h"
#include "velox/type/Type.h"

namespace facebook::alpha {

velox::TypePtr convertToVeloxType(const Type& type);

std::shared_ptr<const Type> convertToAlphaType(const velox::Type& type);

} // namespace facebook::alpha
