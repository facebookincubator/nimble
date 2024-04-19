// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/nimble/velox/SchemaReader.h"
#include "velox/type/Type.h"

namespace facebook::nimble {

velox::TypePtr convertToVeloxType(const Type& type);

std::shared_ptr<const Type> convertToNimbleType(const velox::Type& type);

} // namespace facebook::nimble
