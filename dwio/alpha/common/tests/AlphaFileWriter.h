// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/velox/VeloxWriterOptions.h"
#include "velox/vector/BaseVector.h"

namespace facebook::alpha::test {

std::string createAlphaFile(
    velox::memory::MemoryPool& memoryPool,
    const std::vector<velox::VectorPtr>& vectors,
    alpha::VeloxWriterOptions writerOptions = {});

std::string createAlphaFile(
    velox::memory::MemoryPool& memoryPool,
    const velox::VectorPtr& vector,
    alpha::VeloxWriterOptions writerOptions = {});

} // namespace facebook::alpha::test
