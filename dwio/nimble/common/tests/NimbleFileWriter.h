// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/nimble/velox/VeloxWriterOptions.h"
#include "velox/vector/BaseVector.h"

namespace facebook::nimble::test {

std::string createNimbleFile(
    velox::memory::MemoryPool& memoryPool,
    const std::vector<velox::VectorPtr>& vectors,
    nimble::VeloxWriterOptions writerOptions = {},
    bool flushAfterWrite = true);

std::string createNimbleFile(
    velox::memory::MemoryPool& memoryPool,
    const velox::VectorPtr& vector,
    nimble::VeloxWriterOptions writerOptions = {},
    bool flushAfterWrite = true);

} // namespace facebook::nimble::test
