// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/common/tests/AlphaFileWriter.h"
#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/velox/VeloxWriter.h"
#include "dwio/alpha/velox/VeloxWriterOptions.h"

namespace facebook::alpha::test {

std::string createAlphaFile(
    velox::memory::MemoryPool& memoryPool,
    const velox::VectorPtr& vector,
    alpha::VeloxWriterOptions writerOptions,
    bool flushAfterWrite) {
  return createAlphaFile(
      memoryPool,
      std::vector<facebook::velox::VectorPtr>{vector},
      std::move(writerOptions),
      flushAfterWrite);
}

std::string createAlphaFile(
    velox::memory::MemoryPool& memoryPool,
    const std::vector<facebook::velox::VectorPtr>& vectors,
    alpha::VeloxWriterOptions writerOptions,
    bool flushAfterWrite) {
  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  ALPHA_ASSERT(vectors.size() > 0, "Expecting at least one input vector.");
  auto& type = vectors[0]->type();

  for (int i = 1; i < vectors.size(); ++i) {
    ALPHA_ASSERT(
        vectors[i]->type()->kindEquals(type),
        "All vectors should have the same schema.");
  }

  alpha::VeloxWriter writer(
      memoryPool, type, std::move(writeFile), std::move(writerOptions));
  for (const auto& vector : vectors) {
    writer.write(vector);
    if (flushAfterWrite) {
      writer.flush();
    }
  }
  writer.close();

  return file;
}

} // namespace facebook::alpha::test
