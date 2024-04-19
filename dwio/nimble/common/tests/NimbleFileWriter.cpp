// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/nimble/common/tests/NimbleFileWriter.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "dwio/nimble/velox/VeloxWriterOptions.h"

namespace facebook::nimble::test {

std::string createNimbleFile(
    velox::memory::MemoryPool& memoryPool,
    const velox::VectorPtr& vector,
    nimble::VeloxWriterOptions writerOptions,
    bool flushAfterWrite) {
  return createNimbleFile(
      memoryPool,
      std::vector<facebook::velox::VectorPtr>{vector},
      std::move(writerOptions),
      flushAfterWrite);
}

std::string createNimbleFile(
    velox::memory::MemoryPool& memoryPool,
    const std::vector<facebook::velox::VectorPtr>& vectors,
    nimble::VeloxWriterOptions writerOptions,
    bool flushAfterWrite) {
  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);

  NIMBLE_ASSERT(vectors.size() > 0, "Expecting at least one input vector.");
  auto& type = vectors[0]->type();

  for (int i = 1; i < vectors.size(); ++i) {
    NIMBLE_ASSERT(
        vectors[i]->type()->kindEquals(type),
        "All vectors should have the same schema.");
  }

  nimble::VeloxWriter writer(
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

} // namespace facebook::nimble::test
