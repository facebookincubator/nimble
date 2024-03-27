// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/common/Buffer.h"
#include "dwio/alpha/tablet/Tablet.h"
#include "dwio/alpha/velox/FieldWriter.h"
#include "dwio/alpha/velox/VeloxWriterOptions.h"
#include "velox/common/file/File.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"

// The VeloxWriter takes a velox VectorPtr and writes it to an Alpha file
// format.

namespace facebook::alpha {

namespace detail {

class WriterContext;

} // namespace detail

// Writer that takes velox vector as input and produces alpha file.
class VeloxWriter {
 public:
  struct RunStats {
    uint64_t bytesWritten;
    uint32_t stripeCount;
    std::vector<uint64_t> rowsPerStripe;
    uint64_t flushCpuTimeUsec;
    uint64_t flushWallTimeUsec;
    uint64_t encodingSelectionCpuTimeUsec;
    // These 2 stats should be from memory pool and have better
    // coverage in the future.
    uint64_t inputBufferReallocCount;
    uint64_t inputBufferReallocItemCount;
  };

  VeloxWriter(
      velox::memory::MemoryPool& memoryPool,
      const velox::TypePtr& schema,
      std::unique_ptr<velox::WriteFile> file,
      VeloxWriterOptions options);

  ~VeloxWriter();

  // Return value of 'true' means this write ended with a flush.
  bool write(const velox::VectorPtr& vector);

  void close();

  void flush();

  RunStats getRunStats() const;

 private:
  std::shared_ptr<const velox::dwio::common::TypeWithId> schema_;
  std::unique_ptr<velox::WriteFile> file_;
  std::shared_ptr<velox::memory::MemoryPool> writerMemoryPool_;
  std::shared_ptr<velox::memory::MemoryPool> encodingMemoryPool_;
  std::unique_ptr<detail::WriterContext> context_;
  TabletWriter writer_;
  std::unique_ptr<FieldWriter> root_;

  std::unique_ptr<Buffer> encodingBuffer_;
  std::vector<Stream> streams_;
  std::exception_ptr lastException_;
  const velox::common::SpillConfig* const spillConfig_;

  // Returning 'true' if stripe was written.
  bool tryWriteStripe(bool force = false);
  void writeChunk(bool lastChunk = true);
  uint32_t writeStripe();
};

} // namespace facebook::alpha
