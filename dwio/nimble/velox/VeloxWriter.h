/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/tablet/TabletWriter.h"
#include "dwio/nimble/velox/FieldWriter.h"
#include "dwio/nimble/velox/VeloxWriterOptions.h"
#include "velox/common/file/File.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"

// The VeloxWriter takes a velox VectorPtr and writes it to an Nimble file
// format.

namespace facebook::nimble {

namespace detail {

class WriterContext;

} // namespace detail

// Writer that takes velox vector as input and produces nimble file.
class VeloxWriter {
 public:
  struct RunStats {
    uint64_t bytesWritten;
    uint32_t stripeCount;
    uint64_t rawSize;
    std::vector<uint64_t> rowsPerStripe;
    uint64_t flushCpuTimeUsec;
    uint64_t flushWallTimeUsec;
    uint64_t encodingSelectionCpuTimeUsec;
    // These 2 stats should be from memory pool and have better
    // coverage in the future.
    uint64_t inputBufferReallocCount;
    uint64_t inputBufferReallocItemCount;
    std::unordered_map<offset_size, ColumnStats> columnStats;
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
  MemoryPoolHolder writerMemoryPool_;
  MemoryPoolHolder encodingMemoryPool_;
  std::unique_ptr<detail::WriterContext> context_;
  TabletWriter writer_;
  std::unique_ptr<FieldWriter> root_;

  std::unique_ptr<Buffer> encodingBuffer_;
  std::vector<Stream> streams_;
  std::exception_ptr lastException_;
  const velox::common::SpillConfig* const spillConfig_;

  // Returning 'true' if stripe was flushed.
  bool evalauateFlushPolicy();
  // Returning 'true' if stripe was written.
  bool writeStripe();
  void writeChunk(bool lastChunk = true);
  // Returns 'true' if chunks were written.
  bool writeChunks(
      std::span<const uint32_t> streamIndices,
      bool lastChunk = true);
};

} // namespace facebook::nimble
