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
#include "dwio/nimble/velox/IndexWriter.h"
#include "dwio/nimble/velox/VeloxWriterOptions.h"
#include "velox/common/file/File.h"
#include "velox/dwio/common/ExecutorBarrier.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"

// The VeloxWriter takes a velox VectorPtr and writes it to an Nimble file
// format.

namespace facebook::nimble {

namespace detail {

class WriterContext;

} // namespace detail

using index::IndexWriter;

// Writer that takes velox vector as input and produces nimble file.
class VeloxWriter {
 public:
  VeloxWriter(
      const velox::TypePtr& type,
      std::unique_ptr<velox::WriteFile> file,
      velox::memory::MemoryPool& pool,
      VeloxWriterOptions options);

  ~VeloxWriter();

  // Return value of 'true' means this write ended with a flush.
  bool write(const velox::VectorPtr& input);

  void close();

  void flush();

  struct Stats {
    // Total bytes written to the file.
    uint64_t bytesWritten;
    // Number of stripes written to the file.
    uint32_t stripeCount;
    // Uncompressed size of data written to the file.
    uint64_t rawSize;
    // Number of rows in each stripe.
    std::vector<uint64_t> rowsPerStripe;
    // CPU time spent flushing data in microseconds.
    uint64_t flushCpuTimeUs;
    // Wall clock time spent flushing data in microseconds.
    uint64_t flushWallTimeUs;
    // CPU time spent on encoding selection in microseconds.
    uint64_t encodingSelectionCpuTimeUs;
    // Number of input buffer reallocations. These 2 stats should be from memory
    // pool and have better coverage in the future.
    uint64_t inputBufferReallocCount;
    // Number of items moved during input buffer reallocations.
    uint64_t inputBufferReallocItemCount;
    // Per-column statistics. Only available at file close.
    // NOTE: expected to be exposed as a view, for merging with base stats
    // objects. User needs to explicitly copy.
    std::vector<ColumnStatistics*> columnStats;
  };
  Stats stats() const;

 private:
  inline bool hasIndex() const;

  // Adds index key to the index writer. If barrier is provided, the processing
  // will be added to the barrier for parallel execution.
  void addIndexKey(
      const velox::VectorPtr& input,
      velox::dwio::common::ExecutorBarrier* barrier = nullptr);

  bool shouldFlush(FlushPolicy* policy) const;

  bool shouldChunk(FlushPolicy* policy) const;

  bool flushChunks(
      const std::vector<uint32_t>& indices,
      bool ensureFullChunks,
      FlushPolicy* policy);

  bool encodeStreamChunk(
      StreamData& streamData,
      uint64_t minChunkSize,
      uint64_t maxChunkSize,
      bool ensureFullChunks,
      Stream& stream,
      uint64_t& streamBytes,
      std::atomic_uint64_t& chunkBytes,
      std::atomic_uint64_t& logicalBytes);

  void encodeKeyStreamChunk(bool lastChunk, bool ensureFullChunks);

  // Encodes a single chunk view and writes it to the encoded chunk.
  // Returns the number of bytes written to the encoded chunk.
  uint32_t encodeChunk(const StreamData& chunkView, Chunk& chunk);

  void encodeStream(
      StreamData& streamData,
      uint64_t& streamSize,
      std::atomic_uint64_t& chunkSize);

  void processStream(
      StreamData& streamData,
      uint64_t& streamSize,
      std::atomic_uint64_t& chunkSize);

  // Processes the key stream if index is enabled. This is a no-op if index is
  // not enabled. If barrier is provided, the processing will be added to the
  // barrier for parallel execution.
  void maybeProcessKeyStream(
      velox::dwio::common::ExecutorBarrier* barrier = nullptr);

  // Encodes key stream chunks if index is enabled. This is a no-op if index is
  // not enabled. If barrier is provided, the encoding will be added to the
  // barrier for parallel execution.
  void maybeEncodeKeyStreamChunk(
      bool lastChunk,
      bool ensureFullChunks,
      velox::dwio::common::ExecutorBarrier* barrier = nullptr);

  // Finishes the key stream for the current stripe and returns it.
  // Returns std::nullopt if index is not enabled.
  std::optional<KeyStream> finishKeyStream();

  // Returning 'true' if stripe was flushed.
  bool evaluateFlushPolicy();

  // Returning 'true' if stripe was written.
  bool writeStripe();

  // Encodes and writes all streams to the tablet writer. This method iterates
  // through all field writers to encode their stream data and append them to
  // the tablet. Note: This method does not perform chunking.
  void writeStreams();

  // Writes stream chunks for the specified stream indices. This method performs
  // chunking of encoded stream data and writes them to the tablet writer.
  // Returns 'true' if chunks were written.
  // Parameters:
  //   streamIndices: Indices of streams to write chunks for
  //   ensureFullChunks: If true, ensures chunks meet minimum size requirements
  //   lastChunk: If true, indicates this is the final chunk for the streams
  bool writeChunks(
      std::span<const uint32_t> streamIndices,
      bool ensureFullChunks = false,
      bool lastChunk = false);

  void writeMetadata();
  void writeColumnStats();
  void writeSchema();

  void ensureEncodingBuffer();
  void clearEncodingBuffer();

  void ensureWriteStreams();
  void resetFieldWriter();

  const std::shared_ptr<const velox::dwio::common::TypeWithId> schema_;
  MemoryPoolHolder writerMemoryPool_;
  MemoryPoolHolder encodingMemoryPool_;
  const std::unique_ptr<detail::WriterContext> context_;
  std::unique_ptr<velox::WriteFile> file_;
  const std::unique_ptr<FieldWriter> rootWriter_;
  const std::unique_ptr<IndexWriter> indexWriter_;
  const std::unique_ptr<TabletWriter> tabletWriter_;

  std::unique_ptr<Buffer> encodingBuffer_;
  std::vector<Stream> encodedStreams_;
  std::exception_ptr lastException_;
};

} // namespace facebook::nimble
