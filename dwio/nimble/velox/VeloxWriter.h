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
#include "dwio/nimble/index/ClusterIndexWriter.h"
#include "dwio/nimble/index/HashIndexWriter.h"
#include "dwio/nimble/index/SortedIndexWriter.h"
#include "dwio/nimble/tablet/TabletWriter.h"
#include "dwio/nimble/velox/FieldWriter.h"
#include "dwio/nimble/velox/VeloxWriterOptions.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/file/File.h"
#include "velox/dwio/common/ExecutorBarrier.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"

/// The VeloxWriter takes a velox VectorPtr and writes it to an Nimble file
/// format.

namespace facebook::nimble {

namespace detail {

class WriterContext;

} // namespace detail

/// Writer that takes velox vector as input and produces nimble file.
class VeloxWriter {
 public:
  VeloxWriter(
      const velox::TypePtr& type,
      std::unique_ptr<velox::WriteFile> file,
      velox::memory::MemoryPool& pool,
      VeloxWriterOptions options);

  ~VeloxWriter();

  /// Return value of 'true' means this write ended with a flush.
  bool write(const velox::VectorPtr& input);

  void close();

  void flush();

  struct Stats {
    /// Total bytes written to the file.
    uint64_t writtenBytes;
    /// Number of stripes written to the file.
    uint32_t stripeCount;
    /// Uncompressed size of data written to the file.
    uint64_t inputBytes;
    /// CPU time spent in tabletWriter write in nanoseconds.
    uint64_t writeCpuTimeNs;
    /// Wall clock time spent in tabletWriter write in nanoseconds.
    uint64_t writeWallTimeNs;
    /// CPU time spent ingesting vectors into field writer buffers in
    /// nanoseconds. Sequential — no wall time needed.
    uint64_t ingestionCpuTimeNs;
    /// CPU time spent on encoding and compression in nanoseconds.
    // TODO: Separate encoding and compression costs.
    uint64_t encodingCpuTimeNs;
    /// Wall clock time spent on encoding and compression in nanoseconds.
    /// Encoding is parallelized via encodingExecutor, so wall < CPU.
    uint64_t encodingWallTimeNs;
    /// CPU time spent on encoding selection in nanoseconds. Subset of
    /// encoding timing. Sequential — no wall time needed.
    uint64_t encodingSelectionCpuTimeNs;
    /// Rows per stripe distribution (count/sum/min/max).
    velox::RuntimeMetric rowsPerStripe;
    /// Encoded chunk size distribution in bytes (count/sum/min/max).
    velox::RuntimeMetric chunkSizeBytes;
    /// Per-column statistics. Only available at file close.
    /// NOTE: expected to be exposed as a view, for merging with base stats
    /// objects. User needs to explicitly copy.
    std::vector<ColumnStatistics*> columnStats;
  };

  /// Returns writer statistics.
  Stats stats() const;

 private:
  inline bool hasClusterIndex() const;
  inline bool hasHashIndex() const;
  inline bool hasSortedIndex() const;

  // Adds index keys to all configured index writers. If barrier is provided,
  // the processing will be added to the barrier for parallel execution.
  void addIndexKey(
      const velox::VectorPtr& input,
      velox::dwio::common::ExecutorBarrier* barrier = nullptr);

  void addClusterIndexKey(
      const velox::VectorPtr& input,
      velox::dwio::common::ExecutorBarrier* barrier = nullptr);

  void addHashIndexKey(
      const velox::VectorPtr& input,
      velox::dwio::common::ExecutorBarrier* barrier = nullptr);

  void addSortedIndexKey(
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
  // Finalizes and writes all indexes. Called via TabletWriter close callback.
  void writeIndexes(
      const WriteDataFn& writeDataFn,
      const CreateMetadataSectionFn& createMetadataFn,
      const WriteOptionalSectionFn& writeMetadataFn);

  void ensureEncodingBuffer();
  void clearEncodingBuffer();

  void ensureWriteStreams();
  void resetFieldWriter();

  const std::shared_ptr<const velox::dwio::common::TypeWithId> schema_;
  MemoryPoolHolder writerMemoryPool_;
  MemoryPoolHolder encodingMemoryPool_;
  const std::unique_ptr<detail::WriterContext> context_;
  std::unique_ptr<velox::WriteFile> file_;
  const std::unique_ptr<index::ClusterIndexWriter> clusterIndexWriter_;
  const std::unique_ptr<index::HashIndexWriter> hashIndexWriter_;
  const std::unique_ptr<index::SortedIndexWriter> sortedIndexWriter_;
  const std::unique_ptr<TabletWriter> tabletWriter_;

  std::unique_ptr<FieldWriter> rootWriter_;
  std::unique_ptr<Buffer> encodingBuffer_;
  std::vector<Stream> encodedStreams_;
  std::exception_ptr lastException_;
};

} // namespace facebook::nimble
