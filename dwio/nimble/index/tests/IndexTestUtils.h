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

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/index/StripeIndexGroup.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/tablet/TabletWriter.h"
#include "dwio/nimble/velox/ChunkedStream.h"
#include "dwio/nimble/velox/VeloxWriterOptions.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::nimble::index::test {

/// Generator function type for key columns.
/// Takes (vectorIndex, numRows) and returns a VectorPtr.
using KeyColumnGenerator =
    std::function<velox::VectorPtr(size_t, velox::vector_size_t)>;

/// Map from column name to generator function.
using KeyColumnGeneratorMap =
    std::unordered_map<std::string, KeyColumnGenerator>;

/// Generates random data using VectorFuzzer with specified key column
/// generators.
/// @param rowType The type of the row vector.
/// @param numVectors Number of vectors to generate.
/// @param numRowsPerVector Number of rows per vector.
/// @param keyColumnGenerators Map from column name to generator function.
///        Generator function takes (vectorIndex, numRows) and returns a
///        VectorPtr. Key columns are generated without nulls.
/// @param pool Memory pool for vector allocation.
/// @param nullRatio Null ratio for non-key columns (default 0.1).
std::vector<velox::RowVectorPtr> generateData(
    const velox::RowTypePtr& rowType,
    size_t numVectors,
    velox::vector_size_t numRowsPerVector,
    const KeyColumnGeneratorMap& keyColumnGenerators,
    velox::memory::MemoryPool* pool,
    double nullRatio = 0.1);

/// Writes row vectors to a Nimble file with cluster index.
/// @param filePath Path to write the Nimble file.
/// @param data Vector of RowVectorPtr to write.
/// @param indexConfig Index configuration specifying columns, sort orders, etc.
/// @param pool Memory pool for writing.
/// @param flushPolicyFactory Optional factory for custom flush policy to
/// control
///        stripe sizes. If not provided, uses default flush policy.
void writeFile(
    const std::string& filePath,
    const std::vector<velox::RowVectorPtr>& data,
    IndexConfig indexConfig,
    velox::memory::MemoryPool& pool,
    std::function<std::unique_ptr<FlushPolicy>()> flushPolicyFactory = nullptr);

/// Simple StreamLoader implementation for testing that holds data in memory.
class TestStreamLoader : public StreamLoader {
 public:
  explicit TestStreamLoader(std::string_view data) : data_(data) {}

  const std::string_view getStream() const override {
    return data_;
  }

 private:
  std::string_view data_;
};

/// Wrapper around InMemoryChunkedStream that ensures exactly one chunk is
/// decoded. Use this when the data is expected to contain exactly one chunk.
class SingleChunkDecoder {
 public:
  explicit SingleChunkDecoder(
      velox::memory::MemoryPool& pool,
      std::string_view data)
      : stream_(pool, std::make_unique<TestStreamLoader>(data)) {}

  /// Decodes and returns the single chunk.
  /// Asserts that exactly one chunk exists in the data.
  std::string_view decode() {
    NIMBLE_CHECK(stream_.hasNext(), "Expected at least one chunk");
    auto chunk = stream_.nextChunk();
    NIMBLE_CHECK(!stream_.hasNext(), "Expected exactly one chunk");
    return chunk;
  }

 private:
  InMemoryChunkedStream stream_;
};

/// Holds key stream statistics extracted from a StripeIndexGroup for testing.
struct KeyStreamStats {
  /// Byte offset of key stream for each stripe.
  std::vector<uint32_t> offsets;
  /// Byte size of key stream for each stripe.
  std::vector<uint32_t> sizes;
  /// Accumulated chunk counts per stripe.
  std::vector<uint32_t> chunkCounts;
  /// Accumulated row counts per chunk.
  std::vector<uint32_t> chunkRows;
  /// Byte offset of each chunk within its key stream.
  std::vector<uint32_t> chunkOffsets;
  /// Last key value for each chunk.
  std::vector<std::string> chunkKeys;
};

/// Holds stream position index statistics for a specific stream.
struct StreamStats {
  /// Accumulated chunk count for each stripe for this stream.
  std::vector<uint32_t> chunkCounts;
  /// Accumulated row counts per chunk for this stream.
  std::vector<uint32_t> chunkRows;
  /// Byte offset of each chunk within its stream.
  std::vector<uint32_t> chunkOffsets;
};

/// Specification for a single chunk in a stream (for test data creation).
struct ChunkSpec {
  uint32_t rowCount;
  uint32_t size;
};

/// Specification for a single stream (for test data creation).
struct StreamSpec {
  uint32_t offset;
  std::vector<ChunkSpec> chunks;
};

/// Specification for a key chunk (for test data creation).
struct KeyChunkSpec {
  uint32_t rowCount;
  std::string firstKey;
  std::string lastKey;
};

/// Creates a KeyStream with test data from the given chunk specifications.
/// The buffer is used to store the chunk content and key data.
KeyStream createKeyStream(
    Buffer& buffer,
    const std::vector<KeyChunkSpec>& chunkSpecs);

/// Creates chunks for a stream from the given chunk specifications.
/// The buffer is used to store the chunk content.
std::vector<Chunk> createChunks(
    Buffer& buffer,
    const std::vector<ChunkSpec>& chunkSpecs);

/// Creates a Stream with the specified test parameters.
/// The buffer is used to store the chunk content.
Stream createStream(Buffer& buffer, const StreamSpec& spec);

/// Test helper class for StripeIndexGroup
/// to private members for testing purposes. This is a friend class of
/// StripeIndexGroup.
class StripeIndexGroupTestHelper {
 public:
  explicit StripeIndexGroupTestHelper(const StripeIndexGroup* indexGroup)
      : indexGroup_(indexGroup) {}

  /// Returns the group index.
  uint32_t groupIndex() const {
    return indexGroup_->groupIndex_;
  }

  /// Returns the first stripe index in this group.
  uint32_t firstStripe() const {
    return indexGroup_->firstStripe_;
  }

  /// Returns the number of stripes in this group.
  uint32_t stripeCount() const {
    return indexGroup_->stripeCount_;
  }

  /// Returns the number of streams per stripe.
  uint32_t streamCount() const {
    return indexGroup_->streamCount_;
  }

  /// Returns the key stream statistics for testing verification.
  KeyStreamStats keyStreamStats() const;

  /// Returns the stream position index statistics for the specified stream.
  StreamStats streamStats(uint32_t streamId) const;

  /// Returns the length of a key chunk given its stream offset within a stripe.
  /// Uses the next chunk offset (if exists) or stream length for the last
  /// chunk.
  uint32_t keyChunkLength(
      uint32_t stripeIndex,
      uint32_t chunkStreamOffset,
      uint32_t keyStreamLength) const;

 private:
  const StripeIndexGroup* const indexGroup_;
};

} // namespace facebook::nimble::index::test
