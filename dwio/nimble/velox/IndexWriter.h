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

#include <memory>
#include <optional>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/index/IndexConfig.h"
#include "dwio/nimble/index/KeyEncoder.h"
#include "dwio/nimble/tablet/TabletWriter.h"
#include "dwio/nimble/velox/StreamData.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble {

/// IndexWriter encodes index keys
/// produces encoded chunks ready for tablet writing.
///
/// It uses KeyEncoder to transform specified index columns into byte-comparable
/// keys that enable efficient range-based filtering during reads. The encoded
/// keys preserve the sort order defined by the column types, allowing
/// lexicographic comparison of encoded keys to match logical comparison of
/// original values.
///
/// The IndexWriter encapsulates all key stream processing including:
/// - Key encoding from input vectors
/// - Chunking keys into manageable pieces
/// - Encoding chunks ready for tablet writer
///
/// It supports two encoding modes:
/// - Non-chunked mode: Encodes all accumulated keys in one go
/// - Chunked mode: Progressively encodes keys as they accumulate
///
/// Example usage:
///   auto indexWriter = IndexWriter::create(indexConfig, inputType, context);
///   indexWriter->write(inputBatch);
///   // ... write more batches ...
///   // Non-chunked mode:
///   auto keyStream = indexWriter->encode(buffer);
///   // Or chunked mode with progressive encoding:
///   indexWriter->encodeChunk(buffer, minSize, maxSize, ensureFullChunks,
///   stream);
class IndexWriter {
 public:
  /// Factory method to create an IndexWriter instance.
  ///
  /// @param config Optional index configuration. If not present, returns
  ///               nullptr (no indexing).
  /// @param inputType Type of input data containing the index columns.
  /// @param pool Memory pool for allocations.
  /// @return Unique pointer to IndexWriter, or nullptr if config is not
  ///         present.
  static std::unique_ptr<IndexWriter> create(
      const std::optional<IndexConfig>& config,
      const velox::TypePtr& inputType,
      velox::memory::MemoryPool* pool);

  ~IndexWriter() = default;

  IndexWriter(const IndexWriter&) = delete;
  IndexWriter& operator=(const IndexWriter&) = delete;
  IndexWriter(IndexWriter&&) = delete;
  IndexWriter& operator=(IndexWriter&&) = delete;

  /// Encodes index keys from the input vector and appends them to the internal
  /// key stream.
  ///
  /// If enforceKeyOrder is enabled, verifies that all encoded keys (including
  /// the boundary with previously encoded keys) are in non-descending order.
  /// Throws an exception if keys are found to be out of order.
  ///
  /// @param input Input vector containing rows to encode.
  /// @param buffer Buffer for string allocations during key encoding.
  void write(const velox::VectorPtr& input, Buffer& buffer);

  /// Encodes all accumulated keys into the internal stream.
  /// This is the non-chunked encoding mode.
  /// The encoded stream is retrieved via finishStripe().
  ///
  /// @param buffer Encoding buffer to use for encoding.
  void encodeStream(Buffer& buffer);

  /// Encodes a chunk of accumulated keys into the internal stream.
  /// This is the chunked encoding mode for progressive encoding during writes.
  ///
  /// @param ensureFullChunks If true, only creates chunks at maxChunkSize.
  /// @param lastChunk If true, uses minChunkSize = 0 to flush remaining keys.
  /// @param buffer Encoding buffer to use for encoding.
  /// @return true if a chunk was encoded, false if no chunk was created.
  bool encodeChunk(bool ensureFullChunks, bool lastChunk, Buffer& buffer);

  /// Returns the key stream and clears ownership.
  /// Used at end of stripe to pass the stream to TabletWriter.
  ///
  /// @param buffer Encoding buffer to use for encoding any remaining keys.
  std::optional<KeyStream> finishStripe(Buffer& buffer);

  /// Returns true if there are accumulated keys to encode.
  bool hasKeys() const;

  /// Resets the key stream, clearing all encoded keys.
  /// Called between stripes to start fresh for each stripe.
  void reset();

  /// Closes the writer and releases resources.
  void close();

 private:
  IndexWriter(
      const IndexConfig& config,
      const velox::RowTypePtr& inputType,
      velox::memory::MemoryPool* pool);

  /// Encodes a single chunk and populates the Chunk struct and ChunkKey.
  /// @param data Span of keys to encode.
  /// @param firstKey First key in the chunk.
  /// @param lastKey Last key in the chunk.
  /// @param buffer Encoding buffer.
  /// @param chunk Output chunk to populate.
  /// @param chunkKey Output chunk key to populate with firstKey/lastKey.
  /// @return Number of bytes written.
  uint32_t encodeChunkData(
      std::span<const std::string_view> data,
      std::string_view firstKey,
      std::string_view lastKey,
      Buffer& buffer,
      Chunk& chunk,
      ChunkKey& chunkKey);

  /// Creates a new encoding policy instance for key encoding.
  std::unique_ptr<EncodingSelectionPolicy<std::string_view>>
  createEncodingPolicy() const;

  velox::memory::MemoryPool* const pool_;
  const std::unique_ptr<KeyEncoder> keyEncoder_;
  const std::unique_ptr<ContentStreamData<std::string_view>> keyStream_;
  const EncodingLayout encodingLayout_;
  const bool enforceKeyOrder_;
  const uint64_t minChunkSize_;
  const uint64_t maxChunkSize_;

  // Accumulated stream for chunked encoding
  KeyStream encodedKeyStream_;
};

} // namespace facebook::nimble
