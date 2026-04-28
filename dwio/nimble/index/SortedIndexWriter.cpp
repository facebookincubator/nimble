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
#include "dwio/nimble/index/SortedIndexWriter.h"

#include <algorithm>
#include <limits>
#include <set>

#include <fmt/ranges.h>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingLayout.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/SortedIndexGenerated.h"
#include "dwio/nimble/velox/ChunkedStreamWriter.h"
#include "dwio/nimble/velox/StreamData.h"
#include "flatbuffers/flatbuffers.h"

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Nulls.h"

namespace facebook::nimble::index {

namespace {

std::vector<std::vector<std::string>> extractColumnSets(
    const std::vector<SortedIndexConfig>& configs) {
  std::vector<std::vector<std::string>> columnSets;
  columnSets.reserve(configs.size());
  for (const auto& config : configs) {
    columnSets.emplace_back(config.columns);
  }
  return columnSets;
}

std::unique_ptr<EncodingSelectionPolicy<std::string_view>> createEncodingPolicy(
    const EncodingLayout& layout) {
  return std::make_unique<ReplayedEncodingSelectionPolicy<std::string_view>>(
      layout,
      CompressionOptions{},
      [](DataType) -> std::unique_ptr<EncodingSelectionPolicyBase> {
        return nullptr;
      });
}

} // namespace

// static
uint8_t SortedIndexWriter::rowIdWidth(uint32_t numRows) {
  if (numRows <= std::numeric_limits<uint8_t>::max()) {
    return 1;
  }
  if (numRows <= std::numeric_limits<uint16_t>::max()) {
    return 2;
  }
  if (numRows <= 0xFF'FFFF) {
    return 3;
  }
  return 4;
}

void SortedIndexWriter::IndexAccumulator::sort() {
  std::sort(
      entries.begin(),
      entries.end(),
      [](const IndexEntry& a, const IndexEntry& b) {
        if (a.key != b.key) {
          return a.key < b.key;
        }
        return a.row < b.row;
      });
}

void SortedIndexWriter::IndexAccumulator::clear() {
  entries.clear();
  entries.shrink_to_fit();
  encoder.reset();
  encodingBuffer.reset();
}

SortedIndexWriter::CompositeEntries SortedIndexWriter::buildCompositeEntries(
    const std::vector<IndexEntry>& entries,
    uint8_t idWidth) const {
  uint64_t totalBytes = 0;
  for (const auto& entry : entries) {
    totalBytes += entry.key.size() + idWidth;
  }

  CompositeEntries result;
  result.buffer = velox::AlignedBuffer::allocate<char>(totalBytes, pool_);
  auto* dest = result.buffer->asMutable<char>();

  result.entries.reserve(entries.size());
  for (const auto& entry : entries) {
    auto* entryStart = dest;
    std::memcpy(dest, entry.key.data(), entry.key.size());
    dest += entry.key.size();
    for (int8_t shift = (idWidth - 1) * 8; shift >= 0; shift -= 8) {
      *dest++ = static_cast<char>((entry.row >> shift) & 0xFF);
    }
    result.entries.emplace_back(entryStart, entry.key.size() + idWidth);
  }
  return result;
}

std::unique_ptr<SortedIndexWriter> SortedIndexWriter::create(
    const std::vector<SortedIndexConfig>& configs,
    const velox::TypePtr& inputType,
    velox::memory::MemoryPool* pool) {
  if (configs.empty()) {
    return nullptr;
  }
  NIMBLE_CHECK_NOT_NULL(pool, "memory pool must not be null");
  return std::unique_ptr<SortedIndexWriter>(
      new SortedIndexWriter(configs, velox::asRowType(inputType), pool));
}

SortedIndexWriter::SortedIndexWriter(
    const std::vector<SortedIndexConfig>& configs,
    const velox::RowTypePtr& inputType,
    velox::memory::MemoryPool* pool)
    : pool_{pool},
      keyColumnIndices_{
          getKeyColumnIndices(extractColumnSets(configs), inputType)} {
  NIMBLE_CHECK(!configs.empty(), "Sorted index configs must not be empty");
  accumulators_.reserve(configs.size());
  for (const auto& config : configs) {
    NIMBLE_CHECK(
        !config.columns.empty(), "Sorted index must have at least one column");
    for (size_t i = 0; i < accumulators_.size(); ++i) {
      NIMBLE_CHECK(
          accumulators_[i].config.columns != config.columns,
          "Duplicate sorted index columns: [{}]",
          fmt::join(config.columns, ", "));
    }
    accumulators_.emplace_back(
        IndexAccumulator{
            .config = config,
            .encoder = createKeyEncoder(config.columns, inputType, pool),
        });
  }
}

void SortedIndexWriter::write(const velox::VectorPtr& input) {
  checkNotClosed();
  if (input->size() == 0) {
    return;
  }

  NIMBLE_USER_CHECK_LE(
      numRows_ + static_cast<uint64_t>(input->size()),
      static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()),
      "Sorted index row count exceeds uint32 limit");

  validateNoNullKeys(input, keyColumnIndices_);

  for (auto& accumulator : accumulators_) {
    if (accumulator.encodingBuffer == nullptr) {
      accumulator.encodingBuffer = std::make_unique<Buffer>(*pool_);
    }
    std::vector<std::string_view> keys;
    accumulator.encoder->encode(input, keys, [&accumulator](size_t size) {
      return accumulator.encodingBuffer->reserve(size);
    });

    const auto newSize = accumulator.entries.size() + input->size();
    if (accumulator.entries.capacity() < newSize) {
      accumulator.entries.reserve(
          std::max(accumulator.entries.size() * 2, newSize));
    }
    for (velox::vector_size_t i = 0; i < input->size(); ++i) {
      accumulator.entries.emplace_back(IndexEntry{keys[i], numRows_ + i});
    }
  }

  numRows_ += input->size();
}

void SortedIndexWriter::buildIndex(
    IndexAccumulator& accumulator,
    const WriteDataFn& writeDataFn,
    flatbuffers::FlatBufferBuilder& builder) {
  accumulator.sort();

  const uint8_t idWidth = rowIdWidth(numRows_);
  auto composite = buildCompositeEntries(accumulator.entries, idWidth);

  // Free entries and encoder — composite buffer now owns all key data.
  accumulator.clear();

  // Chunk and encode following ClusterIndexWriter pattern.
  const uint64_t maxRows = accumulator.config.maxRowsPerKeyChunk > 0
      ? accumulator.config.maxRowsPerKeyChunk
      : composite.entries.size();

  const auto numChunks = static_cast<uint32_t>(
      velox::bits::divRoundUp(composite.entries.size(), maxRows));
  std::vector<uint32_t> chunkRows;
  chunkRows.reserve(numChunks);
  std::vector<uint32_t> chunkOffsets;
  chunkOffsets.reserve(numChunks);
  std::vector<std::string_view> chunkKeys;
  chunkKeys.reserve(numChunks);
  std::vector<std::string_view> encodedChunks;
  Buffer encodingBuffer(*pool_);
  uint32_t keyStreamOffset = 0;

  for (size_t offset = 0; offset < composite.entries.size();) {
    const auto remaining =
        static_cast<uint64_t>(composite.entries.size() - offset);
    const auto numChunkRows = remaining < 2 * maxRows ? remaining : maxRows;

    auto span = std::span<const std::string_view>(
        composite.entries.data() + offset, numChunkRows);

    // Encode chunk using EncodingFactory (Prefix or Trivial).
    const auto encoded = EncodingFactory::encode<std::string_view>(
        createEncodingPolicy(accumulator.config.encodingLayout),
        span,
        encodingBuffer);
    NIMBLE_CHECK(!encoded.empty());

    // Wrap with ChunkedStreamWriter.
    ChunkedStreamWriter chunkWriter{encodingBuffer};
    uint32_t chunkEncodedSize = 0;
    for (auto& contentBuffer : chunkWriter.encode(encoded)) {
      encodedChunks.push_back(std::move(contentBuffer));
      chunkEncodedSize += encodedChunks.back().size();
    }

    // Record chunk metadata.
    const uint32_t accumulatedRows =
        chunkRows.empty() ? numChunkRows : chunkRows.back() + numChunkRows;
    chunkRows.emplace_back(accumulatedRows);
    chunkOffsets.emplace_back(keyStreamOffset);
    chunkKeys.emplace_back(
        extractKey(composite.entries[offset + numChunkRows - 1], idWidth));

    keyStreamOffset += chunkEncodedSize;
    offset += numChunkRows;
  }

  // Write key stream data blob to file via WriteDataFn.
  const auto [fileOffset, fileSize] = writeDataFn(encodedChunks);
  NIMBLE_CHECK_EQ(
      keyStreamOffset,
      fileSize,
      "Key stream offset mismatch: tracked {} vs written {}",
      keyStreamOffset,
      fileSize);

  // Build root SortedIndex FlatBuffer.
  auto chunkRowsVec = builder.CreateVector(chunkRows);
  auto chunkOffsetsVec = builder.CreateVector(chunkOffsets);
  auto chunkKeysVec =
      builder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(
          chunkKeys.size(), [&builder, &chunkKeys](size_t i) {
            return builder.CreateString(
                chunkKeys[i].data(), chunkKeys[i].size());
          });
  auto minKeyVec = builder.CreateString(
      composite.entries.front().data(),
      composite.entries.front().size() - idWidth);

  builder.Finish(
      serialization::CreateSortedIndex(
          builder,
          idWidth,
          chunkRowsVec,
          chunkOffsetsVec,
          chunkKeysVec,
          minKeyVec,
          fileOffset,
          fileSize));
}

void SortedIndexWriter::buildDirectoryFlatBuffer(
    flatbuffers::FlatBufferBuilder& builder,
    const std::vector<MetadataSection>& indexSections) const {
  NIMBLE_CHECK(!accumulators_.empty());
  NIMBLE_CHECK_EQ(indexSections.size(), accumulators_.size());

  std::vector<flatbuffers::Offset<serialization::SortedIndexSection>>
      sectionOffsets;
  sectionOffsets.reserve(accumulators_.size());

  for (size_t i = 0; i < accumulators_.size(); ++i) {
    std::vector<flatbuffers::Offset<flatbuffers::String>> columnOffsets;
    columnOffsets.reserve(accumulators_[i].config.columns.size());
    for (const auto& col : accumulators_[i].config.columns) {
      columnOffsets.emplace_back(builder.CreateString(col));
    }
    auto columnsVec = builder.CreateVector(columnOffsets);

    const auto& section = indexSections[i];
    auto metadataSection = serialization::CreateMetadataSection(
        builder,
        section.offset(),
        section.size(),
        static_cast<serialization::CompressionType>(section.compressionType()));

    sectionOffsets.emplace_back(
        serialization::CreateSortedIndexSection(
            builder, columnsVec, metadataSection));
  }

  auto indicesVec = builder.CreateVector(sectionOffsets);
  builder.Finish(
      serialization::CreateSortedIndexDirectory(builder, indicesVec));
}

void SortedIndexWriter::close(
    const WriteDataFn& writeDataFn,
    const CreateMetadataSectionFn& createMetadataFn,
    const WriteOptionalSectionFn& writeMetadataFn) {
  setClosed();

  if (numRows_ == 0) {
    return;
  }

  std::vector<MetadataSection> indexSections;
  indexSections.reserve(accumulators_.size());
  for (auto& accumulator : accumulators_) {
    flatbuffers::FlatBufferBuilder indexBuilder;
    buildIndex(accumulator, writeDataFn, indexBuilder);
    indexSections.emplace_back(createMetadataFn(asStringView(indexBuilder)));
  }

  flatbuffers::FlatBufferBuilder directoryBuilder;
  buildDirectoryFlatBuffer(directoryBuilder, indexSections);
  writeMetadataFn(
      std::string(kSortedIndexSection), asStringView(directoryBuilder));
}

} // namespace facebook::nimble::index
