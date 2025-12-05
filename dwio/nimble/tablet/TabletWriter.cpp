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

#include "dwio/nimble/tablet/TabletWriter.h"

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/tablet/Compression.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/IndexGenerated.h"

namespace facebook::nimble {

std::unique_ptr<TabletWriter> TabletWriter::create(
    velox::WriteFile* file,
    velox::memory::MemoryPool& pool,
    TabletWriter::Options options) {
  return std::unique_ptr<TabletWriter>(
      new TabletWriter(file, pool, std::move(options)));
}

TabletWriter::TabletWriter(
    velox::WriteFile* file,
    velox::memory::MemoryPool& pool,
    TabletWriter::Options options)
    : file_{file},
      pool_(&pool),
      options_(std::move(options)),
      checksum_{ChecksumFactory::create(options_.checksumType)} {}

namespace {
template <typename Source, typename Target = Source>
flatbuffers::Offset<flatbuffers::Vector<Target>> createFlattenedVector(
    flatbuffers::FlatBufferBuilder& builder,
    size_t streamCount,
    const std::vector<std::vector<Source>>& source,
    Target defaultValue) {
  // This method is performing the following:
  // 1. Converts the source vector into FlatBuffers representation. The flat
  // buffer representation is a single dimension array, so this method flattens
  // the source vectors into this single dimension array.
  // 2. Source vector contains a child vector per stripe. Each one of those
  // child vectors might contain different entry count than the final number of
  // streams in the tablet. This is because each stripe might have
  // encountered just subset of the streams. Therefore, this method fills up all
  // missing offsets in the target vector with the provided default value.

  auto stripeCount = source.size();
  std::vector<Target> target(stripeCount * streamCount);
  size_t targetIndex = 0;

  for (auto& items : source) {
    NIMBLE_CHECK_LE(
        items.size(),
        streamCount,
        "Corrupted footer. Stream count exceeds expected total number of streams.");

    for (auto i = 0; i < streamCount; ++i) {
      target[targetIndex++] = i < items.size()
          ? static_cast<Target>(items[i])
          : static_cast<Target>(defaultValue);
    }
  }

  return builder.CreateVector<Target>(target);
}

std::string_view asView(const flatbuffers::FlatBufferBuilder& builder) {
  return {
      reinterpret_cast<const char*>(builder.GetBufferPointer()),
      builder.GetSize()};
}
} // namespace

void TabletWriter::writeIndex() {
  if (!hasIndex() || index_.stripeGroupIndexes.empty()) {
    return;
  }

  flatbuffers::FlatBufferBuilder builder(kInitialFooterSize);

  // Create stripe keys vector
  auto stripeKeysVector =
      builder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(
          index_.stripeKeys.size(), [&builder, this](size_t i) {
            return builder.CreateString(
                index_.stripeKeys[i].data(), index_.stripeKeys[i].size());
          });

  // Create index columns vector from index config
  auto indexColumnsVector =
      builder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(
          options_.indexConfig->columns.size(), [&builder, this](size_t i) {
            return builder.CreateString(options_.indexConfig->columns[i]);
          });

  // Create stripe group indices vector
  auto stripeGroupIndicesVector = builder.CreateVector(stripeGroupIndices_);

  // Create stripe index groups metadata sections
  auto stripeIndexGroupsVector =
      builder.CreateVector<flatbuffers::Offset<serialization::MetadataSection>>(
          index_.stripeGroupIndexes.size(), [&builder, this](size_t i) {
            return serialization::CreateMetadataSection(
                builder,
                index_.stripeGroupIndexes[i].offset(),
                index_.stripeGroupIndexes[i].size(),
                static_cast<serialization::CompressionType>(
                    index_.stripeGroupIndexes[i].compressionType()));
          });

  builder.Finish(
      serialization::CreateIndex(
          builder,
          stripeKeysVector,
          indexColumnsVector,
          stripeGroupIndicesVector,
          stripeIndexGroupsVector));
  writeOptionalSection(std::string(kIndexSection), asView(builder));
}

void TabletWriter::close() {
  const auto stripeCount = stripeOffsets_.size();
  NIMBLE_CHECK(
      stripeCount == stripeSizes_.size() &&
          stripeCount == stripeGroupIndices_.size() &&
          stripeCount == stripeRowCounts_.size(),
      "Stripe count mismatch.");

  // write remaining stripe groups
  tryWriteStripeGroup(true);

  // write key index
  writeIndex();

  // write stripes
  MetadataSection stripes = writeStripes(stripeCount);

  const uint64_t totalRows = std::accumulate(
      stripeRowCounts_.begin(), stripeRowCounts_.end(), uint64_t{0});

  flatbuffers::FlatBufferBuilder builder(kInitialFooterSize);
  // write footer
  builder.Finish(
      serialization::CreateFooter(
          builder,
          totalRows,
          stripeCount > 0 ? serialization::CreateMetadataSection(
                                builder,
                                stripes.offset(),
                                stripes.size(),
                                static_cast<serialization::CompressionType>(
                                    stripes.compressionType()))
                          : 0,
          !stripeGroups_.empty()
              ? builder.CreateVector<
                    flatbuffers::Offset<serialization::MetadataSection>>(
                    stripeGroups_.size(),
                    [this, &builder](size_t i) {
                      return serialization::CreateMetadataSection(
                          builder,
                          stripeGroups_[i].offset(),
                          stripeGroups_[i].size(),
                          static_cast<serialization::CompressionType>(
                              stripeGroups_[i].compressionType()));
                    })
              : 0,
          !optionalSections_.empty()
              ? createOptionalMetadataSection(
                    builder,
                    {optionalSections_.begin(), optionalSections_.end()})
              : 0));

  auto footerStart = file_->size();
  auto footerCompressionType = writeMetadata(asView(builder));

  // End with the fixed length constants.
  const uint64_t footerSize64Bit = (file_->size() - footerStart);
  NIMBLE_CHECK_LE(
      footerSize64Bit,
      std::numeric_limits<uint32_t>::max(),
      "Footer size too big.");
  const uint32_t footerSize = footerSize64Bit;
  writeWithChecksum({reinterpret_cast<const char*>(&footerSize), 4});
  writeWithChecksum({reinterpret_cast<const char*>(&footerCompressionType), 1});
  uint64_t checksum = checksum_->getChecksum();
  file_->append({reinterpret_cast<const char*>(&options_.checksumType), 1});
  file_->append({reinterpret_cast<const char*>(&checksum), 8});
  file_->append({reinterpret_cast<const char*>(&kVersionMajor), 2});
  file_->append({reinterpret_cast<const char*>(&kVersionMinor), 2});
  file_->append({reinterpret_cast<const char*>(&kMagicNumber), 2});
  indexEncodingBuffer_.reset();
}

void TabletWriter::writeStripe(
    uint32_t rowCount,
    std::vector<Stream> streams,
    std::optional<Stream> keyStream) {
  if (UNLIKELY(rowCount == 0)) {
    return;
  }

  stripeRowCounts_.push_back(rowCount);
  stripeOffsets_.push_back(file_->size());

  const auto streamCount = streams.empty()
      ? 0
      : std::max_element(
            streams.begin(),
            streams.end(),
            [](const auto& a, const auto& b) {
              return a.offset < b.offset;
            })->offset +
          1;

  auto& stripeStreamOffsets = streamOffsets_.emplace_back(streamCount, 0);
  auto& stripeStreamSizes = streamSizes_.emplace_back(streamCount, 0);

  ensureIndexWrite(streamCount);

  addStripeIndexKey(keyStream);

  if (options_.layoutPlanner != nullptr) {
    streams = options_.layoutPlanner->getLayout(std::move(streams));
  }

  const auto fileOffset = file_->size();
  NIMBLE_CHECK_EQ(fileOffset, stripeOffsets_.back());
  for (const auto& stream : streams) {
    const uint32_t streamIndex = stream.offset;
    // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
    stripeStreamOffsets[streamIndex] = file_->size() - stripeOffsets_.back();
    for (const auto& chunk : stream.chunks) {
      const auto chunkOffset = file_->size() - stripeOffsets_.back();
      writeChunkWithChecksum(chunk);
      addStreamChunkIndex(streamIndex, chunk, chunkOffset);
    }
    // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
    stripeStreamSizes[streamIndex] = file_->size() -
        (stripeStreamOffsets[streamIndex] + stripeOffsets_.back());
  }
  stripeSizes_.push_back(file_->size() - stripeOffsets_.back());

  writeKeyStream(keyStream);

  stripeGroupIndices_.push_back(stripeGroupIndex_);
  // Write stripe group if size of column offsets/sizes is too large.
  tryWriteStripeGroup();
}

CompressionType TabletWriter::writeMetadata(std::string_view metadata) {
  const auto size = metadata.size();
  const bool shouldCompress = size > options_.metadataCompressionThreshold;
  CompressionType compressionType{CompressionType::Uncompressed};
  std::optional<Vector<char>> compressed;
  if (shouldCompress) {
    compressed = ZstdCompression::compress(*pool_, metadata);
    if (compressed.has_value()) {
      compressionType = CompressionType::Zstd;
      metadata = {compressed->data(), compressed->size()};
    }
  }
  writeWithChecksum(metadata);
  return compressionType;
}

MetadataSection TabletWriter::createMetadataSection(std::string_view metadata) {
  auto offset = file_->size();
  auto compressionType = writeMetadata(metadata);
  auto size = static_cast<uint32_t>(file_->size() - offset);
  return MetadataSection{offset, size, compressionType};
}

void TabletWriter::ensureIndexWrite(size_t streamCount) {
  if (!hasIndex()) {
    return;
  }
  if (indexEncodingBuffer_ == nullptr) {
    indexEncodingBuffer_ = std::make_unique<Buffer>(*pool_);
  }
  if (streamIndexEncodingBuffer_ == nullptr) {
    streamIndexEncodingBuffer_ = std::make_unique<Buffer>(*pool_);
  }
  // Initialize vectors for current stripe if this is the first stripe or a new
  // stripe group
  if (streamIndex_ == nullptr) {
    streamIndex_ = std::make_unique<StreamChunkIndex>();
    streamIndex_->streamChunkRows.resize(streamCount);
    streamIndex_->streamChunkOffsets.resize(streamCount);
  }
  streamIndex_->streamChunkCounts.emplace_back(streamCount, 0);
  streamIndex_->keyStreamChunkCounts.emplace_back(0);
}

void TabletWriter::addStripeIndexKey(const std::optional<Stream>& keyStream) {
  NIMBLE_CHECK_EQ(hasIndex(), keyStream.has_value());
  if (!hasIndex()) {
    return;
  }
  NIMBLE_CHECK(!keyStream->chunks.empty());
  if (index_.stripeKeys.empty()) {
    index_.stripeKeys.push_back(indexEncodingBuffer_->writeString(
        keyStream->chunks[0].firstKey.value()));
  }
  index_.stripeKeys.push_back(indexEncodingBuffer_->writeString(
      keyStream->chunks.back().lastKey.value()));
}

void TabletWriter::writeKeyStream(const std::optional<Stream>& keyStream) {
  NIMBLE_CHECK_EQ(hasIndex(), keyStream.has_value());
  if (!hasIndex()) {
    return;
  }

  const auto keyStreamOffset = file_->size();
  for (const auto& chunk : keyStream->chunks) {
    const auto chunkOffset = file_->size();
    writeChunkWithChecksum(chunk);

    // Track key stream chunk index
    streamIndex_->keyStreamChunkRows.emplace_back(chunk.numRows);
    streamIndex_->keyStreamChunkOffsets.emplace_back(chunkOffset);
    NIMBLE_CHECK(chunk.firstKey.has_value());
    streamIndex_->keyStreamChunkKeys.emplace_back(
        streamIndexEncodingBuffer_->writeString(chunk.lastKey.value()));
    ++streamIndex_->keyStreamChunkCounts.back();
  }

  const auto keyStreamSize = file_->size() - keyStreamOffset;
  streamIndex_->keyStreamOffsets.emplace_back(keyStreamOffset);
  streamIndex_->keyStreamSizes.emplace_back(keyStreamSize);
}

void TabletWriter::addStreamChunkIndex(
    uint32_t streamIndex,
    const Chunk& chunk,
    uint32_t chunkOffset) {
  if (!hasIndex()) {
    return;
  }
  streamIndex_->streamChunkRows[streamIndex].emplace_back(chunk.numRows);
  streamIndex_->streamChunkOffsets[streamIndex].emplace_back(chunkOffset);
  ++streamIndex_->streamChunkCounts.back()[streamIndex];
}

void TabletWriter::writeOptionalSection(
    std::string name,
    std::string_view content) {
  NIMBLE_CHECK(!name.empty(), "Optional section name cannot be empty.");
  NIMBLE_CHECK(
      optionalSections_.find(name) == optionalSections_.end(),
      "Optional section '{}' already exists.",
      name);
  optionalSections_.try_emplace(
      std::move(name), createMetadataSection(content));
}

bool TabletWriter::shouldWriteStripeGroup(bool force) const {
  const auto stripeCount = streamOffsets_.size();
  if (stripeCount == 0) {
    return false;
  }

  // Estimate size
  // 8 bytes for offsets, 4 for size, 1 for compression type, so 13.
  const size_t estimatedSize =
      4 + stripeCount * streamOffsets_.back().size() * 13;
  if (!force && (estimatedSize < options_.metadataFlushThreshold)) {
    return false;
  }
  return true;
}

void TabletWriter::writeStripeGroup(size_t streamCount, size_t stripeCount) {
  flatbuffers::FlatBufferBuilder builder(kInitialFooterSize);
  auto streamOffsets = createFlattenedVector<uint32_t, uint32_t>(
      builder, streamCount, streamOffsets_, 0);
  auto streamSizes = createFlattenedVector<uint32_t, uint32_t>(
      builder, streamCount, streamSizes_, 0);

  builder.Finish(
      serialization::CreateStripeGroup(
          builder, stripeCount, streamOffsets, streamSizes));
  stripeGroups_.emplace_back(createMetadataSection(asView(builder)));
}

void TabletWriter::writeStripeGroupIndex(size_t streamCount) {
  if (!hasIndex()) {
    return;
  }
  NIMBLE_CHECK_NOT_NULL(streamIndex_);

  const size_t stripeCount = streamSizes_.size();

  flatbuffers::FlatBufferBuilder indexBuilder(kInitialIndexSize);

  // Create StripeValueIndex with key stream data
  auto valueIndex = serialization::CreateStripeValueIndex(
      indexBuilder,
      indexBuilder.CreateVector(streamIndex_->keyStreamOffsets),
      indexBuilder.CreateVector(streamIndex_->keyStreamSizes),
      indexBuilder.CreateVector(streamIndex_->keyStreamChunkCounts),
      indexBuilder.CreateVector(streamIndex_->keyStreamChunkRows),
      indexBuilder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(
          streamIndex_->keyStreamChunkKeys.size(),
          [&indexBuilder, this](size_t i) {
            return indexBuilder.CreateString(
                streamIndex_->keyStreamChunkKeys[i].data(),
                streamIndex_->keyStreamChunkKeys[i].size());
          }));

  // Create StripePositionIndex with flattened stream chunk data
  // Flatten stream_chunks: for each stripe, list chunk counts for all streams
  std::vector<uint32_t> flattenedStreamChunks;
  for (const auto& chunkCounts : streamIndex_->streamChunkCounts) {
    for (size_t streamId = 0; streamId < streamCount; ++streamId) {
      flattenedStreamChunks.push_back(
          streamId < chunkCounts.size() ? chunkCounts[streamId] : 0);
    }
  }

  // Flatten stream_chunk_rows: concatenate all chunk rows from all streams
  std::vector<uint32_t> flattenedChunkRows;
  for (const auto& rows : streamIndex_->streamChunkRows) {
    flattenedChunkRows.insert(
        flattenedChunkRows.end(), rows.begin(), rows.end());
  }

  // Flatten stream_chunk_offsets: concatenate all chunk offsets from all
  // streams
  std::vector<uint32_t> flattenedChunkOffsets;
  for (const auto& offsets : streamIndex_->streamChunkOffsets) {
    flattenedChunkOffsets.insert(
        flattenedChunkOffsets.end(), offsets.begin(), offsets.end());
  }

  auto positionIndex = serialization::CreateStripePositionIndex(
      indexBuilder,
      indexBuilder.CreateVector(flattenedStreamChunks),
      indexBuilder.CreateVector(flattenedChunkRows),
      indexBuilder.CreateVector(flattenedChunkOffsets));

  // Create StripeIndexGroup with both value and position indexes
  indexBuilder.Finish(
      serialization::CreateStripeIndexGroup(
          indexBuilder, stripeCount, valueIndex, positionIndex));

  index_.stripeGroupIndexes.push_back(
      createMetadataSection(asView(indexBuilder)));
}

MetadataSection TabletWriter::writeStripes(size_t stripeCount) {
  if (stripeCount == 0) {
    return {};
  }
  flatbuffers::FlatBufferBuilder stripesBuilder(kInitialFooterSize);
  stripesBuilder.Finish(
      serialization::CreateStripes(
          stripesBuilder,
          stripesBuilder.CreateVector(stripeRowCounts_),
          stripesBuilder.CreateVector(stripeOffsets_),
          stripesBuilder.CreateVector(stripeSizes_),
          stripesBuilder.CreateVector(stripeGroupIndices_)));
  return createMetadataSection(asView(stripesBuilder));
}

flatbuffers::Offset<serialization::OptionalMetadataSections>
TabletWriter::createOptionalMetadataSection(
    flatbuffers::FlatBufferBuilder& builder,
    const std::vector<std::pair<std::string, MetadataSection>>&
        optionalSections) {
  return serialization::CreateOptionalMetadataSections(
      builder,
      builder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(
          optionalSections.size(),
          [&builder, &optionalSections](size_t i) {
            return builder.CreateString(optionalSections[i].first);
          }),
      builder.CreateVector<uint64_t>(
          optionalSections.size(),
          [&optionalSections](size_t i) {
            return optionalSections[i].second.offset();
          }),
      builder.CreateVector<uint32_t>(
          optionalSections.size(),
          [&optionalSections](size_t i) {
            return optionalSections[i].second.size();
          }),
      builder.CreateVector<uint8_t>(
          optionalSections.size(), [&optionalSections](size_t i) {
            return static_cast<uint8_t>(
                optionalSections[i].second.compressionType());
          }));
}

void TabletWriter::tryWriteStripeGroup(bool force) {
  if (!shouldWriteStripeGroup(force)) {
    return;
  }

  const auto stripeCount = streamOffsets_.size();
  NIMBLE_CHECK_GT(stripeCount, 0);

  const auto maxStreamCountIt = std::max_element(
      streamOffsets_.begin(),
      streamOffsets_.end(),
      [](const auto& a, const auto& b) { return a.size() < b.size(); });
  NIMBLE_CHECK(maxStreamCountIt != streamOffsets_.end());

  const auto streamCount = maxStreamCountIt->size();

  // Each stripe may have different stream count recorded.
  // We need to pad shorter stripes to the full length of stream count. All
  // these is handled by the |createFlattenedVector| function.
  writeStripeGroup(streamCount, stripeCount);

  writeStripeGroupIndex(streamCount);

  finishStripeGroup();
}

void TabletWriter::finishStripeGroup() {
  streamOffsets_.clear();
  streamSizes_.clear();
  // Reset stream chunk index data for the stripe group
  streamIndex_.reset();
  streamIndexEncodingBuffer_.reset();
  ++stripeGroupIndex_;
}

void TabletWriter::writeWithChecksum(std::string_view data) {
  file_->append(data);
  checksum_->update(data);
}

void TabletWriter::writeWithChecksum(const folly::IOBuf& buf) {
  for (auto buffer : buf) {
    writeWithChecksum(
        {reinterpret_cast<const char*>(buffer.data()), buffer.size()});
  }
}

void TabletWriter::writeChunkWithChecksum(const Chunk& chunk) {
  NIMBLE_CHECK(!chunk.content.empty());
  for (const auto& content : chunk.content) {
    writeWithChecksum(content);
  }
}
} // namespace facebook::nimble
