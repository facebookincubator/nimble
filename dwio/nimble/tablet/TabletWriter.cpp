/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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
#include "dwio/nimble/tablet/FooterGenerated.h"
#include "dwio/nimble/velox/MetadataGenerated.h"

namespace facebook::nimble {
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
    NIMBLE_ASSERT(
        items.size() <= streamCount,
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

void TabletWriter::close() {
  auto stripeCount = stripeOffsets_.size();
  NIMBLE_ASSERT(
      stripeCount == stripeSizes_.size() &&
          stripeCount == stripeGroupIndices_.size() &&
          stripeCount == rowCounts_.size(),
      "Stripe count mismatch.");

  const uint64_t totalRows =
      std::accumulate(rowCounts_.begin(), rowCounts_.end(), uint64_t{0});

  flatbuffers::FlatBufferBuilder builder(kInitialFooterSize);

  // write remaining stripe groups
  tryWriteStripeGroup(true);

  // write stripes
  MetadataSection stripes;
  if (stripeCount > 0) {
    flatbuffers::FlatBufferBuilder stripesBuilder(kInitialFooterSize);
    stripesBuilder.Finish(serialization::CreateStripes(
        stripesBuilder,
        stripesBuilder.CreateVector(rowCounts_),
        stripesBuilder.CreateVector(stripeOffsets_),
        stripesBuilder.CreateVector(stripeSizes_),
        stripesBuilder.CreateVector(stripeGroupIndices_)));
    stripes = createMetadataSection(asView(stripesBuilder));
  }

  auto createOptionalMetadataSection =
      [](flatbuffers::FlatBufferBuilder& builder,
         const std::vector<
             std::pair<std::string, TabletWriter::MetadataSection>>&
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
                  return optionalSections[i].second.offset;
                }),
            builder.CreateVector<uint32_t>(
                optionalSections.size(),
                [&optionalSections](size_t i) {
                  return optionalSections[i].second.size;
                }),
            builder.CreateVector<uint8_t>(
                optionalSections.size(), [&optionalSections](size_t i) {
                  return static_cast<uint8_t>(
                      optionalSections[i].second.compressionType);
                }));
      };

  // write footer
  builder.Finish(serialization::CreateFooter(
      builder,
      totalRows,
      stripeCount > 0 ? serialization::CreateMetadataSection(
                            builder,
                            stripes.offset,
                            stripes.size,
                            static_cast<serialization::CompressionType>(
                                stripes.compressionType))
                      : 0,
      !stripeGroups_.empty()
          ? builder.CreateVector<
                flatbuffers::Offset<serialization::MetadataSection>>(
                stripeGroups_.size(),
                [this, &builder](size_t i) {
                  return serialization::CreateMetadataSection(
                      builder,
                      stripeGroups_[i].offset,
                      stripeGroups_[i].size,
                      static_cast<serialization::CompressionType>(
                          stripeGroups_[i].compressionType));
                })
          : 0,
      !optionalSections_.empty()
          ? createOptionalMetadataSection(
                builder, {optionalSections_.begin(), optionalSections_.end()})
          : 0));

  auto footerStart = file_->size();
  auto footerCompressionType = writeMetadata(asView(builder));

  // End with the fixed length constants.
  const uint64_t footerSize64Bit = (file_->size() - footerStart);
  NIMBLE_ASSERT(
      footerSize64Bit <= std::numeric_limits<uint32_t>::max(),
      fmt::format("Footer size too big: {}.", footerSize64Bit));
  const uint32_t footerSize = footerSize64Bit;
  writeWithChecksum({reinterpret_cast<const char*>(&footerSize), 4});
  writeWithChecksum({reinterpret_cast<const char*>(&footerCompressionType), 1});
  uint64_t checksum = checksum_->getChecksum();
  file_->append({reinterpret_cast<const char*>(&options_.checksumType), 1});
  file_->append({reinterpret_cast<const char*>(&checksum), 8});
  file_->append({reinterpret_cast<const char*>(&kVersionMajor), 2});
  file_->append({reinterpret_cast<const char*>(&kVersionMinor), 2});
  file_->append({reinterpret_cast<const char*>(&kMagicNumber), 2});
}

void TabletWriter::writeStripe(uint32_t rowCount, std::vector<Stream> streams) {
  if (UNLIKELY(rowCount == 0)) {
    return;
  }

  rowCounts_.push_back(rowCount);
  stripeOffsets_.push_back(file_->size());

  auto streamCount = streams.empty()
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

  if (options_.layoutPlanner) {
    streams = options_.layoutPlanner->getLayout(std::move(streams));
  }

  for (const auto& stream : streams) {
    const uint32_t index = stream.offset;

    // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
    stripeStreamOffsets[index] = file_->size() - stripeOffsets_.back();

    for (auto output : stream.content) {
      writeWithChecksum(output);
    }

    // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
    stripeStreamSizes[index] =
        file_->size() - (stripeStreamOffsets[index] + stripeOffsets_.back());
  }

  stripeSizes_.push_back(file_->size() - stripeOffsets_.back());
  stripeGroupIndices_.push_back(stripeGroupIndex_);

  // Write stripe group if size of column offsets/sizes is too large.
  tryWriteStripeGroup();
}

CompressionType TabletWriter::writeMetadata(std::string_view metadata) {
  auto size = metadata.size();
  bool shouldCompress = size > options_.metadataCompressionThreshold;
  CompressionType compressionType = CompressionType::Uncompressed;
  std::optional<Vector<char>> compressed;
  if (shouldCompress) {
    compressed = ZstdCompression::compress(memoryPool_, metadata);
    if (compressed.has_value()) {
      compressionType = CompressionType::Zstd;
      metadata = {compressed->data(), compressed->size()};
    }
  }
  writeWithChecksum(metadata);
  return compressionType;
}

TabletWriter::MetadataSection TabletWriter::createMetadataSection(
    std::string_view metadata) {
  auto offset = file_->size();
  auto compressionType = writeMetadata(metadata);
  auto size = static_cast<uint32_t>(file_->size() - offset);
  return MetadataSection{
      .offset = offset, .size = size, .compressionType = compressionType};
}

void TabletWriter::writeOptionalSection(
    std::string name,
    std::string_view content) {
  NIMBLE_CHECK(!name.empty(), "Optional section name cannot be empty.");
  NIMBLE_CHECK(
      optionalSections_.find(name) == optionalSections_.end(),
      fmt::format("Optional section '{}' already exists.", name));
  optionalSections_.try_emplace(
      std::move(name), createMetadataSection(content));
}

void TabletWriter::tryWriteStripeGroup(bool force) {
  auto stripeCount = streamOffsets_.size();
  NIMBLE_ASSERT(stripeCount == streamSizes_.size(), "Stripe count mismatch.");
  if (stripeCount == 0) {
    return;
  }

  // Estimate size
  // 8 bytes for offsets, 4 for size, 1 for compression type, so 13.
  size_t estimatedSize = 4 + stripeCount * streamOffsets_.back().size() * 13;
  if (!force && (estimatedSize < options_.metadataFlushThreshold)) {
    return;
  }

  auto maxStreamCountIt = std::max_element(
      streamOffsets_.begin(),
      streamOffsets_.end(),
      [](const auto& a, const auto& b) { return a.size() < b.size(); });

  auto streamCount =
      maxStreamCountIt == streamOffsets_.end() ? 0 : maxStreamCountIt->size();

  // Each stripe may have different stream count recorded.
  // We need to pad shorter stripes to the full length of stream count. All
  // these is handled by the |createFlattenedVector| function.
  flatbuffers::FlatBufferBuilder builder(kInitialFooterSize);
  auto streamOffsets = createFlattenedVector<uint32_t, uint32_t>(
      builder, streamCount, streamOffsets_, 0);
  auto streamSizes = createFlattenedVector<uint32_t, uint32_t>(
      builder, streamCount, streamSizes_, 0);

  builder.Finish(serialization::CreateStripeGroup(
      builder, stripeCount, streamOffsets, streamSizes));

  stripeGroups_.push_back(createMetadataSection(asView(builder)));
  ++stripeGroupIndex_;

  streamOffsets_.clear();
  streamSizes_.clear();
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

} // namespace facebook::nimble
