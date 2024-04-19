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
#include "dwio/nimble/tablet/Tablet.h"
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/tablet/Compression.h"
#include "dwio/nimble/tablet/FooterGenerated.h"
#include "folly/compression/Compression.h"
#include "folly/io/Cursor.h"

#include <algorithm>
#include <limits>
#include <numeric>
#include <optional>
#include <tuple>
#include <vector>

namespace facebook::nimble {

DEFINE_bool(
    nimble_disable_coalesce,
    false,
    "Disable read coalescing in Nimble reader.");

DEFINE_uint64(
    nimble_coalesce_max_distance,
    1024 * 1024 * 1.25,
    "Maximum read coalescing distance in Nimble reader. And gap smaller than this value will be coalesced.");

// Here's the layout of the tablet:
//
// stripe 1 streams
// stripe 2 streams
// ...
// stripe k streams
// footer
//
// where the footer is a flatbuffer payload, as described here:
// dwio/nimble/tablet/footer.fbs
// followed by fixed payload:
// 4 bytes footer size + 1 byte footer compression type +
// 1 byte checksum type + 8 bytes checksum +
// 2 bytes major version + 2 bytes minor version +
// 4 bytes magic number.
namespace {

constexpr uint16_t kMagicNumber = 0xA1FA;
constexpr uint64_t kInitialFooterSize = 8 * 1024 * 1024; // 8Mb
constexpr uint16_t kVersionMajor = 0;
constexpr uint16_t kVersionMinor = 1;

// Total size of the fields after the flatbuffer.
constexpr uint32_t kPostscriptSize = 20;

// The following fields in postscript are included in checksum calculation.
// 4 bytes footer size + 1 byte compression type
constexpr uint32_t kPostscriptChecksumedSize = 5;

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

template <typename T>
const T* asFlatBuffersRoot(std::string_view content) {
  return flatbuffers::GetRoot<T>(content.data());
}

size_t copyTo(const folly::IOBuf& source, void* target, size_t size) {
  NIMBLE_DASSERT(
      source.computeChainDataLength() <= size, "Target buffer too small.");
  size_t offset = 0;
  for (const auto& chunk : source) {
    std::copy(chunk.begin(), chunk.end(), static_cast<char*>(target) + offset);
    offset += chunk.size();
  }

  return offset;
}

folly::IOBuf
cloneAndCoalesce(const folly::IOBuf& src, size_t offset, size_t size) {
  folly::io::Cursor cursor(&src);
  NIMBLE_ASSERT(cursor.totalLength() >= offset, "Offset out of range");
  cursor.skip(offset);
  NIMBLE_ASSERT(cursor.totalLength() >= size, "Size out of range");
  folly::IOBuf result;
  cursor.clone(result, size);
  result.coalesceWithHeadroomTailroom(0, 0);
  return result;
}

std::string_view toStringView(const folly::IOBuf& buf) {
  return {reinterpret_cast<const char*>(buf.data()), buf.length()};
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

MetadataBuffer::MetadataBuffer(
    velox::memory::MemoryPool& memoryPool,
    std::string_view ref,
    CompressionType type)
    : buffer_{&memoryPool} {
  switch (type) {
    case CompressionType::Uncompressed: {
      buffer_.resize(ref.size());
      std::copy(ref.cbegin(), ref.cend(), buffer_.begin());
      break;
    }
    case CompressionType::Zstd: {
      buffer_ = ZstdCompression::uncompress(memoryPool, ref);
      break;
    }
    default:
      NIMBLE_UNREACHABLE(fmt::format(
          "Unexpected stream compression type: {}", toString(type)));
  }
}

MetadataBuffer::MetadataBuffer(
    velox::memory::MemoryPool& memoryPool,
    const folly::IOBuf& iobuf,
    size_t offset,
    size_t length,
    CompressionType type)
    : buffer_{&memoryPool} {
  switch (type) {
    case CompressionType::Uncompressed: {
      buffer_.resize(length);
      folly::io::Cursor cursor(&iobuf);
      cursor.skip(offset);
      cursor.pull(buffer_.data(), length);
      break;
    }
    case CompressionType::Zstd: {
      auto compressed = cloneAndCoalesce(iobuf, offset, length);
      buffer_ =
          ZstdCompression::uncompress(memoryPool, toStringView(compressed));
      break;
    }
    default:
      NIMBLE_UNREACHABLE(fmt::format(
          "Unexpected stream compression type: {}", toString(type)));
  }
}

MetadataBuffer::MetadataBuffer(
    velox::memory::MemoryPool& memoryPool,
    const folly::IOBuf& iobuf,
    CompressionType type)
    : MetadataBuffer{
          memoryPool,
          iobuf,
          0,
          iobuf.computeChainDataLength(),
          type} {}

void Tablet::StripeGroup::reset(
    uint32_t stripeGroupIndex,
    const MetadataBuffer& stripes,
    uint32_t stripeIndex,
    std::unique_ptr<MetadataBuffer> stripeGroup) {
  index_ = stripeGroupIndex;
  metadata_ = std::move(stripeGroup);
  auto metadataRoot =
      asFlatBuffersRoot<serialization::StripeGroup>(metadata_->content());
  auto stripesRoot =
      asFlatBuffersRoot<serialization::Stripes>(stripes.content());

  auto streamCount = metadataRoot->stream_offsets()->size();
  NIMBLE_ASSERT(
      streamCount == metadataRoot->stream_sizes()->size(),
      "Unexpected stream metadata");

  auto stripeCount = metadataRoot->stripe_count();
  NIMBLE_ASSERT(stripeCount > 0, "Unexpected stripe count");
  streamCount_ = streamCount / stripeCount;

  streamOffsets_ = metadataRoot->stream_offsets()->data();
  streamSizes_ = metadataRoot->stream_sizes()->data();

  // Find the first stripe that use this stripe group
  auto groupIndices = stripesRoot->group_indices()->data();
  while (stripeIndex > 0) {
    if (groupIndices[stripeIndex] != groupIndices[stripeIndex - 1]) {
      break;
    }
    --stripeIndex;
  }
  firstStripe_ = stripeIndex;
}

std::span<const uint32_t> Tablet::StripeGroup::streamOffsets(
    uint32_t stripe) const {
  return {
      streamOffsets_ + (stripe - firstStripe_) * streamCount_, streamCount_};
}

std::span<const uint32_t> Tablet::StripeGroup::streamSizes(
    uint32_t stripe) const {
  return {streamSizes_ + (stripe - firstStripe_) * streamCount_, streamCount_};
}

Postscript Postscript::parse(std::string_view data) {
  NIMBLE_CHECK(data.size() >= kPostscriptSize, "Invalid postscript length");

  Postscript ps;
  // Read and validate magic
  auto pos = data.data() + data.size() - 2;
  const uint16_t magicNumber = *reinterpret_cast<const uint16_t*>(pos);

  NIMBLE_CHECK(
      magicNumber == kMagicNumber,
      "Magic number mismatch. Not an nimble file!");

  // Read and validate versions
  pos -= 4;
  ps.majorVersion_ = *reinterpret_cast<const uint16_t*>(pos);
  ps.minorVersion_ = *reinterpret_cast<const uint16_t*>(pos + 2);

  NIMBLE_CHECK(
      ps.majorVersion_ <= kVersionMajor,
      fmt::format(
          "Unsupported file version. Reader version: {}, file version: {}",
          kVersionMajor,
          ps.majorVersion_));

  pos -= 14;
  ps.footerSize_ = *reinterpret_cast<const uint32_t*>(pos);

  // How CompressionType is written into and read from postscript requires
  // its size must be 1 byte.
  static_assert(sizeof(CompressionType) == 1);
  ps.footerCompressionType_ =
      *reinterpret_cast<const CompressionType*>(pos + 4);

  // How ChecksumType is written into and read from postscript requires
  // its size must be 1 byte.
  static_assert(sizeof(ChecksumType) == 1);
  ps.checksumType_ = *reinterpret_cast<const ChecksumType*>(pos + 5);
  ps.checksum_ = *reinterpret_cast<const uint64_t*>(pos + 6);

  return ps;
}

Tablet::Tablet(
    MemoryPool& memoryPool,
    std::shared_ptr<velox::ReadFile> readFile,
    Postscript postscript,
    std::string_view footer,
    std::string_view stripes,
    std::string_view stripeGroup,
    std::unordered_map<std::string, std::string_view> optionalSections)
    : memoryPool_{memoryPool},
      file_{readFile.get()},
      ownedFile_{std::move(readFile)},
      ps_{std::move(postscript)},
      footer_{std::make_unique<MetadataBuffer>(memoryPool, footer)},
      stripes_{std::make_unique<MetadataBuffer>(memoryPool, stripes)} {
  stripeGroup_.reset(
      /* stripeGroupIndex */ 0,
      *stripes_,
      /* stripeIndex */ 0,
      std::make_unique<MetadataBuffer>(memoryPool, stripeGroup));
  initStripes();
  for (auto& pair : optionalSections) {
    optionalSectionsCache_.insert(
        {pair.first,
         std::make_unique<MetadataBuffer>(memoryPool, pair.second)});
  }
}

Tablet::Tablet(
    MemoryPool& memoryPool,
    std::shared_ptr<velox::ReadFile> readFile,
    const std::vector<std::string>& preloadOptionalSections)
    : Tablet{memoryPool, readFile.get(), preloadOptionalSections} {
  ownedFile_ = std::move(readFile);
}

Tablet::Tablet(
    MemoryPool& memoryPool,
    velox::ReadFile* readFile,
    const std::vector<std::string>& preloadOptionalSections)
    : memoryPool_{memoryPool}, file_{readFile} {
  // We make an initial read of the last piece of the file, and then do
  // another read if our first one didn't cover the whole footer. We could
  // make this a parameter to the constructor later.
  const auto fileSize = file_->size();
  const uint64_t readSize = std::min(kInitialFooterSize, fileSize);

  NIMBLE_CHECK_FILE(
      readSize >= kPostscriptSize, "Corrupted file. Footer is too small.");

  const uint64_t offset = fileSize - readSize;
  velox::common::Region footerRegion{offset, readSize, "footer"};
  folly::IOBuf footerIOBuf;
  file_->preadv({&footerRegion, 1}, {&footerIOBuf, 1});

  {
    folly::IOBuf psIOBuf = cloneAndCoalesce(
        footerIOBuf,
        footerIOBuf.computeChainDataLength() - kPostscriptSize,
        kPostscriptSize);
    ps_ = Postscript::parse(toStringView(psIOBuf));
  }

  NIMBLE_CHECK(
      ps_.footerSize() + kPostscriptSize <= readSize, "Unexpected footer size");
  footer_ = std::make_unique<MetadataBuffer>(
      memoryPool_,
      footerIOBuf,
      footerIOBuf.computeChainDataLength() - kPostscriptSize - ps_.footerSize(),
      ps_.footerSize(),
      ps_.footerCompressionType());

  auto footerRoot =
      asFlatBuffersRoot<serialization::Footer>(footer_->content());

  auto stripes = footerRoot->stripes();
  if (stripes) {
    // For now, assume stripes section will always be within the initial fetch
    NIMBLE_CHECK(
        stripes->offset() + readSize >= fileSize,
        "Incomplete stripes metadata.");
    stripes_ = std::make_unique<MetadataBuffer>(
        memoryPool_,
        footerIOBuf,
        stripes->offset() + readSize - fileSize,
        stripes->size(),
        static_cast<CompressionType>(stripes->compression_type()));
    auto stripesRoot =
        asFlatBuffersRoot<serialization::Stripes>(stripes_->content());

    auto stripeGroups = footerRoot->stripe_groups();
    NIMBLE_CHECK(
        stripeGroups &&
            (stripeGroups->size() ==
             *stripesRoot->group_indices()->rbegin() + 1),
        "Unexpected stripe group count");

    // Always eagerly load if it's the only stripe group and is already
    // fetched
    auto stripeGroup = stripeGroups->Get(0);
    if (stripeGroups->size() == 1 &&
        stripeGroup->offset() + readSize >= fileSize) {
      stripeGroup_.reset(
          /* stripeGroupIndex */ 0,
          *stripes_,
          /* stripeIndex */ 0,
          std::make_unique<MetadataBuffer>(
              memoryPool_,
              footerIOBuf,
              stripeGroup->offset() + readSize - fileSize,
              stripeGroup->size(),
              static_cast<CompressionType>(stripeGroup->compression_type())));
    }
  }

  initStripes();

  auto optionalSections = footerRoot->optional_sections();
  if (optionalSections) {
    NIMBLE_CHECK(
        optionalSections->names() && optionalSections->offsets() &&
            optionalSections->sizes() &&
            optionalSections->compression_types() &&
            optionalSections->names()->size() ==
                optionalSections->offsets()->size() &&
            optionalSections->names()->size() ==
                optionalSections->sizes()->size() &&
            optionalSections->names()->size() ==
                optionalSections->compression_types()->size(),
        "Invalid optional sections metadata.");

    optionalSections_.reserve(optionalSections->names()->size());

    for (auto i = 0; i < optionalSections->names()->size(); ++i) {
      optionalSections_.insert(std::make_pair(
          optionalSections->names()->GetAsString(i)->str(),
          std::make_tuple(
              optionalSections->offsets()->Get(i),
              optionalSections->sizes()->Get(i),
              static_cast<CompressionType>(
                  optionalSections->compression_types()->Get(i)))));
    }
  }

  std::vector<velox::common::Region> mustRead;

  for (const auto& preload : preloadOptionalSections) {
    auto it = optionalSections_.find(preload);
    if (it == optionalSections_.end()) {
      continue;
    }

    const auto sectionOffset = std::get<0>(it->second);
    const auto sectionSize = std::get<1>(it->second);
    const auto sectionCompressionType = std::get<2>(it->second);

    if (sectionOffset < offset) {
      // Section was not read yet. Need to read from file.
      mustRead.emplace_back(sectionOffset, sectionSize, preload);
    } else {
      // Section already loaded from file
      auto metadata = std::make_unique<MetadataBuffer>(
          memoryPool_,
          footerIOBuf,
          sectionOffset - offset,
          sectionSize,
          sectionCompressionType);
      optionalSectionsCache_.insert({preload, std::move(metadata)});
    }
  }
  if (!mustRead.empty()) {
    std::vector<folly::IOBuf> result(mustRead.size());
    file_->preadv(mustRead, {result.data(), result.size()});
    NIMBLE_ASSERT(
        result.size() == mustRead.size(),
        "Region and IOBuf vector sizes don't match");
    for (size_t i = 0; i < result.size(); ++i) {
      auto iobuf = std::move(result[i]);
      const std::string preload{mustRead[i].label};
      auto metadata = std::make_unique<MetadataBuffer>(
          memoryPool_, iobuf, std::get<2>(optionalSections_[preload]));
      optionalSectionsCache_.insert({preload, std::move(metadata)});
    }
  }
}

uint64_t Tablet::calculateChecksum(
    velox::memory::MemoryPool& memoryPool,
    velox::ReadFile* readFile,
    uint64_t chunkSize) {
  auto postscriptStart = readFile->size() - kPostscriptSize;
  Vector<char> postscript(&memoryPool, kPostscriptSize);
  readFile->pread(postscriptStart, kPostscriptSize, postscript.data());
  ChecksumType checksumType = *reinterpret_cast<ChecksumType*>(
      postscript.data() + kPostscriptChecksumedSize);

  auto checksum = ChecksumFactory::create(checksumType);
  Vector<char> buffer(&memoryPool);
  uint64_t sizeToRead =
      readFile->size() - kPostscriptSize + kPostscriptChecksumedSize;
  uint64_t offset = 0;
  while (sizeToRead > 0) {
    auto sizeOneRead = std::min(chunkSize, sizeToRead);
    buffer.resize(sizeOneRead);
    std::string_view bufferRead =
        readFile->pread(offset, sizeOneRead, buffer.data());
    checksum->update(bufferRead);
    sizeToRead -= sizeOneRead;
    offset += sizeOneRead;
  }

  return checksum->getChecksum();
}

namespace {

// LoadTask describes the task of PReading a region and splitting it into the
// streams within (described by StreamTask).

struct StreamTask {
  // The stream index from the input indices
  uint32_t index;

  // Byte offset for the stream, relative to the beginning of the stripe
  uint32_t offset;

  // Size of the stream
  uint32_t length;
};

struct LoadTask {
  // Relative to first byte in file.
  uint64_t readStart;
  uint32_t readLength;
  std::vector<StreamTask> streamTasks;
};

class PreloadedStreamLoader : public StreamLoader {
 public:
  explicit PreloadedStreamLoader(Vector<char>&& stream)
      : stream_{std::move(stream)} {}
  const std::string_view getStream() const override {
    return {stream_.data(), stream_.size()};
  }

 private:
  const Vector<char> stream_;
};

} // namespace

void Tablet::initStripes() {
  auto footerRoot =
      asFlatBuffersRoot<serialization::Footer>(footer_->content());
  tabletRowCount_ = footerRoot->row_count();

  if (stripes_) {
    auto stripesRoot =
        asFlatBuffersRoot<serialization::Stripes>(stripes_->content());

    stripeCount_ = stripesRoot->row_counts()->size();
    NIMBLE_CHECK(stripeCount_ > 0, "Unexpected stripe count");
    NIMBLE_CHECK(
        stripeCount_ == stripesRoot->offsets()->size() &&
            stripeCount_ == stripesRoot->sizes()->size() &&
            stripeCount_ == stripesRoot->group_indices()->size(),
        "Unexpected stripe count");

    stripeRowCounts_ = stripesRoot->row_counts()->data();
    stripeOffsets_ = stripesRoot->offsets()->data();
  }
}

void Tablet::ensureStripeGroup(uint32_t stripe) const {
  auto footerRoot =
      asFlatBuffersRoot<serialization::Footer>(footer_->content());
  auto stripesRoot =
      asFlatBuffersRoot<serialization::Stripes>(stripes_->content());
  auto targetIndex = stripesRoot->group_indices()->Get(stripe);
  if (targetIndex != stripeGroup_.index()) {
    auto stripeGroup = footerRoot->stripe_groups()->Get(targetIndex);
    velox::common::Region stripeGroupRegion{
        stripeGroup->offset(), stripeGroup->size(), "StripeGroup"};
    folly::IOBuf result;
    file_->preadv({&stripeGroupRegion, 1}, {&result, 1});

    stripeGroup_.reset(
        targetIndex,
        *stripes_,
        stripe,
        std::make_unique<MetadataBuffer>(
            memoryPool_,
            result,
            static_cast<CompressionType>(stripeGroup->compression_type())));
  }
}

std::span<const uint32_t> Tablet::streamOffsets(uint32_t stripe) const {
  ensureStripeGroup(stripe);
  return stripeGroup_.streamOffsets(stripe);
}

std::span<const uint32_t> Tablet::streamSizes(uint32_t stripe) const {
  ensureStripeGroup(stripe);
  return stripeGroup_.streamSizes(stripe);
}

uint32_t Tablet::streamCount(uint32_t stripe) const {
  ensureStripeGroup(stripe);
  return stripeGroup_.streamCount();
}

std::vector<std::unique_ptr<StreamLoader>> Tablet::load(
    uint32_t stripe,
    std::span<const uint32_t> streamIdentifiers,
    std::function<std::string_view(uint32_t)> streamLabel) const {
  NIMBLE_CHECK(stripe < stripeCount_, "Stripe is out of range.");

  const uint64_t stripeOffset = this->stripeOffset(stripe);
  ensureStripeGroup(stripe);
  const auto stripeStreamOffsets = stripeGroup_.streamOffsets(stripe);
  const auto stripeStreamSizes = stripeGroup_.streamSizes(stripe);
  const uint32_t streamsToLoad = streamIdentifiers.size();

  std::vector<std::unique_ptr<StreamLoader>> streams(streamsToLoad);
  std::vector<velox::common::Region> regions;
  std::vector<uint32_t> streamIdx;
  regions.reserve(streamsToLoad);
  streamIdx.reserve(streamsToLoad);

  for (uint32_t i = 0; i < streamsToLoad; ++i) {
    const uint32_t streamIdentifier = streamIdentifiers[i];
    if (streamIdentifier >= stripeGroup_.streamCount()) {
      streams[i] = nullptr;
      continue;
    }

    const uint32_t streamSize = stripeStreamSizes[streamIdentifier];
    if (streamSize == 0) {
      streams[i] = nullptr;
      continue;
    }

    const auto streamStart =
        stripeOffset + stripeStreamOffsets[streamIdentifier];
    regions.emplace_back(
        streamStart, streamSize, streamLabel(streamIdentifier));
    streamIdx.push_back(i);
  }
  if (!regions.empty()) {
    std::vector<folly::IOBuf> iobufs(regions.size());
    file_->preadv(regions, {iobufs.data(), iobufs.size()});
    NIMBLE_DASSERT(iobufs.size() == streamIdx.size(), "Buffer size mismatch.");
    for (uint32_t i = 0; i < streamIdx.size(); ++i) {
      const auto size = iobufs[i].computeChainDataLength();
      Vector<char> vector{&memoryPool_, size};
      copyTo(iobufs[i], vector.data(), vector.size());
      streams[streamIdx[i]] =
          std::make_unique<PreloadedStreamLoader>(std::move(vector));
    }
  }

  return streams;
}

std::optional<Section> Tablet::loadOptionalSection(
    const std::string& name,
    bool keepCache) const {
  NIMBLE_CHECK(!name.empty(), "Optional section name cannot be empty.");
  auto itCache = optionalSectionsCache_.find(name);
  if (itCache != optionalSectionsCache_.end()) {
    if (keepCache) {
      return Section{MetadataBuffer{*itCache->second}};
    } else {
      auto metadata = std::move(itCache->second);
      optionalSectionsCache_.erase(itCache);
      return Section{std::move(*metadata)};
    }
  }

  auto it = optionalSections_.find(name);
  if (it == optionalSections_.end()) {
    return std::nullopt;
  }

  const auto offset = std::get<0>(it->second);
  const auto size = std::get<1>(it->second);
  const auto compressionType = std::get<2>(it->second);

  velox::common::Region region{offset, size, name};
  folly::IOBuf iobuf;
  file_->preadv({&region, 1}, {&iobuf, 1});
  return Section{MetadataBuffer{memoryPool_, iobuf, compressionType}};
}
} // namespace facebook::nimble
