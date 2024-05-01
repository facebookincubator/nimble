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
#include "dwio/nimble/tablet/TabletReader.h"

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/tablet/Compression.h"
#include "dwio/nimble/tablet/Constants.h"
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

void TabletReader::StripeGroup::reset(
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

std::span<const uint32_t> TabletReader::StripeGroup::streamOffsets(
    uint32_t stripe) const {
  return {
      streamOffsets_ + (stripe - firstStripe_) * streamCount_, streamCount_};
}

std::span<const uint32_t> TabletReader::StripeGroup::streamSizes(
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

TabletReader::TabletReader(
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
  auto optionalSectionsCacheLock = optionalSectionsCache_.wlock();
  for (auto& pair : optionalSections) {
    optionalSectionsCacheLock->insert(
        {pair.first,
         std::make_unique<MetadataBuffer>(memoryPool, pair.second)});
  }
}

TabletReader::TabletReader(
    MemoryPool& memoryPool,
    std::shared_ptr<velox::ReadFile> readFile,
    const std::vector<std::string>& preloadOptionalSections)
    : TabletReader{memoryPool, readFile.get(), preloadOptionalSections} {
  ownedFile_ = std::move(readFile);
}

TabletReader::TabletReader(
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
      optionalSectionsCache_.wlock()->insert({preload, std::move(metadata)});
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
      optionalSectionsCache_.wlock()->insert({preload, std::move(metadata)});
    }
  }
}

uint64_t TabletReader::calculateChecksum(
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

void TabletReader::initStripes() {
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

void TabletReader::ensureStripeGroup(uint32_t stripe) const {
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

std::span<const uint32_t> TabletReader::streamOffsets(uint32_t stripe) const {
  ensureStripeGroup(stripe);
  return stripeGroup_.streamOffsets(stripe);
}

std::span<const uint32_t> TabletReader::streamSizes(uint32_t stripe) const {
  ensureStripeGroup(stripe);
  return stripeGroup_.streamSizes(stripe);
}

uint32_t TabletReader::streamCount(uint32_t stripe) const {
  ensureStripeGroup(stripe);
  return stripeGroup_.streamCount();
}

std::vector<std::unique_ptr<StreamLoader>> TabletReader::load(
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

std::optional<Section> TabletReader::loadOptionalSection(
    const std::string& name,
    bool keepCache) const {
  NIMBLE_CHECK(!name.empty(), "Optional section name cannot be empty.");
  {
    auto optionalSectionsCache = optionalSectionsCache_.wlock();
    auto itCache = optionalSectionsCache->find(name);
    if (itCache != optionalSectionsCache->end()) {
      if (keepCache) {
        return Section{MetadataBuffer{*itCache->second}};
      } else {
        auto metadata = std::move(itCache->second);
        optionalSectionsCache->erase(itCache);
        return Section{std::move(*metadata)};
      }
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
