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
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/common/Checksum.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/tablet/Compression.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/FooterGenerated.h"
#include "folly/io/Cursor.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <vector>

namespace facebook::nimble {

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
// 2 bytes magic number.
namespace {

template <typename T>
const T* asFlatBuffersRoot(std::string_view content) {
  return flatbuffers::GetRoot<T>(content.data());
}

size_t copyTo(const folly::IOBuf& source, void* target, size_t size) {
  NIMBLE_DCHECK_LE(
      source.computeChainDataLength(), size, "Target buffer too small.");
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
  NIMBLE_CHECK_GE(cursor.totalLength(), offset, "Offset out of range");
  cursor.skip(offset);
  NIMBLE_CHECK_GE(cursor.totalLength(), size, "Size out of range");
  folly::IOBuf result;
  cursor.clone(result, size);
  result.coalesceWithHeadroomTailroom(0, 0);
  return result;
}

std::string_view toStringView(const folly::IOBuf& buf) {
  return {reinterpret_cast<const char*>(buf.data()), buf.length()};
}

} // namespace

StripeGroup::StripeGroup(
    uint32_t stripeGroupIndex,
    const MetadataBuffer& stripes,
    std::unique_ptr<MetadataBuffer> stripeGroup)
    : metadata_{std::move(stripeGroup)}, index_{stripeGroupIndex} {
  const auto* metadataRoot =
      asFlatBuffersRoot<serialization::StripeGroup>(metadata_->content());
  const auto* stripesRoot =
      asFlatBuffersRoot<serialization::Stripes>(stripes.content());

  const auto streamCount = metadataRoot->stream_offsets()->size();
  NIMBLE_CHECK_EQ(
      streamCount,
      metadataRoot->stream_sizes()->size(),
      "Unexpected stream metadata");

  const auto stripeCount = metadataRoot->stripe_count();
  NIMBLE_CHECK_GT(stripeCount, 0, "Unexpected stripe count");
  streamCount_ = streamCount / stripeCount;

  streamOffsets_ = metadataRoot->stream_offsets()->data();
  streamSizes_ = metadataRoot->stream_sizes()->data();

  // Find the first stripe that use this stripe group
  auto* groupIndices = stripesRoot->group_indices()->data();
  for (uint32_t stripeIndex = 0,
                groupIndicesSize = stripesRoot->group_indices()->size();
       stripeIndex < groupIndicesSize;
       ++stripeIndex) {
    if (groupIndices[stripeIndex] == stripeGroupIndex) {
      firstStripe_ = stripeIndex;
      return;
    }
  }
  NIMBLE_UNREACHABLE("No stripe found for stripe group");
}

std::span<const uint32_t> StripeGroup::streamOffsets(uint32_t stripe) const {
  return {
      streamOffsets_ + (stripe - firstStripe_) * streamCount_, streamCount_};
}

std::span<const uint32_t> StripeGroup::streamSizes(uint32_t stripe) const {
  return {streamSizes_ + (stripe - firstStripe_) * streamCount_, streamCount_};
}

Postscript Postscript::parse(std::string_view data) {
  NIMBLE_CHECK_GE(data.size(), kPostscriptSize, "Invalid postscript length");

  Postscript ps;
  // Read and validate magic
  auto pos = data.data() + data.size() - 2;
  const uint16_t magicNumber = *reinterpret_cast<const uint16_t*>(pos);

  NIMBLE_CHECK_EQ(
      magicNumber, kMagicNumber, "Magic number mismatch. Not a nimble file!");

  // Read and validate versions
  pos -= 4;
  ps.majorVersion_ = *reinterpret_cast<const uint16_t*>(pos);
  ps.minorVersion_ = *reinterpret_cast<const uint16_t*>(pos + 2);

  NIMBLE_CHECK_LE(ps.majorVersion_, kVersionMajor, "Unsupported file version");

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
    std::shared_ptr<velox::ReadFile> readFile,
    MemoryPool& pool,
    Postscript postscript,
    std::string_view footer,
    std::string_view stripes,
    std::string_view stripeGroup,
    std::unordered_map<std::string, std::string_view> optionalSections)
    : pool_{&pool},
      file_{readFile.get()},
      ownedFile_{std::move(readFile)},
      ps_{std::move(postscript)},
      footer_{std::make_unique<MetadataBuffer>(pool, footer)},
      stripes_{std::make_unique<MetadataBuffer>(pool, stripes)},
      stripeGroupCache_{[this](uint32_t stripeGroupIndex) {
        return loadStripeGroup(stripeGroupIndex);
      }} {
  auto stripeGroupPtr =
      stripeGroupCache_.get(0, [this, stripeGroup](uint32_t stripeGroupIndex) {
        return std::make_shared<StripeGroup>(
            stripeGroupIndex,
            *stripes_,
            std::make_unique<MetadataBuffer>(*pool_, stripeGroup));
      });
  *firstStripeGroup_.wlock() = std::move(stripeGroupPtr);
  initStripes();
  auto optionalSectionsCacheLock = optionalSectionsCache_.wlock();
  for (auto& pair : optionalSections) {
    optionalSectionsCacheLock->insert(
        {pair.first, std::make_unique<MetadataBuffer>(*pool_, pair.second)});
  }
}

TabletReader::TabletReader(
    velox::ReadFile* readFile,
    std::shared_ptr<velox::ReadFile> ownedReadFile,
    MemoryPool& pool,
    const std::vector<std::string>& preloadOptionalSections)
    : pool_{&pool},
      file_{readFile},
      ownedFile_{std::move(ownedReadFile)},
      stripeGroupCache_{[this](uint32_t stripeGroupIndex) {
        return loadStripeGroup(stripeGroupIndex);
      }} {
  init(preloadOptionalSections);
}

void TabletReader::init(
    const std::vector<std::string>& preloadOptionalSections) {
  // We make an initial read of the last piece of the file, and then do
  // another read if our first one didn't cover the whole footer. We could
  // make this a parameter to the constructor later.
  const auto fileSize = file_->size();
  const uint64_t footerIoSize = std::min(kInitialFooterSize, fileSize);
  NIMBLE_CHECK_FILE(
      footerIoSize >= kPostscriptSize,
      "Corrupted file. Footer {} is smaller than postscript size {}.",
      velox::succinctBytes(footerIoSize),
      velox::succinctBytes(kPostscriptSize));

  const uint64_t footerIoOffset = fileSize - footerIoSize;
  folly::IOBuf footerIoBuf;
  {
    const velox::common::Region footerIoRegion{
        footerIoOffset, footerIoSize, "footer"};
    file_->preadv({&footerIoRegion, 1}, {&footerIoBuf, 1});
  }
  NIMBLE_CHECK_EQ(footerIoSize, footerIoBuf.computeChainDataLength());

  initPostScript(footerIoBuf, footerIoSize);

  initFooter(footerIoBuf, footerIoSize);

  initStripes(footerIoBuf, footerIoSize, fileSize);

  initOptionalSections(footerIoBuf, footerIoOffset, preloadOptionalSections);
}

void TabletReader::initPostScript(
    const folly::IOBuf& footerIoBuf,
    uint64_t footerIoSize) {
  {
    folly::IOBuf psIoBuf = cloneAndCoalesce(
        footerIoBuf,
        footerIoBuf.computeChainDataLength() - kPostscriptSize,
        kPostscriptSize);
    ps_ = Postscript::parse(toStringView(psIoBuf));
  }

  NIMBLE_CHECK_LE(
      ps_.footerSize() + kPostscriptSize,
      footerIoSize,
      "Unexpected footer size: {}, footer IO size: {}, ps size: {}",
      velox::succinctBytes(ps_.footerSize()),
      velox::succinctBytes(footerIoSize),
      velox::succinctBytes(kPostscriptSize));
}

void TabletReader::initFooter(
    const folly::IOBuf& footerIoBuf,
    uint64_t footerIoSize) {
  NIMBLE_CHECK_NULL(footer_);
  footer_ = std::make_unique<MetadataBuffer>(
      *pool_,
      footerIoBuf,
      footerIoSize - kPostscriptSize - ps_.footerSize(),
      ps_.footerSize(),
      ps_.footerCompressionType());
}

void TabletReader::initStripes(
    const folly::IOBuf& footerIoBuf,
    uint64_t footerIoSize,
    uint64_t fileSize) {
  NIMBLE_CHECK_NOT_NULL(footer_);
  auto* footerRoot =
      asFlatBuffersRoot<serialization::Footer>(footer_->content());
  NIMBLE_CHECK_EQ(tabletRowCount_, 0);
  tabletRowCount_ = footerRoot->row_count();

  auto* stripesSection = footerRoot->stripes();
  if (stripesSection == nullptr) {
    NIMBLE_CHECK_EQ(tabletRowCount_, 0);
    return;
  }

  // NOTE: For now, assume stripes section will always be within the initial
  // fetch.
  NIMBLE_CHECK_GE(
      stripesSection->offset() + footerIoSize,
      fileSize,
      "Incomplete stripes metadata.");
  stripes_ = std::make_unique<MetadataBuffer>(
      *pool_,
      footerIoBuf,
      stripesSection->offset() + footerIoSize - fileSize,
      stripesSection->size(),
      static_cast<CompressionType>(stripesSection->compression_type()));

  const auto* stripes =
      asFlatBuffersRoot<serialization::Stripes>(stripes_->content());
  auto* stripeGroups = footerRoot->stripe_groups();
  NIMBLE_CHECK_NOT_NULL(stripeGroups, "Stripe groups is null.");
  NIMBLE_CHECK_EQ(
      stripeGroups->size(),
      *stripes->group_indices()->rbegin() + 1,
      "Unexpected stripe group count");

  // Always eagerly load if it's the only stripe group and is already
  // fetched
  const auto stripeGroup = stripeGroups->Get(0);
  if (stripeGroups->size() == 1 &&
      stripeGroup->offset() + footerIoSize >= fileSize) {
    auto stripeGroupPtr =
        stripeGroupCache_.get(0, [&](uint32_t stripeGroupIndex) {
          return std::make_shared<StripeGroup>(
              stripeGroupIndex,
              *stripes_,
              std::make_unique<MetadataBuffer>(
                  *pool_,
                  footerIoBuf,
                  stripeGroup->offset() + footerIoSize - fileSize,
                  stripeGroup->size(),
                  static_cast<CompressionType>(
                      stripeGroup->compression_type())));
        });
    *firstStripeGroup_.wlock() = std::move(stripeGroupPtr);
  }

  NIMBLE_CHECK_EQ(stripeCount_, 0);
  stripeCount_ = stripes->row_counts()->size();
  NIMBLE_CHECK_GT(stripeCount_, 0, "Unexpected stripe count");
  NIMBLE_CHECK_EQ(
      stripeCount_, stripes->offsets()->size(), "Unexpected stripe count");
  NIMBLE_CHECK_EQ(
      stripeCount_, stripes->sizes()->size(), "Unexpected stripe count");
  NIMBLE_CHECK_EQ(
      stripeCount_,
      stripes->group_indices()->size(),
      "Unexpected stripe count");

  stripeRowCounts_ = stripes->row_counts()->data();
  stripeOffsets_ = stripes->offsets()->data();
}

void TabletReader::initStripes() {
  const auto* footerRoot =
      asFlatBuffersRoot<serialization::Footer>(footer_->content());
  NIMBLE_CHECK_EQ(tabletRowCount_, 0);
  tabletRowCount_ = footerRoot->row_count();
  if (stripes_ == nullptr) {
    NIMBLE_CHECK_EQ(tabletRowCount_, 0);
    return;
  }

  const auto* stripes =
      asFlatBuffersRoot<serialization::Stripes>(stripes_->content());

  stripeCount_ = stripes->row_counts()->size();
  NIMBLE_CHECK_GT(stripeCount_, 0, "Unexpected stripe count");
  NIMBLE_CHECK_EQ(
      stripeCount_, stripes->offsets()->size(), "Unexpected stripe count");
  NIMBLE_CHECK_EQ(
      stripeCount_, stripes->sizes()->size(), "Unexpected stripe count");
  NIMBLE_CHECK_EQ(
      stripeCount_,
      stripes->group_indices()->size(),
      "Unexpected stripe count");
  stripeRowCounts_ = stripes->row_counts()->data();
  stripeOffsets_ = stripes->offsets()->data();
}

void TabletReader::initOptionalSections(
    const folly::IOBuf& footerIoBuf,
    uint64_t footerIoOffset,
    const std::vector<std::string>& preloadOptionalSections) {
  auto footerRoot =
      asFlatBuffersRoot<serialization::Footer>(footer_->content());

  auto* optionalSections = footerRoot->optional_sections();
  if (optionalSections == nullptr) {
    return;
  }

  NIMBLE_CHECK(optionalSections->names(), "Optional sections names is null.");
  NIMBLE_CHECK(
      optionalSections->offsets(), "Optional sections offsets is null.");
  NIMBLE_CHECK(optionalSections->sizes(), "Optional sections sizes is null.");
  NIMBLE_CHECK(
      optionalSections->compression_types(),
      "Optional sections compression_types is null.");
  NIMBLE_CHECK_EQ(
      optionalSections->names()->size(),
      optionalSections->offsets()->size(),
      "Optional sections names and offsets size mismatch.");
  NIMBLE_CHECK_EQ(
      optionalSections->names()->size(),
      optionalSections->sizes()->size(),
      "Optional sections names and sizes size mismatch.");
  NIMBLE_CHECK_EQ(
      optionalSections->names()->size(),
      optionalSections->compression_types()->size(),
      "Optional sections names and compression_types size mismatch.");

  optionalSections_.reserve(optionalSections->names()->size());

  for (auto i = 0; i < optionalSections->names()->size(); ++i) {
    optionalSections_.insert(
        std::make_pair(
            optionalSections->names()->GetAsString(i)->str(),
            MetadataSection{
                optionalSections->offsets()->Get(i),
                optionalSections->sizes()->Get(i),
                static_cast<CompressionType>(
                    optionalSections->compression_types()->Get(i))}));
  }

  std::vector<velox::common::Region> mustRead;
  for (const auto& preload : preloadOptionalSections) {
    auto it = optionalSections_.find(preload);
    if (it == optionalSections_.end()) {
      continue;
    }

    const auto sectionOffset = it->second.offset();
    const auto sectionSize = it->second.size();
    const auto sectionCompressionType = it->second.compressionType();

    if (sectionOffset < footerIoOffset) {
      // Section was not read yet. Need to read from file.
      mustRead.emplace_back(sectionOffset, sectionSize, preload);
    } else {
      // Section already loaded from file
      auto metadata = std::make_unique<MetadataBuffer>(
          *pool_,
          footerIoBuf,
          sectionOffset - footerIoOffset,
          sectionSize,
          sectionCompressionType);
      optionalSectionsCache_.wlock()->insert({preload, std::move(metadata)});
    }
  }

  if (mustRead.empty()) {
    return;
  }
  std::vector<folly::IOBuf> result(mustRead.size());
  file_->preadv(mustRead, {result.data(), result.size()});
  NIMBLE_CHECK_EQ(
      result.size(),
      mustRead.size(),
      "Region and IOBuf vector sizes don't match");
  for (size_t i = 0; i < result.size(); ++i) {
    auto iobuf = std::move(result[i]);
    const std::string preload{mustRead[i].label};
    auto metadata = std::make_unique<MetadataBuffer>(
        *pool_, iobuf, optionalSections_.at(preload).compressionType());
    optionalSectionsCache_.wlock()->insert({preload, std::move(metadata)});
  }
}

std::shared_ptr<TabletReader> TabletReader::create(
    velox::ReadFile* readFile,
    MemoryPool& pool,
    const std::vector<std::string>& preloadOptionalSections) {
  return std::shared_ptr<TabletReader>(
      new TabletReader(readFile, nullptr, pool, preloadOptionalSections));
}

std::shared_ptr<TabletReader> TabletReader::create(
    std::shared_ptr<velox::ReadFile> readFile,
    MemoryPool& pool,
    const std::vector<std::string>& preloadOptionalSections) {
  return std::shared_ptr<TabletReader>(new TabletReader(
      readFile.get(), readFile, pool, preloadOptionalSections));
}

std::shared_ptr<TabletReader> TabletReader::testingCreate(
    std::shared_ptr<velox::ReadFile> readFile,
    MemoryPool& pool,
    Postscript postscript,
    std::string_view footer,
    std::string_view stripes,
    std::string_view stripeGroup,
    std::unordered_map<std::string, std::string_view> optionalSections) {
  return std::shared_ptr<TabletReader>(new TabletReader(
      std::move(readFile),
      pool,
      std::move(postscript),
      footer,
      stripes,
      stripeGroup,
      std::move(optionalSections)));
}

uint64_t TabletReader::calculateChecksum(
    velox::memory::MemoryPool& pool,
    velox::ReadFile* readFile,
    uint64_t chunkSize) {
  const auto postscriptStart = readFile->size() - kPostscriptSize;
  Vector<char> postscript(&pool, kPostscriptSize);
  readFile->pread(postscriptStart, kPostscriptSize, postscript.data());
  const ChecksumType checksumType = *reinterpret_cast<ChecksumType*>(
      postscript.data() + kPostscriptChecksumedSize);

  const auto checksum = ChecksumFactory::create(checksumType);
  Vector<char> buffer(&pool);
  uint64_t sizeToRead =
      readFile->size() - kPostscriptSize + kPostscriptChecksumedSize;
  uint64_t offset{0};
  while (sizeToRead > 0) {
    const auto sizeOneRead = std::min(chunkSize, sizeToRead);
    buffer.resize(sizeOneRead);
    const std::string_view bufferRead =
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

struct RegionHash {
  size_t operator()(const velox::common::Region& region) const {
    return folly::hash::hash_combine(
        std::hash<size_t>{}(region.offset), std::hash<size_t>{}(region.length));
  }
};

struct RegionEqual {
  bool operator()(
      const velox::common::Region& lhs,
      const velox::common::Region& rhs) const {
    return lhs.offset == rhs.offset && lhs.length == rhs.length;
  }
};

} // namespace

uint32_t TabletReader::stripeGroupIndex(uint32_t stripeIndex) const {
  const auto* stripesRoot =
      asFlatBuffersRoot<serialization::Stripes>(stripes_->content());
  return stripesRoot->group_indices()->Get(stripeIndex);
}

std::shared_ptr<StripeGroup> TabletReader::loadStripeGroup(
    uint32_t stripeGroupIndex) const {
  auto* footerRoot =
      asFlatBuffersRoot<serialization::Footer>(footer_->content());
  auto stripeGroupInfo = footerRoot->stripe_groups()->Get(stripeGroupIndex);
  velox::common::Region stripeGroupRegion{
      stripeGroupInfo->offset(), stripeGroupInfo->size(), "StripeGroup"};
  folly::IOBuf buffer;
  file_->preadv({&stripeGroupRegion, 1}, {&buffer, 1});

  // Reset the first stripe group that was loaded when we load another one
  firstStripeGroup_.wlock()->reset();

  return std::make_shared<StripeGroup>(
      stripeGroupIndex,
      *stripes_,
      std::make_unique<MetadataBuffer>(
          *pool_,
          buffer,
          static_cast<CompressionType>(stripeGroupInfo->compression_type())));
}

std::shared_ptr<StripeGroup> TabletReader::stripeGroup(
    uint32_t stripeGroupIndex) const {
  return stripeGroupCache_.get(stripeGroupIndex);
}

std::span<const uint32_t> TabletReader::streamOffsets(
    const StripeIdentifier& stripe) const {
  NIMBLE_DCHECK_LT(stripe.stripeId(), stripeCount_, "Stripe is out of range.");
  return stripe.stripeGroup()->streamOffsets(stripe.stripeId());
}

std::span<const uint32_t> TabletReader::streamSizes(
    const StripeIdentifier& stripe) const {
  NIMBLE_DCHECK_LT(stripe.stripeId(), stripeCount_, "Stripe is out of range.");
  return stripe.stripeGroup()->streamSizes(stripe.stripeId());
}

uint32_t TabletReader::streamCount(const StripeIdentifier& stripe) const {
  NIMBLE_DCHECK_LT(stripe.stripeId(), stripeCount_, "Stripe is out of range.");
  return stripe.stripeGroup()->streamCount();
}

StripeIdentifier TabletReader::stripeIdentifier(uint32_t stripeIndex) const {
  NIMBLE_CHECK_LT(stripeIndex, stripeCount_, "Stripe is out of range.");
  return StripeIdentifier{
      stripeIndex, stripeGroup(stripeGroupIndex(stripeIndex))};
}

std::vector<std::unique_ptr<StreamLoader>> TabletReader::load(
    const StripeIdentifier& stripe,
    std::span<const uint32_t> streamIdentifiers,
    std::function<std::string_view(uint32_t)> streamLabel) const {
  NIMBLE_CHECK_LT(stripe.stripeId(), stripeCount_, "Stripe is out of range.");

  const uint64_t stripeOffset = this->stripeOffset(stripe.stripeId());
  const auto& stripeGroup = stripe.stripeGroup();
  const auto stripeStreamOffsets =
      stripeGroup->streamOffsets(stripe.stripeId());
  const auto stripeStreamSizes = stripeGroup->streamSizes(stripe.stripeId());
  const uint32_t streamsToLoad = streamIdentifiers.size();

  std::vector<std::unique_ptr<StreamLoader>> streams(streamsToLoad);
  folly::F14FastMap<
      velox::common::Region,
      std::vector<uint32_t>,
      RegionHash,
      RegionEqual>
      regionToStreamIndices;
  std::vector<velox::common::Region> uniqueRegions;
  regionToStreamIndices.reserve(streamsToLoad);
  uniqueRegions.reserve(streamsToLoad);

  for (uint32_t i = 0; i < streamsToLoad; ++i) {
    const uint32_t streamIdentifier = streamIdentifiers[i];
    if (streamIdentifier >= stripeGroup->streamCount()) {
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

    auto it = regionToStreamIndices.emplace(
        velox::common::Region{streamStart, streamSize},
        std::vector<uint32_t>{});
    if (it.second) {
      uniqueRegions.emplace_back(
          streamStart, streamSize, streamLabel(streamIdentifier));
    }
    it.first->second.push_back(i);
  }
  if (!uniqueRegions.empty()) {
    std::vector<folly::IOBuf> iobufs(uniqueRegions.size());
    file_->preadv(uniqueRegions, {iobufs.data(), iobufs.size()});
    NIMBLE_DCHECK_EQ(
        iobufs.size(), uniqueRegions.size(), "Buffer size mismatch.");
    for (uint32_t i = 0; i < uniqueRegions.size(); ++i) {
      // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
      const auto size = iobufs[i].computeChainDataLength();
      const auto& streamIndices = regionToStreamIndices[uniqueRegions[i]];
      for (uint32_t streamIndex : streamIndices) {
        Vector<char> vector{pool_, size};
        copyTo(iobufs[i], vector.data(), vector.size());
        streams[streamIndex] =
            std::make_unique<PreloadedStreamLoader>(std::move(vector));
      }
    }
  }

  return streams;
}

uint64_t TabletReader::totalStreamSize(
    const StripeIdentifier& stripe,
    std::span<const uint32_t> streamIdentifiers) const {
  NIMBLE_CHECK_LT(stripe.stripeId(), stripeCount_, "Stripe is out of range.");
  const auto& stripeGroup = stripe.stripeGroup();

  uint64_t streamSizeSum{0};
  const auto stripeStreamSizes = stripeGroup->streamSizes(stripe.stripeId());
  for (const auto streamId : streamIdentifiers) {
    if (streamId >= stripeGroup->streamCount()) {
      continue;
    }
    streamSizeSum += stripeStreamSizes[streamId];
  }
  return streamSizeSum;
}

std::optional<MetadataSection> TabletReader::stripesMetadata() const {
  auto* footerRoot =
      asFlatBuffersRoot<serialization::Footer>(footer_->content());
  auto* stripes = footerRoot->stripes();
  if (stripes == nullptr) {
    return std::nullopt;
  }
  return MetadataSection{
      stripes->offset(),
      stripes->size(),
      static_cast<CompressionType>(stripes->compression_type())};
}

std::vector<MetadataSection> TabletReader::stripeGroupsMetadata() const {
  std::vector<MetadataSection> groupsMetadata;
  auto* footerRoot =
      asFlatBuffersRoot<serialization::Footer>(footer_->content());
  auto* stripeGroups = footerRoot->stripe_groups();
  if (stripeGroups == nullptr) {
    return groupsMetadata;
  }
  groupsMetadata.reserve(stripeGroups->size());
  std::transform(
      stripeGroups->cbegin(),
      stripeGroups->cend(),
      std::back_inserter(groupsMetadata),
      [](const auto& stripeGroup) {
        return MetadataSection{
            stripeGroup->offset(),
            stripeGroup->size(),
            static_cast<CompressionType>(stripeGroup->compression_type())};
      });
  return groupsMetadata;
}

bool TabletReader::hasOptionalSection(const std::string& name) const {
  const auto it = optionalSections_.find(name);
  return it != optionalSections_.end();
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

  const auto offset = it->second.offset();
  const auto size = it->second.size();
  const auto compressionType = it->second.compressionType();

  velox::common::Region region{offset, size, name};
  folly::IOBuf iobuf;
  file_->preadv({&region, 1}, {&iobuf, 1});
  return Section{MetadataBuffer{*pool_, iobuf, compressionType}};
}
} // namespace facebook::nimble
