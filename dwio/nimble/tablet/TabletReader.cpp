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
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/FooterGenerated.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/SeekableInputStream.h"

#include "folly/io/Cursor.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <vector>

namespace facebook::nimble {

TabletReader::Options TabletReader::configureOptions(
    const velox::dwio::common::ReaderOptions& options,
    velox::dwio::common::BufferedInput* bufferedInput) {
  Options tabletOptions;
  tabletOptions.maxFooterIoBytes = options.footerSpeculativeIoSize();
  tabletOptions.preloadOptionalSections = {
      std::string(kSchemaSection), std::string(kVectorizedStatsSection)};
  if (options.fileMetadataCacheEnabled() && bufferedInput != nullptr) {
    tabletOptions.bufferedInput = bufferedInput;
  }
  return tabletOptions;
}

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

inline const serialization::Footer* footerRoot(const MetadataBuffer& footer) {
  return asFlatBuffersRoot<serialization::Footer>(footer.content());
}

inline const serialization::Stripes* stripesRoot(
    const MetadataBuffer& stripes) {
  return asFlatBuffersRoot<serialization::Stripes>(stripes.content());
}

inline void validateOptionalSections(
    const serialization::OptionalMetadataSections* sections) {
  NIMBLE_CHECK_NOT_NULL(sections, "Optional sections is null.");
  NIMBLE_CHECK(sections->names(), "Optional sections names is null.");
  NIMBLE_CHECK(sections->offsets(), "Optional sections offsets is null.");
  NIMBLE_CHECK(sections->sizes(), "Optional sections sizes is null.");
  NIMBLE_CHECK(
      sections->compression_types(),
      "Optional sections compression_types is null.");
  NIMBLE_CHECK_EQ(
      sections->names()->size(),
      sections->offsets()->size(),
      "Optional sections names and offsets size mismatch.");
  NIMBLE_CHECK_EQ(
      sections->names()->size(),
      sections->sizes()->size(),
      "Optional sections names and sizes size mismatch.");
  NIMBLE_CHECK_EQ(
      sections->names()->size(),
      sections->compression_types()->size(),
      "Optional sections names and compression_types size mismatch.");
}

// Converts a FlatBuffers section (with offset/size/compression_type fields)
// to a MetadataSection.
template <typename T>
MetadataSection toMetadataSection(const T* fbSection) {
  return MetadataSection{
      fbSection->offset(),
      fbSection->size(),
      static_cast<CompressionType>(fbSection->compression_type())};
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
  const auto* stripesParsed = stripesRoot(stripes);

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
  auto* groupIndices = stripesParsed->group_indices()->data();
  for (uint32_t stripeIndex = 0,
                groupIndicesSize = stripesParsed->group_indices()->size();
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

Postscript::Postscript(
    uint32_t footerSize,
    CompressionType footerCompressionType,
    ChecksumType checksumType,
    uint32_t majorVersion,
    uint32_t minorVersion)
    : footerSize_(footerSize),
      footerCompressionType_(footerCompressionType),
      checksum_(0),
      checksumType_(checksumType),
      majorVersion_(majorVersion),
      minorVersion_(minorVersion) {}

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
      bufferedInput_{nullptr},
      ps_{std::move(postscript)},
      footer_{std::make_unique<MetadataBuffer>(pool, footer)},
      stripes_{std::make_unique<MetadataBuffer>(pool, stripes)},
      stripeGroupCache_{[this](uint32_t stripeGroupIndex) {
        return loadStripeGroup(stripeGroupIndex);
      }},
      indexGroupCache_{[this](uint32_t stripeGroupIndex) {
        return loadIndexGroup(stripeGroupIndex);
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
    const Options& options)
    : pool_{&pool},
      file_{readFile},
      ownedFile_{std::move(ownedReadFile)},
      bufferedInput_{options.bufferedInput},
      stripeGroupCache_{[this](uint32_t stripeGroupIndex) {
        return loadStripeGroup(stripeGroupIndex);
      }},
      indexGroupCache_{[this](uint32_t stripeGroupIndex) {
        return loadIndexGroup(stripeGroupIndex);
      }} {
  init(options);
}

void TabletReader::init(const Options& options) {
  fileSize_ = file_->size();
  NIMBLE_CHECK_FILE(
      fileSize_ >= kPostscriptSize,
      "Corrupted file. File size {} is smaller than postscript size {}.",
      velox::succinctBytes(fileSize_),
      velox::succinctBytes(kPostscriptSize));

  if (tryInitFromCache(options)) {
    return;
  }

  uint64_t footerIoSize;
  uint64_t footerIoOffset;
  folly::IOBuf footerIoBuf;

  loadAndInitFooter(
      options.maxFooterIoBytes, footerIoBuf, footerIoSize, footerIoOffset);
  loadStripes(footerIoBuf, footerIoSize, footerIoOffset);
  initStripes();
  if (stripeCount_ > 0) {
    preloadStripeGroup(footerIoBuf);
  }

  initOptionalSections();
  preloadOptionalSections(
      options, makeSectionLoader(footerIoBuf, footerIoOffset));

  initIndex();
  preloadIndexGroup(footerIoBuf);

  cacheMetadata(footerIoBuf, footerIoOffset);
}

void TabletReader::loadAndInitFooter(
    uint64_t maxFooterIoBytes,
    folly::IOBuf& footerIoBuf,
    uint64_t& footerIoSize,
    uint64_t& footerIoOffset) {
  footerIoBuf = folly::IOBuf();
  if (maxFooterIoBytes == 0) {
    // Adaptive mode: read postscript first, then exact footer size.
    // First read: just the postscript (last 20 bytes)
    {
      const velox::common::Region psRegion{
          fileSize_ - kPostscriptSize, kPostscriptSize, "postscript"};
      folly::IOBuf psIoBuf;
      file_->preadv({&psRegion, 1}, {&psIoBuf, 1});
      ps_ = Postscript::parse(toStringView(psIoBuf));
    }

    // Second read: exact footer + postscript
    footerIoSize = ps_.footerSize() + kPostscriptSize;
    footerIoOffset = fileSize_ - footerIoSize;
    {
      const velox::common::Region footerIoRegion{
          footerIoOffset, footerIoSize, "footer"};
      file_->preadv({&footerIoRegion, 1}, {&footerIoBuf, 1});
    }
    NIMBLE_CHECK_EQ(footerIoSize, footerIoBuf.computeChainDataLength());

    initFooter(footerIoBuf, footerIoSize);
  } else {
    // Speculative mode: read maxFooterIoBytes (or fileSize_ if smaller)
    footerIoSize = std::min(maxFooterIoBytes, fileSize_);
    footerIoOffset = fileSize_ - footerIoSize;
    {
      const velox::common::Region footerIoRegion{
          footerIoOffset, footerIoSize, "footer"};
      file_->preadv({&footerIoRegion, 1}, {&footerIoBuf, 1});
    }
    NIMBLE_CHECK_EQ(footerIoSize, footerIoBuf.computeChainDataLength());

    initPostScript(footerIoBuf, footerIoSize);

    // If initial read didn't cover the full footer, do a second read.
    const uint64_t requiredSize = ps_.footerSize() + kPostscriptSize;
    if (requiredSize > footerIoSize) {
      footerIoSize = requiredSize;
      footerIoOffset = fileSize_ - footerIoSize;
      const velox::common::Region footerIoRegion{
          footerIoOffset, footerIoSize, "footer"};
      file_->preadv({&footerIoRegion, 1}, {&footerIoBuf, 1});
      NIMBLE_CHECK_EQ(footerIoSize, footerIoBuf.computeChainDataLength());
    }

    initFooter(footerIoBuf, footerIoSize);
  }
}

void TabletReader::initPostScript(
    const folly::IOBuf& footerIoBuf,
    uint64_t footerIoSize) {
  NIMBLE_CHECK_GE(
      footerIoSize,
      kPostscriptSize,
      "Footer IO size {} must be at least postscript size {}",
      velox::succinctBytes(footerIoSize),
      velox::succinctBytes(kPostscriptSize));

  folly::IOBuf psIoBuf = cloneAndCoalesce(
      footerIoBuf,
      footerIoBuf.computeChainDataLength() - kPostscriptSize,
      kPostscriptSize);
  ps_ = Postscript::parse(toStringView(psIoBuf));
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

void TabletReader::loadStripes(
    folly::IOBuf& footerIoBuf,
    uint64_t& footerIoSize,
    uint64_t& footerIoOffset) {
  NIMBLE_CHECK_NOT_NULL(footer_);
  NIMBLE_CHECK_NULL(stripes_);

  const auto* footer = footerRoot(*footer_);
  auto* stripesSection = footer->stripes();
  if (stripesSection == nullptr) {
    NIMBLE_CHECK_EQ(footer->row_count(), 0);
    return;
  }

  // Compute the lowest offset we need in the buffer: stripes section and
  // index section (if present, may be at a lower offset than stripes).
  uint64_t requiredOffset = stripesSection->offset();
  auto indexIt = optionalSections_.find(std::string{kIndexSection});
  if (indexIt != optionalSections_.end()) {
    requiredOffset = std::min(requiredOffset, indexIt->second.offset());
  }
  const uint64_t requiredSize = fileSize_ - requiredOffset;
  if (requiredSize > footerIoSize) {
    footerIoSize = requiredSize;
    footerIoOffset = fileSize_ - footerIoSize;
    const velox::common::Region footerIoRegion{
        footerIoOffset, footerIoSize, "footer+stripes"};
    file_->preadv({&footerIoRegion, 1}, {&footerIoBuf, 1});
    NIMBLE_CHECK_EQ(footerIoSize, footerIoBuf.computeChainDataLength());
  }
  stripes_ = std::make_unique<MetadataBuffer>(
      *pool_,
      footerIoBuf,
      stripesSection->offset() + footerIoSize - fileSize_,
      stripesSection->size(),
      static_cast<CompressionType>(stripesSection->compression_type()));
}

void TabletReader::initStripes() {
  const auto* footer = footerRoot(*footer_);
  NIMBLE_CHECK_EQ(tabletRowCount_, 0);
  tabletRowCount_ = footer->row_count();
  if (stripes_ == nullptr) {
    NIMBLE_CHECK_EQ(tabletRowCount_, 0);
    return;
  }

  const auto* stripes = stripesRoot(*stripes_);

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

void TabletReader::initOptionalSections() {
  const auto* footer = footerRoot(*footer_);

  auto* optionalSections = footer->optional_sections();
  if (optionalSections == nullptr) {
    return;
  }

  validateOptionalSections(optionalSections);

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
}

std::vector<std::string> TabletReader::preloadSectionNames(
    const Options& options) const {
  auto names = options.preloadOptionalSections;
  if (hasIndex()) {
    names.emplace_back(kIndexSection);
  }
  return names;
}

void TabletReader::preloadOptionalSections(
    const Options& options,
    const SectionLoader& loader) {
  for (const auto& name : preloadSectionNames(options)) {
    auto it = optionalSections_.find(name);
    if (it == optionalSections_.end()) {
      continue;
    }
    optionalSectionsCache_.wlock()->insert({name, loader(it->second)});
  }
}

TabletReader::SectionLoader TabletReader::makeSectionLoader(
    const folly::IOBuf& footerIoBuf,
    uint64_t footerIoOffset) const {
  return
      [this, &footerIoBuf, footerIoOffset](
          const MetadataSection& section) -> std::unique_ptr<MetadataBuffer> {
        if (section.offset() >= footerIoOffset) {
          return std::make_unique<MetadataBuffer>(
              *pool_,
              footerIoBuf,
              section.offset() - footerIoOffset,
              section.size(),
              section.compressionType());
        }
        return readMetadata(section, velox::dwio::common::LogType::FILE);
      };
}

std::shared_ptr<TabletReader> TabletReader::create(
    velox::ReadFile* readFile,
    MemoryPool* pool,
    const Options& options) {
  NIMBLE_CHECK_NOT_NULL(pool);
  return std::shared_ptr<TabletReader>(
      new TabletReader(readFile, nullptr, *pool, options));
}

std::shared_ptr<TabletReader> TabletReader::create(
    std::shared_ptr<velox::ReadFile> readFile,
    MemoryPool* pool,
    const Options& options) {
  NIMBLE_CHECK_NOT_NULL(pool);
  return std::shared_ptr<TabletReader>(
      new TabletReader(readFile.get(), readFile, *pool, options));
}

std::shared_ptr<TabletReader> TabletReader::testingCreate(
    std::shared_ptr<velox::ReadFile> readFile,
    MemoryPool* pool,
    Postscript postscript,
    std::string_view footer,
    std::string_view stripes,
    std::string_view stripeGroup,
    std::unordered_map<std::string, std::string_view> optionalSections) {
  NIMBLE_CHECK_NOT_NULL(pool);
  return std::shared_ptr<TabletReader>(new TabletReader(
      std::move(readFile),
      *pool,
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
  return stripesRoot(*stripes_)->group_indices()->Get(stripeIndex);
}

bool TabletReader::hasCache() const {
  return bufferedInput_ != nullptr && bufferedInput_->hasCache();
}

bool TabletReader::tryLoadAndInitFooterFromCache() {
  // Try to read footer+PS from cache at synthetic offset fileSize_.
  // fileSize_ is a well-known key computable without any file IO.
  auto cached = bufferedInput_->findCachedRegion(fileSize_);
  if (!cached.has_value()) {
    return false;
  }

  // Warm path: parse PS + footer from cached data, zero file IO.
  // Data layout: [footer | PS] (file's natural byte order).
  const auto size = cached->size();
  NIMBLE_CHECK_GE(size, kPostscriptSize, "Cached footer+PS entry too small");

  // PS is only 20 bytes at the tail. The last range always covers it since
  // cache pages are at least 4KB.
  const auto& ranges = cached->ranges();
  const auto& lastRange = ranges.back();
  NIMBLE_CHECK_GE(
      lastRange.size(),
      kPostscriptSize,
      "Last cache range too small to cover postscript");
  ps_ = Postscript::parse(
      std::string_view{
          lastRange.data() + lastRange.size() - kPostscriptSize,
          kPostscriptSize});
  NIMBLE_CHECK_EQ(
      ps_.footerSize() + kPostscriptSize,
      size,
      "Cached footer+PS size mismatch");

  // Build footer MetadataBuffer from an IOBuf chain wrapping the cached
  // ranges (zero-copy). MetadataBuffer copies from the IOBuf cursor
  // internally — no intermediate contiguous copy needed.
  footer_ = std::make_unique<MetadataBuffer>(
      *pool_,
      cached->toIOBuf(),
      0,
      ps_.footerSize(),
      ps_.footerCompressionType());
  return true;
}

bool TabletReader::tryInitFromCache(const Options& options) {
  if (!hasCache()) {
    return false;
  }

  if (!tryLoadAndInitFooterFromCache()) {
    return false;
  }
  initOptionalSections();

  loadStripesAndSections(options);

  initStripes();
  initIndex();
  return true;
}

void TabletReader::loadStripesAndSections(const Options& options) {
  // Enqueue all needed reads, then load once for coalesced IO.
  auto enqueuedStripes = enqueueStripesSection();
  if (!enqueuedStripes.has_value()) {
    return;
  }
  auto enqueuedSections = enqueueOptionalSections(preloadSectionNames(options));

  // Single load — BufferedInput coalesces adjacent regions.
  bufferedInput_->load(velox::dwio::common::LogType::FOOTER);

  loadEnqueuedOptionalSections(std::move(enqueuedSections));

  stripes_ = readMetadata(
      std::move(enqueuedStripes->stream), enqueuedStripes->section);
}

std::unique_ptr<MetadataBuffer> TabletReader::readMetadata(
    const MetadataSection& section,
    velox::dwio::common::LogType logType) const {
  if (bufferedInput_ != nullptr) {
    return readMetadata(
        bufferedInput_->read(section.offset(), section.size(), logType),
        section);
  }

  // Direct file read path.
  folly::IOBuf buffer;
  velox::common::Region region{section.offset(), section.size(), "metadata"};
  file_->preadv({&region, 1}, {&buffer, 1});
  return std::make_unique<MetadataBuffer>(
      *pool_, buffer, section.compressionType());
}

std::unique_ptr<MetadataBuffer> TabletReader::readMetadata(
    std::unique_ptr<velox::dwio::common::SeekableInputStream> stream,
    const MetadataSection& section) const {
  const void* data{nullptr};
  int32_t size{0};
  // Try zero-copy: get contiguous data from stream.
  if (stream->Next(&data, &size) &&
      static_cast<uint64_t>(size) >= section.size()) {
    return std::make_unique<MetadataBuffer>(
        *pool_,
        std::string_view{static_cast<const char*>(data), section.size()},
        section.compressionType());
  }

  // Data is fragmented — copy into IOBuf.
  if (size > 0) {
    stream->BackUp(size);
  }
  folly::IOBuf buffer(folly::IOBuf::CREATE, section.size());
  stream->readFully(
      reinterpret_cast<char*>(buffer.writableData()), section.size());
  buffer.append(section.size());
  return std::make_unique<MetadataBuffer>(
      *pool_,
      std::string_view{
          reinterpret_cast<const char*>(buffer.data()), buffer.length()},
      section.compressionType());
}

std::shared_ptr<StripeGroup> TabletReader::loadStripeGroup(
    uint32_t stripeGroupIndex) const {
  const auto* footer = footerRoot(*footer_);
  auto stripeGroupInfo = footer->stripe_groups()->Get(stripeGroupIndex);

  // Reset the first stripe group that was loaded when we load another one.
  firstStripeGroup_.wlock()->reset();

  const auto section = toMetadataSection(stripeGroupInfo);
  return std::make_shared<StripeGroup>(
      stripeGroupIndex,
      *stripes_,
      readMetadata(section, velox::dwio::common::LogType::GROUP));
}

std::shared_ptr<StripeGroup> TabletReader::stripeGroup(
    uint32_t stripeGroupIndex) const {
  return stripeGroupCache_.get(stripeGroupIndex);
}

std::shared_ptr<StripeIndexGroup> TabletReader::indexGroup(
    uint32_t stripeGroupIndex) const {
  return indexGroupCache_.get(stripeGroupIndex);
}

std::shared_ptr<StripeIndexGroup> TabletReader::loadIndexGroup(
    uint32_t stripeGroupIndex) const {
  NIMBLE_CHECK_NOT_NULL(tabletIndex_, "Index not initialized.");

  // Reset the first index group that was loaded when we load another one.
  firstIndexGroup_.wlock()->reset();

  const auto section = tabletIndex_->groupIndexMetadata(stripeGroupIndex);
  return StripeIndexGroup::create(
      stripeGroupIndex,
      tabletIndex_->rootIndex(),
      readMetadata(section, velox::dwio::common::LogType::GROUP_INDEX));
}

std::pair<std::shared_ptr<StripeGroup>, std::shared_ptr<StripeIndexGroup>>
TabletReader::loadStripeAndIndexGroup(uint32_t stripeGroupIndex) const {
  NIMBLE_CHECK_NOT_NULL(tabletIndex_, "Index not initialized.");

  const auto* footer = footerRoot(*footer_);
  const auto indexSection = tabletIndex_->groupIndexMetadata(stripeGroupIndex);
  const auto groupSection =
      toMetadataSection(footer->stripe_groups()->Get(stripeGroupIndex));

  // Reset strong references when loading different groups.
  firstStripeGroup_.wlock()->reset();
  firstIndexGroup_.wlock()->reset();

  std::unique_ptr<MetadataBuffer> groupMetadata;
  std::unique_ptr<MetadataBuffer> indexMetadata;

  if (bufferedInput_ != nullptr) {
    // Use enqueue/load pattern for coalesced IO. BufferedInput will coalesce
    // adjacent regions into a single read when possible.
    auto groupStream = bufferedInput_->enqueue(
        {groupSection.offset(), groupSection.size(), "stripe_group"});
    auto indexStream = bufferedInput_->enqueue(
        {indexSection.offset(), indexSection.size(), "index_group"});

    // Single load() call - BufferedInput coalesces adjacent regions.
    bufferedInput_->load(velox::dwio::common::LogType::GROUP);

    groupMetadata = readMetadata(std::move(groupStream), groupSection);
    indexMetadata = readMetadata(std::move(indexStream), indexSection);
  } else {
    // Fall back to separate direct file reads.
    groupMetadata =
        readMetadata(groupSection, velox::dwio::common::LogType::GROUP);
    indexMetadata =
        readMetadata(indexSection, velox::dwio::common::LogType::GROUP_INDEX);
  }

  auto stripeGroup = std::make_shared<StripeGroup>(
      stripeGroupIndex, *stripes_, std::move(groupMetadata));
  auto indexGroup = StripeIndexGroup::create(
      stripeGroupIndex, tabletIndex_->rootIndex(), std::move(indexMetadata));
  return {std::move(stripeGroup), std::move(indexGroup)};
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

StripeIdentifier TabletReader::stripeIdentifier(
    uint32_t stripeIndex,
    bool loadIndex) const {
  NIMBLE_CHECK_LT(stripeIndex, stripeCount_, "Stripe is out of range.");
  const auto groupIndex = this->stripeGroupIndex(stripeIndex);

  if (!loadIndex) {
    // Index not requested - load stripe group only.
    return StripeIdentifier{
        stripeIndex, stripeGroup(groupIndex), /*indexGroup=*/nullptr};
  }

  // Index requested - ensure it exists.
  NIMBLE_CHECK_NOT_NULL(
      tabletIndex_,
      "Index loading requested but file has no index. Check index() before requesting index loading.");

  // Both stripe group and index group are needed. Check if either is already
  // cached to avoid unnecessary coalesced loading.
  const bool stripeGroupCached = stripeGroupCache_.hasCachedEntry(groupIndex);
  const bool indexGroupCached = indexGroupCache_.hasCachedEntry(groupIndex);

  if (stripeGroupCached || indexGroupCached) {
    // At least one is already cached — no benefit from coalesced IO, so load
    // each individually (the cached one is a no-op, the other reads from file).
    return StripeIdentifier{
        stripeIndex, stripeGroup(groupIndex), indexGroup(groupIndex)};
  }

  // Neither is cached - use coalesced loading for adjacent metadata sections.
  auto loadedGroups = loadStripeAndIndexGroup(groupIndex);

  // Inject into caches using custom builder that returns the already-loaded
  // objects. This ensures the cache holds the reference.
  auto cachedStripeGroup = stripeGroupCache_.get(
      groupIndex,
      [&loadedGroups](uint32_t) { return std::move(loadedGroups.first); });
  auto cachedIndexGroup = indexGroupCache_.get(
      groupIndex,
      [&loadedGroups](uint32_t) { return std::move(loadedGroups.second); });

  return StripeIdentifier{
      stripeIndex, std::move(cachedStripeGroup), std::move(cachedIndexGroup)};
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
  const auto* footer = footerRoot(*footer_);
  auto* stripes = footer->stripes();
  if (stripes == nullptr) {
    return std::nullopt;
  }
  return toMetadataSection(stripes);
}

std::vector<MetadataSection> TabletReader::stripeGroupsMetadata() const {
  std::vector<MetadataSection> groupsMetadata;
  const auto* footer = footerRoot(*footer_);
  auto* stripeGroups = footer->stripe_groups();
  if (stripeGroups == nullptr) {
    return groupsMetadata;
  }
  groupsMetadata.reserve(stripeGroups->size());
  std::transform(
      stripeGroups->cbegin(),
      stripeGroups->cend(),
      std::back_inserter(groupsMetadata),
      [](const auto& stripeGroup) { return toMetadataSection(stripeGroup); });
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
  auto it = optionalSections_.find(name);
  if (it == optionalSections_.end()) {
    return std::nullopt;
  }

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
  return Section{
      std::move(*readMetadata(it->second, velox::dwio::common::LogType::FILE))};
}

bool TabletReader::hasIndex() const {
  return hasOptionalSection(std::string{kIndexSection});
}

void TabletReader::initIndex() {
  NIMBLE_CHECK_NULL(tabletIndex_, "Index already initialized.");

  if (!hasIndex()) {
    return;
  }

  auto indexSection =
      loadOptionalSection(std::string{kIndexSection}, /*keepCache=*/false);
  NIMBLE_CHECK(indexSection.has_value(), "Failed to load index section.");

  tabletIndex_ = TabletIndex::create(std::move(indexSection.value()));
}

void TabletReader::preloadStripeGroup(const folly::IOBuf& footerIoBuf) {
  NIMBLE_CHECK_NOT_NULL(stripes_);
  if (hasCache()) {
    return;
  }

  const auto* footer = footerRoot(*footer_);
  const auto* stripes = stripesRoot(*stripes_);
  auto* stripeGroups = footer->stripe_groups();
  NIMBLE_CHECK_NOT_NULL(stripeGroups, "Stripe groups is null.");
  NIMBLE_CHECK_EQ(
      stripeGroups->size(),
      *stripes->group_indices()->rbegin() + 1,
      "Unexpected stripe group count");

  // Eagerly cache the first stripe group if it's already in the footer buffer.
  const auto* stripeGroup = stripeGroups->Get(0);
  if (stripeGroups->size() == 1 &&
      stripeGroup->offset() + footerIoBuf.computeChainDataLength() >=
          fileSize_) {
    auto stripeGroupPtr =
        stripeGroupCache_.get(0, [&](uint32_t stripeGroupIndex) {
          return std::make_shared<StripeGroup>(
              stripeGroupIndex,
              *stripes_,
              std::make_unique<MetadataBuffer>(
                  *pool_,
                  footerIoBuf,
                  stripeGroup->offset() + footerIoBuf.computeChainDataLength() -
                      fileSize_,
                  stripeGroup->size(),
                  static_cast<CompressionType>(
                      stripeGroup->compression_type())));
        });
    *firstStripeGroup_.wlock() = std::move(stripeGroupPtr);
  }
}

void TabletReader::preloadIndexGroup(const folly::IOBuf& footerIoBuf) {
  // Skip when cache is available — on-demand reads will hit the cache.
  if (tabletIndex_ == nullptr || hasCache()) {
    return;
  }

  const auto numIndexGroups = tabletIndex_->numIndexGroups();
  if (numIndexGroups == 0) {
    return;
  }

  // Eagerly cache the first index group if it's already in the footer buffer.
  const auto firstIndexGroupMetadata = tabletIndex_->groupIndexMetadata(0);
  if (numIndexGroups == 1 &&
      firstIndexGroupMetadata.offset() + footerIoBuf.computeChainDataLength() >=
          fileSize_) {
    auto indexGroupPtr = indexGroupCache_.get(0, [&](uint32_t indexGroupIndex) {
      return StripeIndexGroup::create(
          indexGroupIndex,
          tabletIndex_->rootIndex(),
          std::make_unique<MetadataBuffer>(
              *pool_,
              footerIoBuf,
              firstIndexGroupMetadata.offset() +
                  footerIoBuf.computeChainDataLength() - fileSize_,
              firstIndexGroupMetadata.size(),
              static_cast<CompressionType>(
                  firstIndexGroupMetadata.compressionType())));
    });
    *firstIndexGroup_.wlock() = std::move(indexGroupPtr);
  }
}

std::vector<TabletReader::EnqueuedSection>
TabletReader::enqueueOptionalSections(
    const std::vector<std::string>& sectionNames) {
  std::vector<EnqueuedSection> enqueuedSections;
  for (const auto& name : sectionNames) {
    auto it = optionalSections_.find(name);
    if (it == optionalSections_.end()) {
      continue;
    }
    enqueuedSections.push_back(
        {name,
         it->second,
         bufferedInput_->enqueue(
             {it->second.offset(), it->second.size(), "optional"})});
  }
  return enqueuedSections;
}

void TabletReader::loadEnqueuedOptionalSections(
    std::vector<EnqueuedSection>&& sections) {
  auto cache = optionalSectionsCache_.wlock();
  for (auto& entry : sections) {
    cache->insert(
        {entry.name, readMetadata(std::move(entry.stream), entry.section)});
  }
}

std::optional<TabletReader::EnqueuedSection>
TabletReader::enqueueStripesSection() {
  const auto* footer = footerRoot(*footer_);
  auto* stripesSection = footer->stripes();
  if (stripesSection == nullptr) {
    NIMBLE_CHECK_EQ(footer->row_count(), 0);
    return std::nullopt;
  }
  NIMBLE_CHECK_GT(footer->row_count(), 0);
  NIMBLE_CHECK_NOT_NULL(bufferedInput_);
  const auto section = toMetadataSection(stripesSection);
  return EnqueuedSection{
      "stripes",
      section,
      bufferedInput_->enqueue({section.offset(), section.size(), "stripes"})};
}

void TabletReader::cacheMetadata(
    const folly::IOBuf& footerIoBuf,
    uint64_t footerIoOffset) {
  if (!hasCache()) {
    return;
  }

  const auto footerIoSize = footerIoBuf.computeChainDataLength();

  // Extracts a region from the IOBuf and caches it without coalescing.
  // Regions outside the buffer are silently skipped. cacheOffset overrides the
  // region offset as the cache key — used for footer+PS which is cached at
  // synthetic key fileSize_ (computable without file IO).
  auto extract = [&](uint64_t regionOffset,
                     uint64_t regionSize,
                     std::optional<uint64_t> cacheOffset = std::nullopt) {
    if (regionOffset < footerIoOffset) {
      return;
    }
    const uint64_t ioBufOffset = regionOffset - footerIoOffset;
    if (ioBufOffset + regionSize > footerIoSize) {
      return;
    }
    bufferedInput_->cacheRegion(
        cacheOffset.value_or(regionOffset),
        regionSize,
        footerIoBuf,
        ioBufOffset);
  };

  // Cache footer+PS at synthetic offset fileSize_ (beyond EOF). This lets
  // tryLoadAndInitFooterFromCache probe the cache using only the file size
  // (known without any IO), avoiding the circular dependency of needing
  // footerSize (from the postscript) to compute the real offset.
  const uint64_t footerPsSize = ps_.footerSize() + kPostscriptSize;
  extract(fileSize_ - footerPsSize, footerPsSize, fileSize_);

  const auto* footer = footerRoot(*footer_);

  // Cache stripes section.
  if (auto* stripesSection = footer->stripes()) {
    extract(stripesSection->offset(), stripesSection->size());
  }

  // Cache first stripe group.
  if (auto* stripeGroups = footer->stripe_groups();
      stripeGroups != nullptr && stripeGroups->size() > 0) {
    auto* stripeGroup = stripeGroups->Get(0);
    extract(stripeGroup->offset(), stripeGroup->size());
  }

  // Cache index section.
  if (tabletIndex_ == nullptr) {
    return;
  }
  auto indexIt = optionalSections_.find(std::string{kIndexSection});
  NIMBLE_CHECK(indexIt != optionalSections_.end());
  extract(indexIt->second.offset(), indexIt->second.size());

  // Cache first index group.
  if (tabletIndex_->numIndexGroups() > 0) {
    const auto indexGroup = tabletIndex_->groupIndexMetadata(0);
    extract(indexGroup.offset(), indexGroup.size());
  }
}

} // namespace facebook::nimble
