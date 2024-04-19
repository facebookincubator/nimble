// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once
#include <span>

#include "dwio/nimble/common/Checksum.h"
#include "dwio/nimble/common/Vector.h"
#include "folly/Range.h"
#include "folly/io/IOBuf.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"

// The Tablet class is the on-disk layout for nimble.
//
// As data is streamed into a tablet, we buffer it until the total amount
// of memory used for buffering hits a chosen limit. Then we convert the
// buffered memory to streams and write them out to disk in a stripe, recording
// their byte ranges. This continues until all data for the file has been
// streamed in, at which point we write out any remaining buffered data and
// write out the byte ranges + some other metadata in the footer.
//
// The general recommendation for the buffering limit is to make it as large
// as the amount of memory you've allocated to a single processing task. The
// rationale being that the highest memory read case (select *) loads all the
// encoded stream, and in the worst case (totally random data) the encoded data
// will be the same size as the raw data.

namespace facebook::nimble {

using MemoryPool = facebook::velox::memory::MemoryPool;

class MetadataBuffer {
 public:
  MetadataBuffer(
      velox::memory::MemoryPool& memoryPool,
      std::string_view ref,
      CompressionType type = CompressionType::Uncompressed);

  MetadataBuffer(
      velox::memory::MemoryPool& memoryPool,
      const folly::IOBuf& iobuf,
      size_t offset,
      size_t length,
      CompressionType type = CompressionType::Uncompressed);

  MetadataBuffer(
      velox::memory::MemoryPool& memoryPool,
      const folly::IOBuf& iobuf,
      CompressionType type = CompressionType::Uncompressed);

  std::string_view content() const {
    return {buffer_.data(), buffer_.size()};
  }

 private:
  Vector<char> buffer_;
};

class Section {
 public:
  explicit Section(MetadataBuffer&& buffer) : buffer_{std::move(buffer)} {}

  std::string_view content() const {
    return buffer_.content();
  }
  explicit operator std::string_view() const {
    return content();
  }

 private:
  MetadataBuffer buffer_;
};

class Postscript {
 public:
  uint32_t footerSize() const {
    return footerSize_;
  }

  CompressionType footerCompressionType() const {
    return footerCompressionType_;
  }

  uint64_t checksum() const {
    return checksum_;
  }

  ChecksumType checksumType() const {
    return checksumType_;
  }

  uint32_t majorVersion() const {
    return majorVersion_;
  }

  uint32_t minorVersion() const {
    return minorVersion_;
  }

  static Postscript parse(std::string_view data);

 private:
  uint32_t footerSize_;
  CompressionType footerCompressionType_;
  uint64_t checksum_;
  ChecksumType checksumType_;
  uint32_t majorVersion_;
  uint32_t minorVersion_;
};

// Stream loader abstraction.
// This is the returned object when loading streams from a tablet.
class StreamLoader {
 public:
  virtual ~StreamLoader() = default;
  virtual const std::string_view getStream() const = 0;
};

// Provides read access to a Tablet written by a TabletWriter.
// Example usage to read all streams from stripe 0 in a file:
//   auto readFile = std::make_unique<LocalReadFile>("/tmp/myfile");
//   Tablet tablet(std::move(readFile));
//   auto serializedStreams = tablet.load(0, std::vector{1, 2});
//  |serializedStreams[i]| now contains the stream corresponding to
//  the stream identifier provided in the input vector.
class Tablet {
 public:
  // Compute checksum from the beginning of the file all the way to footer
  // size and footer compression type field in postscript.
  // chunkSize means each time reads up to chunkSize, until all data are done.
  static uint64_t calculateChecksum(
      MemoryPool& memoryPool,
      velox::ReadFile* readFile,
      uint64_t chunkSize = 256 * 1024 * 1024);

  Tablet(
      MemoryPool& memoryPool,
      velox::ReadFile* readFile,
      const std::vector<std::string>& preloadOptionalSections = {});
  Tablet(
      MemoryPool& memoryPool,
      std::shared_ptr<velox::ReadFile> readFile,
      const std::vector<std::string>& preloadOptionalSections = {});

  // Returns a collection of stream loaders for the given stripe. The stream
  // loaders are returned in the same order as the input stream identifiers
  // span. If a stream was not present in the given stripe a nullptr is returned
  // in its slot.
  std::vector<std::unique_ptr<StreamLoader>> load(
      uint32_t stripe,
      std::span<const uint32_t> streamIdentifiers,
      std::function<std::string_view(uint32_t)> streamLabel = [](uint32_t) {
        return std::string_view{};
      }) const;

  std::optional<Section> loadOptionalSection(
      const std::string& name,
      bool keepCache = false) const;

  uint64_t fileSize() const {
    return file_->size();
  }

  uint32_t footerSize() const {
    return ps_.footerSize();
  }

  CompressionType footerCompressionType() const {
    return ps_.footerCompressionType();
  }

  uint64_t checksum() const {
    return ps_.checksum();
  }

  ChecksumType checksumType() const {
    return ps_.checksumType();
  }

  uint32_t majorVersion() const {
    return ps_.majorVersion();
  }

  uint32_t minorVersion() const {
    return ps_.minorVersion();
  }

  // Number of rows in the whole tablet.
  uint64_t tabletRowCount() const {
    return tabletRowCount_;
  }

  // The number of rows in the given stripe. These sum to tabletRowCount().
  uint32_t stripeRowCount(uint32_t stripe) const {
    return stripeRowCounts_[stripe];
  }

  // The number of stripes in the tablet.
  uint32_t stripeCount() const {
    return stripeCount_;
  }

  uint64_t stripeOffset(uint32_t stripe) const {
    return stripeOffsets_[stripe];
  }

  // Returns stream offsets for the specified stripe. Number of streams is
  // determined by schema node count at the time when stripe is written, so it
  // may have fewer number of items than the final schema node count
  std::span<const uint32_t> streamOffsets(uint32_t stripe) const;

  // Returns stream sizes for the specified stripe. Has same constraint as
  // `streamOffsets()`.
  std::span<const uint32_t> streamSizes(uint32_t stripe) const;

  // Returns stream count for the specified stripe. Has same constraint as
  // `streamOffsets()`.
  uint32_t streamCount(uint32_t stripe) const;

 private:
  struct StripeGroup {
    uint32_t index() const {
      return index_;
    }

    uint32_t streamCount() const {
      return streamCount_;
    }

    void reset(
        uint32_t stripeGroupIndex,
        const MetadataBuffer& stripes,
        uint32_t stripeIndex,
        std::unique_ptr<MetadataBuffer> metadata);

    std::span<const uint32_t> streamOffsets(uint32_t stripe) const;
    std::span<const uint32_t> streamSizes(uint32_t stripe) const;

   private:
    std::unique_ptr<MetadataBuffer> metadata_;
    uint32_t index_{std::numeric_limits<uint32_t>::max()};
    uint32_t streamCount_{0};
    uint32_t firstStripe_{0};
    const uint32_t* streamOffsets_{nullptr};
    const uint32_t* streamSizes_{nullptr};
  };

  void initStripes();

  void ensureStripeGroup(uint32_t stripe) const;

  // For testing use
  Tablet(
      MemoryPool& memoryPool,
      std::shared_ptr<velox::ReadFile> readFile,
      Postscript postscript,
      std::string_view footer,
      std::string_view stripes,
      std::string_view stripeGroup,
      std::unordered_map<std::string, std::string_view> optionalSections = {});

  MemoryPool& memoryPool_;
  velox::ReadFile* file_;
  std::shared_ptr<velox::ReadFile> ownedFile_;

  Postscript ps_;
  std::unique_ptr<MetadataBuffer> footer_;
  std::unique_ptr<MetadataBuffer> stripes_;
  mutable StripeGroup stripeGroup_;

  uint64_t tabletRowCount_;
  uint32_t stripeCount_{0};
  const uint32_t* stripeRowCounts_{nullptr};
  const uint64_t* stripeOffsets_{nullptr};
  std::unordered_map<
      std::string,
      std::tuple<uint64_t, uint32_t, CompressionType>>
      optionalSections_;
  mutable std::unordered_map<std::string, std::unique_ptr<MetadataBuffer>>
      optionalSectionsCache_;

  friend class TabletHelper;
};

struct Stream {
  uint32_t offset;
  std::vector<std::string_view> content;
};

class LayoutPlanner {
 public:
  virtual std::vector<Stream> getLayout(std::vector<Stream>&& streams) = 0;

  virtual ~LayoutPlanner() = default;
};

struct TabletWriterOptions {
  std::unique_ptr<LayoutPlanner> layoutPlanner{nullptr};
  uint32_t metadataFlushThreshold{8 * 1024 * 1024}; // 8Mb
  uint32_t metadataCompressionThreshold{4 * 1024 * 1024}; // 4Mb
  ChecksumType checksumType{ChecksumType::XXH3_64};
};

// Writes a new nimble file.
class TabletWriter {
 public:
  TabletWriter(
      velox::memory::MemoryPool& memoryPool,
      velox::WriteFile* writeFile,
      TabletWriterOptions options = {})
      : file_{writeFile},
        memoryPool_(memoryPool),
        options_(std::move(options)),
        checksum_{ChecksumFactory::create(options_.checksumType)} {}

  // Writes out the footer. Remember that the underlying file is not readable
  // until the write file itself is closed.
  void close();

  // TODO: do we want a stripe header? E.g. per stream min/max (at least for
  // key cols), that sort of thing? We can add later. We'll also want to
  // be able to control the stream order on disk, presumably via a options
  // param to the constructor.
  //
  // The first argument in the map gives the stream name. The second's first
  // gives part gives the uncompressed string returned from a Serialize
  // function, and the second part the compression type to apply when writing to
  // disk. Later we may want to generalize that compression type to include a
  // level or other params.
  //
  // A stream's type must be the same across all stripes.
  void writeStripe(uint32_t rowCount, std::vector<Stream> streams);

  void writeOptionalSection(std::string name, std::string_view content);

  // The number of bytes written so far.
  uint64_t size() const {
    return file_->size();
  }

  // For testing purpose
  uint32_t stripeGroupCount() const {
    return stripeGroups_.size();
  }

 private:
  struct MetadataSection {
    uint64_t offset;
    uint32_t size;
    CompressionType compressionType;
  };

  // Write metadata entry to file
  CompressionType writeMetadata(std::string_view metadata);
  // Write stripe group metadata entry and also add that to footer sections if
  // exceeds metadata flush size.
  void tryWriteStripeGroup(bool force = false);
  // Create metadata section in the file
  MetadataSection createMetadataSection(std::string_view metadata);

  void writeWithChecksum(std::string_view data);
  void writeWithChecksum(const folly::IOBuf& buf);

  velox::WriteFile* file_;
  velox::memory::MemoryPool& memoryPool_;
  TabletWriterOptions options_;
  std::unique_ptr<Checksum> checksum_;

  // Number of rows in each stripe.
  std::vector<uint32_t> rowCounts_;
  // Offsets from start of file to first byte in each stripe.
  std::vector<uint64_t> stripeOffsets_;
  // Sizes of the stripes
  std::vector<uint32_t> stripeSizes_;
  // Stripe group indices
  std::vector<uint32_t> stripeGroupIndices_;
  // Accumulated offsets within each stripe relative to start of the stripe,
  // with one value for each seen stream in the current OR PREVIOUS stripes.
  std::vector<std::vector<uint32_t>> streamOffsets_;
  // Accumulated stream sizes within each stripe. Same behavior as
  // streamOffsets_.
  std::vector<std::vector<uint32_t>> streamSizes_;
  // Current stripe group index
  uint32_t stripeGroupIndex_{0};
  // Stripe groups
  std::vector<MetadataSection> stripeGroups_;
  // Optional sections
  std::unordered_map<std::string, MetadataSection> optionalSections_;
};

} // namespace facebook::nimble
