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
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <thread>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Checksum.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/tests/GTestUtils.h"
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/index/ChunkIndexGroup.h"
#include "dwio/nimble/index/IndexConfig.h"
#include "dwio/nimble/index/IndexLookup.h"
#include "dwio/nimble/index/tests/ClusterIndexTestUtils.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/FileLayout.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/tablet/TabletWriter.h"
#include "dwio/nimble/tablet/tests/TabletTestUtils.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "folly/FileUtil.h"
#include "folly/Random.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "velox/common/file/File.h"

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileHandle.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/io/Options.h"
#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/ExecutorBarrier.h"

using namespace facebook;
using nimble::SortOrder;

namespace {

DEFINE_uint32(
    tablet_tests_seed,
    0,
    "If provided, this seed will be used when executing tests. "
    "Otherwise, a random seed will be used.");

#define GREEN "\033[32m"
#define RESET_COLOR "\033[0m"

// Total size of the fields after the flatbuffer.
constexpr uint32_t kPostscriptSize = 20;

/// BufferedInput test modes for parameterized testing.
enum class BufferedInputMode {
  /// No BufferedInput - direct file reads.
  kNone,
  /// Base BufferedInput class (no cache).
  kBufferedInput,
  /// DirectBufferedInput (no cache, but with coalescing).
  kDirectBufferedInput,
  /// CachedBufferedInput with AsyncDataCache.
  kCachedBufferedInput,
};

std::string bufferedInputModeToString(BufferedInputMode mode) {
  switch (mode) {
    case BufferedInputMode::kNone:
      return "None";
    case BufferedInputMode::kBufferedInput:
      return "BufferedInput";
    case BufferedInputMode::kDirectBufferedInput:
      return "DirectBufferedInput";
    case BufferedInputMode::kCachedBufferedInput:
      return "CachedBufferedInput";
  }
  return "Unknown";
}

struct StripeSpecifications {
  uint32_t rowCount;
  std::vector<uint32_t> streamOffsets;
};

struct StripeData {
  uint32_t rowCount;
  std::vector<nimble::Stream> streams;
};

void printData(std::string prefix, std::string_view data) {
  std::string output;
  for (auto i = 0; i < data.size(); ++i) {
    output += folly::to<std::string>((uint8_t)data[i]) + " ";
  }

  VLOG(1) << prefix << " (" << (void*)data.data() << "): " << output;
}

std::vector<StripeData> createStripesData(
    std::mt19937& rng,
    const std::vector<StripeSpecifications>& stripes,
    nimble::Buffer& buffer) {
  std::vector<StripeData> stripesData;
  stripesData.reserve(stripes.size());

  // Each generator iteration returns a single stripe to write
  for (auto& stripe : stripes) {
    std::vector<nimble::Stream> streams;
    streams.reserve(stripe.streamOffsets.size());
    std::transform(
        stripe.streamOffsets.cbegin(),
        stripe.streamOffsets.cend(),
        std::back_inserter(streams),
        [&rng, &buffer](auto offset) {
          const auto size = folly::Random::rand32(32, rng) + 2;
          auto pos = buffer.reserve(size);
          for (auto i = 0; i < size; ++i) {
            pos[i] = folly::Random::rand32(256, rng);
          }
          printData(folly::to<std::string>("Stream ", offset), {pos, size});

          return nimble::Stream{
              .offset = offset,
              .chunks = {{.content = {std::string_view(pos, size)}}}};
        });
    stripesData.push_back({
        .rowCount = stripe.rowCount,
        .streams = std::move(streams),
    });
  }

  return stripesData;
}

class TabletTest : public ::testing::TestWithParam<BufferedInputMode> {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(2);
  }

  void TearDown() override {
    fileHandle_.reset();
    readerOptions_.reset();
    if (cache_) {
      cache_->shutdown();
      cache_.reset();
    }
    allocator_.reset();
    executor_.reset();
  }

  /// Configures MetadataInput fields on TabletReader::Options based on
  /// test mode.
  void configureMetadataInput(nimble::TabletReader::Options& options) {
    ensureReaderOptions();
    options.ioOptions = *readerOptions_;
  }

  void configureMetadataInput(
      nimble::TabletReader::Options& options,
      BufferedInputMode mode) {
    ensureReaderOptions();
    options.ioOptions = *readerOptions_;

    if (mode == BufferedInputMode::kCachedBufferedInput) {
      if (!allocator_) {
        allocator_ = std::make_shared<velox::memory::MallocAllocator>(
            velox::memory::MemoryAllocator::Options{
                .capacity = 1UL << 30, .reservationByteLimit = 0});
      }
      if (!cache_) {
        cache_ = velox::cache::AsyncDataCache::create(allocator_.get());
      }
      auto& ids = velox::fileIds();
      auto fileKey = fmt::format(
          "testFile_{}", reinterpret_cast<uintptr_t>(readFile_.get()));
      fileHandle_ = std::make_unique<velox::FileHandle>();
      fileHandle_->uuid = velox::StringIdLease(ids, fileKey);
      fileHandle_->groupId = velox::StringIdLease(ids, "testGroup");
      options.cache = cache_.get();
      options.fileHandle = fileHandle_.get();
      options.cacheMetadata = true;
      options.cacheIndex = true;
    }
  }

 private:
  void ensureReaderOptions() {
    if (readerOptions_ == nullptr) {
      readerOptions_ = std::make_unique<velox::io::ReaderOptions>(pool_.get());
      readerOptions_->setDataIoStats(dataIoStats_);
      readerOptions_->setMetadataIoStats(metadataIoStats_);
      readerOptions_->setIndexIoStats(indexIoStats_);
    }
  }

 protected:
  bool expectHasCache() const {
    return GetParam() == BufferedInputMode::kCachedBufferedInput;
  }

  /// Creates a TabletReader with MetadataInput configured based on test mode.
  std::shared_ptr<nimble::TabletReader> createTabletReader(
      std::shared_ptr<velox::ReadFile> readFile,
      nimble::TabletReader::Options options,
      BufferedInputMode mode) {
    readFile_ = readFile;
    configureMetadataInput(options, mode);
    if (fileHandle_ != nullptr) {
      fileHandle_->file = readFile_;
    }
    return nimble::TabletReader::create(readFile_, pool_.get(), options);
  }

  std::shared_ptr<nimble::TabletReader> createTabletReader(
      std::shared_ptr<velox::ReadFile> readFile,
      nimble::TabletReader::Options options = {}) {
    return createTabletReader(
        std::move(readFile), std::move(options), GetParam());
  }

  /// Overload for string file content (creates InMemoryReadFile internally).
  std::shared_ptr<nimble::TabletReader> createTabletReader(
      const std::string& fileContent,
      nimble::TabletReader::Options options = {}) {
    return createTabletReader(
        std::make_shared<velox::InMemoryReadFile>(fileContent),
        std::move(options));
  }

  std::shared_ptr<velox::io::IoStatistics> dataIoStats_{
      std::make_shared<velox::io::IoStatistics>()};
  std::shared_ptr<velox::io::IoStatistics> metadataIoStats_{
      std::make_shared<velox::io::IoStatistics>()};
  std::shared_ptr<velox::io::IoStatistics> indexIoStats_{
      std::make_shared<velox::io::IoStatistics>()};

  std::shared_ptr<velox::ReadFile> readFile_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<velox::memory::MallocAllocator> allocator_;
  std::shared_ptr<velox::cache::AsyncDataCache> cache_;
  std::unique_ptr<velox::FileHandle> fileHandle_;
  std::unique_ptr<velox::io::ReaderOptions> readerOptions_;

  // Runs a single write/read test using input parameters
  void parameterizedTest(
      uint32_t metadataFlushThreshold,
      uint32_t metadataCompressionThreshold,
      const std::vector<StripeData>& stripesData,
      const std::optional<std::function<void(const std::exception&)>>&
          errorVerifier = std::nullopt,
      const std::optional<std::function<uint32_t(uint32_t)>>&
          expectedReadsPerStripe = std::nullopt,
      bool enableStreamDedplication = false) {
    try {
      std::string file;
      velox::InMemoryWriteFile writeFile(&file);
      auto tabletWriter = nimble::TabletWriter::create(
          &writeFile,
          *pool_,
          {
              .layoutPlanner = nullptr,
              .metadataFlushThreshold = metadataFlushThreshold,
              .metadataCompressionThreshold = metadataCompressionThreshold,
              .streamDeduplicationEnabled = enableStreamDedplication,
          });

      EXPECT_EQ(0, tabletWriter->size());

      struct StripeData {
        uint32_t rowCount;
        std::vector<nimble::Stream> streams;
      };

      for (auto& stripe : stripesData) {
        tabletWriter->writeStripe(stripe.rowCount, stripe.streams);
      }

      tabletWriter->close();
      EXPECT_LT(0, tabletWriter->size());
      writeFile.close();
      EXPECT_EQ(writeFile.size(), tabletWriter->size());

      folly::writeFile(file, "/tmp/test.nimble");

      for (auto useChainedBuffers : {false, true}) {
        SCOPED_TRACE(fmt::format("useChainedBuffers={}", useChainedBuffers));
        auto readFile =
            std::make_shared<nimble::testing::InMemoryTrackableReadFile>(
                file, useChainedBuffers);
        auto tablet = createTabletReader(readFile);

        // Get stripe group count through the read path
        nimble::test::TabletReaderTestHelper tabletHelper(tablet.get());
        auto stripeGroupCount = tabletHelper.numStripeGroups();

        EXPECT_EQ(stripesData.size(), tablet->stripeCount());
        EXPECT_EQ(
            std::accumulate(
                stripesData.begin(),
                stripesData.end(),
                uint64_t{0},
                [](uint64_t r, const auto& s) { return r + s.rowCount; }),
            tablet->tabletRowCount());

        VLOG(1) << "Output Tablet -> StripeCount: " << tablet->stripeCount()
                << ", RowCount: " << tablet->tabletRowCount();

        uint32_t maxStreamIdentifiers = 0;
        for (auto stripe = 0; stripe < stripesData.size(); ++stripe) {
          auto stripeIdentifier = tablet->stripeIdentifier(stripe);
          maxStreamIdentifiers = std::max(
              maxStreamIdentifiers, tablet->streamCount(stripeIdentifier));
        }
        std::vector<uint32_t> allStreamIdentifiers(maxStreamIdentifiers);
        std::iota(allStreamIdentifiers.begin(), allStreamIdentifiers.end(), 0);
        std::span<const uint32_t> allStreamIdentifiersSpan{
            allStreamIdentifiers.cbegin(), allStreamIdentifiers.cend()};
        size_t extraReads = 0;
        std::vector<uint64_t> totalStreamSize;
        for (auto stripe = 0; stripe < stripesData.size(); ++stripe) {
          auto stripeIdentifier = tablet->stripeIdentifier(stripe);
          totalStreamSize.push_back(tablet->totalStreamSize(
              stripeIdentifier, allStreamIdentifiersSpan));
        }
        // Now, read all stripes and verify results
        for (auto stripe = 0; stripe < stripesData.size(); ++stripe) {
          EXPECT_EQ(
              stripesData[stripe].rowCount, tablet->stripeRowCount(stripe));

          readFile->resetChunks();
          auto stripeIdentifier = tablet->stripeIdentifier(stripe);
          std::vector<uint32_t> identifiers(
              tablet->streamCount(stripeIdentifier));
          std::iota(identifiers.begin(), identifiers.end(), 0);
          auto serializedStreams = tablet->load(
              stripeIdentifier, {identifiers.cbegin(), identifiers.cend()});
          uint64_t totalStreamSizeExpected = 0;
          for (const auto& stream : serializedStreams) {
            if (stream) {
              totalStreamSizeExpected += stream->getStream().size();
            }
          }
          EXPECT_EQ(totalStreamSize[stripe], totalStreamSizeExpected);
          auto chunks = readFile->chunks();
          auto expectedReads = stripesData[stripe].streams.size();
          if (expectedReadsPerStripe.has_value()) {
            expectedReads = expectedReadsPerStripe.value()(stripe);
          }
          auto diff = chunks.size() - expectedReads;
          EXPECT_LE(diff, 1);
          extraReads += diff;

          for (const auto& chunk : chunks) {
            VLOG(1) << "Chunk Offset: " << chunk.offset
                    << ", Size: " << chunk.size << ", Stripe: " << stripe;
          }

          for (auto i = 0; i < serializedStreams.size(); ++i) {
            // Verify streams content. If stream wasn't written in this stripe,
            // it should return nullopt optional.
            auto found = false;
            for (const auto& stream : stripesData[stripe].streams) {
              if (stream.offset == i) {
                found = true;
                EXPECT_TRUE(serializedStreams[i]);
                std::vector<std::string_view> allContent;
                for (const auto& chunk : stream.chunks) {
                  for (const auto& content : chunk.content) {
                    allContent.push_back(content);
                  }
                }
                auto expectedData = folly::join("", allContent);
                printData(
                    folly::to<std::string>("Expected Stream ", stream.offset),
                    expectedData);
                const auto& actual = serializedStreams[i];
                std::string_view actualData = actual->getStream();
                printData(
                    folly::to<std::string>("Actual Stream ", stream.offset),
                    actualData);
                EXPECT_EQ(expectedData, actualData);
              }
            }
            if (!found) {
              EXPECT_FALSE(serializedStreams[i]);
            }
          }
        }

        if (!expectHasCache()) {
          EXPECT_EQ(extraReads, (stripeGroupCount == 1 ? 0 : stripeGroupCount));
        }

        if (errorVerifier.has_value()) {
          FAIL() << "Error verifier is provided, but no exception was thrown.";
        }
      }
    } catch (const std::exception& e) {
      if (!errorVerifier.has_value()) {
        FAIL() << "Unexpected exception: " << e.what();
      }

      errorVerifier.value()(e);

      // If errorVerifier detected an error, log the exception
      if (::testing::Test::HasFatalFailure()) {
        FAIL() << "Failed verifying exception: " << e.what();
      } else if (::testing::Test::HasNonfatalFailure()) {
        LOG(WARNING) << "Failed verifying exception: " << e.what();
      }
    }
  }

  // Run all permutations of a test using all test parameters
  void test(
      std::vector<StripeSpecifications> stripes,
      std::optional<std::function<void(const std::exception&)>> errorVerifier =
          std::nullopt) {
    std::vector<uint32_t> metadataCompressionThresholds{
        // use size 0 here so it will always force a footer compression
        0,
        // use a large number here so it will not do a footer compression
        1024 * 1024 * 1024};
    std::vector<uint32_t> metadataFlushThresholds{
        0, // force flush
        100, // flush a few
        1024 * 1024 * 1024, // never flush
    };

    auto seed = folly::Random::rand32();
    LOG(INFO) << "seed: " << seed;
    std::mt19937 rng(seed);

    nimble::Buffer buffer{*pool_};
    auto stripesData = createStripesData(rng, stripes, buffer);

    for (auto flushThreshold : metadataFlushThresholds) {
      for (auto compressionThreshold : metadataCompressionThresholds) {
        SCOPED_TRACE(
            fmt::format(
                "flushThreshold={}, compressionThreshold={}",
                flushThreshold,
                compressionThreshold));
        parameterizedTest(
            flushThreshold, compressionThreshold, stripesData, errorVerifier);
      }
    }
  }

  void checksumTest(
      std::mt19937& rng,
      uint32_t metadataCompressionThreshold,
      nimble::ChecksumType checksumType,
      bool checksumChunked,
      std::vector<StripeSpecifications> stripes) {
    std::string file;
    velox::InMemoryWriteFile writeFile(&file);
    auto tabletWriter = nimble::TabletWriter::create(
        &writeFile,
        *pool_,
        {.layoutPlanner = nullptr,
         .metadataCompressionThreshold = metadataCompressionThreshold,
         .checksumType = checksumType});
    EXPECT_EQ(0, tabletWriter->size());

    struct StripeData {
      uint32_t rowCount;
      std::vector<nimble::Stream> streams;
    };

    nimble::Buffer buffer{*pool_};
    auto stripesData = createStripesData(rng, stripes, buffer);

    for (auto& stripe : stripesData) {
      tabletWriter->writeStripe(stripe.rowCount, stripe.streams);
    }

    tabletWriter->close();
    EXPECT_LT(0, tabletWriter->size());
    writeFile.close();
    EXPECT_EQ(writeFile.size(), tabletWriter->size());

    for (auto useChainedBuffers : {false, true}) {
      // Velidate checksum on a good file
      auto readFile =
          std::make_shared<nimble::testing::InMemoryTrackableReadFile>(
              file, useChainedBuffers);
      auto tablet = createTabletReader(readFile);
      auto storedChecksum = tablet->checksum();
      EXPECT_EQ(
          storedChecksum,
          nimble::TabletReader::calculateChecksum(
              *pool_,
              readFile.get(),
              checksumChunked ? writeFile.size() / 3 : writeFile.size()))
          << "metadataCompressionThreshold: " << metadataCompressionThreshold
          << ", checksumType: " << nimble::toString(checksumType)
          << ", checksumChunked: " << checksumChunked;

      // Flip a bit in the stream and verify that checksum can catch the error
      {
        // First, make sure we are working on a clean file
        nimble::testing::InMemoryTrackableReadFile readFileUnchanged(
            file, useChainedBuffers);
        EXPECT_EQ(
            storedChecksum,
            nimble::TabletReader::calculateChecksum(
                *pool_, &readFileUnchanged));

        char& c = file[10];
        c ^= 0x80;
        nimble::testing::InMemoryTrackableReadFile readFileChanged(
            file, useChainedBuffers);
        EXPECT_NE(
            storedChecksum,
            nimble::TabletReader::calculateChecksum(
                *pool_,
                &readFileChanged,
                checksumChunked ? writeFile.size() / 3 : writeFile.size()))
            << "Checksum didn't find corruption when stream content is changed. "
            << "metadataCompressionThreshold: " << metadataCompressionThreshold
            << ", checksumType: " << nimble::toString(checksumType)
            << ", checksumChunked: " << checksumChunked;
        // revert the file back.
        c ^= 0x80;
      }

      // Flip a bit in the flatbuffer footer and verify that checksum can catch
      // the error
      {
        // First, make sure we are working on a clean file
        nimble::testing::InMemoryTrackableReadFile readFileUnchanged(
            file, useChainedBuffers);
        EXPECT_EQ(
            storedChecksum,
            nimble::TabletReader::calculateChecksum(
                *pool_, &readFileUnchanged));

        auto posInFooter =
            tablet->fileSize() - kPostscriptSize - tablet->footerSize() / 2;
        uint8_t& byteInFooter =
            *reinterpret_cast<uint8_t*>(file.data() + posInFooter);
        byteInFooter ^= 0x1;
        nimble::testing::InMemoryTrackableReadFile readFileChanged(
            file, useChainedBuffers);
        EXPECT_NE(
            storedChecksum,
            nimble::TabletReader::calculateChecksum(
                *pool_,
                &readFileChanged,
                checksumChunked ? writeFile.size() / 3 : writeFile.size()))
            << "Checksum didn't find corruption when footer content is changed. "
            << "metadataCompressionThreshold: " << metadataCompressionThreshold
            << ", checksumType: " << nimble::toString(checksumType)
            << ", checksumChunked: " << checksumChunked;
        // revert the file back.
        byteInFooter ^= 0x1;
      }

      // Flip a bit in the footer size field and verify that checksum can catch
      // the error
      {
        // First, make sure we are working on a clean file
        nimble::testing::InMemoryTrackableReadFile readFileUnchanged(
            file, useChainedBuffers);
        EXPECT_EQ(
            storedChecksum,
            nimble::TabletReader::calculateChecksum(
                *pool_, &readFileUnchanged));

        auto footerSizePos = tablet->fileSize() - kPostscriptSize;
        uint32_t& footerSize =
            *reinterpret_cast<uint32_t*>(file.data() + footerSizePos);
        ASSERT_EQ(footerSize, tablet->footerSize());
        footerSize ^= 0x1;

        nimble::testing::InMemoryTrackableReadFile readFileChanged(
            file, useChainedBuffers);
        EXPECT_NE(
            storedChecksum,
            nimble::TabletReader::calculateChecksum(
                *pool_,
                &readFileChanged,
                checksumChunked ? writeFile.size() / 3 : writeFile.size()))
            << "Checksum didn't find corruption when footer size field is changed. "
            << "metadataCompressionThreshold: " << metadataCompressionThreshold
            << ", checksumType: " << nimble::toString(checksumType)
            << ", checksumChunked: " << checksumChunked;
        // revert the file back.
        footerSize ^= 0x1;
      }

      // Flip a bit in the footer compression type field and verify that
      // checksum can catch the error
      {
        // First, make sure we are working on a clean file
        nimble::testing::InMemoryTrackableReadFile readFileUnchanged(
            file, useChainedBuffers);
        EXPECT_EQ(
            storedChecksum,
            nimble::TabletReader::calculateChecksum(
                *pool_, &readFileUnchanged));

        auto footerCompressionTypePos =
            tablet->fileSize() - kPostscriptSize + 4;
        nimble::CompressionType& footerCompressionType =
            *reinterpret_cast<nimble::CompressionType*>(
                file.data() + footerCompressionTypePos);
        ASSERT_EQ(footerCompressionType, tablet->footerCompressionType());
        // Cannot do bit operation on enums, so cast it to integer type.
        uint8_t& typeAsInt =
            *reinterpret_cast<uint8_t*>(&footerCompressionType);
        typeAsInt ^= 0x1;
        nimble::testing::InMemoryTrackableReadFile readFileChanged(
            file, useChainedBuffers);
        EXPECT_NE(
            storedChecksum,
            nimble::TabletReader::calculateChecksum(
                *pool_,
                &readFileChanged,
                checksumChunked ? writeFile.size() / 3 : writeFile.size()))
            << "Checksum didn't find corruption when compression type field is changed. "
            << "metadataCompressionThreshold: " << metadataCompressionThreshold
            << ", checksumType: " << nimble::toString(checksumType)
            << ", checksumChunked: " << checksumChunked;
        // revert the file back.
        typeAsInt ^= 0x1;
      }
    }
  }

  void testStreamDeduplication(
      std::mt19937& rng,
      bool noDuplicates,
      uint32_t chunkCount,
      bool evenChunks) {
    const uint32_t maxStripes = 10;
    const uint32_t maxStreams = 100;
    const uint32_t maxDataSize = 32;

    nimble::Buffer buffer{*pool_};

    LOG(INFO) << GREEN
              << "Stream Deduplication Test. No duplicates: " << noDuplicates
              << ", Chunk Count: " << chunkCount
              << ", Even Chunks: " << evenChunks << RESET_COLOR;

    const auto stripesCount = folly::Random::rand32(maxStripes, rng) + 1;
    std::vector<StripeData> stripesData;
    std::vector<uint32_t> uniqueStreamsPerStripe;
    stripesData.reserve(stripesCount);
    uniqueStreamsPerStripe.reserve(stripesCount);
    for (auto stripe = 0; stripe < stripesCount; ++stripe) {
      // For each stripe:
      // * Generate offsets (with gaps)
      // * Pick how many duplicate groups to have
      // * Assign offsets to duplicate groups
      // * Generate unique strings, one for each duplicate group
      // * For each offset, assign string (based on group), and split it to
      //   chunks
      const auto streamCount = folly::Random::rand32(maxStreams, rng) + 1;
      std::unordered_set<uint32_t> offsets;
      offsets.reserve(streamCount);
      while (offsets.size() < streamCount) {
        offsets.insert(folly::Random::rand32(streamCount * 2, rng));
      }

      const auto duplicateCount = noDuplicates
          ? streamCount
          : folly::Random::rand32(streamCount, rng) + 1;
      uniqueStreamsPerStripe.push_back(duplicateCount);

      std::vector<std::vector<uint32_t>> duplicateGroups{duplicateCount};
      uint32_t index = 0;
      while (!offsets.empty()) {
        auto offset = offsets.extract(offsets.begin());
        duplicateGroups[index++ % duplicateGroups.size()].push_back(
            offset.value());
      }

      std::unordered_set<std::string_view> uniqueStreamData;
      uniqueStreamData.reserve(duplicateCount);
      while (uniqueStreamData.size() < duplicateCount) {
        const auto dataSize = folly::Random::rand32(maxDataSize, rng) + 2;
        auto pos = buffer.reserve(dataSize);
        folly::Random::secureRandom(pos, dataSize);
        uniqueStreamData.emplace(pos, dataSize);
      }

      StripeData stripeData{.rowCount = folly::Random::rand32(10, rng) + 1};
      stripeData.streams.reserve(streamCount);
      for (const auto& duplicateGroup : duplicateGroups) {
        auto data = uniqueStreamData.extract(uniqueStreamData.begin());
        for (auto offset : duplicateGroup) {
          auto getChunkEnds = [](std::mt19937& rng,
                                 uint32_t chunkCount,
                                 bool evenChunks,
                                 std::string_view source) {
            std::vector<size_t> chunkEnds;
            chunkEnds.reserve(chunkCount);

            size_t splitPoint = 0;
            for (auto i = 0; i < chunkCount - 1; ++i) {
              if (source.size() - splitPoint < 3) {
                break;
              }

              splitPoint += evenChunks
                  ? static_cast<uint32_t>(source.size()) / chunkCount
                  : folly::Random::rand32(
                        static_cast<uint32_t>(source.size() - splitPoint) - 2,
                        rng) +
                      1;
              chunkEnds.push_back(splitPoint);
            }
            chunkEnds.push_back(source.size());

            return chunkEnds;
          };

          nimble::Stream stream{.offset = offset};
          auto chunkEnds =
              getChunkEnds(rng, chunkCount, evenChunks, data.value());

          nimble::Chunk chunk;
          chunk.content.reserve(chunkEnds.size());
          size_t start = 0;
          for (size_t chunkEnd : chunkEnds) {
            chunk.content.push_back(
                data.value().substr(start, chunkEnd - start));
            start = chunkEnd;
          }
          stream.chunks.push_back(std::move(chunk));

          for (auto i = 0; i < stream.chunks[0].content.size(); ++i) {
            printData(
                fmt::format(
                    "Stripe: {}, Stream {}, Chunk {}", stripe, offset, i),
                stream.chunks[0].content[i]);
          }

          stripeData.streams.push_back(std::move(stream));
        }
      }
      stripesData.push_back(std::move(stripeData));
    }

    parameterizedTest(
        /* metadataFlushThreshold */
        1024 * 1024 * 1024, // No need to flush stripe groups
        /* metadataCompressionThreshold */ 0,
        stripesData,
        /* errorVerifier */ std::nullopt,
        /* expectedReadsPerStripe */
        [&uniqueStreamsPerStripe](uint32_t stripe) {
          return uniqueStreamsPerStripe[stripe];
        },
        /* enableStreamDedplication */ true);
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_{
      velox::memory::memoryManager()->addRootPool("TabletTest")};
  std::shared_ptr<velox::memory::MemoryPool> pool_{
      rootPool_->addLeafChild("TabletTest")};
};

// --- TabletTest parameterized tests ---

TEST_P(TabletTest, emptyWrite) {
  // Creating an Nimble file without writing any stripes
  test(/* stripes */ {});
}

TEST_P(TabletTest, writeDifferentStreamsPerStripe) {
  // Write different subset of streams in each stripe
  test(
      /* stripes */
      {
          {.rowCount = 20, .streamOffsets = {3, 1}},
          {.rowCount = 30, .streamOffsets = {2}},
          {.rowCount = 20, .streamOffsets = {4}},
      });
}

TEST_P(TabletTest, checksumValidation) {
  std::vector<uint32_t> metadataCompressionThresholds{
      // use size 0 here so it will always force a footer compression
      0,
      // use a large number here so it will not do a footer compression
      1024 * 1024 * 1024};

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (auto metadataCompressionThreshold : metadataCompressionThresholds) {
    for (auto algorithm : {nimble::ChecksumType::XXH3_64}) {
      for (auto checksumChunked : {true, false}) {
        checksumTest(
            rng,
            metadataCompressionThreshold,
            algorithm,
            checksumChunked,
            // Write different subset of streams in each stripe
            /* stripes */
            {
                {.rowCount = 20, .streamOffsets = {1}},
                {.rowCount = 30, .streamOffsets = {2}},
            });
      }
    }
  }
}

TEST_P(TabletTest, optionalSections) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};

  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});

  // size1 is above the compression threshold and may be compressed
  // size2 is below the compression threshold and won't be compressed
  // zeroes is above the compression threshold and is well compressed
  auto size1 = nimble::kMetadataCompressionThreshold +
      folly::Random::rand32(20, 2000000, rng);
  auto size2 =
      folly::Random::rand32(20, nimble::kMetadataCompressionThreshold, rng);

  std::string random1;
  std::string random2;
  std::string zeroes;
  std::string empty;

  random1.resize(size1);
  random2.resize(size2);
  zeroes.resize(size1);

  for (auto i = 0; i < size1; ++i) {
    random1[i] = folly::Random::rand32(256);
  }
  for (auto i = 0; i < size2; ++i) {
    random2[i] = folly::Random::rand32(256);
  }
  for (auto i = 0; i < size1; ++i) {
    zeroes[i] = '\0';
  }

  tabletWriter->writeOptionalSection("section1", random1);
  tabletWriter->writeOptionalSection("section2", random2);
  tabletWriter->writeOptionalSection("section3", zeroes);
  tabletWriter->writeOptionalSection("section4", empty);

  tabletWriter->close();

  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(6);
  facebook::velox::dwio::common::ExecutorBarrier barrier{executor};

  for (auto useChainedBuffers : {false, true}) {
    auto readFile =
        std::make_shared<nimble::testing::InMemoryTrackableReadFile>(
            file, useChainedBuffers);
    auto tablet = createTabletReader(readFile);

    ASSERT_EQ(tablet->optionalSections().size(), 4);

    ASSERT_TRUE(tablet->optionalSections().contains("section1"));
    ASSERT_TRUE(
        tablet->optionalSections().at("section1").compressionType() ==
            nimble::CompressionType::Uncompressed ||
        tablet->optionalSections().at("section1").compressionType() ==
            nimble::CompressionType::Zstd);
    ASSERT_LE(tablet->optionalSections().at("section1").size(), size1);

    ASSERT_TRUE(tablet->optionalSections().contains("section2"));
    ASSERT_EQ(
        tablet->optionalSections().at("section2").compressionType(),
        nimble::CompressionType::Uncompressed);
    ASSERT_EQ(tablet->optionalSections().at("section2").size(), size2);

    ASSERT_TRUE(tablet->optionalSections().contains("section3"));
    ASSERT_EQ(
        tablet->optionalSections().at("section3").compressionType(),
        nimble::CompressionType::Zstd);
    ASSERT_LE(tablet->optionalSections().at("section3").size(), zeroes.size());

    ASSERT_TRUE(tablet->optionalSections().contains("section4"));
    ASSERT_EQ(
        tablet->optionalSections().at("section4").compressionType(),
        nimble::CompressionType::Uncompressed);
    ASSERT_EQ(tablet->optionalSections().at("section4").size(), 0);

    auto check1 = [&]() {
      auto section = tablet->loadOptionalSection("section1");
      ASSERT_TRUE(section.has_value());
      ASSERT_EQ(random1, section->content());
    };

    auto check2 = [&]() {
      auto section = tablet->loadOptionalSection("section2");
      ASSERT_TRUE(section.has_value());
      ASSERT_EQ(random2, section->content());
    };

    auto check3 = [&]() {
      auto section = tablet->loadOptionalSection("section3");
      ASSERT_TRUE(section.has_value());
      ASSERT_EQ(zeroes, section->content());
    };

    auto check4 = [&, emptyStr = std::string()]() {
      auto section = tablet->loadOptionalSection("section4");
      ASSERT_TRUE(section.has_value());
      ASSERT_EQ(emptyStr, section->content());
    };

    auto check5 = [&]() {
      auto section = tablet->loadOptionalSection("section5");
      ASSERT_FALSE(section.has_value());
    };

    for (int i = 0; i < 10; ++i) {
      barrier.add(check1);
      barrier.add(check2);
      barrier.add(check3);
      barrier.add(check4);
      barrier.add(check5);
    }
    barrier.waitAll();
  }
}

TEST_P(TabletTest, optionalSectionsEmpty) {
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});

  tabletWriter->close();

  for (auto useChainedBuffers : {false, true}) {
    auto readFile =
        std::make_shared<nimble::testing::InMemoryTrackableReadFile>(
            file, useChainedBuffers);
    auto tablet = createTabletReader(readFile);

    ASSERT_TRUE(tablet->optionalSections().empty());

    auto section = tablet->loadOptionalSection("section1");
    ASSERT_FALSE(section.has_value());
  }
}

TEST_P(TabletTest, hasOptionalSection) {
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});

  // Write some optional sections
  tabletWriter->writeOptionalSection("section1", "data1");
  tabletWriter->writeOptionalSection("section2", "data2");
  tabletWriter->writeOptionalSection("section3", "data3");

  tabletWriter->close();

  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, true);
  auto tablet = createTabletReader(readFile);

  // Test that hasOptionalSection returns true for existing sections
  EXPECT_TRUE(tablet->hasOptionalSection("section1"));
  EXPECT_TRUE(tablet->hasOptionalSection("section2"));
  EXPECT_TRUE(tablet->hasOptionalSection("section3"));

  // Test that hasOptionalSection returns false for non-existing sections
  EXPECT_FALSE(tablet->hasOptionalSection("section4"));
  EXPECT_FALSE(tablet->hasOptionalSection("nonexistent"));
  EXPECT_FALSE(tablet->hasOptionalSection(""));
  EXPECT_FALSE(
      tablet->hasOptionalSection(std::string(nimble::kClusterIndexSection)));
}

TEST_P(TabletTest, hasOptionalSectionEmpty) {
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});

  tabletWriter->close();

  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, true);
  auto tablet = createTabletReader(readFile);

  // Test that hasOptionalSection returns false when there are no sections
  EXPECT_FALSE(tablet->hasOptionalSection("section1"));
  EXPECT_FALSE(tablet->hasOptionalSection(""));
  EXPECT_FALSE(tablet->hasOptionalSection("any_name"));
}

TEST_P(TabletTest, optionalSectionsPreload) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};

  for ([[maybe_unused]] const auto footerCompressionThreshold :
       {0U, std::numeric_limits<uint32_t>::max()}) {
    std::string file;
    velox::InMemoryWriteFile writeFile(&file);
    auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});

    // Using random string to make sure compression can't compress it well
    std::string random;
    random.resize(20 * 1024 * 1024);
    for (auto i = 0; i < random.size(); ++i) {
      random[i] = folly::Random::rand32(256);
    }

    tabletWriter->writeOptionalSection("section1", "aaaa");
    tabletWriter->writeOptionalSection("section2", "bbbb");
    tabletWriter->writeOptionalSection("section3", random);
    tabletWriter->writeOptionalSection("section4", "dddd");
    tabletWriter->writeOptionalSection("section5", "eeee");
    tabletWriter->close();

    auto verify = [&](std::vector<std::string> preload,
                      size_t expectedInitialReads,
                      std::vector<std::tuple<std::string, size_t, std::string>>
                          expected) {
      for (auto useChainedBuffers : {false, true}) {
        auto readFile =
            std::make_shared<nimble::testing::InMemoryTrackableReadFile>(
                file, useChainedBuffers);
        nimble::TabletReader::Options options;
        options.preloadOptionalSections = preload;
        configureMetadataInput(options);
        auto tablet =
            nimble::TabletReader::create(readFile, pool_.get(), options);

        // Expecting only the initial footer read.
        ASSERT_EQ(expectedInitialReads, readFile->chunks().size());

        for (const auto& e : expected) {
          auto expectedSection = std::get<0>(e);
          auto expectedReads = std::get<1>(e);
          auto expectedContent = std::get<2>(e);
          auto section = tablet->loadOptionalSection(expectedSection);
          ASSERT_TRUE(section.has_value());
          ASSERT_EQ(expectedContent, section->content());
          ASSERT_EQ(expectedReads, readFile->chunks().size());
        }
      }
    };

    // Not preloading anything.
    // Initial reads: single speculative preadv for footer+PS (1).
    // Each section load adds one preadv via MetadataInput.
    verify(
        /* preload */ {},
        /* expectedInitialReads */ 1,
        {
            {"section1", 2, "aaaa"},
            {"section2", 3, "bbbb"},
            {"section3", 4, random},
            {"section4", 5, "dddd"},
            {"section5", 6, "eeee"},
            {"section1", 7, "aaaa"},
        });

    // Preloading small section covered by speculative IOBuf.
    // Initial reads: speculative preadv (1).
    verify(
        /* preload */ {"section4"},
        /* expectedInitialReads */ 1,
        {
            {"section1", 2, "aaaa"},
            {"section2", 3, "bbbb"},
            {"section3", 4, random},
            // This is the preloaded section, so it should not trigger a read
            {"section4", 4, "dddd"},
            {"section5", 5, "eeee"},
            // This is the preloaded section, but the section was already
            // consumed, so it should now trigger a read.
            {"section4", 6, "dddd"},
        });

    // Preloading section not covered by speculative IOBuf.
    // Initial reads: speculative preadv (1) + preload read (2).
    verify(
        /* preload */ {"section3"},
        /* expectedInitialReads */ 2,
        {
            {"section1", 3, "aaaa"},
            {"section2", 4, "bbbb"},
            // This is the preloaded section, so it should not trigger a read
            {"section3", 4, random},
            {"section4", 5, "dddd"},
            {"section5", 6, "eeee"},
            // This is the preloaded section, but the section was already
            // consumed, so it should now trigger a read.
            {"section3", 7, random},
        });

    // Preloading section completely not covered by speculative IOBuf.
    // Initial reads: speculative preadv (1) + preload read (2).
    verify(
        /* preload */ {"section1"},
        /* expectedInitialReads */ 2,
        {
            // This is the preloaded section, so it should not trigger a read
            {"section1", 2, "aaaa"},
            {"section2", 3, "bbbb"},
            {"section3", 4, random},
            {"section4", 5, "dddd"},
            {"section5", 6, "eeee"},
            // This is the preloaded section, but the section was already
            // consumed, so it should now trigger a read.
            {"section1", 7, "aaaa"},
        });

    // Preloading multiple sections. One covered, and the other is not.
    // Initial reads: speculative preadv (1) + preload read (2).
    verify(
        /* preload */ {"section2", "section4"},
        /* expectedInitialReads */ 2,
        {
            {"section1", 3, "aaaa"},
            // This is one of the preloaded sections, so it should not trigger a
            // read
            {"section2", 3, "bbbb"},
            {"section3", 4, random},
            // This is the other preloaded sections, so it should not trigger a
            // read
            {"section4", 4, "dddd"},
            {"section5", 5, "eeee"},
            // This is one of the preloaded section, but the section was already
            // consumed, so it should now trigger a read.
            {"section2", 6, "bbbb"},
            // This is the other preloaded section, but the section was already
            // consumed, so it should now trigger a read.
            {"section4", 7, "dddd"},
        });

    // Preloading all sections. Two are fully covered, and three are
    // partially or not covered.
    // Initial reads: speculative preadv (1) + preload reads for uncovered
    // sections.
    // With MetadataInput coalescing, the number of initial reads depends
    // on how many uncovered sections are coalesced. Skip exact count check
    // and verify content correctness only.
    {
      auto readFile =
          std::make_shared<nimble::testing::InMemoryTrackableReadFile>(
              file, /*useChainedBuffers=*/false);
      nimble::TabletReader::Options options;
      options.preloadOptionalSections = {
          "section2", "section4", "section1", "section3", "section5"};
      configureMetadataInput(options);
      auto tablet =
          nimble::TabletReader::create(readFile, pool_.get(), options);

      for (const auto& name :
           {"section1", "section2", "section3", "section4", "section5"}) {
        auto section = tablet->loadOptionalSection(name);
        ASSERT_TRUE(section.has_value());
      }
      const auto readsAfterPreload = readFile->chunks().size();

      // Re-loading consumed sections should trigger new reads.
      for (const auto& name :
           {"section1", "section2", "section3", "section4", "section5"}) {
        auto section = tablet->loadOptionalSection(name);
        ASSERT_TRUE(section.has_value());
      }
      EXPECT_GT(readFile->chunks().size(), readsAfterPreload);
    }
  }
}

TEST_P(TabletTest, deduplicateStreams) {
  auto seed = FLAGS_tablet_tests_seed > 0 ? FLAGS_tablet_tests_seed
                                          : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (auto noDuplicates : {true, false}) {
    for (auto chunkCount : {1U, 2U, folly::Random::rand32(10, rng) + 1}) {
      for (auto evenChunks : {true, false}) {
        SCOPED_TRACE(
            fmt::format(
                "noDuplicates={}, chunkCount={}, evenChunks={}",
                noDuplicates,
                chunkCount,
                evenChunks));
        testStreamDeduplication(rng, noDuplicates, chunkCount, evenChunks);
      }
    }
  }
}

TEST_P(TabletTest, chunkContentSize) {
  nimble::Chunk chunk;
  EXPECT_EQ(chunk.contentSize(), 0);

  chunk.content = {"hello"};
  EXPECT_EQ(chunk.contentSize(), 5);

  chunk.content = {"hello", "world"};
  EXPECT_EQ(chunk.contentSize(), 10);

  chunk.content = {"", "abc", ""};
  EXPECT_EQ(chunk.contentSize(), 3);
}

TEST_P(TabletTest, streamSize) {
  // Write a file with 2 stripes and verify streamSize API.
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {.streamDeduplicationEnabled = false, .enableChunkIndex = true});

  nimble::Buffer buffer{*pool_};

  // Stripe 0: 3 streams with sizes 10, 20, 8.
  {
    std::vector<nimble::Stream> streams;
    streams.push_back(
        nimble::index::test::createStream(
            buffer, {.offset = 0, .chunks = {{.rowCount = 50, .size = 10}}}));
    streams.push_back(
        nimble::index::test::createStream(
            buffer, {.offset = 1, .chunks = {{.rowCount = 50, .size = 20}}}));
    streams.push_back(
        nimble::index::test::createStream(
            buffer, {.offset = 2, .chunks = {{.rowCount = 50, .size = 8}}}));
    tabletWriter->writeStripe(50, std::move(streams));
  }

  // Stripe 1: 2 streams (fewer than stripe 0).
  {
    std::vector<nimble::Stream> streams;
    streams.push_back(
        nimble::index::test::createStream(
            buffer, {.offset = 0, .chunks = {{.rowCount = 30, .size = 15}}}));
    streams.push_back(
        nimble::index::test::createStream(
            buffer, {.offset = 1, .chunks = {{.rowCount = 30, .size = 25}}}));
    tabletWriter->writeStripe(30, std::move(streams));
  }

  tabletWriter->close();
  writeFile.close();

  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = createTabletReader(readFile, nimble::TabletReader::Options{});

  struct TestCase {
    uint32_t stripeIndex;
    uint32_t streamId;
    uint32_t expectedSize;

    std::string debugString() const {
      return fmt::format(
          "stripe {}, stream {}, expected {}",
          stripeIndex,
          streamId,
          expectedSize);
    }
  };

  std::vector<TestCase> testCases = {
      // Stripe 0: 3 streams with sizes 10, 20, 8.
      {0, 0, 10},
      {0, 1, 20},
      {0, 2, 8},
      // Out of range returns 0.
      {0, 3, 0},
      {0, 100, 0},
      // Stripe 1: 2 streams with sizes 15, 25.
      {1, 0, 15},
      {1, 1, 25},
      // Stream 2 doesn't exist in stripe 1.
      {1, 2, 0},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());
    auto stripe = tablet->stripeIdentifier(testCase.stripeIndex);
    EXPECT_EQ(
        tablet->streamSize(stripe, testCase.streamId), testCase.expectedSize);
  }
}

// TabletWithIndexTest is a derived test fixture for tablet index related tests.
// It inherits the memory pool and helper methods from TabletTest.
class TabletWithIndexTest : public TabletTest {
 protected:
  // Override to enable index loading by default for index tests.
  std::shared_ptr<nimble::TabletReader> createTabletReader(
      std::shared_ptr<velox::ReadFile> readFile,
      nimble::TabletReader::Options options = {}) {
    options.loadClusterIndex = true;
    options.loadDenseIndexes = true;
    return TabletTest::createTabletReader(
        std::move(readFile), std::move(options));
  }

  // Use shared test data structs from ClusterIndexTestUtils.h
  using ChunkSpec = nimble::index::test::ChunkSpec;
  using KeyChunkSpec = nimble::index::test::KeyChunkSpec;
  using StreamSpec = nimble::index::test::StreamSpec;

  // Specification for testing index lookup results
  struct LookupTestCase {
    std::string key;
    std::optional<nimble::RowRange>
        expectedRange; // nullopt means no match expected
  };

  // Specification for key lookup verification through
  // ClusterIndex::lookupChunk
  struct KeyLookupTestCase {
    std::string key; // The key to look up
    uint32_t expectedStripeIndex; // Expected stripe index
    uint32_t expectedFileRowId; // Expected file row ID (from file start)
  };

  // Helper to create a Stream using the shared implementation
  static nimble::Stream createStream(
      nimble::Buffer& buffer,
      const StreamSpec& spec) {
    return nimble::index::test::createStream(buffer, spec);
  }

  // Helper to create multiple streams from specifications
  static std::vector<nimble::Stream> createStreams(
      nimble::Buffer& buffer,
      const std::vector<StreamSpec>& specs) {
    std::vector<nimble::Stream> streams;
    streams.reserve(specs.size());
    for (const auto& spec : specs) {
      streams.push_back(createStream(buffer, spec));
    }
    return streams;
  }

  // Helper to verify tablet index lookups.
  // Uses lower-bound-only lookup to test key-to-row resolution:
  // startRow = first row >= key, endRow = total file rows.
  // Keys beyond all partitions produce empty results.
  static void verifyClusterIndexLookups(
      const nimble::ClusterIndex* index,
      const std::vector<LookupTestCase>& testCases) {
    for (const auto& testCase : testCases) {
      SCOPED_TRACE(fmt::format("key '{}'", testCase.key));
      std::vector<velox::serializer::EncodedKeyBounds> keyBounds = {
          {.lowerKey = testCase.key}};
      auto result = index->lookup(
          nimble::index::IndexLookup::LookupRequest::rangeScan(keyBounds));
      ASSERT_EQ(result.size(), 1);
      if (testCase.expectedRange.has_value()) {
        ASSERT_EQ(result[0].size(), 1);
        EXPECT_EQ(result[0][0].startRow, testCase.expectedRange->startRow);
        EXPECT_EQ(result[0][0].endRow, testCase.expectedRange->endRow);
      } else {
        EXPECT_TRUE(result[0].empty());
      }
    }
  }

  // Helper to verify chunk offsets from index are within stream bounds.
  // This verifies that:
  // 1. All chunk offsets are within [0, streamSize) for the corresponding
  // stream
  // 2. Chunk offsets are monotonically non-decreasing within a stripe
  // The stream offsets and sizes come from the stripe group metadata.
  static void verifyStreamChunkStats(
      const nimble::TabletReader& tablet,
      uint32_t stripeIdx,
      uint32_t streamId,
      const nimble::index::test::StreamStats& streamStats) {
    auto stripeIdentifier = tablet.stripeIdentifier(stripeIdx);

    ASSERT_NE(stripeIdentifier.stripeGroup(), nullptr)
        << "Stripe group should be available for stripe " << stripeIdx;

    // Get stream sizes from stripe group metadata
    auto streamSizes = tablet.streamSizes(stripeIdentifier);
    ASSERT_GT(streamSizes.size(), streamId)
        << "Stream " << streamId << " not found in stripe " << stripeIdx;

    const uint32_t streamSize = streamSizes[streamId];

    // Calculate stripe offset within the chunk index group.
    // Use the ChunkIndexTestHelper to get the first stripe of the group.
    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeIdentifier.chunkIndex().get());
    const uint32_t stripeOffsetInGroup = stripeIdx - chunkHelper.firstStripe();

    // streamStats.chunkCounts now contains accumulated chunk counts for this
    // stream only across stripes. We can directly use these to calculate
    // start index and chunk count for this stripe.
    //
    // For example, if chunkCounts = [3, 7, 9] for stripes 0, 1, 2:
    // - Stripe 0: 3 chunks, start at index 0
    // - Stripe 1: 4 chunks (7-3), start at index 3
    // - Stripe 2: 2 chunks (9-7), start at index 7

    const uint32_t prevCount = (stripeOffsetInGroup == 0)
        ? 0
        : streamStats.chunkCounts[stripeOffsetInGroup - 1];
    const uint32_t currCount = streamStats.chunkCounts[stripeOffsetInGroup];
    const uint32_t numChunksInStripe = currCount - prevCount;
    const uint32_t startChunkIdx = prevCount;

    // Verify chunk offsets are within stream bounds and monotonically
    // increasing
    uint32_t prevOffset = 0;
    for (uint32_t i = 0; i < numChunksInStripe; ++i) {
      uint32_t chunkOffset = streamStats.chunkOffsets[startChunkIdx + i];

      // Chunk offset must be within stream bounds
      EXPECT_LT(chunkOffset, streamSize)
          << "Chunk " << i << " offset " << chunkOffset
          << " exceeds stream size " << streamSize << " for stream " << streamId
          << " in stripe " << stripeIdx;

      // Chunk offsets must be monotonically non-decreasing
      if (i > 0) {
        EXPECT_GE(chunkOffset, prevOffset)
            << "Chunk " << i << " offset " << chunkOffset
            << " is less than previous chunk offset " << prevOffset
            << " for stream " << streamId << " in stripe " << stripeIdx;
      }
      prevOffset = chunkOffset;
    }
  }

  // Helper to verify key lookups through ClusterIndex::lookupChunk.
  // This verifies that:
  // 1. Each key can be looked up through the cluster index
  // 2. The returned row offset is correct (partition-wide)
  // 3. The file row ID (partition start row + row offset) matches expected
  // value
  //
  // lookupChunk returns partition-wide row offsets (accumulated across all
  // sub-partitions in the partition). File row ID = start row of partition's
  // first stripe + row offset.
  static void verifyKeyLookupFileRowIds(
      const nimble::TabletReader& tablet,
      const std::vector<KeyLookupTestCase>& testCases) {
    nimble::test::TabletReaderTestHelper tabletHelper(&tablet);

    // Pre-compute stripe start row offsets
    std::vector<uint64_t> stripeStartRows;
    stripeStartRows.reserve(tablet.stripeCount());
    uint64_t startRow = 0;
    for (uint32_t i = 0; i < tablet.stripeCount(); ++i) {
      stripeStartRows.push_back(startRow);
      startRow += tablet.stripeRowCount(i);
    }

    // Pre-compute partition start rows (row offset of first stripe in each
    // partition).
    uint32_t maxPartition = 0;
    for (uint32_t i = 0; i < tablet.stripeCount(); ++i) {
      maxPartition = std::max(maxPartition, tabletHelper.stripeGroupIndex(i));
    }
    std::vector<uint64_t> partitionStartRows(maxPartition + 1, UINT64_MAX);
    for (uint32_t i = 0; i < tablet.stripeCount(); ++i) {
      const uint32_t partitionIndex = tabletHelper.stripeGroupIndex(i);
      if (partitionStartRows[partitionIndex] == UINT64_MAX) {
        partitionStartRows[partitionIndex] = stripeStartRows[i];
      }
    }

    const auto* clusterIndex = tablet.clusterIndex();
    ASSERT_NE(clusterIndex, nullptr) << "Cluster index should be available";
    nimble::index::test::ClusterIndexTestHelper indexHelper(clusterIndex);

    for (const auto& testCase : testCases) {
      const uint32_t partitionIndex =
          tabletHelper.stripeGroupIndex(testCase.expectedStripeIndex);

      // lookupChunk returns partition-wide row offsets.
      const auto chunkLocation =
          indexHelper.lookupChunk(partitionIndex, testCase.key);

      // Convert partition-wide row offset to file-level row ID.
      const uint64_t globalRowId =
          partitionStartRows[partitionIndex] + chunkLocation.rowOffset;

      EXPECT_EQ(globalRowId, testCase.expectedFileRowId)
          << "Key '" << testCase.key << "' in stripe "
          << testCase.expectedStripeIndex << ": expected file row ID "
          << testCase.expectedFileRowId << ", got " << globalRowId
          << " (partition start row = " << partitionStartRows[partitionIndex]
          << ", row offset = " << chunkLocation.rowOffset << ")";
    }
  }
};

TEST_P(TabletWithIndexTest, stripeIdentifier) {
  // Test that stripeIdentifier returns both stripe group and cluster index.
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);

  nimble::index::test::TestClusterIndexMetadataWriter indexHelper(
      *pool_,
      {"col1"},
      {SortOrder{.ascending = true}},
      /*enforceKeyOrder=*/true);

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          .metadataFlushThreshold = 1024 * 1024 * 1024,
          .streamDeduplicationEnabled = false,
          .enableChunkIndex = true,
          .stripeGroupFlushCallback =
              indexHelper.createStripeGroupFlushCallback(),
          .closeCallback = indexHelper.createCloseCallback(),
      });

  nimble::Buffer buffer{*pool_};

  // Write single stripe with index
  auto streams = createStreams(
      buffer, {{.offset = 0, .chunks = {{.rowCount = 100, .size = 10}}}});
  indexHelper.addStripe({{.rowCount = 100, .key = "bbb"}});
  tabletWriter->writeStripe(100, std::move(streams));
  tabletWriter->close();

  // Read and verify
  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto tablet = createTabletReader(readFile);

  ASSERT_EQ(tablet->stripeCount(), 1);
  ASSERT_NE(tablet->clusterIndex(), nullptr);

  auto stripeId = tablet->stripeIdentifier(0);
  EXPECT_NE(stripeId.stripeGroup(), nullptr);
}

TEST_P(TabletWithIndexTest, singleGroup) {
  // Test writing a tablet with index configuration and reading it back.
  // Each stream has multiple chunks with varying row counts and sizes.
  // Different streams are not synchronized in chunk count, rows, or size.
  // This test ensures only one stripe group is created.
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);

  nimble::index::test::TestClusterIndexMetadataWriter indexHelper(
      *pool_,
      {"col1", "col2"},
      {SortOrder{.ascending = true}, SortOrder{.ascending = true}},
      /*enforceKeyOrder=*/true);

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          // Set a large threshold to ensure all stripes stay in one group
          .metadataFlushThreshold = 1024 * 1024 * 1024,
          .streamDeduplicationEnabled = false,
          .enableChunkIndex = true,
          .stripeGroupFlushCallback =
              indexHelper.createStripeGroupFlushCallback(),
          .closeCallback = indexHelper.createCloseCallback(),
      });

  nimble::Buffer buffer{*pool_};

  // Write stripe 1: Total rows: 100
  // Stream 0: 3 chunks (rows: 30, 45, 25)
  // Stream 1: 2 chunks (rows: 60, 40)
  // Key stream: 2 chunks (rows: 50, 50), keys: "bbb", "ccc"
  {
    auto streams = createStreams(
        buffer,
        {
            {.offset = 0,
             .chunks =
                 {
                     {.rowCount = 30, .size = 10},
                     {.rowCount = 45, .size = 15},
                     {.rowCount = 25, .size = 8},
                 }},
            {.offset = 1,
             .chunks =
                 {
                     {.rowCount = 60, .size = 20},
                     {.rowCount = 40, .size = 12},
                 }},
        });

    indexHelper.addStripe({
        {.rowCount = 50, .key = "bbb"},
        {.rowCount = 50, .key = "ccc"},
    });
    tabletWriter->writeStripe(100, std::move(streams));
  }

  // Write stripe 2: Total rows: 200
  // Stream 0: 4 chunks (rows: 40, 55, 65, 40)
  // Stream 1: 1 chunk (rows: 200)
  // Key stream: 4 chunks (rows: 50, 50, 50, 50), keys: "ddd",
  // "eee", "fff", "ggg"
  {
    auto streams = createStreams(
        buffer,
        {
            {.offset = 0,
             .chunks =
                 {
                     {.rowCount = 40, .size = 5},
                     {.rowCount = 55, .size = 18},
                     {.rowCount = 65, .size = 22},
                     {.rowCount = 40, .size = 7},
                 }},
            {.offset = 1,
             .chunks =
                 {
                     {.rowCount = 200, .size = 50},
                 }},
        });

    indexHelper.addStripe({
        {.rowCount = 50, .key = "ddd"},
        {.rowCount = 50, .key = "eee"},
        {.rowCount = 50, .key = "fff"},
        {.rowCount = 50, .key = "ggg"},
    });
    tabletWriter->writeStripe(200, std::move(streams));
  }

  // Write stripe 3: Total rows: 150
  // Stream 0: 2 chunks (rows: 100, 50)
  // Stream 1: 5 chunks (rows: 20, 35, 40, 25, 30)
  // Key stream: 1 chunk (rows: 150), key: "hhh"
  {
    auto streams = createStreams(
        buffer,
        {
            {.offset = 0,
             .chunks =
                 {
                     {.rowCount = 100, .size = 30},
                     {.rowCount = 50, .size = 14},
                 }},
            {.offset = 1,
             .chunks =
                 {
                     {.rowCount = 20, .size = 6},
                     {.rowCount = 35, .size = 9},
                     {.rowCount = 40, .size = 11},
                     {.rowCount = 25, .size = 4},
                     {.rowCount = 30, .size = 16},
                 }},
        });

    indexHelper.addStripe({
        {.rowCount = 150, .key = "hhh"},
    });
    tabletWriter->writeStripe(150, std::move(streams));
  }

  tabletWriter->close();
  writeFile.close();

  // Read and verify the tablet
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = createTabletReader(readFile);

  // Use test helper to verify stripe group count through the read path
  nimble::test::TabletReaderTestHelper tabletHelper(tablet.get());
  EXPECT_EQ(tabletHelper.numStripeGroups(), 1);

  // Verify basic tablet properties
  EXPECT_EQ(tablet->stripeCount(), 3);
  EXPECT_EQ(tablet->tabletRowCount(), 450);
  EXPECT_EQ(tablet->stripeRowCount(0), 100);
  EXPECT_EQ(tablet->stripeRowCount(1), 200);
  EXPECT_EQ(tablet->stripeRowCount(2), 150);

  // Verify index section exists
  EXPECT_TRUE(
      tablet->hasOptionalSection(std::string(nimble::kClusterIndexSection)));

  // Verify the index is available
  const nimble::ClusterIndex* index = tablet->clusterIndex();
  ASSERT_NE(index, nullptr);

  // Verify only one index group is created
  EXPECT_EQ(index->layout().numPartitions, 1);
  // Verify the first group, index group, and chunk index group is pinned
  // and is the only one (covered by footer IO).
  EXPECT_TRUE(tabletHelper.hasOnlyFirstStripeGroupCached());
  EXPECT_TRUE(tabletHelper.hasOnlyFirstChunkIndexGroupCached());

  // Verify index columns
  EXPECT_EQ(index->indexColumns().size(), 2);
  EXPECT_EQ(index->indexColumns()[0], "col1");
  EXPECT_EQ(index->indexColumns()[1], "col2");

  // Verify key lookups - test all possible keys at boundaries.
  // lookup() returns chunk-level precision: the range starts at the first
  // chunk whose max key >= the query key, and extends to partition end.
  //
  // Chunk layout (all in partition 0, rows 0-449):
  //   Chunk 0: rows [0, 50),   max key "bbb"
  //   Chunk 1: rows [50, 100), max key "ccc"
  //   Chunk 2: rows [100, 150), max key "ddd"
  //   Chunk 3: rows [150, 200), max key "eee"
  //   Chunk 4: rows [200, 250), max key "fff"
  //   Chunk 5: rows [250, 300), max key "ggg"
  //   Chunk 6: rows [300, 450), max key "hhh"
  const nimble::RowRange chunk0(0, 450);
  const nimble::RowRange chunk1(50, 450);
  const nimble::RowRange chunk2(100, 450);
  const nimble::RowRange chunk3(150, 450);
  const nimble::RowRange chunk4(200, 450);
  const nimble::RowRange chunk5(250, 450);
  const nimble::RowRange chunk6(300, 450);
  verifyClusterIndexLookups(
      index,
      {
          // Keys <= "bbb" → chunk 0 (first chunk contains all keys <= "bbb")
          {"000", chunk0},
          {"9", chunk0},
          {"a", chunk0},
          {"aa", chunk0},
          {"aaa", chunk0},
          {"aab", chunk0},
          {"aaZ", chunk0},
          {"abb", chunk0},
          {"b", chunk0},
          {"bb", chunk0},
          {"bba", chunk0},
          {"bbb", chunk0},

          // Keys in (bbb, ccc] → chunk 1
          {"bbc", chunk1},
          {"c", chunk1},
          {"cc", chunk1},
          {"ccb", chunk1},
          {"ccc", chunk1},

          // Keys in (ccc, ddd] → chunk 2
          {"ccd", chunk2},
          {"d", chunk2},
          {"ddd", chunk2},

          // Keys in (ddd, eee] → chunk 3
          {"dde", chunk3},
          {"e", chunk3},
          {"eee", chunk3},

          // Keys in (eee, fff] → chunk 4
          {"eef", chunk4},
          {"f", chunk4},
          {"fff", chunk4},

          // Keys in (fff, ggg] → chunk 5
          {"ffg", chunk5},
          {"g", chunk5},
          {"gg", chunk5},
          {"ggf", chunk5},
          {"ggg", chunk5},

          // Keys in (ggg, hhh] → chunk 6
          {"ggh", chunk6},
          {"h", chunk6},
          {"hh", chunk6},
          {"hhg", chunk6},
          {"hhh", chunk6},

          // Keys after maxKey - should return nullopt
          {"hhi", std::nullopt},
          {"i", std::nullopt},
          {"iii", std::nullopt},
          {"z", std::nullopt},
          {"zzz", std::nullopt},
      });

  // Verify stripeIdentifier returns stripe group and cluster index content
  {
    auto stripeIdWithIndex = tablet->stripeIdentifier(0);
    EXPECT_NE(stripeIdWithIndex.stripeGroup(), nullptr);

    nimble::index::test::ClusterIndexTestHelper clusterIndexTestHelper(
        tablet->clusterIndex());

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeIdWithIndex.chunkIndex().get());
    // We have 2 streams per stripe
    EXPECT_EQ(chunkHelper.streamCount(), 2);

    // Verify partition stats persisted in the index
    // Stripe 0: 2 chunks (rows: 50, 50), keys: "bbb", "ccc"
    // Stripe 1: 4 chunks (rows: 50, 50, 50, 50), keys: "ddd",
    // "eee", "fff", "ggg"
    // Stripe 2: 1 chunk (rows: 150), key: "hhh"
    {
      const auto stats = clusterIndexTestHelper.partitionStats(0);

      // Accumulated row counts per chunk (cumulative across partition)
      // Stripe 0: 50, 100
      // Stripe 1: 100+50=150, 100+100=200, 100+150=250, 100+200=300
      // Stripe 2: 300+150=450
      EXPECT_EQ(
          stats.chunkRows,
          (std::vector<uint32_t>{50, 100, 150, 200, 250, 300, 450}));

      // Last key for each sub-partition
      EXPECT_EQ(
          stats.chunkKeys,
          (std::vector<std::string>{
              "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh"}));
    }

    // Verify stream 0 position index stats
    // Stripe 0: 3 chunks (rows: 30, 45, 25)
    // Stripe 1: 4 chunks (rows: 40, 55, 65, 40)
    // Stripe 2: 2 chunks (rows: 100, 50)
    {
      auto stream0Stats = chunkHelper.streamStats(0);

      // Per-stream accumulated chunk counts:
      // Stripe 0: 3 chunks -> accumulated = 3
      // Stripe 1: 4 chunks -> accumulated = 3 + 4 = 7
      // Stripe 2: 2 chunks -> accumulated = 7 + 2 = 9
      EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{3, 7, 9}));

      // Accumulated row counts per chunk
      // Stripe 0: 30, 75, 100
      // Stripe 1: 40, 95, 160, 200
      // Stripe 2: 100, 150
      EXPECT_EQ(
          stream0Stats.chunkRows,
          (std::vector<uint32_t>{30, 75, 100, 40, 95, 160, 200, 100, 150}));
    }

    // Verify stream 1 position index stats
    // Stripe 0: 2 chunks (rows: 60, 40)
    // Stripe 1: 1 chunk (rows: 200)
    // Stripe 2: 5 chunks (rows: 20, 35, 40, 25, 30)
    {
      auto stream1Stats = chunkHelper.streamStats(1);

      // Per-stream accumulated chunk counts for stream 1:
      // Stripe 0: 2 chunks -> accumulated = 2
      // Stripe 1: 1 chunk -> accumulated = 2 + 1 = 3
      // Stripe 2: 5 chunks -> accumulated = 3 + 5 = 8
      EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{2, 3, 8}));

      // Accumulated row counts per chunk
      // Stripe 0: 60, 100
      // Stripe 1: 200
      // Stripe 2: 20, 55, 95, 120, 150
      EXPECT_EQ(
          stream1Stats.chunkRows,
          (std::vector<uint32_t>{60, 100, 200, 20, 55, 95, 120, 150}));
    }
  }
  // Verify chunk offsets from index match actual stream positions
  // This tests the position index chunk offsets are correctly persisted
  // and can be used to locate chunks within streams
  {
    for (uint32_t stripeIdx = 0; stripeIdx < 3; ++stripeIdx) {
      auto stripeId = tablet->stripeIdentifier(stripeIdx);
      nimble::index::test::ChunkIndexTestHelper chunkHelper(
          stripeId.chunkIndex().get());
      for (uint32_t streamId = 0; streamId < 2; ++streamId) {
        auto streamStats = chunkHelper.streamStats(streamId);
        verifyStreamChunkStats(*tablet, stripeIdx, streamId, streamStats);
      }
    }
  }
  // Verify key lookups through ClusterIndex::lookupChunk return correct
  // global row IDs.
  // Global row ID = stripe start row + row offset within stripe
  //
  // Stripe layout:
  // - Stripe 0: rows 0-99 (100 rows), start row = 0
  // - Stripe 1: rows 100-299 (200 rows), start row = 100
  // - Stripe 2: rows 300-449 (150 rows), start row = 300
  //
  // Key stream chunks (each chunk's rowOffset is accumulated rows before it):
  // Stripe 0: chunk 0 (rows 0-49, rowOffset=0), chunk 1 (rows 50-99,
  // rowOffset=50) Stripe 1: chunk 0 (rows 0-49, rowOffset=0), chunk 1 (rows
  // 50-99, rowOffset=50),
  //           chunk 2 (rows 100-149, rowOffset=100), chunk 3 (rows 150-199,
  //           rowOffset=150)
  // Stripe 2: chunk 0 (rows 0-149, rowOffset=0)
  verifyKeyLookupFileRowIds(
      *tablet,
      {
          // Stripe 0: global rows 0-99
          // Keys in chunk 0 (<=  "bbb"): rowOffset = 0, globalRowId = 0 + 0
          // = 0
          {"aaa", 0, 0},
          {"aab", 0, 0},
          {"bbb", 0, 0},
          // Keys in chunk 1 ("bbb", "ccc"]: rowOffset = 50, globalRowId = 0 +
          // 50 = 50
          {"bbc", 0, 50},
          {"ccc", 0, 50},

          // Stripe 1: global rows 100-299
          // Keys in chunk 0 ("ccc", "ddd"]: rowOffset = 0, globalRowId = 100 +
          // 0 = 100
          {"ccd", 1, 100},
          {"ddd", 1, 100},
          // Keys in chunk 1 ("ddd", "eee"]: rowOffset = 50, globalRowId = 100 +
          // 50 = 150
          {"dde", 1, 150},
          {"eee", 1, 150},
          // Keys in chunk 2 ("eee", "fff"]: rowOffset = 100, globalRowId = 100
          // + 100 = 200
          {"eef", 1, 200},
          {"fff", 1, 200},
          // Keys in chunk 3 ("fff", "ggg"]: rowOffset = 150, globalRowId = 100
          // + 150 = 250
          {"ffg", 1, 250},
          {"ggg", 1, 250},

          // Stripe 2: global rows 300-449
          // Keys in chunk 0 ("ggg", "hhh"]: rowOffset = 0, globalRowId = 300 +
          // 0 = 300
          {"ggh", 2, 300},
          {"hhh", 2, 300},
      });
}

TEST_P(TabletWithIndexTest, multipleGroups) {
  // Test writing a tablet with index configuration and multiple stripe groups.
  // With metadataFlushThreshold = 0, each stripe is in its own group:
  // - Group 0: stripe 0
  // - Group 1: stripe 1
  // - Group 2: stripe 2
  // Each stripe has 2 streams with varying chunk counts to test:
  // - Index lookups across group boundaries
  // - Stream stats in single-stripe groups
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);

  nimble::index::test::TestClusterIndexMetadataWriter indexHelper(
      *pool_,
      {"col1", "col2"},
      {SortOrder{.ascending = true}, SortOrder{.ascending = true}},
      /*enforceKeyOrder=*/true);

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          // Set threshold to 0 to force flush after every stripe
          .metadataFlushThreshold = 0,
          .streamDeduplicationEnabled = false,
          .enableChunkIndex = true,
          .stripeGroupFlushCallback =
              indexHelper.createStripeGroupFlushCallback(),
          .closeCallback = indexHelper.createCloseCallback(),
      });

  nimble::Buffer buffer{*pool_};

  // Write stripe 0 (Group 0): Total rows: 100
  // Stream 0: 2 chunks (rows: 50, 50)
  // Stream 1: 3 chunks (rows: 30, 40, 30)
  // Key stream: 2 chunks (rows: 50, 50), keys: "bbb", "ccc"
  {
    auto streams = createStreams(
        buffer,
        {
            {.offset = 0,
             .chunks =
                 {
                     {.rowCount = 50, .size = 10},
                     {.rowCount = 50, .size = 12},
                 }},
            {.offset = 1,
             .chunks =
                 {
                     {.rowCount = 30, .size = 8},
                     {.rowCount = 40, .size = 10},
                     {.rowCount = 30, .size = 6},
                 }},
        });

    indexHelper.addStripe({
        {.rowCount = 50, .key = "bbb"},
        {.rowCount = 50, .key = "ccc"},
    });
    tabletWriter->writeStripe(100, std::move(streams));
  }

  // Write stripe 1 (Group 1): Total rows: 150
  // Stream 0: 3 chunks (rows: 40, 60, 50)
  // Stream 1: 2 chunks (rows: 80, 70)
  // Key stream: 3 chunks (rows: 50, 50, 50), keys: "ddd", "eee",
  // "fff"
  {
    auto streams = createStreams(
        buffer,
        {
            {.offset = 0,
             .chunks =
                 {
                     {.rowCount = 40, .size = 8},
                     {.rowCount = 60, .size = 14},
                     {.rowCount = 50, .size = 11},
                 }},
            {.offset = 1,
             .chunks =
                 {
                     {.rowCount = 80, .size = 18},
                     {.rowCount = 70, .size = 15},
                 }},
        });

    indexHelper.addStripe({
        {.rowCount = 50, .key = "ddd"},
        {.rowCount = 50, .key = "eee"},
        {.rowCount = 50, .key = "fff"},
    });
    tabletWriter->writeStripe(150, std::move(streams));
  }

  // Write stripe 2 (Group 2): Total rows: 120
  // Stream 0: 2 chunks (rows: 70, 50)
  // Stream 1: 4 chunks (rows: 25, 35, 30, 30)
  // Key stream: 2 chunks (rows: 60, 60), keys: "ggg", "hhh"
  {
    auto streams = createStreams(
        buffer,
        {
            {.offset = 0,
             .chunks =
                 {
                     {.rowCount = 70, .size = 16},
                     {.rowCount = 50, .size = 13},
                 }},
            {.offset = 1,
             .chunks =
                 {
                     {.rowCount = 25, .size = 5},
                     {.rowCount = 35, .size = 7},
                     {.rowCount = 30, .size = 6},
                     {.rowCount = 30, .size = 8},
                 }},
        });

    indexHelper.addStripe({
        {.rowCount = 60, .key = "ggg"},
        {.rowCount = 60, .key = "hhh"},
    });
    tabletWriter->writeStripe(120, std::move(streams));
  }

  tabletWriter->close();
  writeFile.close();

  // Read and verify the tablet
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = createTabletReader(readFile);

  // Use test helper to verify stripe group count
  nimble::test::TabletReaderTestHelper tabletHelper(tablet.get());
  EXPECT_EQ(tabletHelper.numStripeGroups(), 3);

  // Verify basic tablet properties
  EXPECT_EQ(tablet->stripeCount(), 3);
  EXPECT_EQ(tablet->tabletRowCount(), 370); // 100+150+120
  EXPECT_EQ(tablet->stripeRowCount(0), 100);
  EXPECT_EQ(tablet->stripeRowCount(1), 150);
  EXPECT_EQ(tablet->stripeRowCount(2), 120);

  // Verify index section exists
  EXPECT_TRUE(
      tablet->hasOptionalSection(std::string(nimble::kClusterIndexSection)));

  // Verify the index is available
  const nimble::ClusterIndex* index = tablet->clusterIndex();
  ASSERT_NE(index, nullptr);

  // Verify 3 index groups are created (one per stripe)
  EXPECT_EQ(index->layout().numPartitions, 3);

  // Verify no metadata is pinned from preload (more than one group).
  EXPECT_EQ(tabletHelper.cachedStripeGroupCount(), 0);
  EXPECT_EQ(tabletHelper.cachedChunkIndexGroupCount(), 0);

  // Verify index columns
  EXPECT_EQ(index->indexColumns().size(), 2);
  EXPECT_EQ(index->indexColumns()[0], "col1");
  EXPECT_EQ(index->indexColumns()[1], "col2");

  // Verify key lookups across all partitions with chunk-level precision.
  // Partition 0 (stripe 0, rows 0-99):
  //   Chunk 0: 50 rows, key "bbb"   → rows [0, 50)
  //   Chunk 1: 50 rows, key "ccc"   → rows [50, 100)
  // Partition 1 (stripe 1, rows 100-249):
  //   Chunk 0: 50 rows, key "ddd"   → rows [100, 150)
  //   Chunk 1: 50 rows, key "eee"   → rows [150, 200)
  //   Chunk 2: 50 rows, key "fff"   → rows [200, 250)
  // Partition 2 (stripe 2, rows 250-369):
  //   Chunk 0: 60 rows, key "ggg"   → rows [250, 310)
  //   Chunk 1: 60 rows, key "hhh"   → rows [310, 370)
  verifyClusterIndexLookups(
      index,
      {
          // Keys before minKey ("bbb") return full file range.
          {"9", nimble::RowRange(0, 370)},
          {"aa", nimble::RowRange(0, 370)},
          {"aaa", nimble::RowRange(0, 370)},
          {"aab", nimble::RowRange(0, 370)},
          {"b", nimble::RowRange(0, 370)},

          // Partition 0: keys ≤ "bbb" → row 0
          {"bbb", nimble::RowRange(0, 370)},
          // Partition 0: keys in (bbb, ccc] → row 50
          {"ccc", nimble::RowRange(50, 370)},

          // Partition 1: keys in (ccc, ddd] → row 100
          {"ccd", nimble::RowRange(100, 370)},
          {"d", nimble::RowRange(100, 370)},
          {"ddd", nimble::RowRange(100, 370)},
          // Partition 1: keys in (ddd, eee] → row 150
          {"eee", nimble::RowRange(150, 370)},
          // Partition 1: keys in (eee, fff] → row 200
          {"fff", nimble::RowRange(200, 370)},

          // Partition 2: keys in (fff, ggg] → row 250
          {"ffg", nimble::RowRange(250, 370)},
          {"g", nimble::RowRange(250, 370)},
          {"ggg", nimble::RowRange(250, 370)},
          // Partition 2: keys in (ggg, hhh] → row 310
          {"hhh", nimble::RowRange(310, 370)},

          // Keys after maxKey
          {"hhi", std::nullopt},
          {"i", std::nullopt},
      });

  // Verify partition stats and chunk index structure for each group
  nimble::index::test::ClusterIndexTestHelper clusterIndexTestHelper(
      tablet->clusterIndex());

  // Group 0: stripe 0 only
  {
    auto stripeId = tablet->stripeIdentifier(0);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeId.chunkIndex().get());
    EXPECT_EQ(chunkHelper.streamCount(), 2);

    // Verify partition stats for group 0
    const auto stats = clusterIndexTestHelper.partitionStats(0);
    EXPECT_EQ(stats.chunkRows, (std::vector<uint32_t>{50, 100}));
    EXPECT_EQ(stats.chunkKeys, (std::vector<std::string>{"bbb", "ccc"}));

    // Verify stream 0 stats for group 0: 2 chunks (rows: 50, 50)
    auto stream0Stats = chunkHelper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream0Stats.chunkRows, (std::vector<uint32_t>{50, 100}));

    // Verify stream 1 stats for group 0: 3 chunks (rows: 30, 40, 30)
    auto stream1Stats = chunkHelper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(stream1Stats.chunkRows, (std::vector<uint32_t>{30, 70, 100}));
  }

  // Group 1: stripe 1 only
  {
    auto stripeId = tablet->stripeIdentifier(1);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeId.chunkIndex().get());
    EXPECT_EQ(chunkHelper.streamCount(), 2);

    // Verify partition stats for group 1
    // Stripe 1: 3 sub-partitions
    const auto stats = clusterIndexTestHelper.partitionStats(1);
    EXPECT_EQ(stats.chunkRows, (std::vector<uint32_t>{50, 100, 150}));
    EXPECT_EQ(stats.chunkKeys, (std::vector<std::string>{"ddd", "eee", "fff"}));

    // Verify stream 0 stats for group 1
    // Stripe 1: 3 chunks (rows: 40, 60, 50)
    auto stream0Stats = chunkHelper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(stream0Stats.chunkRows, (std::vector<uint32_t>{40, 100, 150}));

    // Verify stream 1 stats for group 1
    // Stripe 1: 2 chunks (rows: 80, 70)
    auto stream1Stats = chunkHelper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream1Stats.chunkRows, (std::vector<uint32_t>{80, 150}));
  }

  // Group 2: stripe 2 only
  {
    auto stripeId = tablet->stripeIdentifier(2);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeId.chunkIndex().get());
    EXPECT_EQ(chunkHelper.streamCount(), 2);

    // Verify partition stats for group 2
    // Stripe 2: 2 sub-partitions
    const auto stats = clusterIndexTestHelper.partitionStats(2);
    EXPECT_EQ(stats.chunkRows, (std::vector<uint32_t>{60, 120}));
    EXPECT_EQ(stats.chunkKeys, (std::vector<std::string>{"ggg", "hhh"}));

    // Verify stream 0 stats for group 2
    // Stripe 2: 2 chunks (rows: 70, 50)
    auto stream0Stats = chunkHelper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream0Stats.chunkRows, (std::vector<uint32_t>{70, 120}));

    // Verify stream 1 stats for group 2
    // Stripe 2: 4 chunks (rows: 25, 35, 30, 30)
    auto stream1Stats = chunkHelper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{4}));
    EXPECT_EQ(stream1Stats.chunkRows, (std::vector<uint32_t>{25, 60, 90, 120}));
  }

  // Verify chunk offsets from index match actual stream positions
  // Test across all stripes in all groups
  {
    for (uint32_t stripeIdx = 0; stripeIdx < 3; ++stripeIdx) {
      auto stripeId = tablet->stripeIdentifier(stripeIdx);
      nimble::index::test::ChunkIndexTestHelper chunkHelper(
          stripeId.chunkIndex().get());
      for (uint32_t streamId = 0; streamId < 2; ++streamId) {
        auto streamStats = chunkHelper.streamStats(streamId);
        verifyStreamChunkStats(*tablet, stripeIdx, streamId, streamStats);
      }
    }
  }

  // Verify key lookups through ClusterIndex::lookupChunk return correct
  // global row IDs.
  // Global row ID = stripe start row + row offset within stripe
  //
  // Stripe layout:
  // - Stripe 0 (Group 0): rows 0-99 (100 rows), start row = 0
  // - Stripe 1 (Group 1): rows 100-249 (150 rows), start row = 100
  // - Stripe 2 (Group 2): rows 250-369 (120 rows), start row = 250
  //
  // Key stream chunks (each chunk's rowOffset is accumulated rows before it):
  // Stripe 0: chunk 0 (rows 0-49, rowOffset=0), chunk 1 (rows 50-99,
  // rowOffset=50) Stripe 1: chunk 0 (rows 0-49, rowOffset=0), chunk 1 (rows
  // 50-99, rowOffset=50),
  //           chunk 2 (rows 100-149, rowOffset=100)
  // Stripe 2: chunk 0 (rows 0-59, rowOffset=0), chunk 1 (rows 60-119,
  // rowOffset=60)
  verifyKeyLookupFileRowIds(
      *tablet,
      {
          // Stripe 0 (Group 0): global rows 0-99
          // Keys in chunk 0 (<= "bbb"): rowOffset = 0, globalRowId = 0 + 0
          // = 0
          {"aaa", 0, 0},
          {"bbb", 0, 0},
          // Keys in chunk 1 ("bbb", "ccc"]: rowOffset = 50, globalRowId = 0 +
          // 50 = 50
          {"bbc", 0, 50},
          {"ccc", 0, 50},

          // Stripe 1 (Group 1): global rows 100-249
          // Keys in chunk 0 ("ccc", "ddd"]: rowOffset = 0, globalRowId = 100 +
          // 0 = 100
          {"ccd", 1, 100},
          {"ddd", 1, 100},
          // Keys in chunk 1 ("ddd", "eee"]: rowOffset = 50, globalRowId = 100 +
          // 50 = 150
          {"dde", 1, 150},
          {"eee", 1, 150},
          // Keys in chunk 2 ("eee", "fff"]: rowOffset = 100, globalRowId = 100
          // + 100 = 200
          {"eef", 1, 200},
          {"fff", 1, 200},

          // Stripe 2 (Group 2): global rows 250-369
          // Keys in chunk 0 ("fff", "ggg"]: rowOffset = 0, globalRowId = 250 +
          // 0 = 250
          {"ffg", 2, 250},
          {"ggg", 2, 250},
          // Keys in chunk 1 ("ggg", "hhh"]: rowOffset = 60, globalRowId = 250 +
          // 60 = 310
          {"ggh", 2, 310},
          {"hhh", 2, 310},
      });
}

TEST_P(TabletWithIndexTest, singleGroupWithEmptyStream) {
  // Test writing a tablet with 4 streams where some streams are empty in
  // certain stripes. This tests the position index handles missing streams
  // correctly.
  //
  // Configuration (4 stripes, 4 streams):
  // - Stream 0: empty in first stripe (stripe 0)
  // - Stream 1: empty in middle stripe (stripe 1)
  // - Stream 2: empty in stripe 2
  // - Stream 3: has data in all stripes
  // - Stripe 3: ALL streams have data (no empty streams)
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);

  nimble::index::test::TestClusterIndexMetadataWriter indexHelper(
      *pool_,
      {"col1"},
      {SortOrder{.ascending = true}},
      /*enforceKeyOrder=*/true);

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          // Set a large threshold to ensure all stripes stay in one group
          .metadataFlushThreshold = 1024 * 1024 * 1024,
          .streamDeduplicationEnabled = false,
          .enableChunkIndex = true,
          .stripeGroupFlushCallback =
              indexHelper.createStripeGroupFlushCallback(),
          .closeCallback = indexHelper.createCloseCallback(),
      });

  nimble::Buffer buffer{*pool_};

  // Write stripe 0: Total rows: 100
  // Stream 0: EMPTY (no data in first stripe)
  // Stream 1: 2 chunks (rows: 50, 50)
  // Stream 2: 3 chunks (rows: 30, 40, 30)
  // Stream 3: 2 chunks (rows: 60, 40)
  // Key stream: 2 chunks (rows: 50, 50), keys: "bbb", "ccc"
  {
    auto streams = createStreams(
        buffer,
        {
            // Stream 0 is empty - not included
            {.offset = 1,
             .chunks =
                 {
                     {.rowCount = 50, .size = 10},
                     {.rowCount = 50, .size = 12},
                 }},
            {.offset = 2,
             .chunks =
                 {
                     {.rowCount = 30, .size = 8},
                     {.rowCount = 40, .size = 10},
                     {.rowCount = 30, .size = 6},
                 }},
            {.offset = 3,
             .chunks =
                 {
                     {.rowCount = 60, .size = 15},
                     {.rowCount = 40, .size = 10},
                 }},
        });

    indexHelper.addStripe({
        {.rowCount = 50, .key = "bbb"},
        {.rowCount = 50, .key = "ccc"},
    });
    tabletWriter->writeStripe(100, std::move(streams));
  }

  // Write stripe 1: Total rows: 150
  // Stream 0: 3 chunks (rows: 40, 60, 50)
  // Stream 1: EMPTY (no data in middle stripe)
  // Stream 2: 2 chunks (rows: 80, 70)
  // Stream 3: 1 chunk (rows: 150)
  // Key stream: 3 chunks (rows: 50, 50, 50), keys: "ddd", "eee",
  // "fff"
  {
    auto streams = createStreams(
        buffer,
        {
            {.offset = 0,
             .chunks =
                 {
                     {.rowCount = 40, .size = 8},
                     {.rowCount = 60, .size = 14},
                     {.rowCount = 50, .size = 11},
                 }},
            // Stream 1 is empty - not included
            {.offset = 2,
             .chunks =
                 {
                     {.rowCount = 80, .size = 18},
                     {.rowCount = 70, .size = 15},
                 }},
            {.offset = 3,
             .chunks =
                 {
                     {.rowCount = 150, .size = 30},
                 }},
        });

    indexHelper.addStripe({
        {.rowCount = 50, .key = "ddd"},
        {.rowCount = 50, .key = "eee"},
        {.rowCount = 50, .key = "fff"},
    });
    tabletWriter->writeStripe(150, std::move(streams));
  }

  // Write stripe 2: Total rows: 120
  // Stream 0: 2 chunks (rows: 70, 50)
  // Stream 1: 4 chunks (rows: 25, 35, 30, 30)
  // Stream 2: EMPTY (no data in this stripe)
  // Stream 3: 3 chunks (rows: 40, 40, 40)
  // Key stream: 2 chunks (rows: 60, 60), keys: "ggg", "hhh"
  {
    auto streams = createStreams(
        buffer,
        {
            {.offset = 0,
             .chunks =
                 {
                     {.rowCount = 70, .size = 16},
                     {.rowCount = 50, .size = 13},
                 }},
            {.offset = 1,
             .chunks =
                 {
                     {.rowCount = 25, .size = 5},
                     {.rowCount = 35, .size = 7},
                     {.rowCount = 30, .size = 6},
                     {.rowCount = 30, .size = 8},
                 }},
            // Stream 2 is empty - not included
            {.offset = 3,
             .chunks =
                 {
                     {.rowCount = 40, .size = 10},
                     {.rowCount = 40, .size = 10},
                     {.rowCount = 40, .size = 10},
                 }},
        });

    indexHelper.addStripe({
        {.rowCount = 60, .key = "ggg"},
        {.rowCount = 60, .key = "hhh"},
    });
    tabletWriter->writeStripe(120, std::move(streams));
  }

  // Write stripe 3: Total rows: 100 - ALL STREAMS HAVE DATA
  // Stream 0: 2 chunks (rows: 50, 50)
  // Stream 1: 2 chunks (rows: 40, 60)
  // Stream 2: 1 chunk (rows: 100)
  // Stream 3: 2 chunks (rows: 30, 70)
  // Key stream: 2 chunks (rows: 50, 50), keys: "iii", "jjj"
  {
    auto streams = createStreams(
        buffer,
        {
            {.offset = 0,
             .chunks =
                 {
                     {.rowCount = 50, .size = 12},
                     {.rowCount = 50, .size = 12},
                 }},
            {.offset = 1,
             .chunks =
                 {
                     {.rowCount = 40, .size = 10},
                     {.rowCount = 60, .size = 14},
                 }},
            {.offset = 2,
             .chunks =
                 {
                     {.rowCount = 100, .size = 25},
                 }},
            {.offset = 3,
             .chunks =
                 {
                     {.rowCount = 30, .size = 8},
                     {.rowCount = 70, .size = 18},
                 }},
        });

    indexHelper.addStripe({
        {.rowCount = 50, .key = "iii"},
        {.rowCount = 50, .key = "jjj"},
    });
    tabletWriter->writeStripe(100, std::move(streams));
  }

  tabletWriter->close();
  writeFile.close();

  // Read and verify the tablet
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = createTabletReader(readFile);

  // Use test helper to verify stripe group count
  nimble::test::TabletReaderTestHelper tabletHelper(tablet.get());
  EXPECT_EQ(tabletHelper.numStripeGroups(), 1);

  // Verify basic tablet properties
  EXPECT_EQ(tablet->stripeCount(), 4);
  EXPECT_EQ(tablet->tabletRowCount(), 470); // 100+150+120+100
  EXPECT_EQ(tablet->stripeRowCount(0), 100);
  EXPECT_EQ(tablet->stripeRowCount(1), 150);
  EXPECT_EQ(tablet->stripeRowCount(2), 120);
  EXPECT_EQ(tablet->stripeRowCount(3), 100);

  // Verify index section exists
  EXPECT_TRUE(
      tablet->hasOptionalSection(std::string(nimble::kClusterIndexSection)));

  // Verify the index is available
  const nimble::ClusterIndex* index = tablet->clusterIndex();
  ASSERT_NE(index, nullptr);

  // Verify only one index group is created
  EXPECT_EQ(index->layout().numPartitions, 1);

  // Load partition and verify stream stats
  nimble::index::test::ClusterIndexTestHelper clusterIndexTestHelper(
      tablet->clusterIndex());

  auto stripeId = tablet->stripeIdentifier(0);

  nimble::index::test::ChunkIndexTestHelper chunkHelper(
      stripeId.chunkIndex().get());
  EXPECT_EQ(chunkHelper.streamCount(), 4);

  // Verify stream 0 position index stats (empty in stripe 0)
  // Stripe 0: 0 chunks (empty)
  // Stripe 1: 3 chunks (rows: 40, 60, 50)
  // Stripe 2: 2 chunks (rows: 70, 50)
  // Stripe 3: 2 chunks (rows: 50, 50)
  {
    auto stream0Stats = chunkHelper.streamStats(0);
    // Per-stream accumulated chunk counts:
    // Stripe 0: 0 chunks -> accumulated = 0
    // Stripe 1: 3 chunks -> accumulated = 0 + 3 = 3
    // Stripe 2: 2 chunks -> accumulated = 3 + 2 = 5
    // Stripe 3: 2 chunks -> accumulated = 5 + 2 = 7
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{0, 3, 5, 7}));
    // Accumulated row counts per chunk (only from stripes with data)
    // Stripe 1: 40, 100, 150
    // Stripe 2: 70, 120
    // Stripe 3: 50, 100
    EXPECT_EQ(
        stream0Stats.chunkRows,
        (std::vector<uint32_t>{40, 100, 150, 70, 120, 50, 100}));
  }

  // Verify stream 1 position index stats (empty in stripe 1)
  // Stripe 0: 2 chunks (rows: 50, 50)
  // Stripe 1: 0 chunks (empty)
  // Stripe 2: 4 chunks (rows: 25, 35, 30, 30)
  // Stripe 3: 2 chunks (rows: 40, 60)
  {
    auto stream1Stats = chunkHelper.streamStats(1);
    // Per-stream accumulated chunk counts:
    // Stripe 0: 2 chunks -> accumulated = 2
    // Stripe 1: 0 chunks -> accumulated = 2 + 0 = 2
    // Stripe 2: 4 chunks -> accumulated = 2 + 4 = 6
    // Stripe 3: 2 chunks -> accumulated = 6 + 2 = 8
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{2, 2, 6, 8}));
    // Accumulated row counts per chunk
    // Stripe 0: 50, 100
    // Stripe 1: (none)
    // Stripe 2: 25, 60, 90, 120
    // Stripe 3: 40, 100
    EXPECT_EQ(
        stream1Stats.chunkRows,
        (std::vector<uint32_t>{50, 100, 25, 60, 90, 120, 40, 100}));
  }

  // Verify stream 2 position index stats (empty in stripe 2)
  // Stripe 0: 3 chunks (rows: 30, 40, 30)
  // Stripe 1: 2 chunks (rows: 80, 70)
  // Stripe 2: 0 chunks (empty)
  // Stripe 3: 1 chunk (rows: 100)
  {
    auto stream2Stats = chunkHelper.streamStats(2);
    // Per-stream accumulated chunk counts:
    // Stripe 0: 3 chunks -> accumulated = 3
    // Stripe 1: 2 chunks -> accumulated = 3 + 2 = 5
    // Stripe 2: 0 chunks -> accumulated = 5 + 0 = 5
    // Stripe 3: 1 chunk -> accumulated = 5 + 1 = 6
    EXPECT_EQ(stream2Stats.chunkCounts, (std::vector<uint32_t>{3, 5, 5, 6}));
    // Accumulated row counts per chunk
    // Stripe 0: 30, 70, 100
    // Stripe 1: 80, 150
    // Stripe 2: (none)
    // Stripe 3: 100
    EXPECT_EQ(
        stream2Stats.chunkRows,
        (std::vector<uint32_t>{30, 70, 100, 80, 150, 100}));
  }

  // Verify stream 3 position index stats (has data in all stripes)
  // Stripe 0: 2 chunks (rows: 60, 40)
  // Stripe 1: 1 chunk (rows: 150)
  // Stripe 2: 3 chunks (rows: 40, 40, 40)
  // Stripe 3: 2 chunks (rows: 30, 70)
  {
    auto stream3Stats = chunkHelper.streamStats(3);
    // Per-stream accumulated chunk counts:
    // Stripe 0: 2 chunks -> accumulated = 2
    // Stripe 1: 1 chunk -> accumulated = 2 + 1 = 3
    // Stripe 2: 3 chunks -> accumulated = 3 + 3 = 6
    // Stripe 3: 2 chunks -> accumulated = 6 + 2 = 8
    EXPECT_EQ(stream3Stats.chunkCounts, (std::vector<uint32_t>{2, 3, 6, 8}));
    // Accumulated row counts per chunk
    // Stripe 0: 60, 100
    // Stripe 1: 150
    // Stripe 2: 40, 80, 120
    // Stripe 3: 30, 100
    EXPECT_EQ(
        stream3Stats.chunkRows,
        (std::vector<uint32_t>{60, 100, 150, 40, 80, 120, 30, 100}));
  }

  // Verify partition stats
  {
    const auto stats = clusterIndexTestHelper.partitionStats(0);
    // Accumulated row counts per chunk (cumulative across partition)
    // Stripe 0: 50, 100
    // Stripe 1: 100+50=150, 100+100=200, 100+150=250
    // Stripe 2: 250+60=310, 250+120=370
    // Stripe 3: 370+50=420, 370+100=470
    EXPECT_EQ(
        stats.chunkRows,
        (std::vector<uint32_t>{50, 100, 150, 200, 250, 310, 370, 420, 470}));
    EXPECT_EQ(
        stats.chunkKeys,
        (std::vector<std::string>{
            "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii", "jjj"}));
  }

  // Verify key lookups with chunk-level precision.
  // Chunk layout (all in partition 0, rows 0-469):
  //   Chunk 0: rows [0, 50),     key "bbb"
  //   Chunk 1: rows [50, 100),   key "ccc"
  //   Chunk 2: rows [100, 150),  key "ddd"
  //   Chunk 3: rows [150, 200),  key "eee"
  //   Chunk 4: rows [200, 250),  key "fff"
  //   Chunk 5: rows [250, 310),  key "ggg"
  //   Chunk 6: rows [310, 370),  key "hhh"
  //   Chunk 7: rows [370, 420),  key "iii"
  //   Chunk 8: rows [420, 470),  key "jjj"
  {
    verifyClusterIndexLookups(
        index,
        {
            {"aaa", nimble::RowRange(0, 470)},
            {"b", nimble::RowRange(0, 470)},
            {"bbb", nimble::RowRange(0, 470)},
            {"ccc", nimble::RowRange(50, 470)},
            {"ddd", nimble::RowRange(100, 470)},
            {"eee", nimble::RowRange(150, 470)},
            {"fff", nimble::RowRange(200, 470)},
            {"ggg", nimble::RowRange(250, 470)},
            {"hhh", nimble::RowRange(310, 470)},
            {"iii", nimble::RowRange(370, 470)},
            {"jjj", nimble::RowRange(420, 470)},
            {"kkk", std::nullopt},
        });
  }

  // Verify key lookups through ClusterIndex::lookupChunk return correct
  // global row IDs.
  // Global row ID = stripe start row + row offset within stripe
  //
  // Stripe layout:
  // - Stripe 0: rows 0-99 (100 rows), start row = 0
  // - Stripe 1: rows 100-249 (150 rows), start row = 100
  // - Stripe 2: rows 250-369 (120 rows), start row = 250
  // - Stripe 3: rows 370-469 (100 rows), start row = 370
  //
  // Key stream chunks (each chunk's rowOffset is accumulated rows before it):
  // Stripe 0: chunk 0 (rows 0-49, rowOffset=0), chunk 1 (rows 50-99,
  // rowOffset=50) Stripe 1: chunk 0-2 (rows 0-49, 50-99, 100-149, rowOffset=0,
  // 50, 100) Stripe 2: chunk 0-1 (rows 0-59, 60-119, rowOffset=0, 60) Stripe 3:
  // chunk 0-1 (rows 0-49, 50-99, rowOffset=0, 50)
  verifyKeyLookupFileRowIds(
      *tablet,
      {
          // Stripe 0: global rows 0-99
          {"aaa", 0, 0},
          {"bbb", 0, 0},
          {"bbc", 0, 50},
          {"ccc", 0, 50},

          // Stripe 1: global rows 100-249
          {"ccd", 1, 100},
          {"ddd", 1, 100},
          {"dde", 1, 150},
          {"eee", 1, 150},
          {"eef", 1, 200},
          {"fff", 1, 200},

          // Stripe 2: global rows 250-369
          {"ffg", 2, 250},
          {"ggg", 2, 250},
          {"ggh", 2, 310},
          {"hhh", 2, 310},

          // Stripe 3: global rows 370-469
          {"hhi", 3, 370},
          {"iii", 3, 370},
          {"iij", 3, 420},
          {"jjj", 3, 420},
      });
}

TEST_P(TabletWithIndexTest, multipleGroupsWithEmptyStream) {
  // Test writing a tablet with 4 streams where some streams are empty in
  // certain stripes, with multiple stripe groups (one per stripe).
  // This tests the position index handles missing streams correctly across
  // group boundaries.
  //
  // Configuration (4 stripes, 4 streams, 4 groups):
  // - Stream 0: empty in first stripe (stripe 0 / group 0)
  // - Stream 1: empty in middle stripe (stripe 1 / group 1)
  // - Stream 2: empty in stripe 2 (group 2)
  // - Stream 3: has data in all stripes
  // - Stripe 3 / Group 3: ALL streams have data (no empty streams)
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);

  nimble::index::test::TestClusterIndexMetadataWriter indexHelper(
      *pool_,
      {"col1"},
      {SortOrder{.ascending = true}},
      /*enforceKeyOrder=*/true);

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          // Set threshold to 0 to force flush after every stripe
          .metadataFlushThreshold = 0,
          .streamDeduplicationEnabled = false,
          .enableChunkIndex = true,
          // Disable chunk index skipping so all groups get a chunk index,
          // even when empty streams reduce the average chunks per stream.
          .chunkIndexMinAvgChunks = 0,
          .stripeGroupFlushCallback =
              indexHelper.createStripeGroupFlushCallback(),
          .closeCallback = indexHelper.createCloseCallback(),
      });

  nimble::Buffer buffer{*pool_};

  // Write stripe 0 (Group 0): Total rows: 100
  // Stream 0: EMPTY (no data in first stripe)
  // Stream 1: 2 chunks (rows: 50, 50)
  // Stream 2: 3 chunks (rows: 30, 40, 30)
  // Stream 3: 2 chunks (rows: 60, 40)
  // Key stream: 2 chunks (rows: 50, 50), keys: "bbb", "ccc"
  {
    auto streams = createStreams(
        buffer,
        {
            // Stream 0 is empty - not included
            {.offset = 1,
             .chunks =
                 {
                     {.rowCount = 50, .size = 10},
                     {.rowCount = 50, .size = 12},
                 }},
            {.offset = 2,
             .chunks =
                 {
                     {.rowCount = 30, .size = 8},
                     {.rowCount = 40, .size = 10},
                     {.rowCount = 30, .size = 6},
                 }},
            {.offset = 3,
             .chunks =
                 {
                     {.rowCount = 60, .size = 15},
                     {.rowCount = 40, .size = 10},
                 }},
        });

    indexHelper.addStripe({
        {.rowCount = 50, .key = "bbb"},
        {.rowCount = 50, .key = "ccc"},
    });
    tabletWriter->writeStripe(100, std::move(streams));
  }

  // Write stripe 1 (Group 1): Total rows: 150
  // Stream 0: 3 chunks (rows: 40, 60, 50)
  // Stream 1: EMPTY (no data in middle stripe)
  // Stream 2: 2 chunks (rows: 80, 70)
  // Stream 3: 1 chunk (rows: 150)
  // Key stream: 3 chunks (rows: 50, 50, 50), keys: "ddd", "eee",
  // "fff"
  {
    auto streams = createStreams(
        buffer,
        {
            {.offset = 0,
             .chunks =
                 {
                     {.rowCount = 40, .size = 8},
                     {.rowCount = 60, .size = 14},
                     {.rowCount = 50, .size = 11},
                 }},
            // Stream 1 is empty - not included
            {.offset = 2,
             .chunks =
                 {
                     {.rowCount = 80, .size = 18},
                     {.rowCount = 70, .size = 15},
                 }},
            {.offset = 3,
             .chunks =
                 {
                     {.rowCount = 150, .size = 30},
                 }},
        });

    indexHelper.addStripe({
        {.rowCount = 50, .key = "ddd"},
        {.rowCount = 50, .key = "eee"},
        {.rowCount = 50, .key = "fff"},
    });
    tabletWriter->writeStripe(150, std::move(streams));
  }

  // Write stripe 2 (Group 2): Total rows: 120
  // Stream 0: 2 chunks (rows: 70, 50)
  // Stream 1: 4 chunks (rows: 25, 35, 30, 30)
  // Stream 2: EMPTY (no data in this stripe)
  // Stream 3: 3 chunks (rows: 40, 40, 40)
  // Key stream: 2 chunks (rows: 60, 60), keys: "ggg", "hhh"
  {
    auto streams = createStreams(
        buffer,
        {
            {.offset = 0,
             .chunks =
                 {
                     {.rowCount = 70, .size = 16},
                     {.rowCount = 50, .size = 13},
                 }},
            {.offset = 1,
             .chunks =
                 {
                     {.rowCount = 25, .size = 5},
                     {.rowCount = 35, .size = 7},
                     {.rowCount = 30, .size = 6},
                     {.rowCount = 30, .size = 8},
                 }},
            // Stream 2 is empty - not included
            {.offset = 3,
             .chunks =
                 {
                     {.rowCount = 40, .size = 10},
                     {.rowCount = 40, .size = 10},
                     {.rowCount = 40, .size = 10},
                 }},
        });

    indexHelper.addStripe({
        {.rowCount = 60, .key = "ggg"},
        {.rowCount = 60, .key = "hhh"},
    });
    tabletWriter->writeStripe(120, std::move(streams));
  }

  // Write stripe 3 (Group 3): Total rows: 100 - ALL STREAMS HAVE DATA
  // Stream 0: 2 chunks (rows: 50, 50)
  // Stream 1: 2 chunks (rows: 40, 60)
  // Stream 2: 1 chunk (rows: 100)
  // Stream 3: 2 chunks (rows: 30, 70)
  // Key stream: 2 chunks (rows: 50, 50), keys: "iii", "jjj"
  {
    auto streams = createStreams(
        buffer,
        {
            {.offset = 0,
             .chunks =
                 {
                     {.rowCount = 50, .size = 12},
                     {.rowCount = 50, .size = 12},
                 }},
            {.offset = 1,
             .chunks =
                 {
                     {.rowCount = 40, .size = 10},
                     {.rowCount = 60, .size = 14},
                 }},
            {.offset = 2,
             .chunks =
                 {
                     {.rowCount = 100, .size = 25},
                 }},
            {.offset = 3,
             .chunks =
                 {
                     {.rowCount = 30, .size = 8},
                     {.rowCount = 70, .size = 18},
                 }},
        });

    indexHelper.addStripe({
        {.rowCount = 50, .key = "iii"},
        {.rowCount = 50, .key = "jjj"},
    });
    tabletWriter->writeStripe(100, std::move(streams));
  }

  tabletWriter->close();
  writeFile.close();

  // Read and verify the tablet
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = createTabletReader(readFile);

  // Use test helper to verify stripe group count
  nimble::test::TabletReaderTestHelper tabletHelper(tablet.get());
  EXPECT_EQ(tabletHelper.numStripeGroups(), 4);

  // Verify basic tablet properties
  EXPECT_EQ(tablet->stripeCount(), 4);
  EXPECT_EQ(tablet->tabletRowCount(), 470); // 100+150+120+100
  EXPECT_EQ(tablet->stripeRowCount(0), 100);
  EXPECT_EQ(tablet->stripeRowCount(1), 150);
  EXPECT_EQ(tablet->stripeRowCount(2), 120);
  EXPECT_EQ(tablet->stripeRowCount(3), 100);

  // Verify index section exists
  EXPECT_TRUE(
      tablet->hasOptionalSection(std::string(nimble::kClusterIndexSection)));

  // Verify the index is available
  const nimble::ClusterIndex* index = tablet->clusterIndex();
  ASSERT_NE(index, nullptr);

  // Verify 4 index groups are created (one per stripe)
  EXPECT_EQ(index->layout().numPartitions, 4);

  // Verify key lookups with chunk-level precision.
  // Partition 0 (rows 0-99):   chunks [0,50) "bbb", [50,100) "ccc"
  // Partition 1 (rows 100-249): chunks [100,150) "ddd", [150,200) "eee",
  //                                    [200,250) "fff"
  // Partition 2 (rows 250-369): chunks [250,310) "ggg", [310,370) "hhh"
  // Partition 3 (rows 370-469): chunks [370,420) "iii", [420,470) "jjj"
  {
    verifyClusterIndexLookups(
        index,
        {
            {"aaa", nimble::RowRange(0, 470)},
            {"b", nimble::RowRange(0, 470)},
            {"bbb", nimble::RowRange(0, 470)},
            {"ccc", nimble::RowRange(50, 470)},
            {"ddd", nimble::RowRange(100, 470)},
            {"eee", nimble::RowRange(150, 470)},
            {"fff", nimble::RowRange(200, 470)},
            {"ggg", nimble::RowRange(250, 470)},
            {"hhh", nimble::RowRange(310, 470)},
            {"iii", nimble::RowRange(370, 470)},
            {"jjj", nimble::RowRange(420, 470)},
            {"kkk", std::nullopt},
        });
  }

  nimble::index::test::ClusterIndexTestHelper clusterIndexTestHelper(
      tablet->clusterIndex());

  // Group 0: stripe 0 only
  // Stream 0: EMPTY
  // Stream 1: 2 chunks (rows: 50, 50)
  // Stream 2: 3 chunks (rows: 30, 40, 30)
  // Stream 3: 2 chunks (rows: 60, 40)
  {
    auto stripeId = tablet->stripeIdentifier(0);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeId.chunkIndex().get());
    // All 4 streams indexed (chunkIndexMinAvgChunks = 0).
    EXPECT_EQ(chunkHelper.streamCount(), 4);

    // Verify partition stats for group 0
    const auto stats = clusterIndexTestHelper.partitionStats(0);
    EXPECT_EQ(stats.chunkRows, (std::vector<uint32_t>{50, 100}));
    EXPECT_EQ(stats.chunkKeys, (std::vector<std::string>{"bbb", "ccc"}));

    // Stream 0: EMPTY → 0 chunks.
    auto stream0Stats = chunkHelper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{0}));
    EXPECT_TRUE(stream0Stats.chunkRows.empty());

    // Stream 1: 2 chunks (rows: 50, 50)
    auto stream1Stats = chunkHelper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream1Stats.chunkRows, (std::vector<uint32_t>{50, 100}));

    // Stream 2: 3 chunks (rows: 30, 40, 30)
    auto stream2Stats = chunkHelper.streamStats(2);
    EXPECT_EQ(stream2Stats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(stream2Stats.chunkRows, (std::vector<uint32_t>{30, 70, 100}));

    // Stream 3: 2 chunks (rows: 60, 40)
    auto stream3Stats = chunkHelper.streamStats(3);
    EXPECT_EQ(stream3Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream3Stats.chunkRows, (std::vector<uint32_t>{60, 100}));
  }

  // Group 1: stripe 1 only
  // Stream 0: 3 chunks (rows: 40, 60, 50)
  // Stream 1: EMPTY
  // Stream 2: 2 chunks (rows: 80, 70)
  // Stream 3: 1 chunk (rows: 150)
  {
    auto stripeId = tablet->stripeIdentifier(1);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeId.chunkIndex().get());
    // All 4 streams indexed (chunkIndexMinAvgChunks = 0).
    EXPECT_EQ(chunkHelper.streamCount(), 4);

    // Verify partition stats for group 1
    const auto stats = clusterIndexTestHelper.partitionStats(1);
    EXPECT_EQ(stats.chunkRows, (std::vector<uint32_t>{50, 100, 150}));
    EXPECT_EQ(stats.chunkKeys, (std::vector<std::string>{"ddd", "eee", "fff"}));

    // Stream 0: 3 chunks (rows: 40, 60, 50)
    auto stream0Stats = chunkHelper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(stream0Stats.chunkRows, (std::vector<uint32_t>{40, 100, 150}));

    // Stream 1: EMPTY → 0 chunks.
    auto stream1Stats = chunkHelper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{0}));
    EXPECT_TRUE(stream1Stats.chunkRows.empty());

    // Stream 2: 2 chunks (rows: 80, 70)
    auto stream2Stats = chunkHelper.streamStats(2);
    EXPECT_EQ(stream2Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream2Stats.chunkRows, (std::vector<uint32_t>{80, 150}));

    // Stream 3: 1 chunk (rows: 150)
    auto stream3Stats = chunkHelper.streamStats(3);
    EXPECT_EQ(stream3Stats.chunkCounts, (std::vector<uint32_t>{1}));
    EXPECT_EQ(stream3Stats.chunkRows, (std::vector<uint32_t>{150}));
  }

  // Group 2: stripe 2 only
  // Stream 0: 2 chunks (rows: 70, 50)
  // Stream 1: 4 chunks (rows: 25, 35, 30, 30)
  // Stream 2: EMPTY
  // Stream 3: 3 chunks (rows: 40, 40, 40)
  {
    auto stripeId = tablet->stripeIdentifier(2);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeId.chunkIndex().get());
    // All 4 streams indexed (chunkIndexMinAvgChunks = 0).
    EXPECT_EQ(chunkHelper.streamCount(), 4);

    // Verify partition stats for group 2
    const auto stats = clusterIndexTestHelper.partitionStats(2);
    EXPECT_EQ(stats.chunkRows, (std::vector<uint32_t>{60, 120}));
    EXPECT_EQ(stats.chunkKeys, (std::vector<std::string>{"ggg", "hhh"}));

    // Stream 0: 2 chunks (rows: 70, 50)
    auto stream0Stats = chunkHelper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream0Stats.chunkRows, (std::vector<uint32_t>{70, 120}));

    // Stream 1: 4 chunks (rows: 25, 35, 30, 30)
    auto stream1Stats = chunkHelper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{4}));
    EXPECT_EQ(stream1Stats.chunkRows, (std::vector<uint32_t>{25, 60, 90, 120}));

    // Stream 2: EMPTY → 0 chunks.
    auto stream2Stats = chunkHelper.streamStats(2);
    EXPECT_EQ(stream2Stats.chunkCounts, (std::vector<uint32_t>{0}));
    EXPECT_TRUE(stream2Stats.chunkRows.empty());

    // Stream 3: 3 chunks (rows: 40, 40, 40)
    auto stream3Stats = chunkHelper.streamStats(3);
    EXPECT_EQ(stream3Stats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(stream3Stats.chunkRows, (std::vector<uint32_t>{40, 80, 120}));
  }

  // Group 3: stripe 3 only - ALL STREAMS HAVE DATA
  // Stream 0: 2 chunks (rows: 50, 50)
  // Stream 1: 2 chunks (rows: 40, 60)
  // Stream 2: 1 chunk (rows: 100)
  // Stream 3: 2 chunks (rows: 30, 70)
  {
    auto stripeId = tablet->stripeIdentifier(3);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeId.chunkIndex().get());
    // All 4 streams indexed (chunkIndexMinAvgChunks = 0).
    EXPECT_EQ(chunkHelper.streamCount(), 4);

    // Verify partition stats for group 3
    const auto stats = clusterIndexTestHelper.partitionStats(3);
    EXPECT_EQ(stats.chunkRows, (std::vector<uint32_t>{50, 100}));
    EXPECT_EQ(stats.chunkKeys, (std::vector<std::string>{"iii", "jjj"}));

    // Stream 0: 2 chunks (rows: 50, 50)
    auto stream0Stats = chunkHelper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream0Stats.chunkRows, (std::vector<uint32_t>{50, 100}));

    // Stream 1: 2 chunks (rows: 40, 60)
    auto stream1Stats = chunkHelper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream1Stats.chunkRows, (std::vector<uint32_t>{40, 100}));

    // Stream 2: 1 chunk (rows: 100)
    auto stream2Stats = chunkHelper.streamStats(2);
    EXPECT_EQ(stream2Stats.chunkCounts, (std::vector<uint32_t>{1}));
    EXPECT_EQ(stream2Stats.chunkRows, (std::vector<uint32_t>{100}));

    // Stream 3: 2 chunks (rows: 30, 70)
    auto stream3Stats = chunkHelper.streamStats(3);
    EXPECT_EQ(stream3Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream3Stats.chunkRows, (std::vector<uint32_t>{30, 100}));
  }

  // Verify key lookups through ClusterIndex::lookupChunk return correct
  // file row IDs.
  // File row ID = stripe start row + row offset within stripe
  //
  // Stripe layout:
  // - Stripe 0 (Group 0): rows 0-99 (100 rows), start row = 0
  // - Stripe 1 (Group 1): rows 100-249 (150 rows), start row = 100
  // - Stripe 2 (Group 2): rows 250-369 (120 rows), start row = 250
  // - Stripe 3 (Group 3): rows 370-469 (100 rows), start row = 370
  //
  // Key stream chunks (each chunk's rowOffset is accumulated rows before it):
  // Stripe 0: chunk 0 (rows 0-49, rowOffset=0), chunk 1 (rows 50-99,
  // rowOffset=50) Stripe 1: chunk 0-2 (rows 0-49, 50-99, 100-149, rowOffset=0,
  // 50, 100) Stripe 2: chunk 0-1 (rows 0-59, 60-119, rowOffset=0, 60) Stripe 3:
  // chunk 0-1 (rows 0-49, 50-99, rowOffset=0, 50)
  verifyKeyLookupFileRowIds(
      *tablet,
      {
          // Stripe 0 (Group 0): global rows 0-99
          {"aaa", 0, 0},
          {"bbb", 0, 0},
          {"bbc", 0, 50},
          {"ccc", 0, 50},

          // Stripe 1 (Group 1): global rows 100-249
          {"ccd", 1, 100},
          {"ddd", 1, 100},
          {"dde", 1, 150},
          {"eee", 1, 150},
          {"eef", 1, 200},
          {"fff", 1, 200},

          // Stripe 2 (Group 2): global rows 250-369
          {"ffg", 2, 250},
          {"ggg", 2, 250},
          {"ggh", 2, 310},
          {"hhh", 2, 310},

          // Stripe 3 (Group 3): global rows 370-469
          {"hhi", 3, 370},
          {"iii", 3, 370},
          {"iij", 3, 420},
          {"jjj", 3, 420},
      });
}

TEST_P(TabletWithIndexTest, streamDeduplication) {
  // Test writing a tablet with stream deduplication enabled and index
  // configuration. Some streams have identical content and should be
  // deduplicated. This test verifies the position index correctly handles
  // deduplicated streams, including that duplicate streams have the same
  // number of chunks as their source streams.
  //
  // Configuration (1 stripe, 1 group, 4 streams):
  // - Stream 0: unique content A, 2 chunks (rows: 40, 60)
  // - Stream 1: duplicate of stream 0 (same content A), 2 chunks (rows: 50, 50)
  // - Stream 2: unique content B, 3 chunks (rows: 30, 40, 30)
  // - Stream 3: duplicate of stream 2 (same content B), 3 chunks (rows: 35, 35,
  //   30)
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);

  nimble::index::test::TestClusterIndexMetadataWriter indexHelper(
      *pool_,
      {"col1"},
      {SortOrder{.ascending = true}},
      /*enforceKeyOrder=*/true);

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          .metadataFlushThreshold = 1024 * 1024 * 1024,
          .streamDeduplicationEnabled = true,
          .enableChunkIndex = true,
          .stripeGroupFlushCallback =
              indexHelper.createStripeGroupFlushCallback(),
          .closeCallback = indexHelper.createCloseCallback(),
      });

  nimble::Buffer buffer{*pool_};

  // Create shared content for duplicate streams
  // Content A: used by stream 0 and stream 1
  const std::string_view contentA = "CONTENT_A_DATA_FOR_DEDUP_TEST";
  auto contentAPos = buffer.reserve(contentA.size());
  std::memcpy(contentAPos, contentA.data(), contentA.size());

  // Content B: used by stream 2 and stream 3
  const std::string_view contentB = "CONTENT_B_DATA_FOR_DEDUP_TEST_LONGER";
  auto contentBPos = buffer.reserve(contentB.size());
  std::memcpy(contentBPos, contentB.data(), contentB.size());

  // Write stripe 0: Total rows: 100
  // Stream 0: content A, 2 chunks (rows: 40, 60)
  // Stream 1: content A (duplicate of stream 0), 2 chunks (rows: 50, 50)
  // Stream 2: content B, 3 chunks (rows: 30, 40, 30)
  // Stream 3: content B (duplicate of stream 2), 3 chunks (rows: 35, 35, 30)
  // Key stream: 2 chunks (rows: 50, 50), keys: "bbb", "ccc"
  {
    std::vector<nimble::Stream> streams;

    // Stream 0: content A, 2 chunks
    // Split content A into 2 parts for 2 chunks
    const size_t splitA = contentA.size() / 2;
    {
      nimble::Stream stream{.offset = 0};
      // Chunk 0: rows 40
      nimble::Chunk chunk0;
      chunk0.rowCount = 40;
      chunk0.content = {std::string_view(contentAPos, splitA)};
      stream.chunks.push_back(std::move(chunk0));
      // Chunk 1: rows 60
      nimble::Chunk chunk1;
      chunk1.rowCount = 60;
      chunk1.content = {
          std::string_view(contentAPos + splitA, contentA.size() - splitA)};
      stream.chunks.push_back(std::move(chunk1));
      streams.push_back(std::move(stream));
    }

    // Stream 1: content A (duplicate - same content as stream 0), 2 chunks
    {
      nimble::Stream stream{.offset = 1};
      // Chunk 0: rows 50
      nimble::Chunk chunk0;
      chunk0.rowCount = 50;
      chunk0.content = {std::string_view(contentAPos, splitA)};
      stream.chunks.push_back(std::move(chunk0));
      // Chunk 1: rows 50
      nimble::Chunk chunk1;
      chunk1.rowCount = 50;
      chunk1.content = {
          std::string_view(contentAPos + splitA, contentA.size() - splitA)};
      stream.chunks.push_back(std::move(chunk1));
      streams.push_back(std::move(stream));
    }

    // Stream 2: content B, 3 chunks
    // Split content B into 3 parts for 3 chunks
    const size_t splitB1 = contentB.size() / 3;
    const size_t splitB2 = 2 * contentB.size() / 3;
    {
      nimble::Stream stream{.offset = 2};
      // Chunk 0: rows 30
      nimble::Chunk chunk0;
      chunk0.rowCount = 30;
      chunk0.content = {std::string_view(contentBPos, splitB1)};
      stream.chunks.push_back(std::move(chunk0));
      // Chunk 1: rows 40
      nimble::Chunk chunk1;
      chunk1.rowCount = 40;
      chunk1.content = {
          std::string_view(contentBPos + splitB1, splitB2 - splitB1)};
      stream.chunks.push_back(std::move(chunk1));
      // Chunk 2: rows 30
      nimble::Chunk chunk2;
      chunk2.rowCount = 30;
      chunk2.content = {
          std::string_view(contentBPos + splitB2, contentB.size() - splitB2)};
      stream.chunks.push_back(std::move(chunk2));
      streams.push_back(std::move(stream));
    }

    // Stream 3: content B (duplicate - same content as stream 2), 3 chunks
    {
      nimble::Stream stream{.offset = 3};
      // Chunk 0: rows 35
      nimble::Chunk chunk0;
      chunk0.rowCount = 35;
      chunk0.content = {std::string_view(contentBPos, splitB1)};
      stream.chunks.push_back(std::move(chunk0));
      // Chunk 1: rows 35
      nimble::Chunk chunk1;
      chunk1.rowCount = 35;
      chunk1.content = {
          std::string_view(contentBPos + splitB1, splitB2 - splitB1)};
      stream.chunks.push_back(std::move(chunk1));
      // Chunk 2: rows 30
      nimble::Chunk chunk2;
      chunk2.rowCount = 30;
      chunk2.content = {
          std::string_view(contentBPos + splitB2, contentB.size() - splitB2)};
      stream.chunks.push_back(std::move(chunk2));
      streams.push_back(std::move(stream));
    }

    indexHelper.addStripe({
        {.rowCount = 50, .key = "bbb"},
        {.rowCount = 50, .key = "ccc"},
    });
    tabletWriter->writeStripe(100, std::move(streams));
  }

  tabletWriter->close();
  writeFile.close();

  // Read and verify the tablet
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = createTabletReader(readFile);

  // Use test helper to verify stripe group count
  nimble::test::TabletReaderTestHelper tabletHelper(tablet.get());
  EXPECT_EQ(tabletHelper.numStripeGroups(), 1);

  // Verify basic tablet properties
  EXPECT_EQ(tablet->stripeCount(), 1);
  EXPECT_EQ(tablet->tabletRowCount(), 100);
  EXPECT_EQ(tablet->stripeRowCount(0), 100);

  // Verify index section exists
  EXPECT_TRUE(
      tablet->hasOptionalSection(std::string(nimble::kClusterIndexSection)));

  // Verify the index is available
  const nimble::ClusterIndex* index = tablet->clusterIndex();
  ASSERT_NE(index, nullptr);

  // Verify only one index group is created
  EXPECT_EQ(index->layout().numPartitions, 1);

  // Verify key lookups with chunk-level precision (single stripe: rows 0-99).
  // Chunk 0: rows [0, 50), key "bbb". Chunk 1: rows [50, 100), key "ccc".
  {
    verifyClusterIndexLookups(
        index,
        {
            {"aaa", nimble::RowRange(0, 100)},
            {"bbb", nimble::RowRange(0, 100)},
            {"ccc", nimble::RowRange(50, 100)},
            {"ddd", std::nullopt},
        });
  }

  // Load partition and verify stream stats
  nimble::index::test::ClusterIndexTestHelper clusterIndexTestHelper(
      tablet->clusterIndex());

  auto stripeId = tablet->stripeIdentifier(0);

  nimble::index::test::ChunkIndexTestHelper chunkHelper(
      stripeId.chunkIndex().get());
  EXPECT_EQ(chunkHelper.streamCount(), 4);

  // Verify partition stats
  {
    const auto stats = clusterIndexTestHelper.partitionStats(0);
    EXPECT_EQ(stats.chunkRows, (std::vector<uint32_t>{50, 100}));
    EXPECT_EQ(stats.chunkKeys, (std::vector<std::string>{"bbb", "ccc"}));
  }

  // Verify stream stats - all streams should have their own position index
  // stats even though content is deduplicated.
  // After deduplication, duplicate streams should have the same number of
  // chunks as their source streams.

  // Stream 0: 2 chunks (rows: 40, 60)
  {
    auto stream0Stats = chunkHelper.streamStats(0);
    EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream0Stats.chunkRows, (std::vector<uint32_t>{40, 100}));
  }

  // Stream 1: 2 chunks (rows: 50, 50) - duplicate content of stream 0
  // Verify duplicate stream has same number of chunks as source stream (0)
  {
    auto stream1Stats = chunkHelper.streamStats(1);
    EXPECT_EQ(stream1Stats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(stream1Stats.chunkRows, (std::vector<uint32_t>{40, 100}));
  }

  // Stream 2: 3 chunks (rows: 30, 40, 30)
  {
    auto stream2Stats = chunkHelper.streamStats(2);
    EXPECT_EQ(stream2Stats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(stream2Stats.chunkRows, (std::vector<uint32_t>{30, 70, 100}));
  }

  // Stream 3: 3 chunks (rows: 35, 35, 30) - duplicate content of stream 2
  // Verify duplicate stream has same number of chunks as source stream (2)
  {
    auto stream3Stats = chunkHelper.streamStats(3);
    EXPECT_EQ(stream3Stats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(stream3Stats.chunkRows, (std::vector<uint32_t>{30, 70, 100}));
  }

  // Verify stream content - deduplicated streams should return same content
  {
    auto stripeIdentifier = tablet->stripeIdentifier(0);
    std::vector<uint32_t> identifiers = {0, 1, 2, 3};
    auto serializedStreams = tablet->load(
        stripeIdentifier, {identifiers.cbegin(), identifiers.cend()});

    // Verify all streams were loaded
    ASSERT_EQ(serializedStreams.size(), 4);
    for (size_t i = 0; i < 4; ++i) {
      ASSERT_TRUE(serializedStreams[i] != nullptr)
          << "Stream " << i << " should be loaded";
    }

    // Verify stream 0 and stream 1 have same content (both are content A)
    EXPECT_EQ(
        serializedStreams[0]->getStream(), serializedStreams[1]->getStream());
    EXPECT_EQ(serializedStreams[0]->getStream(), contentA);

    // Verify stream 2 and stream 3 have same content (both are content B)
    EXPECT_EQ(
        serializedStreams[2]->getStream(), serializedStreams[3]->getStream());
    EXPECT_EQ(serializedStreams[2]->getStream(), contentB);
  }
}

TEST_P(TabletWithIndexTest, keyOrderEnforcement) {
  // Test key order enforcement behavior with enforceKeyOrder = true/false
  for (bool enforceKeyOrder : {true, false}) {
    SCOPED_TRACE(fmt::format("enforceKeyOrder={}", enforceKeyOrder));

    std::string file;
    velox::InMemoryWriteFile writeFile(&file);

    nimble::index::test::TestClusterIndexMetadataWriter indexHelper(
        *pool_,
        {"col1"},
        {SortOrder{.ascending = true}},
        /*enforceKeyOrder=*/enforceKeyOrder,
        /*noDuplicateKey=*/enforceKeyOrder);

    auto tabletWriter = nimble::TabletWriter::create(
        &writeFile,
        *pool_,
        {
            .enableChunkIndex = true,
            .stripeGroupFlushCallback =
                indexHelper.createStripeGroupFlushCallback(),
            .closeCallback = indexHelper.createCloseCallback(),
        });

    nimble::Buffer buffer{*pool_};

    // Write first stripe with key ending at "ddd"
    {
      std::vector<nimble::Stream> streams;
      auto pos = buffer.reserve(10);
      std::memset(pos, 'A', 10);
      streams.push_back(
          {.offset = 0, .chunks = {{.rowCount = 100, .content = {{pos, 10}}}}});

      indexHelper.addStripe({{.rowCount = 100, .key = "ddd"}});
      tabletWriter->writeStripe(100, std::move(streams));
    }

    // Write second stripe with key ending before "ddd" (out of order)
    {
      std::vector<nimble::Stream> streams;
      auto pos = buffer.reserve(10);
      std::memset(pos, 'B', 10);
      streams.push_back(
          {.offset = 0, .chunks = {{.rowCount = 100, .content = {{pos, 10}}}}});

      // Key "bbb" < "ddd", out of order
      if (enforceKeyOrder) {
        // Should throw when enforceKeyOrder is true
        NIMBLE_ASSERT_USER_THROW(
            indexHelper.addStripe({{.rowCount = 100, .key = "bbb"}}),
            "Stripe keys must be in strictly ascending order");
      } else {
        // Should NOT throw when enforceKeyOrder is false
        EXPECT_NO_THROW(
            indexHelper.addStripe({{.rowCount = 100, .key = "bbb"}}));
        tabletWriter->writeStripe(100, std::move(streams));

        tabletWriter->close();
        writeFile.close();

        // Verify file is readable
        auto readFile =
            std::make_shared<nimble::testing::InMemoryTrackableReadFile>(
                file, false);
        auto tablet = createTabletReader(readFile);
        EXPECT_EQ(tablet->stripeCount(), 2);
        EXPECT_TRUE(tablet->hasOptionalSection(
            std::string(nimble::kClusterIndexSection)));
      }
    }
  }
}

TEST_P(TabletWithIndexTest, noIndex) {
  // Test that without index config, no index section is written
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          // No indexConfig
      });

  nimble::Buffer buffer{*pool_};

  std::vector<nimble::Stream> streams;
  auto pos = buffer.reserve(10);
  std::memset(pos, 'A', 10);
  streams.push_back(
      {.offset = 0, .chunks = {{.rowCount = 100, .content = {{pos, 10}}}}});

  tabletWriter->writeStripe(100, std::move(streams));
  tabletWriter->close();
  writeFile.close();

  // Verify no index section and no chunk index section
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = createTabletReader(readFile);
  EXPECT_EQ(tablet->stripeCount(), 1);
  EXPECT_FALSE(
      tablet->hasOptionalSection(std::string(nimble::kClusterIndexSection)));
  EXPECT_FALSE(
      tablet->hasOptionalSection(std::string(nimble::kChunkIndexSection)));
}

TEST_P(TabletWithIndexTest, loadClusterIndex) {
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);

  nimble::index::test::TestClusterIndexMetadataWriter indexHelper(
      *pool_,
      {"col1"},
      {SortOrder{.ascending = true}},
      /*enforceKeyOrder=*/true);

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          .metadataFlushThreshold = 1024 * 1024 * 1024,
          .streamDeduplicationEnabled = false,
          .enableChunkIndex = true,
          .stripeGroupFlushCallback =
              indexHelper.createStripeGroupFlushCallback(),
          .closeCallback = indexHelper.createCloseCallback(),
      });

  nimble::Buffer buffer{*pool_};
  auto streams = createStreams(
      buffer, {{.offset = 0, .chunks = {{.rowCount = 100, .size = 10}}}});
  indexHelper.addStripe({{.rowCount = 100, .key = "bbb"}});
  tabletWriter->writeStripe(100, std::move(streams));
  tabletWriter->close();

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);

  for (bool loadClusterIndex : {false, true}) {
    SCOPED_TRACE(fmt::format("loadClusterIndex={}", loadClusterIndex));
    const auto metadataBytesReadBefore = metadataIoStats_->rawBytesRead();
    const auto indexBytesReadBefore = indexIoStats_->rawBytesRead();
    nimble::TabletReader::Options options;
    options.loadClusterIndex = loadClusterIndex;
    auto tablet = TabletTest::createTabletReader(readFile, options);
    if (loadClusterIndex) {
      ASSERT_NE(tablet->clusterIndex(), nullptr);
      EXPECT_GT(metadataIoStats_->rawBytesRead(), metadataBytesReadBefore);
      // Index key stream data is lazy-loaded on first lookup, not during
      // init. Only metadata IO (root section) happens here.
      EXPECT_EQ(indexIoStats_->rawBytesRead(), indexBytesReadBefore);
    } else {
      EXPECT_EQ(tablet->clusterIndex(), nullptr);
      EXPECT_EQ(metadataIoStats_->rawBytesRead(), metadataBytesReadBefore);
      EXPECT_EQ(indexIoStats_->rawBytesRead(), indexBytesReadBefore);
    }
  }
}

TEST_P(TabletWithIndexTest, loadClusterIndexMissingIoStats) {
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);

  nimble::index::test::TestClusterIndexMetadataWriter indexHelper(
      *pool_,
      {"col1"},
      {SortOrder{.ascending = true}},
      /*enforceKeyOrder=*/true);

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          .metadataFlushThreshold = 1024 * 1024 * 1024,
          .streamDeduplicationEnabled = false,
          .enableChunkIndex = true,
          .stripeGroupFlushCallback =
              indexHelper.createStripeGroupFlushCallback(),
          .closeCallback = indexHelper.createCloseCallback(),
      });

  nimble::Buffer buffer{*pool_};
  auto streams = createStreams(
      buffer, {{.offset = 0, .chunks = {{.rowCount = 100, .size = 10}}}});
  indexHelper.addStripe({{.rowCount = 100, .key = "bbb"}});
  tabletWriter->writeStripe(100, std::move(streams));
  tabletWriter->close();

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  nimble::TabletReader::Options options;
  options.loadClusterIndex = true;
  options.ioOptions.emplace(pool_.get())
      .setMetadataIoStats(std::make_shared<velox::io::IoStatistics>());
  NIMBLE_ASSERT_THROW(
      nimble::TabletReader::create(readFile, pool_.get(), options),
      "indexIoStats must be set");
}

TEST_P(TabletWithIndexTest, loadDenseIndexes) {
  auto type =
      velox::ROW({{"id", velox::INTEGER()}, {"value", velox::VARCHAR()}});
  auto ids = velox::BaseVector::create(velox::INTEGER(), 50, pool_.get());
  auto values = velox::BaseVector::create(velox::VARCHAR(), 50, pool_.get());
  auto* flatIds = ids->asFlatVector<int32_t>();
  auto* flatValues = values->asFlatVector<velox::StringView>();
  std::vector<std::string> valueStrings;
  for (int i = 0; i < 50; ++i) {
    flatIds->set(i, i);
    valueStrings.push_back("v_" + std::to_string(i));
    flatValues->set(i, velox::StringView(valueStrings.back()));
  }
  auto batch = std::make_shared<velox::RowVector>(
      pool_.get(),
      type,
      nullptr,
      50,
      std::vector<velox::VectorPtr>{ids, values});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriterOptions writerOptions;
  writerOptions.hashIndexConfigs.push_back(
      nimble::HashIndexConfig{.columns = {"id"}});
  writerOptions.sortedIndexConfigs.push_back(
      nimble::SortedIndexConfig{.columns = {"value"}});
  nimble::VeloxWriter writer(
      type, std::move(writeFile), *pool_, std::move(writerOptions));
  writer.write(batch);
  writer.close();

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);

  for (bool loadDenseIndexes : {false, true}) {
    SCOPED_TRACE(fmt::format("loadDenseIndexes={}", loadDenseIndexes));
    auto metadataIoStats = std::make_shared<velox::io::IoStatistics>();
    auto indexIoStats = std::make_shared<velox::io::IoStatistics>();
    nimble::TabletReader::Options options;
    options.loadDenseIndexes = loadDenseIndexes;
    options.ioOptions.emplace(pool_.get())
        .setMetadataIoStats(metadataIoStats)
        .setIndexIoStats(indexIoStats);
    auto tablet = nimble::TabletReader::create(readFile, pool_.get(), options);
    const auto metadataBytesAfterOpen = metadataIoStats->rawBytesRead();
    if (loadDenseIndexes) {
      ASSERT_NE(tablet->denseIndex({"id"}), nullptr);
      ASSERT_NE(tablet->denseIndex({"value"}), nullptr);
      EXPECT_GT(indexIoStats->rawBytesRead(), 0);
    } else {
      EXPECT_EQ(tablet->denseIndex({"id"}), nullptr);
      EXPECT_EQ(tablet->denseIndex({"value"}), nullptr);
      EXPECT_EQ(metadataIoStats->rawBytesRead(), metadataBytesAfterOpen);
      EXPECT_EQ(indexIoStats->rawBytesRead(), 0);
    }
  }
}

TEST_P(TabletWithIndexTest, loadDenseIndexesMissingIoStats) {
  auto type = velox::ROW({{"id", velox::INTEGER()}});
  auto ids = velox::BaseVector::create(velox::INTEGER(), 50, pool_.get());
  auto* flatIds = ids->asFlatVector<int32_t>();
  for (int i = 0; i < 50; ++i) {
    flatIds->set(i, i);
  }
  auto batch = std::make_shared<velox::RowVector>(
      pool_.get(), type, nullptr, 50, std::vector<velox::VectorPtr>{ids});

  std::string file;
  auto writeFile = std::make_unique<velox::InMemoryWriteFile>(&file);
  nimble::VeloxWriterOptions writerOptions;
  writerOptions.hashIndexConfigs.push_back(
      nimble::HashIndexConfig{.columns = {"id"}});
  nimble::VeloxWriter writer(
      type, std::move(writeFile), *pool_, std::move(writerOptions));
  writer.write(batch);
  writer.close();

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  nimble::TabletReader::Options options;
  options.loadDenseIndexes = true;
  options.ioOptions.emplace(pool_.get())
      .setMetadataIoStats(std::make_shared<velox::io::IoStatistics>());
  NIMBLE_ASSERT_THROW(
      nimble::TabletReader::create(readFile, pool_.get(), options),
      "indexIoStats must be set");
}

// Tests all four orthogonal config combinations of enableChunkIndex ×
// cluster index (via indexHelper) to verify:
// 1. Neither: no optional sections
// 2. Chunk index only: chunk_index section exists, lookupChunk works
// 3. Cluster index only (via indexHelper): both sections exist (chunk index is
//    auto-enabled), key lookup and chunk lookup work
// 4. Both explicitly: same as #3
TEST_F(TabletWithIndexTest, configCombinations) {
  struct TestConfig {
    bool enableChunkIndex;
    bool enableIndexConfig;
    bool expectChunkIndex;
    bool expectClusterIndex;

    std::string debugString() const {
      return fmt::format(
          "enableChunkIndex={}, enableIndexConfig={}, expectChunkIndex={}, expectClusterIndex={}",
          enableChunkIndex,
          enableIndexConfig,
          expectChunkIndex,
          expectClusterIndex);
    }
  };

  std::vector<TestConfig> configs = {
      // 1. Neither: no optional sections
      {false, false, false, false},
      // 2. Chunk index only
      {true, false, true, false},
      // 3. Cluster index only: chunk index auto-enabled
      {false, true, true, true},
      // 4. Both explicitly
      {true, true, true, true},
  };

  for (const auto& config : configs) {
    SCOPED_TRACE(config.debugString());

    std::string file;
    velox::InMemoryWriteFile writeFile(&file);

    std::optional<nimble::index::test::TestClusterIndexMetadataWriter>
        indexHelper;
    if (config.enableIndexConfig) {
      indexHelper.emplace(
          *pool_,
          std::vector<std::string>{"col1"},
          std::vector<SortOrder>{SortOrder{.ascending = true}},
          /*enforceKeyOrder=*/true);
    }

    auto tabletWriter = nimble::TabletWriter::create(
        &writeFile,
        *pool_,
        {
            .metadataFlushThreshold = 1024 * 1024 * 1024,
            .enableChunkIndex =
                config.enableChunkIndex || config.enableIndexConfig,
            .stripeGroupFlushCallback = indexHelper
                ? indexHelper->createStripeGroupFlushCallback()
                : nimble::TabletWriter::StripeGroupFlushCallback{},
            .closeCallback = indexHelper
                ? indexHelper->createCloseCallback()
                : nimble::TabletWriter::CloseCallback{},
        });

    nimble::Buffer buffer{*pool_};

    // Write 2 stripes with 2 streams each
    for (int stripe = 0; stripe < 2; ++stripe) {
      auto streams = createStreams(
          buffer,
          {
              {.offset = 0,
               .chunks =
                   {
                       {.rowCount = 30, .size = 10},
                       {.rowCount = 20, .size = 8},
                   }},
              {.offset = 1,
               .chunks =
                   {
                       {.rowCount = 50, .size = 15},
                   }},
          });

      if (config.enableIndexConfig) {
        std::string key = stripe == 0 ? "bbb" : "ddd";
        indexHelper->addStripe({
            {.rowCount = 50, .key = key},
        });
      }

      tabletWriter->writeStripe(50, std::move(streams));
    }

    tabletWriter->close();
    writeFile.close();

    // Read back
    auto readFile =
        std::make_shared<nimble::testing::InMemoryTrackableReadFile>(
            file, false);
    nimble::TabletReader::Options readOptions;
    readOptions.loadClusterIndex = true;
    readOptions.loadDenseIndexes = true;
    auto tablet = TabletTest::createTabletReader(
        readFile, readOptions, BufferedInputMode::kNone);
    EXPECT_EQ(tablet->stripeCount(), 2);

    // Verify optional sections
    EXPECT_EQ(
        tablet->hasOptionalSection(std::string(nimble::kChunkIndexSection)),
        config.expectChunkIndex);
    EXPECT_EQ(
        tablet->hasOptionalSection(std::string(nimble::kClusterIndexSection)),
        config.expectClusterIndex);

    // Verify chunk index functionality
    if (config.expectChunkIndex) {
      auto stripeId = tablet->stripeIdentifier(0);
      ASSERT_NE(stripeId.chunkIndex(), nullptr);

      nimble::index::test::ChunkIndexTestHelper chunkHelper(
          stripeId.chunkIndex().get());
      EXPECT_EQ(chunkHelper.streamCount(), 2);

      auto stream0Stats = chunkHelper.streamStats(0);
      // 2 stripes in 1 group, stream 0: 2 chunks each (rows: 30, 20)
      EXPECT_EQ(stream0Stats.chunkCounts, (std::vector<uint32_t>{2, 4}));
      EXPECT_EQ(
          stream0Stats.chunkRows, (std::vector<uint32_t>{30, 50, 30, 50}));
    }

    // Verify cluster index functionality
    if (config.expectClusterIndex) {
      EXPECT_TRUE(tablet->hasClusterIndex());
      const auto* idx = tablet->clusterIndex();
      ASSERT_NE(idx, nullptr);
      EXPECT_EQ(idx->layout().numPartitions, 1);

      // Key lookup — 1 partition with 2 chunks (50 rows each), keys
      // "bbb" and "ddd". Chunk-level precision narrows the start row.
      const nimble::RowRange chunk0(0, 100);
      const nimble::RowRange chunk1(50, 100);
      auto lookupKey = [&](const std::string& key) {
        std::vector<velox::serializer::EncodedKeyBounds> keyBounds = {
            {.lowerKey = key}};
        return idx->lookup(
            nimble::index::IndexLookup::LookupRequest::rangeScan(keyBounds));
      };
      ASSERT_EQ(lookupKey("aaa")[0].size(), 1);
      EXPECT_EQ(lookupKey("aaa")[0][0], chunk0);
      EXPECT_EQ(lookupKey("bbb")[0][0], chunk0);
      EXPECT_EQ(lookupKey("ccc")[0][0], chunk1);
      EXPECT_EQ(lookupKey("ddd")[0][0], chunk1);
      EXPECT_TRUE(lookupKey("eee")[0].empty());
    }

    // When chunk index is not enabled, chunkIndex should be null
    if (!config.expectChunkIndex) {
      auto stripeId = tablet->stripeIdentifier(0);
      EXPECT_EQ(stripeId.chunkIndex(), nullptr);
    }
  }
}

TEST_P(TabletWithIndexTest, emptyFileWithIndexConfig) {
  // Test writing an empty file (no stripes) with index config.
  // The root index should contain only config (columns, sort orders)
  // but no stripe keys or stripe index groups.
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);

  nimble::index::test::TestClusterIndexMetadataWriter indexHelper(
      *pool_,
      {"col1", "col2", "col3"},
      {SortOrder{.ascending = true},
       SortOrder{.ascending = false},
       SortOrder{.ascending = true}},
      /*enforceKeyOrder=*/true,
      /*noDuplicateKey=*/false);

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          .enableChunkIndex = true,
          .stripeGroupFlushCallback =
              indexHelper.createStripeGroupFlushCallback(),
          .closeCallback = indexHelper.createCloseCallback(),
      });

  // Close without writing any stripes.
  tabletWriter->close();
  writeFile.close();

  // Verify the file can be read.
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = createTabletReader(readFile);

  // Verify no stripes.
  EXPECT_EQ(tablet->stripeCount(), 0);

  // Empty file: no data was written, so no cluster index section is produced.
  EXPECT_FALSE(
      tablet->hasOptionalSection(std::string(nimble::kClusterIndexSection)));
  EXPECT_FALSE(tablet->hasClusterIndex());
}

TEST_P(TabletWithIndexTest, fileLayoutWithIndex) {
  // Test FileLayout::create() with non-empty file that has index enabled.
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  nimble::Buffer buffer(*pool_);

  nimble::index::test::TestClusterIndexMetadataWriter indexHelper(
      *pool_, {"col1"}, {SortOrder{.ascending = true}});

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          .metadataFlushThreshold = 1024 * 1024 * 1024,
          .streamDeduplicationEnabled = false,
          .enableChunkIndex = true,
          .stripeGroupFlushCallback =
              indexHelper.createStripeGroupFlushCallback(),
          .closeCallback = indexHelper.createCloseCallback(),
      });

  // Write two stripes with index
  for (int stripe = 0; stripe < 2; ++stripe) {
    auto streams = createStreams(
        buffer, {{.offset = 0, .chunks = {{.rowCount = 100, .size = 50}}}});
    indexHelper.addStripe(
        {{.rowCount = 100, .key = std::to_string(stripe * 100 + 99)}});
    tabletWriter->writeStripe(100, std::move(streams));
  }
  tabletWriter->close();
  writeFile.close();

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto layout = nimble::FileLayout::create(readFile, pool_.get());

  EXPECT_EQ(layout.fileSize, file.size());
  EXPECT_EQ(layout.stripesInfo.size(), 2);
  EXPECT_EQ(layout.stripeGroups.size(), 1);
  // With index and stripes, should have index groups
  EXPECT_EQ(layout.indexPartitions.size(), 1);
  EXPECT_GT(layout.indexPartitions[0].size(), 0);
  // Per-stripe info
  EXPECT_EQ(layout.stripesInfo.size(), 2);
  for (size_t i = 0; i < layout.stripesInfo.size(); ++i) {
    EXPECT_EQ(layout.stripesInfo[i].stripeGroupIndex, 0);
    EXPECT_GT(layout.stripesInfo[i].size, 0);
  }
}

TEST_P(TabletWithIndexTest, fileLayoutOrdering) {
  // Verify the physical layout ordering of a Nimble file with cluster index:
  //   [stripe data] → [stripe group metadata] → [index partition metadata]
  //   → [stripes metadata] → [optional sections] → [footer] → [postscript]
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  nimble::Buffer buffer(*pool_);

  nimble::index::test::TestClusterIndexMetadataWriter indexHelper(
      *pool_, {"col1"}, {SortOrder{.ascending = true}});

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          .metadataFlushThreshold = 1'024 * 1'024 * 1'024,
          .streamDeduplicationEnabled = false,
          .enableChunkIndex = true,
          .stripeGroupFlushCallback =
              indexHelper.createStripeGroupFlushCallback(),
          .closeCallback = indexHelper.createCloseCallback(),
      });

  constexpr int kNumStripes = 3;
  for (int stripe = 0; stripe < kNumStripes; ++stripe) {
    auto streams = createStreams(
        buffer, {{.offset = 0, .chunks = {{.rowCount = 100, .size = 50}}}});
    indexHelper.addStripe(
        {{.rowCount = 100, .key = fmt::format("key_{:03d}", stripe)}});
    tabletWriter->writeStripe(100, std::move(streams));
  }
  tabletWriter->close();
  writeFile.close();

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  const auto layout = nimble::FileLayout::create(readFile, pool_.get());

  // Verify basic structure.
  ASSERT_EQ(layout.stripesInfo.size(), kNumStripes);
  ASSERT_EQ(layout.stripeGroups.size(), 1);
  ASSERT_EQ(layout.indexPartitions.size(), 1);
  EXPECT_GT(layout.footer.size(), 0);

  // Verify stripe data comes first.
  for (size_t i = 0; i < layout.stripesInfo.size(); ++i) {
    SCOPED_TRACE(fmt::format("stripe {}", i));
    EXPECT_GT(layout.stripesInfo[i].size, 0);
    if (i > 0) {
      EXPECT_GE(
          layout.stripesInfo[i].offset,
          layout.stripesInfo[i - 1].offset + layout.stripesInfo[i - 1].size)
          << "Stripes should be in ascending order";
    }
  }

  // Stripe group metadata comes after all stripe data.
  const auto lastStripeEnd =
      layout.stripesInfo.back().offset + layout.stripesInfo.back().size;
  EXPECT_GE(layout.stripeGroups[0].offset(), lastStripeEnd)
      << "Stripe group metadata should come after stripe data";

  // Index partition metadata comes after stripe group metadata.
  EXPECT_GT(layout.indexPartitions[0].offset(), layout.stripeGroups[0].offset())
      << "Index partition metadata should come after stripe group metadata";

  // Optional sections (cluster index, chunk index) come after index partitions.
  const auto clusterIndexIt =
      layout.optionalSections.find(std::string(nimble::kClusterIndexSection));
  ASSERT_NE(clusterIndexIt, layout.optionalSections.end());
  EXPECT_GT(clusterIndexIt->second.offset(), layout.indexPartitions[0].offset())
      << "Cluster index should come after index partition metadata";

  const auto chunkIndexIt =
      layout.optionalSections.find(std::string(nimble::kChunkIndexSection));
  ASSERT_NE(chunkIndexIt, layout.optionalSections.end());
  EXPECT_GT(chunkIndexIt->second.offset(), layout.indexPartitions[0].offset())
      << "Chunk index should come after index partition metadata";

  // Stripes metadata comes after optional sections.
  EXPECT_GT(layout.stripes.offset(), clusterIndexIt->second.offset())
      << "Stripes metadata should come after cluster index";
  EXPECT_GT(layout.stripes.offset(), chunkIndexIt->second.offset())
      << "Stripes metadata should come after chunk index";

  // Footer comes after stripes metadata.
  EXPECT_GT(layout.footer.offset(), layout.stripes.offset())
      << "Footer should come after stripes metadata";

  // Postscript is the last 20 bytes.
  EXPECT_EQ(
      layout.footer.offset() + layout.footer.size() + nimble::kPostscriptSize,
      layout.fileSize)
      << "Footer + postscript should end at file size";
}

TEST_P(TabletWithIndexTest, cacheWarmPath) {
  // Test that a second TabletReader on an indexed file initializes from
  // AsyncDataCache with zero file IO, and that index data is also served
  // from cache on the warm path.
  if (GetParam() != BufferedInputMode::kCachedBufferedInput) {
    GTEST_SKIP() << "Cache warm path only applies to CachedBufferedInput";
  }

  // Write a file with multiple stripes and index.
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  nimble::Buffer buffer(*pool_);

  nimble::index::test::TestClusterIndexMetadataWriter indexHelper(
      *pool_,
      {"col1"},
      {SortOrder{.ascending = true}},
      /*enforceKeyOrder=*/true);

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          .metadataFlushThreshold = 1024 * 1024 * 1024,
          .streamDeduplicationEnabled = false,
          .enableChunkIndex = true,
          .stripeGroupFlushCallback =
              indexHelper.createStripeGroupFlushCallback(),
          .closeCallback = indexHelper.createCloseCallback(),
      });

  constexpr int kNumStripes = 3;
  std::vector<std::string> keys = {"bbb", "ddd", "fff"};
  for (int i = 0; i < kNumStripes; ++i) {
    auto streams = createStreams(
        buffer, {{.offset = 0, .chunks = {{.rowCount = 100, .size = 50}}}});
    indexHelper.addStripe({{.rowCount = 100, .key = keys[i]}});
    tabletWriter->writeStripe(100, std::move(streams));
  }
  tabletWriter->close();
  writeFile.close();

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);

  // Initialize cache before the cold reader so we can verify it starts empty.
  allocator_ = std::make_shared<velox::memory::MallocAllocator>(
      velox::memory::MemoryAllocator::Options{
          .capacity = 1UL << 30, .reservationByteLimit = 0});
  cache_ = velox::cache::AsyncDataCache::create(allocator_.get());

  auto coldCacheStats = cache_->refreshStats();
  EXPECT_EQ(coldCacheStats.numEntries, 0);

  // Preload index sections so the cache warm path can serve them from cache.
  nimble::TabletReader::Options options;
  options.preloadOptionalSections = {
      std::string(nimble::kClusterIndexSection),
      std::string(nimble::kChunkIndexSection)};

  // Cold path: first reader populates the cache.
  {
    auto coldReader = createTabletReader(readFile, options);
    EXPECT_EQ(coldReader->stripeCount(), kNumStripes);
    EXPECT_EQ(coldReader->tabletRowCount(), kNumStripes * 100);
    EXPECT_TRUE(coldReader->hasClusterIndex());
    EXPECT_NE(coldReader->clusterIndex(), nullptr);

    // Cold init bypasses CachedBufferedInput — no IoStatistics tracking.
    EXPECT_EQ(dataIoStats_->ramHit().count(), 0);
    EXPECT_EQ(dataIoStats_->ssdRead().count(), 0);
    EXPECT_EQ(dataIoStats_->read().count(), 0);
    EXPECT_EQ(dataIoStats_->prefetch().count(), 0);

    // Verify stripe groups are accessible.
    for (uint32_t i = 0; i < kNumStripes; ++i) {
      auto stripeId = coldReader->stripeIdentifier(i);
      EXPECT_NE(stripeId.stripeGroup(), nullptr);
    }

    // Verify index lookup works — all stripes in one partition [0, 300).
    std::vector<velox::serializer::EncodedKeyBounds> keyBounds = {
        {.lowerKey = "bbb"}};
    auto result = coldReader->clusterIndex()->lookup(
        nimble::index::IndexLookup::LookupRequest::rangeScan(keyBounds));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result[0].size(), 1);
    EXPECT_EQ(result[0][0], nimble::RowRange(0, 300));
  }

  coldCacheStats = cache_->refreshStats();
  EXPECT_GT(coldCacheStats.numEntries, 0);
  EXPECT_GT(coldCacheStats.numNew, 0);
  EXPECT_EQ(coldCacheStats.numEvict, 0);

  // Reset IO stats and readerOptions for the warm path so the new
  // IoStatistics pointers are picked up by ensureReaderOptions.
  dataIoStats_ = std::make_shared<velox::io::IoStatistics>();
  metadataIoStats_ = std::make_shared<velox::io::IoStatistics>();
  indexIoStats_ = std::make_shared<velox::io::IoStatistics>();
  readerOptions_.reset();

  // Warm path: second reader should initialize from cache with zero IO.
  {
    auto warmReader = createTabletReader(readFile, options);
    EXPECT_EQ(warmReader->stripeCount(), kNumStripes);
    EXPECT_EQ(warmReader->tabletRowCount(), kNumStripes * 100);
    EXPECT_TRUE(warmReader->hasClusterIndex());
    EXPECT_NE(warmReader->clusterIndex(), nullptr);

    // Warm path should serve metadata from RAM cache.
    EXPECT_GT(metadataIoStats_->ramHit().count(), 0);
    EXPECT_EQ(metadataIoStats_->rawBytesRead(), 0);

    // Verify stripe groups are accessible from cache.
    for (uint32_t i = 0; i < kNumStripes; ++i) {
      auto stripeId = warmReader->stripeIdentifier(i);
      EXPECT_NE(stripeId.stripeGroup(), nullptr);
    }

    // Verify index lookup still works from cache — same partition [0, 300).
    std::vector<velox::serializer::EncodedKeyBounds> warmKeyBounds = {
        {.lowerKey = "ddd"}};
    auto warmResult = warmReader->clusterIndex()->lookup(
        nimble::index::IndexLookup::LookupRequest::rangeScan(warmKeyBounds));
    ASSERT_EQ(warmResult.size(), 1);
    ASSERT_EQ(warmResult[0].size(), 1);
    EXPECT_EQ(warmResult[0][0], nimble::RowRange(100, 300));
  }

  // Warm reader should have cache hits and no evictions. The warm reader
  // creates one additional cache entry for the partition metadata (loaded
  // on demand during lookup via loadPartition).
  auto warmCacheStats = cache_->refreshStats();
  EXPECT_EQ(warmCacheStats.numEntries, coldCacheStats.numEntries + 1);
  EXPECT_GT(warmCacheStats.numHit, coldCacheStats.numHit);
  EXPECT_EQ(warmCacheStats.numNew, coldCacheStats.numNew + 1);
  EXPECT_EQ(warmCacheStats.numEvict, 0);
}

INSTANTIATE_TEST_SUITE_P(
    BufferedInputModes,
    TabletWithIndexTest,
    ::testing::Values(
        BufferedInputMode::kNone,
        BufferedInputMode::kBufferedInput,
        BufferedInputMode::kDirectBufferedInput,
        BufferedInputMode::kCachedBufferedInput),
    [](const ::testing::TestParamInfo<BufferedInputMode>& info) {
      return bufferedInputModeToString(info.param);
    });

TEST_P(TabletTest, writeAfterCloseThrows) {
  // Test that write operations throw after close().
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  nimble::Buffer buffer(*pool_);

  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});
  tabletWriter->close();
  writeFile.close();

  // Prepare a stream for writeStripe
  const auto size = 100;
  auto pos = buffer.reserve(size);
  std::memset(pos, 'x', size);
  std::vector<nimble::Stream> streams;
  streams.push_back({
      .offset = 0,
      .chunks = {{.content = {std::string_view(pos, size)}}},
  });

  // writeStripe should throw after close
  NIMBLE_ASSERT_USER_THROW(
      tabletWriter->writeStripe(100, std::move(streams)),
      "TabletWriter is already closed");

  // writeOptionalSection should throw after close
  NIMBLE_ASSERT_USER_THROW(
      tabletWriter->writeOptionalSection("test", "content"),
      "TabletWriter is already closed");

  // close should throw when called twice
  NIMBLE_ASSERT_USER_THROW(
      tabletWriter->close(), "TabletWriter is already closed");
}

TEST_P(TabletTest, readerOptionsAdaptiveMode) {
  // Test adaptive mode (maxFooterIoBytes=0) which reads postscript first,
  // then exact footer size.
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  nimble::Buffer buffer(*pool_);

  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});

  std::vector<nimble::Stream> streams;
  const auto size = 100;
  auto pos = buffer.reserve(size);
  std::memset(pos, 'x', size);
  streams.push_back({
      .offset = 0,
      .chunks = {{.content = {std::string_view(pos, size)}}},
  });
  tabletWriter->writeStripe(500, std::move(streams));
  tabletWriter->close();
  writeFile.close();

  // Read with adaptive mode (maxFooterIoBytes=0)
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  nimble::TabletReader::Options options;
  options.maxFooterIoBytes = 0; // Adaptive mode
  configureMetadataInput(options);
  auto tablet = nimble::TabletReader::create(readFile, pool_.get(), options);

  EXPECT_EQ(tablet->stripeCount(), 1);
  EXPECT_EQ(tablet->stripeRowCount(0), 500);
}

TEST_P(TabletTest, readerOptionsSpeculativeMode) {
  // Test speculative mode (non-zero maxFooterIoBytes).
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  nimble::Buffer buffer(*pool_);

  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});

  std::vector<nimble::Stream> streams;
  const auto size = 100;
  auto pos = buffer.reserve(size);
  std::memset(pos, 'y', size);
  streams.push_back({
      .offset = 0,
      .chunks = {{.content = {std::string_view(pos, size)}}},
  });
  tabletWriter->writeStripe(600, std::move(streams));
  tabletWriter->close();
  writeFile.close();

  // Read with speculative mode
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  nimble::TabletReader::Options options;
  options.maxFooterIoBytes = 1024; // Small speculative read
  configureMetadataInput(options);
  auto tablet = nimble::TabletReader::create(readFile, pool_.get(), options);

  EXPECT_EQ(tablet->stripeCount(), 1);
  EXPECT_EQ(tablet->stripeRowCount(0), 600);
}

TEST_P(TabletTest, configureOptionsIndexFlags) {
  // Verify configureOptions wires loadClusterIndex/loadChunkIndex from
  // ReaderOptions into TabletReader::Options, both as bools and as entries
  // in preloadOptionalSections.
  auto containsSection = [](const std::vector<std::string>& sections,
                            std::string_view name) {
    return std::find(sections.begin(), sections.end(), name) != sections.end();
  };

  velox::dwio::common::ReaderOptions readerOptions(pool_.get());
  readerOptions.setDataIoStats(dataIoStats_);
  readerOptions.setMetadataIoStats(metadataIoStats_);

  {
    SCOPED_TRACE("defaults");
    auto opts = nimble::TabletReader::configureOptions(readerOptions);
    EXPECT_FALSE(opts.loadClusterIndex);
    EXPECT_TRUE(opts.loadChunkIndex);
    EXPECT_TRUE(
        containsSection(opts.preloadOptionalSections, nimble::kSchemaSection));
    EXPECT_FALSE(containsSection(
        opts.preloadOptionalSections, nimble::kClusterIndexSection));
    EXPECT_TRUE(containsSection(
        opts.preloadOptionalSections, nimble::kChunkIndexSection));
  }

  {
    SCOPED_TRACE("both disabled");
    readerOptions.setLoadClusterIndex(false);
    readerOptions.setLoadChunkIndex(false);
    auto opts = nimble::TabletReader::configureOptions(readerOptions);
    EXPECT_FALSE(opts.loadClusterIndex);
    EXPECT_FALSE(opts.loadChunkIndex);
    EXPECT_TRUE(
        containsSection(opts.preloadOptionalSections, nimble::kSchemaSection));
    EXPECT_FALSE(containsSection(
        opts.preloadOptionalSections, nimble::kClusterIndexSection));
    EXPECT_FALSE(containsSection(
        opts.preloadOptionalSections, nimble::kChunkIndexSection));
  }

  {
    SCOPED_TRACE("cluster only");
    readerOptions.setLoadClusterIndex(true);
    readerOptions.setLoadChunkIndex(false);
    auto opts = nimble::TabletReader::configureOptions(readerOptions);
    EXPECT_TRUE(opts.loadClusterIndex);
    EXPECT_FALSE(opts.loadChunkIndex);
    EXPECT_TRUE(containsSection(
        opts.preloadOptionalSections, nimble::kClusterIndexSection));
    EXPECT_FALSE(containsSection(
        opts.preloadOptionalSections, nimble::kChunkIndexSection));
  }

  {
    SCOPED_TRACE("chunk only");
    readerOptions.setLoadClusterIndex(false);
    readerOptions.setLoadChunkIndex(true);
    auto opts = nimble::TabletReader::configureOptions(readerOptions);
    EXPECT_FALSE(opts.loadClusterIndex);
    EXPECT_TRUE(opts.loadChunkIndex);
    EXPECT_FALSE(containsSection(
        opts.preloadOptionalSections, nimble::kClusterIndexSection));
    EXPECT_TRUE(containsSection(
        opts.preloadOptionalSections, nimble::kChunkIndexSection));
  }
}

TEST_P(TabletTest, metadataInputReads) {
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  nimble::Buffer buffer(*pool_);

  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});

  for (int i = 0; i < 3; ++i) {
    std::vector<nimble::Stream> streams;
    const auto size = 100;
    auto pos = buffer.reserve(size);
    std::memset(pos, 'a' + i, size);
    streams.push_back({
        .offset = 0,
        .chunks = {{.rowCount = 100, .content = {std::string_view(pos, size)}}},
    });
    tabletWriter->writeStripe(100, std::move(streams));
  }
  tabletWriter->close();
  writeFile.close();

  auto tablet = createTabletReader(file);
  EXPECT_EQ(tablet->stripeCount(), 3);

  auto stripeId = tablet->stripeIdentifier(0);
  EXPECT_EQ(stripeId.stripeId(), 0);
  EXPECT_NE(stripeId.stripeGroup(), nullptr);
}

TEST_P(TabletTest, rowToStripe) {
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  nimble::Buffer buffer(*pool_);

  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});
  // Write 3 stripes with row counts: 100, 200, 150 (total 450).
  const std::vector<uint32_t> rowCounts = {100, 200, 150};
  for (const auto rowCount : rowCounts) {
    std::vector<nimble::Stream> streams;
    const auto size = 50;
    auto pos = buffer.reserve(size);
    std::memset(pos, 'x', size);
    streams.push_back({
        .offset = 0,
        .chunks = {{.content = {std::string_view(pos, size)}}},
    });
    tabletWriter->writeStripe(rowCount, std::move(streams));
  }
  tabletWriter->close();
  writeFile.close();

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto tablet = createTabletReader(readFile);
  ASSERT_EQ(tablet->stripeCount(), 3);
  ASSERT_EQ(tablet->tabletRowCount(), 450);

  // Verify stripeStartRow.
  EXPECT_EQ(tablet->stripeStartRow(0), 0);
  EXPECT_EQ(tablet->stripeStartRow(1), 100);
  EXPECT_EQ(tablet->stripeStartRow(2), 300);
  // Sentinel: stripeStartRow(stripeCount) == tabletRowCount.
  EXPECT_EQ(tablet->stripeStartRow(3), 450);

  // Verify stripeRowCount.
  EXPECT_EQ(tablet->stripeRowCount(0), 100);
  EXPECT_EQ(tablet->stripeRowCount(1), 200);
  EXPECT_EQ(tablet->stripeRowCount(2), 150);

  // Verify rowToStripe.
  // Stripe 0: rows [0, 100)
  EXPECT_EQ(tablet->rowToStripe(0), 0);
  EXPECT_EQ(tablet->rowToStripe(50), 0);
  EXPECT_EQ(tablet->rowToStripe(99), 0);

  // Stripe 1: rows [100, 300)
  EXPECT_EQ(tablet->rowToStripe(100), 1);
  EXPECT_EQ(tablet->rowToStripe(200), 1);
  EXPECT_EQ(tablet->rowToStripe(299), 1);

  // Stripe 2: rows [300, 450)
  EXPECT_EQ(tablet->rowToStripe(300), 2);
  EXPECT_EQ(tablet->rowToStripe(400), 2);
  EXPECT_EQ(tablet->rowToStripe(449), 2);

  // Out-of-range checks.
  NIMBLE_ASSERT_THROW(
      tablet->rowToStripe(tablet->tabletRowCount()), "exceeds total row count");
  NIMBLE_ASSERT_THROW(
      tablet->rowToStripe(tablet->tabletRowCount() + 1'000),
      "exceeds total row count");
  NIMBLE_ASSERT_THROW(tablet->stripeRowCount(3), "(3 vs. 3)");
  NIMBLE_ASSERT_THROW(tablet->stripeStartRow(4), "(4 vs. 3)");
}

TEST_P(TabletTest, rowToStripeSingleStripe) {
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  nimble::Buffer buffer(*pool_);

  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});
  const auto size = 50;
  auto pos = buffer.reserve(size);
  std::memset(pos, 'x', size);
  std::vector<nimble::Stream> streams;
  streams.push_back({
      .offset = 0,
      .chunks = {{.content = {std::string_view(pos, size)}}},
  });
  tabletWriter->writeStripe(500, std::move(streams));
  tabletWriter->close();
  writeFile.close();

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto tablet = createTabletReader(readFile);
  ASSERT_EQ(tablet->stripeCount(), 1);
  ASSERT_EQ(tablet->tabletRowCount(), 500);

  EXPECT_EQ(tablet->stripeStartRow(0), 0);
  EXPECT_EQ(tablet->stripeStartRow(1), 500);
  EXPECT_EQ(tablet->stripeRowCount(0), 500);

  EXPECT_EQ(tablet->rowToStripe(0), 0);
  EXPECT_EQ(tablet->rowToStripe(250), 0);
  EXPECT_EQ(tablet->rowToStripe(499), 0);

  NIMBLE_ASSERT_THROW(
      tablet->rowToStripe(tablet->tabletRowCount()), "exceeds total row count");
}

TEST_P(TabletTest, cacheWarmPath) {
  if (GetParam() != BufferedInputMode::kCachedBufferedInput) {
    GTEST_SKIP() << "Cache warm path only applies to CachedBufferedInput";
  }

  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  nimble::Buffer buffer(*pool_);

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {.metadataFlushThreshold = 1024 * 1024 * 1024,
       .streamDeduplicationEnabled = false});

  constexpr int kNumStripes = 3;
  for (int i = 0; i < kNumStripes; ++i) {
    std::vector<nimble::Stream> streams;
    const auto size = 50;
    auto* pos = buffer.reserve(size);
    std::memset(pos, 'a' + i, size);
    streams.push_back(
        {.offset = 0,
         .chunks = {
             {.rowCount = 100, .content = {std::string_view(pos, size)}}}});
    tabletWriter->writeStripe(100, std::move(streams));
  }
  tabletWriter->close();
  writeFile.close();

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);

  allocator_ = std::make_shared<velox::memory::MallocAllocator>(
      velox::memory::MemoryAllocator::Options{
          .capacity = 1UL << 30, .reservationByteLimit = 0});
  cache_ = velox::cache::AsyncDataCache::create(allocator_.get());

  auto coldCacheStats = cache_->refreshStats();
  EXPECT_EQ(coldCacheStats.numEntries, 0);

  // Cold path: first reader populates the cache.
  {
    auto coldReader = createTabletReader(readFile);
    EXPECT_EQ(coldReader->stripeCount(), kNumStripes);
    EXPECT_EQ(coldReader->tabletRowCount(), kNumStripes * 100);

    for (uint32_t i = 0; i < kNumStripes; ++i) {
      auto stripeId = coldReader->stripeIdentifier(i);
      EXPECT_NE(stripeId.stripeGroup(), nullptr);
    }
  }

  coldCacheStats = cache_->refreshStats();
  EXPECT_GT(coldCacheStats.numEntries, 0);
  EXPECT_EQ(coldCacheStats.numEvict, 0);

  // Reset IO stats for the warm path.
  dataIoStats_ = std::make_shared<velox::io::IoStatistics>();
  metadataIoStats_ = std::make_shared<velox::io::IoStatistics>();
  indexIoStats_ = std::make_shared<velox::io::IoStatistics>();
  readerOptions_.reset();

  // Warm path: second reader should initialize from cache with zero file IO.
  {
    auto warmReader = createTabletReader(readFile);
    EXPECT_EQ(warmReader->stripeCount(), kNumStripes);
    EXPECT_EQ(warmReader->tabletRowCount(), kNumStripes * 100);

    EXPECT_GT(metadataIoStats_->ramHit().count(), 0);
    EXPECT_EQ(metadataIoStats_->rawBytesRead(), 0);
    EXPECT_EQ(dataIoStats_->rawBytesRead(), 0);
    EXPECT_EQ(indexIoStats_->rawBytesRead(), 0);

    for (uint32_t i = 0; i < kNumStripes; ++i) {
      auto stripeId = warmReader->stripeIdentifier(i);
      EXPECT_NE(stripeId.stripeGroup(), nullptr);
    }
  }

  auto warmCacheStats = cache_->refreshStats();
  EXPECT_EQ(warmCacheStats.numEntries, coldCacheStats.numEntries);
  EXPECT_GT(warmCacheStats.numHit, coldCacheStats.numHit);
  EXPECT_EQ(warmCacheStats.numEvict, 0);
}

INSTANTIATE_TEST_SUITE_P(
    BufferedInputModes,
    TabletTest,
    ::testing::Values(
        BufferedInputMode::kNone,
        BufferedInputMode::kBufferedInput,
        BufferedInputMode::kDirectBufferedInput,
        BufferedInputMode::kCachedBufferedInput),
    [](const ::testing::TestParamInfo<BufferedInputMode>& info) {
      return bufferedInputModeToString(info.param);
    });

// Stress test: concurrent readers with mixed BufferedInput modes and periodic
// cache eviction. Exercises race conditions between cache population, cache
// eviction, and reader initialization.
TEST_F(TabletTest, concurrentReadersWithCacheEviction) {
  auto pool =
      velox::memory::MemoryManager::getInstance()->addRootPool("stressTest");

  // Write a file with multiple stripes.
  std::string file;
  {
    auto writerPool = pool->addLeafChild("writer");
    velox::InMemoryWriteFile writeFile(&file);
    nimble::Buffer buffer(*writerPool);
    auto tabletWriter =
        nimble::TabletWriter::create(&writeFile, *writerPool, {});
    constexpr int kNumStripes = 5;
    for (int i = 0; i < kNumStripes; ++i) {
      std::vector<nimble::Stream> streams;
      const auto size = 200;
      auto pos = buffer.reserve(size);
      std::memset(pos, 'a' + i, size);
      streams.push_back({
          .offset = 0,
          .chunks =
              {{.rowCount = 200, .content = {std::string_view(pos, size)}}},
      });
      tabletWriter->writeStripe(200, std::move(streams));
    }
    tabletWriter->close();
    writeFile.close();
  }

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);

  // Shared cache infrastructure.
  auto allocator = std::make_shared<velox::memory::MallocAllocator>(
      velox::memory::MemoryAllocator::Options{
          .capacity = 1UL << 30, .reservationByteLimit = 0});
  auto cache = velox::cache::AsyncDataCache::create(allocator.get());
  auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(4);

  constexpr int kNumReaderThreads = 8;
  constexpr auto kTestDuration = std::chrono::seconds(20);
  std::atomic_bool stop{false};
  std::atomic_uint64_t readCount{0};

  auto readerFunc = [&](int threadId) {
    auto threadPool = pool->addLeafChild(fmt::format("reader_{}", threadId));
    auto& ids = velox::fileIds();

    while (!stop.load(std::memory_order_relaxed)) {
      auto mode = static_cast<BufferedInputMode>(threadId % 4);
      velox::io::ReaderOptions readerOptions(threadPool.get());
      readerOptions.setDataIoStats(dataIoStats_);
      readerOptions.setMetadataIoStats(metadataIoStats_);
      nimble::TabletReader::Options options;
      options.ioOptions = readerOptions;

      velox::FileHandle fileHandle;
      switch (mode) {
        case BufferedInputMode::kNone:
        case BufferedInputMode::kBufferedInput:
        case BufferedInputMode::kDirectBufferedInput:
          break;

        case BufferedInputMode::kCachedBufferedInput: {
          fileHandle.file = readFile;
          fileHandle.uuid =
              velox::StringIdLease(ids, fmt::format("stress_{}", threadId));
          fileHandle.groupId = velox::StringIdLease(ids, "stressGroup");
          options.cache = cache.get();
          options.fileHandle = &fileHandle;
          break;
        }
      }

      auto reader =
          nimble::TabletReader::create(readFile, threadPool.get(), options);
      EXPECT_EQ(reader->stripeCount(), 5);
      EXPECT_EQ(reader->tabletRowCount(), 1000);

      // Verify stripe data is accessible.
      for (uint32_t i = 0; i < reader->stripeCount(); ++i) {
        auto stripeId = reader->stripeIdentifier(i);
        EXPECT_NE(stripeId.stripeGroup(), nullptr);
      }
      readCount.fetch_add(1, std::memory_order_relaxed);
    }
  };

  // Start reader threads.
  std::vector<std::thread> threads;
  for (int i = 0; i < kNumReaderThreads; ++i) {
    threads.emplace_back(readerFunc, i);
  }

  // Control thread: periodically evict all cache entries to force transitions
  // between warm and cold init paths.
  auto controlThread = std::thread([&] {
    while (!stop.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      cache->shrink(1);
    }
  });

  std::this_thread::sleep_for(kTestDuration);
  stop.store(true, std::memory_order_relaxed);

  for (auto& t : threads) {
    t.join();
  }
  controlThread.join();

  LOG(INFO) << "Stress test: " << readCount.load() << " successful reads in "
            << kTestDuration.count() << "s";
  EXPECT_GT(readCount.load(), 0);

  cache->shutdown();
}

TEST_P(TabletWithIndexTest, metadataSectionUncompressedSize) {
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  nimble::Buffer buffer(*pool_);

  nimble::index::test::TestClusterIndexMetadataWriter indexHelper(
      *pool_, {"col1"}, {SortOrder{.ascending = true}});

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          .metadataFlushThreshold = 1'024 * 1'024 * 1'024,
          .streamDeduplicationEnabled = false,
          .enableChunkIndex = true,
          .stripeGroupFlushCallback =
              indexHelper.createStripeGroupFlushCallback(),
          .closeCallback = indexHelper.createCloseCallback(),
      });

  for (int stripe = 0; stripe < 2; ++stripe) {
    auto streams = createStreams(
        buffer, {{.offset = 0, .chunks = {{.rowCount = 100, .size = 50}}}});
    indexHelper.addStripe(
        {{.rowCount = 100, .key = fmt::format("key_{:03d}", stripe)}});
    tabletWriter->writeStripe(100, std::move(streams));
  }
  tabletWriter->close();
  writeFile.close();

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  const auto layout = nimble::FileLayout::create(readFile, pool_.get());

  ASSERT_TRUE(layout.stripes.uncompressedSize().has_value());
  EXPECT_GT(layout.stripes.uncompressedSize().value(), 0);
  EXPECT_GE(layout.stripes.uncompressedSize().value(), layout.stripes.size());

  for (size_t i = 0; i < layout.stripeGroups.size(); ++i) {
    SCOPED_TRACE(fmt::format("stripeGroup {}", i));
    ASSERT_TRUE(layout.stripeGroups[i].uncompressedSize().has_value());
    EXPECT_GT(layout.stripeGroups[i].uncompressedSize().value(), 0);
    EXPECT_GE(
        layout.stripeGroups[i].uncompressedSize().value(),
        layout.stripeGroups[i].size());
  }

  for (const auto& [name, section] : layout.optionalSections) {
    SCOPED_TRACE(fmt::format("optionalSection '{}'", name));
    ASSERT_TRUE(section.uncompressedSize().has_value());
    EXPECT_GT(section.uncompressedSize().value(), 0);
    EXPECT_GE(section.uncompressedSize().value(), section.size());
  }
}

} // namespace
