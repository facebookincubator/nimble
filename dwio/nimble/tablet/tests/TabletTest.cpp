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
#include "dwio/nimble/index/tests/IndexTestUtils.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tablet/FileLayout.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/tablet/TabletWriter.h"
#include "dwio/nimble/tablet/tests/TabletTestUtils.h"
#include "folly/FileUtil.h"
#include "folly/Random.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "velox/common/file/File.h"

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/DirectBufferedInput.h"
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
    ioStatistics_ = std::make_shared<velox::dwio::common::IoStatistics>();
  }

  void TearDown() override {
    bufferedInput_.reset();
    if (cache_) {
      cache_->shutdown();
      cache_.reset();
    }
    allocator_.reset();
    executor_.reset();
  }

  /// Creates a BufferedInput based on the test mode.
  /// Returns nullptr for kNone mode.
  velox::dwio::common::BufferedInput* createBufferedInput(
      std::shared_ptr<velox::ReadFile> readFile) {
    auto mode = GetParam();

    auto& ids = velox::fileIds();
    fileId_ = std::make_unique<velox::StringIdLease>(ids, "testFile");
    groupId_ = std::make_unique<velox::StringIdLease>(ids, "testGroup");

    velox::io::ReaderOptions readerOptions(pool_.get());

    switch (mode) {
      case BufferedInputMode::kNone:
        return nullptr;

      case BufferedInputMode::kBufferedInput:
        bufferedInput_ = std::make_unique<velox::dwio::common::BufferedInput>(
            readFile, *pool_);
        return bufferedInput_.get();

      case BufferedInputMode::kDirectBufferedInput:
        tracker_ = std::make_shared<velox::cache::ScanTracker>(
            "testTracker", nullptr, 256 << 10);
        bufferedInput_ =
            std::make_unique<velox::dwio::common::DirectBufferedInput>(
                readFile,
                velox::dwio::common::MetricsLog::voidLog(),
                std::move(*fileId_),
                tracker_,
                std::move(*groupId_),
                ioStatistics_,
                nullptr,
                executor_.get(),
                readerOptions);
        return bufferedInput_.get();

      case BufferedInputMode::kCachedBufferedInput:
        if (!allocator_) {
          allocator_ = std::make_shared<velox::memory::MallocAllocator>(
              velox::memory::MemoryAllocator::Options{
                  .capacity = 1UL << 30, .reservationByteLimit = 0});
        }
        if (!cache_) {
          cache_ = velox::cache::AsyncDataCache::create(allocator_.get());
        }
        tracker_ = std::make_shared<velox::cache::ScanTracker>(
            "testTracker", nullptr, 256 << 10);
        bufferedInput_ =
            std::make_unique<velox::dwio::common::CachedBufferedInput>(
                readFile,
                velox::dwio::common::MetricsLog::voidLog(),
                std::move(*fileId_),
                cache_.get(),
                tracker_,
                std::move(*groupId_),
                ioStatistics_,
                nullptr,
                executor_.get(),
                readerOptions);
        return bufferedInput_.get();
    }
    return nullptr;
  }

  bool expectHasCache() const {
    return GetParam() == BufferedInputMode::kCachedBufferedInput;
  }

  /// Creates a TabletReader with BufferedInput configured based on test mode.
  /// This is the primary method tests should use to create readers.
  std::shared_ptr<nimble::TabletReader> createTabletReader(
      std::shared_ptr<velox::ReadFile> readFile,
      nimble::TabletReader::Options options = {}) {
    readFile_ = readFile;
    options.bufferedInput = createBufferedInput(readFile_);
    return nimble::TabletReader::create(readFile_, pool_.get(), options);
  }

  /// Overload for string file content (creates InMemoryReadFile internally).
  std::shared_ptr<nimble::TabletReader> createTabletReader(
      const std::string& fileContent,
      nimble::TabletReader::Options options = {}) {
    return createTabletReader(
        std::make_shared<velox::InMemoryReadFile>(fileContent),
        std::move(options));
  }

  std::shared_ptr<velox::ReadFile> readFile_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<velox::dwio::common::IoStatistics> ioStatistics_;
  std::shared_ptr<velox::memory::MallocAllocator> allocator_;
  std::shared_ptr<velox::cache::AsyncDataCache> cache_;
  std::shared_ptr<velox::cache::ScanTracker> tracker_;
  std::unique_ptr<velox::StringIdLease> fileId_;
  std::unique_ptr<velox::StringIdLease> groupId_;
  std::unique_ptr<velox::dwio::common::BufferedInput> bufferedInput_;

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
        tabletWriter->writeStripe(
            stripe.rowCount, stripe.streams, /*keyStream=*/std::nullopt);
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
        auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});

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

        EXPECT_EQ(extraReads, (stripeGroupCount == 1 ? 0 : stripeGroupCount));

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
      tabletWriter->writeStripe(
          stripe.rowCount, stripe.streams, /*keyStream=*/std::nullopt);
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
      auto tablet =
          nimble::TabletReader::create(readFile, this->pool_.get(), {});
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
    auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});

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
    auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});

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
  auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});

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
  auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});

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
    // Expecting one initial read, to read the footer.
    // Each section load should increment read count
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

    // Preloading small section covered by footer.
    // Expecting one initial read, to read only the footer.
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

    // Preloading section partially covered by footer.
    // Expecting one initial read for the footer, and an additional read to read
    // the full preloaded section.
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

    // Preloading section completely not covered by footer.
    // Expecting one initial read for the footer, and an additional read to read
    // the uncovered preloaded section.
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
    // Expecting one initial read for the footer, and an additional read to read
    // the uncovered preloaded section.
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
    // Expecting one initial read for the footer (and the covered sections), and
    // additional reads to read the partial/uncovered preloaded sections.
    verify(
        /* preload */
        {"section2", "section4", "section1", "section3", "section5"},
        /* expectedInitialReads */ 4,
        {
            // All sections are preloaded, so no addtional reads
            {"section1", 4, "aaaa"},
            {"section2", 4, "bbbb"},
            {"section3", 4, random},
            {"section4", 4, "dddd"},
            {"section5", 4, "eeee"},
            // Now all sections are consumed so all should trigger reads
            {"section1", 5, "aaaa"},
            {"section2", 6, "bbbb"},
            {"section3", 7, random},
            {"section4", 8, "dddd"},
            {"section5", 9, "eeee"},
        });
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

// TabletWithIndexTest is a derived test fixture for tablet index related tests.
// It inherits the memory pool and helper methods from TabletTest.
class TabletWithIndexTest : public TabletTest {
 protected:
  // Use shared test data structs from ClusterIndexTestUtils.h
  using ChunkSpec = nimble::index::test::ChunkSpec;
  using KeyChunkSpec = nimble::index::test::KeyChunkSpec;
  using StreamSpec = nimble::index::test::StreamSpec;

  // Specification for testing index lookup results
  struct LookupTestCase {
    std::string key;
    std::optional<uint32_t>
        expectedStripeIndex; // nullopt means no match expected
  };

  // Specification for key lookup verification through
  // ClusterIndexGroup::lookupChunk
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

  // Helper to create a KeyStream using the shared implementation
  static nimble::KeyStream createKeyStream(
      nimble::Buffer& buffer,
      const std::vector<KeyChunkSpec>& chunkSpecs) {
    return nimble::index::test::createKeyStream(buffer, chunkSpecs);
  }

  // Helper to verify tablet index lookups
  static void verifyClusterIndexLookups(
      const nimble::ClusterIndex* index,
      const std::vector<LookupTestCase>& testCases) {
    for (const auto& testCase : testCases) {
      auto location = index->lookup(testCase.key);
      if (testCase.expectedStripeIndex.has_value()) {
        ASSERT_TRUE(location.has_value())
            << "Expected key '" << testCase.key << "' to match stripe "
            << testCase.expectedStripeIndex.value() << ", but got no match";
        EXPECT_EQ(location->stripeIndex, testCase.expectedStripeIndex.value())
            << "Key '" << testCase.key << "' matched wrong stripe";
      } else {
        EXPECT_FALSE(location.has_value())
            << "Expected key '" << testCase.key
            << "' to have no match, but got stripe " << location->stripeIndex;
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

    ASSERT_NE(stripeIdentifier.clusterIndex(), nullptr)
        << "Index group should be available for stripe " << stripeIdx;
    ASSERT_NE(stripeIdentifier.stripeGroup(), nullptr)
        << "Stripe group should be available for stripe " << stripeIdx;

    nimble::index::test::ClusterIndexGroupTestHelper indexGroupHelper(
        stripeIdentifier.clusterIndex().get());

    // Get stream sizes from stripe group metadata
    auto streamSizes = tablet.streamSizes(stripeIdentifier);
    ASSERT_GT(streamSizes.size(), streamId)
        << "Stream " << streamId << " not found in stripe " << stripeIdx;

    const uint32_t streamSize = streamSizes[streamId];

    // Calculate stripe offset within the index group
    const uint32_t stripeOffsetInGroup =
        stripeIdx - indexGroupHelper.firstStripe();

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

  // Helper to verify key lookups through ClusterIndexGroup::lookupChunk.
  // This verifies that:
  // 1. Each key can be looked up through the index group
  // 2. The returned row offset within the stripe is correct
  // 3. The file row ID (stripe start row + row offset) matches expected value
  //
  // File row ID = sum of row counts of all previous stripes + row offset within
  // stripe
  static void verifyKeyLookupFileRowIds(
      const nimble::TabletReader& tablet,
      const std::vector<KeyLookupTestCase>& testCases) {
    // Pre-compute stripe start row offsets
    std::vector<uint64_t> stripeStartRows;
    stripeStartRows.reserve(tablet.stripeCount());
    uint64_t startRow = 0;
    for (uint32_t i = 0; i < tablet.stripeCount(); ++i) {
      stripeStartRows.push_back(startRow);
      startRow += tablet.stripeRowCount(i);
    }

    for (const auto& testCase : testCases) {
      // Get stripe identifier with index loaded
      auto stripeId = tablet.stripeIdentifier(testCase.expectedStripeIndex);
      ASSERT_NE(stripeId.clusterIndex(), nullptr)
          << "Index group should be available for stripe "
          << testCase.expectedStripeIndex << " when looking up key '"
          << testCase.key << "'";

      // Lookup chunk by encoded key
      auto chunkLocation = stripeId.clusterIndex()->lookupChunk(
          testCase.expectedStripeIndex, testCase.key);
      ASSERT_TRUE(chunkLocation.has_value())
          << "Key '" << testCase.key << "' should be found in stripe "
          << testCase.expectedStripeIndex;

      // Calculate global row ID
      uint64_t globalRowId = stripeStartRows[testCase.expectedStripeIndex] +
          chunkLocation->rowOffset;

      EXPECT_EQ(globalRowId, testCase.expectedFileRowId)
          << "Key '" << testCase.key << "' in stripe "
          << testCase.expectedStripeIndex << ": expected file row ID "
          << testCase.expectedFileRowId << ", got " << globalRowId
          << " (stripe start row = "
          << stripeStartRows[testCase.expectedStripeIndex]
          << ", row offset = " << chunkLocation->rowOffset << ")";
    }
  }
};

TEST_P(TabletWithIndexTest, stripeIdentifier) {
  // Test that stripeIdentifier returns both stripe group and cluster index.
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);

  nimble::ClusterIndexConfig indexConfig{
      .columns = {"col1"},
      .sortOrders = {SortOrder{.ascending = true}},
      .enforceKeyOrder = true,
  };

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          .metadataFlushThreshold = 1024 * 1024 * 1024,
          .streamDeduplicationEnabled = false,
          .indexConfig = indexConfig,
      });

  nimble::Buffer buffer{*pool_};

  // Write single stripe with index
  auto streams = createStreams(
      buffer, {{.offset = 0, .chunks = {{.rowCount = 100, .size = 10}}}});
  auto keyStream = createKeyStream(
      buffer, {{.rowCount = 100, .firstKey = "aaa", .lastKey = "bbb"}});
  tabletWriter->writeStripe(100, std::move(streams), std::move(keyStream));
  tabletWriter->close();

  // Read and verify
  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});

  ASSERT_EQ(tablet->stripeCount(), 1);
  ASSERT_NE(tablet->clusterIndex(), nullptr);

  auto stripeId = tablet->stripeIdentifier(0);
  EXPECT_NE(stripeId.stripeGroup(), nullptr);
  EXPECT_NE(stripeId.clusterIndex(), nullptr);
}

TEST_P(TabletWithIndexTest, singleGroup) {
  // Test writing a tablet with index configuration and reading it back.
  // Each stream has multiple chunks with varying row counts and sizes.
  // Different streams are not synchronized in chunk count, rows, or size.
  // This test ensures only one stripe group is created.
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);

  nimble::ClusterIndexConfig indexConfig{
      .columns = {"col1", "col2"},
      .sortOrders =
          {SortOrder{.ascending = true}, SortOrder{.ascending = true}},
      .enforceKeyOrder = true,
  };

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          // Set a large threshold to ensure all stripes stay in one group
          .metadataFlushThreshold = 1024 * 1024 * 1024,
          .streamDeduplicationEnabled = false,
          .indexConfig = indexConfig,
      });

  nimble::Buffer buffer{*pool_};

  // Write stripe 1: Total rows: 100
  // Stream 0: 3 chunks (rows: 30, 45, 25)
  // Stream 1: 2 chunks (rows: 60, 40)
  // Key stream: 2 chunks (rows: 50, 50), keys: "aaa"->"bbb", "bbb"->"ccc"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {.rowCount = 50, .firstKey = "aaa", .lastKey = "bbb"},
            {.rowCount = 50, .firstKey = "bbb", .lastKey = "ccc"},
        });

    tabletWriter->writeStripe(100, std::move(streams), std::move(keyStream));
  }

  // Write stripe 2: Total rows: 200
  // Stream 0: 4 chunks (rows: 40, 55, 65, 40)
  // Stream 1: 1 chunk (rows: 200)
  // Key stream: 4 chunks (rows: 50, 50, 50, 50), keys: "ccc"->"ddd",
  // "ddd"->"eee", "eee"->"fff", "fff"->"ggg"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {.rowCount = 50, .firstKey = "ccc", .lastKey = "ddd"},
            {.rowCount = 50, .firstKey = "ddd", .lastKey = "eee"},
            {.rowCount = 50, .firstKey = "eee", .lastKey = "fff"},
            {.rowCount = 50, .firstKey = "fff", .lastKey = "ggg"},
        });

    tabletWriter->writeStripe(200, std::move(streams), std::move(keyStream));
  }

  // Write stripe 3: Total rows: 150
  // Stream 0: 2 chunks (rows: 100, 50)
  // Stream 1: 5 chunks (rows: 20, 35, 40, 25, 30)
  // Key stream: 1 chunk (rows: 150), keys: "ggg"->"hhh"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {.rowCount = 150, .firstKey = "ggg", .lastKey = "hhh"},
        });

    tabletWriter->writeStripe(150, std::move(streams), std::move(keyStream));
  }

  tabletWriter->close();
  writeFile.close();

  // Read and verify the tablet
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});

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
  EXPECT_EQ(index->numIndexGroups(), 1);
  // Verify the first group, index group, and chunk index group is pinned
  // and is the only one (covered by footer IO).
  EXPECT_TRUE(tabletHelper.hasOnlyFirstStripeGroupCached());
  EXPECT_TRUE(tabletHelper.hasOnlyFirstIndexGroupCached());
  EXPECT_TRUE(tabletHelper.hasOnlyFirstChunkIndexGroupCached());

  // Verify index columns
  EXPECT_EQ(index->indexColumns().size(), 2);
  EXPECT_EQ(index->indexColumns()[0], "col1");
  EXPECT_EQ(index->indexColumns()[1], "col2");

  // Verify stripe count
  EXPECT_EQ(index->numStripes(), 3);

  // Verify stripe keys
  // First stripe: firstKey="aaa", lastKey="ccc"
  // Second stripe: lastKey="fff"
  // Third stripe: lastKey="hhh"
  // minKey="aaa", stripeKey(0)="ccc", stripeKey(1)="fff", stripeKey(2)="hhh"
  EXPECT_EQ(index->minKey(), "aaa");
  EXPECT_EQ(index->stripeKey(0), "ccc");
  EXPECT_EQ(index->stripeKey(1), "ggg");
  EXPECT_EQ(index->stripeKey(2), "hhh");
  EXPECT_EQ(index->maxKey(), "hhh");

  // Verify key lookups - test all possible keys at boundaries
  // Key boundaries:
  // - minKey = "aaa", maxKey = "hhh"
  // - Stripe 0: covers ["aaa", "ccc"], keys: "aaa"->"bbb", "bbb"->"ccc"
  // - Stripe 1: covers ("ccc", "ggg"], keys: "ccc"->"ddd", "ddd"->"eee",
  //   "eee"->"fff", "fff"->"ggg"
  // - Stripe 2: covers ("ggg", "hhh"], keys: "ggg"->"hhh"
  verifyClusterIndexLookups(
      index,
      {
          // Keys before minKey - should return nullopt
          {"000", std::nullopt},
          {"9", std::nullopt},
          {"a", std::nullopt},
          {"aa", std::nullopt},

          // minKey boundary - stripe 0
          {"aaa", 0},

          // Keys within stripe 0
          {"aab", 0},
          {"aaZ", std::nullopt},
          {"abb", 0},
          {"b", 0},
          {"bbb", 0},
          {"bbc", 0},
          {"c", 0},
          {"cc", 0},
          {"ccb", 0},

          // Stripe 0/1 boundary (stripeKey(0) = "ccc")
          {"ccc", 0},

          // Keys within stripe 1
          {"ccd", 1},
          {"d", 1},
          {"ddd", 1},
          {"dde", 1},
          {"e", 1},
          {"eee", 1},
          {"eef", 1},
          {"f", 1},
          {"fff", 1},
          {"ffg", 1},
          {"g", 1},
          {"gg", 1},
          {"ggf", 1},

          // Stripe 1/2 boundary (stripeKey(1) = "ggg")
          {"ggg", 1},

          // Keys within stripe 2
          {"ggh", 2},
          {"h", 2},
          {"hh", 2},
          {"hhg", 2},

          // maxKey boundary - stripe 2
          {"hhh", 2},

          // Keys after maxKey - should return nullopt
          {"hhi", std::nullopt},
          {"i", std::nullopt},
          {"iii", std::nullopt},
          {"z", std::nullopt},
          {"zzz", std::nullopt},
      });

  // Verify stripeIdentifier returns both stripe group and cluster index
  {
    auto stripeIdWithIndex = tablet->stripeIdentifier(0);
    EXPECT_NE(stripeIdWithIndex.stripeGroup(), nullptr);
    EXPECT_NE(stripeIdWithIndex.clusterIndex(), nullptr);

    // Verify the stripe index group content using test helper
    nimble::index::test::ClusterIndexGroupTestHelper indexGroupHelper(
        stripeIdWithIndex.clusterIndex().get());
    EXPECT_EQ(indexGroupHelper.groupIndex(), 0);
    EXPECT_EQ(indexGroupHelper.firstStripe(), 0);
    EXPECT_EQ(indexGroupHelper.stripeCount(), 3);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeIdWithIndex.chunkIndex().get());
    // We have 2 streams per stripe
    EXPECT_EQ(chunkHelper.streamCount(), 2);

    // Verify key stream stats persisted in the index
    // Stripe 0: 2 chunks (rows: 50, 50), keys: "aaa"->"bbb", "bbb"->"ccc"
    // Stripe 1: 4 chunks (rows: 50, 50, 50, 50), keys: "ccc"->"ddd",
    // "ddd"->"eee", "eee"->"fff", "fff"->"ggg"
    // Stripe 2: 1 chunk (rows: 150), keys: "ggg"->"hhh"
    {
      const auto keyStats = indexGroupHelper.keyStreamStats();

      // Accumulated chunk counts per stripe: 2, 2+4=6, 6+1=7
      EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{2, 6, 7}));

      // Accumulated row counts per chunk (within each stripe, reset per stripe)
      // Stripe 0: 50, 100
      // Stripe 1: 50, 100, 150, 200
      // Stripe 2: 150
      EXPECT_EQ(
          keyStats.chunkRows,
          (std::vector<uint32_t>{50, 100, 50, 100, 150, 200, 150}));

      // Last key for each chunk (chunkKeys stores last keys)
      EXPECT_EQ(
          keyStats.chunkKeys,
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
  EXPECT_TRUE(tabletHelper.hasOnlyFirstIndexGroupCached());

  // Verify key lookups through ClusterIndexGroup::lookupChunk return correct
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
          // Keys in chunk 0 ["aaa", "bbb"]: rowOffset = 0, globalRowId = 0 + 0
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

  nimble::ClusterIndexConfig indexConfig{
      .columns = {"col1", "col2"},
      .sortOrders =
          {SortOrder{.ascending = true}, SortOrder{.ascending = true}},
      .enforceKeyOrder = true,
  };

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          // Set threshold to 0 to force flush after every stripe
          .metadataFlushThreshold = 0,
          .streamDeduplicationEnabled = false,
          .indexConfig = indexConfig,
      });

  nimble::Buffer buffer{*pool_};

  // Write stripe 0 (Group 0): Total rows: 100
  // Stream 0: 2 chunks (rows: 50, 50)
  // Stream 1: 3 chunks (rows: 30, 40, 30)
  // Key stream: 2 chunks (rows: 50, 50), keys: "aaa"->"bbb", "bbb"->"ccc"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {50, "aaa", "bbb"},
            {50, "bbb", "ccc"},
        });

    tabletWriter->writeStripe(100, std::move(streams), std::move(keyStream));
  }

  // Write stripe 1 (Group 1): Total rows: 150
  // Stream 0: 3 chunks (rows: 40, 60, 50)
  // Stream 1: 2 chunks (rows: 80, 70)
  // Key stream: 3 chunks (rows: 50, 50, 50), keys: "ccc"->"ddd", "ddd"->"eee",
  // "eee"->"fff"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {.rowCount = 50, .firstKey = "ccc", .lastKey = "ddd"},
            {.rowCount = 50, .firstKey = "ddd", .lastKey = "eee"},
            {.rowCount = 50, .firstKey = "eee", .lastKey = "fff"},
        });
    tabletWriter->writeStripe(150, std::move(streams), std::move(keyStream));
  }

  // Write stripe 2 (Group 2): Total rows: 120
  // Stream 0: 2 chunks (rows: 70, 50)
  // Stream 1: 4 chunks (rows: 25, 35, 30, 30)
  // Key stream: 2 chunks (rows: 60, 60), keys: "fff"->"ggg", "ggg"->"hhh"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {.rowCount = 60, .firstKey = "fff", .lastKey = "ggg"},
            {.rowCount = 60, .firstKey = "ggg", .lastKey = "hhh"},
        });

    tabletWriter->writeStripe(120, std::move(streams), std::move(keyStream));
  }

  tabletWriter->close();
  writeFile.close();

  // Read and verify the tablet
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});

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
  EXPECT_EQ(index->numIndexGroups(), 3);

  // Verify no metadata is pinned from preload (more than one group).
  EXPECT_EQ(tabletHelper.cachedStripeGroupCount(), 0);
  EXPECT_EQ(tabletHelper.cachedIndexGroupCount(), 0);
  EXPECT_EQ(tabletHelper.cachedChunkIndexGroupCount(), 0);

  // Verify index columns
  EXPECT_EQ(index->indexColumns().size(), 2);
  EXPECT_EQ(index->indexColumns()[0], "col1");
  EXPECT_EQ(index->indexColumns()[1], "col2");

  // Verify stripe count
  EXPECT_EQ(index->numStripes(), 3);

  // Verify stripe keys
  // Stripe 0: "aaa"->"ccc"
  // Stripe 1: "ccc"->"fff"
  // Stripe 2: "fff"->"hhh"
  EXPECT_EQ(index->minKey(), "aaa");
  EXPECT_EQ(index->stripeKey(0), "ccc");
  EXPECT_EQ(index->stripeKey(1), "fff");
  EXPECT_EQ(index->stripeKey(2), "hhh");
  EXPECT_EQ(index->maxKey(), "hhh");

  // Verify key lookups across all stripes and group boundaries
  verifyClusterIndexLookups(
      index,
      {
          // Keys before minKey
          {"9", std::nullopt},
          {"aa", std::nullopt},

          // Stripe 0 (Group 0): ["aaa", "ccc"]
          {"aaa", 0},
          {"aab", 0},
          {"b", 0},
          {"bbb", 0},
          {"ccc", 0},

          // Stripe 1 (Group 1): ("ccc", "fff"]
          {"ccd", 1},
          {"d", 1},
          {"ddd", 1},
          {"eee", 1},
          {"fff", 1},

          // Stripe 2 (Group 2): ("fff", "hhh"]
          {"ffg", 2},
          {"g", 2},
          {"ggg", 2},
          {"hhh", 2},

          // Keys after maxKey
          {"hhi", std::nullopt},
          {"i", std::nullopt},
      });

  // Verify stripeIdentifier and index group structure for each group
  // Group 0: stripe 0 only
  {
    auto stripeId = tablet->stripeIdentifier(0);
    ASSERT_NE(stripeId.clusterIndex(), nullptr);

    nimble::index::test::ClusterIndexGroupTestHelper helper(
        stripeId.clusterIndex().get());
    EXPECT_EQ(helper.groupIndex(), 0);
    EXPECT_EQ(helper.firstStripe(), 0);
    EXPECT_EQ(helper.stripeCount(), 1);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeId.chunkIndex().get());
    EXPECT_EQ(chunkHelper.streamCount(), 2);

    // Verify key stream stats for group 0
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{50, 100}));
    EXPECT_EQ(keyStats.chunkKeys, (std::vector<std::string>{"bbb", "ccc"}));

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
    ASSERT_NE(stripeId.clusterIndex(), nullptr);

    nimble::index::test::ClusterIndexGroupTestHelper helper(
        stripeId.clusterIndex().get());
    EXPECT_EQ(helper.groupIndex(), 1);
    EXPECT_EQ(helper.firstStripe(), 1);
    EXPECT_EQ(helper.stripeCount(), 1);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeId.chunkIndex().get());
    EXPECT_EQ(chunkHelper.streamCount(), 2);

    // Verify key stream stats for group 1
    // Stripe 1: 3 chunks
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{50, 100, 150}));
    EXPECT_EQ(
        keyStats.chunkKeys, (std::vector<std::string>{"ddd", "eee", "fff"}));

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
    ASSERT_NE(stripeId.clusterIndex(), nullptr);

    nimble::index::test::ClusterIndexGroupTestHelper helper(
        stripeId.clusterIndex().get());
    EXPECT_EQ(helper.groupIndex(), 2);
    EXPECT_EQ(helper.firstStripe(), 2);
    EXPECT_EQ(helper.stripeCount(), 1);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeId.chunkIndex().get());
    EXPECT_EQ(chunkHelper.streamCount(), 2);

    // Verify key stream stats for group 2
    // Stripe 2: 2 chunks
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{60, 120}));
    EXPECT_EQ(keyStats.chunkKeys, (std::vector<std::string>{"ggg", "hhh"}));

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

  // Verify key lookups through ClusterIndexGroup::lookupChunk return correct
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
          // Keys in chunk 0 ["aaa", "bbb"]: rowOffset = 0, globalRowId = 0 + 0
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

  nimble::ClusterIndexConfig indexConfig{
      .columns = {"col1"},
      .sortOrders = {SortOrder{.ascending = true}},
      .enforceKeyOrder = true,
  };

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          // Set a large threshold to ensure all stripes stay in one group
          .metadataFlushThreshold = 1024 * 1024 * 1024,
          .streamDeduplicationEnabled = false,
          .indexConfig = indexConfig,
      });

  nimble::Buffer buffer{*pool_};

  // Write stripe 0: Total rows: 100
  // Stream 0: EMPTY (no data in first stripe)
  // Stream 1: 2 chunks (rows: 50, 50)
  // Stream 2: 3 chunks (rows: 30, 40, 30)
  // Stream 3: 2 chunks (rows: 60, 40)
  // Key stream: 2 chunks (rows: 50, 50), keys: "aaa"->"bbb", "bbb"->"ccc"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {50, "aaa", "bbb"},
            {50, "bbb", "ccc"},
        });

    tabletWriter->writeStripe(100, std::move(streams), std::move(keyStream));
  }

  // Write stripe 1: Total rows: 150
  // Stream 0: 3 chunks (rows: 40, 60, 50)
  // Stream 1: EMPTY (no data in middle stripe)
  // Stream 2: 2 chunks (rows: 80, 70)
  // Stream 3: 1 chunk (rows: 150)
  // Key stream: 3 chunks (rows: 50, 50, 50), keys: "ccc"->"ddd", "ddd"->"eee",
  // "eee"->"fff"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {50, "ccc", "ddd"},
            {50, "ddd", "eee"},
            {50, "eee", "fff"},
        });

    tabletWriter->writeStripe(150, std::move(streams), std::move(keyStream));
  }

  // Write stripe 2: Total rows: 120
  // Stream 0: 2 chunks (rows: 70, 50)
  // Stream 1: 4 chunks (rows: 25, 35, 30, 30)
  // Stream 2: EMPTY (no data in this stripe)
  // Stream 3: 3 chunks (rows: 40, 40, 40)
  // Key stream: 2 chunks (rows: 60, 60), keys: "fff"->"ggg", "ggg"->"hhh"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {60, "fff", "ggg"},
            {60, "ggg", "hhh"},
        });

    tabletWriter->writeStripe(120, std::move(streams), std::move(keyStream));
  }

  // Write stripe 3: Total rows: 100 - ALL STREAMS HAVE DATA
  // Stream 0: 2 chunks (rows: 50, 50)
  // Stream 1: 2 chunks (rows: 40, 60)
  // Stream 2: 1 chunk (rows: 100)
  // Stream 3: 2 chunks (rows: 30, 70)
  // Key stream: 2 chunks (rows: 50, 50), keys: "hhh"->"iii", "iii"->"jjj"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {.rowCount = 50, .firstKey = "hhh", .lastKey = "iii"},
            {.rowCount = 50, .firstKey = "iii", .lastKey = "jjj"},
        });

    tabletWriter->writeStripe(100, std::move(streams), std::move(keyStream));
  }

  tabletWriter->close();
  writeFile.close();

  // Read and verify the tablet
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});

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
  EXPECT_EQ(index->numIndexGroups(), 1);

  // Load index group and verify stream stats
  auto stripeId = tablet->stripeIdentifier(0);
  ASSERT_NE(stripeId.clusterIndex(), nullptr);

  nimble::index::test::ClusterIndexGroupTestHelper helper(
      stripeId.clusterIndex().get());
  EXPECT_EQ(helper.groupIndex(), 0);
  EXPECT_EQ(helper.firstStripe(), 0);
  EXPECT_EQ(helper.stripeCount(), 4);

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

  // Verify key stream stats
  {
    const auto keyStats = helper.keyStreamStats();
    // Accumulated chunk counts per stripe: 2, 2+3=5, 5+2=7, 7+2=9
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{2, 5, 7, 9}));
    EXPECT_EQ(
        keyStats.chunkRows,
        (std::vector<uint32_t>{50, 100, 50, 100, 150, 60, 120, 50, 100}));
    EXPECT_EQ(
        keyStats.chunkKeys,
        (std::vector<std::string>{
            "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii", "jjj"}));
  }

  // Verify key lookups
  verifyClusterIndexLookups(
      index,
      {
          {"aaa", 0},
          {"bbb", 0},
          {"ccc", 0},
          {"ddd", 1},
          {"eee", 1},
          {"fff", 1},
          {"ggg", 2},
          {"hhh", 2},
          {"iii", 3},
          {"jjj", 3},
          {"kkk", std::nullopt},
      });

  // Verify key lookups through ClusterIndexGroup::lookupChunk return correct
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

  nimble::ClusterIndexConfig indexConfig{
      .columns = {"col1"},
      .sortOrders = {SortOrder{.ascending = true}},
      .enforceKeyOrder = true,
  };

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          // Set threshold to 0 to force flush after every stripe
          .metadataFlushThreshold = 0,
          .streamDeduplicationEnabled = false,
          // Disable chunk index skipping so all groups get a chunk index,
          // even when empty streams reduce the average chunks per stream.
          .chunkIndexMinAvgChunks = 0,
          .indexConfig = indexConfig,
      });

  nimble::Buffer buffer{*pool_};

  // Write stripe 0 (Group 0): Total rows: 100
  // Stream 0: EMPTY (no data in first stripe)
  // Stream 1: 2 chunks (rows: 50, 50)
  // Stream 2: 3 chunks (rows: 30, 40, 30)
  // Stream 3: 2 chunks (rows: 60, 40)
  // Key stream: 2 chunks (rows: 50, 50), keys: "aaa"->"bbb", "bbb"->"ccc"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {50, "aaa", "bbb"},
            {50, "bbb", "ccc"},
        });

    tabletWriter->writeStripe(100, std::move(streams), std::move(keyStream));
  }

  // Write stripe 1 (Group 1): Total rows: 150
  // Stream 0: 3 chunks (rows: 40, 60, 50)
  // Stream 1: EMPTY (no data in middle stripe)
  // Stream 2: 2 chunks (rows: 80, 70)
  // Stream 3: 1 chunk (rows: 150)
  // Key stream: 3 chunks (rows: 50, 50, 50), keys: "ccc"->"ddd", "ddd"->"eee",
  // "eee"->"fff"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {50, "ccc", "ddd"},
            {50, "ddd", "eee"},
            {50, "eee", "fff"},
        });

    tabletWriter->writeStripe(150, std::move(streams), std::move(keyStream));
  }

  // Write stripe 2 (Group 2): Total rows: 120
  // Stream 0: 2 chunks (rows: 70, 50)
  // Stream 1: 4 chunks (rows: 25, 35, 30, 30)
  // Stream 2: EMPTY (no data in this stripe)
  // Stream 3: 3 chunks (rows: 40, 40, 40)
  // Key stream: 2 chunks (rows: 60, 60), keys: "fff"->"ggg", "ggg"->"hhh"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {60, "fff", "ggg"},
            {60, "ggg", "hhh"},
        });

    tabletWriter->writeStripe(120, std::move(streams), std::move(keyStream));
  }

  // Write stripe 3 (Group 3): Total rows: 100 - ALL STREAMS HAVE DATA
  // Stream 0: 2 chunks (rows: 50, 50)
  // Stream 1: 2 chunks (rows: 40, 60)
  // Stream 2: 1 chunk (rows: 100)
  // Stream 3: 2 chunks (rows: 30, 70)
  // Key stream: 2 chunks (rows: 50, 50), keys: "hhh"->"iii", "iii"->"jjj"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {50, "hhh", "iii"},
            {50, "iii", "jjj"},
        });

    tabletWriter->writeStripe(100, std::move(streams), std::move(keyStream));
  }

  tabletWriter->close();
  writeFile.close();

  // Read and verify the tablet
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});

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
  EXPECT_EQ(index->numIndexGroups(), 4);

  // Verify key lookups across all stripes and group boundaries
  verifyClusterIndexLookups(
      index,
      {
          {"aaa", 0},
          {"bbb", 0},
          {"ccc", 0},
          {"ddd", 1},
          {"eee", 1},
          {"fff", 1},
          {"ggg", 2},
          {"hhh", 2},
          {"iii", 3},
          {"jjj", 3},
          {"kkk", std::nullopt},
      });

  // Group 0: stripe 0 only
  // Stream 0: EMPTY
  // Stream 1: 2 chunks (rows: 50, 50)
  // Stream 2: 3 chunks (rows: 30, 40, 30)
  // Stream 3: 2 chunks (rows: 60, 40)
  {
    auto stripeId = tablet->stripeIdentifier(0);
    ASSERT_NE(stripeId.clusterIndex(), nullptr);

    nimble::index::test::ClusterIndexGroupTestHelper helper(
        stripeId.clusterIndex().get());
    EXPECT_EQ(helper.groupIndex(), 0);
    EXPECT_EQ(helper.firstStripe(), 0);
    EXPECT_EQ(helper.stripeCount(), 1);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeId.chunkIndex().get());
    // All 4 streams indexed (chunkIndexMinAvgChunks = 0).
    EXPECT_EQ(chunkHelper.streamCount(), 4);

    // Verify key stream stats for group 0
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{50, 100}));
    EXPECT_EQ(keyStats.chunkKeys, (std::vector<std::string>{"bbb", "ccc"}));

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
    ASSERT_NE(stripeId.clusterIndex(), nullptr);

    nimble::index::test::ClusterIndexGroupTestHelper helper(
        stripeId.clusterIndex().get());
    EXPECT_EQ(helper.groupIndex(), 1);
    EXPECT_EQ(helper.firstStripe(), 1);
    EXPECT_EQ(helper.stripeCount(), 1);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeId.chunkIndex().get());
    // All 4 streams indexed (chunkIndexMinAvgChunks = 0).
    EXPECT_EQ(chunkHelper.streamCount(), 4);

    // Verify key stream stats for group 1
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{3}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{50, 100, 150}));
    EXPECT_EQ(
        keyStats.chunkKeys, (std::vector<std::string>{"ddd", "eee", "fff"}));

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
    ASSERT_NE(stripeId.clusterIndex(), nullptr);

    nimble::index::test::ClusterIndexGroupTestHelper helper(
        stripeId.clusterIndex().get());
    EXPECT_EQ(helper.groupIndex(), 2);
    EXPECT_EQ(helper.firstStripe(), 2);
    EXPECT_EQ(helper.stripeCount(), 1);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeId.chunkIndex().get());
    // All 4 streams indexed (chunkIndexMinAvgChunks = 0).
    EXPECT_EQ(chunkHelper.streamCount(), 4);

    // Verify key stream stats for group 2
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{60, 120}));
    EXPECT_EQ(keyStats.chunkKeys, (std::vector<std::string>{"ggg", "hhh"}));

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
    ASSERT_NE(stripeId.clusterIndex(), nullptr);

    nimble::index::test::ClusterIndexGroupTestHelper helper(
        stripeId.clusterIndex().get());
    EXPECT_EQ(helper.groupIndex(), 3);
    EXPECT_EQ(helper.firstStripe(), 3);
    EXPECT_EQ(helper.stripeCount(), 1);

    nimble::index::test::ChunkIndexTestHelper chunkHelper(
        stripeId.chunkIndex().get());
    // All 4 streams indexed (chunkIndexMinAvgChunks = 0).
    EXPECT_EQ(chunkHelper.streamCount(), 4);

    // Verify key stream stats for group 3
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{50, 100}));
    EXPECT_EQ(keyStats.chunkKeys, (std::vector<std::string>{"iii", "jjj"}));

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

  // Verify key lookups through ClusterIndexGroup::lookupChunk return correct
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

  nimble::ClusterIndexConfig indexConfig{
      .columns = {"col1"},
      .sortOrders = {SortOrder{.ascending = true}},
      .enforceKeyOrder = true,
  };

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          .metadataFlushThreshold = 1024 * 1024 * 1024,
          .streamDeduplicationEnabled = true,
          .indexConfig = indexConfig,
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
  // Key stream: 2 chunks (rows: 50, 50), keys: "aaa"->"bbb", "bbb"->"ccc"
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

    auto keyStream = createKeyStream(
        buffer,
        {
            {50, "aaa", "bbb"},
            {50, "bbb", "ccc"},
        });

    tabletWriter->writeStripe(100, std::move(streams), std::move(keyStream));
  }

  tabletWriter->close();
  writeFile.close();

  // Read and verify the tablet
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});

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
  EXPECT_EQ(index->numIndexGroups(), 1);

  // Verify key lookups
  verifyClusterIndexLookups(
      index,
      {
          {"aaa", 0},
          {"bbb", 0},
          {"ccc", 0},
          {"ddd", std::nullopt},
      });

  // Load index group and verify stream stats
  auto stripeId = tablet->stripeIdentifier(0);
  ASSERT_NE(stripeId.clusterIndex(), nullptr);

  nimble::index::test::ClusterIndexGroupTestHelper helper(
      stripeId.clusterIndex().get());
  EXPECT_EQ(helper.groupIndex(), 0);
  EXPECT_EQ(helper.firstStripe(), 0);
  EXPECT_EQ(helper.stripeCount(), 1);

  nimble::index::test::ChunkIndexTestHelper chunkHelper(
      stripeId.chunkIndex().get());
  EXPECT_EQ(chunkHelper.streamCount(), 4);

  // Verify key stream stats
  {
    const auto keyStats = helper.keyStreamStats();
    EXPECT_EQ(keyStats.chunkCounts, (std::vector<uint32_t>{2}));
    EXPECT_EQ(keyStats.chunkRows, (std::vector<uint32_t>{50, 100}));
    EXPECT_EQ(keyStats.chunkKeys, (std::vector<std::string>{"bbb", "ccc"}));
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

    nimble::ClusterIndexConfig indexConfig{
        .columns = {"col1"},
        .sortOrders = {SortOrder{.ascending = true}},
        .enforceKeyOrder = enforceKeyOrder,
        .noDuplicateKey = enforceKeyOrder,
    };

    auto tabletWriter = nimble::TabletWriter::create(
        &writeFile,
        *pool_,
        {
            .indexConfig = indexConfig,
        });

    nimble::Buffer buffer{*pool_};

    // Write first stripe with key ending at "ddd"
    {
      std::vector<nimble::Stream> streams;
      auto pos = buffer.reserve(10);
      std::memset(pos, 'A', 10);
      streams.push_back(
          {.offset = 0, .chunks = {{.rowCount = 100, .content = {{pos, 10}}}}});

      auto keyStream = createKeyStream(
          buffer, {{.rowCount = 100, .firstKey = "ccc", .lastKey = "ddd"}});
      tabletWriter->writeStripe(100, std::move(streams), std::move(keyStream));
    }

    // Write second stripe with key ending before "ddd" (out of order)
    {
      std::vector<nimble::Stream> streams;
      auto pos = buffer.reserve(10);
      std::memset(pos, 'B', 10);
      streams.push_back(
          {.offset = 0, .chunks = {{.rowCount = 100, .content = {{pos, 10}}}}});

      // Key "bbb" < "ddd", out of order
      auto keyStream = createKeyStream(
          buffer, {{.rowCount = 100, .firstKey = "aaa", .lastKey = "bbb"}});

      if (enforceKeyOrder) {
        // Should throw when enforceKeyOrder is true
        NIMBLE_ASSERT_USER_THROW(
            tabletWriter->writeStripe(
                100, std::move(streams), std::move(keyStream)),
            "Stripe keys must be in strictly ascending order");
      } else {
        // Should NOT throw when enforceKeyOrder is false
        EXPECT_NO_THROW(tabletWriter->writeStripe(
            100, std::move(streams), std::move(keyStream)));

        tabletWriter->close();
        writeFile.close();

        // Verify file is readable
        auto readFile =
            std::make_shared<nimble::testing::InMemoryTrackableReadFile>(
                file, false);
        auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});
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

  tabletWriter->writeStripe(100, std::move(streams), std::nullopt);
  tabletWriter->close();
  writeFile.close();

  // Verify no index section and no chunk index section
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});
  EXPECT_EQ(tablet->stripeCount(), 1);
  EXPECT_FALSE(
      tablet->hasOptionalSection(std::string(nimble::kClusterIndexSection)));
  EXPECT_FALSE(
      tablet->hasOptionalSection(std::string(nimble::kChunkIndexSection)));
}

// Tests all four orthogonal config combinations of enableChunkIndex ×
// indexConfig to verify:
// 1. Neither: no optional sections
// 2. Chunk index only: chunk_index section exists, lookupChunk works
// 3. Cluster index only (indexConfig): both sections exist (chunk index is
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

    std::optional<nimble::ClusterIndexConfig> indexConfig;
    if (config.enableIndexConfig) {
      indexConfig = nimble::ClusterIndexConfig{
          .columns = {"col1"},
          .sortOrders = {SortOrder{.ascending = true}},
          .enforceKeyOrder = true,
      };
    }

    auto tabletWriter = nimble::TabletWriter::create(
        &writeFile,
        *pool_,
        {
            .metadataFlushThreshold = 1024 * 1024 * 1024,
            .enableChunkIndex = config.enableChunkIndex,
            .indexConfig = indexConfig,
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

      std::optional<nimble::KeyStream> keyStream;
      if (config.enableIndexConfig) {
        std::string firstKey = stripe == 0 ? "aaa" : "ccc";
        std::string lastKey = stripe == 0 ? "bbb" : "ddd";
        keyStream = createKeyStream(
            buffer,
            {
                {.rowCount = 50, .firstKey = firstKey, .lastKey = lastKey},
            });
      }

      tabletWriter->writeStripe(50, std::move(streams), std::move(keyStream));
    }

    tabletWriter->close();
    writeFile.close();

    // Read back
    auto readFile =
        std::make_shared<nimble::testing::InMemoryTrackableReadFile>(
            file, false);
    auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});
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
      EXPECT_EQ(idx->numStripes(), 2);
      EXPECT_EQ(idx->minKey(), "aaa");
      EXPECT_EQ(idx->maxKey(), "ddd");

      // Key lookup
      EXPECT_EQ(idx->lookup("aaa")->stripeIndex, 0);
      EXPECT_EQ(idx->lookup("bbb")->stripeIndex, 0);
      EXPECT_EQ(idx->lookup("ccc")->stripeIndex, 1);
      EXPECT_EQ(idx->lookup("ddd")->stripeIndex, 1);
      EXPECT_FALSE(idx->lookup("eee").has_value());
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

  const nimble::ClusterIndexConfig indexConfig{
      .columns = {"col1", "col2", "col3"},
      .sortOrders =
          {SortOrder{.ascending = true},
           SortOrder{.ascending = false},
           SortOrder{.ascending = true}},
      .enforceKeyOrder = true,
      .noDuplicateKey = false,
  };

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          .indexConfig = indexConfig,
      });

  // Close without writing any stripes.
  tabletWriter->close();
  writeFile.close();

  // Verify the file can be read.
  auto readFile =
      std::make_shared<nimble::testing::InMemoryTrackableReadFile>(file, false);
  auto tablet = nimble::TabletReader::create(readFile, pool_.get(), {});

  // Verify no stripes.
  EXPECT_EQ(tablet->stripeCount(), 0);

  // Verify index section exists (with config only).
  EXPECT_TRUE(
      tablet->hasOptionalSection(std::string(nimble::kClusterIndexSection)));

  // Verify root index has config but no stripe data.
  auto* clusterIndex = tablet->clusterIndex();
  ASSERT_NE(clusterIndex, nullptr);

  // Verify index columns match config.
  const auto& indexColumns = clusterIndex->indexColumns();
  ASSERT_EQ(indexColumns.size(), 3);
  EXPECT_EQ(indexColumns[0], "col1");
  EXPECT_EQ(indexColumns[1], "col2");
  EXPECT_EQ(indexColumns[2], "col3");

  // Verify sort orders match config.
  const auto& sortOrders = clusterIndex->sortOrders();
  ASSERT_EQ(sortOrders.size(), 3);
  EXPECT_TRUE(sortOrders[0].ascending);
  EXPECT_FALSE(sortOrders[1].ascending);
  EXPECT_TRUE(sortOrders[2].ascending);

  // Verify no stripes in index.
  EXPECT_EQ(clusterIndex->numStripes(), 0);
  EXPECT_TRUE(clusterIndex->empty());

  // Verify no index groups.
  EXPECT_EQ(clusterIndex->numIndexGroups(), 0);

  // Verify lookup returns no match for any key.
  EXPECT_FALSE(clusterIndex->lookup("any_key").has_value());
  EXPECT_FALSE(clusterIndex->lookup("").has_value());
  EXPECT_FALSE(clusterIndex->lookup("zzz").has_value());
}

TEST_P(TabletWithIndexTest, fileLayoutWithIndex) {
  // Test FileLayout::create() with non-empty file that has index enabled.
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  nimble::Buffer buffer(*pool_);

  nimble::ClusterIndexConfig indexConfig{
      .columns = {"col1"},
      .sortOrders = {SortOrder{.ascending = true}},
  };

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          .metadataFlushThreshold = 1024 * 1024 * 1024,
          .streamDeduplicationEnabled = false,
          .indexConfig = indexConfig,
      });

  // Write two stripes with index
  for (int stripe = 0; stripe < 2; ++stripe) {
    auto streams = createStreams(
        buffer, {{.offset = 0, .chunks = {{.rowCount = 100, .size = 50}}}});
    auto keyStream = createKeyStream(
        buffer,
        {{.rowCount = 100,
          .firstKey = std::to_string(stripe * 100),
          .lastKey = std::to_string(stripe * 100 + 99)}});
    tabletWriter->writeStripe(100, std::move(streams), std::move(keyStream));
  }
  tabletWriter->close();
  writeFile.close();

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);
  auto layout = nimble::FileLayout::create(readFile, pool_.get());

  EXPECT_EQ(layout.fileSize, file.size());
  EXPECT_EQ(layout.stripesInfo.size(), 2);
  EXPECT_EQ(layout.stripeGroups.size(), 1);
  // With index and stripes, should have index groups
  EXPECT_EQ(layout.indexGroups.size(), 1);
  EXPECT_GT(layout.indexGroups[0].size(), 0);
  // Per-stripe info
  EXPECT_EQ(layout.stripesInfo.size(), 2);
  for (size_t i = 0; i < layout.stripesInfo.size(); ++i) {
    EXPECT_EQ(layout.stripesInfo[i].stripeGroupIndex, 0);
    EXPECT_GT(layout.stripesInfo[i].size, 0);
  }
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

  nimble::ClusterIndexConfig indexConfig{
      .columns = {"col1"},
      .sortOrders = {SortOrder{.ascending = true}},
      .enforceKeyOrder = true,
  };

  auto tabletWriter = nimble::TabletWriter::create(
      &writeFile,
      *pool_,
      {
          .metadataFlushThreshold = 1024 * 1024 * 1024,
          .streamDeduplicationEnabled = false,
          .indexConfig = indexConfig,
      });

  constexpr int kNumStripes = 3;
  std::vector<std::string> firstKeys = {"aaa", "ccc", "eee"};
  std::vector<std::string> lastKeys = {"bbb", "ddd", "fff"};
  for (int i = 0; i < kNumStripes; ++i) {
    auto streams = createStreams(
        buffer, {{.offset = 0, .chunks = {{.rowCount = 100, .size = 50}}}});
    auto keyStream = createKeyStream(
        buffer,
        {{.rowCount = 100, .firstKey = firstKeys[i], .lastKey = lastKeys[i]}});
    tabletWriter->writeStripe(100, std::move(streams), std::move(keyStream));
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
    EXPECT_EQ(ioStatistics_->ramHit().count(), 0);
    EXPECT_EQ(ioStatistics_->ssdRead().count(), 0);
    EXPECT_EQ(ioStatistics_->read().count(), 0);
    EXPECT_EQ(ioStatistics_->prefetch().count(), 0);

    // Verify stripe groups and index groups are accessible.
    for (uint32_t i = 0; i < kNumStripes; ++i) {
      auto stripeId = coldReader->stripeIdentifier(i);
      EXPECT_NE(stripeId.stripeGroup(), nullptr);
      EXPECT_NE(stripeId.clusterIndex(), nullptr);
    }

    // Verify index lookup works.
    auto location = coldReader->clusterIndex()->lookup("bbb");
    ASSERT_TRUE(location.has_value());
    EXPECT_EQ(location->stripeIndex, 0);
  }

  coldCacheStats = cache_->refreshStats();
  EXPECT_GT(coldCacheStats.numEntries, 0);
  EXPECT_GT(coldCacheStats.numNew, 0);
  EXPECT_EQ(coldCacheStats.numEvict, 0);

  // Reset IO stats for the warm path.
  ioStatistics_ = std::make_shared<velox::dwio::common::IoStatistics>();

  // Warm path: second reader should initialize from cache with zero IO.
  {
    auto warmReader = createTabletReader(readFile, options);
    EXPECT_EQ(warmReader->stripeCount(), kNumStripes);
    EXPECT_EQ(warmReader->tabletRowCount(), kNumStripes * 100);
    EXPECT_TRUE(warmReader->hasClusterIndex());
    EXPECT_NE(warmReader->clusterIndex(), nullptr);

    // Warm path should serve all data from RAM cache.
    EXPECT_GT(ioStatistics_->ramHit().count(), 0);
    EXPECT_EQ(ioStatistics_->ssdRead().count(), 0);
    EXPECT_EQ(ioStatistics_->read().count(), 0);
    EXPECT_EQ(ioStatistics_->prefetch().count(), 0);

    // Verify stripe groups and index groups are accessible from cache.
    for (uint32_t i = 0; i < kNumStripes; ++i) {
      auto stripeId = warmReader->stripeIdentifier(i);
      EXPECT_NE(stripeId.stripeGroup(), nullptr);
      EXPECT_NE(stripeId.clusterIndex(), nullptr);
    }

    // Verify index lookup still works from cache.
    auto location = warmReader->clusterIndex()->lookup("ddd");
    ASSERT_TRUE(location.has_value());
    EXPECT_EQ(location->stripeIndex, 1);
  }

  // Warm reader should have additional cache hits, no new entries or evictions.
  auto warmCacheStats = cache_->refreshStats();
  EXPECT_EQ(warmCacheStats.numEntries, coldCacheStats.numEntries);
  EXPECT_GT(warmCacheStats.numHit, coldCacheStats.numHit);
  EXPECT_EQ(warmCacheStats.numNew, coldCacheStats.numNew);
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

  {
    SCOPED_TRACE("defaults");
    auto opts = nimble::TabletReader::configureOptions(readerOptions);
    EXPECT_TRUE(opts.loadClusterIndex);
    EXPECT_TRUE(opts.loadChunkIndex);
    EXPECT_TRUE(
        containsSection(opts.preloadOptionalSections, nimble::kSchemaSection));
    EXPECT_TRUE(containsSection(
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

TEST_P(TabletTest, configureOptionsBufferedInput) {
  // Verify configureOptions only sets bufferedInput when
  // fileMetadataCacheEnabled is true.
  velox::dwio::common::ReaderOptions readerOptions(pool_.get());

  // Create a dummy BufferedInput to pass as the second parameter.
  std::string emptyFile;
  auto readFile = std::make_shared<velox::InMemoryReadFile>(emptyFile);
  velox::dwio::common::BufferedInput bufferedInput(readFile, *pool_);

  {
    SCOPED_TRACE("fileMetadataCacheEnabled=false (default)");
    auto opts =
        nimble::TabletReader::configureOptions(readerOptions, &bufferedInput);
    EXPECT_EQ(opts.bufferedInput, nullptr);
  }

  {
    SCOPED_TRACE("fileMetadataCacheEnabled=true");
    readerOptions.setFileMetadataCacheEnabled(true);
    auto opts =
        nimble::TabletReader::configureOptions(readerOptions, &bufferedInput);
    EXPECT_EQ(opts.bufferedInput, &bufferedInput);
  }
}

TEST_P(TabletTest, bufferedInputMetadataReads) {
  // Test that BufferedInput is used for metadata reads when provided.
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  nimble::Buffer buffer(*pool_);

  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});

  // Write multiple stripes to ensure we have stripe group metadata to read.
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

  nimble::test::TabletReaderTestHelper tabletHelper(tablet.get());
  EXPECT_EQ(tabletHelper.hasCache(), expectHasCache());
  EXPECT_EQ(tablet->stripeCount(), 3);

  // Verify we can read stripe data.
  auto stripeId = tablet->stripeIdentifier(0);
  EXPECT_EQ(stripeId.stripeId(), 0);
  EXPECT_NE(stripeId.stripeGroup(), nullptr);
}

TEST_P(TabletTest, cacheWarmPath) {
  // Test that a second TabletReader on the same file initializes from
  // AsyncDataCache with zero file IO when file-metadata-cache is enabled.
  // Only meaningful for CachedBufferedInput mode.
  if (GetParam() != BufferedInputMode::kCachedBufferedInput) {
    GTEST_SKIP() << "Cache warm path only applies to CachedBufferedInput";
  }

  // Write a file with multiple stripes.
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  nimble::Buffer buffer(*pool_);

  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});
  constexpr int kNumStripes = 3;
  for (int i = 0; i < kNumStripes; ++i) {
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

  auto readFile = std::make_shared<velox::InMemoryReadFile>(file);

  // Initialize cache before the cold reader so we can verify it starts empty.
  // Normally lazy-initialized in createBufferedInput.
  allocator_ = std::make_shared<velox::memory::MallocAllocator>(
      velox::memory::MemoryAllocator::Options{
          .capacity = 1UL << 30, .reservationByteLimit = 0});
  cache_ = velox::cache::AsyncDataCache::create(allocator_.get());

  // Cache should be empty before the cold reader.
  auto coldCacheStats = cache_->refreshStats();
  EXPECT_EQ(coldCacheStats.numEntries, 0);
  EXPECT_EQ(coldCacheStats.numHit, 0);
  EXPECT_EQ(coldCacheStats.numNew, 0);

  // Cold path: first reader populates the cache.
  {
    auto coldReader = createTabletReader(readFile);
    EXPECT_EQ(coldReader->stripeCount(), kNumStripes);
    EXPECT_EQ(coldReader->tabletRowCount(), kNumStripes * 100);

    // Cold init uses direct file_->preadv() which bypasses
    // CachedBufferedInput, so no reads are tracked through IoStatistics.
    EXPECT_EQ(ioStatistics_->ramHit().count(), 0);
    EXPECT_EQ(ioStatistics_->ssdRead().count(), 0);
    EXPECT_EQ(ioStatistics_->read().count(), 0);
    EXPECT_EQ(ioStatistics_->prefetch().count(), 0);

    auto stripeId = coldReader->stripeIdentifier(0);
    EXPECT_EQ(stripeId.stripeId(), 0);
    EXPECT_NE(stripeId.stripeGroup(), nullptr);
  }

  // Cold reader should have populated the cache with metadata entries.
  // numHit may be > 0 from self-hits within the cold reader (coalesced IO can
  // populate a cache entry that is then hit by a subsequent read).
  coldCacheStats = cache_->refreshStats();
  EXPECT_GT(coldCacheStats.numEntries, 0);
  EXPECT_GT(coldCacheStats.numNew, 0);
  EXPECT_EQ(coldCacheStats.numEvict, 0);

  // Reset IO stats for the warm path.
  ioStatistics_ = std::make_shared<velox::dwio::common::IoStatistics>();

  // Warm path: second reader should initialize from cache with zero IO.
  {
    auto warmReader = createTabletReader(readFile);
    EXPECT_EQ(warmReader->stripeCount(), kNumStripes);
    EXPECT_EQ(warmReader->tabletRowCount(), kNumStripes * 100);
    for (uint32_t i = 0; i < kNumStripes; ++i) {
      EXPECT_EQ(warmReader->stripeRowCount(i), 100);
    }

    // Warm path should serve all data from RAM cache with zero storage reads.
    EXPECT_GT(ioStatistics_->ramHit().count(), 0);
    EXPECT_EQ(ioStatistics_->ssdRead().count(), 0);
    EXPECT_EQ(ioStatistics_->read().count(), 0);
    EXPECT_EQ(ioStatistics_->prefetch().count(), 0);

    auto stripeId = warmReader->stripeIdentifier(0);
    EXPECT_EQ(stripeId.stripeId(), 0);
    EXPECT_NE(stripeId.stripeGroup(), nullptr);
  }

  // Warm reader should have additional cache hits beyond what the cold reader
  // generated, with no new entries or evictions.
  auto warmCacheStats = cache_->refreshStats();
  EXPECT_EQ(warmCacheStats.numEntries, coldCacheStats.numEntries);
  EXPECT_GT(warmCacheStats.numHit, coldCacheStats.numHit);
  EXPECT_EQ(warmCacheStats.numNew, coldCacheStats.numNew);
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
TEST(TabletStressTest, concurrentReadersWithCacheEviction) {
  velox::memory::MemoryManager::testingSetInstance({});
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
      // Each thread cycles through a fixed BufferedInput mode.
      auto mode = static_cast<BufferedInputMode>(threadId % 4);
      std::unique_ptr<velox::dwio::common::BufferedInput> bi;
      auto ioStats = std::make_shared<velox::dwio::common::IoStatistics>();
      std::shared_ptr<velox::cache::ScanTracker> tracker;
      velox::io::ReaderOptions readerOptions(threadPool.get());
      nimble::TabletReader::Options options;

      switch (mode) {
        case BufferedInputMode::kNone:
          break;

        case BufferedInputMode::kBufferedInput:
          bi = std::make_unique<velox::dwio::common::BufferedInput>(
              readFile, *threadPool);
          break;

        case BufferedInputMode::kDirectBufferedInput: {
          tracker = std::make_shared<velox::cache::ScanTracker>(
              "tracker", nullptr, 256 << 10);
          velox::StringIdLease fileId(ids, fmt::format("stress_{}", threadId));
          velox::StringIdLease groupId(ids, "stressGroup");
          bi = std::make_unique<velox::dwio::common::DirectBufferedInput>(
              readFile,
              velox::dwio::common::MetricsLog::voidLog(),
              std::move(fileId),
              tracker,
              std::move(groupId),
              ioStats,
              nullptr,
              executor.get(),
              readerOptions);
          break;
        }

        case BufferedInputMode::kCachedBufferedInput: {
          tracker = std::make_shared<velox::cache::ScanTracker>(
              "tracker", nullptr, 256 << 10);
          velox::StringIdLease fileId(ids, "stressFile");
          velox::StringIdLease groupId(ids, "stressGroup");
          bi = std::make_unique<velox::dwio::common::CachedBufferedInput>(
              readFile,
              velox::dwio::common::MetricsLog::voidLog(),
              std::move(fileId),
              cache.get(),
              tracker,
              std::move(groupId),
              ioStats,
              nullptr,
              executor.get(),
              readerOptions);
          break;
        }
      }

      options.bufferedInput = bi.get();
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

} // namespace
