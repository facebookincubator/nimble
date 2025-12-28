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
#include <gtest/gtest.h>
#include <algorithm>
#include <iterator>
#include <limits>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Checksum.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/tablet/TabletWriter.h"
#include "folly/FileUtil.h"
#include "folly/Random.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/ExecutorBarrier.h"

using namespace facebook;

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

class TabletTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {}

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

      auto stripeGroupCount = tabletWriter->testingStripeGroupCount();

      folly::writeFile(file, "/tmp/test.nimble");

      for (auto useChainedBuffers : {false, true}) {
        nimble::testing::InMemoryTrackableReadFile readFile(
            file, useChainedBuffers);
        auto tablet = nimble::TabletReader::create(&readFile, *pool_);
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

          readFile.resetChunks();
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
          auto chunks = readFile.chunks();
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
        VLOG(1) << "FlushThreshold: " << flushThreshold
                << ", CompressionThreshold: " << compressionThreshold;
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
      nimble::testing::InMemoryTrackableReadFile readFile(
          file, useChainedBuffers);
      auto tablet = nimble::TabletReader::create(&readFile, *this->pool_);
      auto storedChecksum = tablet->checksum();
      EXPECT_EQ(
          storedChecksum,
          nimble::TabletReader::calculateChecksum(
              *pool_,
              &readFile,
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

TEST_F(TabletTest, emptyWrite) {
  // Creating an Nimble file without writing any stripes
  test(/* stripes */ {});
}

TEST_F(TabletTest, writeDifferentStreamsPerStripe) {
  // Write different subset of streams in each stripe
  test(
      /* stripes */
      {
          {.rowCount = 20, .streamOffsets = {3, 1}},
          {.rowCount = 30, .streamOffsets = {2}},
          {.rowCount = 20, .streamOffsets = {4}},
      });
}

TEST_F(TabletTest, checksumValidation) {
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

TEST_F(TabletTest, optionalSections) {
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
    nimble::testing::InMemoryTrackableReadFile readFile(
        file, useChainedBuffers);
    auto tablet = nimble::TabletReader::create(&readFile, *pool_);

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

TEST_F(TabletTest, optionalSectionsEmpty) {
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});

  tabletWriter->close();

  for (auto useChainedBuffers : {false, true}) {
    nimble::testing::InMemoryTrackableReadFile readFile(
        file, useChainedBuffers);
    auto tablet = nimble::TabletReader::create(&readFile, *pool_);

    ASSERT_TRUE(tablet->optionalSections().empty());

    auto section = tablet->loadOptionalSection("section1");
    ASSERT_FALSE(section.has_value());
  }
}

TEST_F(TabletTest, hasOptionalSection) {
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});

  // Write some optional sections
  tabletWriter->writeOptionalSection("section1", "data1");
  tabletWriter->writeOptionalSection("section2", "data2");
  tabletWriter->writeOptionalSection("section3", "data3");

  tabletWriter->close();

  nimble::testing::InMemoryTrackableReadFile readFile(file, true);
  auto tablet = nimble::TabletReader::create(&readFile, *pool_);

  // Test that hasOptionalSection returns true for existing sections
  EXPECT_TRUE(tablet->hasOptionalSection("section1"));
  EXPECT_TRUE(tablet->hasOptionalSection("section2"));
  EXPECT_TRUE(tablet->hasOptionalSection("section3"));

  // Test that hasOptionalSection returns false for non-existing sections
  EXPECT_FALSE(tablet->hasOptionalSection("section4"));
  EXPECT_FALSE(tablet->hasOptionalSection("nonexistent"));
  EXPECT_FALSE(tablet->hasOptionalSection(""));
}

TEST_F(TabletTest, hasOptionalSectionEmpty) {
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  auto tabletWriter = nimble::TabletWriter::create(&writeFile, *pool_, {});

  tabletWriter->close();

  nimble::testing::InMemoryTrackableReadFile readFile(file, true);
  auto tablet = nimble::TabletReader::create(&readFile, *pool_);

  // Test that hasOptionalSection returns false when there are no sections
  EXPECT_FALSE(tablet->hasOptionalSection("section1"));
  EXPECT_FALSE(tablet->hasOptionalSection(""));
  EXPECT_FALSE(tablet->hasOptionalSection("any_name"));
}

TEST_F(TabletTest, optionalSectionsPreload) {
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
        nimble::testing::InMemoryTrackableReadFile readFile(
            file, useChainedBuffers);
        auto tablet = nimble::TabletReader::create(&readFile, *pool_, preload);

        // Expecting only the initial footer read.
        ASSERT_EQ(expectedInitialReads, readFile.chunks().size());

        for (const auto& e : expected) {
          auto expectedSection = std::get<0>(e);
          auto expectedReads = std::get<1>(e);
          auto expectedContent = std::get<2>(e);
          auto section = tablet->loadOptionalSection(expectedSection);
          ASSERT_TRUE(section.has_value());
          ASSERT_EQ(expectedContent, section->content());
          ASSERT_EQ(expectedReads, readFile.chunks().size());
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

enum class ActionEnum { kCreated, kDestroyed };

using Action = std::pair<ActionEnum, int>;
using Actions = std::vector<Action>;

class Guard {
 public:
  Guard(int id, Actions& actions) : id_{id}, actions_{actions} {
    actions_.emplace_back(ActionEnum::kCreated, id_);
  }

  ~Guard() {
    actions_.emplace_back(ActionEnum::kDestroyed, id_);
  }

  Guard(const Guard&) = delete;
  Guard(Guard&&) = delete;
  Guard& operator=(const Guard&) = delete;
  Guard& operator=(Guard&&) = delete;

  int id() const {
    return id_;
  }

 private:
  int id_;
  Actions& actions_;
};

TEST_F(TabletTest, referenceCountedCache) {
  Actions actions;
  facebook::nimble::ReferenceCountedCache<int, Guard> cache{
      [&](int id) { return std::make_shared<Guard>(id, actions); }};

  auto e1 = cache.get(0);
  EXPECT_EQ(e1->id(), 0);
  EXPECT_EQ(actions, Actions({{ActionEnum::kCreated, 0}}));

  auto e2 = cache.get(0);
  EXPECT_EQ(e2->id(), 0);
  EXPECT_EQ(actions, Actions({{ActionEnum::kCreated, 0}}));
  e2.reset();
  EXPECT_EQ(actions, Actions({{ActionEnum::kCreated, 0}}));

  auto e3 = cache.get(1);
  EXPECT_EQ(e3->id(), 1);
  EXPECT_EQ(
      actions, Actions({{ActionEnum::kCreated, 0}, {ActionEnum::kCreated, 1}}));

  e1.reset();
  EXPECT_EQ(
      actions,
      Actions(
          {{ActionEnum::kCreated, 0},
           {ActionEnum::kCreated, 1},
           {ActionEnum::kDestroyed, 0}}));

  auto e4 = e3;
  EXPECT_EQ(e4->id(), 1);
  e3.reset();
  EXPECT_EQ(
      actions,
      Actions(
          {{ActionEnum::kCreated, 0},
           {ActionEnum::kCreated, 1},
           {ActionEnum::kDestroyed, 0}}));

  e4.reset();
  EXPECT_EQ(
      actions,
      Actions(
          {{ActionEnum::kCreated, 0},
           {ActionEnum::kCreated, 1},
           {ActionEnum::kDestroyed, 0},
           {ActionEnum::kDestroyed, 1}}));

  auto e5 = cache.get(1);
  EXPECT_EQ(e5->id(), 1);
  EXPECT_EQ(
      actions,
      Actions(
          {{ActionEnum::kCreated, 0},
           {ActionEnum::kCreated, 1},
           {ActionEnum::kDestroyed, 0},
           {ActionEnum::kDestroyed, 1},
           {ActionEnum::kCreated, 1}}));

  auto e6 = cache.get(0);
  EXPECT_EQ(e6->id(), 0);
  EXPECT_EQ(
      actions,
      Actions(
          {{ActionEnum::kCreated, 0},
           {ActionEnum::kCreated, 1},
           {ActionEnum::kDestroyed, 0},
           {ActionEnum::kDestroyed, 1},
           {ActionEnum::kCreated, 1},
           {ActionEnum::kCreated, 0}}));

  e5.reset();
  EXPECT_EQ(
      actions,
      Actions(
          {{ActionEnum::kCreated, 0},
           {ActionEnum::kCreated, 1},
           {ActionEnum::kDestroyed, 0},
           {ActionEnum::kDestroyed, 1},
           {ActionEnum::kCreated, 1},
           {ActionEnum::kCreated, 0},
           {ActionEnum::kDestroyed, 1}}));

  e6.reset();
  EXPECT_EQ(
      actions,
      Actions(
          {{ActionEnum::kCreated, 0},
           {ActionEnum::kCreated, 1},
           {ActionEnum::kDestroyed, 0},
           {ActionEnum::kDestroyed, 1},
           {ActionEnum::kCreated, 1},
           {ActionEnum::kCreated, 0},
           {ActionEnum::kDestroyed, 1},
           {ActionEnum::kDestroyed, 0}}));
}

TEST_F(TabletTest, referenceCountedCacheStressParallelDuplicates) {
  std::atomic_int counter{0};
  facebook::nimble::ReferenceCountedCache<int, int> cache{[&](int id) {
    ++counter;
    return std::make_shared<int>(id);
  }};
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(10);
  velox::dwio::common::ExecutorBarrier barrier(executor);
  constexpr int kEntryIds = 100;
  constexpr int kEntryDuplicates = 10;
  for (int i = 0; i < kEntryIds; ++i) {
    for (int n = 0; n < kEntryDuplicates; ++n) {
      barrier.add([i, &cache]() {
        auto e = cache.get(i);
        EXPECT_EQ(*e, i);
      });
    }
  }
  barrier.waitAll();
  EXPECT_GE(counter.load(), kEntryIds);
}

TEST_F(TabletTest, referenceCountedCacheStressParallelDuplicatesSaveEntries) {
  std::atomic_int counter{0};
  facebook::nimble::ReferenceCountedCache<int, int> cache{[&](int id) {
    ++counter;
    return std::make_shared<int>(id);
  }};
  folly::Synchronized<std::vector<std::shared_ptr<int>>> entries;
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(10);
  velox::dwio::common::ExecutorBarrier barrier(executor);
  constexpr int kEntryIds = 100;
  constexpr int kEntryDuplicates = 10;
  for (int i = 0; i < kEntryIds; ++i) {
    for (int n = 0; n < kEntryDuplicates; ++n) {
      barrier.add([i, &cache, &entries]() {
        auto e = cache.get(i);
        EXPECT_EQ(*e, i);
        entries.wlock()->push_back(e);
      });
    }
  }
  barrier.waitAll();
  EXPECT_EQ(counter.load(), kEntryIds);
}

TEST_F(TabletTest, referenceCountedCacheStress) {
  std::atomic_int counter{0};
  facebook::nimble::ReferenceCountedCache<int, int> cache{[&](int id) {
    ++counter;
    return std::make_shared<int>(id);
  }};
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(10);
  velox::dwio::common::ExecutorBarrier barrier(executor);
  constexpr int kEntryIds = 100;
  constexpr int kEntryDuplicates = 10;
  for (int n = 0; n < kEntryDuplicates; ++n) {
    for (int i = 0; i < kEntryIds; ++i) {
      barrier.add([i, &cache]() {
        auto e = cache.get(i);
        EXPECT_EQ(*e, i);
      });
    }
  }
  barrier.waitAll();
  EXPECT_GE(counter.load(), kEntryIds);
}
TEST_F(TabletTest, referenceCountedCacheStressSaveEntries) {
  std::atomic_int counter{0};
  facebook::nimble::ReferenceCountedCache<int, int> cache{[&](int id) {
    ++counter;
    return std::make_shared<int>(id);
  }};
  folly::Synchronized<std::vector<std::shared_ptr<int>>> entries;
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(10);
  velox::dwio::common::ExecutorBarrier barrier(executor);
  constexpr int kEntryIds = 100;
  constexpr int kEntryDuplicates = 10;
  for (int n = 0; n < kEntryDuplicates; ++n) {
    for (int i = 0; i < kEntryIds; ++i) {
      barrier.add([i, &cache, &entries]() {
        auto e = cache.get(i);
        EXPECT_EQ(*e, i);
        entries.wlock()->push_back(e);
      });
    }
  }
  barrier.waitAll();
  EXPECT_EQ(counter.load(), kEntryIds);
}

TEST_F(TabletTest, deduplicateStreams) {
  auto seed = FLAGS_tablet_tests_seed > 0 ? FLAGS_tablet_tests_seed
                                          : folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (auto noDuplicates : {true, false}) {
    for (auto chunkCount : {1U, 2U, folly::Random::rand32(10, rng) + 1}) {
      for (auto evenChunks : {true, false}) {
        testStreamDeduplication(rng, noDuplicates, chunkCount, evenChunks);
      }
    }
  }
}

TEST_F(TabletTest, chunkContentSize) {
  nimble::Chunk chunk;
  EXPECT_EQ(chunk.contentSize(), 0);

  chunk.content = {"hello"};
  EXPECT_EQ(chunk.contentSize(), 5);

  chunk.content = {"hello", "world"};
  EXPECT_EQ(chunk.contentSize(), 10);

  chunk.content = {"", "abc", ""};
  EXPECT_EQ(chunk.contentSize(), 3);
}
} // namespace
