// Copyright 2004-present Facebook. All Rights Reserved.

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <iterator>
#include <limits>

#include "dwio/alpha/common/Buffer.h"
#include "dwio/alpha/common/Checksum.h"
#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/common/tests/TestUtils.h"
#include "dwio/alpha/tablet/Tablet.h"
#include "folly/FileUtil.h"
#include "folly/Random.h"
#include "folly/experimental/coro/Generator.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"

using namespace ::facebook;

namespace {

// Total size of the fields after the flatbuffer.
constexpr uint32_t kPostscriptSize = 20;

struct StripeSpecifications {
  uint32_t rowCount;
  std::vector<uint32_t> streamOffsets;
};

struct StripeData {
  uint32_t rowCount;
  std::vector<alpha::Stream> streams;
};

void printData(std::string prefix, std::string_view data) {
  std::string output;
  for (auto i = 0; i < data.size(); ++i) {
    output += folly::to<std::string>((uint8_t)data[i]) + " ";
  }

  LOG(INFO) << prefix << " (" << (void*)data.data() << "): " << output;
}

std::vector<StripeData> createStripesData(
    std::mt19937& rng,
    const alpha::CompressionParams& compressionParams,
    const std::vector<StripeSpecifications>& stripes,
    alpha::Buffer& buffer) {
  std::vector<StripeData> stripesData;
  stripesData.reserve(stripes.size());

  // Each generator iteration returns a single stripe to write
  for (auto& stripe : stripes) {
    std::vector<alpha::Stream> streams;
    streams.reserve(stripe.streamOffsets.size());
    std::transform(
        stripe.streamOffsets.cbegin(),
        stripe.streamOffsets.cend(),
        std::back_inserter(streams),
        [&rng, &compressionParams, &buffer](auto offset) {
          const auto size = folly::Random::rand32(32, rng) + 2;
          auto pos = buffer.reserve(size);
          for (auto i = 0; i < size; ++i) {
            pos[i] = folly::Random::rand32(256, rng);
          }
          printData(folly::to<std::string>("Stream ", offset), {pos, size});

          return alpha::Stream{offset, {pos, size}, compressionParams};
        });
    stripesData.push_back({
        .rowCount = stripe.rowCount,
        .streams = std::move(streams),
    });
  }

  return stripesData;
}

// Runs a single write/read test using input parameters
void parameterizedTest(
    std::mt19937& rng,
    velox::memory::MemoryPool& memoryPool,
    uint32_t metadataFlushThreshold,
    uint32_t metadataCompressionThreshold,
    const alpha::CompressionParams& compressionParams,
    std::vector<StripeSpecifications> stripes,
    const std::optional<std::function<void(const std::exception&)>>&
        errorVerifier = std::nullopt) {
  try {
    std::string file;
    velox::InMemoryWriteFile writeFile(&file);
    alpha::TabletWriter tabletWriter{
        memoryPool,
        &writeFile,
        {nullptr, metadataFlushThreshold, metadataCompressionThreshold}};

    EXPECT_EQ(0, tabletWriter.size());

    struct StripeData {
      uint32_t rowCount;
      std::vector<alpha::Stream> streams;
    };

    alpha::Buffer buffer{memoryPool};
    auto stripesData =
        createStripesData(rng, compressionParams, stripes, buffer);
    for (auto& stripe : stripesData) {
      tabletWriter.writeStripe(stripe.rowCount, stripe.streams);
    }

    tabletWriter.close();
    EXPECT_LT(0, tabletWriter.size());
    writeFile.close();
    EXPECT_EQ(writeFile.size(), tabletWriter.size());

    auto stripeGroupCount = tabletWriter.stripeGroupCount();

    folly::writeFile(file, "/tmp/test.alpha");

    alpha::testing::InMemoryTrackableReadFile readFile(file);
    alpha::Tablet tablet{memoryPool, &readFile};
    EXPECT_EQ(stripesData.size(), tablet.stripeCount());
    EXPECT_EQ(
        std::accumulate(
            stripesData.begin(),
            stripesData.end(),
            uint64_t{0},
            [](uint64_t r, const auto& s) { return r + s.rowCount; }),
        tablet.tabletRowCount());

    VLOG(1) << "Output Tablet -> StripeCount: " << tablet.stripeCount()
            << ", RowCount: " << tablet.tabletRowCount()
            << ", Compression: " << toString(compressionParams.type);

    // Now, read all stripes and verify results
    size_t extraReads = 0;
    for (auto stripe = 0; stripe < stripesData.size(); ++stripe) {
      EXPECT_EQ(stripesData[stripe].rowCount, tablet.stripeRowCount(stripe));

      readFile.resetChunks();
      std::vector<uint32_t> identifiers(tablet.streamCount(stripe));
      std::iota(identifiers.begin(), identifiers.end(), 0);
      auto serializedStreams =
          tablet.load(stripe, {identifiers.cbegin(), identifiers.cend()});
      auto chunks = readFile.chunks();
      auto expectedReads = stripesData[stripe].streams.size();
      auto diff = chunks.size() - expectedReads;
      EXPECT_LE(diff, 1);
      extraReads += diff;

      for (const auto& chunk : chunks) {
        VLOG(1) << "Chunk Offset: " << chunk.offset << ", Size: " << chunk.size
                << ", Stripe: " << stripe;
      }

      for (auto i = 0; i < serializedStreams.size(); ++i) {
        // Verify streams content. If stream wasn't written in this stripe, it
        // should return nullopt optional.
        auto found = false;
        for (const auto& stream : stripesData[stripe].streams) {
          if (stream.offset == i) {
            found = true;
            EXPECT_TRUE(serializedStreams[i]);
            printData(
                folly::to<std::string>("Expected Stream ", stream.offset),
                stream.content.front());
            auto actual = serializedStreams[i]->nextChunk();
            printData(
                folly::to<std::string>("Actual Stream ", stream.offset),
                actual);
            EXPECT_EQ(stream.content.front(), actual);
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
  } catch (const std::exception& e) {
    if (!errorVerifier.has_value()) {
      FAIL() << "Unexpected exception: " << e.what();
    }

    errorVerifier.value()(e);

    // If errorVerifier detected an error, log the exception
    if (testing::Test::HasFatalFailure()) {
      FAIL() << "Failed verifying exception: " << e.what();
    } else if (testing::Test::HasNonfatalFailure()) {
      LOG(WARNING) << "Failed verifying exception: " << e.what();
    }
  }
}

// Run all permutations of a test using all test parameters
void test(
    velox::memory::MemoryPool& memoryPool,
    std::vector<StripeSpecifications> stripes,
    std::optional<std::function<void(const std::exception&)>> errorVerifier =
        std::nullopt) {
  std::vector<alpha::CompressionParams> params{
      {.type = alpha::CompressionType::Uncompressed},
      {.type = alpha::CompressionType::Zstd}};
  std::vector<uint64_t> metadataCompressionThresholds{
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

  for (auto flushThreshold : metadataFlushThresholds) {
    for (auto compressionThreshold : metadataCompressionThresholds) {
      for (const auto& param : params) {
        LOG(INFO) << "FlushThreshold: " << flushThreshold
                  << ", CompressionThreshold: " << compressionThreshold
                  << ", CompressionType: " << param.type;
        parameterizedTest(
            rng,
            memoryPool,
            flushThreshold,
            compressionThreshold,
            param,
            stripes,
            errorVerifier);
      }
    }
  }
}

} // namespace

class TabletTestSuite : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

TEST_F(TabletTestSuite, EmptyWrite) {
  // Creating an Alpha file without writing any stripes
  test(
      *this->pool_,
      /* stripes */ {});
}

TEST_F(TabletTestSuite, WriteDifferentStreamsPerStripe) {
  // Write different subset of streams in each stripe
  test(
      *this->pool_,
      /* stripes */
      {
          {.rowCount = 20, .streamOffsets = {3, 1}},
          {.rowCount = 30, .streamOffsets = {2}},
          {.rowCount = 20, .streamOffsets = {4}},
      });
}

namespace {
void checksumTest(
    std::mt19937& rng,
    velox::memory::MemoryPool& memoryPool,
    uint32_t metadataCompressionThreshold,
    alpha::ChecksumType checksumType,
    bool checksumChunked,
    std::vector<StripeSpecifications> stripes) {
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  alpha::TabletWriter tabletWriter{
      memoryPool,
      &writeFile,
      {.layoutPlanner = nullptr,
       .metadataCompressionThreshold = metadataCompressionThreshold,
       .checksumType = checksumType}};
  EXPECT_EQ(0, tabletWriter.size());

  struct StripeData {
    uint32_t rowCount;
    std::vector<alpha::Stream> streams;
  };

  alpha::Buffer buffer{memoryPool};
  auto stripesData = createStripesData(
      rng, {.type = alpha::CompressionType::Zstd}, stripes, buffer);

  for (auto& stripe : stripesData) {
    tabletWriter.writeStripe(stripe.rowCount, stripe.streams);
  }

  tabletWriter.close();
  EXPECT_LT(0, tabletWriter.size());
  writeFile.close();
  EXPECT_EQ(writeFile.size(), tabletWriter.size());

  // Velidate checksum on a good file
  alpha::testing::InMemoryTrackableReadFile readFile(file);
  alpha::Tablet tablet{memoryPool, &readFile};
  auto storedChecksum = tablet.checksum();
  EXPECT_EQ(
      storedChecksum,
      alpha::Tablet::calculateChecksum(
          memoryPool,
          &readFile,
          checksumChunked ? writeFile.size() / 3 : writeFile.size()))
      << "metadataCompressionThreshold: " << metadataCompressionThreshold
      << ", checksumType: " << alpha::toString(checksumType)
      << ", checksumChunked: " << checksumChunked;

  // Flip a bit in the stream and verify that checksum can catch the error
  {
    // First, make sure we are working on a clean file
    alpha::testing::InMemoryTrackableReadFile readFileUnchanged(file);
    EXPECT_EQ(
        storedChecksum,
        alpha::Tablet::calculateChecksum(memoryPool, &readFileUnchanged));

    char& c = file[10];
    c ^= 0x80;
    alpha::testing::InMemoryTrackableReadFile readFileChanged(file);
    EXPECT_NE(
        storedChecksum,
        alpha::Tablet::calculateChecksum(
            memoryPool,
            &readFileChanged,
            checksumChunked ? writeFile.size() / 3 : writeFile.size()))
        << "Checksum didn't find corruption when stream content is changed. "
        << "metadataCompressionThreshold: " << metadataCompressionThreshold
        << ", checksumType: " << alpha::toString(checksumType)
        << ", checksumChunked: " << checksumChunked;
    // revert the file back.
    c ^= 0x80;
  }

  // Flip a bit in the flatbuffer footer and verify that checksum can catch
  // the error
  {
    // First, make sure we are working on a clean file
    alpha::testing::InMemoryTrackableReadFile readFileUnchanged(file);
    EXPECT_EQ(
        storedChecksum,
        alpha::Tablet::calculateChecksum(memoryPool, &readFileUnchanged));

    auto posInFooter =
        tablet.fileSize() - kPostscriptSize - tablet.footerSize() / 2;
    uint8_t& byteInFooter =
        *reinterpret_cast<uint8_t*>(file.data() + posInFooter);
    byteInFooter ^= 0x1;
    alpha::testing::InMemoryTrackableReadFile readFileChanged(file);
    EXPECT_NE(
        storedChecksum,
        alpha::Tablet::calculateChecksum(
            memoryPool,
            &readFileChanged,
            checksumChunked ? writeFile.size() / 3 : writeFile.size()))
        << "Checksum didn't find corruption when footer content is changed. "
        << "metadataCompressionThreshold: " << metadataCompressionThreshold
        << ", checksumType: " << alpha::toString(checksumType)
        << ", checksumChunked: " << checksumChunked;
    // revert the file back.
    byteInFooter ^= 0x1;
  }

  // Flip a bit in the footer size field and verify that checksum can catch
  // the error
  {
    // First, make sure we are working on a clean file
    alpha::testing::InMemoryTrackableReadFile readFileUnchanged(file);
    EXPECT_EQ(
        storedChecksum,
        alpha::Tablet::calculateChecksum(memoryPool, &readFileUnchanged));

    auto footerSizePos = tablet.fileSize() - kPostscriptSize;
    uint32_t& footerSize =
        *reinterpret_cast<uint32_t*>(file.data() + footerSizePos);
    ASSERT_EQ(footerSize, tablet.footerSize());
    footerSize ^= 0x1;
    alpha::testing::InMemoryTrackableReadFile readFileChanged(file);
    EXPECT_NE(
        storedChecksum,
        alpha::Tablet::calculateChecksum(
            memoryPool,
            &readFileChanged,
            checksumChunked ? writeFile.size() / 3 : writeFile.size()))
        << "Checksum didn't find corruption when footer size field is changed. "
        << "metadataCompressionThreshold: " << metadataCompressionThreshold
        << ", checksumType: " << alpha::toString(checksumType)
        << ", checksumChunked: " << checksumChunked;
    // revert the file back.
    footerSize ^= 0x1;
  }

  // Flip a bit in the footer compression type field and verify that checksum
  // can catch the error
  {
    // First, make sure we are working on a clean file
    alpha::testing::InMemoryTrackableReadFile readFileUnchanged(file);
    EXPECT_EQ(
        storedChecksum,
        alpha::Tablet::calculateChecksum(memoryPool, &readFileUnchanged));

    auto footerCompressionTypePos = tablet.fileSize() - kPostscriptSize + 4;
    alpha::CompressionType& footerCompressionType =
        *reinterpret_cast<alpha::CompressionType*>(
            file.data() + footerCompressionTypePos);
    ASSERT_EQ(footerCompressionType, tablet.footerCompressionType());
    // Cannot do bit operation on enums, so cast it to integer type.
    uint8_t& typeAsInt = *reinterpret_cast<uint8_t*>(&footerCompressionType);
    typeAsInt ^= 0x1;
    alpha::testing::InMemoryTrackableReadFile readFileChanged(file);
    EXPECT_NE(
        storedChecksum,
        alpha::Tablet::calculateChecksum(
            memoryPool,
            &readFileChanged,
            checksumChunked ? writeFile.size() / 3 : writeFile.size()))
        << "Checksum didn't find corruption when compression type field is changed. "
        << "metadataCompressionThreshold: " << metadataCompressionThreshold
        << ", checksumType: " << alpha::toString(checksumType)
        << ", checksumChunked: " << checksumChunked;
    // revert the file back.
    typeAsInt ^= 0x1;
  }
}
} // namespace

TEST_F(TabletTestSuite, ChecksumValidation) {
  std::vector<uint64_t> metadataCompressionThresholds{
      // use size 0 here so it will always force a footer compression
      0,
      // use a large number here so it will not do a footer compression
      1024 * 1024 * 1024};

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (auto metadataCompressionThreshold : metadataCompressionThresholds) {
    for (auto algorithm : {alpha::ChecksumType::XXH3_64}) {
      for (auto checksumChunked : {true, false}) {
        checksumTest(
            rng,
            *this->pool_,
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

TEST(TabletTests, OptionalSections) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};

  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  alpha::TabletWriter tabletWriter{*pool, &writeFile};

  auto randomSize = folly::Random::rand32(20, 2000000, rng);
  std::string random;
  random.resize(folly::Random::rand32(20, 2000000, rng));
  for (auto i = 0; i < random.size(); ++i) {
    random[i] = folly::Random::rand32(256);
  }
  {
    const std::string& content = random;
    tabletWriter.writeOptionalSection("section1", content);
  }
  {
    std::string content;
    content.resize(randomSize);
    for (auto i = 0; i < content.size(); ++i) {
      content[i] = '\0';
    }

    tabletWriter.writeOptionalSection("section2", content);
  }
  {
    std::string content;
    tabletWriter.writeOptionalSection("section3", content);
  }

  tabletWriter.close();

  alpha::testing::InMemoryTrackableReadFile readFile(file);
  alpha::Tablet tablet{*pool, &readFile};

  auto section = tablet.loadOptionalSection("section1");
  ASSERT_TRUE(section.has_value());
  ASSERT_EQ(random, section->content());

  std::string expectedContent;
  expectedContent.resize(randomSize);
  for (auto i = 0; i < expectedContent.size(); ++i) {
    expectedContent[i] = '\0';
  }
  section = tablet.loadOptionalSection("section2");
  ASSERT_TRUE(section.has_value());
  ASSERT_EQ(expectedContent, section->content());

  section = tablet.loadOptionalSection("section3");
  ASSERT_TRUE(section.has_value());
  ASSERT_EQ(std::string(), section->content());

  section = tablet.loadOptionalSection("section4");
  ASSERT_FALSE(section.has_value());
}

TEST(TabletTests, OptionalSectionsEmpty) {
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  std::string file;
  velox::InMemoryWriteFile writeFile(&file);
  alpha::TabletWriter tabletWriter{*pool, &writeFile};

  tabletWriter.close();

  alpha::testing::InMemoryTrackableReadFile readFile(file);
  alpha::Tablet tablet{*pool, &readFile};

  auto section = tablet.loadOptionalSection("section1");
  ASSERT_FALSE(section.has_value());
}

TEST(TabletTests, OptionalSectionsPreload) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};

  for (const auto footerCompressionThreshold :
       {0U, std::numeric_limits<uint32_t>::max()}) {
    auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
    std::string file;
    velox::InMemoryWriteFile writeFile(&file);
    alpha::TabletWriter tabletWriter{*pool, &writeFile};

    // Using random string to make sure compression can't compress it well
    std::string random;
    random.resize(20 * 1024 * 1024);
    for (auto i = 0; i < random.size(); ++i) {
      random[i] = folly::Random::rand32(256);
    }

    tabletWriter.writeOptionalSection("section1", "aaaa");
    tabletWriter.writeOptionalSection("section2", "bbbb");
    tabletWriter.writeOptionalSection("section3", random);
    tabletWriter.writeOptionalSection("section4", "dddd");
    tabletWriter.writeOptionalSection("section5", "eeee");
    tabletWriter.close();

    auto verify = [&](std::vector<std::string> preload,
                      size_t expectedInitialReads,
                      std::vector<std::tuple<std::string, size_t, std::string>>
                          expected) {
      alpha::testing::InMemoryTrackableReadFile readFile(file);
      alpha::Tablet tablet{*pool, &readFile, preload};

      // Expecting only the initial footer read.
      ASSERT_EQ(expectedInitialReads, readFile.chunks().size());

      for (const auto& e : expected) {
        auto expectedSection = std::get<0>(e);
        auto expectedReads = std::get<1>(e);
        auto expectedContent = std::get<2>(e);
        auto section = tablet.loadOptionalSection(expectedSection);
        ASSERT_TRUE(section.has_value());
        ASSERT_EQ(expectedContent, section->content());
        ASSERT_EQ(expectedReads, readFile.chunks().size());
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
