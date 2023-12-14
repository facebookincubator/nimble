// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <folly/Singleton.h>
#include <glog/logging.h>
#include "common/init/light.h"
#include "common/strings/Zstd.h"
#include "dwio/alpha/velox/VeloxReader.h"
#include "dwio/common/filesystem/FileSystem.h"
#include "dwio/utils/FeatureReorderingLayoutPlanner.h"
#include "dwio/utils/FileSink.h"
#include "dwio/utils/warm_storage/WarmStorageRetry.h"
#include "fbjava/datainfra-metastore/api/if/gen-cpp2/hive_metastore_types.h"
#include "thrift/lib/cpp2/protocol/CompactProtocol.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/dwio/dwrf/writer/LayoutPlanner.h"
#include "velox/dwio/dwrf/writer/Writer.h"

using namespace ::facebook;

DEFINE_string(
    mode,
    "transform",
    "Operation mode (transform|transform_interactive)");
DEFINE_string(input_file, "", "Input file path");
DEFINE_string(output_file, "", "Output file path");
DEFINE_uint32(batch_size, 200, "Batch row count");
DEFINE_uint64(
    row_count,
    std::numeric_limits<uint64_t>::max(),
    "Max rows to convert");
DEFINE_string(
    serialized_serde,
    "",
    "Serialized version of table metadata serde information. "
    "Serialized payload is serialized using Thrift Compact protocol, Zstd compressed and Base64 encoded.");
DEFINE_string(
    serialized_feature_order,
    "",
    "Serialized version of featureOrdering thrift struct. "
    "Serialized payload is serialized using Thrift Compact protocol, Zstd compressed and Base64 encoded.");

// Initialize dummy Velox stats reporter
folly::Singleton<velox::BaseStatsReporter> reporter([]() {
  return new velox::DummyStatsReporter();
});

template <typename T>
T deserialize(const std::string& source) {
  T result;
  auto compressed = apache::thrift::protocol::base64Decode(source);
  std::string uncompressed;
  if (!strings::zstdDecompress(compressed->moveToFbString(), &uncompressed)) {
    throw std::runtime_error(
        fmt::format("Unable to decompress data: {}", source));
  }
  apache::thrift::CompactSerializer::deserialize(uncompressed, result);
  return result;
}

void run(
    velox::memory::MemoryPool& memoryPool,
    std::string inputFile,
    std::string outputFile) {
  LOG(INFO) << "Input: " << inputFile;
  LOG(INFO) << "Output: " << outputFile;
  LOG(INFO) << "Serde Provided: " << std::boolalpha
            << !FLAGS_serialized_serde.empty();
  LOG(INFO) << "Feature Reordering Provided: " << std::boolalpha
            << !FLAGS_serialized_feature_order.empty();

  dwio::common::request::AccessDescriptor accessDescriptor{
      dwio::common::request::AccessDescriptorBuilder()
          .withClientId("dwrf_transform")
          .build()};

  auto file =
      dwio::file_system::FileSystem::openForRead(inputFile, accessDescriptor);

  auto readerPool = memoryPool.addLeafChild("Reader");
  alpha::VeloxReader reader(*readerPool, file.get());

  std::string_view fileBuffer;
  velox::VectorPtr vector;
  velox::dwrf::WriterOptions options{.schema = reader.getType()};

  if (!FLAGS_serialized_serde.empty()) {
    auto serde =
        deserialize<Apache::Hadoop::Hive::SerDeInfo>(FLAGS_serialized_serde);
    options.config = velox::dwrf::Config::fromMap(serde.get_parameters());
  }

  if (!FLAGS_serialized_feature_order.empty()) {
    options.layoutPlannerFactory =
        [featureOrdering = deserialize<Apache::Hadoop::Hive::FeatureOrdering>(
             FLAGS_serialized_feature_order)](
            const velox::dwio::common::TypeWithId& schema) {
          return std::make_unique<
              facebook::dwio::utils::FeatureReorderingLayoutPlanner>(
              schema, featureOrdering);
        };
  }

  velox::dwrf::Writer writer(
      std::move(options),
      facebook::dwio::utils::DataSinkFactory::create(
          outputFile, accessDescriptor),
      memoryPool);

  while (reader.next(FLAGS_batch_size, vector)) {
    writer.write(vector);
  }

  writer.close();
}

int main(int argc, char* argv[]) {
  auto init = facebook::init::InitFacebookLight{&argc, &argv};
  ALPHA_CHECK(
      FLAGS_mode == "transform" || FLAGS_mode == "transform_interactive",
      fmt::format("Unknown run mode: {}", FLAGS_mode));
  auto pool = facebook::velox::memory::defaultMemoryManager().addRootPool();

  LOG(INFO) << "DWRF Transform - Mode: " << FLAGS_mode;

  try {
    if (FLAGS_mode == "transform_interactive") {
      // Interactive mode - Running in Spark.
      // Spark provides arguments using stdin and expects a result to be
      // sent to stdout when processing is done.
      std::string inputFile;
      std::string outputStagingFile;
      dwio::common::request::AccessDescriptor accessDescriptor{
          dwio::common::request::AccessDescriptorBuilder()
              .withClientId("dwrf_transform")
              .build()};
      while (std::cin >> inputFile >> outputStagingFile) {
        run(*pool, inputFile, outputStagingFile);
        auto outputPath =
            outputStagingFile.substr(0, outputStagingFile.find_last_of('/'));
        auto filename = inputFile.substr(inputFile.find_last_of('/'));
        auto finalOutputPath = outputPath + filename;
        DWIO_CALL_WS_FS_DESCRIPTOR_STATS_WITH_RETRY(
            rename,
            outputStagingFile,
            accessDescriptor,
            /* stats */ nullptr,
            outputStagingFile,
            finalOutputPath,
            /* overwrite */ true,
            /* srctag */ "",
            /* dsttag */ "",
            warm_storage::DirectoryOwnershipTokenPair(),
            warm_storage::WSFileSystemMetadataType::ANY,
            warm_storage::RenameOptions{warm_storage::CommonFileAPIOptions{
                accessDescriptor.state().storageAccessToken}});

        std::cout << finalOutputPath;
      }
    } else {
      ALPHA_CHECK(!FLAGS_input_file.empty(), "Input file missing.");
      ALPHA_CHECK(!FLAGS_output_file.empty(), "Output file missing.");
      run(*pool, FLAGS_input_file, FLAGS_output_file);
    }
  } catch (const std::exception& e) {
    LOG(WARNING) << "*** Error ***\n" << e.what();
    throw;
  }
}
