// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <folly/Singleton.h>
#include <glog/logging.h>
#include <ios>
#include <unordered_map>
#include "common/files/FileUtil.h"
#include "common/init/light.h"
#include "common/strings/Zstd.h"
#include "dwio/alpha/velox/Config.h"
#include "dwio/alpha/velox/EncodingLayoutTree.h"
#include "dwio/alpha/velox/VeloxWriter.h"
#include "dwio/api/AlphaWriterOptionBuilder.h"
#include "dwio/catalog/fbhive/HiveCatalog.h"
#include "dwio/common/filesystem/FileSystem.h"
#include "dwio/tools/FormatConverter.h"
#include "dwio/utils/BufferedWriteFile.h"
#include "dwio/utils/InputStreamFactory.h"
#include "dwio/utils/warm_storage/WarmStorageRetry.h"
#include "fbjava/datainfra-metastore/api/if/gen-cpp2/hive_metastore_types.h"
#include "folly/String.h"
#include "thrift/lib/cpp/protocol/TBase64Utils.h"
#include "thrift/lib/cpp2/protocol/CompactProtocol.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"

using namespace ::facebook;
// using namespace facebook::alpha;

DEFINE_string(
    mode,
    "transform",
    "Operation mode (transform|transform_interactive)");
DEFINE_string(input_file, "", "Input file path");
DEFINE_string(output_file, "", "Output file path");
DEFINE_uint64(batch_size, 512, "Batch row count");
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
DEFINE_uint64(
    raw_stripe_size,
    384 * 1024 * 1024,
    "Raw stripe size threshold to trigger encoding. Only used if 'serialized_serde' is not provided.");
DEFINE_string(
    flatmap_names,
    "float_features;id_list_features;id_score_list_features;native_bytes_array_features;event_based_features",
    "Column names to write as flat maps. Only used if 'serialized_serde' is not provided.");
DEFINE_string(
    dictionary_array_names,
    "",
    "Column names to write as dictionary array");
DEFINE_string(
    feature_reordering_spec,
    "",
    "This spec will be used to retrieve feature reording metadata. "
    "Format: ns:table:partition (where partition is in the format: key1=value1/key2=value2). "
    "Only used if 'serialized_feature_order' is not provided.");
DEFINE_uint64(
    buffered_write_size,
    72 * 1024 * 1024,
    "Buffer size for buffered WS writes");
DEFINE_string(
    encoding_layout_file,
    "",
    "An optional file with captured encoding layout tree, to be used during write.");

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
    std::shared_ptr<velox::memory::MemoryPool> memoryPool,
    std::string inputFile,
    std::string outputFile) {
  LOG(INFO) << "Input: " << inputFile;
  LOG(INFO) << "Output: " << outputFile;
  LOG(INFO) << "Buffered write size: " << FLAGS_buffered_write_size << " bytes";
  LOG(INFO) << "Batch size: " << FLAGS_batch_size << " rows";
  LOG(INFO) << "Encoding Layout File: " << FLAGS_encoding_layout_file;

  auto accessDescriptor = dwio::common::request::AccessDescriptorBuilder{}
                              .withClientId("alpha_transform")
                              .build();

  std::map<std::string, std::string> serdeParameters;
  std::vector<std::string> flatMaps;
  std::vector<std::string> dictionaryArrays;
  if (!FLAGS_serialized_serde.empty()) {
    LOG(INFO) << "Using serde: " << FLAGS_serialized_serde;
    serdeParameters =
        deserialize<Apache::Hadoop::Hive::SerDeInfo>(FLAGS_serialized_serde)
            .get_parameters();
  } else {
    LOG(INFO) << "Raw stripe size: " << FLAGS_raw_stripe_size << " bytes";
    LOG(INFO) << "Flatmap columns: " << FLAGS_flatmap_names;
    LOG(INFO) << "DictionaryArray columns: " << FLAGS_dictionary_array_names;
    alpha::Config config;
    config.set(alpha::Config::RAW_STRIPE_SIZE, FLAGS_raw_stripe_size);
    serdeParameters = config.toSerdeParams();

    if (!FLAGS_flatmap_names.empty()) {
      folly::split(';', FLAGS_flatmap_names, flatMaps);
    }

    if (!FLAGS_dictionary_array_names.empty()) {
      folly::split(';', FLAGS_dictionary_array_names, dictionaryArrays);
    }
  }

  std::string schema;
  std::string encodings;

  dwio::api::WriterOptionOverrides writerOptionOverrides{
      .alphaOverrides = [&](auto& writerOptions) {
        for (const auto& col : flatMaps) {
          writerOptions.flatMapColumns.insert(col);
        }
        for (const auto& col : dictionaryArrays) {
          writerOptions.dictionaryArrayColumns.insert(col);
        }

        if (!FLAGS_encoding_layout_file.empty()) {
          auto encodingFile = dwio::file_system::FileSystem::openForRead(
              FLAGS_encoding_layout_file, accessDescriptor);
          std::string buffer;
          buffer.resize(encodingFile->size());
          ALPHA_CHECK(
              encodingFile->pread(0, buffer.size(), buffer.data()).size() ==
                  buffer.size(),
              "Problem reading encoding layout file. Size mismatch.");

          std::string uncompressed;
          strings::zstdDecompress(buffer, &uncompressed);

          writerOptions.encodingLayoutTree.emplace(
              alpha::EncodingLayoutTree::create(uncompressed));
        }

        if (!FLAGS_serialized_feature_order.empty()) {
          LOG(INFO) << "Using serialized feature reordering";
          auto featureReorderingSource =
              deserialize<Apache::Hadoop::Hive::FeatureOrdering>(
                  FLAGS_serialized_feature_order);
          std::vector<std::tuple<size_t, std::vector<int64_t>>>
              featureReordering;
          featureReordering.reserve(
              featureReorderingSource.featureOrdering_ref()->size());
          for (const auto& column :
               featureReorderingSource.featureOrdering_ref().value()) {
            std::vector<int64_t> features;
            features.reserve(column.featureOrder_ref()->size());
            std::copy(
                column.featureOrder_ref()->cbegin(),
                column.featureOrder_ref()->cend(),
                std::back_inserter(features));
            featureReordering.emplace_back(
                column.columnId_ref()->columnIdentifier_ref().value(),
                std::move(features));
          }

          writerOptions.featureReordering.emplace(std::move(featureReordering));
        }
      }};

  bool fetchFeatureOrder = FLAGS_serialized_feature_order.empty() &&
      !FLAGS_feature_reordering_spec.empty();

  // Load partition metadata so that format converter can fetch the
  // feature order correctly.
  std::shared_ptr<const dwio::catalog::PartitionMetadata> partitionMetadata;
  if (fetchFeatureOrder) {
    LOG(INFO) << "Loading feature reordering with spec: "
              << FLAGS_feature_reordering_spec;
    std::vector<std::string> parts;
    parts.reserve(3);
    folly::split(':', FLAGS_feature_reordering_spec, parts);

    ALPHA_CHECK(
        parts.size() == 3,
        "Invalid feature reordering spec. Expecting <ns>:<table>:<partition>.");

    dwio::catalog::fbhive::HiveCatalog catalog{
        dwio::common::request::AccessDescriptorBuilder{}.build()};
    partitionMetadata = catalog
                            .getPartitionsByNames(
                                /*ns=*/parts[0],
                                /*tableName=*/parts[1],
                                /*partitionNames=*/{parts[2]},
                                /*prunePartitionMetadata=*/false)
                            .front();
  }

  dwio::tools::FormatConverterOptions converterOpts{
      .inputFormat = velox::dwio::common::FileFormat::DWRF,
      .outputFormat = velox::dwio::common::FileFormat::ALPHA,
      .serdeOverride = serdeParameters,
      .batchSize = FLAGS_batch_size,
      .rowCount = FLAGS_row_count,
      .partitionMetadata = std::move(partitionMetadata),
      .fetchFeatureOrder = fetchFeatureOrder,
      .writerOptionOverrides = std::move(writerOptionOverrides)};

  dwio::tools::FormatConverter{std::move(memoryPool), std::move(converterOpts)}
      .run(inputFile, outputFile);
}

int main(int argc, char* argv[]) {
  auto init = facebook::init::InitFacebookLight{&argc, &argv};
  ALPHA_CHECK(
      FLAGS_mode == "transform" || FLAGS_mode == "transform_interactive",
      fmt::format("Unknown run mode: {}", FLAGS_mode));

  LOG(INFO) << "Alpha Transform - Mode: " << FLAGS_mode;
  auto pool =
      facebook::velox::memory::deprecatedDefaultMemoryManager().addRootPool(
          "alpha_transform");
  try {
    if (FLAGS_mode == "transform_interactive") {
      // Interactive mode - Running in Spark.
      // Spark provides arguments using stdin and expects a result to be
      // sent to stdout when processing is done.
      std::string inputFile;
      std::string outputStagingFile;
      dwio::common::request::AccessDescriptor accessDescriptor{
          dwio::common::request::AccessDescriptorBuilder()
              .withClientId("alpha_transform")
              .build()};
      while (std::cin >> inputFile >> outputStagingFile) {
        run(pool, inputFile, outputStagingFile);
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
                accessDescriptor.state().storageAccessToken()}});
        std::cout << finalOutputPath;
      }
    } else {
      ALPHA_CHECK(!FLAGS_input_file.empty(), "Input file missing.");
      ALPHA_CHECK(!FLAGS_output_file.empty(), "Output file missing.");
      run(pool, FLAGS_input_file, FLAGS_output_file);
    }
  } catch (const std::exception& e) {
    LOG(WARNING) << "*** Error ***\n" << e.what();
    throw;
  }
}
