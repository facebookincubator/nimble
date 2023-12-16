// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <folly/Singleton.h>
#include <glog/logging.h>
#include <ios>
#include <unordered_map>
#include "common/init/light.h"
#include "common/strings/URL.h"
#include "common/strings/Zstd.h"
#include "dwio/alpha/velox/VeloxWriter.h"
#include "dwio/api/AlphaWriterOptionBuilder.h"
#include "dwio/catalog/fbhive/HiveCatalog.h"
#include "dwio/common/filesystem/FileSystem.h"
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
DEFINE_uint32(batch_size, 512, "Batch row count");
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
DEFINE_uint32(
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
DEFINE_bool(use_encoding_selection, true, "Enable encoding selection");
DEFINE_uint64(
    buffered_write_size,
    72 * 1024 * 1024,
    "Buffer size for buffered WS writes");

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
  LOG(INFO) << "Using encoding selection: " << std::boolalpha
            << FLAGS_use_encoding_selection;

  auto readerPool = memoryPool->addLeafChild("reader");
  velox::dwio::common::ReaderOptions options{readerPool.get()};
  auto accessDescriptor = dwio::common::request::AccessDescriptorBuilder{}
                              .withClientId("alpha_transform")
                              .build();

  velox::dwrf::DwrfReader reader(
      options,
      std::make_unique<velox::dwio::common::BufferedInput>(
          dwio::utils::InputStreamFactory::create(inputFile, accessDescriptor),
          *readerPool));

  velox::dwio::common::RowReaderOptions rowOptions;
  rowOptions.setReturnFlatVector(true);
  auto rowReader = reader.createRowReader(rowOptions);

  auto writeFilePool = memoryPool->addLeafChild("write_file");
  std::string fileBuffer;
  std::unique_ptr<velox::WriteFile> file =
      std::make_unique<dwio::api::BufferedWriteFile>(
          writeFilePool,
          FLAGS_buffered_write_size,
          dwio::file_system::FileSystem::openForUnbufferedWrite(
              outputFile, accessDescriptor));

  dwio::api::AlphaWriterOptionBuilder optionBuilder;
  if (!FLAGS_serialized_serde.empty()) {
    LOG(INFO) << "Using serde: " << FLAGS_serialized_serde;
    auto serdeSource =
        deserialize<Apache::Hadoop::Hive::SerDeInfo>(FLAGS_serialized_serde);
    optionBuilder.withSerdeParams(
        reader.rowType(), serdeSource.get_parameters());
  } else {
    LOG(INFO) << "Raw stripe size: " << FLAGS_raw_stripe_size << " bytes";
    LOG(INFO) << "Flatmap columns: " << FLAGS_flatmap_names;
    LOG(INFO) << "DictionaryArray columns: " << FLAGS_dictionary_array_names;
    optionBuilder.withDefaultFlushPolicy(FLAGS_raw_stripe_size)
        .withSerdeParams(reader.rowType(), {});
    if (!FLAGS_flatmap_names.empty()) {
      std::vector<std::string> flatMaps;
      folly::split(';', FLAGS_flatmap_names, flatMaps);
      optionBuilder.withFlatMapColumns(flatMaps);
    }
    if (!FLAGS_dictionary_array_names.empty()) {
      std::vector<std::string> dictionaryArrays;
      folly::split(';', FLAGS_dictionary_array_names, dictionaryArrays);
      optionBuilder.withDictionaryArrayColumns(dictionaryArrays);
    }
  }

  if (!FLAGS_serialized_feature_order.empty()) {
    LOG(INFO) << "Using serialized feature reordering";
    auto featureReorderingSource =
        deserialize<Apache::Hadoop::Hive::FeatureOrdering>(
            FLAGS_serialized_feature_order);
    std::vector<std::tuple<size_t, std::vector<int64_t>>> featureReordering;
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

    optionBuilder.withFeatureReordering(std::move(featureReordering));
  } else {
    if (!FLAGS_feature_reordering_spec.empty()) {
      LOG(INFO) << "Loading feature reordering with spec: "
                << FLAGS_feature_reordering_spec;
      std::vector<std::string> parts;
      parts.reserve(3);
      folly::split(':', FLAGS_feature_reordering_spec, parts);

      ALPHA_CHECK(
          parts.size() == 3,
          "Invalid feature reordering spec. Expecting <ns>:<table>:<partition>.");

      std::vector<std::string> segments;
      folly::split('/', parts[2], segments);

      std::unordered_map<std::string, std::string> partitionPairs(
          segments.size());
      for (auto i = 0; i < segments.size(); ++i) {
        std::vector<std::string> pair;
        folly::split('=', segments[i], pair);
        ALPHA_CHECK(
            pair.size() == 2,
            "Partition segments must be in the format key=value.");
        // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
        partitionPairs.insert({pair[0], strings::URL::urlUnescape(pair[1])});
      }

      dwio::catalog::fbhive::HiveCatalog catalog{
          dwio::common::request::AccessDescriptorBuilder{}.build()};
      auto tableProperties =
          catalog.getTableStorageProperties(parts[0], parts[1]);
      std::vector<std::string> partitionValues;
      partitionValues.reserve(tableProperties->getPartitionKeys().size());
      for (const auto& key : tableProperties->getPartitionKeys()) {
        auto it = partitionPairs.find(key);
        ALPHA_CHECK(
            it != partitionPairs.end(),
            fmt::format(
                "Feature reordering partition spec is missing a required key: {}",
                key));
        partitionValues.push_back(std::move(it->second));
      }

      auto partitionProperties = catalog.getPartitionStorageProperties(
          parts[0], parts[1], partitionValues);

      if (partitionProperties) {
        auto source = partitionProperties->getFeatureOrdering();
        if (!source.featureOrdering_ref()->empty()) {
          std::vector<std::tuple<size_t, std::vector<int64_t>>>
              featureReordering;
          featureReordering.reserve(source.featureOrdering_ref()->size());
          for (const auto& column : source.featureOrdering_ref().value()) {
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

          optionBuilder.withFeatureReordering(std::move(featureReordering));
        }
      }
    }
  }

  auto writerOptions = optionBuilder.build();
  writerOptions.useEncodingSelectionPolicy = FLAGS_use_encoding_selection;

  alpha::VeloxWriter writer{
      *memoryPool, reader.rowType(), std::move(file), std::move(writerOptions)};

  auto remainingRows = FLAGS_row_count;
  uint32_t flushCount = 0;
  velox::VectorPtr vector;
  uint64_t batchSize = std::min((uint64_t)FLAGS_batch_size, remainingRows);
  while (rowReader->next(batchSize, vector)) {
    if (writer.write(vector)) {
      ++flushCount;
    }

    remainingRows -= vector->size();
    if (remainingRows == 0) {
      break;
    }

    batchSize = std::min((uint64_t)FLAGS_batch_size, remainingRows);
  }

  writer.close();
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
