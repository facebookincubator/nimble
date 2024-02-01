// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <fmt/chrono.h>
#include <glog/logging.h>
#include <ctime>

#include "common/files/FileUtil.h"
#include "common/init/light.h"
#include "dwio/catalog/Catalog.h"
#include "dwio/catalog/fbhive/HiveCatalog.h"
#include "dwio/catalog/impl/DefaultCatalog.h"
#include "folly/portability/Stdlib.h"
#include "warm_storage/client/File.h"

using namespace ::facebook;

DEFINE_string(ns, "", "Namespace to read from.");
DEFINE_string(table, "", "Table to read from.");
DEFINE_string(partition_filter, "", "partition filter to apply.");
DEFINE_bool(run, false, "Performs a deep analysis of partitions.");
DEFINE_int32(concurrency, 4096, "Number of files to verify in parallel.");
DEFINE_string(
    status_file,
    "",
    "Use status tracking file. Providing the same file across runs will skip successful partitions.");
DEFINE_uint32(
    time_window_hours,
    4,
    "Time window (in hours) to use, "
    "if partition filter is not provided. Assumes that the table has "
    "'ds' and 'ts' partition keys. If not, use partition_filter to "
    "provide an alternate filter.");
DEFINE_bool(enable_logs, false, "Enable GLOG messages (off by default).");

namespace {
constexpr uint16_t kAlphaMagicNumber = 0xA1FA;
constexpr char kPartitionStatusSeparator = '\n';
} // namespace

#define RED "\033[31m"
#define GREEN "\033[32m"
#define YELLOW "\033[33m"
#define BLUE "\033[34m"
#define PURPLE "\033[35m"
#define CYAN "\033[36m"
#define RESET_COLOR "\033[0m"

#define PRINT_ERROR(stream) \
  std::cerr << "--> " << RED << "Error: " << stream << RESET_COLOR << std::endl;

#define PRINT(stream) std::cout << stream << RESET_COLOR << std::endl;

int main(int argc, char* argv[]) {
  if (!FLAGS_enable_logs) {
    FLAGS_minloglevel = 6;
    auto level = folly::to<std::string>(folly::LogLevel::ERR);
    setenv(folly::kLoggingEnvVarName, level.c_str(), /* overwrite */ 0);
  }

  auto init = facebook::init::InitFacebookLight{&argc, &argv};

  if (FLAGS_partition_filter.empty()) {
    time_t now = std::time(nullptr);
    std::tm tm;
    localtime_r(&now, &tm);
    tm.tm_hour -= FLAGS_time_window_hours;
    now = std::mktime(&tm);
    FLAGS_partition_filter = fmt::format(
        "ds >= '{0:%Y-%m-%d}' AND ts >= '{0:%Y-%m-%d+%H:00:99}'",
        fmt::gmtime(now));
  }

  PRINT(
      "Table: " << CYAN << FLAGS_ns << ":" << FLAGS_table << RESET_COLOR << " ["
                << FLAGS_partition_filter << "]");

  dwio::common::request::AccessDescriptor ad =
      dwio::common::request::AccessDescriptorBuilder{}
          .withClientId("alpha.table.verifier")
          .withNamespace(FLAGS_ns)
          .withTable(FLAGS_table)
          .build();

  dwio::catalog::impl::DefaultCatalog catalog{ad};

  if (!catalog.existsTable(FLAGS_ns, FLAGS_table)) {
    PRINT_ERROR("Table doesn't exist.");
    return 1;
  }

  auto partitions = catalog.getPartitionsByFilter(
      FLAGS_ns, FLAGS_table, FLAGS_partition_filter, 1024);
  if (partitions.empty()) {
    PRINT_ERROR("Partition filter returned no partitions.");
    return 1;
  }

  if (!FLAGS_run) {
    for (const auto& partition : partitions) {
      auto& hivePartition =
          dynamic_cast<const dwio::catalog::fbhive::HivePartitionMetadata&>(
              *partition);

      bool isAlpha = hivePartition.sd().inputFormat_ref().value() ==
          "com.facebook.alpha.AlphaInputFormat";

      PRINT(
          (isAlpha ? GREEN "Alpha" RESET_COLOR ": "
                   : PURPLE "ORC" RESET_COLOR ":   ")
          << partition->partitionName());
    }

    PRINT(
        YELLOW
        << "********************************************************************************");
    PRINT(
        YELLOW
        << "* This is a PRINT ONLY run. To actually verify the content of partitions, run  *");
    PRINT(
        YELLOW
        << "* again using the --run argument.                                              *");
    PRINT(
        YELLOW
        << "********************************************************************************");

    return 0;
  }

  std::unordered_set<std::string> successfulPartitions;
  if (!FLAGS_status_file.empty()) {
    if (files::FileUtil::fileExists(FLAGS_status_file)) {
      std::string content;
      files::FileUtil::readFileToString(FLAGS_status_file, &content);
      std::vector<std::string_view> lines;
      folly::split(
          kPartitionStatusSeparator, content, lines, /* ignoreEmpty */ true);
      for (const auto& line : lines) {
        successfulPartitions.emplace(line);
      }
      PRINT(
          "Loaded " << successfulPartitions.size()
                    << " partitions from the status file "
                    << FLAGS_status_file);
    } else {
      if (!files::FileUtil::touch(FLAGS_status_file)) {
        PRINT_ERROR(
            "Unable to create status file "
            << FLAGS_status_file
            << ". Verify that the directory exists and that the file is not locked by another process.");
        return 1;
      }
    }
  }

  bool isError = false;
  for (const auto& partition : partitions) {
    if (successfulPartitions.contains(partition->partitionName())) {
      PRINT(
          PURPLE << "Skipping partition '" << partition->partitionName()
                 << "' (successfully scanned before)");
      continue;
    }
    auto& partitionSd =
        dynamic_cast<const dwio::catalog::fbhive::HivePartitionMetadata&>(
            *partition)
            .sd();

    // Verify partition metadata consistency
    bool isAlpha = partitionSd.inputFormat_ref().value() ==
        "com.facebook.alpha.AlphaInputFormat";
    if (isAlpha) {
      if (partitionSd.outputFormat_ref().value() !=
              "com.facebook.alpha.AlphaOutputFormat" ||
          partitionSd.serdeInfo_ref()->serializationLib_ref().value() !=
              "com.facebook.alpha.AlphaSerde") {
        PRINT_ERROR(
            "Invalid partition metadata for Alpha partition: "
            << partition->partitionName());
        continue;
      }
    } else {
      if (partitionSd.inputFormat_ref().value() !=
              "com.facebook.hive.orc.OrcInputFormat" ||
          partitionSd.outputFormat_ref().value() !=
              "com.facebook.hive.orc.OrcOutputFormat" ||
          partitionSd.serdeInfo_ref()->serializationLib_ref().value() !=
              "com.facebook.hive.orc.OrcSerde") {
        PRINT_ERROR(
            "Invalid partition metadata for ORC partition: "
            << partition->partitionName());
        continue;
      }
    }

    warm_storage::FSSessionOptions sessionOptions;
    sessionOptions.oncall = "dwios";
    auto fs = warm_storage::FileSystem::createFileSystem(
        "dwios.alpha",
        "dwio.alpha.partition_verifier",
        warm_storage::getDefaultFileSystemTimeoutConfig(),
        sessionOptions);

    LOG(INFO) << "Enumerating files in partition location.";
    auto listStatsResult = fs->listStats(partitionSd.location_ref().value());
    if (!listStatsResult.ok()) {
      PRINT_ERROR("Unable to enumerate files in partition.");
      continue;
    }

    auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
        FLAGS_concurrency,
        std::make_unique<folly::LifoSemMPMCQueue<
            folly::CPUThreadPoolExecutor::CPUTask,
            folly::QueueBehaviorIfFull::BLOCK>>(FLAGS_concurrency),
        std::make_shared<folly::NamedThreadFactory>("Verify."));

    std::atomic<uint32_t> alphaFileCount = 0;
    std::atomic<uint32_t> otherFileCount = 0;
    uint32_t processedFileCount = 0;

    for (auto i = 0; i < listStatsResult.value().size(); ++i) {
      auto file = listStatsResult.value()[i].path;
      std::vector<std::string> parts;
      folly::split('/', file, parts);
      if (!parts.empty() && parts[parts.size() - 1].starts_with(".")) {
        std::cout << "Skipping file: " << file << std::endl;
        continue;
      }

      executor->add([file, &alphaFileCount, &otherFileCount, &fs]() {
        LOG(INFO) << "Verifying file " << file << "...";

        auto openReadResult = fs->open(file, warm_storage::FileOpenMode::READ);
        if (!openReadResult.ok()) {
          LOG(FATAL) << "Unable to open file for read.";
        }

        auto sizeResult = openReadResult.value()->size();
        if (!sizeResult.ok()) {
          LOG(FATAL) << "Unable to retrieve file size.";
        }

        if (sizeResult.value() < sizeof(kAlphaMagicNumber)) {
          LOG(FATAL) << "File is corrupted.";
        }

        // To identify the file type, we open the file, and look for the Alpha
        // magic number. DWRF doesn't have a magic number, if we we can't find
        // the Alpha magic number we assume this is a DWRF file. Note: DWRF
        // files should always have at least 2 bytes, as the post-scripts must
        // be bigger than that.
        std::vector<char> buffer(sizeof(kAlphaMagicNumber));
        auto readResult = openReadResult.value()->pread(
            sizeResult.value() - sizeof(kAlphaMagicNumber),
            buffer.size(),
            buffer.data());
        if (!readResult.ok()) {
          LOG(FATAL) << "Unable to read from file.";
        }

        auto bytesRead = readResult.value();
        if (bytesRead != buffer.size()) {
          LOG(FATAL) << "Unable to read buffer from file.";
        }

        if (*reinterpret_cast<const uint16_t*>(buffer.data()) ==
            kAlphaMagicNumber) {
          ++alphaFileCount;
        } else {
          ++otherFileCount;
        }
      });

      ++processedFileCount;
    }

    executor->join();

    LOG(INFO) << "Done loading all files. Total files: " << processedFileCount
              << ", Alpha files: " << alphaFileCount
              << ", Non-Alpha files: " << otherFileCount;

    if (processedFileCount != (alphaFileCount + otherFileCount)) {
      PRINT_ERROR(fmt::format(
          "File count mismatch. Expected {}, actual {}",
          processedFileCount,
          alphaFileCount + otherFileCount));
      continue;
    }

    auto printResult = [](const std::string& partitionName,
                          const std::string& partitionType,
                          uint32_t otherFileCount,
                          uint32_t totalFileCount) {
      if (otherFileCount > 0) {
        PRINT(
            RED << "Error:" << RESET_COLOR << " Partition " << partitionName
                << " is an " << partitionType << " partition but contains "
                << otherFileCount << " non-" << partitionType
                << " files out of " << totalFileCount << " files.");
        return false;
      } else {
        PRINT(
            GREEN << "Success:" << RESET_COLOR << " Partition " << partitionName
                  << " is a valid " << partitionType << " partition.");
        return true;
      }
    };

    if (printResult(
            partition->partitionName(),
            isAlpha ? "Alpha" : "ORC",
            isAlpha ? otherFileCount : alphaFileCount,
            listStatsResult.value().size())) {
      if (!FLAGS_status_file.empty()) {
        files::appendStringToFileOrDie(
            partition->partitionName() + kPartitionStatusSeparator,
            FLAGS_status_file.c_str());
      }
    }
  }
}
