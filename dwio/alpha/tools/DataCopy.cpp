// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <glog/logging.h>
#include "common/init/light.h"
#include "common/strings/UUID.h"
#include "dwio/catalog/Catalog.h"
#include "dwio/catalog/fbhive/HiveCatalog.h"
#include "dwio/catalog/impl/DefaultCatalog.h"
#include "warm_storage/client/File.h"

using namespace ::facebook;

DEFINE_string(source_ns, "", "Namespace to read from");
DEFINE_string(source_table, "", "Table to read from");
DEFINE_string(source_partition, "", "Source partition to read from");
DEFINE_string(target_ns, "", "Namespace to write to");
DEFINE_string(target_table, "", "Table to write to");
DEFINE_string(target_partition, "", "Target partition to write to");
DEFINE_string(query_id, "", "Query id to use when creating staging area");
DEFINE_int32(file_count, 20, "Number of files to copy");
DEFINE_int32(copy_buffer_mb, 512, "Buffer size (in MB) for copying files");
DEFINE_int32(copy_concurrency, 16, "Number of files to copy in parallel");

int main(int argc, char* argv[]) {
  auto init = facebook::init::InitFacebookLight{&argc, &argv};

  auto queryId = FLAGS_query_id;
  if (queryId.empty()) {
    queryId = strings::generateUUID();
  }

  LOG(INFO) << "Source table: " << FLAGS_source_ns << ":" << FLAGS_source_table
            << " [" << FLAGS_source_partition << "]";
  LOG(INFO) << "Target table: " << FLAGS_target_ns << ":" << FLAGS_target_table
            << " [" << FLAGS_target_partition << "]";
  LOG(INFO) << "Number of files to copy: " << FLAGS_file_count;
  LOG(INFO) << "Query id: " << queryId;

  dwio::common::request::AccessDescriptor ad =
      dwio::common::request::AccessDescriptorBuilder{}
          .withClientId("alpha.data.copy")
          .withNamespace(FLAGS_target_ns)
          .withTable(FLAGS_target_table)
          .withQueryId(queryId)
          .build();

  dwio::catalog::impl::DefaultCatalog catalog{ad};

  if (!catalog.existsTable(FLAGS_source_ns, FLAGS_source_table)) {
    LOG(FATAL) << "Source table doesn't exist.";
  }

  if (!catalog.existsTable(FLAGS_target_ns, FLAGS_target_table)) {
    LOG(FATAL) << "Target table doesn't exist.";
  }

  if (!catalog.existsPartition(
          FLAGS_source_ns, FLAGS_source_table, FLAGS_source_partition)) {
    LOG(FATAL) << "Source partition not found.";
  }

  LOG(INFO) << "Retreiving source partition info.";
  auto sourcePartition = catalog.getPartitionByName(
      FLAGS_source_ns, FLAGS_source_table, FLAGS_source_partition);

  auto& sourcePartitionSd =
      dynamic_cast<const dwio::catalog::fbhive::HiveStorageDescriptor&>(
          *sourcePartition->storageDescriptor());

  LOG(INFO) << "Source partition location: " << sourcePartitionSd.location();

  LOG(INFO) << "Retreiving target table metadata.";
  auto targetTable =
      catalog.getTableMetadata(FLAGS_target_ns, FLAGS_target_table);

  warm_storage::FSSessionOptions sessionOptions;
  sessionOptions.oncall = "dwios";
  auto fs = warm_storage::FileSystem::createFileSystem(
      "dwios.alpha",
      "dwio.alpha.data_copy",
      warm_storage::getDefaultFileSystemTimeoutConfig(),
      sessionOptions);

  LOG(INFO) << "Enumerating files in source location.";
  auto listStatsResult = fs->listStats(sourcePartitionSd.location());
  if (!listStatsResult.ok()) {
    LOG(FATAL) << "Unable to enumerate files in source partition.";
  }

  LOG(INFO) << "Creating staging area.";
  auto stagingArea = catalog.createStagingArea(ad);
  auto targetDirectory =
      dynamic_cast<const dwio::catalog::fbhive::HiveStorageDescriptor&>(
          *stagingArea)
          .location() +
      "/" + FLAGS_target_partition;
  LOG(INFO) << "Creating target directory: " << targetDirectory;
  fs->mkdir(targetDirectory);

  auto copyExecutor = std::make_shared<folly::CPUThreadPoolExecutor>(
      FLAGS_copy_concurrency,
      std::make_unique<folly::LifoSemMPMCQueue<
          folly::CPUThreadPoolExecutor::CPUTask,
          folly::QueueBehaviorIfFull::BLOCK>>(FLAGS_copy_concurrency),
      std::make_shared<folly::NamedThreadFactory>("Copy."));

  for (auto i = 0; i < listStatsResult.value().size() && i < FLAGS_file_count;
       ++i) {
    auto source = listStatsResult.value()[i].path;
    auto destination =
        targetDirectory + "/" + source.substr(source.find_last_of('/') + 1);

    copyExecutor->add([source, destination, &fs]() {
      LOG(INFO) << "Copying file from " << source << " to " << destination;

      auto openReadResult = fs->open(source, warm_storage::FileOpenMode::READ);
      if (!openReadResult.ok()) {
        LOG(FATAL) << "Unable to open source file for read.";
      }

      warm_storage::FileCreateOptions createOptions{.overwrite = true};
      auto openWriteResult = fs->create(destination, std::move(createOptions));
      if (!openWriteResult.ok()) {
        LOG(FATAL) << "Unable to open target file for write.";
      }

      std::vector<char> buffer(FLAGS_copy_buffer_mb * 1024 * 1024);
      off_t offset = 0;
      while (true) {
        auto readResult =
            openReadResult.value()->pread(offset, buffer.size(), buffer.data());
        if (!readResult.ok()) {
          LOG(FATAL) << "Unable to read from source file.";
        }
        auto bytesRead = readResult.value();
        LOG(INFO) << "Read " << bytesRead << " bytes.";
        if (bytesRead == 0) {
          break;
        }

        auto writeResult =
            openWriteResult.value()->pwrite(offset, bytesRead, buffer.data());
        if (!writeResult.ok()) {
          LOG(FATAL) << "Unable to write to target file.";
        }

        offset += bytesRead;
      }

      auto closeResult = openWriteResult.value()->close();
      if (!closeResult.ok()) {
        LOG(FATAL) << "Unable to close target file.";
      }
    });
  }

  copyExecutor->join();

  LOG(INFO) << "Copy done.";

  Apache::Hadoop::Hive::Partition ret;
  ret.dbName() = FLAGS_target_ns;
  ret.tableName() = FLAGS_target_table;
  ret.partitionName() = FLAGS_target_partition;
  std::vector<std::string> vals;
  for (auto& pair : dwio::catalog::fbhive::HiveCatalog::parsePartitionKeyValues(
           FLAGS_target_partition)) {
    vals.push_back(pair.second);
  }
  ret.values() = std::move(vals);

  auto& htm = dynamic_cast<const dwio::catalog::fbhive::HiveTableMetadata&>(
      *targetTable);
  auto sd = folly::copy(htm.sd());
  sd.location() = targetDirectory;
  ret.sd() = std::move(sd);

  auto newPartition =
      std::make_shared<dwio::catalog::fbhive::HivePartitionMetadata>(
          ad, targetTable, ret, nullptr);

  if (catalog.existsPartition(
          FLAGS_target_ns, FLAGS_target_table, FLAGS_target_partition)) {
    catalog.alterPartitions({newPartition});
  } else {
    catalog.addPartitions({newPartition});
  }

  LOG(INFO) << "Partition committed.";
}
