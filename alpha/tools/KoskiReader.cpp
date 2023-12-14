// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <folly/Singleton.h>
#include <glog/logging.h>
#include "common/init/light.h"
#include "common/strings/URL.h"
#include "data_preproc/if/gen-cpp2/types_types.h"
#include "data_preproc/logging/ManifoldLogger.h"
#include "dwio/alpha/common/StopWatch.h"
#include "dwio/common/ProcessMemoryTracker.h"
#include "folly/FileUtil.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/executors/task_queue/LifoSemMPMCQueue.h"
#include "koski/connect/dw/dw.h"
#include "koski/connect/dw/dwio/DwioTable.h"
#include "koski/core/ProjectOp.h"
#include "koski/core/QueryConfigKeys.h"
#include "koski/core/TableScanOp.h"
#include "koski/cpp_dataframe/Dataframe.h"
#include "koski/koski_tester/KoskiTester.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/serialization/Serializable.h"

using namespace ::facebook;

DEFINE_string(ns, "", "Namespace to read from");
DEFINE_string(table, "", "Table to read from");
DEFINE_string(
    partition,
    "",
    "Partition to use when reading data, in the format of key1=value1/key2=value2. (not used when reading from a DPP session)");
DEFINE_string(
    map,
    "",
    "Projection to use when reading data (not used when reading from a DPP session). For example: make_row_from_map(id_list_features, make_array(cast_to_integer(66175311)), make_array('a')) as projected");
DEFINE_string(session_id, "", "DPP session to use as input. (Optional)");
DEFINE_int64(row_limit, 100000, "Max rows to read");
DEFINE_bool(print_df, false, "Print the result dataframe content");
DEFINE_int32(
    split_thread_count,
    16,
    "Size of thread pool for split processing");
DEFINE_int32(koski_thread_count, 0, "Size of thread pool for split processing");
DEFINE_int32(
    alpha_decoding_threads,
    0,
    "Size of thread pool to use for parallel Velox vector decoding");
DEFINE_int32(
    alpha_io_threads,
    0,
    "Size of thread pool to use for parallel file system reads");

std::vector<std::tuple<std::string, std::string>> getPartitionPairs(
    const std::string& partition) {
  std::vector<std::string> segments;
  folly::split('/', partition, segments);

  std::vector<std::tuple<std::string, std::string>> partitionPairs(
      segments.size());
  for (auto i = 0; i < segments.size(); ++i) {
    std::vector<std::string> parts;
    folly::split('=', segments[i], parts);
    assert(parts.size() == 2);
    // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
    partitionPairs[i] = {parts[0], strings::URL::urlUnescape(parts[1])};
  }

  return partitionPairs;
}

int main(int argc, char* argv[]) {
  auto init = facebook::init::InitFacebookLight{&argc, &argv};

  std::shared_ptr<const koski::Dataframe> df = nullptr;
  if (!FLAGS_session_id.empty()) {
    LOG(INFO) << "Reading dataframe from session id: " << FLAGS_session_id;

    data_preproc::logging::ManifoldLogger manifoldLogger;
    auto sessionRequest =
        manifoldLogger.fetchCreateSessionRequest(FLAGS_session_id);

    df = koski::deserializeDataframe(
        sessionRequest.dataset_ref()
            ->get_koskiDataset()
            .get_serializedDataframe(),
        [](auto& builder) {
          builder.config(
              {{koski::core::kMaxSplitSizeInBytes, "268435456"},
               {koski::core::kBatchSizeHint, "1024"}});
        });
  } else {
    LOG(INFO) << "Reading table (direct): " << FLAGS_ns << ":" << FLAGS_table;

    df = koski::data_source::dataWarehouse(
        FLAGS_ns, FLAGS_table, koski::Dataframe::testCtx());

    if (!FLAGS_partition.empty()) {
      auto partitionPairs = getPartitionPairs(FLAGS_partition);
      std::vector<std::string> segments(partitionPairs.size());
      for (auto i = 0; i < partitionPairs.size(); ++i) {
        // Yes, we assume the values are strings and wrap them with quotes...
        // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
        segments[i] = std::get<0>(partitionPairs[i]) + "='" +
            std::get<1>(partitionPairs[i]) + "'";
      }
      auto filter = folly::join(" AND ", segments);

      LOG(INFO) << "Using filter: " << filter;
      df = df->filter(filter);
    }

    if (!FLAGS_map.empty()) {
      LOG(INFO) << "Using map: " << FLAGS_map;
      df = df->map({FLAGS_map});
    }
  }

  if (FLAGS_row_limit > 0) {
    df = df->limit(FLAGS_row_limit);
  }

  df = df->rebatch(1024, false);

  if (FLAGS_print_df) {
    df->print();
  }

  auto splits = df->splits();
  auto splitExecutor = std::make_shared<folly::CPUThreadPoolExecutor>(
      FLAGS_split_thread_count,
      std::make_unique<folly::LifoSemMPMCQueue<
          folly::CPUThreadPoolExecutor::CPUTask,
          folly::QueueBehaviorIfFull::BLOCK>>(FLAGS_split_thread_count),
      std::make_shared<folly::NamedThreadFactory>("Koski.S."));
  std::shared_ptr<folly::Executor> koskiExecutor;
  std::shared_ptr<folly::Executor> readerDecodingExecutor;
  std::shared_ptr<folly::Executor> readerIOExecutor;
  if (FLAGS_koski_thread_count > 0) {
    koskiExecutor = std::make_shared<folly::CPUThreadPoolExecutor>(
        FLAGS_koski_thread_count,
        std::make_unique<folly::LifoSemMPMCQueue<
            folly::CPUThreadPoolExecutor::CPUTask,
            folly::QueueBehaviorIfFull::BLOCK>>(FLAGS_koski_thread_count),
        std::make_shared<folly::NamedThreadFactory>("Koski.E."));
  }
  if (FLAGS_alpha_decoding_threads > 0) {
    readerDecodingExecutor = std::make_shared<folly::CPUThreadPoolExecutor>(
        FLAGS_alpha_decoding_threads,
        std::make_unique<folly::LifoSemMPMCQueue<
            folly::CPUThreadPoolExecutor::CPUTask,
            folly::QueueBehaviorIfFull::BLOCK>>(FLAGS_alpha_decoding_threads),
        std::make_shared<folly::NamedThreadFactory>("Alpha.D."));
  }
  if (FLAGS_alpha_io_threads > 0) {
    readerIOExecutor = std::make_shared<folly::CPUThreadPoolExecutor>(
        FLAGS_alpha_io_threads,
        std::make_unique<folly::LifoSemMPMCQueue<
            folly::CPUThreadPoolExecutor::CPUTask,
            folly::QueueBehaviorIfFull::BLOCK>>(FLAGS_alpha_io_threads),
        std::make_shared<folly::NamedThreadFactory>("Alpha.R."));
  }

  // Log resident memory every second
  folly::FunctionScheduler scheduler;
  std::atomic<uint64_t> rowCount = 0;
  std::atomic<uint64_t> batchCount = 0;
  std::atomic<uint64_t> splitCount = 0;
  scheduler.addFunction(
      [&batchCount]() {
        auto resident = dwio::common::ProcessMemoryTracker::residentAccurate();
        LOG(INFO) << "Resident Memory: " << resident
                  << ", Batch Count: " << batchCount;
      },
      std::chrono::seconds(1),
      "Resident memory log");
  scheduler.start();

  alpha::StopWatch sw;
  sw.start();

  std::string split;
  while (splits->next(split)) {
    {
      if (rowCount > FLAGS_row_limit) {
        break;
      }
    }
    splitExecutor->add([&, split] {
      koski::ExecutionOptions opts;
      opts.splits = df->toSplitProvider({split});
      opts.executor = koskiExecutor;
      opts.readerDecodingExecutor = readerDecodingExecutor;
      opts.readerIOExecutor = readerIOExecutor;
      uint64_t localBatchCount = 0;
      uint64_t localRowCount = 0;
      auto it = df->batchIterator(opts);
      for (auto i = it.begin(); i != it.end(); ++i) {
        localRowCount += i->size();
        ++localBatchCount;
      }
      batchCount += localBatchCount;
      rowCount += localRowCount;
      ++splitCount;
    });
  }

  splitExecutor->join();
  sw.stop();

  LOG(INFO) << "Total dataframe read time: " << sw.elapsedMsec()
            << "ms. Batch count: " << batchCount << ", Row count: " << rowCount
            << ", Split Count: " << splitCount;
}

// Initialize dummy Velox stats reporter
folly::Singleton<velox::BaseStatsReporter> reporter([]() {
  return new velox::DummyStatsReporter();
});
